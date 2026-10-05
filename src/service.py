import hashlib
from uuid import uuid4

from .audit import AuditTrail
from .domain import ConflictError, NotFoundError, PermissionDenied, ValidationError
from .rules import RuleEngine


class DomainService:
    def __init__(self, repository, rules=None):
        self.repository = repository
        self.rules = rules or RuleEngine()
        self.audit = AuditTrail(repository)

    def _lookup(self, kind, field, value):
        return self.repository.find_entities(self.rules.normalize_kind(kind), field, value)

    def health(self):
        return {"status": "ok" if self.repository.ping() else "error"}

    def create(self, actor, kind, data, idempotency_key=None, status_override=None):
        kind = self.rules.normalize_kind(kind)
        payload = dict(data or {})
        if idempotency_key:
            existing = self.repository.get_idempotency(actor.user_id, idempotency_key)
            if existing:
                entity = self.repository.get_entity(existing)
                if entity:
                    return entity
        self.rules.validate_create(actor, kind, payload, self._lookup)
        entity_id = str(payload.pop("id", "") or uuid4())
        if self.repository.get_entity(entity_id):
            raise ConflictError("entity already exists: " + entity_id)
        status = status_override or self.rules.initial_status(kind, payload)
        entity = self.repository.create_entity(entity_id, kind, status, payload, actor.user_id)
        self.audit.record(entity_id, actor, "create", None, status, {"kind": kind})
        if idempotency_key:
            self.repository.save_idempotency(actor.user_id, idempotency_key, entity_id)
        return entity

    def transition(self, actor, entity_id, action, data=None, expected_version=None):
        entity = self.repository.get_entity(entity_id)
        if not entity:
            raise NotFoundError("entity not found: " + entity_id)
        expected = int(expected_version) if expected_version is not None else entity["version"]
        if expected_version is not None and int(entity["version"]) != expected:
            raise ConflictError(
                "version conflict: expected %s, found %s" % (expected, entity["version"])
            )
        next_status, patch = self.rules.validate_transition(
            actor, entity, action, dict(data or {}), self._lookup
        )
        merged = dict(entity["data"])
        merged.update(patch)
        updated = self.repository.update_entity(entity_id, expected, next_status, merged)
        self.audit.record(
            entity_id,
            actor,
            action,
            entity["status"],
            updated["status"],
            {"patch": patch},
        )
        if entity["kind"] == "telemetry" and action == "revise":
            self._invalidate_interpolations_for_telemetry(actor, updated)
        return updated

    def merge_offline(self, actor, records):
        """Merge field records by a stable (source_id, record_id) identity."""
        if not isinstance(records, list):
            raise ValidationError("records must be a list")
        created = []
        for raw in records:
            if not isinstance(raw, dict):
                raise ValidationError("each offline record must be an object")
            source_id = str(raw.get("source_id", "")).strip()
            record_id = str(raw.get("record_id", "")).strip()
            if not source_id or not record_id:
                raise ValidationError("source_id and record_id are required")
            digest = hashlib.sha256((source_id + "\0" + record_id).encode("utf-8")).hexdigest()[:32]
            entity_id = "offline-" + digest
            existing = self.repository.get_entity(entity_id)
            if existing:
                created.append(existing)
                continue
            payload = dict(raw)
            self.rules.validate_create(actor, "offline_record", payload, self._lookup)
            entity = self.repository.create_entity(
                entity_id,
                "offline_record",
                self.rules.initial_status("offline_record", payload),
                payload,
                actor.user_id,
            )
            self.audit.record(entity_id, actor, "merge_offline", None, entity["status"], {"source_id": source_id, "record_id": record_id})
            created.append(entity)
        return created

    def get(self, entity_id):
        entity = self.repository.get_entity(entity_id)
        if not entity:
            raise NotFoundError("entity not found: " + entity_id)
        return entity

    def list(self, kind=None, status=None):
        if kind:
            kind = self.rules.normalize_kind(kind)
        return self.repository.list_entities(kind=kind, status=status)

    def audit_log(self, entity_id=None):
        return self.repository.list_audit(entity_id=entity_id)

    _INGEST_ROLES = ("admin", "operator", "engineer")

    def _ensure_ingest_role(self, actor):
        if actor.role not in self._INGEST_ROLES:
            raise PermissionDenied("role %s is not allowed to ingest telemetry" % actor.role)

    def _effective_capacity(self, link):
        status = link["status"]
        capacity = float(link["data"].get("capacity", 0) or 0)
        if status in ("up", "backup_active"):
            return int(capacity)
        if status == "degraded":
            factor = float(link["data"].get("degraded_factor", 0.5) or 0.5)
            return max(1, int(capacity * factor))
        return 0

    @staticmethod
    def _window_key(observed_at):
        return str(observed_at or "")[:13]

    def _next_seq(self, link_id):
        max_seq = 0
        for item in self.repository.list_entities(kind="telemetry"):
            if item["data"].get("link_id") == link_id:
                try:
                    max_seq = max(max_seq, int(item["data"].get("seq", 0)))
                except (TypeError, ValueError):
                    pass
        return max_seq + 1

    @staticmethod
    def _dedupe_key(link_id, record):
        source_id = record.get("source_id")
        record_id = record.get("record_id")
        if source_id not in (None, "") and record_id not in (None, ""):
            return "src:%s:%s" % (source_id, record_id)
        return "tel:%s:%s:%s:%s" % (
            link_id,
            record.get("asset_id"),
            record.get("metric"),
            record.get("observed_at"),
        )

    def _find_by_dedupe(self, dedupe_key):
        for item in self.repository.list_entities(kind="telemetry"):
            if item["data"].get("dedupe_key") == dedupe_key:
                return item
        return None

    def _accepted_in_window(self, link_id, window):
        count = 0
        for item in self.repository.list_entities(kind="telemetry"):
            if item["status"] != "current":
                continue
            if item["data"].get("link_id") != link_id:
                continue
            if self._window_key(item["data"].get("observed_at")) == window:
                count += 1
        return count

    def _backlog_size(self, link_id):
        return sum(
            1
            for item in self.repository.list_entities(kind="telemetry")
            if item["status"] == "queued" and item["data"].get("link_id") == link_id
        )

    def _find_or_create_incident_for_asset(self, actor, asset_id, link):
        for incident in self.repository.list_entities(kind="incident"):
            if incident["status"] in ("open", "diagnosing", "recovery_planned", "recovering"):
                if incident["data"].get("asset_id") == asset_id:
                    return incident
        data = {
            "station_id": link["data"].get("station_id"),
            "asset_id": asset_id,
            "link_id": link["id"],
            "kind": "data_gap",
            "severity": "high" if link["status"] == "down" else "medium",
            "summary": "data gap while link %s" % link["status"],
            "auto_created": True,
        }
        return self.create(actor, "incident", data)

    def _retain_gap_for_record(self, actor, link, record, window):
        asset_id = record.get("asset_id")
        incident = self._find_or_create_incident_for_asset(actor, asset_id, link)
        lost = {
            "dedupe_key": record.get("dedupe_key"),
            "source_id": record.get("source_id"),
            "record_id": record.get("record_id"),
            "observed_at": record.get("observed_at"),
            "window": window,
        }
        gap_data = {
            "incident_id": incident["id"],
            "asset_id": asset_id,
            "link_id": link["id"],
            "start_at": record.get("observed_at"),
            "end_at": record.get("observed_at"),
            "metric": record.get("metric"),
            "reason": "capacity_insufficient",
            "lost_records": [lost],
        }
        gap = self.create(actor, "gap", gap_data)
        self.audit.record(gap["id"], actor, "retain_gap", None, gap["status"], {"link_id": link["id"], "window": window})
        return gap

    def ingest_telemetry(self, actor, link_id, records):
        self._ensure_ingest_role(actor)
        link = self.repository.get_entity(link_id)
        if not link or link["kind"] != "link":
            raise NotFoundError("link not found: " + str(link_id))
        if not isinstance(records, list):
            raise ValidationError("records must be a list")
        effective = self._effective_capacity(link)
        backlog_limit = int(link["data"].get("backlog_limit", 10) or 10)
        result = {"accepted": [], "queued": [], "gaps": [], "duplicates": []}
        for raw in records:
            if not isinstance(raw, dict):
                raise ValidationError("each telemetry record must be an object")
            record = dict(raw)
            record["link_id"] = link_id
            key = self._dedupe_key(link_id, record)
            record["dedupe_key"] = key
            existing = self._find_by_dedupe(key)
            if existing:
                result["duplicates"].append(existing)
                continue
            record["seq"] = self._next_seq(link_id)
            window = self._window_key(record.get("observed_at"))
            accepted = self._accepted_in_window(link_id, window)
            backlog = self._backlog_size(link_id)
            if effective > 0 and accepted < effective:
                entity = self.create(actor, "telemetry", record)
                result["accepted"].append(entity)
            elif backlog < backlog_limit:
                entity = self.create(actor, "telemetry", record, status_override="queued")
                result["queued"].append(entity)
            else:
                gap = self._retain_gap_for_record(actor, link, record, window)
                result["gaps"].append(gap)
        return result

    def retransmit(self, actor, link_id, batch_size=None):
        self._ensure_ingest_role(actor)
        link = self.repository.get_entity(link_id)
        if not link or link["kind"] != "link":
            raise NotFoundError("link not found: " + str(link_id))
        effective = self._effective_capacity(link)
        if batch_size is None:
            batch_size = effective if effective > 0 else 0
        else:
            batch_size = int(batch_size)
        cursor = int(link["data"].get("backlog_cursor", 0) or 0)
        queued = sorted(
            (
                item
                for item in self.repository.list_entities(kind="telemetry")
                if item["status"] == "queued" and item["data"].get("link_id") == link_id
            ),
            key=lambda item: int(item["data"].get("seq", 0) or 0),
        )
        transmitted = []
        if effective > 0 and batch_size > 0:
            for item in queued:
                if len(transmitted) >= batch_size:
                    break
                seq = int(item["data"].get("seq", 0) or 0)
                if seq <= cursor:
                    continue
                key = item["data"].get("dedupe_key")
                existing = self._find_by_dedupe(key)
                if existing and existing["id"] != item["id"] and existing["status"] == "current":
                    self.repository.update_entity(item["id"], item["version"], "duplicate", dict(item["data"]))
                    self.audit.record(item["id"], actor, "dedupe_skip", "queued", "duplicate", {"dedupe_key": key})
                    continue
                self.repository.update_entity(item["id"], item["version"], "current", dict(item["data"]))
                self.audit.record(item["id"], actor, "retransmit", "queued", "current", {"link_id": link_id, "seq": seq})
                transmitted.append(item["id"])
                cursor = seq
        remaining = sum(1 for item in queued if int(item["data"].get("seq", 0) or 0) > cursor)
        link_data = dict(link["data"])
        link_data["backlog_cursor"] = cursor
        self.repository.update_entity(link_id, link["version"], link["status"], link_data)
        return {"transmitted": transmitted, "remaining": remaining, "cursor": cursor}

    def _invalidate_recovery_actions(self, actor, incident_id, revision):
        for action in self.repository.list_entities(kind="recovery_action"):
            if action["data"].get("incident_id") != incident_id:
                continue
            if action["status"] == "cancelled":
                continue
            data = dict(action["data"])
            data["basis_outdated"] = True
            data["invalidated_by_revision"] = revision
            self.repository.update_entity(action["id"], action["version"], "proposed", data)
            self.audit.record(
                action["id"], actor, "invalidate_basis", action["status"], "proposed",
                {"revision": revision},
            )

    def _invalidate_incident_conclusion(self, actor, incident_id, revision):
        incident = self.repository.get_entity(incident_id)
        if not incident:
            return
        if incident["status"] not in ("resolved", "closed"):
            return
        data = dict(incident["data"])
        data["resolution_invalidated"] = True
        data["invalidated_by_revision"] = revision
        self.repository.update_entity(incident["id"], incident["version"], "open", data)
        self.audit.record(
            incident["id"], actor, "invalidate_conclusion", incident["status"], "open",
            {"revision": revision},
        )

    def _invalidate_interpolations_for_telemetry(self, actor, telemetry):
        asset_id = telemetry["data"].get("asset_id")
        try:
            new_revision = int(telemetry["data"].get("revision", 0) or 0)
        except (TypeError, ValueError):
            return []
        if new_revision <= 0:
            return []
        affected = []
        for gap in self.repository.list_entities(kind="gap"):
            if gap["status"] not in ("filled", "accepted"):
                continue
            if gap["data"].get("asset_id") != asset_id:
                continue
            interp = gap["data"].get("interpolation") or {}
            try:
                based_on = int(interp.get("revision", 0) or 0)
            except (TypeError, ValueError):
                based_on = 0
            if based_on >= new_revision:
                continue
            data = dict(gap["data"])
            data["outdated"] = True
            data["invalidated_by_revision"] = new_revision
            data["previous_interpolation"] = interp
            updated_gap = self.repository.update_entity(gap["id"], gap["version"], "open", data)
            self.audit.record(
                gap["id"], actor, "invalidate_interpolation", gap["status"], "open",
                {"revision": new_revision, "based_on": based_on},
            )
            affected.append(updated_gap)
            incident_id = gap["data"].get("incident_id")
            self._invalidate_recovery_actions(actor, incident_id, new_revision)
            self._invalidate_incident_conclusion(actor, incident_id, new_revision)
        tdata = dict(telemetry["data"])
        tdata["invalidated_gaps"] = [item["id"] for item in affected]
        self.repository.update_entity(telemetry["id"], telemetry["version"], telemetry["status"], tdata)
        return affected
