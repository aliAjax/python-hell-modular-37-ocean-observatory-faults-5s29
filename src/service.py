import hashlib
from uuid import uuid4

from .audit import AuditTrail
from .domain import (
    ConflictError,
    NotFoundError,
    PermissionDenied,
    TransferError,
    ValidationError,
)
from .rules import RuleEngine


# Roles allowed to push telemetry and backfill streams.
STREAM_ROLES = {"admin", "operator", "engineer"}


class DomainService:
    def __init__(self, repository, rules=None):
        self.repository = repository
        self.rules = rules or RuleEngine()
        self.audit = AuditTrail(repository)

    def _lookup(self, kind, field, value):
        return self.repository.find_entities(self.rules.normalize_kind(kind), field, value)

    def health(self):
        return {"status": "ok" if self.repository.ping() else "error"}

    def create(self, actor, kind, data, idempotency_key=None):
        kind = self.rules.normalize_kind(kind)
        payload = dict(data or {})
        if idempotency_key:
            existing = self.repository.get_idempotency(actor.user_id, idem_key=idempotency_key)
            if existing:
                entity = self.repository.get_entity(existing)
                if entity:
                    return entity
        self.rules.validate_create(actor, kind, payload, self._lookup)
        entity_id = str(payload.pop("id", "") or uuid4())
        if self.repository.get_entity(entity_id):
            raise ConflictError("entity already exists: " + entity_id)
        status = self.rules.initial_status(kind, payload)
        entity = self.repository.create_entity(entity_id, kind, status, payload, actor.user_id)
        self.audit.record(entity_id, actor, "create", None, status, {"kind": kind})
        return entity

    def _store(self, actor, kind, entity_id, status, payload, action, detail=None):
        """Create an entity directly at a chosen status (internal pipeline use)."""
        if self.repository.get_entity(entity_id):
            return self.repository.get_entity(entity_id)
        entity = self.repository.create_entity(entity_id, kind, status, payload, actor.user_id)
        self.audit.record(entity_id, actor, action, None, status, detail or {"kind": kind})
        return entity

    def transition(self, actor, entity_id, action, data=None, expected_version=None):
        entity = self.repository.get_entity(entity_id)
        if not entity:
            raise NotFoundError("entity not found: " + entity_id)
        kind = self.rules.normalize_kind(entity["kind"])
        if kind in self.rules.OPTIMISTIC_TRANSITIONS and expected_version is None:
            # Two operators editing the same gap/mission must not both succeed:
            # the caller has to state the version it read.
            raise ConflictError(
                "%s transitions require expected_version for optimistic concurrency" % kind
            )
        expected = int(expected_version) if expected_version is not None else entity["version"]
        if kind in self.rules.OPTIMISTIC_TRANSITIONS:
            # Verify the optimistic lock before any rule evaluation so the loser
            # of a concurrent edit sees the version conflict directly.
            current = self.repository.get_entity(entity_id)
            if current["version"] != expected:
                raise ConflictError(
                    "version conflict: expected %s, found %s" % (expected, current["version"])
                )
        if kind == "backfill" and action == "push_batch":
            return self._push_backfill(actor, entity, dict(data or {}))
        updated = self._apply_transition(actor, entity, action, dict(data or {}), expected)
        self._after_transition(actor, entity, updated, action, dict(data or {}))
        return updated

    def _apply_transition(self, actor, entity, action, data, expected_version):
        next_status, patch = self.rules.validate_transition(
            actor, entity, action, data, self._lookup
        )
        merged = dict(entity["data"])
        merged.update(patch)
        updated = self.repository.update_entity(entity["id"], expected_version, next_status, merged)
        self.audit.record(
            entity["id"],
            actor,
            action,
            entity["status"],
            updated["status"],
            {"patch": patch},
        )
        return updated

    # ------------------------------------------------------------------
    # Link degradation -> telemetry queue -> capacity overflow -> gaps
    # ------------------------------------------------------------------
    def ingest_telemetry(self, actor, link_id, samples):
        """Push telemetry samples through a link.

        up / backup_active: delivered immediately.
        degraded: delivered while degraded_capacity allows, the rest queue;
                  once the queue length reaches link capacity the overflow is
                  dropped and recorded as a data gap.
        down: nothing moves; every sample is recorded as gap overflow.
        """
        if actor.role not in STREAM_ROLES:
            raise PermissionDenied("role %s cannot ingest telemetry" % actor.role)
        if not isinstance(samples, list) or not samples:
            raise ValidationError("samples must be a non-empty list")
        link = self.repository.get_entity(link_id)
        if not link or link["kind"] != "link":
            raise NotFoundError("link not found: " + str(link_id))
        delivered, queued, gap = [], [], None
        capacity = int(float(link["data"].get("capacity", 0)))
        degraded_capacity_raw = link["data"].get("degraded_capacity")
        degraded_capacity = int(float(degraded_capacity_raw)) if degraded_capacity_raw not in (None, "") else 0
        link_status = link["status"]
        # Queue position must advance within this batch as samples are stored;
        # re-querying per sample would always see the pre-batch length.
        queue_position = self._queued_count(link["id"]) if link_status == "degraded" else 0
        delivered_credits = degraded_capacity if link_status == "degraded" else 0
        for raw in samples:
            sample = dict(raw or {})
            self._validate_sample(sample, link)
            revision = self._next_revision(sample["asset_id"], sample["metric"])
            sample["revision"] = revision
            if link_status in ("up", "backup_active"):
                delivered.append(self._store_telemetry(actor, sample, "current", "ingest_delivered", {"link_id": link_id, "link_status": link_status}))
                continue
            if link_status == "degraded":
                if queue_position < capacity:
                    # Samples up to degraded throughput go out immediately; the
                    # rest are buffered until the queue (full capacity minus
                    # degraded throughput already used) is full.
                    if delivered_credits > 0:
                        delivered_credits -= 1
                        delivered.append(self._store_telemetry(actor, sample, "current", "ingest_delivered", {"link_id": link["id"], "link_status": link_status}))
                    else:
                        queue_position += 1
                        payload = dict(sample)
                        payload["queued_on_link"] = link["id"]
                        queued.append(self._store_telemetry(actor, payload, "queued", "ingest_queued", {"link_id": link["id"]}))
                else:
                    # Queue full: capacity is exhausted and the missing sample
                    # is retained explicitly as a data gap.
                    gap = self._record_overflow_gap(actor, link, sample)
                continue
            # down: link carries nothing; the missing sample leaves a gap.
            gap = self._record_overflow_gap(actor, link, sample)
        return {
            "link_id": link["id"],
            "link_status": link_status,
            "delivered": [item["id"] for item in delivered],
            "queued": [item["id"] for item in queued],
            "gap_id": gap["id"] if gap else None,
        }

    def _validate_sample(self, sample, link):
        for field in ("asset_id", "metric", "value", "observed_at"):
            if sample.get(field) in (None, ""):
                raise ValidationError("telemetry sample missing field: " + field)
        try:
            float(sample["value"])
        except (TypeError, ValueError):
            raise ValidationError("telemetry value must be numeric")
        asset = self.repository.get_entity(sample["asset_id"])
        if not asset or asset["kind"] != "asset":
            raise ValidationError("telemetry sample references unknown asset")

    def _next_revision(self, asset_id, metric):
        existing = self._lookup("telemetry", "*", None) or []
        revisions = [
            int(item["data"].get("revision", 0))
            for item in existing
            if item["data"].get("asset_id") == asset_id and item["data"].get("metric") == metric
        ]
        return (max(revisions) + 1) if revisions else 1

    def _queued_count(self, link_id):
        return len([
            item
            for item in self.repository.list_entities(kind="telemetry", status="queued")
            if item["data"].get("queued_on_link") == link_id
        ])

    def _telemetry_entity_id(self, sample):
        explicit = sample.get("id")
        if explicit:
            return str(explicit)
        digest = hashlib.sha256(
            (str(sample["asset_id"]) + "\0" + str(sample["metric"]) + "\0" + str(sample["observed_at"]) + "\0" + str(sample.get("record_seq", ""))).encode("utf-8")
        ).hexdigest()[:24]
        return "telemetry-" + digest

    def _store_telemetry(self, actor, sample, status, action, detail=None):
        payload = {key: value for key, value in sample.items() if key != "id"}
        entity_id = self._telemetry_entity_id(sample)
        existing = self.repository.get_entity(entity_id)
        if existing:
            return existing
        return self._store(actor, "telemetry", entity_id, status, payload, action, detail)

    def _record_overflow_gap(self, actor, link, sample):
        """Extend an existing open overflow gap for this link, or open a new one."""
        gaps = self._lookup("gap", "*", None) or []
        open_gap = next(
            (
                g for g in gaps
                if g["status"] in ("open", "estimated")
                and g["data"].get("source") == "link_overflow"
                and g["data"].get("link_id") == link["id"]
                and g["data"].get("asset_id") == sample["asset_id"]
            ),
            None,
        )
        incident = self._active_incident_for(sample["asset_id"], link["id"])
        if open_gap:
            data = dict(open_gap["data"])
            data["end_at"] = max(str(data.get("end_at", "")), str(sample["observed_at"]))
            data["missing_samples"] = int(data.get("missing_samples", 0)) + 1
            updated = self.repository.update_entity(open_gap["id"], open_gap["version"], open_gap["status"], data)
            self.audit.record(open_gap["id"], actor, "overflow_extend", open_gap["status"], open_gap["status"], {"observed_at": sample["observed_at"]})
            return updated
        payload = {
            "link_id": link["id"],
            "asset_id": sample["asset_id"],
            "metric": sample["metric"],
            "start_at": sample["observed_at"],
            "end_at": sample["observed_at"],
            "source": "link_overflow",
            "missing_samples": 1,
        }
        if incident:
            payload["incident_id"] = incident["id"]
        gap_id = "gap-overflow-" + hashlib.sha256(
            (link["id"] + "\0" + sample["asset_id"] + "\0" + sample["metric"] + "\0" + sample["observed_at"]).encode("utf-8")
        ).hexdigest()[:20]
        return self._store(actor, "gap", gap_id, "open", payload, "overflow_gap", {"link_id": link["id"], "reason": "telemetry capacity exceeded"})

    def _active_incident_for(self, asset_id, link_id):
        for incident in self._lookup("incident", "*", None) or []:
            if incident["status"] in ("resolved", "closed"):
                continue
            data = incident["data"]
            if data.get("asset_id") == asset_id or data.get("link_id") == link_id:
                return incident
        return None

    # ------------------------------------------------------------------
    # Revision cascade: higher revision inside a filled window invalidates
    # interpolated conclusions, recovery actions and dispatch missions.
    # ------------------------------------------------------------------
    def _cascade_revision(self, actor, asset_id, metric, new_revision, observed_at, reason):
        invalidated = {"gaps": [], "actions": [], "missions": [], "incidents": [], "missions_flagged": []}
        gaps = self._lookup("gap", "*", None) or []
        stale_gap_ids = set()
        for gap in gaps:
            data = gap["data"]
            if data.get("asset_id") != asset_id:
                continue
            if gap["status"] not in ("estimated", "filled"):
                continue
            basis = int(data.get("basis_revision", 0) or 0)
            in_window = str(data.get("start_at", "")) <= str(observed_at or "") <= str(data.get("end_at", ""))
            if metric is not None and data.get("metric") and data.get("metric") != metric:
                continue
            # Real data landing in the interpolated window supersedes the
            # interpolation: a revision newer than the basis always wins, and
            # an equal revision of measured/backfilled data beats an
            # interpolation that was only inferred at that revision.
            real_data = reason != "gap refilled on a newer revision basis"
            supersedes = new_revision > basis or (real_data and new_revision >= basis)
            if in_window and supersedes:
                updated = self._force_transition(
                    actor,
                    gap,
                    "invalidate",
                    {"reason": reason, "new_revision": new_revision},
                )
                invalidated["gaps"].append(updated["id"])
                stale_gap_ids.add(gap["id"])
        for action in self._lookup("recovery_action", "*", None) or []:
            if action["status"] not in ("proposed", "approved", "running"):
                continue
            if action["data"].get("basis_gap_id") in stale_gap_ids:
                updated = self._force_transition(
                    actor,
                    action,
                    "invalidate",
                    {"reason": reason, "new_revision": new_revision},
                )
                invalidated["actions"].append(updated["id"])
        for mission in self._lookup("mission", "*", None) or []:
            if mission["data"].get("basis_gap_id") not in stale_gap_ids:
                continue
            if mission["status"] in ("planned", "approved"):
                updated = self._force_transition(
                    actor,
                    mission,
                    "invalidate",
                    {"reason": reason, "new_revision": new_revision},
                )
                invalidated["missions"].append(updated["id"])
            elif mission["status"] in ("underway", "completed"):
                # A mission at sea cannot be pulled back; flag the stale basis
                # so the crew and planners can see the evidence changed.
                data = dict(mission["data"])
                data["basis_stale"] = True
                data["stale_reason"] = reason
                data["invalidated_by_revision"] = new_revision
                self.repository.update_entity(mission["id"], mission["version"], mission["status"], data)
                self.audit.record(mission["id"], actor, "basis_flagged", mission["status"], mission["status"], {"new_revision": new_revision})
                invalidated["missions_flagged"].append(mission["id"])
        for incident in self._lookup("incident", "*", None) or []:
            if incident["status"] not in ("diagnosing", "recovery_planned", "recovering"):
                continue
            if incident["data"].get("diagnosis_basis_gap_id") in stale_gap_ids:
                updated = self._force_transition(
                    actor,
                    incident,
                    "invalidate",
                    {
                        "reason": reason,
                        "new_revision": new_revision,
                        "conclusion_stale": True,
                    },
                )
                invalidated["incidents"].append(updated["id"])
        return invalidated

    def _force_transition(self, actor, entity, action, data):
        """Internal transition that bypasses optimistic-lock requirements."""
        from .domain import Actor as _Actor

        system = actor if actor is not None else _Actor("system", "admin")
        next_status, patch = self.rules.validate_transition(system, entity, action, data, self._lookup)
        merged = dict(entity["data"])
        merged.update(patch)
        updated = self.repository.update_entity(entity["id"], entity["version"], next_status, merged)
        self.audit.record(entity["id"], system, action, entity["status"], next_status, {"patch": patch})
        return updated

    # ------------------------------------------------------------------
    # Backfill: checkpoint resume, dedupe by stable record identity
    # ------------------------------------------------------------------
    def create_backfill(self, actor, data, idempotency_key=None):
        return self.create(actor, "backfill", data, idempotency_key)

    def _backfill_record_id(self, backfill, record):
        record_id = str(record.get("record_id", "")).strip()
        if not record_id:
            record_id = str(record.get("seq", "")).strip()
        if not record_id:
            raise ValidationError("each backfill record needs record_id or seq")
        digest = hashlib.sha256(
            (backfill["data"]["source_id"] + "\0" + record_id).encode("utf-8")
        ).hexdigest()[:24]
        return "backfill-" + digest, record_id

    def _push_backfill(self, actor, backfill, data):
        if backfill["status"] != "active":
            raise ConflictError("backfill session is " + backfill["status"])
        records = data.get("records")
        if not isinstance(records, list) or not records:
            raise ValidationError("records must be a non-empty list")
        asset_id = backfill["data"]["asset_id"]
        metric = backfill["data"]["metric"]
        cursor = int(backfill["data"].get("cursor_seq", 0) or 0)
        stored, duplicate, failed_at = [], [], None
        invalidated = {"gaps": [], "actions": [], "missions": [], "incidents": [], "missions_flagged": []}
        next_cursor = cursor
        session = backfill
        for index, raw in enumerate(records):
            record = dict(raw or {})
            try:
                seq = int(record.get("seq"))
            except (TypeError, ValueError):
                raise ValidationError("record seq must be an integer: position %s" % index)
            entity_id, record_id = self._backfill_record_id(backfill, record)
            if self.repository.get_entity(entity_id) or seq <= cursor:
                # Resending after a transport failure: dedupe. It only advances
                # the resume position if it is the record immediately at the
                # checkpoint; out-of-order duplicates are acknowledged only.
                duplicate.append(record_id)
                if seq == next_cursor + 1:
                    next_cursor = seq
                continue
            if seq != next_cursor + 1:
                # Gap in the stream: nothing after this point can commit.
                failed_at = seq
                break
            for field in ("value", "observed_at"):
                if record.get(field) in (None, ""):
                    raise ValidationError("record %s missing field: %s" % (record_id, field))
            revision = self._next_revision(asset_id, metric)
            sample = {
                "id": entity_id,
                "asset_id": asset_id,
                "metric": metric,
                "value": record["value"],
                "observed_at": record["observed_at"],
                "revision": revision,
                "record_seq": record_id,
                "backfill_id": backfill["id"],
            }
            self._store_telemetry(actor, sample, "current", "backfill_record", {"backfill_id": backfill["id"], "seq": seq})
            stored.append(record_id)
            next_cursor = seq
            # Each durable record can invalidate conclusions built on
            # interpolation for the window it lands in.
            cascade = self._cascade_revision(
                actor,
                asset_id,
                metric,
                revision,
                record["observed_at"],
                "backfilled revision supersedes interpolation",
            )
            for key in ("gaps", "actions", "missions", "incidents", "missions_flagged"):
                for value in cascade[key]:
                    if value not in invalidated[key]:
                        invalidated[key].append(value)
            # Commit the checkpoint per record: a failed transfer resumes from
            # the last durable record, never before it.
            session_data = dict(session["data"])
            session_data["cursor_seq"] = next_cursor
            session = self.repository.update_entity(session["id"], session["version"], "active", session_data)
        self.audit.record(
            backfill["id"],
            actor,
            "push_batch",
            "active",
            "active",
            {"stored": stored, "duplicate": duplicate, "cursor_seq": next_cursor},
        )
        result = {
            "backfill_id": backfill["id"],
            "cursor_seq": next_cursor,
            "stored": stored,
            "duplicate": duplicate,
            "cascade": invalidated,
        }
        if failed_at is not None:
            # Simulates a broken transfer: committed records stay durable; the
            # caller retries the batch starting at the returned cursor.
            result["resume_from_seq"] = next_cursor + 1
            result["failed_at_seq"] = failed_at
            raise _BackfillFailure(result)
        return result

    # ------------------------------------------------------------------
    # Post-transition hooks
    # ------------------------------------------------------------------
    def _after_transition(self, actor, before, after, action, data):
        kind = self.rules.normalize_kind(after["kind"])
        if kind == "link" and action in ("restore", "activate_backup"):
            self._flush_link_queue(actor, after)
        if kind == "telemetry" and action == "revise":
            new_revision = int(after["data"].get("revision", 0))
            self._cascade_revision(
                actor,
                after["data"].get("asset_id"),
                after["data"].get("metric"),
                new_revision,
                after["data"].get("observed_at"),
                "late telemetry revision supersedes interpolation",
            )
        if kind == "gap" and action == "refill":
            new_revision = int(after["data"].get("basis_revision", 0))
            self._cascade_revision(
                actor,
                after["data"].get("asset_id"),
                after["data"].get("metric"),
                new_revision,
                after["data"].get("start_at"),
                "gap refilled on a newer revision basis",
            )

    def _flush_link_queue(self, actor, link):
        queued = [
            item
            for item in self.repository.list_entities(kind="telemetry", status="queued")
            if item["data"].get("queued_on_link") == link["id"]
        ]
        queued.sort(key=lambda item: str(item["data"].get("observed_at", "")))
        delivered = []
        for item in queued:
            data = dict(item["data"])
            data.pop("queued_on_link", None)
            updated = self.repository.update_entity(item["id"], item["version"], "current", data)
            self.audit.record(item["id"], actor, "deliver", "queued", "current", {"link_id": link["id"]})
            delivered.append(updated["id"])
        if delivered:
            self.audit.record(link["id"], actor, "flush_queue", link["status"], link["status"], {"delivered": delivered})

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


class _BackfillFailure(TransferError):
    """TransferError carrying the partial push result for the HTTP layer."""

    def __init__(self, result):
        super().__init__("backfill transfer interrupted; resume at seq %s" % result.get("resume_from_seq"))
        self.result = result
