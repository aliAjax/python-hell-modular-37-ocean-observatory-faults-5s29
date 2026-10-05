from datetime import datetime, timedelta, timezone

from .domain import ConflictError, InvalidTransition, PermissionDenied, ValidationError


def _require(data, fields):
    for field in fields:
        value = data.get(field)
        if value is None or value == "" or value == [] or value == {}:
            raise ValidationError("missing required field: " + field)


def _ensure_role(actor, allowed):
    if "*" not in allowed and actor.role not in allowed:
        raise PermissionDenied("role %s is not allowed here" % actor.role)


def _all(lookup, kind):
    return lookup(kind, "*", None) or [] if lookup else []


def _find_one(lookup, kind, field, value):
    rows = lookup(kind, field, value) or [] if lookup else []
    return rows[0] if rows else None


def _number(value, field):
    try:
        number = float(value)
    except (TypeError, ValueError):
        raise ValidationError(field + " must be numeric")
    if number < 0:
        raise ValidationError(field + " must be non-negative")
    return number


def _validate_asset(data, lookup):
    if not _find_one(lookup, "station", "id", data.get("station_id")):
        raise ValidationError("asset requires station")
    if data.get("clock_offset_seconds") not in (None, ""):
        _number(data.get("clock_offset_seconds"), "clock_offset_seconds")


def _validate_link(data, lookup):
    if not _find_one(lookup, "station", "id", data.get("station_id")):
        raise ValidationError("link requires station")
    if not _find_one(lookup, "asset", "id", data.get("asset_id")):
        raise ValidationError("link requires asset")
    _number(data.get("capacity"), "capacity")
    if data.get("degraded_capacity") not in (None, ""):
        degraded = _number(data.get("degraded_capacity"), "degraded_capacity")
        if degraded > _number(data.get("capacity"), "capacity"):
            raise ValidationError("degraded_capacity cannot exceed capacity")


def _validate_telemetry(data, lookup):
    asset = _find_one(lookup, "asset", "id", data.get("asset_id"))
    if not asset:
        raise ValidationError("telemetry requires asset")
    _number(data.get("value"), "value")
    try:
        revision = int(data.get("revision"))
    except (TypeError, ValueError):
        raise ValidationError("revision must be an integer")
    if revision < 1:
        raise ValidationError("revision must be positive")
    for item in _all(lookup, "telemetry"):
        if item["data"].get("asset_id") == data.get("asset_id") and item["data"].get("metric") == data.get("metric"):
            if int(item["data"].get("revision", 0)) >= revision:
                raise ConflictError("telemetry revision must increase")


def _validate_incident(data, lookup):
    if not data.get("station_id") and not data.get("asset_id") and not data.get("link_id"):
        raise ValidationError("incident requires station_id, asset_id or link_id")
    if data.get("severity") not in ("low", "medium", "high", "critical"):
        raise ValidationError("invalid incident severity")
    for item in _all(lookup, "incident"):
        if item["status"] in ("open", "diagnosing", "recovery_planned", "recovering") and item["data"].get("asset_id") == data.get("asset_id") and item["data"].get("kind") == data.get("kind"):
            raise ConflictError("active incident already exists for asset and kind")


def _validate_action(data, lookup):
    incident = _find_one(lookup, "incident", "id", data.get("incident_id"))
    if not incident or incident["status"] in ("resolved", "closed"):
        raise ValidationError("recovery action requires an active incident")
    if data.get("action_type") not in ("remote_restart", "switch_backup", "firmware_rollback", "dispatch_mission"):
        raise ValidationError("invalid action_type")
    key = data.get("dedupe_key")
    for item in _all(lookup, "recovery_action"):
        if item["data"].get("dedupe_key") == key and item["status"] not in ("succeeded", "failed", "cancelled", "invalidated"):
            raise ConflictError("active recovery action already exists for dedupe_key")
    data.update(_snapshot_basis(None, data, lookup))


def _validate_mission(data, lookup):
    if not _find_one(lookup, "station", "id", data.get("station_id")):
        raise ValidationError("mission requires station")
    if not data.get("window_start") or not data.get("window_end"):
        raise ValidationError("mission window is required")
    data.update(_snapshot_basis(None, data, lookup))


def _validate_gap(data, lookup):
    incident_id = data.get("incident_id")
    link_id = data.get("link_id")
    if not incident_id and not link_id:
        raise ValidationError("data gap requires incident_id or link_id")
    if incident_id and not _find_one(lookup, "incident", "id", incident_id):
        raise ValidationError("gap references unknown incident")
    if link_id and not _find_one(lookup, "link", "id", link_id):
        raise ValidationError("gap references unknown link")
    if not data.get("asset_id"):
        raise ValidationError("data gap requires asset_id")
    if not _find_one(lookup, "asset", "id", data.get("asset_id")):
        raise ValidationError("gap references unknown asset")
    if not data.get("start_at") or not data.get("end_at"):
        raise ValidationError("gap window is required")


def _validate_backfill(data, lookup):
    asset = _find_one(lookup, "asset", "id", data.get("asset_id"))
    if not asset:
        raise ValidationError("backfill requires asset")
    if not data.get("source_id"):
        raise ValidationError("backfill requires source_id")
    if not data.get("metric"):
        raise ValidationError("backfill requires metric")


def _snapshot_basis(actor, data, lookup):
    """Carry the interpolation basis of a referenced gap onto actions and missions."""
    gap_id = data.get("basis_gap_id")
    if gap_id:
        gap = _find_one(lookup, "gap", "id", gap_id)
        if not gap:
            raise ValidationError("unknown basis_gap_id")
        if gap["status"] not in ("estimated", "filled"):
            raise ConflictError("basis gap is not usable: " + gap["status"])
        result = {
            "basis_gap_id": gap_id,
            "basis_revision": gap["data"].get("basis_revision"),
            "basis_source": gap["data"].get("fill_source"),
        }
        incident_id = gap["data"].get("incident_id")
        if incident_id:
            result["incident_id"] = incident_id
        return result
    return {}


def _positive_int(value, field):
    try:
        number = int(value)
    except (TypeError, ValueError):
        raise ValidationError(field + " must be an integer")
    if number < 1:
        raise ValidationError(field + " must be positive")
    return number


def _fill_gap(actor, entity, data, lookup):
    source = data.get("source")
    if source not in ("interpolation", "backfill", "field_record"):
        raise ValidationError("source must be interpolation, backfill or field_record")
    basis_revision = _positive_int(data.get("basis_revision"), "basis_revision")
    return {
        "fill_source": source,
        "basis_revision": basis_revision,
        "filled_by": actor.user_id,
        "fill_history": [
            {
                "source": source,
                "basis_revision": basis_revision,
                "by": actor.user_id,
                "at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            }
        ],
    }


def _refill_gap(actor, entity, data, lookup):
    source = data.get("source")
    if source not in ("interpolation", "backfill", "field_record"):
        raise ValidationError("source must be interpolation, backfill or field_record")
    new_revision = _positive_int(data.get("basis_revision"), "basis_revision")
    old_revision = int(entity["data"].get("basis_revision", 0))
    if new_revision <= old_revision:
        raise ConflictError("refill basis_revision must exceed previous basis revision")
    history = list(entity["data"].get("fill_history", []))
    history.append(
        {
            "source": source,
            "basis_revision": new_revision,
            "supersedes": old_revision,
            "by": actor.user_id,
            "at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }
    )
    return {
        "fill_source": source,
        "basis_revision": new_revision,
        "filled_by": actor.user_id,
        "stale_reason": None,
        "fill_history": history,
    }


def _invalidate_gap(actor, entity, data, lookup):
    reason = data.get("reason", "basis revision superseded")
    history = list(entity["data"].get("fill_history", []))
    superseded = {
        "source": entity["data"].get("fill_source"),
        "basis_revision": entity["data"].get("basis_revision"),
        "superseded_by_revision": data.get("new_revision"),
        "at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    return {
        "stale_reason": reason,
        "superseded_fill": superseded,
        "fill_history": history + [superseded],
    }


def _recompute_action(actor, entity, data, lookup):
    gap_id = data.get("basis_gap_id")
    gap = _find_one(lookup, "gap", "id", gap_id) if gap_id else None
    if not gap:
        raise ValidationError("recompute requires a valid basis_gap_id")
    if gap["status"] != "filled":
        raise ConflictError("basis gap must be filled before recompute")
    return {
        "basis_gap_id": gap_id,
        "basis_revision": gap["data"].get("basis_revision"),
        "basis_source": gap["data"].get("fill_source"),
        "stale_reason": None,
        "recomputed_by": actor.user_id,
    }


def _replan_mission(actor, entity, data, lookup):
    gap_id = data.get("basis_gap_id")
    gap = _find_one(lookup, "gap", "id", gap_id) if gap_id else None
    if not gap:
        raise ValidationError("replan requires a valid basis_gap_id")
    if gap["status"] != "filled":
        raise ConflictError("basis gap must be filled before replan")
    return {
        "basis_gap_id": gap_id,
        "basis_revision": gap["data"].get("basis_revision"),
        "basis_source": gap["data"].get("fill_source"),
        "stale_reason": None,
        "replanned_by": actor.user_id,
    }


def _invalidate_derived(actor, entity, data, lookup):
    return {
        "stale_reason": data.get("reason", "basis revision superseded"),
        "invalidated_by_revision": data.get("new_revision"),
    }


def _mission_incident_id(mission, lookup):
    """Resolve the incident a mission belongs to, directly or via its basis gap."""
    incident_id = mission["data"].get("incident_id")
    if incident_id:
        return incident_id
    gap_id = mission["data"].get("basis_gap_id")
    if gap_id:
        gap = _find_one(lookup, "gap", "id", gap_id)
        if gap:
            return gap["data"].get("incident_id")
    return None


def _record_diagnosis(actor, entity, data, lookup):
    patch = {"diagnosed_by": actor.user_id, "conclusion_stale": False}
    gap_id = data.get("basis_gap_id")
    if gap_id:
        gap = _find_one(lookup, "gap", "id", gap_id)
        if not gap:
            raise ValidationError("unknown basis_gap_id")
        patch["diagnosis_basis_gap_id"] = gap_id
        patch["diagnosis_basis_revision"] = gap["data"].get("basis_revision")
    return patch


def _revise_telemetry(actor, entity, data, lookup):
    try:
        new_revision = int(data.get("revision"))
    except (TypeError, ValueError):
        raise ValidationError("revision must be an integer")
    if new_revision <= int(entity["data"].get("revision", 0)):
        raise ConflictError("late revision must increase revision number")
    return {"late_revision": True, "revised_by": actor.user_id}


def _resolve_incident(actor, entity, data, lookup):
    actions = [a for a in _all(lookup, "recovery_action") if a["data"].get("incident_id") == entity["id"] and a["status"] not in ("succeeded", "failed", "cancelled")]
    if actions:
        raise ConflictError("incident cannot resolve while recovery actions are active")
    gaps = [g for g in _all(lookup, "gap") if g["data"].get("incident_id") == entity["id"] and g["status"] not in ("filled", "accepted", "closed")]
    if gaps:
        raise ConflictError("incident cannot resolve while data gaps remain open")
    stale_gaps = [g for g in _all(lookup, "gap") if g["data"].get("incident_id") == entity["id"] and g["data"].get("stale_reason")]
    if stale_gaps:
        raise ConflictError("incident cannot resolve while gap basis is stale")
    missions = [
        m for m in _all(lookup, "mission")
        if _mission_incident_id(m, lookup) == entity["id"]
        and m["status"] in ("planned", "approved", "invalidated")
    ]
    if missions:
        raise ConflictError("incident cannot resolve while dispatch missions are pending or invalidated")
    assets = [a for a in _all(lookup, "asset") if a["status"] in ("faulty", "offline", "rebooting")]
    if entity["data"].get("asset_id") and any(a["id"] == entity["data"].get("asset_id") for a in assets):
        raise ConflictError("affected asset is still unavailable")
    return {"resolved_by": actor.user_id}


def _complete_action(actor, entity, data, lookup):
    if not data.get("outcome"):
        raise ValidationError("outcome is required")
    return {"completed_by": actor.user_id}


def _complete_mission(actor, entity, data, lookup):
    if not data.get("report"):
        raise ValidationError("report is required")
    return {"completed_by": actor.user_id}


class RuleEngine:
    ALIASES = {
        "stations": "station", "assets": "asset", "links": "link", "telemetries": "telemetry",
        "incidents": "incident", "recovery_actions": "recovery_action", "missions": "mission",
        "gaps": "gap", "backfills": "backfill",
    }
    INITIAL_STATUS = {
        "station": "online", "asset": "healthy", "link": "up", "telemetry": "current",
        "incident": "open", "recovery_action": "proposed", "mission": "planned", "gap": "open",
        "backfill": "active",
    }
    TRANSITIONS = {
        "station": {
            "degrade": (("online",), "degraded"),
            "go_offline": (("online", "degraded"), "offline"),
            "resume": (("degraded", "offline"), "online"),
        },
        "asset": {
            "degrade": (("healthy",), "degraded"),
            "fail": (("healthy", "degraded"), "faulty"),
            "start_reboot": (("faulty",), "rebooting"),
            "finish_reboot": (("rebooting",), "healthy"),
            "restore": (("faulty",), "healthy"),
        },
        "link": {
            "degrade": (("up",), "degraded"),
            "fail": (("up", "degraded"), "down"),
            "activate_backup": (("down", "degraded"), "backup_active"),
            "restore": (("down", "backup_active", "degraded"), "up"),
        },
        "telemetry": {
            "mark_stale": (("current",), "stale"),
            "quarantine": (("current", "stale"), "quarantined"),
            "revise": (("current", "stale", "quarantined"), "current"),
            "clear": (("stale",), "current"),
            "deliver": (("queued",), "current"),
        },
        "incident": {
            "diagnose": (("open", "diagnosing"), "diagnosing"),
            "plan_recovery": (("diagnosing",), "recovery_planned"),
            "start_recovery": (("recovery_planned",), "recovering"),
            "resolve": (("recovering",), "resolved"),
            "close": (("resolved",), "closed"),
            "reopen": (("resolved", "closed"), "open"),
            "invalidate": (("open", "diagnosing", "recovery_planned", "recovering"), "diagnosing"),
        },
        "recovery_action": {
            "approve": (("proposed", "invalidated"), "approved"),
            "start": (("approved",), "running"),
            "succeed": (("running",), "succeeded"),
            "fail": (("approved", "running"), "failed"),
            "cancel": (("proposed", "approved", "running", "invalidated"), "cancelled"),
            "recompute": (("invalidated", "approved", "proposed"), "proposed"),
            "invalidate": (("proposed", "approved", "running"), "invalidated"),
        },
        "mission": {
            "approve": (("planned", "invalidated"), "approved"),
            "depart": (("approved",), "underway"),
            "complete": (("underway",), "completed"),
            "cancel": (("planned", "approved", "underway", "invalidated"), "cancelled"),
            "replan": (("invalidated", "planned"), "planned"),
            "invalidate": (("planned", "approved"), "invalidated"),
        },
        "gap": {
            "estimate": (("open",), "estimated"),
            "fill": (("estimated",), "filled"),
            "accept": (("filled", "open"), "accepted"),
            "refill": (("filled",), "filled"),
            "invalidate": (("estimated", "filled"), "open"),
        },
        "backfill": {
            "push_batch": (("active",), "active"),
            "complete": (("active",), "completed"),
            "abort": (("active",), "aborted"),
        },
    }
    CREATE_REQUIRED = {
        "station": ("name", "region"),
        "asset": ("station_id", "asset_type", "serial_no", "last_seen"),
        "link": ("station_id", "asset_id", "link_type", "capacity"),
        "telemetry": ("asset_id", "metric", "value", "observed_at", "revision"),
        "incident": ("kind", "severity", "summary"),
        "recovery_action": ("incident_id", "action_type", "dedupe_key"),
        "mission": ("station_id", "purpose", "window_start", "window_end"),
        "gap": ("asset_id", "start_at", "end_at"),
        "backfill": ("source_id", "asset_id", "metric"),
    }
    ACTION_REQUIRED = {
        ("station", "degrade"): ("reason",),
        ("link", "fail"): ("reason",),
        ("telemetry", "revise"): ("revision",),
        ("recovery_action", "succeed"): ("outcome",),
        ("mission", "complete"): ("report",),
        ("gap", "fill"): ("source", "basis_revision"),
        ("gap", "refill"): ("source", "basis_revision"),
        ("backfill", "push_batch"): ("records",),
        ("incident", "resolve"): ("summary",),
    }
    CREATE_ROLES = {
        "station": ("admin", "engineer"),
        "asset": ("admin", "engineer"),
        "link": ("admin", "engineer"),
        "telemetry": ("admin", "operator", "engineer"),
        "incident": ("admin", "operator", "engineer"),
        "recovery_action": ("admin", "operator", "engineer"),
        "mission": ("admin", "engineer"),
        "gap": ("admin", "operator", "engineer"),
        "backfill": ("admin", "operator", "engineer"),
    }
    # Transitions that must be submitted with the caller's expected_version so
    # concurrent edits to the same gap or mission cannot both succeed.
    OPTIMISTIC_TRANSITIONS = {"gap", "mission"}
    ROLE_ACTIONS = {
        ("station", "degrade"): ("admin", "engineer", "operator"),
        ("station", "go_offline"): ("admin", "engineer", "operator"),
        ("station", "resume"): ("admin", "engineer", "operator"),
        ("link", "fail"): ("admin", "engineer", "operator"),
        ("link", "degrade"): ("admin", "engineer", "operator"),
        ("link", "activate_backup"): ("admin", "engineer", "operator"),
        ("link", "restore"): ("admin", "engineer", "operator"),
        ("asset", "fail"): ("admin", "engineer", "operator"),
        ("asset", "start_reboot"): ("admin", "engineer", "operator"),
        ("asset", "finish_reboot"): ("admin", "engineer", "operator"),
        ("asset", "restore"): ("admin", "engineer", "operator"),
        ("asset", "degrade"): ("admin", "engineer", "operator"),
        ("telemetry", "mark_stale"): ("admin", "operator", "engineer"),
        ("telemetry", "quarantine"): ("admin", "engineer", "operator"),
        ("telemetry", "revise"): ("admin", "operator", "engineer"),
        ("telemetry", "clear"): ("admin", "operator", "engineer"),
        ("telemetry", "deliver"): ("admin", "operator", "engineer"),
        ("incident", "diagnose"): ("admin", "operator", "engineer"),
        ("incident", "plan_recovery"): ("admin", "operator", "engineer"),
        ("incident", "start_recovery"): ("admin", "operator", "engineer"),
        ("incident", "resolve"): ("admin", "engineer"),
        ("incident", "close"): ("admin", "engineer"),
        ("incident", "reopen"): ("admin", "engineer", "operator"),
        ("incident", "invalidate"): ("admin", "engineer", "operator"),
        ("recovery_action", "approve"): ("admin", "engineer"),
        ("recovery_action", "start"): ("admin", "engineer", "operator"),
        ("recovery_action", "succeed"): ("admin", "engineer", "operator"),
        ("recovery_action", "cancel"): ("admin", "engineer", "operator"),
        ("recovery_action", "recompute"): ("admin", "engineer", "operator"),
        ("recovery_action", "invalidate"): ("admin", "engineer", "operator"),
        ("mission", "approve"): ("admin", "engineer"),
        ("mission", "depart"): ("admin", "engineer", "operator"),
        ("mission", "complete"): ("admin", "engineer", "operator"),
        ("mission", "cancel"): ("admin", "engineer", "operator"),
        ("mission", "replan"): ("admin", "engineer", "operator"),
        ("mission", "invalidate"): ("admin", "engineer", "operator"),
        ("gap", "estimate"): ("admin", "engineer", "operator"),
        ("gap", "fill"): ("admin", "engineer", "operator"),
        ("gap", "accept"): ("admin", "engineer", "operator"),
        ("gap", "refill"): ("admin", "engineer", "operator"),
        ("gap", "invalidate"): ("admin", "engineer", "operator"),
        ("backfill", "push_batch"): ("admin", "operator", "engineer"),
        ("backfill", "complete"): ("admin", "operator", "engineer"),
        ("backfill", "abort"): ("admin", "operator", "engineer"),
    }
    CUSTOM_CREATE = {
        "asset": lambda a, d, l: _validate_asset(d, l),
        "link": lambda a, d, l: _validate_link(d, l),
        "telemetry": lambda a, d, l: _validate_telemetry(d, l),
        "incident": lambda a, d, l: _validate_incident(d, l),
        "recovery_action": lambda a, d, l: _validate_action(d, l),
        "mission": lambda a, d, l: _validate_mission(d, l),
        "gap": lambda a, d, l: _validate_gap(d, l),
        "backfill": lambda a, d, l: _validate_backfill(d, l),
    }
    CUSTOM_TRANSITIONS = {
        ("telemetry", "revise"): _revise_telemetry,
        ("incident", "resolve"): _resolve_incident,
        ("recovery_action", "succeed"): _complete_action,
        ("mission", "complete"): _complete_mission,
        ("gap", "fill"): _fill_gap,
        ("gap", "refill"): _refill_gap,
        ("gap", "invalidate"): _invalidate_gap,
        ("incident", "invalidate"): _invalidate_derived,
        ("recovery_action", "invalidate"): _invalidate_derived,
        ("mission", "invalidate"): _invalidate_derived,
        ("recovery_action", "recompute"): _recompute_action,
        ("mission", "replan"): _replan_mission,
        ("incident", "diagnose"): _record_diagnosis,
    }

    def normalize_kind(self, kind):
        return self.ALIASES.get(kind, kind)

    def initial_status(self, kind, data=None):
        kind = self.normalize_kind(kind)
        if kind not in self.INITIAL_STATUS:
            raise ValidationError("unknown kind: " + str(kind))
        return self.INITIAL_STATUS[kind]

    def validate_create(self, actor, kind, data, lookup=None):
        kind = self.normalize_kind(kind)
        if kind not in self.INITIAL_STATUS:
            raise ValidationError("unknown kind: " + str(kind))
        _ensure_role(actor, self.CREATE_ROLES.get(kind, ("admin",)))
        _require(data, self.CREATE_REQUIRED.get(kind, ()))
        custom = self.CUSTOM_CREATE.get(kind)
        if custom:
            custom(actor, data, lookup)
        return dict(data)

    def validate_transition(self, actor, entity, action, data, lookup=None):
        kind = self.normalize_kind(entity["kind"])
        transition = self.TRANSITIONS.get(kind, {}).get(action)
        if not transition:
            raise InvalidTransition("unknown action %s for %s" % (action, kind))
        allowed_statuses, next_status = transition
        allowed = self.ROLE_ACTIONS.get((kind, action), self.ROLE_ACTIONS.get(action, ("admin",)))
        _ensure_role(actor, allowed)
        _require(data, self.ACTION_REQUIRED.get((kind, action), ()))
        custom = self.CUSTOM_TRANSITIONS.get((kind, action))
        extra = custom(actor, entity, data, lookup) if custom else {}
        # Persist submitted data, but strip internal control fields (reason and
        # new_revision drive cascade rules, not entity state).
        patch = {
            key: value
            for key, value in dict(data).items()
            if key not in ("reason", "new_revision")
        }
        if extra:
            patch.update(extra)
        # Status applicability is evaluated last on purpose: callers acting on a
        # stale version must see the version conflict, not a confusing state
        # error computed against a row someone else already moved.
        if entity["status"] not in allowed_statuses:
            raise InvalidTransition("cannot %s from status %s" % (action, entity["status"]))
        return next_status, patch
