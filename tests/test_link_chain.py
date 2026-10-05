import tempfile
import unittest
from pathlib import Path

from src.domain import Actor, ConflictError, TransferError, ValidationError
from src.repository import SQLiteRepository
from src.rules import RuleEngine
from src.service import DomainService


class LinkChainTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.service = DomainService(SQLiteRepository(Path(self.tmp.name) / "test.db"), RuleEngine())
        self.admin = Actor("admin", "admin")
        self.operator = Actor("ops", "operator")
        self.engineer = Actor("eng", "engineer")
        station = self.service.create(self.admin, "station", {"name": "OSN-01", "region": "East"})
        self.station_id = station["id"]
        asset = self.service.create(self.admin, "asset", {"station_id": station["id"], "asset_type": "sensor", "serial_no": "S-1", "last_seen": "2026-10-05T09:00:00Z"})
        self.asset_id = asset["id"]
        link = self.service.create(self.admin, "link", {
            "station_id": station["id"],
            "asset_id": asset["id"],
            "link_type": "fiber",
            "capacity": 3,
            "degraded_capacity": 1,
        })
        self.link_id = link["id"]

    def tearDown(self):
        self.tmp.cleanup()

    def sample(self, minute, value=None):
        return {
            "asset_id": self.asset_id,
            "metric": "pressure",
            "value": 1.0 if value is None else value,
            "observed_at": "2026-10-05T09:%02d:00Z" % minute,
        }

    def test_degraded_link_delivers_queues_and_opens_gap(self):
        link = self.service.get(self.link_id)
        link = self.service.transition(self.operator, link["id"], "degrade", {"reason": "fiber attenuation"}, link["version"])
        self.assertEqual(link["status"], "degraded")

        result = self.service.ingest_telemetry(self.operator, self.link_id, [
            self.sample(1), self.sample(2), self.sample(3), self.sample(4), self.sample(5),
        ])
        # degraded_capacity=1 -> 1 delivered; queue capacity=3 absorbs the next
        # three; the 5th sample overflows and is retained as a gap.
        self.assertEqual(len(result["delivered"]), 1)
        self.assertEqual(len(result["queued"]), 3)
        self.assertIsNotNone(result["gap_id"])
        queued = self.service.list("telemetry", status="queued")
        self.assertEqual(len(queued), 3)
        gap = self.service.get(result["gap_id"])
        self.assertEqual(gap["status"], "open")
        self.assertEqual(gap["data"]["source"], "link_overflow")
        self.assertEqual(gap["data"]["missing_samples"], 1)

        # Further overflow while degraded extends the same open gap instead of duplicating.
        result2 = self.service.ingest_telemetry(self.operator, self.link_id, [self.sample(6)])
        self.assertEqual(result2["gap_id"], gap["id"])
        gap = self.service.get(gap["id"])
        self.assertEqual(gap["data"]["missing_samples"], 2)
        self.assertEqual(gap["data"]["end_at"], "2026-10-05T09:06:00Z")

    def test_queued_telemetry_delivers_on_restore(self):
        self.service.transition(self.operator, self.link_id, "degrade", {"reason": "x"}, self.service.get(self.link_id)["version"])
        self.service.ingest_telemetry(self.operator, self.link_id, [self.sample(1), self.sample(2), self.sample(3)])
        self.assertEqual(len(self.service.list("telemetry", status="queued")), 2)

        self.service.transition(self.operator, self.link_id, "restore", {}, self.service.get(self.link_id)["version"])
        self.assertEqual(self.service.list("telemetry", status="queued"), [])
        current = self.service.list("telemetry", status="current")
        self.assertEqual(len(current), 3)
        for item in current:
            self.assertNotIn("queued_on_link", item["data"])

    def test_down_link_records_every_sample_as_gap(self):
        self.service.transition(self.operator, self.link_id, "degrade", {"reason": "x"}, self.service.get(self.link_id)["version"])
        self.service.transition(self.operator, self.link_id, "fail", {"reason": "cut"}, self.service.get(self.link_id)["version"])
        result = self.service.ingest_telemetry(self.operator, self.link_id, [self.sample(1), self.sample(2)])
        self.assertEqual(result["delivered"], [])
        self.assertIsNotNone(result["gap_id"])
        gap = self.service.get(result["gap_id"])
        self.assertEqual(gap["data"]["missing_samples"], 2)


class RevisionCascadeTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.service = DomainService(SQLiteRepository(Path(self.tmp.name) / "test.db"), RuleEngine())
        self.admin = Actor("admin", "admin")
        station = self.service.create(self.admin, "station", {"name": "S", "region": "R"})
        asset = self.service.create(self.admin, "asset", {"station_id": station["id"], "asset_type": "sensor", "serial_no": "S-1", "last_seen": "2026-10-05T09:00:00Z"})
        link = self.service.create(self.admin, "link", {"station_id": station["id"], "asset_id": asset["id"], "link_type": "fiber", "capacity": 1})
        self.asset_id = asset["id"]
        self.link_id = link["id"]
        self.station_id = station["id"]

    def tearDown(self):
        self.tmp.cleanup()

    def _filled_gap(self):
        incident = self.service.create(self.admin, "incident", {"station_id": self.station_id, "asset_id": self.asset_id, "link_id": self.link_id, "kind": "link_loss", "severity": "high", "summary": "no data"})
        gap = self.service.create(self.admin, "gap", {
            "incident_id": incident["id"],
            "link_id": self.link_id,
            "asset_id": self.asset_id,
            "metric": "pressure",
            "start_at": "2026-10-05T09:00:00Z",
            "end_at": "2026-10-05T09:30:00Z",
        })
        gap = self.service.transition(self.admin, gap["id"], "estimate", {"estimate": "linear"}, gap["version"])
        gap = self.service.transition(self.admin, gap["id"], "fill", {"source": "interpolation", "basis_revision": 1}, gap["version"])
        return incident, gap

    def test_fill_must_record_source_and_basis_revision(self):
        _, gap = self._filled_gap()
        self.assertEqual(gap["data"]["fill_source"], "interpolation")
        self.assertEqual(gap["data"]["basis_revision"], 1)
        self.assertEqual(gap["data"]["fill_history"][0]["by"], "admin")

        # Refusing to fill without provenance.
        gap2 = self.service.create(self.admin, "gap", {
            "asset_id": self.asset_id,
            "link_id": self.link_id,
            "start_at": "2026-10-05T10:00:00Z",
            "end_at": "2026-10-05T10:30:00Z",
        })
        gap2 = self.service.transition(self.admin, gap2["id"], "estimate", {}, gap2["version"])
        with self.assertRaises(ValidationError):
            self.service.transition(self.admin, gap2["id"], "fill", {"source": "interpolation"}, gap2["version"])

    def test_higher_revision_in_window_invalidates_gap_actions_mission_and_conclusion(self):
        incident, gap = self._filled_gap()

        # A recovery action decided on the interpolated evidence.
        action = self.service.create(self.admin, "recovery_action", {
            "incident_id": incident["id"],
            "action_type": "switch_backup",
            "dedupe_key": "switch-1",
            "basis_gap_id": gap["id"],
        })
        self.assertEqual(action["data"]["basis_revision"], 1)
        action = self.service.transition(self.admin, action["id"], "approve", {})

        # A dispatch mission planned on the same stale basis.
        mission = self.service.create(self.admin, "mission", {
            "station_id": self.station_id,
            "purpose": "replace cable",
            "window_start": "2026-10-06T00:00:00Z",
            "window_end": "2026-10-06T12:00:00Z",
            "basis_gap_id": gap["id"],
        })
        mission = self.service.transition(self.admin, mission["id"], "approve", {}, mission["version"])

        # Diagnosis recorded against the interpolation basis.
        incident = self.service.transition(self.admin, incident["id"], "diagnose", {"basis_gap_id": gap["id"]})
        self.assertEqual(incident["data"]["diagnosis_basis_gap_id"], gap["id"])

        # Real late data arrives (higher revision) inside the gap window.
        telemetry = self.service.create(self.admin, "telemetry", {
            "asset_id": self.asset_id,
            "metric": "pressure",
            "value": 9.8,
            "observed_at": "2026-10-05T09:15:00Z",
            "revision": 1,
        })
        updated = self.service.transition(self.admin, telemetry["id"], "revise", {"value": 9.9, "revision": 5}, telemetry["version"])
        self.assertEqual(updated["data"]["revision"], 5)

        gap = self.service.get(gap["id"])
        self.assertEqual(gap["status"], "open")
        self.assertTrue(gap["data"]["stale_reason"])
        action = self.service.get(action["id"])
        self.assertEqual(action["status"], "invalidated")
        mission = self.service.get(mission["id"])
        self.assertEqual(mission["status"], "invalidated")
        incident = self.service.get(incident["id"])
        self.assertEqual(incident["status"], "diagnosing")
        self.assertTrue(incident["data"]["conclusion_stale"])

        # Incident cannot be closed on stale evidence.
        self.service.transition(self.admin, incident["id"], "plan_recovery", {})
        self.service.transition(self.admin, incident["id"], "start_recovery", {})
        with self.assertRaises(ConflictError):
            self.service.transition(self.admin, incident["id"], "resolve", {"summary": "done"})

        # Recompute on the new basis after the gap is filled again.
        gap = self.service.transition(self.admin, gap["id"], "estimate", {}, gap["version"])
        gap = self.service.transition(self.admin, gap["id"], "fill", {"source": "backfill", "basis_revision": 5}, gap["version"])
        action = self.service.transition(self.admin, action["id"], "recompute", {"basis_gap_id": gap["id"]})
        self.assertEqual(action["status"], "proposed")
        self.assertIsNone(action["data"]["stale_reason"])
        mission = self.service.transition(self.admin, mission["id"], "replan", {"basis_gap_id": gap["id"]}, mission["version"])
        self.assertEqual(mission["status"], "planned")
        self.assertEqual(mission["data"]["basis_revision"], 5)

    def test_refill_with_lower_revision_rejected(self):
        _, gap = self._filled_gap()
        with self.assertRaises(ConflictError):
            self.service.transition(self.admin, gap["id"], "refill", {"source": "backfill", "basis_revision": 1}, gap["version"])

    def test_mission_at_sea_is_flagged_not_invalidated(self):
        _, gap = self._filled_gap()
        mission = self.service.create(self.admin, "mission", {
            "station_id": self.station_id,
            "purpose": "repair",
            "window_start": "2026-10-06T00:00:00Z",
            "window_end": "2026-10-07T00:00:00Z",
            "basis_gap_id": gap["id"],
        })
        mission = self.service.transition(self.admin, mission["id"], "approve", {}, mission["version"])
        mission = self.service.transition(self.admin, mission["id"], "depart", {}, mission["version"])

        telemetry = self.service.create(self.admin, "telemetry", {
            "asset_id": self.asset_id, "metric": "pressure", "value": 1,
            "observed_at": "2026-10-05T09:10:00Z", "revision": 1,
        })
        self.service.transition(self.admin, telemetry["id"], "revise", {"value": 2, "revision": 9}, telemetry["version"])

        mission = self.service.get(mission["id"])
        self.assertEqual(mission["status"], "underway")
        self.assertTrue(mission["data"]["basis_stale"])


class BackfillTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.service = DomainService(SQLiteRepository(Path(self.tmp.name) / "test.db"), RuleEngine())
        self.admin = Actor("admin", "admin")
        station = self.service.create(self.admin, "station", {"name": "S", "region": "R"})
        asset = self.service.create(self.admin, "asset", {"station_id": station["id"], "asset_type": "sensor", "serial_no": "S-1", "last_seen": "2026-10-05T09:00:00Z"})
        self.asset_id = asset["id"]
        self.session = self.service.create(self.admin, "backfill", {"source_id": "vessel-upload", "asset_id": asset["id"], "metric": "pressure"})

    def tearDown(self):
        self.tmp.cleanup()

    def records(self, *specs):
        return [
            {"seq": seq, "record_id": "rec-%s" % seq, "value": float(seq),
             "observed_at": "2026-10-05T09:%02d:00Z" % seq}
            for seq in specs
        ]

    def test_failed_transfer_resumes_from_checkpoint_and_dedupes(self):
        # First transfer commits seq 1..2 then breaks before seq 4 (seq 3 missing).
        with self.assertRaises(TransferError) as caught:
            self.service.transition(self.admin, self.session["id"], "push_batch", {"records": self.records(1, 2, 4)})
        failure = caught.exception.result
        self.assertEqual(failure["cursor_seq"], 2)
        self.assertEqual(failure["resume_from_seq"], 3)
        self.assertEqual(failure["stored"], ["rec-1", "rec-2"])

        session = self.service.get(self.session["id"])
        self.assertEqual(session["data"]["cursor_seq"], 2)

        # Client retries the whole batch from the checkpoint. Records 1 and 2
        # are already durable: they must enter the store exactly once.
        result = self.service.transition(self.admin, session["id"], "push_batch", {"records": self.records(1, 2, 3, 4)}, session["version"])
        self.assertEqual(sorted(result["stored"]), ["rec-3", "rec-4"])
        self.assertEqual(sorted(result["duplicate"]), ["rec-1", "rec-2"])
        self.assertEqual(result["cursor_seq"], 4)

        telemetry = [t for t in self.service.list("telemetry") if t["data"].get("backfill_id") == self.session["id"]]
        self.assertEqual(len(telemetry), 4)
        revisions = sorted(int(t["data"]["revision"]) for t in telemetry)
        self.assertEqual(revisions, [1, 2, 3, 4])

        # A second identical retry changes nothing.
        result2 = self.service.transition(self.admin, self.session["id"], "push_batch", {"records": self.records(1, 2, 3, 4)}, self.service.get(self.session["id"])["version"])
        self.assertEqual(result2["stored"], [])
        self.assertEqual(len(self.service.list("telemetry")), 4)

    def test_backfill_within_filled_window_triggers_cascade(self):
        link = self.service.create(self.admin, "link", {"station_id": self.service.list("station")[0]["id"], "asset_id": self.asset_id, "link_type": "fiber", "capacity": 1})
        gap = self.service.create(self.admin, "gap", {
            "asset_id": self.asset_id,
            "link_id": link["id"],
            "metric": "pressure",
            "start_at": "2026-10-05T09:00:00Z",
            "end_at": "2026-10-05T09:30:00Z",
        })
        gap = self.service.transition(self.admin, gap["id"], "estimate", {}, gap["version"])
        gap = self.service.transition(self.admin, gap["id"], "fill", {"source": "interpolation", "basis_revision": 1}, gap["version"])

        result = self.service.transition(self.admin, self.session["id"], "push_batch", {"records": self.records(1)})
        self.assertIn(gap["id"], result["cascade"]["gaps"])
        self.assertEqual(self.service.get(gap["id"])["status"], "open")


class ConcurrencyTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.service = DomainService(SQLiteRepository(Path(self.tmp.name) / "test.db"), RuleEngine())
        self.alice = Actor("alice", "engineer")
        self.bob = Actor("bob", "engineer")
        self.admin = Actor("admin", "admin")
        station = self.service.create(self.admin, "station", {"name": "S", "region": "R"})
        asset = self.service.create(self.admin, "asset", {"station_id": station["id"], "asset_type": "sensor", "serial_no": "S-1", "last_seen": "2026-10-05T09:00:00Z"})
        link = self.service.create(self.admin, "link", {"station_id": station["id"], "asset_id": asset["id"], "link_type": "fiber", "capacity": 1})
        self.gap = self.service.create(self.admin, "gap", {
            "asset_id": asset["id"],
            "link_id": link["id"],
            "start_at": "2026-10-05T09:00:00Z",
            "end_at": "2026-10-05T09:30:00Z",
        })
        self.mission = self.service.create(self.admin, "mission", {
            "station_id": station["id"],
            "purpose": "inspect",
            "window_start": "2026-10-06T00:00:00Z",
            "window_end": "2026-10-06T06:00:00Z",
        })

    def tearDown(self):
        self.tmp.cleanup()

    def test_gap_edit_requires_expected_version(self):
        with self.assertRaises(ConflictError):
            self.service.transition(self.alice, self.gap["id"], "estimate", {"estimate": "x"})

    def test_two_editors_only_one_succeeds(self):
        version = self.gap["version"]
        # Both engineers read version 1; Alice commits first.
        updated = self.service.transition(self.alice, self.gap["id"], "estimate", {"estimate": "alice linear"}, version)
        self.assertEqual(updated["version"], version + 1)
        # Bob's stale write is rejected with the current version reported.
        with self.assertRaises(ConflictError):
            self.service.transition(self.bob, self.gap["id"], "estimate", {"estimate": "bob cubic"}, version)
        self.assertEqual(self.service.get(self.gap["id"])["data"].get("estimate"), "alice linear")

    def test_two_mission_editors_only_one_succeeds(self):
        version = self.mission["version"]
        self.service.transition(self.alice, self.mission["id"], "cancel", {"reason": "weather"}, version)
        with self.assertRaises(ConflictError):
            self.service.transition(self.bob, self.mission["id"], "approve", {}, version)
        self.assertEqual(self.service.get(self.mission["id"])["status"], "cancelled")


if __name__ == "__main__":
    unittest.main()
