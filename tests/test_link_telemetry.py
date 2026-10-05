import json
import tempfile
import threading
import unittest
from http.client import HTTPConnection
from pathlib import Path

from src.domain import Actor, ConflictError, PermissionDenied
from src.http_api import create_server
from src.repository import SQLiteRepository
from src.rules import RuleEngine
from src.service import DomainService


class LinkTelemetryTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.service = DomainService(SQLiteRepository(Path(self.tmp.name) / "test.db"), RuleEngine())
        self.admin = Actor("admin", "admin")
        self.operator = Actor("op", "operator")
        self.viewer = Actor("viewer", "viewer")

    def tearDown(self):
        self.tmp.cleanup()

    def setup_link(self, capacity=10, backlog_limit=10):
        station = self.service.create(self.admin, "station", {"name": "S", "region": "R"})
        asset = self.service.create(
            self.admin, "asset",
            {"station_id": station["id"], "asset_type": "sensor", "serial_no": "A",
             "last_seen": "2026-09-27T09:00:00Z"},
        )
        link = self.service.create(
            self.admin, "link",
            {"station_id": station["id"], "asset_id": asset["id"], "link_type": "fiber",
             "capacity": capacity, "backlog_limit": backlog_limit},
        )
        return station, asset, link

    def rec(self, asset, i, rev=None, source=None):
        record = {
            "asset_id": asset["id"], "metric": "pressure", "value": i,
            "observed_at": "2026-09-27T09:%02d:00Z" % i, "revision": rev or i,
        }
        if source:
            record["source_id"] = source
            record["record_id"] = "r%d" % i
        return record

    def test_degraded_link_queues_telemetry_over_capacity(self):
        station, asset, link = self.setup_link(capacity=2, backlog_limit=5)
        self.service.transition(self.admin, link["id"], "degrade", {"reason": "weather"})
        result = self.service.ingest_telemetry(self.admin, link["id"], [self.rec(asset, i) for i in range(1, 6)])
        self.assertEqual(len(result["accepted"]), 1)
        self.assertEqual(len(result["queued"]), 4)
        self.assertEqual(len(result["gaps"]), 0)
        for item in result["queued"]:
            self.assertEqual(item["status"], "queued")

    def test_capacity_insufficient_retains_gap(self):
        station, asset, link = self.setup_link(capacity=2, backlog_limit=2)
        self.service.transition(self.admin, link["id"], "degrade", {"reason": "weather"})
        result = self.service.ingest_telemetry(self.admin, link["id"], [self.rec(asset, i) for i in range(1, 7)])
        self.assertEqual(len(result["accepted"]), 1)
        self.assertEqual(len(result["queued"]), 2)
        self.assertEqual(len(result["gaps"]), 3)
        gap = result["gaps"][0]
        self.assertEqual(gap["status"], "open")
        self.assertEqual(gap["data"]["reason"], "capacity_insufficient")
        self.assertTrue(gap["data"]["lost_records"])

    def test_link_down_queues_then_retransmits_after_restore(self):
        station, asset, link = self.setup_link(capacity=10, backlog_limit=10)
        self.service.transition(self.admin, link["id"], "fail", {"reason": "cut"})
        result = self.service.ingest_telemetry(self.admin, link["id"], [self.rec(asset, i) for i in range(1, 5)])
        self.assertEqual(len(result["accepted"]), 0)
        self.assertEqual(len(result["queued"]), 4)
        # still down: no progress, breakpoint unchanged
        blocked = self.service.retransmit(self.admin, link["id"])
        self.assertEqual(len(blocked["transmitted"]), 0)
        self.assertEqual(blocked["cursor"], 0)
        # restore and retransmit
        self.service.transition(self.admin, link["id"], "restore", {})
        done = self.service.retransmit(self.admin, link["id"])
        self.assertEqual(len(done["transmitted"]), 4)
        self.assertEqual(done["remaining"], 0)

    def test_gap_fill_records_source_and_revision(self):
        station, asset, link = self.setup_link()
        self.service.create(
            self.admin, "telemetry",
            {"asset_id": asset["id"], "metric": "pressure", "value": 1,
             "observed_at": "2026-09-27T09:00:00Z", "revision": 1},
        )
        incident = self.service.create(
            self.admin, "incident",
            {"asset_id": asset["id"], "kind": "data_gap", "severity": "medium", "summary": "gap"},
        )
        gap = self.service.create(
            self.admin, "gap",
            {"incident_id": incident["id"], "asset_id": asset["id"],
             "start_at": "2026-09-27T09:00:00Z", "end_at": "2026-09-27T09:30:00Z"},
        )
        gap = self.service.transition(self.admin, gap["id"], "estimate", {"estimate": "interpolation"})
        gap = self.service.transition(self.admin, gap["id"], "fill", {"estimate": "series", "method": "linear"})
        interp = gap["data"]["interpolation"]
        self.assertEqual(interp["source"], "admin")
        self.assertEqual(interp["revision"], 1)
        self.assertEqual(interp["method"], "linear")

    def test_revise_invalidates_interpolation_and_dependents(self):
        station, asset, link = self.setup_link()
        self.service.create(
            self.admin, "telemetry",
            {"asset_id": asset["id"], "metric": "pressure", "value": 1,
             "observed_at": "2026-09-27T09:00:00Z", "revision": 1},
        )
        incident = self.service.create(
            self.admin, "incident",
            {"asset_id": asset["id"], "kind": "data_gap", "severity": "medium", "summary": "gap"},
        )
        gap = self.service.create(
            self.admin, "gap",
            {"incident_id": incident["id"], "asset_id": asset["id"],
             "start_at": "2026-09-27T09:00:00Z", "end_at": "2026-09-27T09:30:00Z"},
        )
        gap = self.service.transition(self.admin, gap["id"], "estimate", {"estimate": "interpolation"})
        gap = self.service.transition(self.admin, gap["id"], "fill", {"estimate": "series"})
        action = self.service.create(
            self.admin, "recovery_action",
            {"incident_id": incident["id"], "action_type": "remote_restart", "dedupe_key": "r1"},
        )
        action = self.service.transition(self.admin, action["id"], "approve", {})
        action = self.service.transition(self.admin, action["id"], "start", {})
        action = self.service.transition(self.admin, action["id"], "succeed", {"outcome": "ok"})
        for step in ("diagnose", "plan_recovery", "start_recovery"):
            incident = self.service.transition(self.admin, incident["id"], step, {})
        incident = self.service.transition(self.admin, incident["id"], "resolve", {"summary": "done"})
        self.assertEqual(incident["status"], "resolved")
        # a higher revision arrives: interpolation basis is outdated
        tel = self.service.list("telemetry")[0]
        self.service.transition(self.admin, tel["id"], "revise", {"value": 2, "revision": 2})
        gap = self.service.get(gap["id"])
        self.assertEqual(gap["status"], "open")
        self.assertTrue(gap["data"]["outdated"])
        action = self.service.get(action["id"])
        self.assertEqual(action["status"], "proposed")
        self.assertTrue(action["data"]["basis_outdated"])
        incident = self.service.get(incident["id"])
        self.assertEqual(incident["status"], "open")
        self.assertTrue(incident["data"]["resolution_invalidated"])
        # recalculation loop: re-fill gap on new revision, recalculate action, re-resolve
        gap = self.service.transition(self.admin, gap["id"], "estimate", {"estimate": "interp2"})
        gap = self.service.transition(self.admin, gap["id"], "fill", {"estimate": "series2"})
        self.assertEqual(gap["data"]["interpolation"]["revision"], 2)
        action = self.service.transition(self.admin, action["id"], "recalculate", {})
        self.assertEqual(action["status"], "approved")
        self.assertFalse(action["data"]["basis_outdated"])
        action = self.service.transition(self.admin, action["id"], "start", {})
        action = self.service.transition(self.admin, action["id"], "succeed", {"outcome": "redone"})
        self.assertFalse(action["data"]["basis_outdated"])
        for step in ("diagnose", "plan_recovery", "start_recovery"):
            incident = self.service.transition(self.admin, incident["id"], step, {})
        incident = self.service.transition(self.admin, incident["id"], "resolve", {"summary": "redone"})
        self.assertEqual(incident["status"], "resolved")
        self.assertFalse(incident["data"]["resolution_invalidated"])

    def test_retransmit_resumes_from_breakpoint_and_dedupes(self):
        station, asset, link = self.setup_link(capacity=10, backlog_limit=20)
        self.service.transition(self.admin, link["id"], "degrade", {"reason": "weather"})
        records = [self.rec(asset, i, source="src") for i in range(1, 9)]
        self.service.ingest_telemetry(self.admin, link["id"], records)
        # 5 accepted, 3 queued
        r1 = self.service.retransmit(self.admin, link["id"], batch_size=2)
        self.assertEqual(len(r1["transmitted"]), 2)
        self.assertEqual(r1["remaining"], 1)
        self.assertEqual(r1["cursor"], 7)
        r2 = self.service.retransmit(self.admin, link["id"], batch_size=2)
        self.assertEqual(len(r2["transmitted"]), 1)
        self.assertEqual(r2["remaining"], 0)
        self.assertEqual(r2["cursor"], 8)
        # duplicates: same records ingested again are not stored twice
        dup = self.service.ingest_telemetry(self.admin, link["id"], records)
        self.assertEqual(len(dup["duplicates"]), 8)
        self.assertEqual(len(dup["accepted"]), 0)
        self.assertEqual(len(dup["queued"]), 0)

    def test_ingest_requires_role(self):
        station, asset, link = self.setup_link()
        with self.assertRaises(PermissionDenied):
            self.service.ingest_telemetry(self.viewer, link["id"], [self.rec(asset, 1)])
        with self.assertRaises(PermissionDenied):
            self.service.retransmit(self.viewer, link["id"])

    def test_concurrent_gap_edit_only_one_succeeds(self):
        station, asset, link = self.setup_link()
        incident = self.service.create(
            self.admin, "incident",
            {"asset_id": asset["id"], "kind": "data_gap", "severity": "medium", "summary": "gap"},
        )
        gap = self.service.create(
            self.admin, "gap",
            {"incident_id": incident["id"], "asset_id": asset["id"],
             "start_at": "2026-09-27T09:00:00Z", "end_at": "2026-09-27T09:30:00Z"},
        )
        g1 = self.service.get(gap["id"])
        g2 = self.service.get(gap["id"])
        self.assertEqual(g1["version"], 1)
        self.service.transition(self.admin, gap["id"], "estimate", {"estimate": "a"}, g1["version"])
        with self.assertRaises(ConflictError):
            self.service.transition(self.admin, gap["id"], "estimate", {"estimate": "b"}, g2["version"])

    def test_concurrent_mission_edit_only_one_succeeds(self):
        station, asset, link = self.setup_link()
        mission = self.service.create(
            self.admin, "mission",
            {"station_id": station["id"], "purpose": "repair",
             "window_start": "2026-09-28T09:00:00Z", "window_end": "2026-09-28T18:00:00Z"},
        )
        m1 = self.service.get(mission["id"])
        m2 = self.service.get(mission["id"])
        self.service.transition(self.admin, mission["id"], "approve", {}, m1["version"])
        with self.assertRaises(ConflictError):
            self.service.transition(self.admin, mission["id"], "approve", {}, m2["version"])

    def test_simultaneous_gap_edits_one_winner_one_conflict(self):
        station, asset, link = self.setup_link()
        incident = self.service.create(
            self.admin, "incident",
            {"asset_id": asset["id"], "kind": "data_gap", "severity": "medium", "summary": "gap"},
        )
        gap = self.service.create(
            self.admin, "gap",
            {"incident_id": incident["id"], "asset_id": asset["id"],
             "start_at": "2026-09-27T09:00:00Z", "end_at": "2026-09-27T09:30:00Z"},
        )
        start = threading.Barrier(2)
        results = []

        def edit(estimate):
            entity = self.service.get(gap["id"])
            start.wait()
            try:
                self.service.transition(
                    self.admin, gap["id"], "estimate", {"estimate": estimate}, entity["version"],
                )
                results.append("ok")
            except ConflictError:
                results.append("conflict")

        t1 = threading.Thread(target=edit, args=("a",))
        t2 = threading.Thread(target=edit, args=("b",))
        t1.start()
        t2.start()
        t1.join()
        t2.join()
        self.assertEqual(sorted(results), ["conflict", "ok"])
        gap = self.service.get(gap["id"])
        self.assertEqual(gap["version"], 2)
        self.assertEqual(gap["status"], "estimated")


class LinkTelemetryHttpTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.service = DomainService(SQLiteRepository(Path(self.tmp.name) / "test.db"), RuleEngine())
        self.server = create_server("127.0.0.1", 0, self.service, RuleEngine(), str(Path(__file__).resolve().parent.parent / "static"))
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
        self.tmp.cleanup()

    def call(self, method, path, body=None, headers=None):
        conn = HTTPConnection("127.0.0.1", self.port, timeout=5)
        payload = json.dumps(body).encode("utf-8") if body is not None else None
        conn.request(method, path, payload, headers or {})
        response = conn.getresponse()
        data = response.read()
        conn.close()
        return response.status, json.loads(data.decode("utf-8"))

    def test_ingest_and_retransmit_over_http(self):
        station = self.service.create(Actor("admin", "admin"), "station", {"name": "S", "region": "R"})
        asset = self.service.create(
            Actor("admin", "admin"), "asset",
            {"station_id": station["id"], "asset_type": "sensor", "serial_no": "A",
             "last_seen": "2026-09-27T09:00:00Z"},
        )
        link = self.service.create(
            Actor("admin", "admin"), "link",
            {"station_id": station["id"], "asset_id": asset["id"], "link_type": "fiber",
             "capacity": 2, "backlog_limit": 5},
        )
        self.service.transition(Actor("admin", "admin"), link["id"], "degrade", {"reason": "weather"})
        records = [
            {"asset_id": asset["id"], "metric": "pressure", "value": i,
             "observed_at": "2026-09-27T09:%02d:00Z" % i, "revision": i,
             "source_id": "s", "record_id": "r%d" % i}
            for i in range(1, 5)
        ]
        status, body = self.call(
            "POST", "/api/links/%s/telemetry" % link["id"], {"records": records},
            {"Content-Type": "application/json", "X-User-Id": "admin", "X-Role": "admin"},
        )
        self.assertEqual(status, 200)
        self.assertEqual(len(body["queued"]), 3)
        # retransmit in small batches
        status, body = self.call(
            "POST", "/api/links/%s/retransmit" % link["id"], {"batch_size": 2},
            {"Content-Type": "application/json", "X-User-Id": "admin", "X-Role": "admin"},
        )
        self.assertEqual(status, 200)
        self.assertEqual(len(body["transmitted"]), 2)
        self.assertEqual(body["remaining"], 1)
        # duplicate ingest over http: stored once
        status, body = self.call(
            "POST", "/api/links/%s/telemetry" % link["id"], {"records": records},
            {"Content-Type": "application/json", "X-User-Id": "admin", "X-Role": "admin"},
        )
        self.assertEqual(status, 200)
        self.assertEqual(len(body["duplicates"]), 4)
        self.assertEqual(len(body["accepted"]), 0)


if __name__ == "__main__":
    unittest.main()
