import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from collector_06.engine import run_scenario, load_scenario, validate_options

REPO_ROOT = Path(__file__).resolve().parents[1]


def build_root(directory, routes):
    """routes: {route: [ics_text, ...]}"""
    root = Path(directory)
    manifest = {}
    for route, bodies in routes.items():
        entries = []
        for index, body in enumerate(bodies):
            name = f"{route.strip('/').replace('/', '_')}_{index}.ics"
            (root / name).write_text(body, encoding="utf-8")
            entries.append({"status": 200, "file": name})
        manifest[route] = entries
    (root / "routes.json").write_text(json.dumps(manifest), encoding="utf-8")
    return root


def scenario(directory, routes, sources, run_id="run-x", options=None):
    root = build_root(directory, routes)
    return {
        "name": run_id,
        "run_id": run_id,
        "sources": sources,
        "options": options or {"matching": "explicit_only"},
        "fixture_root": str(root),
    }


def run_twice(manifest, database):
    first = run_scenario(manifest, database)
    second = run_scenario(manifest, database)
    for key in ("sessions", "replays", "unresolved", "history"):
        assert first[key] == second[key], f"{key} 在重复进程后发生漂移"
    assert second["notifications"] == 0, "第二次进程不应产生新通知"
    return first, second


def vevent(fields):
    lines = ["BEGIN:VCALENDAR", "BEGIN:VEVENT"]
    lines.extend(f"{key}:{value}" for key, value in fields)
    lines += ["END:VEVENT", "END:VCALENDAR", ""]
    return "\n".join(lines)


SESSION_V1 = vevent([
    ("UID", "LIVE-A"), ("SEQUENCE", 1),
    ("DTSTART", "20261111T010000Z"), ("DTEND", "20261111T020000Z"),
    ("X-ACTIVITY", "FAIR-A"),
])
SESSION_V2_MOVED = vevent([
    ("UID", "LIVE-A"), ("SEQUENCE", 2),
    ("DTSTART", "20261112T010000Z"), ("DTEND", "20261112T020000Z"),
    ("X-ACTIVITY", "FAIR-A"),
])
CANCEL_V3 = vevent([("UID", "LIVE-A"), ("SEQUENCE", 3), ("STATUS", "CANCELLED")])
REPLAY_OLD = vevent([
    ("UID", "REPLAY-OLD"), ("X-TYPE", "REPLAY"),
    ("X-SESSION", "LIVE-A"), ("X-SESSION-SEQUENCE", 1),
    ("X-URL", "https://video.invalid/old"),
])
REPLAY_ACTIVITY_ONLY = vevent([
    ("UID", "REPLAY-X"), ("X-TYPE", "REPLAY"), ("X-ACTIVITY", "FAIR-A"),
])


class WorkflowProjectionTests(unittest.TestCase):
    def test_rescheduled_replay_anchors_to_shot_revision(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "state.sqlite3"
            manifest = scenario(directory, {
                "/schedule": [SESSION_V1],
                "/move": [SESSION_V2_MOVED],
                "/replay": [REPLAY_OLD],
            }, ["/schedule", "/move", "/replay"], "run-rescheduled")
            result, _ = run_twice(manifest, database)
            (session,) = result["sessions"]
            self.assertEqual(session["uid"], "LIVE-A")
            self.assertEqual(session["sequence"], 2)
            self.assertEqual(session["start"], "20261112T010000Z")
            self.assertEqual(session["state"], "scheduled")
            (replay,) = result["replays"]
            self.assertEqual(replay["session"], "LIVE-A")
            self.assertEqual(replay["session_sequence"], 1)  # 不挂到 seq2
            self.assertEqual(replay["evidence"]["decision"], "explicit_session_revision")
            self.assertEqual(replay["evidence"]["anchored_fields"], ["X-SESSION", "X-SESSION-SEQUENCE"])
            self.assertTrue(replay["evidence"]["target_version_observed"])
            self.assertEqual(result["unresolved"], [])
            self.assertEqual([h["sequence"] for h in result["history"]], [1, 2])
            self.assertEqual([h["kind"] for h in result["history"]], ["scheduled", "rescheduled"])

    def test_late_lower_sequence_only_archives_history(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "state.sqlite3"
            manifest = scenario(directory, {
                "/move": [SESSION_V2_MOVED],
                "/schedule": [SESSION_V1],
            }, ["/move", "/schedule"], "run-late")
            result, _ = run_twice(manifest, database)
            (session,) = result["sessions"]
            self.assertEqual(session["sequence"], 2)  # 当前日程不倒退
            kinds = {(h["sequence"], h["kind"]) for h in result["history"]}
            self.assertEqual(kinds, {(2, "scheduled"), (1, "late_archive")})
    def test_cancel_changes_state_but_keeps_archived_replays(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "state.sqlite3"
            manifest = scenario(directory, {
                "/schedule": [SESSION_V1],
                "/replay": [REPLAY_OLD],
                "/cancel": [CANCEL_V3],
            }, ["/schedule", "/replay", "/cancel"], "run-cancel")
            result, _ = run_twice(manifest, database)
            (session,) = result["sessions"]
            self.assertEqual(session["state"], "cancelled")
            self.assertEqual(session["sequence"], 3)
            self.assertEqual(session["start"], "20261111T010000Z")  # 无 DTSTART 时回填历史时间
            (replay,) = result["replays"]
            self.assertEqual((replay["uid"], replay["session_sequence"]), ("REPLAY-OLD", 1))

    def test_repeated_cancellation_is_idempotent(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "state.sqlite3"
            manifest = scenario(directory, {
                "/schedule": [SESSION_V1],
                "/replay": [REPLAY_OLD],
                "/cancel-a": [CANCEL_V3],
                "/cancel-b": [CANCEL_V3],
            }, ["/schedule", "/replay", "/cancel-a", "/cancel-b"], "run-cancel-twice")
            result, second = run_twice(manifest, database)
            (session,) = result["sessions"]
            self.assertEqual(session["state"], "cancelled")
            self.assertEqual(len([h for h in result["history"] if h["state"] == "cancelled"]), 1)
            self.assertEqual(len(result["replays"]), 1)
            self.assertEqual(second["notifications"], 0)

    def test_activity_only_replay_is_unresolved_under_explicit_only(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "state.sqlite3"
            manifest = scenario(directory, {
                "/schedule": [SESSION_V1],
                "/ambiguous": [REPLAY_ACTIVITY_ONLY],
            }, ["/schedule", "/ambiguous"], "run-unresolved")
            result, _ = run_twice(manifest, database)
            self.assertEqual(result["replays"], [])
            (item,) = result["unresolved"]
            self.assertEqual(item["uid"], "REPLAY-X")
            self.assertEqual(item["reason"], "missing_session_revision")
            self.assertIn("X-ACTIVITY", item["evidence"]["fields"])
            self.assertEqual(item["evidence"]["missing_fields"], ["X-SESSION", "X-SESSION-SEQUENCE"])

    def test_same_sequence_different_schedule_is_conflict_and_not_merged(self):
        conflicting = vevent([
            ("UID", "LIVE-A"), ("SEQUENCE", 1),
            ("DTSTART", "20261111T090000Z"), ("DTEND", "20261111T100000Z"),
            ("X-ACTIVITY", "FAIR-A"),
        ])
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "state.sqlite3"
            manifest = scenario(directory, {
                "/page-a": [SESSION_V1],
                "/page-b": [conflicting],
            }, ["/page-a", "/page-b"], "run-conflict")
            result, second = run_twice(manifest, database)
            (session,) = result["sessions"]
            self.assertEqual(session["sequence"], 1)
            self.assertEqual(session["start"], "20261111T010000Z")  # 不被相邻/冲突版本覆盖
            conflicts = [h for h in result["history"] if h["state"] == "conflict"]
            self.assertEqual(len(conflicts), 1)
            self.assertEqual(conflicts[0]["sequence"], 1)
            self.assertEqual(
                conflicts[0]["evidence"]["decision"],
                "same_sequence_different_schedule_isolated",
            )
            self.assertGreaterEqual(result["notifications"], 1)
            self.assertEqual(second["notifications"], 0)

    def test_recurrence_id_forms_distinct_session_identity(self):
        day_one = vevent([
            ("UID", "LIVE-R"), ("RECURRENCE-ID", "20261111T010000Z"), ("SEQUENCE", 1),
            ("DTSTART", "20261111T010000Z"), ("X-ACTIVITY", "FAIR-R"),
        ])
        day_two = vevent([
            ("UID", "LIVE-R"), ("RECURRENCE-ID", "20261112T010000Z"), ("SEQUENCE", 1),
            ("DTSTART", "20261112T010000Z"), ("X-ACTIVITY", "FAIR-R"),
        ])
        day_one_moved = vevent([
            ("UID", "LIVE-R"), ("RECURRENCE-ID", "20261111T010000Z"), ("SEQUENCE", 2),
            ("DTSTART", "20261111T030000Z"), ("X-ACTIVITY", "FAIR-R"),
        ])
        replay = vevent([
            ("UID", "REPLAY-R"), ("X-TYPE", "REPLAY"),
            ("X-RECURRENCE-ID", "20261111T010000Z"),
            ("X-SESSION", "LIVE-R"), ("X-SESSION-SEQUENCE", 1),
        ])
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "state.sqlite3"
            manifest = scenario(directory, {
                "/cal": [day_one + day_two + day_one_moved],
                "/replay": [replay],
            }, ["/cal", "/replay"], "run-recurring")
            result, _ = run_twice(manifest, database)
            keys = {(s["uid"], s["recurrence_id"], s["sequence"]) for s in result["sessions"]}
            self.assertEqual(keys, {
                ("LIVE-R", "20261111T010000Z", 2),
                ("LIVE-R", "20261112T010000Z", 1),
            })
            (item,) = result["replays"]
            self.assertEqual(item["recurrence_id"], "20261111T010000Z")
            self.assertEqual(item["session_sequence"], 1)  # 改期后仍指向拍摄版本

    def test_replay_to_unknown_revision_is_kept_explicit(self):
        replay_future = vevent([
            ("UID", "REPLAY-F"), ("X-TYPE", "REPLAY"),
            ("X-SESSION", "LIVE-A"), ("X-SESSION-SEQUENCE", 9),
        ])
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "state.sqlite3"
            manifest = scenario(directory, {
                "/schedule": [SESSION_V1],
                "/replay": [replay_future],
            }, ["/schedule", "/replay"], "run-unknown-revision")
            result, _ = run_twice(manifest, database)
            (item,) = result["replays"]
            self.assertEqual(item["session_sequence"], 9)
            self.assertFalse(item["evidence"]["target_version_observed"])
            self.assertEqual(result["unresolved"], [])

    def test_page_refetch_does_not_drop_archived_replays(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "state.sqlite3"
            first_manifest = scenario(directory, {
                "/schedule": [SESSION_V1],
                "/replay": [REPLAY_OLD],
            }, ["/schedule", "/replay"], "run-page-1")
            run_scenario(first_manifest, database)
            updated = scenario(directory, {
                "/schedule": [SESSION_V2_MOVED],
                "/replay": [REPLAY_OLD],
            }, ["/schedule", "/replay"], "run-page-2")
            result = run_scenario(updated, database)
            self.assertEqual(len(result["replays"]), 1)
            self.assertEqual(result["replays"][0]["session_sequence"], 1)
            self.assertEqual(result["sessions"][0]["sequence"], 2)


class ConfigurableMatchingTests(unittest.TestCase):
    def test_explicit_only_rejects_inline_rules(self):
        with self.assertRaisesRegex(ValueError, "explicit_only"):
            validate_options({
                "matching": "explicit_only",
                "match_rules": ["activity_single_session"],
            })

    def test_unknown_mode_and_rule_are_rejected(self):
        with self.assertRaisesRegex(ValueError, "matching"):
            validate_options({"matching": "guess"})
        with self.assertRaisesRegex(ValueError, "match_rules"):
            validate_options({"matching": "configurable", "match_rules": []})
        with self.assertRaisesRegex(ValueError, "未知匹配规则"):
            validate_options({"matching": "configurable", "match_rules": ["neighbor_merge"]})

    def test_activity_single_session_rule_only_when_explicitly_enabled(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "state.sqlite3"
            configured = scenario(directory, {
                "/schedule": [SESSION_V1],
                "/ambiguous": [REPLAY_ACTIVITY_ONLY],
            }, ["/schedule", "/ambiguous"], "run-cfg", {
                "matching": "configurable",
                "match_rules": ["explicit_session_revision", "activity_single_session"],
            })
            result, _ = run_twice(configured, database)
            (item,) = result["replays"]
            self.assertEqual(item["session"], "LIVE-A")
            self.assertEqual(item["evidence"]["decision"], "configured_rule:activity_single_session")
            self.assertEqual(result["unresolved"], [])

    def test_activity_rule_with_multiple_candidates_stays_unresolved(self):
        other = vevent([
            ("UID", "LIVE-B"), ("SEQUENCE", 1),
            ("DTSTART", "20261115T010000Z"), ("X-ACTIVITY", "FAIR-A"),
        ])
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "state.sqlite3"
            configured = scenario(directory, {
                "/schedule": [SESSION_V1 + other],
                "/ambiguous": [REPLAY_ACTIVITY_ONLY],
            }, ["/schedule", "/ambiguous"], "run-cfg-many", {
                "matching": "configurable",
                "match_rules": ["explicit_session_revision", "activity_single_session"],
            })
            result, _ = run_twice(configured, database)
            self.assertEqual(result["replays"], [])
            (item,) = result["unresolved"]
            self.assertEqual(item["reason"], "missing_session_revision")
            self.assertEqual(item["evidence"]["activity_rule_candidates"], 2)

    def test_time_proximity_never_merges_by_default(self):
        near_replay = vevent([
            ("UID", "REPLAY-T"), ("X-TYPE", "REPLAY"), ("X-ACTIVITY", "FAIR-A"),
            ("X-RECORDED-AT", "20261111T010500Z"),
        ])
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "state.sqlite3"
            default_manifest = scenario(directory, {
                "/schedule": [SESSION_V1],
                "/replay": [near_replay],
            }, ["/schedule", "/replay"], "run-near-default")
            default_result, _ = run_twice(default_manifest, database)
            self.assertEqual(default_result["replays"], [])
            (item,) = default_result["unresolved"]
            self.assertEqual(item["reason"], "missing_session_revision")

            configured = scenario(directory, {
                "/schedule": [SESSION_V1],
                "/replay": [near_replay],
            }, ["/schedule", "/replay"], "run-near-cfg", {
                "matching": "configurable",
                "match_rules": ["explicit_session_revision", "time_proximity"],
            })
            cfg_result, _ = run_twice(configured, database)
            (linked,) = cfg_result["replays"]
            self.assertEqual(linked["session"], "LIVE-A")
            self.assertEqual(linked["evidence"]["decision"], "configured_rule:time_proximity")
            self.assertEqual(linked["evidence"]["delta_seconds"], 300.0)


    def test_time_proximity_tie_stays_unresolved(self):
        tied_calendar = vevent([
            ("UID", "L1"), ("SEQUENCE", 1),
            ("DTSTART", "20261111T010000Z"), ("X-ACTIVITY", "FAIR-A"),
        ]) + vevent([
            ("UID", "L2"), ("SEQUENCE", 1),
            ("DTSTART", "20261111T020000Z"), ("X-ACTIVITY", "FAIR-A"),
        ])
        midpoint_replay = vevent([
            ("UID", "REPLAY-TIE"), ("X-TYPE", "REPLAY"), ("X-ACTIVITY", "FAIR-A"),
            ("X-RECORDED-AT", "20261111T013000Z"),
        ])
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "state.sqlite3"
            configured = scenario(directory, {
                "/calendar": [tied_calendar],
                "/replay": [midpoint_replay],
            }, ["/calendar", "/replay"], "run-tie", {
                "matching": "configurable",
                "match_rules": ["explicit_session_revision", "time_proximity"],
            })
            result, _ = run_twice(configured, database)
            self.assertEqual(result["replays"], [])
            (item,) = result["unresolved"]
            self.assertEqual(item["reason"], "missing_session_revision")
            self.assertEqual(item["evidence"]["time_proximity_result"], "tie_or_no_candidate")

    def test_malformed_sequence_and_failed_source_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            bad = vevent([("UID", "X"), ("SEQUENCE", "oops")])
            manifest = scenario(directory, {"/bad": [bad]}, ["/bad"], "run-bad")
            with self.assertRaisesRegex(ValueError, "SEQUENCE"):
                run_scenario(manifest, Path(directory) / "state.sqlite3")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "routes.json").write_text(json.dumps({"/down": [{"status": 503}]}))
            manifest = {
                "name": "down", "run_id": "run-down", "sources": ["/down"],
                "options": {"matching": "explicit_only"}, "fixture_root": str(root),
            }
            with self.assertRaisesRegex(ValueError, "503"):
                run_scenario(manifest, root / "state.sqlite3")


class ScenarioFileAndCliTests(unittest.TestCase):
    def test_public_fixture_scenarios_load_without_expected_branching(self):
        for name in ("rescheduled", "cancel", "unresolved", "late"):
            loaded = load_scenario(REPO_ROOT / "fixtures" / f"{name}.json")
            self.assertEqual(loaded["run_id"], f"run-{name}")
            self.assertNotIn("expected", loaded)

    def test_cli_run_writes_output_and_survives_restart(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "state.sqlite3"
            output = Path(directory) / "rescheduled.json"
            command = [
                sys.executable, "-m", "collector_06.cli",
                "--run", str(REPO_ROOT / "fixtures" / "rescheduled.json"),
                "--database", str(database),
                "--output", str(output),
            ]
            first = subprocess.run(command, cwd=REPO_ROOT, text=True, capture_output=True, timeout=5)
            self.assertEqual(first.returncode, 0, first.stderr)
            payload = json.loads(output.read_text(encoding="utf-8"))
            self.assertEqual(payload["sessions"][0]["uid"], "LIVE-A")
            second = subprocess.run(command, cwd=REPO_ROOT, text=True, capture_output=True, timeout=5)
            self.assertEqual(second.returncode, 0, second.stderr)
            repeated = json.loads(output.read_text(encoding="utf-8"))
            self.assertEqual(repeated["notifications"], 0)
            self.assertEqual(repeated["sessions"], payload["sessions"])


if __name__ == "__main__":
    unittest.main()
