"""0.2.2 P1 additions: dsh skills, description lines, mtime cache, task_status."""

import json
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from zcode_supervisor import hub_server as H


def _make_skill(directory: Path, name: str, description=None, body="secret body"):
    skill = directory / name
    skill.mkdir(parents=True, exist_ok=True)
    if description is None:
        text = f"---\nname: {name}\n---\n\n{body}\n"
    else:
        text = f'---\nname: {name}\ndescription: "{description}"\n---\n\n{body}\n'
    (skill / "SKILL.md").write_text(text, encoding="utf-8")
    return skill / "SKILL.md"


class SkillsListing(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        H._skills_cache.clear()

    def tearDown(self):
        self.tmp.cleanup()

    def test_dsh_worker_lists_user_skills_with_description(self):
        skills_dir = self.root / "skills"
        _make_skill(skills_dir, "zcode-bridge", "读 ZCode 会话并派发 CLI 任务")
        _make_skill(skills_dir, "plain", None)
        listing = H.list_skills_impl("dsh", roots=[(skills_dir, "*/SKILL.md")])
        self.assertEqual(listing["worker"], "dsh")
        by_name = {entry["name"]: entry for entry in listing["skills"]}
        self.assertEqual(by_name["zcode-bridge"]["description"],
                         "读 ZCode 会话并派发 CLI 任务")
        self.assertIsNone(by_name["plain"]["description"])
        # body text never enters the listing
        self.assertNotIn("secret body", json.dumps(listing))

    def test_description_unquoted_and_clipped(self):
        skills_dir = self.root / "skills"
        marker = skills_dir / "long"
        marker.parent.mkdir(parents=True, exist_ok=True)
        marker.mkdir()
        (marker / "SKILL.md").write_text(
            "---\nname: long\ndescription: " + "很" * 500 + "\n---\nbody\n",
            encoding="utf-8")
        listing = H.list_skills_impl("dsh", roots=[(skills_dir, "*/SKILL.md")])
        entry = listing["skills"][0]
        self.assertLessEqual(len(entry["description"] or ""),
                             H._DESCRIPTION_LIMIT)

    def test_cache_invalidates_on_mtime_change(self):
        skills_dir = self.root / "skills"
        marker = _make_skill(skills_dir, "evolving", "first")
        listing = H.list_skills_impl("dsh", roots=[(skills_dir, "*/SKILL.md")])
        self.assertEqual(listing["skills"][0]["description"], "first")
        # content rewritten but the mtime FORCED back: the cache still serves
        # the old parse (the mtime is the cache key)
        original_mtime = marker.stat().st_mtime
        marker.write_text('---\nname: evolving\ndescription: "second"\n---\n',
                          encoding="utf-8")
        os.utime(marker, (original_mtime, original_mtime))
        listing = H.list_skills_impl("dsh", roots=[(skills_dir, "*/SKILL.md")])
        self.assertEqual(listing["skills"][0]["description"], "first")
        # a real mtime move re-reads the frontmatter
        marker.write_text('---\nname: evolving\ndescription: "third"\n---\n',
                          encoding="utf-8")
        current = marker.stat().st_mtime
        os.utime(marker, (current + 5, current + 5))
        listing = H.list_skills_impl("dsh", roots=[(skills_dir, "*/SKILL.md")])
        self.assertEqual(listing["skills"][0]["description"], "third")

    def test_worker_skill_names_never_raises(self):
        names = H.worker_skill_names("zcode")
        self.assertIsInstance(names, list)
        self.assertLessEqual(len(names), 40)


class TaskStatus(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.config = SimpleNamespace(
            state_dir=self.root / "state", work_root=self.root / "work")

    def tearDown(self):
        self.tmp.cleanup()

    def _write_task(self, task_id, status, outcome=None, running_ledger=False):
        record = {"task_id": task_id, "worker": "zcode", "mode": "cold",
                  "status": status, "outcome": outcome, "updated": "now",
                  "contract_id": "c-" + task_id}
        evidence = self.root / "evidence" / task_id
        if running_ledger:
            evidence.mkdir(parents=True, exist_ok=True)
            ledger = evidence / "run.progress.jsonl"
            ledger.write_text(json.dumps(
                {"t": 0.0, "kind": "event", "detail": "tool:Edit"}) + "\n")
            record["evidence_dir"] = str(evidence)
        path = self.config.state_dir / "hub" / "tasks" / f"{task_id}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(record))

    def test_running_task_gets_heartbeat_verdict(self):
        self._write_task("t-run", "running", running_ledger=True)
        self._write_task("t-done", "completed", outcome="delivered")
        report = H.task_status_impl(self.config)
        by_id = {entry["task_id"]: entry for entry in report["tasks"]}
        self.assertEqual(by_id["t-done"]["outcome"], "delivered")
        self.assertNotIn("heartbeat", by_id["t-done"])
        self.assertEqual(by_id["t-run"]["status"], "running")
        self.assertEqual(by_id["t-run"]["heartbeat"]["state"], "running")
        self.assertEqual(by_id["t-run"]["mode"], "cold")

    def test_unknown_task_id_is_an_error(self):
        with self.assertRaises(H.HubError):
            H.task_status_impl(self.config, task_id="missing")

    def test_finished_ledger_reports_outcome(self):
        self._write_task("t-fin", "running", running_ledger=True)
        ledger = (self.root / "evidence" / "t-fin" / "run.progress.jsonl")
        ledger.write_text(json.dumps(
            {"t": 1.0, "kind": "final", "detail": "delivered"}) + "\n")
        report = H.task_status_impl(self.config, task_id="t-fin")
        self.assertEqual(report["tasks"][0]["heartbeat"]["state"], "finished")
        self.assertEqual(report["tasks"][0]["heartbeat"]["outcome"], "delivered")


if __name__ == "__main__":
    unittest.main()
