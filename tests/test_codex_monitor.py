"""codex-status view: rollout tail parsing, dsh labels, composed status."""

import json
import tempfile
import unittest
from pathlib import Path

from zcode_supervisor import codex_monitor


def _rollout(path: Path):
    records = [
        {"timestamp": "2026-11-02T16:00:00.000Z", "type": "session_meta",
         "payload": {"id": "s1", "cwd": "/repo", "cli_version": "0.166.0",
                     "git": {"branch": "main"}}},
        {"timestamp": "2026-11-02T16:01:00.000Z", "type": "response_item",
         "payload": {"type": "message", "role": "developer",
                     "content": [{"type": "input_text", "text": "sys"}]}},
        {"timestamp": "2026-11-02T16:02:00.000Z", "type": "response_item",
         "payload": {"type": "message", "role": "user",
                     "content": [{"type": "input_text",
                                  "text": "# AGENTS.md instructions"}]}},
        {"timestamp": "2026-11-02T16:03:00.000Z", "type": "response_item",
         "payload": {"type": "message", "role": "user",
                     "content": [{"type": "input_text",
                                  "text": "[143] tool send_message call: {}"}]}},
        {"timestamp": "2026-11-02T16:04:00.000Z", "type": "response_item",
         "payload": {"type": "message", "role": "user",
                     "content": [{"type": "input_text",
                                  "text": "  请继续完成审核"}]}},
        {"timestamp": "2026-11-02T16:05:00.000Z", "type": "response_item",
         "payload": {"type": "message", "role": "assistant",
                     "content": [{"type": "output_text", "text": "岸线判断已改为用原始面积计算容量。"}]}},
        {"timestamp": "2026-11-02T16:06:00.000Z", "type": "response_item",
         "payload": {"type": "agent_message", "author": "/root/audit_water",
                     "recipient": "/root",
                     "content": [{"type": "input_text",
                                  "text": "Message Type: FINAL_ANSWER Payload: done"}]}},
        {"timestamp": "2026-11-02T16:07:00.000Z", "type": "event_msg",
         "payload": {"type": "item_completed"}},
    ]
    path.write_text("\n".join(json.dumps(record) for record in records),
                    encoding="utf-8")


class TailSummary(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.rollout = Path(self.tmp.name) / "rollout-x.jsonl"
        _rollout(self.rollout)

    def tearDown(self):
        self.tmp.cleanup()

    def test_whitelist_and_frames(self):
        summary = codex_monitor.tail_summary(self.rollout)
        self.assertEqual(summary["meta"]["branch"], "main")
        self.assertEqual(summary["meta"]["cwd"], "/repo")
        kinds = [item["kind"] for item in summary["recent"]]
        # developer role, instructions, and [N] harness frames are dropped
        self.assertNotIn("developer", kinds)
        texts = [item["text"] for item in summary["recent"]]
        self.assertFalse(any(text.startswith("#") for text in texts))
        self.assertFalse(any(text.startswith("[143]") for text in texts))
        self.assertIn("请继续完成审核", texts[0])
        self.assertTrue(any(kind.startswith("agent:/root/audit_water->/root")
                            for kind in kinds))
        self.assertEqual(summary["last_timestamp"], "2026-11-02T16:07:00.000Z")

    def test_text_clipping(self):
        long_text = "字" * 500
        path = Path(self.tmp.name) / "rollout-long.jsonl"
        path.write_text(json.dumps({
            "timestamp": "2026-11-02T16:00:00.000Z", "type": "response_item",
            "payload": {"type": "message", "role": "assistant",
                        "content": [{"type": "output_text", "text": long_text}]}}),
            encoding="utf-8")
        item = codex_monitor.tail_summary(path)["recent"][0]
        self.assertLessEqual(len(item["text"]), codex_monitor.TEXT_LIMIT)


class ActivityDiscovery(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)

    def tearDown(self):
        self.tmp.cleanup()

    def test_fresh_rollouts_window(self):
        sessions = self.root / "sessions" / "2026" / "11" / "03"
        sessions.mkdir(parents=True)
        recent = sessions / "recent.jsonl"
        stale = sessions / "stale.jsonl"
        recent.write_text("{}\n")
        stale.write_text("{}\n")
        import os
        old = recent.stat().st_mtime - 7200
        os.utime(stale, (old, old))
        found = codex_monitor.fresh_rollouts(self.root, fresh_s=600)
        self.assertEqual([entry["path"].name for entry in found], ["recent.jsonl"])

    def test_dsh_label_extraction(self):
        sessions = self.root / "sessions"
        base = ("--Users-x-repo-ref-handoff-review"
                "-dsh-native-shared-schema-max-1-workspace--")
        (sessions / base).mkdir(parents=True)
        (sessions / "unrelated-dir").mkdir()
        entries = codex_monitor.dsh_activity(self.root, fresh_s=600)
        self.assertEqual(len(entries), 2)
        labelled = [entry for entry in entries if entry.get("label") != entry["dir"]]
        self.assertEqual(len(labelled), 1)
        self.assertEqual(labelled[0]["label"], "native-shared-schema")
        self.assertEqual(labelled[0]["tier"], "1")

    def test_status_without_config(self):
        document = codex_monitor.status(None, fresh_s=600, codex_home=self.root,
                                        dsh_home=self.root)
        self.assertFalse(document["czs_runs"]["available"])
        text = codex_monitor.render_text(document)
        self.assertIn("codex:", text)
        self.assertIn("dsh:", text)
        self.assertIn("czs:", text)


if __name__ == "__main__":
    unittest.main()
