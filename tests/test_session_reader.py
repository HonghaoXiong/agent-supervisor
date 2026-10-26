"""Session reader tests: whitelist-only summaries, truncation, pagination,
and honest unlocatable results. No real ZCode database or codex home is read;
codex rollouts are synthesized inside a temp CODEX_HOME."""

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from zcode_supervisor import session_reader as S


def make_rollout(codex_home: Path, thread_id: str, lines=None) -> Path:
    """Synthesize a rollout file locate_rollout can find and trust."""
    sessions = codex_home / "sessions" / "2026" / "10" / "24"
    sessions.mkdir(parents=True)
    path = sessions / f"rollout-20261024T120000-{thread_id}.jsonl"
    meta = {"type": "session_meta",
            "payload": {"id": thread_id, "session_id": thread_id,
                        "cwd": "/tmp/demo", "cli_version": "0.2.0"}}
    records = [meta] + (lines or [])
    path.write_text("\n".join(json.dumps(record) for record in records) + "\n",
                    encoding="utf-8")
    return path


def make_zcode_db(path: Path, session_id: str = "sess_reader1",
                  message_count: int = 5) -> None:
    """Fixture store mirroring the real 0.17.0 shape: message.data carries
    metadata only (role plus a planted secret); body text lives in part rows
    of type "text", with non-text parts mixed in for the counting rules."""
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path)
    try:
        connection.executescript(
            """
            CREATE TABLE session (
                id TEXT PRIMARY KEY, directory TEXT, title TEXT,
                time_created INTEGER, time_updated INTEGER);
            CREATE TABLE message (
                id TEXT PRIMARY KEY, session_id TEXT, time_created INTEGER,
                data TEXT, sequence INTEGER);
            CREATE TABLE part (
                id TEXT PRIMARY KEY, message_id TEXT, session_id TEXT,
                time_created INTEGER, time_updated INTEGER, data TEXT,
                sequence INTEGER);
            CREATE TABLE tool_usage (
                session_id TEXT, tool_name TEXT, read_only INTEGER,
                destructive INTEGER, approval_status TEXT, status TEXT,
                exit_code INTEGER, duration_ms INTEGER);
            """)
        connection.execute(
            "INSERT INTO session VALUES (?,?,?,?,?)",
            (session_id, "/tmp/project", "reader fixture", 1000, 2000))
        for index in range(message_count):
            body = f"message body {index} " + "x" * (300 if index == 1 else 10)
            data = {
                "role": "user" if index % 2 == 0 else "assistant",
                "time": {"created": 1000 + index},
                "api_key": "sk-SUPER-SECRET-VALUE",
                "usage": {"inputTokens": 999},
            }
            connection.execute(
                "INSERT INTO message VALUES (?,?,?,?,?)",
                (f"m{index}", session_id, 1000 + index,
                 json.dumps(data), index))
            # message 2: two text parts concatenated, plus counted non-text parts
            texts = [body] if index != 2 else [body, " tail"]
            for position, text in enumerate(texts):
                connection.execute(
                    "INSERT INTO part VALUES (?,?,?,?,?,?,?)",
                    (f"p{index}-{position}", f"m{index}", session_id,
                     1000 + index, 1000 + index,
                     json.dumps({"type": "text", "text": text,
                                 "api_key": "sk-PART-SECRET"}),
                     position))
            if index == 2:
                connection.execute(
                    "INSERT INTO part VALUES (?,?,?,?,?,?,?)",
                    (f"p{index}-tool", f"m{index}", session_id,
                     1000 + index, 1000 + index,
                     json.dumps({"type": "tool", "callID": "call-1",
                                 "tool": "Bash", "state": {
                                     "input": {"command": "ls"},
                                     "output": "raw"}}), 2))
                connection.execute(
                    "INSERT INTO part VALUES (?,?,?,?,?,?,?)",
                    (f"p{index}-reason", f"m{index}", session_id,
                     1000 + index, 1000 + index,
                     json.dumps({"type": "reasoning",
                                 "text": "private chain of thought"}), 3))
        connection.execute(
            "INSERT INTO tool_usage VALUES (?,?,?,?,?,?,?,?)",
            (session_id, "Read", 1, 0, "approved", "succeeded", 0, 12))
        connection.execute(
            "INSERT INTO tool_usage VALUES (?,?,?,?,?,?,?,?)",
            (session_id, "Bash", 0, 0, "denied", "failed", 1, 5))
        connection.commit()
    finally:
        connection.close()


class CodexContextTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.codex_home = Path(self.temporary.name) / "codex-home"

    def test_located_rollout_yields_whitelist_summary(self):
        thread = "aaaa1111-2222-3333-4444-555566667777"
        make_rollout(self.codex_home, thread, lines=[
            {"type": "response_item", "payload": {"type": "message",
                                                  "role": "assistant",
                                                  "secret": "sk-live"}},
            {"type": "event_msg", "payload": {"type": "user_message",
                                              "command": "ls -la"}},
        ])
        result = S.read_codex_context(thread, codex_home=self.codex_home)
        self.assertEqual(result["status"], "located")
        self.assertEqual(result["thread_id"], thread)
        self.assertTrue(result["path"].endswith(f"-{thread}.jsonl"))
        summary = result["summary"]
        self.assertEqual(summary["total_lines"], 3)
        self.assertEqual(summary["meta"]["cwd"], "/tmp/demo")
        body = json.dumps(summary)
        self.assertNotIn("sk-live", body)
        # summarize_rollout whitelists type/role/name/command per item only
        self.assertEqual(summary["items"][1], {"type": "response_item",
                                               "role": "assistant"})
        self.assertIn("ls -la", body)

    def test_unlocatable_thread_is_passed_through(self):
        result = S.read_codex_context("deadbeef-0000", codex_home=self.codex_home)
        self.assertEqual(result["status"], "unlocatable")
        self.assertIn("reason", result)


class ZcodeContextTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.db_path = Path(self.temporary.name) / "cli" / "db.sqlite"

    def test_summary_is_whitelist_only_and_truncates_text(self):
        make_zcode_db(self.db_path)
        result = S.read_zcode_context("sess_reader1", db_path=self.db_path)
        self.assertEqual(result["status"], "located")
        self.assertEqual(result["session"]["id"], "sess_reader1")
        self.assertEqual(result["session"]["title"], "reader fixture")
        self.assertEqual(len(result["messages"]), 5)
        first = result["messages"][0]
        self.assertEqual(
            sorted(first),
            ["part_types", "role", "sequence", "text", "text_truncated",
             "time_created", "type"])
        self.assertEqual(first["role"], "user")
        self.assertIsNone(first["type"])  # real 0.17.0 message.data has no type
        self.assertEqual(first["text"], "message body 0 xxxxxxxxxx")
        self.assertEqual(first["part_types"], {"text": 1})
        long = result["messages"][1]
        self.assertEqual(len(long["text"]), S.TEXT_LIMIT)
        self.assertTrue(long["text_truncated"])
        # nothing outside the whitelist may leak, especially the planted keys
        body = json.dumps(result)
        for forbidden in ("api_key", "sk-SUPER-SECRET-VALUE", "sk-PART-SECRET",
                          "inputTokens", "callID", "chain of thought"):
            self.assertNotIn(forbidden, body)
        # tool usage carries exactly its five whitelisted fields
        self.assertEqual(sorted(result["tool_usage"][0]),
                         ["approval_status", "exit_code", "read_only",
                          "status", "tool_name"])
        self.assertEqual(result["tool_usage"][1]["tool_name"], "Bash")
        self.assertEqual(result["tool_usage"][1]["approval_status"], "denied")

    def test_text_parts_are_concatenated_and_other_parts_only_counted(self):
        make_zcode_db(self.db_path)
        result = S.read_zcode_context("sess_reader1", db_path=self.db_path)
        third = result["messages"][2]
        # two text parts joined in part order; tool/reasoning parts are
        # counted only, and the reasoning text never becomes the body
        self.assertEqual(third["text"], "message body 2 xxxxxxxxxx tail")
        self.assertFalse(third["text_truncated"])
        self.assertEqual(third["part_types"],
                         {"text": 2, "tool": 1, "reasoning": 1})

    def test_message_without_text_parts_reads_as_metadata_only(self):
        path = self.db_path
        path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(path)
        try:
            connection.executescript(
                """
                CREATE TABLE session (id TEXT PRIMARY KEY, directory TEXT,
                    title TEXT, time_created INTEGER, time_updated INTEGER);
                CREATE TABLE message (id TEXT PRIMARY KEY, session_id TEXT,
                    time_created INTEGER, data TEXT, sequence INTEGER);
                CREATE TABLE part (id TEXT PRIMARY KEY, message_id TEXT,
                    session_id TEXT, time_created INTEGER, time_updated INTEGER,
                    data TEXT, sequence INTEGER);
                CREATE TABLE tool_usage (session_id TEXT, tool_name TEXT,
                    read_only INTEGER, destructive INTEGER,
                    approval_status TEXT, status TEXT, exit_code INTEGER,
                    duration_ms INTEGER);
                """)
            connection.execute("INSERT INTO session VALUES (?,?,?,?,?)",
                               ("s1", "/tmp", "t", 1, 2))
            connection.execute("INSERT INTO message VALUES (?,?,?,?,?)",
                               ("m0", "s1", 1,
                                json.dumps({"role": "assistant",
                                            "type": "progress"}), 0))
            connection.execute("INSERT INTO part VALUES (?,?,?,?,?,?,?)",
                               ("p0", "m0", "s1", 1, 1,
                                json.dumps({"type": "step-start"}), 0))
            connection.commit()
        finally:
            connection.close()
        result = S.read_zcode_context("s1", db_path=path)
        message = result["messages"][0]
        self.assertEqual(message["role"], "assistant")
        self.assertEqual(message["type"], "progress")
        self.assertIsNone(message["text"])
        self.assertFalse(message["text_truncated"])
        self.assertEqual(message["part_types"], {"step-start": 1})

    def test_pagination_offset_and_next_offset(self):
        make_zcode_db(self.db_path, message_count=5)
        page_one = S.read_zcode_context("sess_reader1", db_path=self.db_path,
                                        max_items=2, offset=0)
        self.assertEqual([m["sequence"] for m in page_one["messages"]], [0, 1])
        self.assertTrue(page_one["truncated"])
        self.assertEqual(page_one["next_offset"], 2)
        page_two = S.read_zcode_context("sess_reader1", db_path=self.db_path,
                                        max_items=2, offset=2)
        self.assertEqual([m["sequence"] for m in page_two["messages"]], [2, 3])
        self.assertEqual(page_two["next_offset"], 4)
        page_three = S.read_zcode_context("sess_reader1", db_path=self.db_path,
                                          max_items=2, offset=4)
        self.assertEqual([m["sequence"] for m in page_three["messages"]], [4])
        self.assertIsNone(page_three["next_offset"])
        self.assertFalse(page_three["truncated"])

    def test_missing_session_and_missing_database_are_unlocatable(self):
        make_zcode_db(self.db_path)
        missing = S.read_zcode_context("sess_nope", db_path=self.db_path)
        self.assertEqual(missing["status"], "unlocatable")
        self.assertIn("reason", missing)
        absent_db = S.read_zcode_context(
            "sess_reader1", db_path=self.db_path.parent / "nope.sqlite")
        self.assertEqual(absent_db["status"], "unlocatable")
        self.assertIn("database not found", absent_db["reason"])

    def test_database_is_opened_read_only(self):
        make_zcode_db(self.db_path)
        before = self.db_path.read_bytes()
        S.read_zcode_context("sess_reader1", db_path=self.db_path)
        self.assertEqual(self.db_path.read_bytes(), before)


class ReadContextDispatchTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.codex_home = Path(self.temporary.name) / "codex-home"
        self.db_path = Path(self.temporary.name) / "db.sqlite"

    def test_dispatch_codex_locator(self):
        thread = "thread-dispatch-1"
        make_rollout(self.codex_home, thread)
        result = S.read_context({"agent": "codex", "thread_id": thread,
                                 "codex_home": str(self.codex_home)})
        self.assertEqual(result["status"], "located")
        self.assertEqual(result["summary"]["total_lines"], 1)

    def test_dispatch_zcode_locator(self):
        make_zcode_db(self.db_path)
        result = S.read_context({"agent": "zcode", "session_id": "sess_reader1",
                                 "db_path": str(self.db_path)})
        self.assertEqual(result["status"], "located")
        self.assertEqual(result["session"]["id"], "sess_reader1")

    def test_invalid_locator_agent_is_rejected(self):
        with self.assertRaises(ValueError):
            S.read_context({"agent": "dsh", "session_id": "x"})
        with self.assertRaises(ValueError):
            S.read_context("not-a-locator")


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
