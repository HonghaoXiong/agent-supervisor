"""Contract tests for the read-only MCP evidence server (reviewer-feedback-1 contract).

Offline tests call implementation functions with temporary evidence trees; the
stdio round-trip spawns the real server as a subprocess. All counterexamples
from reviewer-mcp-contract-review-1.json are first-class tests here.
"""
import hashlib
import json
import sys
import unittest
from unittest.mock import patch
from pathlib import Path
from tempfile import TemporaryDirectory

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from zcode_supervisor import mcp_readonly as M  # noqa: E402


def clean_run(content="receipt output line\n" * 10, exit_code=0, timed_out=False,
              status="completed") -> dict:
    return {
        "outcome": "delivered",
        "delivery_ok": True,
        "session_id": "sess_test",
        "turn_id": "turn_test",
        "process_exit_code": 0,
        "commands": [{
            "command_id": "cmd-a",
            "tool_call_id": "call_test",
            "result": {
                "success": True,
                "content": content,
                "truncated": False,
                "originalBytes": len(content),
                "returnedBytes": len(content),
                "budgetStrategy": "artifact",
                "perf": {"totalMs": 5, "permissionWaitMs": 0,
                         "detail": {"kind": "command", "command": {
                             "exitCode": exit_code, "timedOut": timed_out,
                             "status": status, "runMs": 5}}},
            },
        }],
        "permission_decisions": [],
    }


def make_root(base: Path, runs=(("alpha", 1, clean_run()), ("alpha", 2, clean_run()),
                                ("beta", 1, clean_run()))) -> Path:
    root = base / "evidence"
    root.mkdir(parents=True)
    for task, number, payload in runs:
        directory = root / task
        directory.mkdir(parents=True, exist_ok=True)
        (directory / f"run-{number}.json").write_text(json.dumps(payload))
        (directory / f"run-{number}.events.jsonl").write_text(
            json.dumps({"method": "session/event", "params": {"payload": {"type": "x"}}}) + "\n")
    return root


class ConstrainedSnapshotTests(unittest.TestCase):
    """Counterexamples 1+2: symlinked run/events files must never serve."""

    def test_run_file_symlink_cannot_leak_outside_listing(self):
        with TemporaryDirectory() as tmp:
            base = Path(tmp)
            root = make_root(base)
            outside = base / "outside"
            outside.mkdir()
            payload = clean_run()
            payload["session_id"] = "OUTSIDE-ROOT-SESSION-MARKER"
            (outside / "run-2.json").write_text(json.dumps(payload))
            (root / "alpha" / "run-2.json").unlink()
            (root / "alpha" / "run-2.json").symlink_to(outside / "run-2.json")
            value = M.list_runs_impl(root)
            leaked = [r for r in value["runs"] if r.get("session_id") == "OUTSIDE-ROOT-SESSION-MARKER"]
            self.assertFalse(leaked, "symlinked run content leaked into the listing")
            flagged = [r for r in value["runs"] if r["run_id"] == "alpha/run-2"]
            self.assertEqual(flagged[0]["outcome"], "malformed")
            with self.assertRaises(M.EvidenceError):
                M.get_digest_impl(root, "alpha/run-2")

    def test_events_symlink_cannot_hash_outside_bytes(self):
        with TemporaryDirectory() as tmp:
            base = Path(tmp)
            root = make_root(base)
            outside_bytes = b'{"method":"OUTSIDE"}\n'
            outside = base / "outside.events.jsonl"
            outside.write_bytes(outside_bytes)
            events = root / "alpha" / "run-1.events.jsonl"
            events.unlink()
            events.symlink_to(outside)
            value = M.get_digest_impl(root, "alpha/run-1")
            self.assertEqual(value["events_file"], "rejected:symlink-rejected")
            served = json.dumps(value)
            self.assertNotIn(hashlib.sha256(outside_bytes).hexdigest(), served)
            with self.assertRaises(M.EvidenceError) as caught:
                M.get_evidence_page_impl(root, "alpha/run-1", "events")
            self.assertEqual(caught.exception.code, "symlink-rejected")

    def test_snapshot_rejects_intermediate_symlink_directories(self):
        with TemporaryDirectory() as tmp:
            base = Path(tmp)
            root = make_root(base)
            trap = base / "trap"
            trap.mkdir()
            (root / "alpha" / "run-1.json").unlink()
            (trap / "run-1.json").write_text(json.dumps(clean_run()))
            (root / "alpha" / "run-1.json").symlink_to(trap / "run-1.json")
            with self.assertRaises(M.EvidenceError):
                M.get_evidence_page_impl(root, "alpha/run-1", "run")


class ListingContinuationTests(unittest.TestCase):
    """Counterexample 3: 41 runs listable; .before never a run."""

    def test_forty_one_runs_continue_past_the_page(self):
        with TemporaryDirectory() as tmp:
            root = make_root(Path(tmp),
                             runs=tuple(("bulk", i, clean_run()) for i in range(1, 42)))
            first = M.list_runs_impl(root)
            self.assertLessEqual(first["returned"], M.LIST_PAGE)
            self.assertEqual(first["total_runs"], 41)
            self.assertLessEqual(first["returned"], 20)  # budget-fit shrinks the page
            self.assertEqual(first["total_runs"], 41)
            self.assertIn("next_cursor", first)
            seen = {r["run_id"] for r in first["runs"]}
            rest = M.list_runs_impl(root, cursor=first["next_cursor"])
            seen |= {r["run_id"] for r in rest["runs"]}
            self.assertEqual(len(seen), 41)
            self.assertNotIn("next_cursor", rest)
            with self.assertRaises(M.EvidenceError) as caught:
                M.list_runs_impl(root, cursor="nope/run-1")
            self.assertEqual(caught.exception.code, "cursor-drift")

    def test_before_files_are_ignored_and_counted(self):
        with TemporaryDirectory() as tmp:
            root = make_root(Path(tmp))
            (root / "alpha" / "run-17.before.json").write_text(json.dumps(clean_run()))
            value = M.list_runs_impl(root)
            ids = [r["run_id"] for r in value["runs"]]
            self.assertNotIn("alpha/run-17.before", ids)
            self.assertGreaterEqual(value["ignored_non_run_json_files"], 1)
            with self.assertRaises(M.EvidenceError):
                M.get_digest_impl(root, "alpha/run-17.before")


class VersionBoundPagingTests(unittest.TestCase):
    """Counterexample 4: page drift is rejected, never mixed."""

    def make_root_with_receipt(self, tmp, content="line\n" * 300):
        payload = clean_run(content=content)
        return make_root(Path(tmp), runs=(("alpha", 1, payload),)), payload

    def test_receipt_pages_bind_version_and_reject_drift(self):
        with TemporaryDirectory() as tmp:
            root, _ = self.make_root_with_receipt(tmp)
            first = M.get_receipt_impl(root, "alpha/run-1", "cmd-a", 0, 0, 100)
            version = first["evidence_version"]
            assembled, offset = "", 0
            while True:
                page = M.get_receipt_impl(root, "alpha/run-1", "cmd-a", 0, offset, 100,
                                          expected_version=version)
                assembled += page["receipt"]["content_page"]
                if not page["receipt"]["has_more"]:
                    break
                offset = page["receipt"]["next_offset"]
            self.assertEqual(assembled, "line\n" * 300)
            # Replace the file: the old version must no longer page.
            mutated = clean_run(content="MUTATED" * 50)
            (root / "alpha" / "run-1.json").write_text(json.dumps(mutated))
            with self.assertRaises(M.EvidenceError) as caught:
                M.get_receipt_impl(root, "alpha/run-1", "cmd-a", 0, 0, 100,
                                   expected_version=version)
            self.assertEqual(caught.exception.code, "evidence-drift")

    def test_evidence_pages_cover_full_run_and_events(self):
        with TemporaryDirectory() as tmp:
            root, payload = self.make_root_with_receipt(tmp)
            run_text = json.dumps(payload)
            assembled, offset, run_version = "", 0, None
            while True:
                page = M.get_evidence_page_impl(root, "alpha/run-1", "run", offset, 400,
                                                expected_version=run_version)
                run_version = page["evidence_version"]
                assembled += page["page"]
                self.assertEqual(page["evidence_version"],
                                 hashlib.sha256((root / "alpha" / "run-1.json").read_bytes()).hexdigest())
                if not page["has_more"]:
                    break
                offset = page["next_offset"]
            self.assertEqual(assembled, run_text)
            events_page = M.get_evidence_page_impl(root, "alpha/run-1", "events", 0, 50)
            self.assertEqual(events_page["object_sha256"],
                             hashlib.sha256((root / "alpha" / "run-1.events.jsonl").read_bytes()).hexdigest())


class DuplicateReceiptTests(unittest.TestCase):
    """Counterexample 5: the second occurrence of a command_id is retrievable."""

    def test_second_failed_occurrence_is_retrievable_by_index(self):
        with TemporaryDirectory() as tmp:
            payload = clean_run()
            second = {"command_id": "cmd-a", "tool_call_id": "call_2",
                      "result": {"success": False,
                                 "content": "second attempt failed output",
                                 "truncated": False,
                                 "perf": {"detail": {"kind": "command", "command": {
                                     "exitCode": 2, "timedOut": False, "status": "failed"}}}}}
            payload["commands"] = [payload["commands"][0], second]
            root = make_root(Path(tmp), runs=(("alpha", 1, payload),))
            zeroth = M.get_receipt_impl(root, "alpha/run-1", "cmd-a", 0, 0, 100)
            version = zeroth["evidence_version"]
            first = M.get_receipt_impl(root, "alpha/run-1", "cmd-a", 1, 0, 100,
                                       expected_version=version)
            self.assertEqual(zeroth["receipt"]["exit_code"], 0)
            self.assertEqual(zeroth["occurrence_index"], 0)
            self.assertEqual(zeroth["occurrences_total"], 2)
            self.assertEqual(first["receipt"]["exit_code"], 2)
            self.assertEqual(first["receipt"]["status"], "failed")
            self.assertIn("second attempt failed output", first["receipt"]["content_page"])
            self.assertNotEqual(first["tool_call_id"], zeroth["tool_call_id"])
            with self.assertRaises(M.EvidenceError) as caught:
                M.get_receipt_impl(root, "alpha/run-1", "cmd-a", 2, 0, 100,
                                   expected_version=version)
            self.assertEqual(caught.exception.code, "index-out-of-range")


class BudgetTests(unittest.TestCase):
    """Counterexample 6: serialized-budget paging, emoji included."""

    def test_emoji_page_shrinks_to_fit_inner_budget(self):
        with TemporaryDirectory() as tmp:
            emoji = "🧩" * 2048
            payload = clean_run(content=emoji)
            root = make_root(Path(tmp), runs=(("alpha", 1, payload),))
            page = M.get_receipt_impl(root, "alpha/run-1", "cmd-a", 0, 0, 2000)
            serialized = len(json.dumps(page, ensure_ascii=False).encode())
            self.assertLessEqual(serialized, M.INNER_BUDGET_BYTES)
            self.assertLess(page["receipt"]["effective_limit"], 2000)
            self.assertTrue(page["receipt"]["has_more"])

    def test_wide_backslash_run_stays_within_budget(self):
        with TemporaryDirectory() as tmp:
            payload = clean_run(content="\\" * 3000)
            root = make_root(Path(tmp), runs=(("alpha", 1, payload),))
            page = M.get_receipt_impl(root, "alpha/run-1", "cmd-a", 0, 0, 2000)
            self.assertLessEqual(len(json.dumps(page, ensure_ascii=False).encode()),
                                 M.INNER_BUDGET_BYTES)


class DecisionsTests(unittest.TestCase):
    """Counterexamples 6+7: deny pagination and malformed containers."""

    def test_two_hundred_denials_page_with_counts(self):
        with TemporaryDirectory() as tmp:
            payload = clean_run()
            payload["permission_decisions"] = [
                {"decision": "deny", "tool": "Bash", "reason": f"r{i}"} for i in range(200)]
            root = make_root(Path(tmp), runs=(("alpha", 1, payload),))
            digest_value = M.get_digest_impl(root, "alpha/run-1")
            summary = digest_value["failures"].get("denials_summary")
            self.assertTrue(summary and "object='decisions'" in summary["retrievable_via"])
            total, offset, seen, dec_version = 0, 0, 0, None
            while True:
                page = M.get_decisions_impl(root, "alpha/run-1", offset, 50,
                                            expected_version=dec_version)
                dec_version = page.get("evidence_version")
                seen += page["returned"]
                total = page["total"]
                self.assertLessEqual(len(json.dumps(page, ensure_ascii=False).encode()),
                                     M.INNER_BUDGET_BYTES)
                if not page["has_more"]:
                    break
                offset = page["next_offset"]
            self.assertEqual(seen, 200)
            self.assertEqual(total, 200)
            counts_page = M.get_decisions_impl(root, "alpha/run-1", 0, 1)
            self.assertEqual(counts_page["counts"].get("deny"), 200)

    def test_null_decision_entries_are_controlled(self):
        with TemporaryDirectory() as tmp:
            payload = clean_run()
            payload["permission_decisions"] = [None, {"decision": "allow", "tool": "Write"}]
            root = make_root(Path(tmp), runs=(("alpha", 1, payload),))
            try:
                digest_value = M.get_digest_impl(root, "alpha/run-1")
            except M.EvidenceError as error:
                digest_value = None
                self.assertEqual(error.code, "malformed-evidence")
            if digest_value is not None:
                self.assertIsInstance(digest_value.get("failures"), (dict, type(None)))
            page = M.get_decisions_impl(root, "alpha/run-1", 0, 50)
            self.assertEqual(page["malformed_entries"], 1)
            self.assertEqual(page["decisions"][0]["malformed_entry"], True)
            self.assertEqual(page["decisions"][1]["tool"], "Write")


class DigestSurfaceTests(unittest.TestCase):
    def test_digest_is_served_honestly_with_retrieval_pointers(self):
        with TemporaryDirectory() as tmp:
            root = make_root(Path(tmp))
            value = M.get_digest_impl(root, "alpha/run-1")
            self.assertEqual(value["mode"], "full-review")
            self.assertEqual(value["bindings"], "unknown")
            self.assertIn("evidence_version", value)
            self.assertIn("get_evidence_page", value["full_review_retrieval"]["run_bytes"])
            self.assertEqual(value["run_file"]["sha256"],
                             hashlib.sha256((root / "alpha" / "run-1.json").read_bytes()).hexdigest())

    def test_run_and_task_id_security(self):
        with TemporaryDirectory() as tmp:
            root = make_root(Path(tmp))
            for bad in ("../run-1", "alpha/../../run-1", "/etc/run-1", "alpha/sub/run-1",
                        "", None, 7, "alpha/run-abc", "alpha/run-01%2e%2e"):
                with self.assertRaises(M.EvidenceError):
                    M.resolve_run(root, bad)
            with self.assertRaises(M.EvidenceError):
                M.get_digest_impl(root, "alpha/run-99")


class StdioRoundTripTests(unittest.TestCase):
    """Real MCP stdio transport against a subprocess server (local pipes)."""

    def test_initialize_list_tools_and_calls(self):
        import anyio
        from mcp import ClientSession, StdioServerParameters
        from mcp.client.stdio import stdio_client

        async def scenario():
            params = StdioServerParameters(
                command=sys.executable,
                args=["-m", "zcode_supervisor.mcp_readonly", "--evidence-root", str(root)],
                cwd=str(Path(__file__).resolve().parents[1]))
            async with stdio_client(params) as (read, write):
                async with ClientSession(read, write) as session:
                    init = await session.initialize()
                    self.assertEqual(init.server_info.name, "czs-readonly-evidence")
                    tools = await session.list_tools()
                    self.assertEqual(sorted(t.name for t in tools.tools),
                                     ["get_digest", "get_receipt", "list_runs"])
                    listing = await session.call_tool("list_runs", {})
                    self.assertFalse(listing.is_error)
                    runs = json.loads(listing.content[0].text)["runs"]
                    self.assertTrue(runs)
                    digest = await session.call_tool("get_digest", {"run_id": "alpha/run-1"})
                    self.assertFalse(digest.is_error)
                    body = json.loads(digest.content[0].text)
                    self.assertEqual(body["mode"], "full-review")
                    page = await session.call_tool("get_receipt",
                                                   {"run_id": "alpha/run-1", "object": "run"})
                    self.assertFalse(page.is_error)
                    denied = await session.call_tool(
                        "get_receipt", {"run_id": "alpha/run-1", "object": "nope"})
                    self.assertTrue(denied.is_error)
                    return True

        with TemporaryDirectory() as tmp:
            root = make_root(Path(tmp))
            self.assertTrue(anyio.run(scenario))


if __name__ == "__main__":  # pragma: no cover
    unittest.main()


class ReviewerRaceAndDriftTests(unittest.TestCase):
    """offline-review-3 residual MCP counterexamples, closed."""

    def test_receipt_continuation_without_version_refused(self):
        with TemporaryDirectory() as tmp:
            root = make_root(Path(tmp), runs=(("alpha", 1, clean_run(content="line\n" * 200)),))
            first = M.get_receipt_impl(root, "alpha/run-1", "cmd-a", 0, 0, 50)
            # replace content: continuation without the version must not serve
            payload = clean_run(content="REPLACED" * 100)
            (root / "alpha" / "run-1.json").write_text(json.dumps(payload))
            with self.assertRaises(M.EvidenceError) as caught:
                M.get_receipt_impl(root, "alpha/run-1", "cmd-a", 0, 50, 50)
            self.assertEqual(caught.exception.code, "version-required")
            # with the stale version it is an explicit drift rejection
            with self.assertRaises(M.EvidenceError) as caught:
                M.get_receipt_impl(root, "alpha/run-1", "cmd-a", 0, 50, 50,
                                   expected_version=first["evidence_version"])
            self.assertEqual(caught.exception.code, "evidence-drift")

    def test_events_drift_with_stale_run_version_refused(self):
        with TemporaryDirectory() as tmp:
            root = make_root(Path(tmp))
            first = M.get_evidence_page_impl(root, "alpha/run-1", "events", 0, 10)
            run_version = first["evidence_version"]
            events_version = first["object_version"]
            # mutate ONLY events; run.json (and the echoed run version) is unchanged
            (root / "alpha" / "run-1.events.jsonl").write_text('{"EVIL": true}\n')
            with self.assertRaises(M.EvidenceError) as caught:
                M.get_evidence_page_impl(root, "alpha/run-1", "events", 10, 10,
                                         expected_version=run_version,
                                         expected_object_version=events_version)
            self.assertEqual(caught.exception.code, "evidence-drift")
            self.assertNotEqual(
                hashlib.sha256((root / "alpha" / "run-1.events.jsonl").read_bytes()).hexdigest(),
                events_version)

    def test_same_fd_snapshot_defeats_swap_after_open(self):
        # Deterministic interleave: swap the file to a symlink mid-flight is
        # impossible through the same open descriptor — simulate the attack by
        # replacing the path after open via a subclass that mutates on read.
        with TemporaryDirectory() as tmp:
            root = make_root(Path(tmp))
            run_path = root / "alpha" / "run-1.json"
            original = run_path.read_bytes()
            outside = Path(tmp) / "outside.json"
            outside.write_text('{"OUTSIDE": true}')
            real_open = __import__("os").open
            def racing_open(path, flags, *args, **kwargs):
                fd = real_open(path, flags, *args, **kwargs)
                if str(path).endswith("run-1.json"):
                    # attacker swaps the path right after our open
                    run_path.unlink()
                    run_path.symlink_to(outside)
                return fd
            import zcode_supervisor.mcp_readonly as mod
            with patch("os.open", racing_open):
                data = M.snapshot(run_path, root, "run.json")
            self.assertEqual(data, original)  # bytes come from the original inode

    def test_outer_wire_budget_measured_on_serialization(self):
        # The budget layer is the serialized JSON text actually returned; the
        # MCP envelope adds more on the wire, so the inner payload must fit
        # the INNER budget and the tool result stays well under 8KB total.
        with TemporaryDirectory() as tmp:
            root = make_root(Path(tmp), runs=(("alpha", 1, clean_run(content="\u2003" * 900)),))
            page = M.get_receipt_impl(root, "alpha/run-1", "cmd-a", 0, 0, 2000)
            inner = len(json.dumps(page, ensure_ascii=False).encode())
            self.assertLessEqual(inner, M.INNER_BUDGET_BYTES)
            # worst-case envelope check on the same payload
            envelope = json.dumps({"jsonrpc": "2.0", "id": 1,
                                   "result": {"content": [{"type": "text",
                                                           "text": json.dumps(page, ensure_ascii=False)}]}}).encode()
            self.assertLessEqual(len(envelope), 8192,
                                 f"wire envelope {len(envelope)}B exceeds the 8KB outer budget")

    def test_base64_pages_reproduce_exact_bytes(self):
        with TemporaryDirectory() as tmp:
            root = make_root(Path(tmp))
            raw = (root / "alpha" / "run-1.json").read_bytes()
            import base64
            assembled, offset, version = b"", 0, None
            while True:
                page = M.get_evidence_page_impl(root, "alpha/run-1", "run", offset, 512,
                                                expected_version=version, encoding="base64")
                version = page["evidence_version"]
                assembled += base64.b64decode(page["page"])
                if not page["has_more"]:
                    break
                offset = page["next_offset"]
            self.assertEqual(assembled, raw)
