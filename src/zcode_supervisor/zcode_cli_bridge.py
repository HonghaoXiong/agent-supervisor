"""Deliver an instruction into a native ZCode session through its CLI.

A GUI-created ZCode session has no app-server socket, but the shipped CLI
resumes a persisted session in a new process::

    zcode -p "<instruction>" --resume sess_... --json --cwd <root> --mode ...

The CLI writes the message into the SAME session store and runs the turn, so
the session id, the history and the directory are preserved. This module is
the transport only: it never writes the chat database, and it proves every
delivery by reading the persisted user message back (the CLI's exit code and
its JSON alone are not accepted as delivery evidence).
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import shutil
import os
import subprocess
import tempfile
import time
from pathlib import Path

from .session_reader import _default_zcode_db, _part_rows
from .work_models import WorkError

DEFAULT_CLI = Path("/Applications/ZCode.app/Contents/Resources/glm/zcode.cjs")


def prepared_cli(cli_path: Path) -> Path:
    """Return an entrypoint whose five-level depth resolves the app config.

    The one-shot CLI looks for its bundled provider seed at
    ``<entry dir>/provider/zcode-builtin.json`` then at
    ``<entry dir>/../../../../../config/provider/zcode-builtin.json``.  The
    installed bundle keeps that file at ``Resources/config/provider`` — two
    levels up from ``Resources/glm`` — so a copy of the CLI at a five-level
    depth lets the CLI's own lookup find the app's config without modifying
    the installed bundle or the reviewed checksum.
    """
    app_config = (cli_path.parent.parent / "config" / "provider"
                  / "zcode-builtin.json")
    if not app_config.is_file():
        return cli_path
    digest = hashlib.sha256(cli_path.read_bytes()).hexdigest()[:12]
    root = Path(tempfile.gettempdir()) / f"zsc-zcode-cli-{digest}"
    entry = root / "a" / "b" / "c" / "d" / "e" / cli_path.name
    config = root / "config" / "provider" / app_config.name
    if not entry.is_file() or entry.stat().st_size != cli_path.stat().st_size:
        entry.parent.mkdir(parents=True, exist_ok=True)
        config.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(cli_path, entry)
        shutil.copyfile(app_config, config)
        entry.chmod(0o755)
    return entry
TERMINAL_FINISH = ("stop", "completed", "end_turn")


def cli_capability(cli_path=None) -> dict:
    path = Path(cli_path).expanduser() if cli_path else DEFAULT_CLI
    if not path.is_file():
        return {"state": "native-delivery-unsupported",
                "reason": f"zcode CLI not found: {path}"}
    return {"state": "native-delivery-available", "channel": "cli-resume",
            "cli": str(path)}


def _launch_contract(contract, timeout_s, default_timeout_s, code_root):
    """The CONTRACT owns the launch: its workspace and its timeout win.

    A caller that passes a shorter or longer ``timeout_s`` beside a contract
    cannot widen or narrow what the owner approved; the fallbacks exist only
    for a contract that omitted the field.
    """
    workspace = contract.get("workspace") or code_root
    limit = float(contract.get("timeout_sec") or timeout_s or default_timeout_s)
    return str(workspace), limit


class ZCodeCliBridge:
    """Resume a native ZCode session with a delivered instruction."""

    def __init__(self, cli_path=None, *, node="node", db_path=None,
                 timeout_s=900, mode="edit", runtime_config=None):
        self.cli_path = Path(cli_path).expanduser() if cli_path else DEFAULT_CLI
        self.node = node
        self.runtime_config = runtime_config
        self.db_path = Path(db_path).expanduser() if db_path is not None \
            else _default_zcode_db()
        self.timeout_s = timeout_s
        self.mode = mode

    # -- public -------------------------------------------------------------

    def _launch_env(self):
        """The reviewed runtime environment, or None for a bare bridge.

        The native turn inherits it: PYTHONDONTWRITEBYTECODE keeps the
        model's imports from writing bytecode into the candidate workspace.
        """
        if self.runtime_config is None:
            return None
        return self.runtime_config.environment()

    def deliver(self, session_id, text, *, delivery_id, code_root,
                contract=None, authorization=None, timeout_s=None,
                expected_identity=None, expected_model=None,
                attempt_id=None):
        """Send the APPROVED prompt into ``session_id``; return the receipt."""
        if not isinstance(delivery_id, str) or not delivery_id:
            raise WorkError("schema-invalid", "a native delivery needs an id")
        from .contract_compile import verify_against_authorization
        if not isinstance(contract, dict) or not isinstance(authorization, dict):
            raise WorkError(
                "native-contract-missing",
                "a native continuation must carry the approved contract and "
                "the authorization it was compiled from; the worker is not "
                "spawned without both")
        # The bridge re-checks the contract itself: a caller that skipped the
        # loop cannot hand it a widened write set, a replaced stage command or
        # an argv slot pointing at an unapproved file.
        from .work_contracts import canonical_sha256
        body = {key: value for key, value in contract.items()
                if key != "contract_sha256"}
        claimed = contract.get("contract_sha256")
        if not claimed or claimed != canonical_sha256(body):
            raise WorkError(
                "native-contract-mismatch",
                "the contract carries no digest of its own content; a receipt "
                "cannot be bound to an unhashed contract")
        verify_against_authorization(authorization, contract)
        if contract.get("require_command"):
            raise WorkError(
                "native-command-unenforceable",
                "this contract requires a registered command to run, and a "
                "resumed native CLI turn cannot prove a command executed; "
                "dispatch it through the reviewed runtime instead")
        prompt = contract.get("prompt")
        if not isinstance(prompt, str) or not prompt.strip():
            raise WorkError("native-contract-missing",
                            "the approved contract carries no prompt to deliver")
        text = prompt
        marker = f"[zsc-native-delivery {delivery_id}]"
        # A resumed session runs the model it already recorded; a contract
        # naming a different one cannot be enforced here. Refuse rather than
        # deliver under a model the authorization never approved.
        if expected_model is not None:
            recorded = self._session_model(session_id)
            if recorded.get("model") is None:
                raise WorkError(
                    "native-contract-unenforceable",
                    "the session records no model, so the contract's model "
                    "cannot be checked before delivery")
            if recorded["model"].lower() != str(expected_model).lower():
                raise WorkError(
                    "native-contract-unenforceable",
                    f"the session runs {recorded['model']!r}, not the "
                    f"contract's {expected_model!r}; a resumed session does "
                    "not switch models, so this contract cannot be enforced")
        # Resolve the contract's workspace and its identity BEFORE any
        # precheck: a swap during the precheck must be visible to the launch
        # check (the probe replaced the directory exactly there).
        workspace, limit = _launch_contract(contract, timeout_s,
                                            self.timeout_s, code_root)
        current = os.stat(workspace)
        recorded = (current.st_dev, current.st_ino)
        if isinstance(expected_identity, dict):
            expected_identity = (expected_identity.get("dev"),
                                 expected_identity.get("ino"))
        if expected_identity is not None and tuple(expected_identity) != recorded:
            raise WorkError(
                "workspace-replaced",
                f"the workspace {workspace} is no longer the directory the "
                "owner verified when it claimed the session")
        identity = recorded
        existing = self._find_delivery(session_id, delivery_id, text,
                                       allow_missing_turn=True)
        if existing is not None:
            if not existing.get("turn_id"):
                # The body is already there but no turn started from it.
                # Sending it again would duplicate a message the user sees;
                # the honest outcome is "landed, unverified".
                raise WorkError(
                    "native-delivery-unverified",
                    "the delivery text is already in the session but its row "
                    "carries no turn id; refusing to send it a second time")
            state = self.turn_state(session_id, existing.get("turn_id"))
            if not state.get("completed") or state.get("failed"):
                raise WorkError(
                    "native-delivery-unconfirmed",
                    f"the persisted message's turn "
                    f"{existing.get('turn_id')!r} is not a successful terminal "
                    f"turn (completed={state.get('completed')}, "
                    f"failed={state.get('failed')})")
            return self._receipt(
                existing, session_id, delivery_id, 0.0, "already-delivered",
                contract_sha256=claimed, model=state.get("model"),
                attempt_id=attempt_id)
        state = self._latest_state(session_id)
        if state.get("session_state") not in TERMINAL_FINISH + ("idle",):
            raise WorkError(
                "session-busy",
                f"the native session is {state.get('session_state')!r}; a "
                "delivery waits for the current turn to finish")
        # The CONTRACT owns the launch: its workspace is the cwd the review
        # approved, its timeout bounds the child, and when the owner passes
        # the reviewed runtime config the CLI is the pin-checked copy from
        # prepare_cli (never the app bundle on disk).
        workspace, limit = _launch_contract(contract, timeout_s,
                                            self.timeout_s, code_root)
        if self.runtime_config is not None:
            from .runtime import prepare_cli
            cli = prepare_cli(self.runtime_config)
            node = self.runtime_config.node
        else:
            cli = prepared_cli(self.cli_path)
            node = self.node
        env = self._launch_env()
        argv = [node, str(cli), "-p",
                f"{text}\n\n{marker}",
                "--resume", session_id, "--json", "--cwd", str(workspace),
                "--mode", self.mode]
        started = time.monotonic()
        try:
            current = os.stat(workspace)
            if (current.st_dev, current.st_ino) != identity:
                raise WorkError(
                    "workspace-replaced",
                    f"the workspace {workspace} was replaced while the "
                    "adapter was preparing the launch")
            proc = subprocess.run(argv, capture_output=True, text=True,
                                  timeout=limit, env=env)
        except subprocess.TimeoutExpired:
            raise WorkError("native-delivery-failed",
                            f"the zcode CLI did not finish in {limit}s")
        if proc.returncode != 0:
            raise WorkError(
                "native-delivery-failed",
                f"zcode exited {proc.returncode}: "
                f"{(proc.stderr or proc.stdout)[-400:]}")
        parsed = _parse_json(proc.stdout)
        if not parsed or parsed.get("sessionId") != session_id:
            raise WorkError(
                "session-mismatch",
                f"the CLI resumed {parsed.get('sessionId') if parsed else None!r}, "
                f"not the bound {session_id!r}")
        turn_id = parsed.get("turnId")
        # The receipt comes from the store, never from the CLI's answer alone:
        # wait for the persisted user message carrying THIS delivery id.
        deadline = time.monotonic() + 60
        found = None
        while time.monotonic() < deadline:
            found = self._find_delivery(session_id, delivery_id, text)
            if found is not None and found.get("turn_id") == turn_id:
                break
            time.sleep(1.0)
        if found is None or found.get("turn_id") != turn_id:
            raise WorkError(
                "native-receipt-missing",
                f"no persisted user message carries delivery {delivery_id!r} "
                f"in turn {turn_id!r}; the CLI output alone is not a delivery")
        # The initial send is judged by the SAME turn state the recovery
        # path uses: a stop reply that carries an error, or a last assistant
        # with no model, is not a delivery.
        state = self.turn_state(session_id, turn_id)
        if not state.get("completed") or state.get("failed"):
            raise WorkError(
                "native-delivery-unconfirmed",
                f"the delivery turn {turn_id!r} is not a successful terminal "
                f"turn (completed={state.get('completed')}, "
                f"failed={state.get('failed')}); the CLI output alone is not "
                "a delivery")
        receipt = self._receipt(found, session_id, delivery_id,
                                time.monotonic() - started, "delivered")
        receipt["response"] = parsed.get("response")
        receipt["contract_sha256"] = claimed
        receipt["model"] = state.get("model")
        receipt["attempt_id"] = attempt_id
        if expected_model is not None:
            receipt_model = receipt.get("model")
            if receipt_model is None or receipt_model.lower() != str(
                    expected_model).lower():
                raise WorkError(
                    "native-receipt-mismatch",
                    f"the delivered turn ran {receipt_model!r}, not the "
                    f"contract's {expected_model!r}")
        return receipt

    def _turn_model(self, session_id, turn_id):
        """The model recorded on THIS turn's LAST assistant message.

        No cross-turn and no earlier-message fallback: a final reply without
        a model must fail the owner's receipt check, not borrow the model of
        an earlier reply in the same turn.
        """
        if not turn_id:
            return None
        with self._connect() as connection:
            rows = self._rows(connection, session_id)
        for row in reversed(rows):
            data = _data(row)
            anchor = data.get("anchor") or {}
            if data.get("role") != "assistant":
                continue
            if anchor.get("turnId") != turn_id:
                continue
            return data.get("modelId")
        return None

    def _session_model(self, session_id):
        with self._connect() as connection:
            rows = self._rows(connection, session_id)
        if not rows:
            return {}
        data = _data(rows[-1])
        if data.get("modelId"):
            return {"model": data["modelId"],
                    "provider": data.get("providerId")}
        return {}

    def turn_state(self, session_id, turn_id):
        """Completion + model recorded on THIS turn (no cross-turn fallback)."""
        if not turn_id:
            return {"completed": False, "model": None, "failed": False}
        with self._connect() as connection:
            rows = self._rows(connection, session_id)
        latest = None
        for row in rows:
            data = _data(row)
            if data.get("role") != "assistant":
                continue
            if ((data.get("anchor") or {}).get("turnId")) != turn_id:
                continue
            latest = data
        if latest is None:
            return {"completed": False, "model": None, "failed": False}
        finish = latest.get("finish")
        done = bool((latest.get("time") or {}).get("completed"))
        failed = bool(latest.get("error")) or finish in (
            "failed", "cancelled", "interrupted")
        completed = bool(done and finish in TERMINAL_FINISH and not failed)
        return {"completed": completed, "model": latest.get("modelId"),
                "failed": failed}

    def find(self, session_id, delivery_id, expected_text=None,
             expected_sha256=None):
        found = self._find_delivery(
            session_id, delivery_id, expected_text=expected_text,
            expected_sha256=expected_sha256)
        if found is None:
            return None
        # A re-read applies the SAME turn-state gate as the initial send and
        # the recovery path: a failed or still-running turn is not a receipt.
        state = self.turn_state(session_id, found.get("turn_id"))
        if not state.get("completed") or state.get("failed"):
            raise WorkError(
                "native-delivery-unconfirmed",
                f"the persisted delivery's turn {found.get('turn_id')!r} is "
                f"not a successful terminal turn "
                f"(completed={state.get('completed')}, "
                f"failed={state.get('failed')})")
        receipt = self._receipt(found, session_id, delivery_id, 0.0,
                                "already-delivered", model=state.get("model"))
        return receipt

    # -- store --------------------------------------------------------------

    def _connect(self):
        connection = sqlite3.connect(f"file:{self.db_path}?mode=ro", uri=True,
                                     timeout=5)
        connection.row_factory = sqlite3.Row
        return connection

    def _rows(self, connection, session_id):
        return connection.execute(
            "SELECT id, sequence, data FROM message WHERE session_id=? "
            "ORDER BY sequence", (session_id,)).fetchall()

    @staticmethod
    def _texts(connection, session_id, messages):
        parts = _part_rows(connection, session_id,
                           [message["id"] for message in messages])
        grouped: dict[str, list] = {}
        for part in parts:
            grouped.setdefault(part["message_id"], []).append(part)
        texts = {}
        for message in messages:
            chunks = []
            for part in grouped.get(message["id"], []):
                try:
                    data = json.loads(part["data"])
                except (TypeError, json.JSONDecodeError):
                    continue
                if isinstance(data, dict) and data.get("type") == "text" \
                        and isinstance(data.get("text"), str):
                    chunks.append(data["text"])
            texts[message["id"]] = "".join(chunks)
        return texts

    def _find_delivery(self, session_id, delivery_id, expected_text=None,
                       expected_sha256=None, allow_missing_turn=False):
        # A receipt must be the message this bridge actually sent: the full
        # body plus the marker. A history message that merely quotes the id
        # (a user asking about it, a previous report) is not a delivery.
        marker = f"[zsc-native-delivery {delivery_id}]"
        with self._connect() as connection:
            rows = self._rows(connection, session_id)
            texts = self._texts(connection, session_id, rows)
        user = None
        for row in rows:
            data = _data(row)
            if data.get("role") != "user":
                continue
            body = texts.get(row["id"]) or ""
            if delivery_id not in body:
                continue
            if expected_text is not None \
                    and body.strip() != f"{expected_text}\n\n{marker}".strip():
                continue
            # The digest ties the receipt to the EXACT text persisted before
            # the send; a later message that merely quotes the id hashes to
            # something else (R8: body and turn must come from one message).
            if expected_sha256 is not None \
                    and hashlib.sha256(body.encode("utf-8")).hexdigest() \
                    != expected_sha256:
                continue
            turn = (data.get("anchor") or {}).get("turnId")
            if not isinstance(turn, str) or not turn.strip():
                # text that matches but started no turn proves nothing
                if allow_missing_turn:
                    return {"turn_id": None, "user_message_id": row["id"],
                            "partial": True}
                continue
            user = row
            break
        if user is None:
            return None
        turn_id = ((_data(user).get("anchor") or {}).get("turnId"))
        assistant = None
        for row in rows:
            if row["sequence"] <= user["sequence"]:
                continue
            data = _data(row)
            if data.get("role") != "assistant":
                continue
            if ((data.get("anchor") or {}).get("turnId")) != turn_id:
                continue
            assistant = row
        response = None
        completed = False
        if assistant is not None:
            response = texts.get(assistant["id"]) or None
            data = _data(assistant)
            completed = bool((data.get("time") or {}).get("completed")) \
                and data.get("finish") in TERMINAL_FINISH
        return {"turn_id": turn_id, "user_message_id": user["id"],
                "user_sequence": user["sequence"], "response": response,
                "user_text": body,
                "assistant_message_id": (assistant["id"]
                                         if assistant is not None else None),
                "completed": completed}

    def _latest_state(self, session_id):
        with self._connect() as connection:
            rows = self._rows(connection, session_id)
        if not rows:
            return {"session_state": "unknown"}
        latest = _data(rows[-1])
        completed = bool((latest.get("time") or {}).get("completed"))
        finish = latest.get("finish")
        if latest.get("role") == "assistant" and completed \
                and finish in TERMINAL_FINISH:
            return {"session_state": finish,
                    "turn_id": ((latest.get("anchor") or {}).get("turnId"))}
        return {"session_state": "working",
                "turn_id": ((latest.get("anchor") or {}).get("turnId"))}

    @staticmethod
    def _receipt(found, session_id, delivery_id, waited_s, status,
                 contract_sha256=None, model=None, attempt_id=None):
        # Only validated deliveries are built; recovery and the initial send
        # share the same turn-state gate before this point.
        return {"delivered": True,
                "status": status,
                "channel": "zcode-cli-resume", "session_id": session_id,
                "delivery_id": delivery_id, "turn_id": found.get("turn_id"),
                "user_message_id": found.get("user_message_id"),
                "user_sequence": found.get("user_sequence"),
                "assistant_message_id": found.get("assistant_message_id"),
                "response": found.get("response"),
                "user_text": found.get("user_text"),
                "contract_sha256": contract_sha256,
                "model": model, "attempt_id": attempt_id,
                "waited_s": round(waited_s, 3)}


def _data(row) -> dict:
    try:
        data = json.loads(row["data"]) if row["data"] is not None else None
    except (TypeError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def _parse_json(text: str):
    try:
        return json.loads(text)
    except (TypeError, json.JSONDecodeError):
        pass
    start = text.find("{")
    while start != -1:
        try:
            return json.loads(text[start:])
        except json.JSONDecodeError:
            start = text.find("{", start + 1)
    return None
