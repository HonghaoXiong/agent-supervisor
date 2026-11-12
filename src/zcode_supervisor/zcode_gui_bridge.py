"""Deliver an instruction into a native ZCode session through its macOS GUI.

A GUI-created ZCode session owns no app-server socket, and an external
app-server cannot resume it (the desktop app holds the thread writer).  When
the shipped CLI cannot resume the session either, the remaining supported
channel is the application's own window: focus the window that carries the
session, type into the input box and send.

This module is a transport, not a chat writer: it never touches the session
database for writing.  Every delivery is proven by reading the persisted user
message back and requiring the unique ``delivery_id`` to appear in the target
session, so a successful click or a zero exit code never counts as delivery.
Draft safety and target ambiguity are fail-closed: an unreadable input box, a
non-empty draft, zero or several candidate windows, or a busy session all stop
before typing anything.
"""

from __future__ import annotations

import hashlib
import fcntl
import json
import sqlite3
import subprocess
import os
import sys
import time
from contextlib import closing
from pathlib import Path

from .session_binding import _classify_session
from .session_reader import _default_zcode_db, _part_rows
from .work_models import WorkError

APPLICATION = "ZCode"

_PRE_WRITE_ERRORS = ("zsc-ambiguous-input", "zsc-wrong-input",
                     "zsc-draft-present", "zsc-confirm-unsupported",
                     "zsc-wrong-window")
BUNDLE = Path("/Applications/ZCode.app")
PERMISSION = ("Accessibility (System Settings → Privacy & Security → "
              "Accessibility) for the process that runs osascript")


def _run_osascript(script: str, *, timeout=30) -> str:
    try:
        result = subprocess.run(["osascript", "-e", script],
                                capture_output=True, text=True,
                                timeout=timeout)
    except subprocess.TimeoutExpired:
        raise WorkError("native-delivery-failed",
                        f"osascript did not finish in {timeout}s")
    if result.returncode != 0:
        detail = (result.stderr or result.stdout).strip()
        raise WorkError(
            "desktop-wakeup-unsupported",
            f"osascript could not read or drive {APPLICATION}: {detail}")
    return result.stdout.strip()


def gui_capability(script_runner=None,
                   session_identity_attribute=None) -> dict:
    """Report whether this host can drive the ZCode window at all.

    A window TITLE is only a locator. Auto-typing without a session identity
    the bridge can verify would be guessing, so the capability stays
    unavailable until the host names the accessibility attribute that carries
    the session id (``ZSC_GUI_SESSION_ID_ATTRIBUTE``).
    """
    if sys.platform != "darwin":
        return {"state": "desktop-wakeup-unsupported", "channel": "gui",
                "detail": "the GUI bridge exists only on macOS"}
    if not BUNDLE.exists():
        return {"state": "desktop-wakeup-unsupported", "channel": "gui",
                "detail": f"{BUNDLE} is not installed"}
    runner = script_runner or _run_osascript
    try:
        windows = runner(f'tell application "System Events" to get name of '
                         f'every window of process "{APPLICATION}"')
    except WorkError as error:
        return {"state": "desktop-wakeup-unsupported", "channel": "gui",
                "helper": "osascript (System Events)",
                "application": APPLICATION, "permission": PERMISSION,
                "detail": getattr(error, "message", str(error))}
    attribute = (session_identity_attribute
                 or os.environ.get("ZSC_GUI_SESSION_ID_ATTRIBUTE") or "").strip()
    if not attribute:
        return {"state": "desktop-wakeup-unsupported", "channel": "gui",
                "application": APPLICATION, "windows": windows,
                "detail": "the host exposes no verifiable session identity "
                          "for a ZCode window; set "
                          "ZSC_GUI_SESSION_ID_ATTRIBUTE to the accessibility "
                          "attribute that carries the session id"}
    return {"state": "native-delivery-available", "channel": "gui",
            "application": APPLICATION, "windows": windows,
            "session_identity_attribute": attribute}


def _quote(text: str) -> str:
    return text.replace("\\", "\\\\").replace('"', '\\"')


class ZCodeGuiBridge:
    """Type one proven instruction into the session's own window."""

    def __init__(self, *, db_path=None, script_runner=None, poll_s=1.0,
                 timeout_s=180, sleep=time.sleep,
                 session_identity_attribute=None,
                 evidence_root=None):
        self.db_path = Path(db_path).expanduser() if db_path is not None \
            else _default_zcode_db()
        self.script_runner = script_runner or _run_osascript
        self.poll_s = poll_s
        self.timeout_s = timeout_s
        self.sleep = sleep
        self.session_identity_attribute = (
            session_identity_attribute
            or os.environ.get("ZSC_GUI_SESSION_ID_ATTRIBUTE") or "")
        self.evidence_root = Path(evidence_root or
                                  "/private/tmp/zsc-gui-bridge")

    # -- read-only session helpers -------------------------------------
    def _connect(self):
        connection = sqlite3.connect(f"file:{self.db_path}?mode=ro",
                                     uri=True, timeout=5)
        connection.row_factory = sqlite3.Row
        return closing(connection)

    def _session(self, session_id: str) -> dict:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT id, directory, title FROM session WHERE id=?",
                (session_id,)).fetchone()
        if row is None:
            raise WorkError("native-session-unknown",
                            f"no ZCode session {session_id!r} in {self.db_path}")
        return {"id": row["id"], "directory": row["directory"],
                "title": row["title"] or ""}

    def _messages(self, session_id: str, *, role=None):
        query = "SELECT id, sequence, data FROM message WHERE session_id=?"
        params: list = [session_id]
        if role:
            query += " AND json_extract(data, '$.role')=?"
            params.append(role)
        query += " ORDER BY sequence"
        with self._connect() as connection:
            return connection.execute(query, params).fetchall()

    def find(self, session_id: str, delivery_id: str, expected_text=None,
             expected_sha256=None):
        """Return the persisted user message carrying ``delivery_id``.

        ``expected_text`` makes the match exact: a history row that merely
        quotes the marker (a question about a past delivery) is not this
        delivery, and neither is a message whose body differs from what the
        bridge types now.
        """
        rows = self._messages(session_id, role="user")
        if not rows:
            return None
        with self._connect() as connection:
            parts = _part_rows(connection, session_id,
                               [row["id"] for row in rows])
        by_message: dict[str, list[str]] = {}
        for part in parts:
            if part["message_id"] in {row["id"] for row in rows}:
                data = json.loads(part["data"] or "{}")
                if data.get("type") == "text":
                    by_message.setdefault(part["message_id"], []).append(
                        str(data.get("text") or ""))
        marker = f"[zsc-native-delivery {delivery_id}]"
        for row in rows:
            text = "\n".join(by_message.get(row["id"], []))
            if marker not in text:
                continue
            if expected_text is not None:
                outgoing = f"{expected_text}\n\n{marker}"
                if text.strip() != outgoing.strip():
                    continue
            if expected_sha256 is not None \
                    and hashlib.sha256(text.encode("utf-8")).hexdigest() \
                    != expected_sha256:
                continue
            data = json.loads(row["data"] or "{}")
            return {"session_id": session_id, "message_id": row["id"],
                    "sequence": row["sequence"],
                    "turn_id": ((data.get("anchor") or {}).get("turnId")),
                    "text": text}
        return None

    # -- the delivery --------------------------------------------------
    def deliver(self, session_id: str, text: str, delivery_id: str,
                timeout_s: float | None = None, contract: dict | None = None,
                attempt_id: str | None = None) -> dict:
        marker = f"[zsc-native-delivery {delivery_id}]"
        body = text
        # The GUI can enforce the contract's prompt and model (the workspace
        # and argv are the session's own); a mismatch stops before typing.
        contract_sha256 = (contract or {}).get("contract_sha256")
        if contract is not None:
            if not contract_sha256:
                raise WorkError(
                    "native-contract-mismatch",
                    "the GUI delivery was given a contract with no digest")
            if body != (contract.get("prompt") or body):
                raise WorkError(
                    "native-contract-mismatch",
                    "the text handed to the GUI is not the approved prompt")
            expected_model = contract.get("model")
            if expected_model:
                recorded = self._recorded_model(session_id)
                if recorded is None or recorded.lower() != str(
                        expected_model).lower():
                    raise WorkError(
                        "native-contract-unenforceable",
                        f"the session runs {recorded!r}, not the contract's "
                        f"{expected_model!r}; the GUI cannot switch models")
        if marker not in text:
            # The typed instruction carries the unique id; the read-back below
            # requires it, so a prompt without the id can never be "delivered".
            text = f"{text}\n\n{marker}"
        # The contract's limit covers the whole delivery, including the
        # window/session checks and the typing — not just the wait afterwards.
        limit = float(timeout_s) if timeout_s else self.timeout_s
        deadline = time.monotonic() + limit
        existing = self.find(session_id, delivery_id, expected_text=body)
        if existing is not None:
            # A message that merely MENTIONS the marker is not a delivery:
            # the receipt needs the turn the message started.
            if not existing.get("turn_id"):
                raise WorkError(
                    "native-delivery-unverified",
                    f"message {existing['message_id']!r} mentions the delivery "
                    f"marker but has no turn anchor; refusing to treat an "
                    f"unrelated history row as this delivery")
            state = self.turn_state(session_id, existing["turn_id"])
            if not state["completed"] or state["failed"]:
                raise WorkError(
                    "native-delivery-unconfirmed",
                    f"the message for delivery {delivery_id!r} exists but its "
                    f"turn is not completed successfully; refusing to report "
                    f"it as delivered")
            return {**existing, "delivered": True,
                    "status": "already-delivered",
                    "contract_sha256": contract_sha256,
                    "model": state["model"],
                    "attempt_id": attempt_id}
        session = self._session(session_id)
        with self._connect() as connection:
            state = _classify_session(connection, session_id)
            same_title = connection.execute(
                "SELECT COUNT(*) FROM session WHERE title = ?",
                (session["title"] or "",)).fetchone()[0]
        if state not in ("idle", "turn_completed", "failed", "interrupted"):
            raise WorkError(
                "native-session-busy",
                f"session {session_id!r} is {state!r}; "
                f"the GUI bridge never types into a running turn")
        if not session["title"]:
            raise WorkError(
                "target-window-unverified",
                f"session {session_id!r} carries no title to match a window")
        if same_title > 1:
            raise WorkError(
                "target-window-unverified",
                f"{same_title} sessions carry the title {session['title']!r}; "
                f"a window title cannot prove which chat is focused")
        collision = self._title_collision(session_id, session["title"])
        if collision is not None:
            raise WorkError(
                "target-window-unverified",
                f"another session's decorated title {collision!r} contains "
                f"this chat's title {session['title']!r}; a window name alone "
                f"cannot prove which chat it is")
        names = self._windows(session["title"], deadline)
        baseline = self._last_sequence(session_id)
        # The send is one exclusive action: a second caller (or a duplicated
        # pump) must not be able to type the same delivery while the first is
        # between its checks and its submit. The kernel drops the lock if the
        # process dies, so a crashed attempt stays retryable.
        self.evidence_root.mkdir(parents=True, exist_ok=True)
        lock_path = self.evidence_root / f"intent-{delivery_id}.lock"
        lock_handle = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
        try:
            fcntl.flock(lock_handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            os.close(lock_handle)
            raise WorkError(
                "native-delivery-unconfirmed",
                f"another process already holds delivery {delivery_id!r}; "
                "refusing to type it concurrently") from None
        try:
            return self._deliver_locked(
                session_id, delivery_id, body, text, baseline, limit,
                names[0], contract_sha256, attempt_id, deadline)
        finally:
            os.close(lock_handle)

    def _deliver_locked(self, session_id, delivery_id, body, text, baseline,
                        limit, window, contract_sha256, attempt_id, deadline):
        intent_path = self._persist_intent(session_id, delivery_id, body,
                                           baseline, limit)
        if self.session_identity_attribute:
            self._verify_session_identity(window, session_id, deadline)
        self._check_draft(window, deadline)
        # Only NOW has the delivery become an attempt that can type.
        self._mark_attempt(intent_path, session_id, delivery_id, body,
                           baseline, limit)
        self._send(window, text, deadline, session_id=session_id,
                   identity_attribute=self.session_identity_attribute)
        expected = max(baseline, 0) + 1
        while time.monotonic() < deadline:
            found = self.find(session_id, delivery_id, expected_text=body)
            if found is not None and found["sequence"] > baseline:
                if not found.get("turn_id"):
                    raise WorkError(
                        "native-delivery-unverified",
                        f"the user message {found['message_id']!r} has no turn "
                        f"anchor, so no native turn identity can be reported")
                state = self.turn_state(session_id, found["turn_id"])
                model = state["model"]
                if state["completed"] and not state["failed"] \
                        and model is not None:
                    return {**found, "delivered": True, "status": "delivered",
                            "window": window,
                            "waited_s": round(limit - max(
                                0.0, deadline - time.monotonic()), 3),
                            "contract_sha256": contract_sha256,
                            "model": model,
                            "attempt_id": attempt_id}
                landed = found
            self.sleep(self.poll_s)
        if "landed" in locals() and landed is not None:
            raise WorkError(
                "native-delivery-unconfirmed",
                f"the typed message exists in {session_id!r} but its turn has "
                f"no completed assistant record yet; the delivery cannot be "
                f"confirmed (delivery {delivery_id!r})")
        raise WorkError(
            "native-delivery-unverified",
            f"typed the instruction but no user message with {delivery_id!r} "
            f"was persisted in {session_id!r} (expected sequence > {baseline}); "
            f"the GUI may not have sent it")

    def _verify_session_identity(self, window, session_id, deadline):
        """The window must name the SESSION, not merely a similar title."""
        attribute = _quote(self.session_identity_attribute)
        value = self._run(
            f'tell application "System Events" to get value of attribute '
            f'"{attribute}" of window "{_quote(window)}" of process '
            f'"{APPLICATION}"', deadline).strip()
        # EXACT match: `s1` must not accept `s10` (a substring would send the
        # instruction into a different session).
        if value.strip() != session_id:
            raise WorkError(
                "target-window-unverified",
                f"window {window!r} identifies {value!r}, not the bound "
                f"session {session_id!r}")

    def _persist_intent(self, session_id, delivery_id, body, baseline, limit):
        """Record WHAT will be typed and WHERE, before anything is typed.

        A persisted intent with no reply means a previous process died after
        the send. Typing the same instruction again could duplicate a message
        the user sees, so the bridge refuses a second attempt outright.
        """
        self.evidence_root.mkdir(parents=True, exist_ok=True)
        path = self.evidence_root / f"intent-{delivery_id}.json"
        if path.exists():
            try:
                previous = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                raise WorkError(
                    "native-delivery-unconfirmed",
                    f"the persisted intent for {delivery_id!r} is unreadable; "
                    "the previous attempt cannot be classified and must be "
                    "reconciled before another send") from None
            required = ("session_id", "delivery_id", "body_sha256",
                        "cursor_baseline", "timeout_s")
            if any(key not in previous for key in required):
                raise WorkError(
                    "native-delivery-unconfirmed",
                    f"the persisted intent for {delivery_id!r} is incomplete; "
                    "an unverifiable previous attempt cannot be retried")
            expected = {
                "session_id": session_id, "delivery_id": delivery_id,
                "body_sha256": hashlib.sha256(
                    body.encode("utf-8")).hexdigest(),
                "cursor_baseline": baseline, "timeout_s": limit}
            mismatched = [key for key, value in expected.items()
                          if previous.get(key) != value]
            if mismatched:
                raise WorkError(
                    "native-delivery-unconfirmed",
                    f"the persisted intent for {delivery_id!r} does not "
                    f"describe this delivery (differs in {mismatched}); "
                    "refusing to type a different message")
            attempts = int(previous.get("attempts") or 0)
            if attempts >= 1:
                raise WorkError(
                    "native-delivery-unconfirmed",
                    f"delivery {delivery_id!r} was already attempted "
                    f"({attempts} send(s)) and no persisted message confirms "
                    "it; refusing to type it a second time")
            return path
        # The concurrent gate is the flock held by deliver(): it covers both
        # this new-intent path and the already-persisted path, and the kernel
        # releases it when the process dies.
        _write_json(path, {
            "session_id": session_id, "delivery_id": delivery_id,
            "body_sha256": hashlib.sha256(body.encode("utf-8")).hexdigest(),
            "cursor_baseline": baseline, "timeout_s": limit,
            "attempts": 0,
            "created": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        })
        return path

    def turn_state(self, session_id: str, turn_id: str) -> dict:
        """Completion, failure and model recorded on THIS turn only."""
        # The LAST assistant record governs: a completed reply followed by a
        # still-running or failed one is not a delivered turn (R8).
        latest = None
        for row in self._messages(session_id):
            try:
                data = json.loads(row["data"] or "{}")
            except json.JSONDecodeError:
                continue
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
        completed = bool(done and finish in ("stop", "completed", "end_turn")
                         and not failed)
        return {"completed": completed,
                "model": latest.get("modelId"), "failed": failed}

    def _turn_model(self, session_id: str, turn_id: str):
        """The modelId recorded on THIS turn's assistant messages."""
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT data FROM message WHERE session_id=? "
                "ORDER BY sequence DESC", (session_id,)).fetchall()
        for row in rows:
            try:
                data = json.loads(row[0] or "{}")
            except json.JSONDecodeError:
                continue
            anchor = data.get("anchor") or {}
            if anchor.get("turnId") != turn_id:
                continue
            if data.get("role") == "assistant" and data.get("modelId"):
                return data["modelId"]
        return None

    def _recorded_model(self, session_id: str):
        """The model the store records for this session's latest turn."""
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT data FROM message WHERE session_id=? "
                "ORDER BY sequence", (session_id,)).fetchall()
        for row in reversed(rows):
            try:
                data = json.loads(row[0] or "{}")
            except json.JSONDecodeError:
                continue
            if data.get("modelId"):
                return data["modelId"]
        return None

    def _mark_attempt(self, path, session_id, delivery_id, body, baseline,
                      limit) -> None:
        """Count the send only when the script is about to type.

        The identity is revalidated and only ``attempts == 0`` may become 1:
        a negative or repeated counter would otherwise let the same delivery
        be typed twice.
        """
        try:
            document = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            raise WorkError(
                "native-delivery-unconfirmed",
                f"the intent for {path.name!r} disappeared before the send; "
                "refusing to type an unrecorded attempt") from None
        expected = {
            "session_id": session_id, "delivery_id": delivery_id,
            "body_sha256": hashlib.sha256(body.encode("utf-8")).hexdigest(),
            "cursor_baseline": baseline, "timeout_s": limit}
        mismatched = [key for key, value in expected.items()
                      if document.get(key) != value]
        attempts = int(document.get("attempts") or 0)
        if mismatched or attempts != 0:
            raise WorkError(
                "native-delivery-unconfirmed",
                f"the intent for {delivery_id!r} changed before the send "
                f"(differs in {mismatched}, attempts={attempts}); refusing "
                "to type an unrecorded attempt")
        document["attempts"] = 1
        _write_json(path, document)

    # -- GUI steps -----------------------------------------------------
    def _windows(self, title: str, deadline=None) -> list[str]:
        output = self._run(
            f'tell application "System Events" to get name of every window '
            f'of process "{APPLICATION}"', deadline)
        names = [name.strip() for name in output.split(",") if name.strip()]
        # The app decorates its own window with " — ZCode"; accept exactly
        # that. Any other suffix (e.g. " — followup — ZCode") names a
        # different session, and typing there would address the wrong chat.
        matches = [name for name in names
                   if name == title or name == f"{title} \u2014 ZCode"]
        if not matches:
            raise WorkError(
                "target-window-unverified",
                f"no {APPLICATION} window title contains {title!r} "
                f"(windows: {names})")
        if len(matches) > 1:
            raise WorkError(
                "target-window-unverified",
                f"{len(matches)} {APPLICATION} windows match {title!r}; "
                f"refusing to guess ({matches})")
        return matches

    def _run(self, script: str, deadline=None):
        timeout = None
        if deadline is not None:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise WorkError(
                    "desktop-wakeup-failed",
                    "the delivery contract deadline passed before the next "
                    "window operation; nothing was typed or submitted")
            timeout = remaining
        try:
            return self.script_runner(script, timeout=timeout)
        except TypeError:
            # injected runners keep the one-argument shape; the deadline is
            # still enforced by the explicit checks around every call
            return self.script_runner(script)

    def _title_collision(self, session_id: str, title: str):
        """Another session whose DECORATED title equals this raw title.

        Window names are titles, and the host appends separators to some of
        them; a different chat can therefore own a window whose name equals
        ours. Exact-name matching alone cannot tell them apart, so the bridge
        refuses when the library shows such a collision.
        """
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT id, title FROM session WHERE id != ?",
                (session_id,)).fetchall()
        for row in rows:
            other = (row["title"] or "").strip()
            if not other:
                continue
            if other == title:
                return other
            # A decorated title ("<theirs> — ZCode") can be shown as a window
            # name equal to OUR title; the prefix form is the observable
            # signal, so it is refused rather than guessed around.
            for separator in (" — ", " - ", " – ", " | ", " · "):
                # Either side may carry the decoration: `topic — part` is a
                # prefix of `topic — part — ZCode` at a separator boundary.
                if other.startswith(title + separator) \
                        or title.startswith(other + separator):
                    return other
                if separator in other \
                        and other.split(separator, 1)[0].strip() == title:
                    return other
                if separator in title \
                        and title.split(separator, 1)[0].strip() == other:
                    return other
        return None

    def _check_draft(self, window: str, deadline=None) -> None:
        output = self._run(
            f'tell application "System Events"\n'
            f'  tell process "{APPLICATION}"\n'
            f'    set w to first window whose name is "{_quote(window)}"\n'
            f'    set focused to value of attribute "AXFocusedUIElement" of w\n'
            f'    try\n'
            f'      return value of focused\n'
            f'    on error\n'
            f'      return ""\n'
            f'    end try\n'
            f'  end tell\n'
            f'end tell', deadline)
        if output.strip():
            raise WorkError(
                "draft-present",
                f"the {APPLICATION} input box already holds text; refusing to "
                f"overwrite a human draft")

    def _send(self, window: str, text: str, deadline=None, *,
              session_id=None, identity_attribute=None) -> None:
        self._run(
            f'tell application "System Events"\n'
            f'  tell process "{APPLICATION}"\n'
            f'    set frontmost to true\n'
            f'    set w to first window whose name is "{_quote(window)}"\n'
            f'    perform action "AXRaise" of w\n'
            f'  end tell\n'
            f'end tell', deadline)
        # Raising the window can move focus to another input box, so the
        # draft check runs AGAIN here — after the raise, immediately before
        # typing. A draft that appeared meanwhile is never overwritten.
        self._check_draft(window, deadline)
        if deadline is not None and time.monotonic() >= deadline:
            raise WorkError(
                "native-delivery-failed",
                "the contract deadline expired during the window checks; "
                "nothing was typed")
        # Write, verify and Confirm happen in ONE osascript. A failure AFTER
        # the write clears the SAME element in the same call (AppleScript
        # keeps the reference, so a replaced window can never be touched) and
        # re-raises the original error. Python never guesses the stage.
        quoted = _quote(text)
        identity_check = ""
        if session_id and identity_attribute:
            ident_attr = _quote(identity_attribute)
            ident_value = _quote(session_id)
            identity_check = (
                f'    set ident to value of attribute "{ident_attr}" of w\n'
                f'    if not my sameText(ident, "{ident_value}") then error '
                f'"zsc-wrong-window"\n')
        self._run(
            # AppleScript's ``is`` folds some Unicode variants (a plain space
            # equals a no-break space). Compare code points instead: the
            # handler walks both strings and compares the id of each
            # character, which is the Unicode scalar value.
            f'on sameText(a, b)\n'
            f'  if (count of characters of a) is not (count of characters of b) '
            f'then return false\n'
            f'  repeat with i from 1 to (count of characters of a)\n'
            f'    if (id of character i of a) is not (id of character i of b) '
            f'then return false\n'
            f'  end repeat\n'
            f'  return true\n'
            f'end sameText\n'
            f'tell application "System Events"\n'
            f'  tell process "{APPLICATION}"\n'
            f'    set frontmost to true\n'
            f'    set w to first window whose name is "{_quote(window)}"\n'
            f'    perform action "AXRaise" of w\n'
            f'    set areas to every text area of w\n'
            f'    if (count of areas) is not 1 then error '
            f'"zsc-ambiguous-input"\n'
            f'    set target to item 1 of areas\n'
            f'    if (value of attribute "AXFocusedUIElement" of w) is not '
            f'target then error "zsc-wrong-input"\n'
            f'    if role of target is not "AXTextArea" then error '
            f'"zsc-wrong-input"\n'
            f'    if (value of target) is not "" then error '
            f'"zsc-draft-present"\n'
            f'    if not (exists action "AXConfirm" of target) then error '
            f'"zsc-confirm-unsupported"\n'
            + identity_check +
            f'    set wrote to false\n'
            f'    try\n'
            f'      set value of target to "{quoted}"\n'
            f'      set wrote to true\n'
            f'      if not my sameText(value of target, "{quoted}") then error '
            f'"zsc-wrong-input-after"\n'
            + identity_check.replace("    ", "      ", 1).replace(
                "zsc-wrong-window", "zsc-wrong-window-after", 1) +
            f'      if name of (first window whose value of attribute "AXMain" '
            f'is true) is not "{_quote(window)}" then error '
            f'"zsc-wrong-window-after"\n'
            f'      perform action "AXConfirm" of target\n'
            f'    on error errMsg number errNum\n'
            f'      if wrote then\n'
            f'        try\n'
            f'          if my sameText(value of target, "{quoted}") then set '
            f'value of target to ""\n'
            f'        end try\n'
            f'      end if\n'
            f'      error errMsg number errNum\n'
            f'    end try\n'
            f'  end tell\n'
            f'end tell', deadline)

    def _last_sequence(self, session_id: str) -> int:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT COALESCE(MAX(sequence), 0) FROM message "
                "WHERE session_id=?", (session_id,)).fetchone()
        return int(row[0] or 0)


def _write_json(path, document) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(document, indent=2), encoding="utf-8")
    os.replace(temporary, path)
