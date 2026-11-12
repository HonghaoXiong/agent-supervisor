"""Deliver a prompt into the running Codex desktop chat.

The desktop app keeps the thread writer, so a second app-server cannot resume
the thread (JSON-RPC -32600 ``already has an active writer``). The app's own
wake path is the supported entry:

1. queue the message through the app-server thread queue
   (``thread/queue/add`` with a unique ``clientUserMessageId``),
2. open ``codex://threads/wake/<thread>``: the app connects to its own
   app-server, sees the queued item and resumes the thread,
3. the app dispatches the queued item as a normal user turn and the agent
   answers it inside the same chat.

This module implements that channel plus the receipts the session loop needs:
the queued id, the turn id, the user/agent message ids and the final agent
message. Reads (``thread/turns/list``) are served without the writer lock.
"""

import asyncio
import os
import subprocess
import time
import uuid
from pathlib import Path

from .codex_appserver import CodexAppServerClient
from .work_models import WorkError

DEFAULT_CODEX = "/Applications/ChatGPT.app/Contents/Resources/codex-cli/bin/codex"
APP_BUNDLE = Path("/Applications/ChatGPT.app")
WAKE_URL = "codex://threads/wake/{thread_id}"
POLL_SECONDS = 1.0


class DesktopBridgeError(WorkError):
    def __init__(self, code, message):
        super().__init__(code, message)


def bridge_capability(codex_path=None):
    """Return the desktop delivery capability for this host.

    ``available`` means the queue+wake channel is usable here: macOS, the
    ChatGPT app installed, its bundled codex CLI present and ``open`` able to
    resolve the codex:// scheme. It says nothing about the app being awake --
    the wake deep link launches it.
    """
    path = Path(codex_path or DEFAULT_CODEX)
    if not APP_BUNDLE.exists() or not path.is_file():
        return {"state": "desktop-wakeup-unsupported",
                "channel": "queue-wake",
                "detail": "ChatGPT.app or its bundled codex CLI is missing"}
    return {"state": "desktop-wakeup-available", "channel": "queue-wake",
            "codex_path": str(path)}


class DesktopBridge:
    """Queue+wake delivery into one desktop Codex thread.

    ``client_factory`` and ``opener`` are injection points for tests; the
    defaults spawn the app-server over stdio and call ``open``.
    """

    def __init__(self, codex_path=None, *, codex_home=None,
                 evidence_root=None, timeout_s=600.0, poll_s=POLL_SECONDS,
                 client_factory=None, opener=None, sleep=None):
        self.codex_path = str(codex_path or DEFAULT_CODEX)
        self.codex_home = codex_home or os.path.expanduser("~/.codex")
        self.evidence_root = Path(evidence_root or
                                  "/private/tmp/zsc-desktop-bridge")
        self.timeout_s = float(timeout_s)
        self.poll_s = float(poll_s)
        self._client_factory = client_factory
        self._opener = opener or self._default_opener
        self._sleep = sleep or asyncio.sleep

    # -- public API ---------------------------------------------------------

    async def deliver(self, thread_id, text, *, delivery_id=None,
                      timeout_s=None):
        """Queue ``text`` into ``thread_id`` and wait for the agent's turn.

        Returns a receipt. Re-delivering the same ``delivery_id`` returns the
        recorded delivery (``status="already-delivered"``) without queuing a
        second message; if the earlier attempt is still queued, the wake link
        is re-opened instead of queueing again.
        """
        delivery_id = delivery_id or ("db-" + uuid.uuid4().hex[:16])
        deadline = time.monotonic() + float(timeout_s or self.timeout_s)
        client = await self._start(thread_id, delivery_id)
        try:
            existing = self._find_turn(await self._turns(client, thread_id),
                                       delivery_id)
            if existing is not None and existing.get("state") == "final":
                return self._receipt(existing, "already-delivered",
                                     delivery_id, thread_id, 0.0)
            queued_id = None
            if existing is None:
                queued = [item for item in await self._queue(client, thread_id)
                          if item.get("clientUserMessageId") == delivery_id]
                if not queued:
                    added = await client.request("thread/queue/add", {
                        "threadId": thread_id,
                        "input": [{"type": "text", "text": text}],
                        "clientUserMessageId": delivery_id})
                    queued_id = (added.get("queuedSubmission") or {}).get("id")
                else:
                    queued_id = queued[0].get("id")
            await self._wake(thread_id)
            started = time.monotonic()
            while time.monotonic() < deadline:
                turns = await self._turns(client, thread_id)
                found = self._find_turn(turns, delivery_id)
                if found is not None and found.get("state") == "final":
                    receipt = self._receipt(found, "delivered", delivery_id,
                                            thread_id, time.monotonic() - started)
                    receipt["queued_id"] = queued_id
                    return receipt
                if found is None and queued_id and not any(
                        item.get("id") == queued_id
                        for item in await self._queue(client, thread_id)):
                    # consumed but the turn has not surfaced yet: keep polling
                    pass
                await self._sleep(self.poll_s)
            found = self._find_turn(await self._turns(client, thread_id),
                                    delivery_id)
            if found is not None:
                # the message is IN the thread but the agent has not finished:
                # that is a half turn, never a delivery; keep its identity so a
                # restarted owner resumes THIS turn instead of re-sending.
                receipt = self._receipt(found, "in-progress", delivery_id,
                                        thread_id, time.monotonic() - started)
                receipt["queued_id"] = queued_id
                return receipt
            return {"delivered": False, "status": "queued-not-consumed",
                    "thread_id": thread_id, "delivery_id": delivery_id,
                    "turn_id": None, "final_message": None,
                    "queued_id": queued_id,
                    "waited_s": round(time.monotonic() - started, 3)}
        finally:
            await self._close(client)

    async def find(self, thread_id, delivery_id, *, timeout_s=None):
        """Return the receipt for an earlier delivery, or None."""
        client = await self._start(thread_id, delivery_id)
        try:
            found = self._find_turn(await self._turns(client, thread_id),
                                    delivery_id)
            if found is None:
                return None
            return self._receipt(found, "already-delivered", delivery_id,
                                 thread_id, 0.0)
        finally:
            await self._close(client)

    async def latest_turn(self, thread_id):
        """Latest turn as ``{turn_id, client_id, user_text, started_at_ms}``."""
        client = await self._start(thread_id, "latest")
        try:
            turns = await self._turns(client, thread_id, limit=1)
            if not turns:
                return None
            turn = turns[0]
            user = next((item for item in turn.get("items") or []
                         if item.get("type") == "userMessage"), {})
            content = user.get("content") or [{}]
            return {"turn_id": turn.get("id"),
                    "client_id": user.get("clientId"),
                    "user_text": str((content[0] or {}).get("text") or ""),
                    "started_at_ms": user.get("startedAtMs")}
        finally:
            await self._close(client)

    # -- internals ----------------------------------------------------------

    async def _start(self, thread_id, delivery_id):
        # each attempt gets its own directory: a retried delivery id must not
        # collide with a previous attempt's evidence files
        evidence = (self.evidence_root
                    / f"{thread_id[:20]}-{delivery_id[:24]}"
                    / f"attempt-{uuid.uuid4().hex[:8]}")
        evidence.mkdir(parents=True, exist_ok=True)
        if self._client_factory is not None:
            return await self._client_factory(evidence)
        client = CodexAppServerClient(
            self.codex_path, evidence, experimental_api=True,
            env={"CODEX_HOME": self.codex_home})
        try:
            await client.start()
        except BaseException:
            await client.close()
            raise
        return client

    async def _close(self, client):
        close = getattr(client, "close", None)
        if close is not None:
            await close()

    async def _turns(self, client, thread_id, limit=20):
        result = await client.request("thread/turns/list", {
            "threadId": thread_id, "limit": limit})
        return result.get("data") or []

    async def _queue(self, client, thread_id):
        result = await client.request("thread/queue/list", {
            "threadId": thread_id, "limit": 50})
        return result.get("data") or []

    async def _wake(self, thread_id):
        result = await self._opener(WAKE_URL.format(thread_id=thread_id))
        if result not in (0, None):
            raise DesktopBridgeError(
                "desktop-wakeup-failed",
                f"the wake deep link exited {result}; the app did not accept "
                f"codex://threads/wake/{thread_id}")

    @staticmethod
    def _find_turn(turns, delivery_id):
        for turn in turns:
            items = turn.get("items") or []
            user = next((item for item in items
                         if item.get("type") == "userMessage"
                         and item.get("clientId") == delivery_id), None)
            if user is None:
                continue
            agent = next((item for item in reversed(items)
                          if item.get("type") == "agentMessage"
                          and item.get("phase") == "final_answer"), None)
            answering = any(item.get("type") == "agentMessage"
                            for item in items)
            status = str(turn.get("status") or "")
            completed = status == "completed"
            failed = status in ("failed", "interrupted", "cancelled")
            state = ("final" if (agent is not None and completed)
                     else "failed" if failed
                     else "answering" if (answering or agent is not None)
                     else "user-only")
            return {"turn_id": turn.get("id"), "state": state,
                    "user_message_id": user.get("id"),
                    "user_text": ((user.get("content") or [{}])[0] or {}).get("text"),
                    "agent_message_id": (agent or {}).get("id"),
                    "final_message": (agent or {}).get("text"),
                    "completed_at_ms": (agent or {}).get("completedAtMs")}
        return None

    @staticmethod
    def _receipt(found, status, delivery_id, thread_id, waited_s):
        state = found.get("state")
        final = state == "final"
        return {"delivered": bool(final),
                "status": status if final else (
                    "failed" if state == "failed" else "in-progress"),
                "state": state,
                "thread_id": thread_id,
                "delivery_id": delivery_id, "turn_id": found.get("turn_id"),
                "user_message_id": found.get("user_message_id"),
                "agent_message_id": found.get("agent_message_id"),
                "final_message": found.get("final_message"),
                "waited_s": round(waited_s, 3)}

    @staticmethod
    async def _default_opener(url):
        # open(1) returns immediately; the app consumes the wake link itself.
        result = await asyncio.to_thread(
            subprocess.run, ["open", url], check=False, capture_output=True)
        return result.returncode
