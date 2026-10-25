"""Best-effort local desktop notifications for dispatch and completion.

A notification is a convenience for the human operator only. It is not delivery
evidence, is not queued or retried, and a missing or failed notification must
never be read as a task outcome; czs run results and exit codes stay authoritative.
"""

from __future__ import annotations

import subprocess
import sys

MAX_MESSAGE = 220
OSASCRIPT = "/usr/bin/osascript"


def _escape(text: str) -> str:
    return text.replace("\\", "\\\\").replace('"', '\\"')


def _collapse(value: object) -> str:
    return " ".join(str(value).split())[:MAX_MESSAGE]


def send(title: str, message: object, enabled: bool,
         sound: str | None = None, platform: str | None = None,
         runner=None) -> None:
    """Post one macOS notification, or return quietly when disabled/unsupported.

    Callers must pass ``enabled`` by keyword; only the boolean ``True`` enables
    posting so a mistyped positional argument can never enable notifications.
    """
    if enabled is not True or (platform if platform is not None else sys.platform) != "darwin":
        return
    script = (f'display notification "{_escape(_collapse(message))}" '
              f'with title "{_escape(_collapse(title))}"')
    if sound:
        script += f' sound name "{_escape(_collapse(sound))}"'
    try:
        (runner or subprocess.run)([OSASCRIPT, "-e", script],
                                   capture_output=True, timeout=5, check=False)
    except Exception:
        # Never let operator notifications affect or fail a supervised run.
        pass
