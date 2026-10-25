"""Dispatch/completion notification tests; no real osascript call is made."""

import json
from pathlib import Path
import sys
import tempfile
import unittest

from zcode_supervisor import notify, runtime


class RecordingRunner:
    def __init__(self, error=None):
        self.calls = []
        self.error = error

    def __call__(self, argv, **kwargs):
        self.calls.append((argv, kwargs))
        if self.error is not None:
            raise self.error


class SendTests(unittest.TestCase):
    def test_disabled_or_nondarwin_sends_nothing(self):
        for enabled, platform in ((False, "darwin"), (False, "linux"), (True, "linux"),
                                  (True, "win32"), (False, "win32")):
            with self.subTest(enabled=enabled, platform=platform):
                runner = RecordingRunner()
                notify.send("t", "m", enabled, platform=platform, runner=runner)
                self.assertEqual(runner.calls, [])

    def test_darwin_sends_one_escaped_notification(self):
        runner = RecordingRunner()
        notify.send('title "quoted"', 'line1\nline2 \\ back', True,
                    platform="darwin", runner=runner)
        self.assertEqual(len(runner.calls), 1)
        argv, kwargs = runner.calls[0]
        self.assertEqual(argv[0], "/usr/bin/osascript")
        self.assertEqual(argv[1], "-e")
        self.assertNotIn("\n", argv[2])
        self.assertIn('\\"quoted\\"', argv[2])
        self.assertIn("\\\\ back", argv[2])
        self.assertIs(kwargs.get("check"), False)

    def test_sound_only_when_requested_and_message_is_capped(self):
        plain, sound = RecordingRunner(), RecordingRunner()
        notify.send("t", "m", True, platform="darwin", runner=plain)
        notify.send("t", "x" * 500, True, sound="Basso", platform="darwin", runner=sound)
        self.assertNotIn("sound name", plain.calls[0][0][2])
        self.assertIn('sound name "Basso"', sound.calls[0][0][2])
        self.assertLessEqual(len(sound.calls[0][0][2].split("with title")[0]), notify.MAX_MESSAGE + 40)

    def test_runner_failures_are_never_propagated(self):
        for error in (OSError("gone"), RuntimeError("boom"), TimeoutError()):
            with self.subTest(error=error):
                notify.send("t", "m", True, platform="darwin",
                            runner=RecordingRunner(error=error))

    def test_truthy_non_boolean_enabled_never_posts(self):
        for value in ("yes", "message string", 1, [True]):
            with self.subTest(value=value):
                runner = RecordingRunner()
                notify.send("t", "m", value, platform="darwin", runner=runner)
                self.assertEqual(runner.calls, [])


class NotifyConfigurationTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        self.values = {"schema_version": 1, "work_root": "work", "state_dir": "state",
                       "cache_dir": "cache", "cli_path": "installed.cjs", "node": "node",
                       "compatibility": "pinned-0.16.9"}
        (self.directory / "installed.cjs").write_bytes(b"bundle")

    def load(self):
        path = self.directory / "config.json"
        path.write_text(json.dumps(self.values))
        return runtime.load_config(path)

    def test_defaults_to_platform_and_accepts_explicit_values(self):
        self.assertIs(self.load().notify, sys.platform == "darwin")
        for value in (True, False):
            with self.subTest(value=value):
                self.values["notify"] = value
                self.assertIs(self.load().notify, value)

    def test_non_boolean_notify_is_rejected(self):
        for value in ("yes", 1, None):
            with self.subTest(value=value):
                self.values["notify"] = value
                with self.assertRaisesRegex(ValueError, "notify must be a boolean"):
                    self.load()

    def test_directly_built_config_defaults_to_silent(self):
        config = runtime.Config(work_root=self.directory / "work",
                                state_dir=self.directory / "state",
                                cache_dir=self.directory / "cache",
                                cli_path=self.directory / "installed.cjs")
        self.assertIs(config.notify, False)
        with self.assertRaisesRegex(ValueError, "notify must be a boolean"):
            runtime.Config(work_root=self.directory / "work",
                           state_dir=self.directory / "state",
                           cache_dir=self.directory / "cache",
                           cli_path=self.directory / "installed.cjs", notify="on")
