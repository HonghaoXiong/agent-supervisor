#!/usr/bin/env python3
"""Launch the packaged supervisor without pip, user-site imports or PATH changes."""
from pathlib import Path
import sys

if sys.version_info < (3, 11):
    raise SystemExit("ZCode Supervisor requires Python 3.11 or newer.")

plugin_root = Path(__file__).resolve().parent.parent
library = plugin_root / "lib"
if not (library / "zcode_supervisor" / "cli.py").is_file():
    raise SystemExit("Bundled runtime missing. Install the built macOS release, not the source template.")
sys.dont_write_bytecode = True
sys.path.insert(0, str(library))

from zcode_supervisor.cli import main

if __name__ == "__main__":
    raise SystemExit(main())
