#!/bin/sh
# Find a compatible installed interpreter; never install packages or alter PATH.
set -eu
if [ -n "${CZS_PYTHON:-}" ]; then
    if "$CZS_PYTHON" -c 'import sys; sys.exit(0 if sys.version_info >= (3,11) else 1)' 2>/dev/null; then
        exec "$CZS_PYTHON" -I -B "$@"
    fi
    echo 'CZS_PYTHON must name an executable Python 3.11+ interpreter.' >&2
    exit 2
fi
for candidate in python3 /opt/homebrew/bin/python3 /usr/local/bin/python3 python3.13 python3.12 python3.11; do
    if command -v "$candidate" >/dev/null 2>&1 && "$candidate" -c 'import sys; sys.exit(0 if sys.version_info >= (3,11) else 1)' 2>/dev/null; then
        exec "$candidate" -I -B "$@"
    fi
done
echo 'Python 3.11+ is required. Install it, or set CZS_PYTHON to an existing interpreter, then retry.' >&2
exit 2
