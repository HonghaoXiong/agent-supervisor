#!/bin/bash
# Finder-friendly local plugin installer; no sudo, runtime downloads or model calls.
release_dir="$(cd -- "$(dirname -- "$0")" && pwd)" || exit 1
"$release_dir/scripts/python.sh" "$release_dir/scripts/install.py" "$@"
install_result=$?
if [[ -t 0 && $# -eq 0 ]]; then
    read -r -p 'Press Return to close this window. ' ignored_reply
fi
exit "$install_result"
