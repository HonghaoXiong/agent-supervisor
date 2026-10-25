#!/bin/bash
# Export environment and installed-skill discovery; no model or ZCode task calls.
release_dir="$(cd -- "$(dirname -- "$0")" && pwd)" || exit 1
"$release_dir/scripts/python.sh" "$release_dir/scripts/validate.py" "$@"
validation_result=$?
if [[ -t 0 && $# -eq 0 ]]; then
    read -r -p 'Press Return to close this window. ' ignored_reply
fi
exit "$validation_result"
