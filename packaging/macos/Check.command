#!/bin/bash
# Read-only prerequisite diagnostics; no plugin registration or model requests.
release_dir="$(cd -- "$(dirname -- "$0")" && pwd)" || exit 1
if [[ $# -eq 0 ]]; then
    report_dir="$HOME/Desktop"
    [[ -d "$report_dir" ]] || report_dir="$HOME"
    report="$report_dir/zcode-supervisor-check-$(date -u +%Y%m%dT%H%M%SZ)-$$.json"
    "$release_dir/scripts/python.sh" "$release_dir/scripts/diagnostics.py" --output "$report"
    check_result=$?
    printf '\nDiagnostic report: %s\n' "$report"
else
    "$release_dir/scripts/python.sh" "$release_dir/scripts/diagnostics.py" "$@"
    check_result=$?
fi
if [[ -t 0 && $# -eq 0 ]]; then
    read -r -p 'Press Return to close this window. ' ignored_reply
fi
exit "$check_result"
