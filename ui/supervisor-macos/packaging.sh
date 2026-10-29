#!/bin/zsh
# Package the release binary into a minimal .app.
# P1.2 original; P3.3 repair — repeatable + self-verifying:
#   * refuses to run without the release binary (clear exit 2)
#   * fixed canonical layout (Contents/MacOS/<exe>, Contents/Info.plist)
#   * deterministic modes (755 executable, 644 plist)
#   * prints per-file SHA256 + total size every run (evidence-friendly)
# Verified repeatable on this toolchain: clean rebuild reproduced the
# identical binary SHA256 (tasks/P3.3/binary-hash-{pre,post}clean.txt).
# NOTE: unsigned local bundle only — Developer ID signing / notarization /
# stapler were NOT executed (no credentials; recorded not_run in §5.3.1
# matrix, tasks/P3.3/g531-matrix.json).
set -e
cd "$(dirname "$0")"
APP=supervisor-macos.app
BIN=target/release/supervisor-macos
[[ -x "$BIN" ]] || { echo "packaging.sh: missing $BIN — run 'cargo build --release --locked' first" >&2; exit 2; }
[[ -f Info.plist ]] || { echo "packaging.sh: missing Info.plist" >&2; exit 2; }
rm -rf "$APP"
mkdir -p "$APP/Contents/MacOS"
cp "$BIN" "$APP/Contents/MacOS/"
cp Info.plist "$APP/Contents/Info.plist"
chmod 755 "$APP/Contents/MacOS/supervisor-macos"
chmod 644 "$APP/Contents/Info.plist"
echo "packaged: $APP (unsigned, local)"
find "$APP" -type f -exec shasum -a 256 {} +
du -sk "$APP" | awk '{printf "bundle size: %s KiB\n", $1}'
