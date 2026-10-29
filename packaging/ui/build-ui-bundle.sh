#!/bin/zsh
# Build + package the supervisor macOS UI bundle (P3.3).
# Single repeatable entry point from the repo root:
#   zsh packaging/ui/build-ui-bundle.sh
# = cargo build --release --locked  +  ui/supervisor-macos/packaging.sh
#   + final SHA256/size record. Unsigned local bundle (no Developer ID
#   credentials — see tasks/P3.3/g531-matrix.json: not_run, honestly
#   registered; do NOT claim distribution readiness).
set -e
cd "$(dirname "$0")/../.."          # repo root (worktree)
cargo build --release --locked --manifest-path ui/supervisor-macos/Cargo.toml
zsh ui/supervisor-macos/packaging.sh
