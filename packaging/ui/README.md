# UI bundle (macOS) — build & packaging notes

P3.3 entry point:

```bash
zsh packaging/ui/build-ui-bundle.sh
```

which is exactly:

1. `cargo build --release --locked --manifest-path ui/supervisor-macos/Cargo.toml`
2. `zsh ui/supervisor-macos/packaging.sh` — assembles
   `ui/supervisor-macos/supervisor-macos.app`
   (`Contents/MacOS/supervisor-macos` 755, `Contents/Info.plist` 644)
   and prints per-file SHA256 + bundle size on every run.

Repeatability record (2026-10-29, cargo 1.99.0, arm64 macOS):
a clean rebuild (`cargo clean` + step 1) reproduced the bit-identical
binary SHA256 `6ae8e63417f72218ec099db595c443299defbcbc352ad26730df3d340fca2a1f`
(186 crates, 14.82 s) — evidence in
`tasks/P3.3/build-release-clean.log`, `binary-hash-{pre,post}clean.txt`.

## Signing status — unsigned / not notarized (honest state)

The bundle carries only the linker's ad-hoc signature
(`codesign -dv`: `Signature=adhoc`, `TeamIdentifier=not set`).
`codesign --verify` fails (`code has no resources but signature
indicates they must be present`) and `spctl -a -t exec` rejects it —
raw outputs preserved in `tasks/P3.3/bundle-validation.log`.
No Developer ID credentials were available, so codesign (identity),
notarytool and stapler were NOT executed: this is an
**unsigned / not-notarized acceptance**. The local prototype
conclusion is unaffected; distribution readiness is NOT claimed.

## Bundle facts

- arm64 Mach-O, 14,780,144 bytes; bundle 14,440 KiB
- Info.plist keys: `CFBundleExecutable=supervisor-macos`,
  `CFBundleIdentifier=org.supervisor.readonly`,
  `CFBundleName=supervisor (read-only)`, `CFBundlePackageType=APPL`,
  `CFBundleShortVersionString=0.1.0`, `NSHighResolutionCapable=true`
- The binary exposes NO CLI flags: `--help`/`--version` are not
  implemented (args ignored; GUI launches). Socket selection:
  `CZS_UI_SOCKET` env var, compiled default `.czs/state/ui-events.sock`
  resolved relative to the process working directory.

## Clean-session boundary (no credentials) — measured 2026-10-29

Against the shipped bundle binary with fresh empty `HOME`/`TMPDIR`
sandboxes (`tasks/P3.3/boundary/`):

- socket absent: app stays alive 30 s, no crash, silent stdout/stderr
- fake server hello `v=2`: refused before any snapshot/subscribe —
  13 hello-only connections dropped at the bounded backoff schedule
  (0.30/0.31/0.51/0.81/1.32/1.83/2.54/3.25/4.27/5.29/5.29/5.29 s),
  no retry storm
- `CZS_UI_SOCKET` at a custom absolute path: standard startup observed
  on the wire (`hello` → `snapshot` → `snapshot-events` → `subscribe`,
  exactly one connection/hello)
- compiled default path: same handshake with the env var unset and the
  socket at `<cwd>/.czs/state/ui-events.sock`

GUI interaction gates (keyboard / VoiceOver / zoom / render smoke /
screenshots) were NOT run in that session — see the g531 matrix.
