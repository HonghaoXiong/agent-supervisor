# Contributor instructions

This repository is the portable supervisor, not any scientific application.

- Keep the installed ZCode application and user authentication/configuration unchanged.
- Never commit runtime configuration, provider files, CLI bundles, task snapshots,
  conversation logs, credentials, or real remote job data.
- Tool visibility is separate from permission approval. Unknown CLI fingerprints
  fail closed; do not add yolo, automatic allow, or unchecked fallback modes.
- Preserve session/turn/tool identity, exact-command approval, immutable inputs,
  and actual process exit evidence. Delivery acceptance is not domain acceptance.
- Test with `python -m unittest discover -s tests -v` after editable installation.
- Keep macOS/Linux portability claims separate from live ZCode version validation.
- Do not execute live model calls or remote jobs in CI.
