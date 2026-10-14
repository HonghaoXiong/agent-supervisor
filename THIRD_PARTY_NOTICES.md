# Third-party notices

The original Python implementation and documentation in this repository are
copyright 2026 HonghaoXiong and licensed under Apache License 2.0. See [LICENSE](LICENSE).
Third-party attribution below remains with its respective owner. This project
is independently maintained and is not an official ZCode release.

## ZCode permission compatibility patch

- Project: [zai-org/ZCode](https://github.com/zai-org/ZCode)
- Copyright: **2026 Z.AI Co., Ltd**
- License: **Apache License 2.0**; the full license is included in [LICENSE](LICENSE).
- Reviewed commit: `aac4755666d09fdcd70272fcf063c077a639015f`
- Source: [`apps/zcode-cli/packages/bootstrap/src/app/app-config-options.ts`](https://github.com/zai-org/ZCode/blob/aac4755666d09fdcd70272fcf063c077a639015f/apps/zcode-cli/packages/bootstrap/src/app/app-config-options.ts)
- Upstream [LICENSE](https://github.com/zai-org/ZCode/blob/aac4755666d09fdcd70272fcf063c077a639015f/LICENSE) and [NOTICE.md](https://github.com/zai-org/ZCode/blob/aac4755666d09fdcd70272fcf063c077a639015f/NOTICE.md).

The distributed source diff includes a small excerpt of that source file. The
runtime compatibility helper contains the corresponding compiled matching
fragment. **Modification by HonghaoXiong, 2026:** remove the conversion from
`runtimeConfig.toolAllowlist` to `permission.allowedTools`, while retaining tool
visibility filtering. The helper applies this change only to a verified local
copy of the user's installed CLI. The installed application is not modified.
This change has not been represented as accepted by upstream.

Relevant upstream notice: ZCode's first-party source is licensed under
Apache-2.0; independent third-party components retain their own terms. The
permission patch here does not include those components or grant rights to
ZCode trademarks. No ZCode application, complete CLI bundle, native binary,
provider configuration, credential, or upstream runtime dependency is shipped
with this repository. Local execution still requires a separately obtained
ZCode installation subject to its applicable terms and notices.

## Prior project acknowledged

[AkiGarage/ZCode-supervisor](https://github.com/AkiGarage/ZCode-supervisor)
provided the earlier supervisor workflow evaluated during development. Its
[LICENSE](https://github.com/AkiGarage/ZCode-supervisor/blob/main/LICENSE) is MIT,
copyright 2026 ZCode-supervisor contributors (verified 2026-10-14).

This repository does not vendor that implementation, require its Python package,
or represent itself as its fork or a release of its package. Its name is
acknowledged here to distinguish the projects; our distribution name is
`codex-zcode-supervisor` and command is `czs`.
