# Cold / resident capability matrix

Status vocabulary: **offline-tested** means a deterministic fixture or unit test passed;
**live-verified (scoped)** means the exact capability and task shape named in the row has a
real worker/model receipt. A scoped result is not a claim for every resident lifecycle.
Startup latency is not whole-task latency.

| Capability | Cold | Resident | Evidence / current boundary |
| --- | --- | --- | --- |
| Non-orchestration Read/Write task | live-verified (scoped) | live-verified (scoped) | `followup-resident-cold.json` and `followup-resident-live.json`: identical answer, complete evidence, one provider request and 4024 main-completion tokens each. |
| Session/turn approval ownership | offline-tested; simple live task passed | offline-tested; simple live task passed | The live pair exercises approval/evidence on one non-orchestration task only. |
| Model identity | live-verified (scoped) | live-verified (scoped) | Both live manifests bind the same requested and effective provider/model; tier names are never used as identity. |
| Cancel / restart / attach | offline-tested | offline-tested | Not covered by the live pair; explicit lifecycle evidence is still required. |
| Candidate isolation and child reads | offline-tested | offline-tested | The live pair has no child worker and makes no multi-candidate isolation claim. |
| Skill loading | supported subject to strict preflight | **unsupported** | `hub_server.py` rejects skills on the resident path because resident packets reject orchestration. |
| Native CreateWorkflow / orchestration | live-verified for inline and packaged path template | **unsupported** | `followup-native-live.json` and `followup-template-live.json`; `zcode_resident.py` still rejects orchestration. |
| Mode migration of an active session | not allowed | not allowed | `active-session-mode-immutable`; a new session and fresh qualification are required. |
| Strict provider token/cost budget | unsupported; strict request fails closed | unsupported; strict request fails closed | Public goal RPCs reject/omit `tokenBudget`; no provider monetary-cost field was observed. `followup-provider-budget.json`. |

`choose_execution_mode` therefore selects cold for Skill/Workflow work while the native
resident runtime retains its rejection. Resident can be selected only with equivalent host
qualification and measured whole-task evidence (or an explicit qualified request); a fast
startup alone is insufficient.

The single cold/resident pair measured 8.040254 s versus 7.379395 s. This is one scoped
observation, not a statistically stable speedup claim. Main-completion token counts are
observed-only; team tokens and monetary cost remain unknown.
