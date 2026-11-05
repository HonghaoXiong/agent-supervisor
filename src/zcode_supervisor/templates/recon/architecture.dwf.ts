/* zcode-workflow
{
  "description": "Inspect current architecture sources and return a source-bound ArchitectureBrief/v1 report.",
  "args": {
    "requestJson": { "type": "string", "required": true,
      "description": "Canonical frozen TaskFrame/orientation/manifest JSON" }
  }
}
*/
interface ReconInput {
  frame: { goal: string; task_kinds: string[]; domains: string[] };
  task_frame_sha256: string;
  orientation: unknown;
  manifest: unknown;
}
interface ReconResult { briefJson: string }
const input = JSON.parse(String(args.requestJson)) as ReconInput;
log("Inspect authoritative documents, existing mechanisms, impacts, alternatives and dependencies.");
const result = await agent("recon-author", {
  system: "You are a read-only architecture investigator. Use only Read for source inspection; never use Bash, Write, Edit or other tools. Required source hashes and line ranges are already supplied in the frozen input: use them verbatim and do not recalculate or independently verify them with any tool. Locate current authoritative sources, inspect existing mechanisms and impacts, compare an initial approach with alternatives and costs, identify dependencies and checks, and return only ArchitectureBrief/v1 JSON in briefJson. Do not paste whole source files, do not claim acceptance, and do not invent capabilities."
}).ask<ReconResult>(`Return {briefJson: string}. briefJson must contain ONLY these exact top-level keys: schema_version, task_frame_sha256, summary, orientation, claims, options, impacted_symbols, checks, unknowns, capability_observations. Set schema_version="zsc.architecture-brief/v1" and task_frame_sha256="${input.task_frame_sha256}". summary and orientation are nonempty strings. claims has 1..64 entries with exactly id, statement, status, source_refs, rationale; status is observed|inferred|unknown. Every observed/inferred claim needs a source_refs entry with exactly path, sha256, start_line, end_line copied from the frozen manifest. options has 1..8 entries with exactly id, approach, benefits, costs, limits. impacted_symbols entries have exactly path, symbol, reason. checks has 1..32 entries with exactly id, question, method, expected_evidence, qualification, where qualification is code|numerical|physical|observation|operational. Never add where_not_run or any sixth check field; if a check was not run, say "Not run: ..." inside method. unknowns is an array of nonempty strings. capability_observations is [] unless supported by the supplied capabilities. Do not use architecture-brief/v1, task, sources, findings, observations, open_questions, not_verified, where_not_run or any other custom keys.\nFrozen input:\n${JSON.stringify(input)}`);
report(result);
return result;
