# Change Requests — 2026-06-10 (Intermountain / Kiara)

Working scratchpad so work survives session/power interruption. Each item has
status + notes. Work on a feature branch, commit per finished item, redeploy to
kk_test for smoke, customer redeploys from main when ready.

Baseline at start: main @ f6fd85e, clean tree. Last kk_test bundle index-gPkGblZl.js.

## Items

1. [ ] **Export Benchmarks** — add "Benchmarks" as an exportable section in the
   Export modal. Export everything per benchmark: benchmark question, expected
   SQL, sample result, measurement summary (plain English). Currently export is
   S1/S2 only (`_build_prework_export` / PreworkExportModal). Benchmarks live in
   S4. Export-only (NOT round-trippable like S1/S2) is fine — separate sheet.

2. [ ] **S3 Genie clarifying/follow-up questions** — let analyst configure Genie
   to ask a disambiguation question (e.g. ask "service lines" -> "clinical or
   financial service line?"). NEEDS RESEARCH: how Genie supports clarifying
   questions (likely general_instructions text, or a dedicated instruction
   surface). Confirm mechanism before building.

3. [ ] **Rename S4 title** "COE Review" -> "Design, Review, & Approval".
   TITLE/LABEL ONLY, nowhere else. Watch DUPLICATE SESSION_LABELS:
   Engagement.tsx vs sectionConfig.ts — update both display spots, no logic.

4. [ ] **WHERE clause vs SELECT for SQL Expressions (S3)** — user question:
   sometimes a metric's SQL is a WHERE clause not a SELECT. Investigate the
   metric view YAML builder / sql_code usage and answer whether it works; if
   not, support filter-style expressions (relates to Filter/Date Logic class).

5. [ ] **Define "Type(s)" in S2 too** — term classification type (currently only
   in S3 Classify Terms) should ALSO be settable in S2 vocabulary. Additive.

6. [ ] **!! PRIORITY: Metric View YAML failing !!** — generated YAML produces
   wrong expressions and fails to write to UC. User expected us to validate
   syntax before suggesting. Investigate generation prompt + add REAL validation
   (dry-run create / syntax check via execute_sql) before presenting AND before
   push. This is the must-fix.

7. [ ] **S3 "Example SQL Query" routing option** — let analyst define an example
   SQL query directly (instead of metric/synonym/etc). Maps to Genie example
   queries / SQL instructions. NEEDS RESEARCH on Genie serialized_space shape
   for example queries (see feedback_genie_serialized_space_shape).

8. [ ] **S3 restrict UC pickers to selected Data Sources** — after Data Sources
   are chosen at top of S3, filter the catalog/schema/table pickers below to
   only those sources so users don't hunt. Additive UX.

## DECISIONS (2026-06-10)
- #6 validation = **scratch create + drop** (create throwaway MV in target schema
  via OBO, capture success/exact Spark error, DROP it). Plus deterministic
  structural lint at draft. Auto-fix retry feeds errors back to LLM.
- Sequencing = **all nine in order**, committing each item on branch
  feature/intermountain-2026-06-10. Power is flickering -> commit frequently.

## Progress log (append as we go)
- [x] Scratchpad + branch created (commit on feature/intermountain-2026-06-10)

## Notes / constraints
- Deploy via individual `databricks workspace import` (RAW). NEVER import-dir.
- Keep schema changes additive (new cols). Preserve optimistic-lock contract.
- Test on kk_test `genie-discovery` before any customer redeploy.
- Customer (Intermountain) production NOT yet redeployed from main.
