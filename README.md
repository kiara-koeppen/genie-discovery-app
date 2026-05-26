# Genie Space Discovery App

A structured workbook that guides an analyst + business owner pair through the full discovery needed to configure a Databricks Genie Space — from first conversation about pain points all the way through to a pushed, prototyped, benchmarked space. Workspace-portable: one `app.yaml` edit deploys it into any Databricks workspace.

---

## Disclaimer

This is **not an official Databricks product or supported solution**. It is a personal project shared as-is for reference and experimentation. It is **not intended for production use** and comes with **no warranty, support, or SLA** of any kind from Databricks or the author. Before deploying it in any environment that handles customer, regulated, or otherwise sensitive data, you are responsible for performing your own security review, legal / compliance review, and operational hardening. Any use is at your own risk.

---

## What this app does

The app is a 6-session workbook. Each session is a form backed by a Delta table so engagements persist across days/weeks. Below is the full capability surface, including the non-obvious behaviors that took iteration to get right.

### Session 1 — Business Context (Analyst + BO)
- Capture pain points, existing reports, business context Q&A.
- Free-text fields auto-expand; every table cell supports popup editing for long-form input.

### Session 2 — Questions & Vocabulary (Analyst + BO)
- Build the question bank (what does the BO actually want to ask?).
- Define vocabulary terms and their data meaning (synonyms, definitions).

### Pre-work Excel upload (optional shortcut for S1 + S2)

Sessions 1 and 2 can be pre-populated from an Excel template instead of typed live during the session:

- **"Download Template"** in the engagement header builds a fresh `.xlsx` workbook with sheets for Business Context Discovery, Pain Points, Existing Reports, Question Bank, and Vocabulary & Metrics. The analyst emails it to the BO before the kickoff call.
- **"Upload Pre-Work"** opens a modal that parses the BO's filled-in workbook server-side, shows a preview per section with per-section checkboxes, and lets the analyst pick which sections to apply.
- **Defense in depth on upload**: file-extension check, magic-byte check, size cap, and a whitelist of section keys + row keys per sheet so a crafted payload can't smuggle extra columns into the engagement row.
- **Atomic apply** under the engagement's optimistic lock — partial failures can't leave the engagement half-applied. Apply also advances `current_session` past the highest touched session so the next tab unlocks automatically.

### Session 3 — Technical Design (Analyst solo)

S3 is data-sources-first: pick the tables and metric views the space will use *before* implementing measures, so the rest of S3 (and S4's Data Plan) is grounded in real UC objects from the start.

- **Data Sources panel** (top of S3) — the single place the analyst declares what's in scope:
  - Inline warehouse picker (also propagates to Session 5).
  - **Bulk table-or-MV picker** with 3-level UC cascade (catalog → schema → table). Adds land as either "Table" or "Metric View" automatically, detected via the UC REST API (the SDK's `table_type` field misreports MVs as MANAGED on some versions, so the broad-scan path uses `/api/2.1/unity-catalog/tables` directly).
  - **Existing Metric Views discovery** — broad scan across all visible catalogs (`system.information_schema.tables`) finds every MV whose underlying tables overlap the analyst's scope. Each candidate shows owner, source tables, and (on expand) the actual `DESCRIBE EXTENDED` dimensions / measures so the analyst can decide whether to reuse it instead of re-authoring measures. Checkbox flows the MV into S4's Data Plan automatically.
  - Per-row Notes textarea (lands in `column_configs` description or table description at push).
  - Both tables and MVs flow into Session 4's Data Plan automatically; S4 is read-only with a redirect back to S3.
- **Classify Terms** — every business term captured in Session 2 appears here for classification (one term can have multiple types):
  - **Metric** → auto-creates a row in SQL Expressions for the analyst to fill in.
  - **Filter / Date Logic** → auto-creates a row in Text Instructions.
  - **Synonym** → routes via a sub-picker:
    - *"A column name"* — pushes to `column_configs.synonyms` on the column at S5 push.
    - *"A value in a column"* — pushes to `column_configs.description` and enables Entity Matching on the column (Genie's column_configs schema has no value-aliases field; description + entity-matching toggle is the supported mechanism).
    - *"A general space term"* — auto-populates the Text Instructions section; lands in the space's General Instructions at push.
- **Synonym Routing Summary** — a read-only panel that previews exactly what will be pushed where. Marks any incomplete routing (no column picked, FQN missing the column segment, table not in the Data Plan, value-kind missing a column value, no synonyms in S2 vocab) with a per-row `Won't push: <reason>` chip, excludes them from the "N column / N value / N general" counts, and shows an "X of Y will push at Session 5" banner with an alert listing the gaps to fix. No more silent drops at push time.
- **SQL Expressions** — reusable filter / dimension / measure snippets tied to UC tables. Pick a table via the inline cascade; the synonyms column auto-fills from the matching S2 vocabulary row when the term was classified as Metric.
- **Text Instructions** — analyst guidance that can't be expressed as SQL. Rows are auto-seeded by Classify Terms.
- **Data Gaps** — concepts the BO asked about that have no data home yet.
- **Scope Boundaries** — explicit "we are / are not covering X."
- **Global Filter** — a space-wide WHERE clause (e.g., `voided_flag = 'N'`) that flows through to the metric view YAML and plan.
- **Optional Metric View builder** —
  - Click "Draft YAML" and the app produces a Databricks-spec metric view YAML grounded in the actual UC column schemas of the source tables (DESCRIBE-driven, no hallucinated columns).
  - The YAML honors the global filter, detected PK/FK joins, and the analyst's Session 3 SQL expressions.
  - "Create Metric View" deploys it to UC under your OBO token (respects your grants, not the app SP's). If the FQN already exists the app returns ownership info rather than overwriting blindly. Created MVs auto-appear in the Existing Metric Views section above so the analyst can immediately check them.

### Session 4 — COE Review + Benchmarks (COE group)
- **Readiness Brief** — async LLM call (via the background-job runner, see *Cross-cutting design choices*) that synthesizes Sessions 1–3 + Data Plan into a citation-backed brief with coverage analysis and a gaps section. Auto-fires on first visit if S1–S3 has any content; gated on having at least one S1 response, S2 question, or S3 SQL expression so a brand-new empty engagement doesn't trigger an LLM call.
- **Analyst commentary** — per-gap responses preserved across regenerations via fuzzy-match.
- **Data Plan (read-only)** — the consolidated list of tables and metric views the space will use. **Edits happen in Session 3's Data Sources panel**, not here. S4 shows the plan with a banner pointing back to S3 so analysts don't try to add/remove things in the wrong place. A row per table/view with include=Yes/No and notes; notes flow into column descriptions or `column_configs` at push.
- **Reactive sync**: when a metric view is created or added in Session 3, it auto-appears in Session 4's data plan (no manual copy step).
- **Benchmark Questions** — the acceptance-test bank the space is measured against. Highlights:
  - **Draft N Benchmarks** with a count input (1–50). When existing benchmarks are present, a confirm dialog forces Replace vs. Append.
  - Under the hood the LLM brainstorms **N+10 candidates**, scores each on coverage / BO-phrasing / table coverage / realism, drops duplicates, and returns the top N in priority order.
  - **Draft All Expected SQL** — generates SQL for each question, schema-grounded from live UC DESCRIBE so column names can't be hallucinated. Includes an explicit Databricks SQL dialect-notes block (integer DATE_ADD, ADD_MONTHS, DATEDIFF(end, start), DOUBLE division) so the LLM doesn't emit Postgres syntax.
  - **Two-step SQL → summary flow** — the Measurement Summary is generated *from* the SQL in a second call, so the explanation always matches what the SQL actually does. A refresh button re-derives the summary from the current SQL.
  - **Run SQL inline** — per-row ▶ button and a Run All button execute each benchmark's SQL under the user's OBO token on a warehouse they pick. The runner polls for cold-warehouse startup (50s wait + 1.5s polling, 2-min outer deadline with auto-cancel). Results render in a small table right under the row; the sample result persists even when "Show Expected SQL" is toggled off.
  - SQL is always wrapped in `SELECT * FROM (...) __bm LIMIT N` so an inner `LIMIT` or `ORDER BY` doesn't break the run.
  - **BO approval checkbox** per benchmark — marks the row as validated and unlocks its use as a style exemplar in Session 5 (see below). Toggling this is the only mutation a BO-group user can perform on Session 4; the click is optimistic (UI flips immediately) and persists via a dedicated PATCH endpoint that lives outside the engagement's optimistic-lock so a BO clicking the checkbox can't 409 the analyst's autosave.
- **COE approval gating** — Sessions 5 & 6 are locked until a COE group member approves the engagement. Non-members can view but not approve; membership is checked under OBO so the app can't be tricked with a shared SP.

### Session 5 — Configure Genie Space (Analyst, gated)
This is where discovery becomes a real Genie Space configuration.

**Generate Plan** does all of the following in one click:

1. **Grounds the LLM in real UC schemas.** Before calling the model, it runs `DESCRIBE TABLE` under your OBO token on every in-scope raw table from Session 4 and injects the actual column lists into the prompt with a strict "do not invent columns" rule.
2. **Reads the live Metric View definition from UC.** For every MV in the data plan it runs `SHOW CREATE TABLE` under your OBO token. The LLM sees the real, current measures / dimensions / calcs / filters and is told **not to duplicate** any of them in `sql_measures` / `sql_dimensions` / `sql_filters`. Falls back to Session 3's stored YAML only if the UC fetch fails.
3. **Uses BO-approved benchmark SQL as style exemplars.** Any Session 4 benchmark with `bo_approved=true` flows into a gold-standard queries block. The LLM is told to mirror the style and structure (column qualification, date-arithmetic, grouping patterns) but *not* to copy the queries verbatim — benchmarks remain acceptance tests.
4. **Keeps benchmark questions out of the plan.** Sample questions and example queries that overlap with benchmarks (token-Jaccard ≥ 0.8) are stripped post-hoc; the count of strips is surfaced as a warning so the analyst sees what happened.
5. **Produces the plan** per Databricks best practices:
   - `general_instructions` — one consolidated bullet list, 15 bullets max. Scope, jargon→data mappings, formatting rules, clarification triggers. No metric restatements, no table semantics (those live in UC).
   - `sample_questions` — 5–8 reworded questions from the bank.
   - `sql_filters` / `sql_dimensions` / `sql_measures` — reusable snippets with short-table-name column qualification (`claims.initial_decision`, not bare `initial_decision` and not fully qualified). These are **supplementary** to whatever the MV already governs.
   - `example_queries` — 3–6 full SQL queries with fully-qualified table names; flagged `draft: true` so analyst reviews. Prefers MV references over raw-table joins when the MV can answer the question.
   - `narrative` — short plain-English summary of what got configured.
6. **Fetches joins deterministically.** UC PK/FK constraints between in-scope tables are pulled via the SDK (not LLM-inferred) and auto-seed the joins table.
7. **Analyst-editable joins** — UC-seeded rows are tagged "UC FK" (read-only); the analyst can click "Add manual join" to declare relationships that aren't in UC. Regenerate Plan refreshes UC joins but preserves manual rows.
8. **Push to Genie Space** — create-new or update-existing flow using the Genie REST API, authed OBO as the end user (so the user's `CAN MANAGE` on the space governs the push). The serialized space includes:
   - Tables + metric views from the Data Plan
   - General instructions, sample questions, SQL filters / dimensions / measures, example queries, joins
   - **`column_configs`** — auto-built from Session 3's Classify Terms routing. Column-kind synonyms land in `column_configs.synonyms`; value-kind synonyms land in `column_configs.description` with `enable_entity_matching=true`. Cross-cutting (general-space) synonyms flow through `general_instructions` via the S5 LLM plan.
   - Push honors the engagement's optimistic lock (`If-Match: <updated_at>`). If another writer has bumped the engagement since the page loaded, push returns 409 with a clear "refresh before pushing" message instead of silently overwriting the live space with stale data.

### Session 6 — Prototype Review (Analyst + BO, gated)
- Run through the benchmark questions against the live space.
- Scorecard, fixes log, phrasing notes for the BO to iterate on.

### Cross-cutting design choices

- **Group-based access model.** Three roles, evaluated on every request under OBO:
  - **Analyst (default).** Any authenticated app user can read all engagements and edit Sessions 1–6 except Session 4 COE approval. No email gating — discovery is a team activity.
  - **COE group member** (`COE_GROUP_NAME`). Same as analyst, plus the right to approve/reject Session 4. Sessions 5 & 6 are locked behind COE approval.
  - **BO group member** (`BO_GROUP_NAME`, optional). Tightly restricted: can read all engagements, edit Sessions 1 & 2, view Session 4, and toggle the BO Approved checkbox on benchmarks. Cannot edit S3/S5/S6 or trigger any AI button. The restriction is enforced in two layers: (1) the frontend hides tabs and AI controls; (2) a server-side `before_request` hook on `/api/engagements/<eid>/*` whitelists exactly those operations and 403s everything else, so a BO who bypasses the UI gets nothing.
  - COE wins over BO if a user is in both groups.
- **Async job runner for long LLM calls.** Databricks Apps' gateway has a ~60s timeout on synchronous HTTP, but the readiness brief, generate-plan, draft-MV-YAML, and draft-benchmark-SQL flows can each take 60–180s wall-clock. Every long LLM call is dispatched as a background job (`POST /api/jobs/start` with `task_type=...`); the frontend polls `/api/jobs/<job_id>` every 2s until the job reaches `done` or `failed`. State is persisted in a Delta table (`discovery_jobs`) with token-usage tracking in `discovery_llm_usage`.
- **Per-task LLM tunables.** Each task type has its own `max_tokens` (matched to expected output size) and model selection — precision-critical tasks (plan generation, MV YAML) use the strongest model; quick refinements (summary regeneration) use a faster, cheaper model. Prompt-length capped per task to avoid overruns. Failed LLM calls retry once with exponential backoff before surfacing the error.
- **Optimistic-lock with auto-retry on saves.** Every engagement mutation carries an `If-Match: <updated_at>` header. On 409 (stale token), the frontend silently fetches fresh `updated_at`, retries once. Only on a second 409 does it fall back to a "reloading…" toast. The dedicated BO-Approved PATCH endpoint intentionally lives **outside** this lock so BO clicks never conflict with analyst autosaves.
- **`sql_exec` distinguishes cold-warehouse from genuine empty results.** A SQL statement that doesn't reach a successful terminal state within the wait window raises `SqlTransientError` → HTTP 503, so the frontend can prompt the user to retry instead of misinterpreting a cold start as a 404.
- **OBO-first auth.** Anything that touches customer data — UC listings, warehouse picking, DESCRIBE, SHOW CREATE TABLE, SQL execution, Genie push — runs under the user's forwarded access token (`X-Forwarded-Access-Token`). The app's service principal only owns the engagement Delta table and LLM calls. This prevents the app from becoming a permissions-laundering vector.
- **`/api/warehouses` is OBO-only.** Users only see warehouses they actually have access to. If their token is missing the `sql` scope (incremental-consent drift), the endpoint returns 403 with `reauth_required: true` instead of silently falling back to the SP.
- **Schema-grounded prompting everywhere LLM writes SQL.** Benchmark SQL drafting, metric view YAML drafting, and plan generation all inject real UC column lists so the model cannot hallucinate.
- **Two-step LLM flows** for SQL + summary so the explanation always describes the actual code.
- **Delta-backed persistence** with a single engagement row; JSON columns per session. `ensure_table()` auto-creates the engagement table on first run and additively migrates the schema on every startup so pulling updates doesn't require manual SQL.
- **Popup editing + debounced autosave** on every table — you never lose work between clicks.
- **Floating section nav.** Sessions 3 / 4 / 5 have many accordions; the engagement page renders a left-side rail listing the sessions and an "IN THIS SESSION" sub-rail with every accordion in the current tab. Sub-section IDs (`section-3-data-sources`, `section-3-classify-terms`, ...) are defined in one place (`frontend/src/sessions/sectionConfig.ts`) so the rail and the Accordion `id`s stay in sync.

### Who calls what: OBO vs App Service Principal

The app runs as a service principal but deliberately routes most data-plane calls through the end user's OAuth-on-behalf-of (OBO) token (`X-Forwarded-Access-Token`). This keeps the SP's blast radius small and respects each user's UC grants.

| Operation | Identity | Why |
|---|---|---|
| Read/write the `discovery` engagement table (create, update, get, save) | **App SP** | The SP owns the Delta table so engagements are durable across users. Only place the SP touches customer-adjacent storage. |
| Auto-create / migrate the `discovery` table on startup (`ensure_table`) | **App SP** | SP needs `CREATE TABLE` on `<CATALOG>.<SCHEMA>`. |
| Call the Model Serving LLM endpoint (generate plan, draft SQL, draft MV YAML, summaries) | **App SP** | SP needs `CAN QUERY` on the endpoint. User tokens don't get billed for LLM time; SP does. |
| List SQL warehouses for the Session 5 dropdown (`/api/warehouses`) | **OBO** | Users should only see warehouses they have access to. Returns 403 `reauth_required` if the `sql` scope is missing. |
| UC picker: list catalogs / schemas / tables / columns (Session 3) | **OBO** | Each `/api/uc/*` endpoint calls `user_w.catalogs.list()` / `.schemas.list()` / `.tables.list()` / `.tables.get()`. The SP is never a fallback — if the user can't see it, the picker is empty. |
| `DESCRIBE TABLE` / `SHOW CREATE TABLE` (schema grounding, live MV fetch) | **OBO** | Runs via the user's chosen warehouse; inherits the user's UC SELECT/USE CATALOG grants. |
| `tables.get()` for PK/FK constraints (Session 5 joins seeding) | **OBO** | UC metadata API. Needs the `catalog.tables:read` user scope (see Step 2b). |
| List engagements / read / write / delete engagement (Sessions 1–6) | **App SP for DB, OBO for authz** | The SP writes to the `discovery` Delta table, but every `/api/engagements/<eid>` request is gated by a before-request hook that evaluates the caller's group membership (COE / BO / analyst) and applies the appropriate scope (full edit / S1–S2 + BO-Approved checkbox only / read-only on certain sections). Cross-user tampering is blocked at the API layer. |
| COE approval (Session 4 "Approve/Request Changes") | **OBO** | `/api/engagements/<eid>/coe-approve` rejects with 403 unless the caller is a live COE group member (not just hidden in the UI). |
| Execute benchmark SQL (Session 4 "Run" button) | **OBO** | User's query, user's warehouse, user's data grants. The app never runs arbitrary user-authored SQL under the SP. |
| Create / update / patch Genie Spaces (Session 5 "Push") | **OBO** | Uses the Genie REST API on the user's behalf so their `CAN MANAGE` on the space governs whether the push succeeds. The SP does not manage Genie spaces. |
| Create/replace UC Metric Views (Session 3 "Create MV") | **OBO** | User's grants govern whether they can create the view and whether it overwrites an existing one they don't own. |
| Check COE group membership (Session 4 approval gating) | **OBO** | Uses `current_user.me()` so membership is evaluated against the logged-in user. Keeps the SP out of IAM. |
| Resolve the current user's identity for audit fields | **OBO header** | Reads `X-Forwarded-Email` / `X-Forwarded-User` — set by Databricks Apps from the user's auth. Falls back to the SP identity only if the header is missing. |

Practical upshot: if a user can't see a table in the SQL editor, they can't pull its schema in this app. If they can't `CREATE TABLE` in the target catalog for the metric view, the Create MV button fails with their own UC error, not a fake SP success. The SP holds `CREATE TABLE` grants on the engagement catalog/schema and `CAN QUERY` on the LLM endpoint — nothing else.

---

## Architecture

```
Frontend (React + Vite + MUI)    Backend (Flask)             Storage / Services
-------------------------        -------------------         ---------------------
React SPA                  -->   REST API             -->    Unity Catalog (Delta)
  - 6 session forms                - CRUD + auth              `discovery` table
  - UC pickers                     - OBO passthrough       -->Warehouse (SQL exec)
  - Benchmark runner               - Prompt builders       -->Model Serving (LLM)
  - Join editor                    - Genie REST proxy      -->Genie REST API
  - Push to Genie                  - Databricks SDK
```

- **Frontend**: React 18, TypeScript, Vite, Material UI 5
- **Backend**: Flask, Databricks SDK (statement execution + workspace APIs)
- **LLM**: Model Serving endpoint (default `databricks-claude-haiku-4-5`, HIPAA-eligible on Azure). Switch to Sonnet for higher-precision outputs by setting `LLM_ENDPOINT_NAME` (see below).
- **Storage**: Single Delta table with JSON STRING columns per section
- **Deployment**: Databricks App (`app.yaml`)

## File structure

```
genie-discovery-app/
  app.py                         # Flask backend — all routes, prompt builders, SDK helpers
  app.yaml                       # Databricks App config (warehouse, catalog, schema, COE group, LLM)
  requirements.txt               # flask, databricks-sdk
  frontend/
    index.html
    package.json
    tsconfig.json
    vite.config.ts
    src/
      main.tsx
      App.tsx                    # Router
      api.ts                     # Typed fetch wrappers for every endpoint
      types.ts                   # Shared types
      theme.ts
      pages/
        Home.tsx                 # Engagement list + create (unique-name validation)
        Engagement.tsx           # Tabbed session view with COE-gated locks
      sessions/
        Session1Form.tsx
        Session2Form.tsx
        Session3Form.tsx         # Data Sources + Classify Terms + Routing Summary + SQL Expressions + MV builder
        Session4Form.tsx         # Read-only Data Plan + benchmark runner (draft N+10, run inline, BO approve)
        Session5Form.tsx         # Generate Plan + editable preview + joins + push to Genie
        Session6Form.tsx
        sectionConfig.ts         # Single source of truth for section IDs + labels in the floating nav
      components/
        EditableTable.tsx
        ExpandableTextField.tsx  # Long-form popup editor with autosize
        UCTablePicker.tsx
        UCColumnPicker.tsx
        DataSourcesPanel.tsx     # S3 top-of-page: tables/MVs picker, broad-scan MV discovery, notes
        PreworkUploadModal.tsx   # BO Excel upload → parse preview → atomic apply to S1+S2
        SectionToc.tsx           # Floating section nav (sessions rail + in-this-session sub-rail)
  static/                        # Vite build output (gitignored)
```

---

## Prerequisites

- Databricks workspace with Unity Catalog enabled
- A SQL Warehouse (Serverless recommended)
- A Databricks group to gate COE approval
- Databricks CLI configured with a profile pointing at the target workspace
- Node.js 18+ (for frontend builds)
- Python 3.10+

## Deploy to your workspace

### Step 1 — Pick your workspace resources

You need to decide six things before editing any config:

1. **SQL Warehouse ID** — In Databricks, go to SQL → SQL Warehouses → select your warehouse → Connection details. The ID is the trailing segment of the HTTP Path (e.g., `/sql/1.0/warehouses/<THIS_PART>`).
2. **Catalog** — Where the app should store engagement data. The app's service principal must have `CREATE TABLE` on this catalog/schema.
3. **Schema** — Under that catalog. The schema must already exist; the Delta tables inside it are auto-created on first run.
4. **COE group name** — Create a Databricks group (Account Console → User management → Groups) whose members are allowed to approve engagements in Session 4. Add your COE reviewers to it.
5. **BO group name (optional)** — Create a Databricks group whose members get the restricted business-owner view: read-only on most sections, edit Sessions 1 & 2, view Session 4, and toggle the BO Approved checkbox on benchmark rows. Leave blank to skip — without it, every authenticated user defaults to full analyst access (except COE approval).
6. **Model Serving endpoint** — The name of a chat-completion-compatible served model used by "Generate Plan", "Draft YAML", "Draft Benchmarks", "Draft All SQL", "Generate Brief", and the summary refresh. Defaults to `databricks-claude-haiku-4-5` (HIPAA-eligible, pay-per-token, on Azure). The app's service principal must have `CAN QUERY` on this endpoint. To switch to Sonnet 4.6 for higher-precision plan/SQL/YAML outputs, set `LLM_ENDPOINT_NAME` to `databricks-claude-sonnet-4-6` in `app.yaml`.

### Step 2 — Update `app.yaml`

Replace the `# CHANGE ME` values with your picks from Step 1:

```yaml
env:
  - name: DATABRICKS_WAREHOUSE_ID
    value: "<your-warehouse-id>"
  - name: CATALOG
    value: "<your-catalog>"
  - name: SCHEMA
    value: "<your-schema>"
  - name: COE_GROUP_NAME
    value: "<your-coe-group-name>"
  - name: BO_GROUP_NAME
    value: "<your-bo-group-name>"   # optional; leave empty to skip BO-restricted view
  - name: LLM_ENDPOINT_NAME
    value: "databricks-claude-haiku-4-5"   # default; use "databricks-claude-sonnet-4-6" for higher-precision plan/SQL/YAML
```

Everything else (UC catalog/schema/table picking, metric view detection, PK/FK join detection) resolves dynamically against whatever the app's service principal and the end user can see in your workspace.

**Permissions required on the app's service principal:**
- `CREATE TABLE` on `<CATALOG>.<SCHEMA>` (for engagement storage)
- `MODIFY` and `SELECT` on the auto-created `discovery_jobs` and `discovery_llm_usage` tables (the SP creates them; if you pre-create them yourself, grant these explicitly — without them, async-job state silently fails to persist)
- `CAN USE` on the SQL warehouse
- `CAN QUERY` on the Model Serving endpoint named in `LLM_ENDPOINT_NAME`

**Permissions required on each end user (not the SP):**
- Membership in the COE group (for Session 4 approval; non-members can view but not approve)
- (Optional) Membership in the BO group for the restricted Business-Owner view
- `CAN USE` on at least one SQL warehouse (required for benchmark runs and generate-plan schema grounding)
- `SELECT` / `BROWSE` on the UC tables they intend to reference
- `CAN MANAGE` on the target Genie Space (for Session 5 push)

**Prod pattern for Genie Spaces:** Have your ops team create each space ahead of time (owned by a service principal for durability), grant each analyst `CAN MANAGE`, then drop the space ID into Session 5. The "Create New Space" toggle in Session 5 is for dev/testing only.

### Step 2b — Configure user OAuth scopes (required)

Databricks Apps read the end user's OAuth scopes from a CLI-only setting — **this is not something `app.yaml` can set**, and without it the app will fail silently at runtime (the warehouse dropdown empties, schema grounding fails, Genie push returns 403). Run this once after first deploy:

```bash
databricks apps update genie-discovery --profile <profile> --json '{
  "name": "genie-discovery",
  "user_api_scopes": [
    "sql",
    "catalog.catalogs:read",
    "catalog.schemas:read",
    "catalog.tables:read",
    "dashboards.genie"
  ]
}'
```

> `iam.current-user:read` and `iam.access-control:read` are added to every app's effective scopes by the platform — you do not list them explicitly (the CLI rejects them). Only the app-specific scopes go in this list.

What each scope unlocks:
- `sql` — list warehouses, run `DESCRIBE TABLE` / `SHOW CREATE TABLE` for schema grounding, execute benchmark SQL.
- `catalog.catalogs:read` — list catalogs the user can see (Session 3 UC picker top-level).
- `catalog.schemas:read` — list schemas inside a catalog (Session 3 UC picker second level).
- `catalog.tables:read` — list tables, read columns via `tables.get()`, and read PK/FK constraints for the Session 5 joins section. Without this, most of Session 3 and the auto-seeded joins in Session 5 go blank.
- `dashboards.genie` — create/update Genie Spaces via the Genie REST API on the user's behalf.

> **Everything UC-related runs under the user's OBO token on purpose.** The service principal is not used as a fallback for UC reads — if a user lacks a grant, they'll see the error rather than silently inheriting SP permissions.

After updating scopes, existing users will see an OAuth re-consent prompt on next load. If a user reports the warehouse dropdown is empty and the app returns "reauth_required", have them sign out (or open the app URL in a private window) to trigger the new-scope consent flow.

### Step 3 — Build the frontend

```bash
cd frontend
npm install          # generates a fresh package-lock.json against your npm registry
npm run build        # outputs to ../static/
cd ..
```

> `frontend/package-lock.json` is intentionally gitignored — it's regenerated on first install so your build isn't tied to whichever npm registry the previous author used.

### Step 4 — Upload and deploy

Replace `<you>` with your workspace username and `<profile>` with your Databricks CLI profile name.

```bash
# 1. Create the workspace folder
databricks workspace mkdirs /Workspace/Users/<you>/genie-discovery-app --profile <profile>

# 2. Upload backend + config
databricks workspace import /Workspace/Users/<you>/genie-discovery-app/app.py \
  --file app.py --format AUTO --overwrite --profile <profile>
databricks workspace import /Workspace/Users/<you>/genie-discovery-app/app.yaml \
  --file app.yaml --format AUTO --overwrite --profile <profile>
databricks workspace import /Workspace/Users/<you>/genie-discovery-app/requirements.txt \
  --file requirements.txt --format AUTO --overwrite --profile <profile>

# 3. Upload frontend bundle
databricks workspace import /Workspace/Users/<you>/genie-discovery-app/static/index.html \
  --file static/index.html --format AUTO --overwrite --profile <profile>
# Replace <bundle-hash> with the actual filename from static/assets/
databricks workspace import /Workspace/Users/<you>/genie-discovery-app/static/assets/index-<bundle-hash>.js \
  --file static/assets/index-<bundle-hash>.js --format AUTO --overwrite --profile <profile>

# 4. Create the app (first time only)
databricks apps create genie-discovery --profile <profile>

# 5. Deploy
databricks apps deploy genie-discovery \
  --source-code-path /Workspace/Users/<you>/genie-discovery-app \
  --profile <profile>
```

> **Do not use `databricks workspace import-dir`.** It sweeps up `node_modules` and `.git`, causing deploy timeouts. Upload files individually as shown above.

### Step 5 — First-run sanity check

1. Open the app URL printed by the deploy command.
2. The backend's `ensure_table()` creates the engagement Delta table on first startup — confirm it shows up at `<CATALOG>.<SCHEMA>.discovery`.
3. Click "New Engagement" and verify catalogs from your workspace show up in the UC picker in Session 3.
4. Confirm a COE group member sees approval buttons in Session 4; non-members can view but not approve.
5. Open Session 4, click "Draft N Benchmarks", then "Draft All Expected SQL", then "Run All SQL" to verify the OBO + warehouse + LLM pipeline works end-to-end.

## Local development

```bash
# Frontend (auto-reload on :5173)
cd frontend
npm install
npm run dev

# Backend (:8000)
pip install -r requirements.txt
python app.py
```

## Configuration reference

All config lives in `app.yaml`:

| Variable | Description |
|---|---|
| `DATABRICKS_WAREHOUSE_ID` | SQL Warehouse used by the SP for engagement-table reads/writes |
| `CATALOG` | UC catalog for the engagement Delta table |
| `SCHEMA` | UC schema under `CATALOG` (must exist; table auto-created) |
| `COE_GROUP_NAME` | Databricks group whose members gate Session 4 approval |
| `BO_GROUP_NAME` | (Optional) Databricks group whose members get the restricted Business Owner view (S1/S2 edit, S4 view, BO Approved checkbox only). Leave blank to disable. |
| `LLM_ENDPOINT_NAME` | Model Serving endpoint (chat-completion-compatible) used by every AI button. Defaults to `databricks-claude-haiku-4-5`. Set to `databricks-claude-sonnet-4-6` if you want higher-precision plan/SQL/YAML output at the cost of slower responses. |
| `LLM_HTTP_TIMEOUT_SECONDS` | (Optional) HTTP read timeout in seconds for LLM calls. Defaults to `600`. Raise if the model serving endpoint is slow to return first byte on large prompts. |

The app auto-creates the engagement table and adds any missing Delta columns on startup via `ensure_table()`, so schema migrations happen transparently when you pull updates.

## Status

Functional today:

- All 6 session forms with autosave + popup text editing + bounded auto-retry on stale-token 409
- Floating section nav (sessions rail + in-this-session sub-rail) on every engagement page
- Pre-work Excel upload (BO template download + atomic apply to S1+S2 under optimistic lock)
- Group-based access: default analyst access, COE-restricted approval, optional BO-restricted view (S1/S2 + BO-Approved checkbox only)
- UC pickers, PK/FK join detection (with verbose logging for debugging), broad-scan metric view discovery across all visible catalogs
- Session 3 Data Sources panel: bulk add tables/MVs, inline warehouse picker, notes, MV reuse discovery with `DESCRIBE EXTENDED` previews
- Session 3 Classify Terms with synonym kind picker (column / value / general space) and Routing Summary panel that flags incomplete routings before push
- Session 4 Data Plan rendered read-only (edits flow back from S3's Data Sources panel)
- Async job runner for long LLM calls (avoids the gateway 60s sync timeout); per-task `max_tokens` and model selection; one-retry exponential backoff on transient model errors
- Cold-warehouse aware `sql_exec` (returns 503 with retry hint instead of fake 404)
- Readiness Brief (Session 4): citation-backed, gap analysis, gated on having S1–S3 content before auto-firing
- LLM-drafted metric view YAML (schema-grounded, Databricks-spec) + UC create flow (returns updated_at so post-create autosave doesn't 409)
- Benchmarks: N+10 draft-and-rank, schema-grounded SQL, dialect-aware prompt, inline SQL runner with cold-warehouse polling, two-step summary, Run All, BO approval (optimistic UI + dedicated PATCH endpoint outside the optimistic lock)
- COE gating on Sessions 5 & 6 (OBO-verified group membership)
- Generate Plan (Session 5): schema-grounded, MV-aware, benchmark-style-aware, strips benchmark overlaps, surfaces warnings
- Joins: UC PK/FK auto-seeded + analyst-editable manual joins, regenerate preserves manual rows
- Push to Genie Space: create-new and update-existing via REST API, OBO-authed, full instruction surface serialized (instructions + sample questions + SQL snippets + example queries + joins + `column_configs`); benchmarks land in the Benchmarks tab. Push honors If-Match optimistic lock — refuses to push stale data.

Pending:

- Surface UC column `COMMENT` proposals from Session 2 vocabulary (push definitions to where Genie actually reads them)
- Convert to Databricks Asset Bundle for one-command redeploy
