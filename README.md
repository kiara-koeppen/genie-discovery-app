# Genie Space Discovery App

A structured workbook that guides an analyst + business owner pair through the full discovery needed to configure a Databricks Genie Space — from first conversation about pain points all the way through to a pushed, prototyped, benchmarked space. Workspace-portable: one `app.yaml` edit deploys it into any Databricks workspace.

---

## What this app does

The app is a 6-session workbook. Each session is a form backed by a Delta table so engagements persist across days/weeks. Below is the full capability surface, including the non-obvious behaviors that took iteration to get right.

### Session 1 — Business Context (Analyst + BO)
- Capture pain points, existing reports, business context Q&A.
- Free-text fields auto-expand; every table cell supports popup editing for long-form input.

### Session 2 — Questions & Vocabulary (Analyst + BO)
- Build the question bank (what does the BO actually want to ask?).
- Define vocabulary terms and their data meaning (synonyms, definitions).

### Session 3 — Technical Design (Analyst solo)
- **SQL Expressions** — write reusable filter / dimension / measure snippets tied to UC tables. Analyst classifies type; UI uses 4-level cascading UC pickers (catalog → schema → table → column).
- **Text Instructions** — analyst guidance that can't be expressed as SQL.
- **Data Gaps** — capture concepts the BO asked about that have no data home yet.
- **Scope Boundaries** — explicit "we are / are not covering X."
- **Global Filter** — a space-wide WHERE clause (e.g., `region = 'North'`) that flows through to the metric view YAML and plan.
- **Optional Metric View builder** —
  - Click "Draft YAML" and the app produces a Databricks-spec metric view YAML grounded in the actual UC column schemas of the source tables (DESCRIBE-driven, no hallucinated columns).
  - The YAML honors the global filter, detected PK/FK joins, and the analyst's Session 3 SQL expressions.
  - "Create Metric View" deploys it to UC under your OBO token (respects your grants, not the app SP's). If the FQN already exists the app returns ownership info rather than overwriting blindly.

### Session 4 — COE Review + Benchmarks (COE group)
- **Analyst commentary + auto-summary** of Sessions 1–3.
- **Data plan** — the single source of truth for which tables and metric views will be in scope for the Genie Space. A row per table/view with include=Yes/No and notes.
- **Reactive sync**: when a metric view is created in Session 3, it auto-appears in Session 4's data plan (no manual copy step).
- **Benchmark Questions** — the acceptance-test bank the space is measured against. Highlights:
  - **Draft N Benchmarks** with a count input (1–50). When existing benchmarks are present, a confirm dialog forces Replace vs. Append.
  - Under the hood the LLM brainstorms **N+10 candidates**, scores each on coverage / BO-phrasing / table coverage / realism, drops duplicates, and returns the top N in priority order.
  - **Draft All Expected SQL** — generates SQL for each question, schema-grounded from live UC DESCRIBE so column names can't be hallucinated. Includes an explicit Databricks SQL dialect-notes block (integer DATE_ADD, ADD_MONTHS, DATEDIFF(end, start), DOUBLE division) so the LLM doesn't emit Postgres syntax.
  - **Two-step SQL → summary flow** — the Measurement Summary is generated *from* the SQL in a second call, so the explanation always matches what the SQL actually does. A refresh button re-derives the summary from the current SQL.
  - **Run SQL inline** — per-row ▶ button and a Run All button execute each benchmark's SQL under the user's OBO token on a warehouse they pick. The runner polls for cold-warehouse startup (50s wait + 1.5s polling, 2-min outer deadline with auto-cancel). Results render in a small table right under the row; the sample result persists even when "Show Expected SQL" is toggled off.
  - SQL is always wrapped in `SELECT * FROM (...) __bm LIMIT N` so an inner `LIMIT` or `ORDER BY` doesn't break the run.
  - **BO approval checkbox** per benchmark — marks the row as validated and unlocks its use as a style exemplar in Session 5 (see below).
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
8. **Push to Genie Space** — create-new or update-existing flow using the Genie REST API, authed OBO as the end user (so the user's `CAN MANAGE` on the space governs the push). All four instruction surfaces (instructions + sample questions + SQL snippets + example queries + joins) are serialized and pushed.

### Session 6 — Prototype Review (Analyst + BO, gated)
- Run through the benchmark questions against the live space.
- Scorecard, fixes log, phrasing notes for the BO to iterate on.

### Cross-cutting design choices

- **OBO-first auth.** Anything that touches customer data — UC listings, warehouse picking, DESCRIBE, SHOW CREATE TABLE, SQL execution, Genie push — runs under the user's forwarded access token (`X-Forwarded-Access-Token`). The app's service principal only owns the engagement Delta table and LLM calls. This prevents the app from becoming a permissions-laundering vector.
- **`/api/warehouses` is OBO-only.** Users only see warehouses they actually have access to. If their token is missing the `sql` scope (incremental-consent drift), the endpoint returns 403 with `reauth_required: true` instead of silently falling back to the SP.
- **Schema-grounded prompting everywhere LLM writes SQL.** Benchmark SQL drafting, metric view YAML drafting, and plan generation all inject real UC column lists so the model cannot hallucinate.
- **Two-step LLM flows** for SQL + summary so the explanation always describes the actual code.
- **Delta-backed persistence** with a single engagement row; JSON columns per session. `ensure_columns()` auto-migrates the schema on startup so pulling updates doesn't require manual SQL.
- **Popup editing + debounced autosave** on every table — you never lose work between clicks.

---

## Architecture

```
Frontend (React + Vite + MUI)    Backend (Flask)             Storage / Services
-------------------------        -------------------         ---------------------
React SPA                  -->   REST API             -->    Unity Catalog (Delta)
  - 6 session forms                - CRUD + auth              engagements table
  - UC pickers                     - OBO passthrough       -->Warehouse (SQL exec)
  - Benchmark runner               - Prompt builders       -->Model Serving (LLM)
  - Join editor                    - Genie REST proxy      -->Genie REST API
  - Push to Genie                  - Databricks SDK
```

- **Frontend**: React 18, TypeScript, Vite, Material UI 5
- **Backend**: Flask, Databricks SDK (statement execution + workspace APIs)
- **LLM**: Model Serving endpoint (default `databricks-claude-sonnet-4-6`, HIPAA-eligible on Azure)
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
        Session3Form.tsx         # Includes metric view builder (LLM YAML + UC create)
        Session4Form.tsx         # Includes benchmark runner (draft N+10, run inline, BO approve)
        Session5Form.tsx         # Generate Plan + editable preview + joins + push to Genie
        Session6Form.tsx
      components/
        EditableTable.tsx
        ExpandableTextField.tsx  # Long-form popup editor with autosize
        UCTablePicker.tsx
        UCColumnPicker.tsx
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

You need to decide five things before editing any config:

1. **SQL Warehouse ID** — In Databricks, go to SQL → SQL Warehouses → select your warehouse → Connection details. The ID is the trailing segment of the HTTP Path (e.g., `/sql/1.0/warehouses/<THIS_PART>`).
2. **Catalog** — Where the app should store engagement data. The app's service principal must have `CREATE TABLE` on this catalog/schema.
3. **Schema** — Under that catalog. The schema must already exist; the Delta table inside it is auto-created on first run.
4. **COE group name** — Create a Databricks group (Account Console → User management → Groups) whose members are allowed to approve engagements in Session 4. Add your COE reviewers to it.
5. **Model Serving endpoint** — The name of a chat-completion-compatible served model used by "Generate Plan", "Draft YAML", "Draft Benchmarks", "Draft All SQL", and the summary refresh. Defaults to `databricks-claude-sonnet-4-6` (HIPAA-eligible, pay-per-token, on Azure). The app's service principal must have `CAN QUERY` on this endpoint.

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
  - name: LLM_ENDPOINT_NAME
    value: "databricks-claude-sonnet-4-6"
```

Everything else (UC catalog/schema/table picking, metric view detection, PK/FK join detection) resolves dynamically against whatever the app's service principal and the end user can see in your workspace.

**Permissions required on the app's service principal:**
- `CREATE TABLE` on `<CATALOG>.<SCHEMA>` (for engagement storage)
- `CAN USE` on the SQL warehouse
- `CAN QUERY` on the Model Serving endpoint named in `LLM_ENDPOINT_NAME`

**Permissions required on each end user (not the SP):**
- Membership in the COE group (for Session 4 approval; non-members can view but not approve)
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
    "iam.current-user:read",
    "iam.access-control:read",
    "sql",
    "dashboards.genie"
  ]
}'
```

What each scope unlocks:
- `iam.current-user:read` — resolve the logged-in user for audit trails.
- `iam.access-control:read` — check COE group membership for Session 4 approval gating.
- `sql` — list warehouses, run `DESCRIBE TABLE` / `SHOW CREATE TABLE` for schema grounding, execute benchmark SQL.
- `dashboards.genie` — create/update Genie Spaces via the Genie REST API on the user's behalf.

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
2. The backend's `ensure_columns()` creates the engagement Delta table on first startup — confirm it shows up at `<CATALOG>.<SCHEMA>.engagements`.
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
| `LLM_ENDPOINT_NAME` | Model Serving endpoint (chat-completion-compatible) used by every AI button |

The app auto-adds any missing Delta columns on startup via `ensure_columns()`, so schema migrations happen transparently when you pull updates.

## Status

Functional today:

- All 6 session forms with autosave + popup text editing
- UC pickers, PK/FK join detection (with verbose logging for debugging), metric view discovery
- LLM-drafted metric view YAML (schema-grounded, Databricks-spec) + UC create flow
- Benchmarks: N+10 draft-and-rank, schema-grounded SQL, dialect-aware prompt, inline SQL runner with cold-warehouse polling, two-step summary, Run All, BO approval
- COE gating on Sessions 5 & 6 (OBO-verified group membership)
- Generate Plan (Session 5): schema-grounded, MV-aware, benchmark-style-aware, strips benchmark overlaps, surfaces warnings
- Joins: UC PK/FK auto-seeded + analyst-editable manual joins, regenerate preserves manual rows
- Push to Genie Space: create-new and update-existing via REST API, OBO-authed, all four instruction surfaces serialized

Pending:

- Surface UC column `COMMENT` proposals from Session 2 vocabulary (push definitions to where Genie actually reads them)
- Convert to Databricks Asset Bundle for one-command redeploy
