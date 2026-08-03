# Genie Space Discovery App

A structured, deterministic workbook that guides an analyst + business owner pair through the discovery needed to configure a Databricks Genie Space — from the first conversation about pain points and key terms, through the technical design, to a COE review that marks the engagement Ready for Pilot. Metric views and the Genie space itself are built directly in Databricks; this app captures the discovery that feeds them. Workspace-portable: one `app.yaml` edit deploys it into any Databricks workspace.

---

## Disclaimer

This is **not an official Databricks product or supported solution**. It is a personal project shared as-is for reference and experimentation. It is **not intended for production use** and comes with **no warranty, support, or SLA** of any kind from Databricks or the author. Before deploying it in any environment that handles customer, regulated, or otherwise sensitive data, you are responsible for performing your own security review, legal / compliance review, and operational hardening. Any use is at your own risk.

---

## What this app does

The app is a **4-session workbook**. Each session is a form backed by a Delta table so engagements persist across days/weeks. It is deliberately **deterministic — there are no AI/LLM features**. Metric views and Genie spaces are built directly in Databricks (Genie Code + the metric-view UI) with the Data Architect, using the discovery captured here as input.

### Session 1 — Business Context (Analyst + BO)
- Capture pain points, existing reports, and business context Q&A.
- Free-text fields auto-expand; every table cell supports popup editing for long-form input.
- Engagement metadata — Genie Space name, business owner, analyst, and the **ServiceNow ticket URL** — is edited via the pencil **Edit Engagement Info** dialog next to the title (not inside a session). When set, the ServiceNow link renders under the engagement title on every tab.

### Session 2 — Key Terms & Metrics + Questions (Analyst + BO)
- **Key Terms & Metrics first**: define vocabulary and metrics precisely (synonyms, definitions, calculations) so the Data Architect can carry synonyms into the metric view. An optional **Type** column (Metric / Synonym / Filter / Date Logic) annotates each term.
- **Question Bank second**: the questions the BO's team needs answered. Each question carries a **Type** flag — **Benchmark** (foundational acceptance questions, kept simple), **Testing** (exercise the space during MVP development), **Out of scope** (the space intentionally won't answer — these drive the general text instructions), or **Clarifying** (Genie should ask back on ambiguous requests).

### Pre-work Excel upload (optional shortcut for S1 + S2)
Sessions 1 and 2 can be pre-populated from an Excel template instead of typed live:
- **"Download Template"** builds a fresh `.xlsx` with sheets for Business Context, Pain Points, Existing Reports, Key Terms & Metrics, and Question Bank (with a Type dropdown). The analyst emails it to the BO before kickoff.
- **"Upload Pre-Work"** parses the filled-in workbook server-side, previews each section, and lets the analyst pick which sections to apply.
- **Defense in depth on upload**: extension check, magic-byte check, size cap, and a per-sheet whitelist of section + row keys.
- **Atomic apply** under the engagement's optimistic lock; advances `current_session` so the next tab unlocks.

### Session 3 — Technical Design (Analyst solo)
Slimmed to what the analyst authors before the space/metric view is built in Databricks:
- **Data Sources panel** — pick the tables/views the space will use (3-level UC cascade; Table vs Metric View auto-detected via the UC REST API), with an inline warehouse picker. The app discovers existing Metric Views built on those tables (broad scan across visible catalogs) so the team can reuse them; each candidate shows owner, source tables, and its `DESCRIBE`-driven dimensions/measures. Visible to everyone.
- **Reference: Sessions 1 & 2** — read-only recap of the question bank (with types), key terms, and existing reports while designing.
- **Global Filter** — a free-text comment for the Data Architect describing any filter that should apply to every question (e.g. "always filter clinical_contact to ED encounters"). Not executed by the app.
- **Text Instructions** — general guidance for how the space should behave, including how to handle out-of-scope questions.
- **Data Gap Analysis** — questions the data can't fully support yet, with proposed resolutions.
- **Scope Boundaries** — what the space will and won't cover (aligns with the Out-of-scope questions from S2).

### Session 4 — COE Review (COE group)
The single final gate before the engagement moves from dev to production and into phased piloting (piloting happens outside the app).
- **Analyst "Mark Ready for COE Review"** flips the engagement to `ready_for_review` and notifies the COE (optional Microsoft Teams card). Available to analysts (not BO-only) on a pending or changes-requested engagement; doubles as re-submit.
- **COE members Approve or Request Changes** with notes. **Approve marks the engagement `ready_for_pilot`** ("Ready for Pilot"); **Request Changes** returns it to `in_progress`. Server-enforced COE-group-only, checked under OBO.

### Cross-cutting design choices
- **Group-based access model.** Three roles, evaluated per request under OBO:
  - **Analyst (default).** Read all engagements, edit Sessions 1–3, submit for COE review. No email gating — discovery is a team activity.
  - **COE group member** (`COE_GROUP_NAME`). Plus Approve / Request-changes on Session 4. COE approval sets the terminal `ready_for_pilot` state.
  - **BO group member** (`BO_GROUP_NAME`, optional). Read all engagements, edit Sessions 1 & 2, view the Session 4 review read-only; cannot edit Session 3 or engagement metadata. Enforced in two layers — the frontend hides tabs, and a server-side `before_request` hook whitelists exactly the BO-allowed operations and 403s the rest.
  - COE wins over BO if a user is in both groups.
- **Deterministic only — no LLM.** All content is analyst-authored; there is no model-serving dependency and no background-job runner. Metric views and Genie spaces are built in Databricks.
- **Optimistic-lock with auto-retry on saves.** Every session mutation carries `If-Match: <updated_at>`; on a 409 the frontend refreshes the token and retries once before falling back to a reload.
- **Lock-free side-write.** The "Mark Ready for COE Review" flip (`/request-review`) lives outside the optimistic lock and doesn't bump `updated_at`, so it can't race the analyst's autosave. The COE approval bumps `updated_at`; the client reloads afterward to pick up the new token, reviewer, and status.
- **Unified status chip** on the engagement header and home cards, computed identically in `Engagement.tsx` (`topStatusChip`) and `Home.tsx` (`engagementStatusChip`): `Draft` → `In Progress` → `Ready for COE Review` → `Changes Requested` → **`Ready for Pilot`** (COE approved). The list endpoint returns `coe_approval_status` so home cards can apply it.
- **Export to `.xlsx` or `.csv`.** The Export modal writes Sessions 1 & 2 as either a re-uploadable `.xlsx` (matches the pre-work template) or a flat `.csv` for loading into Genie Code.
- **Optional Microsoft Teams notification** when an engagement is marked Ready for COE Review (best-effort; skipped if `TEAMS_COE_WEBHOOK_URL` is unset).
- **OBO-first auth.** UC listings, warehouse picking, `DESCRIBE`, and metric-view discovery run under the user's forwarded access token (`X-Forwarded-Access-Token`); the app's service principal only owns the engagement Delta table.
- **`sql_exec` distinguishes cold-warehouse from genuine empty results** (`SqlTransientError` → HTTP 503) so the UI can prompt a retry instead of misreading a cold start as a 404.
- **Delta-backed persistence** with a single engagement row and JSON columns per session. `ensure_table()` auto-creates and **additively** migrates the schema on startup, so engagements created by the previous version keep loading after this change — removed columns are left dormant, never dropped.
- **Popup editing + debounced autosave** on every table.
- **Floating section nav.** The left rail lists sessions and the accordions in the current tab; sub-section IDs live in `frontend/src/sessions/sectionConfig.ts` so the rail and the Accordion `id`s stay in sync.

### Who calls what: OBO vs App Service Principal

The app runs as a service principal but deliberately routes every data-plane call through the end user's OAuth-on-behalf-of (OBO) token (`X-Forwarded-Access-Token`). This keeps the SP's blast radius small and respects each user's UC grants.

| Operation | Identity | Why |
|---|---|---|
| Read/write the `discovery` engagement table (create, update, get, save) | **App SP** | The SP owns the Delta table so engagements are durable across users. Only place the SP touches customer-adjacent storage. |
| Auto-create / migrate the `discovery` table on startup (`ensure_table`) | **App SP** | SP needs `CREATE TABLE` on `<CATALOG>.<SCHEMA>`. |
| List SQL warehouses for the Data Sources picker (`/api/warehouses`) | **OBO** | Users should only see warehouses they have access to. Returns 403 `reauth_required` if the `sql` scope is missing. |
| UC picker: list catalogs / schemas / tables / columns (Session 3) | **OBO** | Each `/api/uc/*` endpoint calls `user_w.catalogs.list()` / `.schemas.list()` / `.tables.list()` / `.tables.get()`. The SP is never a fallback — if the user can't see it, the picker is empty. |
| `DESCRIBE`/metric-view discovery for the Data Sources panel | **OBO** | Runs via the user's chosen warehouse; inherits the user's UC SELECT/USE CATALOG grants. |
| List engagements / read / write / delete engagement (Sessions 1–4) | **App SP for DB, OBO for authz** | The SP writes to the `discovery` Delta table, but every `/api/engagements/<eid>` request is gated by a before-request hook that evaluates the caller's group membership (COE / BO / analyst) and applies the appropriate scope (full edit / S1–S2 only / read-only review). Cross-user tampering is blocked at the API layer. |
| COE approval (Session 4 "Approve/Request Changes") | **OBO** | `/api/engagements/<eid>/coe-approve` rejects with 403 unless the caller is a live COE group member (not just hidden in the UI). Approve sets the terminal `ready_for_pilot` status. |
| Edit engagement metadata (incl. ServiceNow URL) | **App SP for DB, OBO for authz** | Saved from the Edit Engagement Info dialog via `PUT /api/engagements/<eid>`, under the optimistic lock. Hidden from BO-only users. |
| Mark Ready for COE Review (Session 4) | **App SP for DB, OBO for authz** | `PUT /api/engagements/<eid>/request-review` sets `coe_approval_status = ready_for_review`. Open to analysts (not BO-only — blocked by the before-request whitelist). Lock-free; only ever sets the review state. |
| Teams "Ready for COE Review" notification | **No Databricks identity** | Best-effort POST of an Adaptive Card to the `TEAMS_COE_WEBHOOK_URL` channel webhook. Skipped if unset; wrapped so a failure never blocks the status flip. |
| Check COE group membership (Session 4 approval gating) | **OBO** | Uses `current_user.me()` so membership is evaluated against the logged-in user. Keeps the SP out of IAM. |
| Resolve the current user's identity for audit fields | **OBO header** | Reads `X-Forwarded-Email` / `X-Forwarded-User` — set by Databricks Apps from the user's auth. Falls back to the SP identity only if the header is missing. |

Practical upshot: if a user can't see a table in the SQL editor, they can't pull its schema in this app. The SP holds `CREATE TABLE` grants on the engagement catalog/schema — nothing else. There is no Model Serving dependency.

---

## Architecture

```
Frontend (React + Vite + MUI)    Backend (Flask)             Storage / Services
-------------------------        -------------------         ---------------------
React SPA                  -->   REST API             -->    Unity Catalog (Delta)
  - 4 session forms                - CRUD + auth              `discovery` table
  - UC pickers                     - OBO passthrough       -->Warehouse (SQL exec)
  - Data Sources panel             - Databricks SDK
```

- **Frontend**: React 19, TypeScript, Vite, Material UI 6
- **Backend**: Flask, Databricks SDK (statement execution + workspace APIs)
- **Storage**: Single Delta table with JSON STRING columns per section
- **Deployment**: Databricks App (`app.yaml`)
- **No LLM / Model Serving dependency** — the app is fully deterministic.

## File structure

```
genie-discovery-app/
  app.py                         # Flask backend — all routes + SDK helpers (no LLM)
  app.yaml                       # Databricks App config (warehouse, catalog, schema, groups)
  requirements.txt               # flask, databricks-sdk, requests, PyYAML, openpyxl
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
        Engagement.tsx           # Tabbed 4-session view
      sessions/
        Session1Form.tsx         # Business Context, Pain Points, Existing Reports
        Session2Form.tsx         # Key Terms & Metrics, then typed Question Bank
        Session3Form.tsx         # Data Sources + Reference + Global Filter + Text Instructions + Gaps + Scope
        Session4Form.tsx         # COE review gate (request review / approve / request changes)
        sectionConfig.ts         # Single source of truth for section IDs + labels in the floating nav
      components/
        EditableTable.tsx
        ExpandableTextField.tsx  # Long-form popup editor with autosize
        UCTablePicker.tsx
        UCColumnPicker.tsx
        DataSourcesPanel.tsx     # S3 top-of-page: tables/MVs picker, broad-scan MV discovery, notes
        PreworkUploadModal.tsx   # BO Excel upload → parse preview → atomic apply to S1+S2
        PreworkExportModal.tsx   # Export S1/S2 to .xlsx (re-uploadable) or .csv (Genie Code)
        ConfirmDialog.tsx        # Reusable confirm modal
        CompareRestoreDialog.tsx # Side-by-side current-vs-previous compare
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

You need to decide five things before editing any config:

1. **SQL Warehouse ID** — In Databricks, go to SQL → SQL Warehouses → select your warehouse → Connection details. The ID is the trailing segment of the HTTP Path (e.g., `/sql/1.0/warehouses/<THIS_PART>`).
2. **Catalog** — Where the app should store engagement data. The app's service principal must have `CREATE TABLE` on this catalog/schema.
3. **Schema** — Under that catalog. The schema must already exist; the Delta table inside it is auto-created on first run.
4. **COE group name** — Create a Databricks group (Account Console → User management → Groups) whose members are allowed to approve engagements in Session 4. Add your COE reviewers to it.
5. **BO group name (optional)** — Create a Databricks group whose members get the restricted business-owner view: edit Sessions 1 & 2 and view the Session 4 COE review read-only. Leave blank to skip — without it, every authenticated user defaults to full analyst access (except COE approval).

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
  - name: TEAMS_COE_WEBHOOK_URL
    value: ""   # optional; Teams channel webhook for "Ready for COE Review" notifications. Leave empty to disable.
```

Everything else (UC catalog/schema/table picking, metric view detection) resolves dynamically against whatever the app's service principal and the end user can see in your workspace.

**Permissions required on the app's service principal:**
- `CREATE TABLE` on `<CATALOG>.<SCHEMA>` (for engagement storage)
- `CAN USE` on the SQL warehouse

**Permissions required on each end user (not the SP):**
- Membership in the COE group (for Session 4 approval; non-members can view but not approve)
- (Optional) Membership in the BO group for the restricted Business-Owner view
- `CAN USE` on at least one SQL warehouse (used by the Session 3 Data Sources panel for metric-view discovery)
- `SELECT` / `BROWSE` on the UC tables they intend to reference

### Step 2b — Configure user OAuth scopes (required)

Databricks Apps read the end user's OAuth scopes from a CLI-only setting — **this is not something `app.yaml` can set**, and without it the app will fail silently at runtime (the warehouse dropdown empties, the UC pickers go blank). Run this once after first deploy:

```bash
databricks apps update genie-discovery --profile <profile> --json '{
  "name": "genie-discovery",
  "user_api_scopes": [
    "sql",
    "catalog.catalogs:read",
    "catalog.schemas:read",
    "catalog.tables:read"
  ]
}'
```

> `iam.current-user:read` and `iam.access-control:read` are added to every app's effective scopes by the platform — you do not list them explicitly (the CLI rejects them). Only the app-specific scopes go in this list.

What each scope unlocks:
- `sql` — list warehouses and run the `DESCRIBE`/metric-view-discovery queries behind the Data Sources panel.
- `catalog.catalogs:read` — list catalogs the user can see (Session 3 UC picker top-level).
- `catalog.schemas:read` — list schemas inside a catalog (Session 3 UC picker second level).
- `catalog.tables:read` — list tables and read columns via `tables.get()` for the Session 3 Data Sources panel. Without this, most of Session 3 goes blank.

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

# 2. Upload everything, including the frontend build.
#    --include static is REQUIRED: static/ is gitignored, and `sync` respects
#    .gitignore, so without this flag the compiled frontend is silently skipped
#    and the app keeps serving the previously deployed bundle.
databricks sync --include static . \
  /Workspace/Users/<you>/genie-discovery-app --profile <profile>

# 3. Create the app (first time only)
databricks apps create genie-discovery --profile <profile>

# 4. Deploy
databricks apps deploy genie-discovery \
  --source-code-path /Workspace/Users/<you>/genie-discovery-app \
  --profile <profile>
```

> **`--include static` is the step everyone misses.** Because `static/` is gitignored, a plain
> `databricks sync` uploads the backend and skips the frontend without any error — the app comes
> back up still serving the old UI. This is the single most common cause of "I redeployed but
> nothing changed."

> **Do not use `databricks workspace import-dir`.** It sweeps up `node_modules` and `.git`,
> causing deploy timeouts, and it overwrites the workspace `app.yaml` with the placeholder
> version from this repo — wiping your real warehouse ID, catalog, and schema.

> `databricks apps deploy` reads the **workspace** folder, not your local disk. Step 2 is what
> moves your build; step 4 only promotes what is already in the workspace.

**Redeploying later, or troubleshooting a redeploy that appears to do nothing?** See
[DEPLOY.md](DEPLOY.md) — it covers the content-hashed bundle filenames, stale-bundle cleanup, and
the exact commands to verify a deploy actually landed.

### Step 5 — First-run sanity check

1. Open the app URL printed by the deploy command.
2. The backend's `ensure_table()` creates the engagement Delta table on first startup — confirm it shows up at `<CATALOG>.<SCHEMA>.discovery`.
3. Click "New Engagement" and verify catalogs from your workspace show up in the UC picker in Session 3.
4. Confirm a COE group member sees Approve / Request Changes in Session 4; non-members can view but not approve.
5. In Session 3, open the Data Sources panel, pick a table, and confirm existing metric views for that table are discovered (verifies the OBO + warehouse path).
6. Approve an engagement in Session 4 and confirm the status chip reads **Ready for Pilot**.

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
| `BO_GROUP_NAME` | (Optional) Databricks group whose members get the restricted Business Owner view (S1/S2 edit, S4 review read-only). Leave blank to disable. |
| `TEAMS_COE_WEBHOOK_URL` | (Optional) Microsoft Teams channel webhook (Power Automate "Workflows" or legacy O365 connector). When set, the app posts an Adaptive Card to that channel each time an analyst marks an engagement "Ready for COE Review". Leave empty to disable. Best practice: store the URL in a Databricks secret and reference it here via `valueFrom`. |

The app auto-creates the engagement table and adds any missing Delta columns on startup via `ensure_table()`, so schema migrations happen transparently when you pull updates.

## Status

This is the **deterministic v2** of the app: all AI/LLM features were removed (the previous AI-heavy version is preserved on the `ai_features` branch). Functional today:

- 4 session forms with autosave + popup text editing + bounded auto-retry on stale-token 409
- Floating section nav (sessions rail + in-this-session sub-rail) on every engagement page
- Pre-work Excel upload (BO template download + atomic apply to S1+S2 under optimistic lock)
- Group-based access: default analyst access, COE-restricted approval, optional BO-restricted view (S1/S2 edit + S4 review read-only)
- Session 2: Key Terms & Metrics first, then a Question Bank where every question is flagged Benchmark / Testing / Out of scope / Clarifying
- Session 3 Data Sources panel: bulk add tables/MVs, inline warehouse picker, notes, and metric-view reuse discovery across visible catalogs with `DESCRIBE`-driven previews; plus Global Filter (comment), Text Instructions, Data Gap Analysis, Scope Boundaries, and a read-only Sessions 1 & 2 reference
- Session 4 COE review gate: analyst "Mark Ready for COE Review" (lock-free `/request-review`, optional Teams notification); COE Approve → **Ready for Pilot**, Request Changes → In Progress
- Unified status chip on the engagement header and home cards following the COE flow (Draft → In Progress → Ready for COE Review → Changes Requested → **Ready for Pilot**)
- ServiceNow ticket URL edited in the Edit Engagement Info dialog and shown as a link under the title
- Export Sessions 1 & 2 to `.xlsx` (re-uploadable) or `.csv` (for Genie Code)
- Cold-warehouse aware `sql_exec` (returns 503 with retry hint instead of a fake 404)
- Delta-backed persistence; `ensure_table()` additively migrates on startup, so engagements from the previous version keep loading

Pending:

- Confirm/iterate the `.csv` export layout against a real Genie Code upload
- Convert to a Databricks Asset Bundle for one-command redeploy
