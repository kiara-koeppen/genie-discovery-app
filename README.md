# Genie Space Discovery App

A structured workbook tool that guides data analysts through discovery sessions with business owners to gather all the requirements needed to configure a Databricks Genie Space. Built for Intermountain Health's train-the-trainer program.

## Overview

The app walks analyst-business owner pairs through 6 sessions:

1. **Business Context** (Analyst + BO) -- pain points, existing reports, business context Q&A
2. **Questions & Vocabulary** (Analyst + BO) -- question bank and vocabulary terms
3. **Technical Design** (Analyst solo) -- classify terms, write SQL expressions, text instructions, data gaps, scope boundaries, optional metric view YAML
4. **COE Review** (COE group) -- analyst commentary, auto-summary of Sessions 1-3, data plan, approve/request changes (gates Sessions 5 & 6)
5. **Configure Genie Space** (Analyst) -- locked until COE approval; select Space, config preview
6. **Prototype Review** (Analyst + BO) -- locked until COE approval; scorecard, fixes log, benchmarks, phrasing notes

Each engagement is persisted to a Delta table, allowing multiple sessions over time with save/resume.

## Architecture

```
Frontend (React + Vite + MUI)    Backend (Flask)         Storage (Delta)
-------------------------        ---------------         ---------------
React SPA                  -->   REST API          -->   Unity Catalog
  - Session forms                  - CRUD endpoints       Delta table
  - UC metadata pickers            - UC metadata proxy     (JSON columns)
  - Editable tables                - Session saves
  - COE gating                     - COE group check
```

- **Frontend**: React 18, TypeScript, Vite, Material UI 5
- **Backend**: Flask, Databricks SDK (statement execution API)
- **Storage**: Single Delta table with JSON STRING columns per section
- **Deployment**: Databricks App (app.yaml)

## File Structure

```
genie-discovery-app/
  app.py                    # Flask backend -- API routes, SQL helpers, UC metadata
  app.yaml                  # Databricks App config (warehouse, catalog, schema, COE group)
  requirements.txt          # Python dependencies (flask, databricks-sdk)
  frontend/
    index.html              # Vite entry point
    package.json
    tsconfig.json
    vite.config.ts
    src/
      main.tsx              # React entry
      App.tsx               # Router setup
      api.ts                # Fetch wrapper for all API calls
      types.ts              # TypeScript interfaces matching Delta table schema
      theme.ts              # MUI theme
      pages/
        Home.tsx            # Engagement list + create (unique-name validation)
        Engagement.tsx      # Tabbed session view with COE-gated locks
      sessions/
        Session1Form.tsx    # Business context, pain points, existing reports
        Session2Form.tsx    # Question bank, vocabulary & metrics
        Session3Form.tsx    # Term classification, SQL expressions, text instructions, optional metric view YAML
        Session4Form.tsx    # COE review: commentary, auto-summary, data plan, approval
        Session5Form.tsx    # Configure Genie Space (locked until COE approval)
        Session6Form.tsx    # Prototype review (locked until COE approval)
      components/
        EditableTable.tsx   # Generic editable table with add/delete rows
        UCTablePicker.tsx   # 3-level cascading picker (catalog > schema > table)
        UCColumnPicker.tsx  # 4-level cascading picker (catalog > schema > table > column)
  static/                   # Vite build output (gitignored)
```

## Prerequisites

- Databricks workspace with Unity Catalog enabled
- A SQL Warehouse (Serverless recommended)
- A Databricks group to gate COE approval (see setup below)
- Databricks CLI configured with a profile pointing at the target workspace
- Node.js 18+ (for frontend builds)
- Python 3.10+

## Deploy to your workspace

A customer can deploy this app by following these four steps.

### Step 1 -- Pick your workspace resources

You need to decide four things before editing any config:

1. **SQL Warehouse ID** -- In Databricks, go to SQL > SQL Warehouses > select your warehouse > Connection details. The ID is the trailing segment of the HTTP Path (e.g., `/sql/1.0/warehouses/<THIS_PART>`).
2. **Catalog** -- Where the app should store engagement data. The app's service principal must have `CREATE TABLE` on this catalog/schema.
3. **Schema** -- Under that catalog. The schema must already exist; the Delta table inside it is auto-created on first run.
4. **COE group name** -- Create a Databricks group (Account Console > User management > Groups) whose members are allowed to approve engagements in Session 4. Add your COE reviewers to it.

### Step 2 -- Update `app.yaml`

Open `app.yaml` and replace the four `# CHANGE ME` values with your picks from Step 1:

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
```

No other file needs editing. Everything else (UC catalog/schema/table picking, metric view detection, PK/FK join detection) resolves dynamically against whatever the app's service principal can see in your workspace.

### Step 3 -- Build the frontend

```bash
cd frontend
npm install
npm run build        # outputs to ../static/
cd ..
```

### Step 4 -- Upload and deploy

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

# 3. Upload frontend bundle (index.html + the hashed JS file under static/assets/)
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

### Step 5 -- First-run sanity check

1. Open the app URL printed by the deploy command.
2. The backend's `ensure_columns()` creates the engagement Delta table on first startup -- confirm it shows up at `<CATALOG>.<SCHEMA>.engagements`.
3. Click "New Engagement" and verify catalogs from your workspace show up in the UC picker in Session 3.
4. Confirm a COE group member can see the approval buttons in Session 4 (non-members can view but not approve).

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
| `DATABRICKS_WAREHOUSE_ID` | SQL Warehouse used for UC metadata and engagement storage |
| `CATALOG` | UC catalog for the engagement Delta table |
| `SCHEMA` | UC schema under `CATALOG` (must exist; table auto-created) |
| `COE_GROUP_NAME` | Databricks group whose members gate Session 4 approval |

The app auto-adds any missing Delta columns on startup via `ensure_columns()`, so schema migrations happen transparently when you pull updates.

## Status

Work in progress. Functional today:

- All 6 session forms
- Term classification with auto-population into SQL Expressions and Text Instructions
- UC pickers (catalog > schema > table > column), PK/FK join detection, metric view discovery
- Optional metric view YAML generation (uses detected joins, follows Databricks YAML spec)
- Engagement creation with unique-name validation
- COE Review with analyst commentary, auto-summary, data plan, approve/request-changes
- Session 5 & 6 locked until COE approval

Pending:

- "Push to Genie Space" button in Session 5 (currently a shell)
- Metric view picker integration in Session 5
- Sample-question mapping from Session 2 at Space creation time
- Warehouse ID capture per engagement (today it's app-level only)
- Convert to Databricks Asset Bundle for one-command deploy
