# Genie Space Discovery App

A structured workbook tool that guides data analysts through discovery sessions with business owners to gather all the requirements needed to configure a Databricks Genie Space. Built for Intermountain Health's train-the-trainer program.

## Overview

The app walks analyst-business owner pairs through 4 sessions:

1. **Business Context Discovery** -- Understand the business domain, pain points, and existing reports
2. **Questions & Vocabulary** -- Capture the questions the team wants to ask and define every business term
3. **Technical Design** -- Classify business terms, write SQL expressions, define text instructions, and identify tables
4. **Prototype Review** -- Test the Genie Space with the business owner and capture what works/fails

Each engagement is persisted to a Delta table, allowing multiple sessions over time with save/resume.

## Architecture

```
Frontend (React + Vite + MUI)    Backend (Flask)         Storage (Delta)
-------------------------        ---------------         ---------------
React SPA                  -->   REST API          -->   Unity Catalog
  - Session forms                  - CRUD endpoints       Delta table
  - UC metadata pickers            - UC metadata proxy     (JSON columns)
  - Editable tables                - Session saves
```

- **Frontend**: React 18, TypeScript, Vite, Material UI 5
- **Backend**: Flask, Databricks SDK (statement execution API)
- **Storage**: Single Delta table with JSON STRING columns per section
- **Deployment**: Databricks App (app.yaml)

## File Structure

```
genie-discovery-app/
  app.py                    # Flask backend -- API routes, SQL helpers, UC metadata
  app.yaml                  # Databricks App config (warehouse, catalog, schema)
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
        Home.tsx            # Engagement list + create
        Engagement.tsx      # Tabbed session view with save/navigation
      sessions/
        Session1Form.tsx    # Business context, pain points, existing reports
        Session2Form.tsx    # Question bank, vocabulary & metrics
        Session3Form.tsx    # Term classification, SQL expressions, text instructions, table summary
        Session4Form.tsx    # Prototype review, fixes log, benchmarks, phrasing notes
      components/
        EditableTable.tsx   # Generic editable table with add/delete rows
        UCTablePicker.tsx   # 3-level cascading picker (catalog > schema > table)
        UCColumnPicker.tsx  # 4-level cascading picker (catalog > schema > table > column)
  static/                   # Vite build output (gitignored)
```

## Prerequisites

- Databricks workspace with Unity Catalog enabled
- SQL Warehouse
- Databricks CLI configured with a profile
- Node.js 18+ (for frontend development)
- Python 3.10+

## Configuration

Environment variables (set in `app.yaml`):

| Variable | Description | Default |
|---|---|---|
| `DATABRICKS_WAREHOUSE_ID` | SQL Warehouse ID | `ad1dd0025031919f` |
| `CATALOG` | UC catalog for the discovery table | `genie_training` |
| `SCHEMA` | UC schema for the discovery table | `genie_discovery` |

The app auto-creates missing columns on startup via `ensure_columns()`.

## Local Development

```bash
# Frontend
cd frontend
npm install
npm run dev          # Vite dev server on :5173

# Backend
pip install -r requirements.txt
python app.py        # Flask on :8000
```

## Deploy to Databricks

```bash
# Build frontend
cd frontend && npm run build

# Upload to workspace
databricks workspace import /Workspace/Users/<you>/genie-discovery-app/app.py --file app.py --overwrite --profile <profile>
databricks workspace import /Workspace/Users/<you>/genie-discovery-app/static/index.html --file static/index.html --overwrite --profile <profile>
databricks workspace import /Workspace/Users/<you>/genie-discovery-app/static/assets/<bundle>.js --file static/assets/<bundle>.js --overwrite --profile <profile>

# Deploy app
databricks apps deploy <app-name> --source-code-path /Workspace/Users/<you>/genie-discovery-app --profile <profile>
```

## Status

Work in progress. Current state:
- Sessions 1-4 forms complete
- UC metadata pickers (catalog/schema/table/column) working
- Term classification with auto-population into SQL Expressions and Text Instructions
- Table summary with join detection and metric view discovery
- Pending: "Create Genie Space" button to auto-generate a Space from collected data
