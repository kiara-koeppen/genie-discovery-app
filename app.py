import json
import os
import uuid
from datetime import datetime, timezone

from flask import Flask, request, jsonify, send_from_directory
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementParameterListItem

app = Flask(__name__, static_folder="static", static_url_path="")

CATALOG = os.getenv("CATALOG", "genie_training")
SCHEMA = os.getenv("SCHEMA", "genie_discovery")
TABLE = f"{CATALOG}.{SCHEMA}.discovery"
WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID", "ad1dd0025031919f")
COE_GROUP = os.getenv("COE_GROUP_NAME", "genie-coe-reviewers")

w = WorkspaceClient()

# Section columns grouped by session
SESSION_COLS = {
    1: ["business_context", "pain_points", "existing_reports"],
    2: ["question_bank", "vocabulary_metrics"],
    3: ["term_classifications", "sql_expressions", "text_instructions",
        "data_gaps", "scope_boundaries", "metric_view_yaml"],
    4: ["analyst_commentary", "auto_summary", "data_plan",
        "coe_approval_status", "coe_approval_notes", "coe_reviewer_email"],
    5: ["genie_space_id", "genie_space_config"],
    6: ["prototype_results", "fixes_log", "benchmarks", "phrasing_notes"],
}

# Columns that store plain strings (not JSON arrays)
SCALAR_COLS = {
    "metric_view_yaml", "analyst_commentary", "auto_summary",
    "coe_approval_status", "coe_approval_notes", "coe_reviewer_email",
    "genie_space_id", "genie_space_config",
}

# All section columns
ALL_SECTION_COLS = sorted(set(col for cols in SESSION_COLS.values() for col in cols))


# ---------------------------------------------------------------------------
# SQL helpers
# ---------------------------------------------------------------------------

def sql_exec(query, params=None):
    sdk_params = None
    if params:
        sdk_params = [
            StatementParameterListItem(name=k, value=str(v) if v is not None else "")
            for k, v in params.items()
        ]
    resp = w.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID,
        statement=query,
        parameters=sdk_params,
        catalog=CATALOG,
        schema=SCHEMA,
    )
    if resp.result and resp.result.data_array:
        cols = [c.name for c in resp.manifest.schema.columns]
        return [dict(zip(cols, row)) for row in resp.result.data_array]
    return []


def sql_run(query, params=None):
    sdk_params = None
    if params:
        sdk_params = [
            StatementParameterListItem(name=k, value=str(v) if v is not None else "")
            for k, v in params.items()
        ]
    w.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID,
        statement=query,
        parameters=sdk_params,
        catalog=CATALOG,
        schema=SCHEMA,
    )


def now_ts():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def get_current_user():
    email = (
        request.headers.get("X-Forwarded-Email")
        or request.headers.get("X-Forwarded-User")
        or request.headers.get("X-Forwarded-Preferred-Username")
    )
    if email:
        return email
    try:
        return w.current_user.me().user_name
    except Exception:
        return "unknown"


def parse_row(row):
    """Parse a raw DB row: decode JSON section columns, build sessions dict."""
    eng = {}
    for k, v in row.items():
        if k in ALL_SECTION_COLS:
            if k in SCALAR_COLS:
                eng[k] = v or ""
            else:
                try:
                    eng[k] = json.loads(v) if v else []
                except (json.JSONDecodeError, TypeError):
                    eng[k] = []
        else:
            eng[k] = v

    eng["sessions"] = {}
    for snum, cols in SESSION_COLS.items():
        session = {}
        for c in cols:
            session[c] = eng.get(c, "" if c in SCALAR_COLS else [])
        eng["sessions"][str(snum)] = session
    return eng


# ---------------------------------------------------------------------------
# DB migration: ensure new columns exist
# ---------------------------------------------------------------------------

def ensure_columns():
    """Add any missing columns to the discovery table."""
    try:
        rows = sql_exec(f"DESCRIBE TABLE {TABLE}")
        existing = {r.get("col_name", "") for r in rows}
        for col in ALL_SECTION_COLS:
            if col not in existing:
                sql_run(f"ALTER TABLE {TABLE} ADD COLUMN {col} STRING")
    except Exception:
        pass

ensure_columns()


# ---------------------------------------------------------------------------
# API: User
# ---------------------------------------------------------------------------

@app.route("/api/user")
def api_user():
    return jsonify({"email": get_current_user()})


@app.route("/api/user/coe-member")
def api_user_coe_member():
    """Check if current user is a member of the COE reviewer group."""
    email = get_current_user()
    try:
        groups = list(w.groups.list(filter=f'displayName eq "{COE_GROUP}"'))
        if groups and groups[0].members:
            for m in groups[0].members:
                if m.display and m.display.lower() == email.lower():
                    return jsonify({"is_member": True})
        return jsonify({"is_member": False})
    except Exception:
        return jsonify({"is_member": False})


# ---------------------------------------------------------------------------
# API: Engagements CRUD
# ---------------------------------------------------------------------------

@app.route("/api/engagements", methods=["GET"])
def list_engagements():
    rows = sql_exec(
        f"SELECT engagement_id, genie_space_name, business_owner_name, "
        f"analyst_name, current_session, status, created_at, updated_at "
        f"FROM {TABLE} ORDER BY updated_at DESC"
    )
    return jsonify(rows)


@app.route("/api/engagements/check-name")
def check_engagement_name():
    """Check if an engagement name is already taken."""
    name = request.args.get("name", "").strip()
    if not name:
        return jsonify({"available": False})
    rows = sql_exec(
        f"SELECT COUNT(*) AS cnt FROM {TABLE} WHERE genie_space_name = :name",
        {"name": name},
    )
    count = int(rows[0]["cnt"]) if rows else 0
    return jsonify({"available": count == 0})


@app.route("/api/engagements", methods=["POST"])
def create_engagement():
    data = request.json
    # Validate required fields
    missing = []
    for field in ["genie_space_name", "business_owner_name", "business_owner_email", "analyst_name"]:
        if not data.get(field, "").strip():
            missing.append(field)
    if missing:
        return jsonify({"error": f"Missing required fields: {', '.join(missing)}"}), 400

    # Check uniqueness
    name = data["genie_space_name"].strip()
    existing = sql_exec(
        f"SELECT COUNT(*) AS cnt FROM {TABLE} WHERE genie_space_name = :name",
        {"name": name},
    )
    if existing and int(existing[0]["cnt"]) > 0:
        return jsonify({"error": "An engagement with this name already exists"}), 409

    eid = str(uuid.uuid4())
    ts = now_ts()
    section_cols_sql = ", ".join(ALL_SECTION_COLS)
    section_defaults = []
    section_params = {}
    for col in ALL_SECTION_COLS:
        param_name = f"default_{col}"
        section_defaults.append(f":{param_name}")
        section_params[param_name] = "" if col in SCALAR_COLS else "[]"

    sql_run(
        f"INSERT INTO {TABLE} "
        f"(engagement_id, genie_space_name, business_owner_name, business_owner_email, "
        f"analyst_name, analyst_email, current_session, status, created_at, updated_at, "
        f"{section_cols_sql}) "
        f"VALUES (:eid, :space_name, :bo_name, :bo_email, :a_name, :a_email, "
        f"1, 'draft', :ts, :ts, {', '.join(section_defaults)})",
        {
            "eid": eid,
            "space_name": name,
            "bo_name": data.get("business_owner_name", "").strip(),
            "bo_email": data.get("business_owner_email", "").strip(),
            "a_name": data.get("analyst_name", "").strip(),
            "a_email": data.get("analyst_email", "").strip(),
            "ts": ts,
            **section_params,
        },
    )
    return jsonify({"engagement_id": eid}), 201


@app.route("/api/engagements/<eid>", methods=["GET"])
def get_engagement(eid):
    rows = sql_exec(f"SELECT * FROM {TABLE} WHERE engagement_id = :eid", {"eid": eid})
    if not rows:
        return jsonify({"error": "Not found"}), 404
    return jsonify(parse_row(rows[0]))


@app.route("/api/engagements/<eid>", methods=["PUT"])
def update_engagement(eid):
    data = request.json
    ts = now_ts()
    sql_run(
        f"UPDATE {TABLE} SET "
        f"genie_space_name = :space_name, business_owner_name = :bo_name, "
        f"business_owner_email = :bo_email, analyst_name = :a_name, "
        f"analyst_email = :a_email, current_session = :session_num, "
        f"status = :status, updated_at = :ts "
        f"WHERE engagement_id = :eid",
        {
            "eid": eid,
            "space_name": data.get("genie_space_name", ""),
            "bo_name": data.get("business_owner_name", ""),
            "bo_email": data.get("business_owner_email", ""),
            "a_name": data.get("analyst_name", ""),
            "a_email": data.get("analyst_email", ""),
            "session_num": str(data.get("current_session", 1)),
            "status": data.get("status", "draft"),
            "ts": ts,
        },
    )
    return jsonify({"success": True})


@app.route("/api/engagements/<eid>", methods=["DELETE"])
def delete_engagement(eid):
    sql_run(f"DELETE FROM {TABLE} WHERE engagement_id = :eid", {"eid": eid})
    return jsonify({"success": True})


# ---------------------------------------------------------------------------
# API: Session saves
# ---------------------------------------------------------------------------

def save_session(eid, session_num, data):
    """Update session columns for an engagement."""
    cols = SESSION_COLS[session_num]
    set_parts = []
    params = {"eid": eid, "ts": now_ts()}

    for col in cols:
        set_parts.append(f"{col} = :{col}")
        if col in SCALAR_COLS:
            params[col] = data.get(col, "")
        else:
            params[col] = json.dumps(data.get(col, []))

    if session_num < 6:
        set_parts.append(f"current_session = GREATEST(current_session, {session_num + 1})")
        set_parts.append("status = 'in_progress'")
    else:
        set_parts.append("current_session = 6")
        set_parts.append("status = 'complete'")

    set_parts.append("updated_at = :ts")
    set_sql = ", ".join(set_parts)

    sql_run(f"UPDATE {TABLE} SET {set_sql} WHERE engagement_id = :eid", params)


@app.route("/api/engagements/<eid>/sessions/1", methods=["PUT"])
def save_session_1(eid):
    save_session(eid, 1, request.json)
    return jsonify({"success": True})


@app.route("/api/engagements/<eid>/sessions/2", methods=["PUT"])
def save_session_2(eid):
    save_session(eid, 2, request.json)
    return jsonify({"success": True})


@app.route("/api/engagements/<eid>/sessions/3", methods=["PUT"])
def save_session_3(eid):
    save_session(eid, 3, request.json)
    return jsonify({"success": True})


@app.route("/api/engagements/<eid>/sessions/4", methods=["PUT"])
def save_session_4(eid):
    save_session(eid, 4, request.json)
    return jsonify({"success": True})


@app.route("/api/engagements/<eid>/sessions/5", methods=["PUT"])
def save_session_5(eid):
    save_session(eid, 5, request.json)
    return jsonify({"success": True})


@app.route("/api/engagements/<eid>/sessions/6", methods=["PUT"])
def save_session_6(eid):
    save_session(eid, 6, request.json)
    return jsonify({"success": True})


# ---------------------------------------------------------------------------
# API: COE Approval
# ---------------------------------------------------------------------------

@app.route("/api/engagements/<eid>/coe-approve", methods=["PUT"])
def coe_approve(eid):
    """Set COE approval status. Only COE group members should call this."""
    data = request.json
    status = data.get("status", "")
    notes = data.get("notes", "")
    reviewer = get_current_user()
    ts = now_ts()
    sql_run(
        f"UPDATE {TABLE} SET "
        f"coe_approval_status = :status, coe_approval_notes = :notes, "
        f"coe_reviewer_email = :reviewer, updated_at = :ts "
        f"WHERE engagement_id = :eid",
        {"eid": eid, "status": status, "notes": notes, "reviewer": reviewer, "ts": ts},
    )
    return jsonify({"success": True})


# ---------------------------------------------------------------------------
# API: Auto-summary (structured, no LLM)
# ---------------------------------------------------------------------------

@app.route("/api/engagements/<eid>/auto-summary")
def auto_summary(eid):
    """Generate a structured summary of sessions 1-3."""
    rows = sql_exec(f"SELECT * FROM {TABLE} WHERE engagement_id = :eid", {"eid": eid})
    if not rows:
        return jsonify({"summary": ""}), 404
    eng = parse_row(rows[0])
    s1 = eng["sessions"]["1"]
    s2 = eng["sessions"]["2"]
    s3 = eng["sessions"]["3"]

    parts = []
    parts.append(f"## Engagement: {eng.get('genie_space_name', 'Untitled')}")
    parts.append(f"**Business Owner:** {eng.get('business_owner_name', '')} ({eng.get('business_owner_email', '')})")
    parts.append(f"**Analyst:** {eng.get('analyst_name', '')} ({eng.get('analyst_email', '')})")
    parts.append("")

    # Session 1
    pain_points = s1.get("pain_points", [])
    reports = s1.get("existing_reports", [])
    parts.append("### Session 1: Business Context")
    if pain_points:
        parts.append(f"**Pain Points:** {len(pain_points)}")
        for pp in pain_points:
            parts.append(f"- {pp.get('description', '')}")
    if reports:
        parts.append(f"**Existing Reports:** {len(reports)}")
        for r in reports:
            parts.append(f"- {r.get('report_name', '')}: {r.get('what_it_shows', '')}")
    parts.append("")

    # Session 2
    questions = s2.get("question_bank", [])
    vocab = s2.get("vocabulary_metrics", [])
    parts.append("### Session 2: Questions & Vocabulary")
    parts.append(f"**Questions Captured:** {len(questions)}")
    parts.append(f"**Vocabulary Terms:** {len(vocab)}")
    if vocab:
        terms_list = ", ".join(v.get("business_term", "") for v in vocab[:10])
        parts.append(f"**Terms:** {terms_list}")
    parts.append("")

    # Session 3
    classifications = s3.get("term_classifications", [])
    sql_exprs = s3.get("sql_expressions", [])
    text_instrs = s3.get("text_instructions", [])
    data_gaps = s3.get("data_gaps", [])
    scope = s3.get("scope_boundaries", [])
    parts.append("### Session 3: Technical Design")
    parts.append(f"**Classified Terms:** {len(classifications)}")
    parts.append(f"**SQL Expressions (Metrics):** {len(sql_exprs)}")
    if sql_exprs:
        for e in sql_exprs:
            parts.append(f"- {e.get('metric_name', '')}: `{e.get('uc_table', '')}`")
    parts.append(f"**Text Instructions:** {len(text_instrs)}")
    parts.append(f"**Data Gaps:** {len(data_gaps)}")
    parts.append(f"**Scope Boundaries:** {len(scope)}")

    # Tables identified
    tables = set()
    for e in sql_exprs:
        t = e.get("uc_table", "")
        if t and len(t.split(".")) == 3:
            tables.add(t)
    if tables:
        parts.append(f"\n**Tables Identified ({len(tables)}):**")
        for t in sorted(tables):
            parts.append(f"- `{t}`")

    return jsonify({"summary": "\n".join(parts)})


# ---------------------------------------------------------------------------
# API: Unity Catalog metadata
# ---------------------------------------------------------------------------

@app.route("/api/uc/catalogs")
def uc_catalogs():
    rows = sql_exec("SHOW CATALOGS")
    return jsonify([r.get("catalog", "") for r in rows if not r.get("catalog", "").startswith("__")])


@app.route("/api/uc/schemas")
def uc_schemas():
    catalog = request.args.get("catalog", "")
    if not catalog:
        return jsonify([])
    rows = sql_exec(f"SHOW SCHEMAS IN `{catalog}`")
    key = "databaseName" if "databaseName" in (rows[0] if rows else {}) else "namespace"
    return jsonify([r.get(key, "") for r in rows if r.get(key, "") != "information_schema"])


@app.route("/api/uc/tables")
def uc_tables():
    catalog = request.args.get("catalog", "")
    schema = request.args.get("schema", "")
    if not catalog or not schema:
        return jsonify([])
    rows = sql_exec(f"SHOW TABLES IN `{catalog}`.`{schema}`")
    return jsonify([r.get("tableName", "") for r in rows])


@app.route("/api/uc/columns")
def uc_columns():
    catalog = request.args.get("catalog", "")
    schema = request.args.get("schema", "")
    table = request.args.get("table", "")
    if not catalog or not schema or not table:
        return jsonify([])
    try:
        rows = sql_exec(f"DESCRIBE TABLE `{catalog}`.`{schema}`.`{table}`")
        if not rows:
            return jsonify([])
        first = rows[0]
        name_key = next((k for k in ("col_name", "column_name", "name") if k in first), "col_name")
        type_key = next((k for k in ("data_type", "type", "Type") if k in first), "data_type")
        return jsonify([
            {"name": r.get(name_key, ""), "type": r.get(type_key, "")}
            for r in rows
            if r.get(name_key, "") and not r.get(name_key, "").startswith("#")
        ])
    except Exception:
        return jsonify([])


@app.route("/api/uc/joins")
def uc_joins():
    """Auto-detect PK/FK relationships between selected tables."""
    tables = request.args.getlist("table")
    if len(tables) < 2:
        return jsonify([])

    results = []
    for tbl in tables:
        parts = tbl.split(".")
        if len(parts) != 3:
            continue
        cat, sch, name = parts
        try:
            fk_rows = sql_exec(
                f"SELECT fk_column_name, pk_table_name, pk_column_name "
                f"FROM `{cat}`.information_schema.table_constraints tc "
                f"JOIN `{cat}`.information_schema.key_column_usage kcu "
                f"ON tc.constraint_name = kcu.constraint_name "
                f"WHERE tc.table_schema = '{sch}' AND tc.table_name = '{name}' "
                f"AND tc.constraint_type = 'FOREIGN KEY'"
            )
            for fk in fk_rows:
                pk_table = fk.get("pk_table_name", "")
                pk_col = fk.get("pk_column_name", "")
                fk_col = fk.get("fk_column_name", "")
                if pk_table and pk_col and fk_col:
                    results.append({
                        "table": f"{name} -> {pk_table}",
                        "keys": f"{name}.{fk_col} = {pk_table}.{pk_col}",
                    })
        except Exception:
            pass

    return jsonify(results)


@app.route("/api/uc/metric-views")
def uc_metric_views():
    """Detect existing metric views in a catalog.schema."""
    catalog_schema = request.args.get("catalog_schema", "")
    if not catalog_schema or "." not in catalog_schema:
        return jsonify([])

    parts = catalog_schema.split(".")
    cat, sch = parts[0], parts[1]
    try:
        rows = sql_exec(f"SHOW VIEWS IN `{cat}`.`{sch}`")
        view_names = [r.get("viewName", "") for r in rows if r.get("viewName", "")]
        metric_views = []
        for vn in view_names:
            try:
                detail = sql_exec(f"DESCRIBE TABLE EXTENDED `{cat}`.`{sch}`.`{vn}`")
                for d in detail:
                    if d.get("col_name", "") == "Type" and "VIEW" in d.get("data_type", "").upper():
                        metric_views.append(f"{cat}.{sch}.{vn}")
                        break
            except Exception:
                pass
        return jsonify(metric_views)
    except Exception:
        return jsonify([])


# ---------------------------------------------------------------------------
# SPA catch-all
# ---------------------------------------------------------------------------

@app.route("/")
@app.route("/engagement/<path:path>")
@app.route("/view/<path:path>")
def serve_spa(path=None):
    return send_from_directory(app.static_folder, "index.html")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
