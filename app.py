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

w = WorkspaceClient()

# Section columns grouped by session
SESSION_COLS = {
    1: ["business_context", "pain_points", "existing_reports"],
    2: ["question_bank", "vocabulary_metrics"],
    3: ["term_classifications", "sql_expressions", "text_instructions", "data_gaps", "scope_boundaries"],
    4: ["prototype_results", "fixes_log", "benchmarks", "phrasing_notes"],
}

# All JSON section columns
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
            try:
                eng[k] = json.loads(v) if v else []
            except (json.JSONDecodeError, TypeError):
                eng[k] = []
        else:
            eng[k] = v

    eng["sessions"] = {
        "1": {c: eng.get(c, []) for c in SESSION_COLS[1]},
        "2": {c: eng.get(c, []) for c in SESSION_COLS[2]},
        "3": {c: eng.get(c, []) for c in SESSION_COLS[3]},
        "4": {c: eng.get(c, []) for c in SESSION_COLS[4]},
    }
    return eng


# ---------------------------------------------------------------------------
# DB migration: ensure new columns exist
# ---------------------------------------------------------------------------

def ensure_columns():
    """Add any missing JSON columns to the discovery table."""
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


@app.route("/api/engagements", methods=["POST"])
def create_engagement():
    data = request.json
    eid = str(uuid.uuid4())
    ts = now_ts()
    empty = "[]"
    section_cols_sql = ", ".join(ALL_SECTION_COLS)
    section_vals_sql = ", ".join([":empty"] * len(ALL_SECTION_COLS))
    sql_run(
        f"INSERT INTO {TABLE} "
        f"(engagement_id, genie_space_name, business_owner_name, business_owner_email, "
        f"analyst_name, analyst_email, current_session, status, created_at, updated_at, "
        f"{section_cols_sql}) "
        f"VALUES (:eid, :space_name, :bo_name, :bo_email, :a_name, :a_email, "
        f"1, 'draft', :ts, :ts, {section_vals_sql})",
        {
            "eid": eid,
            "space_name": data.get("genie_space_name", ""),
            "bo_name": data.get("business_owner_name", ""),
            "bo_email": data.get("business_owner_email", ""),
            "a_name": data.get("analyst_name", ""),
            "a_email": data.get("analyst_email", ""),
            "ts": ts,
            "empty": empty,
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
        params[col] = json.dumps(data.get(col, []))

    if session_num < 4:
        set_parts.append(f"current_session = GREATEST(current_session, {session_num + 1})")
        set_parts.append("status = 'in_progress'")
    else:
        set_parts.append("current_session = 4")
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
        # Detect key names dynamically -- different environments return different keys
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
