import json
import os
import secrets
import traceback
import uuid
from datetime import datetime, timezone

import requests
from flask import Flask, request, jsonify, send_from_directory
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementParameterListItem
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole

app = Flask(__name__, static_folder="static", static_url_path="")

def _require_env(name):
    v = (os.getenv(name) or "").strip()
    if not v:
        raise RuntimeError(
            f"Required environment variable {name!r} is not set. "
            f"Configure it in app.yaml (see README 'Deploy to your workspace')."
        )
    return v

# Required — no defaults, since these encode workspace-specific resources.
CATALOG = _require_env("CATALOG")
SCHEMA = _require_env("SCHEMA")
WAREHOUSE_ID = _require_env("DATABRICKS_WAREHOUSE_ID")
TABLE = f"{CATALOG}.{SCHEMA}.discovery"

# Optional — sensible defaults are OK here.
COE_GROUP = os.getenv("COE_GROUP_NAME") or "genie-coe-reviewers"
LLM_ENDPOINT = os.getenv("LLM_ENDPOINT_NAME") or "databricks-claude-sonnet-4-6"

w = WorkspaceClient()

# Section columns grouped by session
SESSION_COLS = {
    1: ["business_context", "pain_points", "existing_reports"],
    2: ["question_bank", "vocabulary_metrics"],
    3: ["term_classifications", "sql_expressions", "text_instructions",
        "data_gaps", "scope_boundaries", "global_filter",
        "metric_view_yaml", "metric_view_fqn"],
    4: ["analyst_commentary", "auto_summary", "data_plan", "benchmark_questions",
        "coe_approval_status", "coe_approval_notes", "coe_reviewer_email"],
    5: ["genie_space_id", "genie_space_config",
        "plan_general_instructions", "plan_sample_questions", "plan_narrative",
        "plan_sql_filters", "plan_sql_dimensions", "plan_sql_measures",
        "plan_example_queries", "plan_joins",
        "plan_warehouse_id", "genie_space_url", "genie_space_pushed_at"],
    6: ["prototype_results", "fixes_log", "benchmarks", "phrasing_notes"],
}

# Columns that store plain strings (not JSON arrays)
SCALAR_COLS = {
    "global_filter",
    "metric_view_yaml", "metric_view_fqn", "analyst_commentary", "auto_summary",
    "coe_approval_status", "coe_approval_notes", "coe_reviewer_email",
    "genie_space_id", "genie_space_config",
    "plan_general_instructions", "plan_narrative", "plan_warehouse_id",
    "genie_space_url", "genie_space_pushed_at",
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

def ensure_table():
    """Create the engagement Delta table on first run, then add any missing columns.

    The README promises auto-creation; this is that promise. Requires the SP to
    have CREATE TABLE on <CATALOG>.<SCHEMA>.
    """
    section_ddl = ", ".join(f"{col} STRING" for col in ALL_SECTION_COLS)
    sql_run(
        f"CREATE TABLE IF NOT EXISTS {TABLE} ("
        f"engagement_id STRING, genie_space_name STRING, "
        f"business_owner_name STRING, business_owner_email STRING, "
        f"analyst_name STRING, analyst_email STRING, "
        f"current_session INT, status STRING, "
        f"created_at STRING, updated_at STRING, "
        f"{section_ddl}"
        f") USING DELTA"
    )
    rows = sql_exec(f"DESCRIBE TABLE {TABLE}")
    existing = {r.get("col_name", "") for r in rows}
    for col in ALL_SECTION_COLS:
        if col not in existing:
            sql_run(f"ALTER TABLE {TABLE} ADD COLUMN {col} STRING")

ensure_table()


# ---------------------------------------------------------------------------
# API: User
# ---------------------------------------------------------------------------

@app.route("/api/user")
def api_user():
    return jsonify({"email": get_current_user()})


def _user_workspace_client():
    """Build a WorkspaceClient using the forwarded user access token (OBO).

    Forces PAT auth via explicit Config so it does not accidentally combine
    with the app service principal's OAuth creds from env vars.
    Returns None if no user token is available.
    """
    user_token = request.headers.get("X-Forwarded-Access-Token")
    if not user_token:
        return None
    from databricks.sdk import WorkspaceClient as WC
    from databricks.sdk.core import Config
    cfg = Config(host=w.config.host, token=user_token, auth_type="pat")
    return WC(config=cfg)


def _user_is_coe_member(user_w):
    """True if the OBO user is in COE_GROUP. False on any error."""
    if not user_w:
        return False
    try:
        me = user_w.current_user.me()
        for g in (me.groups or []):
            if g.display == COE_GROUP:
                return True
    except Exception:
        pass
    return False


def _authorize_engagement(eid):
    """Gate access to a single engagement row.

    Returns (engagement_dict, None) if the current user is the analyst, the BO,
    or a COE-group member. Returns (None, flask_error_response) otherwise.
    COE members get access so they can review engagements for Session 4 approval.
    """
    rows = sql_exec(f"SELECT * FROM {TABLE} WHERE engagement_id = :eid", {"eid": eid})
    if not rows:
        return None, (jsonify({"error": "Not found"}), 404)
    eng = parse_row(rows[0])
    current = (get_current_user() or "").strip().lower()
    analyst = (eng.get("analyst_email") or "").strip().lower()
    bo = (eng.get("business_owner_email") or "").strip().lower()
    if current and (current == analyst or current == bo):
        return eng, None
    if _user_is_coe_member(_user_workspace_client()):
        return eng, None
    return None, (jsonify({"error": "Forbidden"}), 403)


import re as _re

_UUID_RE = _re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")


@app.before_request
def _gate_engagement_routes():
    """Apply _authorize_engagement to every /api/engagements/<eid>[/...] route.

    Catches all the per-session saves, generate-plan, push-to-genie, etc. without
    having to bolt an auth check onto each handler individually. Only triggers
    when the path segment after /api/engagements/ looks like an engagement UUID
    (so sibling routes like /api/engagements/check-name are not mistakenly gated).
    """
    path = request.path or ""
    prefix = "/api/engagements/"
    if not path.startswith(prefix):
        return None
    remainder = path[len(prefix):]
    if not remainder:
        return None
    eid = remainder.split("/", 1)[0]
    if not _UUID_RE.match(eid):
        return None
    _, err = _authorize_engagement(eid)
    return err


@app.route("/api/warehouses")
def api_warehouses():
    """List SQL warehouses visible to the current user (OBO).

    OBO-only so users only see warehouses they actually have access to. If the
    user's token is missing the `sql` scope (common after app scope changes
    until the user re-authorizes), return a 403 with an actionable message —
    do NOT fall back to the SP client, which would let users pick warehouses
    they can't execute against.
    """
    user_w = _user_workspace_client()
    if not user_w:
        return jsonify({"error": "No user access token"}), 401
    try:
        whs = list(user_w.warehouses.list())
    except Exception as e:
        tb = traceback.format_exc()
        print(f"[/api/warehouses] ERROR: {type(e).__name__}: {e}\n{tb}", flush=True)
        msg = str(e)
        if "does not have required scopes" in msg or "PermissionDenied" in type(e).__name__:
            return jsonify({
                "error": "Your app authorization is missing the `sql` scope. Please sign out of the app (or open in a private window) and re-authorize when prompted.",
                "reauth_required": True,
            }), 403
        return jsonify({"error": f"Failed to list warehouses: {type(e).__name__}: {e}"}), 500
    out = []
    for wh in whs:
        out.append({
            "id": wh.id,
            "name": wh.name,
            "state": str(wh.state) if wh.state else "",
            "size": wh.cluster_size or "",
            "type": str(wh.warehouse_type) if wh.warehouse_type else "",
        })
    out.sort(key=lambda x: x["name"].lower())
    return jsonify(out)


@app.route("/api/user/coe-member")
def api_user_coe_member():
    """Check COE membership using the user's forwarded access token (OBO).

    This respects the user's own permissions rather than requiring the app
    service principal to be a workspace admin. Append ?debug=1 for
    diagnostics.
    """
    email = get_current_user()
    debug = request.args.get("debug") == "1"
    result = {"is_member": False}
    if debug:
        result["email"] = email
        result["coe_group_name"] = COE_GROUP

    user_w = _user_workspace_client()
    if not user_w:
        if debug:
            result["error"] = "no X-Forwarded-Access-Token header"
        return jsonify(result)

    try:
        me = user_w.current_user.me()
        me_groups = me.groups or []
        if debug:
            result["me_group_count"] = len(me_groups)
            result["me_groups"] = [g.display for g in me_groups]
        for g in me_groups:
            if g.display == COE_GROUP:
                result["is_member"] = True
                return jsonify(result)
        return jsonify(result)
    except Exception as e:
        if debug:
            result["error"] = str(e)
            result["error_type"] = type(e).__name__
        return jsonify(result)


# ---------------------------------------------------------------------------
# API: Engagements CRUD
# ---------------------------------------------------------------------------

@app.route("/api/engagements", methods=["GET"])
def list_engagements():
    """Return only engagements where the caller is a stakeholder — or all
    engagements if the caller is a COE-group reviewer.
    """
    current = (get_current_user() or "").strip().lower()
    if _user_is_coe_member(_user_workspace_client()):
        rows = sql_exec(
            f"SELECT engagement_id, genie_space_name, business_owner_name, "
            f"analyst_name, current_session, status, created_at, updated_at "
            f"FROM {TABLE} ORDER BY updated_at DESC"
        )
    else:
        rows = sql_exec(
            f"SELECT engagement_id, genie_space_name, business_owner_name, "
            f"analyst_name, current_session, status, created_at, updated_at "
            f"FROM {TABLE} "
            f"WHERE LOWER(TRIM(analyst_email)) = :u "
            f"   OR LOWER(TRIM(business_owner_email)) = :u "
            f"ORDER BY updated_at DESC",
            {"u": current},
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
    """Set COE approval status. Server-side enforced: caller must be in COE_GROUP.

    Engagement-existence / stakeholder access is already checked by the
    before_request gate. This handler additionally requires the caller to be a
    COE member, so the status cannot be flipped by a direct API call from an
    analyst or BO.
    """
    user_w = _user_workspace_client()
    if not user_w:
        return jsonify({"error": "reauth_required"}), 401
    if not _user_is_coe_member(user_w):
        return jsonify({
            "error": f"Only members of the '{COE_GROUP}' group can approve engagements.",
        }), 403
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
    """Generate an LLM-driven Readiness Brief synthesizing Sessions 1-4 for COE review.

    Replaces the older deterministic dump. The brief includes citation-backed
    narrative, a coverage analysis tying S2 questions to S3 metrics, and a gap
    section that distinguishes acknowledged vs unacknowledged gaps.
    """
    rows = sql_exec(f"SELECT * FROM {TABLE} WHERE engagement_id = :eid", {"eid": eid})
    if not rows:
        return jsonify({"summary": ""}), 404
    eng = parse_row(rows[0])

    try:
        prompt = _build_readiness_brief_prompt(eng)
        result = _call_llm(prompt)
    except Exception as e:
        tb = traceback.format_exc()
        print(f"[readiness-brief] ERROR: {e}\n{tb}", flush=True)
        return jsonify({"error": f"{type(e).__name__}: {e}"}), 500

    brief = str(result.get("brief", "")).strip()
    if not brief:
        return jsonify({"error": "LLM returned empty brief"}), 500
    return jsonify({"summary": brief})


def _build_readiness_brief_prompt(eng):
    """Build the LLM prompt that produces a COE-facing Readiness Brief."""
    s1 = eng["sessions"].get("1", {}) or {}
    s2 = eng["sessions"].get("2", {}) or {}
    s3 = eng["sessions"].get("3", {}) or {}
    s4 = eng["sessions"].get("4", {}) or {}

    lines = []
    lines.append(f"# Engagement: {eng.get('genie_space_name', 'Untitled')}")
    lines.append(f"Business Owner: {eng.get('business_owner_name', '')} <{eng.get('business_owner_email', '')}>")
    lines.append(f"Analyst: {eng.get('analyst_name', '')} <{eng.get('analyst_email', '')}>")
    lines.append("")

    # ----- SESSION 1 -----
    lines.append("## SESSION 1: Business Context")
    bc = s1.get("business_context", []) or []
    if bc:
        lines.append("### Business Context Q&A (BO answers)")
        for b in bc:
            if not isinstance(b, dict):
                continue
            q = (b.get("question") or "").strip()
            why = (b.get("why_it_matters") or "").strip()
            notes = (b.get("response") or "").strip()
            if not (q or notes):
                continue
            lines.append(f"- **Q:** {q}")
            if why:
                lines.append(f"  - *Why it matters:* {why}")
            if notes:
                lines.append(f"  - **BO answer:** {notes}")
    pps = s1.get("pain_points", []) or []
    if pps:
        lines.append("### Pain Points")
        for pp in pps:
            d = (pp.get("description") if isinstance(pp, dict) else str(pp)) or ""
            d = d.strip()
            if d:
                lines.append(f"- {d}")
    er = s1.get("existing_reports", []) or []
    if er:
        lines.append("### Existing Reports")
        for r in er:
            if not isinstance(r, dict):
                continue
            name = (r.get("report_name") or "").strip()
            what = (r.get("what_it_shows") or "").strip()
            freq = (r.get("frequency") or "").strip()
            issues = (r.get("known_issues") or "").strip()
            if not (name or what):
                continue
            line = f"- **{name}**"
            if freq:
                line += f" ({freq})"
            if what:
                line += f": {what}"
            lines.append(line)
            if issues:
                lines.append(f"  - Known issues: {issues}")
    lines.append("")

    # ----- SESSION 2 -----
    lines.append("## SESSION 2: Questions & Vocabulary")
    qb = s2.get("question_bank", []) or []
    if qb:
        lines.append("### Question Bank")
        for i, q in enumerate(qb, 1):
            if not isinstance(q, dict):
                continue
            text = (q.get("question") or q.get("text") or "").strip()
            decision = (q.get("decision_it_drives") or "").strip()
            if not text:
                continue
            lines.append(f"- **Q{i}:** {text}")
            if decision:
                lines.append(f"  - Drives decision: {decision}")
    vm = s2.get("vocabulary_metrics", []) or []
    if vm:
        lines.append("### Vocabulary & Metric Definitions")
        for v in vm:
            if not isinstance(v, dict):
                continue
            term = (v.get("business_term") or "").strip()
            defn = (v.get("definition") or v.get("description") or "").strip()
            synonyms = (v.get("synonyms") or "").strip()
            if not term:
                continue
            line = f"- **{term}**"
            if defn:
                line += f": {defn}"
            if synonyms:
                line += f" (synonyms: {synonyms})"
            lines.append(line)
    lines.append("")

    # ----- SESSION 3 -----
    lines.append("## SESSION 3: Technical Design")
    tc = s3.get("term_classifications", []) or []
    if tc:
        lines.append("### Term Classifications")
        for t in tc:
            if not isinstance(t, dict):
                continue
            term = (t.get("business_term") or t.get("term") or "").strip()
            types = t.get("types") or []
            if isinstance(types, list):
                types_str = ", ".join(str(x) for x in types)
            else:
                types_str = str(types)
            if term:
                lines.append(f"- **{term}** → {types_str}")
    sb = s3.get("scope_boundaries", []) or []
    if sb:
        lines.append("### Scope Boundaries")
        for b in sb:
            if not isinstance(b, dict):
                continue
            item = (b.get("item") or b.get("topic") or "").strip()
            scope_status = (b.get("in_scope") or b.get("status") or "").strip()
            notes = (b.get("notes") or b.get("rationale") or b.get("description") or "").strip()
            if item:
                line = f"- **{item}** ({scope_status})"
                if notes:
                    line += f": {notes}"
                lines.append(line)
    dg = s3.get("data_gaps", []) or []
    if dg:
        lines.append("### Data Gaps (analyst-acknowledged)")
        for g in dg:
            if not isinstance(g, dict):
                continue
            bq = (g.get("business_question") or g.get("topic") or g.get("gap") or "").strip()
            avail = (g.get("data_available") or "").strip()
            gap = (g.get("gap_description") or g.get("description") or g.get("detail") or "").strip()
            res = (g.get("proposed_resolution") or "").strip()
            if not (bq or gap):
                continue
            line = f"- **{bq}**"
            if avail:
                line += f" (data available: {avail})"
            if gap:
                line += f" — {gap}"
            lines.append(line)
            if res:
                lines.append(f"  - Proposed resolution: {res}")
    gf = (s3.get("global_filter") or "").strip()
    if gf:
        lines.append("### Global Filter")
        lines.append(f"```\n{gf}\n```")
    sql_exprs = s3.get("sql_expressions", []) or []
    if sql_exprs:
        lines.append("### SQL Expressions (the core technical design)")
        for e in sql_exprs:
            if not isinstance(e, dict):
                continue
            name = (e.get("metric_name") or "").strip()
            tbl = (e.get("uc_table") or "").strip()
            sql = (e.get("sql_code") or "").strip()
            display = (e.get("display_name") or "").strip()
            synonyms = (e.get("synonyms") or "").strip()
            if not (name or sql):
                continue
            line = f"- **{name}**"
            if display and display != name:
                line += f" (display: {display})"
            if tbl:
                line += f" on `{tbl}`"
            lines.append(line)
            if sql:
                lines.append(f"  - SQL: `{sql}`")
            if synonyms:
                lines.append(f"  - Synonyms: {synonyms}")
    ti = s3.get("text_instructions", []) or []
    if ti:
        lines.append("### Text Instructions / Rules")
        for t in ti:
            if not isinstance(t, dict):
                continue
            title = (t.get("title") or "").strip()
            instr = (t.get("instruction") or "").strip()
            if title or instr:
                lines.append(f"- **{title}**: {instr}")
    mv_yaml = (s3.get("metric_view_yaml") or "").strip()
    if mv_yaml:
        lines.append("### Generated Metric View YAML")
        lines.append(f"```yaml\n{mv_yaml}\n```")
    mv_fqn = (s3.get("metric_view_fqn") or "").strip()
    if mv_fqn:
        lines.append(f"### Created Metric View: `{mv_fqn}`")
    lines.append("")

    # ----- SESSION 4 (data plan only — not the brief itself) -----
    dp = s4.get("data_plan", []) or []
    if dp:
        lines.append("## SESSION 4: Data Plan (current state)")
        for d in dp:
            if not isinstance(d, dict):
                continue
            tbl = (d.get("table_or_view") or "").strip()
            typ = (d.get("type") or "").strip()
            inc = (d.get("include_in_space") or "").strip()
            notes = (d.get("notes") or "").strip()
            if not tbl:
                continue
            line = f"- **{tbl}** ({typ}, include: {inc})"
            if notes:
                line += f" — {notes}"
            lines.append(line)
        lines.append("")

    context = "\n".join(lines)

    return f"""You are preparing a READINESS BRIEF for a Center of Excellence (COE) reviewer who must approve or reject this Genie Space engagement.

The brief gives the COE reviewer a clear, citation-backed picture of:
1. Whether the analyst captured enough information from the business owner to scope a useful Genie Space
2. Whether the technical design (SQL expressions, metric view, data plan) actually addresses what the BO needs
3. What's still NOT addressed, distinguishing acknowledged gaps (analyst-flagged) from unacknowledged coverage gaps (red flags)

CRITICAL RULES:
- CITE your sources. Every concrete claim should reference where it came from. Use citations like `[S1 Pain Points]`, `[S2 Q3]`, `[S3 SQL: denial_rate_pct]`, `[S4 Data Plan]`. Never make a coverage claim without a citation.
- Be SKEPTICAL. The COE is liable for what they approve. Find holes. Do NOT smooth over gaps to make the brief feel coherent. Adversarial review is the goal.
- Distinguish ACKNOWLEDGED gaps (the analyst flagged these in S3 Data Gaps — they are FINE to have) from UNACKNOWLEDGED gaps (S2 questions or existing-report metrics not covered by the design and not flagged — these are RED FLAGS).
- If S3 SQL Expressions, the data plan, or the question bank is empty/sparse, FLAG IT explicitly. Do not pretend the engagement is ready when it isn't.
- The COE will read this in 3-5 minutes. Be specific and concise. No filler.
- Use the BO's language where possible (from S1 Business Context Q&A and S2 Vocabulary) — not invented terminology.

OUTPUT STRUCTURE (markdown, exact section headers in this order):

## TL;DR
3-5 bullets: who the audience is, what they need, what was built, the headline risk.

## What We Learned
2-4 short paragraphs synthesizing S1+S2: the BO's day-to-day, decisions they make, pain points, existing reports they rely on, key vocabulary. Cite every paragraph.

## Technical Approach
2-3 short paragraphs on S3: source tables, key metrics defined, scope decisions, the metric view (or lack of one), the global filter if any. Cite specific SQL expressions or vocabulary terms.

## Data Plan
A short bulleted list of tables and metric views being included in the Genie Space (from S4 Data Plan). Identifier + 1-line purpose each. If empty, flag this as a problem.

## Coverage Analysis
Walk through the S2 Question Bank. For each question (group similar ones if there are many), classify and cite:
- ✅ **Answerable**: name the specific measure/dimension/table that addresses it
- ⚠️ **Partial**: what part is covered, what's missing
- ❌ **Not addressed**: nothing in the design supports this

If the question bank is empty, say so explicitly and flag it as a problem.

## Open Gaps & Risks

### Acknowledged Gaps
List what the analyst already flagged in `S3 Data Gaps`. Brief context per item. These are NOT blockers.

### Unacknowledged Gaps
Coverage failures the analyst did NOT flag — S2 questions or existing-report metrics not supported by the current design. Each with severity: **Low / Medium / High**. THIS IS WHERE COE FOCUSES.

If there are none, say "None identified" — but only if you've genuinely cross-checked every S2 question and existing report against the design.

## Reviewer Recommendation
ONE sentence framing the question for COE. NOT a verdict. Examples:
- "Recommended: approve — coverage is strong, residual gaps are acknowledged and bounded."
- "Recommended: request changes — Q3, Q7, Q11 cannot be answered by the current design and were not flagged."
- "Recommended: clarify before review — Session 3 SQL Expressions has only 2 entries; the engagement is not ready for COE evaluation."

<engagement_context>
{context}
</engagement_context>

Return JSON: {{"brief": "<the markdown brief>"}}. Just the markdown — no markdown fences around the JSON itself."""


# ---------------------------------------------------------------------------
# API: LLM-generated plan (Session 5)
# ---------------------------------------------------------------------------

def _gen_hex_id():
    """Generate a 32-character lowercase hex ID (Genie requirement)."""
    return secrets.token_hex(16)


def _fetch_uc_joins(table_fqns, client=None):
    """For each table, pull declared PK/FK constraints from UC and build join specs.
    Returns a list of {left_table, left_cols, right_table, right_cols, relationship_type, source}.
    Only returns joins where BOTH tables are in the provided list (intra-space joins).

    `client` defaults to the SP workspace client; pass a user-OBO client when the
    SP may not have BROWSE on the source tables.
    """
    c_client = client or w
    in_scope = set(table_fqns)
    joins = []
    seen = set()
    print(f"[joins] in-scope tables: {list(in_scope)}", flush=True)
    for fqn in table_fqns:
        try:
            info = c_client.tables.get(fqn)
        except Exception as e:
            print(f"[joins] tables.get({fqn}) failed: {type(e).__name__}: {e}", flush=True)
            continue
        constraints = getattr(info, "table_constraints", None) or []
        pk_count = sum(1 for c in constraints if getattr(c, "primary_key_constraint", None))
        fk_count = sum(1 for c in constraints if getattr(c, "foreign_key_constraint", None))
        print(
            f"[joins] {fqn}: {len(constraints)} constraint(s) (PK={pk_count}, FK={fk_count})",
            flush=True,
        )
        for c in constraints:
            fk = getattr(c, "foreign_key_constraint", None)
            if not fk:
                continue
            parent_fqn = getattr(fk, "parent_table", None)
            if not parent_fqn or parent_fqn not in in_scope:
                print(
                    f"[joins] FK on {fqn} references {parent_fqn} (out of scope; in-scope list = {sorted(in_scope)})",
                    flush=True,
                )
                continue
            child_cols = list(getattr(fk, "child_columns", []) or [])
            parent_cols = list(getattr(fk, "parent_columns", []) or [])
            key = (fqn, tuple(child_cols), parent_fqn, tuple(parent_cols))
            if key in seen:
                continue
            seen.add(key)
            joins.append({
                "left_table": fqn,
                "left_columns": child_cols,
                "right_table": parent_fqn,
                "right_columns": parent_cols,
                "relationship_type": "MANY_TO_ONE",
                "source": "uc_foreign_key",
            })
    print(f"[joins] returning {len(joins)} join(s)", flush=True)
    return joins


def _normalize_question(s):
    """Lowercase, strip punctuation, collapse whitespace — for overlap checking."""
    import re
    return re.sub(r"\s+", " ", re.sub(r"[^\w\s]", " ", (s or "").lower())).strip()


def _question_overlaps(candidate, benchmarks, threshold=0.8):
    """Return True if candidate question shares >=threshold token overlap with any benchmark."""
    cand = _normalize_question(candidate)
    if not cand:
        return False
    cand_tokens = set(cand.split())
    if not cand_tokens:
        return False
    for b in benchmarks:
        b_tokens = set(_normalize_question(b).split())
        if not b_tokens:
            continue
        jaccard = len(cand_tokens & b_tokens) / len(cand_tokens | b_tokens)
        if jaccard >= threshold:
            return True
    return False


def _strip_benchmark_overlap(questions, benchmarks):
    """Filter out questions that overlap heavily with any benchmark."""
    return [q for q in questions if not _question_overlaps(q, benchmarks)]


def _build_plan_prompt(eng, schemas=None, mv_definitions=None):
    """Build the LLM prompt from sessions 1-4 discovery data.

    `schemas`: optional {fqn: [(col, type), ...]} dict with real UC column
    lists so the LLM cannot hallucinate columns.
    `mv_definitions`: optional {fqn: ddl_string} dict with the live UC
    Metric View definitions fetched via SHOW CREATE TABLE. Takes precedence
    over any YAML stored in Session 3.
    """
    s1 = eng["sessions"]["1"]
    s2 = eng["sessions"]["2"]
    s3 = eng["sessions"]["3"]
    s4 = eng["sessions"]["4"]

    lines = []
    lines.append(f"# Genie Space: {eng.get('genie_space_name', '')}")
    lines.append(f"Business Owner: {eng.get('business_owner_name', '')}")
    lines.append(f"Analyst: {eng.get('analyst_name', '')}")
    lines.append("")

    lines.append("## Session 1: Business Context")
    for pp in s1.get("pain_points", []):
        lines.append(f"- Pain point: {pp.get('description', '')}")
    for r in s1.get("existing_reports", []):
        lines.append(f"- Existing report '{r.get('report_name','')}': {r.get('what_it_shows','')}")
    lines.append("")

    lines.append("## Session 2: Questions & Vocabulary")
    lines.append("### Question Bank (candidates for sample questions)")
    for q in s2.get("question_bank", []):
        text = q.get("question") or q.get("text") or ""
        lines.append(f"- {text}")
    lines.append("### Vocabulary & Metric Definitions")
    for v in s2.get("vocabulary_metrics", []):
        term = v.get("business_term", "")
        defn = v.get("definition") or v.get("description") or ""
        lines.append(f"- **{term}**: {defn}")
    lines.append("")

    lines.append("## Session 3: Technical Design")
    lines.append("### SQL Expressions / Measures")
    for e in s3.get("sql_expressions", []):
        lines.append(
            f"- **{e.get('metric_name','')}** on `{e.get('uc_table','')}`: "
            f"`{e.get('sql_code','')}` (display: {e.get('display_name','')}, "
            f"synonyms: {e.get('synonyms','')})"
        )
    lines.append("### Analyst Text Instructions (MUST be consolidated into one general_instructions)")
    for t in s3.get("text_instructions", []):
        lines.append(f"- **{t.get('title','')}**: {t.get('instruction','')}")
    lines.append("### Data Gaps")
    for g in s3.get("data_gaps", []):
        lines.append(f"- {g.get('gap_description','')}")
    lines.append("### Scope Boundaries")
    for s in s3.get("scope_boundaries", []):
        item = s.get("item") or s.get("topic") or ""
        lines.append(f"- {item}: {s.get('notes','')}")
    lines.append("")

    lines.append("## Session 4: COE-Approved Data Plan")
    if s4.get("analyst_commentary"):
        lines.append(f"### Analyst Commentary\n{s4.get('analyst_commentary','')}")
    lines.append("### Tables & Views in scope")
    for d in s4.get("data_plan", []):
        if d.get("include_in_space") == "Yes":
            lines.append(f"- `{d.get('table_or_view','')}` ({d.get('type','')}): {d.get('notes','')}")

    # Collect every Metric View included in Session 4's data plan. Prefer the
    # LIVE UC definition (fetched by the caller via SHOW CREATE TABLE) over the
    # YAML stored in Session 3 — the analyst may be pointing at a pre-existing
    # MV, or the MV may have been edited in UC after Session 3. Session 3 YAML
    # is the fallback so we still work offline / without a warehouse.
    mv_fqns_in_scope = [
        (d.get("table_or_view") or "").strip()
        for d in s4.get("data_plan", [])
        if d.get("include_in_space") == "Yes"
        and d.get("type") == "Metric View"
        and (d.get("table_or_view") or "").strip()
    ]
    s3_mv_fqn = (s3.get("metric_view_fqn") or "").strip()
    s3_mv_yaml = (s3.get("metric_view_yaml") or "").strip()

    mv_defs_for_prompt = {}
    for fqn in mv_fqns_in_scope:
        live = (mv_definitions or {}).get(fqn, "").strip()
        if live:
            mv_defs_for_prompt[fqn] = ("UC SHOW CREATE TABLE (live)", live)
        elif fqn == s3_mv_fqn and s3_mv_yaml:
            mv_defs_for_prompt[fqn] = ("Session 3 YAML draft (fallback)", s3_mv_yaml)

    mv_block = ""
    if mv_defs_for_prompt:
        parts = []
        for fqn, (src, body) in mv_defs_for_prompt.items():
            parts.append(f"### Metric View: `{fqn}` — source: {src}\n```\n{body}\n```")
        joined_mvs = "\n\n".join(parts)
        mv_block = f"""
<metric_view_definitions>
The Genie Space uses the following governed Metric View(s). The measures, dimensions, calcs, and filters defined IN these definitions are already governed concepts — Genie picks them up from the MV itself.

STRICT RULES:
- Do NOT emit sql_measures that duplicate any `measures:` in a definition below (by name, business meaning, or SQL expression).
- Do NOT emit sql_dimensions that duplicate any `dimensions:` or `calcs:` in a definition below.
- Do NOT emit sql_filters that duplicate any semantics already expressible via the MV's dimensions/calcs.
- sql_filters / sql_dimensions / sql_measures you DO emit must be SUPPLEMENTARY — either for the raw tables in scope that aren't covered by any MV, OR for concepts genuinely missing from the MVs.
- example_queries may reference a Metric View using its FQN — this is preferred over joining raw tables when the MV answers the question.

{joined_mvs}
</metric_view_definitions>
"""

    # Collect benchmark questions for the negative rule
    benchmark_qs = [
        (b.get("question") or "").strip()
        for b in s4.get("benchmark_questions", [])
        if (b.get("question") or "").strip()
    ]

    # Collect BO-approved benchmarks with SQL as gold-standard style exemplars
    gold_standards = []
    for b in s4.get("benchmark_questions", []):
        q = (b.get("question") or "").strip()
        sql = (b.get("expected_sql") or "").strip()
        notes = (b.get("notes") or "").strip()
        if q and sql and b.get("bo_approved"):
            gold_standards.append({"question": q, "sql": sql, "notes": notes})

    discovery = "\n".join(lines)

    benchmarks_block = ""
    if benchmark_qs:
        joined = "\n".join(f"- {q}" for q in benchmark_qs)
        benchmarks_block = f"""
<benchmark_questions>
These are the acceptance-test questions the space will be evaluated against. Do NOT include them verbatim or near-verbatim in sample_questions, example_queries, or sql_snippets — the whole point is to measure whether Genie can answer them using the OTHER configured context. Example queries should still teach the same analytical patterns, but with different wording, scope, or slice.
{joined}
</benchmark_questions>
"""

    gold_block = ""
    if gold_standards:
        parts = []
        for g in gold_standards:
            part = f"-- Q: {g['question']}\n{g['sql']}"
            if g["notes"]:
                part = f"-- Notes: {g['notes']}\n" + part
            parts.append(part)
        joined_sql = "\n\n".join(parts)
        gold_block = f"""
<gold_standard_queries>
The following SQL queries were validated by the business owner during Session 4 as correct, high-quality answers to benchmark questions. Use them as STYLE and STRUCTURE exemplars when writing example_queries and SQL snippets: column qualification conventions, filter patterns, date-arithmetic syntax, grouping choices, and formatting. Do NOT copy them verbatim into example_queries or sample_questions (they are acceptance tests for the space — see <benchmark_questions>). Mirror their style on DIFFERENT questions.
{joined_sql}
</gold_standard_queries>
"""

    schemas_block = ""
    if schemas:
        schema_parts = []
        for fqn in sorted(schemas.keys()):
            cols = schemas[fqn]
            if not cols:
                schema_parts.append(f"Table `{fqn}`: (schema unavailable)")
                continue
            col_lines = "\n".join(f"  - {name} {dtype}" for name, dtype in cols)
            schema_parts.append(f"Table `{fqn}`:\n{col_lines}")
        joined_schemas = "\n\n".join(schema_parts)
        schemas_block = f"""
<table_schemas>
These are the ACTUAL columns that exist on each in-scope table (from UC DESCRIBE). Every column referenced in sql_filters / sql_dimensions / sql_measures / example_queries MUST appear below. Do NOT invent columns. If a needed column does not exist, omit that snippet or example rather than hallucinating.
{joined_schemas}
</table_schemas>
"""

    prompt = f"""You are a Databricks Genie Space configuration expert. An analyst just completed 4 sessions of discovery with a business owner. Use this discovery to populate every instruction surface Genie supports.

<discovery_data>
{discovery}
</discovery_data>
{mv_block}{schemas_block}{gold_block}{benchmarks_block}
Genie Space instruction surfaces (in order of preference per Databricks best practices):
1. SQL Expressions (Filters / Dimensions / Measures) — reusable business concepts attached to a table
2. Example SQL queries — full SQL for complex or frequent questions
3. Text instructions — LAST RESORT for rules that can't live in data/SQL

A single high-quality SQL example teaches Genie more than 20 lines of text instruction. Push logic INTO the data where you can; use text instructions only for things that cannot be expressed as SQL.

Produce a JSON object with exactly these fields:

1. "general_instructions" (string): Short bulleted text (~400-800 chars, 15 bullets max) that will be the space's ONLY text_instruction. Include ONLY:
   - Space scope/purpose (1 bullet)
   - Business-jargon → data mappings not captured as SQL expressions
   - Global response/formatting standards (date format, rounding, required columns)
   - Clarification triggers ("if user asks X without a date range, ask them to specify")
   - Terminology synonyms not captured elsewhere
   Do NOT restate metric definitions — those belong in sql_measures. Do NOT describe table/column semantics — those belong in UC descriptions. Use short atomic bullets starting with "- ". No markdown headers.

2. "sample_questions" (array of 5-8 strings): Curated, reworded sample questions from the question bank. Clear, natural phrasing, covering main use cases. Shown to users when they open the space.

IMPORTANT SQL qualification rule for snippets below: Genie infers the table from qualified column references in the SQL. Every column reference in snippet SQL MUST be prefixed with the SHORT table name (the last segment of the FQN). Example: for table `my_catalog.my_schema.orders`, write `orders.status`, NOT `status` and NOT `my_catalog.my_schema.orders.status`. The `table` field in each entry is metadata for the analyst UI and is NOT pushed to Genie.

3. "sql_filters" (array): Reusable WHERE-clause expressions. Each: {{"name": "snake_case_id", "sql": "short_table.column = 'value'", "table": "catalog.schema.table", "display_name": "Friendly Name", "synonyms": ["..."], "description": "..."}}. Example: {{"name": "cancelled_orders", "sql": "orders.status = 'CANCELLED'", "table": "my_catalog.my_schema.orders", "display_name": "Cancelled Orders"}}

4. "sql_dimensions" (array): Reusable grouping/SELECT column expressions. Same shape as sql_filters. Example: {{"name": "order_year", "sql": "YEAR(orders.created_at)", "table": "my_catalog.my_schema.orders", "display_name": "Order Year"}}

5. "sql_measures" (array): Reusable aggregate expressions (COUNT/SUM/AVG/etc). Same shape. Seed from the analyst's Session 3 SQL Expressions — classify each as filter/dimension/measure based on its SQL (aggregates → measure; WHERE-style predicates → filter; plain column exprs → dimension). Validate syntax and rewrite column references to use the short table prefix (e.g., rewrite `COUNT(CASE WHEN status = 'CANCELLED' THEN 1 END) * 100.0 / COUNT(*)` on table `my_catalog.my_schema.orders` to `COUNT(CASE WHEN orders.status = 'CANCELLED' THEN 1 END) * 100.0 / COUNT(orders.*)`).

6. "example_queries" (array, 3-6 items): Full SQL examples for complex/common questions from the question bank. Each: {{"question": "...", "sql": "...", "draft": true, "usage_guidance": "..."}}. SQL MUST use fully qualified `catalog.schema.table` references because example queries are standalone. Only include questions where you can write reasonably confident SQL given the tables in scope — skip speculative ones. Always set "draft": true so analyst reviews.

7. "narrative" (string): 2-4 sentences explaining what this space does, who it serves, and what was configured. Shown to the analyst before push.

Return ONLY the JSON object. No markdown fences, no preamble, no trailing commentary. Begin with {{ and end with }}."""

    return prompt


def _call_llm(prompt):
    """Call the Databricks serving endpoint with the prompt. Returns parsed JSON."""
    resp = w.serving_endpoints.query(
        name=LLM_ENDPOINT,
        messages=[ChatMessage(role=ChatMessageRole.USER, content=prompt)],
        max_tokens=16000,
        temperature=0.2,
    )
    # Response shape varies: SDK object, dict, or OpenAI-style object
    if isinstance(resp, dict):
        d = resp
    elif hasattr(resp, "as_dict"):
        d = resp.as_dict()
    else:
        d = {"choices": [{"message": {"content": resp.choices[0].message.content}}]}
    content = d["choices"][0]["message"]["content"]

    # Strip markdown fences if model ignored the instruction
    text = content.strip()
    if text.startswith("```"):
        text = text.split("\n", 1)[1] if "\n" in text else text
        if text.endswith("```"):
            text = text.rsplit("\n", 1)[0] if "\n" in text else text[:-3]
        text = text.strip()
        if text.startswith("json"):
            text = text[4:].strip()

    return json.loads(text)


@app.route("/api/engagements/<eid>/generate-plan", methods=["POST"])
def generate_plan(eid):
    """Use the configured LLM to synthesize a Genie Space configuration plan."""
    rows = sql_exec(f"SELECT * FROM {TABLE} WHERE engagement_id = :eid", {"eid": eid})
    if not rows:
        return jsonify({"error": "Not found"}), 404
    eng = parse_row(rows[0])

    body = request.get_json(silent=True) or {}
    warehouse_id = (body.get("warehouse_id") or "").strip()

    warnings = []
    s4 = eng["sessions"]["4"]

    # Resolve in-scope raw tables (metric views excluded from DESCRIBE since they
    # live in UC but their columns are derived — the prompt still references the
    # MV FQN from the data plan separately).
    scope_tables = [
        d.get("table_or_view", "")
        for d in s4.get("data_plan", [])
        if d.get("include_in_space") == "Yes" and d.get("type") != "Metric View"
    ]
    scope_tables = [t for t in scope_tables if t and t.count(".") == 2]

    scope_mv_fqns = [
        (d.get("table_or_view") or "").strip()
        for d in s4.get("data_plan", [])
        if d.get("include_in_space") == "Yes" and d.get("type") == "Metric View"
    ]
    scope_mv_fqns = [t for t in scope_mv_fqns if t and t.count(".") == 2]

    user_w = _user_workspace_client()

    # Resolve a warehouse once; both schema DESCRIBE and MV SHOW CREATE TABLE
    # need it under OBO. Prefer UI-supplied warehouse_id, else first visible.
    wh_to_use = warehouse_id
    if not wh_to_use and user_w is not None and (scope_tables or scope_mv_fqns):
        try:
            whs = list(user_w.warehouses.list())
            if whs:
                wh_to_use = whs[0].id
                print(f"[generate-plan] auto-selected warehouse {wh_to_use}", flush=True)
        except Exception as e:
            print(f"[generate-plan] warehouse auto-select failed: {e}", flush=True)

    # Fetch real UC column schemas to ground the prompt.
    schemas = {}
    if scope_tables:
        if wh_to_use:
            try:
                for t in scope_tables:
                    schemas[t] = _describe_table_columns(t, user_w, wh_to_use)
                missing = [t for t, cols in schemas.items() if not cols]
                if missing:
                    warnings.append(
                        f"Could not describe {len(missing)} table(s) under your permissions: "
                        + ", ".join(missing[:3]) + ("..." if len(missing) > 3 else "")
                    )
            except Exception as e:
                print(f"[generate-plan] schema fetch failed: {e}", flush=True)
                warnings.append("Schema grounding failed; LLM may hallucinate columns. See server logs.")
        else:
            warnings.append("No warehouse available for schema grounding; LLM may hallucinate columns.")

    # Fetch live Metric View definitions from UC so the LLM sees the real,
    # current measures/dimensions/filters — not a potentially-stale Session 3
    # draft. Falls back to Session 3 YAML inside _build_plan_prompt.
    mv_definitions = {}
    if scope_mv_fqns and wh_to_use:
        for fqn in scope_mv_fqns:
            ddl = _fetch_metric_view_definition(fqn, user_w, wh_to_use)
            if ddl:
                mv_definitions[fqn] = ddl
        missing_mvs = [f for f in scope_mv_fqns if f not in mv_definitions]
        if missing_mvs:
            warnings.append(
                "Could not fetch live definition for "
                f"{len(missing_mvs)} metric view(s) from UC: "
                + ", ".join(missing_mvs[:3]) + ("..." if len(missing_mvs) > 3 else "")
                + ". Falling back to Session 3 YAML if available."
            )

    try:
        prompt = _build_plan_prompt(eng, schemas=schemas, mv_definitions=mv_definitions)
        plan = _call_llm(prompt)
    except Exception as e:
        tb = traceback.format_exc()
        print(f"[generate-plan] ERROR: {e}\n{tb}", flush=True)
        return jsonify({
            "error": f"{type(e).__name__}: {e}",
            "endpoint": LLM_ENDPOINT,
            "traceback": tb.splitlines()[-5:],
        }), 500

    # Normalize shape
    def _norm_list(v):
        return v if isinstance(v, list) else []

    general_instructions = str(plan.get("general_instructions", "")).strip()
    sample_questions = [str(q).strip() for q in _norm_list(plan.get("sample_questions")) if str(q).strip()]
    sql_filters = _norm_list(plan.get("sql_filters"))
    sql_dimensions = _norm_list(plan.get("sql_dimensions"))
    sql_measures = _norm_list(plan.get("sql_measures"))
    example_queries = _norm_list(plan.get("example_queries"))
    narrative = str(plan.get("narrative", "")).strip()

    # Belt-and-suspenders: strip any sample_questions / example_queries that
    # overlap with Session 4 benchmark questions. Benchmarks are the acceptance
    # test — they MUST NOT appear as configured answers, otherwise Genie just
    # memorizes them and we lose drift-detection.
    benchmark_qs = [
        (b.get("question") or "").strip()
        for b in s4.get("benchmark_questions", [])
        if (b.get("question") or "").strip()
    ]
    if benchmark_qs:
        before_sq = len(sample_questions)
        sample_questions = _strip_benchmark_overlap(sample_questions, benchmark_qs)
        stripped_sq = before_sq - len(sample_questions)

        before_eq = len(example_queries)
        example_queries = [
            eq for eq in example_queries
            if not _question_overlaps(eq.get("question", ""), benchmark_qs)
        ]
        stripped_eq = before_eq - len(example_queries)

        if stripped_sq:
            warnings.append(
                f"Removed {stripped_sq} sample question(s) that overlapped with Session 4 benchmarks."
            )
        if stripped_eq:
            warnings.append(
                f"Removed {stripped_eq} example query/queries that overlapped with Session 4 benchmarks."
            )

    # Fetch UC PK/FK joins for tables in Session 4's data plan (NOT LLM-generated).
    # Preserve any manually-entered joins the analyst already saved in Session 5.
    existing_joins = eng["sessions"]["5"].get("plan_joins") or []
    manual_joins = [j for j in existing_joins if j.get("source") == "manual"]
    try:
        uc_joins = _fetch_uc_joins(scope_tables, client=user_w or w)
    except Exception as e:
        print(f"[generate-plan] UC join fetch failed: {e}", flush=True)
        uc_joins = []
    joins = uc_joins + manual_joins

    # Persist to session 5
    ts = now_ts()
    sql_run(
        f"UPDATE {TABLE} SET "
        f"plan_general_instructions = :gi, plan_sample_questions = :sq, "
        f"plan_sql_filters = :sf, plan_sql_dimensions = :sd, plan_sql_measures = :sm, "
        f"plan_example_queries = :eq, plan_joins = :jn, "
        f"plan_narrative = :nar, updated_at = :ts "
        f"WHERE engagement_id = :eid",
        {
            "eid": eid,
            "gi": general_instructions,
            "sq": json.dumps(sample_questions),
            "sf": json.dumps(sql_filters),
            "sd": json.dumps(sql_dimensions),
            "sm": json.dumps(sql_measures),
            "eq": json.dumps(example_queries),
            "jn": json.dumps(joins),
            "nar": narrative,
            "ts": ts,
        },
    )

    return jsonify({
        "general_instructions": general_instructions,
        "sample_questions": sample_questions,
        "sql_filters": sql_filters,
        "sql_dimensions": sql_dimensions,
        "sql_measures": sql_measures,
        "example_queries": example_queries,
        "joins": joins,
        "narrative": narrative,
        "warnings": warnings,
    })


# ---------------------------------------------------------------------------
# API: Benchmark drafting (Session 4)
# ---------------------------------------------------------------------------

def _build_benchmark_draft_prompt(eng, count=12):
    """Draft benchmark questions from Sessions 1-3 context."""
    s1 = eng["sessions"]["1"]
    s2 = eng["sessions"]["2"]
    s3 = eng["sessions"]["3"]
    s4 = eng["sessions"]["4"]

    lines = []
    lines.append(f"Genie Space: {eng.get('genie_space_name', '')}")
    lines.append("Pain Points:")
    for pp in s1.get("pain_points", []):
        lines.append(f"- {pp.get('description', '')}")
    lines.append("Question Bank (from business owner):")
    for q in s2.get("question_bank", []):
        lines.append(f"- {q.get('question') or q.get('text') or ''}")
    lines.append("Key Metrics / SQL Expressions:")
    for e in s3.get("sql_expressions", []):
        lines.append(f"- {e.get('metric_name','')} ({e.get('display_name','')}): `{e.get('sql_code','')}` on {e.get('uc_table','')}")
    lines.append("Tables in scope:")
    for d in s4.get("data_plan", []):
        if d.get("include_in_space") == "Yes":
            lines.append(f"- {d.get('table_or_view','')}")
    context = "\n".join(lines)

    overgen = count + 10
    return f"""You are drafting benchmark questions for a Databricks Genie Space. Benchmarks are the acceptance-test set — the space will be measured by how many it answers correctly (>80% target). They represent what a business user would actually ask.

<engagement_context>
{context}
</engagement_context>

Your task: return exactly {count} benchmark questions, ranked by importance. If the business owner only got to test {count} questions, these should be the {count} that best prove whether the Genie Space works for their real job.

Method (do this silently — do not output your working):
1. First brainstorm a candidate pool of ~{overgen} plausible benchmark questions covering the full engagement context.
2. Score each candidate on: coverage of pain points, alignment with the BO's own question bank phrasing, reuse of the key metrics the analyst mapped, coverage of in-scope tables, and realism as a question a business user would actually ask.
3. From the {overgen} candidates, pick the top {count} by overall value. Drop duplicates, near-duplicates, and low-value questions.
4. Order the final {count} from highest-value to lowest-value.

Final output — a JSON array with exactly {count} items. Each item:
{{
  "question": "Natural-language question a business user would ask",
  "category": "Core" or "Edge Case",
  "difficulty": "Easy" or "Medium" or "Hard"
}}

Constraints on the final {count}:
- Every major pain point is tested at least once.
- Every in-scope table appears in at least one question.
- About 70% Core (realistic questions), 30% Edge Case (ambiguous phrasing, boundary conditions, synonym tests, trick wording).
- Difficulty reflects SQL complexity: Easy = single table, simple filter; Medium = aggregation + group by; Hard = multi-table joins or subqueries.
- Include a mix of time-bound (last quarter, YTD) and aggregation styles.
- Prefer the business owner's own phrasing when a matching question exists in their question bank.
- Do NOT draft SQL — just the questions. SQL will be drafted per-row later.

Return ONLY the JSON array of {count} final picks, highest-value first. No markdown fences, no preamble, no commentary about the candidate pool."""


@app.route("/api/engagements/<eid>/draft-benchmarks", methods=["POST"])
def draft_benchmarks(eid):
    """Draft benchmark questions from Sessions 1-3 context."""
    rows = sql_exec(f"SELECT * FROM {TABLE} WHERE engagement_id = :eid", {"eid": eid})
    if not rows:
        return jsonify({"error": "Not found"}), 404
    eng = parse_row(rows[0])

    body = request.json or {}
    try:
        count = int(body.get("count") or 12)
    except (TypeError, ValueError):
        count = 12
    count = max(1, min(50, count))

    try:
        prompt = _build_benchmark_draft_prompt(eng, count=count)
        result = _call_llm(prompt)
    except Exception as e:
        tb = traceback.format_exc()
        print(f"[draft-benchmarks] ERROR: {e}\n{tb}", flush=True)
        return jsonify({"error": f"{type(e).__name__}: {e}"}), 500

    # Normalize — LLM might return a dict with "benchmarks" key or a raw array
    if isinstance(result, dict):
        items = result.get("benchmarks") or result.get("questions") or []
    else:
        items = result or []

    drafted = []
    for item in items:
        if not isinstance(item, dict):
            continue
        q = str(item.get("question", "")).strip()
        if not q:
            continue
        cat = str(item.get("category", "Core")).strip()
        if cat not in ("Core", "Edge Case"):
            cat = "Core"
        diff = str(item.get("difficulty", "Medium")).strip()
        if diff not in ("Easy", "Medium", "Hard"):
            diff = "Medium"
        drafted.append({
            "question": q,
            "category": cat,
            "difficulty": diff,
            "expected_sql": "",
            "notes": "",
            "bo_approved": False,
        })

    return jsonify({"benchmarks": drafted})


@app.route("/api/engagements/<eid>/draft-benchmark-sql", methods=["POST"])
def draft_benchmark_sql(eid):
    """Draft SQL for a single benchmark question using Session 3 technical context."""
    rows = sql_exec(f"SELECT * FROM {TABLE} WHERE engagement_id = :eid", {"eid": eid})
    if not rows:
        return jsonify({"error": "Not found"}), 404
    eng = parse_row(rows[0])

    body = request.json or {}
    question = (body.get("question") or "").strip()
    warehouse_id = (body.get("warehouse_id") or "").strip()
    if not question:
        return jsonify({"error": "question is required"}), 400

    s3 = eng["sessions"]["3"]
    s4 = eng["sessions"]["4"]

    # Collect actual column schemas for every in-scope table so the LLM has
    # ground truth to reference and doesn't invent columns. Uses OBO so we
    # inherit the user's UC grants.
    user_w = _user_workspace_client()
    in_scope_tables = [
        (d.get("table_or_view") or "").strip()
        for d in s4.get("data_plan", [])
        if d.get("include_in_space") == "Yes" and (d.get("table_or_view") or "").count(".") == 2
    ]
    schema_blocks = []
    for t in in_scope_tables:
        cols = _describe_table_columns(t, user_w, warehouse_id) if warehouse_id else []
        if cols:
            col_lines = "\n".join(f"  - {c[0]} ({c[1]})" for c in cols)
            schema_blocks.append(f"{t}:\n{col_lines}")
        else:
            # Fall back to listing just the table; the column-constraint rule
            # below still instructs the LLM not to invent columns.
            schema_blocks.append(f"{t}:\n  (schema unavailable — be conservative with column references)")
    schemas_text = "\n\n".join(schema_blocks) if schema_blocks else "(no tables in scope)"

    metric_lines = []
    for e in s3.get("sql_expressions", []):
        metric_lines.append(
            f"- {e.get('metric_name','')}: `{e.get('sql_code','')}` on {e.get('uc_table','')}"
        )
    metrics_text = "\n".join(metric_lines) if metric_lines else "(none)"

    prompt = f"""Draft the expected SQL answer for one benchmark question. This SQL will run on Databricks SQL (Spark SQL / ANSI dialect) and is the ground-truth query the Genie Space will be scored against.

<table_schemas>
The following are the ONLY tables you may reference, with their exact columns and types. Every column you use in the SQL MUST appear below. Do not invent, rename, or guess column names. If the question seems to require a column that is not listed, pick the closest real column OR leave a SQL comment explaining the gap — do not fabricate.

{schemas_text}
</table_schemas>

<known_metrics>
These are the analyst-mapped SQL expressions. Reuse them verbatim when the benchmark question involves the same measure:
{metrics_text}
</known_metrics>

<dialect_notes>
Write Databricks SQL (Spark SQL). Common gotchas — do NOT use Postgres/MySQL/T-SQL syntax:
- DATE_ADD(date, n) takes an INTEGER number of days, NOT an INTERVAL. Use ADD_MONTHS(date, n) for months and date arithmetic with + INTERVAL for other units.
- For "N months ago": ADD_MONTHS(CURRENT_DATE, -N) or CURRENT_DATE - INTERVAL N MONTH.
- For quarter boundaries: DATE_TRUNC('QUARTER', CURRENT_DATE) for current quarter start; ADD_MONTHS(DATE_TRUNC('QUARTER', CURRENT_DATE), -3) for previous quarter start.
- Use DATEDIFF(end, start) (end-start in days) — note Databricks order is (end, start), not Postgres' (start, end).
- Use DOUBLE division (COUNT(...) * 100.0 / COUNT(*)) to avoid integer truncation.
- No ILIKE on columns unless needed (LIKE is case-sensitive by default; use LOWER() for case-insensitive compares).
- String literals use single quotes. Do not use double quotes for strings — double quotes are identifier quoting in ANSI mode.
</dialect_notes>

Benchmark question:
{question}

Return JSON with exactly:
{{
  "sql": "the SQL query"
}}

Rules:
- Use fully qualified table references (catalog.schema.table) — example queries are standalone.
- Reuse the known SQL expressions where they apply.
- ONLY reference columns that appear in <table_schemas>. Double-check every column name against the list before emitting it. If you're uncertain, prefer a column that clearly exists over one you remember by naming convention.
- Follow the <dialect_notes> — this is Databricks SQL, not Postgres/MySQL.
- Single SQL statement. If you need CTEs, use WITH.
- Return ONLY the JSON. No markdown fences, no preamble."""

    try:
        result = _call_llm(prompt)
        if isinstance(result, dict):
            sql_text = str(result.get("sql", "")).strip()
        else:
            sql_text = str(result or "").strip()
        # Strip code fences if any
        if sql_text.startswith("```"):
            sql_text = sql_text.split("\n", 1)[1] if "\n" in sql_text else sql_text
            if sql_text.endswith("```"):
                sql_text = sql_text.rsplit("\n", 1)[0] if "\n" in sql_text else sql_text[:-3]
            sql_text = sql_text.strip()
            if sql_text.lower().startswith("sql"):
                sql_text = sql_text[3:].lstrip()
    except Exception as e:
        return jsonify({"error": f"{type(e).__name__}: {e}"}), 500

    # Second LLM call — summary is derived from the final SQL, not from the
    # question. Guarantees the plain-English explanation describes what the
    # query actually does.
    explanation = ""
    if sql_text:
        try:
            explanation = _summarize_benchmark_sql(question, sql_text)
        except Exception as e:
            print(f"[draft-benchmark-sql] summary generation failed: {type(e).__name__}: {e}", flush=True)

    return jsonify({"sql": sql_text, "explanation": explanation})


def _summarize_benchmark_sql(question, sql_text):
    """Given the final SQL, produce a plain-English summary. This runs as a
    second LLM call AFTER the SQL is generated so the summary strictly
    describes what the query does, not what the question intends."""
    prompt = f"""Describe what this benchmark SQL is measuring, in plain English for a non-technical business owner.

<benchmark_question>
{question}
</benchmark_question>

<sql>
{sql_text}
</sql>

Write 2-3 sentences answering "how are we measuring this?" — no column names in backticks, no SQL jargon. Example voice: "Counts every claim received in the current calendar year, excluding voided records, and averages the number of days between receipt and final decision." Your summary must describe the SQL exactly as written, including any filters or groupings it applies. Do not describe anything the SQL doesn't actually do.

Return JSON with exactly: {{"explanation": "..."}}. No markdown fences."""
    result = _call_llm(prompt)
    if isinstance(result, dict):
        return str(result.get("explanation", "")).strip()
    return ""


@app.route("/api/engagements/<eid>/draft-benchmark-summary", methods=["POST"])
def draft_benchmark_summary(eid):
    """Draft a plain-English summary of existing SQL for a benchmark question."""
    body = request.json or {}
    question = (body.get("question") or "").strip()
    sql_text = (body.get("sql") or "").strip()
    if not question or not sql_text:
        return jsonify({"error": "question and sql are required"}), 400

    try:
        explanation = _summarize_benchmark_sql(question, sql_text)
    except Exception as e:
        return jsonify({"error": f"{type(e).__name__}: {e}"}), 500

    return jsonify({"explanation": explanation})


@app.route("/api/engagements/<eid>/run-benchmark-sql", methods=["POST"])
def run_benchmark_sql(eid):
    """Execute benchmark SQL via OBO and return a sample of rows for BO review."""
    rows = sql_exec(f"SELECT * FROM {TABLE} WHERE engagement_id = :eid", {"eid": eid})
    if not rows:
        return jsonify({"error": "Not found"}), 404

    body = request.json or {}
    sql_text = (body.get("sql") or "").strip()
    warehouse_id = (body.get("warehouse_id") or "").strip()
    if not sql_text:
        return jsonify({"error": "sql is required"}), 400
    if not warehouse_id:
        return jsonify({"error": "warehouse_id is required"}), 400

    user_w = _user_workspace_client()
    if not user_w:
        return jsonify({"error": "User auth unavailable — reload the app so OBO token is present"}), 401

    # Strip trailing semicolons so the wrapper doesn't break
    stmt = sql_text.rstrip().rstrip(";").strip()

    # Always wrap in an outer LIMIT so the BO preview can't pull huge result
    # sets. Wrapping is safe whether the inner query has its own LIMIT,
    # ORDER BY, or CTEs — the outer cap applies to whatever the inner returns.
    limit_cap = 50
    wrapped = f"SELECT * FROM (\n{stmt}\n) __bm LIMIT {limit_cap}"

    try:
        # wait_timeout=50s lets the call block up to 50s before returning
        # PENDING/RUNNING. We still poll below so a cold warehouse start
        # doesn't surface as a misleading error.
        resp = user_w.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=wrapped,
            wait_timeout="50s",
        )
    except Exception as e:
        return jsonify({"error": f"{type(e).__name__}: {e}"}), 200

    statement_id = resp.statement_id
    state = str(resp.status.state) if resp.status else ""
    import time as _time
    deadline = _time.time() + 120  # up to 2 min total (warehouse cold start + query)
    while statement_id and "SUCCEEDED" not in state and "FAILED" not in state and "CANCELED" not in state and "CLOSED" not in state:
        if _time.time() > deadline:
            try:
                user_w.statement_execution.cancel_execution(statement_id)
            except Exception:
                pass
            return jsonify({"error": "Query timed out after 2 minutes waiting for the warehouse"}), 200
        _time.sleep(1.5)
        try:
            resp = user_w.statement_execution.get_statement(statement_id)
        except Exception as e:
            return jsonify({"error": f"{type(e).__name__}: {e}"}), 200
        state = str(resp.status.state) if resp.status else ""

    if "SUCCEEDED" not in state:
        err = resp.status.error.message if (resp.status and resp.status.error) else f"Statement state: {state}"
        return jsonify({"error": err}), 200

    columns: list[str] = []
    out_rows: list[list] = []
    row_count = 0
    if resp.manifest and resp.manifest.schema and resp.manifest.schema.columns:
        columns = [c.name for c in resp.manifest.schema.columns]
    if resp.result and resp.result.data_array:
        out_rows = [list(r) for r in resp.result.data_array]
        row_count = len(out_rows)
    truncated = row_count >= limit_cap

    return jsonify({
        "columns": columns,
        "rows": out_rows,
        "row_count": row_count,
        "truncated": truncated,
        "limit": limit_cap,
    })


# ---------------------------------------------------------------------------
# API: Metric View authoring (Session 3)
# ---------------------------------------------------------------------------

def _fetch_metric_view_definition(fqn, user_w=None, warehouse_id=None):
    """Return the live UC definition of a Metric View as a string (DDL / YAML).

    Uses SHOW CREATE TABLE under OBO so we inherit the user's UC grants. This
    is the source of truth — the YAML stored in Session 3 could be stale or
    absent if the analyst pointed at a pre-existing MV.
    """
    parts = fqn.split(".")
    if len(parts) != 3 or not (user_w and warehouse_id):
        return ""
    stmt = f"SHOW CREATE TABLE `{parts[0]}`.`{parts[1]}`.`{parts[2]}`"
    try:
        resp = user_w.statement_execution.execute_statement(
            warehouse_id=warehouse_id, statement=stmt,
        )
        state = str(resp.status.state) if resp.status else ""
        if "SUCCEEDED" not in state or not resp.result or not resp.result.data_array:
            print(f"[mv-fetch] {fqn}: state={state}", flush=True)
            return ""
        # SHOW CREATE TABLE returns a single row with a single column (createtab_stmt).
        row = resp.result.data_array[0]
        return str(row[0]) if row else ""
    except Exception as e:
        print(f"[mv-fetch] {fqn} failed: {type(e).__name__}: {e}", flush=True)
        return ""


def _describe_table_columns(fqn, user_w=None, warehouse_id=None):
    """Return a list of (column_name, data_type) for a three-part UC table.

    When `user_w` + `warehouse_id` are provided, run DESCRIBE via OBO so we
    inherit the USER's UC grants (required for personal catalogs the app SP
    can't see). Otherwise fall back to the SP client for back-compat paths.
    """
    parts = fqn.split(".")
    if len(parts) != 3:
        return []
    stmt = f"DESCRIBE TABLE `{parts[0]}`.`{parts[1]}`.`{parts[2]}`"
    rows = []
    try:
        if user_w is not None and warehouse_id:
            resp = user_w.statement_execution.execute_statement(
                warehouse_id=warehouse_id, statement=stmt,
            )
            state = str(resp.status.state) if resp.status else ""
            if "SUCCEEDED" in state and resp.result and resp.result.data_array and resp.manifest:
                cols = [c.name for c in resp.manifest.schema.columns]
                rows = [dict(zip(cols, r)) for r in resp.result.data_array]
        else:
            rows = sql_exec(stmt)
    except Exception:
        return []
    out = []
    for r in rows:
        name = r.get("col_name") or r.get("column_name") or r.get("name") or ""
        if not name or name.startswith("#"):
            continue
        out.append((name, r.get("data_type") or r.get("type") or ""))
    return out


def _collect_engagement_schemas(eng, user_w=None, warehouse_id=None):
    """Return {fqn: [(col, type), ...]} for every table referenced in Session 3."""
    tables = set()
    for e in eng["sessions"]["3"].get("sql_expressions", []):
        t = (e.get("uc_table") or "").strip()
        if t and t.count(".") == 2:
            tables.add(t)
    for d in eng["sessions"]["4"].get("data_plan", []):
        t = (d.get("table_or_view") or "").strip()
        if t and t.count(".") == 2 and d.get("type") != "Metric View":
            tables.add(t)
    return {t: _describe_table_columns(t, user_w, warehouse_id) for t in sorted(tables)}


def _build_mv_yaml_prompt(eng, user_w=None, warehouse_id=None):
    """Build the LLM prompt to draft a UC Metric View YAML from Sessions 1-3."""
    s1 = eng["sessions"]["1"]
    s2 = eng["sessions"]["2"]
    s3 = eng["sessions"]["3"]

    schemas = _collect_engagement_schemas(eng, user_w, warehouse_id)

    lines = []
    lines.append(f"Genie Space: {eng.get('genie_space_name', '')}")
    lines.append("")

    # S1 business context
    bc = s1.get("business_context", [])
    if bc:
        lines.append("## Business Context (S1)")
        for b in bc:
            if isinstance(b, dict):
                lines.append(f"- **{b.get('topic','')}**: {b.get('detail') or b.get('description') or ''}")
            else:
                lines.append(f"- {b}")
        lines.append("")

    lines.append("## Pain Points (S1)")
    for pp in s1.get("pain_points", []):
        lines.append(f"- {pp.get('description', '')}")
    lines.append("")

    er = s1.get("existing_reports", [])
    if er:
        lines.append("## Existing Reports (S1) — metrics analysts already produce today")
        for r in er:
            if isinstance(r, dict):
                lines.append(f"- **{r.get('report_name') or r.get('name','')}**: {r.get('description') or r.get('key_metrics') or ''}")
            else:
                lines.append(f"- {r}")
        lines.append("")

    lines.append("## Business Questions (S2)")
    for q in s2.get("question_bank", []):
        lines.append(f"- {q.get('question') or q.get('text') or ''}")
    lines.append("")
    lines.append("## Vocabulary & Metric Definitions (S2)")
    for v in s2.get("vocabulary_metrics", []):
        lines.append(f"- **{v.get('business_term','')}**: {v.get('definition') or v.get('description') or ''}")
    lines.append("")

    tc = s3.get("term_classifications", [])
    if tc:
        lines.append("## Term Classifications (S3) — how each business term was categorized")
        for t in tc:
            if isinstance(t, dict):
                lines.append(f"- **{t.get('term','')}** → {t.get('classification','')}: {t.get('rationale') or ''}")
        lines.append("")

    sb = s3.get("scope_boundaries", [])
    if sb:
        lines.append("## Scope Boundaries (S3) — what is IN/OUT of scope")
        for b in sb:
            if isinstance(b, dict):
                lines.append(f"- **{b.get('topic','')}** ({b.get('status','')}): {b.get('rationale') or b.get('description') or ''}")
            else:
                lines.append(f"- {b}")
        lines.append("")

    dg = s3.get("data_gaps", [])
    if dg:
        lines.append("## Data Gaps (S3) — things the analyst could NOT map; do NOT try to express these")
        for g in dg:
            if isinstance(g, dict):
                lines.append(f"- **{g.get('gap') or g.get('topic','')}**: {g.get('description') or g.get('detail') or ''}")
            else:
                lines.append(f"- {g}")
        lines.append("")

    gf = (s3.get("global_filter") or "").strip()
    if gf:
        lines.append("## Global Filter (S3) — THE ANALYST SPECIFIED THIS FILTER APPLIES TO EVERY METRIC")
        lines.append(f"```\n{gf}\n```")
        lines.append("Copy this verbatim into the metric view's top-level `filter:` key. Do NOT attempt to restructure or reinterpret it.")
        lines.append("")

    lines.append("## Table Schemas (authoritative column list — do NOT reference columns not listed here)")
    for fqn, cols in schemas.items():
        if not cols:
            lines.append(f"- `{fqn}`: (schema unavailable — be extra careful, only use columns the analyst explicitly mapped)")
            continue
        col_str = ", ".join(f"{c} {t}" for c, t in cols)
        lines.append(f"- `{fqn}`: {col_str}")
    lines.append("")
    lines.append("## Analyst-mapped SQL Expressions (THE CORE INPUT)")
    for e in s3.get("sql_expressions", []):
        lines.append(
            f"- **{e.get('metric_name','')}** on `{e.get('uc_table','')}`: "
            f"`{e.get('sql_code','')}` "
            f"(display: {e.get('display_name','')}, synonyms: {e.get('synonyms','')})"
        )
    lines.append("")
    lines.append("## Analyst Text Instructions / Rules (S3)")
    for t in s3.get("text_instructions", []):
        lines.append(f"- **{t.get('title','')}**: {t.get('instruction','')}")

    context = "\n".join(lines)

    return f"""You are a Databricks Unity Catalog Metric View expert. An analyst has mapped all the business terms, metrics, and rules. Synthesize a complete, spec-compliant metric view YAML (v1.1) from this discovery.

<engagement_context>
{context}
</engagement_context>

<metric_view_yaml_spec>
Valid TOP-LEVEL keys (and ONLY these):
- `version: 1.1` (required, literal)
- `comment` (optional string): description of the metric view
- `source` (required string): the fact/base table as a three-part UC name (`catalog.schema.table`), OR a SQL query string
- `filter` (optional string): a SQL boolean expression applied to every query
- `joins` (optional array): star/snowflake schema joins
- `dimensions` (array): column definitions usable in SELECT/WHERE/GROUP BY (non-aggregates)
- `measures` (array): aggregate expression definitions
- `materialization` (optional): query acceleration config — OMIT unless the analyst explicitly requested it

DO NOT invent keys. There is NO `instructions` key, NO `text_instructions` key, NO `glossary` key. Business rules that are not expressible as a `filter`, dimension, or measure belong on the Genie Space (NOT in the metric view YAML).

DIMENSION fields:
- `name` (required): the dimension alias. Use snake_case identifiers (lowercase letters, digits, underscores). This is how queries reference the dimension. Do NOT use spaces in `name`.
- `expr` (required): SQL expression, scalar, NO aggregate functions
- `comment` (optional): description, appears in Unity Catalog
- `display_name` (optional, <=255 chars): human-readable label for visualization tools (THIS is where you put "Claim ID", "Receipt Date", etc.)
- `synonyms` (optional array, up to 10 strings, each <=255 chars): alternative names for LLM tools. PUT ALL SYNONYMS HERE instead of as separate instructions.

MEASURE fields:
- `name` (required): snake_case identifier. Referenced via `MEASURE(name)` in queries. Do NOT use spaces.
- `expr` (required): aggregate SQL expression (must include COUNT/SUM/AVG/MAX/MIN/etc). Supports FILTER (WHERE ...) clauses.
- `comment`, `display_name`, `synonyms`: same as dimensions

JOIN fields:
- `name` (required): alias for the joined table
- `source` (required): three-part name of the joined table
- `on` OR `using` (one required): join condition. Use `source` prefix to refer to the metric view's base source: `on: source.l_orderkey = orders.o_orderkey`
- `joins` (optional): nested joins for snowflake schemas

FORMATTING rules:
- Column names with spaces or special chars must be escaped with backticks. If the expression starts with a backtick, wrap the whole value in double quotes.
- YAML interprets unquoted colons as key-value separators — wrap any expression containing a colon in double quotes.
- Use `|` block scalar for multi-line expressions.

NAMING convention that has proven reliable:
- `name`: `snake_case_identifier` — no spaces, no quotes needed
- `display_name`: `'Human Readable Label'` — quoted, spaces allowed
- `synonyms`: `['alt1', 'alt2']` — short list

EXAMPLE (follow this structure exactly):
```
version: 1.1
comment: "Claims analytics metric view"
source: catalog.schema.claims
filter: voided_flag = 'N' AND test_flag = 'N'

dimensions:
  - name: claim_id
    expr: claim_id
    display_name: 'Claim ID'
  - name: claim_quarter
    expr: CONCAT('Q', QUARTER(receipt_date), ' ', YEAR(receipt_date))
    display_name: 'Claim Quarter'
    comment: 'Calendar quarter of claim receipt'
  - name: initial_decision
    expr: initial_decision
    display_name: 'Initial Decision'
    synonyms: ['denial status', 'decision', 'outcome']

measures:
  - name: total_claims
    expr: COUNT(1)
    display_name: 'Total Claims'
  - name: denied_claims_count
    expr: COUNT(1) FILTER (WHERE initial_decision = 'DENIED')
    display_name: 'Denied Claims'
    synonyms: ['denials', 'rejections']
  - name: denial_rate_pct
    expr: COUNT(1) FILTER (WHERE initial_decision = 'DENIED') * 100.0 / COUNT(1)
    display_name: 'Denial Rate'
    comment: 'Percent of claims denied on initial adjudication'
  # Notice: measure name (denial_rate_pct) does NOT collide with any column
  # referenced in expr (initial_decision). The suffix _pct / _count makes this safe.
  - name: first_pass_rate_pct
    expr: COUNT(1) FILTER (WHERE first_pass_flag = 'Y') * 100.0 / COUNT(1)
    display_name: 'First-Pass Rate'
```
</metric_view_yaml_spec>

<rules>
1. Classify each analyst SQL expression:
   - Aggregate function (COUNT/SUM/AVG/MAX/MIN/...) in the expression → `measures`
   - No aggregate (plain column, CASE WHEN, DATE_TRUNC, etc.) → `dimensions`
2. Rewrite column references to be UNQUALIFIED (no table prefix). The MV already knows its source. Turn `claims.initial_decision` into `initial_decision`. Only keep a prefix if the reference targets a JOINED table via the join's alias.
3. Put business-term synonyms from the vocabulary into the matching dimension/measure `synonyms:` array. Do NOT emit them as an `instructions` key (that key does not exist).
4. If the analyst supplied a `## Global Filter` section above, copy that SQL verbatim into the top-level `filter:` key — it is the authoritative filter. If a text instruction adds another data-level predicate (e.g., "exclude test claims" → `test_flag = 'N'`), AND the global filter, combine them with `AND`. Skip text instructions that aren't data filters — those belong on the Genie Space, not the MV.
5. `name` is a snake_case identifier. `display_name` is the human label.
6. Prefer `COUNT(1) FILTER (WHERE ...)` over `COUNT(CASE WHEN ... THEN 1 END)` — cleaner and idiomatic.
7. Pick ONE source table as the primary `source`. Join others via `joins:` with `on:` using `source.<col>` to reference the primary. If there's only one table, omit `joins:` entirely.

CRITICAL: NAME-COLLISION SAFETY (this breaks the view if you get it wrong)
8. A measure's or dimension's `name` MUST NOT exactly match any column name referenced inside its own `expr`. If it does, Spark resolves the column reference back to the aggregate itself and throws INVALID_AGGREGATE_FILTER.CONTAINS_AGGREGATE.
   - BAD:  `- name: first_pass_rate`  with `expr: COUNT(1) FILTER (WHERE first_pass_rate = 'Y') * 100.0 / COUNT(1)`
   - GOOD: `- name: first_pass_rate_pct` with `expr: COUNT(1) FILTER (WHERE first_pass_rate = 'Y') * 100.0 / COUNT(1)`
   - Rule of thumb: rates/percentages get a `_rate` or `_pct` suffix; counts get a `_count`; sums get a `_total`. Pick a suffix that reads naturally given the analyst's metric name. Derive `display_name` from the analyst's label so the UI still shows the human name.
9. Two measures/dimensions cannot share the same `name`. A dimension and a measure also cannot share the same name.

SQL LITERAL SAFETY
10. Always quote string literals in SQL expressions (`'Y'`, `'N'`, `'DENIED'`). Do not emit bare `Y`/`N` — YAML 1.1 parses unquoted `Y`/`N` as booleans and Spark will also misinterpret them as identifiers.
11. When writing a filter predicate like `claim_type IN ('Professional', 'Facility')`, keep the string literals quoted with single quotes inside the SQL. If the whole YAML value contains colons or backticks, wrap the value in double quotes.

COLUMN EXISTENCE (do NOT hallucinate)
12. Every bare column reference in any `expr` or `filter` or join `on` MUST be a real column listed under <## Table Schemas> above (or a valid alias from a `joins` block). Do NOT invent columns even if the business vocabulary implies one — if the analyst didn't map it and it's not in the schema list, omit that measure/dimension entirely rather than guessing.
13. Before emitting the final JSON, mentally walk each `expr` you wrote and verify every bare identifier is either (a) a SQL keyword/function, (b) a literal, or (c) a column present in the source table's schema. If it fails, drop or correct that field.
</rules>

<self_check>
After drafting, review your own YAML ONCE before returning it:
- Every column referenced exists in <## Table Schemas>? Replace or drop any that don't.
- Every measure name is distinct from all column names it references in its expr? Add `_pct` / `_count` / `_total` suffix if not.
- All string literals quoted with single quotes? No bare `Y`/`N`?
- Only the allowed top-level keys (version/comment/source/filter/joins/dimensions/measures/materialization)? No `instructions` key?
Only after these checks pass, output the final JSON.
</self_check>

Produce JSON with exactly these fields:
{{
  "yaml": "the complete YAML document as a string, starting with 'version: 1.1'",
  "source_table": "the catalog.schema.table used as the metric view source",
  "suggested_name": "short_snake_case_mv_name"
}}

Return ONLY the JSON. No markdown fences, no prose."""


def _strip_yaml_fences(yaml_text):
    yaml_text = (yaml_text or "").strip()
    if yaml_text.startswith("```"):
        yaml_text = yaml_text.split("\n", 1)[1] if "\n" in yaml_text else yaml_text
        if yaml_text.endswith("```"):
            yaml_text = yaml_text.rsplit("\n", 1)[0] if "\n" in yaml_text else yaml_text[:-3]
        yaml_text = yaml_text.strip()
        if yaml_text.lower().startswith("yaml"):
            yaml_text = yaml_text[4:].lstrip()
    return yaml_text


# SQL keywords / functions the LLM commonly emits as bare tokens. Anything not in
# here that looks like an identifier is treated as a possible column reference.
_SQL_KEYWORDS = {
    "select", "from", "where", "and", "or", "not", "in", "is", "null", "case",
    "when", "then", "else", "end", "as", "on", "using", "between", "like",
    "distinct", "filter", "order", "by", "group", "having", "asc", "desc",
    "cast", "try_cast", "interval", "date", "timestamp", "true", "false",
    "count", "sum", "avg", "min", "max", "median", "any", "every", "some",
    "first", "last", "collect_list", "collect_set", "approx_count_distinct",
    "year", "quarter", "month", "day", "week", "dayofweek", "dayofyear", "hour",
    "minute", "second", "concat", "coalesce", "nullif", "ifnull", "if",
    "date_trunc", "date_add", "date_sub", "datediff", "date_format", "to_date",
    "to_timestamp", "current_date", "current_timestamp", "substring", "substr",
    "length", "upper", "lower", "trim", "ltrim", "rtrim", "replace", "regexp",
    "regexp_replace", "regexp_extract", "split", "abs", "round", "floor",
    "ceil", "ceiling", "greatest", "least", "measure",
}


def _extract_bare_identifiers(expr):
    """Return lowercase identifiers from an expression that could be column refs."""
    import re
    if not expr:
        return set()
    # Remove string literals and backtick-escaped names so we only see bare refs
    cleaned = re.sub(r"'[^']*'", " ", expr)
    cleaned = re.sub(r"`[^`]*`", " ", cleaned)
    # Tokens: letters/digits/underscore, but drop pure numbers
    tokens = re.findall(r"[A-Za-z_][A-Za-z0-9_]*", cleaned)
    out = set()
    for t in tokens:
        tl = t.lower()
        if tl in _SQL_KEYWORDS:
            continue
        # Drop dotted prefixes - we care about final segment for unqualified refs
        out.add(tl)
    return out


def _validate_yaml_columns(yaml_text, schemas):
    """
    Return list of column references that don't exist in the source/join tables.
    schemas is {fqn: [(col, type), ...]}. Best-effort — returns [] on parse failure.
    """
    try:
        import yaml as pyyaml
    except Exception:
        return []
    try:
        doc = pyyaml.safe_load(yaml_text)
    except Exception:
        return []
    if not isinstance(doc, dict):
        return []

    # Build the pool of valid unqualified column names: primary source + all joined tables
    primary_src = (doc.get("source") or "").strip()
    join_sources = []
    for j in (doc.get("joins") or []):
        if isinstance(j, dict) and j.get("source"):
            join_sources.append(j["source"].strip())

    valid_cols = set()
    have_real_schema = False
    for fqn in [primary_src] + join_sources:
        if fqn in schemas and schemas[fqn]:
            have_real_schema = True
            for c, _t in schemas[fqn]:
                valid_cols.add(c.lower())

    # If we couldn't fetch schema for ANY of the referenced tables (permissions,
    # missing warehouse, etc.), skip validation entirely — otherwise every real
    # column would get flagged as hallucinated.
    if not have_real_schema:
        return []

    # Also valid: join aliases (used in "alias.col" prefix form)
    for j in (doc.get("joins") or []):
        if isinstance(j, dict) and j.get("name"):
            valid_cols.add(str(j["name"]).lower())
    # Special "source" alias used in join ON clauses to refer to the base source
    valid_cols.add("source")
    # DO NOT add dimension/measure names — dim/measure exprs resolve against the
    # source table columns, NOT against sibling dim/measure names. Adding them
    # creates a false negative where `name: payer_name / expr: payer_name` passes
    # validation even though `payer_name` is not a real source column.

    missing = set()
    # Scan every expr and filter
    def scan(expr):
        for ident in _extract_bare_identifiers(expr):
            if ident not in valid_cols:
                missing.add(ident)

    scan(doc.get("filter") or "")
    for d in (doc.get("dimensions") or []):
        if isinstance(d, dict):
            scan(d.get("expr") or "")
    for m in (doc.get("measures") or []):
        if isinstance(m, dict):
            scan(m.get("expr") or "")
    for j in (doc.get("joins") or []):
        if isinstance(j, dict):
            scan(j.get("on") or "")

    return sorted(missing)


@app.route("/api/engagements/<eid>/mv-prompt-preview", methods=["GET"])
def mv_prompt_preview(eid):
    """Debug: return the fully-assembled MV YAML prompt for this engagement."""
    rows = sql_exec(f"SELECT * FROM {TABLE} WHERE engagement_id = :eid", {"eid": eid})
    if not rows:
        return jsonify({"error": "Not found"}), 404
    eng = parse_row(rows[0])
    try:
        prompt = _build_mv_yaml_prompt(eng)
    except Exception as e:
        tb = traceback.format_exc()
        print(f"[mv-prompt-preview] ERROR: {e}\n{tb}", flush=True)
        return jsonify({"error": f"{type(e).__name__}: {e}"}), 500
    return jsonify({"prompt": prompt})


@app.route("/api/engagements/<eid>/draft-metric-view-yaml", methods=["POST"])
def draft_metric_view_yaml(eid):
    """Draft a UC Metric View YAML from Sessions 1-3."""
    rows = sql_exec(f"SELECT * FROM {TABLE} WHERE engagement_id = :eid", {"eid": eid})
    if not rows:
        return jsonify({"error": "Not found"}), 404
    eng = parse_row(rows[0])

    body = request.json or {}
    user_w = _user_workspace_client()
    warehouse_id = (body.get("warehouse_id") or "").strip()

    try:
        prompt = _build_mv_yaml_prompt(eng, user_w, warehouse_id)
        result = _call_llm(prompt)
    except Exception as e:
        tb = traceback.format_exc()
        print(f"[draft-mv-yaml] ERROR: {e}\n{tb}", flush=True)
        return jsonify({"error": f"{type(e).__name__}: {e}"}), 500

    yaml_text = _strip_yaml_fences(str(result.get("yaml", "")))
    source_table = str(result.get("source_table", "")).strip()
    suggested_name = str(result.get("suggested_name", "")).strip()

    # Post-draft sanity check: verify every bare column reference exists.
    # If any are missing, retry once with a targeted correction prompt.
    schemas = _collect_engagement_schemas(eng, user_w, warehouse_id)
    missing = _validate_yaml_columns(yaml_text, schemas)
    warnings = []
    if missing:
        print(f"[draft-mv-yaml] validation found missing columns: {missing}", flush=True)
        schema_lines = []
        for fqn, cols in schemas.items():
            if cols:
                schema_lines.append(f"- `{fqn}`: {', '.join(c for c, _ in cols)}")
        fix_prompt = f"""You drafted this metric view YAML, but it references columns that don't exist in the underlying tables.

<your_yaml>
{yaml_text}
</your_yaml>

<missing_columns>
{', '.join(missing)}
</missing_columns>

<authoritative_schema>
{chr(10).join(schema_lines)}
</authoritative_schema>

Rewrite the YAML so every bare column reference in any `expr`, `filter`, or join `on` clause is a column that actually exists in the authoritative schema above. If a missing column represents a concept that cannot be expressed with real columns, DROP that dimension/measure entirely rather than inventing a column. Keep every other rule from the original task (snake_case name fields, no name-column collisions, quoted string literals, only allowed top-level keys).

Return JSON with exactly: {{"yaml": "...", "source_table": "...", "suggested_name": "..."}}. No markdown fences."""
        try:
            result2 = _call_llm(fix_prompt)
            yaml_text2 = _strip_yaml_fences(str(result2.get("yaml", "")))
            missing2 = _validate_yaml_columns(yaml_text2, schemas)
            if yaml_text2 and len(missing2) < len(missing):
                yaml_text = yaml_text2
                source_table = str(result2.get("source_table", source_table)).strip() or source_table
                suggested_name = str(result2.get("suggested_name", suggested_name)).strip() or suggested_name
                if missing2:
                    warnings.append(
                        f"Retry still has {len(missing2)} unresolved column(s): {', '.join(missing2)}. "
                        f"Review the YAML before creating."
                    )
            else:
                warnings.append(
                    f"Columns referenced in YAML that don't exist in the source table: "
                    f"{', '.join(missing)}. Fix these before creating the metric view."
                )
        except Exception as e:
            print(f"[draft-mv-yaml] retry failed: {e}", flush=True)
            warnings.append(
                f"Columns referenced in YAML that don't exist in the source table: "
                f"{', '.join(missing)}. Fix these before creating the metric view."
            )

    return jsonify({
        "yaml": yaml_text,
        "source_table": source_table,
        "suggested_name": suggested_name,
        "warnings": warnings,
    })


def _sql_exec_obo(user_w, query, warehouse_id, catalog=None, schema=None):
    """Run a SQL statement using the user's OBO client against their chosen warehouse."""
    kwargs = {"warehouse_id": warehouse_id, "statement": query}
    if catalog:
        kwargs["catalog"] = catalog
    if schema:
        kwargs["schema"] = schema
    resp = user_w.statement_execution.execute_statement(**kwargs)
    state = resp.status.state if resp.status else None
    if str(state) not in ("StatementState.SUCCEEDED", "SUCCEEDED"):
        err = resp.status.error.message if (resp.status and resp.status.error) else "Unknown error"
        raise RuntimeError(f"Statement failed ({state}): {err}")
    return resp


def _describe_existing_mv_obo(user_w, fqn_quoted, warehouse_id):
    """Return (exists, owner_or_none) for a view, using the user's OBO creds.

    Owner comes from DESCRIBE TABLE EXTENDED. If the user can't DESCRIBE the
    object we treat it as non-existent from their perspective (they can't
    overwrite what they can't see anyway — the CREATE OR REPLACE will still
    surface the real permission error).
    """
    try:
        resp = user_w.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=f"DESCRIBE TABLE EXTENDED {fqn_quoted}",
        )
    except Exception:
        return False, None
    state = str(resp.status.state) if resp.status else ""
    if "SUCCEEDED" not in state:
        return False, None
    if not (resp.result and resp.result.data_array and resp.manifest):
        return True, None
    cols = [c.name for c in resp.manifest.schema.columns]
    try:
        col_name_idx = cols.index("col_name")
        data_type_idx = cols.index("data_type")
    except ValueError:
        return True, None
    owner = None
    for row in resp.result.data_array:
        label = (row[col_name_idx] or "").strip()
        if label.lower() == "owner":
            owner = (row[data_type_idx] or "").strip() or None
            break
    return True, owner


@app.route("/api/engagements/<eid>/create-metric-view", methods=["POST"])
def create_metric_view(eid):
    """Create (or replace) a UC Metric View from YAML via the user's OBO creds."""
    user_w = _user_workspace_client()
    if not user_w:
        return jsonify({"error": "No user access token"}), 401

    data = request.json or {}
    catalog_name = (data.get("catalog") or "").strip()
    schema_name = (data.get("schema") or "").strip()
    mv_name = (data.get("name") or "").strip()
    yaml_body = (data.get("yaml") or "").strip()
    warehouse_id = (data.get("warehouse_id") or "").strip()
    overwrite = bool(data.get("overwrite", False))

    if not all([catalog_name, schema_name, mv_name, yaml_body, warehouse_id]):
        return jsonify({
            "error": "catalog, schema, name, yaml, and warehouse_id are all required",
        }), 400

    # Identifier safety: backtick each segment
    fqn = f"`{catalog_name}`.`{schema_name}`.`{mv_name}`"
    created_fqn = f"{catalog_name}.{schema_name}.{mv_name}"

    # Existence check via OBO — block silent overwrite unless caller opts in.
    exists, owner = _describe_existing_mv_obo(user_w, fqn, warehouse_id)
    if exists and not overwrite:
        return jsonify({
            "error": "exists",
            "exists": True,
            "fqn": created_fqn,
            "owner": owner,
        }), 409

    # Re-run the column validator against the YAML the user is about to push.
    # The draft step already validates + retries, but the user may have hand-
    # edited the YAML since. Block the push if we can still prove a column is
    # hallucinated — much better UX than letting Spark error at CREATE time.
    rows = sql_exec(f"SELECT * FROM {TABLE} WHERE engagement_id = :eid", {"eid": eid})
    if rows:
        eng_for_schema = parse_row(rows[0])
        schemas = _collect_engagement_schemas(eng_for_schema, user_w, warehouse_id)
        missing = _validate_yaml_columns(yaml_body, schemas)
        if missing:
            return jsonify({
                "error": (
                    f"YAML references column(s) that don't exist in the source "
                    f"table(s): {', '.join(missing)}. Fix the YAML or re-draft, "
                    f"then try again."
                ),
                "missing_columns": missing,
            }), 400

    # YAML may contain $$ — escape using a unique delimiter if collision
    delim = "$$"
    if delim in yaml_body:
        delim = "$MV_YAML$"
    stmt = (
        f"CREATE OR REPLACE VIEW {fqn}\n"
        f"WITH METRICS\n"
        f"LANGUAGE YAML\n"
        f"AS {delim}\n{yaml_body}\n{delim}"
    )

    try:
        _sql_exec_obo(user_w, stmt, warehouse_id)
    except Exception as e:
        msg = str(e)
        status = 400
        lowered = msg.lower()
        if "permission" in lowered or "privilege" in lowered or "not authorized" in lowered:
            status = 403
        return jsonify({"error": f"Failed to create metric view: {msg}"}), status

    # Persist to Session 3 (last-created MV) and auto-add to Session 4 data plan
    if rows:
        eng = parse_row(rows[0])
        s4 = eng["sessions"]["4"]
        data_plan = list(s4.get("data_plan", []))
        # Avoid duplicate entry
        if not any((d.get("table_or_view") == created_fqn) for d in data_plan):
            data_plan.append({
                "table_or_view": created_fqn,
                "type": "Metric View",
                "include_in_space": "Yes",
                "notes": "Auto-added from Session 3 metric view creation.",
            })
        ts = now_ts()
        sql_run(
            f"UPDATE {TABLE} SET data_plan = :dp, metric_view_fqn = :fqn, updated_at = :ts "
            f"WHERE engagement_id = :eid",
            {"eid": eid, "dp": json.dumps(data_plan), "fqn": created_fqn, "ts": ts},
        )

    return jsonify({"success": True, "fqn": created_fqn})


# ---------------------------------------------------------------------------
# API: Push to Genie Space (Session 5)
# ---------------------------------------------------------------------------

def _snippet_entry(e, include_alias=True):
    """Build a sql_snippet entry (filter / expression / measure) from an LLM plan item."""
    sql_code = (e.get("sql") or "").strip()
    if not sql_code:
        return None
    entry = {"id": _gen_hex_id(), "sql": [sql_code]}
    if include_alias:
        alias = (e.get("name") or "").strip().lower().replace(" ", "_")
        if not alias:
            return None
        entry["alias"] = alias
    display_name = (e.get("display_name") or "").strip()
    if display_name:
        entry["display_name"] = display_name
    synonyms = e.get("synonyms") or []
    if isinstance(synonyms, str):
        synonyms = [s.strip() for s in synonyms.split(",") if s.strip()]
    if synonyms:
        entry["synonyms"] = list(synonyms)
    return entry


def _build_serialized_space(eng, plan):
    """Build the Genie serialized_space JSON from discovery data + edited plan.
    `plan` is a dict with: general_instructions, sample_questions,
    sql_filters, sql_dimensions, sql_measures, example_queries, joins, benchmarks.
    """
    s4 = eng["sessions"]["4"]

    general_instructions = plan.get("general_instructions", "") or ""
    sample_questions = plan.get("sample_questions") or []
    sql_filters = plan.get("sql_filters") or []
    sql_dimensions = plan.get("sql_dimensions") or []
    sql_measures = plan.get("sql_measures") or []
    example_queries = plan.get("example_queries") or []
    joins_in = plan.get("joins") or []
    benchmarks_in = plan.get("benchmarks") or []

    # Strip any sample or example that overlaps with a benchmark — benchmarks are
    # the acceptance test, they MUST NOT appear as configured answers.
    benchmark_qs = [
        (b.get("question") or "").strip()
        for b in benchmarks_in
        if (b.get("question") or "").strip()
    ]
    if benchmark_qs:
        sample_questions = _strip_benchmark_overlap(sample_questions, benchmark_qs)
        example_queries = [
            eq for eq in example_queries
            if not _question_overlaps(eq.get("question", ""), benchmark_qs)
        ]

    # Tables from Session 4 data plan (only items marked "Yes")
    tables, metric_views = [], []
    for d in s4.get("data_plan", []):
        if d.get("include_in_space") != "Yes":
            continue
        ident = d.get("table_or_view", "").strip()
        if not ident or len(ident.split(".")) != 3:
            continue
        entry = {"identifier": ident}
        notes = (d.get("notes") or "").strip()
        if notes:
            entry["description"] = [notes]
        if d.get("type") == "Metric View":
            metric_views.append(entry)
        else:
            tables.append(entry)
    tables.sort(key=lambda x: x["identifier"])
    metric_views.sort(key=lambda x: x["identifier"])

    # Sample questions
    sq_entries = [{"id": _gen_hex_id(), "question": [q]} for q in sample_questions if q]
    sq_entries.sort(key=lambda x: x["id"])

    # Text instructions (max 1)
    ti_entries = []
    if general_instructions.strip():
        ti_entries.append({"id": _gen_hex_id(), "content": [general_instructions.strip()]})

    # sql_snippets: filters have NO alias; expressions (dimensions) and measures have alias
    filters_out = sorted(
        [e for e in (_snippet_entry(x, include_alias=False) for x in sql_filters) if e],
        key=lambda x: x["id"],
    )
    expressions_out = sorted(
        [e for e in (_snippet_entry(x, include_alias=True) for x in sql_dimensions) if e],
        key=lambda x: x["id"],
    )
    measures_out = sorted(
        [e for e in (_snippet_entry(x, include_alias=True) for x in sql_measures) if e],
        key=lambda x: x["id"],
    )

    # Example queries → example_question_sqls
    eq_entries = []
    for q in example_queries:
        question = (q.get("question") or "").strip()
        sql_text = (q.get("sql") or "").strip()
        if not question or not sql_text:
            continue
        eq = {"id": _gen_hex_id(), "question": [question], "sql": [sql_text]}
        guidance = (q.get("usage_guidance") or "").strip()
        if guidance:
            eq["usage_guidance"] = [guidance]
        eq_entries.append(eq)
    eq_entries.sort(key=lambda x: x["id"])

    # Joins → join_specs (only pushed when backed by UC PK/FK)
    join_entries = []
    for j in joins_in:
        left = (j.get("left_table") or "").strip()
        right = (j.get("right_table") or "").strip()
        lcols = j.get("left_columns") or []
        rcols = j.get("right_columns") or []
        rel = (j.get("relationship_type") or "MANY_TO_ONE").upper()
        if not left or not right or not lcols or not rcols:
            continue
        left_alias = left.split(".")[-1]
        right_alias = right.split(".")[-1]
        conds = [f"{left_alias}.{lc} == {right_alias}.{rc}" for lc, rc in zip(lcols, rcols)]
        cond = " AND ".join(conds) + "\n"
        join_entries.append({
            "id": _gen_hex_id(),
            "left": {"identifier": left, "alias": left_alias},
            "right": {"identifier": right, "alias": right_alias},
            "sql": [cond, f"--rt=FROM_RELATIONSHIP_TYPE_{rel}--"],
        })
    join_entries.sort(key=lambda x: x["id"])

    serialized = {
        "version": 2,
        "config": {"sample_questions": sq_entries},
        "data_sources": {"tables": tables},
        "instructions": {"text_instructions": ti_entries},
    }
    if metric_views:
        serialized["data_sources"]["metric_views"] = metric_views

    sql_snippets = {}
    if filters_out:
        sql_snippets["filters"] = filters_out
    if expressions_out:
        sql_snippets["expressions"] = expressions_out
    if measures_out:
        sql_snippets["measures"] = measures_out
    if sql_snippets:
        serialized["instructions"]["sql_snippets"] = sql_snippets
    if eq_entries:
        serialized["instructions"]["example_question_sqls"] = eq_entries
    if join_entries:
        serialized["instructions"]["join_specs"] = join_entries

    # Benchmarks — top-level key. Only include rows with both question AND SQL.
    bm_entries = []
    for b in benchmarks_in:
        q = (b.get("question") or "").strip()
        sql = (b.get("expected_sql") or "").strip()
        if not q or not sql:
            continue
        bm_entries.append({
            "id": _gen_hex_id(),
            "question": [q],
            "sql_answer": [sql],
        })
    bm_entries.sort(key=lambda x: x["id"])
    if bm_entries:
        serialized["benchmarks"] = bm_entries

    return serialized


def _genie_api_call(user_w, method, path, body=None):
    """Make an authenticated Genie REST API call using the user's token (OBO)."""
    url = f"{user_w.config.host.rstrip('/')}{path}"
    headers = {
        "Authorization": f"Bearer {user_w.config.token}",
        "Content-Type": "application/json",
    }
    resp = requests.request(method, url, headers=headers, json=body, timeout=60)
    if not resp.ok:
        raise RuntimeError(f"Genie API {method} {path} failed ({resp.status_code}): {resp.text[:500]}")
    return resp.json() if resp.text else {}


@app.route("/api/engagements/<eid>/push-to-genie", methods=["POST"])
def push_to_genie(eid):
    """Push the approved plan to a Genie Space (create or update) via OBO."""
    user_w = _user_workspace_client()
    if not user_w:
        return jsonify({"error": "No user access token available"}), 401

    data = request.json or {}
    mode = data.get("mode", "existing")  # "existing" or "new"
    space_id = (data.get("space_id") or "").strip()
    warehouse_id = (data.get("warehouse_id") or "").strip()
    new_title = (data.get("new_title") or "").strip()
    new_description = (data.get("new_description") or "").strip()
    new_parent_path = (data.get("new_parent_path") or "").strip()

    rows = sql_exec(f"SELECT * FROM {TABLE} WHERE engagement_id = :eid", {"eid": eid})
    if not rows:
        return jsonify({"error": "Engagement not found"}), 404
    eng = parse_row(rows[0])
    s5 = eng["sessions"]["5"]

    # Allow caller to pass edited plan pieces (from UI); fall back to persisted values.
    s4 = eng["sessions"]["4"]
    plan = {
        "general_instructions": data.get("general_instructions") if data.get("general_instructions") is not None else s5.get("plan_general_instructions", ""),
        "sample_questions":     data.get("sample_questions") if data.get("sample_questions") is not None else s5.get("plan_sample_questions", []),
        "sql_filters":          data.get("sql_filters") if data.get("sql_filters") is not None else s5.get("plan_sql_filters", []),
        "sql_dimensions":       data.get("sql_dimensions") if data.get("sql_dimensions") is not None else s5.get("plan_sql_dimensions", []),
        "sql_measures":         data.get("sql_measures") if data.get("sql_measures") is not None else s5.get("plan_sql_measures", []),
        "example_queries":      data.get("example_queries") if data.get("example_queries") is not None else s5.get("plan_example_queries", []),
        "joins":                data.get("joins") if data.get("joins") is not None else s5.get("plan_joins", []),
        "benchmarks":           s4.get("benchmark_questions", []),
    }

    if not warehouse_id:
        return jsonify({"error": "warehouse_id is required"}), 400

    try:
        serialized = _build_serialized_space(eng, plan)
    except Exception as e:
        return jsonify({"error": f"Failed to build payload: {e}"}), 400

    result = {"mode": mode, "warnings": []}

    try:
        if mode == "new":
            if not new_title:
                return jsonify({"error": "new_title is required for create mode"}), 400
            if not new_parent_path:
                # Default to the user's workspace folder
                try:
                    me = user_w.current_user.me()
                    new_parent_path = f"/Workspace/Users/{me.user_name}"
                except Exception:
                    return jsonify({"error": "new_parent_path could not be defaulted"}), 400
            body = {
                "title": new_title,
                "description": new_description,
                "parent_path": new_parent_path,
                "warehouse_id": warehouse_id,
                "serialized_space": json.dumps(serialized),
            }
            resp = _genie_api_call(user_w, "POST", "/api/2.0/genie/spaces", body)
            space_id = resp.get("space_id", "")
            result["space_id"] = space_id
            result["created"] = True
        else:
            if not space_id:
                return jsonify({"error": "space_id is required for update mode"}), 400
            # PATCH update (per internal Genie API docs)
            body = {
                "title": eng.get("genie_space_name", ""),
                "warehouse_id": warehouse_id,
                "serialized_space": json.dumps(serialized),
            }
            _genie_api_call(user_w, "PATCH", f"/api/2.0/genie/spaces/{space_id}", body)
            result["space_id"] = space_id
            result["updated"] = True
    except Exception as e:
        return jsonify({"error": str(e), "partial": result}), 500

    space_url = f"{user_w.config.host.rstrip('/')}/genie/rooms/{space_id}"
    result["space_url"] = space_url

    # Persist push results
    ts = now_ts()
    sql_run(
        f"UPDATE {TABLE} SET "
        f"genie_space_id = :sid, genie_space_url = :url, "
        f"genie_space_pushed_at = :pushed, plan_warehouse_id = :wid, "
        f"updated_at = :ts "
        f"WHERE engagement_id = :eid",
        {
            "eid": eid,
            "sid": space_id,
            "url": space_url,
            "pushed": ts,
            "wid": warehouse_id,
            "ts": ts,
        },
    )

    return jsonify(result)


# ---------------------------------------------------------------------------
# API: Unity Catalog metadata
#
# ALL endpoints below run under OBO so analysts only see catalogs/schemas/
# tables/columns/joins that their UC grants permit. The service principal is
# deliberately NOT used as a fallback, since that would leak metadata the user
# cannot actually query. If the forwarded token is missing or lacks the
# catalog.* user scopes, the endpoint returns 401 reauth_required.
# ---------------------------------------------------------------------------

def _require_obo():
    """Shared helper for UC endpoints: return a user-OBO client or a 401 response."""
    user_w = _user_workspace_client()
    if not user_w:
        return None, (jsonify({"error": "reauth_required"}), 401)
    return user_w, None


@app.route("/api/uc/catalogs")
def uc_catalogs():
    user_w, err = _require_obo()
    if err:
        return err
    try:
        cats = list(user_w.catalogs.list())
    except Exception as e:
        print(f"[/api/uc/catalogs] {type(e).__name__}: {e}", flush=True)
        return jsonify([])
    names = [c.name for c in cats if c.name and not c.name.startswith("__")]
    names.sort(key=str.lower)
    return jsonify(names)


@app.route("/api/uc/schemas")
def uc_schemas():
    user_w, err = _require_obo()
    if err:
        return err
    catalog = request.args.get("catalog", "")
    if not catalog:
        return jsonify([])
    try:
        schemas = list(user_w.schemas.list(catalog_name=catalog))
    except Exception as e:
        print(f"[/api/uc/schemas] {type(e).__name__}: {e}", flush=True)
        return jsonify([])
    names = [s.name for s in schemas if s.name and s.name != "information_schema"]
    names.sort(key=str.lower)
    return jsonify(names)


@app.route("/api/uc/tables")
def uc_tables():
    user_w, err = _require_obo()
    if err:
        return err
    catalog = request.args.get("catalog", "")
    schema = request.args.get("schema", "")
    if not catalog or not schema:
        return jsonify([])
    try:
        tables = list(user_w.tables.list(catalog_name=catalog, schema_name=schema))
    except Exception as e:
        print(f"[/api/uc/tables] {type(e).__name__}: {e}", flush=True)
        return jsonify([])
    names = [t.name for t in tables if t.name]
    names.sort(key=str.lower)
    return jsonify(names)


@app.route("/api/uc/columns")
def uc_columns():
    user_w, err = _require_obo()
    if err:
        return err
    catalog = request.args.get("catalog", "")
    schema = request.args.get("schema", "")
    table = request.args.get("table", "")
    if not catalog or not schema or not table:
        return jsonify([])
    try:
        info = user_w.tables.get(f"{catalog}.{schema}.{table}")
    except Exception as e:
        print(f"[/api/uc/columns] {type(e).__name__}: {e}", flush=True)
        return jsonify([])
    cols = getattr(info, "columns", None) or []
    return jsonify([
        {"name": c.name, "type": str(c.type_text or c.type_name or "")}
        for c in cols if getattr(c, "name", None)
    ])


@app.route("/api/uc/joins")
def uc_joins():
    """Auto-detect PK/FK relationships between selected tables via OBO tables.get()."""
    user_w, err = _require_obo()
    if err:
        return err
    tables = request.args.getlist("table")
    if len(tables) < 2:
        return jsonify([])
    in_scope = set(tables)
    results = []
    seen = set()
    for tbl in tables:
        if tbl.count(".") != 2:
            continue
        try:
            info = user_w.tables.get(tbl)
        except Exception as e:
            print(f"[/api/uc/joins] tables.get({tbl}) {type(e).__name__}: {e}", flush=True)
            continue
        for c in getattr(info, "table_constraints", None) or []:
            fk = getattr(c, "foreign_key_constraint", None)
            if not fk:
                continue
            parent = getattr(fk, "parent_table", None)
            if not parent or parent not in in_scope:
                continue
            child_cols = list(getattr(fk, "child_columns", []) or [])
            parent_cols = list(getattr(fk, "parent_columns", []) or [])
            key = (tbl, tuple(child_cols), parent, tuple(parent_cols))
            if key in seen:
                continue
            seen.add(key)
            short_child = tbl.split(".")[-1]
            short_parent = parent.split(".")[-1]
            keys_str = " AND ".join(
                f"{short_child}.{cc} = {short_parent}.{pc}"
                for cc, pc in zip(child_cols, parent_cols)
            ) or f"{short_child} = {short_parent}"
            results.append({
                "table": f"{short_child} -> {short_parent}",
                "keys": keys_str,
            })
    return jsonify(results)


@app.route("/api/uc/metric-views")
def uc_metric_views():
    """Detect existing metric views in a catalog.schema via OBO tables.list()."""
    user_w, err = _require_obo()
    if err:
        return err
    catalog_schema = request.args.get("catalog_schema", "")
    if not catalog_schema or catalog_schema.count(".") != 1:
        return jsonify([])
    cat, sch = catalog_schema.split(".", 1)
    try:
        tables = list(user_w.tables.list(catalog_name=cat, schema_name=sch))
    except Exception as e:
        print(f"[/api/uc/metric-views] {type(e).__name__}: {e}", flush=True)
        return jsonify([])
    results = []
    for t in tables:
        tt = str(getattr(t, "table_type", "") or "").upper()
        # True UC metric views show up as METRIC_VIEW; plain SQL views are VIEW.
        if "METRIC_VIEW" in tt and t.name:
            results.append(f"{cat}.{sch}.{t.name}")
    return jsonify(results)


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
