import json
import os
import random
import secrets
import threading
from concurrent.futures import ThreadPoolExecutor
import time
import traceback
import uuid
from datetime import datetime, timezone
from io import BytesIO

import requests
from flask import Flask, request, jsonify, send_from_directory, send_file
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config
from databricks.sdk.service.sql import StatementParameterListItem, StatementState
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
# BO group: members can view all engagements but only edit S1/S2 + click BO
# Approved on benchmark rows. If unset, no users get BO permissions.
BO_GROUP = os.getenv("BO_GROUP_NAME") or "genie-bo-reviewers"
LLM_ENDPOINT = os.getenv("LLM_ENDPOINT_NAME") or "databricks-claude-haiku-4-5"
# Model-serving first-token latency can exceed the SDK's default 60s HTTP read
# timeout on large prompts (e.g. generate_plan with rich engagements), which the
# SDK then retries for ~5 min before giving up. Use a dedicated client with a
# longer read timeout for LLM calls only; leave non-LLM SDK calls at 60s so
# they still fail fast.
LLM_HTTP_TIMEOUT_SECONDS = int(os.getenv("LLM_HTTP_TIMEOUT_SECONDS") or "600")

# Optional. Microsoft Teams Incoming Webhook URL (Power Automate "Workflows"
# style, or a legacy O365 connector). When an analyst marks an engagement
# "Ready for COE Review", the app posts an Adaptive Card to this webhook's
# channel — which is how the COE group gets notified (everyone in that Teams
# channel sees it). If unset/empty, the notification is silently skipped and
# the status flip still succeeds. Best-effort only; never blocks the workflow.
TEAMS_COE_WEBHOOK_URL = (os.getenv("TEAMS_COE_WEBHOOK_URL") or "").strip()

w = WorkspaceClient()
llm_w = WorkspaceClient(config=Config(http_timeout_seconds=LLM_HTTP_TIMEOUT_SECONDS))

# Section columns grouped by session
SESSION_COLS = {
    1: ["business_context", "pain_points", "existing_reports"],
    2: ["question_bank", "vocabulary_metrics"],
    3: ["term_classifications", "sql_expressions", "text_instructions",
        "clarifying_questions", "example_queries",
        "data_gaps", "scope_boundaries", "global_filter",
        "metric_view_yaml", "metric_view_yaml_previous", "metric_view_fqn"],
    4: ["analyst_commentary", "auto_summary", "data_plan", "benchmark_questions",
        "brief_unacknowledged_gaps",
        "coe_approval_status", "coe_approval_notes", "coe_reviewer_email"],
    5: ["genie_space_id", "genie_space_config",
        "plan_general_instructions", "plan_sample_questions", "plan_narrative",
        "plan_sql_filters", "plan_sql_dimensions", "plan_sql_measures",
        "plan_example_queries", "plan_joins", "plan_previous",
        "plan_warehouse_id", "genie_space_url", "genie_space_pushed_at",
        "acknowledgments"],
    6: ["prototype_results", "fixes_log", "benchmarks", "phrasing_notes"],
    7: ["production_checklist", "prod_access_notes",
        "prod_approval_status", "prod_approval_notes", "prod_reviewer_email"],
}

# The highest session number. Used so save_session's "last session ->
# status=complete" logic isn't hardcoded to a magic number.
LAST_SESSION = max(SESSION_COLS)

# Columns that store plain strings (not JSON-encoded structured data).
# `analyst_commentary` was here historically but is now a JSON object
# {gap_responses, resolved_gaps, legacy_notes?}; removed so save/load
# treat it as JSON.
SCALAR_COLS = {
    "global_filter",
    "metric_view_yaml", "metric_view_yaml_previous", "metric_view_fqn", "auto_summary",
    "coe_approval_status", "coe_approval_notes", "coe_reviewer_email",
    "genie_space_id", "genie_space_config",
    "plan_general_instructions", "plan_narrative", "plan_warehouse_id",
    "genie_space_url", "genie_space_pushed_at",
    "prod_access_notes", "prod_approval_status", "prod_approval_notes",
    "prod_reviewer_email",
}

# Columns whose JSON shape is an object (not an array). Used to pick the
# right empty-default and the right fallback when parse fails.
#
# `plan_previous` is a backup snapshot of the Session 5 plan_* fields, written
# by the frontend immediately before a plan regeneration so the analyst can
# restore the prior version. It rides the normal save/load cycle (it's a
# regular SESSION_COLS[5] column), so nothing special-cases it.
OBJECT_COLS = {"analyst_commentary", "plan_previous", "acknowledgments"}


def _default_section_value(col):
    """The empty-default for a section column (string scalar, [] for arrays, {} for objects)."""
    if col in SCALAR_COLS:
        return ""
    if col in OBJECT_COLS:
        return "{}"
    return "[]"

# All section columns
ALL_SECTION_COLS = sorted(set(col for cols in SESSION_COLS.values() for col in cols))


# ---------------------------------------------------------------------------
# Form-key tolerant accessors
#
# The frontend Session 2 form writes `question_text` (Question Bank) and
# `what_they_mean` (Vocabulary). Several prompt builders historically looked
# for `question` / `definition`, which silently returned empty strings and
# made every LLM-generated artifact (brief, plan, benchmarks, MV YAML)
# blind to the BO-captured questions and vocabulary. These helpers normalize
# access and tolerate a couple of legacy/alternative keys.
# ---------------------------------------------------------------------------

def _qb_question(q):
    """Return the question text from a question_bank entry, or empty string."""
    if not isinstance(q, dict):
        return ""
    return (q.get("question_text") or q.get("question") or q.get("text") or "").strip()


def _vm_definition(v):
    """Return the definition/meaning from a vocabulary_metrics entry."""
    if not isinstance(v, dict):
        return ""
    return (v.get("what_they_mean") or v.get("definition") or v.get("description") or "").strip()


def _er_what(r):
    """Return what an existing_reports entry shows."""
    if not isinstance(r, dict):
        return ""
    return (r.get("what_it_shows") or r.get("description") or r.get("key_metrics") or "").strip()


# ---------------------------------------------------------------------------
# Async job runner
#
# Long-running LLM calls (Readiness Brief, plan generation, etc.) routinely
# exceed the Databricks Apps frontend gateway's ~60s HTTP request timeout.
# This module lets us run those tasks in a background thread and have the
# frontend poll for completion via short, fast HTTP requests.
#
# Architecture:
#   - JOBS: in-memory dict keyed by uuid; values track state + result.
#   - TASK_HANDLERS: registry mapping task_type strings to callables.
#     Decorate a function with @register_task("name") to make it dispatchable.
#   - POST /api/jobs/start launches a daemon thread, returns job_id immediately.
#   - GET /api/jobs/<id> returns current state, result (when done), or error.
#   - Old jobs (>1h) are evicted opportunistically on each /start call.
#
# Tradeoffs:
#   - In-memory state dies on app restart. For this single-instance app that's
#     acceptable: the worst case is the user clicks Regenerate and we run again.
#   - No cancel signal -- a cancelled job's LLM call still completes server-side,
#     we just stop returning the result to the client.
# ---------------------------------------------------------------------------

JOBS = {}
JOBS_LOCK = threading.Lock()
JOB_TTL_SECONDS = 3600  # 1 hour
TASK_HANDLERS = {}

# Delta table for job persistence so app restarts don't strand in-flight jobs.
# Rows are mirrored from the in-memory JOBS dict on every state change. On
# /jobs/<id>, an in-memory miss falls back to a Delta lookup so a redeploy
# mid-poll surfaces a real result instead of "job not found".
JOBS_TABLE = f"{CATALOG}.{SCHEMA}.discovery_jobs"

# LLM cost / usage telemetry. Every _call_llm / _call_llm_raw call appends a
# row here so the customer can see total token spend, per-feature breakdowns,
# and historical latency without standing up a separate dashboard. Best-effort:
# write failures don't break the LLM call.
LLM_USAGE_TABLE = f"{CATALOG}.{SCHEMA}.discovery_llm_usage"


def _log_llm_usage(
    label, endpoint, prompt_chars, output_chars, latency_ms,
    prompt_tokens=0, completion_tokens=0, success=True, error="",
):
    try:
        sql_run(
            f"INSERT INTO {LLM_USAGE_TABLE} VALUES "
            f"(:ts, :label, :endpoint, :pc, :oc, :pt, :ct, :lm, :ok, :err)",
            {
                "ts": now_ts(),
                "label": label or "",
                "endpoint": endpoint or "",
                "pc": str(int(prompt_chars or 0)),
                "oc": str(int(output_chars or 0)),
                "pt": str(int(prompt_tokens or 0)),
                "ct": str(int(completion_tokens or 0)),
                "lm": str(int(latency_ms or 0)),
                "ok": "true" if success else "false",
                "err": str(error or "")[:500],
            },
        )
    except Exception as e:
        print(f"[llm-usage] log failed: {e}", flush=True)


def _persist_job_state(job_id, job):
    """Mirror the current in-memory job state into the Delta jobs table.
    Best-effort: failures are logged but don't crash the worker."""
    try:
        result_json = json.dumps(job.get("result")) if job.get("result") is not None else ""
        finished_at = job.get("finished_at") or 0.0
        sql_run(
            f"MERGE INTO {JOBS_TABLE} AS t "
            f"USING (SELECT :job_id AS job_id) AS s ON t.job_id = s.job_id "
            f"WHEN MATCHED THEN UPDATE SET "
            f"  state = :state, finished_at = :finished_at, "
            f"  result = :result, error = :error "
            f"WHEN NOT MATCHED THEN INSERT "
            f"  (job_id, task_type, creator_email, state, started_at, finished_at, result, error) "
            f"VALUES "
            f"  (:job_id, :task_type, :creator_email, :state, :started_at, :finished_at, :result, :error)",
            {
                "job_id": job_id,
                "task_type": job.get("task_type", ""),
                "creator_email": job.get("creator_email", ""),
                "state": job.get("state", ""),
                "started_at": str(job.get("started_at", 0.0)),
                "finished_at": str(finished_at),
                "result": result_json,
                "error": str(job.get("error") or ""),
            },
        )
    except Exception as e:
        print(f"[jobs persist] {job_id} write failed: {e}", flush=True)


def _load_job_from_delta(job_id):
    """Look up a job in Delta when not present in the in-memory dict (e.g. after
    an app restart). Returns the job dict or None."""
    try:
        rows = sql_exec(
            f"SELECT job_id, task_type, creator_email, state, started_at, "
            f"finished_at, result, error FROM {JOBS_TABLE} WHERE job_id = :j",
            {"j": job_id},
        )
        if not rows:
            return None
        r = rows[0]
        try:
            result = json.loads(r.get("result") or "null")
        except (json.JSONDecodeError, TypeError):
            result = None
        try:
            started_at = float(r.get("started_at") or 0.0)
        except (ValueError, TypeError):
            started_at = 0.0
        return {
            "state": r.get("state") or "",
            "task_type": r.get("task_type") or "",
            "creator_email": r.get("creator_email") or "",
            "started_at": started_at,
            "result": result,
            "error": r.get("error") or "",
        }
    except Exception as e:
        print(f"[jobs persist] read {job_id} failed: {e}", flush=True)
        return None


def _recover_orphan_jobs():
    """At startup, mark any Delta rows still in 'pending' state as 'failed'.
    Their worker threads died with the previous app process, so they will
    never finish. Without this they'd poll forever."""
    try:
        sql_run(
            f"UPDATE {JOBS_TABLE} SET state = 'failed', "
            f"error = 'App restarted before job finished. Please retry.' "
            f"WHERE state = 'pending'"
        )
    except Exception as e:
        print(f"[jobs persist] orphan recovery failed: {e}", flush=True)


def _user_error(label, exc):
    """Log the full exception + traceback under `label`, return a clean
    user-facing string. Strips the exception class name prefix so the UI
    doesn't show 'ValueError: required field missing' to non-engineers --
    just 'required field missing'. Falls back to a generic message for
    blank exception strings."""
    tb = traceback.format_exc()
    print(f"[{label}] {type(exc).__name__}: {exc}\n{tb}", flush=True)
    msg = str(exc).strip()
    if not msg:
        return f"Something went wrong. Check server logs for details (label: {label})."
    return msg


def register_task(name):
    """Decorator: register a function as the handler for a task_type."""
    def decorator(fn):
        TASK_HANDLERS[name] = fn
        return fn
    return decorator


def _cleanup_old_jobs():
    """Evict jobs older than TTL. Called on each /start request -- amortized.
    Cleans both the in-memory dict and the Delta jobs table."""
    now = time.time()
    with JOBS_LOCK:
        stale = [jid for jid, j in JOBS.items() if now - j["started_at"] > JOB_TTL_SECONDS]
        for jid in stale:
            del JOBS[jid]
    cutoff = now - JOB_TTL_SECONDS
    try:
        sql_run(
            f"DELETE FROM {JOBS_TABLE} WHERE started_at < :cutoff",
            {"cutoff": str(cutoff)},
        )
    except Exception as e:
        print(f"[jobs persist] cleanup failed: {e}", flush=True)


def _run_job(job_id, task_type, payload):
    """Background-thread worker. Updates JOBS with result or error and
    mirrors state changes into the Delta jobs table."""
    handler = TASK_HANDLERS.get(task_type)
    if not handler:
        with JOBS_LOCK:
            if job_id in JOBS:
                JOBS[job_id]["state"] = "failed"
                JOBS[job_id]["error"] = f"Unknown task_type: {task_type}"
                JOBS[job_id]["finished_at"] = time.time()
                snapshot = dict(JOBS[job_id])
        _persist_job_state(job_id, snapshot)
        return
    try:
        result = handler(payload)
        with JOBS_LOCK:
            if job_id in JOBS:
                JOBS[job_id]["state"] = "done"
                JOBS[job_id]["result"] = result
                JOBS[job_id]["finished_at"] = time.time()
                snapshot = dict(JOBS[job_id])
            else:
                snapshot = None
    except Exception as e:
        with JOBS_LOCK:
            if job_id in JOBS:
                JOBS[job_id]["state"] = "failed"
                JOBS[job_id]["error"] = _user_error(f"jobs:{task_type}:{job_id}", e)
                JOBS[job_id]["finished_at"] = time.time()
                snapshot = dict(JOBS[job_id])
            else:
                snapshot = None
    if snapshot:
        _persist_job_state(job_id, snapshot)


@app.route("/api/jobs/start", methods=["POST"])
def jobs_start():
    """Kick off a background task. Returns a job_id for polling.

    Authorization model:
    - If payload contains an `engagement_id`, the calling user must be the
      analyst, the BO, or a COE-group member for that engagement. Without
      this check the job runner would bypass the per-engagement gate that
      protects the sync `/api/engagements/<eid>/...` routes.
    - The job is bound to the creator's email so /jobs/<id> can verify
      that the requester is the same user.

    Auto-injects the request's OBO token into payload["_user_token"] so
    background tasks that need to run UC queries under the user's grants
    can do so. The token is held on the worker thread's stack only -- it
    is never written into JOBS state, so it doesn't leak via /jobs/<id>.
    """
    body = request.json or {}
    task_type = (body.get("task_type") or "").strip()
    payload = dict(body.get("payload") or {})  # copy so we can safely mutate
    if not task_type:
        return jsonify({"error": "task_type is required"}), 400
    if task_type not in TASK_HANDLERS:
        return jsonify({"error": f"unknown task_type: {task_type}"}), 400

    # Engagement authorization: every task we currently register is bound to
    # an engagement_id, and the sync analogues are gated. Apply the same gate
    # here for parity. Tasks without an engagement_id are rejected for now;
    # if we ever add a global task, this check can be relaxed deliberately.
    eid_in_payload = (payload.get("engagement_id") or "").strip()
    if not eid_in_payload:
        return jsonify({"error": "payload.engagement_id is required"}), 400
    _, err = _authorize_engagement(eid_in_payload)
    if err:
        return err

    creator_email = (get_current_user() or "").strip().lower()
    if not creator_email:
        return jsonify({"error": "User email not available; cannot start job"}), 401

    payload["_user_token"] = request.headers.get("X-Forwarded-Access-Token") or ""

    _cleanup_old_jobs()

    job_id = uuid.uuid4().hex
    with JOBS_LOCK:
        JOBS[job_id] = {
            "state": "pending",
            "task_type": task_type,
            "creator_email": creator_email,
            "started_at": time.time(),
            "finished_at": 0.0,
            "result": None,
            "error": None,
        }
        snapshot = dict(JOBS[job_id])
    _persist_job_state(job_id, snapshot)

    t = threading.Thread(target=_run_job, args=(job_id, task_type, payload), daemon=True)
    t.start()
    return jsonify({"job_id": job_id})


@app.route("/api/jobs/<job_id>", methods=["GET"])
def jobs_status(job_id):
    """Return current state of a job. Only the user who created the job can
    read it -- prevents leakage if a job_id is exposed (logs, etc.).

    Falls back to the Delta jobs table if the in-memory dict has been
    cleared (app restart), so a poll mid-restart returns the persisted
    final state instead of "job not found".
    """
    requester = (get_current_user() or "").strip().lower()
    with JOBS_LOCK:
        job = JOBS.get(job_id)
    if not job:
        # App may have restarted; check Delta
        job = _load_job_from_delta(job_id)
        if not job:
            return jsonify({"error": "job not found"}), 404
    if requester and job.get("creator_email") and requester != job["creator_email"]:
        # 404 (not 403) so the existence of the job isn't disclosed
        return jsonify({"error": "job not found"}), 404
    return jsonify({
        "state": job.get("state", ""),
        "task_type": job.get("task_type", ""),
        "result": job.get("result"),
        "error": job.get("error"),
        "age_seconds": int(time.time() - (job.get("started_at") or time.time())),
    })


# ---------------------------------------------------------------------------
# SQL helpers
# ---------------------------------------------------------------------------

class SqlTransientError(RuntimeError):
    """Raised when a SQL statement does not reach a successful terminal state
    within the warehouse-warmup window — typically a cold warehouse, network
    blip, or timeout. Distinct from a genuine empty result so callers can tell
    'row doesn't exist' from 'we couldn't ask.'"""


def sql_exec(query, params=None):
    """Execute a read query and return a list of dict rows.

    Returns:
        list[dict]: rows on a SUCCEEDED query (empty list = genuinely no rows).

    Raises:
        SqlTransientError: warehouse cold-start / timeout / non-terminal state.
        RuntimeError: query failed (syntax error, permission denied, etc).

    The wait_timeout is set generously so a cold warehouse has time to spin
    up before we treat the call as transient. Without this, every read
    against a cold warehouse silently returned [] and callers interpreted
    'no results' the same as 'real empty result' — for /api/jobs/start
    that surfaced as a confusing 404 'Engagement not found.'
    """
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
        wait_timeout="50s",  # generous: covers warehouse cold-start
    )

    state = resp.status.state if resp.status else None
    if state == StatementState.SUCCEEDED:
        if resp.result and resp.result.data_array:
            cols = [c.name for c in resp.manifest.schema.columns]
            return [dict(zip(cols, row)) for row in resp.result.data_array]
        return []  # genuine empty result
    if state in (StatementState.FAILED, StatementState.CANCELED, StatementState.CLOSED):
        err = (
            resp.status.error.message
            if resp.status and resp.status.error
            else f"state={state}"
        )
        raise RuntimeError(f"SQL failed: {err}")
    # PENDING / RUNNING / None — non-terminal after wait_timeout
    raise SqlTransientError(
        f"SQL did not complete within wait_timeout (state={state}). "
        "The warehouse may be cold; retry in a few seconds."
    )


def sql_run(query, params=None, *, must_succeed=False):
    """Execute a write statement.

    By default this is fire-and-forget for back-compat -- ensure_table()'s
    CREATE TABLE IF NOT EXISTS calls would otherwise crash startup if the
    app SP lacks CREATE TABLE on the schema (the table may already exist;
    permission is checked before IF NOT EXISTS short-circuits).

    Pass must_succeed=True for writes where silent failure is unacceptable
    (engagement saves, soft-delete, plan persistence). That path waits for
    completion and raises on any non-SUCCEEDED state.
    """
    sdk_params = None
    if params:
        sdk_params = [
            StatementParameterListItem(name=k, value=str(v) if v is not None else "")
            for k, v in params.items()
        ]
    if not must_succeed:
        # Best-effort fire-and-forget. Wait briefly so quick statements
        # commit before we return, but don't raise on failure.
        w.statement_execution.execute_statement(
            warehouse_id=WAREHOUSE_ID,
            statement=query,
            parameters=sdk_params,
            catalog=CATALOG,
            schema=SCHEMA,
            wait_timeout="10s",
        )
        return

    resp = w.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID,
        statement=query,
        parameters=sdk_params,
        catalog=CATALOG,
        schema=SCHEMA,
        wait_timeout="30s",
    )
    state = str(resp.status.state) if resp.status else ""
    statement_id = resp.statement_id
    deadline = time.time() + 60
    while statement_id and ("PENDING" in state or "RUNNING" in state):
        if time.time() > deadline:
            raise RuntimeError(f"sql_run timed out after 90s: {query[:80]}...")
        time.sleep(1.0)
        resp = w.statement_execution.get_statement(statement_id)
        state = str(resp.status.state) if resp.status else ""
    if "SUCCEEDED" not in state:
        err = resp.status.error.message if (resp.status and resp.status.error) else state
        raise RuntimeError(f"sql_run failed [{state}]: {err}; query: {query[:200]}")


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


def _empty_for_col(col):
    """In-memory empty for a column (after parse): str / list / dict."""
    if col in SCALAR_COLS:
        return ""
    if col in OBJECT_COLS:
        return {}
    return []


def _decode_section_value(col, raw):
    """Decode a stored JSON section column. Falls back to per-column legacy
    recovery for analyst_commentary, which used to be either a plain prose
    string OR a Python repr (str(dict)) due to a prior storage bug."""
    if not raw:
        return _empty_for_col(col)
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        if col == "analyst_commentary":
            return _migrate_legacy_commentary(raw)
        return _empty_for_col(col)


def _migrate_legacy_commentary(raw):
    """Recover analyst_commentary that was stored either as Python repr
    `str(dict)` (single-quote bug) or as plain prose (pre-feature). Always
    returns the new structured shape; preserves any human-written prose
    under `legacy_notes` so it isn't silently dropped on upgrade."""
    s = str(raw).strip()
    # Try Python literal_eval for the single-quote dict-repr case
    try:
        import ast
        parsed = ast.literal_eval(s)
        if isinstance(parsed, dict) and ("gap_responses" in parsed or "resolved_gaps" in parsed):
            return {
                "gap_responses": dict(parsed.get("gap_responses") or {}),
                "resolved_gaps": dict(parsed.get("resolved_gaps") or {}),
            }
    except (ValueError, SyntaxError):
        pass
    # Otherwise treat as plain prose -- keep it visible to the analyst as Legacy Notes
    return {"gap_responses": {}, "resolved_gaps": {}, "legacy_notes": s}


def parse_row(row):
    """Parse a raw DB row: decode JSON section columns, build sessions dict."""
    eng = {}
    for k, v in row.items():
        if k in ALL_SECTION_COLS:
            if k in SCALAR_COLS:
                eng[k] = v or ""
            else:
                eng[k] = _decode_section_value(k, v)
        else:
            eng[k] = v

    eng["sessions"] = {}
    for snum, cols in SESSION_COLS.items():
        session = {}
        for c in cols:
            session[c] = eng.get(c, _empty_for_col(c))
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
        f"servicenow_ticket_url STRING, "
        f"current_session INT, status STRING, "
        f"created_at STRING, updated_at STRING, deleted_at STRING, "
        f"{section_ddl}"
        f") USING DELTA"
    )
    rows = sql_exec(f"DESCRIBE TABLE {TABLE}")
    existing = {r.get("col_name", "") for r in rows}
    # Top-level metadata columns added since v1
    for top_col in ("servicenow_ticket_url", "deleted_at"):
        if top_col not in existing:
            sql_run(f"ALTER TABLE {TABLE} ADD COLUMN {top_col} STRING")
    for col in ALL_SECTION_COLS:
        if col not in existing:
            sql_run(f"ALTER TABLE {TABLE} ADD COLUMN {col} STRING")

    # Async job runner persistence -- so app restarts don't strand in-flight jobs.
    sql_run(
        f"CREATE TABLE IF NOT EXISTS {JOBS_TABLE} ("
        f"job_id STRING, task_type STRING, creator_email STRING, "
        f"state STRING, started_at DOUBLE, finished_at DOUBLE, "
        f"result STRING, error STRING"
        f") USING DELTA"
    )
    # LLM cost / usage telemetry table.
    sql_run(
        f"CREATE TABLE IF NOT EXISTS {LLM_USAGE_TABLE} ("
        f"ts STRING, label STRING, endpoint STRING, "
        f"prompt_chars BIGINT, output_chars BIGINT, "
        f"prompt_tokens BIGINT, completion_tokens BIGINT, "
        f"latency_ms BIGINT, success BOOLEAN, error STRING"
        f") USING DELTA"
    )

ensure_table()
_recover_orphan_jobs()


# ---------------------------------------------------------------------------
# API: User
# ---------------------------------------------------------------------------

@app.route("/api/user")
def api_user():
    return jsonify({"email": get_current_user()})


def _user_workspace_client_from_token(user_token):
    """Build a WorkspaceClient from an explicit OBO token. Used by background
    job threads which can't access Flask's request context."""
    if not user_token:
        return None
    from databricks.sdk import WorkspaceClient as WC
    from databricks.sdk.core import Config
    cfg = Config(host=w.config.host, token=user_token, auth_type="pat")
    return WC(config=cfg)


def _user_workspace_client():
    """Build a WorkspaceClient using the forwarded user access token (OBO).
    Pulls the token from the current Flask request. Returns None if no token.
    """
    return _user_workspace_client_from_token(request.headers.get("X-Forwarded-Access-Token"))


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


def _user_is_bo_member(user_w):
    """True if the OBO user is in BO_GROUP. False on any error."""
    if not user_w:
        return False
    try:
        me = user_w.current_user.me()
        for g in (me.groups or []):
            if g.display == BO_GROUP:
                return True
    except Exception:
        pass
    return False


def _user_role(user_w):
    """Return ('coe' | 'bo' | 'analyst' | None) — the user's role for permission
    decisions. COE wins over BO; default authenticated users are 'analyst'.
    Returns None only if there's no OBO client (unauthenticated request).
    """
    if not user_w:
        return None
    is_coe = _user_is_coe_member(user_w)
    if is_coe:
        return "coe"
    if _user_is_bo_member(user_w):
        return "bo"
    return "analyst"


def _authorize_engagement(eid):
    """Verify engagement exists. Read access is open to any authenticated user
    (CAN_USE on the app is the access boundary). Mutation gating happens at the
    section level via _gate_engagement_routes — see permissions model below.

    Permissions model:
      - Default (any authenticated user): full read/write on all engagements,
        EXCEPT clicking Approve in S4 (COE only) and clicking BO Approved on
        benchmark rows (COE or BO group only).
      - COE group: full access including Approve and BO Approved.
      - BO group: read all; edit S1/S2 only; view S4 (read-only except for BO
        Approved checkboxes); cannot touch S3/S5/S6 or any AI/push action.

    Returns (eng, None) on success, or (None, error_response) if not found
    (404) or the warehouse couldn't answer (503 — transient, retry).
    """
    try:
        rows = sql_exec(f"SELECT * FROM {TABLE} WHERE engagement_id = :eid", {"eid": eid})
    except SqlTransientError:
        # Cold warehouse / timeout — distinct from "row doesn't exist." Return
        # 503 so the frontend can show "wait and retry" instead of confusing
        # the user with "Not found."
        return None, (jsonify({
            "error": "Database temporarily unavailable. The SQL warehouse may "
                     "be starting up; retry in a few seconds.",
            "transient": True,
        }), 503)
    if not rows:
        return None, (jsonify({"error": "Engagement not found"}), 404)
    return parse_row(rows[0]), None


import re as _re

_UUID_RE = _re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")


def _bo_can_access(method, sub_path):
    """Whitelist of operations a BO-only user (not COE) is allowed to perform.

    Everything else returns 403 for BOs. The whitelist is intentionally tight
    so adding new endpoints doesn't accidentally open BO access — if a future
    endpoint should be BO-accessible, it must be added here explicitly.
    """
    if method == "GET":
        return True
    if method == "PUT" and sub_path in ("/sessions/1", "/sessions/2"):
        return True
    if method == "PATCH" and sub_path == "/benchmarks/bo-approved":
        return True
    # ServiceNow URL lives in the Session 1 view BOs can edit.
    if method == "PATCH" and sub_path == "/servicenow-url":
        return True
    # Pre-work Excel upload: BOs can upload their own filled-in template and
    # apply it to S1/S2 (the only sessions they have edit rights to anyway).
    # Export is read-only and covers the same S1/S2 data they can already see.
    if method == "POST" and sub_path in (
        "/parse-prework", "/apply-prework", "/export-prework"
    ):
        return True
    return False


@app.before_request
def _gate_engagement_routes():
    """Apply _authorize_engagement to every /api/engagements/<eid>[/...] route,
    then layer BO restrictions on top.

    Layer 1: existence/auth — engagement exists and caller is authenticated.
    Layer 2: section-level role gate — BO-only users are limited to a tight
             whitelist of operations (read everything, edit S1/S2, click the
             BO-Approved checkbox via the dedicated PATCH endpoint).

    Only triggers when the path segment after /api/engagements/ looks like an
    engagement UUID (so sibling routes like /api/engagements/check-name are
    not mistakenly gated).
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
    if err:
        return err
    # Layer 2: BO-only users get a tight whitelist of operations.
    sub_path = remainder[len(eid):]  # "" or "/sessions/1" or "/coe-approve" etc.
    user_w = _user_workspace_client()
    role = _user_role(user_w)
    if role == "bo" and not _bo_can_access(request.method, sub_path):
        return jsonify({
            "error": f"Members of the '{BO_GROUP}' group cannot perform this action."
        }), 403
    return None


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


@app.route("/api/user/role")
def api_user_role():
    """Return the caller's role flags so the frontend can render the right UI.

    Frontend uses this to decide what tabs/buttons/checkboxes to show. The
    backend independently re-checks role on every mutation, so this is
    advisory-only — a frontend that fakes a role still gets 403'd server-side.
    """
    user_w = _user_workspace_client()
    return jsonify({
        "is_coe": _user_is_coe_member(user_w),
        "is_bo": _user_is_bo_member(user_w),
        "coe_group_name": COE_GROUP,
        "bo_group_name": BO_GROUP,
    })


# ---------------------------------------------------------------------------
# API: Engagements CRUD
# ---------------------------------------------------------------------------

@app.route("/api/engagements", methods=["GET"])
def list_engagements():
    """Return all non-deleted engagements. Read visibility is open to any
    authenticated user; the per-section write rules are enforced elsewhere
    (see permissions model in _authorize_engagement).
    """
    # Soft-deleted rows (deleted_at IS NOT NULL/empty) are hidden from the list.
    # The row stays in Delta so an admin can recover it via delete_at IS NULL/SQL.
    not_deleted = "(deleted_at IS NULL OR deleted_at = '')"
    try:
        rows = sql_exec(
            f"SELECT engagement_id, genie_space_name, business_owner_name, "
            f"business_owner_email, analyst_name, analyst_email, "
            f"current_session, status, coe_approval_status, created_at, updated_at "
            f"FROM {TABLE} WHERE {not_deleted} ORDER BY updated_at DESC"
        )
    except SqlTransientError:
        return jsonify({
            "error": "Database temporarily unavailable. The SQL warehouse may "
                     "be starting up; retry in a few seconds.",
            "transient": True,
        }), 503
    return jsonify(rows)


@app.route("/api/engagements/check-name")
def check_engagement_name():
    """Check if an engagement name is already taken.

    Optional `exclude_eid` query param: when renaming an existing engagement,
    its own current name should not count as a conflict.
    Soft-deleted engagements DO NOT count as conflicts -- the name is freed
    for reuse once the row is deleted.
    """
    name = request.args.get("name", "").strip()
    exclude_eid = request.args.get("exclude_eid", "").strip()
    if not name:
        return jsonify({"available": False})
    not_deleted = "(deleted_at IS NULL OR deleted_at = '')"
    if exclude_eid and _UUID_RE.match(exclude_eid):
        rows = sql_exec(
            f"SELECT COUNT(*) AS cnt FROM {TABLE} "
            f"WHERE genie_space_name = :name AND engagement_id != :eid "
            f"AND {not_deleted}",
            {"name": name, "eid": exclude_eid},
        )
    else:
        rows = sql_exec(
            f"SELECT COUNT(*) AS cnt FROM {TABLE} "
            f"WHERE genie_space_name = :name AND {not_deleted}",
            {"name": name},
        )
    count = int(rows[0]["cnt"]) if rows else 0
    return jsonify({"available": count == 0})


@app.route("/api/engagements", methods=["POST"])
def create_engagement():
    # BO-only users cannot create engagements (analysts and COE only).
    user_w = _user_workspace_client()
    if _user_role(user_w) == "bo":
        return jsonify({
            "error": f"Members of the '{BO_GROUP}' group cannot create engagements.",
        }), 403

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
        f"SELECT COUNT(*) AS cnt FROM {TABLE} "
        f"WHERE genie_space_name = :name "
        f"AND (deleted_at IS NULL OR deleted_at = '')",
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
        section_params[param_name] = _default_section_value(col)

    sql_run(
        f"INSERT INTO {TABLE} "
        f"(engagement_id, genie_space_name, business_owner_name, business_owner_email, "
        f"analyst_name, analyst_email, servicenow_ticket_url, "
        f"current_session, status, created_at, updated_at, "
        f"{section_cols_sql}) "
        f"VALUES (:eid, :space_name, :bo_name, :bo_email, :a_name, :a_email, :sn_url, "
        f"1, 'draft', :ts, :ts, {', '.join(section_defaults)})",
        {
            "eid": eid,
            "space_name": name,
            "bo_name": data.get("business_owner_name", "").strip(),
            "bo_email": data.get("business_owner_email", "").strip(),
            "a_name": data.get("analyst_name", "").strip(),
            "a_email": data.get("analyst_email", "").strip(),
            "sn_url": (data.get("servicenow_ticket_url") or "").strip(),
            "ts": ts,
            **section_params,
        },
    )
    return jsonify({"engagement_id": eid}), 201


@app.route("/api/engagements/<eid>", methods=["GET"])
def get_engagement(eid):
    # SELECT * returns updated_at / created_at as TIMESTAMP, but we hand the
    # value out to clients as the optimistic-lock token. Cast to STRING so
    # the load value matches what _check_optimistic_lock will compare against
    # later (Spark's canonical TIMESTAMP-string format).
    rows = sql_exec(
        f"SELECT *, "
        f"CAST(updated_at AS STRING) AS updated_at_str, "
        f"CAST(created_at AS STRING) AS created_at_str "
        f"FROM {TABLE} WHERE engagement_id = :eid",
        {"eid": eid},
    )
    if not rows:
        return jsonify({"error": "Not found"}), 404
    parsed = parse_row(rows[0])
    # Overwrite the TIMESTAMP-typed fields with their string aliases
    if "updated_at_str" in rows[0]:
        parsed["updated_at"] = rows[0].get("updated_at_str") or ""
    if "created_at_str" in rows[0]:
        parsed["created_at"] = rows[0].get("created_at_str") or ""
    return jsonify(parsed)


@app.route("/api/engagements/<eid>", methods=["PUT"])
def update_engagement(eid):
    """Update engagement metadata. Validates genie_space_name uniqueness against
    every OTHER engagement (the current row's own name does not count as a
    conflict, so saving without renaming is always fine).
    """
    data = request.json or {}
    name = (data.get("genie_space_name") or "").strip()
    if not name:
        return jsonify({"error": "genie_space_name is required"}), 400

    # Optimistic-lock check (no-op if If-Match header is absent)
    try:
        _check_optimistic_lock(eid)
    except StaleEngagementError as e:
        return jsonify({
            "error": "stale",
            "current_updated_at": e.current_updated_at,
            "message": "This engagement was updated by another user. Refresh to continue.",
        }), 409
    except ValueError as e:
        return jsonify({"error": str(e)}), 404

    # Uniqueness check, scoped to other (non-soft-deleted) engagements
    dup = sql_exec(
        f"SELECT COUNT(*) AS cnt FROM {TABLE} "
        f"WHERE genie_space_name = :name AND engagement_id != :eid "
        f"AND (deleted_at IS NULL OR deleted_at = '')",
        {"name": name, "eid": eid},
    )
    if dup and int(dup[0]["cnt"]) > 0:
        return jsonify({"error": "An engagement with this name already exists"}), 409

    ts = now_ts()
    sql_run(
        f"UPDATE {TABLE} SET "
        f"genie_space_name = :space_name, business_owner_name = :bo_name, "
        f"business_owner_email = :bo_email, analyst_name = :a_name, "
        f"analyst_email = :a_email, servicenow_ticket_url = :sn_url, "
        f"status = :status, updated_at = :ts "
        f"WHERE engagement_id = :eid",
        {
            "eid": eid,
            "space_name": name,
            "bo_name": (data.get("business_owner_name") or "").strip(),
            "bo_email": (data.get("business_owner_email") or "").strip(),
            "a_name": (data.get("analyst_name") or "").strip(),
            "a_email": (data.get("analyst_email") or "").strip(),
            "sn_url": (data.get("servicenow_ticket_url") or "").strip(),
            "status": data.get("status", "draft"),
            "ts": ts,
        },
    )
    return jsonify({"success": True, "updated_at": _read_updated_at(eid)})


@app.route("/api/engagements/<eid>/servicenow-url", methods=["PATCH"])
def patch_servicenow_url(eid):
    """Update ONLY servicenow_ticket_url.

    Lightweight side-field write, modeled on the BO-Approved PATCH: it lives
    OUTSIDE the optimistic lock and does NOT bump updated_at, so it can never
    race/409 the analyst's session autosave (which would otherwise reload and
    silently revert the typed URL). It also doesn't touch current_session, so
    saving the URL never regresses engagement progress. Existence/auth is
    handled by the before_request gate.
    """
    data = request.json or {}
    url = (data.get("servicenow_ticket_url") or "").strip()
    sql_run(
        f"UPDATE {TABLE} SET servicenow_ticket_url = :u WHERE engagement_id = :eid",
        {"eid": eid, "u": url},
    )
    return jsonify({"success": True})


def _notify_teams_review_ready(eng, eid, analyst_email):
    """Post an Adaptive Card to the COE Teams channel when an engagement is
    marked Ready for COE Review. Best-effort: any failure (unset webhook,
    network error, bad response) is swallowed so it never blocks the status
    flip. Targets a channel webhook, so the whole COE group sees it.
    """
    if not TEAMS_COE_WEBHOOK_URL:
        return  # Feature not configured — silently skip.
    try:
        space_name = (eng.get("genie_space_name") or "Untitled Space") if eng else "Untitled Space"
        sn_url = (eng.get("servicenow_ticket_url") or "") if eng else ""
        # Deep link back to Section 4 of this engagement, derived from the
        # incoming request's host so we don't need a hardcoded app URL.
        try:
            base = request.host_url.rstrip("/")
        except Exception:
            base = ""
        eng_link = f"{base}/engagements/{eid}" if base else ""

        facts = [
            {"title": "Engagement", "value": space_name},
            {"title": "Marked ready by", "value": analyst_email or "an analyst"},
        ]
        if sn_url:
            facts.append({"title": "ServiceNow", "value": f"[Open ticket]({sn_url})"})

        body = [
            {
                "type": "TextBlock",
                "size": "Medium",
                "weight": "Bolder",
                "text": "🔔 Ready for COE Review",
            },
            {
                "type": "TextBlock",
                "text": f"**{space_name}** has been marked ready for Center of Excellence review.",
                "wrap": True,
            },
            {"type": "FactSet", "facts": facts},
        ]
        actions = []
        if eng_link:
            actions.append({
                "type": "Action.OpenUrl",
                "title": "Open in Genie Discovery",
                "url": eng_link,
            })

        card = {
            "type": "AdaptiveCard",
            "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
            "version": "1.4",
            "body": body,
        }
        if actions:
            card["actions"] = actions

        payload = {
            "type": "message",
            "attachments": [
                {
                    "contentType": "application/vnd.microsoft.card.adaptive",
                    "content": card,
                }
            ],
        }
        requests.post(TEAMS_COE_WEBHOOK_URL, json=payload, timeout=10)
    except Exception as e:  # noqa: BLE001 — notification must never break the flip
        print(f"[teams] review-ready notification failed (non-fatal): {e}")


@app.route("/api/engagements/<eid>/request-review", methods=["PUT"])
def request_coe_review(eid):
    """Analyst action: mark an engagement 'Ready for COE Review'.

    Unlike coe-approve (COE-only), this is open to analysts — it only sets the
    status to 'ready_for_review', never to 'approved'/'changes_requested'
    (those stay COE-gated). BO-only users are blocked by the before_request
    gate since this path is NOT in _bo_can_access.

    Lightweight side-write modeled on the ServiceNow-URL PATCH: it lives
    OUTSIDE the optimistic lock and does NOT bump updated_at, so it can't
    race/409 the analyst's in-flight Session 4 autosave. The client mirrors
    the new value into its session-4 draft so the next autosave stays
    consistent. Fires the Teams notification best-effort on success.
    """
    eng, err = _authorize_engagement(eid)
    if err:
        return err
    # Marking ready re-enters the review flow, so a previously-completed
    # (Section 7 signed-off) engagement is no longer complete: roll status back
    # to 'in_progress' so it doesn't read as Complete on the home list. status
    # gates nothing; this keeps display consistent across surfaces.
    sql_run(
        f"UPDATE {TABLE} SET coe_approval_status = 'ready_for_review', "
        f"status = 'in_progress' "
        f"WHERE engagement_id = :eid",
        {"eid": eid},
    )
    _notify_teams_review_ready(eng, eid, get_current_user())
    return jsonify({"success": True, "status": "ready_for_review"})


@app.route("/api/engagements/<eid>", methods=["DELETE"])
def delete_engagement(eid):
    """Soft-delete: mark deleted_at instead of hard-removing the row.
    The row stays in Delta so a careless click doesn't lose months of work.
    Hidden from list_engagements + name-uniqueness checks; recoverable via
    direct URL or by clearing deleted_at via SQL.
    """
    ts = now_ts()
    sql_run(
        f"UPDATE {TABLE} SET deleted_at = :ts, updated_at = :ts "
        f"WHERE engagement_id = :eid",
        {"eid": eid, "ts": ts},
    )
    return jsonify({"success": True})


# ---------------------------------------------------------------------------
# API: Session saves
# ---------------------------------------------------------------------------

class StaleEngagementError(Exception):
    """Raised when an optimistic-lock check on engagement.updated_at fails.
    The HTTP layer translates this into 409 with the current updated_at so
    the client can refresh + retry."""
    def __init__(self, current_updated_at):
        super().__init__("stale engagement")
        self.current_updated_at = current_updated_at


def _check_optimistic_lock(eid):
    """If the request carries `If-Match: <updated_at>`, verify it matches
    the row's current updated_at. Raises StaleEngagementError on conflict.
    No header = no check (back-compat).

    Reads updated_at via CAST AS STRING so the comparison uses Spark's
    canonical TIMESTAMP-string format (the column is TIMESTAMP, not STRING,
    so a raw read can come back in different shapes).
    """
    expected = (request.headers.get("If-Match") or "").strip()
    if not expected:
        return
    actual = _read_updated_at(eid)
    if not actual:
        raise ValueError("Engagement not found")
    if actual != expected:
        raise StaleEngagementError(actual)


def _read_updated_at(eid):
    """Read engagement.updated_at AS STRING so we get the same canonical
    Spark-formatted value we'd compare against later (avoids round-trip
    format mismatches between Python's isoformat and Spark's TIMESTAMP
    string display)."""
    rows = sql_exec(
        f"SELECT CAST(updated_at AS STRING) AS updated_at "
        f"FROM {TABLE} WHERE engagement_id = :eid",
        {"eid": eid},
    )
    if not rows:
        return ""
    return (rows[0].get("updated_at") or "").strip()


def save_session(eid, session_num, data):
    """Update session columns for an engagement. Returns the new updated_at
    timestamp (in Spark's canonical string format) on success. Raises
    StaleEngagementError if the request's If-Match header doesn't match
    the row's current updated_at."""
    _check_optimistic_lock(eid)

    # Section 4 special case: preserve bo_approved on existing benchmark rows
    # so an analyst's full-section save doesn't overwrite a BO's approval.
    # Match by question text (the closest thing to a stable id we have); if
    # the analyst regenerated benchmarks, the question text changes and the
    # row legitimately resets to whatever value the payload supplies.
    if session_num == 4 and isinstance(data, dict) and "benchmark_questions" in data:
        try:
            current_rows = sql_exec(
                f"SELECT benchmark_questions FROM {TABLE} WHERE engagement_id = :eid",
                {"eid": eid},
            )
            if current_rows:
                raw = current_rows[0].get("benchmark_questions") or "[]"
                existing = json.loads(raw) if isinstance(raw, str) else (raw or [])
                approved_by_q = {
                    (r.get("question") or "").strip(): bool(r.get("bo_approved"))
                    for r in (existing or [])
                    if isinstance(r, dict)
                }
                new_list = data.get("benchmark_questions") or []
                for row in new_list:
                    if isinstance(row, dict):
                        q = (row.get("question") or "").strip()
                        if q in approved_by_q:
                            row["bo_approved"] = approved_by_q[q]
                data["benchmark_questions"] = new_list
        except Exception:
            # If anything goes wrong reading the current state, fall back to the
            # payload as-is. This is best-effort; the dedicated PATCH endpoint
            # is the authoritative path for BO approvals.
            pass

    cols = SESSION_COLS[session_num]
    set_parts = []
    ts = now_ts()
    params = {"eid": eid, "ts": ts}

    for col in cols:
        set_parts.append(f"{col} = :{col}")
        if col in SCALAR_COLS:
            params[col] = data.get(col, "")
        else:
            # OBJECT_COLS default to {} (dict), arrays default to []
            default = {} if col in OBJECT_COLS else []
            params[col] = json.dumps(data.get(col, default))

    # Advance the progress pointer (capped at the final session). Completion is
    # owned exclusively by the Session 7 production sign-off (see prod_approve):
    # saving any session NEVER marks the engagement complete, and never
    # downgrades one the COE has already signed off.
    next_session = min(session_num + 1, LAST_SESSION)
    set_parts.append(f"current_session = GREATEST(current_session, {next_session})")
    set_parts.append("status = CASE WHEN status = 'complete' THEN 'complete' ELSE 'in_progress' END")

    set_parts.append("updated_at = :ts")
    set_sql = ", ".join(set_parts)

    sql_run(f"UPDATE {TABLE} SET {set_sql} WHERE engagement_id = :eid", params)
    # Return the canonical stored value -- not Python's isoformat -- so the
    # client's next If-Match round-trips correctly. updated_at is a TIMESTAMP
    # column and Spark reformats on read.
    return _read_updated_at(eid)


def _save_session_response(eid, session_num):
    """Shared wrapper for the per-session save routes: handles optimistic-lock
    conflicts and returns the new updated_at so the client can carry it forward."""
    try:
        ts = save_session(eid, session_num, request.json)
    except StaleEngagementError as e:
        return jsonify({
            "error": "stale",
            "current_updated_at": e.current_updated_at,
            "message": "This engagement was updated by another user. Refresh to continue.",
        }), 409
    except ValueError as e:
        return jsonify({"error": str(e)}), 404
    return jsonify({"success": True, "updated_at": ts})


@app.route("/api/engagements/<eid>/sessions/1", methods=["PUT"])
def save_session_1(eid):
    return _save_session_response(eid, 1)


@app.route("/api/engagements/<eid>/sessions/2", methods=["PUT"])
def save_session_2(eid):
    return _save_session_response(eid, 2)


@app.route("/api/engagements/<eid>/sessions/3", methods=["PUT"])
def save_session_3(eid):
    return _save_session_response(eid, 3)


@app.route("/api/engagements/<eid>/sessions/4", methods=["PUT"])
def save_session_4(eid):
    return _save_session_response(eid, 4)


@app.route("/api/engagements/<eid>/sessions/5", methods=["PUT"])
def save_session_5(eid):
    return _save_session_response(eid, 5)


@app.route("/api/engagements/<eid>/sessions/6", methods=["PUT"])
def save_session_6(eid):
    return _save_session_response(eid, 6)


@app.route("/api/engagements/<eid>/sessions/7", methods=["PUT"])
def save_session_7(eid):
    return _save_session_response(eid, 7)


# ---------------------------------------------------------------------------
# API: Business Owner Pre-Work Excel Upload
# ---------------------------------------------------------------------------
# Lets the analyst (or BO) send the BO a .xlsx template before the working
# session, then upload the filled-in version to populate Sessions 1 and 2.
#
# Three endpoints:
#   GET  /api/template/business-owner-prework.xlsx -- download template
#   POST /api/engagements/<eid>/parse-prework      -- parse + validate, NO mutation
#   POST /api/engagements/<eid>/apply-prework      -- atomic write, optimistic lock
#
# Apply semantics: each chosen section is REPLACED (not appended). The preview
# UI shows the diff so the user opts in section-by-section. This makes
# double-upload idempotent.

PREWORK_TEMPLATE_VERSION = "1.0"
PREWORK_MAX_BYTES = 5 * 1024 * 1024  # 5 MB

# Standard S1 business-context questions. Pre-populated in the template (locked
# columns) so BOs only write the Notes column. Must stay in sync with
# Session1Form.tsx's CONTEXT_QUESTIONS.
_PREWORK_S1_CONTEXT = [
    ("What does your team do day-to-day?", "Scopes the question universe"),
    ("What decisions do you make with data?", "Identifies the high-value questions"),
    ("Who else on your team would use this?", "Sizes the audience and skill range"),
    ("How do you get ad hoc answers today?", "Reveals the bottleneck Genie solves"),
]

# Frequency dropdown values; must match Session1Form.tsx's REPORT_COLS select.
_PREWORK_FREQ_OPTIONS = ["Daily", "Weekly", "Monthly", "Quarterly", "Ad hoc"]

# Per-sheet config drives template generation AND parsing -- single source of
# truth so the two can't drift. Keys map to session column names.
_PREWORK_SHEETS = [
    {
        "name": "S1 Business Context",
        "key": "business_context",
        "session": 1,
        "headers": ["Question", "Why It Matters", "Your Notes"],
        "row_keys": ["question", "why_it_matters", "response"],
        "instruction": ("Answer each question in the 'Your Notes' column. "
                        "Don't edit the Question or Why It Matters columns."),
    },
    {
        "name": "S1 Pain Points",
        "key": "pain_points",
        "session": 1,
        "headers": ["Pain Point"],
        "row_keys": ["description"],
        "instruction": ("List the top frustrations your team has with getting "
                        "data answers today. One pain point per row."),
    },
    {
        "name": "S1 Existing Reports",
        "key": "existing_reports",
        "session": 1,
        "headers": ["Report/Dashboard Name", "What It Shows", "How Often Used", "Known Issues"],
        "row_keys": ["report_name", "what_it_shows", "frequency", "known_issues"],
        "instruction": ("Every report, dashboard, or spreadsheet your team "
                        "references regularly. 'How Often Used' must be one of: "
                        "Daily, Weekly, Monthly, Quarterly, Ad hoc."),
    },
    {
        "name": "S2 Question Bank",
        "key": "question_bank",
        "session": 2,
        "headers": ["Question", "Decision It Drives"],
        "row_keys": ["question_text", "decision_it_drives"],
        "instruction": ("Real questions your team needs answered. For each, "
                        "note the decision that question helps you make."),
    },
    {
        # Renamed from "Vocabulary & Metrics" in the app -- BOs were reading
        # the column labels as a pure glossary and missing that metrics belong
        # here too. Headers and seeded examples make the dual purpose explicit.
        "name": "S2 Key Terms & Metrics",
        "key": "vocabulary_metrics",
        "session": 2,
        "headers": ["Business Term or Metric",
                    "Definition or How It's Calculated",
                    "Other Names / Synonyms"],
        "row_keys": ["business_term", "what_they_mean", "synonyms"],
        "instruction": ("Include BOTH vocabulary (jargon, abbreviations, "
                        "filter logic) AND metrics (anything with a "
                        "calculation). If it's a number you report on, it "
                        "goes here. See the example rows below for both types."),
    },
]


def _build_prework_template():
    """Generate the pre-work .xlsx in memory and return a BytesIO.

    Pre-populates S1 Business Context with the standard prompts and seeds the
    Key Terms & Metrics sheet with three examples (one metric, one vocabulary
    term, one borderline case) to demonstrate the dual purpose. A hidden
    `_meta` sheet stores the template version so we can detect outdated
    uploads at parse time.
    """
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill, Alignment
    from openpyxl.utils import get_column_letter
    from openpyxl.worksheet.datavalidation import DataValidation

    wb = Workbook()
    ws_inst = wb.active
    ws_inst.title = "Instructions"
    ws_inst.column_dimensions["A"].width = 110
    ws_inst["A1"] = "Genie Discovery — Business Owner Pre-Work"
    ws_inst["A1"].font = Font(bold=True, size=14)
    ws_inst["A3"] = ("Your analyst will load this file into the discovery app "
                     "to populate your engagement before the working session.")
    overview_lines = [
        "",
        "How to use this workbook:",
        "  1. Fill out each sheet listed in the tabs below.",
        "  2. Do NOT rename sheets or column headers — the app uses those names to find your answers.",
        "  3. Leave a row blank if you don't have content for it. Blank rows are skipped.",
        "  4. Save the file and send it back to your analyst.",
        "",
        "Sheets in this workbook:",
    ]
    for i, line in enumerate(overview_lines):
        ws_inst[f"A{4+i}"] = line
    overview_end = 4 + len(overview_lines)
    for i, sheet in enumerate(_PREWORK_SHEETS):
        ws_inst[f"A{overview_end+i}"] = f"  • {sheet['name']}: {sheet['instruction']}"
        ws_inst[f"A{overview_end+i}"].alignment = Alignment(wrap_text=True, vertical="top")
        ws_inst.row_dimensions[overview_end + i].height = 45

    header_fill = PatternFill("solid", fgColor="1F4E78")
    header_font = Font(bold=True, color="FFFFFF")
    instruction_font = Font(italic=True, color="555555")

    for sheet in _PREWORK_SHEETS:
        ws = wb.create_sheet(sheet["name"])
        # Row 1: instruction banner spanning all columns
        n_cols = len(sheet["headers"])
        ws.cell(row=1, column=1, value=sheet["instruction"]).font = instruction_font
        if n_cols > 1:
            ws.merge_cells(start_row=1, start_column=1, end_row=1, end_column=n_cols)
        ws.row_dimensions[1].height = 45
        ws.cell(row=1, column=1).alignment = Alignment(wrap_text=True, vertical="top")
        # Row 2: header
        for col_idx, header in enumerate(sheet["headers"], start=1):
            cell = ws.cell(row=2, column=col_idx, value=header)
            cell.fill = header_fill
            cell.font = header_font
            cell.alignment = Alignment(horizontal="left", vertical="center")
            ws.column_dimensions[get_column_letter(col_idx)].width = 40

        # Pre-fill standard S1 context questions on rows 3+
        if sheet["key"] == "business_context":
            for r_idx, (q, why) in enumerate(_PREWORK_S1_CONTEXT, start=3):
                ws.cell(row=r_idx, column=1, value=q)
                ws.cell(row=r_idx, column=2, value=why)
                # Wrap the static columns so long prompts are readable
                ws.cell(row=r_idx, column=1).alignment = Alignment(wrap_text=True, vertical="top")
                ws.cell(row=r_idx, column=2).alignment = Alignment(wrap_text=True, vertical="top")
                ws.row_dimensions[r_idx].height = 40

        # Frequency dropdown on existing_reports' column C
        if sheet["key"] == "existing_reports":
            dv = DataValidation(
                type="list",
                formula1='"' + ",".join(_PREWORK_FREQ_OPTIONS) + '"',
                allow_blank=True,
            )
            ws.add_data_validation(dv)
            dv.add("C3:C200")

        # Seed three examples on the Key Terms & Metrics sheet to demonstrate
        # that BOTH metrics and vocabulary belong here. The parser detects and
        # skips these rows so they don't import as real data even if the BO
        # forgets to delete them.
        if sheet["key"] == "vocabulary_metrics":
            ws.cell(row=6, column=1, value="↓ EXAMPLES (delete these rows and replace with your own) ↓").font = (
                Font(italic=True, color="888888", bold=True)
            )
            ws.merge_cells(start_row=6, start_column=1, end_row=6, end_column=3)
            examples = [
                ("Net Revenue",
                 "Gross sales minus returns and discounts. This is a METRIC -- include calculation logic.",
                 "NR, Net Sales"),
                ("Active Customer",
                 "Customer with at least one order in the trailing 90 days. METRIC -- definition includes the rule.",
                 "Active account"),
                ("SKU",
                 "Stock-keeping unit; the unique identifier for a product. VOCABULARY -- just the definition.",
                 "Item ID, Product code"),
            ]
            for r_idx, (term, defn, syn) in enumerate(examples, start=7):
                ws.cell(row=r_idx, column=1, value=term).font = Font(italic=True, color="888888")
                ws.cell(row=r_idx, column=2, value=defn).font = Font(italic=True, color="888888")
                ws.cell(row=r_idx, column=3, value=syn).font = Font(italic=True, color="888888")
                ws.cell(row=r_idx, column=2).alignment = Alignment(wrap_text=True, vertical="top")
                ws.row_dimensions[r_idx].height = 30

    # Hidden metadata sheet for version detection at parse time
    meta = wb.create_sheet("_meta")
    meta["A1"] = "template_version"
    meta["B1"] = PREWORK_TEMPLATE_VERSION
    meta["A2"] = "generated_at"
    meta["B2"] = now_ts()
    meta.sheet_state = "hidden"

    buf = BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf


# Export-only benchmark sheet. Benchmarks live in Session 4 and are NOT part of
# the re-uploadable pre-work round-trip (they reference run results, bools, and
# nested objects), so this sheet is deliberately kept out of _PREWORK_SHEETS.
_BENCHMARK_EXPORT = {
    "name": "S4 Benchmarks",
    "headers": [
        "Question", "Category", "Difficulty", "Expected SQL",
        "Sample Result", "Measurement Summary (plain English)", "BO Approved",
    ],
    "row_keys": [
        "question", "category", "difficulty", "expected_sql",
        "sample_result", "notes", "bo_approved",
    ],
}


def _format_sample_result(sr):
    """Render a benchmark's sample_result object as readable plain text for a
    spreadsheet cell. Returns '' when there's nothing to show."""
    if not isinstance(sr, dict):
        return ""
    if sr.get("error"):
        return f"ERROR: {sr['error']}"
    cols = sr.get("columns") or []
    rows = sr.get("rows") or []
    if not cols and not rows:
        return ""
    lines = []
    ran_at = sr.get("ran_at")
    if ran_at:
        lines.append(f"(ran {ran_at})")
    if cols:
        lines.append(" | ".join(str(c) for c in cols))
    for r in rows[:25]:
        if isinstance(r, (list, tuple)):
            lines.append(" | ".join("" if v is None else str(v) for v in r))
        else:
            lines.append(str(r))
    rc = sr.get("row_count")
    if isinstance(rc, int) and (rc > 25 or sr.get("truncated")):
        lines.append(f"... ({rc} rows total{', truncated' if sr.get('truncated') else ''})")
    return "\n".join(lines)


def _build_prework_export(selected_keys, data, benchmarks=None):
    """Build a pre-work .xlsx populated with the engagement's current S1/S2 data.

    Mirrors the blank template's sheet names, headers, and `_meta` version so an
    exported file can be edited and re-uploaded through parse/apply-prework
    (full round-trip). Only the sheets whose key is in `selected_keys` are
    included; `data` is {section_key: [row_dict, ...]} already normalized to the
    section's `row_keys` by the caller.

    Differs from _build_prework_template: rows are filled from `data`, the S1
    business-context prompts are NOT auto-seeded (they come in via the data
    rows), and the Key Terms example rows are NOT seeded.
    """
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill, Alignment
    from openpyxl.utils import get_column_letter
    from openpyxl.worksheet.datavalidation import DataValidation

    # Preserve the canonical sheet order; include only the selected sheets.
    sheets = [s for s in _PREWORK_SHEETS if s["key"] in selected_keys]

    wb = Workbook()
    ws_inst = wb.active
    ws_inst.title = "Instructions"
    ws_inst.column_dimensions["A"].width = 110
    ws_inst["A1"] = "Genie Discovery — Engagement Export"
    ws_inst["A1"].font = Font(bold=True, size=14)
    ws_inst["A3"] = ("Exported from the discovery app with this engagement's "
                     "current answers. You can edit it and load it back via "
                     "'Upload Pre-Work' to apply changes to Sessions 1 & 2. "
                     "Don't rename sheets or column headers.")
    ws_inst["A3"].alignment = Alignment(wrap_text=True, vertical="top")
    ws_inst.row_dimensions[3].height = 45
    for i, sheet in enumerate(sheets):
        ws_inst[f"A{5+i}"] = f"  • {sheet['name']}: {sheet['instruction']}"
        ws_inst[f"A{5+i}"].alignment = Alignment(wrap_text=True, vertical="top")
        ws_inst.row_dimensions[5 + i].height = 45

    header_fill = PatternFill("solid", fgColor="1F4E78")
    header_font = Font(bold=True, color="FFFFFF")
    instruction_font = Font(italic=True, color="555555")

    for sheet in sheets:
        ws = wb.create_sheet(sheet["name"])
        n_cols = len(sheet["headers"])
        # Row 1: instruction banner spanning all columns
        ws.cell(row=1, column=1, value=sheet["instruction"]).font = instruction_font
        if n_cols > 1:
            ws.merge_cells(start_row=1, start_column=1, end_row=1, end_column=n_cols)
        ws.row_dimensions[1].height = 45
        ws.cell(row=1, column=1).alignment = Alignment(wrap_text=True, vertical="top")
        # Row 2: header
        for col_idx, header in enumerate(sheet["headers"], start=1):
            cell = ws.cell(row=2, column=col_idx, value=header)
            cell.fill = header_fill
            cell.font = header_font
            cell.alignment = Alignment(horizontal="left", vertical="center")
            ws.column_dimensions[get_column_letter(col_idx)].width = 40

        # Rows 3+: the engagement's current data for this section.
        rows = data.get(sheet["key"], []) or []
        for r_offset, row in enumerate(rows):
            r_idx = 3 + r_offset
            for col_idx, rk in enumerate(sheet["row_keys"], start=1):
                cell = ws.cell(row=r_idx, column=col_idx, value=row.get(rk, ""))
                cell.alignment = Alignment(wrap_text=True, vertical="top")
            ws.row_dimensions[r_idx].height = 30

        # Keep the frequency dropdown so edited rows stay valid on re-upload.
        if sheet["key"] == "existing_reports":
            dv = DataValidation(
                type="list",
                formula1='"' + ",".join(_PREWORK_FREQ_OPTIONS) + '"',
                allow_blank=True,
            )
            ws.add_data_validation(dv)
            dv.add("C3:C200")

    # Export-only Benchmarks sheet (S4). Not part of the re-upload round-trip;
    # the parser ignores unknown sheets, so this is safe to include.
    if benchmarks:
        ws = wb.create_sheet(_BENCHMARK_EXPORT["name"])
        ws.cell(
            row=1, column=1,
            value=("Benchmarks captured in Session 4. Export only — this sheet is not "
                   "read back on upload."),
        ).font = instruction_font
        ws.merge_cells(start_row=1, start_column=1, end_row=1,
                       end_column=len(_BENCHMARK_EXPORT["headers"]))
        ws.row_dimensions[1].height = 30
        ws.cell(row=1, column=1).alignment = Alignment(wrap_text=True, vertical="top")
        for col_idx, header in enumerate(_BENCHMARK_EXPORT["headers"], start=1):
            cell = ws.cell(row=2, column=col_idx, value=header)
            cell.fill = header_fill
            cell.font = header_font
            cell.alignment = Alignment(horizontal="left", vertical="center")
            # Wider columns for SQL and sample result
            width = 60 if header in ("Expected SQL", "Sample Result") else 36
            ws.column_dimensions[get_column_letter(col_idx)].width = width
        for r_offset, b in enumerate(benchmarks):
            r_idx = 3 + r_offset
            for col_idx, rk in enumerate(_BENCHMARK_EXPORT["row_keys"], start=1):
                val = b.get(rk, "")
                cell = ws.cell(row=r_idx, column=col_idx, value=val)
                cell.alignment = Alignment(wrap_text=True, vertical="top")
            ws.row_dimensions[r_idx].height = 60

    # Hidden metadata sheet — same version key the parser reads, so the export
    # re-uploads cleanly. `kind=export` distinguishes it from a blank template.
    meta = wb.create_sheet("_meta")
    meta["A1"] = "template_version"
    meta["B1"] = PREWORK_TEMPLATE_VERSION
    meta["A2"] = "generated_at"
    meta["B2"] = now_ts()
    meta["A3"] = "kind"
    meta["B3"] = "export"
    meta.sheet_state = "hidden"

    buf = BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf


def _parse_prework_xlsx(file_bytes):
    """Parse a filled-in pre-work workbook.

    Returns (parsed, warnings, errors, template_version).
      parsed:   dict keyed by section key with arrays of row-dicts. Empty
                rows are dropped. Seeded example rows on the Key Terms sheet
                are detected and skipped.
      warnings: non-fatal (template version mismatch, unknown frequency value,
                BO edited a standard S1 question prompt).
      errors:   fatal (missing sheets, renamed column headers, parse failure).
      template_version: string from the _meta sheet, "" if absent.

    Strictly read-only; no DB writes. Caller decides whether to apply.
    """
    from openpyxl import load_workbook

    warnings = []
    errors = []
    parsed = {sheet["key"]: [] for sheet in _PREWORK_SHEETS}
    template_version = ""

    try:
        wb = load_workbook(BytesIO(file_bytes), data_only=True)
    except Exception as e:
        return parsed, warnings, [f"Could not open file as .xlsx: {e}"], ""

    if "_meta" in wb.sheetnames:
        try:
            template_version = str(wb["_meta"]["B1"].value or "").strip()
        except Exception:
            template_version = ""
    if not template_version:
        warnings.append(
            "Template version not found in file. This may not have come from "
            "the app's template -- if you see import problems, download a "
            "fresh template from the upload dialog."
        )
    elif template_version != PREWORK_TEMPLATE_VERSION:
        warnings.append(
            f"Template version mismatch (file: {template_version}, current: "
            f"{PREWORK_TEMPLATE_VERSION}). Some fields may not import "
            "correctly. Download a fresh template if you see issues."
        )

    standard_s1_questions = {pair[0] for pair in _PREWORK_S1_CONTEXT}

    for sheet_cfg in _PREWORK_SHEETS:
        name = sheet_cfg["name"]
        if name not in wb.sheetnames:
            errors.append(
                f"Sheet '{name}' is missing. Don't rename or delete sheets — "
                "re-download the template if needed."
            )
            continue
        ws = wb[name]

        # Validate header row (row 2). Case-insensitive, whitespace-trimmed.
        actual = []
        for col_idx in range(1, len(sheet_cfg["headers"]) + 1):
            v = ws.cell(row=2, column=col_idx).value
            actual.append((v or "").strip() if isinstance(v, str) else str(v or "").strip())
        expected = [h.strip() for h in sheet_cfg["headers"]]
        if [h.lower() for h in actual] != [h.lower() for h in expected]:
            errors.append(
                f"Sheet '{name}' headers don't match. Expected {expected!r}, "
                f"got {actual!r}. Re-download the template if columns were changed."
            )
            continue

        # Identify the seeded-example rows on vocabulary_metrics so we don't
        # import them. Triggered by an "EXAMPLES" banner in column A; the next
        # three rows are skipped if they still hold the seeded values.
        skip_rows = set()
        if sheet_cfg["key"] == "vocabulary_metrics":
            for r_idx in range(3, min(ws.max_row, 12) + 1):
                v = ws.cell(row=r_idx, column=1).value
                if isinstance(v, str) and "examples" in v.lower() and (
                    "delete" in v.lower() or "replace" in v.lower()
                ):
                    skip_rows.add(r_idx)
                    skip_rows.update({r_idx + 1, r_idx + 2, r_idx + 3})
                    break

        for r_idx in range(3, ws.max_row + 1):
            if r_idx in skip_rows:
                continue
            values = []
            for col_idx in range(1, len(sheet_cfg["row_keys"]) + 1):
                v = ws.cell(row=r_idx, column=col_idx).value
                if v is None:
                    values.append("")
                elif isinstance(v, str):
                    values.append(v.strip())
                else:
                    values.append(str(v).strip())
            if not any(values):
                continue
            row_dict = dict(zip(sheet_cfg["row_keys"], values))

            if sheet_cfg["key"] == "existing_reports":
                freq = row_dict.get("frequency", "")
                if freq and freq not in _PREWORK_FREQ_OPTIONS:
                    warnings.append(
                        f"'{name}' row {r_idx}: 'How Often Used' value "
                        f"'{freq}' isn't one of {_PREWORK_FREQ_OPTIONS}. "
                        "Imported as-is; you can fix it in the app."
                    )

            if sheet_cfg["key"] == "business_context":
                q = row_dict.get("question", "")
                if q and q not in standard_s1_questions:
                    warnings.append(
                        f"'{name}' row {r_idx}: question text differs from "
                        "the standard prompt. The response will still import."
                    )

            parsed[sheet_cfg["key"]].append(row_dict)

    return parsed, warnings, errors, template_version


def _apply_prework_atomic(eid, sections_to_apply, parsed):
    """Atomically replace the chosen S1/S2 section columns on an engagement.

    sections_to_apply: iterable of section keys (e.g. {'business_context',
                       'question_bank'}). Anything not listed is left alone.
    parsed:            dict from _parse_prework_xlsx, keyed by section key.

    Writes all chosen columns in a single UPDATE so a partial failure can't
    leave the engagement half-applied. Honors If-Match optimistic lock the
    same way save_session does. Returns the new updated_at.
    """
    _check_optimistic_lock(eid)

    section_to_session = {s["key"]: s["session"] for s in _PREWORK_SHEETS}
    valid_keys = set(section_to_session.keys())
    keys = [k for k in sections_to_apply if k in valid_keys]
    if not keys:
        return _read_updated_at(eid)

    ts = now_ts()
    params = {"eid": eid, "ts": ts}
    set_parts = []
    for k in keys:
        set_parts.append(f"{k} = :{k}")
        params[k] = json.dumps(parsed.get(k, []))

    # Unlock the next tab: advance current_session to one past the highest
    # touched session, but never go backwards.
    max_session = max(section_to_session[k] for k in keys)
    set_parts.append(f"current_session = GREATEST(current_session, {max_session + 1})")
    set_parts.append(
        "status = CASE WHEN status = 'complete' THEN 'complete' ELSE 'in_progress' END"
    )
    set_parts.append("updated_at = :ts")

    sql_run(
        f"UPDATE {TABLE} SET {', '.join(set_parts)} WHERE engagement_id = :eid",
        params,
    )
    return _read_updated_at(eid)


@app.route("/api/template/business-owner-prework.xlsx", methods=["GET"])
def download_prework_template():
    """Stream the BO pre-work .xlsx template. Generated fresh per request so
    we never ship a stale binary."""
    try:
        buf = _build_prework_template()
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": f"Failed to build template: {e}"}), 500
    return send_file(
        buf,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        as_attachment=True,
        download_name="genie-discovery-bo-prework.xlsx",
    )


@app.route("/api/engagements/<eid>/parse-prework", methods=["POST"])
def parse_prework(eid):
    """Parse an uploaded pre-work .xlsx and return a preview WITHOUT mutating.

    multipart/form-data, field name `file`. Returns:
      { template_version, warnings: [...], errors: [...],
        preview: { section_key: [row_dict, ...] } }

    If `errors` is non-empty the client must show them and block apply.
    `warnings` are non-fatal and shown alongside the preview.
    """
    if "file" not in request.files:
        return jsonify({"error": "No file uploaded. Use the 'file' form field."}), 400
    f = request.files["file"]
    if not f.filename:
        return jsonify({"error": "No filename in upload."}), 400
    if not f.filename.lower().endswith(".xlsx"):
        return jsonify({
            "error": f"File must be a .xlsx workbook (got '{f.filename}'). "
                     "Re-save the file as .xlsx and try again."
        }), 400
    raw = f.read()
    if len(raw) > PREWORK_MAX_BYTES:
        return jsonify({
            "error": f"File too large ({len(raw)} bytes). Max is {PREWORK_MAX_BYTES} bytes."
        }), 413
    if len(raw) < 100:
        return jsonify({"error": "File appears empty or truncated."}), 400
    # .xlsx is a zip; magic bytes catch the "wrong format saved with right extension" case
    if not raw.startswith(b"PK\x03\x04"):
        return jsonify({
            "error": "File is not a valid .xlsx (zip signature missing). "
                     "It may have been saved as .xls or .csv with the wrong extension."
        }), 400

    parsed, warnings, errors, version = _parse_prework_xlsx(raw)
    return jsonify({
        "template_version": version,
        "warnings": warnings,
        "errors": errors,
        "preview": parsed,
    })


@app.route("/api/engagements/<eid>/apply-prework", methods=["POST"])
def apply_prework(eid):
    """Atomically apply parsed pre-work to an engagement.

    Body: { sections: [section_key, ...], data: {section_key: [row_dict, ...]} }
    The client sends back the data it parsed plus the sections the user opted
    to apply (per-section checkboxes in the preview). Honors If-Match for
    optimistic locking; returns 409 on stale.
    """
    payload = request.get_json(silent=True) or {}
    sections = payload.get("sections") or []
    data = payload.get("data") or {}
    if not isinstance(sections, list) or not isinstance(data, dict):
        return jsonify({"error": "Body must be {sections: [...], data: {...}}"}), 400

    valid_keys = {s["key"] for s in _PREWORK_SHEETS}
    sections_set = {s for s in sections if s in valid_keys}
    if not sections_set:
        return jsonify({"error": "No valid sections selected to apply."}), 400

    # Defensive normalization: only keep recognized row keys per section so a
    # crafted payload can't smuggle extra columns into the DB write. Strip and
    # coerce-to-str so we never serialize an unexpected type.
    normalized = {}
    for s_cfg in _PREWORK_SHEETS:
        k = s_cfg["key"]
        if k not in sections_set:
            continue
        rows = data.get(k) or []
        if not isinstance(rows, list):
            return jsonify({"error": f"data.{k} must be a list."}), 400
        normalized[k] = []
        for r in rows:
            if not isinstance(r, dict):
                continue
            clean = {rk: str(r.get(rk) or "").strip() for rk in s_cfg["row_keys"]}
            if any(clean.values()):
                normalized[k].append(clean)

    try:
        ts = _apply_prework_atomic(eid, sections_set, normalized)
    except StaleEngagementError as e:
        return jsonify({
            "error": "stale",
            "current_updated_at": e.current_updated_at,
            "message": "This engagement was updated by another user. Refresh to continue.",
        }), 409
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500
    return jsonify({"success": True, "updated_at": ts, "applied": sorted(sections_set)})


@app.route("/api/engagements/<eid>/export-prework", methods=["POST"])
def export_prework(eid):
    """Export selected S1/S2 sections to a .xlsx populated with current data.

    Body: { sections: [section_key, ...], data: {section_key: [row_dict, ...]} }
    The client posts the data it currently holds (WYSIWYG with the open forms).
    Read-only: builds and streams the workbook; no DB mutation, so no optimistic
    lock. The file matches the template shape and is re-uploadable via
    parse/apply-prework.
    """
    payload = request.get_json(silent=True) or {}
    sections = payload.get("sections") or []
    data = payload.get("data") or {}
    if not isinstance(sections, list) or not isinstance(data, dict):
        return jsonify({"error": "Body must be {sections: [...], data: {...}}"}), 400

    valid_keys = {s["key"] for s in _PREWORK_SHEETS}
    sections_set = {s for s in sections if s in valid_keys}
    want_benchmarks = "benchmarks" in sections
    if not sections_set and not want_benchmarks:
        return jsonify({"error": "No valid sections selected to export."}), 400

    # Same defensive normalization as apply-prework: keep only recognized row
    # keys per section, coerce to trimmed strings, drop fully-empty rows.
    normalized = {}
    for s_cfg in _PREWORK_SHEETS:
        k = s_cfg["key"]
        if k not in sections_set:
            continue
        rows = data.get(k) or []
        if not isinstance(rows, list):
            return jsonify({"error": f"data.{k} must be a list."}), 400
        normalized[k] = []
        for r in rows:
            if not isinstance(r, dict):
                continue
            clean = {rk: str(r.get(rk) or "").strip() for rk in s_cfg["row_keys"]}
            if any(clean.values()):
                normalized[k].append(clean)

    # Benchmarks (S4) are export-only and have their own shape: flatten the
    # nested sample_result to text, coerce bo_approved to Yes/No, keep the rest
    # as trimmed strings. Drop rows with no question AND no SQL.
    benchmark_rows = None
    if want_benchmarks:
        raw_bms = data.get("benchmarks") or []
        if not isinstance(raw_bms, list):
            return jsonify({"error": "data.benchmarks must be a list."}), 400
        benchmark_rows = []
        for b in raw_bms:
            if not isinstance(b, dict):
                continue
            question = str(b.get("question") or "").strip()
            sql = str(b.get("expected_sql") or "").strip()
            if not question and not sql:
                continue
            benchmark_rows.append({
                "question": question,
                "category": str(b.get("category") or "").strip(),
                "difficulty": str(b.get("difficulty") or "").strip(),
                "expected_sql": sql,
                "sample_result": _format_sample_result(b.get("sample_result")),
                "notes": str(b.get("notes") or "").strip(),
                "bo_approved": "Yes" if b.get("bo_approved") else "No",
            })

    try:
        buf = _build_prework_export(sections_set, normalized, benchmarks=benchmark_rows)
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": f"Failed to build export: {e}"}), 500
    return send_file(
        buf,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        as_attachment=True,
        download_name="genie-discovery-export.xlsx",
    )


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
    # When the COE requests changes, the engagement re-enters the review flow:
    # roll a previously-completed (Section 7 signed-off) engagement back to
    # 'in_progress' so it no longer reads as Complete anywhere (top chip, home
    # list). status gates nothing, so this is purely for consistent display.
    extra_set = ", status = 'in_progress'" if status == "changes_requested" else ""
    sql_run(
        f"UPDATE {TABLE} SET "
        f"coe_approval_status = :status, coe_approval_notes = :notes, "
        f"coe_reviewer_email = :reviewer, updated_at = :ts{extra_set} "
        f"WHERE engagement_id = :eid",
        {"eid": eid, "status": status, "notes": notes, "reviewer": reviewer, "ts": ts},
    )
    return jsonify({"success": True, "updated_at": ts})


@app.route("/api/engagements/<eid>/prod-approve", methods=["PUT"])
def prod_approve(eid):
    """Record the COE production sign-off (Session 7). Same server-side group
    enforcement as coe_approve: only COE members can set it. This is a recorded
    sign-off only -- it does NOT gate any other action (push, completion, etc.).

    Writes prod_* directly (and bumps updated_at) like coe_approve; the client
    refreshes its optimistic-lock token from the returned updated_at so the next
    autosave doesn't 409.
    """
    user_w = _user_workspace_client()
    if not user_w:
        return jsonify({"error": "reauth_required"}), 401
    if not _user_is_coe_member(user_w):
        return jsonify({
            "error": f"Only members of the '{COE_GROUP}' group can sign off engagements.",
        }), 403
    data = request.json or {}
    status = data.get("status", "")
    notes = data.get("notes", "")
    reviewer = get_current_user()
    # Engagement completion is owned by this sign-off: the engagement only shows
    # 'complete' once Section 7 is approved. Anything else (changes_requested,
    # pending) holds it at 'in_progress'.
    eng_status = "complete" if status == "approved" else "in_progress"
    ts = now_ts()
    sql_run(
        f"UPDATE {TABLE} SET "
        f"prod_approval_status = :status, prod_approval_notes = :notes, "
        f"prod_reviewer_email = :reviewer, status = :eng_status, updated_at = :ts "
        f"WHERE engagement_id = :eid",
        {"eid": eid, "status": status, "notes": notes, "reviewer": reviewer,
         "eng_status": eng_status, "ts": ts},
    )
    return jsonify({"success": True, "updated_at": ts, "engagement_status": eng_status})


@app.route("/api/engagements/<eid>/acknowledge", methods=["POST"])
def acknowledge(eid):
    """Record the analyst's Section 5 acknowledgments before Prototype Review
    unlocks: that they reviewed the AI-generated config, won't share the space
    until final sign-off, and will follow Genie best practices.

    Lock-free side-write (like bo_approved / servicenow-url): does NOT bump
    updated_at, so it can't race the session autosave. Server-stamps the
    accepting user + timestamp for accountability. All three boxes must be
    checked to count as accepted.
    """
    user_w = _user_workspace_client()
    if not user_w:
        return jsonify({"error": "reauth_required"}), 401
    data = request.json or {}
    reviewed_ai = bool(data.get("reviewed_ai"))
    no_share = bool(data.get("no_share"))
    best_practices = bool(data.get("best_practices"))
    ack = {
        "reviewed_ai": reviewed_ai,
        "no_share": no_share,
        "best_practices": best_practices,
        # accepted_at is only set when ALL boxes are checked — that's what the
        # frontend gate keys off to unlock Prototype Review.
        "accepted_by": get_current_user() if (reviewed_ai and no_share and best_practices) else "",
        "accepted_at": now_ts() if (reviewed_ai and no_share and best_practices) else "",
    }
    sql_run(
        f"UPDATE {TABLE} SET acknowledgments = :ack WHERE engagement_id = :eid",
        {"eid": eid, "ack": json.dumps(ack)},
    )
    return jsonify({"success": True, "acknowledgments": ack})


@app.route("/api/engagements/<eid>/space-access", methods=["GET"])
def space_access(eid):
    """Best-effort read of who currently has access to the engagement's pushed
    Genie space (Session 7 "Space Access Review").

    IMPORTANT: the REST permissions object-type for Genie spaces is not
    documented in our verified sources, so this is intentionally best-effort:
    we attempt the standard permissions API under OBO and, on ANY failure,
    return {available: false} with a reason. The UI never presents fabricated
    access data -- it falls back to a "manage sharing in Databricks" deep link.
    Confirm/adjust the endpoint during smoke testing; if it works, keep it.
    """
    user_w = _user_workspace_client()
    if not user_w:
        return jsonify({"error": "reauth_required"}), 401

    rows = sql_exec(
        f"SELECT genie_space_id, genie_space_url FROM {TABLE} WHERE engagement_id = :eid",
        {"eid": eid},
    )
    if not rows:
        return jsonify({"error": "Engagement not found"}), 404
    space_id = (rows[0].get("genie_space_id") or "").strip()
    space_url = (rows[0].get("genie_space_url") or "").strip()
    if not space_id:
        return jsonify({
            "available": False,
            "reason": "No Genie space has been pushed for this engagement yet.",
            "space_url": "",
        })

    # Convention-following attempt at the permissions API. Wrapped so a wrong
    # object-type or any error degrades gracefully instead of breaking the tab.
    try:
        resp = _genie_api_call(user_w, "GET", f"/api/2.0/permissions/genie/{space_id}")
        acl = resp.get("access_control_list", []) if isinstance(resp, dict) else []
        entries = []
        for a in acl:
            principal = (
                a.get("user_name") or a.get("group_name")
                or a.get("service_principal_name") or "unknown"
            )
            perms = a.get("all_permissions", []) or []
            levels = [p.get("permission_level") for p in perms if p.get("permission_level")]
            entries.append({"principal": principal, "levels": levels})
        return jsonify({
            "available": True,
            "space_url": space_url,
            "access": entries,
        })
    except Exception as e:
        print(f"[space-access] permissions read failed for {space_id}: {e}", flush=True)
        return jsonify({
            "available": False,
            "reason": "By design, this app can't read a Genie space's access list "
                      "on your behalf (Databricks Apps can't be granted the "
                      "access-management scope). Use the link to view and manage "
                      "who has access directly in Databricks.",
            "space_url": space_url,
        })


@app.route("/api/engagements/<eid>/benchmarks/bo-approved", methods=["PATCH"])
def patch_benchmark_bo_approved(eid):
    """Toggle the bo_approved flag on a single benchmark row.

    Open to any authenticated app user (no group gate) — the BO-Approved
    checkbox is intentionally not gatekept. Intentionally DOES NOT bump
    engagement.updated_at — this is a lightweight checkbox action that lives
    outside the optimistic lock so clicking the checkbox does not 409 the
    analyst's autosave on the same engagement. The analyst's
    save_session(4, ...) preserves bo_approved values from the DB (see merge in
    save_session), so the approval is never overwritten by an analyst.
    """
    user_w = _user_workspace_client()
    if not user_w:
        return jsonify({"error": "reauth_required"}), 401

    body = request.json or {}
    idx = body.get("idx")
    value = bool(body.get("value", False))
    if not isinstance(idx, int) or idx < 0:
        return jsonify({"error": "idx must be a non-negative integer"}), 400

    rows = sql_exec(
        f"SELECT benchmark_questions FROM {TABLE} WHERE engagement_id = :eid",
        {"eid": eid},
    )
    if not rows:
        return jsonify({"error": "Engagement not found"}), 404

    raw = rows[0].get("benchmark_questions") or "[]"
    try:
        bench = json.loads(raw) if isinstance(raw, str) else (raw or [])
    except Exception:
        bench = []

    if not isinstance(bench, list) or idx >= len(bench):
        return jsonify({"error": "Invalid benchmark index"}), 400
    if not isinstance(bench[idx], dict):
        return jsonify({"error": "Benchmark row is malformed"}), 400

    bench[idx]["bo_approved"] = value
    sql_run(
        f"UPDATE {TABLE} SET benchmark_questions = :b WHERE engagement_id = :eid",
        {"eid": eid, "b": json.dumps(bench)},
    )
    return jsonify({"success": True, "idx": idx, "value": value})


# ---------------------------------------------------------------------------
# API: Auto-summary (structured, no LLM)
# ---------------------------------------------------------------------------

def _generate_readiness_brief(eid):
    """Core Readiness Brief logic. Used by both the legacy sync endpoint and
    the async job task. Returns dict {summary, unacknowledged_gaps}.
    Raises on engagement-not-found or LLM/parse failure.

    Uses the brief task's model (default Haiku 4.5 — see _model_for) instead
    of the default Sonnet, since this task is structured extraction with
    citations and a small fast model handles it well.
    """
    rows = sql_exec(f"SELECT * FROM {TABLE} WHERE engagement_id = :eid", {"eid": eid})
    if not rows:
        raise ValueError("Engagement not found")
    eng = parse_row(rows[0])

    prompt = _build_readiness_brief_prompt(eng)
    raw = _call_llm_raw(prompt, label="brief")

    # Output format:
    # <markdown brief>
    # ---STRUCTURED-GAPS---
    # <JSON array of gap objects>
    SENTINEL = "---STRUCTURED-GAPS---"
    brief = ""
    gaps_raw = []
    if SENTINEL in raw:
        parts = raw.split(SENTINEL, 1)
        brief = parts[0].strip()
        gaps_text = parts[1].strip()
        if gaps_text.startswith("```"):
            gaps_text = gaps_text.split("\n", 1)[1] if "\n" in gaps_text else gaps_text
            if gaps_text.endswith("```"):
                gaps_text = gaps_text.rsplit("\n", 1)[0] if "\n" in gaps_text else gaps_text[:-3]
            gaps_text = gaps_text.strip()
            if gaps_text.lower().startswith("json"):
                gaps_text = gaps_text[4:].lstrip()
        try:
            gaps_raw = json.loads(gaps_text)
        except Exception as e:
            print(f"[readiness-brief] gap JSON parse failed: {e}; tail: {raw[-500:]}", flush=True)
            gaps_raw = []
    else:
        brief = raw.strip()
        print("[readiness-brief] sentinel missing; full output treated as brief", flush=True)

    if brief.startswith("```"):
        brief = brief.split("\n", 1)[1] if "\n" in brief else brief
        if brief.endswith("```"):
            brief = brief.rsplit("\n", 1)[0] if "\n" in brief else brief[:-3]
        brief = brief.strip()
        if brief.lower().startswith("markdown"):
            brief = brief[8:].lstrip()

    if not brief:
        raise RuntimeError("LLM returned empty brief")

    gaps = []
    if isinstance(gaps_raw, list):
        for g in gaps_raw:
            if not isinstance(g, dict):
                continue
            title = str(g.get("title", "")).strip()
            if not title:
                continue
            severity = str(g.get("severity", "Medium")).strip().capitalize()
            if severity not in {"Low", "Medium", "High"}:
                severity = "Medium"
            summary = str(g.get("summary", "")).strip()
            cits = g.get("citations") or []
            if not isinstance(cits, list):
                cits = []
            cits = [str(c).strip() for c in cits if str(c).strip()]
            gid = str(g.get("id", "")).strip() or _slug(title)
            gaps.append({
                "id": gid,
                "title": title,
                "severity": severity,
                "summary": summary,
                "citations": cits,
            })

    return {"summary": brief, "unacknowledged_gaps": gaps}


@register_task("readiness_brief")
def _task_readiness_brief(payload):
    """Async task wrapper for the Readiness Brief generator."""
    eid = (payload.get("engagement_id") or "").strip()
    if not eid:
        raise ValueError("engagement_id is required")
    return _generate_readiness_brief(eid)


@app.route("/api/engagements/<eid>/auto-summary")
def auto_summary(eid):
    """Legacy synchronous endpoint. Kept for backward compat; will 504 on long
    briefs. New callers should use POST /api/jobs/start with task_type=
    "readiness_brief" instead.
    """
    try:
        result = _generate_readiness_brief(eid)
    except ValueError as e:
        return jsonify({"summary": "", "error": str(e)}), 404
    except Exception as e:
        return jsonify({"error": _user_error("readiness-brief sync", e)}), 500
    return jsonify(result)


def _slug(text):
    """Lowercase, alnum + dashes only, collapse runs. Used to derive stable gap IDs."""
    import re
    s = re.sub(r"[^a-z0-9]+", "-", (text or "").lower())
    return s.strip("-")


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
            text = _qb_question(q)
            if not text:
                continue
            decision = (q.get("decision_it_drives") or "").strip() if isinstance(q, dict) else ""
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
            defn = _vm_definition(v)
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
        # Truncate very long YAML to keep prompt size + LLM latency bounded.
        # The brief mainly needs to know what dimensions/measures exist; the
        # full text isn't required.
        mv_lines = mv_yaml.splitlines()
        if len(mv_lines) > 60:
            shown = "\n".join(mv_lines[:60])
            lines.append(f"```yaml\n{shown}\n# ... ({len(mv_lines) - 60} more lines truncated for brief generation; full YAML lives in S3)\n```")
        else:
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

OUTPUT STRUCTURE (markdown). Each section header below appears EXACTLY ONCE, in this order. Do not duplicate any `##` heading. Do not invent extra top-level sections. Use bulleted lists, never markdown tables (the renderer doesn't support them).

## TL;DR
3-5 bullets: who the audience is, what they need, what was built, the headline risk.

## What We Learned
2-4 short paragraphs synthesizing S1+S2: the BO's day-to-day, decisions they make, pain points, existing reports they rely on, key vocabulary. Cite every paragraph.

## Technical Approach
2-3 short paragraphs on S3: source tables, key metrics defined, scope decisions, the metric view (or lack of one), the global filter if any. Cite specific SQL expressions or vocabulary terms. If you want to enumerate measures or dimensions, do it inline in prose — do NOT create a separate "Defined Measures" or duplicate "Data Plan" section for that.

## Data Plan
ONE bulleted list of tables and metric views being included in the Genie Space (from S4 Data Plan). Identifier + 1-line purpose each. If empty, flag this as a problem. Do NOT include another Data Plan section anywhere else in the brief.

## Coverage Analysis
Walk through the S2 Question Bank. For each question (group similar ones if there are many), output ONE bullet using this exact shape:

- **Q1: <question text>** — ✅ Answerable | ⚠️ Partial | ❌ Not addressed. <one-sentence justification with citations like [S3 SQL: denial_rate_pct]>

Use bulleted lists only — NEVER use markdown tables (`| col |` syntax) here, the renderer breaks on them. If the question bank is empty, say so explicitly and flag it as a problem.

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

OUTPUT FORMAT — emit the markdown brief, then a sentinel line, then a JSON array of structured unacknowledged gaps. Exactly this format:

<the full markdown brief — every section above, in order, no JSON wrapping, no escaping needed, raw markdown>
---STRUCTURED-GAPS---
[
  {{
    "id": "<stable slug from the title; lowercase, dashes only, no spaces>",
    "title": "<short headline, ~3-7 words>",
    "severity": "Low" | "Medium" | "High",
    "summary": "<1-2 sentences explaining the gap and why it matters>",
    "citations": ["<source tags like 'S1 Business Context Q&A'>"]
  }}
]

CRITICAL:
- The literal sentinel `---STRUCTURED-GAPS---` MUST appear on its own line between the markdown and the JSON. No other use of that string anywhere.
- The markdown brief comes first, raw — DO NOT wrap it in JSON, DO NOT escape its quotes or newlines, DO NOT put it inside a code fence.
- The JSON array after the sentinel MUST contain exactly the same gaps you listed under `### Unacknowledged Gaps` in the markdown — same titles, same order, same severities. The JSON is the source of truth for downstream UI.
- Do NOT include acknowledged gaps in the JSON array — only gaps the analyst did NOT flag in S3 Data Gaps.
- If there are no unacknowledged gaps, the JSON array is just `[]`."""


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
        text = _qb_question(q)
        if text:
            lines.append(f"- {text}")
    lines.append("### Vocabulary & Metric Definitions")
    for v in s2.get("vocabulary_metrics", []):
        if not isinstance(v, dict):
            continue
        term = v.get("business_term", "")
        defn = _vm_definition(v)
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
    # Analyst-authored example queries (S3) — these are deliberate, reviewed
    # examples to surface in the space, distinct from any the LLM drafts.
    aeq = [e for e in s3.get("example_queries", []) if isinstance(e, dict)
           and (e.get("sql") or "").strip()]
    if aeq:
        lines.append("### Analyst-Provided Example Queries (include these verbatim in example_queries, draft=false)")
        for e in aeq:
            q = (e.get("question") or "").strip() or "(no question text)"
            sql = (e.get("sql") or "").strip()
            guid = (e.get("usage_guidance") or "").strip()
            lines.append(f"- Q: {q}\n  SQL: {sql}" + (f"\n  Guidance: {guid}" if guid else ""))
    # Clarifying / disambiguation questions the analyst wants Genie to ask when a
    # request is ambiguous (e.g. "service line" -> clinical vs financial).
    cq = [c for c in s3.get("clarifying_questions", []) if isinstance(c, dict)
          and (c.get("trigger") or "").strip()]
    if cq:
        lines.append("### Analyst Clarifying Questions (fold into general_instructions as disambiguation bullets)")
        for c in cq:
            trig = (c.get("trigger") or "").strip()
            ask = (c.get("clarification") or "").strip()
            lines.append(f"- When the user asks about \"{trig}\": {ask}")
    lines.append("### Data Gaps")
    for g in s3.get("data_gaps", []):
        lines.append(f"- {g.get('gap_description','')}")
    lines.append("### Scope Boundaries")
    for s in s3.get("scope_boundaries", []):
        item = s.get("item") or s.get("topic") or ""
        lines.append(f"- {item}: {s.get('notes','')}")
    lines.append("")

    # Data plan was originally authored in S4 ("COE-Approved Data Plan"); as
    # of the S3 redesign it's authored in Session 3's Data Sources panel and
    # COE just reviews it during S4. The underlying field still lives in S4's
    # column set, but the heading below reflects the new flow so the LLM
    # doesn't infer a different review stage.
    lines.append("## Data Plan (authored in Session 3, reviewed in Session 4)")
    if s4.get("analyst_commentary"):
        lines.append(f"### Session 4 Analyst Commentary\n{s4.get('analyst_commentary','')}")
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
            # Cap each MV body so a sprawling YAML doesn't blow the prompt.
            parts.append(f"### Metric View: `{fqn}` — source: {src}\n```\n{_trunc(body, 4000)}\n```")
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

    # ----- Classified synonyms block -----
    # In Session 3, the analyst classifies each business term and, for terms
    # marked as Synonym, specifies whether the synonym is a column-level alias
    # (e.g. "acct_id" is another name for `customer_id`), a value-level alias
    # (e.g. "voided" is another name for `status = 'CANCELLED'`), or a
    # cross-cutting team term with no specific column. The S5 prompt needs
    # this routing so it doesn't dump column synonyms into general_instructions
    # — they belong on the column itself via Genie's column_configs surface.
    # Unset synonym_target falls back to cross_cutting for backward compat with
    # engagements classified before this routing existed.
    column_syns, value_syns, cross_syns = [], [], []
    s2_vocab_by_term = {
        (v.get("business_term") or "").strip(): v
        for v in (s2.get("vocabulary_metrics") or [])
        if isinstance(v, dict) and (v.get("business_term") or "").strip()
    }
    for c in (s3.get("term_classifications") or []):
        if not isinstance(c, dict):
            continue
        term = (c.get("business_term") or "").strip()
        types = c.get("types") or []
        if not term or "Synonym" not in types:
            continue
        vocab = s2_vocab_by_term.get(term)
        synonyms_raw = (vocab.get("synonyms") if vocab else "") or ""
        synonyms_list = [s.strip() for s in synonyms_raw.split(",") if s.strip()]
        if not synonyms_list:
            continue
        target = c.get("synonym_target") or {}
        kind = (target.get("kind") or "cross_cutting").strip().lower()
        col_fqn = (target.get("column_fqn") or "").strip()
        col_value = (target.get("column_value") or "").strip()
        if kind == "column" and col_fqn:
            column_syns.append((term, synonyms_list, col_fqn))
        elif kind == "value" and col_fqn and col_value:
            value_syns.append((term, synonyms_list, col_fqn, col_value))
        else:
            # Cross-cutting OR incomplete column/value rows (no target picked
            # yet). Fall back to cross_cutting so the LLM has something to
            # work with rather than silently dropping the term.
            cross_syns.append((term, synonyms_list))

    synonyms_block = ""
    if column_syns or value_syns or cross_syns:
        parts = []
        if column_syns:
            sub = ["### Column-level synonyms (alternate names for a SPECIFIC COLUMN)"]
            sub.append("These are AUTO-PUSHED to Genie's column_configs.synonyms at push time. Do NOT")
            sub.append("emit them in general_instructions or sql_expressions synonyms. Surface them")
            sub.append("in `narrative` as a manifest line under \"Pushed to column_configs:\" so the")
            sub.append("analyst sees what got attached to which column.")
            for term, syns, fqn in column_syns:
                joined_syns = ", ".join(f'"{s}"' for s in syns)
                sub.append(f"- `{fqn}` — also called: {joined_syns} (canonical term: \"{term}\")")
            parts.append("\n".join(sub))
        if value_syns:
            sub = ["### Value-level synonyms (alternate names for a SPECIFIC VALUE in a column)"]
            sub.append("These are AUTO-PUSHED at push time as (1) a description line on the column")
            sub.append("mapping value → aliases, and (2) enable_entity_matching=true on the column")
            sub.append("(Genie's supported way to handle value-level matching). Do NOT emit them in")
            sub.append("general_instructions or sql_expressions. Surface them in `narrative` under")
            sub.append("\"Pushed to column_configs:\" as a record of what got attached.")
            for term, syns, fqn, val in value_syns:
                joined_syns = ", ".join(f'"{s}"' for s in syns)
                sub.append(
                    f"- `{fqn}` value `'{val}'` — also called: {joined_syns} "
                    f"(canonical term: \"{term}\")"
                )
            parts.append("\n".join(sub))
        if cross_syns:
            sub = ["### Cross-cutting synonyms (no specific column — OK in general_instructions)"]
            sub.append("These are space-level team jargon with no specific column target. They MAY")
            sub.append("be included in general_instructions if they don't fit any other surface.")
            sub.append("Keep them tight; one bullet per term.")
            for term, syns in cross_syns:
                joined_syns = ", ".join(f'"{s}"' for s in syns)
                sub.append(f"- \"{term}\" — also called: {joined_syns}")
            parts.append("\n".join(sub))
        joined = "\n\n".join(parts)
        synonyms_block = f"""
<classified_synonyms>
The analyst classified each Synonym term in Session 3 with a routing target.
Column- and value-level synonyms in this block are AUTO-PUSHED to Genie's
column_configs at push time (no manual UI action needed). Use this block as
the authoritative routing source — do NOT re-derive from the raw vocab list
above.
{joined}
</classified_synonyms>
"""

    prompt = f"""You are a Databricks Genie Space configuration expert. An analyst just completed 4 sessions of discovery with a business owner. Use this discovery to populate every instruction surface Genie supports.

<discovery_data>
{discovery}
</discovery_data>
{mv_block}{schemas_block}{synonyms_block}{gold_block}{benchmarks_block}
Genie Space instruction surfaces (in order of preference per Databricks best practices):
1. SQL Expressions (Filters / Dimensions / Measures) — reusable business concepts attached to a table
2. Example SQL queries — full SQL for complex or frequent questions
3. Text instructions — LAST RESORT for rules that can't live in data/SQL

A single high-quality SQL example teaches Genie more than 20 lines of text instruction. Push logic INTO the data where you can; use text instructions only for things that cannot be expressed as SQL.

Global budgets (per Databricks Genie best practices):
- TOTAL knowledge-store snippets (sql_filters + sql_dimensions + sql_measures + example_queries combined) MUST stay under 200. Genie enforces this cap. Prefer fewer, higher-quality snippets.
- Aim for ≤ 5 tables in active focus. If the data plan includes more, still emit snippets that span them, but keep example_queries concentrated on the ≤ 5 most-used tables.

Produce a JSON object with exactly these fields:

1. "general_instructions" (string): Short bulleted text (~400-800 chars, 15 bullets max) that will be the space's ONLY text_instruction. Include ONLY content that CANNOT live in a more specific surface. Use these structured sub-buckets, each prefixed by a one-line header bullet:

   - Scope: 1 bullet — what this space answers and who it's for.
   - Out-of-scope: 1-2 bullets — topics Genie should refuse or hand off.
   - Global response standards: date format, rounding, required columns, time-zone, default ordering.
   - Clarification triggers: each as a single bullet using this exact pattern: "When <user_condition> AND <missing_info>, ask: <clarification_question>". Example: "When user asks about revenue AND no date range is specified, ask: which fiscal period (e.g. last quarter, YTD, or a custom range)?" If the engagement context has an "Analyst Clarifying Questions" section, EVERY one of those MUST appear here as a clarification-trigger bullet (preserve the analyst's intent, e.g. "When user asks about 'service line', ask: clinical service line or financial service line?").
   - Summaries: optional 1-2 bullets prefixed with "Summary:" that constrain how Genie phrases its prose answers (e.g. "Summary: always show totals as a single sentence with the metric name, the number formatted with thousands separators, and the period."). Only TEXT instructions affect summaries — SQL expressions and example queries do not. Include this bucket only if the analyst commentary specifies a response style.

   Synonym routing (see <classified_synonyms> block above for the authoritative list):
   - Cross-cutting synonyms (kind="cross_cutting") MAY be included as bullets here. Keep them tight.
   - Column-level synonyms (kind="column") MUST NOT be in general_instructions. They belong on the column via column_configs — surface them in `narrative` as a TODO instead.
   - Value-level synonyms (kind="value") MUST NOT be in general_instructions. They belong as entity matching on the column — surface them in `narrative` as a TODO instead.

   STRICT EXCLUSIONS — do NOT put any of these in general_instructions:
   - Metric definitions / formulas → those go in sql_measures with synonyms attached to the measure itself.
   - Column-level or value-level synonyms (see Synonym routing above) → narrative TODO, not text instructions.
   - Table/column semantics → those belong in UC table/column descriptions.
   - Duplicates of sql_filters / sql_dimensions / sql_measures / example_queries — every fact should live in exactly one surface; conflicting guidance across surfaces degrades quality.

   Use short atomic bullets starting with "- ". No markdown headers.

2. "sample_questions" (array of 5-8 strings): Curated, reworded sample questions from the question bank. Clear, natural phrasing, covering main use cases. Shown to users when they open the space.

IMPORTANT SQL qualification rule for snippets below: Genie infers the table from qualified column references in the SQL. Every column reference in snippet SQL MUST be prefixed with the SHORT table name (the last segment of the FQN). Example: for table `my_catalog.my_schema.orders`, write `orders.status`, NOT `status` and NOT `my_catalog.my_schema.orders.status`. The `table` field in each entry is metadata for the analyst UI and is NOT pushed to Genie.

3. "sql_filters" (array): Reusable WHERE-clause expressions. Each: {{"name": "snake_case_id", "sql": "short_table.column = 'value'", "table": "catalog.schema.table", "display_name": "Friendly Name", "synonyms": ["..."], "description": "..."}}. Example: {{"name": "cancelled_orders", "sql": "orders.status = 'CANCELLED'", "table": "my_catalog.my_schema.orders", "display_name": "Cancelled Orders"}}

4. "sql_dimensions" (array): Reusable grouping/SELECT column expressions. Same shape as sql_filters. Example: {{"name": "order_year", "sql": "YEAR(orders.created_at)", "table": "my_catalog.my_schema.orders", "display_name": "Order Year"}}

5. "sql_measures" (array): Reusable aggregate expressions (COUNT/SUM/AVG/etc). Same shape. Seed from the analyst's Session 3 SQL Expressions — classify each as filter/dimension/measure based on its SQL (aggregates → measure; WHERE-style predicates → filter; plain column exprs → dimension). Validate syntax and rewrite column references to use the short table prefix (e.g., rewrite `COUNT(CASE WHEN status = 'CANCELLED' THEN 1 END) * 100.0 / COUNT(*)` on table `my_catalog.my_schema.orders` to `COUNT(CASE WHEN orders.status = 'CANCELLED' THEN 1 END) * 100.0 / COUNT(orders.*)`).

6. "example_queries" (array, 3-6 items): Full SQL examples for complex/common questions from the question bank. Each: {{"question": "...", "sql": "...", "draft": true, "usage_guidance": "..."}}. SQL MUST use fully qualified `catalog.schema.table` references because example queries are standalone. Only include questions where you can write reasonably confident SQL given the tables in scope — skip speculative ones. Set "draft": true for the ones YOU author so the analyst reviews. EXCEPTION: any query in the engagement context's "Analyst-Provided Example Queries" section MUST be included VERBATIM (do not rewrite the SQL) with "draft": false — the analyst already authored and vetted these. These analyst queries are IN ADDITION to the 3-6 you draft.

   Trusted Assets tip: for the 1-3 highest-value recurring questions (the ones a BO will ask repeatedly with different parameters), write the SQL using `:param_name` placeholders (e.g. `WHERE orders.region = :region`) and note this in usage_guidance. When Genie matches the exact parameterized template, the response is labeled "Trusted" — a major reliability signal. Only do this for questions where you can confidently parameterize; don't force it.

7. "narrative" (string): 3-5 sentences for the analyst review screen plus an optional Pushed-to-column-configs manifest. MUST include:
   - What this space answers (one-line space purpose).
   - Target audience (which roles/teams will use it).
   - Out-of-scope topics (what it intentionally won't cover — pulled from Session 3 Scope Boundaries).
   - What was configured (high-level count summary: N measures, M filters, K example queries).
   - One sentence on KNOWN gaps the analyst should review before push (data gaps, low confidence example_queries, missing benchmarks).

   If <classified_synonyms> contains column-level or value-level synonyms, end the narrative with a "Pushed to column_configs at push time:" line followed by a markdown bulleted list — one bullet per pushed mapping. Format examples:
   - "Column synonyms on `catalog.schema.table.column_name`: \"alt_name_1\", \"alt_name_2\""
   - "Entity matching + value-description on `catalog.schema.table.status` for value 'CANCELLED': \"voided\", \"killed\""
   These are AUTO-PUSHED — analyst doesn't need to set them in the Genie UI manually. The manifest is just a record so the analyst can verify the right metadata landed on the right columns. If <classified_synonyms> has no column/value entries, omit this section entirely.

Return ONLY the JSON object. No markdown fences, no preamble, no trailing commentary. Begin with {{ and end with }}."""

    return prompt


# --- LLM tunables ---
#
# Hard cap on prompt size. Catches runaway engagements before paying for a
# slow LLM call that might time out or return garbage. ~80K chars is roughly
# 20K input tokens — generous, but bounded.
MAX_PROMPT_CHARS = int(os.getenv("LLM_MAX_PROMPT_CHARS") or "80000")

# Per-cell character cap when stringifying a single field into a prompt.
# Prevents one runaway value (long description, multi-paragraph note,
# ten-page MV YAML) from dominating the input. Truncates with an ellipsis.
MAX_CELL_CHARS = int(os.getenv("LLM_MAX_CELL_CHARS") or "1500")


def _trunc(s, max_chars=MAX_CELL_CHARS):
    """Truncate a string for inclusion in an LLM prompt. Returns the original
    if short, else a head slice + ellipsis marker that signals truncation
    to the LLM.
    """
    if s is None:
        return ""
    s = str(s)
    if len(s) <= max_chars:
        return s
    return s[:max_chars] + f"\n…[truncated, {len(s) - max_chars} chars dropped]"

# Realistic max output tokens per task. Sonnet emits ~50 tok/sec, so 16K
# tokens = 5+ min worst case (the timeout cliff). Right-sizing per task is
# the single biggest speedup lever.
TASK_MAX_TOKENS = {
    "brief":              16000,  # citation-heavy markdown + structured gaps trailer
    "plan":               10000,  # large structured JSON (filters/dimensions/measures/example_queries/narrative)
    "mv-yaml":             6000,  # YAML output, typically 2-3K tokens
    "mv-yaml-fix":         6000,  # retry with corrected columns
    "benchmark-draft":     3000,  # 12 questions, ~1-2K tokens
    "benchmark-sql":       1500,  # one SQL query
    "benchmark-sql-fix":   1500,  # retry with error context
    "benchmark-summary":    600,  # one-paragraph plain English
}
DEFAULT_MAX_TOKENS = 8000

# Per-task model overrides. Each defaults to the global LLM_ENDPOINT (Haiku
# 4.5) but can be pointed at a different endpoint via env var. Listed
# explicitly so it's clear which tasks accept overrides. To revert any task
# (or the whole app) to Sonnet 4.6, set the corresponding *_LLM_ENDPOINT_NAME
# env var in app.yaml to "databricks-claude-sonnet-4-6".
TASK_MODEL_ENV = {
    "brief":              "BRIEF_LLM_ENDPOINT_NAME",
    "benchmark-draft":    "BENCHMARK_DRAFT_LLM_ENDPOINT_NAME",
    "benchmark-summary":  "BENCHMARK_SUMMARY_LLM_ENDPOINT_NAME",
}
TASK_MODEL_DEFAULT = {
    "brief":              "databricks-claude-haiku-4-5",
    "benchmark-draft":    "databricks-claude-haiku-4-5",
    "benchmark-summary":  "databricks-claude-haiku-4-5",
}


def _model_for(label):
    """Pick the LLM endpoint for a task: env var override, then per-task default,
    then global LLM_ENDPOINT."""
    env_name = TASK_MODEL_ENV.get(label)
    if env_name:
        configured = os.getenv(env_name)
        if configured:
            return configured
        if label in TASK_MODEL_DEFAULT:
            return TASK_MODEL_DEFAULT[label]
    return LLM_ENDPOINT


def _max_tokens_for(label, override=None):
    """Pick max_tokens for a task. Caller can override; otherwise use the
    per-task default; falls back to DEFAULT_MAX_TOKENS for unknown tags."""
    if override is not None:
        return override
    return TASK_MAX_TOKENS.get(label, DEFAULT_MAX_TOKENS)


def _call_llm_raw(prompt, max_tokens=None, model=None, label=None, retries=2):
    """Call the Databricks serving endpoint and return the raw text content (no JSON parse).

    Use this when the prompt asks for non-JSON output (e.g. markdown with a
    structured trailer) — JSON-wrapping markdown content breaks on every
    backtick or newline the LLM emits.

    `max_tokens` defaults to the per-task value from TASK_MAX_TOKENS based on
    `label`; explicit override wins. Right-sizing this is the biggest speedup
    lever — Sonnet's output rate is ~50 tok/sec.

    `model` overrides the default model selection. Without an override, the
    model is resolved via `_model_for(label)`: per-task env var → per-task
    default → global LLM_ENDPOINT.

    `label` is a short tag for timing logs and per-task tuning lookup.

    `retries` is the number of additional attempts on transient failures
    (5xx, rate limit, network) with exponential backoff. Set to 0 to disable.
    """
    tag = label or "llm"
    endpoint = model or _model_for(tag)
    resolved_max = _max_tokens_for(tag, max_tokens)
    prompt_chars = len(prompt)

    if prompt_chars > MAX_PROMPT_CHARS:
        raise ValueError(
            f"Prompt for '{tag}' is {prompt_chars} chars (cap: {MAX_PROMPT_CHARS}). "
            "Prompt builder needs truncation, or LLM_MAX_PROMPT_CHARS env var "
            "needs to be raised. Aborting before paying for a slow LLM call."
        )

    started = time.time()
    print(f"[{tag}] start endpoint={endpoint} prompt_chars={prompt_chars} max_tokens={resolved_max}", flush=True)
    last_exc = None
    for attempt in range(retries + 1):
        try:
            resp = llm_w.serving_endpoints.query(
                name=endpoint,
                messages=[ChatMessage(role=ChatMessageRole.USER, content=prompt)],
                max_tokens=resolved_max,
                temperature=0.2,
            )
            break
        except Exception as e:
            last_exc = e
            err_str = str(e).lower()
            transient = (
                "rate limit" in err_str
                or "429" in err_str
                or "503" in err_str
                or "504" in err_str
                or "timeout" in err_str
                or "connection" in err_str
            )
            if attempt < retries and transient:
                # Exponential backoff with jitter: 1.5s, 3s, 6s...
                delay = 1.5 * (2 ** attempt) * (0.75 + random.random() * 0.5)
                print(f"[{tag}] transient error, retry {attempt+1}/{retries} in {delay:.1f}s: {type(e).__name__}", flush=True)
                time.sleep(delay)
                continue
            elapsed = time.time() - started
            print(f"[{tag}] FAILED after {elapsed:.1f}s ({attempt+1} attempt(s)): {type(e).__name__}: {e}", flush=True)
            _log_llm_usage(
                tag, endpoint, prompt_chars, 0, int(elapsed * 1000),
                success=False, error=f"{type(e).__name__}: {e}",
            )
            raise
    else:
        # Exhausted retries
        if last_exc:
            raise last_exc

    # Best-effort: pull token counts off the response if the endpoint exposes
    # a `usage` object. Handles both raw-dict and SDK-object response shapes.
    prompt_tokens = 0
    completion_tokens = 0
    try:
        if isinstance(resp, dict) and resp.get("usage"):
            u = resp["usage"]
            prompt_tokens = int(u.get("prompt_tokens") or 0)
            completion_tokens = int(u.get("completion_tokens") or 0)
        elif hasattr(resp, "usage") and resp.usage is not None:
            prompt_tokens = int(getattr(resp.usage, "prompt_tokens", 0) or 0)
            completion_tokens = int(getattr(resp.usage, "completion_tokens", 0) or 0)
    except Exception:
        pass

    if isinstance(resp, dict):
        d = resp
    elif hasattr(resp, "as_dict"):
        d = resp.as_dict()
    else:
        d = {"choices": [{"message": {"content": resp.choices[0].message.content}}]}
    content = d["choices"][0]["message"]["content"]
    elapsed = time.time() - started
    print(f"[{tag}] done in {elapsed:.1f}s output_chars={len(content)} pt={prompt_tokens} ct={completion_tokens}", flush=True)
    _log_llm_usage(
        tag, endpoint, prompt_chars, len(content), int(elapsed * 1000),
        prompt_tokens=prompt_tokens, completion_tokens=completion_tokens,
        success=True,
    )
    return content


def _call_llm(prompt, model=None, label=None, max_tokens=None):
    """Call the LLM and return parsed JSON. Tolerates ```json fences."""
    content = _call_llm_raw(prompt, model=model, label=label, max_tokens=max_tokens)
    text = content.strip()
    if text.startswith("```"):
        text = text.split("\n", 1)[1] if "\n" in text else text
        if text.endswith("```"):
            text = text.rsplit("\n", 1)[0] if "\n" in text else text[:-3]
        text = text.strip()
        if text.startswith("json"):
            text = text[4:].strip()
    return json.loads(text)


def _do_generate_plan(eid, user_token, warehouse_id):
    """Core generate-plan logic. Used by sync endpoint and async task.

    NOTE: this function persists results to Delta on success, so failures
    leave Session 5 untouched (consistent rollback semantics).
    """
    rows = sql_exec(f"SELECT * FROM {TABLE} WHERE engagement_id = :eid", {"eid": eid})
    if not rows:
        raise ValueError("Engagement not found")
    eng = parse_row(rows[0])

    user_w = _user_workspace_client_from_token(user_token)
    return _do_generate_plan_inner(eid, eng, user_w, warehouse_id)


@register_task("generate_plan")
def _task_generate_plan(payload):
    return _do_generate_plan(
        eid=(payload.get("engagement_id") or "").strip(),
        user_token=payload.get("_user_token", ""),
        warehouse_id=(payload.get("warehouse_id") or "").strip(),
    )


@app.route("/api/engagements/<eid>/generate-plan", methods=["POST"])
def generate_plan(eid):
    """Legacy sync endpoint. New callers should use the async job runner
    (POST /api/jobs/start with task_type=generate_plan)."""
    body = request.get_json(silent=True) or {}
    try:
        return jsonify(_do_generate_plan(
            eid,
            user_token=request.headers.get("X-Forwarded-Access-Token") or "",
            warehouse_id=(body.get("warehouse_id") or "").strip(),
        ))
    except ValueError as e:
        return jsonify({"error": str(e)}), 404
    except Exception as e:
        return jsonify({"error": _user_error("generate-plan sync", e)}), 500


def _do_generate_plan_inner(eid, eng, user_w, warehouse_id):
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

    # user_w is now passed in (built from token by the outer wrapper)
    # so this function is safe to call from a background thread.

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

    prompt = _build_plan_prompt(eng, schemas=schemas, mv_definitions=mv_definitions)
    plan = _call_llm(prompt, label="plan")

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

    return {
        "general_instructions": general_instructions,
        "sample_questions": sample_questions,
        "sql_filters": sql_filters,
        "sql_dimensions": sql_dimensions,
        "sql_measures": sql_measures,
        "example_queries": example_queries,
        "joins": joins,
        "narrative": narrative,
        "warnings": warnings,
    }


# ---------------------------------------------------------------------------
# API: Benchmark drafting (Session 4)
# ---------------------------------------------------------------------------

def _build_benchmark_draft_prompt(eng, count=12):
    """Draft benchmark questions from full engagement context (Sessions 1-4)."""
    s1 = eng["sessions"].get("1", {}) or {}
    s2 = eng["sessions"].get("2", {}) or {}
    s3 = eng["sessions"].get("3", {}) or {}
    s4 = eng["sessions"].get("4", {}) or {}

    lines = []
    lines.append(f"Genie Space: {eng.get('genie_space_name', '')}")
    lines.append("")

    # S1: Business Context Q&A (the BO's own words about their work)
    bc = s1.get("business_context", []) or []
    if bc:
        lines.append("Business Context (BO answers — use this language):")
        for b in bc:
            if not isinstance(b, dict):
                continue
            q = (b.get("question") or "").strip()
            notes = (b.get("response") or "").strip()
            if q and notes:
                lines.append(f"- Q: {q}")
                lines.append(f"  A: {notes}")

    lines.append("")
    lines.append("Pain Points:")
    for pp in s1.get("pain_points", []) or []:
        if isinstance(pp, dict):
            lines.append(f"- {pp.get('description', '')}")

    lines.append("")
    lines.append("Question Bank (from business owner — prefer this phrasing):")
    qb = s2.get("question_bank", []) or []
    qb_lines = [f"- {t}" for t in (_qb_question(q) for q in qb) if t]
    if qb_lines:
        lines.extend(qb_lines)
    else:
        lines.append("(empty — no BO-captured questions; rely on Business Context Q&A above for phrasing)")

    # S2: Vocabulary with synonyms (critical for Edge Case generation)
    vm = s2.get("vocabulary_metrics", []) or []
    if vm:
        lines.append("")
        lines.append("Vocabulary & Synonyms (use these in Edge Case questions to test synonym handling):")
        for v in vm:
            if not isinstance(v, dict):
                continue
            term = (v.get("business_term") or "").strip()
            defn = _vm_definition(v)
            synonyms = (v.get("synonyms") or "").strip()
            if term:
                line = f"- {term}"
                if defn:
                    line += f" — {defn}"
                if synonyms:
                    line += f" (synonyms: {synonyms})"
                lines.append(line)

    # S3: Scope boundaries (what's IN/OUT — questions must respect)
    sb = s3.get("scope_boundaries", []) or []
    if sb:
        lines.append("")
        lines.append("Scope Boundaries (do NOT generate questions outside scope):")
        for b in sb:
            if isinstance(b, dict):
                item = (b.get("item") or b.get("topic") or "").strip()
                scope_status = (b.get("in_scope") or b.get("status") or "").strip()
                if item:
                    lines.append(f"- {item}: {scope_status}")

    lines.append("")
    lines.append("Key Metrics / SQL Expressions:")
    for e in s3.get("sql_expressions", []) or []:
        if isinstance(e, dict):
            lines.append(
                f"- {e.get('metric_name','')} ({e.get('display_name','')}): "
                f"`{e.get('sql_code','')}` on {e.get('uc_table','')}"
            )

    # S3: Text instructions (rules the analyst flagged)
    ti = s3.get("text_instructions", []) or []
    if ti:
        lines.append("")
        lines.append("Analyst Rules / Instructions:")
        for t in ti:
            if isinstance(t, dict):
                title = (t.get("title") or "").strip()
                instr = (t.get("instruction") or "").strip()
                if title or instr:
                    lines.append(f"- {title}: {instr}")

    # S3: Metric view YAML (the actual dimensions/measures Genie sees)
    mv_yaml = (s3.get("metric_view_yaml") or "").strip()
    if mv_yaml:
        lines.append("")
        lines.append("Generated Metric View YAML (the actual dimensions/measures the Genie Space exposes):")
        lines.append("```yaml")
        lines.append(mv_yaml)
        lines.append("```")

    # S4: Data plan WITH type (Table vs Metric View)
    lines.append("")
    lines.append("Data Plan (in-scope sources):")
    for d in s4.get("data_plan", []) or []:
        if isinstance(d, dict) and d.get("include_in_space") == "Yes":
            tbl = (d.get("table_or_view") or "").strip()
            typ = (d.get("type") or "Table").strip()
            if tbl:
                lines.append(f"- {tbl} ({typ})")

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
- Every in-scope table or metric view appears in at least one question. If a Metric View is in scope, prioritize testing its measures and dimensions by name.
- About 70% Core (realistic questions), 30% Edge Case. For Edge Cases:
  * Use synonyms from the Vocabulary section to test synonym handling (e.g., if "BCBS" is a synonym for "Blue Cross", a question like "What's BCBS's denial rate?" tests whether Genie maps it correctly).
  * Test ambiguous phrasing, boundary conditions, and trick wording.
- Respect Scope Boundaries — do NOT generate questions about explicitly out-of-scope topics.
- Difficulty reflects SQL complexity: Easy = single table, simple filter; Medium = aggregation + group by; Hard = multi-table joins or subqueries.
- Include a mix of time-bound (last quarter, YTD) and aggregation styles.
- Prefer the business owner's own phrasing — pull from the Question Bank when populated, and from the Business Context Q&A answers otherwise. Use the BO's actual words.
- Do NOT draft SQL — just the questions. SQL will be drafted per-row later.

Return ONLY the JSON array of {count} final picks, highest-value first. No markdown fences, no preamble, no commentary about the candidate pool."""


def _do_draft_benchmarks(eid, count=12):
    """Core draft-benchmarks logic. Used by both sync endpoint and async task."""
    rows = sql_exec(f"SELECT * FROM {TABLE} WHERE engagement_id = :eid", {"eid": eid})
    if not rows:
        raise ValueError("Engagement not found")
    eng = parse_row(rows[0])

    try:
        count = int(count)
    except (TypeError, ValueError):
        count = 12
    count = max(1, min(50, count))

    prompt = _build_benchmark_draft_prompt(eng, count=count)
    result = _call_llm(prompt, label="benchmark-draft")

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
    return {"benchmarks": drafted}


@register_task("draft_benchmarks")
def _task_draft_benchmarks(payload):
    return _do_draft_benchmarks(
        (payload.get("engagement_id") or "").strip(),
        count=payload.get("count", 12),
    )


@app.route("/api/engagements/<eid>/draft-benchmarks", methods=["POST"])
def draft_benchmarks(eid):
    """Legacy sync endpoint. New callers should use the async job runner."""
    body = request.json or {}
    try:
        return jsonify(_do_draft_benchmarks(eid, count=body.get("count", 12)))
    except ValueError as e:
        return jsonify({"error": str(e)}), 404
    except Exception as e:
        return jsonify({"error": _user_error("draft-benchmarks sync", e)}), 500


def _do_draft_benchmark_sql(eid, question, warehouse_id, validate, user_token):
    """Core draft-benchmark-sql logic. Used by sync endpoint and async task."""
    question = (question or "").strip()
    if not question:
        raise ValueError("question is required")

    rows = sql_exec(f"SELECT * FROM {TABLE} WHERE engagement_id = :eid", {"eid": eid})
    if not rows:
        raise ValueError("Engagement not found")
    eng = parse_row(rows[0])

    s3 = eng["sessions"]["3"]
    s4 = eng["sessions"]["4"]

    user_w = _user_workspace_client_from_token(user_token)
    return _do_draft_benchmark_sql_inner(question, warehouse_id, validate, s3, s4, user_w)


@register_task("draft_benchmark_sql")
def _task_draft_benchmark_sql(payload):
    return _do_draft_benchmark_sql(
        eid=(payload.get("engagement_id") or "").strip(),
        question=payload.get("question", ""),
        warehouse_id=(payload.get("warehouse_id") or "").strip(),
        validate=bool(payload.get("validate")),
        user_token=payload.get("_user_token", ""),
    )


@app.route("/api/engagements/<eid>/draft-benchmark-sql", methods=["POST"])
def draft_benchmark_sql(eid):
    """Legacy sync endpoint. New callers should use the async job runner
    (POST /api/jobs/start with task_type=draft_benchmark_sql)."""
    body = request.json or {}
    try:
        return jsonify(_do_draft_benchmark_sql(
            eid,
            question=body.get("question", ""),
            warehouse_id=(body.get("warehouse_id") or "").strip(),
            validate=bool(body.get("validate")),
            user_token=request.headers.get("X-Forwarded-Access-Token") or "",
        ))
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": _user_error("draft-benchmark-sql sync", e)}), 500


def _do_draft_benchmark_sql_inner(question, warehouse_id, validate, s3, s4, user_w):
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

    result = _call_llm(prompt, label="benchmark-sql")
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

    # Optional validation pass: run the drafted SQL on the chosen warehouse.
    # If it fails, do ONE auto-fix retry with the error message so the LLM
    # can correct hallucinated columns / dialect issues. Mirrors the metric
    # view YAML retry pattern.
    validation = None
    if validate and sql_text and warehouse_id:
        validation = {"ran": False, "error": None, "retried": False, "sample_result": None}
        run_result = _execute_benchmark_sql_obo(user_w, sql_text, warehouse_id)
        if run_result.get("error"):
            err_msg = run_result["error"]
            print(f"[draft-benchmark-sql] validation failed: {err_msg}", flush=True)
            # ONE retry with the error embedded in the prompt
            retry_prompt = f"""Your drafted SQL failed when executed on Databricks. Fix it.

<failed_sql>
{sql_text}
</failed_sql>

<execution_error>
{err_msg}
</execution_error>

<table_schemas>
{schemas_text}
</table_schemas>

<known_metrics>
{metrics_text}
</known_metrics>

<benchmark_question>
{question}
</benchmark_question>

Common causes:
- Column name doesn't exist (check the schema list above carefully)
- Wrong dialect (this is Databricks SQL / Spark SQL — see common gotchas: DATE_ADD takes integer days; use ADD_MONTHS for months; DATEDIFF is (end, start); single quotes for strings)
- Missing fully-qualified table reference

Return JSON: {{"sql": "the corrected SQL"}}. No markdown fences, no commentary."""
            try:
                retry_result = _call_llm(retry_prompt, label="benchmark-sql-fix")
                if isinstance(retry_result, dict):
                    retry_sql = str(retry_result.get("sql", "")).strip()
                else:
                    retry_sql = str(retry_result or "").strip()
                if retry_sql.startswith("```"):
                    retry_sql = retry_sql.split("\n", 1)[1] if "\n" in retry_sql else retry_sql
                    if retry_sql.endswith("```"):
                        retry_sql = retry_sql.rsplit("\n", 1)[0] if "\n" in retry_sql else retry_sql[:-3]
                    retry_sql = retry_sql.strip()
                    if retry_sql.lower().startswith("sql"):
                        retry_sql = retry_sql[3:].lstrip()
                if retry_sql:
                    retry_run = _execute_benchmark_sql_obo(user_w, retry_sql, warehouse_id)
                    validation["retried"] = True
                    if retry_run.get("error"):
                        # Both passes failed — keep the original SQL and surface the latest error
                        validation["ran"] = False
                        validation["error"] = retry_run["error"]
                    else:
                        # Retry succeeded — promote the corrected SQL
                        sql_text = retry_sql
                        validation["ran"] = True
                        validation["error"] = None
                        validation["sample_result"] = retry_run
                else:
                    validation["error"] = err_msg
            except Exception as e:
                print(f"[draft-benchmark-sql] retry failed: {type(e).__name__}: {e}", flush=True)
                validation["error"] = err_msg
        else:
            validation["ran"] = True
            validation["sample_result"] = run_result

    # Second LLM call — summary is derived from the final SQL (post-retry if
    # applicable), not from the question. Guarantees the plain-English
    # explanation describes what the query actually does.
    explanation = ""
    if sql_text:
        try:
            explanation = _summarize_benchmark_sql(question, sql_text)
        except Exception as e:
            print(f"[draft-benchmark-sql] summary generation failed: {type(e).__name__}: {e}", flush=True)

    return {
        "sql": sql_text,
        "explanation": explanation,
        "validation": validation,
    }


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
    result = _call_llm(prompt, label="benchmark-summary")
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
        return jsonify({"error": _user_error("draft-benchmark-summary", e)}), 500

    return jsonify({"explanation": explanation})


_SELECT_OR_WITH_RE = _re.compile(r"^\s*(?:WITH\b|SELECT\b)", _re.IGNORECASE | _re.DOTALL)
_DESTRUCTIVE_KEYWORDS = (
    "DELETE", "UPDATE", "INSERT", "MERGE", "DROP", "TRUNCATE", "ALTER",
    "CREATE", "REPLACE", "GRANT", "REVOKE", "OPTIMIZE", "VACUUM", "ANALYZE",
    "REFRESH", "RESTORE", "USE", "SET", "RESET", "COMMENT", "RENAME",
)
_DESTRUCTIVE_RE = _re.compile(
    r"(?:^|[\s;])(" + "|".join(_DESTRUCTIVE_KEYWORDS) + r")\b",
    _re.IGNORECASE,
)


def _is_safe_select(sql_text):
    """Return (True, None) if the SQL is unambiguously a SELECT (or WITH...SELECT);
    (False, reason) otherwise.

    Defense-in-depth against an LLM hallucinating a destructive statement
    that the user happens to have grants for. The outer SELECT * FROM (...)
    wrapper would syntax-error on most DML/DDL anyway, but this is a
    deliberate guard rather than relying on that accident.

    Strips a leading SQL comment block and a single trailing semicolon
    before matching; rejects multi-statement bodies.
    """
    s = (sql_text or "").strip()
    if not s:
        return False, "empty SQL"
    # Strip leading line comments (--) and block comments (/* */)
    while True:
        if s.startswith("--"):
            nl = s.find("\n")
            s = (s[nl + 1:] if nl >= 0 else "").lstrip()
            continue
        if s.startswith("/*"):
            end = s.find("*/")
            if end < 0:
                return False, "unterminated SQL comment"
            s = s[end + 2:].lstrip()
            continue
        break
    # Reject multi-statement bodies (one trailing semicolon is fine)
    if ";" in s.rstrip().rstrip(";"):
        return False, "multiple SQL statements are not allowed"
    if not _SELECT_OR_WITH_RE.match(s):
        return False, "only SELECT (or WITH ... SELECT) statements are allowed"
    # Belt: scan for destructive keywords appearing as standalone words.
    # False positives possible (e.g., a string literal containing 'DELETE')
    # but for a defense-in-depth guard the trade-off is acceptable.
    if _DESTRUCTIVE_RE.search(s):
        return False, "SQL contains a disallowed keyword (DELETE/UPDATE/DROP/etc.)"
    return True, None


def _execute_benchmark_sql_obo(user_w, sql_text, warehouse_id, limit_cap=50):
    """Run benchmark SQL via OBO; return a dict with columns/rows/row_count or error.

    Always returns a dict (never raises). The caller can check for `error`.
    Used by both `/run-benchmark-sql` (analyst-triggered) and the draft
    validation pass (auto-triggered after SQL drafting).
    """
    if not user_w:
        return {"error": "User auth unavailable — reload the app so OBO token is present"}
    if not sql_text:
        return {"error": "sql is required"}
    if not warehouse_id:
        return {"error": "warehouse_id is required"}

    stmt = sql_text.rstrip().rstrip(";").strip()

    # Hard guard: only SELECT / WITH ... SELECT bodies allowed. Defends against
    # an LLM (or a user-edited benchmark) emitting a destructive statement.
    safe, reason = _is_safe_select(stmt)
    if not safe:
        return {"error": f"SQL rejected by safety guard: {reason}"}

    wrapped = f"SELECT * FROM (\n{stmt}\n) __bm LIMIT {limit_cap}"

    try:
        resp = user_w.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=wrapped,
            wait_timeout="50s",
        )
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}

    statement_id = resp.statement_id
    state = str(resp.status.state) if resp.status else ""
    import time as _time
    deadline = _time.time() + 120
    while statement_id and "SUCCEEDED" not in state and "FAILED" not in state and "CANCELED" not in state and "CLOSED" not in state:
        if _time.time() > deadline:
            try:
                user_w.statement_execution.cancel_execution(statement_id)
            except Exception:
                pass
            return {"error": "Query timed out after 2 minutes waiting for the warehouse"}
        _time.sleep(1.5)
        try:
            resp = user_w.statement_execution.get_statement(statement_id)
        except Exception as e:
            return {"error": f"{type(e).__name__}: {e}"}
        state = str(resp.status.state) if resp.status else ""

    if "SUCCEEDED" not in state:
        err = resp.status.error.message if (resp.status and resp.status.error) else f"Statement state: {state}"
        return {"error": err}

    columns: list[str] = []
    out_rows: list[list] = []
    row_count = 0
    if resp.manifest and resp.manifest.schema and resp.manifest.schema.columns:
        columns = [c.name for c in resp.manifest.schema.columns]
    if resp.result and resp.result.data_array:
        out_rows = [list(r) for r in resp.result.data_array]
        row_count = len(out_rows)
    return {
        "columns": columns,
        "rows": out_rows,
        "row_count": row_count,
        "truncated": row_count >= limit_cap,
        "limit": limit_cap,
    }


@app.route("/api/engagements/<eid>/run-benchmark-sql", methods=["POST"])
def run_benchmark_sql(eid):
    """Execute benchmark SQL via OBO and return a sample of rows for BO review."""
    rows = sql_exec(f"SELECT * FROM {TABLE} WHERE engagement_id = :eid", {"eid": eid})
    if not rows:
        return jsonify({"error": "Not found"}), 404

    body = request.json or {}
    sql_text = (body.get("sql") or "").strip()
    warehouse_id = (body.get("warehouse_id") or "").strip()
    user_w = _user_workspace_client()
    result = _execute_benchmark_sql_obo(user_w, sql_text, warehouse_id)
    return jsonify(result), 200


# ---------------------------------------------------------------------------
# API: Metric View authoring (Session 3)
# ---------------------------------------------------------------------------

def _fetch_metric_view_definition(fqn, user_w=None, warehouse_id=None):
    """Return the live UC YAML definition of a Metric View.

    Uses the UC tables REST endpoint under OBO (so the user's UC grants
    apply) and returns the `view_definition` field — the metric view's
    YAML body.

    Previously used SHOW CREATE TABLE, which silently fails on metric views
    in current DBR with UNSUPPORTED_SHOW_CREATE_TABLE.ON_METRIC_VIEW. That
    failure caused the S5 plan prompt's <metric_view_definitions> block to
    be empty for any engagement that referenced an MV, so the LLM had no
    way to see what the MV already covered and would re-author measures
    that already existed.

    `warehouse_id` is accepted for backwards compatibility with the call
    site but is no longer used — the REST API doesn't need a warehouse.
    """
    parts = fqn.split(".")
    if len(parts) != 3 or not user_w:
        return ""
    url = f"{user_w.config.host.rstrip('/')}/api/2.1/unity-catalog/tables/{fqn}"
    try:
        resp = requests.get(
            url,
            headers={"Authorization": f"Bearer {user_w.config.token}"},
            timeout=30,
        )
        if not resp.ok:
            print(f"[mv-fetch] {fqn}: {resp.status_code} {resp.text[:200]}", flush=True)
            return ""
        body = resp.json()
        # `view_definition` holds the YAML body for metric views. For a
        # regular table or non-MV view, it'll be absent or empty -- that's
        # fine, the caller treats "" as "no MV definition available."
        return body.get("view_definition") or ""
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
                lines.append(f"- **{r.get('report_name') or r.get('name','')}**: {_er_what(r)}")
            else:
                lines.append(f"- {r}")
        lines.append("")

    lines.append("## Business Questions (S2)")
    for q in s2.get("question_bank", []):
        text = _qb_question(q)
        if text:
            lines.append(f"- {text}")
    lines.append("")
    lines.append("## Vocabulary & Metric Definitions (S2)")
    for v in s2.get("vocabulary_metrics", []):
        if not isinstance(v, dict):
            continue
        lines.append(f"- **{v.get('business_term','')}**: {_vm_definition(v)}")
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
1. Classify each analyst SQL expression by its SHAPE — analysts sometimes paste a
   bare WHERE-clause predicate where a value expression is expected, so look at
   what the SQL actually is, not just what it's labeled:
   - Aggregate function (COUNT/SUM/AVG/MAX/MIN/...) in the expression → `measures`
   - A bare boolean PREDICATE / WHERE-clause fragment (e.g. `status = 'CANCELLED'`,
     `claim_type IN ('Professional','Facility')`, `receipt_date >= '2024-01-01'`) is
     NOT a measure or dimension value. Handle it as ONE of:
       (a) if it scopes the whole space → fold it into the top-level `filter:` (AND
           it with any existing filter), OR
       (b) if the analyst clearly wants to COUNT/measure the matching rows → wrap it
           as a measure: `COUNT(1) FILTER (WHERE <predicate>)`.
     Pick (a) when the metric_name reads like a scope/exclusion, (b) when it reads
     like a count/rate of the matching subset. NEVER emit a bare predicate as a
     dimension or measure `expr` — that produces a boolean column nobody asked for.
   - No aggregate and not a predicate (plain column, CASE WHEN, DATE_TRUNC, etc.) → `dimensions`
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


# Aggregate functions that make an expression a valid `measure`. A measure expr
# MUST contain at least one of these; a dimension expr must contain NONE.
_AGG_FUNCS = {
    "count", "sum", "avg", "mean", "min", "max", "median", "stddev", "stddev_pop",
    "stddev_samp", "variance", "var_pop", "var_samp", "approx_count_distinct",
    "approx_percentile", "percentile", "percentile_approx", "collect_list",
    "collect_set", "first", "first_value", "last", "last_value", "any", "every",
    "some", "bool_and", "bool_or", "corr", "covar_pop", "covar_samp", "skewness",
    "kurtosis", "count_if", "max_by", "min_by", "grouping", "grouping_id",
    "regr_count", "regr_avgx", "regr_avgy", "regr_slope", "regr_intercept",
    "regr_r2", "regr_sxx", "regr_sxy", "regr_syy",
}

# Top-level keys the metric view YAML v1.1 spec allows. Anything else breaks the
# CREATE ... WITH METRICS write.
_MV_ALLOWED_TOP_KEYS = {
    "version", "comment", "source", "filter", "joins", "dimensions",
    "measures", "materialization",
}


def _expr_has_aggregate(expr):
    """True if a SQL expression contains a call to a known aggregate function."""
    import re
    if not expr:
        return False
    # Strip string literals so a function-looking token inside a string doesn't count
    cleaned = re.sub(r"'[^']*'", " ", str(expr))
    for fn in _AGG_FUNCS:
        if re.search(rf"\b{fn}\s*\(", cleaned, re.IGNORECASE):
            return True
    return False


def _expr_looks_like_predicate(expr):
    """True if an expr looks like a bare WHERE-clause predicate rather than a
    value expression — i.e. a top-level comparison with no surrounding aggregate.
    Used to catch the common analyst mistake of pasting a filter (`status = 'X'`)
    where a measure/dimension value is expected (change request #4)."""
    import re
    if not expr:
        return False
    s = str(expr).strip()
    if _expr_has_aggregate(s):
        return False
    # Remove string literals so operators inside strings don't trigger
    cleaned = re.sub(r"'[^']*'", " ", s)
    # Bare comparison / membership operators at the surface level
    if re.search(r"(<=|>=|!=|<>|\s=\s|\s<\s|\s>\s)", cleaned):
        return True
    if re.search(r"\b(IN|LIKE|ILIKE|RLIKE|BETWEEN|IS\s+NULL|IS\s+NOT\s+NULL)\b",
                 cleaned, re.IGNORECASE):
        return True
    return False


def _lint_mv_yaml(yaml_text):
    """Deterministic structural lint of a metric view YAML body.

    Returns (hard_errors, warnings). hard_errors are issues that WILL break the
    `CREATE OR REPLACE VIEW ... WITH METRICS` write (so we feed them to the LLM
    auto-fix retry and block the push). warnings are likely-wrong-but-creatable
    issues (e.g. a predicate where a value expression was expected — #4).

    This catches the failure classes the column-existence check misses:
    name-collisions (CONTAINS_AGGREGATE), measures without an aggregate,
    dimensions WITH an aggregate, disallowed top-level keys, duplicate names,
    and bare Y/N literals YAML parses as booleans.
    """
    hard, warn = [], []
    try:
        import yaml as pyyaml
    except Exception:
        return hard, warn
    try:
        doc = pyyaml.safe_load(yaml_text)
    except Exception as e:
        return [f"YAML does not parse: {e}"], warn
    if not isinstance(doc, dict):
        return ["YAML root is not a mapping (expected top-level keys like version/source/measures)."], warn

    # Top-level key whitelist
    for k in doc.keys():
        if str(k) not in _MV_ALLOWED_TOP_KEYS:
            hard.append(
                f"Invalid top-level key `{k}`. Allowed keys: "
                f"{', '.join(sorted(_MV_ALLOWED_TOP_KEYS))}. Rules that aren't a "
                f"filter/dimension/measure belong on the Genie Space, not the metric view."
            )
    if not doc.get("source"):
        hard.append("Missing required `source` (the base table as catalog.schema.table).")

    seen_names = {}

    def _check_member(member, kind):
        if not isinstance(member, dict):
            hard.append(f"Each {kind} entry must be a mapping with `name` and `expr`.")
            return
        name = (member.get("name") or "").strip()
        expr = member.get("expr") or ""
        if not name:
            hard.append(f"A {kind} is missing its `name`.")
        else:
            if " " in name:
                hard.append(f"{kind} name `{name}` contains a space — use snake_case.")
            if name.lower() in seen_names:
                hard.append(
                    f"Duplicate name `{name}` (also used as a {seen_names[name.lower()]}). "
                    f"Names must be unique across dimensions and measures."
                )
            seen_names[name.lower()] = kind
        if not expr:
            hard.append(f"{kind} `{name or '?'}` is missing its `expr`.")
            return
        # Name-collision: a measure/dimension name that also appears as a bare
        # identifier inside its own expr triggers CONTAINS_AGGREGATE / resolution errors.
        if name and name.lower() in _extract_bare_identifiers(expr):
            hard.append(
                f"{kind} `{name}` references a column named `{name}` in its own expr. "
                f"Rename the {kind} (e.g. add a _pct/_count/_total suffix) so it doesn't "
                f"collide with the column."
            )
        has_agg = _expr_has_aggregate(expr)
        if kind == "measure" and not has_agg:
            hard.append(
                f"measure `{name}` has no aggregate function in its expr (`{expr}`). "
                f"Measures must aggregate (COUNT/SUM/AVG/...). If this is a filter, move it "
                f"to the top-level `filter:` key; if it's a row-level value, make it a dimension."
            )
        if kind == "dimension" and has_agg:
            hard.append(
                f"dimension `{name}` contains an aggregate function in its expr (`{expr}`). "
                f"Dimensions must be row-level (non-aggregate). Move this to `measures`."
            )
        if kind == "dimension" and _expr_looks_like_predicate(expr):
            warn.append(
                f"dimension `{name}` looks like a filter predicate (`{expr}`), not a value. "
                f"If it's meant to scope the data, move it to the top-level `filter:` key."
            )

    for m in (doc.get("measures") or []):
        _check_member(m, "measure")
    for d in (doc.get("dimensions") or []):
        _check_member(d, "dimension")

    # Bare Y/N booleans inside exprs/filter (YAML 1.1 parses them as booleans)
    import re
    def _bare_yn(expr):
        cleaned = re.sub(r"'[^']*'", " ", str(expr or ""))
        return bool(re.search(r"(=|<>|!=|\bIN\b)\s*[YN]\b", cleaned))
    if _bare_yn(doc.get("filter")):
        warn.append("filter references a bare Y/N — quote string literals ('Y'/'N').")

    return hard, warn


def _dryrun_create_mv(user_w, yaml_body, warehouse_id, catalog, schema):
    """Live validation: create a throwaway metric view from the YAML, then DROP it.

    This is the only way to GUARANTEE the YAML will write to UC — it uses the
    exact same `CREATE OR REPLACE VIEW ... WITH METRICS` engine path. Returns a
    dict: {ok: bool, error: str|None, skipped: str|None}. `skipped` is set (and
    ok=True) when we couldn't run the dry-run for a non-syntax reason — most
    importantly a CREATE permission denial in the scratch schema — so callers
    can fall back to lint without treating it as a validation failure.
    """
    if not (user_w and warehouse_id and catalog and schema and yaml_body):
        return {"ok": True, "error": None, "skipped": "missing warehouse/target"}
    scratch = f"_gdv_validate_{_gen_hex_id()}"
    fqn = f"`{catalog}`.`{schema}`.`{scratch}`"
    delim = "$$" if "$$" not in yaml_body else "$MV_DRYRUN$"
    create_stmt = (
        f"CREATE OR REPLACE VIEW {fqn}\nWITH METRICS\nLANGUAGE YAML\nAS {delim}\n{yaml_body}\n{delim}"
    )
    try:
        _sql_exec_obo(user_w, create_stmt, warehouse_id)
    except Exception as e:
        msg = str(e)
        low = msg.lower()
        # A permission/authorization failure means we can't validate HERE, not
        # that the YAML is wrong. Skip gracefully so lint stays authoritative.
        if any(k in low for k in ("permission", "privilege", "not authorized",
                                  "access denied", "requires", "insufficient")):
            return {"ok": True, "error": None, "skipped": f"no create rights in {catalog}.{schema}"}
        return {"ok": False, "error": msg, "skipped": None}
    finally:
        # Always try to clean up the scratch object.
        try:
            _sql_exec_obo(user_w, f"DROP VIEW IF EXISTS {fqn}", warehouse_id)
        except Exception as drop_err:
            print(f"[mv-dryrun] cleanup of {fqn} failed: {drop_err}", flush=True)
    return {"ok": True, "error": None, "skipped": None}


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
        return jsonify({"error": _user_error("mv-prompt-preview", e)}), 500
    return jsonify({"prompt": prompt})


@app.route("/api/engagements/<eid>/draft-metric-view-yaml", methods=["POST"])
def draft_metric_view_yaml(eid):
    """Legacy sync endpoint. New callers should use the async job runner
    (POST /api/jobs/start with task_type=draft_mv_yaml)."""
    body = request.json or {}
    try:
        return jsonify(_do_draft_mv_yaml(
            eid,
            user_token=request.headers.get("X-Forwarded-Access-Token") or "",
            warehouse_id=(body.get("warehouse_id") or "").strip(),
        ))
    except ValueError as e:
        return jsonify({"error": str(e)}), 404
    except Exception as e:
        return jsonify({"error": _user_error("draft-mv-yaml sync", e)}), 500


def _do_draft_mv_yaml(eid, user_token, warehouse_id):
    """Core MV YAML draft logic. Used by both sync endpoint and async task."""
    rows = sql_exec(f"SELECT * FROM {TABLE} WHERE engagement_id = :eid", {"eid": eid})
    if not rows:
        raise ValueError("Engagement not found")
    eng = parse_row(rows[0])
    user_w = _user_workspace_client_from_token(user_token)
    return _do_draft_mv_yaml_inner(eng, user_w, warehouse_id)


@register_task("draft_mv_yaml")
def _task_draft_mv_yaml(payload):
    return _do_draft_mv_yaml(
        eid=(payload.get("engagement_id") or "").strip(),
        user_token=payload.get("_user_token", ""),
        warehouse_id=(payload.get("warehouse_id") or "").strip(),
    )


def _mv_issue_list(yaml_text, schemas):
    """Combine the column-existence check and the structural lint into one list
    of hard, must-fix problems with a metric view YAML. Returns (issues, warnings)."""
    missing = _validate_yaml_columns(yaml_text, schemas)
    hard, warn = _lint_mv_yaml(yaml_text)
    issues = list(hard)
    if missing:
        issues.append(
            f"References column(s) that don't exist in the source table(s): "
            f"{', '.join(missing)}. Use only real columns or drop that field."
        )
    return issues, warn


def _do_draft_mv_yaml_inner(eng, user_w, warehouse_id):
    prompt = _build_mv_yaml_prompt(eng, user_w, warehouse_id)
    result = _call_llm(prompt, label="mv-yaml")

    yaml_text = _strip_yaml_fences(str(result.get("yaml", "")))
    source_table = str(result.get("source_table", "")).strip()
    suggested_name = str(result.get("suggested_name", "")).strip()

    schemas = _collect_engagement_schemas(eng, user_w, warehouse_id)
    warnings = []

    schema_lines = []
    for fqn, cols in schemas.items():
        if cols:
            schema_lines.append(f"- `{fqn}`: {', '.join(c for c, _ in cols)}")
    schema_block = "\n".join(schema_lines) or "(schema unavailable)"

    def _fix_once(bad_yaml, problems, label):
        """Ask the LLM to repair the YAML given a concrete problem list. Returns
        the repaired (yaml, source_table, suggested_name) or None on failure."""
        fix_prompt = f"""You drafted this Databricks Unity Catalog metric view YAML, but it has problems that will prevent it from being created.

<your_yaml>
{bad_yaml}
</your_yaml>

<problems_to_fix>
{chr(10).join(f'- {p}' for p in problems)}
</problems_to_fix>

<authoritative_schema>
{schema_block}
</authoritative_schema>

Rewrite the YAML so EVERY problem above is resolved. Rules to honor:
- Measures MUST contain an aggregate (COUNT/SUM/AVG/...). Dimensions MUST be row-level (no aggregate). A bare WHERE-style predicate belongs in the top-level `filter:` key, never as a measure/dimension expr.
- No measure/dimension `name` may equal a column referenced in its own expr (add a _pct/_count/_total suffix).
- Names are unique across all dimensions and measures; snake_case, no spaces.
- Only allowed top-level keys: version, comment, source, filter, joins, dimensions, measures, materialization.
- Every bare column reference must be a real column from the authoritative schema. Drop a field rather than invent a column.
- Quote string literals ('Y'/'N'/'DENIED').

Return JSON with exactly: {{"yaml": "...", "source_table": "...", "suggested_name": "..."}}. No markdown fences."""
        try:
            r2 = _call_llm(fix_prompt, label=label)
            return (
                _strip_yaml_fences(str(r2.get("yaml", ""))),
                str(r2.get("source_table", "")).strip(),
                str(r2.get("suggested_name", "")).strip(),
            )
        except Exception as e:
            print(f"[draft-mv-yaml] {label} failed: {e}", flush=True)
            return None

    # Pass 1: deterministic checks (column existence + structural lint).
    issues, lint_warn = _mv_issue_list(yaml_text, schemas)
    if issues:
        print(f"[draft-mv-yaml] structural/column issues: {issues}", flush=True)
        fixed = _fix_once(yaml_text, issues, "mv-yaml-fix")
        if fixed and fixed[0]:
            new_issues, new_warn = _mv_issue_list(fixed[0], schemas)
            if len(new_issues) < len(issues):
                yaml_text = fixed[0]
                source_table = fixed[1] or source_table
                suggested_name = fixed[2] or suggested_name
                issues, lint_warn = new_issues, new_warn
        if issues:
            warnings.append(
                "Structural issues remain — review before creating: " + "; ".join(issues)
            )
    warnings.extend(lint_warn)

    # Pass 2: LIVE validation — actually create the metric view (scratch name)
    # and drop it, so we KNOW it writes. Uses the source table's catalog.schema
    # as the scratch location; skips gracefully if the user lacks CREATE there.
    src_for_scratch = source_table or ((schemas and next(iter(schemas))) or "")
    parts = src_for_scratch.split(".") if src_for_scratch else []
    if not issues and len(parts) == 3 and warehouse_id:
        dry = _dryrun_create_mv(user_w, yaml_text, warehouse_id, parts[0], parts[1])
        if dry.get("skipped"):
            print(f"[draft-mv-yaml] dry-run skipped: {dry['skipped']}", flush=True)
        elif not dry.get("ok"):
            spark_err = dry.get("error") or "unknown error"
            print(f"[draft-mv-yaml] dry-run create FAILED: {spark_err}", flush=True)
            fixed = _fix_once(
                yaml_text,
                [f"When Databricks tried to create this metric view it failed with: {spark_err}"],
                "mv-yaml-dryrun-fix",
            )
            if fixed and fixed[0]:
                # Only accept the retry if it ALSO passes deterministic checks.
                retry_issues, _ = _mv_issue_list(fixed[0], schemas)
                if not retry_issues:
                    dry2 = _dryrun_create_mv(user_w, fixed[0], warehouse_id, parts[0], parts[1])
                    if dry2.get("ok"):
                        yaml_text = fixed[0]
                        source_table = fixed[1] or source_table
                        suggested_name = fixed[2] or suggested_name
                    else:
                        warnings.append(
                            f"The metric view still fails to create: {dry2.get('error') or spark_err}. "
                            f"Review/edit the YAML before creating."
                        )
                else:
                    warnings.append(
                        f"The metric view failed a live create check ({spark_err}) and the auto-fix "
                        f"still has issues. Review the YAML before creating."
                    )
            else:
                warnings.append(
                    f"The metric view failed a live create check: {spark_err}. "
                    f"Review/edit the YAML before creating."
                )

    return {
        "yaml": yaml_text,
        "source_table": source_table,
        "suggested_name": suggested_name,
        "warnings": warnings,
    }


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

    # Re-validate the YAML the user is about to push. The draft step already
    # validates + retries, but the user may have hand-edited the YAML since.
    # Block the push if we can prove it's broken — much better UX than letting
    # Spark error at CREATE time. Two layers: (1) deterministic column + lint
    # checks, (2) a live scratch create+drop in the TARGET schema, which is the
    # definitive "will this write to UC" test.
    rows = sql_exec(f"SELECT * FROM {TABLE} WHERE engagement_id = :eid", {"eid": eid})
    if rows:
        eng_for_schema = parse_row(rows[0])
        schemas = _collect_engagement_schemas(eng_for_schema, user_w, warehouse_id)
        issues, _warn = _mv_issue_list(yaml_body, schemas)
        if issues:
            return jsonify({
                "error": (
                    "The metric view YAML has problems that would fail at create: "
                    + "; ".join(issues) + ". Fix the YAML or re-draft, then try again."
                ),
                "issues": issues,
            }), 400

    # Live dry-run in the real target schema. If it can't create here, surface
    # the exact Spark error instead of a half-applied write. (Skips only on a
    # CREATE-permission denial, which the real create below will surface anyway.)
    dry = _dryrun_create_mv(user_w, yaml_body, warehouse_id, catalog_name, schema_name)
    if not dry.get("ok"):
        return jsonify({
            "error": f"The metric view failed to create: {dry.get('error')}. Fix the YAML and retry.",
            "dryrun_error": dry.get("error"),
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
    new_updated_at = None
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
        new_updated_at = now_ts()
        sql_run(
            f"UPDATE {TABLE} SET data_plan = :dp, metric_view_fqn = :fqn, updated_at = :ts "
            f"WHERE engagement_id = :eid",
            {"eid": eid, "dp": json.dumps(data_plan), "fqn": created_fqn, "ts": new_updated_at},
        )

    # Return updated_at so the frontend can refresh its optimistic-lock token.
    # Without this the next autosave 409s, same pattern as push-to-Genie.
    resp = {"success": True, "fqn": created_fqn}
    if new_updated_at is not None:
        resp["updated_at"] = new_updated_at
    return jsonify(resp)


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

    # ----- Phase 2: Column-level synonyms from Session 3 classifications -----
    # Build column_configs entries for any term classified as Synonym with a
    # column- or value-level target. Verified ColumnConfig schema fields
    # (proto: databricks.datarooms.export.ColumnConfig):
    #   column_name, description (string[]), display_name, synonyms (string[]),
    #   enable_format_assistance (bool), enable_entity_matching (bool)
    # Cross-cutting synonyms are NOT pushed here -- they flow to
    # general_instructions via the S5 LLM plan.
    #
    # NOTE: this overwrites any column_configs the user has set in the Genie
    # UI for the columns we touch (the push is destructive by design;
    # serialized_space PATCHes replace-not-merge). Columns we don't touch are
    # not affected because tables are rebuilt from the data plan each push.
    s3 = eng["sessions"]["3"]
    s2 = eng["sessions"]["2"]
    s2_vocab_by_term = {
        (v.get("business_term") or "").strip(): v
        for v in (s2.get("vocabulary_metrics") or [])
        if isinstance(v, dict) and (v.get("business_term") or "").strip()
    }
    # FQN of in-scope tables (the ones we just built) -- column_configs are only
    # honored when the parent table is in data_sources.tables[]. If an analyst
    # mapped a synonym to a column in a table that isn't in the data plan, we
    # silently skip it (the S5 plan's narrative TODO will still surface it for
    # manual review).
    table_fqns_in_scope = {t["identifier"] for t in tables}

    # column_configs accumulator: {table_fqn: {column_name: {field: value}}}
    cc_by_table = {}
    for c in (s3.get("term_classifications") or []):
        if not isinstance(c, dict):
            continue
        term = (c.get("business_term") or "").strip()
        types = c.get("types") or []
        if not term or "Synonym" not in types:
            continue
        target = c.get("synonym_target") or {}
        kind = (target.get("kind") or "cross_cutting").strip().lower()
        if kind not in ("column", "value"):
            continue  # cross_cutting handled via general_instructions
        fqn = (target.get("column_fqn") or "").strip()
        parts = fqn.split(".")
        if len(parts) != 4:
            continue  # malformed FQN (need catalog.schema.table.column)
        table_fqn = ".".join(parts[:3])
        column_name = parts[3]
        if table_fqn not in table_fqns_in_scope:
            continue  # column's table isn't in the data plan; skip

        vocab = s2_vocab_by_term.get(term)
        raw_syns = (vocab.get("synonyms") if vocab else "") or ""
        syns_list = [s.strip() for s in raw_syns.split(",") if s.strip()]
        if not syns_list:
            continue

        cc_by_table.setdefault(table_fqn, {})
        cc = cc_by_table[table_fqn].setdefault(column_name, {"column_name": column_name})

        if kind == "column":
            # Merge into the column's synonym list (deduped, preserving order)
            existing = cc.get("synonyms") or []
            for s in syns_list + [term]:
                if s and s != column_name and s not in existing:
                    existing.append(s)
            if existing:
                cc["synonyms"] = existing
        elif kind == "value":
            # Value-level: append a description line mapping value -> aliases,
            # and enable entity_matching so Genie can match user phrasings to
            # values automatically. We don't push value synonyms structurally
            # because Genie's column_configs schema has no value-aliases field
            # -- description text + entity_matching toggle is the supported
            # mechanism.
            col_value = (target.get("column_value") or "").strip()
            if not col_value:
                continue
            # The canonical term IS an alias for the value (just like the S2
            # synonyms are). Include it first, dedupe.
            all_aliases = []
            for s in [term] + syns_list:
                s = (s or "").strip()
                if s and s not in all_aliases:
                    all_aliases.append(s)
            # Backslash-escape any literal " in the alias so the rendered
            # description doesn't break out of its surrounding quotes.
            joined = ", ".join(f'"{s.replace(chr(34), chr(92) + chr(34))}"' for s in all_aliases)
            # Same for col_value — analysts can put quotes inside category names.
            col_value_safe = col_value.replace("'", "\\'")
            line = f"Value '{col_value_safe}' is also referred to as: {joined}."
            desc = cc.get("description") or []
            if line not in desc:
                desc.append(line)
            cc["description"] = desc
            cc["enable_entity_matching"] = True

    # Attach the accumulated column_configs to the matching table entries
    if cc_by_table:
        for entry in tables:
            cc_map = cc_by_table.get(entry["identifier"])
            if not cc_map:
                continue
            entry["column_configs"] = sorted(
                cc_map.values(), key=lambda c: c["column_name"]
            )

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

    # Benchmarks — top-level {"questions": [...]}. Each question has an
    # answer: [{format: "SQL", content: [...]}] (array, single element).
    # `content` is an array of strings — Genie joins them as the SQL.
    bm_entries = []
    for b in benchmarks_in:
        q = (b.get("question") or "").strip()
        sql = (b.get("expected_sql") or "").strip()
        if not q or not sql:
            continue
        bm_entries.append({
            "id": _gen_hex_id(),
            "question": [q],
            "answer": [{"format": "SQL", "content": [sql]}],
        })
    bm_entries.sort(key=lambda x: x["id"])
    if bm_entries:
        serialized["benchmarks"] = {"questions": bm_entries}

    return serialized


def _genie_api_call(user_w, method, path, body=None):
    """Make an authenticated Genie REST API call using the user's token (OBO)."""
    url = f"{user_w.config.host.rstrip('/')}{path}"
    headers = {
        "Authorization": f"Bearer {user_w.config.token}",
        "Content-Type": "application/json",
    }
    # DEBUG: log a sample of the body so we can see what's being sent on failure.
    try:
        body_for_log = json.dumps(body) if body else "<none>"
        sample = body_for_log[:300] + (" ..." if len(body_for_log) > 300 else "")
        print(f"[genie-api] {method} {path} body_chars={len(body_for_log)} sample={sample}", flush=True)
    except Exception:
        pass
    resp = requests.request(method, url, headers=headers, json=body, timeout=60)
    if not resp.ok:
        # DEBUG: log full request body when failing so we can diagnose
        print(f"[genie-api] FAILED {method} {path} status={resp.status_code}", flush=True)
        print(f"[genie-api] response: {resp.text[:1000]}", flush=True)
        if body:
            print(f"[genie-api] full body: {json.dumps(body)[:5000]}", flush=True)
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

    # Optimistic-lock check BEFORE we read the engagement: if the client's
    # If-Match doesn't match the row's updated_at, refuse the push. Without
    # this, two analysts working the same engagement can race -- one pushes
    # stale serialized_space while the other is mid-edit, silently clobbering
    # the live Genie space with out-of-date data.
    try:
        _check_optimistic_lock(eid)
    except StaleEngagementError as e:
        return jsonify({
            "error": "stale",
            "current_updated_at": e.current_updated_at,
            "message": "This engagement was updated by another user. Refresh before pushing.",
        }), 409

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
                "serialized_space": json.dumps(serialized, ensure_ascii=False),
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
                "serialized_space": json.dumps(serialized, ensure_ascii=False),
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
    # Return new updated_at so the client can refresh its optimistic-lock token
    # and avoid 409s on the next autosave.
    result["updated_at"] = ts

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
        # Surface the failure instead of swallowing it -- a silent empty list
        # leaves the user staring at an empty picker with no idea why. Auth
        # errors translate to a reauth_required code the frontend already
        # knows how to render; everything else gets a generic error message.
        msg = f"{type(e).__name__}: {e}"
        print(f"[/api/uc/catalogs] {msg}", flush=True)
        status = 401 if "401" in msg or "PERMISSION_DENIED" in msg or "unauthorized" in msg.lower() else 502
        code = "reauth_required" if status == 401 else "catalogs_list_failed"
        return jsonify({"error": code, "message": msg}), status
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
    """Detect existing metric views in a catalog.schema.

    Uses the UC tables REST list endpoint (not the SDK's tables.list)
    because databricks-sdk 0.44.0 returns METRIC_VIEW as TableType.MANAGED
    in TableInfo -- the enum value was added in a later SDK version. REST
    response carries table_type as a string, so the filter works.
    """
    user_w, err = _require_obo()
    if err:
        return err
    catalog_schema = request.args.get("catalog_schema", "")
    if not catalog_schema or catalog_schema.count(".") != 1:
        return jsonify([])
    cat, sch = catalog_schema.split(".", 1)
    try:
        r = requests.get(
            f"{user_w.config.host.rstrip('/')}/api/2.1/unity-catalog/tables",
            params={"catalog_name": cat, "schema_name": sch, "max_results": 200},
            headers={"Authorization": f"Bearer {user_w.config.token}"},
            timeout=15,
        )
        if not r.ok:
            print(f"[/api/uc/metric-views] {r.status_code} {r.text[:200]}", flush=True)
            return jsonify([])
    except Exception as e:
        print(f"[/api/uc/metric-views] {type(e).__name__}: {e}", flush=True)
        return jsonify([])
    results = [
        f"{cat}.{sch}.{t['name']}"
        for t in (r.json().get("tables") or [])
        if t.get("table_type") == "METRIC_VIEW" and t.get("name")
    ]
    return jsonify(results)


@app.route("/api/uc/table-type")
def uc_table_type():
    """Return UC's authoritative table_type for a single FQN.

    Used by S3's Data Sources panel: when the analyst picks a name via
    UCTablePicker, we don't know if it's a managed table, a view, or a
    metric view -- the picker dropdown lists all of them by name. This
    lookup tells us how to categorize on Add.
    """
    user_w, err = _require_obo()
    if err:
        return err
    fqn = request.args.get("fqn", "")
    if not fqn or fqn.count(".") != 2:
        return jsonify({"error": "fqn (3-part) is required"}), 400
    try:
        r = requests.get(
            f"{user_w.config.host.rstrip('/')}/api/2.1/unity-catalog/tables/{fqn}",
            headers={"Authorization": f"Bearer {user_w.config.token}"},
            timeout=15,
        )
    except Exception as e:
        return jsonify({"error": f"{type(e).__name__}: {e}"}), 502
    if not r.ok:
        return jsonify({"error": f"{r.status_code}: {r.text[:200]}"}), r.status_code
    body = r.json()
    return jsonify({
        "fqn": fqn,
        "table_type": body.get("table_type") or "MANAGED",
        "comment": body.get("comment") or "",
    })


def _list_mvs_in_schema(host, hdrs, cat, sch):
    """List MV candidate FQNs in a single (catalog, schema) via REST.

    Returns (fqns, error_message) -- error is None on success. The caller
    surfaces errors in the response so the UI can distinguish "0 MVs in
    scope" from "couldn't read this schema."
    """
    try:
        r = requests.get(
            f"{host}/api/2.1/unity-catalog/tables",
            params={"catalog_name": cat, "schema_name": sch, "max_results": 200},
            headers=hdrs, timeout=15,
        )
    except Exception as e:
        return [], f"{type(e).__name__}: {e}"
    if not r.ok:
        return [], f"HTTP {r.status_code}: {r.text[:200]}"
    fqns = [
        f"{cat}.{sch}.{t['name']}"
        for t in (r.json().get("tables") or [])
        if t.get("table_type") == "METRIC_VIEW" and t.get("name")
    ]
    return fqns, None


def _fetch_mv_dependencies(host, hdrs, fqn):
    """Return ({fqn metadata + dependencies set}, error_message). The
    caller filters by the intersect with picked tables. Per-MV REST calls
    are parallelized in the discovery endpoint via ThreadPoolExecutor."""
    try:
        r = requests.get(
            f"{host}/api/2.1/unity-catalog/tables/{fqn}",
            headers=hdrs, timeout=15,
        )
    except Exception as e:
        return None, f"{type(e).__name__}: {e}"
    if not r.ok:
        return None, f"HTTP {r.status_code}"
    body = r.json()
    deps_raw = (body.get("view_dependencies") or {}).get("dependencies") or []
    dep_fqns = set()
    for d in deps_raw:
        tbl = (d or {}).get("table") or {}
        fn = tbl.get("table_full_name")
        if fn:
            dep_fqns.add(fn)
    return {
        "body": body,
        "dep_fqns": dep_fqns,
    }, None


def _broad_mv_scan(user_w, warehouse_id):
    """Use system.information_schema.tables to find every metric view
    visible to the user, regardless of catalog/schema. Catches MVs in
    personal catalogs that depend on shared source tables -- a common
    DSA pattern that the per-schema scan misses.

    Returns (fqns, error). On any failure (permission denied, warehouse
    cold, system table not enabled) returns ([], err) and the caller falls
    back to per-schema scan.
    """
    if not warehouse_id:
        return [], "no warehouse_id provided"
    try:
        resp = user_w.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement="""
                SELECT table_catalog || '.' || table_schema || '.' || table_name AS fqn
                FROM system.information_schema.tables
                WHERE table_type = 'METRIC_VIEW'
            """,
            wait_timeout="30s",
        )
    except Exception as e:
        return [], f"{type(e).__name__}: {e}"
    state = ""
    if resp.status and resp.status.state:
        state = str(resp.status.state.value if hasattr(resp.status.state, "value")
                    else resp.status.state)
    if state != "SUCCEEDED" or not resp.result:
        msg = ""
        if resp.status and resp.status.error:
            msg = (resp.status.error.message or "")[:200]
        return [], f"system.information_schema query failed ({state}): {msg}"
    rows = resp.result.data_array or []
    return [r[0] for r in rows if r and r[0]], None


@app.route("/api/uc/metric-views-for-tables")
def uc_metric_views_for_tables():
    """Find Metric Views that depend on any of the given source tables.

    Used by S3's data-sources-first flow: the analyst picks tables, the app
    surfaces existing MVs that already use those tables so the analyst can
    reuse them instead of re-authoring measures from scratch.

    Query params:
        fqns: comma-separated catalog.schema.table list (required)
        warehouse_id: optional. If provided, the discovery also queries
            system.information_schema.tables for a broader scan that catches
            MVs in catalogs/schemas different from the picked tables (common
            DSA pattern where MVs live in personal catalogs but reference
            shared source tables). If absent or the broad scan fails, falls
            back to per-(catalog,schema) scan only.

    Response shape:
        {
            "metric_views": [{fqn, catalog, schema, name, comment, owner,
                              updated_at, dependencies}],
            "errors": ["<schema>: <message>", ...],  -- non-empty when one or
                more candidate enumeration calls failed; lets the UI
                distinguish "0 matches" from "couldn't search."
            "warnings": ["..."]  -- non-fatal notes (e.g. broad scan unavailable,
                falling back to schema-only).
            "scope": {
                "broad": bool  -- true iff system.information_schema was used
            }
        }
    """
    user_w, err = _require_obo()
    if err:
        return err
    raw = request.args.get("fqns", "")
    warehouse_id = (request.args.get("warehouse_id") or "").strip()
    picked = {
        t.strip() for t in raw.split(",")
        if t.strip() and t.count(".") == 2
    }
    if not picked:
        return jsonify({"metric_views": [], "errors": [], "warnings": [], "scope": {"broad": False}})

    host = user_w.config.host.rstrip("/")
    hdrs = {"Authorization": f"Bearer {user_w.config.token}"}
    errors = []
    warnings = []
    candidates = set()
    broad_used = False

    # Step 1a: try the broad scan first if a warehouse is available. One SQL
    # query finds every MV visible to the user. If it fails (system table
    # disabled, no warehouse access, etc.) we silently fall back to the
    # narrower per-schema scan -- but the UI gets a warning so the user
    # knows discovery was scoped.
    if warehouse_id:
        broad_fqns, broad_err = _broad_mv_scan(user_w, warehouse_id)
        if broad_err:
            warnings.append(
                f"Broad MV scan via system.information_schema failed: {broad_err}. "
                "Falling back to schema-scoped scan (MVs in different catalogs/"
                "schemas from your picked tables won't be found)."
            )
        else:
            candidates.update(broad_fqns)
            broad_used = True

    # Step 1b: schema-scoped scan as fallback / supplement. Parallelize
    # across the (catalog, schema) pairs of picked tables -- usually 1-3
    # schemas, but parallelism shaves the round-trip cost regardless.
    schemas = {".".join(t.split(".")[:2]) for t in picked}
    if not broad_used:
        with ThreadPoolExecutor(max_workers=8) as pool:
            futures = {
                pool.submit(_list_mvs_in_schema, host, hdrs, *cs.split(".", 1)): cs
                for cs in schemas
            }
            for fut in futures:
                cs = futures[fut]
                fqns, scan_err = fut.result()
                if scan_err:
                    errors.append(f"{cs}: {scan_err}")
                else:
                    candidates.update(fqns)

    if not candidates:
        return jsonify({
            "metric_views": [],
            "errors": errors,
            "warnings": warnings,
            "scope": {"broad": broad_used},
        })

    # Step 2: REST-fetch view_dependencies for every candidate in parallel.
    # The LIST response above returns view_dependencies: null, so we need
    # per-MV GETs to read the dependency list. Parallelism here is the
    # biggest win -- a schema with 30 MVs goes from sequential ~6s to a
    # bounded ~1s. Failed fetches are logged-and-skipped (e.g. private MVs
    # the user lacks grants on).
    results = []
    with ThreadPoolExecutor(max_workers=16) as pool:
        futures = {
            pool.submit(_fetch_mv_dependencies, host, hdrs, fqn): fqn
            for fqn in sorted(candidates)
        }
        for fut in futures:
            fqn = futures[fut]
            data, dep_err = fut.result()
            if dep_err:
                print(f"[/api/uc/metric-views-for-tables] {fqn}: {dep_err}", flush=True)
                continue
            if not (data["dep_fqns"] & picked):
                continue
            body = data["body"]
            parts = fqn.split(".")
            results.append({
                "fqn": fqn,
                "catalog": parts[0],
                "schema": parts[1],
                "name": parts[2],
                "comment": body.get("comment", "") or "",
                "owner": body.get("owner", "") or "",
                "updated_at": body.get("updated_at"),
                "dependencies": sorted(data["dep_fqns"]),
            })

    results.sort(key=lambda x: x["fqn"])
    return jsonify({
        "metric_views": results,
        "errors": errors,
        "warnings": warnings,
        "scope": {"broad": broad_used},
    })


@app.route("/api/uc/metric-view-details")
def uc_metric_view_details():
    """Return structured dimensions + measures for a Metric View.

    Used by S3's data-sources panel to render "what does this MV cover?" --
    column names, display names, synonyms, comments, and measure markers --
    without needing an LLM. The deterministic part of Phase 1.

    Query params:
        fqn:          catalog.schema.metric_view_name (required)
        warehouse_id: SQL warehouse to run DESCRIBE on (required)

    Returns:
        { fqn, dimensions: [...], measures: [...] }
      where each entry is:
        { name, display_name, synonyms: [...], comment, data_type }

    Measure detection: DESCRIBE EXTENDED on a metric view returns each
    column's data_type with a trailing " measure" marker (e.g.
    "bigint measure"). We split on that marker.
    """
    user_w, err = _require_obo()
    if err:
        return err
    fqn = request.args.get("fqn", "")
    warehouse_id = request.args.get("warehouse_id", "")
    if not fqn or fqn.count(".") != 2:
        return jsonify({"error": "fqn (3-part) is required"}), 400
    if not warehouse_id:
        return jsonify({"error": "warehouse_id is required"}), 400

    parts = fqn.split(".")
    stmt = f"DESCRIBE EXTENDED `{parts[0]}`.`{parts[1]}`.`{parts[2]}`"
    try:
        resp = user_w.statement_execution.execute_statement(
            warehouse_id=warehouse_id, statement=stmt, wait_timeout="30s",
        )
    except Exception as e:
        print(f"[/api/uc/metric-view-details] {type(e).__name__}: {e}", flush=True)
        return jsonify({"error": f"{type(e).__name__}: {e}"}), 502

    state = ""
    if resp.status and resp.status.state:
        state = str(resp.status.state.value if hasattr(resp.status.state, "value")
                    else resp.status.state)
    if state != "SUCCEEDED" or not resp.result or not resp.result.data_array:
        msg = ""
        if resp.status and resp.status.error:
            msg = (resp.status.error.message or "")[:300]
        return jsonify({"error": f"DESCRIBE failed ({state}): {msg}"}), 502

    dimensions, measures = [], []
    for row in resp.result.data_array:
        if not row:
            continue
        col_name = (row[0] or "").strip() if row[0] else ""
        # The "# Detailed Table Information" section comes after the
        # columns. Stop on the first blank or comment-banner row -- the
        # column metadata we care about all lives above it.
        if not col_name or col_name.startswith("#"):
            break
        data_type = (row[1] or "").strip() if len(row) > 1 and row[1] else ""
        comment = (row[2] or "").strip() if len(row) > 2 and row[2] else ""
        metadata_raw = (row[3] or "").strip() if len(row) > 3 and row[3] else ""
        meta = {}
        if metadata_raw:
            try:
                meta = json.loads(metadata_raw)
            except Exception:
                meta = {}
        entry = {
            "name": col_name,
            "data_type": data_type,
            "comment": comment,
            "display_name": (meta.get("display_name") or "").strip(),
            "synonyms": meta.get("synonyms") or [],
        }
        if "measure" in data_type.lower():
            measures.append(entry)
        else:
            dimensions.append(entry)
    return jsonify({"fqn": fqn, "dimensions": dimensions, "measures": measures})


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
