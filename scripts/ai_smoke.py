#!/usr/bin/env python3
"""End-to-end AI smoke test for the Genie Discovery App.

Exercises EVERY LLM-backed feature against a live deployment using the async
job runner, and asserts each one returns a usable result (not a JSON-parse
error, not a truncation). Run this before any customer redeploy so we can prove
the AI features actually work instead of guessing.

Usage:
  python scripts/ai_smoke.py --profile kk_test \
      --app https://genie-discovery-669602668219382.2.azure.databricksapps.com \
      --eid 655be6a7-3a68-44ef-8a07-fef0226bfa1e \
      --warehouse ad1dd0025031919f

Token is fetched via `databricks auth token --profile <profile>`.
Exit code 0 = all green; non-zero = at least one AI feature failed.
"""
import argparse
import json
import subprocess
import sys
import time
import urllib.request
import urllib.error


def get_token(profile):
    out = subprocess.check_output(
        ["databricks", "auth", "token", "--profile", profile], text=True
    )
    return json.loads(out)["access_token"]


def _req(method, url, token, body=None, timeout=120):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Authorization", f"Bearer {token}")
    if data:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read().decode())
        except Exception:
            return e.code, {"error": f"HTTP {e.code}"}


def run_job(app, token, task_type, eid, warehouse, poll_timeout=240):
    """Start an async job and poll to completion. Returns (state, result, error)."""
    status, resp = _req(
        "POST", f"{app}/api/jobs/start", token,
        {"task_type": task_type,
         "payload": {"engagement_id": eid, "warehouse_id": warehouse}},
    )
    if status != 200 or "job_id" not in resp:
        return "start_failed", None, f"start HTTP {status}: {resp.get('error', resp)}"
    job_id = resp["job_id"]
    deadline = time.time() + poll_timeout
    while time.time() < deadline:
        time.sleep(3)
        s, jr = _req("GET", f"{app}/api/jobs/{job_id}", token)
        state = jr.get("state", "")
        # Backend terminal states (see _run_job): success="done", failure="failed".
        if state == "done":
            return "succeeded", jr.get("result"), None
        if state == "failed":
            return "failed", jr.get("result"), jr.get("error")
    return "timeout", None, f"did not finish within {poll_timeout}s"


# task_type -> (human label, validator(result) -> (ok, detail))
def _v_plan(r):
    if not isinstance(r, dict):
        return False, f"result not a dict: {type(r)}"
    plan = r.get("plan") if isinstance(r.get("plan"), dict) else r
    need = ["general_instructions", "sample_questions", "example_queries"]
    missing = [k for k in need if k not in plan]
    if missing:
        return False, f"plan missing keys: {missing}; got {list(plan)[:8]}"
    return True, (f"gen_instr={len(str(plan.get('general_instructions','')))}c "
                  f"samples={len(plan.get('sample_questions') or [])} "
                  f"examples={len(plan.get('example_queries') or [])}")


def _v_mv(r):
    if not isinstance(r, dict):
        return False, f"result not a dict: {type(r)}"
    y = (r.get("yaml") or "").strip()
    if not y or "version" not in y:
        return False, f"no usable yaml (len={len(y)})"
    return True, f"yaml={len(y)}c warnings={len(r.get('warnings') or [])}"


def _v_benchmarks(r):
    bms = r.get("benchmarks") if isinstance(r, dict) else r
    if not isinstance(bms, list) or not bms:
        return False, f"no benchmarks list (got {type(bms)})"
    return True, f"{len(bms)} benchmarks"


def _v_brief(r):
    txt = r if isinstance(r, str) else (r.get("summary") or r.get("brief") or "" if isinstance(r, dict) else "")
    if not txt or len(txt) < 50:
        return False, f"brief too short/empty (len={len(txt or '')})"
    return True, f"brief={len(txt)}c"


TASKS = [
    ("draft_mv_yaml", "Metric View YAML", _v_mv),
    ("generate_plan", "Genie Plan", _v_plan),
    ("draft_benchmarks", "Benchmarks", _v_benchmarks),
    ("readiness_brief", "Readiness Brief", _v_brief),
]


def mv_create_check(app, token, eid, warehouse, catalog, schema):
    """Draft the MV YAML AND actually create it in UC (scratch name), then drop
    it. This is the check that matches the real-world failure: 'YAML that errors
    on upload'. Returns (ok, detail)."""
    state, result, error = run_job(app, token, "draft_mv_yaml", eid, warehouse)
    if state != "succeeded":
        return False, f"draft [{state}] {error}"
    yaml_body = (result.get("yaml") or "").strip()
    if not yaml_body:
        return False, "draft produced empty yaml"
    name = f"gdv_smoke_{int(time.time())}"
    status, cr = _req("POST", f"{app}/api/engagements/{eid}/create-metric-view", token,
                      {"catalog": catalog, "schema": schema, "name": name,
                       "yaml": yaml_body, "warehouse_id": warehouse, "overwrite": True})
    if status == 200 and cr.get("success"):
        # best-effort cleanup
        _req("POST", f"{app}/api/engagements/{eid}/run-benchmark-sql", token,
             {"sql": f"DROP VIEW IF EXISTS {cr.get('fqn')}", "warehouse_id": warehouse})
        return True, f"created+dropped {cr.get('fqn')}"
    return False, f"create HTTP {status}: {json.dumps(cr)[:200]}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", required=True)
    ap.add_argument("--app", required=True)
    ap.add_argument("--eid", required=True)
    ap.add_argument("--warehouse", required=True)
    ap.add_argument("--create-catalog", help="if set with --create-schema, also "
                    "verify the drafted MV actually CREATEs in UC here")
    ap.add_argument("--create-schema")
    args = ap.parse_args()

    token = get_token(args.profile)
    print(f"App: {args.app}\nEngagement: {args.eid}\nWarehouse: {args.warehouse}\n")
    all_ok = True
    for task_type, label, validator in TASKS:
        t0 = time.time()
        state, result, error = run_job(args.app, token, task_type, args.eid, args.warehouse)
        dt = time.time() - t0
        if state != "succeeded":
            all_ok = False
            print(f"FAIL  {label:18s} [{state}] {dt:5.1f}s  {error}")
            continue
        ok, detail = validator(result)
        all_ok = all_ok and ok
        print(f"{'PASS ' if ok else 'FAIL '} {label:18s} [{state}] {dt:5.1f}s  {detail}")

    # The check that actually matches 'YAML errors on upload': create it for real.
    if args.create_catalog and args.create_schema:
        t0 = time.time()
        ok, detail = mv_create_check(args.app, token, args.eid, args.warehouse,
                                     args.create_catalog, args.create_schema)
        all_ok = all_ok and ok
        print(f"{'PASS ' if ok else 'FAIL '} {'MV creates in UC':18s} [create] {time.time()-t0:5.1f}s  {detail}")

    print("\n" + ("ALL AI FEATURES GREEN" if all_ok else "SOME AI FEATURES FAILED"))
    sys.exit(0 if all_ok else 1)


if __name__ == "__main__":
    main()
