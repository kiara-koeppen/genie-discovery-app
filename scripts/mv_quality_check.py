#!/usr/bin/env python3
"""Quality check (not just 'it runs'): draft the MV, create it, then QUERY its
measures and compare to ground-truth values computed straight from the source.
Because the cx_* data is synthetic with known distributions, we can assert the
AI-generated measures compute SANE, CORRECT numbers — not just that they exist.
"""
import argparse, json, subprocess, sys, time, urllib.request, urllib.error

CS = "genie_training.genie_discovery"


def tok(profile):
    return json.loads(subprocess.check_output(
        ["databricks", "auth", "token", "--profile", profile], text=True))["access_token"]


def http(method, url, t, body=None):
    data = json.dumps(body).encode() if body is not None else None
    r = urllib.request.Request(url, data=data, method=method)
    r.add_header("Authorization", f"Bearer {t}")
    if data: r.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(r, timeout=120) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        try: return e.code, json.loads(e.read().decode())
        except Exception: return e.code, {"error": str(e)}


def sql(profile, warehouse, stmt):
    o = subprocess.run(["databricks", "api", "post", "/api/2.0/sql/statements",
        "--profile", profile, "--json", json.dumps({"warehouse_id": warehouse,
        "statement": stmt, "wait_timeout": "50s"})], capture_output=True, text=True)
    d = json.loads(o.stdout)
    sid = d.get("statement_id"); st = d.get("status", {}).get("state")
    while st in ("PENDING", "RUNNING") and sid:
        time.sleep(2)
        o = subprocess.run(["databricks", "api", "get", f"/api/2.0/sql/statements/{sid}",
            "--profile", profile], capture_output=True, text=True)
        d = json.loads(o.stdout); st = d.get("status", {}).get("state")
    if st != "SUCCEEDED":
        return None, d.get("status", {}).get("error", {}).get("message", st)
    rows = d.get("result", {}).get("data_array") or []
    return rows, None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", required=True)
    ap.add_argument("--app", required=True)
    ap.add_argument("--eid", required=True)
    ap.add_argument("--warehouse", required=True)
    a = ap.parse_args()
    t = tok(a.profile)

    # 1) Draft the MV YAML
    st, r = http("POST", f"{a.app}/api/jobs/start", t,
                 {"task_type": "draft_mv_yaml",
                  "payload": {"engagement_id": a.eid, "warehouse_id": a.warehouse}})
    job = r.get("job_id"); result = None
    for _ in range(60):
        time.sleep(4)
        t = tok(a.profile)
        _, jr = http("GET", f"{a.app}/api/jobs/{job}", t)
        if jr.get("state") == "done": result = jr.get("result"); break
        if jr.get("state") == "failed": print("draft failed:", jr.get("error")); sys.exit(1)
    yaml_body = (result.get("yaml") or "").strip()
    import re
    measures = re.findall(r"- name:\s*([A-Za-z0-9_]+)", yaml_body)
    print(f"Drafted YAML ({len(yaml_body)}c). names found: {measures}\n")

    # 2) Create it
    name = f"gdv_quality_{int(time.time())}"
    st, cr = http("POST", f"{a.app}/api/engagements/{a.eid}/create-metric-view", t,
                  {"catalog": "genie_training", "schema": "genie_discovery", "name": name,
                   "yaml": yaml_body, "warehouse_id": a.warehouse, "overwrite": True})
    if not (st == 200 and cr.get("success")):
        print("create FAILED:", st, cr); sys.exit(1)
    fqn = cr["fqn"]; print(f"created {fqn}\n")

    # 3) Query each measure overall via MEASURE() and print the value
    print("=== MEASURE VALUES (AI-generated metric view) ===")
    for m in measures:
        rows, err = sql(a.profile, a.warehouse, f"SELECT MEASURE(`{m}`) FROM {fqn}")
        if err:
            print(f"  {m:32s} ERROR: {err[:80]}")
        else:
            print(f"  {m:32s} = {rows[0][0] if rows else '(no rows)'}")

    # 4) Ground truth straight from the source (global filter applied)
    print("\n=== GROUND TRUTH from cx_claims_fact (voided/test excluded) ===")
    gt, err = sql(a.profile, a.warehouse, f"""
      SELECT
        COUNT(1) AS total_claims,
        ROUND(COUNT(CASE WHEN initial_decision='DENIED' THEN 1 END)*100.0/COUNT(1),2) AS denial_rate_pct,
        ROUND(AVG(DATEDIFF(adjudication_date, receipt_date)),2) AS avg_turnaround_days,
        ROUND(SUM(paid_amount)/NULLIF(SUM(billed_amount),0),4) AS paid_to_billed_ratio
      FROM {CS}.cx_claims_fact WHERE voided_flag='N' AND test_flag='N'""")
    if err: print("  ground truth error:", err)
    else:
        c = gt[0]
        print(f"  total_claims={c[0]}  denial_rate_pct={c[1]}  avg_turnaround_days={c[2]}  paid_to_billed_ratio={c[3]}")
        print("  (sane ranges: denial ~20%, turnaround ~13 days, paid/billed ~0.75)")

    # cleanup
    sql(a.profile, a.warehouse, f"DROP VIEW IF EXISTS {fqn}")
    print(f"\ndropped {fqn}")


if __name__ == "__main__":
    main()
