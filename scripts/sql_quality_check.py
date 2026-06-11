#!/usr/bin/env python3
"""Run every plan example_query and every AI-drafted benchmark SQL against the
real data, and report whether each EXECUTES and returns rows. Catches the other
half of quality: generated SQL that's syntactically/semantically wrong."""
import argparse, json, subprocess, sys, time, urllib.request, urllib.error


def tok(p):
    return json.loads(subprocess.check_output(
        ["databricks", "auth", "token", "--profile", p], text=True))["access_token"]


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


def runsql(p, wh, stmt):
    o = subprocess.run(["databricks", "api", "post", "/api/2.0/sql/statements",
        "--profile", p, "--json", json.dumps({"warehouse_id": wh, "statement": stmt,
        "wait_timeout": "50s"})], capture_output=True, text=True)
    d = json.loads(o.stdout); sid = d.get("statement_id"); st = d.get("status", {}).get("state")
    while st in ("PENDING", "RUNNING") and sid:
        time.sleep(2)
        o = subprocess.run(["databricks", "api", "get", f"/api/2.0/sql/statements/{sid}",
            "--profile", p], capture_output=True, text=True)
        d = json.loads(o.stdout); st = d.get("status", {}).get("state")
    if st != "SUCCEEDED":
        return None, d.get("status", {}).get("error", {}).get("message", st)
    return (d.get("result", {}).get("data_array") or []), None


def main():
    ap = argparse.ArgumentParser()
    for f in ("profile", "app", "eid", "warehouse"):
        ap.add_argument(f"--{f}", required=True)
    a = ap.parse_args()
    t = tok(a.profile)

    # plan example_queries (persisted)
    _, eng = http("GET", f"{a.app}/api/engagements/{a.eid}", t)
    eqs = eng["sessions"]["5"].get("plan_example_queries", [])
    print(f"=== PLAN EXAMPLE QUERIES ({len(eqs)}) ===")
    ok_eq = 0
    for e in eqs:
        rows, err = runsql(a.profile, a.warehouse, e.get("sql", ""))
        if err: print(f"  FAIL  {(e.get('question') or '')[:60]}\n        {err[:90]}")
        else:
            ok_eq += 1
            print(f"  PASS  {(e.get('question') or '')[:60]}  -> {len(rows)} row(s), e.g. {rows[0] if rows else '[]'}")
    print(f"  example_queries: {ok_eq}/{len(eqs)} run clean\n")

    # AI-drafted benchmarks
    st, r = http("POST", f"{a.app}/api/jobs/start", t,
                 {"task_type": "draft_benchmarks",
                  "payload": {"engagement_id": a.eid, "warehouse_id": a.warehouse}})
    job = r.get("job_id"); bms = None
    for _ in range(40):
        time.sleep(4); t = tok(a.profile)
        _, jr = http("GET", f"{a.app}/api/jobs/{job}", t)
        if jr.get("state") == "done":
            res = jr.get("result"); bms = res.get("benchmarks") if isinstance(res, dict) else res; break
        if jr.get("state") == "failed": print("benchmark draft failed:", jr.get("error")); break
    bms = bms or []
    print(f"=== AI-DRAFTED BENCHMARKS ({len(bms)}) ===")
    ok_bm = 0
    for b in bms:
        sqltext = b.get("expected_sql", "")
        if not sqltext:
            print(f"  WARN  no SQL: {(b.get('question') or '')[:60]}"); continue
        rows, err = runsql(a.profile, a.warehouse, sqltext)
        if err: print(f"  FAIL  {(b.get('question') or '')[:55]}\n        {err[:90]}")
        else:
            ok_bm += 1
            print(f"  PASS  {(b.get('question') or '')[:55]}  -> {len(rows)} row(s), e.g. {rows[0] if rows else '[]'}")
    print(f"  benchmarks: {ok_bm}/{len(bms)} run clean")


if __name__ == "__main__":
    main()
