#!/usr/bin/env python3
"""Prove the drafted Metric View YAML actually CREATES in Unity Catalog.

This is the test that matches the user's real complaint ("it generates YAML
that errors on upload"). It drafts the MV YAML via the app, then calls the
real create-metric-view route into a scratch name and reports success/failure.
"""
import argparse, json, subprocess, sys, time, urllib.request, urllib.error


def token(profile):
    return json.loads(subprocess.check_output(
        ["databricks", "auth", "token", "--profile", profile], text=True))["access_token"]


def req(method, url, tok, body=None, timeout=120):
    data = json.dumps(body).encode() if body is not None else None
    r = urllib.request.Request(url, data=data, method=method)
    r.add_header("Authorization", f"Bearer {tok}")
    if data:
        r.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(r, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read().decode())
        except Exception:
            return e.code, {"error": f"HTTP {e.code}"}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", required=True)
    ap.add_argument("--app", required=True)
    ap.add_argument("--eid", required=True)
    ap.add_argument("--warehouse", required=True)
    ap.add_argument("--catalog", required=True, help="scratch target catalog")
    ap.add_argument("--schema", required=True, help="scratch target schema (user must have CREATE)")
    a = ap.parse_args()
    tok = token(a.profile)

    # 1) Draft the YAML
    _, r = req("POST", f"{a.app}/api/jobs/start", tok,
               {"task_type": "draft_mv_yaml",
                "payload": {"engagement_id": a.eid, "warehouse_id": a.warehouse}})
    job = r.get("job_id")
    if not job:
        print("FAIL draft start:", r); sys.exit(1)
    result = None
    for _ in range(60):
        time.sleep(4)
        _, jr = req("GET", f"{a.app}/api/jobs/{job}", tok)
        if jr.get("state") == "done":
            result = jr.get("result"); break
        if jr.get("state") == "failed":
            print("FAIL draft job:", jr.get("error")); sys.exit(1)
    if not result:
        print("FAIL draft: timeout"); sys.exit(1)
    yaml_body = (result.get("yaml") or "").strip()
    print(f"Drafted YAML: {len(yaml_body)} chars; warnings={result.get('warnings')}")
    print("----- YAML -----"); print(yaml_body); print("----------------")

    # 2) Actually create it in UC (scratch name), overwrite=true
    name = f"gdv_createtest_{int(time.time())}"
    status, cr = req("POST", f"{a.app}/api/engagements/{a.eid}/create-metric-view", tok,
                     {"catalog": a.catalog, "schema": a.schema, "name": name,
                      "yaml": yaml_body, "warehouse_id": a.warehouse, "overwrite": True})
    if status == 200 and cr.get("success"):
        print(f"PASS  metric view CREATED: {cr.get('fqn')}")
        # best-effort cleanup
        print("(leaving scratch view; drop manually if needed:", cr.get("fqn"), ")")
        sys.exit(0)
    print(f"FAIL  create HTTP {status}: {json.dumps(cr)[:500]}")
    sys.exit(1)


if __name__ == "__main__":
    main()
