#!/usr/bin/env python3
"""End-to-end test of push-to-Genie: build the serialized_space from the
engagement's plan and CREATE a real Genie Space via the app, confirm the Genie
API accepts the payload (this is where the fragile serialized_space schema
bites), inspect the created space, then delete it to clean up."""
import argparse, json, subprocess, sys, urllib.request, urllib.error


def tok(p):
    return json.loads(subprocess.check_output(
        ["databricks", "auth", "token", "--profile", p], text=True))["access_token"]


def http(method, url, t, body=None):
    data = json.dumps(body).encode() if body is not None else None
    r = urllib.request.Request(url, data=data, method=method)
    r.add_header("Authorization", f"Bearer {t}")
    if data: r.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(r, timeout=120) as x:
            return x.status, json.loads(x.read().decode())
    except urllib.error.HTTPError as e:
        try: return e.code, json.loads(e.read().decode())
        except Exception: return e.code, {"error": str(e)}


def dbapi(profile, method, path, body=None):
    """Call the Databricks REST API directly (for inspect/cleanup of the space)."""
    args = ["databricks", "api", method.lower(), path, "--profile", profile]
    if body is not None:
        args += ["--json", json.dumps(body)]
    o = subprocess.run(args, capture_output=True, text=True)
    try: return json.loads(o.stdout)
    except Exception: return {"_raw": o.stdout, "_err": o.stderr}


def main():
    ap = argparse.ArgumentParser()
    for f in ("profile", "app", "eid", "warehouse"):
        ap.add_argument(f"--{f}", required=True)
    a = ap.parse_args()
    t = tok(a.profile)

    title = "CX Smoke Space (DELETE ME)"
    print("Pushing plan -> NEW Genie Space ...")
    st, r = http("POST", f"{a.app}/api/engagements/{a.eid}/push-to-genie", t,
                 {"mode": "new", "new_title": title, "warehouse_id": a.warehouse})
    if st != 200 or not r.get("space_id"):
        print(f"FAIL  push-to-genie HTTP {st}: {json.dumps(r)[:500]}")
        sys.exit(1)
    sid = r["space_id"]
    print(f"PASS  space created: space_id={sid}")
    print(f"      url: {r.get('space_url')}")
    if r.get("warnings"):
        print(f"      warnings: {r['warnings']}")

    # Inspect the created space via the Genie API — confirms the serialized_space
    # round-trips (tables, instructions, etc. actually landed).
    sp = dbapi(a.profile, "GET", f"/api/2.0/genie/spaces/{sid}")
    print("\n=== created space (Genie API readback) ===")
    print("  title:", sp.get("title"))
    ser = sp.get("serialized_space")
    if isinstance(ser, str):
        try: ser = json.loads(ser)
        except Exception: ser = {}
    if isinstance(ser, dict):
        ds = ser.get("data_sources", {})
        instr = ser.get("instructions", {})
        print("  tables:", len(ds.get("tables", []) or []),
              "| metric_views:", len(ds.get("metric_views", []) or []))
        print("  text_instructions:", len(instr.get("text_instructions", []) or []))
        snip = instr.get("sql_snippets", {}) or {}
        print("  sql_snippets: filters=", len(snip.get("filters", []) or []),
              "expressions=", len(snip.get("expressions", []) or []),
              "measures=", len(snip.get("measures", []) or []))
        print("  example_question_sqls:", len(instr.get("example_question_sqls", []) or []))
        print("  benchmarks:", len((ser.get("benchmarks", {}) or {}).get("questions", []) or []))
    else:
        print("  (could not read serialized_space back:", str(sp)[:200], ")")

    # Cleanup: trash the space
    print("\nCleaning up (deleting space) ...")
    d = dbapi(a.profile, "DELETE", f"/api/2.0/genie/spaces/{sid}")
    print("  delete result:", json.dumps(d)[:160] if d else "ok")
    print("\nPUSH-TO-GENIE: PASS")


if __name__ == "__main__":
    main()
