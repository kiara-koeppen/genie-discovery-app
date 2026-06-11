#!/usr/bin/env python3
"""Seed a deliberately COMPLEX engagement for stress-testing the AI features.

Multi-table data plan (fact + 2 dims) to force joins, metrics in varied/messy
real-world forms (CASE, nested-FILTER ratios, NULLIF, date math), a bare
WHERE-clause predicate (#4), a metric that references a JOINED dimension column
(forces the MV to join), a global filter, plus many questions / vocab /
benchmarks. Prints the new engagement id.
"""
import argparse, json, subprocess, sys, urllib.request, urllib.error

CAT = "genie_training.genie_discovery"
CLAIMS = f"{CAT}.cx_claims_fact"
MEMBERS = f"{CAT}.cx_members_dim"
PROVIDERS = f"{CAT}.cx_providers_dim"


def token(profile):
    return json.loads(subprocess.check_output(
        ["databricks", "auth", "token", "--profile", profile], text=True))["access_token"]


def req(method, url, tok, body=None):
    data = json.dumps(body).encode() if body is not None else None
    r = urllib.request.Request(url, data=data, method=method)
    r.add_header("Authorization", f"Bearer {tok}")
    if data:
        r.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(r, timeout=60) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read().decode())
        except Exception:
            return e.code, {"error": f"HTTP {e.code}"}


QUESTIONS = [
    "What is our overall claim denial rate this quarter?",
    "Which provider specialties have the highest denial rates?",
    "What is the average claim turnaround time in days, by claim type?",
    "How many denied claims were appealed, and what share were overturned?",
    "What is the paid-to-billed ratio by line of business?",
    "Which regions have the most high-dollar claims (billed over $10,000)?",
    "How does denial rate differ between in-network and out-of-network providers?",
    "What are the top denial reason codes by volume?",
    "What is total paid amount by member plan type?",
    "How many claims are still pending adjudication?",
    "What is the denial rate for Medicare vs Commercial members?",
    "Which age bands have the highest average billed amount?",
    "What is the appeal overturn rate by specialty?",
    "How many facility claims did we process last month?",
    "What share of claims come from out-of-network providers by region?",
]

VOCAB = [
    ("Denial Rate", "Percent of claims denied on initial adjudication: denied claims / total claims.", "denial %, rejection rate"),
    ("Turnaround Time", "Calendar days from claim receipt to first adjudication decision.", "TAT, cycle time, processing time"),
    ("Appeal Overturn Rate", "Of appealed denied claims, the share whose decision was reversed.", "overturn rate, appeal win rate"),
    ("Paid-to-Billed Ratio", "Total paid amount divided by total billed amount.", "payment ratio, reimbursement rate"),
    ("High-Dollar Claim", "A claim with billed amount over $10,000.", "large claim, high-cost claim"),
    ("Line of Business", "Commercial, Medicare, or Medicaid.", "LOB"),
    ("In-Network", "Provider with network_status = 'IN'.", "INN, par provider"),
    ("Out-of-Network", "Provider with network_status = 'OUT'.", "OON, non-par"),
    ("Voided Claim", "A claim with voided_flag = 'Y'; excluded from all reporting.", "void"),
    ("Test Claim", "A claim with test_flag = 'Y'; excluded from all reporting.", "test row"),
    ("Place of Service", "Code for where the service occurred (11 office, 21 inpatient, etc.).", "POS"),
    ("Claim Type", "Professional, Facility, or Pharmacy.", "bill type"),
    ("Adjudication", "The decision process that approves, denies, or pends a claim.", "adjudicate"),
    ("Pended Claim", "A claim with initial_decision = 'PENDING' awaiting more info.", "pending, suspended"),
    ("Specialty", "Provider's clinical specialty.", "provider type"),
]

# metric_name, uc_table, sql_code  (deliberately varied / messy real-world forms)
METRICS = [
    ("total_claims", CLAIMS, "COUNT(1)"),
    ("denied_claims", CLAIMS, "COUNT(CASE WHEN initial_decision = 'DENIED' THEN 1 END)"),
    ("denial_rate_pct", CLAIMS,
     "COUNT(CASE WHEN initial_decision = 'DENIED' THEN 1 END) * 100.0 / NULLIF(COUNT(1), 0)"),
    ("avg_turnaround_days", CLAIMS, "AVG(DATEDIFF(adjudication_date, receipt_date))"),
    ("appeal_overturn_rate_pct", CLAIMS,
     "COUNT(CASE WHEN appeal_decision = 'OVERTURNED' THEN 1 END) * 100.0 / "
     "NULLIF(COUNT(CASE WHEN aa_ind = 'Y' THEN 1 END), 0)"),
    ("total_paid_amount", CLAIMS, "SUM(paid_amount)"),
    ("paid_to_billed_ratio", CLAIMS, "SUM(paid_amount) / NULLIF(SUM(billed_amount), 0)"),
    ("high_dollar_claim_count", CLAIMS, "COUNT(CASE WHEN billed_amount > 10000 THEN 1 END)"),
    # #4: a bare WHERE-clause predicate where a metric value is expected
    ("facility_only", CLAIMS, "claim_type = 'Facility'"),
    # references a JOINED dim column (not in claims) -> forces a join or a drop
    ("out_of_network_claims", CLAIMS, "COUNT(CASE WHEN network_status = 'OUT' THEN 1 END)"),
]

DATA_PLAN = [
    {"table_or_view": CLAIMS, "type": "Table", "include_in_space": "Yes", "notes": "Claims fact (grain: one row per claim)"},
    {"table_or_view": MEMBERS, "type": "Table", "include_in_space": "Yes", "notes": "Member dimension"},
    {"table_or_view": PROVIDERS, "type": "Table", "include_in_space": "Yes", "notes": "Provider dimension"},
]

BENCHMARKS = [
    {"question": "What is the overall denial rate?", "category": "Core", "difficulty": "Easy",
     "expected_sql": f"SELECT COUNT(CASE WHEN initial_decision='DENIED' THEN 1 END)*100.0/COUNT(1) AS denial_rate_pct FROM {CLAIMS} WHERE voided_flag='N' AND test_flag='N'"},
    {"question": "Average turnaround days by claim type", "category": "Core", "difficulty": "Medium",
     "expected_sql": f"SELECT claim_type, AVG(DATEDIFF(adjudication_date, receipt_date)) AS avg_tat FROM {CLAIMS} WHERE voided_flag='N' AND test_flag='N' GROUP BY claim_type"},
    {"question": "Denial rate by provider specialty", "category": "Core", "difficulty": "Hard",
     "expected_sql": f"SELECT p.specialty, COUNT(CASE WHEN c.initial_decision='DENIED' THEN 1 END)*100.0/COUNT(1) AS denial_rate FROM {CLAIMS} c JOIN {PROVIDERS} p ON c.provider_id=p.provider_id WHERE c.voided_flag='N' AND c.test_flag='N' GROUP BY p.specialty ORDER BY denial_rate DESC"},
    {"question": "Paid to billed ratio by line of business", "category": "Core", "difficulty": "Hard",
     "expected_sql": f"SELECT m.line_of_business, SUM(c.paid_amount)/NULLIF(SUM(c.billed_amount),0) AS paid_to_billed FROM {CLAIMS} c JOIN {MEMBERS} m ON c.member_id=m.member_id WHERE c.voided_flag='N' AND c.test_flag='N' GROUP BY m.line_of_business"},
    {"question": "How many high-dollar claims (billed over 10000) by region", "category": "Edge Case", "difficulty": "Medium",
     "expected_sql": f"SELECT m.region, COUNT(1) FROM {CLAIMS} c JOIN {MEMBERS} m ON c.member_id=m.member_id WHERE c.billed_amount>10000 AND c.voided_flag='N' AND c.test_flag='N' GROUP BY m.region"},
    {"question": "Appeal overturn rate overall", "category": "Edge Case", "difficulty": "Hard",
     "expected_sql": f"SELECT COUNT(CASE WHEN appeal_decision='OVERTURNED' THEN 1 END)*100.0/NULLIF(COUNT(CASE WHEN aa_ind='Y' THEN 1 END),0) AS overturn_rate FROM {CLAIMS} WHERE voided_flag='N' AND test_flag='N'"},
]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", required=True)
    ap.add_argument("--app", required=True)
    ap.add_argument("--eid", help="reuse an existing engagement instead of creating")
    a = ap.parse_args()
    tok = token(a.profile)

    if a.eid:
        eid = a.eid
        print("reusing engagement_id:", eid)
    else:
        name = "CX Claims Analytics (complex smoke)"
        st, r = req("POST", f"{a.app}/api/engagements", tok,
                    {"genie_space_name": name, "business_owner_name": "Test BO",
                     "business_owner_email": "test.bo@example.com", "analyst_name": "Kiara"})
        if st not in (200, 201) or not r.get("engagement_id"):
            print("create failed:", st, r); sys.exit(1)
        eid = r["engagement_id"]
        print("engagement_id:", eid)

    def put(n, body):
        s, rr = req("PUT", f"{a.app}/api/engagements/{eid}/sessions/{n}", tok, body)
        print(f"  saved S{n}: HTTP {s}{'' if s==200 else ' '+json.dumps(rr)[:200]}")

    put(2, {
        "question_bank": [{"question_text": q, "decision_it_drives": ""} for q in QUESTIONS],
        "vocabulary_metrics": [{"business_term": t, "what_they_mean": d, "synonyms": s} for t, d, s in VOCAB],
    })
    put(3, {
        "sql_expressions": [{"metric_name": m, "uc_table": t, "sql_code": c, "synonyms": ""} for m, t, c in METRICS],
        "global_filter": "voided_flag = 'N' AND test_flag = 'N'",
        "term_classifications": [], "text_instructions": [], "example_queries": [],
        "clarifying_questions": [], "data_gaps": [], "scope_boundaries": [],
    })
    put(4, {
        "data_plan": DATA_PLAN,
        "benchmark_questions": [dict(b, bo_approved=True) for b in BENCHMARKS],
        "analyst_commentary": {}, "coe_approval_status": "approved",
        "coe_approval_notes": "seeded", "coe_reviewer_email": "kiara@example.com",
    })
    print("DONE. eid:", eid)


if __name__ == "__main__":
    main()
