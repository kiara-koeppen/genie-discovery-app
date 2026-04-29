# Upgrade Runbook — readiness-brief → main

This runbook covers upgrading an existing deployment from the original `main`
to the current build (Readiness Brief, async transport, optimistic locking,
soft-delete, token telemetry, Push-to-Genie hardening).

The upgrade is **mostly transparent** — `ensure_table()` adds new columns and
auxiliary tables on first startup. The two manual steps below are required.

---

## 1. Service principal grants on auxiliary tables (REQUIRED)

The build adds two new Delta tables to the same `<CATALOG>.<SCHEMA>` location:

- `discovery_jobs` — async job state (used by every "long" LLM button so they
  don't 504 against the gateway timeout).
- `discovery_llm_usage` — per-call token / cost telemetry.

`ensure_table()` creates them on first startup, but the app's service
principal needs `MODIFY, SELECT` on each one to actually write rows. Without
these grants, jobs run but their state is never persisted — meaning a worker
restart strands every in-flight job.

Run **once** as a UC admin after the first deploy of the new build:

```sql
GRANT MODIFY, SELECT ON TABLE <catalog>.<schema>.discovery_jobs
  TO `<service-principal-application-id>`;
GRANT MODIFY, SELECT ON TABLE <catalog>.<schema>.discovery_llm_usage
  TO `<service-principal-application-id>`;
```

Verify by clicking any "Generate Plan" / "Draft Benchmarks" / "Generate Brief"
button and watching the app logs — you should see `[job <id> running]` and
the row should appear in `discovery_jobs` with `state = 'done'`.

## 2. Optional: dedicated brief model

The Readiness Brief uses a separate, faster model so it returns inside ~30s
instead of 5+ minutes. Default is the HIPAA-eligible Haiku 4.5 endpoint.

To override, add to `app.yaml`:

```yaml
env:
  - name: BRIEF_LLM_ENDPOINT_NAME
    value: "databricks-claude-haiku-4-5"   # default if unset
```

The SP needs `CAN QUERY` on whatever endpoint you point at.

## 3. OAuth scopes — no change

Same scope list as before (see README "Step 2b"). No new scopes were added.

---

## What the build does on first startup

`ensure_table()` is idempotent and safe to re-run. On the first request after
upgrade it will:

1. Add `deleted_at STRING` to the engagement table (soft-delete).
2. Add `servicenow_ticket_url STRING` to the engagement table.
3. Add `brief_unacknowledged_gaps STRING` to the session column set for S4.
4. Migrate `analyst_commentary` rows from Python `repr(dict)` strings to
   proper JSON (one-time best-effort, transparent to users).
5. Create `discovery_jobs` and `discovery_llm_usage` if absent.

Existing engagements are unaffected — all new columns are nullable.

## Rollback

If you need to revert to the previous build:

1. Redeploy the previous artifact (`databricks apps deploy --source-code-path
   <previous-snapshot-path>`). Engagements created with the new build will
   still load — the new columns are simply ignored by older code.
2. The `discovery_jobs` and `discovery_llm_usage` tables can be left in place
   (they're only written to by the new build). If you want to remove them:
   `DROP TABLE <catalog>.<schema>.discovery_jobs; DROP TABLE
   <catalog>.<schema>.discovery_llm_usage;`
3. Soft-deleted engagements (rows with `deleted_at` set) will reappear in the
   list under the old build, since old code doesn't filter on `deleted_at`.
   If undesirable, hard-delete them: `DELETE FROM <table> WHERE deleted_at IS
   NOT NULL AND deleted_at != '';`

## Known issues / notes

- **Push-to-Genie payload shape**: The Genie API tightened serialized_space
  validation in early 2026. This build uses the current shape (benchmarks at
  the top level, `answer: [{format: "SQL", content: [sql]}]`). If Databricks
  changes the schema again, push errors will surface as `INVALID_PARAMETER_VALUE`
  with a specific field-mismatch message.
- **Push-to-Genie autosave**: After a successful push, the backend bumps
  `updated_at` and returns it in the response so the client can refresh its
  optimistic-lock token. Do not remove the `updated_at` from the push response
  shape — it prevents a 409 on the next autosave.
- **Home page intermittent empty list**: React StrictMode double-mount race;
  reload fixes. Cosmetic only.
