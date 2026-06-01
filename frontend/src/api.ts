const BASE = "/api";

export interface SqlSnippet {
  name: string;
  sql: string;
  table: string;
  display_name?: string;
  synonyms?: string[] | string;
  description?: string;
}

export interface ExampleQuery {
  question: string;
  sql: string;
  draft?: boolean;
  usage_guidance?: string;
}

export interface UcJoin {
  left_table: string;
  left_columns: string[];
  right_table: string;
  right_columns: string[];
  relationship_type: string;
  source: string;
}

export interface BenchmarkSampleResult {
  ran_at: string;
  columns: string[];
  rows: unknown[][];
  row_count: number;
  truncated: boolean;
  limit: number;
  error?: string;
}

export interface BenchmarkQuestion {
  question: string;
  category: "Core" | "Edge Case";
  difficulty: "Easy" | "Medium" | "Hard";
  expected_sql: string;
  notes?: string;
  bo_approved?: boolean;
  sample_result?: BenchmarkSampleResult;
}

export interface BriefGap {
  id: string;
  title: string;
  severity: "Low" | "Medium" | "High";
  summary: string;
  citations: string[];
}

export interface AnalystCommentary {
  gap_responses?: Record<string, string>;
  resolved_gaps?: Record<string, { title: string; severity: string; response: string }>;
  /**
   * Free-form text from a pre-structured-commentary engagement, preserved
   * verbatim so it doesn't disappear at upgrade time. Read-only.
   */
  legacy_notes?: string;
}

async function json<T>(url: string, opts?: RequestInit): Promise<T> {
  const res = await fetch(`${BASE}${url}`, {
    headers: { "Content-Type": "application/json" },
    ...opts,
  });
  if (!res.ok) {
    const body = await res.json().catch(() => ({} as any));
    // Prefer body.message (human-readable) over body.error (machine code).
    // Without this, a 409 surfaces as Error("stale") which the recovery code
    // can't pattern-match to trigger the refresh-and-retry flow.
    const message = body.message || body.error || `${res.status} ${res.statusText}`;
    const err: any = new Error(message);
    err.status = res.status;
    if (res.status === 409) {
      err.stale = true;
      err.current_updated_at = body.current_updated_at;
    }
    throw err;
  }
  return res.json();
}

// --- Async job runner client helper ---
//
// Backend runs long LLM tasks in background threads; we kick them off via
// startJob() then poll until done. This avoids the ~60s gateway timeout that
// long sync HTTP requests hit.

export interface JobStatus<T> {
  state: "pending" | "done" | "failed";
  task_type: string;
  result: T | null;
  error: string | null;
  age_seconds: number;
}

export async function pollJob<T>(
  jobId: string,
  opts?: {
    intervalMs?: number;
    timeoutMs?: number;
    onProgress?: (ageSeconds: number) => void;
    signal?: AbortSignal;
  },
): Promise<T> {
  const interval = opts?.intervalMs ?? 2000;
  const timeout = opts?.timeoutMs ?? 600000; // 10 min ceiling
  const start = Date.now();

  while (true) {
    if (opts?.signal?.aborted) throw new Error("Cancelled");
    if (Date.now() - start > timeout) throw new Error("Job timed out (client-side)");

    const status = await json<JobStatus<T>>(`/jobs/${jobId}`);
    opts?.onProgress?.(status.age_seconds);

    if (status.state === "done") return status.result as T;
    if (status.state === "failed") throw new Error(status.error || "Job failed");

    await new Promise<void>((resolve) => setTimeout(resolve, interval));
  }
}

/** One-shot helper: kick off a background task and resolve when it's done.
 *  Use for any LLM call that might exceed the gateway's ~60s sync timeout.
 */
export async function runJob<T>(
  task_type: string,
  payload: Record<string, unknown>,
  opts?: {
    onProgress?: (ageSeconds: number) => void;
    intervalMs?: number;
    timeoutMs?: number;
    signal?: AbortSignal;
  },
): Promise<T> {
  const { job_id } = await json<{ job_id: string }>("/jobs/start", {
    method: "POST",
    body: JSON.stringify({ task_type, payload }),
  });
  return pollJob<T>(job_id, opts);
}

export const api = {
  getUser: () => json<{ email: string }>("/user"),

  startJob: (task_type: string, payload: Record<string, unknown>) =>
    json<{ job_id: string }>("/jobs/start", {
      method: "POST",
      body: JSON.stringify({ task_type, payload }),
    }),

  listWarehouses: () =>
    json<{ id: string; name: string; state: string; size: string; type: string }[]>("/warehouses"),

  checkCoeMembership: () => json<{ is_member: boolean }>("/user/coe-member"),

  getUserRole: () =>
    json<{
      is_coe: boolean;
      is_bo: boolean;
      coe_group_name: string;
      bo_group_name: string;
    }>("/user/role"),

  setBenchmarkBoApproved: (id: string, idx: number, value: boolean) =>
    json<{ success: boolean; idx: number; value: boolean }>(
      `/engagements/${id}/benchmarks/bo-approved`,
      { method: "PATCH", body: JSON.stringify({ idx, value }) },
    ),

  listEngagements: () => json<Record<string, string>[]>("/engagements"),

  checkNameAvailable: (name: string, excludeEid?: string) => {
    const params = new URLSearchParams({ name });
    if (excludeEid) params.set("exclude_eid", excludeEid);
    return json<{ available: boolean }>(`/engagements/check-name?${params.toString()}`);
  },

  createEngagement: (data: Record<string, string>) =>
    json<{ engagement_id: string }>("/engagements", {
      method: "POST",
      body: JSON.stringify(data),
    }),

  getEngagement: (id: string) =>
    json<Record<string, unknown>>(`/engagements/${id}`),

  updateEngagement: (
    id: string,
    data: Record<string, unknown>,
    ifMatch?: string,
  ) => {
    const headers: Record<string, string> = { "Content-Type": "application/json" };
    if (ifMatch) headers["If-Match"] = ifMatch;
    return json<{ success: boolean; updated_at?: string }>(
      `/engagements/${id}`,
      { method: "PUT", headers, body: JSON.stringify(data) },
    );
  },

  deleteEngagement: (id: string) =>
    json<{ success: boolean }>(`/engagements/${id}`, { method: "DELETE" }),

  /** Save ONLY the ServiceNow URL. Lightweight write outside the optimistic
   *  lock (no If-Match), so it never races the session autosave or reverts. */
  saveServiceNowUrl: (id: string, url: string) =>
    json<{ success: boolean }>(`/engagements/${id}/servicenow-url`, {
      method: "PATCH",
      body: JSON.stringify({ servicenow_ticket_url: url }),
    }),

  /** Save a session. If `ifMatch` is provided (the engagement's last-known
   * updated_at), the server returns 409 if the row has been changed by
   * another user since. Returns the new updated_at on success. */
  saveSession: (
    id: string,
    sessionNum: number,
    data: Record<string, unknown>,
    ifMatch?: string,
  ) => {
    const headers: Record<string, string> = { "Content-Type": "application/json" };
    if (ifMatch) headers["If-Match"] = ifMatch;
    return json<{ success: boolean; updated_at?: string }>(
      `/engagements/${id}/sessions/${sessionNum}`,
      { method: "PUT", headers, body: JSON.stringify(data) },
    );
  },

  coeApprove: (id: string, data: { status: string; notes: string }) =>
    json<{ success: boolean }>(`/engagements/${id}/coe-approve`, {
      method: "PUT",
      body: JSON.stringify(data),
    }),

  /** Production sign-off (Session 7). COE-only, enforced server-side.
   *  Returns updated_at so the caller can refresh its optimistic-lock token. */
  prodApprove: (id: string, data: { status: string; notes: string }) =>
    json<{ success: boolean; updated_at?: string; engagement_status?: string }>(
      `/engagements/${id}/prod-approve`,
      { method: "PUT", body: JSON.stringify(data) },
    ),

  /** Best-effort "who has access" to the pushed Genie space (Session 7).
   *  available=false when no space is pushed or the permissions read fails;
   *  the UI falls back to the space_url for managing sharing in Databricks. */
  getSpaceAccess: (id: string) =>
    json<{
      available: boolean;
      reason?: string;
      space_url?: string;
      access?: { principal: string; levels: string[] }[];
    }>(`/engagements/${id}/space-access`),

  /** Direct URL for the BO pre-work template download. Use as an href so the
   *  browser handles the file download natively (no fetch + blob dance). */
  preworkTemplateUrl: `${BASE}/template/business-owner-prework.xlsx`,

  /** Upload a filled-in pre-work .xlsx and return a parsed preview. Does NOT
   *  mutate the engagement; the caller must call applyPrework to commit. */
  parsePrework: async (id: string, file: File) => {
    const form = new FormData();
    form.append("file", file);
    const res = await fetch(`${BASE}/engagements/${id}/parse-prework`, {
      method: "POST",
      body: form,
    });
    const body = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(body.error || `${res.status} ${res.statusText}`);
    return body as {
      template_version: string;
      warnings: string[];
      errors: string[];
      preview: Record<string, Record<string, string>[]>;
    };
  },

  /** Commit the parsed pre-work to S1/S2 columns. Honors If-Match optimistic
   *  lock; surfaces 409 with the server's current updated_at so the caller
   *  can prompt the user to refresh. */
  applyPrework: async (
    id: string,
    sections: string[],
    data: Record<string, Record<string, string>[]>,
    ifMatch?: string,
  ) => {
    const headers: Record<string, string> = { "Content-Type": "application/json" };
    if (ifMatch) headers["If-Match"] = ifMatch;
    const res = await fetch(`${BASE}/engagements/${id}/apply-prework`, {
      method: "POST",
      headers,
      body: JSON.stringify({ sections, data }),
    });
    const body = await res.json().catch(() => ({}));
    if (res.status === 409) {
      const err: any = new Error(body.message || "Engagement was updated by another user.");
      err.stale = true;
      err.current_updated_at = body.current_updated_at;
      throw err;
    }
    if (!res.ok) throw new Error(body.error || `${res.status} ${res.statusText}`);
    return body as { success: boolean; updated_at: string; applied: string[] };
  },

  draftBenchmarks: (
    id: string,
    count?: number,
    onProgress?: (s: number) => void,
  ) =>
    runJob<{ benchmarks: BenchmarkQuestion[] }>(
      "draft_benchmarks",
      { engagement_id: id, count: count ?? 12 },
      { onProgress },
    ),

  draftBenchmarkSql: (
    id: string,
    question: string,
    warehouse_id?: string,
    validate?: boolean,
    onProgress?: (s: number) => void,
  ) =>
    runJob<{
      sql: string;
      explanation?: string;
      validation?: {
        ran: boolean;
        error: string | null;
        retried: boolean;
        sample_result: BenchmarkSampleResult | null;
      } | null;
    }>(
      "draft_benchmark_sql",
      {
        engagement_id: id,
        question,
        warehouse_id: warehouse_id || "",
        validate: !!validate,
      },
      { onProgress },
    ),

  draftBenchmarkSummary: (id: string, question: string, sql: string) =>
    json<{ explanation: string }>(`/engagements/${id}/draft-benchmark-summary`, {
      method: "POST",
      body: JSON.stringify({ question, sql }),
    }),

  runBenchmarkSql: (id: string, sql: string, warehouse_id: string) =>
    json<{
      columns?: string[];
      rows?: unknown[][];
      row_count?: number;
      truncated?: boolean;
      limit?: number;
      error?: string;
    }>(`/engagements/${id}/run-benchmark-sql`, {
      method: "POST",
      body: JSON.stringify({ sql, warehouse_id }),
    }),

  generatePlan: (
    id: string,
    warehouse_id?: string,
    onProgress?: (s: number) => void,
  ) =>
    runJob<{
      general_instructions: string;
      sample_questions: string[];
      sql_filters: SqlSnippet[];
      sql_dimensions: SqlSnippet[];
      sql_measures: SqlSnippet[];
      example_queries: ExampleQuery[];
      joins: UcJoin[];
      narrative: string;
      warnings?: string[];
    }>(
      "generate_plan",
      { engagement_id: id, warehouse_id: warehouse_id || "" },
      { onProgress },
    ),

  draftMetricViewYaml: (
    id: string,
    warehouse_id?: string,
    onProgress?: (s: number) => void,
  ) =>
    runJob<{ yaml: string; source_table: string; suggested_name: string; warnings?: string[] }>(
      "draft_mv_yaml",
      { engagement_id: id, warehouse_id: warehouse_id || "" },
      { onProgress },
    ),

  getMvPromptPreview: (id: string) =>
    json<{ prompt: string }>(`/engagements/${id}/mv-prompt-preview`),

  createMetricView: async (
    id: string,
    body: {
      catalog: string;
      schema: string;
      name: string;
      yaml: string;
      warehouse_id: string;
      overwrite?: boolean;
    },
  ): Promise<
    | { success: true; fqn: string; updated_at?: string }
    | { success: false; exists: true; fqn: string; owner: string | null }
  > => {
    const res = await fetch(`${BASE}/engagements/${id}/create-metric-view`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    const payload = await res.json().catch(() => ({}));
    if (res.status === 409 && payload.exists) {
      return { success: false, exists: true, fqn: payload.fqn, owner: payload.owner ?? null };
    }
    if (!res.ok) {
      throw new Error(payload.error || `${res.status} ${res.statusText}`);
    }
    return { success: true, fqn: payload.fqn, updated_at: payload.updated_at };
  },

  listCatalogs: () => json<string[]>("/uc/catalogs"),

  listSchemas: (catalog: string) =>
    json<string[]>(`/uc/schemas?catalog=${encodeURIComponent(catalog)}`),

  /** Return UC's authoritative table_type for a FQN. Used by the Data
   *  Sources panel to categorize a picker selection as Table vs Metric View
   *  on Add (the picker dropdown lists both kinds by name). */
  getTableType: (fqn: string) =>
    json<{ fqn: string; table_type: string; comment: string }>(
      `/uc/table-type?fqn=${encodeURIComponent(fqn)}`,
    ),

  /** S3 data-sources-first flow: find existing Metric Views that depend on
   *  any of the picked source tables. Used to surface reusable MVs to the
   *  analyst so they don't have to re-author measures from scratch.
   *
   *  Passing `warehouseId` enables the broad scan via
   *  system.information_schema (catches cross-catalog MVs). Without it,
   *  discovery falls back to scanning only the (catalog, schema) of each
   *  picked table -- correct but narrower. */
  findMetricViewsForTables: (fqns: string[], warehouseId?: string) => {
    const params = new URLSearchParams({ fqns: fqns.join(",") });
    if (warehouseId) params.set("warehouse_id", warehouseId);
    return json<{
      metric_views: {
        fqn: string;
        catalog: string;
        schema: string;
        name: string;
        comment: string;
        owner: string;
        updated_at?: string;
        dependencies: string[];
      }[];
      errors: string[];
      warnings: string[];
      scope: { broad: boolean };
    }>(`/uc/metric-views-for-tables?${params.toString()}`);
  },

  /** Deterministic "what this MV covers" view: dimensions + measures + each
   *  column's display_name, synonyms, and comment. No LLM needed. */
  fetchMetricViewDetails: (fqn: string, warehouseId: string) =>
    json<{
      fqn: string;
      dimensions: { name: string; display_name: string; synonyms: string[]; comment: string; data_type: string }[];
      measures:   { name: string; display_name: string; synonyms: string[]; comment: string; data_type: string }[];
    }>(`/uc/metric-view-details?fqn=${encodeURIComponent(fqn)}&warehouse_id=${encodeURIComponent(warehouseId)}`),

  /** Push the engagement plan to a Genie Space. Honors If-Match optimistic
   *  lock so a concurrent edit can't race in stale data; 409 surfaces a
   *  stale-error the caller can show as "refresh before pushing". */
  pushToGenie: async (
    id: string,
    body: {
      mode: "existing" | "new";
      space_id?: string;
      warehouse_id: string;
      new_title?: string;
      new_description?: string;
      new_parent_path?: string;
      general_instructions: string;
      sample_questions: string[];
      sql_filters?: SqlSnippet[];
      sql_dimensions?: SqlSnippet[];
      sql_measures?: SqlSnippet[];
      example_queries?: ExampleQuery[];
      joins?: UcJoin[];
    },
    ifMatch?: string,
  ) => {
    const headers: Record<string, string> = { "Content-Type": "application/json" };
    if (ifMatch) headers["If-Match"] = ifMatch;
    const res = await fetch(`${BASE}/engagements/${id}/push-to-genie`, {
      method: "POST",
      headers,
      body: JSON.stringify(body),
    });
    const respBody = await res.json().catch(() => ({}));
    if (res.status === 409) {
      const err: any = new Error(
        respBody.message || "Engagement was updated by another user. Refresh before pushing.",
      );
      err.stale = true;
      err.current_updated_at = respBody.current_updated_at;
      throw err;
    }
    if (!res.ok) throw new Error(respBody.error || `${res.status} ${res.statusText}`);
    return respBody as {
      mode: string;
      space_id: string;
      space_url: string;
      created?: boolean;
      updated?: boolean;
      warnings?: string[];
      updated_at?: string;
    };
  },
};
