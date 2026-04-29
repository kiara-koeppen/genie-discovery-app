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
}

async function json<T>(url: string, opts?: RequestInit): Promise<T> {
  const res = await fetch(`${BASE}${url}`, {
    headers: { "Content-Type": "application/json" },
    ...opts,
  });
  if (!res.ok) {
    const body = await res.json().catch(() => ({}));
    throw new Error(body.error || `${res.status} ${res.statusText}`);
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

  listEngagements: () => json<Record<string, string>[]>("/engagements"),

  checkNameAvailable: (name: string) =>
    json<{ available: boolean }>(`/engagements/check-name?name=${encodeURIComponent(name)}`),

  createEngagement: (data: Record<string, string>) =>
    json<{ engagement_id: string }>("/engagements", {
      method: "POST",
      body: JSON.stringify(data),
    }),

  getEngagement: (id: string) =>
    json<Record<string, unknown>>(`/engagements/${id}`),

  updateEngagement: (id: string, data: Record<string, unknown>) =>
    json<{ success: boolean }>(`/engagements/${id}`, {
      method: "PUT",
      body: JSON.stringify(data),
    }),

  deleteEngagement: (id: string) =>
    json<{ success: boolean }>(`/engagements/${id}`, { method: "DELETE" }),

  saveSession: (id: string, sessionNum: number, data: Record<string, unknown>) =>
    json<{ success: boolean }>(`/engagements/${id}/sessions/${sessionNum}`, {
      method: "PUT",
      body: JSON.stringify(data),
    }),

  coeApprove: (id: string, data: { status: string; notes: string }) =>
    json<{ success: boolean }>(`/engagements/${id}/coe-approve`, {
      method: "PUT",
      body: JSON.stringify(data),
    }),

  draftBenchmarks: (id: string, count?: number) =>
    json<{ benchmarks: BenchmarkQuestion[] }>(`/engagements/${id}/draft-benchmarks`, {
      method: "POST",
      body: JSON.stringify({ count: count ?? 12 }),
    }),

  draftBenchmarkSql: (
    id: string,
    question: string,
    warehouse_id?: string,
    validate?: boolean,
  ) =>
    json<{
      sql: string;
      explanation?: string;
      validation?: {
        ran: boolean;
        error: string | null;
        retried: boolean;
        sample_result: BenchmarkSampleResult | null;
      } | null;
    }>(`/engagements/${id}/draft-benchmark-sql`, {
      method: "POST",
      body: JSON.stringify({
        question,
        warehouse_id: warehouse_id || "",
        validate: !!validate,
      }),
    }),

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

  getAutoSummary: (id: string) =>
    json<{
      summary: string;
      unacknowledged_gaps?: BriefGap[];
    }>(`/engagements/${id}/auto-summary`),

  generatePlan: (id: string, warehouse_id?: string) =>
    json<{
      general_instructions: string;
      sample_questions: string[];
      sql_filters: SqlSnippet[];
      sql_dimensions: SqlSnippet[];
      sql_measures: SqlSnippet[];
      example_queries: ExampleQuery[];
      joins: UcJoin[];
      narrative: string;
      warnings?: string[];
    }>(`/engagements/${id}/generate-plan`, {
      method: "POST",
      body: JSON.stringify({ warehouse_id: warehouse_id || "" }),
    }),

  draftMetricViewYaml: (id: string, warehouse_id?: string) =>
    json<{ yaml: string; source_table: string; suggested_name: string; warnings?: string[] }>(
      `/engagements/${id}/draft-metric-view-yaml`,
      { method: "POST", body: JSON.stringify({ warehouse_id: warehouse_id || "" }) },
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
    | { success: true; fqn: string }
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
    return { success: true, fqn: payload.fqn };
  },

  listCatalogs: () => json<string[]>("/uc/catalogs"),

  listSchemas: (catalog: string) =>
    json<string[]>(`/uc/schemas?catalog=${encodeURIComponent(catalog)}`),

  pushToGenie: (
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
  ) =>
    json<{
      mode: string;
      space_id: string;
      space_url: string;
      created?: boolean;
      updated?: boolean;
      warnings?: string[];
    }>(`/engagements/${id}/push-to-genie`, {
      method: "POST",
      body: JSON.stringify(body),
    }),
};
