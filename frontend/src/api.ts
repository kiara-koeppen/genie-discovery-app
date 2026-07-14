const BASE = "/api";

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

export const api = {
  getUser: () => json<{ email: string }>("/user"),

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

  /** COE review action (S4). Server-enforced COE-group only. "approved" sets
   *  the engagement to 'ready_for_pilot'; "changes_requested" returns it to
   *  'in_progress'. Returns updated_at so the caller can refresh its lock token. */
  coeApprove: (id: string, data: { status: string; notes: string }) =>
    json<{ success: boolean; updated_at?: string }>(`/engagements/${id}/coe-approve`, {
      method: "PUT",
      body: JSON.stringify(data),
    }),

  /** Analyst action: mark an engagement "Ready for COE Review". Open to
   *  analysts (NOT BO-only users); only sets status to 'ready_for_review'.
   *  Lock-free side-write — no If-Match, never bumps updated_at. */
  requestReview: (id: string) =>
    json<{ success: boolean; status: string }>(`/engagements/${id}/request-review`, {
      method: "PUT",
    }),

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

  /** Export selected S1/S2 sections to a populated .xlsx or .csv and trigger a
   *  browser download. Read-only on the server (no mutation, no lock). The
   *  .xlsx matches the template shape and is re-uploadable via parsePrework;
   *  the .csv is a flat export for Genie Code ingestion. */
  exportPrework: async (
    id: string,
    sections: string[],
    data: Record<string, Record<string, string>[]>,
    format: "xlsx" | "csv" = "xlsx",
  ) => {
    const res = await fetch(`${BASE}/engagements/${id}/export-prework`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ sections, data, format }),
    });
    if (!res.ok) {
      const body = await res.json().catch(() => ({} as any));
      throw new Error(body.error || `${res.status} ${res.statusText}`);
    }
    const blob = await res.blob();
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `genie-discovery-export.${format}`;
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(url);
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
};
