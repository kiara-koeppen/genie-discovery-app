const BASE = "/api";

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

export const api = {
  getUser: () => json<{ email: string }>("/user"),

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

  getAutoSummary: (id: string) =>
    json<{ summary: string }>(`/engagements/${id}/auto-summary`),
};
