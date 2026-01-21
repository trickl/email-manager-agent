import type {
  BulkRetentionUpdateResponse,
  CurrentJobResponse,
  DashboardTreeResponse,
  JobStatusResponse,
  MessageSamplesResponse,
  PushResponse,
  RetentionPreviewResponse,
  RetentionRunResponse,
  RetentionDefaultResponse,
  RetentionPlanResponse,
  SyncExistenceResponse,
  TaxonomyLabel,
} from "./types";

export class ApiError extends Error {
  public readonly status: number;
  public readonly bodyText: string;

  constructor(message: string, status: number, bodyText: string) {
    super(message);
    this.status = status;
    this.bodyText = bodyText;
  }
}

function apiBaseUrl(): string {
  // Prefer explicit env var; otherwise rely on Vite dev proxy / same-origin.
  return (import.meta as any).env?.VITE_API_BASE_URL ?? "";
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const base = apiBaseUrl();
  const url = `${base}${path}`;

  const res = await fetch(url, {
    ...init,
    headers: {
      "Content-Type": "application/json",
      ...(init?.headers ?? {}),
    },
  });

  if (!res.ok) {
    const bodyText = await res.text();
    throw new ApiError(`Request failed: ${res.status} ${res.statusText}`, res.status, bodyText);
  }

  return (await res.json()) as T;
}

export const api = {
  getDashboardTree: () => request<DashboardTreeResponse>("/api/dashboard/tree"),

  getMessageSamples: (nodeId: string, limit: number = 25) =>
    request<MessageSamplesResponse>(
      `/api/messages/samples?node_id=${encodeURIComponent(nodeId)}&limit=${encodeURIComponent(
        String(limit)
      )}`
    ),

  startIngestFull: () => request<{ job_id: string }>("/api/jobs/ingest/full", { method: "POST" }),
  startIngestRefresh: () =>
    request<{ job_id: string }>("/api/jobs/ingest/refresh", { method: "POST" }),
  startClusterLabel: () =>
    request<{ job_id: string }>("/api/jobs/cluster-label/run", { method: "POST" }),

  startGmailPushBulk: (batchSize: number = 200) =>
    request<{ job_id: string }>(
      `/api/jobs/gmail/push/bulk?batch_size=${encodeURIComponent(String(batchSize))}`,
      { method: "POST" }
    ),

  startGmailArchivePush: (batchSize: number = 200, dryRun: boolean = false) =>
    request<{ job_id: string }>(
      `/api/jobs/gmail/archive/push?batch_size=${encodeURIComponent(
        String(batchSize)
      )}&dry_run=${encodeURIComponent(String(dryRun))}`,
      { method: "POST" }
    ),

  getCurrentJob: () => request<CurrentJobResponse>("/api/jobs/current"),
  getJobStatus: (jobId: string) => request<JobStatusResponse>(`/api/jobs/${jobId}/status`),

  // Taxonomy CRUD
  getTaxonomy: () => request<TaxonomyLabel[]>("/api/taxonomy"),
  createTaxonomyLabel: (body: {
    name: string;
    description?: string;
    parent_id?: number | null;
    retention_days?: number | null;
    is_active?: boolean;
  }) =>
    request<TaxonomyLabel>("/api/taxonomy", {
      method: "POST",
      body: JSON.stringify(body),
    }),
  updateTaxonomyLabel: (
    id: number,
    body: {
      name?: string | null;
      description?: string | null;
      retention_days?: number | null;
      is_active?: boolean | null;
    }
  ) =>
    request<TaxonomyLabel>(`/api/taxonomy/${encodeURIComponent(String(id))}`, {
      method: "PUT",
      body: JSON.stringify(body),
    }),
  deleteTaxonomyLabel: (id: number) =>
    request<{ deleted: boolean; id: number }>(`/api/taxonomy/${encodeURIComponent(String(id))}`, {
      method: "DELETE",
    }),

  bulkUpdateTaxonomyRetention: (items: Array<{ id: number; retention_days: number | null }>) =>
    request<BulkRetentionUpdateResponse>("/api/taxonomy/retention/bulk", {
      method: "POST",
      body: JSON.stringify({ items }),
    }),

  // Gmail sync
  syncGmailLabelExistence: (dryRun: boolean = false) =>
    request<SyncExistenceResponse>("/api/gmail-sync/labels/sync", {
      method: "POST",
      body: JSON.stringify({ dry_run: dryRun }),
    }),
  pushGmailLabelsBulk: (limit: number = 200, offset: number = 0) =>
    request<PushResponse>("/api/gmail-sync/messages/push-bulk", {
      method: "POST",
      body: JSON.stringify({ limit, offset }),
    }),
  pushGmailLabelsIncremental: (limit: number = 200) =>
    request<PushResponse>("/api/gmail-sync/messages/push-incremental", {
      method: "POST",
      body: JSON.stringify({ limit }),
    }),

  // Retention
  getRetentionDefault: () => request<RetentionDefaultResponse>("/api/taxonomy/retention/default"),
  setRetentionDefault: (retention_default_days: number) =>
    request<RetentionDefaultResponse>("/api/taxonomy/retention/default", {
      method: "PUT",
      body: JSON.stringify({ retention_default_days }),
    }),
  retentionPreview: (limit: number = 25) =>
    request<RetentionPreviewResponse>("/api/gmail-sync/retention/preview", {
      method: "POST",
      body: JSON.stringify({ limit }),
    }),

  retentionPlan: (maxRows: number | null = null) =>
    request<RetentionPlanResponse>("/api/gmail-sync/retention/plan", {
      method: "POST",
      body: JSON.stringify({ max_rows: maxRows }),
    }),

  retentionRun: (limit: number = 500, dryRun: boolean = false) =>
    request<RetentionRunResponse>("/api/gmail-sync/retention/run", {
      method: "POST",
      body: JSON.stringify({ limit, dry_run: dryRun }),
    }),
};
