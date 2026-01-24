import type {
  BulkRetentionUpdateResponse,
  CalendarCheckResponse,
  CalendarPublishResponse,
  CurrentJobResponse,
  DashboardTreeResponse,
  FutureEventsResponse,
  HideEventResponse,
  JobStatusResponse,
  MessageSamplesResponse,
  PaymentsAnalyticsResponse,
  PaymentsListResponse,
  PushOutboxStatusResponse,
  PushResponse,
  RetentionPreviewResponse,
  RetentionRunResponse,
  RetentionDefaultResponse,
  RetentionPlanResponse,
  StatusResponse,
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

  let res: Response;
  try {
    res = await fetch(url, {
      ...init,
      headers: {
        "Content-Type": "application/json",
        ...(init?.headers ?? {}),
      },
    });
  } catch (err) {
    const details = err instanceof Error ? err.message : String(err);
    // status=0 is a conventional sentinel for network/proxy failures.
    throw new ApiError(
      "Network error: could not reach the API (is the backend running?)",
      0,
      details,
    );
  }

  if (!res.ok) {
    const bodyText = await res.text();
    throw new ApiError(`Request failed: ${res.status} ${res.statusText}`, res.status, bodyText);
  }

  return (await res.json()) as T;
}

export const api = {
  getStatus: () => request<StatusResponse>("/status"),

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

  startLabelAuto: (threshold: number = 200) =>
    request<{ job_id: string }>(
      `/api/jobs/label/auto?threshold=${encodeURIComponent(String(threshold))}`,
      { method: "POST" }
    ),

  startGmailPushBulk: (batchSize: number = 200) =>
    request<{ job_id: string }>(
      `/api/jobs/gmail/push/bulk?batch_size=${encodeURIComponent(String(batchSize))}`,
      { method: "POST" }
    ),

  startGmailPushOutbox: (batchSize: number = 250) =>
    request<{ job_id: string }>(
      `/api/jobs/gmail/push/outbox?batch_size=${encodeURIComponent(String(batchSize))}`,
      { method: "POST" }
    ),

  startGmailArchivePush: (batchSize: number = 200, dryRun: boolean = false) =>
    request<{ job_id: string }>(
      `/api/jobs/gmail/archive/push?batch_size=${encodeURIComponent(
        String(batchSize)
      )}&dry_run=${encodeURIComponent(String(dryRun))}`,
      { method: "POST" }
    ),

  startGmailArchiveTrash: (opts?: {
    batch_size?: number;
    dry_run?: boolean;
    remove_archive_label?: boolean;
  }) => {
    const batchSize = opts?.batch_size ?? 250;
    const dryRun = Boolean(opts?.dry_run);
    const removeArchive = Boolean(opts?.remove_archive_label);
    return request<{ job_id: string }>(
      `/api/jobs/gmail/archive/trash?batch_size=${encodeURIComponent(
        String(batchSize)
      )}&dry_run=${encodeURIComponent(String(dryRun))}&remove_archive_label=${encodeURIComponent(
        String(removeArchive)
      )}`,
      { method: "POST" }
    );
  },

  startMaintenance: (opts?: {
    inbox_cleanup_days?: number;
    label_threshold?: number;
    fallback_days?: number;
  }) => {
    const params = new URLSearchParams();
    if (opts?.inbox_cleanup_days !== undefined) {
      params.set("inbox_cleanup_days", String(opts.inbox_cleanup_days));
    }
    if (opts?.label_threshold !== undefined) {
      params.set("label_threshold", String(opts.label_threshold));
    }
    if (opts?.fallback_days !== undefined) {
      params.set("fallback_days", String(opts.fallback_days));
    }
    const suffix = params.toString();
    const path = suffix ? `/api/jobs/maintenance/run?${suffix}` : "/api/jobs/maintenance/run";
    return request<{ job_id: string }>(path, { method: "POST" });
  },

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

  getGmailPushOutboxStatus: () =>
    request<PushOutboxStatusResponse>("/api/gmail-sync/messages/push-outbox/status"),

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

  // Events
  getFutureEvents: (limit: number = 200, includeHidden: boolean = false) =>
    request<FutureEventsResponse>(
      `/api/events/future?limit=${encodeURIComponent(String(limit))}&include_hidden=${encodeURIComponent(
        String(includeHidden)
      )}`
    ),

  hideEvent: (messageId: number) =>
    request<HideEventResponse>(`/api/events/${encodeURIComponent(String(messageId))}/hide`, {
      method: "POST",
    }),

  unhideEvent: (messageId: number) =>
    request<HideEventResponse>(`/api/events/${encodeURIComponent(String(messageId))}/unhide`, {
      method: "POST",
    }),

  checkEventCalendar: (messageId: number) =>
    request<CalendarCheckResponse>(
      `/api/events/${encodeURIComponent(String(messageId))}/calendar/check`,
      { method: "POST" }
    ),

  publishEventCalendar: (messageId: number) =>
    request<CalendarPublishResponse>(
      `/api/events/${encodeURIComponent(String(messageId))}/calendar/publish`,
      { method: "POST" }
    ),

  // Payments
  getPaymentsRecent: (months: number = 3, limit: number = 200, currency?: string | null) => {
    const params = new URLSearchParams({
      months: String(months),
      limit: String(limit),
    });
    if (currency) {
      params.set("currency", currency);
    }
    return request<PaymentsListResponse>(`/api/payments/recent?${params.toString()}`);
  },

  getPaymentsAnalytics: (months: number = 6, currency?: string | null) => {
    const params = new URLSearchParams({
      months: String(months),
    });
    if (currency) {
      params.set("currency", currency);
    }
    return request<PaymentsAnalyticsResponse>(`/api/payments/analytics?${params.toString()}`);
  },
};
