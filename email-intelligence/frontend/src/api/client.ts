import type { CurrentJobResponse, DashboardTreeResponse, JobStatusResponse } from "./types";

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

  startIngestFull: () => request<{ job_id: string }>("/api/jobs/ingest/full", { method: "POST" }),
  startIngestRefresh: () =>
    request<{ job_id: string }>("/api/jobs/ingest/refresh", { method: "POST" }),
  startClusterLabel: () =>
    request<{ job_id: string }>("/api/jobs/cluster-label/run", { method: "POST" }),

  getCurrentJob: () => request<CurrentJobResponse>("/api/jobs/current"),
  getJobStatus: (jobId: string) => request<JobStatusResponse>(`/api/jobs/${jobId}/status`),
};
