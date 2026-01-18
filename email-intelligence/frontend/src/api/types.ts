export type FrequencyLabel = "daily" | "weekly" | "monthly" | "quarterly" | "yearly";

export interface DashboardNode {
  id: string;
  name: string;
  count: number;
  unread_count: number;
  unread_ratio: number;
  frequency?: FrequencyLabel | null;
  children: DashboardNode[];
}

export interface DashboardTreeResponse {
  generated_at: string;
  root: DashboardNode;
}

export type JobState = "queued" | "running" | "succeeded" | "failed";

export interface JobProgress {
  total?: number | null;
  processed: number;
  percent?: number | null;
}

export interface JobCounters {
  inserted: number;
  skipped_existing: number;
  failed: number;
}

export interface JobStatusResponse {
  job_id: string;
  type: string;
  state: JobState;
  phase?: string | null;
  started_at: string;
  updated_at: string;
  progress: JobProgress;
  counters: JobCounters;
  message?: string | null;
  eta_hint?: string | null;
}

export interface CurrentJobResponse {
  active: null | {
    job_id: string;
    type: string;
    state: JobState;
  };
}
