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

export interface EmailMessageSummary {
  gmail_message_id: string;
  subject?: string | null;
  from_domain: string;
  internal_date: string;
  is_unread: boolean;
  category?: string | null;
  subcategory?: string | null;
  label_ids: string[];
  label_names: string[];
}

export interface MessageSamplesResponse {
  node_id: string;
  generated_at: string;
  messages: EmailMessageSummary[];
}
