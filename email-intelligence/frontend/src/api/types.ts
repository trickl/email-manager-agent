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

export interface TaxonomyLabel {
  id: number;
  level: number;
  slug: string;
  name: string;
  description: string;
  parent_id?: number | null;
  retention_days?: number | null;
  is_active: boolean;
  managed_by_system: boolean;

  gmail_label_id?: string | null;
  gmail_label_name: string;

  last_sync_at?: string | null;
  sync_status?: string | null;
  sync_error?: string | null;

  assigned_message_count: number;
}

export interface BulkRetentionUpdateResponse {
  updated: number;
}

export interface SyncExistenceResponse {
  dry_run: boolean;
  created: number;
  updated: number;
  linked_existing: number;
  errors: number;
  generated_at: string;
}

export interface PushResponse {
  attempted: number;
  succeeded: number;
  failed: number;
  generated_at: string;
}

export interface RetentionPreviewItem {
  message_id: number;
  gmail_message_id: string;
  subject?: string | null;
  from_domain: string;
  internal_date: string;
}

export interface RetentionPreviewResponse {
  eligible_count: number;
  sample: RetentionPreviewItem[];
  generated_at: string;
}

export interface RetentionRunResponse {
  dry_run: boolean;
  attempted: number;
  succeeded: number;
  failed: number;
  generated_at: string;
}

export interface RetentionPlanResponse {
  planned: number;
  pending_outbox: number;
  generated_at: string;
}

export interface RetentionDefaultResponse {
  retention_default_days: number;
}
