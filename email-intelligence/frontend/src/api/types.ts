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

export interface StatusResponse {
  current_phase?: string | null;
  total_email_count: number;
  labelled_email_count: number;
  unlabelled_email_count: number;
  cluster_count: number;
  estimated_remaining_clusters: number;
  last_ingested_internal_date?: string | null;
}

export interface PushOutboxStatusResponse {
  pending_outbox: number;
  generated_at: string;
}

export interface FutureEventItem {
  message_id: number;
  event_date: string;
  start_time?: string | null;
  end_time?: string | null;
  end_time_inferred: boolean;
  timezone?: string | null;
  event_type?: string | null;
  event_name?: string | null;

  calendar_event_id?: string | null;
  calendar_checked_at?: string | null;
  calendar_published_at?: string | null;
  hidden_at?: string | null;

  subject?: string | null;
  from_domain: string;
  internal_date: string;
}

export interface FutureEventsResponse {
  generated_at: string;
  events: FutureEventItem[];
}

export interface HideEventResponse {
  message_id: number;
  hidden: boolean;
  hidden_at?: string | null;
}

export interface CalendarCheckResponse {
  message_id: number;
  calendar_ical_uid: string;
  exists: boolean;
  calendar_event_id?: string | null;
  calendar_checked_at: string;
}

export interface CalendarPublishResponse {
  message_id: number;
  calendar_ical_uid: string;
  already_existed: boolean;
  calendar_event_id: string;
  calendar_published_at?: string | null;
}

export interface PaymentItem {
  message_id: number;
  item_name?: string | null;
  vendor_name?: string | null;
  item_category?: string | null;
  cost_amount?: number | null;
  cost_currency?: string | null;
  is_recurring?: boolean | null;
  frequency?: string | null;
  payment_date?: string | null;
  payment_fingerprint?: string | null;
  subject?: string | null;
  from_domain?: string | null;
  internal_date?: string | null;
}

export interface PaymentsListResponse {
  generated_at: string;
  payments: PaymentItem[];
}

export interface SpendByVendor {
  vendor: string;
  total_spend: number;
}

export interface SpendByCategory {
  category: string;
  total_spend: number;
}

export interface SpendByRecurring {
  kind: string;
  payment_count: number;
  total_spend: number;
}

export interface SpendByFrequency {
  frequency: string;
  payment_count: number;
  total_spend: number;
}

export interface SpendByMonth {
  month: string;
  total_spend: number;
  payment_count: number;
}

export interface PaymentsAnalyticsResponse {
  generated_at: string;
  window_start: string;
  window_end: string;
  currency?: string | null;
  available_currencies: string[];
  payment_count: number;
  total_spend: number;
  by_vendor: SpendByVendor[];
  by_category: SpendByCategory[];
  by_recurring: SpendByRecurring[];
  by_frequency: SpendByFrequency[];
  by_month: SpendByMonth[];
}
