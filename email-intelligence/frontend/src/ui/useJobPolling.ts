import { useEffect, useMemo, useRef, useState } from "react";
import { api, ApiError } from "../api/client";
import type { CurrentJobResponse, JobStatusResponse } from "../api/types";

export type JobType = "ingest_full" | "ingest_refresh" | "cluster_label";

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function apiBaseUrl(): string {
  // Mirror api/client.ts behavior for EventSource URLs.
  return (import.meta as any).env?.VITE_API_BASE_URL ?? "";
}

export function useJobPolling(): {
  activeJob: CurrentJobResponse["active"] | null;
  jobStatus: JobStatusResponse | null;
  lastCompletedJobId: string | null;
  startJob: (t: JobType) => Promise<void>;
} {
  const [activeJob, setActiveJob] = useState<CurrentJobResponse["active"] | null>(null);
  const [jobStatus, setJobStatus] = useState<JobStatusResponse | null>(null);
  const [lastCompletedJobId, setLastCompletedJobId] = useState<string | null>(null);

  // Coalesce frequent status updates (especially via SSE) to avoid excessive renders.
  // Note: this also indirectly reduces dashboard refresh frequency (DashboardPage listens
  // to jobStatus.updated_at changes).
  const lastJobStatusEmitAtRef = useRef<number>(0);
  const pendingJobStatusRef = useRef<JobStatusResponse | null>(null);
  const jobStatusFlushTimerRef = useRef<number | null>(null);

  const statusPollAbort = useRef<AbortController | null>(null);

  // Always poll current job every 5 seconds.
  useEffect(() => {
    let cancelled = false;

    async function loop() {
      while (!cancelled) {
        try {
          const current = await api.getCurrentJob();
          if (!cancelled) setActiveJob(current.active);
        } catch {
          // If backend is down, keep trying; UI will show idle.
          if (!cancelled) setActiveJob(null);
        }

        await sleep(5000);
      }
    }

    loop();

    return () => {
      cancelled = true;
    };
  }, []);

  // When there's an active job, prefer SSE for status updates (fallback to polling).
  useEffect(() => {
    statusPollAbort.current?.abort();
    statusPollAbort.current = new AbortController();

    let cancelled = false;

    let es: EventSource | null = null;
    let usingFallbackPolling = false;
    let lastStatusAt = Date.now();
    let watchdog: number | null = null;

    function clearJobStatusFlushTimer() {
      if (jobStatusFlushTimerRef.current) {
        window.clearTimeout(jobStatusFlushTimerRef.current);
      }
      jobStatusFlushTimerRef.current = null;
    }

    function emitJobStatus(status: JobStatusResponse) {
      pendingJobStatusRef.current = null;
      lastJobStatusEmitAtRef.current = Date.now();
      setJobStatus(status);
    }

    function setJobStatusThrottled(status: JobStatusResponse, opts?: { force?: boolean }) {
      if (cancelled) return;

      const force = Boolean(opts?.force);
      const now = Date.now();

      // Always emit terminal states immediately.
      if (force || status.state === "succeeded" || status.state === "failed") {
        clearJobStatusFlushTimer();
        emitJobStatus(status);
        return;
      }

      const minIntervalMs = 500;
      const elapsed = now - lastJobStatusEmitAtRef.current;

      if (elapsed >= minIntervalMs) {
        clearJobStatusFlushTimer();
        emitJobStatus(status);
        return;
      }

      // Otherwise, keep only the latest status and schedule a flush.
      pendingJobStatusRef.current = status;
      if (jobStatusFlushTimerRef.current) return;

      const delay = Math.max(0, minIntervalMs - elapsed);
      jobStatusFlushTimerRef.current = window.setTimeout(() => {
        jobStatusFlushTimerRef.current = null;
        if (cancelled) return;
        const pending = pendingJobStatusRef.current;
        if (pending) emitJobStatus(pending);
      }, delay);
    }

    function closeSse() {
      try {
        es?.close();
      } catch {
        // ignore
      }
      es = null;
    }

    function startFallbackPolling(jobId: string) {
      if (usingFallbackPolling) return;
      usingFallbackPolling = true;

      async function loop() {
        while (!cancelled) {
          try {
            const status = await api.getJobStatus(jobId);
            if (cancelled) return;

            // Polling is already low-frequency (2s), but we route through the same
            // throttler for consistency.
            setJobStatusThrottled(status, { force: true });

            if (status.state === "succeeded" || status.state === "failed") {
              setLastCompletedJobId(status.job_id);
              return;
            }
          } catch (e) {
            if (cancelled) return;
            const msg = e instanceof ApiError ? e.bodyText || e.message : String(e);
            // Keep previous jobStatus if we have one.
            setJobStatus((prev: JobStatusResponse | null) => prev ?? null);
            console.warn("job status poll failed", msg);
          }

          await sleep(2000);
        }
      }

      loop();
    }

    if (activeJob?.job_id) {
      const jobId = activeJob.job_id;

      // Reset throttling window per job so the first status is shown immediately.
      clearJobStatusFlushTimer();
      pendingJobStatusRef.current = null;
      lastJobStatusEmitAtRef.current = 0;

      // Try SSE first (lower latency + less polling overhead).
      try {
        const url = `${apiBaseUrl()}/api/jobs/${encodeURIComponent(jobId)}/events`;
        es = new EventSource(url);

        es.addEventListener("job_status", (evt: MessageEvent) => {
          if (cancelled) return;
          lastStatusAt = Date.now();

          try {
            const status = JSON.parse(String(evt.data)) as JobStatusResponse;
            setJobStatusThrottled(status);
            if (status.state === "succeeded" || status.state === "failed") {
              setLastCompletedJobId(status.job_id);
              closeSse();
            }
          } catch (e) {
            console.warn("job SSE parse failed", e);
          }
        });

        es.onerror = () => {
          // Most browsers call onerror for transient disconnects too.
          // We fall back to polling if the stream becomes unreliable.
          if (cancelled) return;
          closeSse();
          startFallbackPolling(jobId);
        };

        // If we don't receive any status updates in a while, assume SSE isn't working and poll.
        watchdog = window.setInterval(() => {
          if (cancelled) return;
          if (Date.now() - lastStatusAt > 20000) {
            closeSse();
            startFallbackPolling(jobId);
          }
        }, 5000);
      } catch {
        startFallbackPolling(jobId);
      }
    } else {
      setJobStatus(null);
    }

    return () => {
      cancelled = true;
      statusPollAbort.current?.abort();

      clearJobStatusFlushTimer();
      pendingJobStatusRef.current = null;

      if (watchdog) {
        window.clearInterval(watchdog);
      }
      closeSse();
    };
  }, [activeJob?.job_id]);

  async function startJob(t: JobType): Promise<void> {
    setLastCompletedJobId(null);
    if (t === "ingest_full") {
      await api.startIngestFull();
      const current = await api.getCurrentJob();
      setActiveJob(current.active);
      return;
    }
    if (t === "ingest_refresh") {
      await api.startIngestRefresh();
      const current = await api.getCurrentJob();
      setActiveJob(current.active);
      return;
    }
    await api.startClusterLabel();
    const current = await api.getCurrentJob();
    setActiveJob(current.active);
  }

  return useMemo(
    () => ({
      activeJob,
      jobStatus,
      lastCompletedJobId,
      startJob,
    }),
    [activeJob, jobStatus, lastCompletedJobId]
  );
}
