import { useEffect, useMemo, useRef, useState } from "react";
import { api, ApiError } from "../api/client";
import type { CurrentJobResponse, JobStatusResponse } from "../api/types";

export type JobType = "ingest_full" | "ingest_refresh" | "cluster_label";

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
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

  // When there's an active job, poll its status more aggressively.
  useEffect(() => {
    statusPollAbort.current?.abort();
    statusPollAbort.current = new AbortController();

    let cancelled = false;

    async function loop(jobId: string) {
      while (!cancelled) {
        try {
          const status = await api.getJobStatus(jobId);
          if (cancelled) return;
          setJobStatus(status);

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

    if (activeJob?.job_id) {
      loop(activeJob.job_id);
    } else {
      setJobStatus(null);
    }

    return () => {
      cancelled = true;
      statusPollAbort.current?.abort();
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
