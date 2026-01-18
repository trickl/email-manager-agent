import { Link } from "react-router-dom";
import TopBar from "../ui/TopBar";
import { useJobPolling } from "../ui/useJobPolling";

export default function JobsPage() {
  const { jobStatus, startJob, activeJob } = useJobPolling();
  const disabled = activeJob?.state === "running" || activeJob?.state === "queued";

  return (
    <div style={{ height: "100vh", display: "flex", flexDirection: "column" }}>
      <TopBar
        title="Email Intelligence"
        jobStatus={jobStatus}
        onIngestFull={() => startJob("ingest_full")}
        onIngestRefresh={() => startJob("ingest_refresh")}
        onClusterLabel={() => startJob("cluster_label")}
        disabled={disabled}
      />
      <div style={{ padding: 16 }}>
        <h2 style={{ marginTop: 0 }}>Jobs</h2>
        <p style={{ color: "#6b7280" }}>
          This is a placeholder for job history. For now, the dashboard top bar shows the active job.
        </p>
        <p>
          <Link to="/">Back to dashboard</Link>
        </p>
        {jobStatus && (
          <pre
            style={{
              background: "#111827",
              color: "#e5e7eb",
              padding: 12,
              borderRadius: 8,
              overflow: "auto",
            }}
          >
            {JSON.stringify(jobStatus, null, 2)}
          </pre>
        )}
      </div>
    </div>
  );
}
