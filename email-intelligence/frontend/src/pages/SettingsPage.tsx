import { Link } from "react-router-dom";
import TopBar from "../ui/TopBar";
import { useJobPolling } from "../ui/useJobPolling";

export default function SettingsPage() {
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
        <h2 style={{ marginTop: 0 }}>Settings</h2>
        <p style={{ color: "#6b7280" }}>
          Placeholder. Taxonomy configuration will live here later.
        </p>
        <p>
          <Link to="/">Back to dashboard</Link>
        </p>
      </div>
    </div>
  );
}
