import type { CSSProperties } from "react";
import { Link, useLocation } from "react-router-dom";
import type { JobStatusResponse } from "../api/types";

function pillColor(state?: string): { bg: string; fg: string } {
  switch (state) {
    case "running":
      return { bg: "#dbeafe", fg: "#1d4ed8" };
    case "queued":
      return { bg: "#fef3c7", fg: "#92400e" };
    case "succeeded":
      return { bg: "#dcfce7", fg: "#166534" };
    case "failed":
      return { bg: "#fee2e2", fg: "#991b1b" };
    default:
      return { bg: "#f3f4f6", fg: "#374151" };
  }
}

export default function TopBar(props: {
  title: string;
  jobStatus: JobStatusResponse | null;
  disabled: boolean;
  onIngestFull: () => void;
  onIngestRefresh: () => void;
  onClusterLabel: () => void;
}) {
  const loc = useLocation();
  const status = props.jobStatus;
  const pill = pillColor(status?.state);

  const progressText = (() => {
    if (!status) return "idle";
    const p = status.progress;
    if (status.state === "running" || status.state === "queued") {
      if (p.total != null && p.total > 0) return `${p.processed}/${p.total}`;
      return `${p.processed}`;
    }
    return status.state;
  })();

  return (
    <div
      style={{
        padding: "10px 12px",
        display: "flex",
        alignItems: "center",
        justifyContent: "space-between",
        gap: 12,
      }}
    >
      <div style={{ display: "flex", alignItems: "center", gap: 12, minWidth: 0 }}>
        <div style={{ fontWeight: 800, letterSpacing: 0.2 }}>{props.title}</div>

        <nav style={{ display: "flex", gap: 10, fontSize: 13 }}>
          <Link
            to="/"
            style={{
              textDecoration: "none",
              color: loc.pathname === "/" ? "#111827" : "#6b7280",
              fontWeight: loc.pathname === "/" ? 700 : 500,
            }}
          >
            Dashboard
          </Link>
          <Link
            to="/jobs"
            style={{
              textDecoration: "none",
              color: loc.pathname.startsWith("/jobs") ? "#111827" : "#6b7280",
              fontWeight: loc.pathname.startsWith("/jobs") ? 700 : 500,
            }}
          >
            Jobs
          </Link>
          <Link
            to="/settings"
            style={{
              textDecoration: "none",
              color: loc.pathname.startsWith("/settings") ? "#111827" : "#6b7280",
              fontWeight: loc.pathname.startsWith("/settings") ? 700 : 500,
            }}
          >
            Settings
          </Link>
        </nav>

        <div
          style={{
            marginLeft: 8,
            padding: "3px 8px",
            background: pill.bg,
            color: pill.fg,
            borderRadius: 999,
            fontSize: 12,
            fontWeight: 700,
            display: "flex",
            gap: 6,
            alignItems: "center",
            whiteSpace: "nowrap",
          }}
          title={status ? `${status.type} (${status.state})` : "No active job"}
        >
          <span>{status ? status.type : "no job"}</span>
          <span style={{ opacity: 0.85 }}>•</span>
          <span>{progressText}</span>
        </div>

        {status?.message && (
          <div
            style={{
              fontSize: 12,
              color: "#6b7280",
              overflow: "hidden",
              textOverflow: "ellipsis",
              whiteSpace: "nowrap",
              maxWidth: 520,
            }}
            title={status.message}
          >
            {status.message}
          </div>
        )}
      </div>

      <div style={{ display: "flex", gap: 8, flexShrink: 0 }}>
        <button
          onClick={props.onIngestFull}
          disabled={props.disabled}
          style={buttonStyle(props.disabled)}
          title="Ingest metadata from Gmail (full)"
        >
          Ingest (Full)
        </button>
        <button
          onClick={props.onIngestRefresh}
          disabled={props.disabled}
          style={buttonStyle(props.disabled)}
          title="Ingest metadata since checkpoint"
        >
          Ingest (Refresh)
        </button>
        <button
          onClick={props.onClusterLabel}
          disabled={props.disabled}
          style={buttonStyle(props.disabled)}
          title="Cluster + label existing messages"
        >
          Cluster + Label
        </button>
      </div>
    </div>
  );
}

function buttonStyle(disabled: boolean): CSSProperties {
  return {
    border: "1px solid #e5e7eb",
    background: disabled ? "#f9fafb" : "white",
    padding: "7px 10px",
    borderRadius: 10,
    cursor: disabled ? "not-allowed" : "pointer",
    fontSize: 13,
    fontWeight: 600,
    color: disabled ? "#9ca3af" : "#111827",
  };
}
