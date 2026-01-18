import type { DashboardNode } from "../api/types";
import { unreadBucketText, usefulnessBandLabel, usefulnessColor } from "../utils/colors";

export default function DetailPanel(props: { node: DashboardNode | null }) {
  const n = props.node;
  if (!n) return null;

  const unreadPct = Math.round(n.unread_ratio * 100);
  const usefulLabel = usefulnessBandLabel(n.unread_ratio);
  const usefulColor = usefulnessColor(n.unread_ratio);

  return (
    <div
      style={{
        border: "1px solid #e5e7eb",
        borderRadius: 12,
        padding: 12,
        background: "white",
      }}
    >
      <div style={{ display: "flex", justifyContent: "space-between", gap: 12 }}>
        <div style={{ minWidth: 0 }}>
          <div
            style={{
              fontSize: 12,
              color: "#6b7280",
              letterSpacing: 0.3,
              textTransform: "uppercase",
            }}
          >
            Selection
          </div>
          <div
            style={{
              fontSize: 16,
              fontWeight: 800,
              color: "#111827",
              overflow: "hidden",
              textOverflow: "ellipsis",
              whiteSpace: "nowrap",
            }}
            title={n.name}
          >
            {n.name}
          </div>
        </div>

        <div style={{ display: "flex", alignItems: "center", gap: 10, flexShrink: 0 }}>
          <div
            style={{
              width: 12,
              height: 12,
              borderRadius: 999,
              background: usefulColor,
              border: "1px solid rgba(17,24,39,0.15)",
            }}
            title={usefulLabel}
          />
          <div style={{ fontSize: 12, color: "#374151", fontWeight: 700 }}>{usefulLabel}</div>
        </div>
      </div>

      <div
        style={{
          marginTop: 10,
          display: "grid",
          gridTemplateColumns: "repeat(4, minmax(0, 1fr))",
          gap: 10,
        }}
      >
        <Metric label="Messages" value={`${n.count}`} />
        <Metric label="Unread" value={`${n.unread_count}`} />
        <Metric label="Unread %" value={`${unreadPct}%`} />
        <Metric label="Children" value={`${n.children?.length ?? 0}`} />
      </div>

      {n.frequency && (
        <div style={{ marginTop: 10 }}>
          <Metric label="Frequency" value={n.frequency} />
        </div>
      )}

      <div style={{ marginTop: 10, fontSize: 12, color: "#6b7280" }}>
        Unread: {unreadBucketText(n.unread_ratio)} ({unreadPct}%). This uses unread ratio as a proxy
        for “usefulness”.
      </div>

      <details style={{ marginTop: 10 }}>
        <summary style={{ cursor: "pointer", color: "#374151", fontWeight: 600 }}>
          Raw node
        </summary>
        <pre
          style={{
            marginTop: 8,
            background: "#111827",
            color: "#e5e7eb",
            padding: 10,
            borderRadius: 10,
            overflow: "auto",
            maxHeight: 240,
          }}
        >
          {JSON.stringify(n, null, 2)}
        </pre>
      </details>
    </div>
  );
}

function Metric(props: { label: string; value: string }) {
  return (
    <div
      style={{
        border: "1px solid #e5e7eb",
        background: "#f9fafb",
        borderRadius: 12,
        padding: 10,
      }}
    >
      <div style={{ fontSize: 11, color: "#6b7280", textTransform: "uppercase" }}>
        {props.label}
      </div>
      <div style={{ fontSize: 16, fontWeight: 800, color: "#111827" }}>{props.value}</div>
    </div>
  );
}
