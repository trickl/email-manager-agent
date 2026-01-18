import { ResponsiveSunburst } from "@nivo/sunburst";
import type { MouseEvent } from "react";
import type { DashboardNode } from "../api/types";
import { usefulnessColor } from "../utils/colors";

type SunburstDatum = {
  id: string;
  name: string;
  value: number;
  unread_ratio: number;
  frequency?: string | null;
  children?: SunburstDatum[];
};

function toDatum(n: DashboardNode): SunburstDatum {
  return {
    id: n.id,
    name: n.name,
    value: Math.max(0, n.count),
    unread_ratio: n.unread_ratio,
    frequency: n.frequency ?? null,
    children: (n.children ?? []).map(toDatum),
  };
}

export default function SunburstPanel(props: {
  root: DashboardNode | null;
  breadcrumb: DashboardNode[];
  onSelectNode: (id: string) => void;
  onBackToRoot: () => void;
}) {
  const root = props.root;

  return (
    <div style={{ flex: 1, minHeight: 0, display: "flex", flexDirection: "column" }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline" }}>
        <h2 style={{ margin: 0, fontSize: 14, letterSpacing: 0.3, color: "#374151" }}>
          Sunburst
        </h2>

        <div style={{ fontSize: 12, color: "#6b7280" }}>
          <button
            onClick={props.onBackToRoot}
            style={{
              border: "1px solid #e5e7eb",
              background: "white",
              padding: "4px 8px",
              borderRadius: 10,
              cursor: "pointer",
              marginRight: 8,
            }}
          >
            Back to root
          </button>
          {props.breadcrumb.map((b, idx) => (
            <span key={b.id}>
              {idx > 0 ? " / " : ""}
              <a
                href="#"
                onClick={(e: MouseEvent<HTMLAnchorElement>) => {
                  e.preventDefault();
                  props.onSelectNode(b.id);
                }}
                style={{ color: "#2563eb", textDecoration: "none" }}
              >
                {b.name}
              </a>
            </span>
          ))}
        </div>
      </div>

      <div
        style={{
          marginTop: 10,
          flex: 1,
          minHeight: 0,
          border: "1px solid #e5e7eb",
          borderRadius: 12,
          overflow: "hidden",
          background: "white",
        }}
      >
        {!root ? (
          <div style={{ padding: 12, color: "#6b7280" }}>No data.</div>
        ) : (
          <ResponsiveSunburst
            data={toDatum(root)}
            id="id"
            value="value"
            margin={{ top: 10, right: 10, bottom: 10, left: 10 }}
            cornerRadius={2}
            borderWidth={1}
            borderColor={{ from: "color", modifiers: [["darker", 0.8]] }}
            colors={(d: any) => usefulnessColor((d.data as any).unread_ratio)}
            childColor={{ from: "color", modifiers: [["brighter", 0.15]] }}
            enableArcLabels={false}
            tooltip={({ id, value, data }: any) => {
              const unreadRatio = (data as any).unread_ratio as number;
              const unreadPct = Math.round(unreadRatio * 100);
              const freq = (data as any).frequency as string | null;
              return (
                <div
                  style={{
                    padding: 10,
                    background: "white",
                    border: "1px solid #e5e7eb",
                    borderRadius: 10,
                    boxShadow: "0 8px 24px rgba(0,0,0,0.12)",
                    maxWidth: 360,
                  }}
                >
                  <div style={{ fontWeight: 800, marginBottom: 6 }}>{String(id)}</div>
                  <div style={{ fontSize: 12, color: "#374151" }}>{value} messages</div>
                  <div style={{ fontSize: 12, color: "#6b7280" }}>{unreadPct}% unread</div>
                  {freq && <div style={{ fontSize: 12, color: "#6b7280" }}>freq: {freq}</div>}
                </div>
              );
            }}
            onClick={(node) => {
              const id = String(node.id);
              props.onSelectNode(id);
            }}
            transitionMode="pushIn"
          />
        )}
      </div>

      <div style={{ marginTop: 10, fontSize: 12, color: "#6b7280" }}>
        Tip: click a segment to drill into that node.
      </div>
    </div>
  );
}
