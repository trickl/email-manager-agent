import type { ReactNode } from "react";

export default function SplitLayout(props: { left: ReactNode; right: ReactNode }) {
  return (
    <div
      style={{
        flex: 1,
        minHeight: 0,
        display: "grid",
        gridTemplateColumns: "360px 1fr",
        borderTop: "1px solid #e5e7eb",
      }}
    >
      <div style={{ borderRight: "1px solid #e5e7eb", minHeight: 0 }}>{props.left}</div>
      <div style={{ minHeight: 0 }}>{props.right}</div>
    </div>
  );
}
