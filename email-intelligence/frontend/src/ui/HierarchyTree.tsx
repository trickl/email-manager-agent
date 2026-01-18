import type { MouseEvent } from "react";
import { useMemo, useState } from "react";
import type { DashboardNode } from "../api/types";

type ColorForNode = (n: DashboardNode) => string;

type Props = {
  root: DashboardNode;
  selectedId: string;
  onSelect: (id: string) => void;
  colorForNode: ColorForNode;
  badgeForNode?: (n: DashboardNode) => string;
  subtitleForNode?: (n: DashboardNode) => string;
};

export default function HierarchyTree(props: Props) {
  const [expanded, setExpanded] = useState<Set<string>>(() => new Set([props.root.id]));

  const flattened = useMemo(() => {
    const rows: Array<{ node: DashboardNode; depth: number }> = [];

    function walk(node: DashboardNode, depth: number) {
      rows.push({ node, depth });
      if (!expanded.has(node.id)) return;
      for (const child of node.children ?? []) walk(child, depth + 1);
    }

    walk(props.root, 0);
    return rows;
  }, [props.root, expanded]);

  function toggle(id: string) {
    setExpanded((prev: Set<string>) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  }

  return (
    <div style={{ marginTop: 10 }}>
      {flattened.map(({ node, depth }: { node: DashboardNode; depth: number }) => {
        const isSelected = node.id === props.selectedId;
        const hasChildren = (node.children?.length ?? 0) > 0;
        const isExpanded = expanded.has(node.id);
        const color = props.colorForNode(node);
        const subtitle = props.subtitleForNode?.(node);
        const badge = props.badgeForNode?.(node);

        return (
          <div
            key={node.id}
            style={{
              display: "flex",
              alignItems: "center",
              gap: 8,
              padding: "6px 6px",
              marginLeft: depth * 14,
              borderRadius: 10,
              background: isSelected ? "#eff6ff" : "transparent",
              cursor: "pointer",
              userSelect: "none",
            }}
            onClick={() => props.onSelect(node.id)}
          >
            <button
              onClick={(e: MouseEvent<HTMLButtonElement>) => {
                e.stopPropagation();
                if (hasChildren) toggle(node.id);
              }}
              disabled={!hasChildren}
              aria-label={hasChildren ? (isExpanded ? "Collapse" : "Expand") : "No children"}
              style={{
                width: 20,
                height: 20,
                borderRadius: 6,
                border: "1px solid #e5e7eb",
                background: hasChildren ? "white" : "#f3f4f6",
                color: "#374151",
                cursor: hasChildren ? "pointer" : "not-allowed",
                fontSize: 12,
                lineHeight: "18px",
              }}
              title={hasChildren ? (isExpanded ? "Collapse" : "Expand") : "No children"}
            >
              {hasChildren ? (isExpanded ? "▾" : "▸") : "·"}
            </button>

            <div
              style={{
                width: 10,
                height: 10,
                borderRadius: 999,
                background: color,
                border: "1px solid rgba(17,24,39,0.15)",
              }}
              title={subtitle ?? ""}
            />

            <div style={{ minWidth: 0, flex: 1 }}>
              <div
                style={{
                  display: "flex",
                  justifyContent: "space-between",
                  gap: 8,
                  alignItems: "baseline",
                }}
              >
                <div
                  style={{
                    fontWeight: isSelected ? 800 : 650,
                    color: "#111827",
                    overflow: "hidden",
                    textOverflow: "ellipsis",
                    whiteSpace: "nowrap",
                  }}
                  title={node.name}
                >
                  {node.name}
                </div>

                <div style={{ display: "flex", gap: 6, alignItems: "center", flexShrink: 0 }}>
                  {badge && (
                    <span
                      style={{
                        fontSize: 11,
                        color: "#6b7280",
                        background: "#f3f4f6",
                        padding: "2px 6px",
                        borderRadius: 999,
                      }}
                    >
                      {badge}
                    </span>
                  )}
                  <span
                    style={{
                      fontVariantNumeric: "tabular-nums",
                      fontSize: 12,
                      color: "#374151",
                      background: "#f9fafb",
                      border: "1px solid #e5e7eb",
                      padding: "2px 7px",
                      borderRadius: 999,
                    }}
                    title={`${node.count} messages`}
                  >
                    {node.count}
                  </span>
                </div>
              </div>
              {subtitle && (
                <div style={{ fontSize: 11, color: "#6b7280", marginTop: 1 }}>{subtitle}</div>
              )}
            </div>
          </div>
        );
      })}
    </div>
  );
}
