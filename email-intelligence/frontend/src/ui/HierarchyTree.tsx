import type { MouseEvent } from "react";
import { useMemo, useState } from "react";
import ChevronRightIcon from "@mui/icons-material/ChevronRight";
import ExpandMoreIcon from "@mui/icons-material/ExpandMore";
import Box from "@mui/material/Box";
import ButtonBase from "@mui/material/ButtonBase";
import Chip from "@mui/material/Chip";
import IconButton from "@mui/material/IconButton";
import Typography from "@mui/material/Typography";
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
    const rows: Array<{ node: DashboardNode; depth: number; isPending: boolean }> = [];

    function walk(node: DashboardNode, depth: number, parentPending: boolean) {
      const isPending = parentPending || node.name === "Pending labelling";
      rows.push({ node, depth, isPending });
      if (!expanded.has(node.id)) return;
      for (const child of node.children ?? []) walk(child, depth + 1, isPending);
    }

    walk(props.root, 0, false);
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
    <Box sx={{ mt: 1.25 }}>
      {flattened.map(
        ({ node, depth, isPending }: { node: DashboardNode; depth: number; isPending: boolean }) => {
        const isSelected = node.id === props.selectedId;
        const hasChildren = (node.children?.length ?? 0) > 0;
        const isExpanded = expanded.has(node.id);

        // The "Pending labelling" branch is unprocessed: unread ratio is informative, but
        // it's misleading to treat it as a usefulness/value proxy.
        const color = isPending ? "#9ca3af" : props.colorForNode(node);
        const subtitle = isPending ? "Unknown" : props.subtitleForNode?.(node);
        const badge = isPending ? "Pending" : props.badgeForNode?.(node);

        return (
          <ButtonBase
            key={node.id}
            onClick={() => props.onSelect(node.id)}
            component="div"
            role="button"
            tabIndex={0}
            onKeyDown={(e) => {
              // Keep the row keyboard-accessible even though it's not a native <button>.
              if (e.key === "Enter" || e.key === " ") {
                e.preventDefault();
                props.onSelect(node.id);
              }
            }}
            sx={{
              width: "100%",
              justifyContent: "flex-start",
              textAlign: "left",
              borderRadius: 2,
              px: 0.75,
              py: 0.75,
              // IMPORTANT: do NOT indent via margin-left while also setting width: 100%.
              // That pattern can make the element wider than its container and introduce
              // a horizontal scrollbar for the whole dashboard.
              pl: `calc(6px + ${depth * 14}px)`,
              display: "flex",
              alignItems: "center",
              gap: 1,
              bgcolor: isSelected ? "action.selected" : "transparent",
              userSelect: "none",
            }}
          >
            <IconButton
              size="small"
              onClick={(e: MouseEvent<HTMLButtonElement>) => {
                e.stopPropagation();
                if (hasChildren) toggle(node.id);
              }}
              disabled={!hasChildren}
              aria-label={hasChildren ? (isExpanded ? "Collapse" : "Expand") : "No children"}
              sx={{
                width: 28,
                height: 28,
                borderRadius: 1.5,
                border: 1,
                borderColor: "divider",
              }}
            >
              {hasChildren ? (isExpanded ? <ExpandMoreIcon /> : <ChevronRightIcon />) : <span />}
            </IconButton>

            <Box
              sx={{
                width: 10,
                height: 10,
                borderRadius: 999,
                bgcolor: color,
                border: "1px solid",
                borderColor: "divider",
                flexShrink: 0,
              }}
              title={subtitle ?? ""}
            />

            <Box sx={{ minWidth: 0, flex: 1 }}>
              <Box
                sx={{
                  display: "flex",
                  justifyContent: "space-between",
                  gap: 1,
                  alignItems: "baseline",
                }}
              >
                <Typography
                  variant="body2"
                  sx={{
                    fontWeight: isSelected ? 900 : 700,
                    overflow: "hidden",
                    textOverflow: "ellipsis",
                    whiteSpace: "nowrap",
                  }}
                  title={node.name}
                >
                  {node.name}
                </Typography>

                <Box sx={{ display: "flex", gap: 0.75, alignItems: "center", flexShrink: 0 }}>
                  {badge && <Chip size="small" label={badge} variant="outlined" />}
                  <Chip
                    size="small"
                    label={node.count}
                    variant="outlined"
                    title={`${node.count} messages`}
                    sx={{ fontVariantNumeric: "tabular-nums", fontWeight: 800 }}
                  />
                </Box>
              </Box>

              {subtitle && (
                <Typography
                  variant="caption"
                  sx={{ display: "block", color: "text.secondary", mt: 0.25 }}
                >
                  {subtitle}
                </Typography>
              )}
            </Box>
          </ButtonBase>
        );
      }
      )}
    </Box>
  );
}
