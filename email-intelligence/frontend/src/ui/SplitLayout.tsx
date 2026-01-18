import type { ReactNode } from "react";
import ChevronRightIcon from "@mui/icons-material/ChevronRight";
import Box from "@mui/material/Box";
import IconButton from "@mui/material/IconButton";
import Tooltip from "@mui/material/Tooltip";

export default function SplitLayout(props: {
  left: ReactNode;
  right: ReactNode;
  leftWidth?: number;
  collapsedLeftWidth?: number;
  leftCollapsed?: boolean;
  onToggleLeftCollapsed?: () => void;
  expandLeftLabel?: string;
}) {
  const leftWidth = props.leftWidth ?? 360;
  const collapsedLeftWidth = props.collapsedLeftWidth ?? 44;
  const leftCollapsed = props.leftCollapsed ?? false;
  const expandLeftLabel = props.expandLeftLabel ?? "Show sidebar";

  return (
    <Box
      sx={{
        flex: 1,
        minHeight: 0,
        minWidth: 0,
        display: "grid",
        gridTemplateColumns: `${leftCollapsed ? collapsedLeftWidth : leftWidth}px 1fr`,
        // Prevent grid children from forcing horizontal overflow due to intrinsic min-width.
        overflow: "hidden",
        transition: "grid-template-columns 150ms ease",
      }}
    >
      <Box sx={{ borderRight: 1, borderColor: "divider", minHeight: 0, minWidth: 0 }}>
        {leftCollapsed ? (
          <Box
            sx={{
              height: "100%",
              display: "flex",
              alignItems: "flex-start",
              justifyContent: "center",
              pt: 1,
            }}
          >
            {props.onToggleLeftCollapsed && (
              <Tooltip title={expandLeftLabel}>
                <IconButton
                  size="small"
                  onClick={props.onToggleLeftCollapsed}
                  aria-label={expandLeftLabel}
                  sx={{ border: 1, borderColor: "divider" }}
                >
                  <ChevronRightIcon fontSize="small" />
                </IconButton>
              </Tooltip>
            )}
          </Box>
        ) : (
          props.left
        )}
      </Box>
      <Box sx={{ minHeight: 0, minWidth: 0 }}>{props.right}</Box>
    </Box>
  );
}
