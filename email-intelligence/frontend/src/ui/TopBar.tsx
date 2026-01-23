import DarkModeIcon from "@mui/icons-material/DarkMode";
import LightModeIcon from "@mui/icons-material/LightMode";
import AppBar from "@mui/material/AppBar";
import Box from "@mui/material/Box";
import Button from "@mui/material/Button";
import Chip from "@mui/material/Chip";
import IconButton from "@mui/material/IconButton";
import Tooltip from "@mui/material/Tooltip";
import Typography from "@mui/material/Typography";
import { Link as RouterLink, useLocation } from "react-router-dom";
import type { JobStatusResponse } from "../api/types";
import { useColorMode } from "../theme/AppThemeProvider";

function chipColor(
  state?: string
): "default" | "info" | "warning" | "success" | "error" {
  switch (state) {
    case "running":
      return "info";
    case "queued":
      return "warning";
    case "succeeded":
      return "success";
    case "failed":
      return "error";
    default:
      return "default";
  }
}

export default function TopBar(props: {
  title: string;
  jobStatus: JobStatusResponse | null;
}) {
  const loc = useLocation();
  const { mode, toggleMode } = useColorMode();
  const status = props.jobStatus;
  const chip = chipColor(status?.state);

  const progressText = (() => {
    if (!status) return "idle";
    const p = status.progress;
    if (status.state === "running" || status.state === "queued") {
      const parts: string[] = [];
      if (p.total != null && p.total > 0) {
        // Gmail / backend may provide only a rough total estimate. If it becomes inconsistent
        // (processed > total), avoid showing a misleading fraction/percent.
        if (p.processed <= p.total) {
          const pct = p.percent != null ? `${p.percent.toFixed(1)}%` : undefined;
          parts.push(`${p.processed}/${p.total}`);
          if (pct) parts.push(pct);
        } else {
          parts.push(`${p.processed} processed`);
          parts.push(`est ${p.total}`);
        }
      } else {
        parts.push(`${p.processed}`);
      }
      if (status.eta_hint) parts.push(status.eta_hint);
      return parts.join(" • ");
    }
    return status.state;
  })();

  return (
    <AppBar position="static" color="default" elevation={0}>
      <Box
        sx={{
          px: 1.5,
          py: 1,
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          gap: 1.5,
          borderBottom: 1,
          borderColor: "divider",
        }}
      >
        <Box sx={{ display: "flex", alignItems: "center", gap: 1.5, minWidth: 0 }}>
          <Typography variant="subtitle1" sx={{ fontWeight: 800, letterSpacing: 0.2 }}>
            {props.title}
          </Typography>

          <Box sx={{ display: "flex", gap: 0.5 }} component="nav">
            <Button
              component={RouterLink}
              to="/"
              size="small"
              sx={{
                textTransform: "none",
                fontWeight: loc.pathname === "/" ? 800 : 600,
                color: loc.pathname === "/" ? "text.primary" : "text.secondary",
              }}
            >
              Dashboard
            </Button>
            <Button
              component={RouterLink}
              to="/events"
              size="small"
              sx={{
                textTransform: "none",
                fontWeight: loc.pathname.startsWith("/events") ? 800 : 600,
                color: loc.pathname.startsWith("/events")
                  ? "text.primary"
                  : "text.secondary",
              }}
            >
              Events
            </Button>
            <Button
              component={RouterLink}
              to="/financials"
              size="small"
              sx={{
                textTransform: "none",
                fontWeight: loc.pathname.startsWith("/financials") ? 800 : 600,
                color: loc.pathname.startsWith("/financials")
                  ? "text.primary"
                  : "text.secondary",
              }}
            >
              Financials
            </Button>
            <Button
              component={RouterLink}
              to="/jobs"
              size="small"
              sx={{
                textTransform: "none",
                fontWeight: loc.pathname.startsWith("/jobs") ? 800 : 600,
                color: loc.pathname.startsWith("/jobs") ? "text.primary" : "text.secondary",
              }}
            >
              Jobs
            </Button>
            <Button
              component={RouterLink}
              to="/categories"
              size="small"
              sx={{
                textTransform: "none",
                fontWeight: loc.pathname.startsWith("/categories") ? 800 : 600,
                color: loc.pathname.startsWith("/categories")
                  ? "text.primary"
                  : "text.secondary",
              }}
            >
              Categories
            </Button>
          </Box>

          <Chip
            size="small"
            color={chip}
            label={`${status ? status.type : "no job"} • ${progressText}`}
            title={status ? `${status.type} (${status.state})` : "No active job"}
            sx={{ fontWeight: 800, ml: 0.5 }}
          />

          {status?.message && (
            <Typography
              variant="caption"
              sx={{
                color: "text.secondary",
                overflow: "hidden",
                textOverflow: "ellipsis",
                whiteSpace: "nowrap",
                maxWidth: 560,
              }}
              title={status.message}
            >
              {status.message}
            </Typography>
          )}
        </Box>

        <Box sx={{ display: "flex", alignItems: "center", gap: 1, flexShrink: 0 }}>
          <Tooltip title={`Switch to ${mode === "light" ? "dark" : "light"} theme`}>
            <IconButton
              onClick={toggleMode}
              edge="end"
              color="inherit"
              aria-label="toggle theme"
              sx={{ ml: 0.25 }}
            >
              {mode === "light" ? <DarkModeIcon /> : <LightModeIcon />}
            </IconButton>
          </Tooltip>
        </Box>
      </Box>
    </AppBar>
  );
}
