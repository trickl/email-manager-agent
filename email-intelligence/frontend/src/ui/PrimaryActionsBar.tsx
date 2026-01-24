import ArchiveOutlinedIcon from "@mui/icons-material/ArchiveOutlined";
import CallMadeOutlinedIcon from "@mui/icons-material/CallMadeOutlined";
import CallReceivedOutlinedIcon from "@mui/icons-material/CallReceivedOutlined";
import DeleteOutlineOutlinedIcon from "@mui/icons-material/DeleteOutlineOutlined";
import MoreVertIcon from "@mui/icons-material/MoreVert";
import Alert from "@mui/material/Alert";
import Box from "@mui/material/Box";
import Button from "@mui/material/Button";
import Dialog from "@mui/material/Dialog";
import DialogActions from "@mui/material/DialogActions";
import DialogContent from "@mui/material/DialogContent";
import DialogTitle from "@mui/material/DialogTitle";
import Menu from "@mui/material/Menu";
import MenuItem from "@mui/material/MenuItem";
import Paper from "@mui/material/Paper";
import Stack from "@mui/material/Stack";
import Typography from "@mui/material/Typography";
import type { MouseEvent } from "react";
import { useMemo, useState } from "react";

import { api, ApiError } from "../api/client";
import type { JobType } from "./useJobPolling";

type ActionKey = "maintenance" | "ingest" | "label" | "archive" | "delete";

export default function PrimaryActionsBar(props: {
  disabled: boolean;
  startJob: (t: JobType) => Promise<void>;
}): JSX.Element {
  const [confirmOpen, setConfirmOpen] = useState(false);
  const [activeAction, setActiveAction] = useState<ActionKey | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [menuAnchorEl, setMenuAnchorEl] = useState<null | HTMLElement>(null);
  const menuOpen = Boolean(menuAnchorEl);

  const actions = useMemo(() => {
    return {
      maintenance: {
        title: "Run maintenance",
        icon: <ArchiveOutlinedIcon fontSize="small" />,
        buttonColor: "primary" as const,
        description: (
          <>
            <Typography variant="body2" sx={{ mb: 1 }}>
              Runs the full incremental maintenance pipeline: ingest, auto-label, label push,
              retention plan + archive push, inbox aging, and event/payment extraction.
            </Typography>
            <Typography variant="body2">
              This is safe and incremental. You can track progress in the header.
            </Typography>
          </>
        ),
        confirmLabel: "Run maintenance",
      },
      ingest: {
        title: "Ingest",
        icon: <CallReceivedOutlinedIcon fontSize="small" />,
        buttonColor: "primary" as const,
        description: (
          <>
            <Typography variant="body2" sx={{ mb: 1 }}>
              Pulls email metadata from Gmail into the dashboard (no message bodies).
            </Typography>
            <Typography variant="body2" sx={{ mb: 1 }}>
              We automatically choose the right mode:
            </Typography>
            <Box component="ul" sx={{ m: 0, pl: 2 }}>
              <li>
                <Typography variant="body2">
                  If this is your first run (no data yet): do a <b>full ingest</b>.
                </Typography>
              </li>
              <li>
                <Typography variant="body2">
                  Otherwise: do a <b>refresh ingest</b> from the last checkpoint.
                </Typography>
              </li>
            </Box>
          </>
        ),
        confirmLabel: "Ingest now",
      },
      label: {
        title: "Label",
        icon: <CallMadeOutlinedIcon fontSize="small" />,
        buttonColor: "secondary" as const,
        description: (
          <>
            <Typography variant="body2" sx={{ mb: 1 }}>
              Automatically assigns taxonomy labels to unlabelled messages.
            </Typography>
            <Typography variant="body2">
              This runs in the background; you can watch progress in the header.
            </Typography>
          </>
        ),
        confirmLabel: "Label now",
      },
      archive: {
        title: "Archive",
        icon: <ArchiveOutlinedIcon fontSize="small" />,
        buttonColor: "warning" as const,
        description: (
          <>
            <Typography variant="body2" sx={{ mb: 1 }}>
              Applies your retention policy to plan which messages should be archived, then applies the
              archive marker label in Gmail.
            </Typography>
            <Typography variant="body2">
              This is designed to be safe and reversible (it does not permanently delete mail).
            </Typography>
          </>
        ),
        confirmLabel: "Archive now",
      },
      delete: {
        title: "Delete",
        icon: <DeleteOutlineOutlinedIcon fontSize="small" />,
        buttonColor: "error" as const,
        description: (
          <>
            <Typography variant="body2" sx={{ mb: 1 }}>
              Moves messages that already have the archive marker label to Gmail Trash.
            </Typography>
            <Typography variant="body2" sx={{ mb: 1 }}>
              Gmail will auto-delete Trash after ~30 days (unless you empty Trash sooner).
            </Typography>
            <Alert severity="warning" sx={{ mt: 1 }}>
              This only affects messages marked for archive ("Email Archive").
            </Alert>
          </>
        ),
        confirmLabel: "Move to Trash",
      },
    };
  }, []);

  function openConfirm(which: ActionKey) {
    setError(null);
    setActiveAction(which);
    setConfirmOpen(true);
  }

  function openMenu(event: MouseEvent<HTMLElement>) {
    setMenuAnchorEl(event.currentTarget);
  }

  function closeMenu() {
    setMenuAnchorEl(null);
  }

  async function runConfirmed() {
    if (!activeAction) return;
    setBusy(true);
    setError(null);

    try {
      if (activeAction === "maintenance") {
        await props.startJob("maintenance");
      } else if (activeAction === "ingest") {
        const status = await api.getStatus();
        const hasData = (status.total_email_count ?? 0) > 0;
        await props.startJob(hasData ? "ingest_refresh" : "ingest_full");
      } else if (activeAction === "label") {
        await props.startJob("label_auto");
      } else if (activeAction === "archive") {
        // Step 1: compute a worklist based on retention policy
        await api.retentionPlan(50_000);
        // Step 2: apply the archive marker label to Gmail in a monitored job
        await props.startJob("gmail_archive_push");
      } else if (activeAction === "delete") {
        await props.startJob("gmail_archive_trash");
      }

      setConfirmOpen(false);
      setActiveAction(null);
    } catch (e: any) {
      const msg = e instanceof ApiError ? e.bodyText || e.message : e?.message ?? String(e);
      setError(msg || "Action failed");
    } finally {
      setBusy(false);
    }
  }

  const active = activeAction ? actions[activeAction] : null;

  return (
    <Paper
      variant="outlined"
      sx={{
        p: 1.25,
        mb: 1.5,
        borderRadius: 2,
        background: "linear-gradient(180deg, rgba(0,0,0,0.02), rgba(0,0,0,0))",
      }}
    >
      <Stack
        direction={{ xs: "column", sm: "row" }}
        spacing={1}
        alignItems={{ xs: "stretch", sm: "center" }}
        justifyContent="space-between"
      >
        <Box sx={{ minWidth: 240 }}>
          <Typography variant="subtitle2" sx={{ fontWeight: 900 }}>
            Maintenance
          </Typography>
          <Typography variant="caption" sx={{ color: "text.secondary" }}>
            Run the full pipeline in one click. Other actions are in the menu.
          </Typography>
        </Box>

        <Stack direction={{ xs: "column", sm: "row" }} spacing={1} alignItems="stretch">
          <Button
            size="large"
            variant="contained"
            color={actions.maintenance.buttonColor}
            startIcon={actions.maintenance.icon}
            onClick={() => openConfirm("maintenance")}
            disabled={props.disabled}
            sx={{ fontWeight: 900, textTransform: "none", minWidth: 200 }}
          >
            {actions.maintenance.title}
          </Button>
          <Button
            size="large"
            variant="outlined"
            startIcon={<MoreVertIcon fontSize="small" />}
            onClick={openMenu}
            disabled={props.disabled}
            sx={{ fontWeight: 900, textTransform: "none" }}
          >
            More actions
          </Button>
        </Stack>
      </Stack>

      <Menu
        anchorEl={menuAnchorEl}
        open={menuOpen}
        onClose={closeMenu}
        anchorOrigin={{ vertical: "bottom", horizontal: "right" }}
        transformOrigin={{ vertical: "top", horizontal: "right" }}
      >
        <MenuItem
          onClick={() => {
            closeMenu();
            openConfirm("ingest");
          }}
        >
          {actions.ingest.title}
        </MenuItem>
        <MenuItem
          onClick={() => {
            closeMenu();
            openConfirm("label");
          }}
        >
          {actions.label.title}
        </MenuItem>
        <MenuItem
          onClick={() => {
            closeMenu();
            openConfirm("archive");
          }}
        >
          {actions.archive.title}
        </MenuItem>
        <MenuItem
          onClick={() => {
            closeMenu();
            openConfirm("delete");
          }}
        >
          {actions.delete.title}
        </MenuItem>
      </Menu>

      <Dialog open={confirmOpen} onClose={() => (busy ? null : setConfirmOpen(false))} maxWidth="sm" fullWidth>
        <DialogTitle sx={{ fontWeight: 900 }}>
          {active ? active.title : "Confirm"}
        </DialogTitle>
        <DialogContent>
          {error && (
            <Alert severity="error" sx={{ mb: 2, whiteSpace: "pre-wrap" }}>
              {error}
            </Alert>
          )}

          {active?.description}
        </DialogContent>
        <DialogActions>
          <Button
            onClick={() => {
              if (busy) return;
              setConfirmOpen(false);
              setActiveAction(null);
              setError(null);
            }}
          >
            Cancel
          </Button>
          <Button variant="contained" onClick={runConfirmed} disabled={busy || props.disabled}>
            {busy ? "Working…" : active?.confirmLabel ?? "Confirm"}
          </Button>
        </DialogActions>
      </Dialog>
    </Paper>
  );
}
