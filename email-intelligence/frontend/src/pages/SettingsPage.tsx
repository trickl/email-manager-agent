import Alert from "@mui/material/Alert";
import Box from "@mui/material/Box";
import Button from "@mui/material/Button";
import Dialog from "@mui/material/Dialog";
import DialogActions from "@mui/material/DialogActions";
import DialogContent from "@mui/material/DialogContent";
import DialogTitle from "@mui/material/DialogTitle";
import FormControl from "@mui/material/FormControl";
import FormControlLabel from "@mui/material/FormControlLabel";
import InputLabel from "@mui/material/InputLabel";
import Link from "@mui/material/Link";
import MenuItem from "@mui/material/MenuItem";
import Paper from "@mui/material/Paper";
import Select from "@mui/material/Select";
import Stack from "@mui/material/Stack";
import Switch from "@mui/material/Switch";
import Table from "@mui/material/Table";
import TableBody from "@mui/material/TableBody";
import TableCell from "@mui/material/TableCell";
import TableHead from "@mui/material/TableHead";
import TableRow from "@mui/material/TableRow";
import TextField from "@mui/material/TextField";
import Typography from "@mui/material/Typography";
import { Link as RouterLink } from "react-router-dom";
import { useEffect, useMemo, useState } from "react";

import { api } from "../api/client";
import type { RetentionPreviewResponse, TaxonomyLabel } from "../api/types";
import TopBar from "../ui/TopBar";
import { useJobPolling } from "../ui/useJobPolling";

type LabelFormState = {
  name: string;
  description: string;
  parent_id: number | null;
  retention_days: string;
  is_active: boolean;
};

function toFormState(l?: TaxonomyLabel): LabelFormState {
  return {
    name: l?.name ?? "",
    description: l?.description ?? "",
    parent_id: (l?.parent_id as any) ?? null,
    retention_days: l?.retention_days != null ? String(l.retention_days) : "",
    is_active: l?.is_active ?? true,
  };
}

export default function SettingsPage() {
  const { jobStatus, startJob, activeJob } = useJobPolling();
  const disabled = activeJob?.state === "running" || activeJob?.state === "queued";

  const [labels, setLabels] = useState<TaxonomyLabel[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const [createOpen, setCreateOpen] = useState(false);
  const [editOpen, setEditOpen] = useState(false);
  const [activeLabel, setActiveLabel] = useState<TaxonomyLabel | null>(null);
  const [form, setForm] = useState<LabelFormState>(toFormState());

  const [retentionPreview, setRetentionPreview] = useState<RetentionPreviewResponse | null>(null);

  const parents = useMemo(
    () => labels.filter((l) => l.level === 1).sort((a, b) => a.name.localeCompare(b.name)),
    [labels]
  );

  async function refresh() {
    setLoading(true);
    setError(null);
    try {
      const data = await api.getTaxonomy();
      setLabels(data);
    } catch (e: any) {
      setError(e?.message ?? "Failed to load taxonomy");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    refresh();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  async function onCreate() {
    setError(null);
    try {
      const retention_days = form.retention_days.trim() ? Number(form.retention_days.trim()) : null;
      await api.createTaxonomyLabel({
        name: form.name,
        description: form.description,
        parent_id: form.parent_id,
        retention_days,
        is_active: form.is_active,
      });
      setCreateOpen(false);
      await refresh();
    } catch (e: any) {
      setError(e?.bodyText ?? e?.message ?? "Create failed");
    }
  }

  async function onUpdate() {
    if (!activeLabel) return;
    setError(null);
    try {
      const retention_days = form.retention_days.trim() ? Number(form.retention_days.trim()) : null;
      await api.updateTaxonomyLabel(activeLabel.id, {
        name: form.name,
        description: form.description,
        retention_days,
        is_active: form.is_active,
      });
      setEditOpen(false);
      setActiveLabel(null);
      await refresh();
    } catch (e: any) {
      setError(e?.bodyText ?? e?.message ?? "Update failed");
    }
  }

  async function onDelete(l: TaxonomyLabel) {
    const ok = window.confirm(`Delete taxonomy label "${l.name}"? This cannot be undone.`);
    if (!ok) return;

    setError(null);
    try {
      await api.deleteTaxonomyLabel(l.id);
      await refresh();
    } catch (e: any) {
      setError(e?.bodyText ?? e?.message ?? "Delete failed");
    }
  }

  async function onSyncLabels() {
    setError(null);
    try {
      await api.syncGmailLabelExistence(false);
      await refresh();
    } catch (e: any) {
      setError(e?.bodyText ?? e?.message ?? "Gmail label sync failed");
    }
  }

  async function onPushIncremental() {
    setError(null);
    try {
      await api.pushGmailLabelsIncremental(200);
    } catch (e: any) {
      setError(e?.bodyText ?? e?.message ?? "Incremental push failed");
    }
  }

  async function onPushBulk() {
    setError(null);
    try {
      await api.pushGmailLabelsBulk(200, 0);
    } catch (e: any) {
      setError(e?.bodyText ?? e?.message ?? "Bulk push failed");
    }
  }

  async function onPushBulkJob() {
    const ok = window.confirm(
      "Start a background Gmail bulk push job now? You can watch progress in the top bar (percent + ETA)."
    );
    if (!ok) return;

    setError(null);
    try {
      await startJob("gmail_push_bulk");
    } catch (e: any) {
      setError(e?.bodyText ?? e?.message ?? "Failed to start Gmail push job");
    }
  }

  async function onRetentionPreview() {
    setError(null);
    try {
      const resp = await api.retentionPreview(25);
      setRetentionPreview(resp);
    } catch (e: any) {
      setError(e?.bodyText ?? e?.message ?? "Retention preview failed");
    }
  }

  async function onRetentionRun() {
    const ok = window.confirm(
      "Run retention archive sweep now? This removes INBOX and adds an archive marker label."
    );
    if (!ok) return;

    setError(null);
    try {
      await api.retentionRun(500, false);
      await refresh();
    } catch (e: any) {
      setError(e?.bodyText ?? e?.message ?? "Retention run failed");
    }
  }

  return (
    <Box sx={{ height: "100vh", display: "flex", flexDirection: "column" }}>
      <TopBar
        title="Email Intelligence"
        jobStatus={jobStatus}
        onIngestFull={() => startJob("ingest_full")}
        onIngestRefresh={() => startJob("ingest_refresh")}
        onClusterLabel={() => startJob("cluster_label")}
        disabled={disabled}
      />
      <Box sx={{ p: 2 }}>
        <Stack direction={{ xs: "column", sm: "row" }} spacing={1} sx={{ mb: 2 }}>
          <Box sx={{ flex: 1 }}>
            <Typography variant="h6" sx={{ fontWeight: 900, mb: 0.25 }}>
              Taxonomy & Gmail Label Sync
            </Typography>
            <Typography variant="body2" sx={{ color: "text.secondary" }}>
              Manage taxonomy labels, sync them to Gmail, and run retention-based archiving.
            </Typography>
            <Typography variant="body2" sx={{ mt: 0.5 }}>
              <Link component={RouterLink} to="/" underline="hover">
                Back to dashboard
              </Link>
            </Typography>
          </Box>
          <Stack direction={{ xs: "column", sm: "row" }} spacing={1} alignItems="stretch">
            <Button variant="outlined" onClick={refresh} disabled={loading}>
              Refresh
            </Button>
            <Button variant="contained" onClick={() => (setForm(toFormState()), setCreateOpen(true))}>
              New label
            </Button>
          </Stack>
        </Stack>

        {error && (
          <Alert severity="error" sx={{ mb: 2, whiteSpace: "pre-wrap" }}>
            {error}
          </Alert>
        )}

        <Stack direction={{ xs: "column", sm: "row" }} spacing={1} sx={{ mb: 2 }}>
          <Button variant="outlined" onClick={onSyncLabels}>
            Sync Gmail label existence
          </Button>
          <Button variant="outlined" onClick={onPushIncremental}>
            Push labels (incremental)
          </Button>
          <Button variant="outlined" onClick={onPushBulk}>
            Push labels (bulk)
          </Button>
          <Button color="warning" variant="outlined" onClick={onPushBulkJob}>
            Push labels (bulk job)
          </Button>
          <Button variant="outlined" onClick={onRetentionPreview}>
            Retention preview
          </Button>
          <Button color="warning" variant="contained" onClick={onRetentionRun}>
            Run retention (archive)
          </Button>
        </Stack>

        {retentionPreview && (
          <Alert severity="info" sx={{ mb: 2 }}>
            Eligible for archive: <b>{retentionPreview.eligible_count}</b> (showing sample of{" "}
            {retentionPreview.sample.length}).
          </Alert>
        )}

        <Paper variant="outlined" sx={{ overflow: "auto" }}>
          <Table size="small" stickyHeader>
            <TableHead>
              <TableRow>
                <TableCell>Name</TableCell>
                <TableCell>Level</TableCell>
                <TableCell>Retention (days)</TableCell>
                <TableCell>Active</TableCell>
                <TableCell>Assigned</TableCell>
                <TableCell>Gmail label</TableCell>
                <TableCell>Sync</TableCell>
                <TableCell align="right">Actions</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {labels.map((l) => (
                <TableRow key={l.id} hover>
                  <TableCell sx={{ minWidth: 240 }}>
                    <Typography variant="body2" sx={{ fontWeight: 700 }}>
                      {l.name}
                    </Typography>
                    <Typography variant="caption" sx={{ color: "text.secondary" }}>
                      {l.slug}
                    </Typography>
                  </TableCell>
                  <TableCell>{l.level}</TableCell>
                  <TableCell>{l.retention_days ?? "—"}</TableCell>
                  <TableCell>{l.is_active ? "Yes" : "No"}</TableCell>
                  <TableCell>{l.assigned_message_count}</TableCell>
                  <TableCell sx={{ minWidth: 260 }}>
                    <Typography variant="body2">{l.gmail_label_name}</Typography>
                    <Typography variant="caption" sx={{ color: "text.secondary" }}>
                      {l.gmail_label_id ?? "(not linked)"}
                    </Typography>
                  </TableCell>
                  <TableCell sx={{ minWidth: 180 }}>
                    <Typography variant="body2">{l.sync_status ?? "—"}</Typography>
                    {l.sync_error && (
                      <Typography variant="caption" sx={{ color: "error.main" }}>
                        {l.sync_error}
                      </Typography>
                    )}
                  </TableCell>
                  <TableCell align="right">
                    <Stack direction="row" spacing={1} justifyContent="flex-end">
                      <Button
                        size="small"
                        variant="outlined"
                        onClick={() => {
                          setActiveLabel(l);
                          setForm(toFormState(l));
                          setEditOpen(true);
                        }}
                      >
                        Edit
                      </Button>
                      <Button size="small" color="error" variant="outlined" onClick={() => onDelete(l)}>
                        Delete
                      </Button>
                    </Stack>
                  </TableCell>
                </TableRow>
              ))}
              {labels.length === 0 && (
                <TableRow>
                  <TableCell colSpan={8}>
                    <Typography variant="body2" sx={{ color: "text.secondary" }}>
                      {loading ? "Loading…" : "No taxonomy labels found."}
                    </Typography>
                  </TableCell>
                </TableRow>
              )}
            </TableBody>
          </Table>
        </Paper>
      </Box>

      <Dialog open={createOpen} onClose={() => setCreateOpen(false)} maxWidth="sm" fullWidth>
        <DialogTitle>New taxonomy label</DialogTitle>
        <DialogContent>
          <Stack spacing={2} sx={{ mt: 1 }}>
            <TextField
              label="Name"
              value={form.name}
              onChange={(e) => setForm((f) => ({ ...f, name: e.target.value }))}
              fullWidth
            />
            <TextField
              label="Description"
              value={form.description}
              onChange={(e) => setForm((f) => ({ ...f, description: e.target.value }))}
              fullWidth
              multiline
              minRows={2}
            />
            <FormControl fullWidth>
              <InputLabel>Parent (optional)</InputLabel>
              <Select
                label="Parent (optional)"
                value={form.parent_id ?? ""}
                onChange={(e) =>
                  setForm((f) => ({ ...f, parent_id: e.target.value ? Number(e.target.value) : null }))
                }
              >
                <MenuItem value="">
                  <em>None (Tier-1)</em>
                </MenuItem>
                {parents.map((p) => (
                  <MenuItem key={p.id} value={p.id}>
                    {p.name}
                  </MenuItem>
                ))}
              </Select>
            </FormControl>
            <TextField
              label="Retention days (optional)"
              value={form.retention_days}
              onChange={(e) => setForm((f) => ({ ...f, retention_days: e.target.value }))}
              fullWidth
              helperText="If set, messages assigned this label become eligible for archive after N days."
            />
            <FormControlLabel
              control={
                <Switch
                  checked={form.is_active}
                  onChange={(e) => setForm((f) => ({ ...f, is_active: e.target.checked }))}
                />
              }
              label="Active"
            />
          </Stack>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setCreateOpen(false)}>Cancel</Button>
          <Button variant="contained" onClick={onCreate} disabled={!form.name.trim()}>
            Create
          </Button>
        </DialogActions>
      </Dialog>

      <Dialog open={editOpen} onClose={() => setEditOpen(false)} maxWidth="sm" fullWidth>
        <DialogTitle>Edit taxonomy label</DialogTitle>
        <DialogContent>
          <Stack spacing={2} sx={{ mt: 1 }}>
            <TextField
              label="Name"
              value={form.name}
              onChange={(e) => setForm((f) => ({ ...f, name: e.target.value }))}
              fullWidth
            />
            <TextField
              label="Description"
              value={form.description}
              onChange={(e) => setForm((f) => ({ ...f, description: e.target.value }))}
              fullWidth
              multiline
              minRows={2}
            />
            <TextField
              label="Retention days (optional)"
              value={form.retention_days}
              onChange={(e) => setForm((f) => ({ ...f, retention_days: e.target.value }))}
              fullWidth
            />
            <FormControlLabel
              control={
                <Switch
                  checked={form.is_active}
                  onChange={(e) => setForm((f) => ({ ...f, is_active: e.target.checked }))}
                />
              }
              label="Active"
            />
          </Stack>
        </DialogContent>
        <DialogActions>
          <Button
            onClick={() => {
              setEditOpen(false);
              setActiveLabel(null);
            }}
          >
            Cancel
          </Button>
          <Button variant="contained" onClick={onUpdate} disabled={!form.name.trim()}>
            Save
          </Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
}
