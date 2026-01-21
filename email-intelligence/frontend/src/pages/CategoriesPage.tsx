import ExpandLessIcon from "@mui/icons-material/ExpandLess";
import ExpandMoreIcon from "@mui/icons-material/ExpandMore";
import Alert from "@mui/material/Alert";
import Autocomplete from "@mui/material/Autocomplete";
import Box from "@mui/material/Box";
import Button from "@mui/material/Button";
import Dialog from "@mui/material/Dialog";
import DialogActions from "@mui/material/DialogActions";
import DialogContent from "@mui/material/DialogContent";
import DialogTitle from "@mui/material/DialogTitle";
import FormControl from "@mui/material/FormControl";
import FormControlLabel from "@mui/material/FormControlLabel";
import IconButton from "@mui/material/IconButton";
import InputLabel from "@mui/material/InputLabel";
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
import Snackbar from "@mui/material/Snackbar";
import { useEffect, useMemo, useRef, useState } from "react";

import { api } from "../api/client";
import type {
  PushResponse,
  RetentionPlanResponse,
  RetentionPreviewResponse,
  SyncExistenceResponse,
  TaxonomyLabel,
} from "../api/types";
import TopBar from "../ui/TopBar";
import { useJobPolling } from "../ui/useJobPolling";

type RetentionUnit = "days" | "weeks" | "months" | "years";

type RetentionMode = "inherit" | "custom";

type RetentionDraft = {
  value: string; // empty string means "unset"
  unit: RetentionUnit;
};

const RETENTION_QUICK_VALUES: number[] = [1, 2, 3, 4, 6, 12, 18, 24];

function daysToDraft(days?: number | null): RetentionDraft {
  if (days == null) return { value: "", unit: "days" };

  // Prefer the largest unit that divides evenly.
  if (days % 365 === 0) return { value: String(days / 365), unit: "years" };
  if (days % 30 === 0) return { value: String(days / 30), unit: "months" };
  if (days % 7 === 0) return { value: String(days / 7), unit: "weeks" };
  return { value: String(days), unit: "days" };
}

function draftToDays(d: RetentionDraft): number | null {
  const raw = d.value.trim();
  if (!raw) return null;
  const n = Number(raw);
  if (!Number.isFinite(n) || !Number.isInteger(n) || n <= 0) return NaN;

  const unitDays: Record<RetentionUnit, number> = {
    days: 1,
    weeks: 7,
    months: 30,
    years: 365,
  };
  return n * unitDays[d.unit];
}

function formatDays(days: number): string {
  const d = daysToDraft(days);
  if (!d.value) return `${days} days`;
  const n = Number(d.value);
  const unitLabel = n === 1 ? d.unit.slice(0, -1) : d.unit;
  return `${n} ${unitLabel}`;
}

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

type TreeRow = {
  type: "category";
  depth: number;
  label: TaxonomyLabel;
  parent?: TaxonomyLabel;
  isExpandable: boolean;
};

function buildTreeRows(
  labels: TaxonomyLabel[],
  expanded: Set<number>
): TreeRow[] {
  const byId = new Map<number, TaxonomyLabel>();
  for (const l of labels) byId.set(l.id, l);

  const childrenByParent = new Map<number, TaxonomyLabel[]>();
  for (const l of labels) {
    if (l.parent_id == null) continue;
    const pid = Number(l.parent_id);
    const arr = childrenByParent.get(pid) ?? [];
    arr.push(l);
    childrenByParent.set(pid, arr);
  }
  for (const arr of childrenByParent.values()) {
    arr.sort((a, b) => a.name.localeCompare(b.name));
  }

  const roots = labels
    .filter((l) => l.parent_id == null)
    .slice()
    .sort((a, b) => a.name.localeCompare(b.name));

  const out: TreeRow[] = [];
  for (const r of roots) {
    const children = childrenByParent.get(r.id) ?? [];
    const expandable = children.length > 0;
    out.push({ type: "category", depth: 0, label: r, isExpandable: expandable });
    if (expandable && expanded.has(r.id)) {
      for (const c of children) {
        out.push({
          type: "category",
          depth: 1,
          label: c,
          parent: r,
          isExpandable: false,
        });
      }
    }
  }
  return out;
}

export default function CategoriesPage() {
  const { jobStatus, startJob, activeJob } = useJobPolling();
  const disabled = activeJob?.state === "running" || activeJob?.state === "queued";
  const [actionBusy, setActionBusy] = useState(false);

  const [toast, setToast] = useState<{
    open: boolean;
    severity: "success" | "info" | "warning" | "error";
    message: string;
  }>({ open: false, severity: "info", message: "" });

  const showToast = (
    severity: "success" | "info" | "warning" | "error",
    message: string
  ) => setToast({ open: true, severity, message });

  const [lastSyncExistence, setLastSyncExistence] = useState<SyncExistenceResponse | null>(null);
  const [lastPush, setLastPush] = useState<{
    mode: "bulk" | "incremental";
    limit: number;
    offset?: number;
    resp: PushResponse;
  } | null>(null);

  const actionDisabled = disabled || actionBusy;

  const [labels, setLabels] = useState<TaxonomyLabel[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const [retentionDefaultDays, setRetentionDefaultDays] = useState<number>(365 * 2);
  const [retentionDefaultDraft, setRetentionDefaultDraft] = useState<RetentionDraft>(() =>
    daysToDraft(365 * 2)
  );

  const [retentionDraftById, setRetentionDraftById] = useState<Record<number, RetentionDraft>>({});
  const [retentionModeById, setRetentionModeById] = useState<Record<number, RetentionMode>>({});

  const [expandedTier1, setExpandedTier1] = useState<Set<number>>(() => new Set());

  const [createOpen, setCreateOpen] = useState(false);
  const [editOpen, setEditOpen] = useState(false);
  const [activeLabel, setActiveLabel] = useState<TaxonomyLabel | null>(null);
  const [form, setForm] = useState<LabelFormState>(toFormState());

  const [retentionPreview, setRetentionPreview] = useState<RetentionPreviewResponse | null>(null);
  const [retentionPlan, setRetentionPlan] = useState<RetentionPlanResponse | null>(null);

  const parents = useMemo(
    () => labels.filter((l) => l.level === 1).sort((a, b) => a.name.localeCompare(b.name)),
    [labels]
  );

  const childrenByParentId = useMemo(() => {
    const m = new Map<number, TaxonomyLabel[]>();
    for (const l of labels) {
      if (l.parent_id == null) continue;
      const pid = Number(l.parent_id);
      const arr = m.get(pid) ?? [];
      arr.push(l);
      m.set(pid, arr);
    }
    return m;
  }, [labels]);

  const directCountById = useMemo(() => {
    const m = new Map<number, number>();
    for (const l of labels) m.set(l.id, l.assigned_message_count ?? 0);
    return m;
  }, [labels]);

  const rollupAssignedCount = useMemo(() => {
    const m = new Map<number, number>();
    for (const l of labels) {
      const direct = directCountById.get(l.id) ?? 0;
      if (l.level !== 1) {
        m.set(l.id, direct);
        continue;
      }
      const kids = childrenByParentId.get(l.id) ?? [];
      const childSum = kids.reduce((acc, c) => acc + (directCountById.get(c.id) ?? 0), 0);
      m.set(l.id, direct + childSum);
    }
    return m;
  }, [childrenByParentId, directCountById, labels]);

  const treeRows = useMemo(() => buildTreeRows(labels, expandedTier1), [labels, expandedTier1]);

  async function refresh() {
    setLoading(true);
    setError(null);
    try {
      const [data, defaultResp] = await Promise.all([
        api.getTaxonomy(),
        api.getRetentionDefault(),
      ]);
      setLabels(data);

      const nextDefaultDays = defaultResp?.retention_default_days ?? 365 * 2;
      setRetentionDefaultDays(nextDefaultDays);
      setRetentionDefaultDraft(daysToDraft(nextDefaultDays));

      // Reset retention editor state to match server.
      setRetentionModeById(() => {
        const next: Record<number, RetentionMode> = {};
        for (const l of data) {
          // If unset in DB, we inherit. This is the default for Tier-2.
          next[l.id] = l.retention_days == null ? "inherit" : "custom";
        }
        return next;
      });

      // Reset retention editor state to match server.
      setRetentionDraftById(() => {
        const next: Record<number, RetentionDraft> = {};
        for (const l of data) next[l.id] = daysToDraft(l.retention_days);
        return next;
      });

      // If the user hasn't expanded anything yet, default to expanded so hierarchy is visible.
      setExpandedTier1((prev) => {
        if (prev.size > 0) return prev;
        const next = new Set<number>();
        for (const l of data) {
          if (l.level === 1) next.add(l.id);
        }
        return next;
      });
    } catch (e: any) {
      setError(e?.message ?? "Failed to load categories");
    } finally {
      setLoading(false);
    }
  }

  const localRetentionDaysById = useMemo(() => {
    // For UI display we want to use the *draft* value when mode is custom,
    // and NULL (inherit/default) when mode is inherit.
    const next: Record<number, number | null> = {};
    for (const l of labels) {
      const mode = retentionModeById[l.id] ?? (l.retention_days == null ? "inherit" : "custom");
      if (mode === "inherit") {
        next[l.id] = null;
        continue;
      }

      const draft = retentionDraftById[l.id] ?? daysToDraft(l.retention_days);
      next[l.id] = draftToDays(draft);
    }
    return next;
  }, [labels, retentionDraftById, retentionModeById]);

  function effectiveRetentionDays(l: TaxonomyLabel): number {
    // NOTE: This is for UI display and should reflect *unsaved edits* in the draft.
    const own = localRetentionDaysById[l.id];

    if (l.level === 1) {
      if (typeof own === "number" && Number.isFinite(own)) return own;
      return retentionDefaultDays;
    }

    if (typeof own === "number" && Number.isFinite(own)) return own;

    const parent = l.parent_id != null ? labels.find((p) => p.id === Number(l.parent_id)) : null;
    if (!parent) return retentionDefaultDays;
    return effectiveRetentionDays(parent);
  }

  const retentionChanges = useMemo(() => {
    const items: Array<{ id: number; retention_days: number | null }> = [];
    const invalidIds: number[] = [];

    for (const l of labels) {
      const mode = retentionModeById[l.id] ?? (l.retention_days == null ? "inherit" : "custom");

      if (mode === "inherit") {
        // Inherit/default means persist NULL.
        const current = l.retention_days ?? null;
        if (current !== null) items.push({ id: l.id, retention_days: null });
        continue;
      }

      const draft = retentionDraftById[l.id] ?? daysToDraft(l.retention_days);
      const draftDays = draftToDays(draft);
      if (Number.isNaN(draftDays as any)) {
        invalidIds.push(l.id);
        continue;
      }

      const current = l.retention_days ?? null;
      if ((draftDays ?? null) !== current) items.push({ id: l.id, retention_days: draftDays ?? null });
    }

    return { items, invalidIds };
  }, [labels, retentionDraftById, retentionModeById]);

  const [retentionSaving, setRetentionSaving] = useState(false);
  const [retentionSaveError, setRetentionSaveError] = useState<string | null>(null);
  const retentionSaveTokenRef = useRef(0);

  function applyRetentionItemsLocally(items: Array<{ id: number; retention_days: number | null }>) {
    const byId = new Map<number, number | null>();
    for (const it of items) byId.set(it.id, it.retention_days);

    setLabels((prev) =>
      prev.map((l) => {
        if (!byId.has(l.id)) return l;
        return { ...l, retention_days: byId.get(l.id) ?? null };
      })
    );
  }

  async function saveRetentionChangesNow() {
    if (retentionChanges.invalidIds.length > 0) return;
    if (retentionChanges.items.length === 0) return;

    const token = ++retentionSaveTokenRef.current;
    setRetentionSaveError(null);
    setRetentionSaving(true);
    try {
      await api.bulkUpdateTaxonomyRetention(retentionChanges.items);
      if (token !== retentionSaveTokenRef.current) return;
      applyRetentionItemsLocally(retentionChanges.items);
    } catch (e: any) {
      if (token !== retentionSaveTokenRef.current) return;
      const msg = e?.bodyText ?? e?.message ?? "Failed to save retention changes";
      setRetentionSaveError(msg);
      showToast("error", "Failed to save retention changes.");
    } finally {
      if (token === retentionSaveTokenRef.current) setRetentionSaving(false);
    }
  }

  const retentionDefaultDirty = useMemo(() => {
    const d = draftToDays(retentionDefaultDraft);
    if (d == null) return true; // default cannot be unset
    if (Number.isNaN(d as any)) return true;
    return d !== retentionDefaultDays;
  }, [retentionDefaultDraft, retentionDefaultDays]);

  const [retentionDefaultSaving, setRetentionDefaultSaving] = useState(false);
  const [retentionDefaultSaveError, setRetentionDefaultSaveError] = useState<string | null>(null);
  const retentionDefaultSaveTokenRef = useRef(0);
  const retentionDefaultDidInitRef = useRef(false);

  async function saveRetentionDefaultNow() {
    const d = draftToDays(retentionDefaultDraft);
    if (d == null || Number.isNaN(d as any)) return;

    const token = ++retentionDefaultSaveTokenRef.current;
    setRetentionDefaultSaveError(null);
    setRetentionDefaultSaving(true);
    try {
      const resp = await api.setRetentionDefault(d);
      if (token !== retentionDefaultSaveTokenRef.current) return;
      setRetentionDefaultDays(resp.retention_default_days);
      setRetentionDefaultDraft(daysToDraft(resp.retention_default_days));
    } catch (e: any) {
      if (token !== retentionDefaultSaveTokenRef.current) return;
      const msg = e?.bodyText ?? e?.message ?? "Failed to save default retention";
      setRetentionDefaultSaveError(msg);
      showToast("error", "Failed to save default retention.");
    } finally {
      if (token === retentionDefaultSaveTokenRef.current) setRetentionDefaultSaving(false);
    }
  }

  // Auto-save retention default (Tier 0).
  useEffect(() => {
    // Avoid saving on initial load.
    if (!retentionDefaultDidInitRef.current) {
      retentionDefaultDidInitRef.current = true;
      return;
    }

    // Don't auto-save if invalid or unchanged.
    if (!retentionDefaultDirty) return;

    const d = draftToDays(retentionDefaultDraft);
    if (d == null || Number.isNaN(d as any)) return;

    const token = ++retentionDefaultSaveTokenRef.current;
    setRetentionDefaultSaveError(null);

    const t = window.setTimeout(async () => {
      setRetentionDefaultSaving(true);
      try {
        const resp = await api.setRetentionDefault(d);
        if (token !== retentionDefaultSaveTokenRef.current) return;
        setRetentionDefaultDays(resp.retention_default_days);
        setRetentionDefaultDraft(daysToDraft(resp.retention_default_days));
      } catch (e: any) {
        if (token !== retentionDefaultSaveTokenRef.current) return;
        const msg = e?.bodyText ?? e?.message ?? "Failed to save default retention";
        setRetentionDefaultSaveError(msg);
        showToast("error", "Failed to auto-save default retention.");
      } finally {
        if (token === retentionDefaultSaveTokenRef.current) setRetentionDefaultSaving(false);
      }
    }, 650);

    return () => window.clearTimeout(t);
  }, [retentionDefaultDraft, retentionDefaultDirty]);

  // Auto-save per-category retention (Tier 1/2).
  useEffect(() => {
    // Clear error when there are no changes.
    if (retentionChanges.items.length === 0) {
      setRetentionSaveError(null);
      return;
    }

    // Don't auto-save while values are invalid.
    if (retentionChanges.invalidIds.length > 0) return;

    const token = ++retentionSaveTokenRef.current;
    setRetentionSaveError(null);

    const t = window.setTimeout(async () => {
      setRetentionSaving(true);
      try {
        await api.bulkUpdateTaxonomyRetention(retentionChanges.items);
        if (token !== retentionSaveTokenRef.current) return;
        applyRetentionItemsLocally(retentionChanges.items);
      } catch (e: any) {
        if (token !== retentionSaveTokenRef.current) return;
        const msg = e?.bodyText ?? e?.message ?? "Failed to save retention changes";
        setRetentionSaveError(msg);
        showToast("error", "Failed to auto-save retention changes.");
      } finally {
        if (token === retentionSaveTokenRef.current) setRetentionSaving(false);
      }
    }, 650);

    return () => window.clearTimeout(t);
  }, [retentionChanges.items, retentionChanges.invalidIds]);

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
    const ok = window.confirm(`Delete category "${l.name}"? This cannot be undone.`);
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
    setActionBusy(true);
    try {
      const resp = await api.syncGmailLabelExistence(false);
      setLastSyncExistence(resp);
      showToast(
        resp.errors > 0 ? "warning" : "success",
        `Gmail label sync done: created ${resp.created}, updated ${resp.updated}, linked ${resp.linked_existing}, errors ${resp.errors}.`
      );
      await refresh();
    } catch (e: any) {
      setError(e?.bodyText ?? e?.message ?? "Gmail label sync failed");
      showToast("error", "Gmail label sync failed (see error above).");
    } finally {
      setActionBusy(false);
    }
  }

  async function onPushIncremental() {
    setError(null);
    setActionBusy(true);
    try {
      const resp = await api.pushGmailLabelsIncremental(200);
      setLastPush({ mode: "incremental", limit: 200, resp });
      showToast(
        resp.failed > 0 ? "warning" : "success",
        `Incremental push finished: attempted ${resp.attempted}, ok ${resp.succeeded}, failed ${resp.failed}.`
      );
    } catch (e: any) {
      setError(e?.bodyText ?? e?.message ?? "Incremental push failed");
      showToast("error", "Incremental push failed (see error above).");
    } finally {
      setActionBusy(false);
    }
  }

  async function onPushBulk() {
    setError(null);
    setActionBusy(true);
    try {
      const resp = await api.pushGmailLabelsBulk(200, 0);
      setLastPush({ mode: "bulk", limit: 200, offset: 0, resp });
      showToast(
        resp.failed > 0 ? "warning" : "success",
        `Bulk push (first page) finished: attempted ${resp.attempted}, ok ${resp.succeeded}, failed ${resp.failed}.`
      );
    } catch (e: any) {
      setError(e?.bodyText ?? e?.message ?? "Bulk push failed");
      showToast("error", "Bulk push failed (see error above).");
    } finally {
      setActionBusy(false);
    }
  }

  async function onPushBulkJob() {
    const ok = window.confirm(
      "Start a background Gmail bulk push job now? You can watch progress in the top bar (percent + ETA)."
    );
    if (!ok) return;

    setError(null);
    setActionBusy(true);
    try {
      await startJob("gmail_push_bulk");
      showToast("info", "Started Gmail bulk push job. Watch the top bar for progress.");
    } catch (e: any) {
      setError(e?.bodyText ?? e?.message ?? "Failed to start Gmail push job");
      showToast("error", "Failed to start Gmail push job (see error above).");
    } finally {
      setActionBusy(false);
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

  async function onRetentionPlan() {
    const ok = window.confirm(
      "Plan retention archive candidates now? This computes eligible messages using our database and persists a worklist (outbox) for a separate Gmail push job."
    );
    if (!ok) return;

    setError(null);
    setActionBusy(true);
    try {
      const resp = await api.retentionPlan(50_000);
      setRetentionPlan(resp);
      showToast(
        resp.planned > 0 ? "success" : "info",
        `Planned ${resp.planned} archive action(s). Pending outbox: ${resp.pending_outbox}.`
      );
    } catch (e: any) {
      setError(e?.bodyText ?? e?.message ?? "Retention plan failed");
      showToast("error", "Retention planning failed (see error above).");
    } finally {
      setActionBusy(false);
    }
  }

  async function onArchivePushJob() {
    const ok = window.confirm(
      "Start a background Gmail Archive push job now? This consumes the planned outbox and applies the Archive marker label in batches."
    );
    if (!ok) return;

    setError(null);
    setActionBusy(true);
    try {
      await startJob("gmail_archive_push");
      showToast("info", "Started Gmail archive push job. Watch the top bar for progress.");
    } catch (e: any) {
      setError(e?.bodyText ?? e?.message ?? "Failed to start Gmail archive push job");
      showToast("error", "Failed to start Gmail archive push job (see error above).");
    } finally {
      setActionBusy(false);
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
              Categorisation (Categories)
            </Typography>
            <Typography variant="body2" sx={{ color: "text.secondary" }}>
              Categories are stored in the backend taxonomy but presented here as a hierarchy.
            </Typography>
          </Box>
          <Stack direction={{ xs: "column", sm: "row" }} spacing={1} alignItems="stretch">
            <Button variant="outlined" onClick={refresh} disabled={loading}>
              Refresh
            </Button>
            <Button
              variant="contained"
              onClick={() => {
                setForm(toFormState());
                setCreateOpen(true);
              }}
            >
              New category
            </Button>
          </Stack>
        </Stack>

        {error && (
          <Alert severity="error" sx={{ mb: 2, whiteSpace: "pre-wrap" }}>
            {error}
          </Alert>
        )}

        <Paper variant="outlined" sx={{ p: 1.25, mb: 2 }}>
          <Stack
            direction={{ xs: "column", md: "row" }}
            spacing={1}
            alignItems={{ xs: "stretch", md: "center" }}
          >
            <Box sx={{ flex: 1, minWidth: 260 }}>
              <Typography variant="subtitle2" sx={{ fontWeight: 900 }}>
                Default retention (Tier 0)
              </Typography>
              <Typography variant="caption" sx={{ color: "text.secondary" }}>
                Used when Tier 1 is set to “Default”, and indirectly when Tier 2 inherits from Tier 1.
              </Typography>
            </Box>

            <Stack direction="row" spacing={1} alignItems="center" flexWrap="wrap">
              <Autocomplete
                size="small"
                freeSolo
                options={RETENTION_QUICK_VALUES}
                getOptionLabel={(option) => String(option)}
                value={retentionDefaultDraft.value === "" ? null : Number(retentionDefaultDraft.value)}
                inputValue={retentionDefaultDraft.value}
                onInputChange={(_evt, inputValue) =>
                  setRetentionDefaultDraft((prev) => ({ ...prev, value: inputValue }))
                }
                onChange={(_evt, newValue) => {
                  const nextValue =
                    newValue == null
                      ? ""
                      : typeof newValue === "number"
                      ? String(newValue)
                      : String(newValue);
                  setRetentionDefaultDraft((prev) => ({ ...prev, value: nextValue }));
                }}
                renderInput={(params) => (
                  <TextField
                    {...params}
                    placeholder="2"
                    sx={{ width: 90 }}
                    inputProps={{
                      ...params.inputProps,
                      inputMode: "numeric",
                      pattern: "[0-9]*",
                    }}
                  />
                )}
              />

              <FormControl size="small" sx={{ minWidth: 110 }}>
                <Select
                  value={retentionDefaultDraft.unit}
                  onChange={(e) =>
                    setRetentionDefaultDraft((prev) => ({
                      ...prev,
                      unit: e.target.value as RetentionUnit,
                    }))
                  }
                >
                  <MenuItem value="days">days</MenuItem>
                  <MenuItem value="weeks">weeks</MenuItem>
                  <MenuItem value="months">months</MenuItem>
                  <MenuItem value="years">years</MenuItem>
                </Select>
              </FormControl>

              <Box sx={{ minWidth: 180 }}>
                {retentionDefaultSaveError ? (
                  <Stack direction="row" spacing={1} alignItems="center">
                    <Button
                      size="small"
                      variant="outlined"
                      color="error"
                      onClick={saveRetentionDefaultNow}
                      disabled={retentionDefaultSaving}
                    >
                      Retry save
                    </Button>
                    <Typography variant="caption" sx={{ color: "error.main" }}>
                      Save failed
                    </Typography>
                  </Stack>
                ) : retentionDefaultSaving ? (
                  <Typography variant="caption" sx={{ color: "text.secondary" }}>
                    Saving…
                  </Typography>
                ) : retentionDefaultDirty ? (
                  <Typography variant="caption" sx={{ color: "text.secondary" }}>
                    Will auto-save…
                  </Typography>
                ) : (
                  <Typography variant="caption" sx={{ color: "text.secondary" }}>
                    Saved
                  </Typography>
                )}
              </Box>

              <Typography variant="caption" sx={{ color: "text.secondary", whiteSpace: "nowrap" }}>
                Current: <b>{formatDays(retentionDefaultDays)}</b>
              </Typography>
            </Stack>
          </Stack>
        </Paper>

        <Stack direction={{ xs: "column", sm: "row" }} spacing={1} sx={{ mb: 2 }}>
          <Button variant="outlined" onClick={onSyncLabels} disabled={actionDisabled}>
            Sync Gmail labels (create/rename)
          </Button>
          <Button variant="outlined" onClick={onPushIncremental} disabled={actionDisabled}>
            Push to Gmail (incremental)
          </Button>
          <Button variant="outlined" onClick={onPushBulk} disabled={actionDisabled}>
            Push to Gmail (bulk)
          </Button>
          <Button
            variant="outlined"
            color="warning"
            onClick={onPushBulkJob}
            disabled={actionDisabled}
          >
            Push to Gmail (bulk job)
          </Button>
          <Button variant="outlined" onClick={onRetentionPreview}>
            Retention preview
          </Button>
          <Button color="primary" variant="outlined" onClick={onRetentionPlan} disabled={actionDisabled}>
            Plan archive candidates
          </Button>
          <Button
            color="warning"
            variant="contained"
            onClick={onArchivePushJob}
            disabled={actionDisabled}
          >
            Push Email Archive label (job)
          </Button>
          <Box sx={{ display: "flex", alignItems: "center", px: 1 }}>
            {retentionChanges.invalidIds.length > 0 ? (
              <Typography variant="caption" sx={{ color: "error.main" }}>
                Retention: invalid value(s)
              </Typography>
            ) : retentionSaveError ? (
              <Stack direction="row" spacing={1} alignItems="center">
                <Typography variant="caption" sx={{ color: "error.main" }}>
                  Retention save failed
                </Typography>
                <Button
                  size="small"
                  variant="outlined"
                  color="error"
                  onClick={saveRetentionChangesNow}
                  disabled={retentionSaving}
                >
                  Retry
                </Button>
              </Stack>
            ) : retentionSaving ? (
              <Typography variant="caption" sx={{ color: "text.secondary" }}>
                Saving retention…
              </Typography>
            ) : retentionChanges.items.length > 0 ? (
              <Typography variant="caption" sx={{ color: "text.secondary" }}>
                Will auto-save retention…
              </Typography>
            ) : (
              <Typography variant="caption" sx={{ color: "text.secondary" }}>
                Retention saved
              </Typography>
            )}
          </Box>
        </Stack>

        {lastSyncExistence && (
          <Alert severity={lastSyncExistence.errors > 0 ? "warning" : "success"} sx={{ mb: 2 }}>
            Gmail label sync: created <b>{lastSyncExistence.created}</b>, updated <b>{lastSyncExistence.updated}</b>,
            linked <b>{lastSyncExistence.linked_existing}</b>, errors <b>{lastSyncExistence.errors}</b>.
          </Alert>
        )}

        {lastPush && (
          <Alert severity={lastPush.resp.failed > 0 ? "warning" : "success"} sx={{ mb: 2 }}>
            {lastPush.mode === "bulk" ? (
              <>
                Bulk push (limit {lastPush.limit}, offset {lastPush.offset ?? 0}): attempted{" "}
                <b>{lastPush.resp.attempted}</b>, ok <b>{lastPush.resp.succeeded}</b>, failed{" "}
                <b>{lastPush.resp.failed}</b>. Note: this pushes a single page; use “bulk all” for a full run.
              </>
            ) : (
              <>
                Incremental push (limit {lastPush.limit}): attempted <b>{lastPush.resp.attempted}</b>, ok{" "}
                <b>{lastPush.resp.succeeded}</b>, failed <b>{lastPush.resp.failed}</b>.
              </>
            )}
          </Alert>
        )}

        {activeJob?.type === "gmail_push_bulk" && jobStatus && (
          <Alert severity={jobStatus.state === "failed" ? "error" : "info"} sx={{ mb: 2 }}>
            Gmail bulk push job: <b>{jobStatus.state}</b> — processed <b>{jobStatus.progress.processed}</b>
            {jobStatus.progress.total != null ? (
              <>
                /<b>{jobStatus.progress.total}</b>
              </>
            ) : null}
            {jobStatus.progress.percent != null ? (
              <>
                {" "}(<b>{jobStatus.progress.percent.toFixed(1)}%</b>)
              </>
            ) : null}
            {jobStatus.eta_hint ? (
              <>
                {" "}ETA <b>{jobStatus.eta_hint}</b>
              </>
            ) : null}
            . Ok <b>{jobStatus.counters.inserted}</b>, failed <b>{jobStatus.counters.failed}</b>.
          </Alert>
        )}

        {jobStatus?.type === "gmail_archive_push" && (
          <Alert severity={jobStatus.state === "failed" ? "error" : "info"} sx={{ mb: 2 }}>
            Gmail archive push job: <b>{jobStatus.state}</b> — processed <b>{jobStatus.progress.processed}</b>
            {jobStatus.progress.total != null ? (
              <>
                /<b>{jobStatus.progress.total}</b>
              </>
            ) : null}
            {jobStatus.progress.percent != null ? (
              <>
                {" "}(<b>{jobStatus.progress.percent.toFixed(1)}%</b>)
              </>
            ) : null}
            {jobStatus.eta_hint ? (
              <>
                {" "}ETA <b>{jobStatus.eta_hint}</b>
              </>
            ) : null}
            . Ok <b>{jobStatus.counters.inserted}</b>, failed <b>{jobStatus.counters.failed}</b>.
            {jobStatus.message ? (
              <>
                <br />
                <span style={{ whiteSpace: "pre-wrap" }}>{jobStatus.message}</span>
              </>
            ) : null}
          </Alert>
        )}

        {retentionPreview && (
          <Alert severity="info" sx={{ mb: 2 }}>
            Eligible for archive: <b>{retentionPreview.eligible_count}</b> (showing sample of{" "}
            {retentionPreview.sample.length}).
          </Alert>
        )}

        {retentionPlan && (
          <Alert severity={retentionPlan.planned > 0 ? "success" : "info"} sx={{ mb: 2 }}>
            Planned archive actions: <b>{retentionPlan.planned}</b>. Pending outbox: <b>{retentionPlan.pending_outbox}</b>.
          </Alert>
        )}

        <Paper variant="outlined" sx={{ overflow: "auto" }}>
          <Table size="small" stickyHeader>
            <TableHead>
              <TableRow>
                <TableCell>Category</TableCell>
                <TableCell>Retention</TableCell>
                <TableCell>Active</TableCell>
                <TableCell>Assigned</TableCell>
                <TableCell>Gmail label</TableCell>
                <TableCell>Sync</TableCell>
                <TableCell align="right">Actions</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {treeRows.map((r) => {
                const l = r.label;
                const isTier1 = r.depth === 0;
                const pad = 1 + r.depth * 2;

                const draft = retentionDraftById[l.id] ?? daysToDraft(l.retention_days);
                const draftDays = draftToDays(draft);
                const mode = retentionModeById[l.id] ?? (l.retention_days == null ? "inherit" : "custom");
                const effectiveDays = effectiveRetentionDays(l);

                const parent = l.parent_id != null ? labels.find((p) => p.id === Number(l.parent_id)) : null;
                const inheritedFromDays =
                  l.level === 1
                    ? retentionDefaultDays
                    : (parent ? effectiveRetentionDays(parent) : retentionDefaultDays);
                const retentionInvalid = mode === "custom" && Number.isNaN(draftDays as any);

                return (
                  <TableRow key={l.id} hover>
                    <TableCell sx={{ minWidth: 320 }}>
                      <Stack direction="row" spacing={0.75} alignItems="center">
                        {isTier1 ? (
                          <IconButton
                            size="small"
                            disabled={!r.isExpandable}
                            onClick={() => {
                              setExpandedTier1((prev) => {
                                const next = new Set(prev);
                                if (next.has(l.id)) next.delete(l.id);
                                else next.add(l.id);
                                return next;
                              });
                            }}
                            sx={{ ml: 0.25 }}
                          >
                            {r.isExpandable && expandedTier1.has(l.id) ? (
                              <ExpandLessIcon fontSize="small" />
                            ) : (
                              <ExpandMoreIcon fontSize="small" />
                            )}
                          </IconButton>
                        ) : (
                          <Box sx={{ width: 34 }} />
                        )}

                        <Box sx={{ pl: pad * 0.5, minWidth: 0 }}>
                          <Typography variant="body2" sx={{ fontWeight: isTier1 ? 900 : 700 }}>
                            {l.name}
                          </Typography>
                          <Typography variant="caption" sx={{ color: "text.secondary" }}>
                            {l.slug}
                          </Typography>
                        </Box>
                      </Stack>
                    </TableCell>
                    <TableCell sx={{ minWidth: 220 }}>
                      <Stack direction="row" spacing={1} alignItems="center">
                        <FormControl size="small" sx={{ minWidth: 110 }}>
                          <Select
                            value={mode}
                            onChange={(e) => {
                              const nextMode = e.target.value as RetentionMode;
                              setRetentionModeById((prev) => ({ ...prev, [l.id]: nextMode }));

                              if (nextMode === "custom") {
                                setRetentionDraftById((prev) => {
                                  const existing = prev[l.id];
                                  if (existing && existing.value.trim()) return prev;
                                  return { ...prev, [l.id]: daysToDraft(effectiveDays) };
                                });
                              }
                            }}
                          >
                            <MenuItem value="inherit">{l.level === 1 ? "Default" : "Inherit"}</MenuItem>
                            <MenuItem value="custom">Custom</MenuItem>
                          </Select>
                        </FormControl>

                        {mode === "custom" ? (
                          <>
                            <Autocomplete
                              size="small"
                              freeSolo
                              options={RETENTION_QUICK_VALUES}
                              getOptionLabel={(option) => String(option)}
                              value={draft.value === "" ? null : Number(draft.value)}
                              inputValue={draft.value}
                              onInputChange={(_evt, inputValue) => {
                                setRetentionDraftById((prev) => ({
                                  ...prev,
                                  [l.id]: { ...draft, value: inputValue },
                                }));
                              }}
                              onChange={(_evt, newValue) => {
                                const nextValue =
                                  newValue == null
                                    ? ""
                                    : typeof newValue === "number"
                                    ? String(newValue)
                                    : String(newValue);

                                setRetentionDraftById((prev) => ({
                                  ...prev,
                                  [l.id]: { ...draft, value: nextValue },
                                }));
                              }}
                              renderInput={(params) => (
                                <TextField
                                  {...params}
                                  placeholder="—"
                                  error={retentionInvalid}
                                  sx={{ width: 90 }}
                                  inputProps={{
                                    ...params.inputProps,
                                    inputMode: "numeric",
                                    pattern: "[0-9]*",
                                  }}
                                />
                              )}
                            />
                            <FormControl size="small" sx={{ minWidth: 110 }}>
                              <Select
                                value={draft.unit}
                                onChange={(e) => {
                                  const unit = e.target.value as RetentionUnit;
                                  setRetentionDraftById((prev) => ({
                                    ...prev,
                                    [l.id]: { ...draft, unit },
                                  }));
                                }}
                              >
                                <MenuItem value="days">days</MenuItem>
                                <MenuItem value="weeks">weeks</MenuItem>
                                <MenuItem value="months">months</MenuItem>
                                <MenuItem value="years">years</MenuItem>
                              </Select>
                            </FormControl>
                            <Typography
                              variant="caption"
                              sx={{ color: "text.secondary", whiteSpace: "nowrap" }}
                            >
                              {draftDays == null
                                ? "unset"
                                : retentionInvalid
                                ? "invalid"
                                : `≈ ${draftDays} days`}
                            </Typography>
                          </>
                        ) : (
                          <Typography variant="body2" sx={{ color: "text.secondary" }}>
                            {l.level === 1
                              ? `Uses global: ${formatDays(inheritedFromDays)}`
                              : `Uses ${parent?.name ?? "Tier 1"}: ${formatDays(inheritedFromDays)}`}
                          </Typography>
                        )}
                      </Stack>
                      <Typography variant="caption" sx={{ color: "text.secondary" }}>
                        Effective: <b>{formatDays(effectiveDays)}</b> (≈ {effectiveDays} days). Months are treated as 30 days.
                      </Typography>
                    </TableCell>
                    <TableCell>{l.is_active ? "Yes" : "No"}</TableCell>
                    <TableCell>
                      {(() => {
                        const direct = directCountById.get(l.id) ?? 0;
                        const total = rollupAssignedCount.get(l.id) ?? direct;
                        if (l.level === 1 && total !== direct) {
                          return (
                            <Box>
                              <Typography variant="body2" sx={{ fontWeight: 800 }}>
                                {total}
                              </Typography>
                              <Typography variant="caption" sx={{ color: "text.secondary" }}>
                                direct {direct}
                              </Typography>
                            </Box>
                          );
                        }
                        return <Typography variant="body2">{direct}</Typography>;
                      })()}
                    </TableCell>
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
                        <Button
                          size="small"
                          color="error"
                          variant="outlined"
                          onClick={() => onDelete(l)}
                        >
                          Delete
                        </Button>
                      </Stack>
                    </TableCell>
                  </TableRow>
                );
              })}
              {labels.length === 0 && (
                <TableRow>
                  <TableCell colSpan={7}>
                    <Typography variant="body2" sx={{ color: "text.secondary" }}>
                      {loading ? "Loading…" : "No categories found."}
                    </Typography>
                  </TableCell>
                </TableRow>
              )}
            </TableBody>
          </Table>
        </Paper>

        <Snackbar
          open={toast.open}
          autoHideDuration={6000}
          onClose={() => setToast((t) => ({ ...t, open: false }))}
          anchorOrigin={{ vertical: "bottom", horizontal: "center" }}
        >
          <Alert
            onClose={() => setToast((t) => ({ ...t, open: false }))}
            severity={toast.severity}
            sx={{ width: "100%" }}
          >
            {toast.message}
          </Alert>
        </Snackbar>
      </Box>

      <Dialog open={createOpen} onClose={() => setCreateOpen(false)} maxWidth="sm" fullWidth>
        <DialogTitle>New category</DialogTitle>
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
                  <em>None (top-level)</em>
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
              helperText="If set, messages assigned this category become eligible for archive after N days."
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
        <DialogTitle>Edit category</DialogTitle>
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
