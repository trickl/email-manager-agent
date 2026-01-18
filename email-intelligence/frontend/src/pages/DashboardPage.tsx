import { useEffect, useMemo, useRef, useState } from "react";
import ChevronLeftIcon from "@mui/icons-material/ChevronLeft";
import RefreshIcon from "@mui/icons-material/Refresh";
import Alert from "@mui/material/Alert";
import Box from "@mui/material/Box";
import IconButton from "@mui/material/IconButton";
import Tooltip from "@mui/material/Tooltip";
import Typography from "@mui/material/Typography";
import { api, ApiError } from "../api/client";
import type { DashboardNode, DashboardTreeResponse } from "../api/types";
import { usefulnessBandLabel, usefulnessColor } from "../utils/colors";
import { findNode, pathToNode } from "../utils/tree";
import TopBar from "../ui/TopBar";
import SplitLayout from "../ui/SplitLayout";
import HierarchyTree from "../ui/HierarchyTree";
import SunburstPanel from "../ui/SunburstPanel";
import DetailPanel from "../ui/DetailPanel";
import { useJobPolling } from "../ui/useJobPolling";

export default function DashboardPage() {
  const [tree, setTree] = useState<DashboardTreeResponse | null>(null);
  const [selectedNodeId, setSelectedNodeId] = useState<string>("root");
  const [error, setError] = useState<string | null>(null);
  const [isLoadingTree, setIsLoadingTree] = useState<boolean>(false);
  const [leftCollapsed, setLeftCollapsed] = useState<boolean>(false);

  const isLoadingTreeRef = useRef<boolean>(false);
  const lastTreeRefreshAtRef = useRef<number>(0);

  const { activeJob, jobStatus, startJob, lastCompletedJobId } = useJobPolling();

  useEffect(() => {
    isLoadingTreeRef.current = isLoadingTree;
  }, [isLoadingTree]);

  async function loadTree() {
    setIsLoadingTree(true);
    setError(null);
    try {
      const data = await api.getDashboardTree();
      setTree(data);
    } catch (e) {
      const msg = e instanceof ApiError ? e.bodyText || e.message : String(e);
      setError(msg);
    } finally {
      setIsLoadingTree(false);
    }
  }

  useEffect(() => {
    loadTree();
  }, []);

  // Keep the dashboard totals in sync with job progress.
  //
  // Job progress is now delivered via SSE (when available), so the jobStatus can
  // update multiple times per second. The dashboard tree query is heavier, so we
  // throttle refreshes but trigger them *in response to jobStatus updates*.
  useEffect(() => {
    const running = activeJob?.state === "running" || activeJob?.state === "queued";
    if (!running) return;
    if (!jobStatus) return;

    // Avoid doing work when the tab isn't visible.
    const isVisible = typeof document === "undefined" || document.visibilityState === "visible";
    if (!isVisible) return;

    const now = Date.now();
    const minIntervalMs = 5000;
    const elapsed = now - lastTreeRefreshAtRef.current;

    if (elapsed < minIntervalMs) return;
    if (isLoadingTreeRef.current) return;

    lastTreeRefreshAtRef.current = now;
    loadTree();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [activeJob?.state, jobStatus?.updated_at]);

  // Refresh tree automatically when a job completes.
  useEffect(() => {
    if (lastCompletedJobId) loadTree();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [lastCompletedJobId]);

  const rootNode: DashboardNode | null = tree?.root ?? null;

  const totalEmailCount = rootNode?.count ?? 0;

  const unprocessedEmailCount = useMemo(() => {
    if (!rootNode) return 0;
    // The backend exposes a "Pending labelling" node representing unprocessed messages.
    // We sum counts across all matches just in case the tree shape changes.
    let sum = 0;
    const stack: DashboardNode[] = [rootNode];
    while (stack.length) {
      const cur = stack.pop()!;
      if (cur.name === "Pending labelling") sum += cur.count;
      for (const c of cur.children ?? []) stack.push(c);
    }
    return sum;
  }, [rootNode]);

  const selectedNode: DashboardNode | null = useMemo(() => {
    if (!rootNode) return null;
    return findNode(rootNode, selectedNodeId) ?? rootNode;
  }, [rootNode, selectedNodeId]);

  const breadcrumb = useMemo(() => {
    if (!rootNode) return [];
    const p = pathToNode(rootNode, selectedNodeId);
    return p ?? [rootNode];
  }, [rootNode, selectedNodeId]);

  const isPendingSelection = useMemo(() => {
    return breadcrumb.some((b) => b.name === "Pending labelling");
  }, [breadcrumb]);

  const jobDisabled = activeJob?.state === "running" || activeJob?.state === "queued";

  return (
    <Box sx={{ height: "100vh", display: "flex", flexDirection: "column" }}>
      <TopBar
        title="Email Intelligence"
        jobStatus={jobStatus}
        onIngestFull={() => startJob("ingest_full")}
        onIngestRefresh={() => startJob("ingest_refresh")}
        onClusterLabel={() => startJob("cluster_label")}
        disabled={jobDisabled}
      />

      <SplitLayout
        leftCollapsed={leftCollapsed}
        onToggleLeftCollapsed={() => setLeftCollapsed(false)}
        expandLeftLabel="Show categories"
        left={
          <Box sx={{ p: 1.5, height: "100%", overflow: "auto" }}>
            <Box sx={{ display: "flex", justifyContent: "space-between", gap: 1, alignItems: "center" }}>
              <Box sx={{ display: "flex", alignItems: "center", gap: 0.75, minWidth: 0 }}>
                <Tooltip title={leftCollapsed ? "Show categories" : "Hide categories"}>
                  <IconButton
                    size="small"
                    onClick={() => setLeftCollapsed((v) => !v)}
                    aria-label={leftCollapsed ? "Show categories" : "Hide categories"}
                    sx={{ border: 1, borderColor: "divider" }}
                  >
                    <ChevronLeftIcon fontSize="small" />
                  </IconButton>
                </Tooltip>

                <Typography
                  variant="subtitle2"
                  sx={{ fontWeight: 800, letterSpacing: 0.3, overflow: "hidden", textOverflow: "ellipsis" }}
                >
                  Categories
                </Typography>
              </Box>

              <Tooltip title="Refresh categories">
                <span>
                  <IconButton
                    onClick={() => loadTree()}
                    disabled={isLoadingTree}
                    size="small"
                    aria-label="Refresh categories"
                    sx={{ border: 1, borderColor: "divider" }}
                  >
                    <RefreshIcon fontSize="small" />
                  </IconButton>
                </span>
              </Tooltip>
            </Box>

            {error && (
              <Alert severity="error" sx={{ mt: 1.5 }}>
                <Box component="pre" sx={{ m: 0, whiteSpace: "pre-wrap" }}>
                  {error}
                </Box>
              </Alert>
            )}

            {!rootNode && !error && (
              <Typography variant="body2" sx={{ mt: 1.5, color: "text.secondary" }}>
                No data yet. Run ingestion to begin.
              </Typography>
            )}

            {rootNode && (
              <HierarchyTree
                root={rootNode}
                selectedId={selectedNodeId}
                onSelect={setSelectedNodeId}
                colorForNode={(n: DashboardNode) => usefulnessColor(n.unread_ratio)}
                badgeForNode={(n: DashboardNode) => `${Math.round(n.unread_ratio * 100)}% unread`}
                subtitleForNode={(n: DashboardNode) => usefulnessBandLabel(n.unread_ratio)}
              />
            )}
          </Box>
        }
        right={
          <Box
            sx={{
              p: 1.5,
              height: "100%",
              overflow: "hidden",
              display: "flex",
              flexDirection: "column",
            }}
          >
            <SunburstPanel
              root={selectedNode ?? rootNode}
              totalEmailCount={totalEmailCount}
              unprocessedEmailCount={unprocessedEmailCount}
              breadcrumb={breadcrumb}
              onSelectNode={(id: string) => setSelectedNodeId(id)}
              onBackToRoot={() => setSelectedNodeId("root")}
            />
            <Box sx={{ mt: 1.25, flex: "0 0 auto" }}>
              <DetailPanel node={selectedNode ?? rootNode} isPending={isPendingSelection} />
            </Box>
          </Box>
        }
      />
    </Box>
  );
}
