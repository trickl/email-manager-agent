import { useEffect, useMemo, useState } from "react";
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

  const { activeJob, jobStatus, startJob, lastCompletedJobId } = useJobPolling();

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

  // Refresh tree automatically when a job completes.
  useEffect(() => {
    if (lastCompletedJobId) loadTree();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [lastCompletedJobId]);

  const rootNode: DashboardNode | null = tree?.root ?? null;

  const selectedNode: DashboardNode | null = useMemo(() => {
    if (!rootNode) return null;
    return findNode(rootNode, selectedNodeId) ?? rootNode;
  }, [rootNode, selectedNodeId]);

  const breadcrumb = useMemo(() => {
    if (!rootNode) return [];
    const p = pathToNode(rootNode, selectedNodeId);
    return p ?? [rootNode];
  }, [rootNode, selectedNodeId]);

  const jobDisabled = activeJob?.state === "running" || activeJob?.state === "queued";

  return (
    <div style={{ height: "100vh", display: "flex", flexDirection: "column" }}>
      <TopBar
        title="Email Intelligence"
        jobStatus={jobStatus}
        onIngestFull={() => startJob("ingest_full")}
        onIngestRefresh={() => startJob("ingest_refresh")}
        onClusterLabel={() => startJob("cluster_label")}
        disabled={jobDisabled}
      />

      <SplitLayout
        left={
          <div style={{ padding: "12px", height: "100%", overflow: "auto" }}>
            <div style={{ display: "flex", justifyContent: "space-between", gap: 8 }}>
              <h2 style={{ margin: 0, fontSize: 14, letterSpacing: 0.3, color: "#374151" }}>
                Hierarchy
              </h2>
              <button
                onClick={() => loadTree()}
                disabled={isLoadingTree}
                style={{
                  border: "1px solid #e5e7eb",
                  background: "white",
                  padding: "6px 10px",
                  borderRadius: 8,
                  cursor: isLoadingTree ? "not-allowed" : "pointer",
                }}
              >
                Refresh
              </button>
            </div>

            {error && (
              <div style={{ marginTop: 12, padding: 10, background: "#fee2e2", borderRadius: 8 }}>
                <div style={{ fontWeight: 600, color: "#991b1b" }}>Error</div>
                <pre style={{ margin: 0, whiteSpace: "pre-wrap", color: "#7f1d1d" }}>{error}</pre>
              </div>
            )}

            {!rootNode && !error && (
              <div style={{ marginTop: 12, color: "#6b7280" }}>
                No data yet. Run ingestion to begin.
              </div>
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
          </div>
        }
        right={
          <div style={{ padding: 12, height: "100%", overflow: "hidden", display: "flex", flexDirection: "column" }}>
            <SunburstPanel
              root={selectedNode ?? rootNode}
              breadcrumb={breadcrumb}
              onSelectNode={(id: string) => setSelectedNodeId(id)}
              onBackToRoot={() => setSelectedNodeId("root")}
            />
            <div style={{ marginTop: 10, flex: "0 0 auto" }}>
              <DetailPanel node={selectedNode ?? rootNode} />
            </div>
          </div>
        }
      />
    </div>
  );
}
