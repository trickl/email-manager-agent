import { ResponsiveSunburst } from "@nivo/sunburst";
import Box from "@mui/material/Box";
import Button from "@mui/material/Button";
import Link from "@mui/material/Link";
import Paper from "@mui/material/Paper";
import Typography from "@mui/material/Typography";
import { animated } from "@react-spring/web";
import type { MouseEvent } from "react";
import type { DashboardNode } from "../api/types";
import { usefulnessColor } from "../utils/colors";

type SunburstDatum = {
  /**
   * Unique identifier used by Nivo/React to key nodes.
   *
   * Note: `DashboardNode.id` is not globally unique (e.g. sender:<domain> can appear
   * in multiple places). We keep that as `id` for selection semantics, and use
   * this `key` for chart identity.
   */
  key: string;
  id: string;
  name: string;
  value: number;
  unread_ratio: number;
  frequency?: string | null;
  is_pending?: boolean;
  children?: SunburstDatum[];
};

function shortenLabel(label: string, maxLen: number): string {
  const s = String(label ?? "").trim();
  if (s.length <= maxLen) return s;
  return `${s.slice(0, Math.max(0, maxLen - 1))}…`;
}

function approxTextWidthPx(text: string, fontSizePx: number): number {
  // A pragmatic estimate so we can draw a background pill without measuring DOM.
  // Works well enough for short tier-1 labels.
  const avgCharWidth = fontSizePx * 0.62;
  return Math.max(0, text.length) * avgCharWidth;
}

function nodeKind(id: string): string {
  if (!id) return "";
  if (id === "root") return "root";
  const idx = id.indexOf(":");
  return idx === -1 ? id : id.slice(0, idx);
}

function defaultMaxDepthForRoot(root: DashboardNode): number {
  // Depth is relative to the `root` passed to the sunburst.
  // We show full depth (incl. senders) only when the user has drilled into a cluster.
  const kind = nodeKind(root.id);
  if (kind === "cluster") return 1; // cluster -> sender
  if (kind === "sender") return 0;
  // root/cat/sub: stop at cluster level for performance.
  return 3; // root -> cat -> sub -> cluster
}

function toDatum(
  n: DashboardNode,
  opts: {
    parentPending?: boolean;
    parentKey?: string;
    depth?: number;
    maxDepth: number;
    maxChildren: number;
  },
): SunburstDatum {
  const parentPending = opts.parentPending ?? false;
  const parentKey = opts.parentKey ?? "";
  const depth = opts.depth ?? 0;

  const isPending = parentPending || n.name === "Pending labelling";
  const safeId = n.id.replace(/\//g, "-");
  const key = parentKey ? `${parentKey}/${safeId}` : safeId;

  const shouldExpand = depth < opts.maxDepth;
  const rawChildren = (n.children ?? []) as DashboardNode[];

  let children: SunburstDatum[] | undefined = undefined;
  if (shouldExpand && rawChildren.length > 0) {
    // Sort children by count descending so the big segments stay visible.
    const sorted = [...rawChildren].sort((a, b) => (b.count ?? 0) - (a.count ?? 0));

    const head = sorted.slice(0, opts.maxChildren);
    const tail = sorted.slice(opts.maxChildren);

    children = head.map((c) =>
      toDatum(c, {
        parentPending: isPending,
        parentKey: key,
        depth: depth + 1,
        maxDepth: opts.maxDepth,
        maxChildren: opts.maxChildren,
      }),
    );

    if (tail.length > 0) {
      const tailCount = tail.reduce((sum, c) => sum + (c.count ?? 0), 0);
      const tailUnread = tail.reduce((sum, c) => sum + (c.unread_ratio ?? 0) * (c.count ?? 0), 0);
      const tailRatio = tailCount > 0 ? tailUnread / tailCount : 0;

      const otherKey = `${key}/__other__`;
      const otherLabel = `Other (${tail.length})`;

      children.push({
        key: otherKey,
        id: "", // synthetic aggregate: not selectable
        name: otherLabel,
        value: Math.max(0, tailCount),
        unread_ratio: tailRatio,
        frequency: null,
        is_pending: isPending,
        children: undefined,
      });
    }
  }

  return {
    key,
    id: n.id,
    name: n.name,
    value: Math.max(0, n.count),
    unread_ratio: n.unread_ratio,
    frequency: n.frequency ?? null,
    is_pending: isPending,
    children,
  };
}

export default function SunburstPanel(props: {
  root: DashboardNode | null;
  totalEmailCount: number;
  unprocessedEmailCount: number;
  breadcrumb: DashboardNode[];
  onSelectNode: (id: string) => void;
  onBackToRoot: () => void;
}) {
  const root = props.root;

  const lowColor = usefulnessColor(1); // unread_ratio=1 => usefulness=0
  const highColor = usefulnessColor(0); // unread_ratio=0 => usefulness=1

  const maxDepth = root ? defaultMaxDepthForRoot(root) : 0;
  // Hard cap per node to avoid rendering thousands of arcs/labels.
  // This mostly matters at the sender level.
  const maxChildren = 120;

  const nf = new Intl.NumberFormat();
  const totalText = nf.format(props.totalEmailCount);
  const unprocessedText = nf.format(props.unprocessedEmailCount);

  const unprocessedPct =
    props.totalEmailCount > 0 ? (props.unprocessedEmailCount / props.totalEmailCount) * 100 : 0;
  const showUnprocessedPct = props.totalEmailCount > 0 && unprocessedPct > 5;
  const unprocessedPctText = `${unprocessedPct.toFixed(1)}%`;

  return (
    <Box sx={{ flex: 1, minHeight: 0, display: "flex", flexDirection: "column" }}>
      <Box
        sx={{
          display: "flex",
          justifyContent: "space-between",
          alignItems: "baseline",
          gap: 1.25,
          flexWrap: "wrap",
        }}
      >
        <Typography variant="subtitle2" sx={{ fontWeight: 800, letterSpacing: 0.3 }}>
          Categorisation
        </Typography>

        <Box sx={{ display: "flex", alignItems: "center", gap: 1, flexWrap: "wrap" }}>
          <Box
            aria-label="Usefulness legend (low value to high value)"
            sx={{
              display: "flex",
              alignItems: "center",
              gap: 0.75,
              px: 1,
              py: 0.5,
              borderRadius: 999,
              border: "1px solid",
              borderColor: "divider",
              bgcolor: "background.paper",
            }}
          >
            <Typography variant="caption" sx={{ color: "text.secondary", fontWeight: 700 }}>
              low value
            </Typography>
            <Box
              sx={{
                width: 110,
                height: 10,
                borderRadius: 999,
                background: `linear-gradient(90deg, ${lowColor}, ${highColor})`,
                border: "1px solid rgba(255,255,255,0.12)",
              }}
            />
            <Typography variant="caption" sx={{ color: "text.secondary", fontWeight: 700 }}>
              high value
            </Typography>
          </Box>

          <Button
            onClick={props.onBackToRoot}
            size="small"
            variant="outlined"
            sx={{ textTransform: "none", fontWeight: 700 }}
          >
            Back to root
          </Button>
          <Typography variant="caption" sx={{ color: "text.secondary" }}>
            {props.breadcrumb.map((b, idx) => (
              <span key={b.id}>
                {idx > 0 ? " / " : ""}
                <Link
                  component="button"
                  type="button"
                  onClick={(e: MouseEvent) => {
                    e.preventDefault();
                    props.onSelectNode(b.id);
                  }}
                  underline="hover"
                  sx={{ fontWeight: 700 }}
                >
                  {b.name}
                </Link>
              </span>
            ))}
          </Typography>
        </Box>
      </Box>

      <Paper
        variant="outlined"
        sx={{
          mt: 1.25,
          flex: 1,
          minHeight: 0,
          borderRadius: 2,
          overflow: "hidden",
          bgcolor: "background.paper",
          position: "relative",
        }}
      >
        {!root ? (
          <Typography variant="body2" sx={{ p: 1.5, color: "text.secondary" }}>
            No data.
          </Typography>
        ) : (
          <>
            <Box
              sx={{
                position: "absolute",
                inset: 0,
                display: "flex",
                alignItems: "center",
                justifyContent: "center",
                pointerEvents: "none",
              }}
            >
              <Paper
                variant="outlined"
                sx={{
                  px: 2,
                  py: 1.25,
                  borderRadius: 3,
                  bgcolor: "background.paper",
                  textAlign: "center",
                  boxShadow: 3,
                  // Avoid forcing horizontal overflow on narrow viewports.
                  width: "min(260px, 92%)",
                }}
              >
                <Typography variant="overline" sx={{ color: "text.secondary", letterSpacing: 0.7 }}>
                  Total Emails
                </Typography>
                <Typography variant="h5" sx={{ fontWeight: 900, mt: -0.5 }}>
                  {totalText}
                </Typography>

                <Box sx={{ mt: 0.75 }}>
                  <Typography
                    variant="overline"
                    sx={{ color: "text.secondary", letterSpacing: 0.7 }}
                  >
                    Unprocessed Emails
                  </Typography>
                  <Typography variant="h6" sx={{ fontWeight: 900, mt: -0.5 }}>
                    {unprocessedText}
                  </Typography>

                  {showUnprocessedPct && (
                    <Typography variant="caption" sx={{ display: "block", color: "text.secondary" }}>
                      {unprocessedPctText} of total
                    </Typography>
                  )}
                </Box>
              </Paper>
            </Box>

            <ResponsiveSunburst
              data={toDatum(root, { maxDepth, maxChildren })}
              id={(d: any) => String((d as any).key)}
              value="value"
              margin={{ top: 10, right: 10, bottom: 10, left: 10 }}
              cornerRadius={2}
              borderWidth={1}
              borderColor={{ from: "color", modifiers: [["darker", 0.8]] }}
              colors={(d: any) => {
                const data = d.data as any;
                if (data?.is_pending) return "#9ca3af";
                return usefulnessColor(data?.unread_ratio);
              }}
              childColor={{ from: "color", modifiers: [["brighter", 0.15]] }}
              // Always show labels for tier-1 segments (children of the current root).
              // This gives a “tooltip-like” always-visible hint without having to hover.
              enableArcLabels={true}
              arcLabel={(node: any) => {
                // Depth is relative to the `data` root we pass into the sunburst.
                // root=0, children=1, grandchildren=2...
                const depth = Number((node as any)?.depth ?? NaN);
                if (depth !== 1) return "";
                const name = String((node as any)?.data?.name ?? (node as any)?.id ?? "");
                return shortenLabel(name, 18);
              }}
              arcLabelsSkipAngle={5}
              arcLabelsRadiusOffset={0.66}
              arcLabelsTextColor="#ffffff"
              arcLabelsComponent={({ label, style }: any) => {
                if (!label) return <g />;

                const fontSize = 11;
                const padX = 8;
                const padY = 4;
                const textWidth = approxTextWidthPx(String(label), fontSize);
                const w = Math.ceil(textWidth + padX * 2);
                const h = fontSize + padY * 2;
                const rx = Math.ceil(h / 2);

                return (
                  <animated.g transform={style.transform} style={{ pointerEvents: "none" }}>
                    <rect
                      x={-w / 2}
                      y={-h / 2}
                      width={w}
                      height={h}
                      rx={rx}
                      ry={rx}
                      fill="rgba(0,0,0,0.62)"
                      stroke="rgba(255,255,255,0.18)"
                      strokeWidth={1}
                    />
                    <text
                      textAnchor="middle"
                      dominantBaseline="central"
                      fill={style.textColor}
                      style={{ fontSize, fontWeight: 800 }}
                    >
                      {label}
                    </text>
                  </animated.g>
                );
              }}
              tooltip={({ id, value, data }: any) => {
                const unreadRatio = (data as any).unread_ratio as number;
                const unreadPct = Math.round(unreadRatio * 100);
                const freq = (data as any).frequency as string | null;
                const isPending = Boolean((data as any).is_pending);
                const name = String((data as any).name ?? id);
                return (
                  <Paper
                    variant="outlined"
                    sx={{
                      p: 1.25,
                      borderRadius: 2,
                      boxShadow: 6,
                      maxWidth: 360,
                    }}
                  >
                    <Typography variant="subtitle2" sx={{ fontWeight: 900, mb: 0.5 }}>
                      {name}
                    </Typography>
                    <Typography
                      variant="caption"
                      sx={{ display: "block", color: "text.secondary" }}
                    >
                      {value} messages
                    </Typography>
                    {isPending ? (
                      <Typography
                        variant="caption"
                        sx={{ display: "block", color: "text.secondary" }}
                      >
                        Value: Unknown (pending labelling)
                      </Typography>
                    ) : (
                      <Typography
                        variant="caption"
                        sx={{ display: "block", color: "text.secondary" }}
                      >
                        {unreadPct}% unread
                      </Typography>
                    )}
                    {freq && (
                      <Typography
                        variant="caption"
                        sx={{ display: "block", color: "text.secondary" }}
                      >
                        freq: {freq}
                      </Typography>
                    )}
                  </Paper>
                );
              }}
              onClick={(node) => {
                // `node.id` is the chart identity (our `SunburstDatum.key`).
                // For selection semantics we want the original dashboard node id.
                const id = String((node as any)?.data?.id ?? "");
                if (!id) return;
                props.onSelectNode(id);
              }}
              transitionMode="pushIn"
            />
          </>
        )}
      </Paper>

      <Typography variant="caption" sx={{ mt: 1.25, color: "text.secondary" }}>
        Tip: click a segment to drill into that node. For performance, the chart groups long tails
        into “Other” and only shows sender-level detail once you drill into a cluster.
      </Typography>
    </Box>
  );
}
