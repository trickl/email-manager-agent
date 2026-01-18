import { useEffect, useState } from "react";
import ExpandMoreIcon from "@mui/icons-material/ExpandMore";
import Accordion from "@mui/material/Accordion";
import AccordionDetails from "@mui/material/AccordionDetails";
import AccordionSummary from "@mui/material/AccordionSummary";
import Alert from "@mui/material/Alert";
import Box from "@mui/material/Box";
import Chip from "@mui/material/Chip";
import CircularProgress from "@mui/material/CircularProgress";
import Paper from "@mui/material/Paper";
import Typography from "@mui/material/Typography";
import { api, ApiError } from "../api/client";
import type { DashboardNode, EmailMessageSummary } from "../api/types";
import { unreadBucketText, usefulnessBandLabel, usefulnessColor } from "../utils/colors";

export default function DetailPanel(props: { node: DashboardNode | null; isPending?: boolean }) {
  const n = props.node;
  const nodeId = n?.id ?? "";

  const [samples, setSamples] = useState<EmailMessageSummary[] | null>(null);
  const [samplesError, setSamplesError] = useState<string | null>(null);
  const [isLoadingSamples, setIsLoadingSamples] = useState<boolean>(false);

  useEffect(() => {
    let cancelled = false;

    if (!nodeId) {
      setSamples(null);
      setSamplesError(null);
      setIsLoadingSamples(false);
      return;
    }

    async function load() {
      setIsLoadingSamples(true);
      setSamplesError(null);
      try {
        const resp = await api.getMessageSamples(nodeId, 25);
        if (!cancelled) setSamples(resp.messages);
      } catch (e) {
        const msg = e instanceof ApiError ? e.bodyText || e.message : String(e);
        if (!cancelled) setSamplesError(msg);
      } finally {
        if (!cancelled) setIsLoadingSamples(false);
      }
    }

    // Only fetch samples when it makes sense (avoid noise for huge intermediate nodes).
    // Users can still click into cluster/sender nodes to see concrete messages.
    const kind = nodeId.split(":", 1)[0];
    if (kind === "cluster" || kind === "sender") {
      load();
    } else {
      setSamples(null);
      setSamplesError(null);
      setIsLoadingSamples(false);
    }

    return () => {
      cancelled = true;
    };
  }, [nodeId]);

  if (!n) return null;

  const isPending = Boolean(props.isPending) || n.name === "Pending labelling";

  const unreadPct = Math.round(n.unread_ratio * 100);
  const usefulLabel = isPending ? "Unknown" : usefulnessBandLabel(n.unread_ratio);
  const usefulColor = isPending ? "#9ca3af" : usefulnessColor(n.unread_ratio);

  return (
    <Paper variant="outlined" sx={{ borderRadius: 2, p: 1.5 }}>
      <Box sx={{ display: "flex", justifyContent: "space-between", gap: 1.5 }}>
        <Box sx={{ minWidth: 0 }}>
          <Typography
            variant="overline"
            sx={{ color: "text.secondary", letterSpacing: 0.6 }}
          >
            Selection
          </Typography>
          <Typography
            variant="subtitle1"
            sx={{
              fontWeight: 900,
              overflow: "hidden",
              textOverflow: "ellipsis",
              whiteSpace: "nowrap",
            }}
            title={n.name}
          >
            {n.name}
          </Typography>
        </Box>

        <Box sx={{ display: "flex", alignItems: "center", gap: 1.25, flexShrink: 0 }}>
          <Box
            sx={{
              width: 12,
              height: 12,
              borderRadius: 999,
              bgcolor: usefulColor,
              border: "1px solid",
              borderColor: "divider",
            }}
            title={usefulLabel}
          />
          <Typography variant="body2" sx={{ fontWeight: 800 }}>
            {usefulLabel}
          </Typography>
        </Box>
      </Box>

      <Box
        sx={{
          mt: 1.25,
          display: "grid",
          gridTemplateColumns: "repeat(4, minmax(0, 1fr))",
          gap: 1.25,
        }}
      >
        <Metric label="Messages" value={`${n.count}`} />
        <Metric label="Unread" value={`${n.unread_count}`} />
        <Metric label="Unread %" value={`${unreadPct}%`} />
        <Metric label="Children" value={`${n.children?.length ?? 0}`} />
      </Box>

      {n.frequency && (
        <Box sx={{ mt: 1.25 }}>
          <Metric label="Frequency" value={n.frequency} />
        </Box>
      )}

      <Typography variant="body2" sx={{ mt: 1.25, color: "text.secondary" }}>
        {isPending ? (
          <>
            Pending labelling: overall usefulness/value is unknown until this branch is processed.
          </>
        ) : (
          <>
            Unread: {unreadBucketText(n.unread_ratio)} ({unreadPct}%). This uses unread ratio as a
            proxy for “usefulness”.
          </>
        )}
      </Typography>

      <Accordion variant="outlined" sx={{ mt: 1.25 }}>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Typography variant="body2" sx={{ fontWeight: 800 }}>
            Raw node
          </Typography>
        </AccordionSummary>
        <AccordionDetails>
          <Paper
            variant="outlined"
            sx={{ p: 1.25, maxHeight: 260, overflow: "auto", bgcolor: "background.default" }}
          >
            <Box component="pre" sx={{ m: 0 }}>
              {JSON.stringify(n, null, 2)}
            </Box>
          </Paper>
        </AccordionDetails>
      </Accordion>

      {(n.id.startsWith("cluster:") || n.id.startsWith("sender:")) && (
        <Box sx={{ mt: 1.5 }}>
          <Typography variant="body2" sx={{ fontWeight: 900, mb: 0.75 }}>
            Sample messages
          </Typography>

          {samplesError && (
            <Alert severity="error" sx={{ mb: 1 }}>
              <Box component="pre" sx={{ m: 0, whiteSpace: "pre-wrap" }}>
                {samplesError}
              </Box>
            </Alert>
          )}

          {!samplesError && isLoadingSamples && (
            <Box sx={{ display: "flex", alignItems: "center", gap: 1, color: "text.secondary" }}>
              <CircularProgress size={16} />
              <Typography variant="body2">Loading…</Typography>
            </Box>
          )}

          {!samplesError && !isLoadingSamples && (samples?.length ?? 0) === 0 && (
            <Typography variant="body2" sx={{ color: "text.secondary" }}>
              No samples found.
            </Typography>
          )}

          {!samplesError && !isLoadingSamples && samples && samples.length > 0 && (
            <Paper variant="outlined" sx={{ borderRadius: 2, overflow: "hidden" }}>
              {samples.slice(0, 25).map((m, idx) => (
                <Box
                  key={m.gmail_message_id}
                  sx={{
                    p: 1.25,
                    borderTop: idx === 0 ? 0 : 1,
                    borderColor: "divider",
                    display: "grid",
                    gridTemplateColumns: "1fr auto",
                    gap: 1.25,
                    alignItems: "start",
                  }}
                >
                  <Box sx={{ minWidth: 0 }}>
                    <Typography
                      variant="body2"
                      sx={{
                        fontWeight: 800,
                        overflow: "hidden",
                        textOverflow: "ellipsis",
                        whiteSpace: "nowrap",
                      }}
                      title={m.subject ?? "(no subject)"}
                    >
                      {m.is_unread ? "• " : ""}
                      {m.subject ?? "(no subject)"}
                    </Typography>
                    <Typography variant="caption" sx={{ display: "block", color: "text.secondary", mt: 0.25 }}>
                      {new Date(m.internal_date).toLocaleString()} · {m.from_domain}
                    </Typography>

                    <Box sx={{ display: "flex", flexWrap: "wrap", gap: 0.75, mt: 0.75 }}>
                      {(m.label_names?.length ? m.label_names : m.label_ids).map((l) => (
                        <LabelChip key={l} label={l} />
                      ))}
                    </Box>
                  </Box>

                  <Box sx={{ textAlign: "right" }}>
                    <Typography variant="overline" sx={{ color: "text.secondary", letterSpacing: 0.6 }}>
                      Category
                    </Typography>
                    <Typography variant="body2" sx={{ fontWeight: 900 }}>
                      {m.category ?? "(pending)"}
                    </Typography>
                    {m.subcategory && (
                      <Typography variant="body2" sx={{ color: "text.secondary" }}>
                        {m.subcategory}
                      </Typography>
                    )}
                  </Box>
                </Box>
              ))}
            </Paper>
          )}
        </Box>
      )}
    </Paper>
  );
}

function Metric(props: { label: string; value: string }) {
  return (
    <Paper variant="outlined" sx={{ borderRadius: 2, p: 1.25, bgcolor: "background.default" }}>
      <Typography variant="overline" sx={{ color: "text.secondary", letterSpacing: 0.6 }}>
        {props.label}
      </Typography>
      <Typography variant="subtitle1" sx={{ fontWeight: 900, mt: -0.25 }}>
        {props.value}
      </Typography>
    </Paper>
  );
}

function LabelChip(props: { label: string }) {
  const isSystem = /^[A-Z0-9_]+$/.test(props.label) || props.label.startsWith("CATEGORY_");
  return (
    <Chip
      size="small"
      variant={isSystem ? "outlined" : "filled"}
      label={props.label}
      title={props.label}
      sx={{
        maxWidth: 260,
        ".MuiChip-label": {
          overflow: "hidden",
          textOverflow: "ellipsis",
          whiteSpace: "nowrap",
        },
        fontWeight: 800,
      }}
      color={isSystem ? "default" : "info"}
    />
  );
}
