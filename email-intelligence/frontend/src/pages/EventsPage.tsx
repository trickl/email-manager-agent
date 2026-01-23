import Alert from "@mui/material/Alert";
import Box from "@mui/material/Box";
import Button from "@mui/material/Button";
import Chip from "@mui/material/Chip";
import FormControlLabel from "@mui/material/FormControlLabel";
import Paper from "@mui/material/Paper";
import Stack from "@mui/material/Stack";
import Switch from "@mui/material/Switch";
import Table from "@mui/material/Table";
import TableBody from "@mui/material/TableBody";
import TableCell from "@mui/material/TableCell";
import TableHead from "@mui/material/TableHead";
import TableRow from "@mui/material/TableRow";
import Typography from "@mui/material/Typography";
import CalendarMonthIcon from "@mui/icons-material/CalendarMonth";
import UndoIcon from "@mui/icons-material/Undo";
import VisibilityOffIcon from "@mui/icons-material/VisibilityOff";
import { useEffect, useMemo, useState } from "react";
import { api } from "../api/client";
import type { FutureEventItem } from "../api/types";
import TopBar from "../ui/TopBar";
import { useJobPolling } from "../ui/useJobPolling";

function fmtTime(t?: string | null): string {
  if (!t) return "—";
  // API returns HH:MM:SS
  const m = t.trim().match(/^\d{2}:\d{2}/);
  return m ? m[0] : t;
}
function fmtDate(isoDate: string): string {
  // isoDate is expected to be YYYY-MM-DD. Parse as UTC midnight to avoid timezone drift.
  const d = new Date(`${isoDate}T00:00:00Z`);
  if (Number.isNaN(d.getTime())) {
    return isoDate;
  }

  const currentYear = new Date().getFullYear();
  const year = d.getUTCFullYear();

  const opts: Intl.DateTimeFormatOptions = {
    weekday: "short",
    month: "long",
    day: "numeric",
    timeZone: "UTC",
  };
  if (year !== currentYear) {
    opts.year = "numeric";
  }

  return new Intl.DateTimeFormat(undefined, opts).format(d);
}

function calendarChip(e: FutureEventItem): {
  label: string;
  color: "default" | "success" | "warning";
} {
  const inCal = Boolean(e.calendar_event_id);
  const checked = Boolean(e.calendar_checked_at);

  if (inCal) return { label: "In calendar", color: "success" };
  if (checked) return { label: "Not found", color: "warning" };
  return { label: "Unknown", color: "default" };
}

export default function EventsPage() {
  const { jobStatus } = useJobPolling();

  const [events, setEvents] = useState<FutureEventItem[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [includeHidden, setIncludeHidden] = useState(false);

  const [busyById, setBusyById] = useState<Record<number, boolean>>({});
  const anyBusy = useMemo(() => Object.values(busyById).some(Boolean), [busyById]);

  async function refresh() {
    setLoading(true);
    setError(null);
    try {
      const resp = await api.getFutureEvents(500, includeHidden);
      setEvents(resp.events);
    } catch (e: any) {
      setEvents([]);
      setError(e?.bodyText ?? e?.message ?? "Failed to load future events");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    refresh();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [includeHidden]);

  async function withBusy<T>(messageId: number, fn: () => Promise<T>): Promise<T | null> {
    setBusyById((prev) => ({ ...prev, [messageId]: true }));
    try {
      return await fn();
    } catch (e: any) {
      setError(e?.bodyText ?? e?.message ?? "Request failed");
      return null;
    } finally {
      setBusyById((prev) => ({ ...prev, [messageId]: false }));
    }
  }

  async function onHide(messageId: number) {
    const resp = await withBusy(messageId, () => api.hideEvent(messageId));
    if (!resp) return;

    // If we're not including hidden items, remove it from the list immediately.
    if (!includeHidden) {
      setEvents((prev) => prev.filter((e) => e.message_id !== messageId));
    } else {
      setEvents((prev) =>
        prev.map((e) => (e.message_id === messageId ? { ...e, hidden_at: resp.hidden_at } : e))
      );
    }
  }

  async function onUnhide(messageId: number) {
    const resp = await withBusy(messageId, () => api.unhideEvent(messageId));
    if (!resp) return;

    setEvents((prev) =>
      prev.map((e) =>
        e.message_id === messageId
          ? {
              ...e,
              hidden_at: resp.hidden_at ?? null,
            }
          : e
      )
    );
  }

  async function onPublishCalendar(messageId: number) {
    const resp = await withBusy(messageId, () => api.publishEventCalendar(messageId));
    if (!resp) return;

    setEvents((prev) =>
      prev.map((e) =>
        e.message_id === messageId
          ? {
              ...e,
              calendar_event_id: resp.calendar_event_id,
              calendar_checked_at: new Date().toISOString(),
              calendar_published_at: resp.already_existed
                ? e.calendar_published_at ?? null
                : resp.calendar_published_at ?? new Date().toISOString(),
            }
          : e
      )
    );
  }

  return (
    <Box sx={{ height: "100vh", display: "flex", flexDirection: "column" }}>
      <TopBar title="Email Intelligence" jobStatus={jobStatus} />

      <Box sx={{ p: 2 }}>
        <Stack direction={{ xs: "column", sm: "row" }} spacing={1} sx={{ mb: 2 }}>
          <Box sx={{ flex: 1 }}>
            <Typography variant="h6" sx={{ fontWeight: 900, mb: 0.25 }}>
              Future Events
            </Typography>
            <Typography variant="body2" sx={{ color: "text.secondary" }}>
              Extracted from email and optionally published to Google Calendar.
            </Typography>
          </Box>

          <Stack direction={{ xs: "column", sm: "row" }} spacing={1} alignItems="stretch">
            <FormControlLabel
              control={
                <Switch
                  checked={includeHidden}
                  onChange={(e) => setIncludeHidden(e.target.checked)}
                />
              }
              label="Include hidden"
            />

            <Button variant="outlined" onClick={refresh} disabled={loading}>
              Refresh
            </Button>
          </Stack>
        </Stack>

        {error && (
          <Alert severity="error" sx={{ mb: 2, whiteSpace: "pre-wrap" }}>
            {/** Add a little extra guidance for the common 'backend not running' case. */}
            {(() => {
              // This relies on the ApiError message we throw for network failures.
              const looksLikeNetwork =
                typeof error === "string" &&
                (error.includes("Network error") || error.includes("Failed to fetch"));
              if (!looksLikeNetwork) {
                return null;
              }
              return (
                <Box sx={{ mt: 1 }}>
                  <Typography variant="body2">
                    Tip: the UI talks to the backend on <code>localhost:8000</code> (via the Vite proxy). If
                    the backend isn’t running, the page can’t load events.
                  </Typography>
                </Box>
              );
            })()}
            {error}
          </Alert>
        )}

        <Paper variant="outlined" sx={{ overflow: "auto" }}>
          <Table size="small" stickyHeader>
            <TableHead>
              <TableRow>
                <TableCell>Date</TableCell>
                <TableCell>Time</TableCell>
                <TableCell>Type</TableCell>
                <TableCell>Event</TableCell>
                <TableCell>Source</TableCell>
                <TableCell>Calendar</TableCell>
                <TableCell align="right">Actions</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {events.map((e) => {
                const chip = calendarChip(e);
                const busy = Boolean(busyById[e.message_id]);
                const inCalendar = Boolean(e.calendar_event_id);
                const isHidden = Boolean(e.hidden_at);

                const timeLabel = e.start_time
                  ? `${fmtTime(e.start_time)}–${fmtTime(e.end_time)}`
                  : "All day";

                return (
                  <TableRow
                    key={e.message_id}
                    hover
                    sx={
                      isHidden
                        ? {
                            opacity: 0.72,
                          }
                        : undefined
                    }
                  >
                    <TableCell sx={{ whiteSpace: "nowrap" }}>{fmtDate(e.event_date)}</TableCell>
                    <TableCell sx={{ whiteSpace: "nowrap" }}>
                      {timeLabel}
                      {e.end_time_inferred ? (
                        <Typography
                          variant="caption"
                          sx={{ display: "block", color: "text.secondary" }}
                        >
                          end inferred
                        </Typography>
                      ) : null}
                    </TableCell>
                    <TableCell sx={{ whiteSpace: "nowrap" }}>{e.event_type ?? "—"}</TableCell>
                    <TableCell sx={{ minWidth: 260 }}>
                      <Typography variant="body2" sx={{ fontWeight: 800 }}>
                        {e.event_name ?? "(unnamed event)"}
                      </Typography>
                      {e.timezone ? (
                        <Typography variant="caption" sx={{ color: "text.secondary" }}>
                          tz: {e.timezone}
                        </Typography>
                      ) : null}
                    </TableCell>
                    <TableCell sx={{ minWidth: 280 }}>
                      <Typography variant="body2">{e.subject ?? "(no subject)"}</Typography>
                      <Typography variant="caption" sx={{ color: "text.secondary" }}>
                        {e.from_domain}
                      </Typography>
                      {e.hidden_at ? (
                        <Typography variant="caption" sx={{ color: "warning.main", display: "block" }}>
                          hidden
                        </Typography>
                      ) : null}
                    </TableCell>
                    <TableCell sx={{ whiteSpace: "nowrap" }}>
                      <Stack direction="row" spacing={1} alignItems="center">
                        <Chip size="small" label={chip.label} color={chip.color} />
                      </Stack>
                    </TableCell>
                    <TableCell align="right">
                      <Stack direction="row" spacing={1} justifyContent="flex-end">
                        <Button
                          size="small"
                          variant="outlined"
                          startIcon={<CalendarMonthIcon />}
                          onClick={() => onPublishCalendar(e.message_id)}
                          disabled={busy || loading || anyBusy || inCalendar}
                        >
                          Push to Google Calendar
                        </Button>
                        <Button
                          size="small"
                          color={isHidden ? "success" : "warning"}
                          variant={isHidden ? "outlined" : "contained"}
                          startIcon={isHidden ? <UndoIcon /> : <VisibilityOffIcon />}
                          onClick={() => (isHidden ? onUnhide(e.message_id) : onHide(e.message_id))}
                          disabled={busy || loading || anyBusy}
                        >
                          {isHidden ? "Unhide" : "Hide"}
                        </Button>
                      </Stack>
                    </TableCell>
                  </TableRow>
                );
              })}

              {events.length === 0 && (
                <TableRow>
                  <TableCell colSpan={7}>
                    <Typography variant="body2" sx={{ color: "text.secondary" }}>
                      {loading ? "Loading…" : "No future events found."}
                    </Typography>
                  </TableCell>
                </TableRow>
              )}
            </TableBody>
          </Table>
        </Paper>
      </Box>
    </Box>
  );
}
