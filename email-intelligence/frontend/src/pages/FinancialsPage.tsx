import Alert from "@mui/material/Alert";
import Box from "@mui/material/Box";
import Card from "@mui/material/Card";
import CardContent from "@mui/material/CardContent";
import Divider from "@mui/material/Divider";
import Paper from "@mui/material/Paper";
import Stack from "@mui/material/Stack";
import Table from "@mui/material/Table";
import TableBody from "@mui/material/TableBody";
import TableCell from "@mui/material/TableCell";
import TableHead from "@mui/material/TableHead";
import TableRow from "@mui/material/TableRow";
import Typography from "@mui/material/Typography";
import IconButton from "@mui/material/IconButton";
import ToggleButton from "@mui/material/ToggleButton";
import ToggleButtonGroup from "@mui/material/ToggleButtonGroup";
import { ResponsivePie } from "@nivo/pie";
import ExpandLessIcon from "@mui/icons-material/ExpandLess";
import ExpandMoreIcon from "@mui/icons-material/ExpandMore";
import { useEffect, useMemo, useState } from "react";
import { api, ApiError } from "../api/client";
import type { PaymentItem, PaymentsAnalyticsResponse } from "../api/types";
import TopBar from "../ui/TopBar";
import { useJobPolling } from "../ui/useJobPolling";

type PieTooltipArgs = {
  datum: {
    id: string | number;
    value: number;
  };
};

function formatCurrency(value: number, currency?: string | null): string {
  const cur = currency ?? "USD";
  try {
    return new Intl.NumberFormat(undefined, {
      style: "currency",
      currency: cur,
      maximumFractionDigits: 2,
    }).format(value);
  } catch {
    return `${cur} ${value.toFixed(2)}`;
  }
}

function fmtDate(isoDate?: string | null): string {
  if (!isoDate) return "—";
  const d = new Date(`${isoDate}T00:00:00Z`);
  if (Number.isNaN(d.getTime())) return isoDate;
  return new Intl.DateTimeFormat(undefined, {
    month: "short",
    day: "numeric",
    year: "numeric",
    timeZone: "UTC",
  }).format(d);
}

function categoryColor(category?: string | null): string {
  switch ((category || "").toLowerCase()) {
    case "food":
      return "#F59E0B";
    case "entertainment":
      return "#A855F7";
    case "technology":
      return "#3B82F6";
    case "lifestyle":
      return "#EC4899";
    case "domestic bills":
      return "#10B981";
    case "mixed":
      return "#6B7280";
    default:
      return "#9CA3AF";
  }
}

export default function FinancialsPage() {
  const { jobStatus } = useJobPolling();
  const [analytics, setAnalytics] = useState<PaymentsAnalyticsResponse | null>(null);
  const [recent, setRecent] = useState<PaymentItem[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [viewMode, setViewMode] = useState<"vendor" | "calendar">("vendor");
  const [expandedVendors, setExpandedVendors] = useState<Record<string, boolean>>({});

  const currency = analytics?.currency ?? null;

  async function refresh() {
    setLoading(true);
    setError(null);
    try {
      const analyticsResp = await api.getPaymentsAnalytics(6);
      setAnalytics(analyticsResp);

      const recentResp = await api.getPaymentsRecent(12, 500, analyticsResp.currency ?? null);
      setRecent(recentResp.payments);
    } catch (e: any) {
      const msg = e instanceof ApiError ? e.bodyText || e.message : String(e);
      setError(msg);
      setAnalytics(null);
      setRecent([]);
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    refresh();
  }, []);

  const vendorPie = useMemo(() => {
    const rows = analytics?.by_vendor ?? [];
    return rows.map((r) => ({
      id: r.vendor,
      label: r.vendor,
      value: r.total_spend,
    }));
  }, [analytics]);

  const categoryPie = useMemo(() => {
    const rows = analytics?.by_category ?? [];
    return rows.map((r) => ({
      id: r.category,
      label: r.category,
      value: r.total_spend,
    }));
  }, [analytics]);

  const recurringPie = useMemo(() => {
    const rows = analytics?.by_recurring ?? [];
    return rows.map((r) => ({
      id: r.kind,
      label: r.kind === "recurring" ? "Recurring" : "One-off",
      value: r.total_spend,
    }));
  }, [analytics]);

  const frequencyRows = analytics?.by_frequency ?? [];
  const monthlyRows = analytics?.by_month ?? [];

  const vendorGroups = useMemo(() => {
    const map = new Map<
      string,
      {
        vendor: string;
        rows: PaymentItem[];
        total: number;
        mostRecent: Date | null;
        summaryType: string;
        summaryCategory: string;
      }
    >();

    for (const row of recent) {
      const vendor = (row.vendor_name || "Unknown").trim() || "Unknown";
      const key = vendor.toLowerCase();
      const existing = map.get(key) ?? {
        vendor,
        rows: [],
        total: 0,
        mostRecent: null as Date | null,
        summaryType: "Unknown",
        summaryCategory: "Other",
      };

      existing.rows.push(row);
      if (typeof row.cost_amount === "number") {
        existing.total += row.cost_amount;
      }

      const dateCandidate = row.payment_date
        ? new Date(`${row.payment_date}T00:00:00Z`)
        : row.internal_date
          ? new Date(row.internal_date)
          : null;
      if (dateCandidate && !Number.isNaN(dateCandidate.getTime())) {
        if (!existing.mostRecent || dateCandidate > existing.mostRecent) {
          existing.mostRecent = dateCandidate;
        }
      }

      map.set(key, existing);
    }

    for (const group of map.values()) {
      const types = new Set<string>();
      const categories = new Set<string>();
      for (const row of group.rows) {
        if (row.is_recurring === true) {
          types.add("Recurring");
        } else if (row.is_recurring === false) {
          types.add("One-off");
        } else {
          types.add("Unknown");
        }

        const cat = (row.item_category || "Other").trim() || "Other";
        categories.add(cat);
      }
      if (types.size === 1) {
        group.summaryType = Array.from(types)[0];
      } else {
        group.summaryType = "Mixed";
      }

      if (categories.size === 1) {
        group.summaryCategory = Array.from(categories)[0];
      } else {
        group.summaryCategory = "Mixed";
      }
    }

    return Array.from(map.values()).sort((a, b) => {
      const at = a.mostRecent ? a.mostRecent.getTime() : 0;
      const bt = b.mostRecent ? b.mostRecent.getTime() : 0;
      return bt - at;
    });
  }, [recent]);

  const maxVendorTotal = useMemo(() => {
    let max = 0;
    for (const group of vendorGroups) {
      if (group.total > max) {
        max = group.total;
      }
    }
    return max;
  }, [vendorGroups]);

  return (
    <Box sx={{ height: "100vh", display: "flex", flexDirection: "column" }}>
      <TopBar title="Email Intelligence" jobStatus={jobStatus} />

      <Box sx={{ p: 2, flex: 1, overflow: "auto" }}>
        <Stack direction={{ xs: "column", sm: "row" }} spacing={1} sx={{ mb: 2 }}>
          <Box sx={{ flex: 1 }}>
            <Typography variant="h6" sx={{ fontWeight: 900, mb: 0.25 }}>
              Financial Insights
            </Typography>
            <Typography variant="body2" sx={{ color: "text.secondary" }}>
              Payments extracted from email with deduped reporting for spend insights.
            </Typography>
          </Box>
        </Stack>

        {error && (
          <Alert severity="error" sx={{ mb: 2, whiteSpace: "pre-wrap" }}>
            {error}
          </Alert>
        )}

        <Stack direction={{ xs: "column", md: "row" }} spacing={2} sx={{ mb: 2 }}>
          <Card variant="outlined" sx={{ flex: 1 }}>
            <CardContent>
              <Typography variant="overline" sx={{ color: "text.secondary" }}>
                Total spend (last 6 months)
              </Typography>
              <Typography variant="h5" sx={{ fontWeight: 800 }}>
                {analytics ? formatCurrency(analytics.total_spend, currency) : "—"}
              </Typography>
              <Typography variant="body2" sx={{ color: "text.secondary" }}>
                {analytics ? `${analytics.payment_count} deduped payments` : ""}
              </Typography>
            </CardContent>
          </Card>
          <Card variant="outlined" sx={{ flex: 1 }}>
            <CardContent>
              <Typography variant="overline" sx={{ color: "text.secondary" }}>
                Currency focus
              </Typography>
              <Typography variant="h6" sx={{ fontWeight: 700 }}>
                {currency ?? "—"}
              </Typography>
              <Typography variant="body2" sx={{ color: "text.secondary" }}>
                {analytics?.available_currencies?.length
                  ? `Seen: ${analytics.available_currencies.join(", ")}`
                  : "No currency data yet"}
              </Typography>
            </CardContent>
          </Card>
        </Stack>

        <Stack direction={{ xs: "column", lg: "row" }} spacing={2} sx={{ mb: 2 }}>
          <Paper variant="outlined" sx={{ flex: 1, height: 320, p: 2 }}>
            <Typography variant="subtitle1" sx={{ fontWeight: 700, mb: 1 }}>
              Spend distribution by vendor
            </Typography>
            <Box sx={{ height: 260 }}>
              {vendorPie.length > 0 ? (
                <ResponsivePie
                  data={vendorPie}
                  margin={{ top: 20, right: 20, bottom: 40, left: 20 }}
                  innerRadius={0.5}
                  padAngle={0.7}
                  cornerRadius={3}
                  activeOuterRadiusOffset={6}
                  colors={{ scheme: "paired" }}
                  legends={[
                    {
                      anchor: "bottom",
                      direction: "row",
                      justify: false,
                      translateY: 30,
                      itemWidth: 90,
                      itemHeight: 14,
                      itemsSpacing: 4,
                      symbolSize: 10,
                      symbolShape: "circle",
                    },
                  ]}
                  tooltip={({ datum }: PieTooltipArgs) => (
                    <Box sx={{ p: 1, bgcolor: "background.paper", borderRadius: 1 }}>
                      <Typography variant="caption" sx={{ display: "block" }}>
                        {datum.id}
                      </Typography>
                      <Typography variant="body2" sx={{ fontWeight: 700 }}>
                        {formatCurrency(Number(datum.value), currency)}
                      </Typography>
                    </Box>
                  )}
                />
              ) : (
                <Typography variant="body2" sx={{ color: "text.secondary" }}>
                  {loading ? "Loading…" : "No vendor data yet."}
                </Typography>
              )}
            </Box>
          </Paper>
        </Stack>

        <Stack direction={{ xs: "column", lg: "row" }} spacing={2} sx={{ mb: 2 }}>
          <Paper variant="outlined" sx={{ flex: 1, height: 320, p: 2 }}>
            <Typography variant="subtitle1" sx={{ fontWeight: 700, mb: 1 }}>
              Spend distribution by category
            </Typography>
            <Box sx={{ height: 260 }}>
              {categoryPie.length > 0 ? (
                <ResponsivePie
                  data={categoryPie}
                  margin={{ top: 20, right: 20, bottom: 40, left: 20 }}
                  innerRadius={0.55}
                  padAngle={0.7}
                  cornerRadius={3}
                  activeOuterRadiusOffset={6}
                  colors={{ scheme: "pastel1" }}
                  legends={[
                    {
                      anchor: "bottom",
                      direction: "row",
                      justify: false,
                      translateY: 30,
                      itemWidth: 110,
                      itemHeight: 14,
                      itemsSpacing: 4,
                      symbolSize: 10,
                      symbolShape: "circle",
                    },
                  ]}
                  tooltip={({ datum }: PieTooltipArgs) => (
                    <Box sx={{ p: 1, bgcolor: "background.paper", borderRadius: 1 }}>
                      <Typography variant="caption" sx={{ display: "block" }}>
                        {datum.id}
                      </Typography>
                      <Typography variant="body2" sx={{ fontWeight: 700 }}>
                        {formatCurrency(Number(datum.value), currency)}
                      </Typography>
                    </Box>
                  )}
                />
              ) : (
                <Typography variant="body2" sx={{ color: "text.secondary" }}>
                  {loading ? "Loading…" : "No category data yet."}
                </Typography>
              )}
            </Box>
          </Paper>

          <Paper variant="outlined" sx={{ flex: 1, height: 320, p: 2 }}>
            <Typography variant="subtitle1" sx={{ fontWeight: 700, mb: 1 }}>
              Recurring vs one-off spend
            </Typography>
            <Box sx={{ height: 260 }}>
              {recurringPie.length > 0 ? (
                <ResponsivePie
                  data={recurringPie}
                  margin={{ top: 20, right: 20, bottom: 40, left: 20 }}
                  innerRadius={0.6}
                  padAngle={0.7}
                  cornerRadius={3}
                  activeOuterRadiusOffset={6}
                  colors={{ scheme: "set2" }}
                  legends={[
                    {
                      anchor: "bottom",
                      direction: "row",
                      justify: false,
                      translateY: 30,
                      itemWidth: 100,
                      itemHeight: 14,
                      itemsSpacing: 4,
                      symbolSize: 10,
                      symbolShape: "circle",
                    },
                  ]}
                  tooltip={({ datum }: PieTooltipArgs) => (
                    <Box sx={{ p: 1, bgcolor: "background.paper", borderRadius: 1 }}>
                      <Typography variant="caption" sx={{ display: "block" }}>
                        {datum.id}
                      </Typography>
                      <Typography variant="body2" sx={{ fontWeight: 700 }}>
                        {formatCurrency(Number(datum.value), currency)}
                      </Typography>
                    </Box>
                  )}
                />
              ) : (
                <Typography variant="body2" sx={{ color: "text.secondary" }}>
                  {loading ? "Loading…" : "No recurring data yet."}
                </Typography>
              )}
            </Box>
          </Paper>
        </Stack>

        <Stack direction={{ xs: "column", lg: "row" }} spacing={2} sx={{ mb: 2 }}>
          <Paper variant="outlined" sx={{ flex: 1, p: 2 }}>
            <Typography variant="subtitle1" sx={{ fontWeight: 700, mb: 1 }}>
              Recurring frequency breakdown
            </Typography>
            <Table size="small">
              <TableHead>
                <TableRow>
                  <TableCell>Frequency</TableCell>
                  <TableCell align="right">Payments</TableCell>
                  <TableCell align="right">Total spend</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {frequencyRows.map((row) => (
                  <TableRow key={row.frequency}>
                    <TableCell sx={{ textTransform: "capitalize" }}>{row.frequency}</TableCell>
                    <TableCell align="right">{row.payment_count}</TableCell>
                    <TableCell align="right">
                      {formatCurrency(row.total_spend, currency)}
                    </TableCell>
                  </TableRow>
                ))}
                {frequencyRows.length === 0 && (
                  <TableRow>
                    <TableCell colSpan={3}>
                      <Typography variant="body2" sx={{ color: "text.secondary" }}>
                        {loading ? "Loading…" : "No recurring frequency data yet."}
                      </Typography>
                    </TableCell>
                  </TableRow>
                )}
              </TableBody>
            </Table>
          </Paper>

          <Paper variant="outlined" sx={{ flex: 1, p: 2 }}>
            <Typography variant="subtitle1" sx={{ fontWeight: 700, mb: 1 }}>
              Monthly spend trend
            </Typography>
            <Table size="small">
              <TableHead>
                <TableRow>
                  <TableCell>Month</TableCell>
                  <TableCell align="right">Payments</TableCell>
                  <TableCell align="right">Total spend</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {monthlyRows.map((row) => (
                  <TableRow key={row.month}>
                    <TableCell>{fmtDate(row.month)}</TableCell>
                    <TableCell align="right">{row.payment_count}</TableCell>
                    <TableCell align="right">
                      {formatCurrency(row.total_spend, currency)}
                    </TableCell>
                  </TableRow>
                ))}
                {monthlyRows.length === 0 && (
                  <TableRow>
                    <TableCell colSpan={3}>
                      <Typography variant="body2" sx={{ color: "text.secondary" }}>
                        {loading ? "Loading…" : "No monthly data yet."}
                      </Typography>
                    </TableCell>
                  </TableRow>
                )}
              </TableBody>
            </Table>
          </Paper>
        </Stack>

        <Paper variant="outlined" sx={{ p: 2 }}>
          <Stack direction={{ xs: "column", sm: "row" }} spacing={1} alignItems="center">
            <Typography variant="subtitle1" sx={{ fontWeight: 700, flex: 1 }}>
              Outgoing payments (last 12 months)
            </Typography>
            <ToggleButtonGroup
              size="small"
              value={viewMode}
              exclusive
              onChange={(_, next) => {
                if (next) setViewMode(next);
              }}
            >
              <ToggleButton value="vendor">Vendor view</ToggleButton>
              <ToggleButton value="calendar">Calendar view</ToggleButton>
            </ToggleButtonGroup>
          </Stack>
          <Divider sx={{ mb: 1 }} />
          <Table size="small">
            <TableHead>
              <TableRow>
                <TableCell>Date</TableCell>
                <TableCell>Vendor</TableCell>
                <TableCell>Category</TableCell>
                <TableCell>Item</TableCell>
                <TableCell align="right">Amount</TableCell>
                <TableCell>Type</TableCell>
                <TableCell>Source</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {viewMode === "calendar" &&
                recent.map((row) => (
                  <TableRow key={row.message_id}>
                    <TableCell>{fmtDate(row.payment_date)}</TableCell>
                    <TableCell>{row.vendor_name ?? "—"}</TableCell>
                    <TableCell>
                      <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>
                        <Box
                          sx={{
                            width: 10,
                            height: 10,
                            borderRadius: "50%",
                            bgcolor: categoryColor(row.item_category),
                          }}
                        />
                        <Typography variant="body2">
                          {row.item_category ?? "Other"}
                        </Typography>
                      </Box>
                    </TableCell>
                    <TableCell>{row.item_name ?? "—"}</TableCell>
                    <TableCell align="right">
                      {row.cost_amount != null
                        ? formatCurrency(row.cost_amount, row.cost_currency)
                        : "—"}
                    </TableCell>
                    <TableCell>
                      {row.is_recurring ? `Recurring (${row.frequency ?? "—"})` : "One-off"}
                    </TableCell>
                    <TableCell>
                      <Typography variant="body2">{row.subject ?? "(no subject)"}</Typography>
                      <Typography variant="caption" sx={{ color: "text.secondary" }}>
                        {row.from_domain ?? "—"}
                      </Typography>
                    </TableCell>
                  </TableRow>
                ))}

              {viewMode === "vendor" &&
                vendorGroups.flatMap((group) => {
                  const key = group.vendor.toLowerCase();
                  const expanded = Boolean(expandedVendors[key]);
                  const barPct =
                    maxVendorTotal > 0 ? Math.min(100, (group.total / maxVendorTotal) * 100) : 0;

                  const headerRow = (
                    <TableRow key={`${group.vendor}-summary`} sx={{ bgcolor: "action.hover" }}>
                      <TableCell>
                        <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>
                          <IconButton
                            size="small"
                            onClick={() =>
                              setExpandedVendors((prev) => ({
                                ...prev,
                                [key]: !expanded,
                              }))
                            }
                          >
                            {expanded ? <ExpandLessIcon /> : <ExpandMoreIcon />}
                          </IconButton>
                          <Typography variant="body2">Most recent</Typography>
                        </Box>
                      </TableCell>
                      <TableCell>
                        <Typography variant="body2" sx={{ fontWeight: 700 }}>
                          {group.vendor}
                        </Typography>
                      </TableCell>
                      <TableCell>
                        <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>
                          <Box
                            sx={{
                              width: 10,
                              height: 10,
                              borderRadius: "50%",
                              bgcolor: categoryColor(group.summaryCategory),
                            }}
                          />
                          <Typography variant="body2">{group.summaryCategory}</Typography>
                        </Box>
                      </TableCell>
                      <TableCell>—</TableCell>
                      <TableCell align="right">
                        <Box sx={{ position: "relative", width: "100%", minWidth: 90 }}>
                          <Box
                            sx={{
                              position: "absolute",
                              inset: 0,
                              width: `${barPct}%`,
                              bgcolor: "primary.main",
                              opacity: 0.16,
                              borderRadius: 1,
                            }}
                          />
                          <Box sx={{ position: "relative", zIndex: 1, textAlign: "right" }}>
                            {formatCurrency(group.total, currency)}
                          </Box>
                        </Box>
                      </TableCell>
                      <TableCell>{group.summaryType}</TableCell>
                      <TableCell />
                    </TableRow>
                  );

                  const detailRows = expanded
                    ? group.rows.map((row) => (
                        <TableRow key={row.message_id}>
                          <TableCell>{fmtDate(row.payment_date)}</TableCell>
                          <TableCell>{row.vendor_name ?? "—"}</TableCell>
                          <TableCell>
                            <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>
                              <Box
                                sx={{
                                  width: 10,
                                  height: 10,
                                  borderRadius: "50%",
                                  bgcolor: categoryColor(row.item_category),
                                }}
                              />
                              <Typography variant="body2">
                                {row.item_category ?? "Other"}
                              </Typography>
                            </Box>
                          </TableCell>
                          <TableCell>{row.item_name ?? "—"}</TableCell>
                          <TableCell align="right">
                            {row.cost_amount != null
                              ? formatCurrency(row.cost_amount, row.cost_currency)
                              : "—"}
                          </TableCell>
                          <TableCell>
                            {row.is_recurring
                              ? `Recurring (${row.frequency ?? "—"})`
                              : "One-off"}
                          </TableCell>
                          <TableCell>
                            <Typography variant="body2">
                              {row.subject ?? "(no subject)"}
                            </Typography>
                            <Typography variant="caption" sx={{ color: "text.secondary" }}>
                              {row.from_domain ?? "—"}
                            </Typography>
                          </TableCell>
                        </TableRow>
                      ))
                    : [];

                  return [headerRow, ...detailRows];
                })}
              {recent.length === 0 && (
                <TableRow>
                  <TableCell colSpan={7}>
                    <Typography variant="body2" sx={{ color: "text.secondary" }}>
                      {loading ? "Loading…" : "No recent payments found."}
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
