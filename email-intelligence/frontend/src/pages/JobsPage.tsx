import Box from "@mui/material/Box";
import Button from "@mui/material/Button";
import Link from "@mui/material/Link";
import Paper from "@mui/material/Paper";
import Stack from "@mui/material/Stack";
import Typography from "@mui/material/Typography";
import { Link as RouterLink } from "react-router-dom";
import TopBar from "../ui/TopBar";
import { useJobPolling } from "../ui/useJobPolling";

export default function JobsPage() {
  const { jobStatus, startJob, activeJob } = useJobPolling();
  const disabled = activeJob?.state === "running" || activeJob?.state === "queued";

  return (
    <Box sx={{ height: "100vh", display: "flex", flexDirection: "column" }}>
      <TopBar
        title="Email Intelligence"
        jobStatus={jobStatus}
      />
      <Box sx={{ p: 2 }}>
        <Typography variant="h6" sx={{ fontWeight: 900, mb: 0.75 }}>
          Jobs
        </Typography>
        <Typography variant="body2" sx={{ color: "text.secondary" }}>
          This is a placeholder for job history. For now, the top bar shows the active job.
        </Typography>
        <Typography variant="body2" sx={{ mt: 1 }}>
          <Link component={RouterLink} to="/" underline="hover">
            Back to dashboard
          </Link>
        </Typography>
        <Stack direction="row" spacing={1} sx={{ mt: 2, flexWrap: "wrap" }}>
          <Button
            variant="contained"
            size="small"
            disabled={disabled}
            onClick={() => startJob("maintenance")}
          >
            Run maintenance
          </Button>
        </Stack>
        {jobStatus && (
          <Paper
            variant="outlined"
            sx={{
              mt: 2,
              p: 1.5,
              overflow: "auto",
              bgcolor: "background.default",
            }}
          >
            <Box component="pre" sx={{ m: 0 }}>
              {JSON.stringify(jobStatus, null, 2)}
            </Box>
          </Paper>
        )}
      </Box>
    </Box>
  );
}
