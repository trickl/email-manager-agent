import Box from "@mui/material/Box";
import Typography from "@mui/material/Typography";

export default function EmptyDashboard() {
  return (
    <Box sx={{ mt: 3 }}>
      <Typography variant="body2" sx={{ color: "text.secondary" }}>
        No email data indexed yet.
      </Typography>
      <Typography variant="body2" sx={{ color: "text.secondary" }}>
        Connect Gmail to begin analysis.
      </Typography>
    </Box>
  );
}
