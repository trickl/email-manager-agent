import type { ReactNode } from "react";
import Box from "@mui/material/Box";

export default function SplitLayout(props: { left: ReactNode; right: ReactNode }) {
  return (
    <Box
      sx={{
        flex: 1,
        minHeight: 0,
        minWidth: 0,
        display: "grid",
        gridTemplateColumns: "360px 1fr",
        // Prevent grid children from forcing horizontal overflow due to intrinsic min-width.
        overflow: "hidden",
      }}
    >
      <Box sx={{ borderRight: 1, borderColor: "divider", minHeight: 0, minWidth: 0 }}>
        {props.left}
      </Box>
      <Box sx={{ minHeight: 0, minWidth: 0 }}>{props.right}</Box>
    </Box>
  );
}
