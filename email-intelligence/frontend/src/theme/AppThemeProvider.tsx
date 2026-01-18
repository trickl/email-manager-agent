import CssBaseline from "@mui/material/CssBaseline";
import { createTheme, ThemeProvider } from "@mui/material/styles";
import React, { createContext, useContext, useEffect, useMemo, useState } from "react";

export type ColorMode = "light" | "dark";

type ColorModeContextValue = {
  mode: ColorMode;
  toggleMode: () => void;
  setMode: (mode: ColorMode) => void;
};

const ColorModeContext = createContext<ColorModeContextValue | null>(null);

const STORAGE_KEY = "email-intelligence.colorMode";

function defaultModeByLocalTime(): ColorMode {
  const hour = new Date().getHours();
  // "Sensible" default: light in daytime, dark at night.
  return hour >= 7 && hour < 19 ? "light" : "dark";
}

function isColorMode(value: unknown): value is ColorMode {
  return value === "light" || value === "dark";
}

export function useColorMode(): ColorModeContextValue {
  const ctx = useContext(ColorModeContext);
  if (!ctx) {
    throw new Error("useColorMode must be used within <AppThemeProvider>");
  }
  return ctx;
}

export default function AppThemeProvider(props: { children: React.ReactNode }) {
  const [mode, setMode] = useState<ColorMode>(() => {
    try {
      const stored = localStorage.getItem(STORAGE_KEY);
      if (isColorMode(stored)) return stored;
    } catch {
      // ignore
    }
    return defaultModeByLocalTime();
  });

  useEffect(() => {
    try {
      localStorage.setItem(STORAGE_KEY, mode);
    } catch {
      // ignore
    }
  }, [mode]);

  const ctxValue = useMemo<ColorModeContextValue>(() => {
    return {
      mode,
      setMode,
      toggleMode: () => setMode((prev) => (prev === "light" ? "dark" : "light")),
    };
  }, [mode]);

  const theme = useMemo(() => {
    return createTheme({
      palette: {
        mode,
        primary: {
          main: "#2563eb",
        },
      },
      typography: {
        fontFamily:
          'ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, "Apple Color Emoji", "Segoe UI Emoji"',
      },
      shape: {
        borderRadius: 12,
      },
      components: {
        MuiCssBaseline: {
          styleOverrides: {
            body: {
              // Helpful when embedding full-height pages.
              minHeight: "100vh",
            },
          },
        },
      },
    });
  }, [mode]);

  return (
    <ColorModeContext.Provider value={ctxValue}>
      <ThemeProvider theme={theme}>
        <CssBaseline />
        {props.children}
      </ThemeProvider>
    </ColorModeContext.Provider>
  );
}
