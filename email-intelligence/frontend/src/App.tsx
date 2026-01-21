import { Navigate, Route, Routes } from "react-router-dom";
import DashboardPage from "./pages/DashboardPage";
import JobsPage from "./pages/JobsPage";
import CategoriesPage from "./pages/CategoriesPage";

export default function App() {
  return (
    <Routes>
      <Route path="/" element={<DashboardPage />} />
      <Route path="/jobs" element={<JobsPage />} />
      <Route path="/categories" element={<CategoriesPage />} />
      <Route path="/settings" element={<Navigate to="/categories" replace />} />
      <Route path="*" element={<Navigate to="/" replace />} />
    </Routes>
  );
}
