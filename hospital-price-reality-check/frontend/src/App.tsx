import { useEffect } from "react";
import { Route, Routes, Navigate, useLocation } from "react-router-dom";
import { Layout } from "./components/Layout";
import { Home } from "./pages/Home";
import { Explore } from "./pages/Explore";
import { CodeDetail } from "./pages/CodeDetail";
import { Hospitals } from "./pages/Hospitals";
import { HospitalDetail } from "./pages/HospitalDetail";
import { Leaderboard } from "./pages/Leaderboard";
import { HowWeDidThis } from "./pages/HowWeDidThis";

function ScrollToTop() {
  const { pathname } = useLocation();
  useEffect(() => {
    window.scrollTo({ top: 0, left: 0, behavior: "auto" });
  }, [pathname]);
  return null;
}

export default function App() {
  return (
    <Layout>
      <ScrollToTop />
      <Routes>
        <Route path="/" element={<Home />} />
        <Route path="/explore" element={<Explore />} />
        <Route path="/explore/:system/:code" element={<CodeDetail />} />
        <Route path="/hospitals" element={<Hospitals />} />
        <Route path="/hospitals/:hospitalId" element={<HospitalDetail />} />
        <Route path="/leaderboard" element={<Leaderboard />} />
        <Route path="/how-we-did-this" element={<HowWeDidThis />} />
        <Route path="/methodology" element={<Navigate to="/how-we-did-this" replace />} />
        <Route path="/burla" element={<Navigate to="/how-we-did-this" replace />} />
      </Routes>
    </Layout>
  );
}
