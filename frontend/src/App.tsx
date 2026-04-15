import { Routes, Route } from "react-router-dom";
import Home from "./pages/Home";
import Engagement from "./pages/Engagement";

export default function App() {
  return (
    <Routes>
      <Route path="/" element={<Home />} />
      <Route path="/engagement/:id" element={<Engagement />} />
      <Route path="/view/:id" element={<Engagement readOnly />} />
    </Routes>
  );
}
