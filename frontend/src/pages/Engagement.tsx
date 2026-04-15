import { useEffect, useState, useCallback } from "react";
import { useParams, useNavigate } from "react-router-dom";
import {
  Box, Typography, Tabs, Tab, Button, CircularProgress, Alert, Snackbar,
  Chip, IconButton, Paper,
} from "@mui/material";
import ArrowBackIcon from "@mui/icons-material/ArrowBack";
import LockIcon from "@mui/icons-material/Lock";
import { api } from "../api";
import Session1Form from "../sessions/Session1Form";
import Session2Form from "../sessions/Session2Form";
import Session3Form from "../sessions/Session3Form";
import Session4Form from "../sessions/Session4Form";

const SESSION_LABELS = [
  "1: Business Context",
  "2: Questions & Vocabulary",
  "3: Technical Design",
  "4: Prototype Review",
];

interface Props {
  readOnly?: boolean;
}

export default function Engagement({ readOnly = false }: Props) {
  const { id } = useParams<{ id: string }>();
  const nav = useNavigate();
  const [data, setData] = useState<any>(null);
  const [loading, setLoading] = useState(true);
  const [tab, setTab] = useState(0);
  const [saving, setSaving] = useState(false);
  const [toast, setToast] = useState("");
  const [sessionDrafts, setSessionDrafts] = useState<Record<number, any>>({});

  const load = useCallback(async () => {
    if (!id) return;
    setLoading(true);
    try {
      const eng: any = await api.getEngagement(id);
      setData(eng);
      const s = eng.sessions || {};
      setSessionDrafts({
        1: s["1"] || {},
        2: s["2"] || {},
        3: s["3"] || {},
        4: s["4"] || {},
      });
    } catch {
      setData(null);
    }
    setLoading(false);
  }, [id]);

  useEffect(() => { load(); }, [load]);

  const handleSave = async (sessionNum: number) => {
    if (!id) return;
    setSaving(true);
    try {
      await api.saveSession(id, sessionNum, sessionDrafts[sessionNum]);
      setToast(`Session ${sessionNum} saved`);
      await load();
    } catch (err: any) {
      setToast(`Error saving: ${err.message}`);
    }
    setSaving(false);
  };

  const updateDraft = (sessionNum: number, section: string, rows: any[]) => {
    setSessionDrafts((prev) => ({
      ...prev,
      [sessionNum]: { ...prev[sessionNum], [section]: rows },
    }));
  };

  if (loading) {
    return (
      <Box sx={{ textAlign: "center", py: 12 }}>
        <CircularProgress />
      </Box>
    );
  }

  if (!data) {
    return (
      <Box sx={{ maxWidth: 800, mx: "auto", p: 4 }}>
        <Alert severity="error">Engagement not found.</Alert>
      </Box>
    );
  }

  const sessionProps = (num: number) => ({
    data: sessionDrafts[num] || {},
    onChange: (section: string, rows: any[]) => updateDraft(num, section, rows),
    readOnly,
  });

  return (
    <Box sx={{ maxWidth: 1200, mx: "auto", p: 3 }}>
      {/* Header */}
      <Box sx={{ display: "flex", alignItems: "center", gap: 1, mb: 1 }}>
        <IconButton onClick={() => nav("/")} size="small">
          <ArrowBackIcon />
        </IconButton>
        <Typography variant="h5" sx={{ flexGrow: 1 }}>
          {data.genie_space_name || "Untitled Space"}
        </Typography>
        {readOnly && (
          <Chip icon={<LockIcon />} label="Read-Only View" color="info" size="small" />
        )}
        <Chip
          label={String(data.status).replace("_", " ")}
          size="small"
          color={data.status === "complete" ? "success" : data.status === "in_progress" ? "warning" : "default"}
        />
      </Box>

      <Typography variant="body2" color="text.secondary" sx={{ ml: 6, mb: 3 }}>
        Owner: {data.business_owner_name} &middot; Analyst: {data.analyst_name}
      </Typography>

      {/* Session Tabs */}
      <Paper sx={{ mb: 2 }}>
        <Tabs value={tab} onChange={(_, v) => setTab(v)} variant="fullWidth">
          {SESSION_LABELS.map((label, i) => (
            <Tab key={i} label={label} />
          ))}
        </Tabs>
      </Paper>

      {/* Session Content */}
      <Box sx={{ mb: 3 }}>
        {tab === 0 && <Session1Form {...sessionProps(1)} />}
        {tab === 1 && <Session2Form {...sessionProps(2)} />}
        {tab === 2 && (
          <Session3Form
            {...sessionProps(3)}
            session1Data={sessionDrafts[1]}
            session2Data={sessionDrafts[2]}
          />
        )}
        {tab === 3 && <Session4Form {...sessionProps(4)} />}
      </Box>

      {/* Save Button */}
      {!readOnly && (
        <Box sx={{ display: "flex", justifyContent: "flex-end", gap: 2, mb: 4 }}>
          <Button
            variant="contained"
            size="large"
            onClick={() => handleSave(tab + 1)}
            disabled={saving}
          >
            {saving ? "Saving..." : `Save Session ${tab + 1}`}
          </Button>
        </Box>
      )}

      <Snackbar
        open={!!toast}
        autoHideDuration={3000}
        onClose={() => setToast("")}
        message={toast}
      />
    </Box>
  );
}
