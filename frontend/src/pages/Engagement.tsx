import { useEffect, useState, useCallback, useRef, type ReactElement } from "react";
import { useParams, useNavigate } from "react-router-dom";
import {
  Box, Typography, Tabs, Tab, Button, CircularProgress, Alert, Snackbar,
  Chip, IconButton, Paper, Tooltip,
} from "@mui/material";
import ArrowBackIcon from "@mui/icons-material/ArrowBack";
import LockIcon from "@mui/icons-material/Lock";
import CloudDoneIcon from "@mui/icons-material/CloudDone";
import CloudSyncIcon from "@mui/icons-material/CloudSync";
import CloudOffIcon from "@mui/icons-material/CloudOff";
import { api } from "../api";
import Session1Form from "../sessions/Session1Form";
import Session2Form from "../sessions/Session2Form";
import Session3Form from "../sessions/Session3Form";
import Session4Form from "../sessions/Session4Form";
import Session5Form from "../sessions/Session5Form";
import Session6Form from "../sessions/Session6Form";

const SESSION_LABELS = [
  "1: Business Context",
  "2: Questions & Vocabulary",
  "3: Technical Design",
  "4: COE Review",
  "5: Configure Space",
  "6: Prototype Review",
];

const AUTOSAVE_DELAY_MS = 2000;

type SaveStatus = "idle" | "dirty" | "saving" | "saved" | "error";

interface Props {
  readOnly?: boolean;
}

export default function Engagement({ readOnly = false }: Props) {
  const { id } = useParams<{ id: string }>();
  const nav = useNavigate();
  const [data, setData] = useState<any>(null);
  const [loading, setLoading] = useState(true);
  const [tab, setTab] = useState(0);
  const [toast, setToast] = useState("");
  const [sessionDrafts, setSessionDrafts] = useState<Record<number, any>>({});
  const [isCoeMember, setIsCoeMember] = useState(false);
  const [saveStatus, setSaveStatus] = useState<SaveStatus>("idle");
  const autosaveTimer = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);
  const skipNextAutosave = useRef(true);

  const load = useCallback(async () => {
    if (!id) return;
    setLoading(true);
    try {
      const [eng, coe] = await Promise.all([
        api.getEngagement(id) as Promise<any>,
        api.checkCoeMembership(),
      ]);
      setData(eng);
      setIsCoeMember(coe.is_member);
      const s = eng.sessions || {};
      skipNextAutosave.current = true;
      setSessionDrafts({
        1: s["1"] || {},
        2: s["2"] || {},
        3: s["3"] || {},
        4: s["4"] || {},
        5: s["5"] || {},
        6: s["6"] || {},
      });
      setSaveStatus("idle");
    } catch {
      setData(null);
    }
    setLoading(false);
  }, [id]);

  useEffect(() => { load(); }, [load]);

  const persistSession = useCallback(async (sessionNum: number) => {
    if (!id) return;
    setSaveStatus("saving");
    try {
      await api.saveSession(id, sessionNum, sessionDrafts[sessionNum]);
      setSaveStatus("saved");
    } catch (err: any) {
      setSaveStatus("error");
      setToast(`Error saving: ${err.message}`);
    }
  }, [id, sessionDrafts]);

  // Debounced autosave: fires AUTOSAVE_DELAY_MS after the last draft change
  useEffect(() => {
    if (readOnly || !id) return;
    if (skipNextAutosave.current) {
      skipNextAutosave.current = false;
      return;
    }
    setSaveStatus("dirty");
    if (autosaveTimer.current) clearTimeout(autosaveTimer.current);
    autosaveTimer.current = setTimeout(() => {
      persistSession(tab + 1);
    }, AUTOSAVE_DELAY_MS);
    return () => {
      if (autosaveTimer.current) clearTimeout(autosaveTimer.current);
    };
  }, [sessionDrafts, tab, readOnly, id, persistSession]);

  const handleManualSave = async () => {
    if (autosaveTimer.current) clearTimeout(autosaveTimer.current);
    await persistSession(tab + 1);
    setToast(`Session ${tab + 1} saved`);
  };

  const updateDraft = (sessionNum: number, section: string, value: any) => {
    setSessionDrafts((prev) => ({
      ...prev,
      [sessionNum]: { ...prev[sessionNum], [section]: value },
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
    onChange: (section: string, value: any) => updateDraft(num, section, value),
    readOnly,
  });

  // COE approval gating
  const coeApprovalStatus = sessionDrafts[4]?.coe_approval_status || "";
  const isApproved = coeApprovalStatus === "approved";

  const renderSaveIndicator = () => {
    if (readOnly) return null;
    const statusDisplay: Record<SaveStatus, { icon: ReactElement; label: string; color: string }> = {
      idle: { icon: <CloudDoneIcon fontSize="small" />, label: "All changes saved", color: "text.secondary" },
      dirty: { icon: <CloudSyncIcon fontSize="small" />, label: "Unsaved changes", color: "warning.main" },
      saving: { icon: <CloudSyncIcon fontSize="small" />, label: "Saving...", color: "info.main" },
      saved: { icon: <CloudDoneIcon fontSize="small" />, label: "Saved", color: "success.main" },
      error: { icon: <CloudOffIcon fontSize="small" />, label: "Save failed", color: "error.main" },
    };
    const s = statusDisplay[saveStatus];
    return (
      <Box sx={{ display: "flex", alignItems: "center", gap: 0.5, color: s.color, fontSize: 13 }}>
        {s.icon}
        <Typography variant="caption" sx={{ color: "inherit" }}>{s.label}</Typography>
      </Box>
    );
  };

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
        {renderSaveIndicator()}
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
          {SESSION_LABELS.map((label, i) => {
            const locked = (i === 4 || i === 5) && !isApproved;
            return (
              <Tab
                key={i}
                label={
                  locked ? (
                    <Tooltip title="Requires COE approval">
                      <Box sx={{ display: "flex", alignItems: "center", gap: 0.5, opacity: 0.5 }}>
                        <LockIcon sx={{ fontSize: 14 }} />
                        {label}
                      </Box>
                    </Tooltip>
                  ) : label
                }
                disabled={locked}
              />
            );
          })}
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
        {tab === 3 && (
          <Session4Form
            {...sessionProps(4)}
            session1Data={sessionDrafts[1]}
            session2Data={sessionDrafts[2]}
            session3Data={sessionDrafts[3]}
            engagementId={id}
            isCoeMember={isCoeMember}
          />
        )}
        {tab === 4 && (
          <Session5Form
            {...sessionProps(5)}
            session3Data={sessionDrafts[3]}
            session4Data={sessionDrafts[4]}
            engagementId={id}
          />
        )}
        {tab === 5 && <Session6Form {...sessionProps(6)} />}
      </Box>

      {/* Save Button */}
      {!readOnly && (
        <Box sx={{ display: "flex", justifyContent: "flex-end", gap: 2, mb: 4 }}>
          <Button
            variant="contained"
            size="large"
            onClick={handleManualSave}
            disabled={saveStatus === "saving"}
          >
            {saveStatus === "saving" ? "Saving..." : `Save Session ${tab + 1}`}
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
