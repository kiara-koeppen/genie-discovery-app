import { useEffect, useState, useCallback, useRef, type ReactElement } from "react";
import { useParams, useNavigate } from "react-router-dom";
import {
  Box, Typography, Tabs, Tab, Button, CircularProgress, Alert, Snackbar,
  Chip, IconButton, Paper, Tooltip, Dialog, DialogTitle, DialogContent,
  DialogActions, TextField, Stack, Link,
} from "@mui/material";
import ArrowBackIcon from "@mui/icons-material/ArrowBack";
import LockIcon from "@mui/icons-material/Lock";
import EditIcon from "@mui/icons-material/Edit";
import OpenInNewIcon from "@mui/icons-material/OpenInNew";
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

interface EngagementMeta {
  genie_space_name: string;
  business_owner_name: string;
  business_owner_email: string;
  analyst_name: string;
  analyst_email: string;
  servicenow_ticket_url: string;
}

export default function Engagement({ readOnly = false }: Props) {
  const { id } = useParams<{ id: string }>();
  const nav = useNavigate();
  const [data, setData] = useState<any>(null);
  const [loading, setLoading] = useState(true);
  const [tab, setTab] = useState(0);
  const [toast, setToast] = useState("");
  const [sessionDrafts, setSessionDrafts] = useState<Record<number, any>>({});
  const [meta, setMeta] = useState<EngagementMeta>({
    genie_space_name: "",
    business_owner_name: "",
    business_owner_email: "",
    analyst_name: "",
    analyst_email: "",
    servicenow_ticket_url: "",
  });
  const [metaError, setMetaError] = useState<string>("");
  const [isCoeMember, setIsCoeMember] = useState(false);
  const [saveStatus, setSaveStatus] = useState<SaveStatus>("idle");
  const autosaveTimer = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);
  const skipNextAutosave = useRef(true);
  // Last-known updated_at for the engagement, used as an If-Match optimistic-
  // lock token. Advances after every successful save; reset on load/refresh.
  const updatedAtRef = useRef<string>("");
  // The session number that received the most recent updateDraft call.
  // Autosave saves THIS session, not the currently-visible tab, so flipping
  // tabs after an edit doesn't redirect the save to a session the user never
  // touched. Initial value 1 is fine -- skipNextAutosave gates the first run.
  const lastEditedSessionRef = useRef<number>(1);

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
      updatedAtRef.current = String(eng.updated_at || "");
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
      setMeta({
        genie_space_name: String(eng.genie_space_name || ""),
        business_owner_name: String(eng.business_owner_name || ""),
        business_owner_email: String(eng.business_owner_email || ""),
        analyst_name: String(eng.analyst_name || ""),
        analyst_email: String(eng.analyst_email || ""),
        servicenow_ticket_url: String(eng.servicenow_ticket_url || ""),
      });
      setMetaError("");
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
      const res = await api.saveSession(
        id,
        sessionNum,
        sessionDrafts[sessionNum],
        updatedAtRef.current,
      );
      if (res.updated_at) updatedAtRef.current = res.updated_at;
      setSaveStatus("saved");
    } catch (err: any) {
      setSaveStatus("error");
      const msg = err?.message || "Save failed";
      // 409 stale: backend message is friendly. Reload to recover.
      if (msg.toLowerCase().includes("updated by another user")) {
        setToast("This engagement was updated elsewhere. Reloading...");
        await load();
      } else {
        setToast(`Error saving: ${msg}`);
      }
    }
  }, [id, sessionDrafts, load]);

  // Debounced autosave: fires AUTOSAVE_DELAY_MS after a real edit (not after
  // a tab change). `tab` is intentionally NOT in the deps so navigating
  // between tabs without editing doesn't queue a save. Saves the session the
  // user actually modified, captured in lastEditedSessionRef by updateDraft.
  useEffect(() => {
    if (readOnly || !id) return;
    if (skipNextAutosave.current) {
      skipNextAutosave.current = false;
      return;
    }
    setSaveStatus("dirty");
    if (autosaveTimer.current) clearTimeout(autosaveTimer.current);
    autosaveTimer.current = setTimeout(() => {
      persistSession(lastEditedSessionRef.current);
    }, AUTOSAVE_DELAY_MS);
    return () => {
      if (autosaveTimer.current) clearTimeout(autosaveTimer.current);
    };
  }, [sessionDrafts, readOnly, id, persistSession]);

  const handleManualSave = async () => {
    if (autosaveTimer.current) clearTimeout(autosaveTimer.current);
    await persistSession(tab + 1);
    setToast(`Session ${tab + 1} saved`);
  };

  const updateDraft = (sessionNum: number, section: string, value: any) => {
    lastEditedSessionRef.current = sessionNum;
    setSessionDrafts((prev) => ({
      ...prev,
      [sessionNum]: { ...prev[sessionNum], [section]: value },
    }));
  };

  // --- ServiceNow URL: edited inline in Section 1, autosaved separately ---
  // Lives in `meta` like the rest of the engagement metadata, but has its own
  // autosave path because it's not part of the dialog flow.
  const snUrlSaveTimer = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);
  const snUrlInitialLoad = useRef(true);

  const updateServiceNowUrl = useCallback((value: string) => {
    setMeta((prev) => ({ ...prev, servicenow_ticket_url: value }));
  }, []);

  useEffect(() => {
    if (readOnly || !id) return;
    if (snUrlInitialLoad.current) {
      snUrlInitialLoad.current = false;
      return;
    }
    setSaveStatus("dirty");
    if (snUrlSaveTimer.current) clearTimeout(snUrlSaveTimer.current);
    snUrlSaveTimer.current = setTimeout(async () => {
      setSaveStatus("saving");
      try {
        const res = await api.updateEngagement(
          id,
          { ...meta, status: data?.status || "in_progress" },
          updatedAtRef.current,
        );
        if (res.updated_at) updatedAtRef.current = res.updated_at;
        setData((prev: any) => (prev ? { ...prev, servicenow_ticket_url: meta.servicenow_ticket_url } : prev));
        setSaveStatus("saved");
      } catch (err: any) {
        setSaveStatus("error");
        const msg = err?.message || "unknown";
        if (msg.toLowerCase().includes("updated by another user")) {
          setToast("This engagement was updated elsewhere. Reloading...");
          await load();
        } else {
          setToast(`Error saving ServiceNow link: ${msg}`);
        }
      }
    }, AUTOSAVE_DELAY_MS);
    return () => {
      if (snUrlSaveTimer.current) clearTimeout(snUrlSaveTimer.current);
    };
  }, [meta.servicenow_ticket_url, readOnly, id]); // eslint-disable-line react-hooks/exhaustive-deps

  // --- Engagement metadata edit dialog ---
  const [editOpen, setEditOpen] = useState(false);
  const [draftMeta, setDraftMeta] = useState<EngagementMeta>(meta);
  const [savingMeta, setSavingMeta] = useState(false);
  const [nameAvailable, setNameAvailable] = useState<boolean | null>(null);
  const [nameChecking, setNameChecking] = useState(false);
  const nameCheckTimer = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);

  const openEdit = () => {
    setDraftMeta(meta);
    setMetaError("");
    setNameAvailable(null);
    setEditOpen(true);
  };
  const closeEdit = () => {
    if (nameCheckTimer.current) clearTimeout(nameCheckTimer.current);
    setEditOpen(false);
  };
  const updateDraftMeta = (field: keyof EngagementMeta, value: string) => {
    setDraftMeta((prev) => ({ ...prev, [field]: value }));
  };

  // Real-time name availability check (excludes the current engagement so the
  // BO can save without renaming).
  useEffect(() => {
    if (!editOpen || !id || !draftMeta.genie_space_name.trim()) {
      setNameAvailable(null);
      return;
    }
    if (draftMeta.genie_space_name.trim() === meta.genie_space_name.trim()) {
      // Unchanged — no need to flag it as available/unavailable
      setNameAvailable(null);
      return;
    }
    if (nameCheckTimer.current) clearTimeout(nameCheckTimer.current);
    setNameChecking(true);
    nameCheckTimer.current = setTimeout(async () => {
      try {
        const res = await api.checkNameAvailable(draftMeta.genie_space_name.trim(), id);
        setNameAvailable(res.available);
      } catch {
        setNameAvailable(null);
      }
      setNameChecking(false);
    }, 400);
    return () => {
      if (nameCheckTimer.current) clearTimeout(nameCheckTimer.current);
    };
  }, [draftMeta.genie_space_name, editOpen, id, meta.genie_space_name]);

  const draftNameValid = !!draftMeta.genie_space_name.trim() && nameAvailable !== false;
  const canSaveMeta = draftNameValid && !savingMeta;

  const saveMeta = async () => {
    if (!id || !canSaveMeta) return;
    setSavingMeta(true);
    setMetaError("");
    try {
      const res = await api.updateEngagement(
        id,
        { ...draftMeta, status: data?.status || "in_progress" },
        updatedAtRef.current,
      );
      if (res.updated_at) updatedAtRef.current = res.updated_at;
      setMeta(draftMeta);
      setData((prev: any) => (prev ? { ...prev, ...draftMeta } : prev));
      setSaveStatus("saved");
      setEditOpen(false);
    } catch (err: any) {
      const msg = err?.message || "Save failed";
      if (msg.toLowerCase().includes("updated by another user")) {
        setMetaError("This engagement was updated elsewhere. Close this dialog and refresh.");
      } else {
        setMetaError(msg);
      }
    }
    setSavingMeta(false);
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
        <Typography variant="h5">
          {data.genie_space_name || "Untitled Space"}
        </Typography>
        {!readOnly && (
          <Tooltip title="Edit engagement info">
            <IconButton size="small" onClick={openEdit} sx={{ ml: 0.5 }}>
              <EditIcon fontSize="small" />
            </IconButton>
          </Tooltip>
        )}
        <Box sx={{ flexGrow: 1 }} />
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

      <Stack
        direction="row"
        alignItems="center"
        spacing={1}
        sx={{ ml: 6, mb: 3, color: "text.secondary", fontSize: 14 }}
      >
        <Typography variant="body2" color="text.secondary">
          Owner: {data.business_owner_name} &middot; Analyst: {data.analyst_name}
        </Typography>
        {data.servicenow_ticket_url && (
          <>
            <Typography variant="body2" color="text.secondary">&middot;</Typography>
            <Link
              href={data.servicenow_ticket_url}
              target="_blank"
              rel="noopener noreferrer"
              variant="body2"
              sx={{ display: "inline-flex", alignItems: "center", gap: 0.5 }}
            >
              ServiceNow ticket <OpenInNewIcon sx={{ fontSize: 12 }} />
            </Link>
          </>
        )}
      </Stack>

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
        {tab === 0 && (
          <Session1Form
            {...sessionProps(1)}
            serviceNowUrl={meta.servicenow_ticket_url}
            onServiceNowUrlChange={updateServiceNowUrl}
          />
        )}
        {tab === 1 && <Session2Form {...sessionProps(2)} />}
        {tab === 2 && (
          <Session3Form
            {...sessionProps(3)}
            session1Data={sessionDrafts[1]}
            session2Data={sessionDrafts[2]}
            engagementId={id}
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

      {/* Edit Engagement Info dialog -- triggered by the pencil next to the title */}
      <Dialog open={editOpen} onClose={closeEdit} maxWidth="sm" fullWidth>
        <DialogTitle>Edit Engagement Info</DialogTitle>
        <DialogContent>
          {metaError && (
            <Alert severity="error" sx={{ mb: 2 }} onClose={() => setMetaError("")}>
              {metaError}
            </Alert>
          )}
          <Stack spacing={2} sx={{ mt: 1 }}>
            <TextField
              label="Genie Space Name"
              value={draftMeta.genie_space_name}
              onChange={(e) => updateDraftMeta("genie_space_name", e.target.value)}
              required
              fullWidth
              size="small"
              error={nameAvailable === false || !draftMeta.genie_space_name.trim()}
              helperText={
                !draftMeta.genie_space_name.trim()
                  ? "Required"
                  : nameChecking
                    ? "Checking availability..."
                    : nameAvailable === false
                      ? "Another engagement already uses this name"
                      : nameAvailable === true
                        ? "Name is available"
                        : "Used as the unique identifier across all engagements"
              }
            />

            <Stack direction={{ xs: "column", sm: "row" }} spacing={2}>
              <TextField
                label="Business Owner Name"
                value={draftMeta.business_owner_name}
                onChange={(e) => updateDraftMeta("business_owner_name", e.target.value)}
                fullWidth
                size="small"
              />
              <TextField
                label="Business Owner Email"
                value={draftMeta.business_owner_email}
                onChange={(e) => updateDraftMeta("business_owner_email", e.target.value)}
                fullWidth
                size="small"
                type="email"
              />
            </Stack>

            <Stack direction={{ xs: "column", sm: "row" }} spacing={2}>
              <TextField
                label="Analyst Name"
                value={draftMeta.analyst_name}
                onChange={(e) => updateDraftMeta("analyst_name", e.target.value)}
                fullWidth
                size="small"
              />
              <TextField
                label="Analyst Email"
                value={draftMeta.analyst_email}
                onChange={(e) => updateDraftMeta("analyst_email", e.target.value)}
                fullWidth
                size="small"
                type="email"
              />
            </Stack>
          </Stack>
        </DialogContent>
        <DialogActions>
          <Button onClick={closeEdit} disabled={savingMeta}>Cancel</Button>
          <Button
            variant="contained"
            onClick={saveMeta}
            disabled={!canSaveMeta}
          >
            {savingMeta ? "Saving..." : "Save"}
          </Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
}
