import { useEffect, useState, useRef } from "react";
import { useNavigate } from "react-router-dom";
import {
  Box, Typography, Button, Card, CardContent, CardActions, Grid2 as Grid,
  Dialog, DialogTitle, DialogContent, DialogActions, TextField,
  Chip, IconButton, CircularProgress, Alert,
} from "@mui/material";
import AddIcon from "@mui/icons-material/Add";
import DeleteIcon from "@mui/icons-material/Delete";
import VisibilityIcon from "@mui/icons-material/Visibility";
import EditIcon from "@mui/icons-material/Edit";
import { api } from "../api";

const SESSION_LABELS = [
  "Business Context", "Questions & Vocabulary", "Technical Design",
  "COE Review", "Configure Space", "Prototype Review",
];

const STATUS_COLORS: Record<string, "default" | "warning" | "success"> = {
  draft: "default",
  in_progress: "warning",
  complete: "success",
};

export default function Home() {
  const nav = useNavigate();
  const [engagements, setEngagements] = useState<Record<string, string>[]>([]);
  const [loading, setLoading] = useState(true);
  const [userEmail, setUserEmail] = useState("");
  const [dialogOpen, setDialogOpen] = useState(false);
  const [creating, setCreating] = useState(false);
  const [createError, setCreateError] = useState("");
  const [nameAvailable, setNameAvailable] = useState<boolean | null>(null);
  const [checkingName, setCheckingName] = useState(false);
  const nameCheckTimer = useRef<ReturnType<typeof setTimeout>>(undefined);
  const [form, setForm] = useState({
    genie_space_name: "",
    business_owner_name: "",
    business_owner_email: "",
    analyst_name: "",
    analyst_email: "",
  });

  const load = async () => {
    setLoading(true);
    try {
      const [engs, user] = await Promise.all([api.listEngagements(), api.getUser()]);
      setEngagements(engs);
      setUserEmail(user.email);
      setForm((f) => ({ ...f, analyst_email: user.email }));
    } catch {
      /* ignore */
    }
    setLoading(false);
  };

  useEffect(() => { load(); }, []);

  // Debounced name uniqueness check
  const checkName = (name: string) => {
    if (nameCheckTimer.current) clearTimeout(nameCheckTimer.current);
    if (!name.trim()) {
      setNameAvailable(null);
      return;
    }
    setCheckingName(true);
    nameCheckTimer.current = setTimeout(async () => {
      try {
        const res = await api.checkNameAvailable(name.trim());
        setNameAvailable(res.available);
      } catch {
        setNameAvailable(null);
      }
      setCheckingName(false);
    }, 500);
  };

  const handleCreate = async () => {
    setCreating(true);
    setCreateError("");
    try {
      const res = await api.createEngagement(form);
      setDialogOpen(false);
      nav(`/engagement/${res.engagement_id}`);
    } catch (err: any) {
      setCreateError(err.message || "Failed to create engagement");
    } finally {
      setCreating(false);
    }
  };

  const handleDelete = async (eid: string) => {
    if (!confirm("Delete this engagement and all its session data?")) return;
    await api.deleteEngagement(eid);
    load();
  };

  const isFormValid =
    form.genie_space_name.trim() &&
    form.business_owner_name.trim() &&
    form.business_owner_email.trim() &&
    form.analyst_name.trim() &&
    nameAvailable !== false;

  return (
    <Box sx={{ maxWidth: 1100, mx: "auto", p: 4 }}>
      <Box sx={{ display: "flex", justifyContent: "space-between", alignItems: "center", mb: 4 }}>
        <Box>
          <Typography variant="h4">Genie Space Discovery</Typography>
          <Typography color="text.secondary">
            Manage discovery engagements for new Genie Spaces
          </Typography>
        </Box>
        <Button variant="contained" startIcon={<AddIcon />} onClick={() => {
          setDialogOpen(true);
          setCreateError("");
          setNameAvailable(null);
        }}>
          New Engagement
        </Button>
      </Box>

      {loading ? (
        <Box sx={{ textAlign: "center", py: 8 }}><CircularProgress /></Box>
      ) : engagements.length === 0 ? (
        <Alert severity="info">
          No engagements yet. Click "New Engagement" to start your first discovery.
        </Alert>
      ) : (
        <Grid container spacing={2}>
          {engagements.map((e) => (
            <Grid size={{ xs: 12, sm: 6, md: 4 }} key={e.engagement_id}>
              <Card sx={{ height: "100%", display: "flex", flexDirection: "column" }}>
                <CardContent sx={{ flexGrow: 1 }}>
                  <Typography variant="h6" gutterBottom noWrap>
                    {e.genie_space_name || "Untitled Space"}
                  </Typography>
                  <Typography variant="body2" color="text.secondary" gutterBottom>
                    Owner: {e.business_owner_name || "TBD"}
                  </Typography>
                  <Typography variant="body2" color="text.secondary" gutterBottom>
                    Analyst: {e.analyst_name || "TBD"}
                  </Typography>
                  <Box sx={{ mt: 1, display: "flex", gap: 1, flexWrap: "wrap" }}>
                    <Chip
                      label={e.status?.replace("_", " ") || "draft"}
                      size="small"
                      color={STATUS_COLORS[e.status] || "default"}
                    />
                    <Chip
                      label={`Session ${e.current_session || 1}: ${SESSION_LABELS[Number(e.current_session || 1) - 1] || ""}`}
                      size="small"
                      variant="outlined"
                    />
                  </Box>
                </CardContent>
                <CardActions>
                  <IconButton size="small" onClick={() => nav(`/engagement/${e.engagement_id}`)} title="Edit">
                    <EditIcon fontSize="small" />
                  </IconButton>
                  <IconButton size="small" onClick={() => nav(`/view/${e.engagement_id}`)} title="Read-only view">
                    <VisibilityIcon fontSize="small" />
                  </IconButton>
                  <Box sx={{ flexGrow: 1 }} />
                  <IconButton size="small" onClick={() => handleDelete(e.engagement_id)} color="error" title="Delete">
                    <DeleteIcon fontSize="small" />
                  </IconButton>
                </CardActions>
              </Card>
            </Grid>
          ))}
        </Grid>
      )}

      {/* New Engagement Dialog */}
      <Dialog open={dialogOpen} onClose={() => setDialogOpen(false)} maxWidth="sm" fullWidth>
        <DialogTitle>New Discovery Engagement</DialogTitle>
        <DialogContent sx={{ display: "flex", flexDirection: "column", gap: 2, pt: "16px !important" }}>
          {createError && <Alert severity="error">{createError}</Alert>}
          <TextField
            label="Genie Space Name"
            value={form.genie_space_name}
            onChange={(e) => {
              setForm({ ...form, genie_space_name: e.target.value });
              checkName(e.target.value);
            }}
            fullWidth
            required
            error={nameAvailable === false}
            helperText={
              checkingName ? "Checking availability..." :
              nameAvailable === false ? "This name is already taken" :
              nameAvailable === true ? "Name is available" : ""
            }
            color={nameAvailable === true ? "success" : undefined}
          />
          <TextField
            label="Business Owner Name"
            value={form.business_owner_name}
            onChange={(e) => setForm({ ...form, business_owner_name: e.target.value })}
            fullWidth
            required
          />
          <TextField
            label="Business Owner Email"
            value={form.business_owner_email}
            onChange={(e) => setForm({ ...form, business_owner_email: e.target.value })}
            fullWidth
            required
          />
          <TextField
            label="Analyst Name"
            value={form.analyst_name}
            onChange={(e) => setForm({ ...form, analyst_name: e.target.value })}
            fullWidth
            required
          />
          <TextField
            label="Analyst Email"
            value={form.analyst_email}
            onChange={(e) => setForm({ ...form, analyst_email: e.target.value })}
            fullWidth
          />
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setDialogOpen(false)}>Cancel</Button>
          <Button variant="contained" onClick={handleCreate} disabled={!isFormValid || creating}>
            {creating ? "Creating..." : "Create"}
          </Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
}
