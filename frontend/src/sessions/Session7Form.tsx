import { useEffect, useState } from "react";
import {
  Typography, Box, Accordion, AccordionSummary, AccordionDetails, Alert,
  Checkbox, TextField, Button, Chip, Stack, Link, List, ListItem,
  CircularProgress, Divider,
} from "@mui/material";
import ExpandMoreIcon from "@mui/icons-material/ExpandMore";
import OpenInNewIcon from "@mui/icons-material/OpenInNew";
import RefreshIcon from "@mui/icons-material/Refresh";
import CheckCircleIcon from "@mui/icons-material/CheckCircle";
import LockIcon from "@mui/icons-material/Lock";
import ExpandableTextField from "../components/ExpandableTextField";
import { api } from "../api";
import type { ProductionChecklistItem } from "../types";

interface Props {
  data: Record<string, any>;
  onChange: (section: string, value: any) => void;
  readOnly?: boolean;
  engagementId?: string;
  isCoeMember?: boolean;
  /** Genie space id/url live on Session 5; passed in so the access review can
   *  link to the right space. */
  session5Data?: Record<string, any>;
  /** Called after a successful sign-off so the parent can refresh its
   *  optimistic-lock token (updatedAtRef) and avoid 409s on autosave. */
  onApproved?: (updatedAt: string) => void;
}

// Seeded when the checklist is empty. The analyst can edit text inline; toggling
// any box persists the full list (so defaults become real saved rows).
const DEFAULT_CHECKLIST: ProductionChecklistItem[] = [
  { label: "Gold tables are in place (space no longer points at innovation-zone data)", done: false, notes: "" },
  { label: "Benchmark questions pass at or above the 80% target", done: false, notes: "" },
  { label: "Data plan finalized and reviewed", done: false, notes: "" },
  { label: "Genie space pushed to its production location", done: false, notes: "" },
  { label: "End-user access list confirmed with the business owner", done: false, notes: "" },
];

type SpaceAccess = {
  available: boolean;
  reason?: string;
  space_url?: string;
  access?: { principal: string; levels: string[] }[];
};

export default function Session7Form({
  data, onChange, readOnly, engagementId, isCoeMember, session5Data, onApproved,
}: Props) {
  const checklist: ProductionChecklistItem[] =
    data.production_checklist?.length ? data.production_checklist : DEFAULT_CHECKLIST;

  const updateChecklist = (idx: number, patch: Partial<ProductionChecklistItem>) => {
    const next = checklist.map((row, i) => (i === idx ? { ...row, ...patch } : row));
    onChange("production_checklist", next);
  };

  // --- Space Access Review ---------------------------------------------------
  const [access, setAccess] = useState<SpaceAccess | null>(null);
  const [accessLoading, setAccessLoading] = useState(false);
  const [accessError, setAccessError] = useState("");
  const spacePushed = !!(session5Data?.genie_space_id || data.genie_space_id);

  const loadAccess = async () => {
    if (!engagementId) return;
    setAccessLoading(true);
    setAccessError("");
    try {
      setAccess(await api.getSpaceAccess(engagementId));
    } catch (e: any) {
      setAccessError(e.message || String(e));
    } finally {
      setAccessLoading(false);
    }
  };

  useEffect(() => {
    if (engagementId && spacePushed) loadAccess();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [engagementId, spacePushed]);

  const spaceUrl = access?.space_url || session5Data?.genie_space_url || data.genie_space_url || "";

  // --- Production Sign-off ----------------------------------------------------
  const approvalStatus = data.prod_approval_status || "pending";
  const [signoffNotes, setSignoffNotes] = useState<string>(data.prod_approval_notes || "");
  const [signing, setSigning] = useState(false);
  const [signError, setSignError] = useState("");

  const submitSignoff = async (status: string) => {
    if (!engagementId) return;
    setSigning(true);
    setSignError("");
    try {
      const res = await api.prodApprove(engagementId, { status, notes: signoffNotes });
      // Refresh the parent's lock token BEFORE the onChange cascade triggers an
      // autosave, otherwise the next save 409s.
      if (res.updated_at) onApproved?.(res.updated_at);
      onChange("prod_approval_status", status);
      onChange("prod_approval_notes", signoffNotes);
    } catch (e: any) {
      setSignError(e.message || String(e));
    } finally {
      setSigning(false);
    }
  };

  const statusChip = (() => {
    if (approvalStatus === "approved") {
      return <Chip icon={<CheckCircleIcon />} label="Signed off" color="success" size="small" />;
    }
    if (approvalStatus === "changes_requested") {
      return <Chip label="Changes requested" color="warning" size="small" />;
    }
    return <Chip label="Pending sign-off" size="small" />;
  })();

  return (
    <Box>
      <Alert severity="info" sx={{ mb: 2 }}>
        <strong>Production Review</strong> is the final step before a space is treated as live.
        Confirm the space is production-ready, review who has access, and record the COE sign-off.
        This step is a recorded checkpoint — it doesn't block any other action.
      </Alert>

      {/* 1. Production Readiness Checklist */}
      <Accordion id="section-7-readiness" defaultExpanded>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Typography variant="h6">Production Readiness Checklist</Typography>
        </AccordionSummary>
        <AccordionDetails>
          <List dense disablePadding>
            {checklist.map((item, idx) => (
              <ListItem key={idx} alignItems="flex-start" sx={{ px: 0, py: 1 }} divider>
                <Checkbox
                  checked={!!item.done}
                  disabled={readOnly}
                  onChange={(e) => updateChecklist(idx, { done: e.target.checked })}
                  sx={{ mt: -0.5 }}
                />
                <Box sx={{ flex: 1, minWidth: 0 }}>
                  <Typography variant="body2" sx={{ fontWeight: 500 }}>{item.label}</Typography>
                  <TextField
                    value={item.notes || ""}
                    onChange={(e) => updateChecklist(idx, { notes: e.target.value })}
                    placeholder="Notes (optional)"
                    variant="standard"
                    fullWidth
                    disabled={readOnly}
                    sx={{ mt: 0.5 }}
                  />
                </Box>
              </ListItem>
            ))}
          </List>
        </AccordionDetails>
      </Accordion>

      {/* 2. Space Access Review */}
      <Accordion id="section-7-access" defaultExpanded>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Typography variant="h6">Space Access Review</Typography>
        </AccordionSummary>
        <AccordionDetails>
          <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
            Reconcile who actually has access to the Genie space against who should.
            Note: Genie has no "build but can't share" permission, so sharing can't be
            locked from this app — this view is for visibility and reconciliation.
          </Typography>

          {!spacePushed && (
            <Alert severity="warning" sx={{ mb: 2 }}>
              No Genie space has been pushed yet (see Session 5). Once it's pushed,
              its access list will show here.
            </Alert>
          )}

          {spacePushed && (
            <Box sx={{ mb: 2 }}>
              <Stack direction="row" spacing={1} sx={{ mb: 1, flexWrap: "wrap" }}>
                <Button
                  size="small"
                  variant="outlined"
                  startIcon={accessLoading ? <CircularProgress size={14} /> : <RefreshIcon />}
                  onClick={loadAccess}
                  disabled={accessLoading}
                >
                  Refresh access list
                </Button>
                {spaceUrl && (
                  <Button
                    size="small"
                    component={Link}
                    href={spaceUrl}
                    target="_blank"
                    rel="noopener"
                    endIcon={<OpenInNewIcon sx={{ fontSize: 14 }} />}
                  >
                    Manage sharing in Databricks
                  </Button>
                )}
              </Stack>

              {accessError && <Alert severity="error" sx={{ mb: 1 }}>{accessError}</Alert>}

              {access && access.available && (access.access?.length ? (
                <List dense sx={{ border: "1px solid", borderColor: "divider", borderRadius: 1 }}>
                  {access.access.map((a, i) => (
                    <ListItem key={i} divider={i < (access.access?.length || 0) - 1}>
                      <Typography variant="body2" sx={{ flex: 1, fontFamily: "monospace" }}>
                        {a.principal}
                      </Typography>
                      <Stack direction="row" spacing={0.5}>
                        {a.levels.map((l) => (
                          <Chip key={l} label={l.replace("CAN_", "CAN ")} size="small" variant="outlined" />
                        ))}
                      </Stack>
                    </ListItem>
                  ))}
                </List>
              ) : (
                <Alert severity="info">No explicit grants returned for this space.</Alert>
              ))}

              {access && !access.available && (
                <Alert severity="info">
                  {access.reason || "Live access list isn't available here."}{" "}
                  Use “Manage sharing in Databricks” to view and adjust who has access.
                </Alert>
              )}
            </Box>
          )}

          <Divider sx={{ my: 2 }} />

          <Typography variant="subtitle2" sx={{ mb: 1 }}>
            Intended access (who should have access)
          </Typography>
          <ExpandableTextField
            value={data.prod_access_notes || ""}
            onChange={(v) => onChange("prod_access_notes", v)}
            label="Intended access notes"
            placeholder="e.g. Finance Analysts group (CAN RUN); COE (CAN MANAGE). Document the agreed access so it can be checked against the live list above."
            disabled={readOnly}
            minRows={3}
            dialogTitle="Edit intended access notes"
          />
        </AccordionDetails>
      </Accordion>

      {/* 3. Production Sign-off */}
      <Accordion id="section-7-signoff" defaultExpanded>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>
            <Typography variant="h6">Production Sign-off</Typography>
            {statusChip}
          </Box>
        </AccordionSummary>
        <AccordionDetails>
          {data.prod_reviewer_email && approvalStatus !== "pending" && (
            <Typography variant="body2" color="text.secondary" sx={{ mb: 1 }}>
              Last recorded by <strong>{data.prod_reviewer_email}</strong>
              {data.prod_approval_notes ? ` — “${data.prod_approval_notes}”` : ""}
            </Typography>
          )}

          {!isCoeMember && (
            <Alert severity="info" icon={<LockIcon />}>
              Only members of the COE group can record the production sign-off.
            </Alert>
          )}

          {isCoeMember && !readOnly && (
            <Box>
              <TextField
                value={signoffNotes}
                onChange={(e) => setSignoffNotes(e.target.value)}
                label="Sign-off notes (optional)"
                fullWidth
                multiline
                minRows={2}
                sx={{ mb: 2 }}
              />
              {signError && <Alert severity="error" sx={{ mb: 2 }}>{signError}</Alert>}
              <Stack direction="row" spacing={1} sx={{ flexWrap: "wrap" }}>
                <Button
                  variant="contained"
                  color="success"
                  startIcon={signing ? <CircularProgress size={16} /> : <CheckCircleIcon />}
                  onClick={() => submitSignoff("approved")}
                  disabled={signing}
                >
                  Sign off — production ready
                </Button>
                <Button
                  variant="outlined"
                  color="warning"
                  onClick={() => submitSignoff("changes_requested")}
                  disabled={signing}
                >
                  Request changes
                </Button>
              </Stack>
            </Box>
          )}
        </AccordionDetails>
      </Accordion>
    </Box>
  );
}
