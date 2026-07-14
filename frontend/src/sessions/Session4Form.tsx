import { useState } from "react";
import {
  Box, Typography, Alert, Chip, Button, Stack, TextField, Divider,
} from "@mui/material";
import CheckCircleIcon from "@mui/icons-material/CheckCircle";
import ErrorIcon from "@mui/icons-material/Error";
import HourglassTopIcon from "@mui/icons-material/HourglassTop";
import PendingIcon from "@mui/icons-material/Pending";
import SendIcon from "@mui/icons-material/Send";
import { api } from "../api";
import ReadinessSummary from "../components/ReadinessSummary";

interface Props {
  data: Record<string, any>;
  /** Present for interface parity with the other session forms; the COE gate
   *  writes through dedicated endpoints + reload, not the session draft. */
  onChange?: (section: string, value: any) => void;
  readOnly?: boolean;
  engagementId?: string;
  isCoeMember?: boolean;
  isBoOnly?: boolean;
  /** Sessions 1-3 drafts — read-only, feed the deterministic Readiness Summary. */
  session1Data?: Record<string, any>;
  session2Data?: Record<string, any>;
  session3Data?: Record<string, any>;
  /** Reload the engagement after a status change so every surface reflects
   *  the server truth (status, reviewer, chip) without clobbering columns. */
  onReload?: () => void;
}

export default function Session4Form({
  data, readOnly, engagementId, isCoeMember, isBoOnly,
  session1Data, session2Data, session3Data, onReload,
}: Props) {
  const approvalStatus = data.coe_approval_status || "pending";
  const [approvalNotes, setApprovalNotes] = useState<string>(data.coe_approval_notes || "");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");

  const handleApproval = async (status: string) => {
    if (!engagementId) return;
    setBusy(true);
    setError("");
    try {
      await api.coeApprove(engagementId, { status, notes: approvalNotes });
      onReload?.();
    } catch (e: any) {
      setError(e?.message || "Failed to record the review decision.");
    }
    setBusy(false);
  };

  const handleRequestReview = async () => {
    if (!engagementId) return;
    setBusy(true);
    setError("");
    try {
      await api.requestReview(engagementId);
      onReload?.();
    } catch (e: any) {
      setError(e?.message || "Failed to submit for review.");
    }
    setBusy(false);
  };

  const statusChip = () => {
    switch (approvalStatus) {
      case "approved":
        return <Chip icon={<CheckCircleIcon />} label="Approved — Ready for Pilot" color="success" />;
      case "changes_requested":
        return <Chip icon={<ErrorIcon />} label="Changes Requested" color="warning" />;
      case "ready_for_review":
        return <Chip icon={<HourglassTopIcon />} label="Ready for COE Review" color="info" />;
      default:
        return <Chip icon={<PendingIcon />} label="Pending Review" color="default" />;
    }
  };

  return (
    <Box>
      <Alert severity="info" sx={{ mb: 2 }}>
        <strong>COE Review:</strong> The final gate before this engagement moves from dev to
        production and into phased piloting. The analyst submits the work from Sessions 1-3;
        a Center of Excellence reviewer approves it or requests changes. Approving marks the
        engagement <strong>Ready for Pilot</strong>.
      </Alert>

      {error && (
        <Alert severity="error" sx={{ mb: 2 }} onClose={() => setError("")}>
          {error}
        </Alert>
      )}

      {/* Deterministic readiness brief, computed from Sessions 1-3 (no AI). */}
      <ReadinessSummary s1={session1Data} s2={session2Data} s3={session3Data} />

      {/* Approval status */}
      <Box sx={{ mb: 2, display: "flex", alignItems: "center", gap: 2 }}>
        <Typography variant="subtitle1"><strong>Approval Status:</strong></Typography>
        {statusChip()}
        {data.coe_reviewer_email && (
          <Typography variant="body2" color="text.secondary">
            Reviewed by: {data.coe_reviewer_email}
          </Typography>
        )}
      </Box>

      {data.coe_approval_notes && approvalStatus === "changes_requested" && (
        <Alert severity="warning" sx={{ mb: 2 }}>
          <strong>COE Feedback:</strong> {data.coe_approval_notes}
        </Alert>
      )}

      {/* Analyst submit-for-review control. Available to anyone who can edit
          (not BO-only, not read-only). Hidden once the COE has approved. */}
      {!readOnly && !isBoOnly && approvalStatus !== "approved" && (
        <Box sx={{ mb: 2 }}>
          {approvalStatus === "ready_for_review" ? (
            <Alert
              severity="info"
              action={
                <Button
                  color="inherit"
                  size="small"
                  startIcon={<SendIcon />}
                  disabled={busy}
                  onClick={handleRequestReview}
                >
                  Notify COE again
                </Button>
              }
            >
              Marked <strong>Ready for COE Review</strong>. The COE has been notified.
            </Alert>
          ) : (
            <Stack direction="row" alignItems="center" spacing={2}>
              <Button
                variant="contained"
                startIcon={<SendIcon />}
                disabled={busy}
                onClick={handleRequestReview}
              >
                {busy ? "Submitting…" : "Mark Ready for COE Review"}
              </Button>
              <Typography variant="body2" color="text.secondary">
                {approvalStatus === "changes_requested"
                  ? "Addressed the feedback? Re-submit to notify the COE."
                  : "Done with Sessions 1-3? Submit to notify the COE that this is ready to review."}
              </Typography>
            </Stack>
          )}
        </Box>
      )}

      {/* COE-only approval controls */}
      {isCoeMember && !readOnly && (
        <>
          <Divider sx={{ my: 3 }} />
          <Stack direction="row" alignItems="center" spacing={1} sx={{ mb: 1 }}>
            <Typography variant="h6">COE Review Controls</Typography>
            <Chip label="COE Only" size="small" color="primary" />
          </Stack>
          <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
            Review the analyst's work in Sessions 1-3. Approve to mark the engagement
            Ready for Pilot, or request changes with specific feedback.
          </Typography>
          <TextField
            multiline
            minRows={3}
            fullWidth
            label="Review Notes / Feedback"
            placeholder="Provide feedback or approval notes..."
            value={approvalNotes}
            onChange={(e) => setApprovalNotes(e.target.value)}
            sx={{ mb: 2 }}
          />
          <Box sx={{ display: "flex", gap: 2 }}>
            <Button
              variant="contained"
              color="success"
              disabled={busy}
              onClick={() => handleApproval("approved")}
            >
              Approve
            </Button>
            <Button
              variant="outlined"
              color="warning"
              disabled={busy}
              onClick={() => handleApproval("changes_requested")}
            >
              Request Changes
            </Button>
          </Box>
        </>
      )}
    </Box>
  );
}
