import { useEffect, useState, useMemo } from "react";
import {
  Typography, Box, Accordion, AccordionSummary, AccordionDetails, Alert,
  TextField, Button, Chip, Paper, Divider,
} from "@mui/material";
import ExpandMoreIcon from "@mui/icons-material/ExpandMore";
import CheckCircleIcon from "@mui/icons-material/CheckCircle";
import PendingIcon from "@mui/icons-material/Pending";
import ErrorIcon from "@mui/icons-material/Error";
import EditableTable from "../components/EditableTable";
import { api } from "../api";
import type { ColumnDef } from "../types";

const DATA_PLAN_COLS: ColumnDef[] = [
  { key: "table_or_view", label: "Table / Metric View", type: "uc_table" },
  { key: "type", label: "Type", width: 140, type: "select", options: ["Table", "Metric View"] },
  { key: "include_in_space", label: "Include in Genie Space?", width: 160, type: "select", options: ["Yes", "No", "TBD"] },
  { key: "notes", label: "Notes", type: "textarea" },
];

interface Props {
  data: Record<string, any>;
  onChange: (section: string, value: any) => void;
  readOnly?: boolean;
  session1Data?: Record<string, any>;
  session2Data?: Record<string, any>;
  session3Data?: Record<string, any>;
  engagementId?: string;
  isCoeMember?: boolean;
}

export default function Session4Form({
  data, onChange, readOnly, session3Data, engagementId, isCoeMember,
}: Props) {
  const [summary, setSummary] = useState("");
  const [loadingSummary, setLoadingSummary] = useState(false);
  const [approvalNotes, setApprovalNotes] = useState("");

  const approvalStatus = data.coe_approval_status || "pending";

  // Pre-populate data plan from Session 3 tables
  const session3Tables = useMemo(() => {
    const tables = new Set<string>();
    (session3Data?.sql_expressions || []).forEach((e: any) => {
      if (e.uc_table && e.uc_table.split(".").length === 3) tables.add(e.uc_table);
    });
    return Array.from(tables);
  }, [session3Data]);

  // On first load, seed data plan from Session 3 if empty
  useEffect(() => {
    if ((data.data_plan || []).length === 0 && session3Tables.length > 0) {
      const plan = session3Tables.map((t) => ({
        table_or_view: t,
        type: "Table",
        include_in_space: "Yes",
        notes: "",
      }));
      onChange("data_plan", plan);
    }
  }, [session3Tables]); // eslint-disable-line react-hooks/exhaustive-deps

  // Fetch auto-summary
  const fetchSummary = async () => {
    if (!engagementId) return;
    setLoadingSummary(true);
    try {
      const res = await api.getAutoSummary(engagementId);
      setSummary(res.summary);
      onChange("auto_summary", res.summary);
    } catch {
      setSummary("Failed to generate summary.");
    }
    setLoadingSummary(false);
  };

  useEffect(() => {
    if (data.auto_summary) {
      setSummary(data.auto_summary);
    } else if (engagementId) {
      fetchSummary();
    }
  }, [engagementId]); // eslint-disable-line react-hooks/exhaustive-deps

  const handleApproval = async (status: string) => {
    if (!engagementId) return;
    try {
      await api.coeApprove(engagementId, { status, notes: approvalNotes });
      onChange("coe_approval_status", status);
      onChange("coe_approval_notes", approvalNotes);
    } catch {
      // handled silently
    }
  };

  const statusChip = () => {
    switch (approvalStatus) {
      case "approved":
        return <Chip icon={<CheckCircleIcon />} label="Approved" color="success" />;
      case "changes_requested":
        return <Chip icon={<ErrorIcon />} label="Changes Requested" color="warning" />;
      default:
        return <Chip icon={<PendingIcon />} label="Pending Review" color="default" />;
    }
  };

  // Metric view YAML from Session 3
  const metricViewYaml = session3Data?.metric_view_yaml || "";

  return (
    <Box>
      <Alert severity="info" sx={{ mb: 2 }}>
        <strong>COE Review:</strong> The analyst submits their work from Sessions 1-3 for Center of Excellence
        approval. Include your commentary, review the data plan, and ensure metric views are addressed.
        COE reviewers will approve or request changes before the Genie Space can be configured.
      </Alert>

      {/* Approval Status */}
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

      {/* Analyst Commentary */}
      <Accordion defaultExpanded>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Typography variant="h6">Analyst Commentary</Typography>
        </AccordionSummary>
        <AccordionDetails>
          <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
            Summarize what you learned from the business owner, your technical approach,
            and anything the COE should know when reviewing this engagement.
          </Typography>
          <TextField
            multiline
            minRows={6}
            fullWidth
            placeholder="Describe your findings, approach, and recommendations for the COE..."
            value={data.analyst_commentary || ""}
            onChange={(e) => onChange("analyst_commentary", e.target.value)}
            disabled={readOnly}
          />
        </AccordionDetails>
      </Accordion>

      {/* Auto-Summary */}
      <Accordion defaultExpanded>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Typography variant="h6">Sessions 1-3 Summary</Typography>
        </AccordionSummary>
        <AccordionDetails>
          <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
            Auto-generated snapshot of what was captured in Sessions 1-3.
          </Typography>
          {!readOnly && (
            <Button
              size="small"
              variant="outlined"
              onClick={fetchSummary}
              disabled={loadingSummary}
              sx={{ mb: 2 }}
            >
              {loadingSummary ? "Generating..." : "Refresh Summary"}
            </Button>
          )}
          <Paper variant="outlined" sx={{ p: 2, bgcolor: "grey.50", whiteSpace: "pre-wrap", fontSize: 14 }}>
            {summary || "No summary available. Click Refresh to generate."}
          </Paper>
        </AccordionDetails>
      </Accordion>

      {/* Data Plan */}
      <Accordion defaultExpanded>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>
            <Typography variant="h6">Data Plan</Typography>
            <Chip
              label={`${(data.data_plan || []).length} items`}
              size="small"
              variant="outlined"
            />
          </Box>
        </AccordionSummary>
        <AccordionDetails>
          <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
            Tables and metric views that will be included in the Genie Space. Pre-populated
            from Session 3. Add additional tables or metric views you have created.
          </Typography>
          <EditableTable
            columns={DATA_PLAN_COLS}
            rows={data.data_plan || []}
            onChange={(rows) => onChange("data_plan", rows)}
            readOnly={readOnly}
          />
        </AccordionDetails>
      </Accordion>

      {/* Metric View YAML */}
      <Accordion defaultExpanded={!!metricViewYaml}>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Typography variant="h6">Recommended Metric View</Typography>
        </AccordionSummary>
        <AccordionDetails>
          <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
            Generated metric view YAML from Session 3 based on the SQL expressions defined.
            The analyst should create this metric view in the workspace before requesting approval.
          </Typography>
          {metricViewYaml ? (
            <Paper variant="outlined" sx={{ p: 2, bgcolor: "grey.900", color: "grey.100", fontFamily: "monospace", fontSize: 13, whiteSpace: "pre-wrap", overflowX: "auto" }}>
              {metricViewYaml}
            </Paper>
          ) : (
            <Alert severity="info">
              No metric view YAML has been generated yet. Complete Session 3's SQL Expressions
              section to generate a recommendation.
            </Alert>
          )}
        </AccordionDetails>
      </Accordion>

      {/* COE Approval Controls */}
      {isCoeMember && !readOnly && (
        <>
          <Divider sx={{ my: 3 }} />
          <Accordion defaultExpanded>
            <AccordionSummary expandIcon={<ExpandMoreIcon />}>
              <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>
                <Typography variant="h6">COE Review Controls</Typography>
                <Chip label="COE Only" size="small" color="primary" />
              </Box>
            </AccordionSummary>
            <AccordionDetails>
              <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
                Review the analyst's work above. Approve to unlock Sessions 5 & 6,
                or request changes with specific feedback.
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
                  onClick={() => handleApproval("approved")}
                >
                  Approve
                </Button>
                <Button
                  variant="outlined"
                  color="warning"
                  onClick={() => handleApproval("changes_requested")}
                >
                  Request Changes
                </Button>
              </Box>
            </AccordionDetails>
          </Accordion>
        </>
      )}
    </Box>
  );
}
