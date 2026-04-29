import { useEffect, useRef, useState } from "react";
import {
  Typography, Box, Accordion, AccordionSummary, AccordionDetails, Alert,
  TextField, Stack, InputAdornment, Link, Tooltip,
} from "@mui/material";
import ExpandMoreIcon from "@mui/icons-material/ExpandMore";
import LinkIcon from "@mui/icons-material/Link";
import OpenInNewIcon from "@mui/icons-material/OpenInNew";
import { api } from "../api";
import EditableTable from "../components/EditableTable";
import type { ColumnDef } from "../types";

const CONTEXT_QUESTIONS = [
  { question: "What does your team do day-to-day?", why_it_matters: "Scopes the question universe" },
  { question: "What decisions do you make with data?", why_it_matters: "Identifies the high-value questions" },
  { question: "What reports do you use today?", why_it_matters: "Reveals existing metric definitions" },
  { question: "What is painful about the current process?", why_it_matters: "Identifies adoption drivers" },
  { question: "Who else on your team would use this?", why_it_matters: "Sizes the audience and skill range" },
  { question: "What tools/dashboards do you currently rely on?", why_it_matters: "Identifies what the space replaces" },
  { question: "How do you get ad hoc answers today?", why_it_matters: "Reveals the bottleneck Genie solves" },
];

const CONTEXT_COLS: ColumnDef[] = [
  { key: "question", label: "Question", width: 250, type: "textarea" },
  { key: "why_it_matters", label: "Why It Matters for Genie", width: 250, type: "textarea" },
  { key: "response", label: "Notes", type: "textarea" },
];

const PAIN_COLS: ColumnDef[] = [
  { key: "description", label: "Pain Point", type: "textarea" },
];

const REPORT_COLS: ColumnDef[] = [
  { key: "report_name", label: "Report/Dashboard Name" },
  { key: "what_it_shows", label: "What It Shows" },
  { key: "frequency", label: "How Often Used", width: 140, type: "select", options: ["Daily", "Weekly", "Monthly", "Quarterly", "Ad hoc"] },
  { key: "known_issues", label: "Known Issues", type: "textarea" },
];


interface EngagementMeta {
  genie_space_name: string;
  business_owner_name: string;
  business_owner_email: string;
  analyst_name: string;
  analyst_email: string;
  servicenow_ticket_url: string;
}

interface Props {
  data: Record<string, any>;
  onChange: (section: string, rows: any[]) => void;
  readOnly?: boolean;
  engagementId?: string;
  meta?: EngagementMeta;
  onMetaChange?: (field: keyof EngagementMeta, value: string) => void;
  metaError?: string;
}

export default function Session1Form({
  data, onChange, readOnly, engagementId, meta, onMetaChange, metaError,
}: Props) {
  const context = data.business_context?.length
    ? data.business_context
    : CONTEXT_QUESTIONS.map((q) => ({ ...q, response: "" }));

  // Debounced uniqueness check on genie_space_name. Excludes the current
  // engagement so the BO can save without renaming.
  const [nameAvailable, setNameAvailable] = useState<boolean | null>(null);
  const [nameChecking, setNameChecking] = useState(false);
  const checkTimer = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);
  const currentName = meta?.genie_space_name || "";
  useEffect(() => {
    if (!engagementId || !currentName.trim()) {
      setNameAvailable(null);
      return;
    }
    if (checkTimer.current) clearTimeout(checkTimer.current);
    setNameChecking(true);
    checkTimer.current = setTimeout(async () => {
      try {
        const res = await api.checkNameAvailable(currentName.trim(), engagementId);
        setNameAvailable(res.available);
      } catch {
        setNameAvailable(null);
      }
      setNameChecking(false);
    }, 600);
    return () => {
      if (checkTimer.current) clearTimeout(checkTimer.current);
    };
  }, [currentName, engagementId]);

  const isValidUrl = (s: string) => {
    if (!s.trim()) return true;
    try {
      const u = new URL(s);
      return u.protocol === "http:" || u.protocol === "https:";
    } catch {
      return false;
    }
  };
  const snUrl = meta?.servicenow_ticket_url || "";
  const snUrlValid = isValidUrl(snUrl);

  return (
    <Box>
      <Alert severity="info" sx={{ mb: 2 }}>
        <strong>Session Goal:</strong> Understand the team, their workflow, their pain points, and the vocabulary they use.
        You should leave this session knowing enough to scope the Genie Space and begin drafting the question bank.
      </Alert>

      {meta && onMetaChange && (
        <Accordion defaultExpanded>
          <AccordionSummary expandIcon={<ExpandMoreIcon />}>
            <Typography variant="h6">Engagement Info</Typography>
          </AccordionSummary>
          <AccordionDetails>
            <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
              Engagement metadata. Edit any field; changes autosave. The Genie Space
              name must be unique across all engagements.
            </Typography>

            {metaError && (
              <Alert severity="error" sx={{ mb: 2 }}>{metaError}</Alert>
            )}

            <Stack spacing={2}>
              <TextField
                label="ServiceNow Ticket URL"
                value={snUrl}
                onChange={(e) => onMetaChange("servicenow_ticket_url", e.target.value)}
                disabled={readOnly}
                placeholder="https://yourorg.service-now.com/..."
                fullWidth
                size="small"
                error={!snUrlValid}
                helperText={
                  !snUrlValid
                    ? "Enter a valid http(s) URL or leave blank"
                    : "Optional. Paste the link to the originating ServiceNow ticket."
                }
                InputProps={{
                  startAdornment: (
                    <InputAdornment position="start">
                      <LinkIcon fontSize="small" />
                    </InputAdornment>
                  ),
                  endAdornment: snUrl && snUrlValid ? (
                    <InputAdornment position="end">
                      <Tooltip title="Open ticket in new tab">
                        <Link href={snUrl} target="_blank" rel="noopener noreferrer">
                          <OpenInNewIcon fontSize="small" />
                        </Link>
                      </Tooltip>
                    </InputAdornment>
                  ) : null,
                }}
              />

              <TextField
                label="Genie Space Name"
                value={meta.genie_space_name}
                onChange={(e) => onMetaChange("genie_space_name", e.target.value)}
                disabled={readOnly}
                required
                fullWidth
                size="small"
                error={nameAvailable === false}
                helperText={
                  nameChecking
                    ? "Checking availability..."
                    : nameAvailable === false
                      ? "Another engagement already uses this name"
                      : nameAvailable === true
                        ? "Name is available"
                        : "Used as the unique identifier for this engagement"
                }
              />

              <Stack direction={{ xs: "column", sm: "row" }} spacing={2}>
                <TextField
                  label="Business Owner Name"
                  value={meta.business_owner_name}
                  onChange={(e) => onMetaChange("business_owner_name", e.target.value)}
                  disabled={readOnly}
                  fullWidth
                  size="small"
                />
                <TextField
                  label="Business Owner Email"
                  value={meta.business_owner_email}
                  onChange={(e) => onMetaChange("business_owner_email", e.target.value)}
                  disabled={readOnly}
                  fullWidth
                  size="small"
                  type="email"
                />
              </Stack>

              <Stack direction={{ xs: "column", sm: "row" }} spacing={2}>
                <TextField
                  label="Analyst Name"
                  value={meta.analyst_name}
                  onChange={(e) => onMetaChange("analyst_name", e.target.value)}
                  disabled={readOnly}
                  fullWidth
                  size="small"
                />
                <TextField
                  label="Analyst Email"
                  value={meta.analyst_email}
                  onChange={(e) => onMetaChange("analyst_email", e.target.value)}
                  disabled={readOnly}
                  fullWidth
                  size="small"
                  type="email"
                />
              </Stack>
            </Stack>
          </AccordionDetails>
        </Accordion>
      )}

      <Accordion defaultExpanded>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Typography variant="h6">Business Context Discovery</Typography>
        </AccordionSummary>
        <AccordionDetails>
          <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
            Ask each question. Capture their answers in the Notes column. Use their language, not yours.
          </Typography>
          <EditableTable
            columns={CONTEXT_COLS}
            rows={context}
            onChange={(rows) => onChange("business_context", rows)}
            readOnly={readOnly}
          />
        </AccordionDetails>
      </Accordion>

      <Accordion defaultExpanded>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Typography variant="h6">Pain Points</Typography>
        </AccordionSummary>
        <AccordionDetails>
          <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
            Ask: "What are the top frustrations your team has with getting data answers today?"
            Listen for: slow turnaround, inconsistent definitions, manual processes, broken reports, lack of self-service.
          </Typography>
          <EditableTable
            columns={PAIN_COLS}
            rows={data.pain_points || []}
            onChange={(rows) => onChange("pain_points", rows)}
            readOnly={readOnly}
          />
        </AccordionDetails>
      </Accordion>

      <Accordion defaultExpanded>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Typography variant="h6">Existing Reports & Data Sources</Typography>
        </AccordionSummary>
        <AccordionDetails>
          <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
            Capture every report, dashboard, or spreadsheet the team references regularly.
            This inventory tells you what the Genie Space needs to match or replace.
          </Typography>
          <EditableTable
            columns={REPORT_COLS}
            rows={data.existing_reports || []}
            onChange={(rows) => onChange("existing_reports", rows)}
            readOnly={readOnly}
          />
        </AccordionDetails>
      </Accordion>

    </Box>
  );
}
