import {
  Typography, Box, Accordion, AccordionSummary, AccordionDetails, Alert,
  TextField, InputAdornment, Link, Tooltip,
} from "@mui/material";
import ExpandMoreIcon from "@mui/icons-material/ExpandMore";
import LinkIcon from "@mui/icons-material/Link";
import OpenInNewIcon from "@mui/icons-material/OpenInNew";
import EditableTable from "../components/EditableTable";
import type { ColumnDef } from "../types";

const CONTEXT_QUESTIONS = [
  { question: "What does your team do day-to-day?", why_it_matters: "Scopes the question universe" },
  { question: "What decisions do you make with data?", why_it_matters: "Identifies the high-value questions" },
  { question: "Who else on your team would use this?", why_it_matters: "Sizes the audience and skill range" },
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

const isValidHttpUrl = (s: string) => {
  if (!s.trim()) return true;
  try {
    const u = new URL(s);
    return u.protocol === "http:" || u.protocol === "https:";
  } catch {
    return false;
  }
};


interface Props {
  data: Record<string, any>;
  onChange: (section: string, rows: any[]) => void;
  readOnly?: boolean;
  serviceNowUrl?: string;
  onServiceNowUrlChange?: (value: string) => void;
}

export default function Session1Form({
  data, onChange, readOnly, serviceNowUrl, onServiceNowUrlChange,
}: Props) {
  const context = data.business_context?.length
    ? data.business_context
    : CONTEXT_QUESTIONS.map((q) => ({ ...q, response: "" }));

  const url = serviceNowUrl || "";
  const urlValid = isValidHttpUrl(url);
  const urlEditable = !readOnly && !!onServiceNowUrlChange;

  return (
    <Box>
      <Alert severity="info" sx={{ mb: 2 }}>
        <strong>Session Goal:</strong> Understand the team, their workflow, their pain points, and the vocabulary they use.
        You should leave this session knowing enough to scope the Genie Space and begin drafting the question bank.
      </Alert>

      {urlEditable && (
        <Box sx={{ mb: 2 }}>
          <TextField
            label="ServiceNow Ticket URL"
            value={url}
            onChange={(e) => onServiceNowUrlChange?.(e.target.value)}
            placeholder="https://yourorg.service-now.com/..."
            fullWidth
            size="small"
            error={!urlValid}
            helperText={
              !urlValid
                ? "Enter a valid http(s) URL or leave blank"
                : "Optional. Link to the originating ServiceNow ticket. Autosaves."
            }
            InputProps={{
              startAdornment: (
                <InputAdornment position="start">
                  <LinkIcon fontSize="small" />
                </InputAdornment>
              ),
              endAdornment: url && urlValid ? (
                <InputAdornment position="end">
                  <Tooltip title="Open ticket in new tab">
                    <Link href={url} target="_blank" rel="noopener noreferrer">
                      <OpenInNewIcon fontSize="small" />
                    </Link>
                  </Tooltip>
                </InputAdornment>
              ) : null,
            }}
          />
        </Box>
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
