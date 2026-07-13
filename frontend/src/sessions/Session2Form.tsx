import { Typography, Box, Accordion, AccordionSummary, AccordionDetails, Alert } from "@mui/material";
import ExpandMoreIcon from "@mui/icons-material/ExpandMore";
import EditableTable from "../components/EditableTable";
import type { ColumnDef } from "../types";

// Keep in sync with QUESTION_TYPE_OPTIONS in app.py and the Type dropdown in
// the pre-work template.
const QUESTION_TYPE_OPTIONS = ["Benchmark", "Testing", "Out of scope", "Clarifying"];

const QUESTION_COLS: ColumnDef[] = [
  { key: "question_text", label: "Question", type: "textarea" },
  { key: "type", label: "Type", width: 150, type: "select", options: QUESTION_TYPE_OPTIONS },
  { key: "decision_it_drives", label: "Decision It Drives", type: "textarea" },
];

const VOCAB_COLS: ColumnDef[] = [
  { key: "business_term", label: "Business Term or Metric" },
  { key: "what_they_mean", label: "Definition or How It's Calculated", type: "textarea" },
  { key: "synonyms", label: "Other Names / Synonyms" },
  {
    key: "term_type",
    label: "Type (optional)",
    width: 150,
    type: "select",
    options: ["Metric", "Synonym", "Filter", "Date Logic"],
  },
];

interface Props {
  data: Record<string, any>;
  onChange: (section: string, rows: any[]) => void;
  readOnly?: boolean;
}

export default function Session2Form({ data, onChange, readOnly }: Props) {
  return (
    <Box>
      <Alert severity="info" sx={{ mb: 2 }}>
        <strong>Session Goal:</strong> Define every term and metric precisely in the
        business owner's language, then capture the questions their team needs answered.
        Flag each question's <strong>Type</strong> so it's clear which are benchmarks,
        which are for testing, which are out of scope, and which are clarifying.
      </Alert>

      <Accordion id="section-2-key-terms" defaultExpanded>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Typography variant="h6">Key Terms & Metrics</Typography>
        </AccordionSummary>
        <AccordionDetails>
          <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
            Capture BOTH vocabulary AND metrics here: jargon and abbreviations
            ("SKU", "AOV"), filter logic, date references, AND any number the
            team reports on with its calculation ("Net Revenue = gross sales
            minus returns and discounts"). If it's a term they use OR a number
            they report on, it goes in this table. Capture synonyms so the Data
            Architect can carry them into the metric view.
          </Typography>
          <EditableTable
            columns={VOCAB_COLS}
            rows={data.vocabulary_metrics || []}
            onChange={(rows) => onChange("vocabulary_metrics", rows)}
            readOnly={readOnly}
          />
        </AccordionDetails>
      </Accordion>

      <Accordion id="section-2-question-bank" defaultExpanded>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Typography variant="h6">Question Bank</Typography>
        </AccordionSummary>
        <AccordionDetails>
          <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
            Real questions the team needs answered. For each, set the{" "}
            <strong>Type</strong>:
            <br />
            <strong>Benchmark</strong> — foundational questions the space must answer
            correctly (keep them simple and straightforward). <strong>Testing</strong> —
            questions to exercise the space during MVP development.{" "}
            <strong>Out of scope</strong> — questions this space intentionally won't
            answer (these drive the general text instructions). <strong>Clarifying</strong>{" "}
            — questions Genie should ask back when a request is ambiguous.
          </Typography>
          <EditableTable
            columns={QUESTION_COLS}
            rows={data.question_bank || []}
            onChange={(rows) => onChange("question_bank", rows)}
            readOnly={readOnly}
          />
          {!readOnly && (
            <Typography variant="body2" color="text.secondary" sx={{ mt: 1 }}>
              Total questions: {(data.question_bank || []).length}
            </Typography>
          )}
        </AccordionDetails>
      </Accordion>
    </Box>
  );
}
