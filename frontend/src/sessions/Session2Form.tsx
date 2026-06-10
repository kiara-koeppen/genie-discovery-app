import { Typography, Box, Accordion, AccordionSummary, AccordionDetails, Alert, Divider } from "@mui/material";
import ExpandMoreIcon from "@mui/icons-material/ExpandMore";
import EditableTable from "../components/EditableTable";
import type { ColumnDef } from "../types";

const QUESTION_COLS: ColumnDef[] = [
  { key: "question_text", label: "Question", type: "textarea" },
  { key: "decision_it_drives", label: "Decision It Drives", type: "textarea" },
];

// Keep these options in sync with TERM_TYPES in Session3Form.tsx — a Type set
// here seeds the Session 3 classification for that term.
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
        <strong>Session Goal:</strong> Capture the questions the business owner's team wants to ask,
        and define every term and metric precisely in their language. You should leave this session with
        a complete vocabulary of the business domain and a prioritized list of questions.
      </Alert>

      <Accordion id="section-2-question-bank" defaultExpanded>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Typography variant="h6">Question Bank</Typography>
        </AccordionSummary>
        <AccordionDetails>
          <Typography variant="body2" color="text.secondary" sx={{ mb: 1 }}>
            Use three approaches to draw out questions:
          </Typography>
          <Box sx={{ mb: 2, pl: 2 }}>
            <Typography variant="body2" color="text.secondary">
              <strong>1. Anchor to a Real Moment:</strong> "Think about last Tuesday. What was the first data question you needed answered?"
            </Typography>
            <Typography variant="body2" color="text.secondary">
              <strong>2. Walk Through the Report:</strong> "Let's go through your weekly report cell by cell. This number here -- how is it calculated?"
            </Typography>
            <Typography variant="body2" color="text.secondary">
              <strong>3. Chain of Command:</strong> "When your director asks you for a number, what does she usually want to know?"
            </Typography>
          </Box>
          <Divider sx={{ mb: 2 }} />
          <EditableTable
            columns={QUESTION_COLS}
            rows={data.question_bank || []}
            onChange={(rows) => onChange("question_bank", rows)}
            readOnly={readOnly}
          />
          {!readOnly && (
            <Typography variant="body2" color="text.secondary" sx={{ mt: 1 }}>
              Total questions: {(data.question_bank || []).length} / Target: 20+
            </Typography>
          )}
        </AccordionDetails>
      </Accordion>

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
            they report on, it goes in this table.
          </Typography>
          <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
            <strong>Type</strong> is optional here — set it if you already know
            whether a term is a Metric, Synonym, Filter, or Date Logic. It pre-fills
            the classification in <strong>Session 3</strong>, where you can refine it
            (a term can have more than one type there). Leave it blank to classify
            entirely in Session 3.
          </Typography>
          <EditableTable
            columns={VOCAB_COLS}
            rows={data.vocabulary_metrics || []}
            onChange={(rows) => onChange("vocabulary_metrics", rows)}
            readOnly={readOnly}
          />
        </AccordionDetails>
      </Accordion>
    </Box>
  );
}
