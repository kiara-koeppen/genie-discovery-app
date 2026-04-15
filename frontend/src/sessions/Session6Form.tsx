import { Typography, Box, Accordion, AccordionSummary, AccordionDetails, Alert } from "@mui/material";
import ExpandMoreIcon from "@mui/icons-material/ExpandMore";
import EditableTable from "../components/EditableTable";
import type { ColumnDef } from "../types";

const RESULT_COLS: ColumnDef[] = [
  { key: "question_asked", label: "Question Asked (exact phrasing)", type: "textarea" },
  { key: "result", label: "Result", type: "textarea" },
  { key: "pass_fail", label: "Pass/Fail", width: 100, type: "select", options: ["Pass", "Fail"] },
  { key: "business_owner_reaction", label: "Reaction" },
  { key: "failure_diagnosis", label: "Failure Diagnosis", type: "select",
    options: ["Wrong Table", "Wrong Column", "Wrong Values", "Wrong Join", "Wrong Metric", "N/A"] },
  { key: "proposed_fix", label: "Proposed Fix", type: "textarea" },
];

const FIX_COLS: ColumnDef[] = [
  { key: "question", label: "Question" },
  { key: "failure_mode", label: "Failure Mode", type: "select",
    options: ["Wrong Table", "Wrong Column", "Wrong Values", "Wrong Join", "Wrong Metric"] },
  { key: "specific_fix", label: "Specific Fix Needed", type: "textarea" },
  { key: "priority", label: "Priority", width: 100, type: "select", options: ["High", "Med", "Low"] },
  { key: "fixed", label: "Fixed?", width: 90, type: "select", options: ["Yes", "No"] },
];

const BENCHMARK_COLS: ColumnDef[] = [
  { key: "question", label: "Question", type: "textarea" },
  { key: "expected_answer", label: "Expected Answer" },
  { key: "source_of_truth", label: "Source of Truth" },
  { key: "category", label: "Category", width: 140, type: "select", options: ["Layup", "Edge", "Negative", "Ambiguity"] },
];

const PHRASING_COLS: ColumnDef[] = [
  { key: "original_phrasing", label: "What They Typed First" },
  { key: "rephrased_to", label: "What They Rephrased To" },
  { key: "entity_matching_needed", label: "Entity Matching / Instruction Needed", type: "textarea" },
];

interface Props {
  data: Record<string, any>;
  onChange: (section: string, rows: any[]) => void;
  readOnly?: boolean;
}

export default function Session6Form({ data, onChange, readOnly }: Props) {
  const results = data.prototype_results || [];
  const passed = results.filter((r: any) => r.pass_fail === "Pass").length;
  const failed = results.filter((r: any) => r.pass_fail === "Fail").length;
  const total = passed + failed;
  const rate = total > 0 ? Math.round((passed / total) * 100) : 0;

  return (
    <Box>
      <Alert severity="info" sx={{ mb: 2 }}>
        <strong>Session Goal:</strong> Let the business owner test the prototype using their natural language.
        Capture what works, what fails, and what is confusing. Do NOT demo it yourself -- hand the keyboard to the business owner.
      </Alert>

      <Accordion defaultExpanded>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Typography variant="h6">Prototype Review Scorecard</Typography>
        </AccordionSummary>
        <AccordionDetails>
          <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
            For every question the business owner asks, record the result. After each answer,
            ask: "Is this the number you expected? Where would you normally verify this?"
          </Typography>
          <EditableTable
            columns={RESULT_COLS}
            rows={results}
            onChange={(rows) => onChange("prototype_results", rows)}
            readOnly={readOnly}
          />
          {total > 0 && (
            <Box sx={{ mt: 1, p: 1.5, bgcolor: rate >= 80 ? "success.50" : "warning.50", borderRadius: 1 }}>
              <Typography variant="body2">
                <strong>Tested:</strong> {total} &middot; <strong>Passed:</strong> {passed} &middot;
                <strong> Failed:</strong> {failed} &middot; <strong>Pass Rate:</strong> {rate}% / Target: 80%+
              </Typography>
            </Box>
          )}
        </AccordionDetails>
      </Accordion>

      <Accordion defaultExpanded>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Typography variant="h6">Fixes Log</Typography>
        </AccordionSummary>
        <AccordionDetails>
          <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
            For each failure above, document the specific fix. After applying any fix, re-ask the question
            in a new chat, then re-run all benchmarks to check for regressions.
          </Typography>
          <EditableTable
            columns={FIX_COLS}
            rows={data.fixes_log || []}
            onChange={(rows) => onChange("fixes_log", rows)}
            readOnly={readOnly}
          />
        </AccordionDetails>
      </Accordion>

      <Accordion defaultExpanded>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Typography variant="h6">New Benchmarks Captured</Typography>
        </AccordionSummary>
        <AccordionDetails>
          <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
            Questions that worked well during the review should become benchmarks.
          </Typography>
          <EditableTable
            columns={BENCHMARK_COLS}
            rows={data.benchmarks || []}
            onChange={(rows) => onChange("benchmarks", rows)}
            readOnly={readOnly}
          />
        </AccordionDetails>
      </Accordion>

      <Accordion defaultExpanded>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Typography variant="h6">Phrasing & Entity Matching Notes</Typography>
        </AccordionSummary>
        <AccordionDetails>
          <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
            Questions where the business owner hesitated, rephrased, or used unexpected language.
            These reveal entity matching and instruction gaps.
          </Typography>
          <EditableTable
            columns={PHRASING_COLS}
            rows={data.phrasing_notes || []}
            onChange={(rows) => onChange("phrasing_notes", rows)}
            readOnly={readOnly}
          />
        </AccordionDetails>
      </Accordion>
    </Box>
  );
}
