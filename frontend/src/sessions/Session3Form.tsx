import {
  Typography, Box, Accordion, AccordionSummary, AccordionDetails, Alert, Chip,
  TextField, List, ListItem, ListItemText,
} from "@mui/material";
import ExpandMoreIcon from "@mui/icons-material/ExpandMore";
import InfoOutlinedIcon from "@mui/icons-material/InfoOutlined";
import EditableTable from "../components/EditableTable";
import DataSourcesPanel from "../components/DataSourcesPanel";
import type { ColumnDef } from "../types";

const TEXT_INSTR_COLS: ColumnDef[] = [
  { key: "title", label: "Title" },
  { key: "instruction", label: "Instruction", type: "textarea" },
];

const GAP_COLS: ColumnDef[] = [
  { key: "business_question", label: "Business Question", type: "textarea" },
  { key: "data_available", label: "Data Available?", width: 130, type: "select", options: ["Yes", "No", "Partial"] },
  { key: "gap_description", label: "Gap", type: "textarea" },
  { key: "proposed_resolution", label: "Proposed Resolution", type: "textarea" },
];

const SCOPE_COLS: ColumnDef[] = [
  { key: "item", label: "Topic / Question Area", type: "textarea" },
  { key: "in_scope", label: "Scope", width: 130, type: "select", options: ["In Scope", "Out of Scope"] },
  { key: "notes", label: "Notes / Redirect", type: "textarea" },
];

interface Props {
  data: Record<string, any>;
  onChange: (section: string, value: any) => void;
  readOnly?: boolean;
  session1Data?: Record<string, any>;
  session2Data?: Record<string, any>;
  /** True when the current user is a BO (not COE). BOs don't see S3 at all,
   *  but keep the guard so the panel stays read-only if it's ever rendered. */
  isBoOnly?: boolean;
  engagementId?: string;
}

export default function Session3Form({
  data, onChange, readOnly, session1Data, session2Data, isBoOnly,
}: Props) {
  const questions = (session2Data?.question_bank as any[]) || [];
  const reports = (session1Data?.existing_reports as any[]) || [];
  const terms = (session2Data?.vocabulary_metrics as any[]) || [];
  const panelReadOnly = readOnly || isBoOnly;

  return (
    <Box>
      <Alert severity="info" sx={{ mb: 2 }}>
        <strong>Session Goal:</strong> The analyst identifies the source data, records a
        global filter for the Data Architect, and captures the text instructions, data gaps,
        and scope boundaries that shape the space. Metric views are built directly in
        Databricks (Genie Code + the metric-view UI), not here.
      </Alert>

      {/* Data Sources */}
      <Accordion id="section-3-data-sources" defaultExpanded>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>
            <Typography variant="h6">Data Sources</Typography>
            <Chip label={`${(data.data_plan || []).length} selected`} size="small" variant="outlined" />
          </Box>
        </AccordionSummary>
        <AccordionDetails>
          <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
            Identify the tables and views this space will use. The app surfaces any existing
            metric views built on those tables so the team knows what's already available.
          </Typography>
          <DataSourcesPanel
            dataPlan={data.data_plan || []}
            onChangeDataPlan={(next) => onChange("data_plan", next)}
            warehouseId={data.plan_warehouse_id || ""}
            onChangeWarehouseId={panelReadOnly ? undefined : (id) => onChange("plan_warehouse_id", id)}
            readOnly={panelReadOnly}
          />
        </AccordionDetails>
      </Accordion>

      {/* Reference: Sessions 1 & 2 (read-only recap) */}
      {(questions.length > 0 || reports.length > 0 || terms.length > 0) && (
        <Accordion id="section-3-reference">
          <AccordionSummary expandIcon={<ExpandMoreIcon />}>
            <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>
              <InfoOutlinedIcon color="action" fontSize="small" />
              <Typography variant="h6">Reference: Sessions 1 & 2</Typography>
              <Chip label="Read-Only" size="small" variant="outlined" />
            </Box>
          </AccordionSummary>
          <AccordionDetails>
            {questions.length > 0 && (
              <Box sx={{ mb: 2 }}>
                <Typography variant="subtitle2" sx={{ mb: 1 }}>
                  Question Bank ({questions.length})
                </Typography>
                <List dense disablePadding>
                  {questions.map((q, i) => (
                    <ListItem key={i} disableGutters sx={{ alignItems: "flex-start" }}>
                      {q.type && <Chip label={q.type} size="small" sx={{ mr: 1, mt: 0.3 }} />}
                      <ListItemText
                        primary={q.question_text || "(untitled)"}
                        secondary={q.decision_it_drives || undefined}
                      />
                    </ListItem>
                  ))}
                </List>
              </Box>
            )}
            {terms.length > 0 && (
              <Box sx={{ mb: 2 }}>
                <Typography variant="subtitle2" sx={{ mb: 1 }}>
                  Key Terms & Metrics ({terms.length})
                </Typography>
                <List dense disablePadding>
                  {terms.map((t, i) => (
                    <ListItem key={i} disableGutters>
                      <ListItemText
                        primary={t.business_term || "(untitled)"}
                        secondary={[t.what_they_mean, t.synonyms && `Synonyms: ${t.synonyms}`]
                          .filter(Boolean)
                          .join(" — ") || undefined}
                      />
                    </ListItem>
                  ))}
                </List>
              </Box>
            )}
            {reports.length > 0 && (
              <Box>
                <Typography variant="subtitle2" sx={{ mb: 1 }}>
                  Existing Reports ({reports.length})
                </Typography>
                <List dense disablePadding>
                  {reports.map((r, i) => (
                    <ListItem key={i} disableGutters>
                      <ListItemText
                        primary={r.report_name || "(untitled)"}
                        secondary={r.what_it_shows || undefined}
                      />
                    </ListItem>
                  ))}
                </List>
              </Box>
            )}
          </AccordionDetails>
        </Accordion>
      )}

      {/* Global Filter (comment for the Data Architect) */}
      <Accordion id="section-3-global-filter">
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Typography variant="h6">Global Filter</Typography>
        </AccordionSummary>
        <AccordionDetails>
          <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
            A note for the Data Architect describing any filter that should apply to every
            question in this space (e.g. "always filter clinical_contact to ED encounters").
            Free text — this is a comment, not executed by the app.
          </Typography>
          <TextField
            fullWidth
            multiline
            minRows={3}
            placeholder="Describe the global filter for the Data Architect…"
            value={data.global_filter || ""}
            onChange={(e) => onChange("global_filter", e.target.value)}
            disabled={readOnly}
          />
        </AccordionDetails>
      </Accordion>

      {/* Text Instructions */}
      <Accordion id="section-3-text-instructions">
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Typography variant="h6">Text Instructions</Typography>
        </AccordionSummary>
        <AccordionDetails>
          <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
            General guidance for how the space should behave — including how to handle
            out-of-scope questions. Feeds the general instructions built later in Databricks.
          </Typography>
          <EditableTable
            columns={TEXT_INSTR_COLS}
            rows={data.text_instructions || []}
            onChange={(rows) => onChange("text_instructions", rows)}
            readOnly={readOnly}
          />
        </AccordionDetails>
      </Accordion>

      {/* Data Gap Analysis */}
      <Accordion id="section-3-data-gaps">
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Typography variant="h6">Data Gap Analysis</Typography>
        </AccordionSummary>
        <AccordionDetails>
          <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
            Questions the team wants answered that the available data can't fully support yet,
            with a proposed resolution. Flagging a gap here documents a known, accepted limit.
          </Typography>
          <EditableTable
            columns={GAP_COLS}
            rows={data.data_gaps || []}
            onChange={(rows) => onChange("data_gaps", rows)}
            readOnly={readOnly}
          />
        </AccordionDetails>
      </Accordion>

      {/* Scope Boundaries */}
      <Accordion id="section-3-scope-boundaries">
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Typography variant="h6">Scope Boundaries</Typography>
        </AccordionSummary>
        <AccordionDetails>
          <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
            What this space will and won't cover. Out-of-scope areas here should line up with
            the "Out of scope" questions flagged in Session 2.
          </Typography>
          <EditableTable
            columns={SCOPE_COLS}
            rows={data.scope_boundaries || []}
            onChange={(rows) => onChange("scope_boundaries", rows)}
            readOnly={readOnly}
          />
        </AccordionDetails>
      </Accordion>
    </Box>
  );
}
