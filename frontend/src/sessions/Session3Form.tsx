import { useEffect, useState, useMemo } from "react";
import {
  Typography, Box, Accordion, AccordionSummary, AccordionDetails, Alert, Chip,
  Paper, Table, TableBody, TableCell, TableContainer, TableHead, TableRow,
  Select, MenuItem, Checkbox, ListItemText, LinearProgress,
} from "@mui/material";
import ExpandMoreIcon from "@mui/icons-material/ExpandMore";
import TableChartIcon from "@mui/icons-material/TableChart";
import InfoOutlinedIcon from "@mui/icons-material/InfoOutlined";
import EditableTable from "../components/EditableTable";
import type { ColumnDef } from "../types";

const SQL_EXPR_COLS: ColumnDef[] = [
  { key: "metric_name", label: "Metric Name" },
  { key: "uc_table", label: "Table", type: "uc_table" },
  { key: "sql_code", label: "SQL Code", type: "textarea" },
  { key: "synonyms", label: "Synonyms" },
  { key: "instructions", label: "Genie Instructions", type: "textarea" },
];

const TEXT_INSTR_COLS: ColumnDef[] = [
  { key: "title", label: "Title" },
  { key: "instruction", label: "Instruction", type: "textarea" },
];

const GAP_COLS: ColumnDef[] = [
  { key: "business_question", label: "Business Question", type: "textarea" },
  { key: "data_available", label: "Data Available?", width: 130, type: "select", options: ["Yes", "No", "Partial"] },
  { key: "gap_description", label: "Gap" },
  { key: "proposed_resolution", label: "Proposed Resolution", type: "textarea" },
];

const SCOPE_COLS: ColumnDef[] = [
  { key: "item", label: "Topic / Question Area" },
  { key: "in_scope", label: "Scope", width: 130, type: "select", options: ["In Scope", "Out of Scope"] },
  { key: "notes", label: "Notes / Redirect" },
];

const TERM_TYPES = ["Metric", "Filter", "Date Logic"];

interface Props {
  data: Record<string, any>;
  onChange: (section: string, rows: any[]) => void;
  readOnly?: boolean;
  session1Data?: Record<string, any>;
  session2Data?: Record<string, any>;
}

export default function Session3Form({ data, onChange, readOnly, session1Data, session2Data }: Props) {
  const [joins, setJoins] = useState<{ table: string; keys: string }[]>([]);
  const [metricViews, setMetricViews] = useState<string[]>([]);

  // Session 2 vocabulary
  const vocabTerms = useMemo(
    () => (session2Data?.vocabulary_metrics || []).filter((v: any) => v.business_term),
    [session2Data],
  );

  // Classification lookup: term -> types[]
  const typeMap = useMemo(() => {
    const map = new Map<string, string[]>();
    (data.term_classifications || []).forEach((c: any) =>
      map.set(c.business_term, c.types || []),
    );
    return map;
  }, [data.term_classifications]);

  const classifiedCount = useMemo(
    () => vocabTerms.filter((v: any) => (typeMap.get(v.business_term) || []).length > 0).length,
    [vocabTerms, typeMap],
  );

  // Derive unique tables from sql_expressions
  const selectedTables = useMemo(() => {
    const tables = new Set<string>();
    (data.sql_expressions || []).forEach((e: any) => {
      if (e.uc_table && e.uc_table.split(".").length === 3) tables.add(e.uc_table);
    });
    return Array.from(tables);
  }, [data.sql_expressions]);

  // Auto-detect PK/FK joins
  useEffect(() => {
    if (selectedTables.length < 2) { setJoins([]); return; }
    const params = selectedTables.map((t) => `table=${encodeURIComponent(t)}`).join("&");
    fetch(`/api/uc/joins?${params}`)
      .then((r) => r.json())
      .then(setJoins)
      .catch(() => setJoins([]));
  }, [selectedTables]);

  // Detect existing metric views
  useEffect(() => {
    if (selectedTables.length === 0) { setMetricViews([]); return; }
    const schemaSet = new Set<string>();
    selectedTables.forEach((t) => schemaSet.add(t.split(".").slice(0, 2).join(".")));
    Promise.all(
      Array.from(schemaSet).map((s) =>
        fetch(`/api/uc/metric-views?catalog_schema=${encodeURIComponent(s)}`)
          .then((r) => r.json())
          .catch(() => []),
      ),
    ).then((results) => setMetricViews(results.flat()));
  }, [selectedTables]);

  // --- Classification handler (multi-type) ---
  const handleClassify = (termName: string, newTypes: string[]) => {
    // Update classifications
    const classifications = [...(data.term_classifications || [])];
    const idx = classifications.findIndex((c: any) => c.business_term === termName);
    const oldTypes: string[] = idx >= 0 ? (classifications[idx].types || []) : [];

    if (idx >= 0) classifications[idx] = { business_term: termName, types: newTypes };
    else classifications.push({ business_term: termName, types: newTypes });
    onChange("term_classifications", classifications);

    const addedTypes = newTypes.filter((t) => !oldTypes.includes(t));
    const removedTypes = oldTypes.filter((t) => !newTypes.includes(t));

    if (addedTypes.length === 0 && removedTypes.length === 0) return;

    const vocab = vocabTerms.find((v: any) => v.business_term === termName);

    // Build changes locally to avoid stale-state issues across multiple onChange calls
    let exprs = [...(data.sql_expressions || [])];
    let instrs = [...(data.text_instructions || [])];
    let exprsChanged = false;
    let instrsChanged = false;

    // Add rows for newly selected types
    for (const type of addedTypes) {
      if (type === "Metric" && !exprs.some((e: any) => e.metric_name === termName)) {
        exprs.push({
          metric_name: termName, uc_table: "", sql_code: "",
          synonyms: vocab?.synonyms || "", instructions: "",
        });
        exprsChanged = true;
      } else if (
        (type === "Filter" || type === "Date Logic") &&
        !instrs.some((i: any) => i.title === termName)
      ) {
        instrs.push({ title: termName, instruction: "" });
        instrsChanged = true;
      }
    }

    // Remove rows for deselected types
    for (const type of removedTypes) {
      if (type === "Metric") {
        const before = exprs.length;
        exprs = exprs.filter((e: any) => e.metric_name !== termName);
        if (exprs.length !== before) exprsChanged = true;
      } else if (type === "Filter" || type === "Date Logic") {
        // Only remove if neither Filter nor Date Logic is still selected
        const stillHasFilterOrDateLogic = newTypes.includes("Filter") || newTypes.includes("Date Logic");
        if (!stillHasFilterOrDateLogic) {
          const before = instrs.length;
          instrs = instrs.filter((i: any) => i.title !== termName);
          if (instrs.length !== before) instrsChanged = true;
        }
      }
    }

    if (exprsChanged) onChange("sql_expressions", exprs);
    if (instrsChanged) onChange("text_instructions", instrs);
  };

  // Reference data
  const questions = session2Data?.question_bank || [];
  const reports = session1Data?.existing_reports || [];

  // Implementation section counts
  const metricCount = (data.sql_expressions || []).length;
  const instrCount = (data.text_instructions || []).length;

  return (
    <Box>
      <Alert severity="info" sx={{ mb: 2 }}>
        <strong>Session Goal:</strong> This is your solo technical work. Classify each business term
        from Session 2, then implement it: metrics get SQL expressions with a UC table,
        and filters/date logic get text instructions.
      </Alert>

      {/* ---- Reference Panel ---- */}
      {(questions.length > 0 || reports.length > 0) && (
        <Accordion>
          <AccordionSummary expandIcon={<ExpandMoreIcon />}>
            <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>
              <InfoOutlinedIcon color="action" fontSize="small" />
              <Typography variant="h6">Reference: Sessions 1 & 2</Typography>
              <Chip label="Read-Only" size="small" variant="outlined" />
            </Box>
          </AccordionSummary>
          <AccordionDetails>
            <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
              Review the questions and reports from your business owner sessions.
            </Typography>
            {questions.length > 0 && (
              <Box sx={{ mb: 3 }}>
                <Typography variant="subtitle2" sx={{ mb: 1 }}>
                  Question Bank ({questions.length} questions)
                </Typography>
                <TableContainer component={Paper} variant="outlined">
                  <Table size="small">
                    <TableHead>
                      <TableRow sx={{ bgcolor: "grey.50" }}>
                        <TableCell sx={{ fontWeight: 600 }}>Question</TableCell>
                        <TableCell sx={{ fontWeight: 600 }}>Decision It Drives</TableCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {questions.map((q: any, i: number) => (
                        <TableRow key={i}>
                          <TableCell sx={{ fontSize: 13 }}>{q.question_text}</TableCell>
                          <TableCell sx={{ fontSize: 13 }}>{q.decision_it_drives}</TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </TableContainer>
              </Box>
            )}
            {reports.length > 0 && (
              <Box>
                <Typography variant="subtitle2" sx={{ mb: 1 }}>
                  Existing Reports ({reports.length})
                </Typography>
                <TableContainer component={Paper} variant="outlined">
                  <Table size="small">
                    <TableHead>
                      <TableRow sx={{ bgcolor: "grey.50" }}>
                        <TableCell sx={{ fontWeight: 600 }}>Report Name</TableCell>
                        <TableCell sx={{ fontWeight: 600 }}>What It Shows</TableCell>
                        <TableCell sx={{ fontWeight: 600 }}>Known Issues</TableCell>
                      </TableRow>
                    </TableHead>
                    <TableBody>
                      {reports.map((r: any, i: number) => (
                        <TableRow key={i}>
                          <TableCell sx={{ fontSize: 13 }}>{r.report_name}</TableCell>
                          <TableCell sx={{ fontSize: 13 }}>{r.what_it_shows}</TableCell>
                          <TableCell sx={{ fontSize: 13 }}>{r.known_issues}</TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </TableContainer>
              </Box>
            )}
          </AccordionDetails>
        </Accordion>
      )}

      {/* ---- Classify Terms ---- */}
      {vocabTerms.length > 0 ? (
        <Accordion defaultExpanded>
          <AccordionSummary expandIcon={<ExpandMoreIcon />}>
            <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>
              <Typography variant="h6">Classify Terms</Typography>
              <Chip
                label={`${classifiedCount} / ${vocabTerms.length} classified`}
                size="small"
                color={classifiedCount === vocabTerms.length ? "success" : "default"}
                variant="outlined"
              />
            </Box>
          </AccordionSummary>
          <AccordionDetails>
            <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
              Here are the business terms from Session 2. Classify each one (a term can have multiple types).
              Selecting a type auto-populates it into the matching section below.
              <strong> Metric</strong> = SQL expression.{" "}
              <strong>Filter / Date Logic</strong> = text instruction.
            </Typography>

            <Box sx={{ mb: 2 }}>
              <LinearProgress
                variant="determinate"
                value={vocabTerms.length > 0 ? (classifiedCount / vocabTerms.length) * 100 : 0}
                sx={{ height: 8, borderRadius: 4 }}
              />
            </Box>

            <TableContainer component={Paper} variant="outlined">
              <Table size="small">
                <TableHead>
                  <TableRow sx={{ bgcolor: "grey.50" }}>
                    <TableCell sx={{ fontWeight: 600 }}>Business Term</TableCell>
                    <TableCell sx={{ fontWeight: 600 }}>What They Mean</TableCell>
                    <TableCell sx={{ fontWeight: 600 }}>Synonyms</TableCell>
                    <TableCell sx={{ fontWeight: 600, width: 200 }}>Type(s)</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {vocabTerms.map((v: any, i: number) => {
                    const types = typeMap.get(v.business_term) || [];
                    return (
                      <TableRow key={i} hover>
                        <TableCell sx={{ fontSize: 14, fontWeight: 500 }}>{v.business_term}</TableCell>
                        <TableCell sx={{ fontSize: 13, color: "text.secondary" }}>{v.what_they_mean}</TableCell>
                        <TableCell sx={{ fontSize: 13, color: "text.secondary" }}>{v.synonyms}</TableCell>
                        <TableCell>
                          {readOnly ? (
                            <span>{types.length > 0 ? types.join(", ") : "--"}</span>
                          ) : (
                            <Select
                              multiple
                              size="small"
                              fullWidth
                              value={types}
                              onChange={(e) => handleClassify(v.business_term, e.target.value as string[])}
                              displayEmpty
                              renderValue={(selected) => {
                                const sel = selected as string[];
                                if (sel.length === 0) return <span style={{ color: "#999" }}>--</span>;
                                return sel.join(", ");
                              }}
                            >
                              {TERM_TYPES.map((t) => (
                                <MenuItem key={t} value={t}>
                                  <Checkbox size="small" checked={types.includes(t)} />
                                  <ListItemText primary={t} />
                                </MenuItem>
                              ))}
                            </Select>
                          )}
                        </TableCell>
                      </TableRow>
                    );
                  })}
                </TableBody>
              </Table>
            </TableContainer>
          </AccordionDetails>
        </Accordion>
      ) : (
        <Alert severity="warning" sx={{ mb: 2 }}>
          No vocabulary terms from Session 2 yet. Complete Session 2 first to populate terms here.
        </Alert>
      )}

      {/* ---- SQL Expressions (Metrics) ---- */}
      <Accordion defaultExpanded>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>
            <Typography variant="h6">SQL Expressions</Typography>
            {metricCount > 0 && (
              <Chip label={`${metricCount}`} size="small" variant="outlined" />
            )}
          </Box>
        </AccordionSummary>
        <AccordionDetails>
          <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
            Each metric maps to a Genie Space "Common SQL Expression." Pick the UC table,
            then write SQL using table-qualified column names (e.g., <code>claims.initial_decision</code>).
            <strong> Genie Instructions</strong> tells Genie when/how to use this metric
            (e.g., "Use this metric when the user asks about denial rates. Always filter to completed claims only.").
            Rows are auto-added when you classify a term as Metric above.
          </Typography>
          <EditableTable
            columns={SQL_EXPR_COLS}
            rows={data.sql_expressions || []}
            onChange={(rows) => onChange("sql_expressions", rows)}
            readOnly={readOnly}
          />
        </AccordionDetails>
      </Accordion>

      {/* ---- Text Instructions ---- */}
      <Accordion defaultExpanded>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>
            <Typography variant="h6">Text Instructions</Typography>
            {instrCount > 0 && (
              <Chip label={`${instrCount}`} size="small" variant="outlined" />
            )}
          </Box>
        </AccordionSummary>
        <AccordionDetails>
          <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
            Instructions for how Genie should handle filters, date logic, or other rules
            that can't be expressed as SQL.
            Rows are auto-added when you classify a term as Filter or Date Logic above.
          </Typography>
          <EditableTable
            columns={TEXT_INSTR_COLS}
            rows={data.text_instructions || []}
            onChange={(rows) => onChange("text_instructions", rows)}
            readOnly={readOnly}
          />
        </AccordionDetails>
      </Accordion>

      {/* ---- Table Summary (always visible) ---- */}
      <Accordion defaultExpanded>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>
            <Typography variant="h6">Table Summary</Typography>
            <Chip
              icon={<TableChartIcon />}
              label={`${selectedTables.length} table${selectedTables.length === 1 ? "" : "s"}`}
              color={
                selectedTables.length === 0
                  ? "default"
                  : selectedTables.length <= 5
                    ? "success"
                    : selectedTables.length <= 10
                      ? "warning"
                      : "error"
              }
              variant="outlined"
              size="small"
            />
          </Box>
        </AccordionSummary>
        <AccordionDetails>
          {selectedTables.length === 0 ? (
            <Typography variant="body2" color="text.secondary">
              No tables identified yet. As you select UC tables and columns in the sections above,
              they will appear here with join detection and metric view recommendations.
            </Typography>
          ) : (
            <>
              <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
                Auto-derived from the tables and columns referenced in your implementations above.
              </Typography>

              <Box sx={{ display: "flex", flexWrap: "wrap", gap: 0.5, mb: 2 }}>
                {selectedTables.map((tbl) => (
                  <Chip
                    key={tbl}
                    label={tbl}
                    size="small"
                    sx={{ fontFamily: "monospace", fontSize: 12 }}
                  />
                ))}
              </Box>

              {selectedTables.length > 5 && (
                <Alert severity="warning" sx={{ mb: 1 }}>
                  <strong>Consider Metric Views.</strong> You have {selectedTables.length} tables.
                  Genie Spaces perform best with fewer, well-structured tables. Use metric views
                  to pre-aggregate calculations and reduce the number of raw tables.
                </Alert>
              )}

              {joins.length > 0 && (
                <Alert severity="success" sx={{ mb: 1 }}>
                  <strong>Detected join relationships:</strong>
                  <Box component="ul" sx={{ mb: 0, mt: 0.5, pl: 2 }}>
                    {joins.map((j, i) => (
                      <li key={i}>
                        <Typography variant="body2" sx={{ fontFamily: "monospace", fontSize: 13 }}>
                          {j.table} -- {j.keys}
                        </Typography>
                      </li>
                    ))}
                  </Box>
                </Alert>
              )}

              {metricViews.length > 0 && (
                <Alert severity="info" sx={{ mt: 1 }}>
                  <strong>Existing metric views found:</strong>
                  <Box component="ul" sx={{ mb: 0, mt: 0.5, pl: 2 }}>
                    {metricViews.map((mv, i) => (
                      <li key={i}>
                        <Typography variant="body2" sx={{ fontFamily: "monospace", fontSize: 13 }}>
                          {mv}
                        </Typography>
                      </li>
                    ))}
                  </Box>
                  Consider using these instead of raw tables where they cover your metrics.
                </Alert>
              )}
            </>
          )}
        </AccordionDetails>
      </Accordion>

      {/* ---- Data Gap Analysis ---- */}
      <Accordion defaultExpanded>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Typography variant="h6">Data Gap Analysis</Typography>
        </AccordionSummary>
        <AccordionDetails>
          <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
            Identify gaps -- questions the business owner asked that the data cannot answer.
            The business owner must approve workarounds before you build.
          </Typography>
          <EditableTable
            columns={GAP_COLS}
            rows={data.data_gaps || []}
            onChange={(rows) => onChange("data_gaps", rows)}
            readOnly={readOnly}
          />
        </AccordionDetails>
      </Accordion>

      {/* ---- Scope Boundaries ---- */}
      <Accordion defaultExpanded>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Typography variant="h6">Scope Boundaries</Typography>
        </AccordionSummary>
        <AccordionDetails>
          <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
            Document what the space does and does not cover. Out-of-scope items become text
            instructions telling Genie what NOT to answer.
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
