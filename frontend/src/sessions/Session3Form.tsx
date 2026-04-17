import { useEffect, useState, useMemo } from "react";
import {
  Typography, Box, Accordion, AccordionSummary, AccordionDetails, Alert, Chip,
  Paper, Table, TableBody, TableCell, TableContainer, TableHead, TableRow,
  Select, MenuItem, Checkbox, ListItemText, LinearProgress, TextField, Button,
  CircularProgress, Stack, FormControl, InputLabel,
  Dialog, DialogTitle, DialogContent, DialogContentText, DialogActions,
} from "@mui/material";
import ExpandMoreIcon from "@mui/icons-material/ExpandMore";
import TableChartIcon from "@mui/icons-material/TableChart";
import InfoOutlinedIcon from "@mui/icons-material/InfoOutlined";
import AutoAwesomeIcon from "@mui/icons-material/AutoAwesome";
import CheckCircleIcon from "@mui/icons-material/CheckCircle";
import EditableTable from "../components/EditableTable";
import ExpandableTextField from "../components/ExpandableTextField";
import { api } from "../api";
import type { ColumnDef } from "../types";

const SQL_EXPR_COLS: ColumnDef[] = [
  { key: "metric_name", label: "Metric Name" },
  { key: "uc_table", label: "Table", type: "uc_table" },
  { key: "sql_code", label: "SQL Code", type: "textarea" },
  { key: "synonyms", label: "Synonyms" },
];

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

const TERM_TYPES = ["Metric", "Synonym", "Filter", "Date Logic"];

interface Props {
  data: Record<string, any>;
  onChange: (section: string, value: any) => void;
  readOnly?: boolean;
  session1Data?: Record<string, any>;
  session2Data?: Record<string, any>;
  engagementId?: string;
}

export default function Session3Form({ data, onChange, readOnly, session1Data, session2Data, engagementId }: Props) {
  const [joins, setJoins] = useState<{ table: string; keys: string }[]>([]);
  const [metricViews, setMetricViews] = useState<string[]>([]);

  // Metric View builder state
  const [mvCatalogs, setMvCatalogs] = useState<string[]>([]);
  const [mvSchemas, setMvSchemas] = useState<string[]>([]);
  const [mvWarehouses, setMvWarehouses] = useState<{ id: string; name: string }[]>([]);
  const [mvCatalog, setMvCatalog] = useState<string>("");
  const [mvSchema, setMvSchema] = useState<string>("");
  const [mvName, setMvName] = useState<string>("");
  const [mvWarehouseId, setMvWarehouseId] = useState<string>("");
  const [mvDrafting, setMvDrafting] = useState(false);
  const [mvCreating, setMvCreating] = useState(false);
  const [mvError, setMvError] = useState<string>("");
  const [mvSuccess, setMvSuccess] = useState<string>("");
  const [mvWarnings, setMvWarnings] = useState<string[]>([]);
  const [mvConflict, setMvConflict] = useState<{ fqn: string; owner: string | null } | null>(null);

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

    // Types that create text instructions
    const INSTR_TYPES = ["Filter", "Date Logic", "Synonym"];

    // Add rows for newly selected types
    for (const type of addedTypes) {
      if (type === "Metric" && !exprs.some((e: any) => e.metric_name === termName)) {
        exprs.push({
          metric_name: termName, uc_table: "", sql_code: "",
          synonyms: vocab?.synonyms || "",
        });
        exprsChanged = true;
      } else if (
        INSTR_TYPES.includes(type) &&
        !instrs.some((i: any) => i.title === termName)
      ) {
        const synonymList = vocab?.synonyms || "";
        const prefill = type === "Synonym" && synonymList
          ? `When users say "${synonymList.split(",").map((s: string) => s.trim()).join('" or "')}", they mean "${termName}".`
          : "";
        instrs.push({ title: termName, instruction: prefill });
        instrsChanged = true;
      }
    }

    // Remove rows for deselected types
    for (const type of removedTypes) {
      if (type === "Metric") {
        const before = exprs.length;
        exprs = exprs.filter((e: any) => e.metric_name !== termName);
        if (exprs.length !== before) exprsChanged = true;
      } else if (INSTR_TYPES.includes(type)) {
        // Only remove if no instruction-producing type is still selected
        const stillHasInstrType = newTypes.some((t) => INSTR_TYPES.includes(t));
        if (!stillHasInstrType) {
          const before = instrs.length;
          instrs = instrs.filter((i: any) => i.title !== termName);
          if (instrs.length !== before) instrsChanged = true;
        }
      }
    }

    if (exprsChanged) onChange("sql_expressions", exprs);
    if (instrsChanged) onChange("text_instructions", instrs);
  };

  // Load catalogs + warehouses once for the MV builder
  useEffect(() => {
    api.listCatalogs().then(setMvCatalogs).catch(() => setMvCatalogs([]));
    api.listWarehouses()
      .then((ws) => setMvWarehouses(ws.map((w) => ({ id: w.id, name: w.name }))))
      .catch(() => setMvWarehouses([]));
  }, []);

  useEffect(() => {
    if (!mvCatalog) { setMvSchemas([]); return; }
    api.listSchemas(mvCatalog).then(setMvSchemas).catch(() => setMvSchemas([]));
  }, [mvCatalog]);

  // Pre-fill MV name from first selected table, if empty
  useEffect(() => {
    if (!mvName && selectedTables[0]) {
      const parts = selectedTables[0].split(".");
      if (parts.length === 3) setMvName(`${parts[2]}_mv`);
    }
  }, [selectedTables, mvName]);

  // Seed a single warehouse if only one available
  useEffect(() => {
    if (!mvWarehouseId && mvWarehouses.length > 0) setMvWarehouseId(mvWarehouses[0].id);
  }, [mvWarehouses, mvWarehouseId]);

  const handleDraftMvYaml = async () => {
    if (!engagementId) return;
    setMvDrafting(true);
    setMvError("");
    setMvSuccess("");
    setMvWarnings([]);
    try {
      const res = await api.draftMetricViewYaml(engagementId, mvWarehouseId);
      onChange("metric_view_yaml", res.yaml);
      if (res.suggested_name && !mvName) setMvName(res.suggested_name);
      if (res.warnings && res.warnings.length > 0) setMvWarnings(res.warnings);
    } catch (e: any) {
      setMvError(e.message || String(e));
    } finally {
      setMvDrafting(false);
    }
  };

  const submitCreateMv = async (overwrite: boolean) => {
    if (!engagementId) return;
    setMvCreating(true);
    try {
      const res = await api.createMetricView(engagementId, {
        catalog: mvCatalog,
        schema: mvSchema,
        name: mvName,
        yaml: data.metric_view_yaml,
        warehouse_id: mvWarehouseId,
        overwrite,
      });
      if (res.success) {
        onChange("metric_view_fqn", res.fqn);
        setMvSuccess(`${overwrite ? "Overwrote" : "Created"} ${res.fqn}`);
        setMvConflict(null);
      } else {
        // 409 exists
        setMvConflict({ fqn: res.fqn, owner: res.owner });
      }
    } catch (e: any) {
      setMvError(e.message || String(e));
    } finally {
      setMvCreating(false);
    }
  };

  const handleCreateMv = async () => {
    if (!engagementId) return;
    setMvError("");
    setMvSuccess("");
    setMvConflict(null);
    if (!mvCatalog || !mvSchema || !mvName || !mvWarehouseId || !data.metric_view_yaml) {
      setMvError("Pick a catalog, schema, warehouse, and name, and make sure the YAML isn't empty.");
      return;
    }
    await submitCreateMv(false);
  };

  const handleConfirmOverwrite = async () => {
    setMvError("");
    setMvSuccess("");
    await submitCreateMv(true);
  };

  // Enable the "Generate YAML with AI" button once the analyst has done real data mapping
  const mvReady = useMemo(() => {
    const exprs = data.sql_expressions || [];
    const withTable = exprs.filter((e: any) => e.uc_table && e.sql_code);
    return withTable.length >= 1 && selectedTables.length >= 1;
  }, [data.sql_expressions, selectedTables]);

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
              <strong>Synonym</strong> = text instruction (entity matching).{" "}
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

      {/* ---- Global Filter ---- */}
      <Accordion defaultExpanded>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>
            <Typography variant="h6">Global Filter</Typography>
            {data.global_filter && (
              <Chip label="Set" size="small" color="success" variant="outlined" />
            )}
          </Box>
        </AccordionSummary>
        <AccordionDetails>
          <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
            A SQL boolean expression applied to every metric. Use for row-level
            exclusions that apply across the board — e.g., excluding test rows,
            voided records, or out-of-scope categories. This becomes the metric
            view's top-level <code>filter:</code> and is included in the generate-plan prompt.
            Leave blank if none apply.
          </Typography>
          <TextField
            fullWidth
            multiline
            minRows={2}
            placeholder="voided_flag = 'N' AND test_flag = 'N' AND claim_type IN ('Professional', 'Facility')"
            value={data.global_filter || ""}
            onChange={(e) => onChange("global_filter", e.target.value)}
            disabled={readOnly}
            sx={{ "& .MuiInputBase-input": { fontFamily: "monospace", fontSize: 13 } }}
          />
        </AccordionDetails>
      </Accordion>

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
            Each metric becomes a measure on the generated metric view. Pick the UC table,
            then write SQL using table-qualified column names (e.g., <code>claims.initial_decision</code>).
            Put business-rule filters that apply to every metric (e.g., "exclude voided claims") in
            <strong> Global Filter</strong> above — not per row. Rows are auto-added when you classify
            a term as Metric above.
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

      {/* ---- Metric View Builder (LLM-driven) ---- */}
      <Accordion defaultExpanded>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>
            <Typography variant="h6">Metric View (Recommended)</Typography>
            {data.metric_view_fqn && (
              <Chip
                icon={<CheckCircleIcon />}
                label={data.metric_view_fqn}
                size="small"
                color="success"
                variant="outlined"
                sx={{ fontFamily: "monospace", fontSize: 12 }}
              />
            )}
          </Box>
        </AccordionSummary>
        <AccordionDetails>
          <Alert severity="info" sx={{ mb: 2 }}>
            <strong>Why metric views?</strong> They give Genie a reusable, governed semantic
            layer so the same measure is calculated the same way everywhere. Finish mapping
            your data above first, then draft a metric view from that work.
          </Alert>

          {!mvReady && (
            <Alert severity="warning" sx={{ mb: 2 }}>
              Fill in at least one SQL expression with a UC table before drafting a metric view.
              You know the data best - that mapping is what the LLM uses as context.
            </Alert>
          )}

          <Stack direction="row" spacing={1} sx={{ mb: 2, flexWrap: "wrap" }}>
            <Button
              variant="contained"
              startIcon={mvDrafting ? <CircularProgress size={16} /> : <AutoAwesomeIcon />}
              onClick={handleDraftMvYaml}
              disabled={readOnly || !mvReady || mvDrafting || !engagementId}
            >
              {data.metric_view_yaml ? "Redraft YAML with AI" : "Generate YAML with AI"}
            </Button>
            <Typography variant="caption" color="text.secondary" sx={{ alignSelf: "center" }}>
              Uses Sessions 1-3 as context. Always review before creating.
            </Typography>
          </Stack>

          <ExpandableTextField
            value={data.metric_view_yaml || ""}
            onChange={(v) => onChange("metric_view_yaml", v)}
            label="Metric View YAML"
            placeholder="version: 1.1\nsource: catalog.schema.table\n..."
            disabled={readOnly}
            minRows={12}
            monospace
            dialogTitle="Edit Metric View YAML"
          />

          {mvWarnings.length > 0 && (
            <Alert severity="warning" sx={{ mt: 2 }}>
              <strong>Sanity-check flagged these issues in the draft YAML:</strong>
              <Box component="ul" sx={{ mb: 0, mt: 0.5, pl: 2 }}>
                {mvWarnings.map((w, i) => <li key={i}>{w}</li>)}
              </Box>
            </Alert>
          )}

          {data.metric_view_yaml && !readOnly && (
            <Box sx={{ mt: 3 }}>
              <Typography variant="subtitle2" sx={{ mb: 1 }}>
                Create this Metric View in Unity Catalog
              </Typography>
              <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
                Pick a catalog and schema you have <code>CREATE TABLE</code> permission on.
                This runs <code>CREATE OR REPLACE VIEW ... WITH METRICS LANGUAGE YAML</code> as
                you, so your UC permissions apply.
              </Typography>

              <Stack direction="row" spacing={2} sx={{ mb: 2, flexWrap: "wrap" }}>
                <FormControl size="small" sx={{ minWidth: 200 }}>
                  <InputLabel>Catalog</InputLabel>
                  <Select
                    label="Catalog"
                    value={mvCatalog}
                    onChange={(e) => { setMvCatalog(e.target.value); setMvSchema(""); }}
                  >
                    {mvCatalogs.map((c) => <MenuItem key={c} value={c}>{c}</MenuItem>)}
                  </Select>
                </FormControl>

                <FormControl size="small" sx={{ minWidth: 200 }} disabled={!mvCatalog}>
                  <InputLabel>Schema</InputLabel>
                  <Select
                    label="Schema"
                    value={mvSchema}
                    onChange={(e) => setMvSchema(e.target.value)}
                  >
                    {mvSchemas.map((s) => <MenuItem key={s} value={s}>{s}</MenuItem>)}
                  </Select>
                </FormControl>

                <TextField
                  size="small"
                  label="View Name"
                  value={mvName}
                  onChange={(e) => setMvName(e.target.value.replace(/[^a-zA-Z0-9_]/g, "_"))}
                  sx={{ minWidth: 200 }}
                />

                <FormControl size="small" sx={{ minWidth: 220 }}>
                  <InputLabel>Warehouse</InputLabel>
                  <Select
                    label="Warehouse"
                    value={mvWarehouseId}
                    onChange={(e) => setMvWarehouseId(e.target.value)}
                  >
                    {mvWarehouses.map((w) => (
                      <MenuItem key={w.id} value={w.id}>{w.name}</MenuItem>
                    ))}
                  </Select>
                </FormControl>
              </Stack>

              <Button
                variant="contained"
                color="primary"
                onClick={handleCreateMv}
                disabled={mvCreating || !mvCatalog || !mvSchema || !mvName || !mvWarehouseId}
                startIcon={mvCreating ? <CircularProgress size={16} /> : null}
              >
                {mvCreating ? "Creating..." : "Create Metric View"}
              </Button>

              {mvSuccess && (
                <Alert severity="success" sx={{ mt: 2 }}>
                  {mvSuccess}. It has been added to your Session 4 data plan as a Metric View.
                </Alert>
              )}
              {mvError && (
                <Alert severity="error" sx={{ mt: 2 }}>
                  {mvError}
                </Alert>
              )}
            </Box>
          )}
        </AccordionDetails>
      </Accordion>

      <Dialog open={!!mvConflict} onClose={() => setMvConflict(null)} maxWidth="sm" fullWidth>
        <DialogTitle>Metric view already exists</DialogTitle>
        <DialogContent>
          <DialogContentText component="div">
            <Box sx={{ mb: 1 }}>
              <code>{mvConflict?.fqn}</code> already exists
              {mvConflict?.owner ? (
                <> — owned by <code>{mvConflict.owner}</code></>
              ) : null}
              .
            </Box>
            <Box sx={{ mb: 1 }}>
              Overwriting will replace its YAML definition with the draft above.
              You need <strong>MANAGE</strong> or ownership on the view for this
              to succeed. If you don't have permission, the overwrite will fail
              with a UC permissions error.
            </Box>
          </DialogContentText>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setMvConflict(null)} disabled={mvCreating}>
            Cancel
          </Button>
          <Button
            variant="contained"
            color="warning"
            onClick={handleConfirmOverwrite}
            disabled={mvCreating}
            startIcon={mvCreating ? <CircularProgress size={16} /> : null}
          >
            Overwrite
          </Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
}
