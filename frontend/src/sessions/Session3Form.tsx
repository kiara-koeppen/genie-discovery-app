import { useEffect, useState, useMemo, Fragment, type ReactNode } from "react";
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
import RestoreIcon from "@mui/icons-material/Restore";
import EditableTable from "../components/EditableTable";
import ExpandableTextField from "../components/ExpandableTextField";
import UCColumnPicker from "../components/UCColumnPicker";
import DataSourcesPanel from "../components/DataSourcesPanel";
import ConfirmDialog from "../components/ConfirmDialog";
import CompareRestoreDialog from "../components/CompareRestoreDialog";
import { api } from "../api";
import type { ColumnDef, SynonymTarget } from "../types";

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

// #7: analyst-authored example SQL queries (surfaced verbatim in the Genie space).
const EXAMPLE_QUERY_COLS: ColumnDef[] = [
  { key: "question", label: "Question", type: "textarea" },
  { key: "sql", label: "Example SQL", type: "textarea" },
  { key: "usage_guidance", label: "Usage Guidance (optional)", type: "textarea" },
];

// #2: clarifying / disambiguation questions Genie should ask on ambiguous terms.
const CLARIFYING_COLS: ColumnDef[] = [
  { key: "trigger", label: "When the user asks about…" },
  { key: "clarification", label: "Genie should ask…", type: "textarea" },
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
  /** S4's draft -- needed so the new Data Sources panel can read+write the
   *  data_plan field (which lives in S4's column set today). */
  session4Data?: Record<string, any>;
  /** Writer for S4 fields. The Data Sources panel uses this to update data_plan
   *  from S3. The parent (Engagement.tsx) routes it through to the S4 save path. */
  onChangeSession4?: (section: string, value: any) => void;
  /** Writer for S5 fields. The Data Sources panel uses this for the inline
   *  warehouse picker (which persists to plan_warehouse_id so the same
   *  warehouse carries through to Generate Plan). */
  onChangeSession5?: (section: string, value: any) => void;
  /** SQL warehouse ID for DESCRIBE EXTENDED on metric views + the broad MV
   *  discovery scan. Resolved by the parent from S5's plan_warehouse_id;
   *  empty is OK -- the panel renders an inline picker to set it. */
  warehouseId?: string;
  /** True when the current user is a BO (not COE). Forces read-only on the
   *  Data Sources panel since BOs can't write to S3/S4 fields. */
  isBoOnly?: boolean;
  engagementId?: string;
  /** Called after Create Metric View persists, so the parent can refresh its
   *  optimistic-lock token (updatedAtRef) and avoid a 409 on next autosave. */
  onMetricViewCreated?: (updatedAt: string) => void;
}

export default function Session3Form({
  data, onChange, readOnly, session1Data, session2Data, session4Data,
  onChangeSession4, onChangeSession5, warehouseId, isBoOnly, engagementId,
  onMetricViewCreated,
}: Props) {
  const [joins, setJoins] = useState<{ table: string; keys: string }[]>([]);
  const [metricViews, setMetricViews] = useState<string[]>([]);
  const [showRowFilterDdl, setShowRowFilterDdl] = useState(false);

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
  // Guard the destructive "redraft" (overwrites existing YAML). See ConfirmDialog below.
  const [confirmRedraftOpen, setConfirmRedraftOpen] = useState(false);
  // Side-by-side compare before committing a YAML restore.
  const [compareMvOpen, setCompareMvOpen] = useState(false);

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

  // Data Sources the analyst chose at the top of S3 (lives on S4's data_plan).
  // Used to restrict the SQL Expressions table picker so they don't hunt the
  // full catalog tree (#8). Includes tables + metric views that are in scope.
  const dataSourceFqns = useMemo(
    () =>
      (session4Data?.data_plan || [])
        .filter((d: any) => d.include_in_space === "Yes")
        .map((d: any) => (d.table_or_view || "").trim())
        .filter((t: string) => t && t.split(".").length === 3),
    [session4Data],
  );

  // Build a starter UC row-filter DDL from the global filter + in-scope tables.
  // This is the HARD-enforcement path (enforced on every query, any tool). It's
  // a review-before-run template: column types are guessed (STRING) and every
  // table is assumed to carry the referenced columns.
  const rowFilterDdl = useMemo(() => {
    const predicate = (data.global_filter || "").trim();
    if (!predicate || dataSourceFqns.length === 0) return "";
    // Extract candidate column names: identifiers that aren't SQL keywords and
    // aren't inside string literals.
    const KEYWORDS = new Set([
      "and", "or", "not", "in", "is", "null", "like", "ilike", "rlike", "between",
      "true", "false", "case", "when", "then", "else", "end", "cast", "as",
      "current_date", "current_timestamp", "interval", "date", "timestamp",
    ]);
    const noStrings = predicate.replace(/'[^']*'/g, " ");
    const cols = Array.from(
      new Set(
        (noStrings.match(/[A-Za-z_][A-Za-z0-9_]*/g) || [])
          .filter((t: string) => !KEYWORDS.has(t.toLowerCase()))
          .filter((t: string) => !/^\d/.test(t)),
      ),
    );
    if (cols.length === 0) return "";
    // Define the function in the first data source's catalog.schema.
    const [cat, sch] = dataSourceFqns[0].split(".");
    const fnFqn = `\`${cat}\`.\`${sch}\`.genie_global_filter`;
    const params = cols.map((c) => `${c} STRING`).join(", ");
    const onCols = cols.join(", ");
    const alters = dataSourceFqns
      .map((fqn: string) => {
        const [c, s, t] = fqn.split(".");
        return `ALTER TABLE \`${c}\`.\`${s}\`.\`${t}\` SET ROW FILTER ${fnFqn} ON (${onCols});`;
      })
      .join("\n");
    return [
      "-- Hard enforcement (optional): a UC row filter is applied to EVERY query",
      "-- against these tables, from any tool, per user. Review column names + types",
      "-- (guessed STRING below) and confirm each table has these columns. Run as a",
      "-- table owner. Docs: Row filters and column masks.",
      `CREATE OR REPLACE FUNCTION ${fnFqn}(${params})`,
      "  RETURN " + predicate + ";",
      "",
      alters,
    ].join("\n");
  }, [data.global_filter, dataSourceFqns]);

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

  // --- Auto-row reconciliation ---
  //
  // A term has an auto-generated SQL Expressions row IFF it is classified as
  // Metric. A term has an auto-generated Text Instructions row IFF it is
  // classified as Filter, Date Logic, OR Synonym with kind = general space
  // term (cross_cutting). For column- and value-kind synonyms, NO text_instr
  // row is created -- those route to column_configs at push, not to space-
  // level text instructions.
  //
  // This single invariant replaces the old "track added/removed types and
  // mutate" logic. Whenever classifications or a synonym target change, we
  // call _reconcileAutoRows with the new classification state, and it
  // computes the deltas vs. the current exprs/instrs.
  const _buildAutoInstrPrefill = (vocab: any, termName: string) => {
    const synonymList = vocab?.synonyms || "";
    if (!synonymList) return "";
    const parts = synonymList.split(",").map((s: string) => s.trim()).filter(Boolean);
    if (!parts.length) return "";
    return `When users say "${parts.join('" or "')}", they mean "${termName}".`;
  };

  const _reconcileAutoRows = (
    classifications: any[],
    exprs: any[],
    instrs: any[],
  ): { exprs: any[]; instrs: any[]; changed: boolean } => {
    // Build {termName: types, target} index from the new classification state
    const byTerm = new Map<string, { types: string[]; target?: SynonymTarget }>();
    for (const c of classifications) {
      if (!c?.business_term) continue;
      byTerm.set(c.business_term, {
        types: c.types || [],
        target: c.synonym_target,
      });
    }

    let changed = false;
    let nextExprs = exprs;
    let nextInstrs = instrs;

    for (const v of vocabTerms) {
      const term = v.business_term;
      if (!term) continue;
      const info = byTerm.get(term) || { types: [] };
      const types = info.types;

      // -- Metric row
      const shouldHaveExpr = types.includes("Metric");
      const hasExpr = nextExprs.some((e: any) => e.metric_name === term);
      if (shouldHaveExpr && !hasExpr) {
        nextExprs = [...nextExprs, {
          metric_name: term, uc_table: "", sql_code: "",
          synonyms: v.synonyms || "",
        }];
        changed = true;
      } else if (!shouldHaveExpr && hasExpr) {
        nextExprs = nextExprs.filter((e: any) => e.metric_name !== term);
        changed = true;
      }

      // -- Text instruction row (Filter, Date Logic, or Synonym/cross_cutting)
      const synKind = (info.target?.kind || "cross_cutting");
      const isGeneralSpaceSynonym =
        types.includes("Synonym") && synKind === "cross_cutting";
      const shouldHaveInstr =
        types.includes("Filter") ||
        types.includes("Date Logic") ||
        isGeneralSpaceSynonym;

      const instrIdx = nextInstrs.findIndex((i: any) => i.title === term);
      const hasInstr = instrIdx >= 0;
      if (shouldHaveInstr && !hasInstr) {
        // Pre-fill only for general-space-term synonyms; Filter / Date Logic
        // get an empty instruction (analyst writes the rule).
        const prefill = isGeneralSpaceSynonym
          ? _buildAutoInstrPrefill(v, term)
          : "";
        nextInstrs = [...nextInstrs, { title: term, instruction: prefill }];
        changed = true;
      } else if (!shouldHaveInstr && hasInstr) {
        nextInstrs = nextInstrs.filter((i: any) => i.title !== term);
        changed = true;
      }
    }
    return { exprs: nextExprs, instrs: nextInstrs, changed };
  };

  // --- Classification handler (multi-type) ---
  const handleClassify = (termName: string, newTypes: string[]) => {
    const classifications = [...(data.term_classifications || [])];
    const idx = classifications.findIndex((c: any) => c.business_term === termName);
    const oldTarget: SynonymTarget | undefined =
      idx >= 0 ? classifications[idx].synonym_target : undefined;

    const newRow: any = { business_term: termName, types: newTypes };
    // Preserve existing synonym_target if "Synonym" is still selected; drop
    // it entirely if Synonym was unchecked.
    if (newTypes.includes("Synonym") && oldTarget) {
      newRow.synonym_target = oldTarget;
    }
    if (idx >= 0) classifications[idx] = newRow;
    else classifications.push(newRow);
    onChange("term_classifications", classifications);

    const res = _reconcileAutoRows(
      classifications,
      data.sql_expressions || [],
      data.text_instructions || [],
    );
    if (res.changed) {
      onChange("sql_expressions", res.exprs);
      onChange("text_instructions", res.instrs);
    }
  };

  // --- Synonym target handler ---
  // Stores the routing decision (column / value / general space term) for a
  // term classified as Synonym. Each change re-reconciles auto-rows so a kind
  // flip (e.g. general space term → column) removes the orphaned text_instr
  // entry, and a flip the other direction adds a fresh pre-filled instruction.
  const handleSynonymTarget = (termName: string, patch: Partial<SynonymTarget>) => {
    const classifications = [...(data.term_classifications || [])];
    const idx = classifications.findIndex((c: any) => c.business_term === termName);
    if (idx < 0) return; // shouldn't happen — sub-row only renders when classified
    const current: SynonymTarget = classifications[idx].synonym_target || {
      kind: "cross_cutting",
    };
    const merged: SynonymTarget = { ...current, ...patch };
    if (merged.kind === "cross_cutting") {
      delete merged.column_fqn;
      delete merged.column_value;
    } else if (merged.kind === "column") {
      delete merged.column_value;
    }
    classifications[idx] = { ...classifications[idx], synonym_target: merged };
    onChange("term_classifications", classifications);

    const res = _reconcileAutoRows(
      classifications,
      data.sql_expressions || [],
      data.text_instructions || [],
    );
    if (res.changed) {
      onChange("sql_expressions", res.exprs);
      onChange("text_instructions", res.instrs);
    }
  };

  // Seed classifications from any Type set in Session 2. Runs only for terms
  // that have an S2 `term_type` AND no existing S3 classification entry, so it
  // never overrides a classification the analyst made (or cleared) here. Mirrors
  // handleClassify so auto-rows (e.g. a Metric's SQL Expression row) get created.
  useEffect(() => {
    if (readOnly) return;
    const existing = data.term_classifications || [];
    const haveTerm = new Set(existing.map((c: any) => c.business_term));
    const toSeed = vocabTerms.filter(
      (v: any) => v.term_type && !haveTerm.has(v.business_term),
    );
    if (toSeed.length === 0) return;
    const classifications = [...existing];
    for (const v of toSeed) {
      classifications.push({ business_term: v.business_term, types: [v.term_type] });
    }
    onChange("term_classifications", classifications);
    const res = _reconcileAutoRows(
      classifications,
      data.sql_expressions || [],
      data.text_instructions || [],
    );
    if (res.changed) {
      onChange("sql_expressions", res.exprs);
      onChange("text_instructions", res.instrs);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [vocabTerms, readOnly]);

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
    // Snapshot the current YAML BEFORE the call so the analyst can restore it
    // if the AI redraft isn't what they wanted. `data` doesn't change during
    // the await, so capturing it here is safe.
    const prevYaml = (data.metric_view_yaml || "").trim();
    setMvDrafting(true);
    setMvError("");
    setMvSuccess("");
    setMvWarnings([]);
    try {
      const res = await api.draftMetricViewYaml(engagementId, mvWarehouseId);
      if (prevYaml) onChange("metric_view_yaml_previous", data.metric_view_yaml);
      onChange("metric_view_yaml", res.yaml);
      if (res.suggested_name && !mvName) setMvName(res.suggested_name);
      if (res.warnings && res.warnings.length > 0) setMvWarnings(res.warnings);
    } catch (e: any) {
      setMvError(e.message || String(e));
    } finally {
      setMvDrafting(false);
    }
  };

  // Redraft is destructive (overwrites the current YAML). If there's existing
  // YAML, confirm first; otherwise (first draft) just run it.
  const requestDraftMvYaml = () => {
    if ((data.metric_view_yaml || "").trim()) setConfirmRedraftOpen(true);
    else handleDraftMvYaml();
  };

  // Swap current <-> previous so Restore is a reversible toggle (undo/redo).
  const handleRestoreMvYaml = () => {
    const prev = data.metric_view_yaml_previous || "";
    if (!prev) return;
    const cur = data.metric_view_yaml || "";
    onChange("metric_view_yaml", prev);
    onChange("metric_view_yaml_previous", cur);
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
        // Refresh parent's optimistic-lock token BEFORE the onChange cascade
        // triggers an autosave — otherwise the next autosave 409s.
        if (res.updated_at) onMetricViewCreated?.(res.updated_at);
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
        <strong>Session Goal:</strong> This is your solo technical work. Pick the tables your
        Genie space will use (top), reuse any existing Metric Views that already cover those
        tables, then classify each business term from Session 2 and implement it.
      </Alert>

      {/* ---- Data Sources (top -- the analyst starts here) ---- */}
      <Accordion id="section-3-data-sources" defaultExpanded>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>
            <Typography variant="h6">Data Sources</Typography>
            <Chip
              label={`${(session4Data?.data_plan || []).filter((d: any) => d.include_in_space === "Yes").length} in scope`}
              size="small"
              variant="outlined"
            />
          </Box>
        </AccordionSummary>
        <AccordionDetails>
          <DataSourcesPanel
            dataPlan={session4Data?.data_plan || []}
            onChangeDataPlan={(next) => onChangeSession4?.("data_plan", next)}
            warehouseId={warehouseId || ""}
            onChangeWarehouseId={
              onChangeSession5
                ? (id) => onChangeSession5("plan_warehouse_id", id)
                : undefined
            }
            readOnly={readOnly || isBoOnly || !onChangeSession4}
          />
        </AccordionDetails>
      </Accordion>

      {/* ---- Reference Panel ---- */}
      {(questions.length > 0 || reports.length > 0) && (
        <Accordion id="section-3-reference">
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
        <Accordion id="section-3-classify-terms" defaultExpanded>
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
              <strong>Filter / Date Logic</strong> = text instruction.{" "}
              <strong>Synonym</strong> = route via the kind picker that appears below the row.
            </Typography>
            <Alert severity="info" sx={{ mb: 2 }} variant="outlined">
              <Typography variant="body2">
                <strong>For Synonyms:</strong> after checking the Synonym type, specify whether the
                synonym refers to:
              </Typography>
              <Box component="ul" sx={{ pl: 3, my: 0.5 }}>
                <li><Typography variant="body2"><strong>A column name</strong> — the column itself goes by this synonym. Lands in the column's <em>Synonyms</em> field at push.</Typography></li>
                <li><Typography variant="body2"><strong>A value in a column</strong> — a specific value in the column goes by this synonym. Lands in the column's <em>Description</em> at push (and turns on Entity Matching for the column).</Typography></li>
                <li><Typography variant="body2"><strong>A general space term</strong> — not tied to any column; just team vocabulary. Lands in the space's <em>General Instructions</em> at push.</Typography></li>
              </Box>
              <Typography variant="body2" sx={{ mt: 0.5 }}>
                All three are pushed automatically when you go through Session 5. No manual UI work needed in the Genie space afterwards.
              </Typography>
            </Alert>

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
                    const isSynonym = types.includes("Synonym");
                    // Look up the saved synonym_target for this term (if any)
                    const classification = (data.term_classifications || []).find(
                      (c: any) => c.business_term === v.business_term,
                    );
                    const target: SynonymTarget = classification?.synonym_target || {
                      kind: "cross_cutting",
                    };
                    // Incomplete state: kind picked but the required follow-up field is empty.
                    // Surfaced as a small warning chip so the analyst notices before pushing.
                    const targetIncomplete =
                      isSynonym &&
                      ((target.kind === "column" && !target.column_fqn) ||
                        (target.kind === "value" && (!target.column_fqn || !target.column_value)));

                    return (
                      <Fragment key={i}>
                        <TableRow hover>
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
                        {isSynonym && (
                          <TableRow sx={{ bgcolor: "action.hover" }}>
                            <TableCell colSpan={4} sx={{ py: 1.5, borderTop: "none" }}>
                              <Stack direction="column" spacing={1.5} sx={{ pl: 2 }}>
                                <Stack direction="row" alignItems="center" spacing={1} flexWrap="wrap">
                                  <Typography variant="caption" sx={{ fontWeight: 600, color: "text.secondary", minWidth: 120 }}>
                                    Synonym for:
                                  </Typography>
                                  <FormControl size="small" sx={{ minWidth: 220 }} disabled={readOnly}>
                                    <Select
                                      value={target.kind}
                                      onChange={(e) =>
                                        handleSynonymTarget(v.business_term, {
                                          kind: e.target.value as SynonymTarget["kind"],
                                        })
                                      }
                                    >
                                      <MenuItem value="column">A column name</MenuItem>
                                      <MenuItem value="value">A value in a column</MenuItem>
                                      <MenuItem value="cross_cutting">A general space term</MenuItem>
                                    </Select>
                                  </FormControl>
                                  {targetIncomplete && (
                                    <Chip
                                      size="small"
                                      color="warning"
                                      variant="outlined"
                                      label="Pick a column to finish"
                                    />
                                  )}
                                </Stack>
                                {/* Single dynamic helper line explains both what this kind means
                                    AND where the synonym lands at push. Keeps the dropdown options
                                    short so they don't overflow the row width. */}
                                <Typography variant="caption" color="text.secondary" sx={{ pl: 15, mt: -0.5 }}>
                                  {target.kind === "column" &&
                                    "The column itself goes by this synonym. Lands in the column's Synonyms field at push."}
                                  {target.kind === "value" &&
                                    "A specific value in the column goes by this synonym. Lands in the column's Description at push (and enables Entity Matching on the column)."}
                                  {target.kind === "cross_cutting" &&
                                    "Not tied to any column — just team vocabulary. Lands in the space's General Instructions at push."}
                                </Typography>
                                {(target.kind === "column" || target.kind === "value") && (
                                  <Stack direction="row" alignItems="flex-start" spacing={1} flexWrap="wrap">
                                    <Typography variant="caption" sx={{ fontWeight: 600, color: "text.secondary", minWidth: 120, pt: 1 }}>
                                      Target column:
                                    </Typography>
                                    <UCColumnPicker
                                      value={target.column_fqn || ""}
                                      onChange={(fqn) =>
                                        handleSynonymTarget(v.business_term, { column_fqn: fqn })
                                      }
                                      readOnly={readOnly}
                                    />
                                  </Stack>
                                )}
                                {target.kind === "value" && (
                                  <Stack direction="column" spacing={0.5}>
                                    <Stack direction="row" alignItems="center" spacing={1} flexWrap="wrap">
                                      <Typography variant="caption" sx={{ fontWeight: 600, color: "text.secondary", minWidth: 120 }}>
                                        Column value:
                                      </Typography>
                                      <TextField
                                        size="small"
                                        placeholder="e.g. CANCELLED"
                                        value={target.column_value || ""}
                                        onChange={(e) =>
                                          handleSynonymTarget(v.business_term, {
                                            column_value: e.target.value,
                                          })
                                        }
                                        disabled={readOnly}
                                        sx={{ minWidth: 220 }}
                                      />
                                    </Stack>
                                    <Typography variant="caption" color="text.secondary" sx={{ pl: 15 }}>
                                      The specific value the synonyms refer to.
                                    </Typography>
                                  </Stack>
                                )}
                              </Stack>
                            </TableCell>
                          </TableRow>
                        )}
                      </Fragment>
                    );
                  })}
                </TableBody>
              </Table>
            </TableContainer>

            {/* ---- Synonym Routing Summary (read-only) ---- */}
            {/* Groups every Synonym-classified term by where it'll land at S5
                push. Renders only when at least one term is classified as
                Synonym so blank engagements stay quiet.

                Each row is tagged "will push" vs "will NOT push" using the same
                rules the backend applies in _build_serialized_space:
                  - column kind: needs a 4-part FQN AND that table must be in
                    the data plan (otherwise backend drops silently)
                  - value kind:  same + a non-empty column_value
                  - cross_cutting: always valid
                We surface invalid rows in red with a "Won't push" chip so the
                analyst sees the gap before clicking Push, instead of finding
                out by testing the live Genie space. */}
            {(() => {
              interface SynRow {
                term: string;
                synonyms: string[];
                kind: string;
                column_fqn: string;
                column_value: string;
                /** False if the backend will silently filter this out at push
                 *  time. Mirrors the filter rules in app.py:_build_serialized_space. */
                willPush: boolean;
                /** Human-readable reason; only set when willPush=false. */
                blockReason: string;
              }
              // In-scope table FQNs (must match _build_serialized_space.table_fqns_in_scope).
              const inScopeTableFqns = new Set<string>(
                (session4Data?.data_plan || [])
                  .filter((d: any) => d.include_in_space === "Yes" && d.type !== "Metric View")
                  .map((d: any) => (d.table_or_view || "").trim())
                  .filter((fqn: string) => fqn && fqn.split(".").length === 3),
              );
              const synonymRows: SynRow[] = (data.term_classifications || [])
                .filter((c: any) => (c.types || []).includes("Synonym"))
                .map((c: any) => {
                  const vocab = vocabTerms.find((v: any) => v.business_term === c.business_term);
                  const target = c.synonym_target || { kind: "cross_cutting" };
                  const kind = target.kind;
                  const fqn = (target.column_fqn || "").trim();
                  const colValue = (target.column_value || "").trim();
                  const synonyms = (vocab?.synonyms || "").split(",").map((s: string) => s.trim()).filter(Boolean);

                  let willPush = true;
                  let blockReason = "";
                  if (kind === "column" || kind === "value") {
                    const parts = fqn.split(".");
                    if (!fqn) {
                      willPush = false;
                      blockReason = "no column picked";
                    } else if (parts.length !== 4) {
                      willPush = false;
                      blockReason = "FQN missing column (need catalog.schema.table.column)";
                    } else {
                      const tableFqn = parts.slice(0, 3).join(".");
                      if (!inScopeTableFqns.has(tableFqn)) {
                        willPush = false;
                        blockReason = `${tableFqn} is not in the data plan above`;
                      } else if (kind === "value" && !colValue) {
                        willPush = false;
                        blockReason = "value kind needs a non-empty column value";
                      }
                    }
                    if (willPush && synonyms.length === 0) {
                      willPush = false;
                      blockReason = "no synonyms in S2 vocab";
                    }
                  }
                  return { term: c.business_term, synonyms, kind, column_fqn: fqn, column_value: colValue, willPush, blockReason };
                });
              if (!synonymRows.length) return null;

              const columnEntries = synonymRows.filter((r: SynRow) => r.kind === "column");
              const valueEntries  = synonymRows.filter((r: SynRow) => r.kind === "value");
              const generalEntries = synonymRows.filter((r: SynRow) => r.kind === "cross_cutting");

              const colPush  = columnEntries.filter((r) => r.willPush).length;
              const valPush  = valueEntries.filter((r) => r.willPush).length;
              const genPush  = generalEntries.filter((r) => r.willPush).length;
              const totalRows = synonymRows.length;
              const totalPush = colPush + valPush + genPush;
              const blockedRows = totalRows - totalPush;

              const renderEntries = (entries: SynRow[], fmt: (r: SynRow) => ReactNode) =>
                entries.length === 0 ? null : (
                  <Box component="ul" sx={{ pl: 3, my: 0.5, "& li": { mb: 0.25 } }}>
                    {entries.map((r: SynRow, i: number) => (
                      <li key={i} style={r.willPush ? undefined : { opacity: 0.65 }}>{fmt(r)}</li>
                    ))}
                  </Box>
                );

              // Render a "Won't push: <reason>" chip after the row content.
              const blockChip = (r: SynRow) =>
                r.willPush ? null : (
                  <Chip
                    size="small"
                    color="warning"
                    variant="outlined"
                    label={`Won't push: ${r.blockReason}`}
                    sx={{ ml: 1, height: 18, fontSize: 11 }}
                  />
                );

              return (
                <Box sx={{ mt: 3, p: 2, bgcolor: "action.hover", borderRadius: 1 }}>
                  <Stack direction="row" alignItems="center" spacing={1} sx={{ mb: 1.5, flexWrap: "wrap", gap: 0.5 }}>
                    <Typography variant="subtitle2" sx={{ fontWeight: 600 }}>
                      Synonym Routing Summary
                    </Typography>
                    <Chip size="small" variant="outlined" label={`${colPush} column`} />
                    <Chip size="small" variant="outlined" label={`${valPush} value`} />
                    <Chip size="small" variant="outlined" label={`${genPush} general space`} />
                    {blockedRows > 0 && (
                      <Chip
                        size="small"
                        color="warning"
                        variant="filled"
                        label={`${blockedRows} won't push`}
                      />
                    )}
                    <Typography variant="caption" color="text.secondary" sx={{ ml: "auto" }}>
                      {totalPush} of {totalRows} synonyms will push at Session 5.
                    </Typography>
                  </Stack>

                  {blockedRows > 0 && (
                    <Alert severity="warning" sx={{ mb: 1.5, py: 0.5 }}>
                      {blockedRows} synonym routing{blockedRows === 1 ? " is" : "s are"} incomplete and will be silently
                      dropped at push. Fix the highlighted rows below — pick a column, add the table to the data plan,
                      or fill in the column value as the warning indicates.
                    </Alert>
                  )}

                  {columnEntries.length > 0 && (
                    <Box sx={{ mb: 1.5 }}>
                      <Typography variant="body2" sx={{ fontWeight: 500 }}>
                        Will be pushed to <code>column.synonyms</code>:
                      </Typography>
                      {renderEntries(columnEntries, (r) => (
                        <Typography variant="body2" component="span">
                          <code style={{ fontSize: 12 }}>{r.column_fqn || <em style={{ color: "#c62828" }}>(no column picked)</em>}</code>
                          {" ← "}
                          {r.synonyms.length > 0
                            ? r.synonyms.map((s: string, i: number) => <code key={i} style={{ fontSize: 12, marginRight: 4 }}>"{s}"</code>)
                            : <em style={{ color: "#999" }}>(no synonyms in S2 vocab)</em>}
                          {" "}<span style={{ color: "#999" }}>(from "{r.term}")</span>
                          {blockChip(r)}
                        </Typography>
                      ))}
                    </Box>
                  )}

                  {valueEntries.length > 0 && (
                    <Box sx={{ mb: 1.5 }}>
                      <Typography variant="body2" sx={{ fontWeight: 500 }}>
                        Will be pushed to <code>column.description</code> + Entity Matching enabled:
                      </Typography>
                      {renderEntries(valueEntries, (r) => (
                        <Typography variant="body2" component="span">
                          <code style={{ fontSize: 12 }}>{r.column_fqn || <em style={{ color: "#c62828" }}>(no column picked)</em>}</code>
                          {r.column_value && <> value <code style={{ fontSize: 12 }}>'{r.column_value}'</code></>}
                          {" ← "}
                          {r.synonyms.length > 0
                            ? r.synonyms.map((s: string, i: number) => <code key={i} style={{ fontSize: 12, marginRight: 4 }}>"{s}"</code>)
                            : <em style={{ color: "#999" }}>(no synonyms in S2 vocab)</em>}
                          {" "}<span style={{ color: "#999" }}>(from "{r.term}")</span>
                          {blockChip(r)}
                        </Typography>
                      ))}
                    </Box>
                  )}

                  {generalEntries.length > 0 && (
                    <Box>
                      <Typography variant="body2" sx={{ fontWeight: 500 }}>
                        Will appear in the space's <code>General Instructions</code>:
                      </Typography>
                      {renderEntries(generalEntries, (r) => (
                        <Typography variant="body2" component="span">
                          <strong>{r.term}</strong>
                          {" → "}
                          {r.synonyms.length > 0
                            ? r.synonyms.map((s: string, i: number) => <code key={i} style={{ fontSize: 12, marginRight: 4 }}>"{s}"</code>)
                            : <em style={{ color: "#999" }}>(no synonyms in S2 vocab)</em>}
                          <Typography variant="caption" color="text.secondary" sx={{ ml: 1 }}>
                            (auto-pre-filled in the Text Instructions section below — you can edit the phrasing)
                          </Typography>
                          {blockChip(r)}
                        </Typography>
                      ))}
                    </Box>
                  )}
                </Box>
              );
            })()}
          </AccordionDetails>
        </Accordion>
      ) : (
        <Alert severity="warning" sx={{ mb: 2 }}>
          No vocabulary terms from Session 2 yet. Complete Session 2 first to populate terms here.
        </Alert>
      )}

      {/* ---- Global Filter ---- */}
      <Accordion id="section-3-global-filter" defaultExpanded>
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
            A SQL boolean expression applied across the board — e.g., excluding
            test rows, voided records, or out-of-scope categories. It becomes the
            metric view's top-level <code>filter:</code> (hard) AND, when you generate
            the plan in Session 5, a mandatory instruction + a filter on every example
            query so Genie applies it to <strong>raw-table queries too</strong>, not just
            the metric view. Leave blank if none apply.
          </Typography>
          <Alert severity="info" sx={{ mb: 2 }}>
            Instruction-level enforcement is best-effort — Genie usually honors it but
            can miss it on novel questions. For <strong>guaranteed</strong> enforcement on
            every query (any tool, per user), apply it as a Unity Catalog row filter on the
            source tables. Use the generated DDL below as a starting point.
          </Alert>
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
          {(data.global_filter || "").trim() && dataSourceFqns.length > 0 && (
            <Box sx={{ mt: 2 }}>
              <Button
                size="small"
                variant="text"
                onClick={() => setShowRowFilterDdl((v) => !v)}
              >
                {showRowFilterDdl ? "Hide" : "Show"} hard-enforcement DDL (UC row filter)
              </Button>
              {showRowFilterDdl && (
                <Box sx={{ mt: 1 }}>
                  <Box sx={{ display: "flex", justifyContent: "flex-end", mb: 0.5 }}>
                    <Button
                      size="small"
                      onClick={() => navigator.clipboard?.writeText(rowFilterDdl)}
                    >
                      Copy DDL
                    </Button>
                  </Box>
                  <Box
                    component="pre"
                    sx={{
                      bgcolor: "grey.50", border: "1px solid", borderColor: "divider",
                      borderRadius: 1, p: 1.5, fontSize: 12, fontFamily: "monospace",
                      overflowX: "auto", whiteSpace: "pre", m: 0,
                    }}
                  >
                    {rowFilterDdl}
                  </Box>
                  <Typography variant="caption" color="text.secondary" sx={{ display: "block", mt: 1 }}>
                    Review before running: confirm the column list and types, and that every
                    table actually has those columns. Run as a table owner. This changes the
                    tables for ALL consumers, not just this Genie space.
                  </Typography>
                </Box>
              )}
            </Box>
          )}
        </AccordionDetails>
      </Accordion>

      {/* ---- SQL Expressions (Metrics) ---- */}
      <Accordion id="section-3-sql-expressions" defaultExpanded>
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
          <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
            <strong>Aggregate or a WHERE clause?</strong> An aggregate
            (e.g. <code>COUNT(1)</code>, <code>SUM(paid_amount)</code>) becomes a measure.
            A bare condition / WHERE-clause fragment also works
            (e.g. <code>initial_decision = 'DENIED'</code>): the generator will either
            fold it into the metric view's filter or count the matching rows as
            <code> COUNT(1) FILTER (WHERE …)</code>, depending on how the metric reads.
            You don't have to rewrite it as a SELECT.
          </Typography>
          {dataSourceFqns.length > 0 && (
            <Typography variant="body2" color="text.secondary" sx={{ mb: 1 }}>
              The Table picker is limited to your {dataSourceFqns.length} chosen
              Data Source{dataSourceFqns.length === 1 ? "" : "s"}. Pick
              <em> Browse all catalogs…</em> in the dropdown if you need another table.
            </Typography>
          )}
          <EditableTable
            columns={SQL_EXPR_COLS}
            rows={data.sql_expressions || []}
            onChange={(rows) => onChange("sql_expressions", rows)}
            readOnly={readOnly}
            restrictTables={dataSourceFqns}
          />
        </AccordionDetails>
      </Accordion>

      {/* ---- Example Queries (#7) ---- */}
      <Accordion id="section-3-example-queries" defaultExpanded>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>
            <Typography variant="h6">Example Queries</Typography>
            {(data.example_queries || []).length > 0 && (
              <Chip label={`${(data.example_queries || []).length}`} size="small" variant="outlined" />
            )}
          </Box>
        </AccordionSummary>
        <AccordionDetails>
          <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
            When you already know the SQL for an important question, define it here
            instead of forcing it into a metric or synonym. These are surfaced to
            Genie verbatim as example queries (not drafts) when you generate the
            plan in Session 5. Use fully qualified <code>catalog.schema.table</code>{" "}
            names — example queries are standalone.
          </Typography>
          <EditableTable
            columns={EXAMPLE_QUERY_COLS}
            rows={data.example_queries || []}
            onChange={(rows) => onChange("example_queries", rows)}
            readOnly={readOnly}
          />
        </AccordionDetails>
      </Accordion>

      {/* ---- Clarifying Questions (#2) ---- */}
      <Accordion id="section-3-clarifying-questions" defaultExpanded>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>
            <Typography variant="h6">Clarifying Questions</Typography>
            {(data.clarifying_questions || []).length > 0 && (
              <Chip label={`${(data.clarifying_questions || []).length}`} size="small" variant="outlined" />
            )}
          </Box>
        </AccordionSummary>
        <AccordionDetails>
          <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
            For ambiguous terms, tell Genie to ask a follow-up before answering.
            Example: when someone asks about <em>"service lines"</em>, Genie can ask
            whether they mean <em>clinical</em> or <em>financial</em> service line.
            These become clarification triggers in the space's instructions when you
            generate the plan in Session 5.
          </Typography>
          <EditableTable
            columns={CLARIFYING_COLS}
            rows={data.clarifying_questions || []}
            onChange={(rows) => onChange("clarifying_questions", rows)}
            readOnly={readOnly}
          />
        </AccordionDetails>
      </Accordion>

      {/* ---- Text Instructions ---- */}
      <Accordion id="section-3-text-instructions" defaultExpanded>
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
      <Accordion id="section-3-table-summary" defaultExpanded>
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
      <Accordion id="section-3-data-gaps" defaultExpanded>
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
      <Accordion id="section-3-scope-boundaries" defaultExpanded>
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
      <Accordion id="section-3-metric-view" defaultExpanded>
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
              onClick={requestDraftMvYaml}
              disabled={readOnly || !mvReady || mvDrafting || !engagementId}
            >
              {data.metric_view_yaml ? "Redraft YAML with AI" : "Generate YAML with AI"}
            </Button>
            {data.metric_view_yaml_previous && !readOnly && (
              <Button
                variant="outlined"
                startIcon={<RestoreIcon />}
                onClick={() => setCompareMvOpen(true)}
                disabled={mvDrafting}
              >
                Restore previous version
              </Button>
            )}
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

      <ConfirmDialog
        open={confirmRedraftOpen}
        title="Redraft the metric view YAML?"
        message={
          "This replaces your current YAML with a fresh AI draft.\n\n" +
          "Your current version is saved first, so you can use \"Restore previous version\" to get it back."
        }
        confirmLabel="Redraft"
        onConfirm={() => { setConfirmRedraftOpen(false); handleDraftMvYaml(); }}
        onCancel={() => setConfirmRedraftOpen(false)}
      />

      <CompareRestoreDialog
        open={compareMvOpen}
        title="Compare metric view YAML versions"
        rows={[{
          label: "Metric View YAML",
          current: data.metric_view_yaml || "",
          previous: data.metric_view_yaml_previous || "",
        }]}
        onConfirm={() => { setCompareMvOpen(false); handleRestoreMvYaml(); }}
        onCancel={() => setCompareMvOpen(false)}
      />
    </Box>
  );
}
