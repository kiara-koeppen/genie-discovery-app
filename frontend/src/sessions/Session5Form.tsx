import { useState, useEffect } from "react";
import {
  Typography, Box, Accordion, AccordionSummary, AccordionDetails, Alert,
  TextField, Button, Chip, Paper, Divider, ToggleButton, ToggleButtonGroup,
  Link, List, ListItem, IconButton, CircularProgress, MenuItem, Select,
  Table, TableBody, TableCell, TableHead, TableRow, Stack,
} from "@mui/material";
import ExpandMoreIcon from "@mui/icons-material/ExpandMore";
import AutoAwesomeIcon from "@mui/icons-material/AutoAwesome";
import RocketLaunchIcon from "@mui/icons-material/RocketLaunch";
import DeleteIcon from "@mui/icons-material/Delete";
import AddIcon from "@mui/icons-material/Add";
import OpenInNewIcon from "@mui/icons-material/OpenInNew";
import LockIcon from "@mui/icons-material/Lock";
import ExpandableTextField from "../components/ExpandableTextField";
import { api, SqlSnippet, ExampleQuery, UcJoin, BenchmarkQuestion } from "../api";

interface Props {
  data: Record<string, any>;
  onChange: (section: string, value: any) => void;
  readOnly?: boolean;
  session3Data?: Record<string, any>;
  session4Data?: Record<string, any>;
  engagementId?: string;
}

type SnippetKey = "plan_sql_filters" | "plan_sql_dimensions" | "plan_sql_measures";

export default function Session5Form({
  data, onChange, readOnly, session4Data, engagementId,
}: Props) {
  const [generating, setGenerating] = useState(false);
  const [generateError, setGenerateError] = useState<string>("");
  const [pushing, setPushing] = useState(false);
  const [pushResult, setPushResult] = useState<string>("");
  const [pushError, setPushError] = useState<string>("");
  const [mode, setMode] = useState<"existing" | "new">("existing");
  const [newTitle, setNewTitle] = useState("");
  const [newDescription, setNewDescription] = useState("");
  const [warehouses, setWarehouses] = useState<
    { id: string; name: string; state: string; size: string; type: string }[]
  >([]);
  const [warehouseLoadError, setWarehouseLoadError] = useState("");

  const dataPlan = session4Data?.data_plan || [];
  const includedItems = dataPlan.filter((d: any) => d.include_in_space === "Yes");
  const scopeTables: string[] = includedItems
    .filter((d: any) => d.type !== "Metric View")
    .map((d: any) => d.table_or_view)
    .filter(Boolean);
  const hasMetricView = includedItems.some((d: any) => d.type === "Metric View");
  const hasRawTables = includedItems.some((d: any) => d.type !== "Metric View");
  const sqlExprMode: "hidden" | "additional" | "primary" =
    hasMetricView && !hasRawTables ? "hidden" :
    hasMetricView && hasRawTables ? "additional" :
    "primary";

  const sampleQuestions: string[] = data.plan_sample_questions || [];
  const filters: SqlSnippet[] = data.plan_sql_filters || [];
  const dimensions: SqlSnippet[] = data.plan_sql_dimensions || [];
  const measures: SqlSnippet[] = data.plan_sql_measures || [];
  const exampleQueries: ExampleQuery[] = data.plan_example_queries || [];
  const joins: UcJoin[] = data.plan_joins || [];
  const benchmarks: BenchmarkQuestion[] = session4Data?.benchmark_questions || [];
  const pushableBenchmarks = benchmarks.filter(
    (b) => b.question?.trim() && b.expected_sql?.trim(),
  );

  useEffect(() => {
    if (!newTitle && data.genie_space_name) setNewTitle(data.genie_space_name);
  }, [data.genie_space_name]); // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => {
    api.listWarehouses()
      .then(setWarehouses)
      .catch((err) => setWarehouseLoadError(err.message || "Failed to load warehouses"));
  }, []);

  const handleGenerate = async () => {
    if (!engagementId) return;
    setGenerating(true);
    setGenerateError("");
    try {
      const plan = await api.generatePlan(engagementId);
      onChange("plan_general_instructions", plan.general_instructions);
      onChange("plan_sample_questions", plan.sample_questions);
      onChange("plan_sql_filters", plan.sql_filters);
      onChange("plan_sql_dimensions", plan.sql_dimensions);
      onChange("plan_sql_measures", plan.sql_measures);
      onChange("plan_example_queries", plan.example_queries);
      onChange("plan_joins", plan.joins);
      onChange("plan_narrative", plan.narrative);
    } catch (err: any) {
      setGenerateError(err.message || "Generate failed");
    }
    setGenerating(false);
  };

  // Sample questions handlers
  const updateQuestion = (idx: number, value: string) => {
    const next = [...sampleQuestions]; next[idx] = value;
    onChange("plan_sample_questions", next);
  };
  const removeQuestion = (idx: number) =>
    onChange("plan_sample_questions", sampleQuestions.filter((_, i) => i !== idx));
  const addQuestion = () => onChange("plan_sample_questions", [...sampleQuestions, ""]);

  // Snippet handlers
  const updateSnippet = (key: SnippetKey, idx: number, field: keyof SqlSnippet, value: any) => {
    const list = [...(data[key] || [])];
    list[idx] = { ...list[idx], [field]: value };
    onChange(key, list);
  };
  const removeSnippet = (key: SnippetKey, idx: number) => {
    const list = [...(data[key] || [])];
    list.splice(idx, 1);
    onChange(key, list);
  };
  const addSnippet = (key: SnippetKey) => {
    const list = [...(data[key] || [])];
    list.push({ name: "", sql: "", table: scopeTables[0] || "", display_name: "" });
    onChange(key, list);
  };

  // Example query handlers
  const updateExample = (idx: number, field: keyof ExampleQuery, value: any) => {
    const list = [...exampleQueries]; list[idx] = { ...list[idx], [field]: value };
    onChange("plan_example_queries", list);
  };
  const removeExample = (idx: number) =>
    onChange("plan_example_queries", exampleQueries.filter((_, i) => i !== idx));
  const addExample = () =>
    onChange("plan_example_queries", [...exampleQueries, { question: "", sql: "", draft: true, usage_guidance: "" }]);

  const handlePush = async () => {
    if (!engagementId) return;
    setPushing(true); setPushError(""); setPushResult("");
    try {
      const body: Parameters<typeof api.pushToGenie>[1] = {
        mode,
        warehouse_id: data.plan_warehouse_id || "",
        general_instructions: data.plan_general_instructions || "",
        sample_questions: sampleQuestions,
        sql_filters: filters,
        sql_dimensions: dimensions,
        sql_measures: measures,
        example_queries: exampleQueries,
        joins,
      };
      if (mode === "existing") body.space_id = data.genie_space_id || "";
      else { body.new_title = newTitle; body.new_description = newDescription; }
      const res = await api.pushToGenie(engagementId, body);
      onChange("genie_space_id", res.space_id);
      onChange("genie_space_url", res.space_url);
      onChange("genie_space_pushed_at", new Date().toISOString());
      setPushResult(res.created
        ? `Created new Genie Space. View it at ${res.space_url}`
        : `Updated Genie Space. View it at ${res.space_url}`);
    } catch (err: any) {
      setPushError(err.message || "Push failed");
    }
    setPushing(false);
  };

  const hasPlan = !!(data.plan_general_instructions || sampleQuestions.length || measures.length);
  const canPush =
    hasPlan &&
    (data.plan_warehouse_id || "").trim() &&
    (mode === "existing" ? (data.genie_space_id || "").trim() : newTitle.trim());

  const SnippetTable = ({ title, items, sectionKey, helperText, hasAlias = true }: {
    title: string; items: SqlSnippet[]; sectionKey: SnippetKey;
    helperText: string; hasAlias?: boolean;
  }) => (
    <Box sx={{ mb: 2 }}>
      <Stack direction="row" alignItems="center" spacing={1} sx={{ mb: 0.5 }}>
        <Typography variant="subtitle2">{title} ({items.length})</Typography>
      </Stack>
      <Typography variant="caption" color="text.secondary" sx={{ display: "block", mb: 1 }}>
        {helperText}
      </Typography>
      {items.length > 0 && (
        <Table size="small" sx={{ mb: 1 }}>
          <TableHead>
            <TableRow>
              {hasAlias && <TableCell sx={{ width: "18%" }}>Name</TableCell>}
              <TableCell sx={{ width: "18%" }}>Display name</TableCell>
              <TableCell sx={{ width: "22%" }}>Table</TableCell>
              <TableCell>SQL</TableCell>
              {!readOnly && <TableCell sx={{ width: 40 }} />}
            </TableRow>
          </TableHead>
          <TableBody>
            {items.map((item, i) => (
              <TableRow key={i}>
                {hasAlias && (
                  <TableCell>
                    <TextField size="small" fullWidth value={item.name || ""}
                      onChange={(e) => updateSnippet(sectionKey, i, "name", e.target.value)}
                      disabled={readOnly} />
                  </TableCell>
                )}
                <TableCell>
                  <TextField size="small" fullWidth value={item.display_name || ""}
                    onChange={(e) => updateSnippet(sectionKey, i, "display_name", e.target.value)}
                    disabled={readOnly} />
                </TableCell>
                <TableCell>
                  <Select size="small" fullWidth value={item.table || ""}
                    onChange={(e) => updateSnippet(sectionKey, i, "table", e.target.value)}
                    disabled={readOnly} sx={{ fontFamily: "monospace", fontSize: 12 }}>
                    {scopeTables.map((t) => (
                      <MenuItem key={t} value={t} sx={{ fontFamily: "monospace", fontSize: 12 }}>{t}</MenuItem>
                    ))}
                  </Select>
                </TableCell>
                <TableCell>
                  <TextField size="small" fullWidth multiline value={item.sql || ""}
                    onChange={(e) => updateSnippet(sectionKey, i, "sql", e.target.value)}
                    disabled={readOnly}
                    InputProps={{ sx: { fontFamily: "monospace", fontSize: 12 } }} />
                </TableCell>
                {!readOnly && (
                  <TableCell>
                    <IconButton size="small" onClick={() => removeSnippet(sectionKey, i)}>
                      <DeleteIcon fontSize="small" />
                    </IconButton>
                  </TableCell>
                )}
              </TableRow>
            ))}
          </TableBody>
        </Table>
      )}
      {!readOnly && (
        <Button size="small" startIcon={<AddIcon />} onClick={() => addSnippet(sectionKey)}>
          Add {title.toLowerCase().replace(/s$/, "")}
        </Button>
      )}
    </Box>
  );

  return (
    <Box>
      <Alert severity="info" sx={{ mb: 2 }}>
        <strong>Configure Genie Space:</strong> Generate an AI-synthesized configuration plan from
        Sessions 1-4, review and edit each section, then push to a Genie Space. The plan populates
        every Genie instruction surface: text instructions, SQL Expressions, example queries, and
        (via UC) joins.
      </Alert>

      {/* Generate Plan */}
      <Accordion defaultExpanded>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>
            <AutoAwesomeIcon color="primary" />
            <Typography variant="h6">AI-Generated Plan</Typography>
            {hasPlan && <Chip label="Generated" size="small" color="success" />}
          </Box>
        </AccordionSummary>
        <AccordionDetails>
          <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
            Uses Sessions 1-4 to populate every Genie instruction surface. Review and edit each
            section below before pushing.
          </Typography>
          {!readOnly && (
            <Button variant="contained" color="primary" onClick={handleGenerate} disabled={generating}
              startIcon={generating ? <CircularProgress size={18} /> : <AutoAwesomeIcon />}
              sx={{ mb: 2 }}>
              {generating ? "Generating..." : hasPlan ? "Regenerate Plan" : "Generate Plan"}
            </Button>
          )}
          {generateError && (
            <Alert severity="error" sx={{ mb: 2, whiteSpace: "pre-wrap" }}>
              <strong>Generate Plan failed:</strong> {generateError}
            </Alert>
          )}
          {data.plan_narrative && (
            <Paper variant="outlined" sx={{ p: 2, bgcolor: "grey.50" }}>
              <Typography variant="caption" color="text.secondary">Plan summary</Typography>
              <Typography variant="body2" sx={{ mt: 0.5 }}>{data.plan_narrative}</Typography>
            </Paper>
          )}
        </AccordionDetails>
      </Accordion>

      {/* Text Instructions */}
      <Accordion defaultExpanded>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Typography variant="h6">Text Instructions (General)</Typography>
        </AccordionSummary>
        <AccordionDetails>
          <Typography variant="body2" color="text.secondary" sx={{ mb: 1 }}>
            The single text instruction Genie allows per space. Short, atomic bullets. Don't restate
            metric definitions (those live in SQL Expressions). Target ~400-800 chars.
          </Typography>
          <ExpandableTextField
            minRows={6}
            placeholder="Click Generate Plan, or write your own bulleted rules..."
            value={data.plan_general_instructions || ""}
            onChange={(v) => onChange("plan_general_instructions", v)}
            disabled={readOnly}
            dialogTitle="General Instructions" />
          <Typography variant="caption" color="text.secondary" sx={{ mt: 0.5, display: "block" }}>
            {(data.plan_general_instructions || "").length} chars
          </Typography>

          <Typography variant="subtitle2" sx={{ mt: 2, mb: 0.5 }}>
            Sample Questions ({sampleQuestions.length})
          </Typography>
          <List dense>
            {sampleQuestions.map((q, i) => (
              <ListItem key={i} disableGutters
                secondaryAction={!readOnly && (
                  <IconButton edge="end" size="small" onClick={() => removeQuestion(i)}>
                    <DeleteIcon fontSize="small" />
                  </IconButton>
                )}>
                <TextField fullWidth size="small" value={q}
                  onChange={(e) => updateQuestion(i, e.target.value)} disabled={readOnly} />
              </ListItem>
            ))}
          </List>
          {!readOnly && (
            <Button size="small" startIcon={<AddIcon />} onClick={addQuestion}>Add question</Button>
          )}
        </AccordionDetails>
      </Accordion>

      {/* Data Sources (tables + UC joins) */}
      <Accordion defaultExpanded>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Stack direction="row" alignItems="center" spacing={1}>
            <Typography variant="h6">Data Sources</Typography>
            <Chip label={`${includedItems.length} tables/views`} size="small" variant="outlined" />
            <Chip label={`${joins.length} joins`} size="small" variant="outlined" />
          </Stack>
        </AccordionSummary>
        <AccordionDetails>
          <Typography variant="subtitle2" sx={{ mb: 0.5 }}>
            Tables & Views in Scope (from Session 4 data plan)
          </Typography>
          {includedItems.length > 0 ? (
            <Box sx={{ display: "flex", flexWrap: "wrap", gap: 0.5, mb: 2 }}>
              {includedItems.map((item: any, i: number) => (
                <Chip
                  key={i}
                  label={item.table_or_view}
                  size="small"
                  color={item.type === "Metric View" ? "primary" : "default"}
                  sx={{ fontFamily: "monospace", fontSize: 12 }}
                />
              ))}
            </Box>
          ) : (
            <Alert severity="warning" sx={{ mb: 2 }}>
              No tables/views marked "Yes" in Session 4 data plan. Go back and include at least one.
            </Alert>
          )}

          <Divider sx={{ my: 2 }} />

          <Stack direction="row" alignItems="center" spacing={1} sx={{ mb: 0.5 }}>
            <Typography variant="subtitle2">Joins</Typography>
            <Chip icon={<LockIcon />} label="Read-only, from UC PK/FK" size="small" />
          </Stack>
          <Typography variant="body2" color="text.secondary" sx={{ mb: 1 }}>
            Auto-detected from declared PK/FK constraints in Unity Catalog. If no joins appear,
            Genie will rely on runtime auto-detection. To add explicit joins, declare them as UC
            constraints on the tables.
          </Typography>
          {joins.length > 0 ? (
            <Table size="small">
              <TableHead>
                <TableRow>
                  <TableCell>Left table</TableCell>
                  <TableCell>Left cols</TableCell>
                  <TableCell>Right table</TableCell>
                  <TableCell>Right cols</TableCell>
                  <TableCell>Relationship</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {joins.map((j, i) => (
                  <TableRow key={i}>
                    <TableCell sx={{ fontFamily: "monospace", fontSize: 12 }}>{j.left_table}</TableCell>
                    <TableCell sx={{ fontFamily: "monospace", fontSize: 12 }}>{(j.left_columns || []).join(", ")}</TableCell>
                    <TableCell sx={{ fontFamily: "monospace", fontSize: 12 }}>{j.right_table}</TableCell>
                    <TableCell sx={{ fontFamily: "monospace", fontSize: 12 }}>{(j.right_columns || []).join(", ")}</TableCell>
                    <TableCell>{j.relationship_type}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          ) : (
            <Alert severity="info">
              No PK/FK constraints detected across the tables in scope. Click Regenerate Plan after
              declaring constraints in UC.
            </Alert>
          )}
        </AccordionDetails>
      </Accordion>

      {/* SQL Expressions */}
      {sqlExprMode !== "hidden" && (
        <Accordion defaultExpanded>
          <AccordionSummary expandIcon={<ExpandMoreIcon />}>
            <Typography variant="h6">
              {sqlExprMode === "additional" ? "Additional SQL Expressions" : "SQL Expressions"}
              {" "}({filters.length + dimensions.length + measures.length})
            </Typography>
          </AccordionSummary>
          <AccordionDetails>
            {sqlExprMode === "additional" && (
              <Alert severity="info" sx={{ mb: 2 }}>
                Your metric view already defines measures and dimensions for the governed concepts.
                Only add SQL expressions here for the raw tables you included alongside the metric
                view - don't duplicate metrics that already live in the MV.
              </Alert>
            )}
            <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
              Business concepts as reusable SQL. Columns in each SQL expression must be prefixed with
              the short table name (e.g., <code>claims.initial_decision</code>). The Table field is
              analyst metadata for grouping; Genie infers the table from the SQL's column references.
            </Typography>
            <SnippetTable
              title="Filters" items={filters} sectionKey="plan_sql_filters"
              helperText="WHERE-clause snippets. No alias. Example: claims.initial_decision = 'DENIED'"
              hasAlias={false} />
            <Divider sx={{ my: 1 }} />
            <SnippetTable
              title="Dimensions" items={dimensions} sectionKey="plan_sql_dimensions"
              helperText="Grouping or SELECT expressions. Example: YEAR(claims.receipt_date)" />
            <Divider sx={{ my: 1 }} />
            <SnippetTable
              title="Measures" items={measures} sectionKey="plan_sql_measures"
              helperText="Aggregates. Example: COUNT(CASE WHEN claims.initial_decision = 'DENIED' THEN 1 END)" />
          </AccordionDetails>
        </Accordion>
      )}

      {/* Example Queries */}
      <Accordion defaultExpanded>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Typography variant="h6">Example SQL Queries ({exampleQueries.length})</Typography>
        </AccordionSummary>
        <AccordionDetails>
          <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
            Full SQL examples for complex questions. Must use fully qualified
            <code> catalog.schema.table</code> references. LLM-generated examples are marked DRAFT —
            verify before push.
          </Typography>
          {exampleQueries.map((eq, i) => (
            <Paper key={i} variant="outlined" sx={{ p: 2, mb: 2 }}>
              <Stack direction="row" alignItems="center" spacing={1} sx={{ mb: 1 }}>
                {eq.draft && <Chip label="DRAFT - verify" size="small" color="warning" />}
                {!readOnly && (
                  <IconButton size="small" onClick={() => removeExample(i)} sx={{ ml: "auto" }}>
                    <DeleteIcon fontSize="small" />
                  </IconButton>
                )}
              </Stack>
              <TextField fullWidth size="small" label="Question" sx={{ mb: 1 }}
                value={eq.question} disabled={readOnly}
                onChange={(e) => updateExample(i, "question", e.target.value)} />
              <TextField fullWidth size="small" label="SQL" multiline minRows={3} sx={{ mb: 1 }}
                value={eq.sql} disabled={readOnly}
                onChange={(e) => updateExample(i, "sql", e.target.value)}
                InputProps={{ sx: { fontFamily: "monospace", fontSize: 12 } }} />
              <TextField fullWidth size="small" label="Usage guidance (optional)"
                value={eq.usage_guidance || ""} disabled={readOnly}
                onChange={(e) => updateExample(i, "usage_guidance", e.target.value)} />
            </Paper>
          ))}
          {!readOnly && (
            <Button size="small" startIcon={<AddIcon />} onClick={addExample}>Add example query</Button>
          )}
        </AccordionDetails>
      </Accordion>

      {/* Benchmarks (read-only, from Session 4) */}
      <Accordion defaultExpanded={benchmarks.length > 0}>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Stack direction="row" alignItems="center" spacing={1}>
            <Typography variant="h6">Benchmark Questions</Typography>
            <Chip icon={<LockIcon />} label="Read-only, from Session 4" size="small" />
            <Chip label={`${pushableBenchmarks.length} will be pushed`} size="small" variant="outlined" />
          </Stack>
        </AccordionSummary>
        <AccordionDetails>
          <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
            These benchmark questions are pushed to Genie as the acceptance-test set. They are NOT
            included as sample questions or example SQL queries — Genie must answer them from the
            other configured context. Edit in Session 4.
          </Typography>
          {pushableBenchmarks.length === 0 ? (
            <Alert severity="warning">
              No pushable benchmarks. Go to Session 4 and draft/approve at least a few benchmark
              questions with expected SQL before pushing.
            </Alert>
          ) : (
            <Table size="small">
              <TableHead>
                <TableRow>
                  <TableCell>Question</TableCell>
                  <TableCell sx={{ width: 110 }}>Category</TableCell>
                  <TableCell sx={{ width: 100 }}>Difficulty</TableCell>
                  <TableCell sx={{ width: 110 }}>BO approved</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {pushableBenchmarks.map((b, i) => (
                  <TableRow key={i}>
                    <TableCell>{b.question}</TableCell>
                    <TableCell>
                      <Chip label={b.category} size="small"
                        color={b.category === "Edge Case" ? "warning" : "default"} />
                    </TableCell>
                    <TableCell>{b.difficulty}</TableCell>
                    <TableCell>
                      {b.bo_approved
                        ? <Chip label="Yes" size="small" color="success" />
                        : <Chip label="No" size="small" />}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          )}
        </AccordionDetails>
      </Accordion>

      {/* Push to Genie */}
      <Accordion defaultExpanded>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>
            <RocketLaunchIcon color="action" />
            <Typography variant="h6">Push to Genie Space</Typography>
          </Box>
        </AccordionSummary>
        <AccordionDetails>
          <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
            The push runs as you (OBO) — you need CAN MANAGE on the target space. In production,
            your ops team should create the space, own it with a service principal, and grant you
            CAN MANAGE.
          </Typography>
          <ToggleButtonGroup value={mode} exclusive onChange={(_, v) => v && setMode(v)}
            size="small" sx={{ mb: 2 }} disabled={readOnly}>
            <ToggleButton value="existing">Update Existing Space</ToggleButton>
            <ToggleButton value="new">Create New Space (dev only)</ToggleButton>
          </ToggleButtonGroup>
          <Divider sx={{ mb: 2 }} />
          {mode === "existing" ? (
            <TextField fullWidth label="Genie Space ID" sx={{ mb: 2 }}
              placeholder="e.g. 01f0abc123... (paste from Genie Settings)"
              value={data.genie_space_id || ""}
              onChange={(e) => onChange("genie_space_id", e.target.value)}
              disabled={readOnly}
              helperText="The existing space to update. Must grant your user CAN MANAGE." />
          ) : (
            <>
              <TextField fullWidth label="New Space Title" sx={{ mb: 2 }}
                value={newTitle} onChange={(e) => setNewTitle(e.target.value)} disabled={readOnly} />
              <TextField fullWidth label="Description" multiline minRows={2} sx={{ mb: 2 }}
                value={newDescription} onChange={(e) => setNewDescription(e.target.value)}
                disabled={readOnly} />
            </>
          )}
          <TextField fullWidth select label="SQL Warehouse" sx={{ mb: 2 }}
            value={data.plan_warehouse_id || ""}
            onChange={(e) => onChange("plan_warehouse_id", e.target.value)}
            disabled={readOnly || warehouses.length === 0}
            helperText={
              warehouseLoadError
                ? `Could not load warehouses: ${warehouseLoadError}`
                : warehouses.length === 0
                  ? "Loading warehouses..."
                  : "SQL warehouse the Genie Space will use to run queries."
            }>
            {warehouses.map((wh) => (
              <MenuItem key={wh.id} value={wh.id}>
                <Box sx={{ display: "flex", alignItems: "center", gap: 1, width: "100%" }}>
                  <Typography variant="body2">{wh.name}</Typography>
                  {wh.size && (
                    <Chip label={wh.size} size="small" variant="outlined" sx={{ fontSize: 10, height: 18 }} />
                  )}
                  {wh.state && (
                    <Chip
                      label={wh.state.replace(/^STATE_/, "").toLowerCase()}
                      size="small"
                      color={wh.state.includes("RUNNING") ? "success" : "default"}
                      sx={{ fontSize: 10, height: 18 }}
                    />
                  )}
                  <Typography variant="caption" color="text.secondary" sx={{ ml: "auto", fontFamily: "monospace" }}>
                    {wh.id}
                  </Typography>
                </Box>
              </MenuItem>
            ))}
          </TextField>
          {!readOnly && (
            <Button variant="contained" color="success" size="large" onClick={handlePush}
              disabled={pushing || !canPush}
              startIcon={pushing ? <CircularProgress size={18} color="inherit" /> : <RocketLaunchIcon />}>
              {pushing ? "Pushing..." : mode === "new" ? "Create Space" : "Update Space"}
            </Button>
          )}
          {pushError && <Alert severity="error" sx={{ mt: 2 }}>{pushError}</Alert>}
          {pushResult && <Alert severity="success" sx={{ mt: 2 }}>{pushResult}</Alert>}
          {data.genie_space_url && (
            <Paper variant="outlined" sx={{ p: 2, mt: 2, bgcolor: "grey.50" }}>
              <Typography variant="caption" color="text.secondary">
                Last push: {data.genie_space_pushed_at || "unknown"}
              </Typography>
              <Box sx={{ mt: 0.5 }}>
                <Link href={data.genie_space_url} target="_blank" rel="noopener">
                  Open Genie Space <OpenInNewIcon sx={{ fontSize: 14, verticalAlign: "middle" }} />
                </Link>
              </Box>
            </Paper>
          )}
        </AccordionDetails>
      </Accordion>
    </Box>
  );
}
