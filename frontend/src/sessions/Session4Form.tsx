import { useEffect, useState, useMemo, useRef } from "react";
import {
  Typography, Box, Accordion, AccordionSummary, AccordionDetails, Alert,
  TextField, Button, Chip, Paper, Divider, Stack, IconButton, MenuItem, Select,
  Checkbox, CircularProgress, Tooltip,
} from "@mui/material";
import ExpandMoreIcon from "@mui/icons-material/ExpandMore";
import CheckCircleIcon from "@mui/icons-material/CheckCircle";
import PendingIcon from "@mui/icons-material/Pending";
import ErrorIcon from "@mui/icons-material/Error";
import AutoAwesomeIcon from "@mui/icons-material/AutoAwesome";
import AddIcon from "@mui/icons-material/Add";
import DeleteIcon from "@mui/icons-material/Delete";
import EditableTable from "../components/EditableTable";
import ExpandableTextField from "../components/ExpandableTextField";
import { api, BenchmarkQuestion } from "../api";
import type { ColumnDef } from "../types";

const DATA_PLAN_COLS: ColumnDef[] = [
  { key: "table_or_view", label: "Table / Metric View", type: "uc_table" },
  { key: "type", label: "Type", width: 140, type: "select", options: ["Table", "Metric View"] },
  { key: "include_in_space", label: "Include in Genie Space?", width: 160, type: "select", options: ["Yes", "No", "TBD"] },
  { key: "notes", label: "Notes", type: "textarea" },
];

interface Props {
  data: Record<string, any>;
  onChange: (section: string, value: any) => void;
  readOnly?: boolean;
  session1Data?: Record<string, any>;
  session2Data?: Record<string, any>;
  session3Data?: Record<string, any>;
  engagementId?: string;
  isCoeMember?: boolean;
}

export default function Session4Form({
  data, onChange, readOnly, session3Data, engagementId, isCoeMember,
}: Props) {
  const [summary, setSummary] = useState("");
  const [loadingSummary, setLoadingSummary] = useState(false);
  const [approvalNotes, setApprovalNotes] = useState("");
  const [draftingBenchmarks, setDraftingBenchmarks] = useState(false);
  const [draftingSqlIdx, setDraftingSqlIdx] = useState<number | null>(null);
  const [draftingAllSql, setDraftingAllSql] = useState(false);

  const approvalStatus = data.coe_approval_status || "pending";
  const benchmarks: BenchmarkQuestion[] = data.benchmark_questions || [];
  const approvedBenchmarkCount = benchmarks.filter(
    (b) => b.bo_approved && b.question?.trim() && b.expected_sql?.trim(),
  ).length;
  const canApprove = approvedBenchmarkCount >= 5;

  // Pre-populate data plan from Session 3 tables
  const session3Tables = useMemo(() => {
    const tables = new Set<string>();
    (session3Data?.sql_expressions || []).forEach((e: any) => {
      if (e.uc_table && e.uc_table.split(".").length === 3) tables.add(e.uc_table);
    });
    return Array.from(tables);
  }, [session3Data]);

  // Seed data plan from Session 3 tables on first mount, only if the analyst
  // hasn't put a real table in any row yet. Ref guard prevents clobbering
  // later edits when session3Tables re-computes.
  const seeded = useRef(false);
  useEffect(() => {
    if (seeded.current) return;
    if (session3Tables.length === 0) return;
    const currentRows = data.data_plan || [];
    const hasRealData = currentRows.some((r: any) => r.table_or_view);
    if (hasRealData) {
      seeded.current = true;
      return;
    }
    const plan = session3Tables.map((t) => ({
      table_or_view: t,
      type: "Table",
      include_in_space: "Yes",
      notes: "",
    }));
    onChange("data_plan", plan);
    seeded.current = true;
  }, [session3Tables]); // eslint-disable-line react-hooks/exhaustive-deps

  // Fetch auto-summary
  const fetchSummary = async () => {
    if (!engagementId) return;
    setLoadingSummary(true);
    try {
      const res = await api.getAutoSummary(engagementId);
      setSummary(res.summary);
      onChange("auto_summary", res.summary);
    } catch {
      setSummary("Failed to generate summary.");
    }
    setLoadingSummary(false);
  };

  useEffect(() => {
    if (data.auto_summary) {
      setSummary(data.auto_summary);
    } else if (engagementId) {
      fetchSummary();
    }
  }, [engagementId]); // eslint-disable-line react-hooks/exhaustive-deps

  // Benchmark handlers
  const updateBenchmark = (idx: number, field: keyof BenchmarkQuestion, value: any) => {
    const next = [...benchmarks];
    next[idx] = { ...next[idx], [field]: value };
    onChange("benchmark_questions", next);
  };
  const removeBenchmark = (idx: number) =>
    onChange("benchmark_questions", benchmarks.filter((_, i) => i !== idx));
  const addBenchmark = () =>
    onChange("benchmark_questions", [
      ...benchmarks,
      { question: "", category: "Core", difficulty: "Medium", expected_sql: "", notes: "", bo_approved: false },
    ]);
  const draftAllBenchmarks = async () => {
    if (!engagementId) return;
    setDraftingBenchmarks(true);
    try {
      const res = await api.draftBenchmarks(engagementId);
      onChange("benchmark_questions", [...benchmarks, ...res.benchmarks]);
    } catch (err) {
      // surface via notes on first row if nothing else
      console.error(err);
    }
    setDraftingBenchmarks(false);
  };
  const draftSqlForRow = async (idx: number) => {
    if (!engagementId) return;
    const q = benchmarks[idx]?.question?.trim();
    if (!q) return;
    setDraftingSqlIdx(idx);
    try {
      const res = await api.draftBenchmarkSql(engagementId, q);
      updateBenchmark(idx, "expected_sql", res.sql);
    } catch (err) {
      console.error(err);
    }
    setDraftingSqlIdx(null);
  };
  const draftAllSql = async () => {
    if (!engagementId) return;
    setDraftingAllSql(true);
    const next = [...benchmarks];
    for (let i = 0; i < next.length; i++) {
      const b = next[i];
      const q = b.question?.trim();
      if (!q || b.expected_sql?.trim()) continue; // skip blanks and already-filled rows
      try {
        const res = await api.draftBenchmarkSql(engagementId, q);
        next[i] = { ...next[i], expected_sql: res.sql };
        onChange("benchmark_questions", [...next]);
      } catch (err) {
        console.error(err);
      }
    }
    setDraftingAllSql(false);
  };

  const handleApproval = async (status: string) => {
    if (!engagementId) return;
    try {
      await api.coeApprove(engagementId, { status, notes: approvalNotes });
      onChange("coe_approval_status", status);
      onChange("coe_approval_notes", approvalNotes);
    } catch {
      // handled silently
    }
  };

  const statusChip = () => {
    switch (approvalStatus) {
      case "approved":
        return <Chip icon={<CheckCircleIcon />} label="Approved" color="success" />;
      case "changes_requested":
        return <Chip icon={<ErrorIcon />} label="Changes Requested" color="warning" />;
      default:
        return <Chip icon={<PendingIcon />} label="Pending Review" color="default" />;
    }
  };

  // Metric view YAML from Session 3
  const metricViewYaml = session3Data?.metric_view_yaml || "";

  return (
    <Box>
      <Alert severity="info" sx={{ mb: 2 }}>
        <strong>COE Review:</strong> The analyst submits their work from Sessions 1-3 for Center of Excellence
        approval. Include your commentary, review the data plan, and ensure metric views are addressed.
        COE reviewers will approve or request changes before the Genie Space can be configured.
      </Alert>

      {/* Approval Status */}
      <Box sx={{ mb: 2, display: "flex", alignItems: "center", gap: 2 }}>
        <Typography variant="subtitle1"><strong>Approval Status:</strong></Typography>
        {statusChip()}
        {data.coe_reviewer_email && (
          <Typography variant="body2" color="text.secondary">
            Reviewed by: {data.coe_reviewer_email}
          </Typography>
        )}
      </Box>

      {data.coe_approval_notes && approvalStatus === "changes_requested" && (
        <Alert severity="warning" sx={{ mb: 2 }}>
          <strong>COE Feedback:</strong> {data.coe_approval_notes}
        </Alert>
      )}

      {/* Analyst Commentary */}
      <Accordion defaultExpanded>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Typography variant="h6">Analyst Commentary</Typography>
        </AccordionSummary>
        <AccordionDetails>
          <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
            Summarize what you learned from the business owner, your technical approach,
            and anything the COE should know when reviewing this engagement.
          </Typography>
          <ExpandableTextField
            minRows={6}
            placeholder="Describe your findings, approach, and recommendations for the COE..."
            value={data.analyst_commentary || ""}
            onChange={(v) => onChange("analyst_commentary", v)}
            disabled={readOnly}
            dialogTitle="Analyst Commentary"
          />
        </AccordionDetails>
      </Accordion>

      {/* Auto-Summary */}
      <Accordion defaultExpanded>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Typography variant="h6">Sessions 1-3 Summary</Typography>
        </AccordionSummary>
        <AccordionDetails>
          <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
            Auto-generated snapshot of what was captured in Sessions 1-3.
          </Typography>
          {!readOnly && (
            <Button
              size="small"
              variant="outlined"
              onClick={fetchSummary}
              disabled={loadingSummary}
              sx={{ mb: 2 }}
            >
              {loadingSummary ? "Generating..." : "Refresh Summary"}
            </Button>
          )}
          <Paper variant="outlined" sx={{ p: 2, bgcolor: "grey.50", whiteSpace: "pre-wrap", fontSize: 14 }}>
            {summary || "No summary available. Click Refresh to generate."}
          </Paper>
        </AccordionDetails>
      </Accordion>

      {/* Data Plan */}
      <Accordion defaultExpanded>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>
            <Typography variant="h6">Data Plan</Typography>
            <Chip
              label={`${(data.data_plan || []).length} items`}
              size="small"
              variant="outlined"
            />
          </Box>
        </AccordionSummary>
        <AccordionDetails>
          <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
            Tables and metric views that will be included in the Genie Space. Pre-populated
            from Session 3. Add additional tables or metric views you have created.
          </Typography>
          <EditableTable
            columns={DATA_PLAN_COLS}
            rows={data.data_plan || []}
            onChange={(rows) => onChange("data_plan", rows)}
            readOnly={readOnly}
          />
        </AccordionDetails>
      </Accordion>

      {/* Benchmark Questions */}
      <Accordion defaultExpanded>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Stack direction="row" alignItems="center" spacing={1}>
            <Typography variant="h6">Benchmark Questions</Typography>
            <Chip label={`${benchmarks.length} drafted`} size="small" variant="outlined" />
            <Chip
              label={`${approvedBenchmarkCount} BO-approved`}
              size="small"
              color={approvedBenchmarkCount >= 5 ? "success" : "default"}
            />
          </Stack>
        </AccordionSummary>
        <AccordionDetails>
          <Alert severity="info" sx={{ mb: 2 }}>
            These are the <strong>acceptance-test questions</strong> the Genie Space will be scored
            against (target &gt;80% pass rate). The analyst, business owner, and COE reviewer must
            agree on these before the space is built. They will NOT be pushed as sample questions
            or example queries — the whole point is to measure whether Genie can answer them from
            the other configured context. Minimum 5 BO-approved to unlock COE approval.
          </Alert>

          {!readOnly && (
            <Stack direction="row" spacing={1} sx={{ mb: 2, flexWrap: "wrap" }}>
              <Button
                variant="outlined"
                size="small"
                startIcon={draftingBenchmarks ? <CircularProgress size={14} /> : <AutoAwesomeIcon />}
                onClick={draftAllBenchmarks}
                disabled={draftingBenchmarks}
              >
                {draftingBenchmarks ? "Drafting..." : "Draft Benchmarks from Sessions 1-3"}
              </Button>
              <Button
                variant="outlined"
                size="small"
                color="secondary"
                startIcon={draftingAllSql ? <CircularProgress size={14} /> : <AutoAwesomeIcon />}
                onClick={draftAllSql}
                disabled={draftingAllSql || benchmarks.filter((b) => b.question?.trim() && !b.expected_sql?.trim()).length === 0}
              >
                {draftingAllSql ? "Drafting SQL..." : "Draft All Expected SQL"}
              </Button>
              <Button size="small" startIcon={<AddIcon />} onClick={addBenchmark}>
                Add blank row
              </Button>
            </Stack>
          )}
          {!readOnly && (
            <Alert severity="warning" sx={{ mb: 2 }} variant="outlined">
              LLM-drafted SQL is a starting point only. <strong>Verify every query</strong> runs
              against your data and returns what the question asks before marking it BO-approved.
            </Alert>
          )}

          {benchmarks.length > 0 && (
            <Stack spacing={2}>
              {benchmarks.map((b, i) => (
                <Paper key={i} variant="outlined" sx={{ p: 2 }}>
                  <Stack direction="row" alignItems="center" spacing={1} sx={{ mb: 1 }}>
                    <Chip label={`#${i + 1}`} size="small" />
                    <Select
                      size="small"
                      value={b.category || "Core"}
                      onChange={(e) => updateBenchmark(i, "category", e.target.value)}
                      disabled={readOnly}
                      sx={{ minWidth: 120 }}
                    >
                      <MenuItem value="Core">Core</MenuItem>
                      <MenuItem value="Edge Case">Edge Case</MenuItem>
                    </Select>
                    <Select
                      size="small"
                      value={b.difficulty || "Medium"}
                      onChange={(e) => updateBenchmark(i, "difficulty", e.target.value)}
                      disabled={readOnly}
                      sx={{ minWidth: 110 }}
                    >
                      <MenuItem value="Easy">Easy</MenuItem>
                      <MenuItem value="Medium">Medium</MenuItem>
                      <MenuItem value="Hard">Hard</MenuItem>
                    </Select>
                    <Box sx={{ flex: 1 }} />
                    <Tooltip
                      title={
                        !b.question?.trim() || !b.expected_sql?.trim()
                          ? "Question and SQL required before BO approval"
                          : "BO approved"
                      }
                    >
                      <span>
                        <Stack direction="row" alignItems="center" spacing={0.5}>
                          <Checkbox
                            size="small"
                            checked={!!b.bo_approved}
                            onChange={(e) => updateBenchmark(i, "bo_approved", e.target.checked)}
                            disabled={readOnly || !b.question?.trim() || !b.expected_sql?.trim()}
                          />
                          <Typography variant="caption">BO approved</Typography>
                        </Stack>
                      </span>
                    </Tooltip>
                    {!readOnly && (
                      <IconButton size="small" onClick={() => removeBenchmark(i)}>
                        <DeleteIcon fontSize="small" />
                      </IconButton>
                    )}
                  </Stack>

                  <TextField
                    fullWidth multiline size="small"
                    label="Question"
                    value={b.question || ""}
                    onChange={(e) => updateBenchmark(i, "question", e.target.value)}
                    disabled={readOnly}
                    sx={{ mb: 1.5 }}
                  />

                  <Stack direction="row" alignItems="center" spacing={1} sx={{ mb: 0.5 }}>
                    <Typography variant="caption" color="text.secondary">
                      Expected SQL
                    </Typography>
                    {!readOnly && (
                      <Tooltip title="Draft SQL for this row with AI">
                        <span>
                          <IconButton
                            size="small"
                            onClick={() => draftSqlForRow(i)}
                            disabled={draftingSqlIdx === i || !b.question?.trim()}
                          >
                            {draftingSqlIdx === i ? <CircularProgress size={14} /> : <AutoAwesomeIcon fontSize="small" />}
                          </IconButton>
                        </span>
                      </Tooltip>
                    )}
                  </Stack>
                  <ExpandableTextField
                    value={b.expected_sql || ""}
                    onChange={(v) => updateBenchmark(i, "expected_sql", v)}
                    placeholder="SELECT ..."
                    disabled={readOnly}
                    minRows={4}
                    monospace
                    dialogTitle={`Expected SQL — Benchmark #${i + 1}`}
                  />

                  <Box sx={{ mt: 1.5 }}>
                    <Typography variant="caption" color="text.secondary" sx={{ mb: 0.5, display: "block" }}>
                      Notes
                    </Typography>
                    <ExpandableTextField
                      value={b.notes || ""}
                      onChange={(v) => updateBenchmark(i, "notes", v)}
                      placeholder="Clarifications, edge-case assumptions, acceptance notes..."
                      disabled={readOnly}
                      minRows={2}
                      dialogTitle={`Notes — Benchmark #${i + 1}`}
                    />
                  </Box>
                </Paper>
              ))}
            </Stack>
          )}
          {benchmarks.length === 0 && (
            <Typography variant="body2" color="text.secondary" sx={{ mt: 1 }}>
              No benchmarks yet. Click "Draft Benchmarks from Sessions 1-3" to seed a starting set.
            </Typography>
          )}
        </AccordionDetails>
      </Accordion>

      {/* Metric View YAML */}
      <Accordion defaultExpanded={!!metricViewYaml}>
        <AccordionSummary expandIcon={<ExpandMoreIcon />}>
          <Typography variant="h6">Recommended Metric View</Typography>
        </AccordionSummary>
        <AccordionDetails>
          <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
            Generated metric view YAML from Session 3 based on the SQL expressions defined.
            The analyst should create this metric view in the workspace before requesting approval.
          </Typography>
          {metricViewYaml ? (
            <Paper variant="outlined" sx={{ p: 2, bgcolor: "grey.900", color: "grey.100", fontFamily: "monospace", fontSize: 13, whiteSpace: "pre-wrap", overflowX: "auto" }}>
              {metricViewYaml}
            </Paper>
          ) : (
            <Alert severity="info">
              No metric view YAML has been generated yet. Complete Session 3's SQL Expressions
              section to generate a recommendation.
            </Alert>
          )}
        </AccordionDetails>
      </Accordion>

      {/* COE Approval Controls */}
      {isCoeMember && !readOnly && (
        <>
          <Divider sx={{ my: 3 }} />
          <Accordion defaultExpanded>
            <AccordionSummary expandIcon={<ExpandMoreIcon />}>
              <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>
                <Typography variant="h6">COE Review Controls</Typography>
                <Chip label="COE Only" size="small" color="primary" />
              </Box>
            </AccordionSummary>
            <AccordionDetails>
              <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
                Review the analyst's work above. Approve to unlock Sessions 5 & 6,
                or request changes with specific feedback.
              </Typography>
              <TextField
                multiline
                minRows={3}
                fullWidth
                label="Review Notes / Feedback"
                placeholder="Provide feedback or approval notes..."
                value={approvalNotes}
                onChange={(e) => setApprovalNotes(e.target.value)}
                sx={{ mb: 2 }}
              />
              {!canApprove && (
                <Alert severity="warning" sx={{ mb: 2 }}>
                  Approval is blocked until at least 5 benchmark questions are BO-approved and have expected SQL. Currently {approvedBenchmarkCount} of 5 required.
                </Alert>
              )}
              <Box sx={{ display: "flex", gap: 2 }}>
                <Tooltip title={canApprove ? "" : "Requires >=5 BO-approved benchmarks"}>
                  <span>
                    <Button
                      variant="contained"
                      color="success"
                      onClick={() => handleApproval("approved")}
                      disabled={!canApprove}
                    >
                      Approve
                    </Button>
                  </span>
                </Tooltip>
                <Button
                  variant="outlined"
                  color="warning"
                  onClick={() => handleApproval("changes_requested")}
                >
                  Request Changes
                </Button>
              </Box>
            </AccordionDetails>
          </Accordion>
        </>
      )}
    </Box>
  );
}
