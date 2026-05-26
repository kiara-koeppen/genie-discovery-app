import { useState, useEffect, useMemo } from "react";
import {
  Box, Typography, Stack, IconButton, Button, Paper, Alert,
  CircularProgress, Accordion, AccordionSummary, AccordionDetails,
  Checkbox, Table, TableBody, TableCell, TableHead, TableRow,
} from "@mui/material";
import ExpandMoreIcon from "@mui/icons-material/ExpandMore";
import DeleteIcon from "@mui/icons-material/Delete";
import AddIcon from "@mui/icons-material/Add";
import TableChartIcon from "@mui/icons-material/TableChart";
import InsightsIcon from "@mui/icons-material/Insights";
import UCTablePicker from "./UCTablePicker";
import { api } from "../api";

/**
 * S3 "Data Sources" panel. The analyst picks tables they know the Genie space
 * will use; the app auto-discovers existing Metric Views built on those tables
 * so the analyst can reuse them instead of re-authoring measures.
 *
 * Source of truth is the engagement's `data_plan` field (lives on S4 in the
 * current schema). This component reads + writes that field via the
 * dataPlan/onChangeDataPlan props -- the parent (Session3Form) passes
 * session4Data through.
 *
 * Phase 1 implementation. Discovery is deterministic via the UC tables REST
 * endpoint (no LLM). Phase 2 would layer an LLM coverage analysis on top
 * (match MV measures against the BO question bank) -- not built yet.
 */

interface DataPlanEntry {
  table_or_view: string;
  type: string;
  include_in_space: string;
  notes: string;
}

interface Props {
  dataPlan: DataPlanEntry[];
  onChangeDataPlan: (next: DataPlanEntry[]) => void;
  /** SQL warehouse used to run DESCRIBE EXTENDED for MV details. If empty,
   *  the "view columns" expander shows a hint pointing to Session 5's warehouse picker. */
  warehouseId: string;
  readOnly?: boolean;
}

interface DiscoveredMv {
  fqn: string;
  catalog: string;
  schema: string;
  name: string;
  comment: string;
  owner: string;
  updated_at?: string;
  dependencies: string[];
}

interface MvColumnEntry {
  name: string;
  display_name: string;
  synonyms: string[];
  comment: string;
  data_type: string;
}

interface MvDetails {
  fqn: string;
  dimensions: MvColumnEntry[];
  measures: MvColumnEntry[];
}

export default function DataSourcesPanel({
  dataPlan, onChangeDataPlan, warehouseId, readOnly,
}: Props) {
  const [pickerValue, setPickerValue] = useState("");
  const [discoveredMvs, setDiscoveredMvs] = useState<DiscoveredMv[]>([]);
  const [discoveringMvs, setDiscoveringMvs] = useState(false);
  const [discoveryError, setDiscoveryError] = useState("");
  // Per-MV details cache (DESCRIBE EXTENDED) -- fetched lazily on first expand.
  const [mvDetailsCache, setMvDetailsCache] = useState<Record<string, MvDetails>>({});
  const [loadingDetails, setLoadingDetails] = useState<Set<string>>(new Set());
  const [detailsErrors, setDetailsErrors] = useState<Record<string, string>>({});

  // Derived: tables vs metric views, both filtered to "Yes" inclusion.
  const tableEntries = useMemo(
    () => dataPlan.filter(d => d.include_in_space === "Yes" && d.type !== "Metric View"),
    [dataPlan],
  );
  const mvEntries = useMemo(
    () => dataPlan.filter(d => d.include_in_space === "Yes" && d.type === "Metric View"),
    [dataPlan],
  );
  const pickedTableFqns = useMemo(
    () => tableEntries.map(d => d.table_or_view).filter(t => t && t.split(".").length === 3),
    [tableEntries],
  );

  // Trigger MV discovery whenever the picked tables change. Memo key is the
  // sorted join so we don't re-fire on dataPlan reorders that don't affect
  // the table set.
  const pickedKey = useMemo(
    () => [...pickedTableFqns].sort().join(","),
    [pickedTableFqns],
  );
  useEffect(() => {
    if (!pickedTableFqns.length) {
      setDiscoveredMvs([]);
      setDiscoveryError("");
      return;
    }
    let cancelled = false;
    setDiscoveringMvs(true);
    setDiscoveryError("");
    api.findMetricViewsForTables(pickedTableFqns)
      .then(results => { if (!cancelled) setDiscoveredMvs(results); })
      .catch(err => { if (!cancelled) setDiscoveryError(err?.message || "Discovery failed"); })
      .finally(() => { if (!cancelled) setDiscoveringMvs(false); });
    return () => { cancelled = true; };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [pickedKey]);

  const handleAddTable = () => {
    if (!pickerValue || pickerValue.split(".").length !== 3) return;
    if (dataPlan.some(d => d.table_or_view === pickerValue)) return;
    onChangeDataPlan([
      ...dataPlan,
      { table_or_view: pickerValue, type: "Table", include_in_space: "Yes", notes: "" },
    ]);
    setPickerValue("");
  };

  const handleRemove = (fqn: string) => {
    onChangeDataPlan(dataPlan.filter(d => d.table_or_view !== fqn));
  };

  const handleToggleMv = (mv: DiscoveredMv, include: boolean) => {
    if (include) {
      if (dataPlan.some(d => d.table_or_view === mv.fqn)) return;
      onChangeDataPlan([
        ...dataPlan,
        {
          table_or_view: mv.fqn,
          type: "Metric View",
          include_in_space: "Yes",
          notes: mv.comment || "",
        },
      ]);
    } else {
      onChangeDataPlan(dataPlan.filter(d => d.table_or_view !== mv.fqn));
    }
  };

  const loadDetails = async (fqn: string) => {
    if (mvDetailsCache[fqn]) return;
    if (!warehouseId) {
      setDetailsErrors(prev => ({
        ...prev,
        [fqn]: "Select a SQL warehouse in Session 5 first — DESCRIBE EXTENDED needs one to run.",
      }));
      return;
    }
    setLoadingDetails(prev => new Set(prev).add(fqn));
    try {
      const details = await api.fetchMetricViewDetails(fqn, warehouseId);
      setMvDetailsCache(prev => ({ ...prev, [fqn]: details }));
      setDetailsErrors(prev => {
        const next = { ...prev };
        delete next[fqn];
        return next;
      });
    } catch (err: any) {
      setDetailsErrors(prev => ({
        ...prev,
        [fqn]: err?.message || "Details fetch failed",
      }));
    } finally {
      setLoadingDetails(prev => {
        const next = new Set(prev);
        next.delete(fqn);
        return next;
      });
    }
  };

  // Combine in-plan MVs and discovered MVs into one display list. In-plan
  // MVs that aren't in the current discovery set get a "pinned from earlier"
  // marker -- their checkbox stays checked but disabled (since we'd need
  // discovery data to re-add them after un-checking).
  const allMvFqns = useMemo(() => {
    const set = new Set<string>();
    mvEntries.forEach(e => set.add(e.table_or_view));
    discoveredMvs.forEach(m => set.add(m.fqn));
    return Array.from(set).sort();
  }, [mvEntries, discoveredMvs]);

  return (
    <Box>
      <Alert severity="info" sx={{ mb: 2 }}>
        Pick the tables your Genie space will use. The app auto-discovers existing
        Metric Views built on those tables so you can reuse them instead of re-authoring
        measures. Checked items flow into the Session 4 Data Plan automatically.
      </Alert>

      {/* Tables / Views */}
      <Typography variant="subtitle2" sx={{ mb: 1, fontWeight: 600 }}>
        Tables / Views in scope ({tableEntries.length})
      </Typography>
      {tableEntries.length === 0 ? (
        <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
          No tables picked yet. Add at least one below to discover related Metric Views.
        </Typography>
      ) : (
        <Stack spacing={0.5} sx={{ mb: 2 }}>
          {tableEntries.map((e, i) => (
            <Stack key={i} direction="row" alignItems="center" spacing={1}>
              <TableChartIcon fontSize="small" color="action" />
              <Typography variant="body2" sx={{ fontFamily: "monospace", fontSize: 13, flexGrow: 1 }}>
                {e.table_or_view}
              </Typography>
              <Typography variant="caption" color="text.secondary">
                {e.type || "Table"}
              </Typography>
              {!readOnly && (
                <IconButton size="small" onClick={() => handleRemove(e.table_or_view)} title="Remove from data plan">
                  <DeleteIcon fontSize="small" />
                </IconButton>
              )}
            </Stack>
          ))}
        </Stack>
      )}

      {!readOnly && (
        <Paper variant="outlined" sx={{ p: 1.5, mb: 3 }}>
          <Typography variant="caption" color="text.secondary" sx={{ display: "block", mb: 1 }}>
            Add a table to the data plan:
          </Typography>
          <Stack direction="row" alignItems="center" spacing={1} flexWrap="wrap">
            <Box sx={{ flexGrow: 1, minWidth: 320 }}>
              <UCTablePicker value={pickerValue} onChange={setPickerValue} />
            </Box>
            <Button
              variant="contained"
              size="small"
              onClick={handleAddTable}
              disabled={!pickerValue || pickerValue.split(".").length !== 3}
              startIcon={<AddIcon />}
            >
              Add
            </Button>
          </Stack>
        </Paper>
      )}

      {/* Metric Views */}
      {pickedTableFqns.length > 0 && (
        <Box>
          <Stack direction="row" alignItems="center" spacing={1} sx={{ mb: 1 }}>
            <InsightsIcon fontSize="small" color="primary" />
            <Typography variant="subtitle2" sx={{ fontWeight: 600 }}>
              Existing Metric Views built on these tables ({allMvFqns.length} found)
            </Typography>
            {discoveringMvs && <CircularProgress size={14} />}
          </Stack>

          {discoveryError && (
            <Alert severity="warning" sx={{ mb: 2 }}>{discoveryError}</Alert>
          )}

          {allMvFqns.length === 0 && !discoveringMvs && (
            <Alert severity="info" variant="outlined" sx={{ mb: 2 }}>
              No existing Metric Views in these schemas use the picked tables.
              You can author a new one from scratch in the Metric View section below.
            </Alert>
          )}

          {allMvFqns.map(fqn => {
            const discovered = discoveredMvs.find(m => m.fqn === fqn);
            const checked = mvEntries.some(e => e.table_or_view === fqn);
            const details = mvDetailsCache[fqn];
            const loading = loadingDetails.has(fqn);
            const detailErr = detailsErrors[fqn];
            return (
              <Paper
                key={fqn}
                variant="outlined"
                sx={{
                  p: 1.5,
                  mb: 1.5,
                  bgcolor: checked ? "action.selected" : "background.paper",
                }}
              >
                <Stack direction="row" alignItems="flex-start" spacing={1}>
                  {!readOnly && (
                    <Checkbox
                      checked={checked}
                      onChange={(e) => discovered && handleToggleMv(discovered, e.target.checked)}
                      disabled={!discovered}
                      sx={{ p: 0.5, mt: 0.5 }}
                    />
                  )}
                  <Box sx={{ flexGrow: 1, minWidth: 0 }}>
                    <Typography
                      variant="body2"
                      sx={{ fontFamily: "monospace", fontSize: 13, fontWeight: 600, wordBreak: "break-all" }}
                    >
                      {fqn}
                    </Typography>
                    {discovered?.comment && (
                      <Typography variant="body2" color="text.secondary" sx={{ mt: 0.25 }}>
                        {discovered.comment}
                      </Typography>
                    )}
                    {discovered && (
                      <Typography variant="caption" color="text.secondary" sx={{ display: "block", mt: 0.5 }}>
                        Owner: {discovered.owner || "(unknown)"}
                        {" · Uses: "}
                        {discovered.dependencies.map(d => (
                          <code key={d} style={{ fontSize: 11, marginRight: 4 }}>{d}</code>
                        ))}
                      </Typography>
                    )}
                    {!discovered && (
                      <Typography variant="caption" color="warning.main" sx={{ display: "block", mt: 0.5 }}>
                        Pinned from a previous data plan. Not in the current discovery
                        scope -- the tables it depends on may have been removed.
                      </Typography>
                    )}

                    <Accordion
                      variant="outlined"
                      sx={{ mt: 1, "&::before": { display: "none" } }}
                      onChange={(_, expanded) => { if (expanded) loadDetails(fqn); }}
                    >
                      <AccordionSummary expandIcon={<ExpandMoreIcon />}>
                        <Typography variant="caption">
                          View dimensions and measures
                        </Typography>
                      </AccordionSummary>
                      <AccordionDetails>
                        {loading && (
                          <Stack direction="row" alignItems="center" spacing={1}>
                            <CircularProgress size={14} />
                            <Typography variant="caption" color="text.secondary">
                              Loading DESCRIBE EXTENDED…
                            </Typography>
                          </Stack>
                        )}
                        {detailErr && (
                          <Alert severity="error" sx={{ mb: 1 }}>{detailErr}</Alert>
                        )}
                        {details && (
                          <Box>
                            <Typography variant="caption" sx={{ fontWeight: 600, color: "text.secondary" }}>
                              Measures ({details.measures.length})
                            </Typography>
                            {details.measures.length === 0 ? (
                              <Typography variant="caption" color="text.secondary" sx={{ display: "block", ml: 1, mb: 2 }}>
                                (none)
                              </Typography>
                            ) : (
                              <Table size="small" sx={{ mb: 2 }}>
                                <TableHead>
                                  <TableRow sx={{ bgcolor: "grey.50" }}>
                                    <TableCell sx={{ fontWeight: 600, py: 0.5 }}>Display Name</TableCell>
                                    <TableCell sx={{ fontWeight: 600, py: 0.5 }}>Column</TableCell>
                                    <TableCell sx={{ fontWeight: 600, py: 0.5 }}>Description</TableCell>
                                    <TableCell sx={{ fontWeight: 600, py: 0.5 }}>Synonyms</TableCell>
                                  </TableRow>
                                </TableHead>
                                <TableBody>
                                  {details.measures.map((m, i) => (
                                    <TableRow key={i}>
                                      <TableCell sx={{ py: 0.5, fontSize: 13 }}>
                                        {m.display_name || m.name}
                                      </TableCell>
                                      <TableCell sx={{ py: 0.5, fontFamily: "monospace", fontSize: 12 }}>
                                        {m.name}
                                      </TableCell>
                                      <TableCell sx={{ py: 0.5, fontSize: 12 }}>
                                        {m.comment}
                                      </TableCell>
                                      <TableCell sx={{ py: 0.5, fontSize: 12 }}>
                                        {m.synonyms.join(", ")}
                                      </TableCell>
                                    </TableRow>
                                  ))}
                                </TableBody>
                              </Table>
                            )}
                            <Typography variant="caption" sx={{ fontWeight: 600, color: "text.secondary" }}>
                              Dimensions ({details.dimensions.length})
                            </Typography>
                            {details.dimensions.length === 0 ? (
                              <Typography variant="caption" color="text.secondary" sx={{ display: "block", ml: 1 }}>
                                (none)
                              </Typography>
                            ) : (
                              <Table size="small">
                                <TableHead>
                                  <TableRow sx={{ bgcolor: "grey.50" }}>
                                    <TableCell sx={{ fontWeight: 600, py: 0.5 }}>Display Name</TableCell>
                                    <TableCell sx={{ fontWeight: 600, py: 0.5 }}>Column</TableCell>
                                    <TableCell sx={{ fontWeight: 600, py: 0.5 }}>Type</TableCell>
                                    <TableCell sx={{ fontWeight: 600, py: 0.5 }}>Synonyms</TableCell>
                                  </TableRow>
                                </TableHead>
                                <TableBody>
                                  {details.dimensions.map((d, i) => (
                                    <TableRow key={i}>
                                      <TableCell sx={{ py: 0.5, fontSize: 13 }}>
                                        {d.display_name || d.name}
                                      </TableCell>
                                      <TableCell sx={{ py: 0.5, fontFamily: "monospace", fontSize: 12 }}>
                                        {d.name}
                                      </TableCell>
                                      <TableCell sx={{ py: 0.5, fontSize: 12 }}>
                                        {d.data_type}
                                      </TableCell>
                                      <TableCell sx={{ py: 0.5, fontSize: 12 }}>
                                        {d.synonyms.join(", ")}
                                      </TableCell>
                                    </TableRow>
                                  ))}
                                </TableBody>
                              </Table>
                            )}
                          </Box>
                        )}
                      </AccordionDetails>
                    </Accordion>
                  </Box>
                </Stack>
              </Paper>
            );
          })}
        </Box>
      )}
    </Box>
  );
}
