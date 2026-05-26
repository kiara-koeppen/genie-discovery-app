import { useState, useEffect, useMemo } from "react";
import {
  Box, Typography, Stack, IconButton, Button, Paper, Alert,
  CircularProgress, Accordion, AccordionSummary, AccordionDetails,
  Checkbox, Table, TableBody, TableCell, TableHead, TableRow,
  FormControl, InputLabel, Select, MenuItem, TextField,
} from "@mui/material";
import ExpandMoreIcon from "@mui/icons-material/ExpandMore";
import DeleteIcon from "@mui/icons-material/Delete";
import AddIcon from "@mui/icons-material/Add";
import TableChartIcon from "@mui/icons-material/TableChart";
import InsightsIcon from "@mui/icons-material/Insights";
import WarningAmberIcon from "@mui/icons-material/WarningAmber";
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
  /** SQL warehouse used for (a) DESCRIBE EXTENDED on MV details and (b)
   *  the broad discovery scan via system.information_schema. */
  warehouseId: string;
  /** Persists a new warehouse_id selection back to the engagement (the
   *  inline picker writes to S5's plan_warehouse_id so it's used in the
   *  Generate Plan flow too). Optional -- if absent, the picker is hidden. */
  onChangeWarehouseId?: (warehouseId: string) => void;
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
  dataPlan, onChangeDataPlan, warehouseId, onChangeWarehouseId, readOnly,
}: Props) {
  const [pickerValue, setPickerValue] = useState("");
  // Loading state for the Add button -- briefly true while we look up
  // the picked FQN's table_type to decide which bucket it belongs in.
  const [addingTable, setAddingTable] = useState(false);
  const [addError, setAddError] = useState("");
  const [discoveredMvs, setDiscoveredMvs] = useState<DiscoveredMv[]>([]);
  const [discoveringMvs, setDiscoveringMvs] = useState(false);
  // Discovery surface: fatal/partial errors per schema, non-fatal warnings,
  // and whether the broad scan via system.information_schema ran. Lets the
  // UI distinguish "0 MVs in scope" from "couldn't search" and tells the
  // analyst when discovery is narrowed.
  const [discoveryErrors, setDiscoveryErrors] = useState<string[]>([]);
  const [discoveryWarnings, setDiscoveryWarnings] = useState<string[]>([]);
  const [scopeBroad, setScopeBroad] = useState(false);
  // Per-MV details cache (DESCRIBE EXTENDED) -- fetched lazily on first expand.
  const [mvDetailsCache, setMvDetailsCache] = useState<Record<string, MvDetails>>({});
  const [loadingDetails, setLoadingDetails] = useState<Set<string>>(new Set());
  const [detailsErrors, setDetailsErrors] = useState<Record<string, string>>({});
  // Warehouse list for the inline picker (#8). Loaded once when the panel
  // mounts. The picker only renders when no warehouse is set yet AND the
  // parent has wired onChangeWarehouseId.
  const [warehouses, setWarehouses] = useState<
    { id: string; name: string; state: string; size: string }[]
  >([]);

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
      setDiscoveryErrors([]);
      setDiscoveryWarnings([]);
      setScopeBroad(false);
      return;
    }
    let cancelled = false;
    setDiscoveringMvs(true);
    setDiscoveryErrors([]);
    setDiscoveryWarnings([]);
    api.findMetricViewsForTables(pickedTableFqns, warehouseId || undefined)
      .then(result => {
        if (cancelled) return;
        setDiscoveredMvs(result.metric_views);
        setDiscoveryErrors(result.errors || []);
        setDiscoveryWarnings(result.warnings || []);
        setScopeBroad(!!result.scope?.broad);
      })
      .catch(err => {
        if (cancelled) return;
        setDiscoveryErrors([err?.message || "Discovery failed"]);
      })
      .finally(() => { if (!cancelled) setDiscoveringMvs(false); });
    return () => { cancelled = true; };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [pickedKey, warehouseId]);

  // Load warehouse list for the inline picker on mount (only if the parent
  // wired the onChange callback -- otherwise the picker doesn't render).
  useEffect(() => {
    if (!onChangeWarehouseId) return;
    api.listWarehouses()
      .then(setWarehouses)
      .catch(() => setWarehouses([]));
  }, [onChangeWarehouseId]);

  const handleAddTable = async () => {
    if (!pickerValue || pickerValue.split(".").length !== 3) return;
    if (dataPlan.some(d => d.table_or_view === pickerValue)) {
      setAddError("That FQN is already in the data plan.");
      return;
    }
    setAddingTable(true);
    setAddError("");
    try {
      // The picker dropdown lists managed tables, regular SQL views, and
      // metric views all by name -- we can't tell them apart from the FQN
      // alone. Hit /api/uc/table-type to read the authoritative table_type
      // and categorize accordingly. MVs land in the Metric Views bucket
      // (type="Metric View"); everything else lands in Tables (type="Table"
      // for now -- regular views are rare in Genie flows and don't need a
      // separate bucket).
      const info = await api.getTableType(pickerValue);
      const isMv = info.table_type === "METRIC_VIEW";
      onChangeDataPlan([
        ...dataPlan,
        {
          table_or_view: pickerValue,
          type: isMv ? "Metric View" : "Table",
          include_in_space: "Yes",
          notes: info.comment || "",
        },
      ]);
      // Bulk-add UX (#10): preserve catalog+schema after each Add so the
      // analyst can keep adding from the same schema without re-navigating
      // the dropdowns. Resets just the Table portion of the FQN.
      const parts = pickerValue.split(".");
      setPickerValue(`${parts[0]}.${parts[1]}`);
    } catch (err: any) {
      setAddError(err?.message || "Failed to look up table type.");
    } finally {
      setAddingTable(false);
    }
  };

  // Inline notes editing for data plan entries (#11). The old EditableTable
  // in S4 exposed a notes column; the new panel didn't. We add it back as a
  // small TextField under each entry so existing notes (auto-filled from
  // table descriptions on Add) can be edited and so analyst-authored
  // commentary survives the round trip.
  const handleNotesChange = (fqn: string, notes: string) => {
    onChangeDataPlan(
      dataPlan.map(d => (d.table_or_view === fqn ? { ...d, notes } : d)),
    );
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

      {/* Inline warehouse picker (#8). Renders only when the analyst hasn't
          configured a warehouse yet AND the parent passed onChangeWarehouseId.
          Wiring through to S5's plan_warehouse_id means setting it once here
          also unblocks the Generate Plan flow. */}
      {!readOnly && !warehouseId && onChangeWarehouseId && (
        <Paper variant="outlined" sx={{ p: 1.5, mb: 2 }}>
          <Stack direction="row" alignItems="center" spacing={1} flexWrap="wrap">
            <Typography variant="body2" sx={{ fontWeight: 500, flexShrink: 0 }}>
              SQL warehouse:
            </Typography>
            <FormControl size="small" sx={{ minWidth: 280 }}>
              <InputLabel>Pick a warehouse</InputLabel>
              <Select
                label="Pick a warehouse"
                value=""
                onChange={(e) => onChangeWarehouseId(e.target.value)}
              >
                <MenuItem value="">--</MenuItem>
                {warehouses.map((w) => (
                  <MenuItem key={w.id} value={w.id}>
                    {w.name} <Typography component="span" variant="caption" color="text.secondary" sx={{ ml: 0.5 }}>
                      ({w.size} · {w.state})
                    </Typography>
                  </MenuItem>
                ))}
              </Select>
            </FormControl>
            <Typography variant="caption" color="text.secondary">
              Used for MV details (DESCRIBE EXTENDED) and the broad MV discovery scan.
              Setting this here also propagates to Session 5's Generate Plan.
            </Typography>
          </Stack>
        </Paper>
      )}

      {/* Tables / Views */}
      <Typography variant="subtitle2" sx={{ mb: 1, fontWeight: 600 }}>
        Tables / Views in scope ({tableEntries.length})
      </Typography>
      {tableEntries.length === 0 ? (
        <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
          No tables picked yet. Add at least one below to discover related Metric Views.
        </Typography>
      ) : (
        <Stack spacing={1} sx={{ mb: 2 }}>
          {tableEntries.map((e, i) => (
            <Box key={i}>
              <Stack direction="row" alignItems="center" spacing={1}>
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
              {/* Notes (#11): inline editable. Surfaces the existing notes
                  field on data_plan entries (was previously editable only
                  in S4's old data plan table). */}
              <Box sx={{ pl: 4 }}>
                <TextField
                  size="small"
                  variant="standard"
                  fullWidth
                  placeholder="Notes (optional)"
                  value={e.notes || ""}
                  onChange={(ev) => handleNotesChange(e.table_or_view, ev.target.value)}
                  disabled={readOnly}
                  InputProps={{ sx: { fontSize: 12 } }}
                />
              </Box>
            </Box>
          ))}
        </Stack>
      )}

      {!readOnly && (
        <Paper variant="outlined" sx={{ p: 1.5, mb: 3 }}>
          <Typography variant="caption" color="text.secondary" sx={{ display: "block", mb: 1 }}>
            Add a table or metric view to the data plan. Metric views land in
            the section below; tables / views land above.
          </Typography>
          <Stack direction="row" alignItems="center" spacing={1} flexWrap="wrap">
            <Box sx={{ flexGrow: 1, minWidth: 320 }}>
              <UCTablePicker value={pickerValue} onChange={setPickerValue} />
            </Box>
            <Button
              variant="contained"
              size="small"
              onClick={handleAddTable}
              disabled={
                !pickerValue ||
                pickerValue.split(".").length !== 3 ||
                addingTable
              }
              startIcon={addingTable ? <CircularProgress size={14} /> : <AddIcon />}
            >
              {addingTable ? "Adding…" : "Add"}
            </Button>
          </Stack>
          {addError && (
            <Alert severity="error" sx={{ mt: 1 }} onClose={() => setAddError("")}>
              {addError}
            </Alert>
          )}
        </Paper>
      )}

      {/* Metric Views */}
      {pickedTableFqns.length > 0 && (
        <Box>
          <Stack direction="row" alignItems="center" spacing={1} sx={{ mb: 1 }} flexWrap="wrap">
            <InsightsIcon fontSize="small" color="primary" />
            <Typography variant="subtitle2" sx={{ fontWeight: 600 }}>
              Existing Metric Views built on these tables ({allMvFqns.length} found)
            </Typography>
            {discoveringMvs && <CircularProgress size={14} />}
            {!discoveringMvs && (
              <Typography variant="caption" color="text.secondary" sx={{ ml: 0.5 }}>
                {scopeBroad
                  ? "scanned all visible catalogs"
                  : "scanned only the schemas of your picked tables"}
              </Typography>
            )}
          </Stack>

          {/* Discovery errors -- one or more schemas couldn't be searched.
              Distinguishes "0 matches" (no alert) from "couldn't search this
              schema" (warning). */}
          {discoveryErrors.length > 0 && (
            <Alert severity="warning" icon={<WarningAmberIcon />} sx={{ mb: 2 }}>
              <Typography variant="body2" sx={{ fontWeight: 500, mb: 0.5 }}>
                Some scopes couldn't be searched ({discoveryErrors.length}):
              </Typography>
              <Box component="ul" sx={{ pl: 3, my: 0.5, "& li": { fontSize: 12 } }}>
                {discoveryErrors.map((e, i) => (
                  <li key={i}><Typography variant="caption">{e}</Typography></li>
                ))}
              </Box>
              <Typography variant="caption">
                Discovered MVs below may be incomplete. Most common cause: the
                user doesn't have READ on one of the picked tables' schemas.
              </Typography>
            </Alert>
          )}

          {/* Non-fatal warnings, e.g. broad scan fell back to schema-only. */}
          {discoveryWarnings.length > 0 && (
            <Alert severity="info" variant="outlined" sx={{ mb: 2 }}>
              {discoveryWarnings.map((w, i) => (
                <Typography key={i} variant="caption" sx={{ display: "block" }}>
                  {w}
                </Typography>
              ))}
            </Alert>
          )}

          {allMvFqns.length === 0 && !discoveringMvs && discoveryErrors.length === 0 && (
            <Alert severity="info" variant="outlined" sx={{ mb: 2 }}>
              No existing Metric Views use the picked tables {scopeBroad ? "(scanned all visible catalogs)" : "in these schemas"}.
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
                        Pinned from an earlier data plan. Not in the current
                        discovery scope — its source tables are in a different
                        schema from your picked tables. To re-discover, add one
                        of its source tables above. To remove, click the trash
                        icon at right.
                      </Typography>
                    )}

                    {/* Notes (#11): inline editable, only when this MV is
                        actually checked into the data plan (otherwise it's
                        a discovery candidate that hasn't been picked yet). */}
                    {checked && (
                      <TextField
                        size="small"
                        variant="standard"
                        fullWidth
                        placeholder="Notes (optional)"
                        value={mvEntries.find(e => e.table_or_view === fqn)?.notes || ""}
                        onChange={(ev) => handleNotesChange(fqn, ev.target.value)}
                        disabled={readOnly}
                        InputProps={{ sx: { fontSize: 12 } }}
                        sx={{ mt: 0.5 }}
                      />
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
                  {!readOnly && !discovered && (
                    <IconButton
                      size="small"
                      onClick={() => handleRemove(fqn)}
                      title="Remove this Metric View from the data plan"
                      sx={{ mt: 0.5 }}
                    >
                      <DeleteIcon fontSize="small" />
                    </IconButton>
                  )}
                </Stack>
              </Paper>
            );
          })}
        </Box>
      )}
    </Box>
  );
}
