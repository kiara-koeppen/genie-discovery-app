import { useState, useEffect, useMemo } from "react";
import {
  Dialog, DialogTitle, DialogContent, DialogActions, Button,
  Box, Typography, Checkbox, FormControlLabel, Stack, Alert, Chip,
} from "@mui/material";
import SaveAltIcon from "@mui/icons-material/SaveAlt";
import { api, BenchmarkQuestion } from "../api";

// Order and labels match the backend's _PREWORK_SHEETS (the only sections that
// export to a clean tabular layout). Backend is authoritative; if you add a
// section there, add it here too.
const SECTION_LABELS: { key: string; label: string; session: number }[] = [
  { key: "business_context", label: "Business Context", session: 1 },
  { key: "pain_points", label: "Pain Points", session: 1 },
  { key: "existing_reports", label: "Existing Reports", session: 1 },
  { key: "question_bank", label: "Question Bank", session: 2 },
  { key: "vocabulary_metrics", label: "Key Terms & Metrics", session: 2 },
];

// Export-only key for S4 benchmarks. Not in SECTION_LABELS because it isn't part
// of the re-uploadable round-trip and carries a different data shape.
const BENCHMARKS_KEY = "benchmarks";

interface Props {
  open: boolean;
  engagementId: string;
  /** Current S1/S2 data, keyed by section. Rows are exported verbatim (WYSIWYG
   *  with the open forms). */
  currentData: Record<string, Record<string, string>[]>;
  /** Current S4 benchmark rows (export-only). */
  benchmarks?: BenchmarkQuestion[];
  onClose: () => void;
}

export default function PreworkExportModal({
  open, engagementId, currentData, benchmarks = [], onClose,
}: Props) {
  const counts = useMemo(() => {
    const c: Record<string, number> = {};
    for (const s of SECTION_LABELS) c[s.key] = (currentData[s.key] || []).length;
    c[BENCHMARKS_KEY] = benchmarks.length;
    return c;
  }, [currentData, benchmarks]);

  const [selected, setSelected] = useState<Record<string, boolean>>({});
  const [exporting, setExporting] = useState(false);
  const [error, setError] = useState("");

  // On open, default-check the sections that actually have data. Depend only on
  // `open` so a background autosave changing `currentData` can't clobber the
  // user's in-dialog checkbox choices.
  useEffect(() => {
    if (!open) return;
    const init: Record<string, boolean> = {};
    for (const s of SECTION_LABELS) init[s.key] = counts[s.key] > 0;
    init[BENCHMARKS_KEY] = counts[BENCHMARKS_KEY] > 0;
    setSelected(init);
    setError("");
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open]);

  const chosen = [
    ...SECTION_LABELS.filter((s) => selected[s.key]).map((s) => s.key),
    ...(selected[BENCHMARKS_KEY] ? [BENCHMARKS_KEY] : []),
  ];

  const doExport = async () => {
    setExporting(true);
    setError("");
    try {
      const data: Record<string, Record<string, string>[]> = {};
      for (const k of chosen) {
        if (k === BENCHMARKS_KEY) continue;
        data[k] = currentData[k] || [];
      }
      await api.exportPrework(
        engagementId,
        chosen,
        data,
        selected[BENCHMARKS_KEY] ? benchmarks : undefined,
      );
      onClose();
    } catch (e: any) {
      setError(e?.message || "Export failed");
    } finally {
      setExporting(false);
    }
  };

  const renderSection = (s: { key: string; label: string }) => {
    const n = counts[s.key] || 0;
    return (
      <FormControlLabel
        key={s.key}
        control={
          <Checkbox
            size="small"
            checked={!!selected[s.key]}
            onChange={(e) =>
              setSelected((prev) => ({ ...prev, [s.key]: e.target.checked }))
            }
          />
        }
        label={
          <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>
            <span>{s.label}</span>
            <Chip
              size="small"
              variant="outlined"
              label={n === 0 ? "empty" : `${n} row${n === 1 ? "" : "s"}`}
              color={n === 0 ? "default" : "primary"}
            />
          </Box>
        }
      />
    );
  };

  return (
    <Dialog open={open} onClose={exporting ? undefined : onClose} maxWidth="sm" fullWidth>
      <DialogTitle>Export to Excel</DialogTitle>
      <DialogContent dividers>
        <Typography variant="body2" sx={{ mb: 2 }}>
          Choose which sections to export. Sessions 1 & 2 match the pre-work
          template, so you can edit them and load them back via{" "}
          <strong>Upload Pre-Work</strong>. Benchmarks are export-only.
        </Typography>

        <Typography variant="overline" color="text.secondary">
          Session 1 — Business Context
        </Typography>
        <Stack sx={{ mb: 1.5, pl: 0.5 }}>
          {SECTION_LABELS.filter((s) => s.session === 1).map(renderSection)}
        </Stack>

        <Typography variant="overline" color="text.secondary">
          Session 2 — Questions & Vocabulary
        </Typography>
        <Stack sx={{ mb: 1.5, pl: 0.5 }}>
          {SECTION_LABELS.filter((s) => s.session === 2).map(renderSection)}
        </Stack>

        <Typography variant="overline" color="text.secondary">
          Session 4 — Design Review & Approval
        </Typography>
        <Stack sx={{ pl: 0.5 }}>
          {renderSection({ key: BENCHMARKS_KEY, label: "Benchmarks" })}
        </Stack>

        {error && (
          <Alert severity="error" sx={{ mt: 2 }}>
            {error}
          </Alert>
        )}
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose} disabled={exporting}>
          Cancel
        </Button>
        <Button
          variant="contained"
          startIcon={<SaveAltIcon />}
          onClick={doExport}
          disabled={exporting || chosen.length === 0}
        >
          {exporting ? "Exporting…" : `Export ${chosen.length || ""} .xlsx`}
        </Button>
      </DialogActions>
    </Dialog>
  );
}
