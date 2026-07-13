import { useState, useEffect, useMemo } from "react";
import {
  Dialog, DialogTitle, DialogContent, DialogActions, Button,
  Box, Typography, Checkbox, FormControlLabel, Stack, Alert, Chip,
  ToggleButton, ToggleButtonGroup,
} from "@mui/material";
import SaveAltIcon from "@mui/icons-material/SaveAlt";
import { api } from "../api";

// Order and labels match the backend's _PREWORK_SHEETS. Backend is
// authoritative; if you add a section there, add it here too.
const SECTION_LABELS: { key: string; label: string; session: number }[] = [
  { key: "business_context", label: "Business Context", session: 1 },
  { key: "pain_points", label: "Pain Points", session: 1 },
  { key: "existing_reports", label: "Existing Reports", session: 1 },
  { key: "vocabulary_metrics", label: "Key Terms & Metrics", session: 2 },
  { key: "question_bank", label: "Question Bank", session: 2 },
];

interface Props {
  open: boolean;
  engagementId: string;
  /** Current S1/S2 data, keyed by section. Rows are exported verbatim (WYSIWYG
   *  with the open forms). */
  currentData: Record<string, Record<string, string>[]>;
  onClose: () => void;
}

export default function PreworkExportModal({
  open, engagementId, currentData, onClose,
}: Props) {
  const counts = useMemo(() => {
    const c: Record<string, number> = {};
    for (const s of SECTION_LABELS) c[s.key] = (currentData[s.key] || []).length;
    return c;
  }, [currentData]);

  const [selected, setSelected] = useState<Record<string, boolean>>({});
  const [format, setFormat] = useState<"xlsx" | "csv">("xlsx");
  const [exporting, setExporting] = useState(false);
  const [error, setError] = useState("");

  // On open, default-check the sections that actually have data. Depend only on
  // `open` so a background autosave changing `currentData` can't clobber the
  // user's in-dialog checkbox choices.
  useEffect(() => {
    if (!open) return;
    const init: Record<string, boolean> = {};
    for (const s of SECTION_LABELS) init[s.key] = counts[s.key] > 0;
    setSelected(init);
    setError("");
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open]);

  const chosen = SECTION_LABELS.filter((s) => selected[s.key]).map((s) => s.key);

  const doExport = async () => {
    setExporting(true);
    setError("");
    try {
      const data: Record<string, Record<string, string>[]> = {};
      for (const k of chosen) data[k] = currentData[k] || [];
      await api.exportPrework(engagementId, chosen, data, format);
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
      <DialogTitle>Export</DialogTitle>
      <DialogContent dividers>
        <Typography variant="body2" sx={{ mb: 2 }}>
          Choose which sections to export. The <strong>.xlsx</strong> matches the pre-work
          template, so you can edit it and load it back via <strong>Upload Pre-Work</strong>.
          The <strong>.csv</strong> is a flat export for loading into Genie Code.
        </Typography>

        <Box sx={{ mb: 2 }}>
          <Typography variant="overline" color="text.secondary">Format</Typography>
          <Box>
            <ToggleButtonGroup
              size="small"
              exclusive
              value={format}
              onChange={(_, v) => { if (v) setFormat(v); }}
            >
              <ToggleButton value="xlsx">.xlsx (re-uploadable)</ToggleButton>
              <ToggleButton value="csv">.csv (Genie Code)</ToggleButton>
            </ToggleButtonGroup>
          </Box>
        </Box>

        <Typography variant="overline" color="text.secondary">
          Session 1 — Business Context
        </Typography>
        <Stack sx={{ mb: 1.5, pl: 0.5 }}>
          {SECTION_LABELS.filter((s) => s.session === 1).map(renderSection)}
        </Stack>

        <Typography variant="overline" color="text.secondary">
          Session 2 — Key Terms & Questions
        </Typography>
        <Stack sx={{ pl: 0.5 }}>
          {SECTION_LABELS.filter((s) => s.session === 2).map(renderSection)}
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
          {exporting ? "Exporting…" : `Export .${format}`}
        </Button>
      </DialogActions>
    </Dialog>
  );
}
