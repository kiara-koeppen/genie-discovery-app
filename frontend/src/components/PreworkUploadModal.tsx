import { useState, useRef } from "react";
import {
  Dialog, DialogTitle, DialogContent, DialogActions, Button,
  Box, Typography, Alert, Stack, Chip, Checkbox, FormControlLabel,
  Divider, LinearProgress, Link, Paper,
} from "@mui/material";
import UploadFileIcon from "@mui/icons-material/UploadFile";
import DownloadIcon from "@mui/icons-material/Download";
import WarningAmberIcon from "@mui/icons-material/WarningAmber";
import ErrorOutlineIcon from "@mui/icons-material/ErrorOutline";
import { api } from "../api";

// Order and labels match the backend's _PREWORK_SHEETS. Kept in sync manually;
// if you add/rename a section in app.py, update this list too. The backend is
// authoritative -- we only display what it returned.
const SECTION_LABELS: { key: string; label: string; session: number }[] = [
  { key: "business_context", label: "Business Context", session: 1 },
  { key: "pain_points", label: "Pain Points", session: 1 },
  { key: "existing_reports", label: "Existing Reports", session: 1 },
  { key: "question_bank", label: "Question Bank", session: 2 },
  { key: "vocabulary_metrics", label: "Key Terms & Metrics", session: 2 },
];

interface ParseResult {
  template_version: string;
  warnings: string[];
  errors: string[];
  preview: Record<string, Record<string, string>[]>;
}

interface Props {
  open: boolean;
  engagementId: string;
  /** Map of current section data so we can show a diff (rows now vs incoming). */
  currentData: Record<string, unknown[]>;
  /** Last-known updated_at for optimistic-lock continuity. */
  ifMatch: string;
  onClose: () => void;
  /** Fires after a successful apply. Passes the new updated_at so the parent
   *  can refresh its ref and trigger a reload of engagement state. */
  onApplied: (newUpdatedAt: string, appliedSections: string[]) => void;
}

export default function PreworkUploadModal({
  open, engagementId, currentData, ifMatch, onClose, onApplied,
}: Props) {
  const fileInputRef = useRef<HTMLInputElement>(null);
  const [filename, setFilename] = useState<string>("");
  const [parsing, setParsing] = useState(false);
  const [applying, setApplying] = useState(false);
  const [parseResult, setParseResult] = useState<ParseResult | null>(null);
  const [parseError, setParseError] = useState<string>("");
  const [applyError, setApplyError] = useState<string>("");
  // Section -> apply? Default to true for sections that have incoming rows.
  const [applyFlags, setApplyFlags] = useState<Record<string, boolean>>({});

  const reset = () => {
    setFilename("");
    setParseResult(null);
    setParseError("");
    setApplyError("");
    setApplyFlags({});
    if (fileInputRef.current) fileInputRef.current.value = "";
  };

  const handleClose = () => {
    if (parsing || applying) return; // don't allow closing mid-flight
    reset();
    onClose();
  };

  const handleFile = async (file: File) => {
    setFilename(file.name);
    setParseError("");
    setParseResult(null);
    setApplyError("");
    setParsing(true);
    try {
      const result = await api.parsePrework(engagementId, file);
      setParseResult(result);
      // Default each section's apply flag: on if there are any incoming rows.
      const flags: Record<string, boolean> = {};
      for (const s of SECTION_LABELS) {
        flags[s.key] = (result.preview[s.key]?.length ?? 0) > 0;
      }
      setApplyFlags(flags);
    } catch (e: any) {
      setParseError(e?.message || "Failed to parse file.");
    } finally {
      setParsing(false);
    }
  };

  const handleApply = async () => {
    if (!parseResult) return;
    const sections = SECTION_LABELS.map((s) => s.key).filter((k) => applyFlags[k]);
    if (!sections.length) {
      setApplyError("Select at least one section to apply.");
      return;
    }
    setApplying(true);
    setApplyError("");
    try {
      const res = await api.applyPrework(
        engagementId,
        sections,
        parseResult.preview,
        ifMatch,
      );
      onApplied(res.updated_at, res.applied);
      reset();
      onClose();
    } catch (e: any) {
      if (e?.stale) {
        setApplyError(
          "This engagement was just updated by someone else. Close this dialog and refresh the page, then try again.",
        );
      } else {
        setApplyError(e?.message || "Apply failed.");
      }
    } finally {
      setApplying(false);
    }
  };

  const hasErrors = !!parseResult && parseResult.errors.length > 0;
  const incomingTotal = parseResult
    ? Object.values(parseResult.preview).reduce((acc, rows) => acc + rows.length, 0)
    : 0;

  return (
    <Dialog open={open} onClose={handleClose} maxWidth="md" fullWidth>
      <DialogTitle>Upload Business Owner Pre-Work</DialogTitle>
      <DialogContent dividers>
        <Alert severity="info" sx={{ mb: 2 }}>
          <Typography variant="body2" sx={{ mb: 1 }}>
            Send the template below to your business owner before the working
            session. When they return it, upload it here to populate Sessions 1
            and 2 with their answers.
          </Typography>
          <Link
            href={api.preworkTemplateUrl}
            download
            sx={{ display: "inline-flex", alignItems: "center", gap: 0.5, fontWeight: 500 }}
          >
            <DownloadIcon fontSize="small" /> Download blank template (.xlsx)
          </Link>
        </Alert>

        {/* File picker */}
        <Paper variant="outlined" sx={{ p: 2, mb: 2 }}>
          <Stack direction="row" alignItems="center" spacing={2}>
            <Button
              variant="contained"
              startIcon={<UploadFileIcon />}
              component="label"
              disabled={parsing || applying}
            >
              Choose .xlsx file
              <input
                ref={fileInputRef}
                type="file"
                hidden
                accept=".xlsx,application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                onChange={(e) => {
                  const f = e.target.files?.[0];
                  if (f) handleFile(f);
                }}
              />
            </Button>
            <Box sx={{ flexGrow: 1, minWidth: 0 }}>
              {filename ? (
                <Typography variant="body2" sx={{ overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                  {filename}
                </Typography>
              ) : (
                <Typography variant="body2" color="text.secondary">
                  No file selected
                </Typography>
              )}
            </Box>
          </Stack>
          {parsing && <LinearProgress sx={{ mt: 2 }} />}
        </Paper>

        {parseError && (
          <Alert severity="error" icon={<ErrorOutlineIcon />} sx={{ mb: 2 }}>
            {parseError}
          </Alert>
        )}

        {/* Parse result: errors block applying, warnings are informational */}
        {parseResult && hasErrors && (
          <Alert severity="error" icon={<ErrorOutlineIcon />} sx={{ mb: 2 }}>
            <Typography variant="body2" sx={{ fontWeight: 500, mb: 0.5 }}>
              Can't import this file:
            </Typography>
            <ul style={{ margin: 0, paddingLeft: 20 }}>
              {parseResult.errors.map((err, i) => (
                <li key={i}><Typography variant="body2">{err}</Typography></li>
              ))}
            </ul>
          </Alert>
        )}

        {parseResult && !hasErrors && parseResult.warnings.length > 0 && (
          <Alert severity="warning" icon={<WarningAmberIcon />} sx={{ mb: 2 }}>
            <Typography variant="body2" sx={{ fontWeight: 500, mb: 0.5 }}>
              {parseResult.warnings.length} warning{parseResult.warnings.length === 1 ? "" : "s"} — review before applying:
            </Typography>
            <ul style={{ margin: 0, paddingLeft: 20 }}>
              {parseResult.warnings.map((w, i) => (
                <li key={i}><Typography variant="body2">{w}</Typography></li>
              ))}
            </ul>
          </Alert>
        )}

        {/* Per-section preview with apply checkboxes */}
        {parseResult && !hasErrors && (
          <Box>
            <Typography variant="subtitle2" sx={{ mb: 1 }}>
              Preview ({incomingTotal} row{incomingTotal === 1 ? "" : "s"} total).
              Each checked section will <strong>replace</strong> the existing rows
              in that section.
            </Typography>
            <Stack spacing={1.5}>
              {SECTION_LABELS.map((s) => {
                const incoming = parseResult.preview[s.key] || [];
                const existing = (currentData[s.key] as unknown[] | undefined) || [];
                const hasIncoming = incoming.length > 0;
                return (
                  <Paper
                    key={s.key}
                    variant="outlined"
                    sx={{
                      p: 1.5,
                      opacity: hasIncoming ? 1 : 0.55,
                      bgcolor: applyFlags[s.key] ? "action.selected" : "background.paper",
                    }}
                  >
                    <Stack direction="row" alignItems="center" spacing={1}>
                      <FormControlLabel
                        control={
                          <Checkbox
                            checked={!!applyFlags[s.key]}
                            disabled={!hasIncoming}
                            onChange={(e) =>
                              setApplyFlags({ ...applyFlags, [s.key]: e.target.checked })
                            }
                          />
                        }
                        label={
                          <Box>
                            <Typography variant="body2" sx={{ fontWeight: 500 }}>
                              {s.label}
                            </Typography>
                            <Typography variant="caption" color="text.secondary">
                              Session {s.session}
                            </Typography>
                          </Box>
                        }
                        sx={{ flexGrow: 1, m: 0 }}
                      />
                      <Stack direction="row" spacing={1}>
                        <Chip
                          size="small"
                          label={`${existing.length} now`}
                          variant="outlined"
                        />
                        <Chip
                          size="small"
                          color={hasIncoming ? "primary" : "default"}
                          label={`${incoming.length} incoming`}
                        />
                      </Stack>
                    </Stack>
                    {hasIncoming && applyFlags[s.key] && existing.length > 0 && (
                      <Box sx={{ mt: 1, pl: 4 }}>
                        <Typography variant="caption" color="warning.main">
                          ⚠ {existing.length} existing row{existing.length === 1 ? "" : "s"} will be replaced.
                        </Typography>
                      </Box>
                    )}
                  </Paper>
                );
              })}
            </Stack>
            <Divider sx={{ my: 2 }} />
            <Typography variant="caption" color="text.secondary">
              Template version detected: {parseResult.template_version || "unknown"}
            </Typography>
          </Box>
        )}

        {applyError && (
          <Alert severity="error" sx={{ mt: 2 }}>{applyError}</Alert>
        )}
        {applying && <LinearProgress sx={{ mt: 2 }} />}
      </DialogContent>
      <DialogActions>
        <Button onClick={handleClose} disabled={parsing || applying}>
          Cancel
        </Button>
        <Button
          variant="contained"
          onClick={handleApply}
          disabled={
            !parseResult ||
            hasErrors ||
            applying ||
            parsing ||
            !Object.values(applyFlags).some(Boolean)
          }
        >
          {applying ? "Applying…" : "Apply Selected"}
        </Button>
      </DialogActions>
    </Dialog>
  );
}
