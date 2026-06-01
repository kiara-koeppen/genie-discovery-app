import {
  Dialog, DialogTitle, DialogContent, DialogActions, Button, Box, Typography, Chip,
} from "@mui/material";

export interface CompareRow {
  label: string;
  /** The value the user currently has (left column). */
  current: string;
  /** The backed-up value that would be restored (right column). */
  previous: string;
}

interface Props {
  open: boolean;
  title: string;
  rows: CompareRow[];
  currentLabel?: string;
  previousLabel?: string;
  confirmLabel?: string;
  /** Commit the restore (replace current with previous). */
  onConfirm: () => void;
  /** Close without restoring (keep current). */
  onCancel: () => void;
}

/**
 * Side-by-side comparison before committing a restore. Shows the user's CURRENT
 * value next to the PREVIOUS (backed-up) value for each field, so they can see
 * exactly what they'd be reverting to before they commit — or cancel and keep
 * what they have. Used by the S3 metric-view-YAML and S5 plan restore flows.
 */
export default function CompareRestoreDialog({
  open,
  title,
  rows,
  currentLabel = "Your current version (will be replaced)",
  previousLabel = "Previous version (will be restored)",
  confirmLabel = "Restore previous version",
  onConfirm,
  onCancel,
}: Props) {
  const cell = (text: string, tint: string) => (
    <Box
      sx={{
        flex: 1,
        minWidth: 0,
        bgcolor: tint,
        border: "1px solid",
        borderColor: "divider",
        borderRadius: 1,
        p: 1.5,
        maxHeight: 360,
        overflow: "auto",
        whiteSpace: "pre-wrap",
        wordBreak: "break-word",
        fontFamily: "monospace",
        fontSize: 12.5,
        lineHeight: 1.5,
      }}
    >
      {text || <em style={{ opacity: 0.5, fontStyle: "italic" }}>(empty)</em>}
    </Box>
  );

  return (
    <Dialog open={open} onClose={onCancel} maxWidth="lg" fullWidth>
      <DialogTitle>{title}</DialogTitle>
      <DialogContent dividers>
        <Box sx={{ display: "flex", gap: 2, mb: 1 }}>
          <Box sx={{ flex: 1 }}>
            <Chip size="small" label={currentLabel} color="default" variant="outlined" />
          </Box>
          <Box sx={{ flex: 1 }}>
            <Chip size="small" label={previousLabel} color="primary" variant="outlined" />
          </Box>
        </Box>
        {rows.map((row, i) => (
          <Box key={i} sx={{ mb: 2.5 }}>
            <Typography variant="subtitle2" sx={{ mb: 0.5 }}>{row.label}</Typography>
            <Box sx={{ display: "flex", gap: 2, alignItems: "stretch" }}>
              {cell(row.current, "background.paper")}
              {cell(row.previous, "action.hover")}
            </Box>
          </Box>
        ))}
      </DialogContent>
      <DialogActions>
        <Button onClick={onCancel}>Keep current</Button>
        <Button onClick={onConfirm} variant="contained" color="primary">
          {confirmLabel}
        </Button>
      </DialogActions>
    </Dialog>
  );
}
