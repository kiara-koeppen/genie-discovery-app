import { useState } from "react";
import {
  Box, TextField, IconButton, Tooltip,
  Dialog, DialogTitle, DialogContent, DialogActions, Button,
} from "@mui/material";
import OpenInFullIcon from "@mui/icons-material/OpenInFull";

interface Props {
  value: string;
  onChange: (value: string) => void;
  label?: string;
  placeholder?: string;
  disabled?: boolean;
  minRows?: number;
  monospace?: boolean;
  dialogTitle?: string;
}

export default function ExpandableTextField({
  value, onChange, label, placeholder, disabled, minRows = 4, monospace, dialogTitle,
}: Props) {
  const [open, setOpen] = useState(false);
  const [draft, setDraft] = useState("");

  const openDialog = () => {
    setDraft(value || "");
    setOpen(true);
  };

  const save = () => {
    onChange(draft);
    setOpen(false);
  };

  const fontSx = monospace
    ? { "& .MuiInputBase-input": { fontFamily: "monospace", fontSize: 14 } }
    : {};

  return (
    <Box sx={{ position: "relative" }}>
      <TextField
        multiline
        fullWidth
        label={label}
        placeholder={placeholder}
        value={value || ""}
        onChange={(e) => onChange(e.target.value)}
        disabled={disabled}
        minRows={minRows}
        sx={{
          ...fontSx,
          "& .MuiInputBase-input": { ...(fontSx["& .MuiInputBase-input"] || {}), pr: 4 },
        }}
      />
      {!disabled && (
        <Tooltip title="Expand to edit">
          <IconButton
            size="small"
            onClick={openDialog}
            sx={{
              position: "absolute",
              top: label ? 12 : 6,
              right: 6,
              opacity: 0.6,
              "&:hover": { opacity: 1, bgcolor: "grey.100" },
            }}
          >
            <OpenInFullIcon sx={{ fontSize: 16 }} />
          </IconButton>
        </Tooltip>
      )}
      <Dialog open={open} onClose={() => setOpen(false)} maxWidth="md" fullWidth>
        <DialogTitle>{dialogTitle || label || "Edit"}</DialogTitle>
        <DialogContent>
          <TextField
            autoFocus
            multiline
            fullWidth
            minRows={18}
            value={draft}
            onChange={(e) => setDraft(e.target.value)}
            variant="outlined"
            sx={{ mt: 1, ...fontSx }}
          />
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setOpen(false)}>Cancel</Button>
          <Button variant="contained" onClick={save}>Save</Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
}
