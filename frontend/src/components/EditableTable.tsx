import { useState } from "react";
import {
  Table, TableBody, TableCell, TableContainer, TableHead, TableRow,
  TextField, IconButton, Button, Select, MenuItem, Paper,
  Dialog, DialogTitle, DialogContent, DialogActions, Box, Tooltip,
} from "@mui/material";
import AddIcon from "@mui/icons-material/Add";
import DeleteIcon from "@mui/icons-material/Delete";
import OpenInFullIcon from "@mui/icons-material/OpenInFull";
import UCColumnPicker from "./UCColumnPicker";
import UCTablePicker from "./UCTablePicker";
import type { ColumnDef } from "../types";

interface Props {
  columns: ColumnDef[];
  rows: Record<string, string>[];
  onChange: (rows: Record<string, string>[]) => void;
  readOnly?: boolean;
}

interface ExpandedCell {
  rowIdx: number;
  colKey: string;
  colLabel: string;
  value: string;
}

export default function EditableTable({ columns, rows, onChange, readOnly }: Props) {
  const [expanded, setExpanded] = useState<ExpandedCell | null>(null);
  const [expandedDraft, setExpandedDraft] = useState("");

  const addRow = () => {
    const blank: Record<string, string> = {};
    columns.forEach((c) => (blank[c.key] = ""));
    onChange([...rows, blank]);
  };

  const removeRow = (idx: number) => {
    onChange(rows.filter((_, i) => i !== idx));
  };

  const update = (idx: number, key: string, value: string) => {
    const next = rows.map((r, i) => (i === idx ? { ...r, [key]: value } : r));
    onChange(next);
  };

  const openExpand = (rowIdx: number, col: ColumnDef) => {
    const value = rows[rowIdx]?.[col.key] || "";
    setExpanded({ rowIdx, colKey: col.key, colLabel: col.label, value });
    setExpandedDraft(value);
  };

  const saveExpand = () => {
    if (expanded) update(expanded.rowIdx, expanded.colKey, expandedDraft);
    setExpanded(null);
  };

  return (
    <TableContainer component={Paper} variant="outlined" sx={{ mb: 2 }}>
      <Table size="small">
        <TableHead>
          <TableRow sx={{ bgcolor: "grey.50" }}>
            {columns.map((c) => (
              <TableCell key={c.key} sx={{ fontWeight: 600, whiteSpace: "nowrap", width: c.width }}>
                {c.label}
              </TableCell>
            ))}
            {!readOnly && <TableCell sx={{ width: 48 }} />}
          </TableRow>
        </TableHead>
        <TableBody>
          {rows.map((row, idx) => (
            <TableRow key={idx} hover>
              {columns.map((c) => (
                <TableCell key={c.key} sx={{ verticalAlign: "top", py: 0.75 }}>
                  {readOnly || (c.readOnlyField === true) || (typeof c.readOnlyField === "string" && !!row[c.readOnlyField]) ? (
                    <span style={{ whiteSpace: "pre-wrap", fontSize: 14 }}>{row[c.key] || ""}</span>
                  ) : c.type === "uc_table" ? (
                    <UCTablePicker
                      value={row[c.key] || ""}
                      onChange={(v) => update(idx, c.key, v)}
                      readOnly={readOnly}
                    />
                  ) : c.type === "uc_column" ? (
                    <UCColumnPicker
                      value={row[c.key] || ""}
                      onChange={(v) => update(idx, c.key, v)}
                      readOnly={readOnly}
                    />
                  ) : c.type === "select" ? (
                    <Select
                      size="small"
                      fullWidth
                      value={row[c.key] || ""}
                      onChange={(e) => update(idx, c.key, e.target.value)}
                      displayEmpty
                    >
                      <MenuItem value="">--</MenuItem>
                      {c.options?.map((o) => (
                        <MenuItem key={o} value={o}>{o}</MenuItem>
                      ))}
                    </Select>
                  ) : c.type === "textarea" ? (
                    <Box sx={{ position: "relative" }}>
                      <TextField
                        size="small"
                        fullWidth
                        multiline
                        minRows={2}
                        value={row[c.key] || ""}
                        onChange={(e) => update(idx, c.key, e.target.value)}
                        variant="outlined"
                        sx={{
                          "& .MuiOutlinedInput-root": { fontSize: 14 },
                          "& .MuiInputBase-input": { pr: 4 },
                        }}
                      />
                      <Tooltip title="Expand to edit">
                        <IconButton
                          size="small"
                          onClick={() => openExpand(idx, c)}
                          sx={{
                            position: "absolute",
                            top: 4,
                            right: 4,
                            opacity: 0.6,
                            "&:hover": { opacity: 1, bgcolor: "grey.100" },
                          }}
                        >
                          <OpenInFullIcon sx={{ fontSize: 14 }} />
                        </IconButton>
                      </Tooltip>
                    </Box>
                  ) : (
                    <TextField
                      size="small"
                      fullWidth
                      value={row[c.key] || ""}
                      onChange={(e) => update(idx, c.key, e.target.value)}
                      variant="outlined"
                      sx={{ "& .MuiOutlinedInput-root": { fontSize: 14 } }}
                    />
                  )}
                </TableCell>
              ))}
              {!readOnly && (
                <TableCell sx={{ py: 0.75 }}>
                  <IconButton size="small" onClick={() => removeRow(idx)} color="error">
                    <DeleteIcon fontSize="small" />
                  </IconButton>
                </TableCell>
              )}
            </TableRow>
          ))}
          {rows.length === 0 && (
            <TableRow>
              <TableCell colSpan={columns.length + (readOnly ? 0 : 1)} align="center" sx={{ py: 3, color: "text.secondary" }}>
                No entries yet
              </TableCell>
            </TableRow>
          )}
        </TableBody>
      </Table>
      {!readOnly && (
        <Button startIcon={<AddIcon />} onClick={addRow} sx={{ m: 1 }} size="small">
          Add Row
        </Button>
      )}

      {/* Expanded edit dialog */}
      <Dialog
        open={!!expanded}
        onClose={() => setExpanded(null)}
        maxWidth="md"
        fullWidth
      >
        <DialogTitle>{expanded?.colLabel}</DialogTitle>
        <DialogContent>
          <TextField
            autoFocus
            multiline
            fullWidth
            minRows={18}
            value={expandedDraft}
            onChange={(e) => setExpandedDraft(e.target.value)}
            variant="outlined"
            sx={{
              mt: 1,
              "& .MuiInputBase-input": { fontFamily: "monospace", fontSize: 14 },
            }}
          />
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setExpanded(null)}>Cancel</Button>
          <Button variant="contained" onClick={saveExpand}>Save</Button>
        </DialogActions>
      </Dialog>
    </TableContainer>
  );
}
