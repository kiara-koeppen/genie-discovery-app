import { useEffect, useState, useCallback } from "react";
import { Box, FormControl, InputLabel, Select, MenuItem, Typography } from "@mui/material";

interface Props {
  value: string;
  onChange: (value: string) => void;
  readOnly?: boolean;
}

const cache: Record<string, any[]> = {};

async function fetchCached(url: string): Promise<any[]> {
  if (cache[url]) return cache[url];
  const res = await fetch(url);
  const data = await res.json();
  cache[url] = data;
  return data;
}

export default function UCColumnPicker({ value, onChange, readOnly }: Props) {
  const [catalogs, setCatalogs] = useState<string[]>([]);
  const [schemas, setSchemas] = useState<string[]>([]);
  const [tables, setTables] = useState<string[]>([]);
  const [columns, setColumns] = useState<{ name: string; type: string }[]>([]);

  // Parse current value: "catalog.schema.table.column"
  const parts = value ? value.split(".") : [];
  const selCatalog = parts[0] || "";
  const selSchema = parts[1] || "";
  const selTable = parts[2] || "";
  const selColumn = parts.slice(3).join(".") || "";

  useEffect(() => {
    fetchCached("/api/uc/catalogs").then(setCatalogs);
  }, []);

  useEffect(() => {
    if (selCatalog) {
      fetchCached(`/api/uc/schemas?catalog=${encodeURIComponent(selCatalog)}`).then(setSchemas);
    } else {
      setSchemas([]);
    }
  }, [selCatalog]);

  useEffect(() => {
    if (selCatalog && selSchema) {
      fetchCached(`/api/uc/tables?catalog=${encodeURIComponent(selCatalog)}&schema=${encodeURIComponent(selSchema)}`).then(setTables);
    } else {
      setTables([]);
    }
  }, [selCatalog, selSchema]);

  useEffect(() => {
    if (selCatalog && selSchema && selTable) {
      fetchCached(`/api/uc/columns?catalog=${encodeURIComponent(selCatalog)}&schema=${encodeURIComponent(selSchema)}&table=${encodeURIComponent(selTable)}`).then(setColumns);
    } else {
      setColumns([]);
    }
  }, [selCatalog, selSchema, selTable]);

  const buildValue = useCallback(
    (cat: string, sch: string, tbl: string, col: string) => {
      if (col) return `${cat}.${sch}.${tbl}.${col}`;
      if (tbl) return `${cat}.${sch}.${tbl}`;
      if (sch) return `${cat}.${sch}`;
      if (cat) return cat;
      return "";
    },
    [],
  );

  if (readOnly) {
    return <Typography variant="body2" sx={{ fontSize: 14 }}>{value || ""}</Typography>;
  }

  return (
    <Box sx={{ display: "flex", gap: 0.5, flexWrap: "wrap", minWidth: 400 }}>
      <FormControl size="small" sx={{ minWidth: 120, flex: 1 }}>
        <InputLabel>Catalog</InputLabel>
        <Select
          value={selCatalog}
          label="Catalog"
          onChange={(e) => onChange(buildValue(e.target.value, "", "", ""))}
        >
          <MenuItem value="">--</MenuItem>
          {catalogs.map((c) => (
            <MenuItem key={c} value={c}>{c}</MenuItem>
          ))}
        </Select>
      </FormControl>

      <FormControl size="small" sx={{ minWidth: 120, flex: 1 }}>
        <InputLabel>Schema</InputLabel>
        <Select
          value={selSchema}
          label="Schema"
          onChange={(e) => onChange(buildValue(selCatalog, e.target.value, "", ""))}
          disabled={!selCatalog}
        >
          <MenuItem value="">--</MenuItem>
          {schemas.map((s) => (
            <MenuItem key={s} value={s}>{s}</MenuItem>
          ))}
        </Select>
      </FormControl>

      <FormControl size="small" sx={{ minWidth: 120, flex: 1 }}>
        <InputLabel>Table</InputLabel>
        <Select
          value={selTable}
          label="Table"
          onChange={(e) => onChange(buildValue(selCatalog, selSchema, e.target.value, ""))}
          disabled={!selSchema}
        >
          <MenuItem value="">--</MenuItem>
          {tables.map((t) => (
            <MenuItem key={t} value={t}>{t}</MenuItem>
          ))}
        </Select>
      </FormControl>

      <FormControl size="small" sx={{ minWidth: 140, flex: 1 }}>
        <InputLabel>Column</InputLabel>
        <Select
          value={selColumn}
          label="Column"
          onChange={(e) => onChange(buildValue(selCatalog, selSchema, selTable, e.target.value))}
          disabled={!selTable}
        >
          <MenuItem value="">--</MenuItem>
          {columns.map((c) => (
            <MenuItem key={c.name} value={c.name}>
              {c.name} <Typography component="span" variant="caption" color="text.secondary" sx={{ ml: 0.5 }}>({c.type})</Typography>
            </MenuItem>
          ))}
        </Select>
      </FormControl>
    </Box>
  );
}
