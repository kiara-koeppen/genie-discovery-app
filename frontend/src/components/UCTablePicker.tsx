import { useEffect, useState, useCallback } from "react";
import { Box, FormControl, InputLabel, Select, MenuItem, Typography } from "@mui/material";

interface Props {
  value: string;
  onChange: (value: string) => void;
  readOnly?: boolean;
  /** When provided and non-empty, the picker shows a single dropdown limited to
   *  these fully-qualified table names (e.g. the engagement's chosen Data
   *  Sources) instead of the catalog -> schema -> table cascade, so the analyst
   *  doesn't have to hunt. A "Browse all…" escape hatch falls back to the full
   *  cascade for the rare case a table outside the data sources is needed. */
  restrictTo?: string[];
}

const cache: Record<string, any[]> = {};

async function fetchCached(url: string): Promise<any[]> {
  if (cache[url]) return cache[url];
  const res = await fetch(url);
  const data = await res.json().catch(() => []);
  // /api/uc/catalogs returns {error, message} on failure now; coerce to []
  // here so the picker still renders cleanly. Errors are logged for debug.
  if (!Array.isArray(data)) {
    console.warn(`[UC picker] ${url} returned non-array response:`, data);
    return [];
  }
  cache[url] = data;
  return data;
}

export default function UCTablePicker({ value, onChange, readOnly, restrictTo }: Props) {
  const [catalogs, setCatalogs] = useState<string[]>([]);
  const [schemas, setSchemas] = useState<string[]>([]);
  const [tables, setTables] = useState<string[]>([]);
  // When restrictTo is active, let the user opt into the full cascade per-cell.
  const [browseAll, setBrowseAll] = useState(false);

  const parts = value ? value.split(".") : [];
  const selCatalog = parts[0] || "";
  const selSchema = parts[1] || "";
  const selTable = parts[2] || "";

  const restricted = !!(restrictTo && restrictTo.length > 0) && !browseAll;

  useEffect(() => {
    if (restricted) return;
    fetchCached("/api/uc/catalogs").then(setCatalogs);
  }, [restricted]);

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

  const buildValue = useCallback(
    (cat: string, sch: string, tbl: string) => {
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

  // Restricted mode: a single dropdown of the chosen Data Sources. Includes the
  // current value even if it's no longer among them, plus a "Browse all…" escape
  // hatch to the full cascade.
  if (restricted) {
    const opts = Array.from(new Set([...(restrictTo || []), ...(value ? [value] : [])]));
    return (
      <FormControl size="small" sx={{ minWidth: 260 }}>
        <InputLabel>Table</InputLabel>
        <Select
          value={value || ""}
          label="Table"
          onChange={(e) => {
            const v = e.target.value as string;
            if (v === "__browse_all__") { setBrowseAll(true); return; }
            onChange(v);
          }}
        >
          <MenuItem value="">--</MenuItem>
          {opts.map((t) => (
            <MenuItem key={t} value={t}>{t}</MenuItem>
          ))}
          <MenuItem value="__browse_all__" sx={{ fontStyle: "italic" }}>
            Browse all catalogs…
          </MenuItem>
        </Select>
      </FormControl>
    );
  }

  return (
    <Box sx={{ display: "flex", gap: 0.5, flexWrap: "wrap", minWidth: 320 }}>
      <FormControl size="small" sx={{ minWidth: 120, flex: 1 }}>
        <InputLabel>Catalog</InputLabel>
        <Select
          value={selCatalog}
          label="Catalog"
          onChange={(e) => onChange(buildValue(e.target.value, "", ""))}
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
          onChange={(e) => onChange(buildValue(selCatalog, e.target.value, ""))}
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
          onChange={(e) => onChange(buildValue(selCatalog, selSchema, e.target.value))}
          disabled={!selSchema}
        >
          <MenuItem value="">--</MenuItem>
          {tables.map((t) => (
            <MenuItem key={t} value={t}>{t}</MenuItem>
          ))}
        </Select>
      </FormControl>
    </Box>
  );
}
