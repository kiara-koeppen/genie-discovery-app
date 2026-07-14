import {
  Box, Paper, Typography, Chip, Stack, Divider, List, ListItem, ListItemText,
} from "@mui/material";
import CheckCircleIcon from "@mui/icons-material/CheckCircle";
import WarningAmberIcon from "@mui/icons-material/WarningAmber";
import InfoOutlinedIcon from "@mui/icons-material/InfoOutlined";

/**
 * Deterministic S4 readiness brief. Computed entirely from Sessions 1-3 —
 * no LLM. Two parts: rule-based coverage checks (the readiness signal) and a
 * detailed breakdown of the actual design content (tables, questions, terms,
 * gaps) so the COE reviewer sees everything in one place.
 */

type Row = Record<string, any>;
interface Props {
  s1?: Row;
  s2?: Row;
  s3?: Row;
}

type Check = { level: "pass" | "warn" | "info"; text: string };

const arr = (v: any): Row[] => (Array.isArray(v) ? v : []);
const str = (v: any): string => (typeof v === "string" ? v.trim() : "");

export default function ReadinessSummary({ s1 = {}, s2 = {}, s3 = {} }: Props) {
  const painPoints = arr(s1.pain_points);
  const reports = arr(s1.existing_reports);
  const questions = arr(s2.question_bank);
  const terms = arr(s2.vocabulary_metrics);
  const dataPlan = arr(s3.data_plan);
  const globalFilter = str(s3.global_filter);
  const textInstr = arr(s3.text_instructions);
  const gaps = arr(s3.data_gaps);
  const boundaries = arr(s3.scope_boundaries);

  const byType = (t: string) =>
    questions.filter((q) => str(q.type) === t);
  const benchmarks = byType("Benchmark");
  const testing = byType("Testing");
  const outOfScope = byType("Out of scope");
  const clarifying = byType("Clarifying");
  const untyped = questions.filter((q) => !str(q.type));

  // ---- Coverage checks (fixed rules, no AI) ----
  const checks: Check[] = [];
  checks.push(
    benchmarks.length > 0
      ? { level: "pass", text: `${benchmarks.length} Benchmark question${benchmarks.length === 1 ? "" : "s"} defined` }
      : { level: "warn", text: "No Benchmark questions defined — add the foundational questions the space must answer" },
  );
  if (untyped.length > 0)
    checks.push({ level: "warn", text: `${untyped.length} question${untyped.length === 1 ? "" : "s"} have no Type set` });

  // out-of-scope addressed = its auto Scope Boundary row has a redirect note,
  // or a matching text instruction exists.
  const unaddressedOos = outOfScope.filter((q) => {
    const qt = str(q.question_text);
    const b = boundaries.find((r) => str(r.oos_src) === qt || str(r.item) === qt);
    return !(b && str(b.notes));
  });
  if (outOfScope.length > 0) {
    checks.push(
      unaddressedOos.length === 0
        ? { level: "pass", text: `All ${outOfScope.length} out-of-scope question${outOfScope.length === 1 ? "" : "s"} have a redirect note in Scope Boundaries` }
        : { level: "warn", text: `${unaddressedOos.length} of ${outOfScope.length} out-of-scope question(s) need a redirect note in Scope Boundaries or a Text Instruction` },
    );
  }

  const missingClar = clarifying.filter((q) => !str(q.clarification));
  if (clarifying.length > 0)
    checks.push(
      missingClar.length === 0
        ? { level: "pass", text: `All ${clarifying.length} Clarifying question${clarifying.length === 1 ? "" : "s"} have a follow-up` }
        : { level: "warn", text: `${missingClar.length} Clarifying question(s) missing the follow-up Genie should ask` },
    );

  checks.push(
    dataPlan.length > 0
      ? { level: "pass", text: `${dataPlan.length} data source${dataPlan.length === 1 ? "" : "s"} selected` }
      : { level: "warn", text: "No data sources selected in Session 3" },
  );
  if (gaps.length > 0)
    checks.push({ level: "info", text: `${gaps.length} data gap${gaps.length === 1 ? "" : "s"} flagged (acknowledged limits — see below)` });

  const icon = (lvl: Check["level"]) =>
    lvl === "pass" ? <CheckCircleIcon color="success" fontSize="small" />
      : lvl === "warn" ? <WarningAmberIcon color="warning" fontSize="small" />
        : <InfoOutlinedIcon color="info" fontSize="small" />;

  const warnCount = checks.filter((c) => c.level === "warn").length;

  const Section = ({ title, count, children }: { title: string; count?: number; children: React.ReactNode }) => (
    <Box sx={{ mb: 1.5 }}>
      <Stack direction="row" spacing={1} alignItems="center" sx={{ mb: 0.5 }}>
        <Typography variant="subtitle2">{title}</Typography>
        {count !== undefined && <Chip size="small" label={count} variant="outlined" />}
      </Stack>
      {children}
    </Box>
  );

  const bullets = (items: string[]) =>
    items.length ? (
      <List dense disablePadding sx={{ pl: 1 }}>
        {items.map((t, i) => (
          <ListItem key={i} disableGutters sx={{ py: 0.1 }}>
            <ListItemText primaryTypographyProps={{ variant: "body2" }} primary={t} />
          </ListItem>
        ))}
      </List>
    ) : (
      <Typography variant="body2" color="text.secondary" sx={{ pl: 1, fontStyle: "italic" }}>none</Typography>
    );

  return (
    <Paper variant="outlined" sx={{ p: 2, mb: 3 }}>
      <Stack direction="row" spacing={1} alignItems="center" sx={{ mb: 1 }}>
        <Typography variant="h6">Readiness Summary</Typography>
        <Chip size="small" label="auto-generated from Sessions 1–3" variant="outlined" />
        {warnCount > 0
          ? <Chip size="small" color="warning" label={`${warnCount} to review`} />
          : <Chip size="small" color="success" label="no gaps flagged" />}
      </Stack>

      {/* Rollup */}
      <Typography variant="body2" color="text.secondary" sx={{ mb: 1 }}>
        {painPoints.length} pain points · {reports.length} reports · {questions.length} questions
        ({benchmarks.length} Benchmark, {testing.length} Testing, {outOfScope.length} Out of scope,
        {" "}{clarifying.length} Clarifying) · {terms.length} key terms · {dataPlan.length} data sources
        {globalFilter ? " · global filter set" : ""}
      </Typography>

      {/* Coverage checks */}
      <Box sx={{ mb: 1.5 }}>
        {checks.map((c, i) => (
          <Stack key={i} direction="row" spacing={1} alignItems="flex-start" sx={{ py: 0.25 }}>
            {icon(c.level)}
            <Typography variant="body2">{c.text}</Typography>
          </Stack>
        ))}
      </Box>

      <Divider sx={{ my: 1.5 }} />
      <Typography variant="overline" color="text.secondary">Detail</Typography>

      <Section title="Data Sources" count={dataPlan.length}>
        {bullets(dataPlan.map((d) => {
          const t = str(d.table_or_view);
          const kind = str(d.type);
          const notes = str(d.notes);
          return [t || "(unnamed)", kind && `— ${kind}`, notes && `· ${notes}`].filter(Boolean).join(" ");
        }))}
      </Section>
      {globalFilter && (
        <Section title="Global Filter">
          <Typography variant="body2" sx={{ pl: 1 }}>{globalFilter}</Typography>
        </Section>
      )}

      <Section title="Benchmark Questions" count={benchmarks.length}>
        {bullets(benchmarks.map((q) => str(q.question_text) || "(blank)"))}
      </Section>
      {testing.length > 0 && (
        <Section title="Testing Questions" count={testing.length}>
          {bullets(testing.map((q) => str(q.question_text) || "(blank)"))}
        </Section>
      )}
      {outOfScope.length > 0 && (
        <Section title="Out-of-scope Questions" count={outOfScope.length}>
          {bullets(outOfScope.map((q) => str(q.question_text) || "(blank)"))}
        </Section>
      )}
      {clarifying.length > 0 && (
        <Section title="Clarifying Questions" count={clarifying.length}>
          {bullets(clarifying.map((q) => {
            const qt = str(q.question_text) || "(blank)";
            const c = str(q.clarification);
            return c ? `${qt}  →  Genie asks: ${c}` : `${qt}  →  (no follow-up yet)`;
          }))}
        </Section>
      )}

      <Section title="Key Terms & Metrics" count={terms.length}>
        {bullets(terms.map((t) => {
          const term = str(t.business_term) || "(unnamed)";
          const def = str(t.what_they_mean);
          const syn = str(t.synonyms);
          return [term, def && `— ${def}`, syn && `(syn: ${syn})`].filter(Boolean).join(" ");
        }))}
      </Section>

      {textInstr.length > 0 && (
        <Section title="Text Instructions" count={textInstr.length}>
          {bullets(textInstr.map((t) => str(t.title) || str(t.instruction) || "(blank)"))}
        </Section>
      )}
      {gaps.length > 0 && (
        <Section title="Data Gaps" count={gaps.length}>
          {bullets(gaps.map((g) => {
            const q = str(g.business_question);
            const gap = str(g.gap_description);
            return [q, gap && `— ${gap}`].filter(Boolean).join(" ") || "(blank)";
          }))}
        </Section>
      )}
      {boundaries.length > 0 && (
        <Section title="Scope Boundaries" count={boundaries.length}>
          {bullets(boundaries.map((b) => {
            const item = str(b.item) || "(blank)";
            const scope = str(b.in_scope);
            const notes = str(b.notes);
            return [item, scope && `[${scope}]`, notes && `· ${notes}`].filter(Boolean).join(" ");
          }))}
        </Section>
      )}
    </Paper>
  );
}
