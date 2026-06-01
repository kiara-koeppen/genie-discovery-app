/* ------------------------------------------------------------------ */
/* Data models matching the Delta tables                              */
/* ------------------------------------------------------------------ */

export interface Engagement {
  engagement_id: string;
  genie_space_name: string;
  business_owner_name: string;
  business_owner_email: string;
  analyst_name: string;
  analyst_email: string;
  current_session: number;
  status: string;
  created_at: string;
  updated_at: string;
  sessions?: SessionData;
}

export interface SessionData {
  "1": Session1Data;
  "2": Session2Data;
  "3": Session3Data;
  "4": Session4Data;
  "5": Session5Data;
  "6": Session6Data;
  "7": Session7Data;
}

/* Session 1: Business Context Discovery */
export interface BusinessContext {
  question: string;
  why_it_matters: string;
  response: string;
}

export interface PainPoint {
  rank: string;
  description: string;
}

export interface ExistingReport {
  report_name: string;
  what_it_shows: string;
  frequency: string;
  known_issues: string;
}

export interface Session1Data {
  business_context: BusinessContext[];
  pain_points: PainPoint[];
  existing_reports: ExistingReport[];
}

/* Session 2: Questions & Vocabulary */
export interface QuestionBankEntry {
  question_text: string;
  decision_it_drives: string;
}

export interface VocabMetricEntry {
  business_term: string;
  what_they_mean: string;
  synonyms: string;
}

export interface Session2Data {
  question_bank: QuestionBankEntry[];
  vocabulary_metrics: VocabMetricEntry[];
}

/* Session 3: Technical Design & Data Mapping */
export interface TermClassification {
  business_term: string;
  types: string[];
  /**
   * Only populated when "Synonym" is in `types`. Tells the S5 prompt where to
   * route this term's synonyms:
   *   - kind="column": the term is another name for a specific column
   *     (alternate column name). Pushed to column_configs on column_fqn.
   *   - kind="value": the term is another name for a specific VALUE inside a
   *     column (entity matching). Pushed to column_configs entity matching
   *     on column_fqn for column_value.
   *   - kind="cross_cutting": general team jargon with no specific column.
   *     Falls back to general_instructions.
   *
   * If absent (legacy / unset), treated as cross_cutting for backward compat.
   */
  synonym_target?: SynonymTarget;
}

export interface SynonymTarget {
  kind: "column" | "value" | "cross_cutting";
  /** FQN of the column for "column" or "value" kind. Empty for "cross_cutting". */
  column_fqn?: string;
  /** For "value" kind only: which value the synonym refers to. */
  column_value?: string;
}

export interface SqlExpression {
  metric_name: string;
  uc_table: string;
  sql_code: string;
  synonyms: string;
  instructions: string;
}

export interface TextInstruction {
  title: string;
  instruction: string;
}

export interface DataGap {
  business_question: string;
  data_available: string;
  gap_description: string;
  proposed_resolution: string;
}

export interface ScopeBoundary {
  item: string;
  in_scope: string;
  notes: string;
}

export interface Session3Data {
  term_classifications: TermClassification[];
  sql_expressions: SqlExpression[];
  text_instructions: TextInstruction[];
  data_gaps: DataGap[];
  scope_boundaries: ScopeBoundary[];
  metric_view_yaml: string;
  /** Backup of the prior YAML, written before an AI redraft so it can be restored. */
  metric_view_yaml_previous?: string;
}

/* Session 4: COE Review */
export interface DataPlanEntry {
  table_or_view: string;
  type: string;
  include_in_space: string;
  notes: string;
}

export interface Session4Data {
  analyst_commentary: string;
  auto_summary: string;
  data_plan: DataPlanEntry[];
  coe_approval_status: string;
  coe_approval_notes: string;
  coe_reviewer_email: string;
}

/* Session 5: Configure Genie Space */
export interface Session5Data {
  genie_space_id: string;
  genie_space_config: string;
  /** Backup snapshot of the plan_* fields, written immediately before a plan
   *  regeneration so the analyst can restore the prior version. */
  plan_previous?: Record<string, any>;
}

/* Session 6: Prototype Review */
export interface PrototypeResult {
  question_asked: string;
  result: string;
  pass_fail: string;
  business_owner_reaction: string;
  failure_diagnosis: string;
  proposed_fix: string;
}

export interface FixEntry {
  question: string;
  failure_mode: string;
  specific_fix: string;
  priority: string;
  fixed: string;
}

export interface Benchmark {
  question: string;
  expected_answer: string;
  source_of_truth: string;
  category: string;
}

export interface PhrasingNote {
  original_phrasing: string;
  rephrased_to: string;
}

export interface Session6Data {
  prototype_results: PrototypeResult[];
  fixes_log: FixEntry[];
  benchmarks: Benchmark[];
  phrasing_notes: PhrasingNote[];
}

/* Session 7: Production Review */
export interface ProductionChecklistItem {
  label: string;
  done: boolean;
  notes: string;
}

export interface Session7Data {
  production_checklist: ProductionChecklistItem[];
  /** Free-text notes on who SHOULD have access, for reconciliation against
   *  the live Genie space ACL. */
  prod_access_notes: string;
  /** COE production sign-off (recorded only — does not gate anything). */
  prod_approval_status: string;
  prod_approval_notes: string;
  prod_reviewer_email: string;
}

/* Editable table column config */
export interface ColumnDef {
  key: string;
  label: string;
  width?: number;
  type?: "text" | "textarea" | "select" | "uc_column" | "uc_table";
  options?: string[];
  readOnlyField?: boolean | string;
}
