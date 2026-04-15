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
}

/* Session 4: Prototype Review */
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

export interface Session4Data {
  prototype_results: PrototypeResult[];
  fixes_log: FixEntry[];
  benchmarks: Benchmark[];
  phrasing_notes: PhrasingNote[];
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
