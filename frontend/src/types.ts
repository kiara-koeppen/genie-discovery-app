/* ------------------------------------------------------------------ */
/* Data models matching the Delta tables (deterministic v2, 4 sessions) */
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

/* Session 2: Key Terms & Metrics + Questions */
export type QuestionType = "Benchmark" | "Testing" | "Out of scope" | "Clarifying";

export interface QuestionBankEntry {
  question_text: string;
  /** Benchmark | Testing | Out of scope | Clarifying (may be "" until flagged). */
  type: string;
  decision_it_drives: string;
  /** Only meaningful for Clarifying-type questions: the follow-up Genie should
   *  ask back when the request is ambiguous. Blank/greyed for other types. */
  clarification?: string;
}

export interface VocabMetricEntry {
  business_term: string;
  what_they_mean: string;
  synonyms: string;
}

export interface Session2Data {
  vocabulary_metrics: VocabMetricEntry[];
  question_bank: QuestionBankEntry[];
}

/* Session 3: Technical Design (slimmed) */
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
  /** Set on rows auto-added from an S2 "Out of scope" question — holds the
   *  source question text so re-opening S3 doesn't duplicate the row. */
  oos_src?: string;
}

/** A source table/view the analyst identified for this engagement. */
export interface DataPlanEntry {
  table_or_view: string;
  type: string;
  include_in_space: string;
  notes: string;
}

export interface Session3Data {
  data_plan: DataPlanEntry[];
  plan_warehouse_id: string;
  /** Free-text global filter comment for the Data Architect. */
  global_filter: string;
  text_instructions: TextInstruction[];
  data_gaps: DataGap[];
  scope_boundaries: ScopeBoundary[];
}

/* Session 4: COE Review (approval gate only) */
export interface Session4Data {
  coe_approval_status: string;
  coe_approval_notes: string;
  coe_reviewer_email: string;
}

/* Editable table column config */
export interface ColumnDef {
  key: string;
  label: string;
  width?: number;
  type?: "text" | "textarea" | "select" | "uc_column" | "uc_table";
  options?: string[];
  readOnlyField?: boolean | string;
  /** Cell is editable only when the row's `field` equals `equals`; otherwise it
   *  renders greyed/disabled. Used e.g. so a Clarification cell is active only
   *  on Clarifying-type question rows. */
  enabledWhen?: { field: string; equals: string };
}
