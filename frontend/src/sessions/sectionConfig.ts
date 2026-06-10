/**
 * Section nav config. Single source of truth for:
 *   - the floating SectionToc sidebar (what to render in the rail)
 *   - the `id` attributes on each Accordion (target for anchor scrolling)
 *
 * Adding/renaming a sub-section in a session form? Update the matching entry
 * here AND set `id={SESSION_SECTIONS[N][i].id}` on the corresponding Accordion.
 * The id<->label coupling is intentional so it's obvious when one drifts.
 */
export interface SectionEntry {
  /** DOM id on the Accordion. Anchor links scroll to this. */
  id: string;
  /** Label rendered in the side rail. */
  label: string;
}

export const SESSION_LABELS: Record<number, string> = {
  1: "1: Business Context",
  2: "2: Questions & Vocabulary",
  3: "3: Technical Design",
  4: "4: Design, Review, & Approval",
  5: "5: Configure Space",
  6: "6: Prototype Review",
  7: "7: Production Review",
};

export const SESSION_SECTIONS: Record<number, SectionEntry[]> = {
  1: [
    { id: "section-1-business-context", label: "Business Context Discovery" },
    { id: "section-1-pain-points", label: "Pain Points" },
    { id: "section-1-existing-reports", label: "Existing Reports" },
  ],
  2: [
    { id: "section-2-question-bank", label: "Question Bank" },
    { id: "section-2-key-terms", label: "Key Terms & Metrics" },
  ],
  3: [
    { id: "section-3-data-sources", label: "Data Sources" },
    { id: "section-3-reference", label: "Reference: Sessions 1 & 2" },
    { id: "section-3-classify-terms", label: "Classify Terms" },
    { id: "section-3-global-filter", label: "Global Filter" },
    { id: "section-3-sql-expressions", label: "SQL Expressions" },
    { id: "section-3-text-instructions", label: "Text Instructions" },
    { id: "section-3-table-summary", label: "Table Summary" },
    { id: "section-3-data-gaps", label: "Data Gap Analysis" },
    { id: "section-3-scope-boundaries", label: "Scope Boundaries" },
    { id: "section-3-metric-view", label: "Metric View" },
  ],
  4: [
    { id: "section-4-data-plan", label: "Data Plan" },
    { id: "section-4-readiness-brief", label: "Readiness Brief" },
    { id: "section-4-analyst-commentary", label: "Analyst Commentary" },
    { id: "section-4-benchmarks", label: "Benchmark Questions" },
    { id: "section-4-coe-controls", label: "COE Review Controls" },
  ],
  5: [
    { id: "section-5-ai-plan", label: "AI-Generated Plan" },
    { id: "section-5-text-instructions", label: "Text Instructions (General)" },
    { id: "section-5-data-sources", label: "Data Sources" },
    { id: "section-5-examples", label: "Example SQL Queries" },
    { id: "section-5-benchmarks", label: "Benchmark Questions" },
    { id: "section-5-push", label: "Push to Genie Space" },
    { id: "section-5-acknowledgments", label: "Acknowledgments" },
  ],
  6: [
    { id: "section-6-scorecard", label: "Prototype Review Scorecard" },
    { id: "section-6-fixes", label: "Fixes Log" },
    { id: "section-6-new-benchmarks", label: "New Benchmarks Captured" },
    { id: "section-6-phrasing", label: "Phrasing & Entity Matching Notes" },
  ],
  7: [
    { id: "section-7-readiness", label: "Production Readiness Checklist" },
    { id: "section-7-access", label: "Space Access Review" },
    { id: "section-7-signoff", label: "Production Sign-off" },
  ],
};
