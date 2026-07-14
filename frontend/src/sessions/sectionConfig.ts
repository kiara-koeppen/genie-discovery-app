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
  2: "2: Key Terms & Questions",
  3: "3: Technical Design",
  4: "4: COE Review",
};

export const SESSION_SECTIONS: Record<number, SectionEntry[]> = {
  1: [
    { id: "section-1-business-context", label: "Business Context Discovery" },
    { id: "section-1-pain-points", label: "Pain Points" },
    { id: "section-1-existing-reports", label: "Existing Reports" },
  ],
  2: [
    { id: "section-2-key-terms", label: "Key Terms & Metrics" },
    { id: "section-2-question-bank", label: "Question Bank" },
  ],
  3: [
    { id: "section-3-data-sources", label: "Data Sources" },
    { id: "section-3-reference", label: "Reference: Sessions 1 & 2" },
    { id: "section-3-global-filter", label: "Global Filter" },
    { id: "section-3-text-instructions", label: "Text Instructions" },
    { id: "section-3-data-gaps", label: "Data Gap Analysis" },
    { id: "section-3-scope-boundaries", label: "Scope Boundaries" },
  ],
  4: [
    { id: "section-4-coe-controls", label: "COE Review Controls" },
  ],
};
