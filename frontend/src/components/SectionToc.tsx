import { useEffect, useState } from "react";
import { Box, Typography, Tooltip } from "@mui/material";
import LockIcon from "@mui/icons-material/Lock";
import { SESSION_LABELS, SESSION_SECTIONS, type SectionEntry } from "../sessions/sectionConfig";

interface Props {
  /** Active session tab (0-indexed to match the existing Tabs state). */
  currentSession: number;
  onSessionChange: (sessionIdx: number) => void;
  /** Session tabs that are visible at all (BOs see a subset). 0-indexed. */
  visibleSessions: number[];
  /** Session tabs that exist but are gated (S5/S6 until COE approves). 0-indexed. */
  lockedSessions: number[];
}

/**
 * Floating left-rail navigator. Sticky on scroll. Hidden below ~900px (the
 * app's main content max-width is 1200, so dropping the sidebar on narrow
 * viewports avoids cramped layout). Top half = jump between sessions; bottom
 * half = jump between Accordion sub-sections within the current session.
 *
 * Anchor scrolling uses native scrollIntoView with a small top offset so the
 * sticky header doesn't cover the target.
 */
export default function SectionToc({
  currentSession, onSessionChange, visibleSessions, lockedSessions,
}: Props) {
  // 1-indexed session number for lookups (state is 0-indexed)
  const sessionNum = currentSession + 1;
  const subSections: SectionEntry[] = SESSION_SECTIONS[sessionNum] || [];
  const [activeId, setActiveId] = useState<string>("");

  // Scrollspy: highlight whichever sub-section is closest to the top of the
  // viewport. IntersectionObserver fires once per visibility change; we pick
  // the topmost intersecting entry. Re-binds when the session changes (so
  // observed targets match the visible accordions).
  useEffect(() => {
    if (!subSections.length) return;
    const observed: HTMLElement[] = [];
    for (const s of subSections) {
      const el = document.getElementById(s.id);
      if (el) observed.push(el);
    }
    if (!observed.length) return;

    const visible = new Set<string>();
    const observer = new IntersectionObserver(
      (entries) => {
        for (const e of entries) {
          if (e.isIntersecting) visible.add(e.target.id);
          else visible.delete(e.target.id);
        }
        // Pick the first (topmost in source order) visible section to highlight.
        for (const s of subSections) {
          if (visible.has(s.id)) {
            setActiveId(s.id);
            return;
          }
        }
      },
      {
        // Trigger when the section's top edge crosses ~25% down from the
        // viewport top. Tweakable: smaller -> highlight changes later as you
        // scroll, larger -> changes earlier.
        rootMargin: "-15% 0px -70% 0px",
        threshold: 0,
      },
    );
    for (const el of observed) observer.observe(el);
    return () => observer.disconnect();
  }, [sessionNum, subSections.length]);

  const scrollTo = (id: string) => {
    const el = document.getElementById(id);
    if (!el) return;
    const rect = el.getBoundingClientRect();
    const top = window.scrollY + rect.top - 80; // leave room above for header
    window.scrollTo({ top, behavior: "smooth" });
    setActiveId(id);
  };

  return (
    <Box
      sx={{
        // Hidden below ~900px to preserve the existing narrow layout.
        display: { xs: "none", md: "block" },
        position: "sticky",
        top: 16,
        alignSelf: "flex-start",
        flexShrink: 0,
        width: 200,
        maxHeight: "calc(100vh - 32px)",
        overflowY: "auto",
        pr: 1,
      }}
    >
      <Box sx={{ mb: 2 }}>
        <Typography variant="overline" color="text.secondary" sx={{ fontWeight: 600, letterSpacing: 0.8 }}>
          Sessions
        </Typography>
        <Box sx={{ display: "flex", flexDirection: "column", mt: 0.5 }}>
          {Object.keys(SESSION_LABELS).map((k) => {
            const sn = Number(k); // 1-indexed
            const idx = sn - 1; // 0-indexed for state
            const isVisible = visibleSessions.includes(idx);
            if (!isVisible) return null;
            const isLocked = lockedSessions.includes(idx);
            const isActive = idx === currentSession;
            return (
              <Box
                key={k}
                onClick={() => {
                  if (isLocked) return;
                  onSessionChange(idx);
                  // After Tabs change re-renders the form, scroll to its top.
                  // setTimeout 0 lets React commit the tab change first.
                  setTimeout(() => window.scrollTo({ top: 0, behavior: "smooth" }), 0);
                }}
                sx={{
                  display: "flex",
                  alignItems: "center",
                  gap: 0.75,
                  px: 1,
                  py: 0.5,
                  borderRadius: 1,
                  cursor: isLocked ? "not-allowed" : "pointer",
                  opacity: isLocked ? 0.4 : 1,
                  bgcolor: isActive ? "action.selected" : "transparent",
                  borderLeft: isActive ? "3px solid" : "3px solid transparent",
                  borderLeftColor: isActive ? "primary.main" : "transparent",
                  "&:hover": isLocked ? {} : { bgcolor: "action.hover" },
                  fontSize: 13,
                }}
              >
                {isLocked ? (
                  <Tooltip title="Requires COE approval">
                    <LockIcon sx={{ fontSize: 14 }} />
                  </Tooltip>
                ) : null}
                <Typography variant="body2" sx={{ fontWeight: isActive ? 600 : 400 }}>
                  {SESSION_LABELS[sn]}
                </Typography>
              </Box>
            );
          })}
        </Box>
      </Box>

      {subSections.length > 0 && (
        <Box>
          <Typography variant="overline" color="text.secondary" sx={{ fontWeight: 600, letterSpacing: 0.8 }}>
            In this session
          </Typography>
          <Box sx={{ display: "flex", flexDirection: "column", mt: 0.5 }}>
            {subSections.map((s) => {
              const isActive = s.id === activeId;
              return (
                <Box
                  key={s.id}
                  onClick={() => scrollTo(s.id)}
                  sx={{
                    px: 1,
                    py: 0.5,
                    borderRadius: 1,
                    cursor: "pointer",
                    bgcolor: isActive ? "action.selected" : "transparent",
                    borderLeft: "3px solid",
                    borderLeftColor: isActive ? "primary.main" : "transparent",
                    "&:hover": { bgcolor: "action.hover" },
                  }}
                >
                  <Typography
                    variant="body2"
                    sx={{
                      fontWeight: isActive ? 600 : 400,
                      fontSize: 13,
                      color: isActive ? "text.primary" : "text.secondary",
                    }}
                  >
                    {s.label}
                  </Typography>
                </Box>
              );
            })}
          </Box>
        </Box>
      )}
    </Box>
  );
}
