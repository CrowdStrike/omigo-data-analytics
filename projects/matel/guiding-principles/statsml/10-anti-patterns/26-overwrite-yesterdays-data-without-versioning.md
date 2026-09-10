# Overwrite Yesterday's Data Without Versioning

**Page type:** detail page (anti-pattern-pair layout: two card-sections, each a two-column table — text left 45%, canvas right 55%)
**HTML title tag:** Overwrite Yesterday's Data Without Versioning

**Subtitle:** Bug yesterday → today's table wrong. Yesterday's table gone — can't compare, can't roll back

## The Anti-Pattern

Daily pipeline overwrites the "current" table. No history is kept. When something goes wrong, investigation and rollback are impossible.

**Key point (red-left-border callout):** Once overwritten, the previous state is permanently lost. You cannot diff, audit, or revert.

**Domain examples:**

- Daily/hourly batch pipelines
- Feature store updates
- Reporting tables

### Visualization (canvas `c1`, 720×300)

Timeline diagram: three day boxes on a timeline, first two crossed out as overwritten, third is current but buggy.

- **Title (bold 14px `#1a5276`, centered at (w/2, 25)):** "Daily Pipeline: Overwrite Mode".
- **Timeline:** 2px `#bbb` horizontal line at y=150 from x=80 to x=w−80, with filled `#bbb` triangular arrowhead at the right end.
- **Day boxes:** 100×50 rounded boxes (radius 4) sitting 20px above the timeline, centered at x=160 ("Day 1", status gone), x=340 ("Day 2", status gone), x=520 ("Day 3", status buggy).
  - Gone boxes: fill `rgba(200,200,200,0.3)`, stroke `#aaa` 2px; label bold 13px `#666`; sub-label 11px `#999` "(overwritten)"; a thick red X (3px `#e74c3c`, corner-to-corner strokes inset 10/5px) drawn over the box; bold 13px `#e74c3c` "GONE!" centered 30px below the timeline dot.
  - Buggy box: fill `rgba(231,76,60,0.15)`, stroke `#e74c3c`; label bold 13px `#e74c3c` "Day 3"; sub-label 11px `#e74c3c` "(current, buggy)".
- **Timeline dots:** 5px-radius filled circles on the timeline under each box — `#aaa` for gone days, `#e74c3c` for the buggy day.
- **Warning note:** 12px `#e67e22`, left-aligned at (x=80, y=h−55): "Each run destroys the previous state".
- **Bottom label:** bold 13px `#e74c3c` centered at (w/2, h−30): "Can't compare, can't roll back".

## The Design Pattern

Partition by date. Never delete. Compare today vs yesterday automatically. Keep 30+ days of history.

**Key point (green-left-border callout, `border-left-color: #27ae60`):** Every run lands in its own partition. The "current" view points to the latest, but all history remains accessible.

**Steps:**

- Write each run to a date-partitioned path
- Create a "latest" view/symlink pointing to newest partition
- Auto-diff today vs yesterday on each run
- Rollback = update pointer to previous partition
- Retain at least 30 days before archiving

### Visualization (canvas `c2`, 720×300)

Timeline diagram: three preserved date partitions on a green timeline, with diff arrows between them and a rollback arrow.

- **Title (bold 14px `#1a5276`, centered at (w/2, 25)):** "Daily Pipeline: Partition Mode".
- **Timeline:** 2px `#27ae60` horizontal line at y=150 from x=80 to x=w−80, filled `#27ae60` triangular arrowhead at the right end.
- **Partition boxes:** 110×50 rounded boxes (radius 4) 20px above the timeline, centered at x=160 ("dt=08-01"), x=340 ("dt=08-02"), x=520 ("dt=08-03"). All: fill `rgba(39,174,96,0.12)`, stroke `#27ae60` 2px; label bold 13px `#1a5276`; sub-label 11px `#27ae60` "preserved". Green (`#27ae60`) 5px-radius dot on the timeline under each.
- **Pairwise diff arrows:** dashed (dash 4/3) 1.5px `#1a5276` arrows below the timeline (y=170): from x=215→285 and x=395→465, each with a small open arrowhead. Centered bold 12px `#1a5276` label at (w/2, y=188): "diff any two dates".
- **Spanning diff arrow:** dashed (dash 6/3) 2px `#27ae60` double-headed arrow from x=160 to x=520 at y=202 (open arrowheads at both ends).
- **Rollback arrow:** dashed (dash 4/2) 2px `#e67e22` quadratic curve from (500, h−55) arcing up through (420, h−75) to (360, h−55), open arrowhead at the left end; 11px `#e67e22` label "rollback" at (430, h−68).
- **Bottom label:** bold 12px `#27ae60` centered at (w/2, h−30): "Rollback: point to previous partition".

## Regeneration instructions

- **Template/layout:** anti-pattern-pair detail page. h1 with `border-bottom: 2px solid #2980b9`, `.subtitle` paragraph, then two `.card-section` divs ("The Anti-Pattern", "The Design Pattern"). Each section: `h2` (1.3rem, `#1a5276`, 2px `#2980b9` bottom border) followed by a `table.layout` with one `<tr>`: left `td.text-col` (45%) holding paragraph + `.key-point` callout + bold "Domain examples:"/"Steps:" label (inline style: margin-top 12px, weight 600, 0.92rem) + `<ul>`; right `td.viz-col` (55%) holding one `<canvas>`.
- **Page CSS:** universal reset; body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; `.subtitle` `#666` 0.95rem, margin-bottom 32px; `.card-section` margin-bottom 40px; table cells `vertical-align: top`, padding 12px; canvas `width: 100%`, `1px solid #e0e0e0` border, radius 4px; `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c` (overridden to `#27ae60` for the design-pattern callout), padding 8px 12px, 0.9rem; `.example` italic `#555` 0.9rem; ul 0.92rem with 20px left margin. No nav bar, no back/home links.
- **Canvas:** each canvas drawn at intrinsic 720×300 and scaled via `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) through a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`; box fills use rgba variants.
- **Links:** none on this page; in regenerated HTML any card links elsewhere use `.html` extensions.
