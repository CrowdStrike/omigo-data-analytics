# Relative Time in Pipeline Queries

**Page type:** detail page (anti-pattern-pair layout: two card-sections, each a two-column table — text left 45%, canvas right 55%)
**HTML title tag:** Relative Time in Pipeline Queries

**Subtitle:** NOW() - INTERVAL shifts on retry — different data, no error, 'success'

## The Anti-Pattern

`WHERE ts > NOW() - INTERVAL '1 hour'`

Pipeline fails at 10:02, retries at 10:07 — window silently shifts 5 min. No error is raised. The query "succeeds" with different data.

**Key point (red-left-border callout):** The result set changes between attempts with no indication anything went wrong.

**Domain examples:**

- Scheduled ETL
- Airflow DAGs
- Cron-triggered pipelines

### Visualization (canvas `c1`, 720×300)

Timeline diagram: two overlapping one-hour query windows (original run vs retry) with the non-overlapping 5-minute gaps highlighted in red.

- **Title (bold 13px `#2c3e50`, left-aligned at (60, 25)):** "NOW() - INTERVAL shifts silently on retry".
- **Timeline:** 2px `#2c3e50` horizontal line at y=150 from x=60 to x=w−40, filled triangular arrowhead at the right end. Tick marks with 11px `#2c3e50` centered labels: "09:02" at x=120, "09:07" at x=180, "10:02" at x=500, "10:07" at x=560.
- **Window 1 (failed run):** rectangle from x=120 to x=500, 30px tall at y=70; fill `rgba(26,82,118,0.15)`, stroke `#1a5276` 2px; bold 12px `#1a5276` centered label above: "Window @ 10:02 (failed)".
- **Window 2 (retry):** rectangle from x=180 to x=560, 30px tall at y=190; fill `rgba(230,126,34,0.15)`, stroke `#e67e22` 2px; bold 12px `#e67e22` centered label below: "Window @ 10:07 (retry)".
- **Difference zones:** `rgba(231,76,60,0.3)` overlays — the leftmost 60px of window 1 (09:02–09:07, in window 1 only) and the rightmost 60px of window 2 (10:02–10:07, in window 2 only).
- **Difference callout:** bold 13px `#e74c3c` centered at (350, 140): "5 min of different data!", with two 1.5px `#e74c3c` arrows pointing from the callout to the left gap (to ~(150, 95)) and the right gap (to ~(530, 195)).
- **Execution points:** 6px-radius filled dot on the timeline at x=500 in `#e74c3c` with 11px label "FAIL" above; dot at x=560 in `#e67e22` with label "RETRY" above.
- **Bottom warning:** bold 12px `#e74c3c` centered at (w/2, h−25): "Backfill? Impossible. Reproduce? Can't.".

## The Design Pattern

Pre-construct absolute time boundaries. Pass as parameters. Same params = same results.

**Steps:**

- Scheduler computes `window_start` and `window_end` once
- Pass both as immutable parameters to the query
- Query becomes a pure function of its inputs
- Retries reuse the same parameters
- Backfills replay any historical window trivially

**Key point (green-left-border callout, `border-left-color: #27ae60`):** Deterministic. Reproducible. Backfillable.

### Visualization (canvas `c2`, 720×300)

Flow diagram: scheduler → fixed parameters → pure-function query → retry with identical output, plus a backfill/audit box.

- **Title (bold 13px `#2c3e50`, left-aligned at (40, 25)):** "Fixed boundaries → pure function → safe retries".
- **Scheduler box:** 160×50 at (40, 40); fill `rgba(26,82,118,0.1)`, stroke `#1a5276` 2px; bold 13px `#1a5276` "Scheduler" and 11px "(computes once)", centered.
- **Parameters box:** 200×50 at (280, 40); fill `rgba(39,174,96,0.1)`, stroke `#27ae60` 2px; bold 12px `#27ae60` "window_start, window_end" and 11px `#2c3e50` "(fixed, immutable)".
- **Query box:** 200×50 at (280, 130); fill `rgba(26,82,118,0.1)`, stroke `#1a5276` 2px; bold 13px `#1a5276` "Query" and 11px `#2c3e50` "(pure function of inputs)".
- **Retry box:** 150×80 at (530, 100); fill `rgba(39,174,96,0.08)`, dashed (dash 5/3) 1.5px `#27ae60` border; contents centered: bold 12px `#27ae60` "Retry", 11px `#2c3e50` "Same params" and "→ Same results", bold 11px `#27ae60` "✓ Identical output".
- **Arrows:** 2px `#1a5276` arrow (filled head) scheduler→parameters; 2px `#27ae60` arrow parameters→query; 1.5px `#27ae60` arrow query→retry.
- **Backfill box:** 560×55 at (80, 210); fill `rgba(39,174,96,0.06)`, stroke `#27ae60` 1.5px; two left-aligned 12px `#2c3e50` lines: 'Backfill: replay(window_start="09:00", window_end="10:00") → exact same query, any time' and "Audit: every run is traceable to its explicit time parameters".
- **Bottom confirmation:** bold 13px `#27ae60` centered at (w/2, h−15): "Deterministic. Reproducible. Backfillable.".

## Regeneration instructions

- **Template/layout:** anti-pattern-pair detail page. h1 with `border-bottom: 2px solid #2980b9`, `.subtitle` paragraph, then two `.card-section` divs ("The Anti-Pattern", "The Design Pattern"). Each section: `h2` (1.3rem, `#1a5276`, 2px `#2980b9` bottom border) followed by a `table.layout` with one `<tr>`: left `td.text-col` (45%) holding paragraphs (the SQL snippet in `<code>`) + `.key-point` callout + bold "Domain examples:"/"Steps:" label + `<ul>`; right `td.viz-col` (55%) holding one `<canvas>` (these two carry `height="300"` attributes). In the design-pattern section the Steps list comes before the green key-point callout.
- **Page CSS:** universal reset; body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; `.subtitle` `#666` 0.95rem, margin-bottom 32px; `.card-section` margin-bottom 40px; table cells `vertical-align: top`, padding 12px; canvas `width: 100%`, `1px solid #e0e0e0` border, radius 4px; `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c` (overridden to `#27ae60` for the design-pattern callout), padding 8px 12px, 0.9rem; `.example` italic `#555` 0.9rem; ul 0.92rem with 20px left margin. No nav bar, no back/home links.
- **Canvas:** each canvas drawn at intrinsic 720×300 and scaled via `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) through a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`; box/window fills use rgba variants.
- **Links:** none on this page; in regenerated HTML any card links elsewhere use `.html` extensions.
