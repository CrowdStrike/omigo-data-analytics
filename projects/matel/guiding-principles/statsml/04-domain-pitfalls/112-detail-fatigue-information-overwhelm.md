# Detail Fatigue / Information Overwhelm

**Page type:** detail page (two-column obj-table layout: text left 50%, canvas right 50%, one h2 + one-row table per pitfall)
**HTML title tag:** 112. Detail Fatigue / Information Overwhelm

**Subtitle:** When AI makes exhaustive detail cheap, human review collapses — completeness crowds out comprehension.

## AI-Generated 50-Page Specs Nobody Reads

- AI produces exhaustive documentation that gets rubber-stamped
- Approval becomes performative — sign-off without comprehension

**Example:** Teams approve 50-page AI-generated specs in 4 minutes average review time — statistically impossible to have read them.

### Visualization (canvas `c1`, declared 720×300, drawn at 720×200)

Line chart: actual review time vs expected reading time as spec length grows, on a light blue background.

- **Background:** `#eaf2f8`.
- **Title (17px, `#1a5276`, at (190, 18)):** "Spec Pages vs Actual Review Time (minutes)".
- **Data:** pages `[5, 10, 20, 30, 40, 50, 75, 100]` → review minutes `[12, 18, 22, 20, 15, 8, 5, 4]`; x scaled to pages/100 over 600px starting x=60; y scaled to minutes/25 over 155px, baseline y=185.
- **Actual line:** red `#e74c3c`, width 2.5, with 4px-radius red dots at each point.
- **Expected line:** dashed green `#27ae60` (dash 4/4, width 1.5) straight diagonal from (60, 185) to (660, 30).
- **Labels (11px):** "Expected (if reading)" in `#27ae60` at (550, 40); "Actual review time" in `#e74c3c` at (500, 130); axis hints in `#1a5276`: "Pages →" at (620, 198), "Min ↑" at (30, 30).

## Schemas Too Detailed to Review

- Database schemas with 500+ fields auto-generated
- Reviewers can't identify redundancy, conflicts, or errors

**Example:** An AI-generated schema had 3 contradictory definitions of "user_status" across 847 fields — nobody caught it for 6 months.

### Visualization (canvas `c2`, declared 720×300, drawn at 720×200)

Declining line chart: error detection rate vs schema size, on a light yellow background.

- **Background:** `#fef9e7`.
- **Title (17px, `#1a5276`, at (195, 18)):** "Schema Fields vs Error Detection Rate (%)".
- **Data:** fields `[50, 100, 200, 350, 500, 700, 850]` → detection `[92, 85, 70, 48, 25, 12, 5]` percent; points spaced 90px starting x=80; y = percent/100 over 155px, baseline y=185.
- **Line:** purple `#8e44ad`, width 2.5, with 5px-radius purple dots.
- **Labels (11px, `#1a5276`):** field count under each point at y=198; percentage (e.g. "92%") above each dot; axis hint "# Fields →" at (600, 198).

## Alert Fatigue (100 Dashboards)

- More dashboards = less attention per dashboard
- Critical alerts buried in noise of informational ones

**Example:** SOC team has 127 dashboards. Average time viewing each: 11 seconds/day. Critical breach alert was on dashboard #94.

### Visualization (canvas `c3`, declared 720×300, drawn at 720×200)

Decaying area/line chart: seconds of attention per dashboard as dashboard count grows, with a shaded danger zone, on a light red background.

- **Background:** `#fdedec`.
- **Title (17px, `#1a5276`, at (210, 18)):** "Dashboards vs Seconds of Attention Each".
- **Data:** dashboards `[5, 10, 20, 40, 60, 80, 100, 127]` → seconds `[300, 180, 90, 40, 22, 15, 12, 11]`; x scaled to count/130 over 600px starting x=60; y scaled to seconds/320 over 155px, baseline y=185.
- **Line:** blue `#2980b9`, width 2.5, with area fill `rgba(41,128,185,0.2)` under the curve.
- **Danger zone:** translucent red `rgba(231,76,60,0.1)` rectangle covering x beyond 60 dashboards (from x=60+(60/130)·600 to plot right), y=30–185, with 11px `#e74c3c` label "< 30 sec = cannot process" at (400, 80).

## Documentation So Complete It's Unusable

- 200-page API doc vs 2-page quickstart — users need the latter
- Completeness and usability are often inversely correlated

**Example:** API with 200-page docs: 12% developer adoption. Competitor with 2-page quickstart: 67% adoption. More docs ≠ more useful.

### Visualization (canvas `c4`, declared 720×300, drawn at 720×200)

Line chart: developer adoption vs documentation length, with a circled sweet spot, on a light green background.

- **Background:** `#eafaf1`.
- **Title (17px, `#1a5276`, at (190, 18)):** "Documentation Length vs Developer Adoption".
- **Data:** doc pages `[2, 5, 10, 20, 50, 100, 200]` → adoption `[67, 72, 60, 45, 28, 18, 12]` percent; points spaced 90px starting x=80; y = adoption/80 over 155px, baseline y=185.
- **Line:** green `#27ae60`, width 2.5, with 5px-radius green dots.
- **Labels (11px, `#1a5276`):** page count + "pg" (e.g. "2pg") under each point at y=198; percentage above each dot.
- **Annotation:** 11px `#1a5276` text "Sweet spot: 5-10 pages" at (120, 50) with a dashed green circle (radius 20, dash 3/2) around the peak at (170, 62).

## PRs Too Large to Review

- 5000-line AI-generated PRs exceed human review capacity
- Reviewer skims → bugs pass → technical debt accumulates

**Example:** Bug detection rate: 85% for PRs under 200 lines, 11% for PRs over 2000 lines. AI PRs average 4,800 lines.

### Visualization (canvas `c5`, declared 720×300, drawn at 720×200)

Color-coded bar chart: bug detection rate by PR size, on a light purple background.

- **Background:** `#f4ecf7`.
- **Title (17px, `#1a5276`, at (220, 18)):** "PR Size (lines) vs Bug Detection Rate".
- **Data:** PR lines `[50, 100, 200, 500, 1000, 2000, 3000, 5000]` → detection `[92, 88, 85, 68, 45, 22, 14, 11]` percent; bars 50px wide, spaced 80px starting x=60; height = percent/100 × 150, baseline y=185.
- **Bar colors:** green `#27ae60` if detection > 60, orange `#f39c12` if > 30, else red `#e74c3c`.
- **Labels (10px, `#1a5276`):** line count under each bar (thousands rendered as "1K", "2K", "3K", "5K"); percentage above each bar.
- **Annotation (11px, `#e74c3c`, at (520, 100)):** "AI PRs avg here →".

## Compliance Checkbox Theater

- AI fills every compliance field — technically complete, practically meaningless
- Auditors can't distinguish genuine compliance from generated text

**Example:** Company passed SOC2 audit with 100% AI-generated control descriptions. None reflected actual processes. Breach occurred 3 months later.

### Visualization (canvas `c6`, declared 720×300, drawn at 720×200)

Grouped bar chart: human vs AI-generated compliance scores across four dimensions, on a light blue background.

- **Background:** `#ebf5fb`.
- **Title (17px, `#1a5276`, at (210, 18)):** "Compliance Scores: Real vs AI-Generated".
- **Categories (10px labels at y=196):** Accuracy, Completeness, Specificity, Reflects Reality; groups spaced 160px starting x=80.
- **Human values (green `#27ae60`, 30px bars):** `[72, 65, 70, 85]`; **AI values (red `#e74c3c`, 30px bars offset 35px):** `[95, 99, 92, 8]`; height = percent/100 × 140, baseline y=180.
- **Legend (11px, at x=560):** green swatch "Human", red swatch "AI-Gen".

## Meeting Transcripts: Everything Captured, Nothing Highlighted

- Full transcription without summarization = useless archive
- Important decisions buried in hours of recorded chatter

**Example:** Company records 2,000 hours of meetings/week. Employees search transcripts 0.3 times/month. Critical decisions are still lost.

### Visualization (canvas `c7`, declared 720×300, drawn at 720×200)

Dual-line chart: recorded hours rising while searches collapse, on a light yellow background.

- **Background:** `#fef9e7`.
- **Title (17px, `#1a5276`, at (155, 18)):** "Meeting Hours Recorded vs Transcript Searches/Month".
- **Months (11px, `#1a5276`, at y=198):** Jan–Aug, spaced 78px starting x=80.
- **Hours series (scale max 2400):** `[500, 800, 1200, 1500, 1800, 2000, 2100, 2200]`; blue `#2980b9` line, width 2.
- **Searches series (scale max 6):** `[5, 4.5, 3.8, 2.1, 1.2, 0.5, 0.3, 0.3]`; red `#e74c3c` line, width 2.
- **Geometry:** 150px plot height, baseline y=185.
- **Labels (11px):** "Hours recorded" in `#2980b9` at (550, 45); "Searches/employee" in `#e74c3c` at (550, 150).

## The Paradox: More Detail → Less Understanding

- Information density beyond cognitive load reduces comprehension
- Diminishing returns become negative returns past a threshold

**Example:** Study: comprehension peaks at 5-7 pages, then drops. At 50 pages, readers retain less than they would from a 3-page summary.

### Visualization (canvas `c8`, declared 720×300, drawn at 720×200)

Skewed inverted-U comprehension curve with annotations, on a light purple background.

- **Background:** `#f4ecf7`.
- **Title (17px, `#1a5276`, at (250, 18)):** "The Detail-Comprehension Curve".
- **Curve:** Gaussian-shaped y = 90·exp(−((x−20)/15)²) sampled over x 0–100, mapped to plot x=60–680 and drawn with amplitude ×1.7 above baseline y=185 (peak early, long right decay); stroke `#2980b9` width 2.5, area fill `rgba(41,128,185,0.1)`.
- **Annotations (11px):** "Peak: 5-7 pages" in `#27ae60` at (150, 38); "50+ pages: LESS understood than 3-page summary" in `#e74c3c` at (350, 140); axis hints in `#1a5276`: "Detail Level →" at (560, 198), "Comprehension ↑" at (10, 30).
- **Marker:** vertical dashed red `#e74c3c` line (dash 3/3) at x=370 from y=30 to y=185, labeled "Negative returns" in `#e74c3c` at (375, 50).

## Regeneration instructions

- **Layout:** standard detail-page structure — one `<h2>` per pitfall (1.4em `#1a5276`, bottom border `2px solid #2980b9`, padding-bottom 8px), each followed by a one-row `.obj-table`: left `<td>` (40%) holds `.obj-title` (same text as h2) + two-bullet list + bold-labeled Example paragraph; right `<td>` (60%, centered) holds the canvas. Even rows background `#fafcfe`; cell borders `1px solid #e0e0e0`, padding 20px 24px.
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; subtitle `#666` 1.05em; ul 0.9em `#333`; p 0.95em `#333`; `strong` in `#1a5276`; `.obj-title` 1.05em weight 600 `#1a5276`. `.philosophy` callout style defined (background `#f0f4f8`, left border `4px solid #2980b9`) but unused. No nav bar, no back/home links.
- **Canvas:** HTML attributes declare `width="720" height="300"`, but the shared `setupCanvas(id)` helper renders each at 720×200 CSS pixels — it sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Titles 17px system; labels 10–11px. Each chart has a distinct pastel full-canvas background tint as noted.
- **Palette:** primary blue `#1a5276`/`#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#f39c12`, purple `#8e44ad`.
- Card links elsewhere point to this page as `domains/112-detail-fatigue.html` in regenerated HTML.
