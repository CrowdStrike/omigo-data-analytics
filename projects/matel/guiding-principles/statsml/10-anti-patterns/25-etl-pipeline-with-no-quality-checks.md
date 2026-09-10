# ETL Pipeline with No Data Quality Checks

**Page type:** detail page (anti-pattern-pair layout: two card-sections, each a two-column table — text left 45%, canvas right 55%)
**HTML title tag:** ETL Pipeline with No Data Quality Checks

**Subtitle:** Garbage at step 2 propagates silently to step 5 — discovered weeks later by user complaint

## The Anti-Pattern

Data flows raw → cleaned → transformed → modeled → served with no validation between steps. Each stage blindly trusts its input, so corruption introduced early silently propagates downstream until an end-user notices broken reports or wrong predictions.

**Key point (red-left-border callout):** No assertions, no schema checks, no distribution monitoring between pipeline stages.

*Domain examples:*

- Airflow DAGs with no intermediate data checks
- Spark jobs chaining transformations without validation
- dbt models lacking tests on source freshness and row counts

### Visualization (canvas `c1`, 720×300)

Pipeline flow diagram: five stage boxes with arrows, showing garbage introduced at step 2 propagating unchecked to step 5.

- **Stages:** `['Raw', 'Cleaned', 'Transformed', 'Modeled', 'Served']`, boxes 100×40 (rounded corners, radius 4), starting at x=40, y=130, gap 120 between box origins. Above each box, gray (`#666`) 10px label "Step 1" … "Step 5" (at y=startY−8).
- **Box colors:** stage 0 (Raw): fill `rgba(26,82,118,0.15)`, stroke `#1a5276`, 2px; stages 1–4: fill `rgba(231,76,60,0.15)`, stroke `#e74c3c`. Stage label text bold 12px, centered in box, `#1a5276` for stage 0, `#e74c3c` otherwise.
- **Arrows between boxes:** 2px line with filled triangular arrowhead at mid-box height; first arrow (Raw→Cleaned) in `#1a5276`, subsequent arrows in `#e74c3c`.
- **Garbage indicator:** bold 11px `#e74c3c` centered under step 2 box: "💥 Garbage introduced" (at startY+boxH+22).
- **Propagation wave:** dashed line (dash 4/3, 1.5px, `rgba(231,76,60,0.5)`) from center of step 2 box to center of step 5 box at y=startY+boxH+28.
- **Top label:** bold 13px `#e74c3c` centered at (w/2, 50): "No checks between stages!".
- **Discovery label:** bold 11px `#e67e22` centered above step 5 box (y=startY−35): "Discovered at step 5, weeks later", with a short 1.5px `#e67e22` downward arrow (line from y=startY−28 to y=startY−14 plus filled triangular head) pointing at the box.
- **Silence indicators:** gray `#999` 9px "(no check)" text centered in each of the 4 gaps between boxes (at y=startY+boxH/2+30).
- **Timeline:** thin `#ccc` line across the bottom from x=40 to x=w−40 at y=h−30; `#999` 10px labels "Day 1" (left-aligned at x=50) and "Weeks later..." (right-aligned at x=w−50), at y=h−15.

## The Design Pattern

Validation gate between every stage: schema + distribution + business rules. HALT on failure. The pipeline stops at the first sign of corruption, preventing downstream contamination.

**Key point (red-left-border callout):** Every stage boundary is a checkpoint: validate schema, check distributions, enforce business rules. Fail fast.

- Schema validation: column types, nullability, expected columns
- Distribution checks: null rate, value ranges, cardinality drift
- Business rules: referential integrity, monotonicity, completeness
- HALT and alert on any violation — never pass garbage forward

### Visualization (canvas `c2`, 720×300)

Pipeline flow diagram with diamond validation gates between stages; the gate after step 2 halts, protecting downstream stages.

- **Stages:** same five names, boxes 90×38 (rounded, radius 4), starting at x=30, y=135, gap 138. "Step N" gray 10px labels above each box.
- **Box colors:** stages 0–1: fill `rgba(26,82,118,0.15)`, stroke `#1a5276`, label `#1a5276` bold 11px; stages 2–4 (never reached): fill `rgba(200,200,200,0.2)`, stroke `#bbb`, label `#999`.
- **Gates:** diamond shape between each pair of boxes (points 12px above/below arrow line, spanning 24px horizontally), 2px stroke. Gates 1, 3, 4 (indices 0, 2, 3): green — stroke `#27ae60`, fill `rgba(39,174,96,0.2)`, label "PASS" bold 8px below. Gate 2 (index 1, after Cleaned): red — stroke `#e74c3c`, fill `rgba(231,76,60,0.2)`, label "HALT" bold 8px below.
- **Arrow segments:** before each gate, 1.5px line — `#1a5276` for the first, `#e74c3c` into the halting gate, `#ccc` for unreached gates; after passing gates, `#27ae60` line with filled green arrowhead. After the halted gate: a red X mark (2.5px `#e74c3c`, two crossing 10px strokes) instead of a continuing arrow.
- **HALT callout:** bold 12px `#e74c3c` centered at (w/2, 45): "HALT! Null rate exceeded tolerance", with a dashed (dash 3/2, 1px) `#e74c3c` pointer line down to the halting gate.
- **Garbage label:** 10px `#e74c3c` centered under step 2 box: "garbage detected" (at startY+boxH+18).
- **Shield icons:** 16px "🛡️" in `#ddd` centered under each of stages 3–5 (at startY+boxH+20).
- **Bottom label:** bold 11px `#27ae60` centered at (w/2, h−25): "Pipeline stops — downstream protected".

## Regeneration instructions

- **Template/layout:** anti-pattern-pair detail page. h1 with `border-bottom: 2px solid #2980b9`, `.subtitle` paragraph, then two `.card-section` divs ("The Anti-Pattern", "The Design Pattern"). Each section: `h2` (1.3rem, `#1a5276`, 2px `#2980b9` bottom border) followed by a `table.layout` with one `<tr>`: left `td.text-col` (45%) holding paragraph + `.key-point` callout + `.example` label + `<ul>`; right `td.viz-col` (55%) holding one `<canvas>`.
- **Page CSS:** universal reset (`* { margin:0; padding:0; box-sizing:border-box }`); body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; `.subtitle` `#666` 0.95rem, margin-bottom 32px; `.card-section` margin-bottom 40px; table cells `vertical-align: top`, padding 12px; canvas `width: 100%`, `1px solid #e0e0e0` border, radius 4px; `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem; `.example` italic `#555` 0.9rem; ul 0.92rem with 20px left margin. No nav bar, no back/home links.
- **Canvas:** each canvas is drawn at intrinsic 720×300 and scaled via `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) through a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`; bar/box fills use rgba variants of these.
- **Links:** none on this page; in regenerated HTML any card links elsewhere use `.html` extensions.
