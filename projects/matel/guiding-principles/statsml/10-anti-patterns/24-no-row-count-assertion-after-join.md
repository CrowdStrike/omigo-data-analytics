# No Row Count Assertion After JOIN

**Page type:** detail page (anti-pattern-pairs two-section layout: one `.card-section` per pattern, each with a two-column table — text left 45%, canvas right 55%)
**HTML title tag:** No Row Count Assertion After JOIN

**Subtitle:** Users (10K) JOIN Orders (50K) = 50K rows. Nobody checked. Revenue per user wrong by 5x.

## The Anti-Pattern

JOIN produces unexpected row count, nobody validates. Fanout or dropped rows go unnoticed.

**Key point (red-left-border callout):** No assertion = silent error. Downstream metrics corrupted without any signal.

**Domain examples:**

- Any pipeline with joins
- ETL
- Feature engineering on relational data

### Visualization (canvas `c1`, 720×300)

Flow diagram: two input tables joined into an unvalidated, fanned-out result.

- **Input boxes:** two solid `#1a5276` 140×70 boxes at x=40 — "Users" (y=60) with subtitle "10K rows", and "Orders" (y=170) with subtitle "50K rows"; titles bold 14px white, subtitles 13px white, all centered.
- **Join arrows:** two 2px `#2c3e50` lines converging from the boxes toward a point around (270,150); between them, bold 13px `#e67e22` label "JOIN" at (240,130).
- **Result arrow:** 2px `#2c3e50` horizontal arrow with chevron head from (300,150) to (380,150).
- **Result box:** solid red `#e74c3c` 160×80 box at (390,110); white centered text: bold 14px "Result", 13px "50K rows", 11px "(expected 10K)".
- **Annotations:** bold 14px `#e74c3c` centered below the box: "5x fanout! Revenue/user WRONG"; italic 12px `#999` below that: "No assertion = silent error".
- **X mark:** 3px red `#e74c3c` X drawn at ~(610–640, 120–150) with 11px red caption "No check" beneath it.

## The Design Pattern

Assert row count before/after every join. If output > left input → fanout → investigate. If output < left → dropped rows → investigate.

**Key point (green-left-border callout, `#27ae60`):** Gate every join with a count assertion. Halt on unexpected changes.

**Steps:**

- Record row count of left table before join
- Execute join
- Assert output count ≈ left count (within tolerance)
- If assertion fails → halt and investigate
- Only proceed when count is validated

### Visualization (canvas `c2`, 720×300)

Flow diagram: same join, but an assertion gate catches the fanout and halts the pipeline.

- **Input boxes:** two solid `#1a5276` 120×70 boxes at x=30 — "Users" (y=60, "10K rows") and "Orders" (y=170, "50K rows"); titles bold 13px white, subtitles 12px white.
- **Join arrows:** two 2px `#2c3e50` lines converging toward ~(220,150); bold 12px `#e67e22` label "JOIN" at (200,130).
- **Arrow to gate:** 2px `#2c3e50` horizontal arrow with chevron head from (250,150) to (320,150).
- **Assertion gate:** solid green `#27ae60` 160×90 box at (330,105); white centered text lines: bold 13px "ASSERT", 12px "count ≈ 10K?", bold 12px "Result: 50K", bold 11px "FAIL! 50K ≠ 10K".
- **Halt arrow:** 2px red `#e74c3c` horizontal arrow with chevron head from (490,150) to (560,150).
- **HALT box:** solid red `#e74c3c` 130×60 box at (565,120); white centered text: bold 13px "HALT!", 11px "Fanout detected", 11px "→ investigate".
- **Annotations:** bold 14px `#27ae60` centered at (380,250): "✓ Gate catches error before proceeding"; vertical dashed red line (`#e74c3c`, dash 5/3, width 3) dropping from the HALT box to y=270, with 11px red caption "Pipeline blocked" below.

## Regeneration instructions

- **Template/layout:** anti-pattern-pairs detail page. h1 with 2px `#2980b9` bottom border, `.subtitle`, then two `.card-section` blocks ("The Anti-Pattern", "The Design Pattern"). Each section: h2 (1.3rem `#1a5276`, 2px `#2980b9` bottom border) followed by a `table.layout` (width 100%, border-collapse) with one row: `td.text-col` (45%) holding a paragraph, a `.key-point` callout, a bold "Domain examples:"/"Steps:" lead-in (margin-top 12px, weight 600, 0.92rem) and a `<ul>`; `td.viz-col` (55%) holding the canvas.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `.key-point` background `#f8f9fa`, `border-left: 3px solid #e74c3c` (design-pattern callout overrides border-left-color to `#27ae60`), padding 8px 12px, 0.9rem; ul 0.92rem. Canvas elements `width: 100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic size 720×300 via a shared `setup(id)` helper that sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Both canvases carry `height="300"` attributes in the HTML. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange, dark ink `#2c3e50`, gray text `#999`.
- **Links:** none on this page; in regenerated HTML any card links elsewhere use `.html` extensions.
