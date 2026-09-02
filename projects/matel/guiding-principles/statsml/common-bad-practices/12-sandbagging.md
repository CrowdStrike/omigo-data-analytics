# Sandbagging Estimates

**Page type:** detail page (two-column obj-table layout: text left ~40%, canvases right ~60%, one row per section)
**HTML title tag:** Sandbagging Estimates — Common Bad Practices

**Subtitle:** Metric Manipulation — Estimate 8 weeks for 3-week work. Deliver in 5. 'Beat expectations by 37%!'

## Section 1: The Practice

- Know it takes 3 weeks. Estimate 8. Deliver in 5.
- "Beat expectations by 37%! Ahead of schedule!"
- Consistent under-promising trains leadership to expect slow delivery. Actual velocity is permanently hidden behind inflated estimates.

### Visualization (canvas `c1`, 720×260)

Three horizontal timeline bars (weeks): actual work vs estimate vs delivery.

- **Scale:** bars start at x=160, 8 weeks = 480px (60px per week), bar height 32, rows 55px apart starting at y=40. Right-aligned bold 16px `#1a5276` row labels left of the bars; white bold 18px value text centered inside each bar.
  - Row 1 "Actual work:" — green `#27ae60` bar, 3 weeks wide, stroke `#1f8c4e`, inside text "3 weeks".
  - Row 2 "Estimate given:" — orange `#e67e22` bar, 8 weeks wide, stroke `#d35400`, inside text "8 weeks".
  - Row 3 "Delivered:" — blue `#2980b9` bar, 5 weeks wide, stroke `#1a5276`, inside text "5 weeks".
- **Annotation:** bold 18px red `#e74c3c` text to the right of the delivered bar: "\"Beat expectations by 37%!\"".
- **Bottom label (italic 14px `#666`, centered, y = h−15):** "The margin is manufactured by the estimate. A transparent 3-week estimate delivered in 3 reads as \"average\" by comparison."

## Section 2: The Compounding Effect

- Pad consistently: leadership calibrates to "8 weeks per project." Deliver in 5 — you look fast.
- Someone who estimates transparently (3 weeks) and delivers in 3 looks "average."
- Same speed, different perception. Padded estimates penalize transparent estimators by comparison — and distort the planning data everyone else relies on.

### Visualization (canvas `c2`, 720×300)

Calibration-drift line chart: leadership's planned duration ratchets upward over six successive projects while actual work stays flat.

- **Data (deterministic):** planned duration per project = [3, 4.5, 6, 7, 7.5, 8] weeks (padding compounds); actual work = 3 weeks flat.
- **Title (bold 16px `#1a5276`, centered, y=18):** "Leadership Calibration Drift Across Six Projects".
- **Axes:** plot area x = 60–685, y = 35 (top) to 250 (bottom), y-scale 0–9 weeks. Horizontal gridlines `#e8e8e8` at 0/2/4/6/8 with right-aligned 12px `#666` tick labels at x=52; rotated "weeks" axis label at x=16. X labels "Project 1"…"Project 6" (12px `#666`, centered, y=268) at 6 evenly spaced points inset 45px from plot edges; axis line `#999` at y=250.
- **Gap shading:** polygon between the planned line and the flat actual line filled `rgba(230,126,34,0.15)` (widening wedge).
- **Actual line:** flat blue `#1a5276`, width 2.5, at 3 weeks; bold 13px blue label below its right end: "Actual work: 3 weeks, every time".
- **Planned line:** orange `#e67e22`, width 2.5, with filled circle markers r=4 at each project; 11px orange value labels 9px above markers for projects 2–6 ("4.5", "6", "7", "7.5", "8"); bold 13px orange label right-aligned left of the last marker: "Planned duration".
- **Insight annotation (bold 15px `#e74c3c`, left-aligned at x=75, two lines y=52/71):** "After six padded projects, the org" / "plans 8 weeks for 3 weeks of work."
- **Bottom caption (italic 13px `#666`, centered, y = h−6):** "Illustrative — each \"ahead of schedule\" delivery ratchets the next plan upward."

### Visualization (canvas `c3`, 720×300)

True-scale side-by-side bar timelines for Ana and Raj on a single shared week axis.

- **Shared axis:** x = 150 to 630 (60px per week, 0–8 weeks), vertical gridlines `#eeeeee` from y=30 to y=200 at every week, axis line `#999` at y=200, centered 12px `#666` tick labels 0–8 at y=216, "weeks" label centered at y=234.
- **Title (bold 16px `#1a5276`, centered, y=18):** "Estimates and Deliveries on One Week Axis (True Scale)".
- **Bars:** all start at x=150, height 20, white bold 13px left-inset (8px) inside label; right-aligned 13px `#333` row labels ("Estimate" / "Delivered") at x=140.
  - **Ana group:** bold 14px green `#27ae60` group label "Ana (transparent)" at (150, 44). Estimate bar y=50, 3 weeks, fill `rgba(39,174,96,0.45)` stroke `#27ae60`, inside text "Estimate: 3 weeks". Delivered bar y=76, 3 weeks, solid `#27ae60` stroke `#1f8c4e`, inside text "Delivered: 3 weeks". Bold 15px red `#e74c3c` verdict right of the delivered bar: "Review: \"Meets expectations.\"".
  - **Raj group:** bold 14px red `#e74c3c` group label "Raj (padded)" at (150, 132). Estimate bar y=138, 8 weeks, fill `rgba(230,126,34,0.45)` stroke `#d35400`, inside text "Estimate: 8 weeks (padded)". Delivered bar y=164, 5 weeks, solid `#e67e22` stroke `#d35400`, inside text "Delivered: 5 weeks". Bold 15px green `#27ae60` verdict right of the delivered bar: "Review: \"Exceeds expectations!\"".
- **Bottom caption (italic 14px `#666`, centered, y = h−10):** "Same axis, true scale: the rating reflects the padding, not the speed."

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table, two `<tr>` rows (The Practice / The Compounding Effect); left `<td>` (40%) holds `.obj-title` + bullets, right `<td>` (60%, centered) holds the canvas(es) — row 2 stacks `c2` and `c3`.
- **Page style:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; `.subtitle` `#666` 1.0em; `p` `#333` 0.95em; `ul` 0.9em `#333`, `li` margin 6px 0; table cell borders `1px solid #e0e0e0`, padding 20px 24px, vertical-align middle; `.obj-title` 1.05em, weight 600, `#1a5276`; `strong` in `#1a5276`. No nav bar, no back/home links.
- **Canvas:** declare intrinsic `width`/`height` attributes as given per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper; `canvas { display: block; margin: 0 auto; }`. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray text `#666`/`#333`.
