# Redundant Features

**Page type:** detail page (tutorial layout: one `.card-section` per concept, each with h2 + two-column `table.layout` — text left 50%, canvas right 50%)
**HTML title tag:** Redundant Features

**Subtitle:** When two columns say the same thing, the model gains nothing — and everything you read off the model gets messy

## One Measurement Wearing Two Costumes

**Tags:** core idea (blue), running example (green)

- **The setup** — a health model's table has height_cm and height_inches as separate columns
- **Same fact twice** — every inches value is just the cm value divided by 2.54
- **Check one row** — 152 cm ÷ 2.54 = 59.8 in; every row obeys the same rule
- **Correlation 1.00** — plot one against the other and every point sits on one straight line
- **Zero new information** — the second column tells the model nothing it didn't already know

*Example:* Asking someone their height in cm and then again in inches is one question, not two.

**Key point:** A redundant feature is a column you could compute from another — the model gains nothing from carrying it.

### Visualization (canvas `c1`, 720×300)

Scatter plot: height_cm vs height_inches with every point on one exact line, plus a right-side annotation panel.

- **Title (bold 16px `#1a5276`, top center):** "height_cm vs height_inches: Every Point on One Line".
- **Data points (cm, inches):** (152, 59.8), (160, 63.0), (168, 66.1), (175, 68.9), (183, 72.0); each a 7px blue `#2a78d6` dot with 12px coordinate label like "(152, 59.8)" to its upper right.
- **Axes:** L-shaped gray `#999` axis; padding top 55, bottom 55, left 80, right 200; x range 148–188 cm with ticks 150/160/170/180; y range 57–75 inches with ticks 58/62/66/70/74; axis captions "height_cm" (bottom center) and rotated "height_inches" (left).
- **Exact line:** dashed orange `#d95926` line (dash 6/4, 2px) for inches = cm / 2.54, drawn from cm=150 to cm=186.
- **Annotation panel (right side, x = width−185):** bold 14px orange "inches = cm / 2.54"; bold 14px red `#e74c3c` "correlation r = 1.00"; then 12px text lines "no scatter, no surprise —" / "the second column carries" / "zero new information".

## The Credit Splits, the Readout Lies

**Tags:** worked example (green), trap (red)

- **Before** — with one height column, height gets importance 0.30, second overall
- **After** — with both columns, height_cm gets 0.17 and height_inches gets 0.13
- **Accuracy unchanged** — 0.79 both times, which is exactly why nobody notices
- **Add them back** — 0.17 + 0.13 = 0.30; the signal is intact, only the credit split
- **The damage** — height falls from 2nd place to 3rd and 5th and reads as a weak signal

*Example:* A reader of the importance chart concludes height barely matters — it is the No. 2 signal.

**Key point:** Redundant features rarely hurt accuracy — they hurt everything you conclude FROM the model.

### Visualization (canvas `c2`, 720×300)

Two-panel before/after bar comparison of importance credit splitting, dashed `#bdc3c7` vertical divider at x=345.

- **Title (bold 16px `#1a5276`, top center):** "Importance Before and After Adding height_inches".
- **Left panel (x=40, width 285):** title bold 13px ink "BEFORE: one height column", subtitle 12px muted "height ranks 2nd"; bars: weight 0.35 (aqua `#199e70`), height 0.30 (blue `#2a78d6`), age 0.20 (violet `#4a3aa7`), smoker 0.15 (yellow `#c98500`).
- **Right panel (x=370, width 315):** title "AFTER: both height columns", subtitle "the 0.30 splits into 0.17 + 0.13"; bars: weight 0.36 (aqua), height cm 0.17 (blue), age 0.20 (violet), smoker 0.14 (yellow), height inches 0.13 (magenta `#d55181`).
- **Geometry:** baseline y=218, chart height 130, value scale max 0.40; bar values bold 12px above bars; 11px multi-line labels below baseline.
- **Bracket:** red `#e74c3c` 2px bracket linking the height cm and height inches bars in the right panel, labeled bold 12px red centered: "one signal, two half-credits".
- **Caption (italic 11px muted, bottom center):** "accuracy 0.79 in both models — illustrative importances".

## Spotting the Twins With a Correlation Heatmap

**Tags:** where it's used (blue), how to check (green)

- **The tool** — compute the correlation for every pair of columns, color the grid
- **Perfect twins** — height_cm vs height_inches lights up at 1.00
- **Near-twins** — weight vs bmi shows 0.86: not identical, but mostly overlapping
- **Rule of thumb** — investigate any pair above 0.9; usually keep one, drop the other
- **Which to keep** — the one that's easier to explain, cheaper to get, or less often missing

*Example:* One glance at the heatmap found in seconds what reading 60 column names never would.

**Key point:** The heatmap is the cheapest redundancy detector — run it before training, not after.

### Visualization (canvas `c3`, 720×300)

Correlation heatmap: 6×6 grid of pairwise |correlation| values with highlighted twin cells and right-side annotations.

- **Title (bold 16px `#1a5276`, top center):** "Pairwise |correlation| — the Twins Jump Out".
- **Features (rows and columns):** height_cm, height_in, weight, bmi, age, smoker.
- **Matrix (rows in that order):**
  - height_cm: [1.00, 1.00, 0.44, 0.05, 0.10, 0.03]
  - height_in: [1.00, 1.00, 0.44, 0.05, 0.10, 0.03]
  - weight: [0.44, 0.44, 1.00, 0.86, 0.18, 0.08]
  - bmi: [0.05, 0.05, 0.86, 1.00, 0.15, 0.09]
  - age: [0.10, 0.10, 0.18, 0.15, 1.00, 0.12]
  - smoker: [0.03, 0.03, 0.08, 0.09, 0.12, 1.00]
- **Geometry:** 32px cells starting at (190, 46); diagonal cells flat `#eef1f4` with gray `#b0b7bf` numbers; off-diagonal cells white-to-blue ramp `rgba(42,120,214, 0.06 + v*0.72)`; cell values 11px (white text when v ≥ 0.55, bold when v ≥ 0.86); row labels 12px right-aligned, column labels rotated 45° below.
- **Highlights:** red `#e74c3c` 3px outline around the height_cm/height_in cell (1.00); orange `#d95926` 3px outline around the weight/bmi cell (0.86).
- **Right annotations:** bold 13px red "1.00 — exact twins:", 12px text "height_cm & height_in" / "drop one, keep one"; bold 13px orange "0.86 — near-twins:", 12px text "weight & bmi overlap" / "a lot; test before dropping"; italic 11px muted "illustrative correlations".

## Keep One Copy, Not Zero

**Tags:** common confusion (orange), noise (red)

- **Don't drop both** — deleting height_cm AND height_inches throws away a real signal
- **Correlated ≠ useless** — weight and bmi overlap at 0.86, yet each holds some unique info
- **1.00 is safe to cut** — an exact copy can always go; below that, test before dropping
- **Linear models suffer more** — twin coefficients swing wildly across slightly different samples
- **Trees hide it** — accuracy holds, so the mess shows up only in the importance chart

*Example:* The pair (+9.0, −8.7) nets out to the honest 0.3 — but each number alone is nonsense.

**Key point:** Fix redundancy by keeping one good copy per fact — the goal is one column per fact, not fewer facts.

### Visualization (canvas `c4`, 720×300)

Two-panel bar chart: coefficient stability across 3 refits on resampled data with one height column vs both twin columns, dashed `#bdc3c7` vertical divider at x=300.

- **Title (bold 16px `#1a5276`, top center):** "Linear Model Coefficients Across 3 Refits on Resampled Data".
- **Left panel (x=45, width 220, header bold 13px green `#008300`):** "ONE height column" — green bars for runs 1–3 with values +0.30, +0.31, +0.32 (bold 12px labels above); zero line at y=150; footer bold 12px green: "steady: ~0.31 every time".
- **Right panel (x=330, width 360, header bold 13px red `#e74c3c`):** "BOTH height columns (twins)" — three run groups of paired bars above/below a zero line at y=150 (scale 9px per unit): run 1: +9.0 / −8.7, run 2: −4.2 / +4.5, run 3: +2.1 / −1.8; height_cm bars blue `#2a78d6`, height_inches bars magenta `#d55181`; bold 11px value labels at bar ends; under each group, "run N" 12px and bold 11px muted "sum 0.3" / "sum 0.3" / "sum 0.3".
- **Legend (top right of right panel):** blue swatch "height_cm", magenta swatch "height_inches" (12px).
- **Callout (bold 12px red, centered above right panel):** "each sum stays ~ +0.3 — the individual numbers mean nothing".
- **Caption (italic 11px muted, bottom center):** "illustrative coefficients; left panel scale exaggerated for visibility".

## Regeneration instructions

- **Layout:** tutorial detail page. h1 (2rem, `#1a5276`, 2px `#2980b9` bottom border) + `.subtitle` (`#666`, 0.95rem), then four `.card-section` blocks (40px bottom margin). Each section: `<h2>` (1.3rem, `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row — left `td.text-col` (50%) holds tags, bullets, `.example`, `.key-point`; right `td.viz-col` (50%) holds the canvas.
- **Text column structure:** `.tags` row of pill spans (0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`); `<ul>` bullets (0.92rem) each opening with `<b>` term in `#1a5276`; italic `.example` paragraph (`#555`, 0.9rem); `.key-point` callout (background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem) with `<strong>` lead.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; universal `box-sizing: border-box` reset; canvases have `width:100%`, 1px `#e0e0e0` border, 4px radius.
- **Canvas JS:** shared palette object `P = { blue:#2a78d6, green:#008300, magenta:#d55181, yellow:#c98500, aqua:#199e70, orange:#d95926, violet:#4a3aa7, ink:#1a5276, text:#2c3e50, mute:#6b7280, grid:#e5e9ef }`; shared `setup(id)` helper scales each 720×300 canvas by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Site palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
