# Owning the Denominator

**Page type:** detail page (two-column obj-table layout: text left ~40%, canvas right ~60%; single row with multiple titled blocks and two stacked canvases)
**HTML title tag:** Owning the Denominator — Common Bad Practices

**Subtitle:** Attribution Gaming — Can't grow the numerator? Shrink the denominator. Rate "improves."

## Section 1: The Practice

- Can't increase conversions (numerator flat at 1000). Exclude "invalid" traffic from denominator.
- Was 1000/100K = 1.0%. Redefine to exclude bots, bounces, "low-intent" visits. Now: 1000/50K = 2.0%.
- "Conversion rate doubled!" Absolute conversions unchanged. Real business impact: zero. But the rate looks like a win.

## Section 2: The Model Accuracy Version

- Can't improve accuracy on hard cases? Remove hard cases from eval set ("outliers," "edge cases," "data quality issues").
- Accuracy jumps from 80% to 93%. "Model improved!" You stopped measuring where it fails.

## Section 3: Variant — Segment Exclusion

- "Conversion rate for engaged users improved 25%!" — because you defined "engaged" to exclude the segment where conversion dropped.
- The definition of "engaged" is the denominator manipulation.

## Section 4: Variant — Time Window

- "This month's rate is 3% (vs 2.5% last month)!" But this month has 20% fewer total events due to a bug.
- Numerator dropped 10%, denominator dropped 20%. Rate "improved" while everything actually got worse.

**Why it persists:** Rate metrics are the industry standard. "Conversion rate" not "total conversions." "Accuracy" not "number of correct predictions." Whoever controls what counts as the denominator (the population) controls the result. And denominator changes are invisible in the headline number.

**The tell:** Track the denominator over time. If it's shrinking while the rate "improves" — denominator manipulation. Always ask: "what changed in the ABSOLUTE numbers?" The rate is a ratio — check both parts.

### Visualization (canvas `c1`, 720×340)

Three-line time series: the same monthly revenue divided by three different denominator choices — the "qualified users" line climbs while the honest per-total-user line declines.

- **Background:** full-canvas `#f9f9f9`. **Title (bold 16px `#1a5276`, top center, y=22):** "Same Revenue, Three Denominators".
- **Plot area:** x from 55 to 668, y from 62 to 268; y-axis $ revenue per user, 0–5, gridlines `#e0e0e0` (baseline `#999`) every $1 with 11px `#666` right-aligned labels "$0".."$5"; x-axis 12 months Jan–Dec, 11px `#666` labels on every other month (Jan, Mar, May, Jul, Sep, Nov) at y=284.
- **Data (deterministic; the numerator is the identical revenue series $100K declining to $91K for all three lines — only the denominator differs):**
  - Revenue ÷ "qualified" users (green `#27ae60`, width 2.5): [2.50, 2.66, 2.78, 2.91, 3.06, 3.23, 3.43, 3.65, 3.92, 4.23, 4.38, 4.55] — denominator 40K shrinking to 20K as the "qualified" definition tightens; up 82%.
  - Revenue ÷ active users (blue `#1a5276`, width 2): [2.50, 2.49, 2.44, 2.41, 2.36, 2.31, 2.29, 2.24, 2.21, 2.16, 2.14, 2.09] — denominator 40K→43.5K; drifting down.
  - Revenue ÷ total users (red `#e74c3c`, width 2): [2.00, 1.94, 1.85, 1.77, 1.69, 1.62, 1.55, 1.48, 1.42, 1.37, 1.31, 1.26] — denominator 50K→72K; the honest line, down 37%.
- **Markers:** filled circles (radius 3) in the line color at every point.
- **Legend (11px `#333`, single centered row at y≈42, 14×3 color swatches):** "÷ 'qualified' users (the deck)" (green), "÷ active users" (blue), "÷ total users (honest)" (red).
- **End labels (bold 12px, left-aligned at x=674 beside the last points):** green "+82%" and red "−37%".
- **Insight annotation (bold 13px `#e74c3c`, centered at y=306):** "Same numerator every month — only the choice of denominator differs."
- **Caption (bottom center, italic 13px `#555`, y=h−8):** "(Pick the shrinking denominator and a declining business looks like growth.)"

### Visualization (canvas `c2`, 720×300)

Decile histogram: average monthly spend per user by decile D1–D10, with the bottom four deciles excluded as "low-intent" — the reported average jumps 53% while no user changed.

- **Background:** full-canvas `#f9f9f9`. **Title (bold 16px `#1a5276`, top center, y=20):** "Exclude the Bottom, Inflate the Average".
- **Plot area:** x from 55 to 700, y from 45 to 225; y-axis avg monthly spend $0–$120, gridlines `#e0e0e0` (baseline `#999`) every $30 with 11px `#666` right-aligned labels "$0".."$120"; x labels "D1".."D10" 11px `#666` centered under each slot at y=241.
- **Bars (deterministic):** spend per decile = [$2, $5, $9, $14, $20, $28, $38, $52, $74, $118]; 10 equal slots, bar width 44 centered in each slot; D5–D10 filled rgba(26,82,118,0.35) with `#1a5276` 1.5px border; D1–D4 filled `#dddddd` with `#e74c3c` 1.5px dashed ([4,3]) border. Bold 11px `#333` value label ("$2".."$118") 4px above each bar.
- **Exclusion zone:** rgba(231,76,60,0.07) band behind the D1–D4 slots spanning the full plot height, with italic 11px `#e74c3c` label 'excluded as "low-intent"' centered in the band at y=57.
- **Mean lines:** true average of all 10 deciles = $36 — red (`#e74c3c`) dashed ([6,4], width 1.5) line across the full plot at y for $36, bold 12px red label "true average: $36" centered over the excluded band at y=165; reported average of D5–D10 only = $55 — green (`#27ae60`) dashed ([6,4], width 1.5) line spanning only the D5–D10 slots at y for $55, bold 12px green label "reported average: $55" left-aligned at x=318, y=136.
- **Insight annotation (bold 13px `#e74c3c`, centered at y=262):** "Average '+53%' — not a single user spent a dollar more."
- **Caption (bottom center, italic 13px `#555`, y=h−8):** "(Cutting the bottom four deciles moves the average from $36 to $55.)"

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table with a single `<tr>`; left `<td>` (40%) holds four `.obj-title` blocks (The Practice, The Model Accuracy Version, Variant — Segment Exclusion, Variant — Time Window — the latter three with `style="margin-top:14px;"`) each followed by its bullet list, then the two closing `<p>` paragraphs (**Why it persists** / **The tell** with `strong` lead-ins); right `<td>` (60%, centered) holds canvases `c1` and `c2` stacked.
- **Page style:** body system sans-serif, white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; subtitle `#666` 1.0em; `p` 0.95em `#333`; `ul` 0.9em `#333`, `li` margin 6px 0; table cell borders `1px solid #e0e0e0`, padding 20px 24px, vertical-align middle; `.obj-title` 1.05em, weight 600, `#1a5276`; `strong` in `#1a5276`. No nav bar, no back/home links.
- **Canvas:** declare intrinsic `width`/`height` attributes as given per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper; `canvas { display: block; margin: 0 auto; }`. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`; gray text `#666`/`#555`/`#999`/`#333`.
- **Note:** in regenerated HTML, any card links use `.html` extensions (this page has none).
