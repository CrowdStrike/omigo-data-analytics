# Dashboard Cherry-Picking

**Page type:** detail page (two-column obj-table layout: text left ~40%, canvas right ~60%, one row per section)
**HTML title tag:** Dashboard Cherry-Picking — Common Bad Practices

**Subtitle:** Metric Manipulation — 30 metrics exist. Present the 3 that are up.

## Section 1: The Practice

- You have 30 metrics in your monitoring. 27 are flat or declining. 3 are up (by chance, seasonality, or minor effort).
- Executive review slide deck shows the 3. "Strong performance across key metrics."
- The other 27 don't exist for leadership. Not technically lying — every number presented is real — but the SELECTION creates a completely false impression.

### Visualization (canvas `c1`, 720×340)

A 6×5 grid of 30 tiny 8-point sparkline tiles: 27 flat/declining gray, 3 rising green with bold frames marking the ones picked for the deck.

- **Grid:** 6 columns × 5 rows of 100×38 tiles, 10px gaps, starting at (35, 28). Each tile has a border: `#e0e0e0` width 1 for unpicked, bold `#1a5276` width 2.5 frame for the 3 picked tiles (indices 7, 18, 24).
- **Sparklines (deterministic, seeded by tile index — no randomness):** 8 points per tile, inset 8px horizontally and 6px vertically; value scale 0–100 mapped to inner tile height.
  - Picked (green, `#27ae60`, width 2, rising): `v = 30 + 6.2*j + 4*sin(idx*1.1 + j*0.9)` for j = 0..7.
  - Unpicked (gray `#aaaaaa`, width 1.5, flat or declining): `v = 58 − (idx%3)*1.6*j + 7*sin(idx*2.3 + j*1.7)`.
- **Insight annotation (centered under the grid):** bold 18px red `#e74c3c` "3 of 30 shown." (28px below grid), then 13px `#666` "The 27 flat or declining sparklines never reach the deck." (48px below grid).
- **Caption (bottom center, italic 13px `#666`, y = h−8):** "\"Strong performance across key metrics\" — every number shown is real; the selection is not."

## Section 2: The Creation Trick

- Create a NEW metric you know is trending up. "We're now tracking 'engaged session minutes' and it's growing 20% MoM!"
- Nobody tracked this before. You defined it specifically because it was growing. The metric was born to be cherry-picked.

### Visualization (canvas `c2`, 720×280)

Twelve-month line of one metric ("engaged session minutes") that was already rising before anyone defined it; the definition point merely starts the reporting.

- **Data (deterministic):** monthly values `[80, 84, 89, 93, 97, 103, 108, 113, 119, 126, 133, 140]`; metric defined at month 9 (index 8).
- **Title (bold 16px `#1a5276`, centered, y=18):** "\"Engaged Session Minutes\": the Trend Predates the Metric".
- **Axes:** chart area x = 60–660, y = 40 (top) to 225 (bottom), value range 70–150. Horizontal gridlines `#eeeeee` at 80/100/120/140 with right-aligned 11px `#666` tick labels; L-shaped `#ccc` axes; 12px `#666` month labels "M1"–"M12" centered at y=243.
- **Pre-definition history (months 1–9):** dashed gray `#999` line (dash 6/4, width 2); italic 12px gray label "untracked history (already rising)" centered below the line at M4.
- **Post-definition (months 9–12):** solid blue `#1a5276` line, width 3; bold 12px blue label "tracked & presented" right-aligned below the line near the right end.
- **Definition marker:** vertical dashed orange `#e67e22` line (dash 4/4, width 1.5) at month 9 from top to axis; filled orange circle r=6 on the series at month 9; bold 12px orange label "metric defined here" left-aligned 8px right of the marker, 42px below the point.
- **Insight annotation (bold 14px `#e74c3c`, left-aligned at x=75, two lines y=58/76):** "The trend existed before the metric did —" / "selection disguised as discovery."
- **Caption (bottom center, italic 13px `#666`, y = h−8):** "Illustrative — the metric was defined at month 9 because it was already rising."

### Visualization (canvas `c3`, 720×300)

Two overlaid indexed lines with the divergence shaded: the average of the 3 presented metrics rises to 126 while the median of all 30 metrics stays flat at ~99.

- **Data (deterministic, index month 1 = 100):** presented avg `[100, 102, 104, 107, 109, 112, 114, 117, 120, 122, 124, 126]`; portfolio median `[100, 100.5, 99.8, 100.2, 99.6, 100.1, 99.4, 99.8, 99.2, 99.6, 99.0, 99.0]`.
- **Title (bold 16px `#1a5276`, centered, y=18):** "Indexed to 100: the 3 Presented Metrics vs All 30".
- **Axes:** chart area x = 60–660, y = 42 (top) to 235 (bottom), value range 92–130. Horizontal gridlines `#eeeeee` at 100/110/120/130 with right-aligned 11px `#666` tick labels; L-shaped `#ccc` axes; 12px `#666` month labels "M1"–"M12" centered at y=253.
- **Divergence shading:** polygon between the two lines filled `rgba(39,174,96,0.12)`.
- **Median line (all 30):** blue `#1a5276`, width 2.5, essentially flat; bold 12px blue label right-aligned at chartR, 20px below its endpoint: "All 30 metrics (median): −1%".
- **Presented line (3 picked):** green `#27ae60`, width 3, rising; bold 12px green label right-aligned at chartR, 10px above its endpoint: "3 presented metrics (avg): +26%".
- **Insight annotation (bold 15px `#e74c3c`, left-aligned at (75, 66)):** "The deck's +26% vs the portfolio's −1%."
- **Caption (bottom center, italic 13px `#666`, y = h−6):** "Illustrative index (month 1 = 100). Same dashboard, same period — only the selection differs."

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table, one `<tr>` per section; left `<td>` (40%) holds `.obj-title` + bullets, right `<td>` (60%, centered) holds the canvas(es). Row 1: The Practice + canvas `c1`. Row 2: The Creation Trick + canvases `c2` and `c3` stacked.
- **Page style:** body system sans-serif, white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; subtitle `#666` 1.0em; `p` 0.95em `#333`; `ul` 0.9em `#333`, `li` margin 6px 0; table cell borders `1px solid #e0e0e0`, padding 20px 24px, vertical-align middle; `.obj-title` 1.05em, weight 600, `#1a5276`; `strong` in `#1a5276`. No nav bar, no back/home links.
- **Canvas:** declare intrinsic `width`/`height` attributes as given per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper; `canvas { display: block; margin: 0 auto; }`. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`; gray text `#666`/`#999`/`#333`.
- **Note:** in regenerated HTML, any card links use `.html` extensions (this page has none).
