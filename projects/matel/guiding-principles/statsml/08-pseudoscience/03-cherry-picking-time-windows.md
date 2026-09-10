# Cherry-Picking Time Windows / Subgroups

**Page type:** detail page (two-column obj-table layout: text left 40%, canvases right 60%, single section)
**HTML title tag:** Cherry-Picking Time Windows — Pseudoscience in Data Analysis

**Subtitle:** Conclusion determined first, then data window chosen to confirm it

## Section 1: "Let Me Show You the Data" (From the ONE Window That Confirms My Thesis)

- **Investment:** "This strategy returned 40% annually!" — measured from the March 2020 bottom to the December 2021 top, the best possible start/end dates. The full picture: -5% over 5 years, including the drawdowns they conveniently omit.
- **Health supplement:** "Study shows 30% improvement!" — in one of 12 measured outcomes, in a subgroup of male, 25-30, active, non-smoking participants. The other 11 outcomes and all other demographics showed nothing; only the one positive hit was reported.
- **Product metric:** "Feature X improved engagement 25%!" — in the first 2 weeks, among opted-in users (already engaged), on the mobile-only metric. The full picture: +3% blended, decaying to 0% by week 6.
- **Climate denial:** "No warming since 1998!" — 1998 was an extreme El Niño peak, so starting at the highest point makes the trend look flat. Start from 1997 or 1999 and the warming is clear; the start date was chosen to produce the conclusion.

**Why it's pseudoscience:** The conclusion was determined first and the window/subgroup chosen to confirm it — real science defines the hypothesis and analysis plan before seeing the data, and a window chosen after seeing results is storytelling, not analysis.

### Visualization (canvas `c1`, 720×340)

Line chart of a price series with two shaded windows telling opposite stories. Margins: left 60, right 40, top 50, bottom 30.

- **Title (bold 17px, `#1a5276`, top center):** "Same Data, Two Stories (Depending on Where You Start)".
- **Data:** 20-point series plotted left to right, values (on a 0–100 y-scale): `[50, 55, 48, 52, 80, 75, 70, 65, 72, 85, 90, 60, 55, 65, 70, 75, 80, 85, 78, 72]`. Line in `#2980b9`, width 2.
- **Red window (peak-to-trough):** full-height shaded band `rgba(231,76,60,0.15)` from point 4 to point 11; label in 17px `#e74c3c` centered in the band near the top: ""Down 35%!"".
- **Green window (trough-to-peak):** full-height shaded band `rgba(39,174,96,0.15)` from point 11 to point 16; label in 17px `#27ae60`: ""Up 55%!"".
- **Bottom annotation (bold 17px `#555`, centered):** "Full picture: roughly flat. Start/end date chosen → any story you want."

### Visualization (canvas `c2`, 720×300)

Wavy line with three overlapping analyst windows, each with its own bold segment, dashed trend line, and contradictory label.

- **Title (bold 17px Arial, `#1a5276`, top center):** "The Analyst's Choice: Same Data, Different Windows".
- **Data:** 100 points generated as `sin(i*0.08)*30 + sin(i*0.03)*20 + cos(i*0.12)*15`, plotted around a baseline at 55% of the height, x from 30 to w-30. Full line drawn thin in `#bdc3c7`, width 1.5.
- **Windows:** each highlighted with a translucent band (window color + `22` alpha suffix, 120px tall centered on the baseline), the segment redrawn bold (width 2.5) in the window color, plus a dashed (4/3) straight trend line between the segment endpoints and a bold 16px quoted label:
  - Points 10–45, green `#27ae60`, label ""+40%"" above the baseline (offset -35).
  - Points 35–70, red `#e74c3c`, label ""-20%"" below the baseline (offset +35).
  - Points 55–90, orange `#e67e22`, label ""Flat"" above the baseline (offset -35).
- **Bottom annotation (bold 18px Arial, `#7f8c8d`, centered):** "Same data. Three reports. Three "conclusions." The window determines the story."

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: one full-width table with a single `<tr>`; left `<td>` (40%) holds `.obj-title` + `<ul>` bullets + trailing `<p>` paragraph, right `<td>` (60%, centered) holds both canvases (`c1` then `c2`) stacked.
- **Page style:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; `.subtitle` `#666` 1.0em; `p` 0.95em `#333`; `ul` 0.9em `#333`; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em, weight 600, `#1a5276`; `strong` in `#1a5276`. No nav bar, no back/home links.
- **Canvas:** declare intrinsic `width`/`height` attributes as given per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Each chart drawn in its own IIFE. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, grays `#555`/`#7f8c8d`/`#bdc3c7`.
