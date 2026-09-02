# Standards Lock-In — Too Much Adoption, Too Early

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one h2 + table per aspect, plus philosophy callouts and a summary table)
**HTML title tag:** Standards Lock-In — Too Much Adoption, Too Early

**Subtitle:** Fahrenheit, QWERTY, p < 0.05, your company's "active user" definition — standards win by being early, not by being right, and adoption freezes them before their flaws are visible.

## Callout (philosophy box, top)

**The paradox:** Everyone agrees 0.05 is arbitrary; it will still be the threshold in fifty years. A standard's value comes from being *shared*, not from being *right* — and the adoption that makes it succeed is exactly what removes the ability to fix it.

## 1. The Trap: Adoption Is the Goal — and the Cage

**Obj-title:** The Vanishing Fix-It Window

Easy to change while nobody knows what's wrong; unchangeable by the time everyone does (the Collingridge dilemma).

Math-box:

Shipped early, still here: `QWERTY` (built around typebar jamming), `MM/DD/YYYY`, `p < 0.05`
Designed carefully, still struggling: `metric in the US`, `ISO 8601 in daily use`, `Esperanto`

- **The race rewards first, not best:** entrenched standards look hastily designed because the race selects for shipping before understanding.
- **"Borderline similar" is immortal:** a badly broken standard gets replaced; an almost-as-good one never does.

### Visualization (canvas `canvas1`, 720×360)

Two curves over the life of a standard: ability to change it (falling) vs knowledge of its flaws (rising), with the tiny overlap window shaded.

- **Layout:** origin at (70, 300), plot width 600, plot height 240. Axes `#1a5276`, width 2.
- **Data (functions):** changeability(t) = 100·exp(−t/12); knowledge(t) = 100·(1 − exp(−(t−8)/15)) clamped at 0 for t < 8; t = 0..60 (years since launch). Value scale 0–110 mapped to plot height.
- **Overlap window:** region between t where knowledge > 25 and changeability > 25 (approx t = 13..17) filled `rgba(230,126,34,0.2)` from axis to top of plot, labeled in bold 15px `#e67e22` above the region (two lines): "the fix-it window —" / "if it exists at all".
- **Axis labels:** x: "Years since the standard shipped"; y (rotated): "Percent of maximum" — both `#1a5276`, 15px. X ticks at 0, 15, 30, 45, 60 in `#666`, gridlines `#eee`.
- **Changeability curve:** green `#27ae60`, width 2.5, bold 15px green label "how easy it is to change" near t=4 above the curve.
- **Knowledge curve:** red `#e74c3c`, width 2.5, bold 15px red label "how much we know is wrong with it" near t=34 above the curve.
- **Title (bold 17px `#1a5276`, top center):** "Easy to Change While Unknown, Known Only Once Unchangeable".

## 2. There Is Often No "Correct" — Just Camps Behind Boundaries

**Obj-title:** The Correctness Spectrum

Math-box:

Purely arbitrary — nothing to be right about: `driving side`, `plug shapes`
Taste with camps on both sides: `Celsius vs Fahrenheit`, `date formats`
Genuinely ordered — and the lower one survives: `metric's base-10`, `ISO 8601 (sortable)`, `A4's √2 ratio`

- **Boundaries sustain camps:** a standard only needs coordination within its interaction network — country, culture, and org borders partition it.
- **Convergence follows interaction, not correctness:** aviation converged globally; science is metric even inside the US.
- **Lock-in breeds believers:** familiarity gets laundered into conviction — "we can't switch" quietly becomes "we don't want to switch".

### Visualization (canvas `canvas2`, 720×360)

Horizontal spectrum: examples placed along an axis from "no correctness exists" to "clear ordering exists", with a bracket showing the paradox zone.

- **Layout:** horizontal axis line at y=200 from x=70 to x=670, `#1a5276` width 2, arrowheads at both ends.
- **End labels (bold 15px):** left under the axis: "purely arbitrary" / "(nothing to be right about)" in `#27ae60`, two lines centered near x=110; right under the axis: "genuinely ordered" / "(and the lower one survives)" in `#e74c3c`, two lines centered near x=620.
- **Example dots:** filled circles radius 6 on the axis with 14px `#333` labels alternating above (y=170) and below (y=235) the axis, at evenly spaced x positions: "driving side" (x=110, `#27ae60`), "plug shapes" (x=190, `#27ae60`), "week start day" (x=270, `#e67e22`), "Fahrenheit vs Celsius" (x=360, `#e67e22`), "date formats" (x=450, `#e67e22`), "imperial vs metric" (x=540, `#e74c3c`), "US Letter vs A4" (x=620, `#e74c3c`).
- **Paradox bracket:** square bracket drawn in `#1a5276` width 1.5 spanning x=500..660 at y=270, with bold 15px `#1a5276` centered label below (two lines): "the paradox lives here:" / "an ordering exists, and it doesn't matter".
- **Title (bold 17px `#1a5276`, top center):** "Most Standards Have Nothing to Be Correct About — Some Do, and Still Lose".

## 3. Measurement Scales Lock Hardest — the Data Defends Them

**Obj-title:** History as the Installed Base

A scale's installed base is everything ever recorded in it — the switching cost grows with every measurement taken.

Math-box:

`98.6°F` body temperature — one 19th-century study, still taught
`BMI 25 / 30` — round numbers from 1830s Belgian men, now in insurance and policy
`10,000 steps` — a 1960s pedometer's marketing name (*manpo-kei*), now health guidance

- **A company metric is a measurement scale for the business:** "active user = opened app, 7-day rolling" is a Fahrenheit — arbitrary but load-bearing.
- **The data defends it:** changing the definition means restating all history or carrying an asterisk forever, so the inferior definition carries on with no defenders at all.

### Visualization (canvas `canvas3`, 720×360)

Time series of a business metric with a definition change at month 24 — showing the discontinuity and the two bad options.

- **Layout:** origin at (70, 300), plot width 600, plot height 240. Axes `#1a5276`, width 2.
- **Data (functions):** old definition: v(t) = 100 + 2.2·t + 6·sin(t/2.2) for t = 0..24; new definition: v(t) = 0.82·(100 + 2.2·t + 6·sin(t/2.2)) for t = 24..48 (an 18% level drop at the change). Value scale 0–220. Months t = 0..48, x ticks at 0, 12, 24, 36, 48 in `#666`, gridlines `#eee`.
- **Old-definition segment:** blue `#1a5276`, width 2.5, solid. **New-definition segment:** blue `#1a5276`, width 2.5, solid, starting at the dropped level.
- **Discontinuity marker:** dashed red `#e74c3c` vertical line at t=24 (dash 5/5) full plot height, bold red 15px label near the top right of the line (two lines): "definition change —" / "the series breaks here".
- **Ghost line:** dotted gray `#999` (dash 2/4) continuation of the old definition from t=24 to t=48, 14px gray label "what the old definition would have shown" near t=36 above the ghost line.
- **Restatement note (14px `#666`, two lines, left-aligned near t=2 at value ~35):** "the only alternatives: restate all history," / "or carry this asterisk forever".
- **Axis labels:** x: "Months"; y (rotated): "Weekly active users (indexed)" — `#1a5276`, 15px.
- **Title (bold 17px `#1a5276`, top center):** "Change the Definition and You Break Every Chart That Ever Used It".

## 4. The Statistician's Own Locked-In Numbers

**Obj-title:** Conventions We All Carry

Math-box:

`p < 0.05` — Fisher, 1925: a convenient round number
`80% power` — why is β exactly 4α? No reason
`1.5×IQR` — Tukey: "1 is too small, 2 is too large"
`n ≥ 30`, `80/20 split`, `k = 10` — rules of thumb hardened into method

- **"T-test everything" is locked in too:** Welch's is the safer default and results are borderline similar, so Student's survives in textbooks and software defaults.
- **The honest caveat:** a shared threshold — even an arbitrary one — buys comparability across studies; the lock-in and the benefit are the same thing.

### Visualization (canvas `canvas4`, 720×360)

Horizontal bar chart: five locked-in statistical constants, bar length = decades in service, each bar labeled with its origin story.

- **Layout:** bars start at x=190, max extent to x=660; five horizontal bars, top bar at y=70, spacing 48px, bar height 26.
- **Scale:** bar length proportional to years in service as of the 2020s, mapped so 100 years = 470px. Light `#eee` vertical gridlines at 25, 50, 75, 100 years with 13px `#999` tick labels along a baseline at the bottom; x axis label "Years in service" in `#1a5276` 15px.
- **Bars (fill `rgba(26,82,118,0.35)`, border `#1a5276` width 1):** "p < 0.05" (100 yrs), "n ≥ 30 rule" (~90 yrs), "1.5×IQR whiskers" (~50 yrs), "80% power" (~60 yrs), "80/20 split" (~55 yrs).
- **Left labels:** bold 15px `#1a5276`, right-aligned at x=180, vertically centered per bar.
- **Origin annotations:** 13px `#666` inside or right of each bar: "Fisher's round number", "textbook rule of thumb", "'1 is too small, 2 is too large'", "β = 4α, no reason", "convention hardened into method".
- **Title (bold 17px `#1a5276`, top center):** "Convenience Choices, Now Load-Bearing for a Century".

## 5. The Only Two Exits

**Obj-title:** How Standards Actually Get Replaced

Organic, voluntary migration to a better standard essentially never happens.

Math-box:

**Flag day by authority:** Sweden's *Dagen H* (1967, `overnight at 4:50 a.m.`), UK decimalisation (1971), US stocks off `1/8ths` (2001)
**Backward compatibility:** UTF-8 — every ASCII file `already is` valid UTF-8; contrast IPv6, decades of crawling

- **No third door:** without authority or disguise, the standard is permanent — the threshold stays 0.05, the thermostat stays Fahrenheit.
- **The org version:** an exec mandate with a cutover date, or a new metric published alongside the old until the old stops being cited.

### Visualization (canvas `canvas5`, 720×360)

Two adoption S-curves: a backward-compatible successor (fast) vs an incompatible one (crawling), with a flag-day step function for contrast.

- **Layout:** origin at (70, 300), plot width 600, plot height 240. Axes `#1a5276`, width 2. Value scale 0–100 (% adoption), y gridlines `#eee` at 25, 50, 75, 100 with `#666` labels; t = 0..30 years, x ticks every 10 in `#666`.
- **Backward-compatible curve:** green `#27ae60`, width 2.5: a(t) = 100/(1 + exp(−0.55·(t−8))); bold 15px green label "backward compatible (UTF-8)" near t=9.5 at value 62.
- **Incompatible curve:** red `#e74c3c`, width 2.5: a(t) = 100/(1 + exp(−0.16·(t−26))); bold 15px red label (two lines) "incompatible successor —" / "decades of crawling" near t=15 at values 32/24.
- **Flag-day step:** orange `#e67e22`, width 2.5, dashed (7/5): 2% for t < 5, vertical jump to 97% at t=5, flat after; bold 15px orange label "flag day (Dagen H)" near t=6 at value 88.
- **Axis labels:** x: "Years after the successor appears"; y (rotated): "Adoption (%)" — `#1a5276`, 15px.
- **Title (bold 17px `#1a5276`, top center):** "Voluntary Migration Never Happens — Authority or Disguise Does".

## 6. The Complete Picture

Summary table (`.summary-table`, header row + 4 rows):

| What gets locked in | Example | What defends it | Escape that has worked |
|---|---|---|---|
| **Measurement scales** | Fahrenheit, BMI thresholds, "active user" definitions | Every record ever written in the old scale | Flag day: decimalisation-style cutover with a date |
| **Interfaces** | QWERTY, phone vs calculator keypads | Trained muscle memory, installed hardware | Almost none — layouts outlive their design briefs |
| **Protocols & formats** | Date formats, CSV, week-start conventions | Every counterpart must understand you | Backward compatibility: the successor reads the incumbent |
| **Thresholds & conventions** | p < 0.05, 80% power, 1.5×IQR | Comparability across studies and orgs | Journal/regulator mandate — rare and contested |

## Callout (philosophy box, bottom)

**One sentence:** A standard doesn't win by being right — it wins by being early, and winning removes the ability to fix it; your company's metric definitions entered that trap the day the second dashboard used them.

## Regeneration instructions

- **Layout:** detail page. h1, `.subtitle`, opening `.philosophy` callout, then per aspect: `<h2>N. Title</h2>` (h2 1.4em `#1a5276`, bottom border `2px solid #2980b9`, padding-bottom 8px) followed by a one-row `.obj-table` — left `<td>` (45%) holds `.obj-title`, an optional single one-sentence lead line, `.math-box`, bullets; right `<td>` (55%, centered) holds the canvas. Section 6 is a `.summary-table`; page closes with a `.philosophy` callout.
- **Text density:** compact style — no lead paragraphs beyond one sentence, math-boxes are bare example lists, bullets are one sentence each.
- **Page style:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; subtitle `#666` 1.05em; table cell borders `1px solid #e0e0e0`, padding 20px 24px, vertical-align middle; `.obj-title` 1.05em weight 600 `#1a5276`; `strong` `#1a5276`; ul 0.95em `#333`. No nav bar, no back/home links.
- **Callout style:** `.philosophy` — background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em.
- **Math box:** `.math-box` — background `#f8fafb`, border `1px solid #e0e0e0`, radius 6px, padding 16px 20px, 0.95em; `code` background `#eef2f7`, padding 2px 6px, radius 3px, 1em.
- **Summary table:** `.summary-table` — 0.9em, th background `#f0f4f8` `#1a5276` padding 10px 14px left-aligned, td padding 10px 14px, borders `1px solid #e0e0e0`.
- **Canvas:** intrinsic 720×360 each; a shared `setupCanvas(id, w, h)` sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray text `#666`/`#999`, accent `#2980b9`.
