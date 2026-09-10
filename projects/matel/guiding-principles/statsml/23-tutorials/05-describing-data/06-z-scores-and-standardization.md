# Z-Scores & Standardization

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Z-Scores & Standardization

**Subtitle:** A z-score re-measures any value as "how many standard deviations from the mean" — turning scores, minutes, and dollars into one comparable ruler

## Two Tests, One Question: Which Score Was Better?

**Tags:** `core idea` (blue), `comparing scores` (green), `standard deviations` (orange)

- **Maya's week** — she scored 82 on the math test and 74 on the history test
- **Raw compare** — 82 beats 74, so math looks like her stronger subject at first glance
- **The classes** — math averaged 70 with a spread (sd) of 8; history averaged 66 with sd 4
- **Count the spreads** — 82 sits 1.5 sd's above math's average; 74 sits 2.0 above history's
- **The z-score** — "how many sd's from the mean" — so history wins, +2.0 vs +1.5

*Example (italic):* Maya's "worse" 74 was actually the rarer score — hardly anyone in history class got that far above their average.

**Key point:** A raw score means nothing without its class. A z-score restates every value as "sd's from the mean", so any two scores become comparable.

### Visualization (canvas `c1`, 720×300)

Dual-panel bell curves: the math class distribution (left) and history class distribution (right), each with the mean marked and Maya's score flagged, split by a vertical dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "Maya's Two Scores on Their Own Class Curves".
- **Data:** math mean 70, sd 8, Maya 82 (z = +1.5); history mean 66, sd 4, Maya 74 (z = +2.0). Curves drawn deterministically as `y = exp(-0.5*((x-mean)/sd)^2)` scaled to 150px height — no randomness.
- **Left panel (math):** axis origin x=55, width 280, baseline y=245, chart height 175; score range 46–94; blue `#2a78d6` 2.5px curve; dashed `#1a5276` vertical line at 70 labeled "mean 70" (12px); blue 6px dot on the curve at 82 with a 1.5px blue drop line to the baseline; blue bold 12px annotation "82 → z = +1.5"; x ticks 46, 54, 62, 70, 78, 86, 94 (12px `#444`); caption 12px `#444` "Math: mean 70, sd 8".
- **Right panel (history):** axis origin x=400, width 280, same baseline/height; score range 54–78; green `#008300` 2.5px curve; dashed `#1a5276` vertical at 66 labeled "mean 66"; green 6px dot at 74 with drop line; green bold 13px annotation, two lines: "74 → z = +2.0" / "the stronger score"; x ticks 54, 58, 62, 66, 70, 74, 78; caption "History: mean 66, sd 4".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.

## The Recipe: Subtract the Mean, Divide by the SD

**Tags:** `worked example` (blue), `rule of thumb` (green)

- **The recipe** — z = (score − mean) ÷ sd: subtract the class average, divide by the spread
- **Maya's math** — (82 − 70) ÷ 8 = 12 ÷ 8 = +1.5; her history: (74 − 66) ÷ 4 = +2.0
- **Five classmates** — math scores 54, 62, 70, 78, 90 become z = −2.0, −1.0, 0, +1.0, +2.5
- **Zero means average** — the student at 70 lands exactly at z = 0; below-mean scores go negative
- **Unit-free** — z carries no points, dollars, or minutes; it is a pure count of sd's

*Example (italic):* The classmate who scored 54 sits at (54 − 70) ÷ 8 = −2.0 — two full standard deviations below the class.

**Key point:** Standardizing is just relabeling the ruler — 0 at the mean, one tick per standard deviation. The data points themselves never move.

### Visualization (canvas `c2`, 720×300)

Two horizontal number-line "rulers": the five classmates plus Maya on the raw-score ruler (top) and the z-score ruler (bottom), with light vertical connectors showing each dot keeps its position — only the labels change.

- **Title (bold 15px, `#1a5276`, top center):** "Five Classmates: Raw Scores → Z-Scores".
- **Data:** scores `[54, 62, 70, 78, 90]` with z `["−2.0", "−1.0", "0", "+1.0", "+2.5"]`; Maya's 82 → "+1.5" highlighted separately.
- **Rulers:** both from x=70, width 580, 2px `#999` lines; raw ruler at y=95 spanning 46→94 (linear), z ruler at y=205 spanning −3→+3; each value at horizontal fraction `(score−46)/48` on both rulers (z is a linear relabel, so positions match); thin 1px `#bdc3c7` vertical connectors between paired dots.
- **Raw ruler:** heading bold 12px `#444` "raw scores (46 → 94)"; blue `#2a78d6` 6px dots; score labels 12px `#444` above each dot.
- **Z ruler:** heading bold 12px `#444` "z-scores ((score − 70) ÷ 8)"; green `#008300` 6px dots; z labels 12px below each dot; tick at z = 0 labeled "0 = class average" in bold 12px `#1a5276`.
- **Maya:** magenta `#d55181` 7px dot on both rulers at score 82 / z +1.5, with bold magenta 12px label "Maya 82 → z = +1.5".
- **Takeaway (bold 13px green, bottom center):** "same positions, new ruler: z counts sd's from the mean".

## Where Z-Scores Do Real Work

**Tags:** `where it's used` (blue), `outlier flag` (orange), `feature scaling` (green)

- **Outlier alarm** — on bell-shaped data only ~0.3% of points land beyond |z| = 3; flag those
- **The typo** — a recorded 106 gives z = (106 − 70) ÷ 8 = +4.5 on a test capped at 100
- **68–95–99.7** — about 68% of a bell sits within 1 sd, 95% within 2, 99.7% within 3
- **Feature scaling** — models mixing ages and incomes standardize both so neither dominates
- **One shared ruler** — z lets you compare scores, heights, and load times on equal terms

*Example (italic):* A z of +4.5 on this test would be roughly a several-in-a-million score — a data-entry typo is the far likelier story.

**Key point:** Z-scores turn "is this value weird?" into one number. On roughly bell-shaped data, |z| > 3 is the standard first-pass outlier flag.

### Visualization (canvas `c3`, 720×300)

A z-score number line with nested 68/95/99.7 bands centered at zero, the six class scores as dots, and one impossible entry flagged far to the right.

- **Title (bold 15px, `#1a5276`, top center):** "The Z Ruler as an Outlier Alarm".
- **Axis:** horizontal 2px `#999` line at y=170 from x=70, width 580; z range −4 → +5 (9 units, ~64.4px per unit); integer ticks −4 to +5 labeled 12px `#444`.
- **Nested bands (rectangles from y=70 down to y=170, centered on z = 0):** |z| ≤ 1 fill `rgba(42,120,214,0.25)` labeled "68%"; |z| ≤ 2 fill `rgba(42,120,214,0.15)` labeled "95%"; |z| ≤ 3 fill `rgba(42,120,214,0.08)` labeled "99.7%"; band labels bold 12px `#1a5276` just above each band's top edge.
- **Class dots:** blue `#2a78d6` 6px dots on the axis at z = `[−2.0, −1.0, 0, +1.0, +1.5, +2.5]` (the five classmates plus Maya).
- **Outlier:** red `#e74c3c` 7px dot at z = +4.5 with bold red 13px annotation, two lines: "recorded 106 → z = +4.5" / "impossible score: typo".
- **Caption (12px `#444`, bottom center):** "the 68–95–99.7 shares hold for bell-shaped data".

## Z-Scores Don't Make Data Normal

**Tags:** `common mistake` (red), `skewed data` (orange)

- **The hope** — people z-score skewed data and expect a bell; standardization never reshapes
- **Homework minutes** — 100 students, mean 42, sd 35, hard right skew: most quick, a few marathon
- **After z-scoring** — the biggest bar is still the biggest bar; only the axis labels changed
- **Broken rule** — the 68–95–99.7 shares are a bell-curve fact, not a z-score fact
- **Skewed outliers** — on heavy-tailed data, |z| > 3 fires on perfectly ordinary values

*Example (italic):* The student at 170 minutes sits at z = (170 − 42) ÷ 35 = +3.7, yet marathon homework nights are routine here — no typo, just skew.

**Common mistake:** Believing standardization normalizes data. Z-scoring only shifts and rescales; the shape — skew, tails, gaps — survives untouched.

### Visualization (canvas `c4`, 720×300)

Dual-panel histogram: the same right-skewed homework-minutes data in raw units (left) and after z-scoring (right) — identical bar heights, different axis labels — split by a dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "Homework Minutes: Raw vs Z-Scored (illustrative)".
- **Data (both panels):** counts `[34, 26, 16, 10, 6, 4, 2, 1, 1]` in nine 20-minute bins from 0 to 180; mean 42, sd 35.
- **Left panel (raw):** axis origin x=50, width 290, baseline y=240, chart height 175, y scale max 40; bars fill `rgba(42,120,214,0.45)` with 1px `#2a78d6` stroke; bin edge labels every other edge "0", "40", "80", "120", "160" (11px `#444`); orange `#d95926` bold 12px annotation "right-skewed"; caption 12px `#444` "minutes; mean 42, sd 35".
- **Right panel (z-scored):** axis origin x=395, width 290, same baseline/height/scale; identical counts; bars fill `rgba(0,131,0,0.4)` with 1px `#008300` stroke; edge labels at the same five positions "−1.2", "−0.1", "1.1", "2.2", "3.4" (11px, = (edge − 42) ÷ 35); magenta `#d55181` bold 13px annotation, two lines: "same shape —" / "z moved the ruler, not the bars"; caption "z = (minutes − 42) ÷ 35".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- All chart data is hardcoded literal arrays; the bell curves in `c1` use the closed-form Gaussian formula (deterministic). No `Math.random()`. In regenerated HTML, any card links would use `.html` extensions (this page has no links).
