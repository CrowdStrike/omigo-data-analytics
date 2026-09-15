# The Central Limit Theorem

**Page type:** detail page (tutorial page: h2 card-sections, each a two-column layout table — text left 50%, canvas right 50%)
**HTML title tag:** The Central Limit Theorem

**Subtitle:** Average enough random things and the averages form a bell curve — no matter what the original data looks like

## One Die Is Flat, Ten Dice Averaged Are a Bell

**Tags:** `core idea` (blue), `running example` (green)

- **One die** — each face 1..6 is equally likely: the shape is a flat line, no bell anywhere
- **The move** — roll 10 dice, write down their average; repeat many times
- **The surprise** — those averages pile up around 3.5 in a bell shape
- **Why** — an average near 3.5 can happen many ways; near 1 or 6 only one way
- **The theorem** — averages of enough independent draws from almost any real-world data drift toward a bell

*Example:* No single roll is ever 3.5 — yet 3.5 is exactly where the averages crowd together.

**Key point:** the bell is a property of averaging, not of dice — flat in, bell out.

This section's viz cell holds both canvases side by side in a `.viz-pair` flex row.

### Visualization (canvas `c1a`, 310×340)

Bar chart: one die's flat distribution.

- **Title (bold 15px, `#1a5276`, top center):** "One Die: Flat"
- **Data:** six equal bars, one per face 1–6, each 16.7% (labeled "1/6" above each bar); y max 25.
- **Axes:** padding top 56, bottom 56, left 46, right 12; horizontal baseline in `#999`; bars 32px wide, evenly spaced; face numbers 1–6 below bars; x-axis title "face rolled" in muted gray `#6b7280`.
- **Bars:** `rgba(42,120,214,0.7)`.
- **Annotation (bold 13px orange `#d95926`, centered above the plot):** "no bell in sight"

### Visualization (canvas `c1b`, 310×340)

Histogram: average of 10 dice — bell shape (illustrative theoretical shape).

- **Title (bold 15px, `#1a5276`, top center):** "Average of 10 Dice: Bell"
- **Data:** 7 evenly spaced bins centered at 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0 with heights `[0.5, 4, 15, 23, 15, 4, 0.5]`; y max 27; minimum drawn bar height 2px.
- **Axes:** padding top 56, bottom 62, left 46, right 12; horizontal baseline in `#999`; bars 24px wide; x tick labels "2.0", "3.0", "3.5", "4.0", "5.0" under bins 0, 2, 3, 4, 6; caption "average of the 10 faces" with "(illustrative)" on a second line below, in muted gray.
- **Bars:** green — the central 3.5 bin solid `#008300`, all others `rgba(0,131,0,0.45)`.
- **Annotation (bold 13px green `#008300`, centered above the plot):** "averages crowd around 3.5"

## Two Dice by Hand: 36 Combos Make a Triangle

**Tags:** `worked example` (green), `by hand` (blue)

- **List them** — two dice give 6 × 6 = 36 equally likely pairs
- **Sum 2** — only 1+1: one way out of 36
- **Sum 7** — 1+6, 2+5, 3+4, 4+3, 5+2, 6+1: six ways out of 36
- **Count all** — ways for sums 2..12: 1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1
- **Already bending** — just two dice turn the flat line into a triangle; more dice round it

*Example:* Casinos price 7 as the most common roll for exactly this counting reason: 6/36 vs 1/36.

**Key point:** middle results have more recipes than extreme results — that counting imbalance is the whole engine behind the bell.

### Visualization (canvas `c2`, 720×300)

Bar chart: exact triangle of two-dice sums.

- **Title (bold 15px, `#1a5276`, top center):** "Sum of Two Dice: Ways Out of 36 (exact, count them yourself)"
- **Data:** sums `[2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]` with ways `[1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1]`; y max 7.
- **Axes:** padding top 56, bottom 56, left 65, right 30; horizontal baseline in `#999`; bars 42px wide, evenly spaced; x-axis title "sum of the two dice" in muted gray.
- **Bars:** `rgba(42,120,214,0.6)` except the sum-7 bar in orange `#d95926`; bold 12px count labels "1/36" … "6/36" above each bar; sum values below.
- **Annotation (bold 13px orange, centered over the 7 bar at the top):** "7 has six recipes; 2 has one — the middle always wins"

## Why Your Daily Averages Look Normal

**Tags:** `where it's used` (orange), `core idea` (blue)

- **Coffee shop** — single orders are skewed: lots of $3–6 cups, a rare $25 group order
- **Daily average** — average of ~50 orders per day: the days form a tidy bell near $7.00
- **Same trick** — a day is just 50 dice rolled at once; averaging built the bell
- **Payoff** — t-tests, confidence intervals, A/B tests all lean on this bell of averages
- **Without it** — you'd need to know every metric's true shape before testing anything

*Example:* Order values are nothing like normal, yet Monday-to-Sunday average spend charts look bell-shaped.

**Key point:** the CLT is why skewed raw data still yields well-behaved averages — it is the license behind most everyday statistics.

### Visualization (canvas `c3`, 720×300)

Split panel: skewed histogram of single order values (left) vs bell of daily averages (right), divided by a vertical dashed `#bdc3c7` line (dash 4/3) at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "Coffee Shop: Single Orders vs Daily Averages (illustrative)"
- **Left histogram:** 13 bins for order values $2 to $26 (step $2) with heights `[18, 30, 24, 15, 9, 6, 4, 3, 2, 1.5, 1, 0.8, 0.6]`; y max 33; plot from x=55 to x=330, baseline y=240, top y=60; bars in `rgba(213,81,129,0.65)` (magenta). X tick labels "$2", "$14", "$26" under bins 0, 6, 12. Caption (bold 13px magenta `#d55181`, centered at (192,278)): "single orders: skewed, long tail".
- **Right histogram:** 9 bins for daily average spend $5.80 to $8.20 (centered on the order mean ~$7) with heights `[1, 3, 8, 16, 22, 16, 8, 3, 1]`; y max 26; plot from x=400 to x=690, baseline y=240; bars in `rgba(25,158,112,0.5)` with the central bin solid aqua `#199e70`. X tick labels "$5.80", "$7.00", "$8.20" under bins 0, 4, 8. Caption (bold 13px aqua, centered at (545,278)): "daily averages of ~50 orders: a bell".

## The CLT Does Not Fix Your Raw Data

**Tags:** `common mistake` (red), `rule of thumb` (blue)

- **The mix-up** — "more data makes it normal" — no: raw data keeps its shape forever
- **What changes** — only the distribution of the *average* becomes bell-shaped and narrower
- **More n** — the bell of averages tightens like 1 ÷ √n; the raw histogram just gets smoother
- **Rule of thumb** — n ≈ 30 often suffices, but heavy skew or outliers demand more
- **Fine print** — draws must be independent-ish; one whale order dominating a day breaks the magic

*Example:* A million coffee orders still show the same skewed shape — only their averages are normal.

**Common mistake:** testing the raw data for normality and panicking — the CLT only ever promised normality for the averages.

### Visualization (canvas `c4`, 720×300)

Curve overlay: the raw skewed distribution stays put while the sampling distribution of the mean tightens as n grows (n = 2, 10, 40).

- **Title (bold 15px, `#1a5276`, top center):** "Only the Bell of Averages Tightens as n Grows (illustrative)"
- **Axes:** padding top 52, bottom 52, left 60, right 185; horizontal baseline in `#999`; value axis normalized 0..1; x-axis caption "order value / sample average" in muted gray; common y max 34.
- **Raw curve:** magenta `#d55181`, width 3, through normalized points x `[0.02, 0.06, 0.10, 0.15, 0.22, 0.30, 0.40, 0.52, 0.66, 0.82, 0.98]` with heights `[2, 30, 24, 16, 10, 6.5, 4, 2.5, 1.5, 0.8, 0.3]` (peaked at 0.06, long right tail).
- **Bells of the mean:** Gaussian curves centered on the raw mean (~0.22), drawn over 60 steps: n=2 (center 0.19, left sd 0.09 / right sd 0.15 so it stays visibly right-skewed, peak 9, `rgba(42,120,214,0.55)`); n=10 (center 0.22, sd 0.075, peak 18, `rgba(42,120,214,0.8)`); n=40 (center 0.22, sd 0.038, peak 30, violet `#4a3aa7`); width 2.5.
- **Annotations (bold 13px, left-aligned):** magenta "raw data: skewed at any n" near the raw peak; violet "mean, n=40" beside its peak; blue "n=10" and "n=2" beside their curves.
- **Legend (right side, 12px):** magenta swatch "raw order values"; violet swatch "averages of n orders".

## Regeneration instructions

- **Template:** tutorial detail page (tutorials style, see `tutorials/CLAUDE.md`). h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks. Each section: `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout`. Every section uses `.text-col` (50%) / `.viz-col` (50%); sections 2–4 hold one 720×300 canvas. One section places canvases `c1a`/`c1b` (310×340 each) side by side inside its single viz cell, wrapped in a `.viz-pair` flex row (`display:flex; gap:10px`, each canvas `flex:1 1 0; min-width:0`).
- **Text cell structure:** `.tags` row of colored pills, `<ul>` of one-line bullets each opening with `<b>bold term</b>` (colored `#1a5276`), one italic `.example` paragraph, one `.key-point` callout (background `#f8f9fa`, left border `3px solid #1a5276`); the last section's callout is prefixed "Common mistake:" instead of "Key point:". The bullet word "average" in section 4 is italic (`<i>`).
- **Tag pill styles:** 0.72rem bold, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; bullets 0.92rem; canvases `width:100%` with `1px solid #e0e0e0` border, 4px radius. HTML entities used in text: `&times;` (×), `&ndash;` (–), `&divide;` (÷), `&radic;` (√), `&asymp;` (≈).
- **Chart palette (JS object P):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Project palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- **Canvas:** intrinsic sizes per chart (720×300 or 310×340); sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) via a shared `setup(id, W, H)` helper (`ctx.scale` back to logical coordinates). All data hardcoded literal arrays — no `Math.random()`; invented shapes labeled "illustrative".
- No nav bar, no back/home links, no cross-page links. In regenerated HTML any card links would use `.html` extensions.
