# Non-Parametric Tests

**Page type:** detail page (tutorial layout: `.card-section` blocks, each with h2 + two-column `table.layout` — text left 50% with tag pills/bullets/example/key-point, canvas right 50%)
**HTML title tag:** Non-Parametric Tests

**Subtitle:** Tests that rank the data instead of assuming a bell curve — built for skewed, weird, or tiny samples

## Two Servers, Twelve Page Loads, One Ugly Outlier

**Tags:** `core idea` (blue), `running example` (green), `skewed data` (orange)

- **The setup** — 6 page loads on the new server, 6 on the old, timed in seconds
- **New server** — 0.8, 1.0, 1.1, 1.3, 1.4, 1.7 seconds: consistently quick
- **Old server** — 1.5, 1.9, 2.3, 2.6, 3.1 ... and one 8.9-second monster
- **The problem** — load times aren't bell-shaped; that 8.9 drags the old average to 3.4
- **The trick** — replace each time with its RANK (1st fastest ... 12th); 8.9 becomes just "12th"

*Example:* As a raw value, 8.9 is a wrecking ball; as a rank, it's merely "last place" — no worse than 3.2 would be.

**Key point:** ranking keeps the ORDER of the data and throws away the distances — so one wild value can't dominate the test.

### Visualization (canvas `c1`, 720×300)

Number-line dot plot of the 12 load times on two rows, with the outlier flagged.

- **Title (bold 15px, `#1a5276`, top center):** "12 Page Loads in Seconds — One Monster Value".
- **Data:** new server = `[0.8, 1.0, 1.1, 1.3, 1.4, 1.7]` (green `#008300` dots, row y=110); old server = `[1.5, 1.9, 2.3, 2.6, 3.1, 8.9]` (orange `#d95926` dots, row y=185; the 8.9 dot is red `#e74c3c`). Dot radius 8.
- **Axis:** horizontal gray `#999` line at y=235, x range 0–9.5s, tick labels "0s", "2s", "4s", "6s", "8s" (12px); axis caption: "page load time (seconds)".
- **Row labels (bold 13px):** green: "new server: 0.8 – 1.7s"; orange: "old server: 1.5 – 3.1s ...".
- **Outlier callout:** red `#e74c3c` vertical pointer line above the 8.9 dot, label bold 13px red right-aligned: "8.9s — drags the old average to 3.4s".
- **Annotation (bold 13px violet `#4a3aa7`, centered, y=56):** "nothing here looks like a bell curve".

## The Rank-Sum Check, Fully by Hand

**Tags:** `worked example` (green), `small numbers` (blue)

- **Step 1** — pool all 12 times, sort, rank 1 (fastest) to 12 (slowest)
- **Step 2** — new server takes ranks 1, 2, 3, 4, 5, 7 — rank sum = 22
- **Step 3** — old server takes ranks 6, 8, 9, 10, 11, 12 — rank sum = 56
- **Step 4** — if servers were equal, each side would expect a rank sum near 39
- **Step 5** — a split this lopsided happens by luck ~0.4% of the time: p ≈ 0.004

*Example:* This is the Mann-Whitney / rank-sum test: 22 vs the expected 39 is what "significant" looks like in ranks.

**Key point:** the whole test is arithmetic on ranks 1-12 — no bell curve, no standard deviation, nothing to assume about the shape.

### Visualization (canvas `c2`, 720×300)

Rank-transformation strip of 12 sorted cells plus rank-sum bars with the expected marker.

- **Title (bold 15px, `#1a5276`, top center):** "Sort Everything Together, Hand Out Ranks 1–12".
- **Strip (12 cells, y=70, height 46, spanning the width with 50/30 side padding):** sorted values with group: 0.8(n), 1.0(n), 1.1(n), 1.3(n), 1.4(n), 1.5(o), 1.7(n), 1.9(o), 2.3(o), 2.6(o), 3.1(o), 8.9(o). New-server cells fill `rgba(0,131,0,0.15)` with green `#008300` border; old-server cells fill `rgba(217,89,38,0.15)` with orange `#d95926` border. Each cell shows the value (bold 13px `#2c3e50`, top) and rank label "r1"…"r12" (bold 12px in the group color, bottom).
- **Strip caption (12px gray, centered):** "seconds (top) and rank (bottom) — green = new server, orange = old server".
- **Rank-sum bars** (start x=250, max width 330 at value 60, height 26, 0.7 alpha):
  - "new server ranks 1+2+3+4+5+7": 22, green `#008300`
  - "old server ranks 6+8+9+10+11+12": 56, orange `#d95926`
  - Value labels bold 14px right of bars; labels 12px `#1a5276` right-aligned left of bars.
- **Expected marker:** dashed violet `#4a3aa7` vertical line (dash 5/4, width 2) at 39, labeled bold 12px violet: "39 = expected if equal".
- **Caption (bold 13px red `#e74c3c`, centered, y=285):** "22 vs 56 — a split this clean happens by luck only ~0.4% of the time (p ≈ 0.004)".

## The Same Data Fools the t-Test

**Tags:** `why it matters` (orange), `outliers` (red)

- **t-test verdict** — on the raw seconds: t ≈ 1.9, p ≈ 0.11 — "no clear difference"
- **Rank verdict** — on the ranks: p ≈ 0.004 — "old server is clearly slower"
- **The culprit** — 8.9 inflates the old server's spread, drowning its own signal in fake noise
- **Real data** — load times, incomes, session lengths, house prices: skewed with monsters
- **Rule of thumb** — long tail or tiny sample? Reach for ranks before means

*Example:* The outlier that PROVES the old server misbehaves is exactly what makes the t-test shrug at it.

**Key point:** the t-test punishes big outliers by inflating its noise estimate — the rank test just files them as "last place" and sees the pattern.

### Visualization (canvas `c3`, 720×300)

Two horizontal p-value bars against the 0.05 alarm line: t-test vs rank test on the same data.

- **Title (bold 15px, `#1a5276`, top center):** "One Dataset, Two Verdicts".
- **Bars** (start x=260, max width 360 at p=0.15, height 30, 0.7 alpha, minimum drawn width 6px):
  - "t-test on raw seconds": p = 0.11, gray `#6b7280`; verdict right of bar in bold 13px red `#e74c3c`: 'p ≈ 0.11 — "could be luck"'
  - "rank test on ranks 1–12": p = 0.004, aqua `#199e70`; verdict in bold 13px green `#008300`: 'p ≈ 0.004 — "old server is slower"'
  - Bar labels bold 13px `#1a5276` right-aligned left of bars.
- **Alarm line:** dashed red `#e74c3c` vertical line (dash 5/4, width 2) at p = 0.05, labeled bold 12px red above: "p = 0.05 line".
- **Explanation strip (12px gray, centered, y=235/253):** "why the t-test misses: the 8.9s outlier inflates the old server's spread," / "so the 2.2s average gap looks small next to the inflated noise".
- **Takeaway (bold 13px orange `#d95926`, centered, y=282):** "the outlier is evidence AGAINST the old server — the t-test converts it into doubt".

## What People Get Wrong: "No Assumptions" and "Same Question"

**Tags:** `common mistake` (red), `trade-off` (orange)

- **Different question** — rank tests ask "does one group tend to sit higher?", not "are means equal?"
- **Means still differ** — here old mean 3.4 vs median 2.45; the outlier owns the mean
- **Not assumption-free** — still needs independent observations and comparable shapes
- **The price** — on genuinely bell-shaped data, ranks waste a little power (~5%)
- **Not a cure-all** — if the outlier is a data bug, fix the data; don't just switch tests

*Example:* A report says "average load time 3.4s" — yet 5 of 6 old-server loads were under 3.2s; the median (2.45s) tells it straight.

**Key point:** non-parametric means "no bell-curve assumption", not "no assumptions" — and it answers a rank question, not a mean question.

### Visualization (canvas `c4`, 720×300)

Grouped bar chart: mean vs median per server, showing the outlier owns only the mean.

- **Title (bold 15px, `#1a5276`, top center):** "Mean vs Median: the Outlier Owns Only the Mean".
- **Data:** new server — mean 1.22s, median 1.2s (group center at 25% of plot width); old server — mean 3.38s, median 2.45s (center at 72%).
- **Axes:** L-shaped gray `#999`, padding top 56, bottom 60, left 60, right 30; y 0–4.0s with gridlines `#e5e9ef` and labels "0s"–"4s" at integers.
- **Bars:** 70px wide per bar, 0.7 alpha; mean bar in magenta `#d55181` left of each group center, median bar in aqua `#199e70` right; value labels bold 13px above bars ("1.22s", "1.2s", "3.38s", "2.45s"); group labels 13px below baseline: "new server", "old server".
- **Legend (12px, top left of plot):** magenta swatch "mean (what the t-test compares)"; aqua swatch "median (what ranks reflect)".
- **Annotation (bold 13px red `#e74c3c`):** "one 8.9s load pushes the old mean 0.9s above its own median".
- **Caption (bold 12px violet `#4a3aa7`, bottom center):** "different tests answer different questions — pick the question first".

## Regeneration instructions

- **Layout:** tutorial topic page. `<h1>` (no index number), `.subtitle` paragraph, then four `.card-section` blocks. Each section: `<h2>` (1.3rem, `#1a5276`, 2px solid `#2980b9` bottom border) + `table.layout` (width 100%, border-collapse) with one `<tr>`: left `td.text-col` (50%) holding `.tags` pills, a `<ul>` of bullets (each starting with `<b>` term in `#1a5276`), an italic `.example` paragraph, and a `.key-point` callout; right `td.viz-col` (50%) holding one canvas.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; `.key-point` background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem; `.example` italic `#555` 0.9rem; ul 0.92rem.
- **Tag pills:** `.tag` inline-block 0.72rem bold, padding 2px 10px, radius 10px; blue = `rgba(26,82,118,0.12)`/`#1a5276`, green = `rgba(39,174,96,0.15)`/`#27ae60`, red = `rgba(231,76,60,0.12)`/`#e74c3c`, orange = `rgba(230,126,34,0.15)`/`#e67e22`.
- **Canvas:** each 720×300 intrinsic, CSS `width:100%`, border `1px solid #e0e0e0` radius 4px; scaled by `window.devicePixelRatio` via a shared `setup(id)` helper (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). The raw data arrays NEW_T and OLD_T are shared across charts as script-level constants.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- No nav bar, no back/home links, no cross-page links. In regenerated HTML, any card links use `.html` extensions.
