# Power Laws

**Page type:** detail page (tutorial topic page: `.card-section` blocks, each an h2 + two-column `table.layout` — text left 50%, canvas right 50%)
**HTML title tag:** Power Laws

**Subtitle:** Data where doubling the size always keeps the same fixed fraction of items — a few giants, a vast floor of small ones, and no "typical" value anywhere

## Video Views: A Few Giants, a Floor of Thousands

Tags: `core idea` (blue), `running example` (green)

- **The data** — view counts for every video on a small video site (illustrative numbers)
- **The giant** — the top video has 80,000 views; video #40 sits near 13,000; #400 near 4,000
- **The floor** — thousands of videos share the bottom, each with a few hundred views
- **No bell curve** — there is no hump in the middle; just a cliff and a long flat floor
- **Same shape elsewhere** — city populations, word frequencies, wealth, website links

*Example:* One country has a handful of mega-cities, some mid-size cities, and thousands of small towns — same cliff, different data.

**Key point:** **Power law, seen before defined:** when items are ranked biggest-first and each doubling of rank drops the size by the same fixed proportion, sizes span many multiples — that is a power law.

### Visualization (canvas `c1`, 720×300)

Bar chart: rank-ordered view counts for the top 40 videos — a cliff and a floor.

- **Title (bold 15px, `#1a5276`, top center):** "Views by Rank: Top 40 Videos (illustrative)"
- **Data:** 40 bars; bar i (0-indexed) has value `80 / sqrt(i+1)` thousand views (deterministic formula matching the ÷4-per-doubling tail, no randomness). Rank 1 = 80k, rank 40 near 13k.
- **Axes:** y scale 0 to max 85 (thousands of views); L-shaped gray `#999` axis lines; padding top 52, bottom 56, left 64, right 25.
- **Colors:** first 3 bars orange `#d95926`, remaining bars blue `#2a78d6`; bar width = plot width / 40 with 1px inset each side.
- **Annotations:** bold 13px orange `#d95926` "#1: 80k views" near top-left of plot; bold 13px blue `#2a78d6` "#40: near 13k — and thousands more below it" at ~30% width in the lower half; bold 13px red `#e74c3c` "no hump in the middle: a cliff, then a floor" at ~30% width, ~42% height.
- **Axis labels (12px `#444`):** x "videos ranked by views (1 = most viewed)" bottom center; y "views (thousands)" rotated vertical on the left.

## The Doubling Rule You Can Check by Hand

Tags: `worked example` (green), `rule of thumb` (blue)

- **Count them** — videos with at least 1,000 views: 6,400 of them
- **Double once** — at least 2,000 views: 1,600 videos, exactly a quarter as many
- **Double again** — at least 4,000: 400. At least 8,000: 100. At least 16,000: 25
- **The rule** — every time the view bar doubles, one quarter of the videos survive
- **The signature** — a fixed fraction per doubling, at every scale, is the power law

*Example:* Check it: 6,400 ÷ 4 = 1,600, ÷ 4 = 400, ÷ 4 = 100, ÷ 4 = 25 — the same ÷4 works at every step.

**Key point:** **Contrast with a bell curve:** for people's heights, "at least 7 feet" doesn't keep a quarter of "at least 3.5 feet" — it keeps almost nobody. Bell tails die fast; power-law tails don't.

### Visualization (canvas `c2`, 720×300)

Bar chart: the doubling table as five bars, each one quarter the previous.

- **Title (bold 15px, `#1a5276`, top center):** "Videos With at Least X Views: Quarter Survive Each Doubling"
- **Data:** labels `['1k+', '2k+', '4k+', '8k+', '16k+']`, counts `[6400, 1600, 400, 100, 25]` (minimum bar height 3px so the tiny bars stay visible).
- **Bar colors in order:** blue `#2a78d6`, aqua `#199e70`, green `#008300`, violet `#4a3aa7`, orange `#d95926`; bar width 55% of slot.
- **Axes:** y scale 0 to 7000; L-shaped gray `#999` axis; padding top 52, bottom 58, left 64, right 25. Count value (locale-formatted, bold 12px `#333`) above each bar, label below.
- **Annotations:** magenta `#d55181` bold 13px "÷4" centered between each adjacent pair of bars (4 occurrences, at slot boundaries, y = plot top + 58); magenta bold 13px "double the bar, keep one quarter — at every scale" at ~55% width near the top.
- **Axis labels (12px `#444`):** x "minimum view count (each step doubles)" bottom center; y "number of videos" rotated vertical.

## The Fingerprint: A Straight Line on Log-Log Axes

Tags: `how to detect` (blue), `worked example` (green)

- **The trick** — put both axes on a log scale, where each step means "times 10" or "times 2"
- **Plot the table** — the five points (1k, 6400) ... (16k, 25) land on one straight line
- **Why straight** — "÷4 per doubling" is a constant step down per constant step right
- **The slope** — here every doubling drops the count 4x; a steeper line means a thinner tail
- **The contrast** — a bell curve on the same axes bends and then plunges; it can't stay straight

*Example:* Plot city sizes, word counts, or earthquake energies on log-log paper and the same straight line appears.

**Key point:** **Quick test:** log-log plot looks straight over several doublings → suspect a power law. Curved and plunging → a bell-like shape with a fast-dying tail.

### Visualization (canvas `c3`, 720×300)

Log-log line chart: the same five points form a straight line; a dashed bell-curve comparison bends and plunges.

- **Title (bold 15px, `#1a5276`, top center):** "The Same Five Numbers on Log-Log Axes"
- **Power-law series:** x positions 0–4 (log2 of threshold, labels `1k, 2k, 4k, 8k, 16k`), y values = log10 of counts `[3.81, 3.20, 2.60, 2.00, 1.40]`; drawn as a single straight green `#008300` line (width 3) from the first to the last point, with 6px-radius green dots at all five points.
- **Bell-curve comparison series:** same x positions, log10 values `[3.81, 3.72, 3.35, 2.45, 1.02]`; dashed gray `#6b7280` line (width 2, dash 6/5), no dots.
- **Axes/scales:** y from 1 to 4 in log10 units with horizontal gridlines `#e5e9ef` at ticks labeled `10`, `100`, `1,000`, `10,000` (right-aligned 12px `#555`); L-shaped gray `#999` axis; padding top 52, bottom 58, left 70, right 180.
- **Annotations:** bold 13px green "straight line = power law" near (x=1.15, y=3.5); bold 12px gray `#6b7280` "a bell curve bends, then plunges" near (x=1.35, y=2.05).
- **Legend (top right, x = width−168):** green line swatch + "video views" (12px `#222`); dashed gray swatch + "bell curve (compare)".
- **Axis labels (12px `#444`):** x "minimum views (log scale)" bottom center; y "videos with at least that many (log)" rotated vertical.

## Why It Matters: There Is No "Typical" Video

Tags: `80/20` (orange), `common mistake` (red), `where it's used` (blue)

- **80/20 shape** — in many catalogs the top 20% of videos carry most of the views (the classic 80/20)
- **Mean vs median** — mean 480 views, median 90: the mean is 5x what most videos get
- **No typical value** — no single number stands for "a video"; every summary misleads someone
- **Plan for the head** — caching, servers, and payouts are driven by the giants, not the average
- **Sampling trap** — a small random sample likely misses every giant and underestimates totals

*Example:* Budgeting bandwidth for "the average video's 480 views" fails twice: most videos need far less, the top one needs 160x more.

**Key point:** **The takeaway:** when data is power-law shaped, stop asking "what's typical?" and start asking "how much do the top few carry?"

### Visualization (canvas `c4`, 720×300)

Split panel: left, two stacked horizontal 80/20 split bars; right, mean vs median bars.

- **Title (bold 15px, `#1a5276`, top center):** "The 80/20 Shape — and Why \"Average Views\" Misleads (illustrative)"
- **Left panel (two horizontal split bars, x=40, width 330, height 40, white 2px borders, white bold 13px in-segment labels, bold 12px `#1a5276` label above each bar):**
  - Bar 1 at y=80, label "share of videos": left segment 20% orange `#d95926` labeled "top 20%", right segment 80% blue `#2a78d6` labeled "the other 80% of videos".
  - Bar 2 at y=170, label "share of views": left segment 80% orange labeled "top 20% of videos collect 80% of views", right segment 20% blue labeled "20%".
  - Below (bold 13px orange, y=248): "a fifth of the videos carry four fifths of the traffic".
- **Divider:** dashed vertical `#bdc3c7` line at x=430 (dash 4/3).
- **Right panel (two bars, baseline y=235, chart height 150, scale max 550):** "median video" 90 views in green `#008300`; "mean" 480 views in magenta `#d55181`; bars 70px wide starting at x=480 spaced 120px; value labels "90 views" / "480 views" bold 13px `#333` above bars, names 12px below baseline.
- **Annotations:** bold 13px magenta two lines centered at x=580: "mean is 5x the median —" (y=62) / "it describes no actual video" (y=80); 12px `#444` "views per video" at x=580 bottom.

## Regeneration instructions

- **Layout:** tutorial topic page. `<h1>` (no index number) with 2px `#2980b9` bottom border, `.subtitle` paragraph, then four `.card-section` blocks. Each section: `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) followed by `table.layout` (100% width, collapsed) with one row: `td.text-col` (50%) and `td.viz-col` (50%), both 12px padding, top-aligned.
- **Text cell structure:** `.tags` row of pill spans, then a `<ul>` of 4–5 one-line bullets each opening with `<b>bold term</b>` (bold terms colored `#1a5276`), one italic `.example` paragraph, one `.key-point` callout.
- **Tag pills:** 0.72rem, weight 600, padding 2px 10px, radius 10px. Colors — blue: bg `rgba(26,82,118,0.12)` text `#1a5276`; green: bg `rgba(39,174,96,0.15)` text `#27ae60`; red: bg `rgba(231,76,60,0.12)` text `#e74c3c`; orange: bg `rgba(230,126,34,0.15)` text `#e67e22`.
- **Key-point callout:** background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem. `.example`: italic, `#555`, 0.9rem.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; canvases `width:100%` with `1px solid #e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** each canvas declares intrinsic `width="720" height="300"`; a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. All data is hardcoded/deterministic (no `Math.random()`). Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object `P`:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette anchors: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- **Links:** this page has no card links; any grid page linking here uses the `.html` extension in regenerated HTML.
