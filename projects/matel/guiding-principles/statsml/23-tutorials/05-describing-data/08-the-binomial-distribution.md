# The Binomial Distribution

**Page type:** detail page (tutorial page: 4 card-sections, each a two-column layout table — text left 50%, canvas right 50%)
**HTML title tag:** The Binomial Distribution

**Subtitle:** Counting successes out of a fixed number of tries — like the number of heads in twenty coin flips

## Flip a Coin Twenty Times, Count the Heads

**Tags:** `coin flips` (blue), `yes/no trials` (green), `counts` (blue)

- **The setup** — 20 flips of a fair coin; each flip is heads or tails; count the heads
- **The answer varies** — repeat the whole 20-flip session and you get 9, then 12, then 10…
- **Most likely: 10** — but exactly 10 heads happens in only 17.6% of sessions
- **Near-misses rule** — 9 or 11 heads each happen 16.0% of the time; 8 or 12 each 12.0%
- **Three ingredients** — a fixed number of tries (20), same chance each try (50%), independent tries

*Example:* All 20 heads has probability 1 in 1,048,576 — possible, but you would rightly suspect the coin.

**Key point:** The binomial distribution lists, for every possible head-count 0–20, how often that count shows up — a whole menu of outcomes, not one prediction.

### Visualization (canvas `c1`, 720×300)

Bar chart of the binomial(20, 0.5) probability mass function.

- **Title (bold 15px, ink #1a5276, centered):** "Chance of Each Head-Count in 20 Fair Flips"
- **Data:** P(k heads in 20 fair flips) as percent, k = 0..20: `[0.0, 0.0, 0.02, 0.11, 0.46, 1.48, 3.70, 7.39, 12.01, 16.02, 17.62, 16.02, 12.01, 7.39, 3.70, 1.48, 0.46, 0.11, 0.02, 0.0, 0.0]`
- **Axes:** padding top 48 / bottom 46 / left 55 / right 20; y max 20% with labels at 0%, 5%, 10%, 15%, 20% (gridlines `#e5e9ef` above 0); x labels 0..20 under each bar; axis lines `#999`
- **Bars:** 21 bars, width = column width minus 4px; bar for k=10 in green `#008300`, all others blue `#2a78d6`
- **X-axis caption (mute `#6b7280`):** "number of heads in 20 flips"
- **Annotations:** green (`#008300`) bold 13px left-aligned at ~11.2 bars in: "10 heads: most likely, yet only 17.6%"; below it orange (`#d95926`) bold 12px: "9 or 11: 16.0% each"

## Three Flips, All Eight Outcomes, By Hand

**Tags:** `worked example` (green), `counts` (blue)

- **Shrink it** — with 3 flips there are only 2×2×2 = 8 equally likely sequences
- **List them** — TTT, TTH, THT, HTT, THH, HTH, HHT, HHH; each has probability 1/8
- **Group by heads** — 0 heads: 1 way; 1 head: 3 ways; 2 heads: 3 ways; 3 heads: 1 way
- **Read off odds** — P(0) = 1/8, P(1) = 3/8, P(2) = 3/8, P(3) = 1/8; they sum to 8/8
- **Why middles win** — middle counts have more sequences that produce them; extremes have one

*Example:* "2 heads in 3 flips" is 3 times likelier than "3 heads" purely because 3 orderings give it.

**Key point:** Binomial probabilities are just counting: (ways to arrange k heads) × (chance of any one sequence). The 20-flip chart is this same idea, scaled up.

### Visualization (canvas `c2`, 720×300)

Two-panel diagram: eight enumerated 3-flip sequences on the left, collapsed 1-3-3-1 bar chart on the right, divided by a vertical dashed line at x=330 (`#bdc3c7`, dash 4/3).

- **Title (bold 15px, ink #1a5276, centered):** "3 Flips: 8 Sequences Collapse into 1–3–3–1"
- **Left panel:** heading (bold 12px, text `#2c3e50`, centered at x=175): "all 8 sequences, each with probability 1/8". Eight rounded boxes (104×30px, fill `rgba(42,120,214,0.08)`) in a 2-column × 4-row grid starting at x=65/195, y=80, row spacing 44px. Sequences with head-counts: TTT (0H), TTH (1H), THT (1H), HTT (1H), THH (2H), HTH (2H), HHT (2H), HHH (3H). Each box's 2px border and bold 13px sequence label are colored by head count using `[#6b7280, #2a78d6, #199e70, #d95926]` for 0/1/2/3 heads; a mute "kH" label sits to the right inside the box. Footer (mute 12px, x=175, y=272): "color = number of heads"
- **Right panel:** 4 bars at x=370, panel width 320, baseline y=235, chart height 155, y-scale max 3.6. Values (ways): `[1, 3, 3, 1]`, bar width 56, colored by the same 4-color head-count palette. Value labels "1/8", "3/8", "3/8", "1/8" bold above bars; x labels "0 heads" … "3 heads"
- **Annotations:** magenta (`#d55181`) bold 13px centered at panel top: "middle counts have more orderings"; mute 12px below baseline: "P(k) = ways × 1/8 — check: 1+3+3+1 = 8"

## Where You Meet It: Conversions, Defects, A/B Tests

**Tags:** `where it's used` (blue), `rule of thumb` (green)

- **Everywhere yes/no** — 20 visitors: how many sign up? 20 parts: how many defective?
- **Mean** — tries × chance = 20 × 0.5 = 10 expected heads
- **Spread** — sd = √(20 × 0.5 × 0.5) = √5 ≈ 2.24 heads
- **Normal range** — mean ± 2 sd ≈ 6 to 14 heads covers about 95% of fair-coin sessions
- **Surprise detector** — 18 heads from a fair coin: ~0.02% chance; suspect the coin, not luck

*Example:* An A/B test is two coins: if B converts 18 of 20 while A's rate says 10, that gap is no accident.

**Key point:** The binomial tells you which head-counts are boring luck and which demand an explanation — that boundary is the heart of every A/B test.

### Visualization (canvas `c3`, 720×300)

Same binomial(20, 0.5) bar chart with the central 95% band highlighted and an extreme value flagged.

- **Title (bold 15px, ink #1a5276, centered):** "Fair Coin, 20 Flips: What Counts as Boring Luck?"
- **Data:** same B20 percent array as canvas c1; same axes/padding (top 48, bottom 46, left 55, right 20), y max 20%, x labels 0..20
- **95% band:** background rectangle over k=6..14 filled `rgba(0,131,0,0.10)` spanning full chart height
- **Bars:** k in 6..14 green `#008300`; all other bars red `#e74c3c`
- **Marker at 18:** vertical dashed red line (`#e74c3c`, width 2, dash 5/4) at k=18.5 slot from top+44 to baseline; right-aligned bold red 12px two-line label: "18 heads: ~0.02% —" / "suspect the coin"
- **Annotations (centered near k=10.5):** green bold 13px: "6–14 heads ≈ 95% of sessions"; ink `#1a5276` bold 12px below: "mean 10, sd √5 ≈ 2.24"
- **X-axis caption (mute):** "number of heads in 20 flips"

## The Mistake: Expecting Exactly Half

**Tags:** `common mistake` (red), `intuition trap` (orange)

- **"Fair = 50/50 split"** — no: exactly 10 of 20 happens in barely 1 session in 6
- **More flips, rarer** — exactly half falls from 24.6% (n=10) to 2.5% (n=1,000)
- **What does converge** — the fraction of heads hugs 50% ever tighter as n grows
- **No memory** — after 5 tails in a row, the next flip is still 50/50; coins owe you nothing
- **Check the recipe** — changing success chance mid-run or linked trials break the binomial

*Example:* 505 heads in 1,000 flips is completely normal; demanding exactly 500 misreads what "fair" means.

**Common mistake:** Fairness promises the ratio settles near one half — it never promises an exact split, and the exact split itself gets rarer as you flip more.

### Visualization (canvas `c4`, 720×300)

Bar chart: probability of exactly n/2 heads shrinking as n grows.

- **Title (bold 15px, ink #1a5276, centered):** ""Exactly Half Heads" Gets Rarer the More You Flip"
- **Data:** n values `10, 20, 50, 100, 500, 1,000` with P(exactly half) percent `[24.6, 17.6, 11.2, 8.0, 3.6, 2.5]`
- **Axes:** padding top 48 / bottom 60 / left 55 / right 25; y max 28% with labels at 0%, 10%, 20% and gridlines `#e5e9ef`; x labels "n = 10" … "n = 1,000"
- **Bars:** width 66, evenly gapped, all violet `#4a3aa7`; bold value labels ("24.6%" etc., text `#2c3e50`) above each bar
- **X-axis caption (mute, centered):** "flips per session — bar = chance of exactly n/2 heads"
- **Annotations (left-aligned at ~38% chart width):** magenta (`#d55181`) bold 13px: "the exact split fades…"; green (`#008300`) below: "…while the fraction of heads locks onto 50%"

## Regeneration instructions

- **Layout:** tutorial detail page. `<h1>` (no index number) + `.subtitle`, then four `.card-section` blocks, each with an `<h2>` (1.3rem, `#1a5276`, 2px solid `#2980b9` bottom border) and a `table.layout` (width 100%, border-collapse) containing one row: `td.text-col` (50%) and `td.viz-col` (50%), both `vertical-align: top`, padding 12px.
- **Text column structure:** `.tags` pill row first, then a `<ul>` of one-line bullets each opening with `<b>bold term</b>` (bold terms colored `#1a5276`), one italic `.example` paragraph, one `.key-point` callout (background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem).
- **Tag pills:** inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px. Colors — blue: bg `rgba(26,82,118,0.12)` text `#1a5276`; green: bg `rgba(39,174,96,0.15)` text `#27ae60`; red: bg `rgba(231,76,60,0.12)` text `#e74c3c`; orange: bg `rgba(230,126,34,0.15)` text `#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px solid `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; canvases `width: 100%` with `1px solid #e0e0e0` border, 4px radius.
- **Canvases:** all 720×300 intrinsic; scaled via `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) through a shared `setup(id)` helper. Data arrays are hardcoded literals (no `Math.random()`). Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- No nav bar, no back/home links, no cross-page links. In regenerated HTML any card links use `.html` extensions (this page has none).
