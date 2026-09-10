# The Log-Normal Distribution

**Page type:** detail page (tutorial page: 4 card-sections, each a two-column layout table — text left 50%, canvas right 50%)
**HTML title tag:** The Log-Normal Distribution

**Subtitle:** Data that grows by multiplying, not adding — incomes, file sizes, and other long-right-tail shapes

## 1,000 Household Incomes: the Lopsided Hill

**Tags:** `incomes` (blue), `long tail` (orange), `core idea` (blue)

- **The setup** — survey 1,000 households; the median income is $50k, the mean is $65k
- **The shape** — a steep hill near $30k with a tail stretching past $200k: not a bell
- **No mirror** — nobody can earn $80k below the median, but plenty earn $80k above it
- **Why multiplied** — raises are percentages: 5% of a big salary is more dollars than 5% of a small one
- **Gaps compound** — multiply many small percentage nudges and the gaps stretch rightward

*Example:* 275 of the 1,000 households sit in the $20k–40k bins, while 28 earn over $200k.

**Key point:** Adding many small effects gives a bell; multiplying them gives this lopsided hill. Log-normal is the bell's multiplicative twin.

### Visualization (canvas `c1`, 720×300)

Right-skewed histogram of 1,000 household incomes in $20k bins with mean/median markers.

- **Title (bold 15px, ink #1a5276, centered):** "1,000 Household Incomes, $20k Bins (illustrative)"
- **Data:** bin counts for bins 0–20, 20–40, …, 180–200, 200+: `[104, 275, 220, 143, 89, 56, 35, 24, 15, 11, 28]` (sums to 1000); x labels `0, 20, 40, 60, 80, 100, 120, 140, 160, 180, 200+`
- **Axes:** padding top 48 / bottom 46 / left 55 / right 20; y max 300 with labels 0, 100, 200 (gridlines `#e5e9ef`); axis lines `#999`
- **Bars:** the 200+ bin orange `#d95926`; all others blue `#2a78d6`; count labels (12px, text `#2c3e50`) above each bar
- **X-axis caption (mute `#6b7280`):** "household income ($k)"
- **Median marker:** vertical dashed green line (`#008300`, width 2, dash 6/4) at $50k (2.5 bins in); green bold 13px label: "median $50k"
- **Mean marker:** vertical dashed magenta line (`#d55181`) at $65k (3.25 bins in); magenta bold 13px label: "mean $65k — pulled up by the tail"
- **Tail annotation:** orange bold 12px at ~5.6 bins in: "28 households above $200k stretch the tail"

## The Trick: Equal Ratios Become Equal Steps

**Tags:** `worked example` (green), `multiplicative` (blue)

- **Four incomes** — $25k, $50k, $100k, $200k: each one is exactly double the last
- **Raw ruler** — on a normal axis the gaps are 25, 50, and 100: wildly uneven
- **Log ruler** — log10 gives 1.40, 1.70, 2.00, 2.30: perfectly even steps of 0.30
- **The definition** — data is log-normal when its logs form a normal bell curve
- **Hand check** — ×2 anywhere adds the same 0.30 in log-land, rich or poor alike

*Example:* Going $25k→$50k and $100k→$200k are the same "distance" in log-land: one doubling.

**Key point:** The log turns multiplication into addition. That one move converts the lopsided income hill into an ordinary bell you already know how to handle.

### Visualization (canvas `c2`, 720×300)

Two horizontal number lines ("rulers") showing the same four incomes on a raw axis vs a log axis.

- **Title (bold 15px, ink #1a5276, centered):** "$25k → $50k → $100k → $200k: Two Rulers, One Dataset"
- **Layout:** lines from x=70 spanning width−130; Row A at y=110, Row B at y=215; axis lines `#999`
- **Row A (raw axis 0..210):** heading (bold 13px, text `#2c3e50`, left-aligned): "raw dollars: gaps of 25, 50, 100 — uneven". Orange (`#d95926`) dots (radius 7) at 25, 50, 100, 200 with labels "$25k", "$50k", "$100k", "$200k" above; gap-width labels (mute 12px) below at midpoints: "25", "50", "100"
- **Row B (log10 axis 1.3..2.4):** heading (bold 13px): "log10 dollars: 1.40, 1.70, 2.00, 2.30 — equal steps of 0.30". Green (`#008300`) dots at log values `1.40, 1.70, 2.00, 2.30` with value labels above ("1.40" etc.); green bold "+0.30" labels below each of the three gaps
- **Takeaway (magenta `#d55181` bold 13px, centered at y=268):** "each doubling = the same 0.30 step — multiplication became addition"

## Why It Matters: Take Logs, Get the Bell Back

**Tags:** `where it's used` (blue), `best practice` (green)

- **Same 1,000 homes** — re-bin the incomes by their logarithm and the histogram turns symmetric
- **The family** — file sizes, session lengths, city populations, stock prices, house prices
- **Report medians** — the mean ($65k) sits above what ~64% of households earn; median ($50k) is honest
- **Model on logs** — regressions and z-scores behave far better on log(income) than on income
- **Spot it fast** — all values positive and mean well above median: try the log transform

*Example:* A $30k household and a $90k household differ by the same log-step as $90k versus $270k.

**Key point:** If the raw data is a lopsided hill, analyze the logs and translate back at the end — you get bell-curve tools on multiplicative data.

### Visualization (canvas `c3`, 720×300)

Symmetric histogram of the same 1,000 households binned by log(income) — the bell reappears.

- **Title (bold 15px, ink #1a5276, centered):** "Same 1,000 Households, Binned by log(Income): a Bell"
- **Data:** ln(income $k) bins of width 0.3 from 2.7 to 5.1 (dollars: $15k..$164k), tails pooled; counts: `[47, 57, 95, 134, 160, 162, 137, 98, 60, 50]`; x labels (bin start in dollars, $k): `<15, 15, 20, 27, 37, 49, 67, 90, 121, 164+`
- **Axes:** padding top 48 / bottom 62 / left 55 / right 20; y max 190 with labels 0, 50, 100, 150 (gridlines `#e5e9ef`)
- **Bars:** the two peak bins (indexes 4 and 5) solid green `#008300`; all others `rgba(0,131,0,0.4)`; count labels (12px, text `#2c3e50`) above bars
- **Two-line x caption (mute):** "bin start, back in dollars ($k) — equal log-width bins" / "note: equal bin widths in log-land are widening dollar ranges"
- **Annotations (left-aligned at ~5.8 bins in):** magenta (`#d55181`) bold 13px: "symmetric again — bell-curve tools apply here"; green bold 12px below: "peak near log($49k) ≈ the median"

## The Mistake: Using Bell-Curve Rules on the Raw Data

**Tags:** `common mistake` (red), `long tail` (orange)

- **The numbers** — our incomes have mean $65k and standard deviation $54k (tail-inflated)
- **The blunder** — "95% within mean ± 2 sd" gives −$43k to $173k: negative incomes
- **Wrong center** — quoting the $65k mean as "typical" overstates most households' reality
- **Outlier trap** — 3-sd clipping on raw incomes deletes real, legitimate high earners
- **The fix** — apply the 68–95–99.7 rule to log(income), where the bell actually lives

*Example:* A report once bounded "normal incomes" at mean ± 2 sd — the lower bound owed people money.

**Common mistake:** Bell-curve arithmetic on long-tail data produces impossible ranges and false outliers. The rule is fine — it is just aimed at the wrong axis.

### Visualization (canvas `c4`, 720×300)

Number-line diagram: the mean ± 2 sd band on raw incomes extends into negative dollars.

- **Title (bold 15px, ink #1a5276, centered):** "Mean ± 2 SD on Raw Incomes: the Band Goes Negative"
- **Layout:** number line at y=150, from x=70 spanning width−130, mapping −60 to 220 ($k); tick labels (mute 12px) at −40, 0, 50, 100, 150, 200 as "$-40k" … "$200k"
- **Impossible region:** rectangle from −60 to 0 filled `rgba(231,76,60,0.12)` (y 70–230), with red (`#e74c3c`) bold 13px centered two-line label: "impossible" / "incomes"
- **±2sd band:** rectangle from −43 to 173 ($k), 44px tall centered on the line, fill `rgba(213,81,129,0.20)`, stroke magenta `#d55181` width 2
- **Tick markers (2px vertical lines with bold 12px labels):** −43 red `#e74c3c` "mean − 2sd = −$43k" (above); 65 magenta `#d55181` "mean $65k" (above); 50 green `#008300` "median $50k" (below); 173 violet `#4a3aa7` "mean + 2sd = $173k" (above)
- **Footer (mute 12px, centered, y=248):** "mean $65k, sd $54k: 65 − 2×54 = −43 — the bell rule breaks on the raw axis"
- **Fix line (green bold 13px, centered, y=272):** "fix: apply 68–95–99.7 to log(income), then convert the bounds back to dollars"

## Regeneration instructions

- **Layout:** tutorial detail page. `<h1>` (no index number) + `.subtitle`, then four `.card-section` blocks, each with an `<h2>` (1.3rem, `#1a5276`, 2px solid `#2980b9` bottom border) and a `table.layout` (width 100%, border-collapse) containing one row: `td.text-col` (50%) and `td.viz-col` (50%), both `vertical-align: top`, padding 12px.
- **Text column structure:** `.tags` pill row first, then a `<ul>` of one-line bullets each opening with `<b>bold term</b>` (bold terms colored `#1a5276`), one italic `.example` paragraph, one `.key-point` callout (background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem).
- **Tag pills:** inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px. Colors — blue: bg `rgba(26,82,118,0.12)` text `#1a5276`; green: bg `rgba(39,174,96,0.15)` text `#27ae60`; red: bg `rgba(231,76,60,0.12)` text `#e74c3c`; orange: bg `rgba(230,126,34,0.15)` text `#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px solid `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; canvases `width: 100%` with `1px solid #e0e0e0` border, 4px radius.
- **Canvases:** all 720×300 intrinsic; scaled via `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) through a shared `setup(id)` helper. Data arrays are hardcoded literals (no `Math.random()`); invented tallies are labeled "(illustrative)" in chart titles. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- No nav bar, no back/home links, no cross-page links. In regenerated HTML any card links use `.html` extensions (this page has none).
