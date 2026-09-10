# The Distribution Family Tree

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** The Distribution Family Tree

**Subtitle:** Binomial, Poisson, exponential, gamma, and normal are not five separate facts — they are one coffee shop's arrivals asked five different questions, connected by limits and sums

## One Coffee Shop, Five Distributions

**Tags:** `core idea` (blue), `family tree` (green), `one example` (orange)

- **The shop** — a corner coffee shop gets on average 12 customers per hour, arriving one at a time
- **Binomial** — "how many of 60 minutes saw an arrival?" is a count of 60 yes/no coin flips
- **Poisson** — slice minutes into seconds and the count becomes "arrivals per hour", no n left
- **Exponential & gamma** — the wait for the next customer, and for the 3rd, retell it in time
- **Normal** — add up a whole day of arrivals and the total settles into a bell
- **One tree** — each arrow is a limit or a sum, so learning one node teaches its neighbors

*Example (italic):* The same 12-customers-per-hour shop answers "how many?", "how long?", and "how much in total?" — and each question picks its own branch of the tree.

**Key point:** These five distributions form one connected family: binomial → Poisson by slicing time finer, Poisson → exponential by asking "how long?", exponentials summed → gamma, and everything → normal in the large.

### Visualization (canvas `c1`, 720×300)

Family-tree diagram: five labeled boxes connected by arrows, each arrow labeled with the limit or operation that transforms one distribution into the next.

- **Title (bold 15px, `#1a5276`, top center):** "One Shop's Arrivals: the Distribution Family Tree".
- **Boxes:** rounded rects (radius 6), 2px borders, fill white; name bold 13px, shop question 11px `#6b7280` on a second line inside each box.
  - **Binomial** at x=25, y=118, w=125, h=64, border blue `#2a78d6`; lines "Binomial(60, 0.2)" / "arrivals in 60 min".
  - **Poisson** at x=215, y=118, w=125, h=64, border green `#008300`; lines "Poisson(12)" / "arrivals per hour".
  - **Exponential** at x=405, y=38, w=125, h=64, border aqua `#199e70`; lines "Exponential(0.2)" / "wait for next, min".
  - **Gamma** at x=405, y=198, w=125, h=64, border violet `#4a3aa7`; lines "Gamma(3, 0.2)" / "wait for 3rd, min".
  - **Normal** at x=575, y=118, w=125, h=64, border orange `#d95926`; lines "Normal(144, 12)" / "customers per day".
- **Arrows (2px lines with small filled triangle heads, label bold 11px in the arrow's color):**
  - Binomial → Poisson: horizontal at y=150 from x=150 to x=215, blue; label above: "slice time finer (n→∞, np=12)".
  - Poisson → Exponential: from (340, 132) to (405, 76), aqua; label above-left: "ask 'how long?'".
  - Exponential → Gamma: vertical at x=467 from y=102 to y=198, violet; label right: "sum 3 waits".
  - Poisson → Normal: horizontal at y=150 from x=340 to x=575, green; label below: "large λ (144 per day)".
  - Gamma → Normal: from (530, 224) to (575, 172), orange dashed (dash 4/3); label below-right: "many waits".
- **Caption (12px `#444`, bottom center, y=292):** "every arrow is a limit or a sum — same shop, five questions (illustrative rates)".

## Slicing the Hour: Binomial Becomes Poisson

**Tags:** `worked example` (blue), `limit` (green)

- **Slice the hour** — treat each of 60 minutes as a coin flip: a customer arrives (p = 0.2) or not
- **Binomial count** — arrivals in the hour follow Binomial(60, 0.2), with mean 60 × 0.2 = 12
- **Slice finer** — 3,600 seconds at p = 1/300 keeps mean 12, but "which n?" stops mattering
- **The limit** — as slices shrink with np fixed at 12, Binomial(n, p) settles onto Poisson(12)
- **Check by hand** — P(exactly 12) is 0.128 under Binomial(60, 0.2) and 0.114 under Poisson(12)

*Example (italic):* Whether you flip 60 minute-coins or 3,600 second-coins, the shop still averages 12 customers — Poisson(12) is the version with the coins sliced away.

**Key point:** Poisson is the binomial with the trials sliced infinitely fine: keep the mean np fixed, let n grow, and only the rate λ = 12 survives. That is why counts of rare events over time are Poisson.

### Visualization (canvas `c2`, 720×300)

Paired bar chart: Binomial(60, 0.2) and Poisson(12) probabilities side by side for k = 4..20, showing near-identical shapes.

- **Title (bold 15px, `#1a5276`, top center):** "Arrivals per Hour: Binomial(60, 0.2) vs Poisson(12)".
- **Data:** k values 4–20; Binomial pmf `[0.0029, 0.0082, 0.0187, 0.0361, 0.0598, 0.0864, 0.1102, 0.1252, 0.1278, 0.1180, 0.0990, 0.0759, 0.0534, 0.0345, 0.0206, 0.0114, 0.0058]`; Poisson pmf `[0.0053, 0.0127, 0.0255, 0.0437, 0.0655, 0.0874, 0.1048, 0.1144, 0.1144, 0.1056, 0.0905, 0.0724, 0.0543, 0.0383, 0.0255, 0.0161, 0.0097]`.
- **Layout:** axis origin x=60, plot width 620, baseline y=245, chart height 185, y scale 0–0.14; 17 k-slots, each slot holds two 14px bars side by side (binomial left fill `rgba(42,120,214,0.55)`, Poisson right fill `rgba(0,131,0,0.45)`); k labels 12px `#444` below baseline (every value 4..20).
- **Y-axis:** ticks at 0, 0.05, 0.10 with 11px `#6b7280` labels and light `#e5e9ef` gridlines.
- **Legend (top right, 12px):** blue swatch "Binomial(60, 0.2)", green swatch "Poisson(12)".
- **Annotation (bold 12px green, above k=12 bars):** "both peak near 12: P(12) = 0.128 vs 0.114".
- **Caption (12px `#444`, bottom center):** "same mean np = λ = 12 — slicing minutes into seconds erases the difference".

## Flipping the Question: Waits Give Exponential and Gamma

**Tags:** `worked example` (blue), `waiting time` (orange)

- **Flip it** — instead of "how many per hour?", ask "how many minutes until the next customer?"
- **Exponential wait** — at 12 per hour the next customer takes on average 60 / 12 = 5 minutes
- **Memoryless** — after 10 quiet minutes, the expected extra wait is still 5 minutes, not less
- **Gamma = summed waits** — the wait for the 3rd customer is three exponential waits added, mean 15
- **Shape change** — one wait peaks at 0 minutes; three summed waits peak at (3−1)/0.2 = 10 minutes

*Example (italic):* The barista who needs 3 customers to justify a fresh pot waits Gamma(3, 0.2) minutes — most likely around 10, on average 15.

**Key point:** Exponential is the Poisson process read sideways — time between events instead of events per time — and gamma is just k exponential waits stacked end to end. Same shop, same rate 0.2 per minute.

### Visualization (canvas `c3`, 720×300)

Two smooth density curves on shared axes: the exponential wait for the next customer and the gamma wait for the 3rd, sampled at 2-minute steps.

- **Title (bold 15px, `#1a5276`, top center):** "Minutes of Waiting: Next Customer vs 3rd Customer (rate 0.2/min)".
- **Data (t = 0, 2, 4, ..., 30):** Exponential(0.2) pdf `[0.2000, 0.1341, 0.0899, 0.0602, 0.0404, 0.0271, 0.0181, 0.0122, 0.0082, 0.0055, 0.0037, 0.0025, 0.0016, 0.0011, 0.0007, 0.0005]`; Gamma(3, 0.2) pdf `[0.0000, 0.0107, 0.0288, 0.0434, 0.0517, 0.0541, 0.0523, 0.0477, 0.0417, 0.0354, 0.0293, 0.0238, 0.0190, 0.0149, 0.0116, 0.0089]`.
- **Layout:** axis origin x=60, plot width 620, baseline y=245, chart height 185, x maps 0–30 minutes, y scale 0–0.21; x ticks every 5 minutes with 12px `#444` labels; y-axis unlabeled beyond "density" 11px `#6b7280` rotated.
- **Curves:** exponential in aqua `#199e70` 3px with 4px dots at each sample; gamma in violet `#4a3aa7` 3px with 4px dots.
- **Mean markers:** dashed vertical 1.5px lines (dash 4/3) at t=5 (aqua, label bold 12px "mean 5 min") and t=15 (violet, label bold 12px "mean 15 min"), labels near the top of each line.
- **Annotations (bold 12px):** aqua near (2, 0.2 point): "next customer: most likely right away"; violet near the gamma peak at t=10: "3rd customer: peak at 10 min".
- **Legend (top right, 12px):** aqua swatch "Exponential(0.2) — wait for next", violet swatch "Gamma(3, 0.2) — wait for 3rd".

## Everything Flows to the Bell — But Not Too Early

**Tags:** `where it's used` (blue), `common mistake` (red)

- **Daily total** — 12 open hours × 12 per hour makes Poisson(144) customers per day, sd = 12
- **The bell appears** — Poisson(144) sits on Normal(144, 12) almost exactly; P(144) = 0.033 both ways
- **Why it matters** — totals and averages earn normal-based tools even when raw data never does
- **Too early** — a slow item selling Poisson(2) per day is skewed; its bell puts weight below 0
- **Rule of thumb** — trust the normal stand-in for counts once the mean is roughly 30 or more

*Example (italic):* The daily footfall report (mean 144) can use normal confidence intervals, but the slow pastry selling 2 a day cannot — its Normal(2, 1.41) curve predicts negative sales.

**Common mistake:** Jumping to the normal because "everything is normal in the end". The bell is where the family tree flows for large counts and long sums — for small counts, stay with the actual binomial or Poisson node.

### Visualization (canvas `c4`, 720×300)

Dual-panel chart: Poisson(2) bars with a badly fitting normal curve (left) vs Poisson(144) bars with a near-perfect normal curve (right), split by a dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "When the Bell Fits: Poisson(2) vs Poisson(144)".
- **Left panel (Poisson 2):** axis origin x=55, width 285, baseline y=240, chart height 170, y scale 0–0.30; bars for k=0..8, pmf `[0.1353, 0.2707, 0.2707, 0.1804, 0.0902, 0.0361, 0.0120, 0.0034, 0.0009]`, fill `rgba(42,120,214,0.45)`, k labels 11px `#444`; x mapping runs k=−2..8 so the curve's negative tail is visible; red `#e74c3c` 2.5px normal curve through points (k, pdf) for k=−2..8: `[0.0052, 0.0297, 0.1038, 0.2197, 0.2821, 0.2197, 0.1038, 0.0297, 0.0052]` (Normal mean 2, sd 1.41); red bold 12px annotation with arrow to the k<0 region: "bell spills below zero"; caption 12px `#444` "mean 2: skewed, bell misfits".
- **Right panel (Poisson 144):** axis origin x=395, width 285, same baseline/height, y scale 0–0.04; bars for k = 112, 116, ..., 176, pmf `[0.0008, 0.0020, 0.0044, 0.0083, 0.0140, 0.0207, 0.0273, 0.0319, 0.0332, 0.0310, 0.0260, 0.0196, 0.0134, 0.0082, 0.0046, 0.0023, 0.0011]`, fill `rgba(0,131,0,0.4)`; only k labels 112, 144, 176 shown 11px `#444`; green `#008300` 2.5px normal curve through the same k values with Normal(144, 12) pdf `[0.0009, 0.0022, 0.0045, 0.0083, 0.0137, 0.0202, 0.0266, 0.0314, 0.0332, 0.0314, 0.0266, 0.0202, 0.0137, 0.0083, 0.0045, 0.0022, 0.0009]`; green bold 13px annotation "mean 144: bell fits like a glove"; caption "curve and bars agree to 3 decimals".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
