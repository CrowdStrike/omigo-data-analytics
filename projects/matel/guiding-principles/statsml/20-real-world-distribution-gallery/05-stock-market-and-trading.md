# Stock Market & Trading — Distribution Patterns

**Page type:** detail page (three-column obj-table layout: text left ~38%, primary canvas middle ~31%, insight canvas right ~31%; one table per section; centered page subtitle under h1)
**HTML title tag:** Stock Market & Trading — Distribution Patterns

**Subtitle (`.page-sub`):** Cases where a careful, sensible-looking analysis of market data returns a confidently wrong answer — and the histogram shows you exactly why. No finance or statistics jargon assumed. Each row takes one part of the histogram — the middle, the gaps, the edge, the tail, the outline — and shows how that part can mislead you. All charts are simulations, so you can watch the mechanism produce the shape instead of taking anyone's word for it.

## An Average That Almost Nobody Actually Gets

**Pitfall label (color `#795548`):** THE MIDDLE LIES

**Belief callout:** **What looks obvious:** A bet that gains 50% on heads and loses 40% on tails has an average outcome of +5% per round. Positive average means repeat it often and you come out ahead.

The arithmetic is correct and the conclusion is wrong. Run it 30 times and most players are left with a fraction of what they started with. The reason is that gains and losses *multiply* rather than add: a 50% gain followed by a 40% loss leaves you at 0.9×, down 10%, no matter which came first. The "+5% average" is being propped up by a handful of extraordinary winners far off to the right, and it describes a place on the chart where hardly any real outcomes sit.

- The chart is drawn on a squeezed scale where each step right means 10× more money — the only way to fit all outcomes on one page
- The average and the middle-most outcome land in completely different places; the average sits out where the bars are almost invisible
- Most players end below where they started, yet the average is well above it — both facts are true at once
- The top 1% of players hold more than half of all the money in the game

*Term note:* If you want to look it up: log-normal distribution, and the gap between arithmetic and geometric mean.

### Visualization (canvas `canvas1`, 420×360)

Histogram (shared `drawHistogram` helper) of final wealth on a log10 scale.

- **Title:** "Where 20,000 Players Ended Up"; **subtitle (`#7f8c8d` 10px):** "each step right = 10× more money".
- **Data generation:** seeded mulberry32(42); 20,000 players, start $100, 30 rounds of ×1.5 (heads) or ×0.6 (tails) with p=0.5; plot log10(final wealth).
- **Bins/axes:** 46 bins over range [-3, 5] (log10 dollars); 4 x ticks formatted as money: "0.1¢", "$0.XX", "$N", "$Nk", "$Nm"; x-axis label "Final amount (squeezed scale)".
- **Colors:** bar fill `rgba(44,62,80,0.4)`, stroke `#2c3e50`; density line `#1a5276` 2px with SE band `rgba(230,126,34,0.22)`; axes `#333`.
- **Overlay:** dashed gray `rgba(127,140,141,0.9)` vertical at $100 labeled bold 9px "STARTED"; solid green `#27ae60` 2.5px vertical at the median labeled "middle-most" / "$NN" (right-aligned); solid red `#e74c3c` 2.5px vertical at the mean labeled "AVERAGE" / "$NNN" (left-aligned); red bracket along the bottom from the left edge to the start line labeled bold `#c0392b` "NN% ended below their starting point" (computed, most players).

### Visualization (canvas `canvas1b`, 400×360)

Spaghetti path chart: 220 players' wealth trajectories on a log scale, with promised vs typical trajectories.

- **Title:** "The Same Game, Player by Player"; **subtitle:** "220 players followed for 30 rounds".
- **Zones:** above the $100 start line shaded `rgba(39,174,96,0.06)`; below shaded `rgba(231,76,60,0.06)`; dashed (4/3) gray start line at log10 = 2.
- **Paths (colored by final log10 wealth):** ≥3 → `rgba(39,174,96,0.55)` 1.6px; ≥2 → `rgba(41,128,185,0.35)` 0.8px; else `rgba(192,57,43,0.28)` 0.8px; y clamped to [-3, 5].
- **Reference curves:** solid red `#e74c3c` 3px "average promise" curve 100·1.05^t, labeled three-line bold 10px right of plot: "what the" / "average" / "promises"; dashed (7/4) green `#27ae60` 3px typical curve 100·0.9^(t/2), labeled `#1e8449`: "what a typical" / "player gets".
- **Callout (bold 10px `#1a5276`, centered below axis):** "Top 1% of players hold NN% of all the money" (computed, >50%).
- **Axes:** x ticks 0, 10, 20, 30 with label "Round"; y tick labels "$10k", "$100", "$1", "1¢"; axes `#333`; margins top 38, right 58, bottom 46, left 44.

## The Prices That Were Missing — And What They Cost

**Pitfall label (color `#2980b9`):** THE GAPS LIE

**Belief callout:** **What looks obvious:** To find something wrong in pricing data, study the prices that are there. Look at the tall bars, the outliers, the unusual values.

In the early 1990s US shares were priced in eighths of a dollar, so quotes could only land on multiples of 12.5¢. Two researchers did something almost embarrassingly simple: they counted how often each eighth was used. For many of the most heavily traded shares, **half the available prices were nearly unused**. Quotes clustered on the even eighths and skipped the odd ones in between. Nothing was odd about the bars that were present — the entire finding lived in the bars that were absent.

- Skipping every other price doubles the smallest possible gap between the buying and selling price — a cost paid quietly on every single trade
- No summary number would have caught this. An average price, a spread, a standard deviation: all perfectly unremarkable
- It led to a government investigation, a settlement of roughly a billion dollars, and rule changes that eventually moved US markets to decimals
- The best part: once the finding was published, the missing prices started being used within days. Publishing the chart changed the data it described

*Term note:* If you want to look it up: Christie & Schultz (1994), odd-eighth avoidance.

### Visualization (canvas `canvas2`, 420×360)

Histogram (shared `drawHistogram` helper, density off) of quoted price endings on eighths of a dollar.

- **Title:** "How Often Each Price Ending Was Used"; **subtitle:** "prices could only land on eighths of a dollar".
- **Data generation:** 6,000 quotes sampled from weights per eighth `[0.30, 0.015, 0.22, 0.012, 0.20, 0.013, 0.19, 0.020]` (even eighths heavily favoured); values placed at k/8.
- **Bins/axes:** 40 bins over [0, 1]; 4 x ticks with 2-decimal labels; x-axis label "Price ending (fraction of a dollar)".
- **Bar coloring:** only bins landing on eighths (every 5th bin) are visible — even eighths `rgba(41,128,185,0.55)`, odd eighths `rgba(231,76,60,0.55)`; all other bins transparent.
- **Overlay:** for each odd eighth (1/8, 3/8, 5/8, 7/8) a dotted (2/2) red drop line with bold 9px `#c0392b` label "k/8"; centered bold 11px `#c0392b` headline "These four prices barely existed"; bold 9px `#1a5276` labels "0/8", "2/8", "4/8", "6/8" above the tall even bars.

### Visualization (canvas `canvas2b`, 400×360)

Two-row before/after slot chart: share of quotes at each price ending before the study vs days after publication.

- **Title:** "Publishing the Chart Changed the Data"; **subtitle:** "share of quotes at each price ending".
- **Rows:** "Before / the study" using the even-favoured weights above; "Days after / publication" using `[0.155, 0.11, 0.14, 0.115, 0.145, 0.10, 0.135, 0.10]` (nearly even).
- **Cells:** 8 slots per row, outlined `rgba(149,165,166,0.5)` boxes (max height 56px) filled from the bottom proportional to share — odd eighths `rgba(231,76,60,0.6)`, even eighths `rgba(41,128,185,0.6)`; percentage labels bold 9px `#333` below each slot; "k/8" labels above the top row only.
- **Arrow:** orange `#e67e22` 2px downward arrow between the rows.
- **Legend:** blue swatch "even eighths", red swatch "odd eighths" (10px `#555`).
- **Bottom callout (bold 10px `#c0392b`, centered):** "No rule changed. Only the fact that someone was looking."
- **Layout:** margins top 38, right 16, bottom 52, left 74.

## A Cliff Where There Should Be a Slope

**Pitfall label (color `#27ae60`):** THE EDGE LIES

**Belief callout:** **What looks obvious:** A sharp spike or a vertical drop in the data is a discovery. Find one, and you have found something real about the world.

Companies report profits; analysts publish forecasts beforehand. Chart "actual minus forecast" and you do not get a smooth hill. You get a **cliff**: strikingly few companies miss by a hair, and a pile-up of companies beating it by a penny or two. The peak is not at zero — it sits just past it. Smooth shapes come from many small independent causes, so a vertical edge suggests somebody was steering toward a target. But here is the twist that makes this the most useful case on the page: other researchers argued a good part of that cliff is an *artifact* of dividing every company's profit by its size to make firms comparable, plus which firms made it into the data at all. That debate is still open.

- The lopsidedness is the finding, not the sharpness. A symmetric spike would be evidence of nothing
- The gap on the left and the excess on the right are roughly the same size — consistent with firms moving across the line rather than appearing from nowhere
- Steering does not require faked accounts: shifting when a cost is recorded, or guiding forecasts down so the bar is easy to clear, both produce this shape
- Before announcing an edge you found, check whether your own scaling and filtering created it

*Term note:* If you want to look it up: Burgstahler & Dichev (1997), challenged by Durtschi & Easton (2005).

### Visualization (canvas `canvas3`, 420×360)

Histogram (shared `drawHistogram` helper, density off) of earnings surprise in cents with a manufactured cliff at zero.

- **Title:** "Profit Versus Forecast, in Pennies per Share"; **subtitle:** "each bar is one penny of surprise".
- **Data generation:** 9,000 firms; raw surprise = round(randn()·5.5) cents; "management" step: firms in [-3, -1] cents are moved with probability 0.62 to +1 (60%), else 0 or +2 — creating a deficit of small misses and an excess of small beats.
- **Bins/axes:** 36 bins over [-18, 18]; 6 integer x ticks; x-axis label "Missed forecast  ←   0   →  Beat forecast".
- **Bar coloring:** bins centered in (-3.5, -0.5) red `rgba(231,76,60,0.6)` (the deficit); bins in (-0.5, 2.5) green `rgba(39,174,96,0.6)` (the excess); all others `rgba(41,128,185,0.35)`; strokes `#2980b9`; **no density overlay** (`density:false` — the smoothed line + SE band would round off the vertical cliff at zero that is the finding).
- **Overlay:** dashed (3/3) `#2c3e50` 2px vertical at 0 labeled bold 9px two-line "exactly" / "on target"; bold 10px labels — red `#c0392b` right-aligned "too few" / "small misses" left of zero, green `#1e8449` left-aligned "too many" / "small beats" right of zero; orange `#e67e22` arrow pointing to the tallest bar with bold `#d35400` label "peak is at +N¢," / "not at zero".

### Visualization (canvas `canvas3b`, 400×360)

Outline-comparison chart: the surprise distribution with and without steering.

- **Title:** "The Shape With and Without Steering"; **subtitle:** "same firms, one group nudged across the line".
- **Series:** unmanaged (raw) histogram outline as a gray filled silhouette `rgba(149,165,166,0.22)` with dashed (6/4) `rgba(127,140,141,0.95)` 2px outline; actual (managed) counts as a solid `#1a5276` 3px line; x window [-12, 12] cents.
- **Difference shading:** between the two curves — red `rgba(231,76,60,0.4)` over the deficit zone (-3.5 to -0.5) and green `rgba(39,174,96,0.4)` over the excess zone (-0.5 to 2.5).
- **Zero line:** dashed (3/3) `#2c3e50` 2px vertical.
- **Migration arrow:** orange `#e67e22` 2.5px arrow from -2¢ to +1.2¢ labeled bold `#d35400` "N% of all firms moved this way" (computed).
- **Legend:** dashed gray line sample "if nobody steered (smooth)"; solid blue line sample "what gets reported (cliff)".
- **Bottom caution (italic 10px `#c0392b`, centered):** "Caution: dividing by company size can fake this too".
- **Axes:** x ticks -12 to +12 in steps of 4 (signed labels); y ticks at quarters; axes `#333`; margins top 38, right 16, bottom 60, left 44.

## The Number That Never Settles, However Much Data You Add

**Pitfall label (color `#e74c3c`):** THE TAIL LIES

**Belief callout:** **What looks obvious:** More data gives a steadier estimate. Collect enough and any measurement stops wobbling and homes in on the truth.

Take the standard measure of "how extreme the extremes are." Compute it on one year of daily price moves, then two, then ten. It does not settle down — it keeps climbing, and it lurches upward every time a new record-breaking day arrives. This is not noisy data or a bug in your code. Price moves have edges so heavy that this quantity **has no true value to converge to**; the calculation depends on a fourth-power term that is dominated by the single worst day in the sample. Your program returns a precise-looking number that mostly encodes how long your dataset was.

- Run the same calculation on many separate five-year stretches of the same process and the answers scatter wildly — the chart of those answers has no meaningful centre
- Compare with well-behaved bell-curve data, where the same estimate flattens out quickly and stays put
- The giveaway: one observation can move the result by more than all the others combined
- Practical habit — plot your estimate against sample size. If the line is still moving at the right edge, do not quote the number

*Term note:* If you want to look it up: kurtosis, heavy tails, and infinite fourth moments.

### Visualization (canvas `canvas4`, 420×360)

Histogram (shared `drawHistogram` helper) of kurtosis estimates from 600 independent five-year samples.

- **Title:** "Same Process, 600 Separate Five-Year Studies"; **subtitle:** "each study reports one \"how extreme\" score".
- **Data generation:** heavy-tailed draws from a t-distribution with 3 degrees of freedom (z / sqrt((a²+b²+d²)/3) with standard normals); 600 samples of 1,250 observations each; kurtosis (`tailScore`: m4/m2²) computed per sample.
- **Bins/axes:** 40 bins over [0, 100]; 5 integer x ticks; x-axis label "Tail-heaviness score reported"; note in red italic 9px top-right when values exceed the axis: "N.N% above axis (in end bars)".
- **Colors:** bar fill `rgba(142,68,173,0.35)`, stroke `#8e44ad`; density line `#1a5276` with SE band.
- **Overlay:** dashed green `#27ae60` vertical at 3 labeled `#1e8449` bold 9px "a bell curve" / "would score 3"; centered bold 10px `#6c3483` headline "Answers ranged from N to NNN — all from identical data" (computed min/max); `#8e44ad` 10px "middle-most answer: NN" (computed median); right-aligned italic 9px `#c0392b` "no meaningful centre → do not quote a single number".

### Visualization (canvas `canvas4b`, 400×360)

Running-estimate trace chart: kurtosis vs sample size (log x) for market-like vs bell-curve data.

- **Title:** "Watching the Estimate as Data Piles Up"; **subtitle:** "purple = market-like data, green = bell-curve data".
- **Reference:** green band `rgba(39,174,96,0.10)` between scores 1.5 and 4.5; dashed (5/3) `#27ae60` line at 3.
- **Traces:** running kurtosis computed as data accumulates from n=30 to n=6,000 (log10 x scale); 3 bell-curve traces `rgba(39,174,96,0.75)` 1.6px (settle near 3); 5 heavy-tailed traces in purples `rgba(142,68,173,0.9)`, `rgba(155,89,182,0.85)`, `rgba(108,52,131,0.85)`, `rgba(142,68,173,0.6)`, `rgba(125,60,152,0.7)` 2px (keep climbing and jumping); y range 0–60.
- **Annotations:** bold 10px `#1e8449` "settles down and stays put" near the green line; bold 10px `#6c3483` right-aligned "still climbing at the right edge"; 9px `#8e44ad` "every jump = one new worst day".
- **Axes:** x ticks "30", "100", "1 yr" (250), "5 yr" (1250), "24 yr" (6000) with label "Days of data collected"; y ticks 0–60 in 15s; axes `#333`; margins top 38, right 16, bottom 52, left 46.
- **Bottom callout (bold 10px `#c0392b`, centered):** "More data did not help. It moved the answer."

## The Bell Curve That Appears When You Step Back

**Pitfall label (color `#8e44ad`):** THE OUTLINE LIES

**Belief callout:** **What looks obvious:** A dataset has a shape. Once you have plotted it and named the shape, that is a fact about the thing you measured.

Same shares, same year, one dataset. Measure the change minute by minute and you get a sharp spike with wild outliers, nothing like a bell curve. Measure the change month by month and you get a fairly ordinary bell curve. Nothing about the shares changed — only **how far apart you took your readings**. There is a sharper version too: stop using the clock. Measure the change over every 500 trades instead of every 5 minutes and much of the strangeness dissolves — consistent with activity arriving in bursts, which makes "an hour" a wildly inconsistent unit of market activity.

- "This data is heavy-tailed" is an incomplete sentence — heavy-tailed measured over what interval?
- Adding up many readings averages the calm and busy stretches together, and that mixing is what pulls the shape back toward a bell curve
- The tail score falls along a predictable path as the interval widens, so this is a property of the measuring, not a quirk of one dataset
- Before naming a shape, try two or three intervals — and ask whether the clock is even the right ruler for what you are measuring

*Term note:* If you want to look it up: aggregational Gaussianity; trade time versus clock time.

### Visualization (canvas `canvas5`, 420×360)

Histogram (shared `drawHistogram` helper) of minute-by-minute moves with a matched bell-curve overlay.

- **Title:** "Minute-by-Minute Moves"; **subtitle:** "sharp spike, wild edges — tail score N.N" (computed kurtosis of the sample).
- **Data generation:** one-minute move = randn(), tripled with probability 0.10 (volatility mixture); 200,000 base draws, first 20,000 plotted.
- **Bins/axes:** 56 bins over [-6, 6]; 4 integer x ticks; x-axis label "Move over one minute".
- **Colors:** bar fill `rgba(231,76,60,0.35)`, stroke `#e74c3c`; density line `#1a5276` with SE band.
- **Overlay:** dashed (6/4) green `#27ae60` 2px matched-standard-deviation bell curve scaled to 60% of max bar height, labeled bold 9px `#1e8449` "bell curve of the same width"; centered bold 10px `#c0392b` headline "taller in the middle, fatter at the edges".

### Visualization (canvas `canvas5b`, 400×360)

Descending curve of tail score vs reading interval, with mini histogram silhouettes at each step.

- **Title:** "Widen the Interval, Watch the Shape Change"; **subtitle:** "identical data throughout — only the reading interval differs".
- **Data:** the same 200,000 one-minute moves aggregated over n = 1, 5, 15, 60, 390 minutes (labels "1 min", "5 min", "15 min", "1 hr", "1 day"; sums rescaled by 1/√n); kurtosis computed per aggregation level (falls from ~high toward 3).
- **Reference:** green band `rgba(39,174,96,0.10)` below score 3.2; dashed (5/3) `#27ae60` line at 3 labeled bold 9px `#1e8449` "a bell curve sits here (score 3)".
- **Curve:** red `#e74c3c` 3px descending line through the 5 points with a vertical gradient fill beneath (from `rgba(231,76,60,0.28)` at top to `rgba(39,174,96,0.10)` at bottom); y range 2.5–9.
- **Mini silhouettes:** at each point, a 34×20px white inset box (gray border) with a 22-bin mini-histogram of that aggregation level — red for 1 min, green for 1 day, blue for the middle steps; red `#c0392b` 4.5px dot on the curve with the score printed bold 10px `#333` below it.
- **Callout (bold 10px `#c0392b`):** "same data, N.N → N.N" (first and last scores).
- **Axes:** x tick labels the interval names with label "Gap between readings"; y ticks 3, 5, 7, 9; axes `#333`; margins top 38, right 18, bottom 56, left 46.
- **Bottom callout (bold 10px `#1a5276`, centered):** ""Heavy-tailed" is only half a sentence without an interval".

## Regeneration instructions

- **Layout:** h1 + centered `.page-sub` subtitle paragraph, then one `<table class="obj-table">` per section, each a single `<tr>` with three `<td>`s: text cell (38%) holding `<span class="pitfall-label">`, `<h3>`, a `.belief` callout div, paragraph, `<ul>`, and a `.termname` div; middle cell (31%, centered) with the primary canvas (width=420, height=360); right cell (31%, centered) with the insight canvas (width=400, height=360).
- **Page CSS:** body system sans-serif (-apple-system stack), margin 20px, background `#f9f9f9`, text `#333`; h1 `#1a5276` centered; `.page-sub` centered `#666` 0.95em max-width 900px; table cells `1px solid #2980b9`, 12px padding; h3 `#1a5276` 1.0em weight 700; p/li 14px; `.pitfall-label` inline-block bold 0.72em uppercase letter-spacing 0.5px; `.belief` background `#fdf3f2`, left border `3px solid #e74c3c`, padding 8px 12px, 13.5px, with `b` in `#c0392b`; `.termname` `#7f8c8d` 12.5px italic; `canvas { width: 100%; height: auto; }`.
- **Pitfall label colors:** assigned in document order from the cycling array `["#795548", "#2980b9", "#27ae60", "#e74c3c", "#8e44ad", "#e67e22", "#16a085", "#d35400", "#c0392b", "#1abc9c"]` via a small script setting `style.color` on each `.pitfall-label`.
- **Charts:** seeded mulberry32(42) RNG shared across all cards, with Box-Muller `randn()` and a `tailScore` (kurtosis m4/m2²) helper. Shared `setupCanvas(id)` scales by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) and paints a white background. Shared `drawHistogram(canvasId, data, options)` helper: title bold 13px `#1a5276` + optional gray 10px subtitle, bars (optional per-bar `barColorFn`), Gaussian-kernel smoothed density line `#1a5276` 2px with 95% SE band `rgba(230,126,34,0.22)` (sigma 1.5, effective N clamped [30, 200], disabled per chart via `density:false` — the price-endings chart `canvas2` and the earnings-cliff chart `canvas3` render without it), caller-supplied `overlay` callback drawn in data coordinates, custom tick formatting, axes `#333`, tick labels `#555` 10px, and honest off-axis reporting in red italic when data falls outside the plotted range. Right-column charts are custom-drawn with the same styling.
- **Palette:** primary blue `#1a5276`, accent blue `#2980b9`, green `#27ae60` / `#1e8449`, red `#e74c3c` / `#c0392b`, orange `#e67e22` / `#d35400`, purple `#8e44ad` / `#6c3483`, dark slate `#2c3e50`, gray `#7f8c8d`.
- No nav bar, no back/home links. In regenerated HTML any card links would use `.html` extensions.
