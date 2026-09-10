# Describing Data

**Page type:** grid page (single flat card navigation grid, 4 columns)
**HTML title tag:** Describing Data

**Subtitle:** How to summarize a pile of numbers honestly — where its center is, how it spreads, what shape it takes, and when the summary hides more than it shows.

## Cards

Each card links to a topic page under `describing-data/`. The card shows a colored uppercase subcategory label (`.card-num`), a numbered title, a one-line description, and 2-4 topic tag pills. All cards sit in one flat `.nav-grid`; the colored labels carry the grouping.

| # | Category | Title | Link | Description | Topic tags |
|---|----------|-------|------|-------------|------------|
| 1 | CENTER & SPREAD | Mean, Median, Mode | [05-describing-data/01-mean-median-mode.md](05-describing-data/01-mean-median-mode.md) | Three different answers to "what's a typical value?" — and why they disagree. | average, typical value, skew |
| 2 | CENTER & SPREAD | Variance & Standard Deviation | [05-describing-data/02-variance-and-standard-deviation.md](05-describing-data/02-variance-and-standard-deviation.md) | A single number for how far values wander from the average. | spread, deviation, original units |
| 3 | CENTER & SPREAD | Percentiles & Quartiles | [05-describing-data/03-percentiles-and-quartiles.md](05-describing-data/03-percentiles-and-quartiles.md) | Ranking values instead of averaging them — where does one point sit among all the others? | ranking, p50 / p95, quartiles |
| 4 | CENTER & SPREAD | Range & IQR | [05-describing-data/04-range-and-iqr.md](05-describing-data/04-range-and-iqr.md) | Measuring spread from the ends versus from the middle half, where one wild value can't distort it. | min / max, middle 50%, spread |
| 5 | CENTER & SPREAD | Weighted Averages | [05-describing-data/05-weighted-averages.md](05-describing-data/05-weighted-averages.md) | When some numbers should count more than others — and the plain average gets it wrong. | weights, group sizes, unequal counts |
| 6 | CENTER & SPREAD | Z-Scores & Standardization | [05-describing-data/06-z-scores-and-standardization.md](05-describing-data/06-z-scores-and-standardization.md) | Re-measuring any value as standard deviations from the mean — one ruler for scores, minutes, and dollars. | standardization, comparing scores, standard deviations |
| 7 | SHAPES OF DATA | The Normal Distribution | [05-describing-data/07-the-normal-distribution.md](05-describing-data/07-the-normal-distribution.md) | The bell curve — why so many measurements pile up around a middle and taper off evenly. | bell curve, 68-95-99.7, symmetry |
| 8 | SHAPES OF DATA | The Binomial Distribution | [05-describing-data/08-the-binomial-distribution.md](05-describing-data/08-the-binomial-distribution.md) | Counting successes out of a fixed number of tries, like heads in twenty coin flips. | yes/no trials, coin flips, counts |
| 9 | SHAPES OF DATA | The Poisson Distribution | [05-describing-data/09-the-poisson-distribution.md](05-describing-data/09-the-poisson-distribution.md) | How many rare events land in a window of time — customer arrivals, typos, machine failures. | rare events, arrivals, rates |
| 10 | SHAPES OF DATA | The Uniform Distribution | [05-describing-data/10-the-uniform-distribution.md](05-describing-data/10-the-uniform-distribution.md) | Every value equally likely — the flat shape behind dice rolls and random number generators. | equal chance, dice, random draws |
| 11 | SHAPES OF DATA | The Log-Normal Distribution | [05-describing-data/11-the-log-normal-distribution.md](05-describing-data/11-the-log-normal-distribution.md) | Data that grows by multiplying, not adding — incomes, file sizes, and other long-right-tail shapes. | multiplicative, long tail, incomes |
| 12 | SHAPES OF DATA | Skew & Heavy Tails | [05-describing-data/12-skew-and-heavy-tails.md](05-describing-data/12-skew-and-heavy-tails.md) | When data leans to one side or throws extreme values far more often than the bell curve predicts. | asymmetry, extreme values, tails |
| 13 | SHAPES OF DATA | Power Laws | [05-describing-data/13-power-laws.md](05-describing-data/13-power-laws.md) | A few giants and countless small entries — city sizes, word counts, and viral posts follow this pattern. | 80/20, scale-free, giants vs many |
| 14 | SHAPES OF DATA | Bimodal Data: Two Populations in One | [05-describing-data/14-bimodal-data-two-populations-in-one.md](05-describing-data/14-bimodal-data-two-populations-in-one.md) | Two humps in one histogram usually means two different groups got mixed into one dataset. | two humps, mixed groups, mixtures |
| 15 | SHAPES OF DATA | The Exponential Distribution | [05-describing-data/15-the-exponential-distribution.md](05-describing-data/15-the-exponential-distribution.md) | How long until the next event when arrivals come at a steady rate — short waits common, long waits rare. | waiting time, memoryless, arrivals |
| 16 | SHAPES OF DATA | The Beta Distribution | [05-describing-data/16-the-beta-distribution.md](05-describing-data/16-the-beta-distribution.md) | A curve over the rate itself — a few clicks give a wide hump of maybes, many clicks a confident spike. | rates, uncertainty, conversion |
| 17 | SHAPES OF DATA | Gamma & Chi-Squared | [05-describing-data/17-gamma-and-chi-squared.md](05-describing-data/17-gamma-and-chi-squared.md) | The wait for the k-th random arrival — stacked exponential gaps, with chi-squared as the famous special case. | waiting time, k-th arrival, test statistics |
| 18 | SHAPES OF DATA | The Distribution Family Tree | [05-describing-data/18-the-distribution-family-tree.md](05-describing-data/18-the-distribution-family-tree.md) | Binomial, Poisson, exponential, gamma, and normal as one story asked five different questions. | connections, limits, big picture |
| 19 | SHAPES OF DATA | Distributions Without Means | [05-describing-data/19-distributions-without-means.md](05-describing-data/19-distributions-without-means.md) | Data so heavy-tailed the average never settles — ten thousand points and the mean is still wild. | Cauchy, heavy tails, unstable mean |
| 20 | SHAPES OF DATA | Zero-Inflated Data | [05-describing-data/20-zero-inflated-data.md](05-describing-data/20-zero-inflated-data.md) | A giant spike of zeros next to an ordinary hill — "did it happen?" and "how much?" are two separate questions. | zero spike, two processes, counts |
| 21 | SHAPES OF DATA | Hazard Rates & the Bathtub Curve | [05-describing-data/21-hazard-rates-and-the-bathtub-curve.md](05-describing-data/21-hazard-rates-and-the-bathtub-curve.md) | Of the units still alive right now, what share fails next — and why many products trace a bathtub shape. | failure rates, survival, bathtub curve |
| 22 | ODD DATA POINTS | Outliers | [05-describing-data/22-outliers.md](05-describing-data/22-outliers.md) | Values that sit far from the rest — sometimes errors, sometimes the most important points you have. | extreme values, error vs signal, detection |
| 23 | ODD DATA POINTS | Robust Statistics | [05-describing-data/23-robust-statistics.md](05-describing-data/23-robust-statistics.md) | Summaries like the median that stay steady even when a few wild values sneak into the data. | median, MAD, stability |
| 24 | ODD DATA POINTS | Winsorizing & Trimming | [05-describing-data/24-winsorizing-and-trimming.md](05-describing-data/24-winsorizing-and-trimming.md) | Two ways to tame extreme values: cap them at a limit, or drop them from the ends entirely. | capping, trimmed mean, percentile caps |
| 25 | ODD DATA POINTS | Extreme Value Theory | [05-describing-data/25-extreme-value-theory.md](05-describing-data/25-extreme-value-theory.md) | The statistics of the worst case — modeling the biggest flood, crash, or spike instead of the typical day. | maxima, tail risk, records |
| 26 | SUMMARIZING HONESTLY | Histograms & Bin Choice | [05-describing-data/26-histograms-and-bin-choice.md](05-describing-data/26-histograms-and-bin-choice.md) | The same data can look smooth, spiky, or two-humped depending on how wide you draw the bars. | bins, shape, visualization |
| 27 | SUMMARIZING HONESTLY | Box Plots | [05-describing-data/27-box-plots.md](05-describing-data/27-box-plots.md) | Five numbers in one picture — median, quartiles, and whiskers that flag the unusual points. | five-number summary, whiskers, comparison |
| 28 | SUMMARIZING HONESTLY | When the Mean Lies | [05-describing-data/28-when-the-mean-lies.md](05-describing-data/28-when-the-mean-lies.md) | Cases where the average is technically correct but describes almost nobody in the data. | skewed data, misleading average, income data |
| 29 | SUMMARIZING HONESTLY | Aggregation Hides Detail | [05-describing-data/29-aggregation-hides-detail.md](05-describing-data/29-aggregation-hides-detail.md) | Rolling data up into one number can erase the very pattern you needed to see. | roll-ups, lost patterns, segments |
| 30 | SUMMARIZING HONESTLY | Kernel Density Estimation | [05-describing-data/30-kernel-density-estimation.md](05-describing-data/30-kernel-density-estimation.md) | A smooth bump on every data point, added up — a histogram's shape without arbitrary bin edges. | smooth density, no bins, visualization |

## Regeneration instructions

To rebuild the HTML from this spec:

- **Template:** nav-grid style. Single page: h1, `.subtitle` paragraph, then one flat `.nav-grid` of `.nav-card` anchors (no h2 section headings).
- **Layout:** `.nav-grid` is CSS grid, `repeat(4, 1fr)`, 16px gap, 15px top margin; responsive: 3 columns below 1400px, 2 below 1100px, 1 below 600px.
- **Links:** the tables above link to `.md` versions for markdown navigation; in the regenerated HTML each card's `href` is the same path with an `.html` extension instead.
- **Card structure:** `<a class="nav-card" href="...">` containing `<div class="card-num">SUBCATEGORY</div>`, `<h3>N. Topic Title</h3>` (unpadded index number matching the 2-digit zero-padded file number), `<p>description</p>`, then `<div class="topics">` of `<span class="topic-tag">` pills.
- **Category label colors** (applied by a small script mapping `.card-num` text to color): CENTER & SPREAD `#2980b9`; SHAPES OF DATA `#27ae60`; ODD DATA POINTS `#e67e22`; SUMMARIZING HONESTLY `#8e44ad`. Default `.card-num` CSS color is `#2980b9`, 0.75em bold.
- **Card style:** background `#ffffff`, border `1px solid #d8d8d8`, radius 10px, padding 20px, shadow `0 2px 4px rgba(0,0,0,0.05)`; hover: border `#2980b9`, `translateY(-2px)`. h3 `#1a3a4a` 1em; description `#555` 0.85em. Topic tags: background `#f0f0f0`, border `1px solid #ccc`, radius 4px, padding 2px 6px, 0.7em `#666`, in a flex-wrap row with 4px gap.
- **Page style:** body system sans-serif, background `#f5f5f0`, text `#2a2a2a`, padding 40px, line-height 1.6; h1 1.8em `#2980b9`; subtitle `#666` 1.05em. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`. No canvases on this page; detail pages use `window.devicePixelRatio` scaling for canvases.
