# Feature Engineering — From Raw Signals to Model-Ready Features

**Page type:** detail page (compact card-sections: one h2 per section, two-column layout table with tag pills + labeled bullets left ~45%, canvas right ~55%)
**HTML title tag:** Feature Engineering — From Raw Signals to Model-Ready Features

**Subtitle:** Every engineered feature encodes an assumption — ratios, windows, encodings, and counters that turn noisy raw signals into predictive power, and the leakage, skew, and gaming traps hiding inside each

## Ratios Beat Raw Counts

Tags: technique (blue), failure mode (red), defense (green)

- **Rates over counts** — ratios remove the exposure confound
- **Tiny denominators** — 1 click / 2 impressions = 50% outranks proven 8%
- **Bayesian smoothing** — (clicks + α) / (impressions + α + β), α/(α+β) = global rate
- **Prior strength** — 10–100 virtual impressions; fit Beta prior empirically
- **Segment priors** — smooth within segment, not the whole catalog

*Example:* Example: ad systems rank on smoothed CTR; one 5-star review cannot outrank 500 averaging 4.6.

**Failure mode:** Without a prior, the smallest-denominator rows get the most extreme feature values.

### Visualization (canvas `c1`, 720×300)

Scatter + curve on a log-x axis. Title (bold 14px `#1a5276`, left-aligned): "Raw CTR Explodes at Tiny Denominators; Smoothed CTR Does Not". Padding top 34, bottom 40, left 60, right 150; L-shaped gray (`#999`) axes; x = impressions on log scale (2 to ~10,000), y = CTR, max 0.55.

- **Raw CTR points (red `#e74c3c` 4px dots), [impressions, CTR]:** `[2,0.50], [3,0.00], [4,0.25], [5,0.40], [8,0.00], [10,0.30], [15,0.13], [20,0.00], [30,0.17], [50,0.10], [80,0.09], [150,0.07], [300,0.06], [600,0.045], [1200,0.055], [2500,0.05], [5000,0.048], [9000,0.051]`.
- **Smoothed curve (blue `#1a5276` 3px):** through the same x values with `(raw*n + 0.05*20) / (n + 20)` — prior of 20 virtual impressions at global rate 0.05.
- **Reference line:** dashed green (`#27ae60`, 1.5px, dash 5/4) horizontal at 0.05 (true rate).
- **Annotations:** bold red 11px "1 click / 2 impressions = 50%" beside the leftmost point; bold blue 11px "prior pulls small samples to global rate".
- **Axis labels:** `#444` 12px "impressions (log scale)" bottom center; rotated "CTR" on the left.
- **Legend (x = w-140):** red dot "Raw CTR"; blue line "Smoothed"; dashed green line "True rate 5%".

## Time-Windowed Aggregations & Decay

Tags: technique (blue), rule of thumb (blue), failure mode (red)

- **Trend detector** — short-window / long-window ratio of 1d/7d/28d aggregates
- **Match half-life** — minutes for fraud, 7–28d interest, 28–90d churn, 52w retail
- **Exponential decay** — smooth alternative to hard window cutoffs
- **Velocity features** — transactions per card in 10 min / 1 h / 24 h
- **Classic leakage** — calendar week instead of 7 days before each row

*Example:* Example: M5 demand-forecasting winners stacked 7/14/28/56-day lags and rolling means.

**Rule of thumb:** Every window must end strictly before the label timestamp, with time-based validation splits.

### Visualization (canvas `c2`, 720×300)

Timeline diagram of aggregation windows. Title (bold 14px `#1a5276`, left-aligned): "Windows Must End Strictly Before the Label Time".

- **Timeline:** horizontal gray axis at y=245 with arrowhead, labeled "time" at right.
- **Label time:** dashed blue (`#1a5276`, 2px, dash 4/4) vertical line at 70% width, labeled bold blue 12px "label time"; region to its right shaded `rgba(231,76,60,0.12)` labeled bold red 12px "future = leakage".
- **Window bars (20px tall, labeled at left):** "28d agg" (`rgba(26,82,118,0.35)`, y=75) spanning 28 day-units up to the label line; "7d agg" (`rgba(26,82,118,0.55)`, y=115) spanning 7 day-units; "1d agg" (solid `#1a5276`, y=155) spanning 1 day-unit; "bad win" (red `#e74c3c`, y=195) spanning from 4 day-units before to 3 day-units after the label line, annotated bold red 11px "crosses the label boundary".
- **Annotations:** bold green 12px "valid: all windows end before label" top-left; bottom caption bold blue 11px "short-window / long-window ratio = trend detector".

## Per-User Baselines & RFM

Tags: signal (blue), technique (blue), best practice (green)

- **RFM triple** — recency/frequency/monetary, quantile-scored; still top churn/LTV features
- **Per-user z-score** — (x − user_mean) / user_std; need ~20–30 events first
- **Cold-start** — use segment/global prior plus is_new indicator
- **Fraud** — $500 on a $50-max card outranks $5,000 corporate
- **Item-side** — watch time by video length, reviews by category mean

*Example:* Example: skip-rate z-scored per user predicts churn where raw skip counts do not.

**Key point:** Absolute features rank users; user-relative features rank behavior — the signal is deviation from one's own baseline.

### Visualization (canvas `c3`, 720×300)

Two-panel dot-strip diagram with connecting arrow. Title (bold 14px `#1a5276`, left-aligned): "Same $200 Transaction: Absolute Scale vs. z-Score Against Own Baseline".

- **Left panel (absolute dollar scale, x from $0 to $900, axis at y=240):** two horizontal strips of `rgba(26,82,118,0.35)` 4px dots — User A history (heavy spender) `[280, 340, 390, 430, 470, 500, 520, 560, 610, 660, 720, 790]` at y=110 labeled bold blue "User A: avg $480/txn"; User B history (light spender) `[6, 10, 13, 16, 18, 20, 22, 25, 28, 32, 38, 44]` at y=190 labeled "User B: avg $22/txn". An orange `#e67e22` diamond marker at $200 on both strips, each labeled bold orange "$200". Axis labels "$0", "$900", "absolute dollar scale".
- **Connecting arrow:** blue 2px arrow between panels labeled 10px "z-score".
- **Right panel (z-score axis from −3σ to +7σ, ticks at −2/0/2/4/6σ, axis at y=165):** shaded green `rgba(39,174,96,0.10)` "normal zone" band from −2σ to +2σ; green `#27ae60` 7px dot at −1.9σ labeled bold "A: -1.9σ"; red `#e74c3c` 7px dot at +6σ labeled bold "B: +6σ FLAG"; caption `#444` 11px "z-scored vs own trailing baseline".
- **Bottom caption (bold blue 12px, centered):** "Same raw value, opposite meaning once normalized per user".

## Binning & Discretization

Tags: technique (blue), rule of thumb (blue), failure mode (red)

- **Step-function fit** — each bin gets its own weight; tames outliers
- **Bin count** — 10–20 quantile bins; denser where data is dense
- **WOE binning** — credit-scorecard standard; each bin's risk is inspectable
- **Built-in** — LightGBM bins internally, default 255 bins
- **Edge artifacts** — $999 vs $1,001 land in different bins; use splines/monotonic constraints if it hurts

*Example:* Example: equal-width bins on skewed spend put 95% of rows in one bin — always quantile.

**Failure mode:** Quantile edges computed on train+test leak — fit edges on training only and freeze for serving.

### Visualization (canvas `c4`, 720×300)

Smooth curve vs step-function overlay. Title (bold 14px `#1a5276`, left-aligned): "Quantile Bins: Step-Function Fit to a Non-Linear Shape". Padding top 34, bottom 40, left 60, right 150; L-shaped gray axes.

- **True effect:** smooth green `#27ae60` 2.5px curve `f(x) = x / (x + 0.25)` over x in [0,1].
- **Binned fit:** blue `#1a5276` 3px horizontal step segments, one per bin, at `f(bin midpoint)`; quantile bin edges `[0, 0.05, 0.11, 0.19, 0.30, 0.45, 0.65, 1.0]` (denser where data is dense), edge ticks in `rgba(26,82,118,0.35)` on the x-axis.
- **Edge artifact:** two orange `#e67e22` 4px dots straddling the edge at x=0.11 on adjacent steps, annotated bold orange 12px two lines "edge artifact: near-equal x," / "different bin, different prediction".
- **X-axis label:** `#444` 12px "raw feature x (bin edges denser where data is dense)".
- **Legend (x = w-140):** green line "True effect"; blue line "Binned fit".

## Log Transforms & Scaling

Tags: technique (blue), rule of thumb (blue), defense (green)

- **Who needs it** — linear, kNN, SVM, neural nets; GBDTs are monotone-invariant
- **When** — max/median ratio over ~100; log1p handles zeros
- **Where used** — log(spend) for LTV/revenue, log dwell time, log house prices
- **Skew trap** — unfrozen train mean/std constants cause online/offline skew
- **Negatives** — returns/refunds break plain log; use signed log or split features

*Example:* Example: spend spanning $10–$50,000 becomes near-symmetric after log1p.

**Fix:** Fit scaling constants on training only, version them with the model, apply identically at serving.

### Visualization (canvas `c5`, 720×300)

Side-by-side histograms with transform arrow. Title (bold 14px `#1a5276`, left-aligned): "Spend per Customer: Raw vs. log1p".

- **Left histogram (raw, right-skewed):** 18 bins `[88, 42, 22, 12, 7, 4, 3, 2, 1, 1, 1, 0, 0, 1, 0, 0, 0, 1]` (scale max 90), red `#e74c3c` at 55% alpha, zero bins skipped. Annotations: bold red 12px "raw: skewed, outliers set the scale"; orange 11px "lone whales out here" near the tail. X labels "$0", "$50k".
- **Arrow:** blue 2px arrow between panels labeled 10px "log1p".
- **Right histogram (log1p, near-symmetric):** 14 bins `[2, 5, 12, 25, 42, 58, 66, 60, 46, 30, 16, 8, 3, 1]` (scale max 70), fill `rgba(26,82,118,0.35)`. Annotation: bold green 12px "log1p: symmetric, model-ready". X label "log(1 + $)".
- **Bottom caption (bold blue 12px, centered):** "4 orders of magnitude compressed into a usable range".

## Target Encoding for High Cardinality

Tags: technique (blue), failure mode (red), defense (green)

- **Dense encoding** — one column replaces 50,000 one-hot columns
- **The leak** — merchant seen 3 times encodes its own label
- **Symptom** — huge train/validation gap concentrated in rare categories
- **Fix** — out-of-fold encoding plus smoothing, m ≈ 10–100 virtual samples
- **Time series** — strict time cutoff; CatBoost's ordered statistics do this natively

*Example:* Example: naive encoding trains at 0.94 AUC, validates at 0.68; out-of-fold gives honest 0.81/0.79.

**Trap:** Any target-derived feature must be computed as of each row's timestamp, or it is the answer key.

### Visualization (canvas `c6`, 720×300)

Grouped bar chart of AUC. Title (bold 14px `#1a5276`, left-aligned): "Target Encoding: Naive Leaks the Label, Out-of-Fold Holds Up". Padding top 40, bottom 55, left 60, right 30; y scale from base 0.5 to 1.0; rotated y label "AUC".

- **Groups (bar width 80):** "Naive encoding" — train 0.94 (`rgba(26,82,118,0.35)`), val 0.68 (red `#e74c3c`); "Out-of-fold encoding" — train 0.81 (`rgba(26,82,118,0.35)`), val 0.79 (green `#27ae60`). Bold 12px value labels above bars; "train"/"val" labels and bold blue 13px group labels below.
- **Gap bracket:** orange `#e67e22` 2px bracket spanning 0.94 down to 0.68 on the naive group, labeled bold orange 12px two lines "0.26 gap =" / "memorized labels".

## Frequency Encoding & the Cold-Start Tail

Tags: technique (blue), signal (blue), failure mode (red)

- **Leak-free** — no label involved; popularity itself is signal
- **Rarity risk** — rare user agents and merchants correlate with fraud
- **Power-law tail** — collapse categories under ~10–50 occurrences into "rare" bucket
- **Cold-start** — new category defaults to rare bucket plus is_new indicator
- **Staleness** — frozen count tables make new popular products look rare

*Example:* Example: exact user-agent frequency is a workhorse fraud feature — bot strings are near-unique.

**Failure mode:** Identical counts make unrelated categories indistinguishable — pair with another encoding, never use alone.

### Visualization (canvas `c7`, 720×300)

Zipf-style rank-frequency bar chart. Title (bold 14px `#1a5276`, left-aligned): "Category Frequency Is Power-Law: Keep the Head, Bucket the Tail". Padding top 40, bottom 45, left 60, right 30; L-shaped gray axes; rotated y label "row count", x label "category rank by frequency".

- **Data:** 40 rank positions, frequency = `100 / (rank+1)` (Zipf, scale max 100, min bar height 2px). Ranks below cutoff 22 in `rgba(26,82,118,0.35)` (head), from rank 22 onward orange `#e67e22` (tail).
- **Cutoff:** dashed red (2px, dash 4/4) vertical line at rank 22, labeled bold red 12px "count < threshold (~10-50)".
- **Annotations:** bold blue 12px "head: keep as-is"; bold orange 12px "tail: collapse into one \"rare\" bucket"; green 2px arrow into the rare bucket labeled bold green 11px two lines "new category (cold start)" / "defaults here + is_new flag".

## Interaction Terms & Crosses

Tags: technique (blue), trade-off (orange), privacy risk (red)

- **Cross signal** — marginals can be flat while the cross carries everything
- **Proven lift** — Wide & Deep hand-crosses; Facebook GBDT+LR leaf crosses
- **Hashing trick** — map crosses into 2^18–2^24 buckets, collisions as noise
- **Learned crosses** — two-tower, FMs, DCN; explicit crosses still win on small data
- **Blowup** — 10k × 10k = 10^8 combos; select or hash, never enumerate

*Example:* Example: news CTR peaks at breakfast, games at night — only hour×category sees it.

**Trap:** A cross of two safe features can become an identifier — zip×birthdate is nearly a person.

### Visualization (canvas `c8`, 720×300)

Heatmap of hour × category CTR. Title (bold 14px `#1a5276`, left-aligned): "Hour × Category CTR: the Cross Carries the Signal". Padding top 40, bottom 55, left 110, right 180.

- **Grid:** columns = hours `['6am', '9am', '12pm', '3pm', '6pm', '9pm', '12am', '3am']`; rows = categories `['News', 'Food', 'Games']`; values: News `[0.9, 1.0, 0.5, 0.4, 0.5, 0.3, 0.2, 0.1]`; Food `[0.2, 0.3, 0.9, 0.4, 1.0, 0.6, 0.2, 0.1]`; Games `[0.1, 0.1, 0.2, 0.3, 0.5, 0.8, 1.0, 0.9]`. Cell fill `rgba(26,82,118, 0.06 + v*0.75)`.
- **Peak cells:** orange `#e67e22` 2px outlines on News@9am, Food@6pm, Games@12am.
- **Right sidebar text:** `#222` 12px "Row and column averages are nearly equal — the marginal features look flat."; bold blue 12px "Only the cross sees the peaks."; orange 11px "peak cells outlined".
- **Bottom caption (bold blue 12px, centered):** "Hash or select crosses — never enumerate 10k × 10k combinations".

## Counters, Freshness & Train/Serve Skew

Tags: signal (blue), failure mode (red), gaming (orange)

- **The skew** — training uses batch snapshots, serving computes live values
- **Symptom** — great offline metrics, flat or negative online A/B
- **Feedback loop** — boosted counters sustain boosts; reserve exploration traffic
- **Gaming** — actors slow-drip under velocity thresholds, time bursts to boost windows
- **Freshness** — monitor staleness like uptime; alert on serving/training distribution distance

*Example:* Example: a model trained on batch step functions meets live intraday peaks in production.

**Key point:** Log the feature value at serving time and train on the logged value — most production "model quality" incidents are feature incidents.

### Visualization (canvas `c9`, 720×300)

Two-line chart over hour of day. Title (bold 14px `#1a5276`, left-aligned): "Train/Serve Skew: Same Feature Name, Two Different Values". Padding top 40, bottom 45, left 60, right 170; L-shaped gray axes; rotated y label "views last hour", x label "hour of day".

- **Data (12 points, y max 100):** serving (live) `[12, 15, 22, 40, 68, 85, 90, 82, 60, 45, 30, 18]` — solid green `#27ae60` 3px; training (batch snapshot, step function) `[20, 20, 20, 20, 55, 55, 55, 55, 40, 40, 40, 40]` — solid red `#e74c3c` 3px.
- **Skew annotation:** orange 1.5px vertical connector at the peak (index 6) between the two lines, labeled bold orange 12px two lines "skew: model never" / "saw this value".
- **Legend (x = w-158):** green swatch "Serving (live)"; red swatch "Training (batch)"; bold blue 11px note "Fix: log serving values, train on the logged values".

## Missingness as Signal

Tags: signal (blue), best practice (green), failure mode (red)

- **MNAR everywhere** — skipped income fields, declined location, sensors failing under load
- **Thin-file** — credit treats "no bureau file" as its own risk segment
- **Native handling** — XGBoost/LightGBM learn missing direction; pass NaN, don't impute first
- **Imputation trap** — mean-imputing without an indicator erases the signal
- **Serving pitfall** — batch NULLs vs online timeout defaults differ

*Example:* Example: income provided defaults at 4%, missing at 13%; imputing blends both to 6%.

**Rule of thumb:** Above ~5% missing, add an is_missing indicator — and keep imputation constants identical between training and serving.

### Visualization (canvas `c10`, 720×300)

Three-bar chart of default rate. Title (bold 14px `#1a5276`, left-aligned): "Default Rate by Income-Field Status: Missingness Is the Signal". Padding top 40, bottom 50, left 70, right 210; y scale max 0.15; rotated y label "default rate".

- **Bars (width 100, gap 45):** "Income provided" 4% green `#27ae60`; "Income missing" 13% red `#e74c3c`; "Mean-imputed blend" 6% orange `#e67e22`. Bold 14px percentage labels above; 12px labels below.
- **Merge arrow:** dashed gray (`#999`, 1.5px, dash 4/3) quadratic curve from the provided bar over to the blend bar, labeled `#666` 11px "imputation merges the populations".
- **Right sidebar text (x = w-198):** `#222` 12px "Mean-imputing without an indicator merges the 4% and 13% populations into one washed-out blend."; bold orange 12px "Add is_missing first — then impute."

## Regeneration instructions

- **Layout:** most-powerful-signals compact style. One `.card-section` per topic: `<h2>` (unnumbered, 1.3rem `#1a5276`, 2px solid `#2980b9` bottom border), then a `table.layout` with one row — left `td.text-col` (45%) holding `.tags` pill row, a `<ul>` of labeled bullets (`<li><b>Label</b> — text`), an italic `.example` paragraph, and a `.key-point` callout; right `td.viz-col` (55%) holding one `<canvas width="720" height="300">` scaled to `width: 100%` with 1px `#e0e0e0` border, 4px radius.
- **Tag pills:** `.tag` inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px. Colors: blue `rgba(26,82,118,0.12)`/`#1a5276`; green `rgba(39,174,96,0.15)`/`#27ae60`; red `rgba(231,76,60,0.12)`/`#e74c3c`; orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Key-point callout:** background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem; `<strong>` lead-in label.
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; `ul` 0.92rem with `li b` in `#1a5276`; `.example` italic `#555` 0.9rem. Superscripts (2^18 etc.) use `<sup>` in HTML.
- **Canvas:** shared `setup(id)` helper — backing store sized to rendered width × `window.devicePixelRatio` (display capped via `style.maxWidth`), `ctx.scale` back to logical coordinates, logical size 720×300.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`, gray text `#444`/`#555`.
- In regenerated HTML, any card links use `.html` extensions (this page has no outbound links).
