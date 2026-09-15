# Pitfall: Aggregation Destroys Signal

**Page type:** detail page (card-section layout: one h2 section per block, two-column table text left 45% / canvas right 55%)
**HTML title tag:** Aggregation Destroys Signal

**Subtitle:** Summarizing into one number loses the pattern that matters.

## Section 1: The Problem

**Tags:** `the trap` (red pill), `aggregation` (blue pill)

- **Collapse to scalars** — mean, sum, and count features erase the pattern inside the series
- **Pattern beats level** — volatility, trend, and sequence often predict more than the total
- **Spending shape** — a steady $400/month and one $4,800 December both total $5,000 a year
- **Session mix** — fifty 6-second bounces and one engaged visit share a 5-minute average
- **Clinical swings** — stable glucose and 60–180 swings can both average 110 mg/dL
- **Trend direction** — two $100K products differ when one grows 15% and the other shrinks 10%

*Example:* Churners' usage slides 80→40→30 hours while loyal users hold steady at 50, yet both average 50 hours a month.

**Impact:** Two entities with identical summary statistics can have opposite patterns and outcomes, and the model cannot tell them apart.

### Visualization (canvas `c1`, 720×300)

Side-by-side monthly bar charts: two users with identical mean spend but opposite volatility.

- **Title (bold 14px, `#1a5276`, top center):** "Monthly Spend: Mean = $400 for Both Users".
- **Left group (User A, steady):** bars for Jan–Jun with values `[410, 395, 405, 390, 400, 405]`, scaled against max 500 over 120px height, baseline y=220; bars 35px wide, 50px spacing, starting x=100; fill `rgba(39,174,96,0.6)`, stroke `#27ae60` width 1.5. Month labels in 9px gray `#666` below bars. Green labels above group: bold "User A: Steady (loyal)" and "Mean = $400, Std = $7".
- **Right group (User B, volatile):** bars for Jan–Jun with values `[50, 80, 200, 850, 150, 1070]`, scaled against max 1200 over 120px, starting x=450; fill `rgba(231,76,60,0.6)`, stroke `#e74c3c` width 1.5. Red labels: bold "User B: Volatile (risky)" and "Mean = $400, Std = $420".
- **Baseline:** dashed gray `#999` line (dash 4/3) at y=220 from x=90 to x=740.
- **Caption (bold 12px red `#e74c3c`, bottom center):** "Mean is identical. But volatility and pattern are completely different!"

## Section 2: Why It Happens

**Tags:** `root cause` (orange pill), `lossy summaries` (blue pill)

- **Fixed-length inputs** — tabular models want one row per entity, so sequences get collapsed
- **Convenience** — mean, sum, and count are trivial; trend and autocorrelation take thought
- **Reporting reuse** — pipelines built for dashboards get reused as ML features unquestioned
- **Lossy compression** — cutting dimensionality feels virtuous but discards predictive signal
- **Level vs dynamics** — the target hinges on trend and volatility, but the model sees only level

*Example:* Twelve fraud transactions arrive in 8 minutes while a legitimate user spreads 12 over 30 days, yet the totals match.

**Root Cause:** The outcome is driven by dynamics, but aggregation hands the model only the level, capping its predictive power.

### Visualization (canvas `c2`, 720×300)

Two line charts: declining vs stable usage, same mean.

- **Title (bold 14px, `#1a5276`, top center):** "Usage Over 6 Months: Mean = 50 hours for Both".
- **Left series (churner):** values `[80, 70, 55, 40, 30, 25]` over M1–M6, plotted from x=120 to x=320, y scaled by value/100 over 140px chart height, baseline y=220; line `#e74c3c` width 3 with 4px-radius red dots. Red labels centered above: bold "Declining: Churner" and "Mean = 50h, Trend = -11h/mo".
- **Right series (loyal):** values `[48, 52, 50, 51, 49, 50]`, plotted x=480 to x=680; line `#27ae60` width 3 with 4px green dots. Green labels: bold "Stable: Loyal" and "Mean = 50h, Trend = +0.3h/mo".
- **Axis:** light gray `#ccc` horizontal line at y=220 from x=100 to x=700; month labels M1–M6 in 9px `#666` under each series (40px spacing).
- **Caption (bold 12px red `#e74c3c`, bottom center):** "Aggregated mean hides the trend. Model cannot predict churn."

## Section 3: The Correct Approach

**Tags:** `the fix` (green pill), `pattern features` (blue pill)

- **Volatility** — add standard deviation beside the mean; spread often predicts more than level
- **Trend** — add a linear slope over time or the ratio of recent to historical values
- **Recency** — compare recent windows to older ones, such as last 7 days vs last 30 days
- **Event structure** — count periods above a reference, longest streak, time since last event
- **Sequence models** — LSTMs and transformers consume the raw sequence instead of summaries
- **Measure the loss** — the lift from adding pattern features shows how much signal was destroyed

*Example:* Adding a -$800/month income slope, $14K volatility, and a $52K recent-3-month feature lifts AUC from 0.73 to 0.86.

**Fix:** Don't stop at mean/sum/count — add variance, trend, and recency features, or model the sequence directly with LSTMs or transformers.

### Visualization (canvas `c3`, 720×300)

Box diagram comparing feature sets plus an AUC scorecard.

- **Title (bold 14px, `#1a5276`, top center):** "Feature Engineering: From Aggregation to Pattern".
- **Left box (BAD):** white rectangle 200×120 at (60, 60), stroke `#e74c3c` width 2; bold red heading "BAD: Aggregation Only"; 10px `#333` bullet list: "mean_spend", "total_spend", "count_transactions", "avg_transaction_value"; bold red 10px caption "Pattern lost!" at bottom.
- **Arrow:** blue `#2980b9` line from (260, 120) to (300, 120), bold 11px blue label "Add" above.
- **Right box (GOOD):** white rectangle 240×180 at (300, 60), stroke `#27ae60` width 2; bold green heading "GOOD: Pattern Features"; bullet list: "mean_spend (keep this)", "std_spend (volatility)", "trend_slope (up/down)", "recent_vs_historical (last 7d / last 30d)", "time_since_last_transaction", "longest_inactive_streak", "count_above_threshold", "autocorrelation (periodicity)"; bold green caption "Pattern captured!".
- **Scorecard box:** white rectangle 130×80 at (560, 100), stroke `#1a5276` width 2; bold blue heading "Model AUC"; red line "Aggregation: 0.73", green line "+ Pattern: 0.86", bold orange `#e67e22` line "Lift: +13 pts".
- **Caption (bold 11px `#1a5276`, bottom center):** "Mean is not enough. Model needs variance, trend, recency."

## Regeneration instructions

- **Layout:** three `.card-section` blocks, each with an `<h2>` (1.3rem `#1a5276`, 2px solid `#2980b9` bottom border) and a `table.layout` with one row: left `td.text-col` (45%) holding `.tags` pills, `<ul>` bullets, `.example` italic paragraph, and `.key-point` callout; right `td.viz-col` (55%) holding the canvas.
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; subtitle `#666` 0.95rem; `li b` in `#1a5276`; bullets 0.92rem.
- **Tag pills:** inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px — blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Key-point callout:** background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem.
- **Canvas:** intrinsic 720×300, CSS `width: 100%`, 1px `#e0e0e0` border with 4px radius; scale via a shared `setup(id)` helper using `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, grays `#666`/`#444`/`#333`. No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
