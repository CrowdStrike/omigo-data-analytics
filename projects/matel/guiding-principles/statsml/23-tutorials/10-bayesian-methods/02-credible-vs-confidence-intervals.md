# Credible vs Confidence Intervals

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Credible vs Confidence Intervals

**Subtitle:** Two intervals with nearly the same numbers make completely different promises — one rates the recipe over many repeats, the other states a probability about the parameter itself

## One Oat-Milk Survey, Two Interval Claims

**Tags:** `core idea` (blue), `two schools` (green), `same numbers` (orange)

- **The survey** — a coffee shop checks 200 orders and finds 48 with oat milk, a 24% sample rate
- **The question** — what is the true oat-milk rate across all customers, not just these 200?
- **Confidence interval** — the frequentist answer from this sample is 18.1% to 29.9%
- **Credible interval** — the Bayesian answer (with a flat prior) is 18.4% to 30.2%
- **The trap** — the numbers nearly match, so people assume the two claims mean the same thing

*Example (italic):* Two analysts hand the owner near-identical intervals — but only one of them is allowed to say "95% chance".

**Key point:** The intervals may overlap almost perfectly, yet the "95%" attaches to different things: the interval-making procedure in one, the true rate itself in the other.

### Visualization (canvas `c1`, 720×300)

Single number line (15% → 35%) with the two intervals drawn as stacked horizontal bars above it, showing how close the numbers are.

- **Title (bold 15px, `#1a5276`, top center):** "Same Survey, Two Intervals: 48 Oat-Milk Orders out of 200".
- **Data:** sample rate 0.24; confidence interval `[0.181, 0.299]`; credible interval `[0.184, 0.302]`.
- **Axis:** horizontal 2px `#999` line at y=210 from x=70, width 580; x maps 0.15–0.35 linearly (`x = 70 + (v - 0.15) / 0.20 * 580`); ticks at 0.15, 0.20, 0.25, 0.30, 0.35 labeled "15%"…"35%" (12px `#444` below).
- **Confidence bar:** blue `#2a78d6` 6px horizontal bar at y=110 from 18.1% to 29.9%, 12px vertical end caps; bold 13px blue label to the left of the bar: "confidence: 18.1% – 29.9%".
- **Credible bar:** green `#008300` 6px bar at y=160 from 18.4% to 30.2%, same caps; bold 13px green label "credible: 18.4% – 30.2%".
- **Sample dot:** ink `#1a5276` 6px dot on the axis at 24% with 12px label "p̂ = 24%" below the tick labels row (or just above the axis).
- **Annotation (bold 13px magenta `#d55181`, top right area):** two lines "nearly the same numbers," / "very different claims".

## What 95% Confidence Actually Promises

**Tags:** `worked example` (blue), `repeated sampling` (green)

- **Fixed truth** — imagine the true rate really is 25%; it never moves and gets no probability
- **Random interval** — each fresh sample of 200 orders gives a new p̂ and a new interval
- **The promise** — 95% of intervals built this way would capture the fixed 25% in the long run
- **One interval** — your single printed interval either contains 25% or it doesn't; no 95% about it
- **Unlucky sample** — p̂ = 32% gives [26.1%, 37.9%] (fixed ±5.9-pt width), missing 25% entirely

*Example (italic):* Rerunning the 200-order survey 20 times gives 20 different intervals; 19 of the 20 cover the true 25%.

**Key point:** "95% confidence" is a grade for the interval-making recipe across many hypothetical repeats — not a probability about the one interval on your report.

### Visualization (canvas `c2`, 720×300)

Coverage plot: 20 vertical confidence intervals from 20 repeated samples, one horizontal dashed line at the true rate, one interval missing it.

- **Title (bold 15px, `#1a5276`, top center):** "20 Repeated Surveys, 20 Intervals: 19 Catch the Truth (illustrative)".
- **Data:** true rate 0.25; sample proportions `[0.245, 0.230, 0.262, 0.255, 0.218, 0.240, 0.271, 0.226, 0.249, 0.320, 0.237, 0.258, 0.222, 0.266, 0.243, 0.229, 0.252, 0.235, 0.261, 0.247]`; each interval = p̂ ± 0.059.
- **Layout:** plot area x=70 width 580, y maps rate 0.14 (baseline y=250) to 0.40 (top y=55); interval i centered at `x = 70 + (i + 0.5) * 29` for i = 0..19; light 1px `#e5e9ef` gridlines at 0.15, 0.20, 0.25, 0.30, 0.35 with 12px `#444` labels "15%"…"35%" on the left.
- **True-rate line:** ink `#1a5276` dashed (dash 5/4) horizontal line at rate 0.25 across the plot; bold 12px ink label "true rate 25%" at its right end.
- **Intervals:** 3px vertical lines from p̂−0.059 to p̂+0.059 with a 4px dot at p̂; blue `#2a78d6` for the 19 that cross 0.25; orange `#d95926` for sample #10 (p̂ = 0.32, interval [0.261, 0.379]).
- **Annotation (bold 13px orange `#d95926`, above the orange interval):** two lines "this one misses —" / "1 of 20 ≈ 5%".
- **Caption (12px `#444`, bottom center):** "each vertical bar = one survey of 200 orders, interval = p̂ ± 5.9 points".

## What 95% Credible Actually Promises

**Tags:** `worked example` (blue), `posterior` (green), `prior` (orange)

- **Fixed data** — the Bayesian keeps the one sample (48 of 200) and treats the rate as uncertain
- **Prior** — start with a flat prior Beta(1,1): every rate from 0% to 100% equally believable
- **Posterior** — 48 hits and 152 misses update it to Beta(49, 153), a curve peaked near 24%
- **The interval** — the middle 95% of that posterior curve runs from 18.4% to 30.2%
- **The claim** — given this data and prior, there is a 95% probability the true rate is inside

*Example (italic):* The owner can literally bet at 19-to-1 odds that the oat-milk rate lies between 18.4% and 30.2%.

**Key point:** A credible interval is a direct probability statement about the parameter — the thing everyone wishes the confidence interval were. The price of admission is a prior.

### Visualization (canvas `c3`, 720×300)

Posterior density curve for the oat-milk rate with the middle 95% shaded, plus a flat dashed line showing the prior it started from.

- **Title (bold 15px, `#1a5276`, top center):** "Posterior Belief After 48 of 200: Beta(49, 153)".
- **Data:** relative density heights (peak = 1.0) at rates `[0.14, 0.16, 0.18, 0.20, 0.22, 0.24, 0.26, 0.28, 0.30, 0.32, 0.34, 0.36]`: heights `[0.003, 0.023, 0.115, 0.368, 0.754, 0.996, 0.846, 0.462, 0.162, 0.037, 0.005, 0.001]`; credible bounds 0.184 and 0.302.
- **Layout:** x maps rate 0.12–0.38 to x=70…650; baseline y=250; height 1.0 maps to y=70; x ticks at 0.15, 0.20, 0.25, 0.30, 0.35 labeled "15%"…"35%" (12px `#444`).
- **Curve:** green `#008300` 3px smooth polyline through the 12 points.
- **Shaded 95% region:** fill `rgba(0,131,0,0.18)` under the curve between rate 0.184 and 0.302 (interpolate curve heights at the bounds), down to the baseline.
- **Bound lines:** two green dashed (dash 4/3) vertical lines at 0.184 and 0.302, bold 12px green labels "18.4%" and "30.2%" just above the baseline beside each line.
- **Prior line:** mute `#6b7280` dashed horizontal line at height 0.06 (y ≈ 239) across the plot, 12px mute label "flat prior Beta(1,1)" at its left end above the line.
- **Annotation (bold 13px green, above the peak, arrow optional):** "95% of the belief sits in here".

## The Sentence Everyone Gets Wrong

**Tags:** `common mistake` (red), `small samples` (orange), `rule of thumb` (green)

- **The sentence** — "there's a 95% chance the true rate is in 18.1%–29.9%" — wrong for a CI
- **Why it's wrong** — frequentist rules fix the parameter; probability belongs to the interval only
- **Why it survives** — with flat priors and big samples the two intervals nearly coincide
- **When they split** — strong priors, small samples, or boundary values pull them far apart
- **Safe phrasing** — CI: "a method right 95% of the time"; credible: "95% probability, given the prior"

*Example (italic):* With 2 oat-milk orders out of 5, the Wald CI is 40% ± 42.9% → [−2.9%, 82.9%], while the credible interval [11.8%, 77.7%] never leaves 0–100%.

**Common mistake:** Reading "95% confidence" as "95% probability the parameter is in this interval". That probability statement needs a prior — it is the credible interval's claim, not the confidence interval's.

### Visualization (canvas `c4`, 720×300)

Number line from −10% to 100% comparing the tiny-sample (2 of 5) Wald confidence interval, which dips below zero, with the credible interval, which stays legal.

- **Title (bold 15px, `#1a5276`, top center):** "2 Oat-Milk Orders out of 5: Where the Two Intervals Split".
- **Data:** p̂ = 0.40; Wald CI `[-0.029, 0.829]`; credible interval (Beta(3,4), 95% equal-tailed) `[0.118, 0.777]`.
- **Axis:** 2px `#999` horizontal line at y=230 from x=70, width 580; x maps −0.10 to 1.00 (`x = 70 + (v + 0.10) / 1.10 * 580`); ticks at 0, 0.25, 0.50, 0.75, 1.00 labeled "0%", "25%", "50%", "75%", "100%" (12px `#444`).
- **Impossible zone:** fill `rgba(231,76,60,0.10)` rectangle left of the 0% tick from y=70 to the axis; red `#e74c3c` dashed (dash 4/3) vertical line at 0%.
- **Wald bar:** blue `#2a78d6` 6px bar at y=120 from −2.9% to 82.9% with 12px end caps; bold 13px blue label above it "Wald CI: −2.9% to 82.9%".
- **Credible bar:** green `#008300` 6px bar at y=180 from 11.8% to 77.7%, same caps; bold 13px green label above it "credible: 11.8% to 77.7%".
- **Sample dot:** ink `#1a5276` 6px dot at 40% on the axis, 12px label "p̂ = 40%" below.
- **Annotation (bold 13px red `#e74c3c`, inside the impossible zone, two lines):** "impossible:" / "rate below 0%".
- **Takeaway (bold 13px magenta `#d55181`, bottom center at y=285):** "small samples expose the difference — the posterior can never claim an impossible rate".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
