# Likelihood vs Probability

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Likelihood vs Probability

**Subtitle:** Probability fixes the model and asks about the data; likelihood fixes the data and asks about the model — the same formula read in opposite directions

## Seven Coupons Out of Ten

**Tags:** `core idea` (blue), `same formula` (green), `two questions` (orange)

- **The café** — a café hands coupons to 10 customers, and 7 of them come back to redeem
- **One formula** — chance of exactly 7 redemptions is 120 × p^7 × (1−p)^3 for redemption rate p
- **Probability** — fix the rate at p = 0.7 and ask "which outcome counts k should I expect?"
- **Likelihood** — fix the outcome at 7 of 10 and ask "which rate p explains it best?"
- **Opposite scans** — probability scans across outcomes; likelihood scans across candidate rates

*Example (italic):* Plugging p = 0.7 into the formula gives 0.267 — the probability of the data, and equally the likelihood of the rate.

**Key point:** Probability holds the model fixed and varies the data; likelihood holds the data fixed and varies the model. Same number, opposite question.

### Visualization (canvas `c1`, 720×300)

Dual-panel bar chart: the probability distribution over outcomes k at fixed p=0.7 (left) vs the likelihood over candidate rates p at fixed data 7-of-10 (right), split by a vertical dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "Same Formula, Two Readings: 7 Coupons Redeemed out of 10".
- **Left panel (probability):** outcomes k = 0..10 with probabilities `[0.000, 0.000, 0.001, 0.009, 0.037, 0.103, 0.200, 0.267, 0.233, 0.121, 0.028]` (Binomial n=10, p=0.7); axis origin x=50, width 290, baseline y=240, chart height 175, y scale 0–0.30; bars fill `rgba(42,120,214,0.45)` except the k=7 bar in solid orange `#d95926` with bold 12px orange label "0.267" above it; k labels 11px `#444` below each bar; caption 12px `#444` "probability: rate fixed at p = 0.7, outcome k varies".
- **Right panel (likelihood):** rates p = 0.1..0.9 with likelihoods `[0.000, 0.001, 0.009, 0.042, 0.117, 0.215, 0.267, 0.201, 0.057]` (L(p) = 120 p^7 (1−p)^3); axis origin x=395, width 290, same baseline/height/scale; bars fill `rgba(0,131,0,0.4)` except the p=0.7 bar in solid orange `#d95926` with bold 12px orange label "0.267"; p labels ".1"–".9" 11px `#444` below; green bold 13px annotation "peaks at p = 0.7"; caption "likelihood: data fixed at 7 of 10, rate p varies".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.

## Scoring Three Candidate Rates by Hand

**Tags:** `worked example` (blue), `rule of thumb` (green)

- **Candidates** — audition three redemption rates for the café: p = 0.3, p = 0.5, and p = 0.7
- **p = 0.3** — 120 × 0.3^7 × 0.7^3 = 0.009; this rate almost never produces 7 redemptions
- **p = 0.5** — 120 × 0.5^10 = 120/1024 = 0.117; a coin-flip rate does it sometimes
- **p = 0.7** — 120 × 0.7^7 × 0.3^3 = 0.267, the best score of the three candidates
- **Ratios rule** — 0.267 / 0.117 ≈ 2.3, so p = 0.7 explains the data 2.3× better than p = 0.5

*Example (italic):* Against p = 0.3 the ratio is 0.267 / 0.009 ≈ 30 — the data all but rules that rate out.

**Key point:** Likelihood scores only matter relative to each other — compare candidate rates by ratio, never by absolute size.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart scoring the three candidate rates on the same likelihood scale, with ratio annotations.

- **Title (bold 15px, `#1a5276`, top center):** "Three Candidate Rates Scored Against '7 of 10 Redeemed'".
- **Data:** rows `p = 0.3` → 0.009, `p = 0.5` → 0.117, `p = 0.7` → 0.267.
- **Bars:** start x=150, max width 460, scale max 0.28, bar height 26; rows at y=85, y=145, y=205; p=0.3 fill `rgba(213,81,129,0.55)` (magenta), p=0.5 fill `rgba(42,120,214,0.55)` (blue), p=0.7 fill `rgba(0,131,0,0.55)` (green); minimum bar length 5px so the 0.009 bar stays visible.
- **Labels:** row labels bold 13px `#1a5276` ("p = 0.3" etc.) left of each bar; value labels bold 13px in each bar's color ("0.009", "0.117", "0.267") just right of each bar end.
- **Annotations:** magenta bold 12px "≈ 30× below the best" right of the p=0.3 bar; green bold 13px "2.3× better than p = 0.5" right-aligned just above the end of the p=0.7 bar.
- **Caption (12px `#444`, bottom center):** "L(p) = 120 × p^7 × (1−p)^3 — likelihood of rate p given the café's 7-of-10 result".

## The Peak Is the Estimate

**Tags:** `where it's used` (blue), `maximum likelihood` (green)

- **Full sweep** — score every rate from p = 0.05 to p = 0.95 and plot the whole likelihood curve
- **The peak** — the curve tops out at p = 0.7, exactly 7/10, the fraction actually observed
- **MLE** — the peak's location is the maximum likelihood estimate, written p̂ ("p-hat")
- **Everywhere** — logistic regression, A/B conversion rates, and mixture models all fit this way
- **Sharp vs flat** — a sharp peak means the data pins the rate down; a flat one begs for more data

*Example (italic):* With 7 redemptions in 10 offers the curve peaks at p̂ = 0.70, scoring 0.267 — no other rate scores higher.

**Key point:** Maximum likelihood is just "pick the parameter value that gives your actual data the highest score."

### Visualization (canvas `c3`, 720×300)

Single-panel line chart of the full likelihood curve L(p) over the rate p, with the peak marked at p = 0.7.

- **Title (bold 15px, `#1a5276`, top center):** "The Likelihood Curve for the Café's Redemption Rate".
- **Data:** p grid 0.05 to 0.95 in steps of 0.05 (19 points); L values `[0.000, 0.000, 0.000, 0.001, 0.003, 0.009, 0.021, 0.042, 0.075, 0.117, 0.166, 0.215, 0.252, 0.267, 0.250, 0.201, 0.130, 0.057, 0.010]`.
- **Axes:** origin x=60, width 600, baseline y=240, chart height 180; x maps p over 0–1 with tick labels "0.1"–"0.9" (12px `#444`); y scale 0–0.30 with ticks 0, 0.1, 0.2, 0.3 (12px `#444`); axes 2px `#1a5276`.
- **Curve:** green `#008300` 3px line through the 19 points with 3px green dots.
- **Peak marker:** 6px orange `#d95926` dot at (0.70, 0.267); dashed `#d95926` (dash 4/3) vertical drop line from the dot to the baseline; orange bold 13px annotation "p̂ = 7/10 = 0.70" beside the peak.
- **Secondary annotation (blue `#2a78d6` bold 12px, over the left flank):** "rates below 0.4 score almost zero".
- **Caption (12px `#444`, bottom center):** "likelihood of rate p given 7 redemptions in 10 offers".

## It Is Not the Probability of p

**Tags:** `common mistake` (red), `rule of thumb` (green)

- **Rows sum to 1** — reading across outcomes at a fixed rate, the probabilities total 1.000
- **Columns don't** — reading down rates at fixed data k=7: 0.009 + 0.117 + 0.267 = 0.393
- **Not a distribution** — the area under this whole likelihood curve is about 0.09, not 1
- **Wrong sentence** — "there is a 26.7% chance p is 0.7" is false; that claim needs a prior (Bayes)
- **Right sentence** — "a rate of p = 0.7 gives the observed data probability 0.267"

*Example (italic):* The same table cell 0.267 is a real probability across its row, but only a comparative score down its column.

**Common mistake:** Treating likelihood values as probabilities of the parameter. They don't sum to 1 — only ratios between candidates mean anything, and turning them into parameter probabilities requires a Bayesian prior.

### Visualization (canvas `c4`, 720×300)

A 3×11 table of Binomial(10, p) probabilities with one row highlighted (probability, sums to 1) and one column highlighted (likelihood, does not).

- **Title (bold 15px, `#1a5276`, top center):** "One Table, Two Directions: Rows Are Probability, Columns Are Likelihood".
- **Data (rows p = 0.3, 0.5, 0.7; columns k = 0..10):**
  - p = 0.3: `[0.028, 0.121, 0.233, 0.267, 0.200, 0.103, 0.037, 0.009, 0.001, 0.000, 0.000]`
  - p = 0.5: `[0.001, 0.010, 0.044, 0.117, 0.205, 0.246, 0.205, 0.117, 0.044, 0.010, 0.001]`
  - p = 0.7: `[0.000, 0.000, 0.001, 0.009, 0.037, 0.103, 0.200, 0.267, 0.233, 0.121, 0.028]`
- **Grid layout:** cells 46px wide × 44px tall, first cell at x=95; rows at y=95, y=139, y=183; column headers "k=0".."k=10" bold 11px `#444` above the grid; row labels "p = 0.3", "p = 0.5", "p = 0.7" bold 12px `#1a5276` left of each row; cell values 11px `#2c3e50` centered; cell borders 1px `#e5e9ef`.
- **Row highlight:** the p = 0.7 row cells filled `rgba(42,120,214,0.15)`; blue bold 12px "≈ 1" right of each row at x=612 (all three rows, since every row sums to 1).
- **Column highlight:** the k = 7 column cells filled `rgba(0,131,0,0.18)` (the p=0.7 / k=7 cell gets both fills and a 2px orange `#d95926` outline).
- **Annotations (below the grid):** blue bold 12px at y=252 "read across a row → probability of outcomes, sums to 1"; green bold 13px at y=274 "read down column k=7 → likelihood of rates: 0.009 + 0.117 + 0.267 = 0.393, not 1".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- All chart data is hardcoded (no randomness): Binomial(10, p) probabilities and L(p) = 120 p^7 (1−p)^3 values as listed above.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
