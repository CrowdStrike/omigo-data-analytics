# MCMC Simulation

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** MCMC Simulation

**Subtitle:** When the posterior has no formula you can integrate, simulate a walk whose visits pile up into the distribution — then every mean, interval, and tail risk is just counting draws

## A Rate Nobody Can Integrate

**Tags:** `core idea` (blue), `simulation` (green), `posterior draws` (orange)

- **The clinic** — 45 of 225 appointments were no-shows; the posterior for the true rate is a curve
- **Easy to score** — any single rate can be scored: how well does 18% explain 45 out of 225?
- **Hard to total** — means, intervals, and tail chances are integrals; real models have no formula
- **The move** — simulate a walker that visits rates in proportion to their posterior probability
- **The prize** — the pile of visited values stands in for the posterior; questions become counting

*Example (italic):* Instead of integrating to get P(rate > 25%), run the walker for 5,000 steps and count how many visited rates exceed 0.25.

**Key point:** MCMC simulation replaces calculus with counting — simulate draws from the posterior, then treat the draws as the distribution.

### Visualization (canvas `c1`, 720×300)

The target posterior density curve over the no-show rate, with a "score one point" marker and the tail integral shaded as the hard part.

- **Title (bold 15px, `#1a5276`, top center):** "The Posterior: Easy to Score at a Point, Hard to Total".
- **Curve:** density `d(x) = exp(-0.5 * ((x - 0.201) / 0.0267)^2)` plotted for x in 0.10–0.32 (step 0.002); blue `#2a78d6` 3px line; area under the whole curve filled `rgba(42,120,214,0.10)`.
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 185; x maps 0.10–0.32; 1px `#999` axis lines; x ticks at 10/15/20/25/30% in 12px `#444`.
- **Score marker:** green `#008300` dashed vertical (dash 4/3) at x=0.18 up to the curve, 4px green dot on the curve at density 0.734; green bold 12px label "score one rate: fine" beside it.
- **Tail shading:** area under the curve for x ≥ 0.25 filled `rgba(213,81,129,0.30)`; magenta `#d55181` bold 12px two-line label upper right: "P(rate > 25%) = an integral —" / "no formula in real models".
- **Caption (12px `#444`, bottom right):** "posterior after 45 no-shows in 225 appointments (illustrative)".

## Watching the Pile Become the Posterior

**Tags:** `worked example` (blue), `convergence` (green)

- **The walk** — each step proposes a nearby rate and accepts or rejects it; every visit is logged
- **50 draws** — a lumpy pile: bars of 1 and 13 sit side by side; no curve is visible yet
- **500 draws** — the hill shape emerges around 20%, still ragged at the edges
- **5,000 draws** — the histogram hugs the true posterior curve; the simulation IS the distribution
- **Burn-in** — the first steps still echo the starting value; standard practice discards them

*Example (italic):* All three panels come from one continuous run — the only difference is how long the walker was allowed to keep dropping values into the pile.

**Key point:** Run the chain long enough and the histogram of visited values converges to the posterior — that convergence is what makes the counting trick legal.

### Visualization (canvas `c2`, 720×300)

Three mini-panel histograms — the same chain at 50, 500, and 5,000 draws — each with the true posterior curve overlaid.

- **Title (bold 15px, `#1a5276`, top center):** "One Chain, Three Checkpoints: the Pile Becomes the Curve".
- **Panels:** three, left x-origins 60 / 270 / 480, each 190px wide; shared baseline y=240, bars up to 160px tall; panel titles bold 13px `#2c3e50` above each: "50 draws", "500 draws", "5,000 draws".
- **Bins:** 9 bins, centers 0.13–0.29 step 0.02 (x range 0.12–0.30 per panel); bar width 19px with 2px gaps.
- **Counts (hardcoded):** 50 draws `[0, 1, 4, 9, 13, 12, 7, 3, 1]`; 500 draws `[3, 13, 47, 90, 124, 113, 72, 29, 9]`; 5,000 draws `[25, 151, 442, 937, 1219, 1090, 703, 320, 113]`. Each panel normalizes bar height to its own max count.
- **Bars:** fill `rgba(42,120,214,0.45)`, 1.5px `#2a78d6` top edge.
- **Curve overlay:** in each panel, `y = baseY - 160 * exp(-0.5 * ((x - 0.201) / 0.0267)^2)` drawn in green `#008300` 2px across the panel's x range.
- **Ticks:** 15% and 25% labels 11px `#6b7280` under each panel.
- **Annotation (green `#008300`, bold 12px, above right panel):** "histogram ≈ posterior".
- **Caption (12px `#444`, bottom right):** "one continuous run, snapshots at three lengths (illustrative)".

## Every Question Becomes Counting Draws

**Tags:** `where it's used` (blue), `credible interval` (green), `transformations` (orange)

- **Mean** — the posterior mean is just the average of the 5,000 draws: 0.201
- **Interval** — sort the draws; the middle 90% run from 0.158 to 0.245: the credible interval
- **Tail chance** — P(rate > 25%) = the share of draws above 0.25 = 3%
- **Free transforms** — push each draw through cost = rate × 12,000 × $60 for the cost distribution
- **No new math** — each answer took one pass over the same draws; no integral was ever computed

*Example (italic):* The clinic's yearly no-show cost comes out as a full distribution — mean $145k, 90% interval $114k–$176k — from the same 5,000 draws.

**Key point:** Once you hold draws from the posterior, any summary — mean, interval, tail risk, transformed cost — is one counting pass over the draws.

### Visualization (canvas `c3`, 720×300)

The 5,000-draw histogram annotated with the three answers read directly off it: mean marker, shaded 90% credible band, magenta tail bars.

- **Title (bold 15px, `#1a5276`, top center):** "Answers Read Straight Off the 5,000 Draws".
- **Bins:** 17 bins, centers 0.125–0.285 step 0.01; percent heights (hardcoded) `[0.3, 0.7, 1.6, 3.4, 6.0, 9.3, 12.5, 14.6, 14.8, 13.1, 10.0, 6.7, 3.8, 1.9, 0.8, 0.3, 0.1]`.
- **Axes:** origin x=65, baseline y=240, plot width 590, plot height 170; x maps 0.12–0.29; y scale 0–16%; x ticks at 12/16/20/24/28% in 12px `#444`; 1px `#999` axis lines.
- **Bars:** width 31px; fill `rgba(42,120,214,0.45)` with 1.5px `#2a78d6` top edge; bars centered above 0.25 switch to fill `rgba(213,81,129,0.5)` with `#d55181` edge.
- **Credible band:** rectangle from x=0.158 to x=0.245, top y=72 to baseline, fill `rgba(0,131,0,0.08)` with dashed `#008300` vertical edges (dash 4/3); green bold 12px label just above: "90% of draws: 0.158 – 0.245".
- **Mean marker:** blue `#2a78d6` dashed vertical (dash 4/3) at 0.201 from y=100 to baseline; blue bold 12px label to its right: "mean 0.201 (average of draws)".
- **Tail label (magenta `#d55181`, bold 12px, right side ≈ y=130):** "3% of draws above 0.25".
- **Caption (12px `#444`, bottom right):** "same draws, three answers — no integrals (illustrative)".

## Precision You Have to Earn

**Tags:** `common mistake` (red), `Monte Carlo error` (orange), `rule of thumb` (green)

- **Two errors** — the posterior's width is real uncertainty; the simulation adds its own on top
- **Shrinks slowly** — Monte Carlo error falls like 1/√N: 100× more draws buys one extra digit
- **Tails are worst** — P(rate > 25%) rests on ~3% of draws; 50 draws hold about 1 or 2 of them
- **Correlated steps** — neighboring draws are near-copies; 5,000 steps hold less than 5,000 draws' worth
- **The mistake** — quoting 3.14159%-style precision from a run that only earned "about 3%"

*Example (italic):* With 50 draws the tail estimate landed at 8%; with 500 it read 3.8%; only by 5,000 did it settle near the true 3% — rough long after the histogram looked smooth.

**Common mistake:** Reporting simulation output at full decimal precision. The draws pin the posterior down only to their Monte Carlo error — check that error before quoting digits.

### Visualization (canvas `c4`, 720×300)

Dot-and-whisker convergence plot: the tail-chance estimate at four run lengths, whiskers shrinking like 1/√N toward the dashed true value.

- **Title (bold 15px, `#1a5276`, top center):** "The Tail Estimate Earns Its Digits Slowly (1/√N)".
- **Data:** run lengths `["N = 50", "N = 500", "N = 5,000", "N = 50,000"]` at x = 150/300/450/600; estimates `[8.0, 3.8, 3.2, 3.0]` percent; ±1 Monte Carlo standard error whiskers `[3.8, 0.86, 0.25, 0.08]` percentage points.
- **Axes:** baseline y=240, plot height 175, y scale 0–12%; y gridlines 1px `#e5e9ef` at 0/3/6/9/12 with 11px `#6b7280` labels at x=48; run-length labels 12px `#444` below baseline.
- **Truth line:** dashed green `#008300` 1.5px horizontal (dash 4/3) at 3.0%; green 12px label at the right end: "true tail chance 3%".
- **Dots & whiskers:** blue `#2a78d6` 5px dots at each estimate; 2px blue vertical whiskers to ±1 SE with 8px horizontal caps; bold 12px blue value label above each dot: "8.0%", "3.8%", "3.2%", "3.0%".
- **Annotation (violet `#4a3aa7`, bold 13px, upper right):** "100× the draws ≈ 10× less error".
- **Caption (12px `#444`, bottom right):** "whiskers = ±1 Monte Carlo standard error (illustrative)".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **No randomness:** all histogram counts, estimates, and whisker widths are the hardcoded literal arrays above; the only computed values are the deterministic density curve `exp(-0.5 * ((x - 0.201) / 0.0267)^2)` — nothing is generated randomly at render time.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
