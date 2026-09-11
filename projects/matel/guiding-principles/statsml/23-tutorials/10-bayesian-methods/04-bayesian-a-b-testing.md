# Bayesian A/B Testing

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Bayesian A/B Testing

**Subtitle:** Instead of asking "is this data surprising if nothing changed?", compute the probability the variant is actually better — a number a decision can be built on

## Two Buttons, Two Belief Curves

**Tags:** `core idea` (blue), `posterior` (green), `A/B test` (orange)

- **The test** — a tea shop shows 1,000 checkout visitors button A and another 1,000 button B
- **The results** — A converts 50 of 1,000 visitors (5.0%); B converts 60 of 1,000 (6.0%)
- **The catch** — true rates are uncertain; a rerun of the test could easily land at 5.4% vs 5.7%
- **Belief curves** — Bayes turns each count into a curve of plausible true rates (a posterior)
- **The overlap** — B's curve sits to the right of A's but they overlap a lot: B is *probably* better

*Example (italic):* A's curve has mean 5.1% and B's 6.1%, yet both curves say a true rate of 5.7% is quite plausible for either button.

**Key point:** A Bayesian A/B test replaces two lonely percentages with two belief curves. Every question — including "which button is better?" — is read directly off the curves.

### Visualization (canvas `c1`, 720×300)

Two overlapping posterior belief curves for the true conversion rate of button A (blue) and button B (green), drawn over one shared axis.

- **Title (bold 15px, `#1a5276`, top center):** "What the True Rate Could Be: Belief Curves for A and B".
- **Data:** x = conversion rate 2.5%–9.0% in 0.5-point steps (14 points); curve A heights `[0.001, 0.011, 0.073, 0.291, 0.693, 0.990, 0.849, 0.437, 0.135, 0.025, 0.003, 0, 0, 0]`; curve B heights `[0, 0, 0.003, 0.022, 0.109, 0.351, 0.732, 0.991, 0.871, 0.496, 0.183, 0.044, 0.007, 0.001]` (illustrative, peak-normalized to 1).
- **Layout:** axis origin x=60, plot width 620, baseline y=245, chart height 185; x maps 2.5%→9.0% linearly; y maps 0→1.05.
- **Curves:** smooth polylines through the 14 points, 3px lines: A blue `#2a78d6` with fill `rgba(42,120,214,0.15)` to baseline, B green `#008300` with fill `rgba(0,131,0,0.15)`.
- **Axis labels:** ticks at 3–9% labeled "3%".."9%" 12px `#444` below baseline; ink `#1a5276` 1.5px axis line.
- **Curve labels (bold 13px):** blue "A: 50/1,000 (mean 5.1%)" above A's peak; green "B: 60/1,000 (mean 6.1%)" above B's peak.
- **Annotation (bold 12px orange `#d95926`, over the overlap zone near x≈5.6%):** "big overlap — B probably, not certainly, better".
- **Caption (12px `#444`, bottom right):** "posterior curves, flat prior (illustrative)".

## Counting the Ways B Wins

**Tags:** `worked example` (blue), `probability B is better` (green)

- **The trick** — draw one plausible rate from each curve, note which is higher, repeat 10,000 times
- **The count** — B's draw comes out higher in 8,400 of the 10,000 pairs, so P(B better) = 84%
- **The lift curve** — subtracting the draws gives a belief curve for the lift B − A, centered at +1.0 point
- **The interval** — 95% of that lift curve sits between −1.0 and +3.0 points (illustrative)
- **Direct read** — 84% answers the question the shop actually asked: "what's the chance B beats A?"

*Example (italic):* Of 10,000 paired draws, B wins 8,400 and loses 1,600 — exactly the green 84% shaded in the chart.

**Key point:** "Probability B is better" is just the share of the lift curve sitting above zero — here 84%, one honest number instead of a significance verdict.

### Visualization (canvas `c2`, 720×300)

Belief curve for the lift B − A in percentage points, shaded green above zero (B better, 84%) and magenta below zero (A better, 16%).

- **Title (bold 15px, `#1a5276`, top center):** "The Lift B − A: 84% of the Curve Says B Is Better".
- **Data:** x = lift −2.5 to +4.5 points in 0.5 steps (15 points); heights `[0.003, 0.014, 0.053, 0.152, 0.346, 0.624, 0.889, 1.000, 0.889, 0.624, 0.346, 0.152, 0.053, 0.014, 0.003]` (illustrative, peak at +1.0).
- **Layout:** axis origin x=60, plot width 620, baseline y=245, chart height 185; x maps −2.5→+4.5; y maps 0→1.05.
- **Curve:** ink `#1a5276` 3px smooth polyline through all 15 points.
- **Shading:** area under the curve for x ≥ 0 filled `rgba(0,131,0,0.25)`; area for x < 0 filled `rgba(213,81,129,0.25)`; region boundary at x = 0.
- **Zero line:** dashed `#1a5276` (dash 4/3) vertical line at x=0 from baseline to y=50.
- **Axis labels:** ticks at −2, −1, 0, +1, +2, +3, +4 labeled 12px `#444`; small tick marks on the baseline.
- **Annotations:** green `#008300` bold 13px "84%: B better" centered over the green area (near x≈+1.5, y≈110); magenta `#d55181` bold 12px "16%: A better" over the magenta area (near x≈−1.2, y≈200).
- **Caption (12px `#444`, bottom right):** "lift = B − A in percentage points; mean +1.0, 95% between −1.0 and +3.0 (illustrative)".

## The Same Data, Two Verdicts

**Tags:** `where it's used` (blue), `p-value contrast` (orange)

- **The p-value** — a classical test on the same 50-vs-60 counts gives p = 0.33: "not significant"
- **What p means** — if the buttons were identical, data at least this extreme appears 33% of the time
- **What 84% means** — given the data actually seen, there is an 84% chance B truly converts better
- **The decision** — "84% B is better" feeds a call (ship B if switching is cheap); 0.33 feeds nothing
- **Any look** — P(B better) is meaningful at any week, but ship-on-first-cross rules still need care

*Example (italic):* Week by week P(B better) climbs 65% → 71% → 78% → 84%, while the p-value only drifts down 0.70 → 0.58 → 0.43 → 0.33.

**Key point:** A p-value measures how surprising the data is under "no difference". The Bayesian number measures what the shop cares about: which button is better, and by how much.

### Visualization (canvas `c3`, 720×300)

Line chart over four weekly check-ins: P(B better) rising in green, the p-value (shown as a percent) falling in orange, with a dashed 95% decision bar.

- **Title (bold 15px, `#1a5276`, top center):** "Four Weekly Looks at the Same Test: P(B Better) vs p-Value".
- **Data:** weeks `["W1", "W2", "W3", "W4"]`; cumulative conversions A `[13, 26, 38, 50]` and B `[15, 30, 45, 60]` out of `[250, 500, 750, 1000]` visitors per arm; P(B better) % `[65, 71, 78, 84]`; p-value ×100 `[70, 58, 43, 33]` (illustrative).
- **Layout:** axis origin x=60, plot width 600, baseline y=245, chart height 185; y scale 0–100; week points evenly spaced at x = 130, 290, 450, 610.
- **Green line:** P(B better) in `#008300`, 3px with 5px dots; bold 12px green value labels "65%", "71%", "78%", "84%" above each dot.
- **Orange line:** p-value in `#d95926`, 3px with 5px dots; bold 12px orange labels "p=.70", "p=.58", "p=.43", "p=.33" below each dot.
- **Decision bar:** dashed `#1a5276` (dash 4/3) horizontal line at y for 95, labeled bold 12px ink "95% decision bar" at its right end.
- **Axis labels:** week labels 12px `#444` below baseline; y ticks 0, 25, 50, 75, 100 labeled 11px `#6b7280` with light `#e5e9ef` gridlines.
- **Annotations:** green bold 13px "chance B is better keeps climbing" near the green line's midpoint; orange bold 12px "still 'not significant'" near the last p dot.
- **Caption (12px `#444`, bottom center):** "cumulative data: A 13/250 → 50/1,000, B 15/250 → 60/1,000 (illustrative)".

## 84% Is Not 1 Minus the p-Value

**Tags:** `common mistake` (red), `rule of thumb` (green)

- **The trap** — with p = 0.33 people compute 1 − 0.33 = 67% and call it "confidence B is better"
- **Wrong number** — the actual probability B is better is 84%; the two measure different things
- **The prior** — 84% assumes a flat prior; a skeptic who expects no difference gets about 76%
- **Direction only** — 84% is the chance B is better *at all*, not the chance the lift is a full point
- **Say it out loud** — a Bayesian report should state its prior right next to its probability

*Example (italic):* Two analysts, same 50-vs-60 data: a flat prior says 84%, a skeptical prior says 76% — both honest once the prior is stated.

**Common mistake:** Reading 1 − p as the probability the variant is better. A p-value is computed assuming no difference exists, so it can never be flipped into the probability that a difference exists.

### Visualization (canvas `c4`, 720×300)

Three horizontal bars comparing numbers people call "the chance B is better": the illegitimate 1 − p, and two legitimate posteriors under different priors.

- **Title (bold 15px, `#1a5276`, top center):** "Three Numbers People Call 'Chance B Is Better'".
- **Data:** bars `[67, 84, 76]` with left labels "1 − p (p = 0.33)", "posterior, flat prior", "posterior, skeptical prior" (illustrative).
- **Layout:** bar rows at y = 85, 145, 205, each 30px tall; labels right-aligned 12px `#444` ending at x=245; bars start x=260, full scale (100%) = 420px wide; light `#e5e9ef` vertical gridlines at 0/25/50/75/100% with 11px `#6b7280` tick labels at y=262.
- **Bar 1 (1 − p):** fill `rgba(213,81,129,0.45)`, 2px dashed `#d55181` border; bold 13px magenta value "67%" at bar end; bold 12px magenta tag "not the chance B is better" to its right.
- **Bar 2 (flat prior):** fill `rgba(0,131,0,0.45)`, value "84%" bold 13px green `#008300` at bar end.
- **Bar 3 (skeptical prior):** fill `rgba(25,158,112,0.45)`, value "76%" bold 13px aqua `#199e70` at bar end.
- **Takeaway (bold 13px magenta `#d55181`, centered at y=290):** "a p-value can't be flipped into 'chance B wins' — only a posterior says that, and its prior comes with it".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- All curve heights and probabilities are hardcoded illustrative arrays — no `Math.random()`, no runtime statistics; draw the polylines and shaded regions directly from the arrays above.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
