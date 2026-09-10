# Monte Carlo Methods

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Monte Carlo Methods

**Subtitle:** Throw darts at random, count where they land, and the count itself becomes the answer — random sampling as a calculator for problems too hard to solve exactly

## Throwing Darts at a Square Board

**Tags:** `core idea` (blue), `random sampling` (green), `area by counting` (orange)

- **The board** — a 1×1 square wall with a quarter circle of radius 1 painted from one corner
- **The throw** — darts land anywhere on the square with no aim; every spot is equally likely
- **The count** — 16 of 20 darts land inside the painted curve: a fraction of 16/20 = 0.80
- **The areas** — the quarter circle covers π/4 of the square, so the inside fraction sits near π/4
- **The estimate** — multiply by 4: π ≈ 4 × 0.80 = 3.20, from nothing but counting darts
- **The name** — this is a Monte Carlo method: estimate a number by sampling at random and counting

*Example (italic):* Nobody measured anything — 20 random darts and one division put π within 0.06 of its true value 3.14.

**Key point:** A Monte Carlo method turns a hard quantity into a counting problem: sample at random, count the hits, and the hit fraction estimates the answer.

### Visualization (canvas `c1`, 720×300)

Scatter of 20 hardcoded darts on a 1×1 board with an inscribed quarter circle; inside darts green, outside darts orange, tally block on the left.

- **Title (bold 15px, `#1a5276`, top center):** "20 Darts on a 1×1 Board: 16 Land Inside the Quarter Circle (illustrative)".
- **Board:** square from (250, 35) to (490, 275), 2px `#999` border; quarter circle centered at the bottom-left corner (250, 275), radius 240, arc from (490, 275) up to (250, 35), stroked blue `#2a78d6` 2px, region under the arc filled `rgba(42,120,214,0.10)`.
- **Mapping:** dart (x, y) in [0,1]² plots at px = 250 + x·240, py = 275 − y·240.
- **Inside darts (16, green `#008300` 5px dots):** (0.12,0.31), (0.25,0.62), (0.40,0.18), (0.55,0.47), (0.08,0.77), (0.33,0.85), (0.61,0.22), (0.72,0.40), (0.18,0.52), (0.47,0.70), (0.66,0.58), (0.29,0.09), (0.52,0.33), (0.83,0.28), (0.38,0.55), (0.15,0.90).
- **Outside darts (4, orange `#d95926` 5px dots):** (0.92,0.65), (0.78,0.81), (0.95,0.44), (0.60,0.88).
- **Tally block (left, from x=40, starting y=90, 13px):** legend dot + "inside curve: 16" in green, dot + "outside: 4" in orange, then "fraction inside = 16/20 = 0.80" in `#444`, then bold 14px ink `#1a5276` "π ≈ 4 × 0.80 = 3.20".
- **Annotation (bold 12px blue `#2a78d6`, right of the arc near (500, 120)):** "curve area = π/4" with a short arrow into the filled region.
- **Caption (12px `#444`, bottom center):** "quarter circle area π/4 ≈ 0.785 of the square — so darts inside ≈ 78.5% in the long run".

## Twenty Darts, Counted by Hand

**Tags:** `worked example` (blue), `running estimate` (green)

- **The recipe** — after every dart, the estimate is 4 × (darts inside) ÷ (darts thrown), nothing more
- **Dart 5** — 4 of the first 5 land inside: estimate 4 × 4/5 = 3.20
- **Dart 10** — a cold streak leaves 7 of 10 inside and drags the estimate down to 2.80
- **Dart 14** — 11 of 14 inside gives 4 × 11/14 = 3.14, briefly dead-on by pure luck
- **Dart 20** — 16 of 20 inside: the estimate settles at 3.20, off from π by 0.06

*Example (italic):* Dart 3 misses the curve and the estimate crashes from 4.00 to 2.67 — one dart moves the answer a lot early on.

**Key point:** The estimate is recomputed after every dart by the same one-line formula; it lurches wildly at first and calms down as the count grows.

### Visualization (canvas `c2`, 720×300)

Line chart of the running estimate 4 × inside/thrown after each of the 20 darts, with a dashed true-π reference line.

- **Title (bold 15px, `#1a5276`, top center):** "Running Estimate After Each Dart: 4 × inside ÷ thrown".
- **Data:** darts 1–20; cumulative inside `[1, 2, 2, 3, 4, 4, 5, 5, 6, 7, 8, 9, 10, 11, 12, 13, 13, 14, 15, 16]`; running estimate `[4.00, 4.00, 2.67, 3.00, 3.20, 2.67, 2.86, 2.50, 2.67, 2.80, 2.91, 3.00, 3.08, 3.14, 3.20, 3.25, 3.06, 3.11, 3.16, 3.20]`.
- **Axes:** origin x=60, plot width 610, baseline y=250, plot height 195; y scale 2.4–4.2 with 12px `#444` gridline labels at 2.5, 3.0, 3.5, 4.0 (gridlines 1px `#e5e9ef`); x ticks every dart, labels 1, 5, 10, 15, 20 in 12px `#444`; x-axis caption 12px `#444` "darts thrown".
- **Estimate line:** blue `#2a78d6` 3px polyline with 4px dots at every dart.
- **π reference:** dashed (dash 5/4) ink `#1a5276` 2px horizontal line at y for 3.1416, right-end label bold 12px ink "π = 3.14".
- **Annotation 1 (bold 12px orange `#d95926`, near dart 3 point):** "one early miss: 4.00 → 2.67".
- **Annotation 2 (bold 13px green `#008300`, near dart 20 point):** "20 darts: 3.20 (off by 0.06)".

## More Darts, Better Estimate

**Tags:** `where it's used` (blue), `rule of thumb` (green), `convergence` (orange)

- **The pattern** — 100 darts gave 3.24, then 1,000 gave 3.172, and 100,000 gave 3.1444
- **Slow gains** — the wobble shrinks like 1/√N: 100× more darts buys only 10× more precision
- **The trade** — you swap an impossible exact formula for an easy approximate count
- **Where it lives** — risk simulation, option pricing, Bayesian posteriors, and A/B power checks
- **Same skeleton** — every one of them is still "sample at random, count hits, average the result"

*Example (italic):* A bank cannot write a formula for "probability the portfolio loses over $1M", so it simulates 100,000 random market days and counts them.

**Key point:** Monte Carlo trades precision for possibility — accuracy improves only as 1/√N, but it works on problems where no exact formula exists at all.

### Visualization (canvas `c3`, 720×300)

Convergence line chart: the π estimate at five increasing dart counts on a log-spaced x-axis, closing in on the dashed true value.

- **Title (bold 15px, `#1a5276`, top center):** "Estimate of π vs Number of Darts (illustrative runs)".
- **Data:** N values `[10, 100, 1000, 10000, 100000]` with hits inside `[7, 81, 793, 7875, 78610]` and estimates `[2.80, 3.24, 3.172, 3.1500, 3.1444]`.
- **Axes:** origin x=65, plot width 590, baseline y=245, plot height 190; x positions equally spaced per decade (log spacing): x = 65 + i·147.5 for i = 0..4; x tick labels "10", "100", "1k", "10k", "100k" in 12px `#444`, axis caption 12px `#444` "darts thrown (log scale)"; y scale 2.6–3.5 with 12px `#444` labels at 2.6, 2.8, 3.0, 3.2, 3.4 and 1px `#e5e9ef` gridlines.
- **Estimate line:** green `#008300` 3px polyline with 5px dots; each dot labeled with its estimate in bold 12px green above (or below when the point is above the π line): "2.80", "3.24", "3.172", "3.1500", "3.1444".
- **π reference:** dashed (dash 5/4) ink `#1a5276` 2px horizontal line at 3.1416, right-end label bold 12px ink "π = 3.1416".
- **Annotation (bold 13px orange `#d95926`, upper right around (430, 70)):** "100× more darts ≈ 10× closer".

## Same Recipe, Different Answer Each Run

**Tags:** `common mistake` (red), `rule of thumb` (green)

- **Rerun it** — five separate 100-dart runs gave 3.04, 3.28, 3.12, 2.96, 3.20 — all correct runs
- **Not a bug** — the spread is the method: a Monte Carlo answer is a random number near the truth
- **Report spread** — quote estimate ± wobble (about ±0.16 at 100 darts), never the bare number
- **Trust digits** — 100 darts earn roughly one reliable digit; 10,000 earn about two
- **Fix the seed** — record the random seed so any run can be reproduced exactly later

*Example (italic):* An analyst reported "π = 3.28" from one 100-dart run to two decimals — the next run said 2.96 and both were working as intended.

**Common mistake:** Reading precise digits off one small run. The output is itself random; without the spread across runs, the extra decimals are decoration.

### Visualization (canvas `c4`, 720×300)

Two vertical dot strips comparing five repeated runs at N=100 (wide scatter) and five at N=10,000 (tight cluster) against the dashed true π.

- **Title (bold 15px, `#1a5276`, top center):** "Five Repeated Runs: 100 Darts vs 10,000 Darts (illustrative)".
- **Data:** N=100 estimates `[3.04, 3.28, 3.12, 2.96, 3.20]` (range 0.32); N=10,000 estimates `[3.1468, 3.1272, 3.1520, 3.1380, 3.1424]` (range 0.025).
- **Axes:** shared y scale 2.85–3.40, plot from y=45 to y=250; y labels 12px `#444` at 2.9, 3.0, 3.1, 3.2, 3.3 with 1px `#e5e9ef` gridlines from x=70 to x=650.
- **Left strip (N=100):** center x=230; five orange `#d95926` 6px dots at the five estimates, jittered horizontally at x offsets `[-18, -9, 0, 9, 18]`; below baseline, bold 12px `#444` label "100 darts per run"; orange bracket (2px) to the right of the dots spanning 2.96–3.28 with bold 12px orange label "spread 0.32".
- **Right strip (N=10,000):** center x=500; five green `#008300` 6px dots at the five estimates, same x-offset jitter; label "10,000 darts per run"; green bracket spanning 3.1272–3.1520 with bold 12px green label "spread 0.025".
- **π reference:** dashed (dash 5/4) ink `#1a5276` 2px horizontal line across the plot at 3.1416, right-end label bold 12px ink "π = 3.1416".
- **Takeaway (bold 13px magenta `#d55181`, bottom center at y=290):** "every run is right — the answer comes with a wobble, so report the spread too".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- All dart positions, cumulative counts, and run estimates are fixed illustrative arrays — no `Math.random()` anywhere; every number in the text bullets matches its chart data.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
