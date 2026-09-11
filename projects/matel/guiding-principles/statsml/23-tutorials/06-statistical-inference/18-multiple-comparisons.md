# Multiple Comparisons

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Multiple Comparisons

**Subtitle:** Every extra test buys another lottery ticket for a false alarm — run twenty and a phantom "winner" is more likely than not, unless you tighten the bar with Bonferroni or FDR

## Twenty Button Colors, One Phantom Winner

**Tags:** `core idea` (blue), `false positive` (red), `worked example` (green)

- **The shop** — an online store A/B tests 20 checkout-button colors; none actually changes sales
- **The rule** — each test calls a color a "winner" when its p-value lands below 0.05
- **The catch** — even with zero real effects, each test has a 5% chance of a fluke win
- **The phantom** — color #12 (teal) comes back p = 0.03 and the team celebrates a fake winner
- **The count** — at 0.05 per test, 20 tests are expected to produce about 20 × 0.05 = 1 fluke

*Example (italic):* The teal button's p = 0.03 looks convincing alone — but it is exactly the one fluke that twenty tests of do-nothing colors were expected to produce.

**Key point:** A p-value guards one test. Run many tests and the 5% fluke chances stack up — some "discovery" is almost guaranteed even when nothing is real.

### Visualization (canvas `c1`, 720×300)

Lollipop chart of the 20 p-values from the button-color tests, with a dashed significance line at 0.05 and the one fluke below it highlighted.

- **Title (bold 15px, `#1a5276`, top center):** "20 Tests of Do-Nothing Colors: One Fluke Under p = 0.05".
- **Data:** colors numbered 1–20 with p-values `[0.72, 0.31, 0.55, 0.09, 0.88, 0.44, 0.63, 0.21, 0.97, 0.38, 0.51, 0.03, 0.77, 0.26, 0.69, 0.14, 0.83, 0.47, 0.59, 0.92]` (illustrative).
- **Axes:** origin x=60, plot width 620, baseline y=245, plot height 185; y scale 0–1 with ticks 0, 0.25, 0.5, 0.75, 1 labeled 12px `#6b7280` at left; x positions evenly spaced, color numbers 1–20 as 11px `#444` labels below baseline.
- **Lollipops:** 2px stem from baseline to value plus 5px dot; blue `#2a78d6` for all except #12, which is magenta `#d55181` with a 7px dot.
- **Threshold line:** dashed red `#e74c3c` (dash 5/4) horizontal at p = 0.05, labeled "p = 0.05" bold 12px red at the right end.
- **Annotation:** magenta bold 13px centered in the strip above the plot (y≈48) with a long arrow down to color #12: "teal, p = 0.03 — the phantom winner".
- **Caption (12px `#444`, bottom left):** "all 20 colors truly do nothing (illustrative p-values)".

## How Fast the Phantom Odds Grow

**Tags:** `worked example` (blue), `rule of thumb` (green)

- **One test** — with a 0.05 cutoff, a single do-nothing test falsely "wins" 5% of the time
- **Stacking** — the chance of at least one fluke in m tests is 1 − 0.95^m, and it climbs fast
- **Five tests** — 1 − 0.95^5 = 23%, already almost a coin-flip's worth of phantom risk
- **Fourteen tests** — the curve crosses 50% at m = 14; a phantom is now more likely than not
- **Twenty tests** — 1 − 0.95^20 = 64%; twenty tests nearly guarantee at least one fake winner

*Example (italic):* The store's 20-color experiment carried a 64% chance of at least one phantom — the teal fluke was the expected outcome, not bad luck.

**Key point:** The family-wise false-alarm rate is 1 − 0.95^m, not 5%. It hits 23% by five tests, passes 50% at fourteen, and reaches 64% at twenty.

### Visualization (canvas `c2`, 720×300)

Line chart of the family-wise false-positive probability 1 − 0.95^m for m = 1 to 20 tests, with callouts at m = 1, 5, 14, and 20.

- **Title (bold 15px, `#1a5276`, top center):** "Chance of At Least One Fluke vs Number of Tests (α = 0.05)".
- **Data:** m = 1..20; probabilities `[0.05, 0.10, 0.14, 0.19, 0.23, 0.26, 0.30, 0.34, 0.37, 0.40, 0.43, 0.46, 0.49, 0.51, 0.54, 0.56, 0.58, 0.60, 0.62, 0.64]` (= 1 − 0.95^m, rounded).
- **Axes:** origin x=65, plot width 600, baseline y=245, plot height 185; y scale 0–0.7 with ticks 0%, 25%, 50% labeled 12px `#6b7280`; x ticks at m = 1, 5, 10, 15, 20 labeled 12px `#444`; x-axis caption 12px `#444` "number of tests (m)".
- **Curve:** blue `#2a78d6` 3px line through all 20 points, 4px dots at m = 1, 5, 14, 20.
- **Reference line:** dashed `#bdc3c7` (dash 4/3) horizontal at 50%, labeled "50%" 11px `#6b7280`.
- **Callouts:** bold 12px labels at the four marked dots — "1 test: 5%" (blue), "5 tests: 23%" (blue), "14 tests: 51% — flukes now favored" (orange `#d95926`), "20 tests: 64%" (magenta `#d55181`).
- **Caption (12px `#444`, bottom left):** "1 − 0.95^m: each test keeps its own 5% fluke chance".

## Two Fixes: Bonferroni and FDR

**Tags:** `core idea` (blue), `Bonferroni` (orange), `FDR` (green)

- **Bonferroni** — split the budget: with 20 tests, require p below 0.05 / 20 = 0.0025 for each
- **The effect** — the teal fluke at 0.03 fails 0.0025 easily; the phantom winner disappears
- **The cost** — Bonferroni is strict; modest real effects can fail the 0.0025 bar too
- **FDR (BH)** — sort p-values and give rank i the looser bar i × 0.0025; keep hits up to the last pass
- **Next quarter** — 3 colors truly help: p-values 0.001, 0.004, 0.007 all pass FDR; Bonferroni keeps only 0.001

*Example (italic):* On the quarter with three real winners, naive 0.05 keeps four hits (one phantom at 0.03), Bonferroni keeps one, and FDR keeps exactly the three real ones.

**Key point:** Bonferroni controls the chance of ANY fluke and is strict; FDR controls the FRACTION of your keepers that are flukes and retains more real effects.

### Visualization (canvas `c3`, 720×300)

Dot plot of the six smallest sorted p-values from the second-quarter experiment, with three decision lines: naive 0.05, Bonferroni 0.0025, and the rising Benjamini–Hochberg (FDR) ramp.

- **Title (bold 15px, `#1a5276`, top center):** "Sorted p-Values vs Three Bars: Naive, Bonferroni, FDR".
- **Data:** ranks i = 1..6 of the 20 sorted p-values: `[0.001, 0.004, 0.007, 0.030, 0.080, 0.140]`; BH ramp thresholds i × (0.05 / 20): `[0.0025, 0.005, 0.0075, 0.010, 0.0125, 0.015]`.
- **Axes:** origin x=70, plot width 590, baseline y=245, plot height 185; y is a log scale from 0.0005 to 0.15 with gridline ticks at 0.001, 0.005, 0.02, 0.05, 0.15 labeled 12px `#6b7280`; x positions evenly spaced, labels "rank 1".."rank 6" 12px `#444` below baseline; x-axis caption 12px `#444` "six smallest of 20 p-values, sorted — log y-scale (illustrative)".
- **Naive line:** dashed red `#e74c3c` (dash 5/4) horizontal at 0.05, labeled "naive 0.05" bold 12px red at right.
- **Bonferroni line:** solid orange `#d95926` 2px horizontal at 0.0025, labeled "Bonferroni 0.05/20 = 0.0025" bold 12px orange.
- **FDR ramp:** green `#008300` 2.5px line through the six BH thresholds, labeled "FDR ramp i × 0.0025" bold 12px green above its right end.
- **Dots:** 7px; ranks 1–3 green `#008300` (pass FDR; real effects), rank 4 magenta `#d55181` (p = 0.030 — passes only the naive bar), ranks 5–6 gray `#6b7280`; each dot labeled with its p-value 11px `#444`.
- **Annotation (bold 13px):** green "FDR keeps 3" near rank 3, orange "Bonferroni keeps 1" near rank 1, magenta "phantom: naive only" near rank 4.

## Reporting Only the Winner

**Tags:** `common mistake` (red), `where it's used` (orange)

- **The slide** — the report shows one chart: "teal button, p = 0.03, significant" — the 19 losers vanish
- **The reader** — anyone seeing one test at p = 0.03 reasonably assumes a 5% fluke risk, not 64%
- **The rule** — the correction must count every test RUN, not just the ones that got reported
- **Hidden tests** — segments, metrics, and weekly re-checks are comparisons too, and they stack the same way
- **The check** — always ask "how many tests were run in total?" before trusting any single winner

*Example (italic):* A dashboard that slices two metrics by 10 segments has quietly run 20 comparisons — its "significant" segment is the teal button again.

**Common mistake:** Correcting only the comparisons that made it into the report. Every test run — every segment, metric, and peek — buys a lottery ticket, whether or not it is shown.

### Visualization (canvas `c4`, 720×300)

Two-panel comparison: the reported story (one lonely significant test) vs the full story (all 20 tests with the expected fluke count), split by a vertical dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "What the Slide Shows vs What Was Actually Run".
- **Left panel ("the slide"):** heading bold 13px `#444` "reported: 1 test" at top left of panel; a single magenta `#d55181` lollipop (2px stem, 7px dot) for teal at p = 0.03, x centered near 190; axis origin x=80, plot width 220, baseline y=235, plot height 160, y scale 0–0.25 with tick 0.05; dashed red `#e74c3c` line at 0.05 labeled "0.05" 11px red; blue bold 13px caption under panel: "looks like a 5% fluke risk".
- **Right panel ("the truth"):** heading bold 13px `#444` "actually run: 20 tests"; the section-1 p-values `[0.72, 0.31, 0.55, 0.09, 0.88, 0.44, 0.63, 0.21, 0.97, 0.38, 0.51, 0.03, 0.77, 0.26, 0.69, 0.14, 0.83, 0.47, 0.59, 0.92]` as small 4px dots (blue `#2a78d6`, teal #12 magenta 6px), axis origin x=400, plot width 280, baseline y=235, plot height 160, y scale 0–1; dashed red line at 0.05; magenta bold 13px caption under panel: "real fluke risk: 64%".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.
- **Takeaway (bold 13px `#d95926`, centered at y=290):** "expected flukes at 0.05: 20 × 0.05 = 1 — the slide is showing you that one".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- All data arrays are hardcoded literals (no `Math.random()`); invented numbers carry an "illustrative" caption label. In regenerated HTML, any card links would use `.html` extensions (this page has no links).
