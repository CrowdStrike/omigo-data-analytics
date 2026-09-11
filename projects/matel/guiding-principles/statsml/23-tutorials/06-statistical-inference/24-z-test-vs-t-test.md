# Z-Test vs T-Test

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Z-Test vs T-Test

**Subtitle:** Both tests ask the same question — is my sample mean too far from the target? The z-test assumes you KNOW the spread; the t-test admits you estimated it, and pays for that honesty with fatter tails

## Nine Espresso Shots and One Question

**Tags:** `core idea` (blue), `running example` (orange), `σ known vs estimated` (green)

- **The shop** — a café's espresso machine is supposed to dose 18.0 g of coffee per shot
- **The check** — the barista weighs 9 shots: 17.4, 17.6, 17.8, 18.2, 18.6, 19.0, 19.4, 19.6, 19.8 g
- **The average** — the sample mean is 18.6 g, sitting 0.6 g above the 18.0 g target
- **The question** — is 0.6 g a real drift in the machine, or just normal shot-to-shot wobble?
- **Road one (z)** — the manufacturer's spec sheet says σ = 0.9 g, so the spread is KNOWN
- **Road two (t)** — no spec sheet: estimate the spread from the 9 shots themselves (s = 0.9 g)

*Example (italic):* Same nine weights, same mean of 18.6 g — the only difference is whether the 0.9 came from the spec sheet or from the shots.

**Key point:** The z-test and t-test differ in exactly one input: where the spread number comes from. Known σ → z-test; σ estimated from the sample → t-test.

### Visualization (canvas `c1`, 720×300)

Horizontal dot plot of the nine shot weights on a grams axis, with a dashed target line at 18.0 g and a solid mean line at 18.6 g.

- **Title (bold 15px, `#1a5276`, top center):** "Nine Espresso Shots: Target 18.0 g, Sample Mean 18.6 g".
- **Data:** weights `[17.4, 17.6, 17.8, 18.2, 18.6, 19.0, 19.4, 19.6, 19.8]` (mean 18.6, sample sd s = 0.9 exactly).
- **Axis:** horizontal 2px `#999` line at y=185 from x=70, width 580, mapping grams 17.0 → 20.0; ticks with labels "17", "18", "19", "20" (12px `#444`) below.
- **Dots:** blue `#2a78d6` 7px circles on the axis at each weight; the tied visual spacing is real (values are distinct so no jitter needed).
- **Target line:** dashed green `#008300` (dash 5/4) vertical line at 18.0 from y=60 to y=205; green bold 12px label "target 18.0 g" above its top.
- **Mean line:** solid magenta `#d55181` 2px vertical line at 18.6 from y=60 to y=205; magenta bold 13px label "sample mean 18.6 g" above its top (right-aligned to avoid the target label).
- **Gap bracket:** orange `#d95926` 3px horizontal bracket from 18.0 to 18.6 at y=95 with bold 12px label "gap = 0.6 g" above it.
- **Caption (12px `#444`, bottom center):** "spread of the 9 shots: s = 0.9 g — same number as the spec-sheet σ, by design".

## The Same 2.0 Gets Two Different Verdicts

**Tags:** `worked example` (blue), `rule of thumb` (green)

- **Standard error** — with n = 9, SE = 0.9 / √9 = 0.3 g on either road, spec sheet or sample
- **The statistic** — (18.6 − 18.0) / 0.3 = 2.0, identical arithmetic for both tests
- **z verdict** — the normal cutoff is 1.96, so z = 2.0 clears it: p ≈ 4.6%, significant
- **t verdict** — the t cutoff with df = 8 is 2.306, so t = 2.0 falls short: p ≈ 8.1%, not significant
- **Same data** — nothing changed but the reference curve the 2.0 is compared against

*Example (italic):* The barista computes 2.0 once, then gets "the machine drifted" from the z-table and "could be luck" from the t-table.

**Key point:** The test statistic is the same number; the verdict differs because the t distribution demands a bigger value (2.306 vs 1.96) before calling a 9-shot result real.

### Visualization (canvas `c2`, 720×300)

Dual-panel density chart: the observed statistic 2.0 against the standard normal curve (left, z-test) and against the t distribution with 8 df (right, t-test), split by a vertical dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "Statistic = 2.0: z-Test Says Yes, t-Test Says Not Yet".
- **Curve formulas (deterministic, no randomness):** normal pdf `0.3989 * exp(-x*x/2)`; t(8) pdf `0.3867 * Math.pow(1 + x*x/8, -4.5)`; both evaluated at x from −4 to 4 in steps of 0.05.
- **Left panel (z-test):** axis origin x=55, width 280, baseline y=235, chart height 165, y scale 0–0.42; blue `#2a78d6` 2.5px normal curve; shade the rejection tails beyond ±1.96 with `rgba(42,120,214,0.30)`; dashed `#6b7280` vertical cutoff lines at ±1.96 with 11px labels "−1.96" and "1.96"; solid orange `#d95926` 2.5px marker line at x=2.0 with a 6px dot on the curve; orange bold 12px annotation, two lines: "z = 2.0 > 1.96" / "significant, p ≈ 4.6%"; caption 12px `#444` "normal curve — σ known from spec sheet".
- **Right panel (t-test):** axis origin x=400, width 280, same baseline/height/scale; green `#008300` 2.5px t(8) curve; shade tails beyond ±2.306 with `rgba(0,131,0,0.28)`; dashed cutoffs at ±2.306 labeled "−2.31" and "2.31"; the same orange marker line at x=2.0; green bold 12px annotation, two lines: "t = 2.0 < 2.31" / "not significant, p ≈ 8.1%"; caption "t curve, df = 8 — σ estimated from 9 shots".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.

## Why Small Samples Get Fatter Tails

**Tags:** `core idea` (blue), `where it's used` (orange)

- **The hidden wobble** — s = 0.9 is itself computed from 9 shots, so s wobbles from sample to sample
- **Unlucky spreads** — a 9-shot sample can easily produce s = 0.6, which inflates the statistic to 3.0
- **Two uncertainties** — the t curve stacks the mean's wobble AND the spread's wobble into one shape
- **Fatter tails** — big statistics happen more often by luck, so the t curve holds more tail area
- **Tail areas at 2.0** — beyond ±2.0: normal 4.6%, t with df 8 is 8.1%, t with df 3 is 13.9%
- **Convergence** — more shots pin down s, so the t curve tightens toward the normal as n grows

*Example (italic):* With only 4 shots (df = 3), a statistic of 2.0 happens by pure luck 13.9% of the time — triple the normal curve's 4.6%.

**Key point:** Fatter tails are not a fudge factor — they are the exact price of using a spread you estimated instead of one you knew. Fewer observations, wobblier s, fatter tails.

### Visualization (canvas `c3`, 720×300)

Single-panel overlay of three density curves — standard normal, t(8), and t(3) — with the tail region beyond |2.0| annotated with each curve's two-sided tail probability.

- **Title (bold 15px, `#1a5276`, top center):** "Fatter Tails: Normal vs t(df=8) vs t(df=3)".
- **Curve formulas (deterministic):** normal `0.3989 * exp(-x*x/2)` (blue `#2a78d6`, 2.5px); t(8) `0.3867 * Math.pow(1 + x*x/8, -4.5)` (green `#008300`, 2.5px); t(3) `0.3676 * Math.pow(1 + x*x/3, -2)` (magenta `#d55181`, 2.5px); x from −4.5 to 4.5 step 0.05.
- **Axis:** origin x=60, width 600, baseline y=245, chart height 180, y scale 0–0.42; x ticks at −4, −2, 0, 2, 4 (12px `#444`); dashed `#6b7280` vertical lines at ±2.0 labeled "−2.0" and "2.0" (11px).
- **Legend (top left, from x=75 y=55, 12px):** color swatch + label per curve: "normal (σ known)" blue, "t, df = 8 (9 shots)" green, "t, df = 3 (4 shots)" magenta.
- **Tail annotations (right side, x≈545, stacked from y=110, bold 12px, arrowed to the right tail):** "beyond ±2.0:" in `#444`, then "normal 4.6%" blue, "t(8) 8.1%" green, "t(3) 13.9%" magenta.
- **Peak note (11px `#6b7280`, near apex):** "peaks: 0.399 / 0.387 / 0.368 — fat tails come from lowered peaks".
- **Caption (12px `#444`, bottom center):** "all three curves are centered at 0 with area 1 — only the tail weight differs".

## Which Test Do You Actually Reach For?

**Tags:** `common mistake` (red), `rule of thumb` (green)

- **The catch** — a true, known σ is rare in practice; spec sheets and "known" spreads are usually estimates too
- **Default** — when in doubt use the t-test: with σ genuinely known it costs almost nothing extra
- **Shrinking penalty** — the 5% t cutoff falls from 4.30 (df 2) to 2.31 (df 8) to 2.04 (df 30) toward 1.96
- **n ≥ 30 folklore** — by df 30 the cutoffs differ by ~4%, which is why textbooks say the tests "merge"
- **Where z survives** — proportion tests (conversion rates) use z because p̂ pins down the spread formula

*Example (italic):* An analyst z-tested 9 shots with s pretending to be σ and reported p ≈ 4.6% — the honest t-test says p ≈ 8.1%.

**Common mistake:** Plugging the sample s into a z-test as if it were a known σ. With small n this quietly shrinks the cutoff from 2.31 to 1.96 and manufactures significance that is not there.

### Visualization (canvas `c4`, 720×300)

Line chart of the two-sided 5% t cutoff versus degrees of freedom, decaying toward a dashed horizontal z line at 1.96.

- **Title (bold 15px, `#1a5276`, top center):** "The 5% Cutoff: t Approaches z as Samples Grow".
- **Data:** df values `[2, 3, 5, 8, 15, 30, 100]` at evenly spaced x positions; cutoffs `[4.30, 3.18, 2.57, 2.31, 2.13, 2.04, 1.98]`.
- **Axis:** origin x=70, width 560, baseline y=240, chart height 175, y scale 1.8–4.5; y ticks at 2.0, 2.5, 3.0, 3.5, 4.0, 4.5 (12px `#444`, light `#e5e9ef` gridlines); df labels 12px `#444` below the baseline.
- **t line:** violet `#4a3aa7` 3px line through the seven points with 5px dots; each point labeled with its cutoff value (bold 11px violet, above the dot).
- **z line:** dashed ink `#1a5276` (dash 5/4) horizontal line at y-value 1.96 spanning the axis; ink bold 12px label "z cutoff = 1.96" at its right end.
- **Highlight:** orange `#d95926` 9px ring around the df = 8 point with bold 12px annotation, two lines: "the 9-shot espresso test" / "lives here: 2.31".
- **Caption (12px `#444`, bottom center):** "by df = 30 the t cutoff is within ~4% of 1.96 — the source of the n ≥ 30 rule of thumb".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Determinism:** no `Math.random()`; density curves use the closed-form pdf constants given in each chart spec; all point data (shot weights, df values, cutoffs, tail percentages) is hardcoded exactly as listed.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
