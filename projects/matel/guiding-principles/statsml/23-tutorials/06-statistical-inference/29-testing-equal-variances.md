# Testing Equal Variances

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Testing Equal Variances

**Subtitle:** Levene's test checks whether two groups scatter equally around their centers — the precondition that decides between Student's t and Welch's t

## Two Couriers, One Average, Two Spreads

**Tags:** `core idea` (blue), `variance` (green), `spread vs center` (orange)

- **The couriers** — Courier A and Courier B each made 10 deliveries and both average 31 minutes
- **Spread A** — A's times run 28 to 34 minutes, a standard deviation of about 1.8 minutes
- **Spread B** — B's times run 16 to 43 minutes, a standard deviation of about 8.8 minutes
- **Variance** — squaring the spread gives variance: about 3.3 for A versus 78 for B, a 23× gap
- **The question** — "equal variances" asks whether two groups scatter equally around their centers

*Example (italic):* You can plan lunch around Courier A's 31-minute average, but Courier B's identical average can mean 16 minutes or 43.

**Key point:** Two groups can share the same mean and still behave completely differently. Equal variance is a claim about spread, and it needs its own test — separate from any test of means.

### Visualization (canvas `c1`, 720×300)

Two horizontal dot strips showing every delivery time for Courier A (top) and Courier B (bottom) on a shared minutes axis, with a dashed vertical line at the common mean of 31.

- **Title (bold 15px, `#1a5276`, top center):** "Same 31-Minute Average, Very Different Spread".
- **Data:** Courier A times `[28, 29, 30, 30, 31, 31, 32, 32, 33, 34]`; Courier B times `[16, 21, 25, 28, 30, 32, 35, 38, 42, 43]` (minutes).
- **Axis:** horizontal 2px `#999` line at y=250 from x=70 to x=650, mapping 10–50 minutes linearly; ticks with 12px `#444` labels at 10, 15, 20, 25, 30, 35, 40, 45, 50.
- **Courier A strip (y=110):** blue `#2a78d6` 7px dots; duplicate values (30, 31, 32) stack a second dot 12px above the first; row label bold 13px blue "Courier A" at x=70 above the strip; blue bold 12px annotation "sd ≈ 1.8 min" to the right of the cluster.
- **Courier B strip (y=195):** orange `#d95926` 7px dots (no duplicates); row label bold 13px orange "Courier B" at x=70 above the strip; orange bold 12px annotation "sd ≈ 8.8 min" near the right end.
- **Mean line:** dashed `#1a5276` (dash 4/3) vertical line at the x of 31 minutes from y=55 to y=250; ink bold 12px label "both means = 31 min" beside its top.
- **Caption (12px `#444`, bottom center):** "10 deliveries each; variance 3.3 vs 78 — a 23× gap in spread".

## Levene's Trick: Distances From the Median

**Tags:** `worked example` (blue), `levene's test` (green)

- **Center first** — both couriers have median 31, so subtract 31 from every delivery time
- **Drop the sign** — keep absolute deviations: A gives 3,2,1,1,0,0,1,1,2,3 and B gives 15,10,6,3,1,1,4,7,11,12
- **Average them** — A's deliveries sit 1.4 minutes from the median on average; B's sit 7.0 away
- **The trick** — Levene's test is an ordinary means test (t / ANOVA) run on those distances
- **The score** — here t ≈ 3.6 (F ≈ 12.7), p ≈ 0.002: the 1.4 vs 7.0 gap is no accident
- **Reading it** — a small p rejects "equal variances"; the two spreads genuinely differ

*Example (italic):* Courier B's 16-minute delivery becomes |16 − 31| = 15, the largest distance Levene feeds into its means test.

**Key point:** Levene's test is a means test in disguise: turn spread into a per-row number (distance from the median — the robust standard centering), then test whether those distances have equal averages.

### Visualization (canvas `c2`, 720×300)

Bar chart of all 20 absolute deviations — Courier A's ten bars on the left, Courier B's ten on the right — with a dashed group-mean line over each block.

- **Title (bold 15px, `#1a5276`, top center):** "Levene's Input: Each Delivery's Distance From the Median (31)".
- **Data:** Courier A distances `[3, 2, 1, 1, 0, 0, 1, 1, 2, 3]` (mean 1.4); Courier B distances `[15, 10, 6, 3, 1, 1, 4, 7, 11, 12]` (mean 7.0).
- **Axes:** baseline y=245, chart height 185, y scale 0–16; y ticks 0, 4, 8, 12, 16 with 12px `#444` labels and light `#e5e9ef` gridlines; y-axis title 12px `#444` "minutes from median".
- **Courier A block:** ten bars starting x=75, bar width 20, gap 6, fill `rgba(42,120,214,0.45)`, 1px `#2a78d6` stroke; block label bold 13px blue "Courier A" centered below at y=265.
- **Courier B block:** ten bars starting x=390, same widths, fill `rgba(217,89,38,0.45)`, 1px `#d95926` stroke; block label bold 13px orange "Courier B" centered below.
- **Mean lines:** dashed blue `#2a78d6` (dash 5/3) horizontal line across the A block at the y of 1.4, bold 12px blue label "mean = 1.4"; dashed orange `#d95926` line across the B block at the y of 7.0, bold 12px orange label "mean = 7.0".
- **Annotation (bold 13px green `#008300`, upper left area):** "t ≈ 3.6, p ≈ 0.002 → spreads differ".

## Why Student's t Cares — and Welch's Doesn't

**Tags:** `where it's used` (blue), `student vs welch` (orange), `failure mode` (red)

- **Two t-tests** — Student's t pools both spreads into one shared number; Welch's t keeps them separate
- **The precondition** — pooling is only fair when the variances are equal; that is Student's assumption
- **Bad combo** — unequal spreads plus unequal group sizes push Student's t off its promised 5% error
- **Which way** — a small noisy group inflates false alarms to ~15%; a small quiet one drops them to ~1%
- **Welch** — Welch's t stays near 5% in every combination, at almost no cost when spreads are equal

*Example (italic):* With 10 noisy Courier-B-style rows against 40 quiet ones, Student's t cries "difference!" three times as often as it should.

**Key point:** Equal variances is exactly the precondition that separates Student's t from Welch's t — and Welch's is the safe choice whenever you cannot vouch for it.

### Visualization (canvas `c3`, 720×300)

Grouped bar chart: three unequal-group-size scenarios on the x-axis, each with a Student's t bar and a Welch's t bar showing the actual false-alarm rate against the promised 5% line.

- **Title (bold 15px, `#1a5276`, top center):** "False-Alarm Rate at a Promised 5%: Student's t vs Welch's t (illustrative)".
- **Data:** scenarios `["small group noisy", "equal spread", "big group noisy"]`, all with n = 10 vs 40; Student's t rates `[15, 5, 1]` %; Welch's t rates `[5, 5, 5]` %.
- **Axes:** origin x=70, baseline y=240, chart height 175, plot width 560, y scale 0–18%; y ticks 0, 5, 10, 15 with 12px `#444` labels and light `#e5e9ef` gridlines.
- **Bars:** per scenario a pair of 52px-wide bars 8px apart, pairs centered at x ≈ 175, 360, 545; Student's fill `rgba(42,120,214,0.55)` with 1px `#2a78d6` stroke, Welch's fill `rgba(0,131,0,0.45)` with 1px `#008300` stroke; each bar's rate printed bold 12px in its color above the bar ("15%", "5%", ...).
- **Nominal line:** dashed red `#e74c3c` (dash 5/3) horizontal line at the y of 5%, bold 12px red label "promised 5%" at its right end.
- **Scenario labels:** 12px `#444`, two lines under each pair (e.g. "small group noisy" / "n = 10 vs 40").
- **Legend (top right, 12px):** blue swatch "Student's t (pooled)", green swatch "Welch's t".
- **Annotation (bold 13px magenta `#d55181`, above the first pair):** "3× the promised alarms".

## The Gatekeeper Mistake

**Tags:** `common mistake` (red), `rule of thumb` (green)

- **The old recipe** — run Levene first, then pick Student's t if p > 0.05 and Welch's t otherwise
- **Problem 1** — Levene is weak at small n: with 10 rows per group it often misses a real 2× spread gap
- **Problem 2** — letting the data pick its own test distorts the final p-value of the whole two-step
- **Not proof** — Levene's p > 0.05 means "no evidence of unequal spread", never "spreads are equal"
- **Modern advice** — default to Welch for comparing means; keep Levene for when spread IS the question

*Example (italic):* A team "confirmed" equal variances with p = 0.31 from ten rows per group — a test far too weak to see the 2× gap that was really there.

**Common mistake:** Using Levene as a gatekeeper for the t-test. Skip the gate and use Welch's t by default; run Levene when spread itself is the question — like deciding which courier is more consistent.

### Visualization (canvas `c4`, 720×300)

Side-by-side flow diagram: the old two-stage "test then choose" pipeline on the left, the one-step Welch default on the right, split by a dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "Two Workflows for Comparing Means".
- **Boxes:** rounded rectangles (6px radius), 1.5px `#1a5276` border, fill `#f8f9fa`, bold 12px `#1a5276` centered text; arrows 2px `#6b7280` vertical lines with small filled triangle heads.
- **Left panel (old recipe):** "two groups of times" box (170×34, centered x=185, y=48) → arrow → "Levene's test on spreads" box (185×34, centered x=185, y=112) → two diverging arrows labeled 11px `#444` "p > 0.05" (left) and "p ≤ 0.05" (right) → "Student's t" box (110×34, centered x=110, y=186) and "Welch's t" box (110×34, centered x=262, y=186).
- **Left verdict (bold 12px red `#e74c3c`, two centered lines at y=245/y=261):** "data picks its own test —" / "the 5% promise breaks".
- **Right panel (default):** "two groups of times" box (170×34, centered x=540, y=48) → one long arrow → "Welch's t" box (130×36, 2px `#008300` border, centered x=540, y=170).
- **Right verdict (bold 12px green `#008300`, two centered lines at y=245/y=261):** "one step —" / "keeps its 5% promise".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.
- **Panel headings (bold 13px `#444`, centered at y=30):** "test, then choose" (x=185) and "just use Welch" (x=540).

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
