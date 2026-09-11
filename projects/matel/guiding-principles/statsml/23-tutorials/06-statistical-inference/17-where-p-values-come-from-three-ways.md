# Where P-Values Come From, Three Ways

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Where P-Values Come From, Three Ways

**Subtitle:** A p-value answers one question — "if nothing were going on, how often would a result this big appear by luck?" — and you can compute it three ways: a formula, a shuffle, or a simulation

## One Tip Jar, One Question

**Tags:** `core idea` (blue), `running example` (green), `null hypothesis` (orange)

- **The shop** — a café hangs a new tip-jar sign and logs daily tips: 10 old-sign days, 10 new-sign days
- **The gap** — old-sign days average $43.50, new-sign days average $49.50: a $6.00 gap
- **The skeptic** — maybe the sign did nothing and the luckier days just landed on the new-sign side
- **The question** — if the sign truly did nothing, how often would pure luck produce a $6 gap?
- **The p-value** — that "how often" is the p-value; small means luck is a poor explanation

*Example (italic):* Ten old-sign days average $43.50 in tips and ten new-sign days average $49.50 — is the $6 gap the sign, or luck?

**Key point:** A p-value answers exactly one question: assuming nothing is going on, what fraction of the time would a result at least this big appear? The three roads below are just three ways to compute that fraction.

### Visualization (canvas `c1`, 720×300)

Dot-strip plot: the 20 daily tip totals on one shared dollar axis, old-sign days on the top row, new-sign days on the bottom row, with mean markers and a gap bracket.

- **Title (bold 15px, `#1a5276`, top center):** "Twenty Days of Tips: Old Sign vs New Sign (illustrative)".
- **Data:** old-sign tips `[38, 52, 41, 47, 35, 44, 50, 39, 46, 43]` (mean 43.5); new-sign tips `[49, 44, 55, 47, 58, 42, 51, 53, 46, 50]` (mean 49.5).
- **Axis:** shared horizontal dollar axis, $32–$60 mapped to x=70..650; 2px `#999` line at y=250; tick labels "$35", "$40", "$45", "$50", "$55" (12px `#444`) below.
- **Old row:** row label "old sign" bold 12px `#2a78d6` at left x=8, y=110; blue `#2a78d6` 6px dots at y=110; vertical blue 3px mean tick from y=95 to y=125 at $43.50 with bold 13px blue label "mean $43.50" above.
- **New row:** row label "new sign" bold 12px `#008300` at x=8, y=200; green `#008300` 6px dots at y=200; vertical green 3px mean tick from y=185 to y=215 at $49.50 with bold 13px green label "mean $49.50" below.
- **Gap bracket:** orange `#d95926` 3px horizontal bracket at y=155 spanning $43.50 to $49.50, small end ticks, bold 13px orange label "gap = $6.00" centered above the bracket.
- **Caption (12px `#444`, bottom center):** "each dot = one day's tips in dollars".

## Road 1: A Formula and a Curve

**Tags:** `analytic` (blue), `worked example` (green), `assumptions` (red)

- **The recipe** — divide the gap by its typical luck-wobble: t = 6.00 / 2.32 ≈ 2.59
- **The wobble** — day-to-day spread is about $5.20, so a 10-vs-10 gap wobbles by about $2.32
- **The curve** — if the sign does nothing, t follows a known bell curve (a t-curve with 18 df)
- **The area** — the two tails beyond ±2.59 hold about 1.9% of the curve, so p ≈ 0.019
- **The catch** — the curve is only right if its assumptions (roughly normal, similar spread) hold

*Example (italic):* Feed the 20 tip totals to any t-test tool and it prints t = 2.59, p = 0.019 — this curve is where those numbers live.

**Key point:** An analytic p-value is an area under a theoretical curve — instant and exact, but only as trustworthy as the assumptions that picked the curve.

### Visualization (canvas `c2`, 720×300)

Bell curve (t-distribution with 18 df, drawn as a standard bell shape) with the two tails beyond ±2.59 shaded, showing the p-value as an area.

- **Title (bold 15px, `#1a5276`, top center):** "The Analytic Road: p = the Shaded Area Under the Curve".
- **Axis:** t from −4 to +4 mapped to x=70..650; baseline 2px `#999` at y=240; tick labels "−4", "−2", "0", "+2", "+4" (12px `#444`) below; axis caption "t (gap measured in wobble units)" 12px `#444`.
- **Curve:** bell shape `y = baseline − 170 · exp(−t²/2)` sampled at t steps of 0.05, ink `#1a5276` 2px line (deterministic formula, no randomness).
- **Shaded tails:** fill `rgba(213,81,129,0.45)` under the curve for t ≤ −2.59 and t ≥ +2.59, down to the baseline.
- **Cutoff lines:** dashed blue `#2a78d6` (dash 4/3) vertical lines at t = −2.59 and t = +2.59 from baseline up to the curve, each labeled bold 12px blue "±2.59" just above the baseline.
- **Formula annotation (bold 13px `#2a78d6`, top left at x=80, y=60, two lines):** "t = gap / wobble" / "= 6.00 / 2.32 ≈ 2.59".
- **Area annotation (bold 13px `#d55181`, right of the right tail with a short arrow to it):** "two tails ≈ 1.9% of area → p ≈ 0.019".

## Road 2: Shuffle the Labels

**Tags:** `permutation` (blue), `worked example` (green), `no assumptions` (orange)

- **The move** — if the sign did nothing, the labels "old" and "new" are meaningless decorations
- **The shuffle** — deal the 20 labels back onto the same 20 days at random, recompute the gap
- **Repeat** — 1,000 shuffles give 1,000 gaps that label-luck alone can produce
- **Count** — 20 of the 1,000 shuffled gaps reached ±$6.00, so p = 20/1000 = 0.020
- **No curve needed** — the data builds its own luck distribution; no normality assumption

*Example (italic):* One shuffle happens to deal the $52 and $50 days onto the "new" side and still yields a gap of only $1.30.

**Key point:** A permutation p-value is a literal count: the fraction of label shuffles whose gap is at least as big as the one you actually observed.

### Visualization (canvas `c3`, 720×300)

Histogram of the 1,000 shuffled gaps (rounded to the nearest dollar), with the bars at and beyond the observed ±$6 gap highlighted.

- **Title (bold 15px, `#1a5276`, top center):** "The Permutation Road: 1,000 Shuffled Gaps (illustrative)".
- **Data:** bin centers `[-7, -6, -5, -4, -3, -2, -1, 0, 1, 2, 3, 4, 5, 6, 7]` (shuffled gap in dollars, rounded); counts `[2, 8, 17, 39, 75, 119, 157, 166, 157, 119, 75, 39, 17, 8, 2]` (sum 1,000).
- **Axis:** origin x=70, plot width 580, baseline 2px `#999` at y=240, chart height 175, count scale 0–180; bin-center labels "−7" … "+7" 11px `#444` below each bar; axis caption "shuffled gap ($)" 12px `#444`.
- **Bars:** fill `rgba(42,120,214,0.45)`, 1px `#2a78d6` outline, small gaps between bars; the four extreme bars (centers −7, −6, +6, +7) instead fill `rgba(213,81,129,0.6)` with 1px `#d55181` outline.
- **Observed lines:** dashed magenta `#d55181` (dash 4/3) vertical lines at gap = −6 and gap = +6 from baseline to y=55, the right one labeled bold 12px magenta "observed ±$6".
- **Annotation (bold 13px `#d55181`, top right, two lines):** "20 of 1,000 shuffles beat ±$6" / "p = 20/1000 = 0.020".
- **Caption (12px `#444`, bottom center):** "bars = shuffled gaps rounded to the nearest dollar".

## Road 3: Build Fake Worlds

**Tags:** `simulation` (blue), `worked example` (green), `common mistake` (red)

- **The null world** — pretend the sign does nothing: every day's tips come from one shared pot
- **The pot** — a bell curve with the pooled mean $46.50 and spread $5.20 (illustrative)
- **Fake experiments** — draw 10 "old" + 10 "new" days from the pot, 1,000 times, gap each time
- **Count again** — 21 of the 1,000 fake experiments reached ±$6.00, so p = 21/1000 = 0.021
- **Three roads agree** — 0.019, 0.020, 0.021: one question, three ways to answer it
- **Not a verdict** — p is how often luck makes a $6 gap, not the chance the sign works

*Example (italic):* When your statistic has no textbook curve — a gap in medians, say — the fake-world road still works unchanged.

**Key point:** Simulation asks the p-value question directly: invent a world where nothing is going on, run the experiment there many times, and count. When no formula exists, this road is always open.

### Visualization (canvas `c4`, 720×300)

Dual panel split by a vertical dashed divider at x=430: histogram of the 1,000 simulated gaps (left), and a bar comparison of the three p-values (right).

- **Title (bold 15px, `#1a5276`, top center):** "The Simulation Road — and All Three Roads Compared".
- **Left panel (simulated gaps):** bin centers `[-7, -6, -5, -4, -3, -2, -1, 0, 1, 2, 3, 4, 5, 6, 7]`; counts `[3, 9, 18, 40, 74, 120, 156, 167, 155, 118, 76, 38, 17, 7, 2]` (sum 1,000); axis origin x=55, plot width 340, baseline 2px `#999` at y=240, chart height 165, count scale 0–180; bars fill `rgba(0,131,0,0.4)` with 1px `#008300` outline; the four extreme bars (centers −7, −6, +6, +7) fill `rgba(217,89,38,0.6)` with 1px `#d95926` outline; bin labels only at "−6", "0", "+6" 11px `#444`; bold 12px orange `#d95926` annotation, two lines: "21 of 1,000 fake worlds" / "→ p = 0.021"; caption 11px `#444` "simulated gaps, null world (illustrative)".
- **Right panel (three p-values):** heading bold 12px `#444` "three roads, one answer:" at x=460, y=70; three horizontal bars 20px tall starting at x=460, length scaled over 0–0.05 mapped to 0–200px: formula p=0.019 fill `rgba(42,120,214,0.55)`, shuffle p=0.020 fill `rgba(213,81,129,0.55)`, fake worlds p=0.021 fill `rgba(0,131,0,0.55)`; bar rows at y=95, 140, 185; left-side row labels "formula", "shuffle", "fake worlds" 12px `#444`; value labels "0.019", "0.020", "0.021" bold 12px in each bar's color to the right of each bar.
- **Reference line:** dashed `#c98500` (dash 4/3) vertical line at the 0.05 position (x=660) from y=80 to y=215, labeled bold 11px `#c98500` "0.05" above.
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=430 from y=38 to h-12.

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data integrity:** all histogram counts and dot values are the hardcoded literal arrays above — no `Math.random()`; the bell curve in `c2` uses the deterministic formula given. Text numbers (43.50, 49.50, 6.00, 5.20, 2.32, 2.59, 0.019, 20/1000 = 0.020, 21/1000 = 0.021, 46.50) must match the chart specs exactly.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
