# 'At Least One' Probability

**Page type:** detail page (tutorial card-sections: h2 with blue underline per section, two-column layout table — text left 50%, canvas right 50%)
**HTML title tag:** 'At Least One' Probability

**Subtitle:** To get the chance that something happens at least once, flip the question: find the chance it never happens, then subtract from 1

## A 1% Daily Risk Becomes a 26% Monthly Risk

**Tags:** `core idea` (blue), `counter-intuitive` (orange)

- **The setup** — a service has a 1% chance of an outage on any given day
- **The question** — over 30 days, what's the chance of at least one outage?
- **The answer** — about 26%, far more than the 1% a single day suggests
- **Why it grows** — every extra day is one more chance for the lucky streak to break
- **Not 30%** — simply adding 1% thirty times over-counts months with two bad days

*Example:* A "one bad day in a hundred" service still has a 1-in-4 chance of a bad month.

**Key point:** Small risks repeated many times stop being small — the real question is always "over how many tries?"

### Visualization (canvas `c1`, 720×300)

Line chart: P(at least one outage) vs days, with a naive 1%-per-day straight line for contrast.

- **Title (bold 15px, `#1a5276`, top center):** "Chance of At Least One Outage Grows With the Days".
- **Data (true curve):** days `[0, 7, 14, 21, 30, 45, 60, 75, 90]`, probability % `[0, 6.8, 13.1, 19.0, 26.0, 36.4, 45.3, 52.9, 59.5]`; x from 0 to 90, y from 0 to 100.
- **Axes:** padding top 52, bottom 52, left 62, right 30; axis lines `#999`; x ticks at 0, 15, 30, 45, 60, 75, 90 (12px `#222`); x-axis label "days of running the service" (12px `#444`); rotated y-axis label "P(at least one outage), %".
- **Naive line:** dashed (6/4) mute `#6b7280`, width 2, straight from (0, 0%) to (90, 90%), labeled in bold 12px mute "naive: add 1% per day" (at x≈58, y≈70%).
- **True curve:** connected line in blue `#2a78d6`, width 3; 4px blue dots, the 30-day point orange `#d95926` and 6px.
- **Annotations:** bold 13px orange "30 days → 26%" to the right of the 30-day point; bold 12px blue "true curve: 1 − 0.99ᵈ" (at x≈8, y≈38%).

## Flip It: The Chance That Nothing Happens

**Tags:** `worked example` (green), `rule of thumb` (blue)

- **Step 1** — chance of no outage on one day: 1 − 0.01 = 0.99
- **Step 2** — two clean days in a row: 0.99 × 0.99 = 0.9801
- **Step 3** — three clean days: 0.9703, so P(at least one outage) = 2.97%
- **Step 4** — thirty clean days: 0.99 multiplied 30 times = 0.740
- **Flip** — P(at least one outage in 30 days) = 1 − 0.740 = 26%

*Example:* Multiply 0.99 by itself 30 times on a calculator: 0.7397 — the flip does the rest.

**Key point:** "At least one" equals 1 minus "none" — and "none" is just multiplication, as long as the days don't affect each other.

### Visualization (canvas `c2`, 720×300)

Bar chart: the clean-streak ("nothing happens") probability shrinking day by day.

- **Title (bold 15px, `#1a5276`, top center):** "The \"Nothing Happens\" Chance Shrinks With Every Day".
- **Data:** days `[1, 5, 10, 15, 20, 25, 30]`, clean-streak % `[99.0, 95.1, 90.4, 86.0, 81.8, 77.8, 74.0]`; y scale max 100.
- **Axes:** padding top 52, bottom 52, left 62, right 25; axis lines `#999`; x-axis label "days without an outage (each day multiplies by 0.99)" (12px `#444`); rotated y-axis label "P(clean streak), %".
- **Bars:** width 62px, evenly gapped; last bar (day 30) filled green `#008300`, all others `rgba(26,82,118,0.35)`; percentage value labels 12px `#444` above bars, day labels 12px `#222` below.
- **Annotation (bold 13px green, right-aligned near top):** "day 30: streak at 74% → 1 − 0.74 = 26% chance of at least one outage".

## Where This Bites a Data Scientist

**Tags:** `where it's used` (blue), `common mistake` (red)

- **Multiple A/B tests** — 20 tests at a 5% false-alarm rate: 1 − 0.95²⁰ = 64% chance of a fake win
- **Alert floods** — a 0.1% false alarm per check turns into daily noise across 1,000s of checks
- **SLA math** — "99.9% per day" still means a ~31% chance of at least one bad day per year
- **Rare-row errors** — a tiny per-row bug rate almost surely appears somewhere in a big table
- **The reflex** — before trusting "unlikely", multiply the "not happening" chances across every try

*Example:* A dashboard running 20 significance tests will flash at least one false "winner" 64% of the time.

**Key point:** A per-test error rate means little on its own — compute the at-least-one chance across everything you ran.

### Visualization (canvas `c3`, 720×300)

Line chart: probability of at least one false positive vs number of A/B tests run.

- **Title (bold 15px, `#1a5276`, top center):** "Run More Tests, Guarantee a False \"Winner\" (5% each)".
- **Data:** tests `[0, 1, 5, 10, 20, 30, 40, 60]`, probability % `[0, 5.0, 22.6, 40.1, 64.2, 78.5, 87.1, 95.4]`; x from 0 to 60, y from 0 to 100.
- **Axes:** padding top 52, bottom 52, left 62, right 30; axis lines `#999`; x tick labels are the data x-values (except 0), 12px `#222` under each point; x-axis label "number of A/B tests run" (12px `#444`); rotated y-axis label "P(≥1 false positive), %".
- **Series:** connected line in magenta `#d55181`, width 3; 4px magenta dots, the 20-test point orange `#d95926` and 6px.
- **Annotations:** bold 13px orange "20 tests → 64% chance of at least one false alarm" to the right of the 20-test point; 12px mute `#6b7280` "same flip: 1 − 0.95ᵏ" (at x≈38, y≈38%).

## Why Adding Percentages Fails

**Tags:** `common mistake` (red), `rule of thumb` (blue)

- **Close at first** — for 3 days, adding gives 3% and the true answer is 2.97%
- **Then it drifts** — at 30 days adding says 30%; the real answer is 26%
- **Then it breaks** — at 200 days adding says 200%, impossible; the truth is 86.6%
- **The reason** — adding double-counts the runs where it happens more than once
- **Safe shortcut** — p × n is fine for small p and few tries; sanity-check with the flip

*Example:* Adding 1% for each of 200 days claims a 200% risk — the flip method caps it at 86.6%.

**Key point:** Chances of overlapping events never simply add; the flip method can never exceed 100%.

### Visualization (canvas `c4`, 720×300)

Line chart: naive addition crashing through the 100% ceiling vs the true flip-method curve over 200 days.

- **Title (bold 15px, `#1a5276`, top center):** "Adding 1% Per Day Crashes Through the 100% Ceiling".
- **Data (true curve):** days `[0, 25, 50, 75, 100, 150, 200]`, probability % `[0, 22.2, 39.5, 52.9, 63.4, 77.9, 86.6]`; x from 0 to 200, y from 0 to 100.
- **Axes:** padding top 52, bottom 52, left 62, right 30; axis lines `#999`; x ticks at 0, 50, 100, 150, 200 (12px `#222`); x-axis label "days (1% outage chance each)" (12px `#444`); rotated y-axis label "P(at least one outage), %".
- **100% ceiling:** dashed (5/4) red `#e74c3c`, width 1.5, at y=100, labeled in bold 12px red "100% — nothing can be more likely than certain".
- **Naive line:** dashed (6/4) mute `#6b7280`, width 2, from (0, 0%) rising to hit the ceiling at (100, 100%), then continuing along the ceiling to day 200 with a sparser dash (2/4); labeled in bold 12px mute "naive addition: claims 200% by day 200" (at x≈84, y≈88%).
- **True curve:** connected line in aqua `#199e70`, width 3, with 4px aqua dots.
- **Annotation (bold 13px aqua, at x≈105, y≈72%):** "the flip: 200 days → 86.6%, never past 100%".

## Regeneration instructions

- **Layout:** tutorial detail page. `<h1>` + `.subtitle`, then 4 `.card-section` blocks, each with an `<h2>` (1.3rem, `#1a5276`, 2px solid `#2980b9` bottom border) and a `table.layout` (width 100%, border-collapse) with one `<tr>`: left `td.text-col` (50%) holding `.tags` pills, a `<ul>` of bullets, an italic `.example` line, and a `.key-point` callout; right `td.viz-col` (50%) holding a `<canvas>` 720×300 at `width:100%` with 1px `#e0e0e0` border, 4px radius.
- **Page CSS:** body system-ui sans, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; `<ul>` 0.92rem; `li b` colored `#1a5276`.
- **Tag pills:** `.tag` inline-block 0.72rem weight 600, padding 2px 10px, radius 10px; blue = `rgba(26,82,118,0.12)` bg / `#1a5276` text; green = `rgba(39,174,96,0.15)` / `#27ae60`; red = `rgba(231,76,60,0.12)` / `#e74c3c`; orange = `rgba(230,126,34,0.15)` / `#e67e22`.
- **Callout style:** `.key-point` — background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem. `.example` — italic, `#555`, 0.9rem.
- **Canvas:** logical size 720×300 per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Hardcoded data arrays, no `Math.random()`. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
