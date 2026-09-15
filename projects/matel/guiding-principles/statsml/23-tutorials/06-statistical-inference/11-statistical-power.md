# Statistical Power

**Page type:** detail page (tutorial: 4 card-sections, each h2 + two-column layout table, text left 50% with tag pills / canvas right 50%)
**HTML title tag:** Statistical Power

**Subtitle:** Power is the chance your test catches an effect that is really there — a real 1% lift in conversion looks identical to nothing in a test that is too small.

## The Same Real 1% Lift, Hunted by Two Tests

Tags: `core idea` (blue), `running example` (green)

- **The truth** — a new page REALLY lifts conversion from 10% to 11%; we know it, the test doesn't
- **Test A** — 200 users total (100 per page); it catches the lift ~6 times in 100 tries
- **Test B** — 20,000 users total (10,000 per page); it catches the lift ~64 times in 100
- **Power** — that catch rate is the test's power: A has 6%, B has 64%
- **Why A fails** — its measured lift wobbles ±4.3 points, so a 1-point truth drowns

*Example:* Same page, same real win — one test is a metal detector, the other a magnet the size of a shoe.

**Key point:** power is a property of the test's design, fixed before any data arrives — mostly by sample size and effect size.

This section's viz cell holds both canvases side by side in a `.viz-pair` flex row.

### Visualization (canvas `c1a`, 310×300)

"Detector" panel: a bell curve of the measured lift around the true +1 point, with a significance bar and a shaded catch region.

- **Title (bold 15px, `#1a5276`, top center):** "Test A: 200 users".
- **Power label (bold 14px green `#008300`, centered, y=44):** "power = 6%".
- **Curve:** Gaussian shape (unnormalized `exp(-0.5*((x-1)/sd)^2)`) centered at +1 with sd = 4.3, stroked orange `#d95926` width 3; x range −12 to +14 (measured lift, points); padding top 66, bottom 52, left 20, right 20.
- **Catch region:** area under the curve beyond the bar (+8.5 to xMax) filled `rgba(0,131,0,0.20)`.
- **Axis:** gray `#999` baseline; tick labels at −10, −5, 0, +5, +10; axis title "measured lift, points".
- **Truth marker:** vertical dashed violet `#4a3aa7` line (dash 4/3, width 2) at +1, labeled "truth: +1" bold 12px violet at top.
- **Significance bar:** vertical solid red `#e74c3c` line (width 2) at +8.5, labeled "bar: +8.5" bold 12px red.

### Visualization (canvas `c1b`, 310×300)

Same detector panel design for the big test.

- **Title:** "Test B: 20,000 users"; power label "power = 64%".
- **Curve:** centered at +1 with sd = 0.43, stroked blue `#2a78d6` width 3; drawn on its own zoomed x range −2 to +3 (tick labels −2, −1, 0, +1, +2, +3) so the tight curve and its shaded catch region stay readable; significance bar at +0.85 labeled "bar: +0.85"; catch region shaded green beyond +0.85; same truth marker and styling as `c1a`.

## Working the Catch Rate by Hand

Tags: `worked example` (green), `small numbers` (blue)

- **Wobble at 100/page** — the measured gap wobbles about ±4.3 points around the true +1
- **Bar to clear** — "significant" needs the measured gap to reach ~2 wobbles: 8.5 points
- **Catch rate** — a gap that starts at +1 reaches 8.5 only ~6% of the time: power 6%
- **Wobble at 10,000/page** — shrinks to ±0.43 points; the bar drops to ~0.85 points
- **Now the truth clears it** — +1 beats 0.85 about 64% of the time: power 64%
- **The 80% standard** — reaching it here needs ~15,000 users per page (30,000 total)

*Example:* Check one step: 200-user wobble ≈ √(0.1×0.9/100 + 0.11×0.89/100) ≈ 0.043 = 4.3 points.

**Key point:** even the 20,000-user test misses this real win 1 time in 3 — small effects are expensive to see.

### Visualization (canvas `c2`, 720×300)

Line chart: power vs users per page for the same real 10%→11% lift.

- **Title (bold 15px, `#1a5276`, top center):** "Power to Catch a Real 10% to 11% Lift, by Users per Page".
- **Data (computed two-proportion power, alpha = 0.05, two-sided):** users per page `[100, 500, 1000, 2000, 5000, 10000, 15000, 20000]` → power % `[6, 8, 11, 18, 37, 64, 81, 90]`.
- **Axes:** x from 0 to 21,000 with tick labels 0, 5k, 10k, 15k, 20k; y from 0 to 100% with labels at 0, 25, 50, 80, 100%; axis title "users per page (two-proportion test, 5% cutoff)"; gray `#999` L-axes; padding top 56, bottom 56, left 70, right 30.
- **80% line:** horizontal dashed green `#008300` (dash 5/4, width 1.5) at 80%, labeled "80% standard" bold 12px green.
- **Curve:** blue `#2a78d6` line width 3 connecting the points, 4px blue dots at each point.
- **Markers (radius-7 dots with bold 12px labels):** orange `#d95926` at (100, 6%) labeled "Test A (100/page): 6%"; aqua `#199e70` at (10,000, 64%) labeled "Test B (10,000/page): 64%"; green `#008300` at (15,000, 81%) labeled "~15,000/page reaches 80%".

## What Underpowered Testing Costs a Business

Tags: `where it's used` (blue), `hidden cost` (red)

- **Ten real wins** — imagine 10 features, each truly worth +1 point of conversion
- **200-user tests** — catch about 1 of the 10; the other 9 real wins get killed as "no effect"
- **20,000-user tests** — catch about 6 of 10; 30,000-user tests catch about 8
- **Invisible losses** — nobody sees the killed wins; the dashboard only shows what shipped
- **Plan first** — compute the needed sample size BEFORE launching, from the lift you care about

*Example:* "We tested it, it did nothing" often means "our test had a 6% chance of noticing anything."

**Key point:** an underpowered test is a coin-flip dressed up as science — running it costs traffic and buries real improvements.

### Visualization (canvas `c3`, 720×300)

Stacked bar chart: of 10 real +1-point wins, how many each test size ships vs kills.

- **Title (bold 15px, `#1a5276`, top center):** "10 Features, Each Truly Worth +1 Point: How Many Survive Testing?".
- **Data (three stacked bars, 110px wide, scale max 10):**
  - "200-user tests": 1 caught, 9 missed
  - "20,000-user tests": 6 caught, 4 missed
  - "30,000-user tests": 8 caught, 2 missed
- **Segments:** caught on the bottom filled `rgba(0,131,0,0.65)` labeled "N shipped" (bold 13px dark green `#005a00`); missed on top filled `rgba(213,81,129,0.35)` labeled "N real wins killed" (bold 13px magenta `#d55181` above the bar); whole bar outlined gray `#6b7280`. Group labels 13px below the baseline; gray `#999` baseline; padding top 60, bottom 66, left 70, right 30.
- **Takeaway (bold 13px magenta `#d55181`, bottom center):** "the killed wins never appear on any dashboard — power is the invisible-loss dial".

## The Two Misreadings of a Weak Test

Tags: `common mistake` (red), `winner's curse` (orange)

- **Misreading 1** — "not significant" gets read as "no effect"; at 6% power, missing is expected
- **Misreading 2** — when the weak test DOES flag a win, the measured lift is a wild exaggeration
- **Why** — the 200-user test only flags gaps of 8.5+ points; the truth is 1 point
- **Winner's curse** — the lucky "significant" result reports ~8x the real effect
- **Sequel flop** — the rollout then "underperforms the test" — it was never an 8-point win

*Example:* A 200-user test reported "+9 points, significant!"; the true lift was +1 — the launch looked like a failure.

**Key point:** a weak test is wrong in both directions — it misses real wins, and the wins it reports are inflated.

### Visualization (canvas `c4`, 720×300)

Bell curve of the 200-user test's measured lift with the flagged tail highlighted — the winner's curse.

- **Title (bold 15px, `#1a5276`, top center):** "Winner's Curse: the Only Wins a 200-User Test Reports Are Huge Flukes".
- **Curve:** Gaussian centered at +1 with sd = 4.3, stroked blue `#2a78d6` width 3; x range −12 to +16; padding top 56, bottom 56, left 40, right 30.
- **Flagged region:** area under the curve beyond +8.5 filled `rgba(217,89,38,0.30)`.
- **Axis:** gray `#999` baseline; tick labels −10, −5, 0, +5, +10, +15; axis title "lift the test measures, points (true lift = +1)".
- **Truth marker:** vertical dashed violet `#4a3aa7` line (dash 4/3, width 2) at +1, labeled "truth: +1 point" bold 13px violet.
- **Significance bar:** vertical red `#e74c3c` line (width 2) at +8.5, labeled "significance bar: +8.5" bold 13px red.
- **Annotations:** bold 13px orange `#d95926` two-line label near the tail: "the ~6% that get flagged" / "report 8x the true effect"; bold 12px blue centered over the curve body: "94% of runs land here: real win called \"no effect\"".

## Regeneration instructions

- **Layout:** tutorial page — `<h1>` + `.subtitle`, then 4 `.card-section` blocks. Each has an `<h2>` (1.3rem `#1a5276`, 2px solid `#2980b9` bottom border) and a `table.layout` (width 100%, border-collapse, cell padding 12px, vertical-align top). Every section uses one row with `.text-col` (50%) and `.viz-col` (50%); section 1 places canvases `c1a`/`c1b` (310×300 each) side by side inside its single viz cell, wrapped in a `.viz-pair` flex row (`display:flex; gap:10px`, each canvas `flex:1 1 0; min-width:0`).
- **Text cell structure:** `.tags` row of pills, then a `<ul>` of bullets each opening with `<b>bold term</b>` (bold terms colored `#1a5276`), one italic `.example` paragraph (italic, `#555`, 0.9rem), one `.key-point` callout (background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem) whose "Key point:" prefix is `<strong>`.
- **Tag pills:** `.tag` inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px. Variants: blue `rgba(26,82,118,0.12)`/`#1a5276`; green `rgba(39,174,96,0.15)`/`#27ae60`; red `rgba(231,76,60,0.12)`/`#e74c3c`; orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; subtitle `#666` 0.95rem. Canvases have `width: 100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas JS:** shared palette object `P = { blue:'#2a78d6', green:'#008300', magenta:'#d55181', yellow:'#c98500', aqua:'#199e70', orange:'#d95926', violet:'#4a3aa7', ink:'#1a5276', text:'#2c3e50', mute:'#6b7280', grid:'#e5e9ef' }`; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates; a `gauss(x, mu, sd)` helper for the bell curves; a reusable `detectorPanel(id, title, sd, bar, power, curveColor, xMin, xMax, ticks)` function draws `c1a`/`c1b`. Hardcoded literal data arrays — no `Math.random()`. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Site palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- In regenerated HTML, any card links use `.html` extensions (this page has none).
