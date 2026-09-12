# Loss Functions

**Page type:** detail page (tutorial card-sections: one h2 + two-column table per section, text left 50%, canvas right 50%)
**HTML title tag:** Loss Functions

**Subtitle:** A single number that scores how wrong the model is — training is just the effort to make it smaller

## One Pizza Shop, Two Guessing Rules

**Tags:** `core idea` (blue), `error score` (green)

- **The job** — a pizza shop wants to tell callers how many minutes delivery will take
- **Rule A** — always say 25 minutes, no matter how far the house is
- **Rule B** — say 13 minutes plus 4 minutes per km of distance
- **Every guess misses** — the error is simply actual minutes minus predicted minutes
- **The loss** — one number that adds all those misses into a single score per rule
- **Training** — nothing more than searching for the rule with the smallest score

*Example (italic):* Four past deliveries at 1, 2, 4 and 5 km took 18, 22, 31 and 34 minutes — score both rules on them.

**Key point:** **A loss function** turns "how wrong is this model?" into one number, so any two rules — however different — can be compared and the better one chosen.

### Visualization (canvas `c1`, 720×300)

Scatter plot of the four deliveries with the two prediction rules overlaid and dashed error segments to Rule A.

- **Title (bold 15px, `#1a5276`, top center):** "Four Deliveries, Two Rules — Every Miss Is Measurable"
- **Data points (ink `#1a5276`, 5.5px-radius filled dots):** km = `[1, 2, 4, 5]`, minutes = `[18, 22, 31, 34]`
- **Axes:** x from 0 to 6 km (integer ticks 0–6), label "distance (km)"; y from 0 to 40 minutes (ticks every 10), rotated label "minutes". Axis lines `#999`, tick labels muted `#6b7280` 12px. Padding: top 46, bottom 46, left 60, right 170.
- **Rule A line:** horizontal orange (`#d95926`) line at y=25, width 3, from x=0.3 to x=5.8.
- **Rule B line:** green (`#008300`) line y = 13 + 4×km, width 3, from x=0.3 to x=5.8.
- **Error segments:** dashed magenta (`#d55181`, dash 4/3, width 2) vertical segments from each actual point to the Rule A line.
- **Annotation (bold 13px magenta, near x=0.5, y≈37 and y≈33):** "dashed gaps = Rule A's misses:" / "−7, −3, 6, 9 minutes"
- **Legend (right margin, 12px):** ink dot "actual deliveries"; orange line "Rule A: flat 25"; green line "Rule B: 13 + 4×km".

## Scoring the Two Rules by Hand

**Tags:** `worked example` (green), `squared error` (blue)

- **Rule A errors** — guessing 25 gives misses of −7, −3, 6 and 9 minutes
- **Square them** — 49, 9, 36, 81; squaring kills the minus signs and inflates big misses
- **Rule A total** — 49 + 9 + 36 + 81 = 175, or a mean squared error of 43.75
- **Rule B errors** — 13 + 4×km predicts 17, 21, 29, 33, missing by 1, 1, 2 and 1
- **Rule B total** — 1 + 1 + 4 + 1 = 7, a mean squared error of just 1.75
- **Verdict** — 1.75 beats 43.75; the loss picked the winner with no judgement calls

*Example (italic):* You can redo the whole comparison on a napkin: four subtractions, four squarings, one sum per rule.

**Key point:** **Mean squared error (MSE)** is the most common loss for predicting numbers: average of (actual − predicted)². Smaller is better; zero means every guess was exact.

### Visualization (canvas `c2`, 720×300)

Grouped bar chart of per-delivery squared errors, Rule A vs Rule B, with totals annotated in the right margin.

- **Title (bold 15px, `#1a5276`, top center):** "Squared Error per Delivery: Rule A vs Rule B"
- **Data:** Rule A squared errors `[49, 9, 36, 81]`, Rule B squared errors `[1, 1, 4, 1]`, grouped by delivery distance labels "1 km", "2 km", "4 km", "5 km".
- **Axes:** y from 0 to 90 (ticks every 30, muted `#6b7280` 12px); x-axis footer label (muted): "the four past deliveries". Axis lines `#999`. Padding: top 52, bottom 52, left 60, right 180.
- **Bars:** 34px wide, side by side per group (3px from group center). Rule A: orange `#d95926` at 50% alpha fill with 2px orange stroke. Rule B: green `#008300` at 50% alpha fill with 2px green stroke. Bold 12px value labels above each bar in the bar's color.
- **Legend (right margin, 12px):** orange swatch "Rule A (flat 25)"; green swatch "Rule B (13 + 4×km)".
- **Annotations (right margin, bold 13px):** orange: "Rule A total: 175" / "MSE = 43.75"; green: "Rule B total: 7" / "MSE = 1.75 — wins".

## Why Squaring? Big Misses Hurt More Than Small Ones

**Tags:** `what to minimize` (blue), `design choice` (orange)

- **Squared error** — a 9-minute miss costs 81 while a 1-minute miss costs 1, an 81x gap
- **Absolute error** — counts the same misses as 9 and 1; big misses hurt only 9x more
- **The choice matters** — squared loss makes the model terrified of rare huge misses
- **Different answers** — minimizing squared error targets the average; absolute targets the median
- **Classification too** — spam filters minimize a different loss (cross-entropy) on probabilities
- **Same recipe** — whatever the task, define wrongness as a number, then push it down

*Example (italic):* If one delivery got stuck in traffic for an hour, squared loss would bend the whole rule to appease it.

**Key point:** **The loss defines what "good" means.** Change the loss and the same data yields a different model — picking it is a modeling decision, not a technicality.

### Visualization (canvas `c3`, 720×300)

Line chart comparing penalty curves: absolute error |e| (straight line) vs squared error e² (parabola) on the same axes.

- **Title (bold 15px, `#1a5276`, top center):** "How Much Does a Miss Cost? Squared vs Absolute"
- **Axes:** x from 0 to 10 (ticks every 2), label "size of miss (minutes)"; y from 0 to 100 (ticks every 25). Axis lines `#999`, tick labels muted `#6b7280` 12px. Padding: top 50, bottom 50, left 65, right 175.
- **Absolute error line:** blue `#2a78d6`, width 3, straight from (0,0) to (10,10).
- **Squared error curve:** magenta `#d55181`, width 3, parabola y = e² sampled over 50 segments from e=0 to e=10.
- **Markers:** 6px-radius dots at (9, 81) magenta and (9, 9) blue.
- **Annotations (bold 13px):** magenta, right-aligned near (8.7, 81): "squared: a 9-min miss costs 81"; blue, left-aligned near (3.2, 24): "absolute: the same miss costs 9".
- **Legend (right margin, 12px):** magenta line "squared error e²"; blue line "absolute error |e|"; muted note "penalty on y-axis".

## The Confusion: Loss Is for the Machine, Not the Report

**Tags:** `common mistake` (red), `error score` (blue)

- **Weird units** — Rule B's loss of 1.75 is in squared minutes; nobody plans a route in those
- **RMSE** — take the square root: √1.75 ≈ 1.32 minutes, a typical miss, readable again
- **MAE** — the plain average miss: (1+1+2+1)/4 = 1.25 minutes, even easier to explain
- **Same rule, three scores** — 1.75, 1.32 and 1.25 all describe one model; know which you cite
- **Train loss can lie** — a rule can score near zero on past deliveries and flop on new ones

*Example (italic):* Telling the shop owner "our loss is 1.75" earns a blank stare; "we're typically about 1.3 minutes off" does not.

**Key point:** **The confusion:** the loss is the number training minimizes; the metric is the number humans judge by. They are related but not the same — always translate before reporting.

### Visualization (canvas `c4`, 720×300)

Three-bar chart: the same Rule B model reported three ways (MSE, RMSE, MAE) with unit and note labels under each bar.

- **Title (bold 15px, `#1a5276`, top center):** "One Rule (13 + 4×km), Three Ways to Report It"
- **Data:** names `["MSE", "RMSE", "MAE"]`, values `[1.75, 1.32, 1.25]`, units `["squared minutes", "minutes", "minutes"]`, notes `["what training minimizes", "typical miss, readable", "plain average miss"]`, colors `[#4a3aa7 (violet), #199e70 (aqua), #2a78d6 (blue)]`.
- **Axes:** y from 0 to 2.0 (ticks every 0.5, one decimal). Axis lines `#999`, tick labels muted `#6b7280` 12px. Padding: top 56, bottom 66, left 70, right 40.
- **Bars:** 80px wide, centered in thirds of the plot; fill at 45% alpha of each bar's color with 2px stroke in the same color; bold 14px value label (two decimals) above each bar in the bar's color; below the axis, per bar: bold 13px name (`#2c3e50`), 12px muted unit, 12px muted note.
- **Annotation (bold 13px magenta `#d55181`, centered at y=48):** "same model — minimize the loss, but report a number humans can read"

## Regeneration instructions

- **Layout:** tutorial detail page. h1, `.subtitle` paragraph, then four `.card-section` blocks. Each section: `<h2>` with 2px `#2980b9` bottom border, then `table.layout` (full width, border-collapse) with one `<tr>`: left `<td class="text-col">` (50%) holding `.tags` pill row, `<ul>` bullets (each starting with `<b>` term in `#1a5276`), italic `.example` paragraph, and `.key-point` callout; right `<td class="viz-col">` (50%) holding one canvas.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with 2px `#2980b9` bottom border; h2 1.3rem `#1a5276`; subtitle `#666` 0.95rem. Canvas `width:100%`, 1px `#e0e0e0` border, 4px radius. `.key-point`: background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem. `.example`: italic `#555` 0.9rem. Bullets 0.92rem.
- **Tag pills:** `.tag` inline-block 0.72rem bold, padding 2px 10px, radius 10px. blue: bg rgba(26,82,118,0.12) / `#1a5276`; green: bg rgba(39,174,96,0.15) / `#27ae60`; red: bg rgba(231,76,60,0.12) / `#e74c3c`; orange: bg rgba(230,126,34,0.15) / `#e67e22`.
- **Chart palette (JS object P):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Canvas:** intrinsic 720×300 attributes; a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Data arrays are hardcoded literals (shared running example: KM `[1,2,4,5]`, MIN `[18,22,31,34]`). Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
