# Model Monitoring & Drift

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Model Monitoring & Drift

**Subtitle:** A deployed model is a snapshot of the world on the day it was trained — monitoring is how you notice the world has moved on while the model stood perfectly still

## The Bakery Model That Went Stale

**Tags:** `core idea` (blue), `silent decay` (orange), `same model, new world` (green)

- **The bakery** — a model trained on last year's sales tells the baker how many croissants to bake each morning
- **A good run** — for five weeks the guess lands within 2–3 croissants of actual sales, day after day
- **The change** — in week 6 a new office building opens down the street, and mornings get crowded
- **Same model** — the code and weights are untouched, yet by week 12 it under-bakes by 18 croissants a day
- **Drift** — the model didn't break; the world it memorized quietly stopped existing

*Example (italic):* In week 5 the bakery sold out around 11:40am; by week 12 it sold out at 9:15 — same recipe, same model, different street.

**Key point:** A model's accuracy has a shelf life — it decays not because the model changed, but because the world did.

### Visualization (canvas `c1`, 720×300)

Single-panel line chart: average daily bake error (croissants over or short) for weeks 1–12, flat around zero until the office opens in week 6, then sliding steadily negative.

- **Title (bold 15px, `#1a5276`, top center):** "Same Model Every Week — the Error Drifts Anyway".
- **Axes:** origin x=60, plot area x=60..660, top y=55, bottom y=245; x = weeks 1–12, 12px `#444` tick labels "wk 1"…"wk 12" below y=245; y = croissants over(+)/short(−) from +5 to −20, labels "+5", "0", "−5", "−10", "−15", "−20" (12px `#444`), light `#e5e9ef` gridlines at each, zero line 2px `#999` at y=93.
- **Error line:** weekly values at weeks 1–12 = `[2, -1, 1, -2, 0, -3, -6, -9, -12, -15, -17, -18]`; blue `#2a78d6` 3px line with 4px dots for weeks 1–6, red `#e74c3c` 3px line with 4px dots for weeks 6–12 (error state).
- **Event marker:** vertical dashed orange `#d95926` (dash 4/3) line at week 6 from y=55 to y=245; bold 12px orange label at its top: "new office opens".
- **Annotation (bold 13px red `#e74c3c`, near x=520, y=200):** two lines: "18 short per day by week 12 —" / "the model never changed".
- **Caption (12px `#444`, bottom right):** "illustrative — daily bake error, weekly average".

## Two Share Tables and a Subtraction

**Tags:** `worked example` (blue), `input drift` (green)

- **The check** — split each day's customers into hour buckets and compare the shares against training
- **Training shape** — before 9am used to bring 25 of every 100 customers: 10 in 7–8am, 15 in 8–9am
- **This month** — the same window now brings 45 of every 100: 22 in 7–8am, 23 in 8–9am
- **The shift** — 45 − 25 = a 20-point jump in the before-9am share, checkable with pencil and paper
- **Early signal** — the inputs moved weeks before the lost sales showed up in any error report

*Example (italic):* Before 9am held 25% of customers in training and 45% now — the office crowd appears in the input data before it appears in the losses.

**Key point:** Drift detection can be this plain: two share tables and a subtraction — the before-9am share moved 25% → 45%.

### Visualization (canvas `c2`, 720×300)

Grouped bar chart: share of daily customers by hour bucket, training period vs this month, with the two before-9am groups bracketed to show the 25% → 45% jump.

- **Title (bold 15px, `#1a5276`, top center):** "Where the Customers Come From: Training vs This Month".
- **Axes:** origin x=60, baseline y=245, plot width 600; y = share of daily customers 0–35%, 12px `#444` labels "0%", "10%", "20%", "30%", light `#e5e9ef` gridlines; x = five hour buckets labeled "7–8", "8–9", "9–10", "10–11", "11–12" (12px `#444`, centered under each group).
- **Bars:** per bucket two bars 24px wide, 6px apart, group centers evenly spaced across the plot; training (blue `#2a78d6`, fill `rgba(42,120,214,0.35)`, 2px blue border) = `[10, 15, 25, 30, 20]`; this month (orange `#d95926`, fill `rgba(217,89,38,0.35)`, 2px orange border) = `[22, 23, 25, 18, 12]`; 12px value labels ("10%", "22%", …) above each bar in the bar's color.
- **Legend (top right, 12px):** blue swatch "training", orange swatch "this month".
- **Bracket:** thin `#6b7280` bracket spanning the first two groups just above their bars, with bold 13px ink `#1a5276` label centered above: "before 9am: 25% → 45%".
- **Annotation (bold 12px green `#008300`, near x=470, y=95):** "inputs shifted before any error showed".
- **Caption (12px `#444`, bottom right):** "illustrative — share of daily customers by hour".

## Why Deployed Models Need a Watchman

**Tags:** `where it's used` (blue), `late labels` (orange), `alerts` (green)

- **Silent failure** — a stale model doesn't crash or log errors; it keeps answering, just wrongly
- **Late labels** — in many systems the truth arrives weeks later (fraud, churn), so error alarms lag
- **Watch inputs** — input distributions exist the moment a prediction is made, no label required
- **Two alarms** — here the input-drift alarm fired in week 7; the error alarm only fired in week 10
- **Retrain trigger** — good teams retrain on a drift alarm or a schedule, not when someone complains

*Example (italic):* A churn model scores customers today, but who actually left isn't known for 60 days — input drift is the only same-day smoke detector.

**Key point:** Watching the inputs bought 3 weeks of warning that watching the accuracy alone could not give.

### Visualization (canvas `c3`, 720×300)

Two-row alarm timeline on a shared week axis: the input-drift monitor turns orange in week 7, the error monitor turns red in week 10, and the gap between them is shaded as bought warning time.

- **Title (bold 15px, `#1a5276`, top center):** "Two Monitors, Two Alarm Times".
- **Axis:** horizontal 2px `#999` line at y=250 from x=200 to x=680 (width 480), weeks 1–12; 12px `#444` tick labels "wk 1", "wk 3", "wk 5", "wk 7", "wk 9", "wk 11" below.
- **Rows (12px `#444` labels left-aligned at x=20):**
  - Row 1 (y=110), "input drift monitor": 14px-tall rounded bar weeks 1–12; fill `rgba(0,131,0,0.25)` from week 1 to 7, `rgba(217,89,38,0.45)` from week 7 to 12; orange `#d95926` 7px alarm dot at week 7 with bold 12px orange label above: "alarm wk 7".
  - Row 2 (y=180), "prediction error monitor": same bar style; fill `rgba(0,131,0,0.25)` from week 1 to 10, `rgba(231,76,60,0.45)` from week 10 to 12; red `#e74c3c` 7px alarm dot at week 10 with bold 12px red label above: "alarm wk 10".
- **Warning band:** vertical shaded band `rgba(230,126,34,0.12)` from week 7 to week 10 spanning y=70 to y=250, dashed `#d95926` (dash 4/3) edges.
- **Annotation (bold 13px violet `#4a3aa7`, centered in the band near y=80):** two lines: "3 weeks of warning —" / "inputs spoke before the errors did".
- **Caption (12px `#444`, bottom right):** "illustrative — alarm weeks match the bakery story".

## Data Drift vs Concept Drift

**Tags:** `common mistake` (red), `two kinds of drift` (orange)

- **Data drift** — the inputs moved but the rule held: more early customers, same croissants per customer
- **Concept drift** — the inputs look the same but the rule changed: 40 customers now buy 36, not 52
- **Different fixes** — data drift often just needs retraining on fresh data; concept drift may need new features
- **The tell** — inputs moved with the old rule intact means data drift; steady inputs with growing error means concept drift
- **The mistake** — calling every accuracy drop "drift" and retraining blindly without checking which kind

*Example (italic):* A delivery app starts taking the pastry orders: the same 40-customer morning now sells 36 croissants instead of 52 — the customers didn't change, their behavior did.

**Common mistake:** Treating drift as one thing. Check the inputs first: if they moved, it's data drift; if they held steady while errors grew, the relationship itself changed.

### Visualization (canvas `c4`, 720×300)

Two side-by-side scatter panels on shared customer/croissant axes: left panel shows points sliding right along an unchanged fit line (data drift); right panel shows the same input range with the fit line itself rotating down (concept drift).

- **Panel geometry:** left plot x=60..340, right plot x=420..700; both with baseline y=245, top y=70; x = morning customers 0–80, y = croissants sold 0–90; 12px `#444` tick labels "0", "40", "80" on x and "0", "45", "90" on y of each panel.
- **Panel titles (bold 13px, `#1a5276`, centered over each panel):** left "Data drift — inputs moved, rule held"; right "Concept drift — inputs same, rule changed".
- **Left panel:** grey `#6b7280` 2px fit line y = 20 + 0.8x from (0, 20) to (80, 84); blue `#2a78d6` 5px training dots at `[[20,36],[25,41],[30,43],[35,48],[40,52]]`; orange `#d95926` 5px this-month dots at `[[55,64],[60,67],[65,73],[70,75],[75,81]]`; 12px labels "training" (blue, near the blue cluster) and "now" (orange, near the orange cluster); light `#6b7280` arrow from the blue cluster toward the orange cluster along the line.
- **Right panel:** dashed grey `#6b7280` (dash 6/4) 2px old line y = 20 + 0.8x from (0, 20) to (80, 84); solid magenta `#d55181` 3px new line y = 20 + 0.4x from (0, 20) to (80, 52); blue 5px old dots at `[[20,37],[30,45],[40,52],[50,59],[60,69]]`; magenta 5px new dots at `[[20,29],[30,31],[40,36],[50,41],[60,43]]`; vertical dashed `#6b7280` (dash 4/3) guide at x=40 from the new line up to the old line, small tick dots on both lines.
- **Annotation (bold 12px magenta `#d55181`, right panel near x=40 guide, two lines):** "same 40-customer morning:" / "52 → 36 croissants".
- **Caption (12px `#444`, bottom right):** "illustrative — five points per group".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`. Red reserved for genuine error/alarm states (the post-drift error line and the error alarm).
- **Data:** all series are the hardcoded literal arrays above (no randomness); the weekly errors, hour-bucket shares (each column sums to 100), alarm weeks, and the 52 → 36 rule change must match the numbers stated in the text bullets and captions; every invented number carries an "illustrative" caption.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
