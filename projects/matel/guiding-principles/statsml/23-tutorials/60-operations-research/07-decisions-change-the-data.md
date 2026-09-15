# Decisions Change the Data

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Decisions Change the Data

**Subtitle:** The plan you run decides what gets logged — so tomorrow's forecast learns from a world your own decisions filtered

## You Only See What You Offered

**Tags:** `core idea` (blue), `feedback loop` (green)

- **The store** — a grocery store logs sales only for products it stocked; unstocked demand leaves no trace
- **The courier** — a driver logs traffic only on streets actually driven; skipped streets go unmeasured
- **The site** — clicks get logged only on items actually shown; hidden items score zero forever
- **The filter** — the log is not the world; it is the world seen through your own plan
- **The trap** — the data looks complete because every row is real, yet whole options never appear

*Example (italic):* The store never stocked oat milk, so its sales log "proves" that nobody wants oat milk.

**Key point:** A log records the world under your plan, not the world itself — whatever the plan never offered simply does not exist in the data.

### Visualization (canvas `c1`, 720×300)

Two side-by-side bar panels split by a light divider at x=360: full true demand for five products on the left, and the same chart on the right with the two unstocked products reduced to dashed empty outlines.

- **Title (bold 15px, `#1a5276`, top center):** "The World vs Your Log (illustrative)".
- **Divider:** 1px `#e5e9ef` vertical line at x=360 from y=44 to y=262.
- **Left panel header (bold 13px `#2c3e50`, centered at (188, 56)):** "the world — true demand".
- **Right panel header (bold 13px `#2c3e50`, centered at (548, 56)):** "your log — demand under your plan".
- **Shared bar geometry:** five products `A B C D E`, demands `[120, 90, 70, 50, 40]`; baseline y=232, plot height 150, value scale 0–140, bar width 40, bar x positions 70/128/186/244/302 (left panel) and the same +360 (right panel); product letters 12px `#444` centered at y=248.
- **Left bars:** all five filled `rgba(42,120,214,0.55)` with bold 12px blue `#2a78d6` value labels above each bar top.
- **Right bars:** A, B, C (the stocked ones) drawn exactly like the left panel; D and E drawn as dashed outlines only — no fill, 1.5px `#b8c4cf` stroke with `setLineDash([4,3])` at their true-demand heights, a mute 12px "?" centered inside each, and a shared caption "never observed" 12px `#6b7280` centered between them at y=104.
- **Insight (bold 13px magenta `#d55181`, centered at (360, 288)):** "the log only contains what the plan exposed".

## The Staffing Spiral, by the Numbers

**Tags:** `worked example` (blue), `self-inflicted decline` (orange)

- **The rule** — the store staffs one cashier per 40 forecast shoppers; too few cashiers, long lines
- **Week 1** — forecast 200, staff 5 cashiers; lines crawl, 40 shoppers walk out, log shows 160 sales
- **Week 2** — forecast drops to 160, staff 4; 70 walk out, log shows 130 — the "decline" continues
- **Week 3** — forecast 130, staff 3; 90 walk out, log shows 110 — the spiral keeps feeding itself
- **Week 4** — forecast 110, staff 3; 95 walk out, log shows 105 — yet demand never moved at all

*Example (italic):* True demand was 200 shoppers every single week — the falling sales curve is the staffing plan's shadow, not the market's.

**Key point:** Inside the log, a decision-caused decline looks exactly like falling demand — you can only tell them apart by knowing what the plan offered each week.

### Visualization (canvas `c2`, 720×300)

Two lines over four weeks: flat dashed true demand at 200 on top, solid declining logged sales below it.

- **Title (bold 15px, `#1a5276`, top center):** "Logged Sales Fall While True Demand Never Moves".
- **Axes:** 1px `#999`, origin (64, 236), x to (692, 236), y top at 56; y tick labels `0 / 100 / 200` 12px `#6b7280` right-aligned at x=56, value scale 0–220 over 180px; week labels "week 1..week 4" 12px `#444` centered under their x positions at y=254.
- **X positions:** weeks 1–4 at x = 130/290/450/610.
- **True demand line:** dashed (`setLineDash([7,5])`) 2.5px blue `#2a78d6` horizontal at value 200; label "true demand: 200 every week" bold 12px blue, left-aligned at (140, yOf(200) − 12).
- **Logged sales line:** solid 2.5px magenta `#d55181` through `[160, 130, 110, 105]` with 4px filled dots and bold 12px magenta value labels 12px below each dot.
- **Annotation (bold 13px magenta `#d55181`, centered at (400, 178)):** "the decline is self-inflicted (illustrative)".
- **Caption (12px `#6b7280`, centered at y=286):** "logged sales = 200 true shoppers minus whoever walked out of the line".

## Why the Next Model Inherits It

**Tags:** `feedback loop` (orange), `training data` (green)

- **The training set** — next week's forecast is fit on logged sales, not on true demand
- **The fingerprint** — the model learns the staffing plan's filter and calls it a demand trend
- **Retraining** — every retrain bakes the last plan in deeper; the model gets worse, confidently
- **The loop** — the plan shapes the logs, the logs train the forecast, the forecast sets the plan
- **No alarm rings** — backtests look great, because the model predicts its own logs perfectly

*Example (italic):* By week 4 the model forecasts 105 shoppers with excellent historical accuracy — measured against the very logs its plan created.

**Key point:** When decisions feed the training data, retraining does not wash the bias out — it compounds it, and standard accuracy metrics will applaud.

### Visualization (canvas `c3`, 720×300)

A circular four-box loop diagram — plan, logs, training data, forecast — with clockwise arrows and a "bias enters here" marker on the plan-to-logs arrow.

- **Title (bold 15px, `#1a5276`, top center):** "The Loop: Your Plan Ends Up Inside Your Model".
- **Boxes (fill `#fbfcfd`, 2px colored border, bold 13px colored header centered at box y+19, 12px `#2c3e50` sub-line centered at y+37):**
  - "PLAN" (blue `#2a78d6`) / "staff 4 cashiers" at (285, 52, 150×46) — top
  - "LOGS" (magenta `#d55181`) / "walk-outs never recorded" at (505, 142, 190×46) — right
  - "TRAINING DATA" (yellow `#c98500`) / "last month of logged sales" at (265, 232, 190×46) — bottom
  - "FORECAST" (violet `#4a3aa7`) / "predicts 130, not 200" at (30, 142, 180×46) — left
- **Arrows (2px `#6b7280` with filled mute arrowheads), clockwise:** PLAN right edge → LOGS top edge; LOGS bottom edge → TRAINING DATA right edge; TRAINING DATA left edge → FORECAST bottom edge; FORECAST top edge → PLAN left edge.
- **Bias marker:** bold 13px magenta `#d55181` text "bias enters here" at (600, 92), with a short 1.5px magenta pointer line from (598, 100) down-left to the midpoint of the PLAN→LOGS arrow.
- **Caption (12px `#6b7280`, centered at (360, 294)):** "each cycle around the loop, the plan's fingerprint gets rebranded as demand".

## What Careful Teams Do

**Tags:** `rule of thumb` (green), `where it's used` (blue)

- **Keep exploring** — deliberately over-staff a random hour now and then to observe unthrottled demand
- **Log the offer** — record lanes open, items shown, and wait times, not just the completed sales
- **Missing, not absent** — a walked-away shopper is missing data, not evidence of zero demand
- **Read logs as pairs** — every logged number means "under that plan"; interpret the two together

*Example (italic):* One deliberately over-staffed Saturday would have shown 200 shoppers and snapped the spiral in a single week.

**Key point:** When your decisions shape your data, the log tells you about your plan as much as about the world — buy back unbiased data with a little planned randomness.

### Visualization (canvas `c4`, 720×300)

Three stacked practice boxes, each a habit that breaks the feedback loop, with a green takeaway line beneath.

- **Title (bold 15px, `#1a5276`, top center):** "Three Habits That Break the Spiral".
- **Boxes:** three rectangles at x=60, width 600, height 56, y = 52 / 128 / 204; fill `#fbfcfd`, 2px colored border. Each has a bold 13px colored header (left-aligned at x+16, y+23) and a 12px `#2c3e50` sub-line (left-aligned at x+16, y+43):
  - "KEEP EXPLORING" (green `#008300`) / "over-staff a random hour on purpose — buy a clean look at true demand"
  - "LOG THE OFFER" (blue `#2a78d6`) / "record lanes open, items shown, wait times — not just completed sales"
  - "TREAT GAPS AS MISSING" (orange `#d95926`) / "a walked-away shopper is missing data, not zero demand"
- **Takeaway (bold 13px green `#008300`, centered at (360, 288)):** "a little planned randomness is the price of unbiased data".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) (and the displayed CSS width) and calls `ctx.scale(...)`; charts stored in an array and redrawn on debounced window resize.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data integrity:** all series are hardcoded literals (no `Math.random()`); the week-by-week numbers (200/160/130/110/105 forecasts, 160/130/110/105 logged) must match between bullets and the `c2` chart.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
