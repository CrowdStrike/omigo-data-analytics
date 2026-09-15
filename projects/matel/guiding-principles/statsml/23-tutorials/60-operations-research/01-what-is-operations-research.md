# What Is Operations Research

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** What Is Operations Research

**Subtitle:** The decision layer on top of data science — prediction answers "what will happen", operations research answers "what should we do about it"

## Two Different Questions

**Tags:** `core idea` (blue), `operations research` (green)

- **The scene** — a supermarket on a busy Saturday: shoppers pour in and checkout lines grow
- **Question one** — "how many shoppers will arrive?" — the forecast says 240 at the noon peak
- **Question two** — "how many checkout lanes do we open each hour?" — that is the staffing plan
- **Different math** — the forecast is a prediction model; the plan weighs costs, rules, and limits
- **Different owner** — a data scientist builds the forecast; an operations planner sets the lanes

*Example (italic):* The forecast can be perfect and the Saturday still fails — if nobody turns 240 shoppers into a lane count.

**Key point:** Data science answers "what will happen"; operations research answers "what should we do about it" — two different questions, owned by two different kinds of math.

### Visualization (canvas `c1`, 720×300)

Two side-by-side panels split by a light divider at x=360: a forecast curve on the left, a staffing bar plan on the right, one mute arrow between them.

- **Title (bold 15px, `#1a5276`, top center):** "Two Questions, Two Layers (illustrative)".
- **Divider:** 1px `#e5e9ef` vertical line at x=360 from y=44 to y=262.
- **Left panel header (bold 13px blue `#2a78d6`, centered at (180, 56)):** "WHAT WILL HAPPEN — forecast".
- **Left chart:** axes 1px `#999` with origin (58, 232), plot to (330, 232), top at y=78; hours `10a 11a 12p 1p 2p` at x = 90/145/200/255/310, 11px `#6b7280` labels at y=248; shoppers/hr `[120, 180, 240, 200, 140]` scaled 0–260; blue `#2a78d6` 2.5px polyline with 3.5px filled dots; peak value "240" bold 12px blue above the noon dot; y-axis caption "shoppers/hr" 11px mute at (58, 70).
- **Right panel header (bold 13px green `#008300`, centered at (545, 56)):** "WHAT SHOULD WE DO — staffing plan".
- **Right chart:** same baseline y=232, bars at x = 420/475/530/585/640 (width 36), lanes `[4, 6, 8, 7, 5]` scaled 0–9 over 140px; fill `rgba(0,131,0,0.4)`, 1px green stroke; bold 13px green value labels above each bar; same hour labels below; y-axis caption "lanes open" 11px mute at (408, 70).
- **Arrow:** 2px mute `#6b7280` line from (338, 150) to (390, 150) with filled arrowhead at the right end.
- **Bottom caption (12px `#6b7280`, centered at y=284):** "same Saturday, two different questions — prediction on the left, decision on the right".

## Predict, Then Optimize

**Tags:** `worked example` (blue), `lane math` (orange)

- **The forecast** — shoppers per hour: 10am 120, 11am 180, noon 240, 1pm 200, 2pm 140
- **The service rate** — one cashier serves about 30 shoppers per hour
- **The rule** — lanes needed = forecast ÷ 30, rounded up so no hour is left short
- **The plan** — 120→4, 180→6, 240→8, 200→7, 140→5 lanes; redo any hour by hand
- **The layering** — the plan is plain arithmetic sitting on top of the forecast's output

*Example (italic):* Take 1pm yourself: 200 ÷ 30 = 6.67, round up to 7 lanes — the decision is the forecast plus one rule.

**Key point:** The staffing plan is arithmetic ON TOP of the forecast — change the forecast and the plan changes with it, but they remain two separate layers.

### Visualization (canvas `c2`, 720×300)

One bar chart of the shopper forecast with the computed lane count annotated above each bar.

- **Title (bold 15px, `#1a5276`, top center):** "Forecast In, Lane Count Out (illustrative)".
- **Axes:** 1px `#999`, origin (66, 226), x to (690, 226), y top at 60; y-axis caption "shoppers/hr" 11px mute at (66, 52).
- **Bars:** hours `10am 11am 12pm 1pm 2pm` at x = 100/220/340/460/580 (width 84), values `[120, 180, 240, 200, 140]` scaled 0–260 over 166px; fill `rgba(42,120,214,0.5)`, 1px blue stroke; shopper value bold 12px blue `#2a78d6` inside near the top of each bar (y of top + 18); hour labels 12px `#444` at y=242.
- **Lane labels:** bold 13px green `#008300` centered above each bar (bar top − 10): "→ 4 lanes", "→ 6 lanes", "→ 8 lanes", "→ 7 lanes", "→ 5 lanes".
- **Rule note (bold 12px orange `#d95926`, centered at y=268):** "lanes = forecast ÷ 30 per cashier, rounded up".
- **Caption (12px `#6b7280`, centered at y=288):** "blue numbers are the prediction; green numbers are the decision built from it".

## Where the Pairing Runs the World

**Tags:** `where it's used` (blue), `predict-then-optimize` (green)

- **Airlines** — no-show predictions feed the overbooking level: how many extra seats to sell
- **Delivery fleets** — travel-time estimates feed the route each van drives that day
- **Hospitals** — patient-arrival forecasts feed the nurse shift roster for the week
- **Ride-hailing** — demand forecasts feed the driver incentives offered per neighborhood
- **The split** — the data scientist owns the input number; the optimizer owns the output plan

*Example (italic):* A no-show model says 8% of ticket holders won't fly; an overbooking rule turns that into "sell 12 extra seats".

**Key point:** The same two-layer pattern runs schedules, routes, and prices everywhere: a forecast someone predicted, then a decision someone optimized from it.

### Visualization (canvas `c3`, 720×300)

A four-row flow diagram: forecast box → arrow → decision box per industry.

- **Title (bold 15px, `#1a5276`, top center):** "Forecast → Decision, Four Industries".
- **Column headers (bold 12px, centered at y=52):** "forecast (data science)" in blue `#2a78d6` at x=205, "decision (operations research)" in green `#008300` at x=545.
- **Rows (y = 64, 116, 168, 220; box height 40):** left boxes at x=60 width 290, right boxes at x=400 width 290; fill `#fbfcfd`; left border 2px blue, right border 2px green; centered 12px `#2c3e50` text:
  - "airline: no-show prediction" → "overbooking level"
  - "delivery fleet: travel-time estimates" → "routes for every van"
  - "hospital: arrival forecast" → "nurse shift roster"
  - "ride-hailing: demand forecast" → "driver incentives"
- **Arrows:** 1.5px mute `#6b7280` from (352, row mid) to (396, row mid) with filled arrowhead per row.
- **Caption (bold 12px violet `#4a3aa7`, centered at y=284):** "the data scientist owns the left column; the optimizer owns the right".

## The Common Confusion

**Tags:** `common mistake` (red)

- **The trap** — treating the model's output as the answer: "the forecast says 240, we're done"
- **Nothing happened yet** — 240 shoppers is a number; no lane opens until a rule converts it
- **The missing layer** — costs and rules: 30 shoppers/hr per cashier, wages, queue limits
- **Two layers, two owners** — predicting is one kind of math; deciding is another
- **The test** — ask "so what do we DO?"; if no plan is written down, the work is half-done

*Example (italic):* A report saying "noon peak: 240 shoppers" has predicted; only "open 8 lanes at noon" has decided.

**Common mistake:** Believing the model's output is the answer. Predict-then-optimize is a two-layer pattern, and the layers are owned by different math — a forecast is never a decision by itself.

### Visualization (canvas `c4`, 720×300)

A three-box pipeline (forecast → costs & rules → decision) with a crossed-out shortcut arrow that tries to skip the middle layer.

- **Title (bold 15px, `#1a5276`, top center):** "A Forecast Is Not a Decision".
- **Boxes (y=110, height 64):** "FORECAST" at x=48 width 180, 2px blue `#2a78d6` border, bold 13px blue header at box top+26, sub-line 12px `#2c3e50` "240 shoppers at noon" at top+48; "COSTS & RULES" at x=272 width 180, 2px orange `#d95926` border, sub-line "30/hr per cashier, wages"; "DECISION" at x=496 width 180, 2px green `#008300` border, sub-line "open 8 lanes at noon". All fills `#fbfcfd`.
- **Pipeline arrows:** 2px mute `#6b7280` from (232, 142) to (268, 142) and from (456, 142) to (492, 142), filled arrowheads.
- **Shortcut arc:** 2px red `#e74c3c` curved line (quadratic, control point (362, 30)) from (138, 106) to (586, 106) with a red arrowhead at the right end; a bold 14px red "✕" at the arc's top (362, 52); label bold 12px red centered at (362, 74): "\"the forecast IS the plan\" — no, it isn't".
- **Caption (bold 13px magenta `#d55181`, centered at y=240):** "nothing happens until costs and rules turn the number into a plan".
- **Sub-caption (12px `#6b7280`, centered at y=262):** "layer 1 predicts, layer 2 decides — different math, different owners".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) (and the CSS-width ratio) and calls `ctx.scale(...)`; charts stored in an array, redrawn on debounced window resize.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red `#e74c3c` only for the crossed-out shortcut in c4. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data integrity:** hardcoded arrays only — shoppers/hr `[120, 180, 240, 200, 140]`, lanes `[4, 6, 8, 7, 5]` (= ceil(forecast/30)); invented numbers carry "(illustrative)" in chart titles.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
