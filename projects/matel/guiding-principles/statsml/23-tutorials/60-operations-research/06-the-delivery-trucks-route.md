# The Delivery Truck's Route

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** The Delivery Truck's Route

**Subtitle:** One driver, a hundred stops, billions of possible orders — how the route gets picked in seconds and why it avoids left turns

## Stop Order Is Money

**Tags:** `worked example` (blue), `routing` (green)

- **The scene** — one driver, four stops (A, B, C, E): leave the depot, visit all four, come back
- **Order as loaded** — Depot → E → A → C → B → Depot: 9 + 8 + 6 + 3 + 7 = 33 miles
- **The best order** — Depot → A → B → E → C → Depot: 3 + 4 + 5 + 4 + 5 = 21 miles
- **Same table** — nothing changed but the visiting order, yet the day is 12 miles shorter
- **Fleet stakes** — 12 miles × 500 trucks × 300 days = 1.8 million truck-miles saved a year
- **In dollars** — at roughly $1 per truck-mile in fuel, time, and wear, that's ~$1.8M a year

*Example (italic):* Every leg mileage is on the two maps — add each five-leg tour yourself and you get 33 and 21 from the same table.

**Key point:** The visiting order is a free decision — same truck, same driver, same stops — yet it moves real money. Picking the sequence well IS the product.

### Visualization (canvas `c1`, 720×300)

Two schematic route maps side by side — identical five locations, different visiting order. The bad tour crosses itself; the good one doesn't.

- **Title (bold 15px, `#1a5276`, top center):** "Same Four Stops, Two Visiting Orders (miles, illustrative)".
- **Divider:** 1px `#e5e9ef` vertical line at x=360 from y=38 to y=248.
- **Panel node coordinates (local, add ox=8 for left panel, ox=368 for right):** Depot D square 16×16 centered (60,200) fill `#1a5276`, white bold 11px "D"; stops A(100,130), B(185,85), C(205,185), E(300,120) as r=11 circles fill `#fbfcfd` stroke 2px `#2a78d6`, bold 12px blue letter centered.
- **Panel headers (bold 13px, centered at local x=180, y=50):** left orange `#d95926` "as loaded — 33 miles"; right green `#008300` "best order — 21 miles".
- **Left route (2px orange polyline drawn under nodes):** D→E(9), E→A(8), A→C(6), C→B(3), B→D(7); bold 12px orange mile labels near leg midpoints (offsets tuned so none overlap). Legs cross — visual sign of waste.
- **Right route (2px green polyline):** D→A(3), A→B(4), B→E(5), E→C(4), C→D(5); bold 12px green mile labels, no crossings.
- **Annotation (bold 13px green `#008300`, centered at y=268):** "same stops, same mileage table — the order alone saves 12 miles a day".
- **Caption (12px `#6b7280`, centered at y=288):** "leg labels are miles from one shared distance table (illustrative, not to scale)".

## Good Enough, Fast

**Tags:** `core idea` (blue), `heuristics` (orange)

- **Small is easy** — four stops have only 12 distinct orders; you can check every one by hand
- **The wall** — 100 stops have ~9 × 10^157 possible orders; no computer could ever check them all
- **The trick** — start with any decent route, then keep trying small swaps of two stops
- **Keep what helps** — a swap that shortens the route stays; one that doesn't is undone
- **Stop when flat** — when swaps stop helping, take the route: near-best, never proven best
- **Seconds vs decades** — close-to-best in seconds beats provably perfect in decades

*Example (italic):* The planner's curve falls from 148 miles to about 91 in a few hundred swap attempts, then flattens — so it stops and prints the route.

**Key point:** Nobody finds THE best 100-stop route. Planners improve until improvement dries up, because a good route now is worth more than a perfect route next century.

### Visualization (canvas `c2`, 720×300)

An improvement curve: route length dropping steeply over swap attempts, then flattening, with a "stop here" annotation and a dashed unknown-true-best line below.

- **Title (bold 15px, `#1a5276`, top center):** "Keep Swapping Until It Stops Helping (illustrative)".
- **Axes:** 1px `#999`; plot area left=64, right=690, top=48, bottom=248. X = swap attempts 0–400, ticks 0/100/200/300/400 (12px mute, y=266). Y = route miles 80–150, ticks 80/100/120/140 (12px mute, right-aligned at x=56) with light `#e5e9ef` gridlines. Axis captions 12px mute: "swap attempts" centered (377,284); "route length (miles)" left-aligned at (64,40).
- **Data (hardcoded, x = 0..400 step 20):** miles = [148, 137, 128, 120, 113, 107.5, 103, 99.5, 97, 95, 93.5, 92.5, 91.9, 91.5, 91.3, 91.2, 91.15, 91.1, 91.1, 91.1, 91.1]; 2.5px blue `#2a78d6` line, r=3 blue dots every third point.
- **Start marker (bold 12px orange `#d95926`, left-aligned at (80,58)):** "start: any decent route (148 mi)".
- **Dashed line (1.5px `#6b7280`, dash [6,5]) at 89 miles**, 12px mute label "true best — unknown, somewhere down here" left-aligned at (70, just above the line).
- **Annotation (bold 13px green `#008300`, centered at (490,178)):** "improvements dry up — stop here and drive", with a 1.5px green arrow from (490,188) down to the curve near x=300.

## The Famous Left-Turn Rule

**Tags:** `where it's used` (blue), `constraints` (green)

- **The rule** — a large parcel carrier's planner famously builds routes that avoid left turns
- **Why lefts hurt** — a left waits across oncoming traffic: idle fuel, lost time, crash risk
- **The loopy fix** — three right turns around a block skip the wait, so routes look loopy
- **Odd but optimal** — a constraint that looks odd on the map wins on the objective: cost and safety
- **The lesson** — optimize the real objective (time, fuel, risk), not how the route looks

*Example (italic):* Illustrative: one left across a busy road can idle a truck for a minute; over hundreds of turns a day per truck, the loops pay for themselves.

**Key point:** Judge an optimizer by its objective, not by intuition — a route that looks wrong on the map can be measurably faster, cheaper, and safer.

### Visualization (canvas `c3`, 720×300)

Two panels: a left turn waiting across oncoming traffic vs a right-favoring loop around a block that ends up heading the same way.

- **Title (bold 15px, `#1a5276`, top center):** "One Left Turn vs Three Rights (illustrative)".
- **Divider:** 1px `#e5e9ef` vertical at x=360 from y=38 to y=250.
- **Left panel:** header bold 13px `#2c3e50` centered (185,50): "turning left: wait across traffic". Roads as `#e9edf1` bands: horizontal (30,130,310×48), vertical (150,60,48×188); dashed 1px `#b8c4cf` centerlines at y=154 and x=174. Truck: blue `#2a78d6` rect (180,200,12,24), 12px blue label "truck" at (216,216). Intended path: dashed 2px orange `#d95926` from (186,200) up then quadratic curve left exiting west at y=142, arrowhead at (66,142) — it cuts across the oncoming lane. Oncoming: two violet `#4a3aa7` rects (156,70,12,22) and (156,105,12,22) with small down arrows, 12px violet "oncoming" right-aligned at (144,81). Panel caption bold 12px orange centered (185,266): "waits across oncoming traffic — idle fuel, crash exposure".
- **Right panel:** header bold 13px centered (540,50): "three rights around the block". Roads: horizontal bands (390,100,300×32) and (390,190,300×32); vertical bands (430,100,32×122) and (600,100,32×122); block rect (462,132,138×58) fill `#f4f6f8` stroke `#d5dbe1`, 11px mute "block" centered (531,165). Green `#008300` 2.5px path: north up x=454 from y=245 to y=124, east along y=116 to x=608, south down x=608 to y=198, west along y=206 to x=386 with arrowhead — three right turns, ends heading west (same as one left). Skipped left: short dashed 1.5px orange curve from (454,238) toward (398,212) with a small orange "×" and bold 11px orange label "the skipped left" centered (430,250). Panel caption bold 12px green centered (540,266): "longer path, no waiting — quicker and safer on average".
- **Insight (bold 13px magenta `#d55181`, centered at y=290):** "optimize the objective, not the look: loopy routes finish faster".

## What Feeds the Route

**Tags:** `where it's used` (blue), `data in, decision out` (orange)

- **Traffic history** — travel-time estimates per road, per hour of day, come from months of drives
- **Volume forecast** — tomorrow's package count per neighborhood is a prediction, not a fact
- **Time windows** — promised delivery slots become hard constraints the route must respect
- **Garbage in** — a bad travel-time estimate makes the "optimal" route optimal for a fake city
- **Two crafts** — data science builds forecasts upstream; routing searches over them downstream

*Example (italic):* If the model says a bridge takes 5 minutes at 8am when it really takes 20, the planner happily routes every truck across it.

**Key point:** Routing is a forecast-fed search over stop orders — the answer is only as good as the forecasts feeding it, and good-enough-now beats perfect-too-late.

### Visualization (canvas `c4`, 720×300)

A flow diagram: three input boxes converging into a route-planner box, which emits today's route.

- **Title (bold 15px, `#1a5276`, top center):** "What Feeds the Route Planner".
- **Input boxes:** x=40, width 252, height 54, y = 52 / 126 / 200; fill `#fbfcfd`, 2px colored border; bold 13px header at (x+14, y+22), 12px `#2c3e50` sub-line at (x+14, y+42):
  - "TRAFFIC HISTORY" (blue `#2a78d6`) / "travel time per road, per hour of day"
  - "PACKAGE FORECAST" (green `#008300`) / "predicted parcels per neighborhood"
  - "TIME WINDOWS" (orange `#d95926`) / "promised delivery slots (hard limits)"
- **Arrows:** 1.5px `#6b7280` from each box's right edge (x=296, mid-height) converging to (348,153), one filled mute arrowhead pointing right.
- **Planner box:** (356,118) 176×70, fill `#fbfcfd`, 2px violet `#4a3aa7` border; bold 13px violet "ROUTE PLANNER" centered (444,148); 12px `#2c3e50` "search over stop orders" centered (444,170).
- **Output arrow:** 1.5px mute from (532,153) to (562,153) with arrowhead; output box (570,124) 136×58, 2px green border; bold 13px green "TODAY'S ROUTE" centered (638,150); 12px "stop order + ETAs" centered (638,171).
- **Caption (12px `#6b7280`, centered at y=262):** "data science upstream, routing search downstream".
- **Insight (bold 13px magenta `#d55181`, centered at y=286):** "the route is only as good as the forecasts feeding it".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data integrity:** the mileage table is fixed (D–A 3, D–B 7, D–C 5, D–E 9, A–B 4, A–C 6, A–E 8, B–C 3, B–E 5, C–E 4); tour totals 33 and 21 were verified by enumerating all 12 distinct four-stop tours (21 is the true minimum, 33 the true maximum). The 10^157 figure is 100! ≈ 9.3 × 10^157. All other numbers are labeled illustrative. In regenerated HTML, write 10^157 as `10<sup>157</sup>`; the superscript is the only markup beyond the shared skeleton.
- This page has no links.
