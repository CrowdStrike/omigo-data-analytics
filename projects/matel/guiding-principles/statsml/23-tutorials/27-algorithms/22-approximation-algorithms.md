# Approximation Algorithms

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Approximation Algorithms

**Subtitle:** When trying every answer would take longer than a lifetime, an approximation algorithm gives a fast answer plus a proof of how close it is — provably close beats hopefully close

## A Pizza Run With Too Many Orders to Try

**Tags:** `core idea` (blue), `intractable` (orange), `guarantee` (green)

- **The job** — a pizza driver must visit 5 houses and return; which visiting order gives the shortest drive?
- **Try them all** — 5 houses means only 60 possible orders; a computer checks those in a blink
- **The explosion** — 10 houses means 1.8 million orders; 25 houses means about 7.8×10²⁴ — never finishing
- **Give up on perfect** — an approximation algorithm returns a good route fast, plus a proof it is close
- **The promise** — "2-approximation" means: never more than twice the best route, on any map, guaranteed

*Example (italic):* Checking every order for 25 houses at a billion routes per second would take about 250 million years — the driver needs an answer before the pizza is cold.

**Key point:** An approximation algorithm trades perfection for speed but keeps a mathematical guarantee: the answer is provably within a known factor of the best one possible.

### Visualization (canvas `c1`, 720×300)

Single-panel bar chart on a log scale: number of routes to check versus number of houses, with a dashed reference line for what one computer can do in a year.

- **Title (bold 15px, `#1a5276`, top center):** "Routes to Check: h Houses Means h!/2 Orders".
- **Axes:** origin x=70, baseline y=245, plot width 580, plot height 185; y is log10 scale from 10⁰ to 10²⁵ with light `#e5e9ef` gridlines and 12px `#444` labels at 10⁵, 10¹⁰, 10¹⁵, 10²⁰, 10²⁵; x has five bar positions labeled "5 houses", "10", "15", "20", "25" (12px `#444`).
- **Bars:** width 60, centers at x = `[130, 245, 360, 475, 590]`; route counts `[60, 1.8e6, 6.5e11, 1.2e18, 7.8e24]` (log10 heights `[1.78, 6.26, 11.8, 18.08, 24.9]`, i.e. bar tops at y = 245 − log10/25×185); fill `rgba(42,120,214,0.35)`, border 2px blue `#2a78d6`.
- **Value labels (bold 12px `#2a78d6`, above each bar):** "60", "1.8M", "6.5×10¹¹", "1.2×10¹⁸", "7.8×10²⁴".
- **Reference line:** horizontal dashed `#6b7280` (dash 4/3) line at 3×10¹⁶ (y≈123); 12px `#6b7280` label at its left end: "one computer-year at a billion routes/sec".
- **Annotation (bold 13px orange `#d95926`, near x=150, y=85):** two lines: "trying every order stops working" / "somewhere past 15 houses".
- **Caption (12px `#444`, bottom right):** "route counts are exact (h!/2); timing illustrative".

## Connect, Double, Shortcut: a Route With a Receipt

**Tags:** `worked example` (blue), `certificate` (green)

- **The map** — restaurant R plus houses A, B, C, E on a city grid; driving distance counted in blocks
- **Cheapest wiring** — the shortest network linking all five spots: R–A(3), A–B(4), B–C(4), C–E(4) = 15 blocks
- **The floor** — any full route must at least connect every house, so no route can beat 15 blocks
- **Walk and skip** — trace the wiring out and back (30 blocks), then skip stops already visited
- **The route** — R→A→B→C→E→R = 3+4+4+4+5 = 20 blocks; skipping repeats only ever shortens the walk
- **The receipt** — 20 ≤ 2×15, so this route is at most 20/15 ≈ 1.33× the best — no brute force needed

*Example (italic):* The driver never checked all 60 orders, yet can tell the boss: "my 20-block route is at most 33% longer than the perfect one — here is the proof."

**Key point:** The wiring cost (15) is a floor under every possible route, and the shortcut walk is at most double the wiring — that pair of facts is the whole guarantee.

### Visualization (canvas `c2`, 720×300)

Single-panel map of the delivery grid: the five stops as dots, the cheapest connecting network in solid blue with block lengths, and the dashed green return leg that completes the 20-block route.

- **Title (bold 15px, `#1a5276`, top center):** "Cheapest Wiring (15 blocks) → Route of 20 Blocks".
- **Grid:** light `#e5e9ef` 1px lines every block; block (bx, by) maps to px x = 90 + bx×85, y = 255 − by×45 (bx 0–6, by 0–4).
- **Stops (7px dots, ink `#1a5276`, bold 13px `#1a5276` labels beside each):** R at block (0,0) → px (90,255) labeled "Restaurant"; A (0,3) → (90,120); B (3,4) → (345,75); C (6,3) → (600,120); E (5,0) → (515,255).
- **Wiring edges (solid blue `#2a78d6` 3px, bold 12px blue length label at each midpoint):** R–A "3", A–B "4", B–C "4", C–E "4".
- **Return leg (dashed green `#008300` 3px, dash 6/4):** E back to R, bold 12px green label "5" at its midpoint.
- **Annotation (bold 13px green `#008300`, near px (330, 190)):** two lines: "route = 3+4+4+4+5 = 20 blocks" / "floor = 15, so at most 1.33× the best".
- **Caption (12px `#444`, bottom right):** "distances are city-block counts; illustrative map".

## Why a Guarantee Beats a Gamble

**Tags:** `where it's used` (blue), `heuristic vs guarantee` (orange)

- **Everywhere** — routing, picking cluster centers, scheduling jobs, choosing a smallest covering set
- **Exact is rare** — for these problems nobody knows a fast exact method; brute force explodes as the first chart shows
- **The gamble** — a rule of thumb like "always drive to the nearest unvisited house" carries no promise
- **The bad day** — on day 5 the nearest-next rule drives 41 blocks; the ceiling for that map is 38
- **The contract** — the guaranteed route stays at 26 that day; by proof it can never cross its ceiling

*Example (italic):* On day 5's awkward map the nearest-next rule wandered 41 blocks while the guaranteed method drove 26 — bad luck cannot touch a proven bound.

**Key point:** A heuristic is a hope and an approximation algorithm is a contract: its worst case is bounded before you ever run it on your data.

### Visualization (canvas `c3`, 720×300)

Grouped bar chart over six delivery days: nearest-next heuristic versus the guaranteed route, with each day's proven ceiling (2× floor) drawn as a dashed tick — the heuristic breaks its ceiling once, the guaranteed route never can.

- **Title (bold 15px, `#1a5276`, top center):** "Six Days of Deliveries: a Hope vs a Contract".
- **Axes:** origin x=70, baseline y=245, plot width 580, plot height 185; y = blocks driven 0 to 45, light `#e5e9ef` gridlines with 12px `#444` labels at 10, 20, 30, 40; x = six day groups centered at `[125, 220, 315, 410, 505, 600]`, 12px `#444` labels "day 1"–"day 6".
- **Heuristic bars (left of each pair, width 32):** values `[21, 24, 34, 22, 41, 26]`; fill `rgba(217,89,38,0.35)`, border 2px orange `#d95926` (bar height = v/45×185).
- **Guaranteed bars (right of each pair, width 32):** values `[20, 22, 25, 21, 26, 23]`; fill `rgba(0,131,0,0.35)`, border 2px green `#008300`.
- **Ceiling ticks:** per-day dashed ink `#1a5276` (dash 4/3) horizontal segments spanning each group at heights `[30, 32, 36, 30, 38, 34]`; one 11px `#1a5276` label "ceiling = 2 × floor" beside day 1's tick.
- **Legend (12px, top left inside plot):** orange swatch "nearest-next heuristic", green swatch "guaranteed route".
- **Annotation (bold 12px red `#e74c3c`, above day 5's tall bar):** two lines: "41 breaks the 38 ceiling —" / "the guaranteed route never can".
- **Caption (12px `#444`, bottom right):** "illustrative six days".

## The 2 Is a Ceiling, Not a Forecast

**Tags:** `common mistake` (red), `worst case` (orange)

- **The word "2"** — a 2-approximation's answers are not usually twice the best; 2 is the worst ever allowed
- **Typical days** — over 20 delivery days the route lands between 1.08× and 1.60× of its floor, mostly near 1.3×
- **Ceiling untouched** — no day reaches 2.0; the bound holds even for maps designed by an adversary
- **Even better** — the floor understates the true best route, so the real gap is smaller than these ratios
- **Wrong dismissal** — "2× off is useless" throws away a method that runs near 1.3× and is provably ≤ 2×

*Example (italic):* A manager hears "2-approximation" and budgets for doubled driving; the season's routes averaged about 1.3× the floor and never crossed 1.60×.

**Common mistake:** Reading the approximation factor as the expected error. It is a worst-case ceiling that must survive the nastiest input imaginable; typical results sit far below it.

### Visualization (canvas `c4`, 720×300)

Dot-strip chart: twenty days of route quality plotted as ratio (route ÷ floor) on a horizontal axis from 1.0 to 2.1, with the worst-case ceiling at 2.0 shown far to the right of every dot.

- **Title (bold 15px, `#1a5276`, top center):** "Twenty Days of Routes: Where They Land Below the Ceiling".
- **Axis:** horizontal 2px `#999` line at y=200 from x=80 to x=640 (width 560), ratio 1.0 to 2.1 (ratio r maps to px x = 80 + (r−1.0)/1.1×560); 12px `#444` tick labels "1.0", "1.2", "1.4", "1.6", "1.8", "2.0" below.
- **Dots (7px green `#008300`):** one per day at ratios `[1.08, 1.12, 1.15, 1.18, 1.20, 1.22, 1.25, 1.26, 1.28, 1.30, 1.31, 1.33, 1.35, 1.36, 1.38, 1.40, 1.42, 1.45, 1.52, 1.60]`; stack dots vertically in 14px steps (y = 186, 172, ...) when within 0.02 of a neighbor so none overlap.
- **Typical marker:** vertical dashed blue `#2a78d6` (dash 4/3) line at ratio 1.30 from y=110 to the axis, bold 12px blue label "typical ≈ 1.3×" at its top.
- **Ceiling line:** vertical dashed red `#e74c3c` (dash 4/3) line at ratio 2.0 from y=90 to the axis, bold 12px red label "worst-case ceiling 2.0" at its top.
- **Annotation (bold 13px violet `#4a3aa7`, near x=330, y=115):** "the 2 is a promise, not a prediction".
- **Caption (12px `#444`, bottom right):** "ratio = route ÷ floor, 20 illustrative days".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all bar values, map coordinates, day values, ceilings, and ratio arrays are the hardcoded literals above (no randomness); c1 route counts are exact h!/2 values, c2 block distances are city-block (Manhattan) counts on the stated grid, c3/c4 day data are labeled illustrative.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
