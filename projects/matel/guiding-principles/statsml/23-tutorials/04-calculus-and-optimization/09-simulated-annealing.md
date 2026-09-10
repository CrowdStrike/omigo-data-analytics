# Simulated Annealing

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Simulated Annealing

**Subtitle:** Escape a shallow valley by sometimes accepting a worse move — be adventurous while the search is "hot", turn strict as it cools, and you can climb over hills a greedy search never crosses

## A Coffee Cart Stuck at the Wrong Corner

**Tags:** `core idea` (blue), `local minimum` (orange), `hill climbing` (green)

- **The street** — a coffee cart can park at street spots 0–20, and each spot wastes some minutes per day.
- **Two dips** — spot 5 wastes 42 minutes, spot 15 wastes only 20, with a 60-minute hill at spot 10.
- **Greedy rule** — hill climbing moves to a neighbor only if it is cheaper, so it never goes uphill.
- **Stuck** — from spot 3, greedy slides to spot 5 and stops: both neighbors cost 44, worse than 42.
- **The fix** — simulated annealing sometimes accepts a worse spot, letting it cross the hill to spot 15.

*Example (italic):* An owner who only ever moves to a cheaper neighboring spot retires at spot 5, never learning that spot 15 wastes half as many minutes.

**Key point:** A search that never accepts a worse move stops at the first valley it finds. Reaching a deeper valley requires being willing to climb sometimes.

### Visualization (canvas `c1`, 720×300)

Single-panel line chart of the cost landscape over the 21 street spots, with the greedy path marked into the shallow valley and the global best marked in the deep one.

- **Title (bold 15px, `#1a5276`, top center):** "Daily Wasted Minutes per Spot: Greedy Stops in the Shallow Valley".
- **Data:** spots `0..20`; costs `[70, 62, 55, 48, 44, 42, 44, 48, 54, 58, 60, 56, 48, 38, 27, 20, 24, 32, 44, 56, 66]`.
- **Axes:** origin x=60, width 600, baseline y=250, chart height 190; y scale 0–75 with gridlines `#e5e9ef` at 20/40/60 labeled 12px `#6b7280`; x ticks at spots 0, 5, 10, 15, 20 labeled 12px `#444`; ink `#1a5276` 1.5px axis lines; x-axis label 12px `#6b7280` "street spot".
- **Landscape:** blue `#2a78d6` 3px polyline through all 21 points with 3px dots.
- **Greedy path:** orange `#d95926` bold arrows along spots 3 → 4 → 5 just above the curve; 6px orange dot at spot 5; orange bold 13px annotation "greedy stuck here: 42 min" above spot 5.
- **Global best:** 6px green `#008300` dot at spot 15; green bold 13px annotation "global best: 20 min" above spot 15.
- **Hill:** mute `#6b7280` 12px annotation "60-min hill blocks greedy" above spot 10.
- **Caption (12px `#444`, bottom right):** "illustrative costs".

## The Coin Flip That Allows Uphill Moves

**Tags:** `worked example` (blue), `acceptance rule` (green), `temperature` (orange)

- **The rule** — a downhill move is always taken; an uphill move of size Δ is taken with chance e^(−Δ/T).
- **Temperature** — T is a dial: while T is high the cart is adventurous, and as T falls it turns picky.
- **Small climb, hot** — hopping 42 → 44 (Δ = 2) at T = 10 is accepted with chance e^(−0.2) ≈ 0.82.
- **Small climb, cold** — the same Δ = 2 hop at T = 1 falls to e^(−2) ≈ 0.14, so it is usually refused.
- **Big climb, cold** — a Δ = 4 hop at T = 1 has chance e^(−4) ≈ 0.02, so late uphill is nearly banned.

*Example (italic):* At T = 10 the cart takes the 42 → 44 hop about 4 times in 5; at T = 1 it takes the same hop about 1 time in 7.

**Key point:** One formula, e^(−Δ/T), does all the work: worse moves stay possible, but bigger climbs and colder temperatures both make them rarer.

### Visualization (canvas `c2`, 720×300)

Single-panel chart of acceptance probability e^(−Δ/T) versus climb size Δ for three temperatures, with the three worked-example points marked.

- **Title (bold 15px, `#1a5276`, top center):** "Chance of Accepting an Uphill Move: e^(−Δ/T)".
- **Data:** Δ sampled at `0, 0.5, 1, ..., 10` (deterministic, no randomness); three curves computed as `Math.exp(-d/T)` for T = 10, T = 4, T = 1.
- **Axes:** origin x=60, width 600, baseline y=250, chart height 190; y scale 0–1 with gridlines `#e5e9ef` at 0.25/0.5/0.75 labeled 12px `#6b7280`; x ticks at 0, 2, 4, 6, 8, 10 labeled 12px `#444`; x-axis label 12px `#6b7280` "size of the climb Δ (minutes)".
- **Curves:** T = 10 blue `#2a78d6` 3px, T = 4 aqua `#199e70` 3px, T = 1 magenta `#d55181` 3px; each labeled bold 13px in its own color at the right end ("T = 10", "T = 4", "T = 1").
- **Marked points:** 5px dots with bold 12px labels — blue at (2, 0.82) labeled "0.82", magenta at (2, 0.14) labeled "0.14", magenta at (4, 0.02) labeled "0.02".
- **Annotation (orange `#d95926` bold 13px, upper right):** "hot = adventurous, cold = picky".

## One Fixed Walk From the Shallow Valley to the Deep One

**Tags:** `worked example` (blue), `cooling schedule` (green), `escape` (orange)

- **The run** — one fixed 12-move run starts at spot 3 with T = 12, cooling by 1 per move down to T = 1.
- **Warm-up** — moves 1–2 are easy downhill hops to spot 5, the same place where greedy retired.
- **The escape** — moves 3–7 accept five uphill hops in a row, climbing 42 → 60 minutes over the hill.
- **The payoff** — moves 8–12 ride downhill from spot 10 to spot 15, ending at the 20-minute best.
- **Locked in** — the final proposal 20 → 24 at T = 1 has chance e^(−4) ≈ 0.02, so the cart stays put.

*Example (italic):* The very moves greedy forbids — five uphill hops taken while hot — are exactly what carries the cart past the 60-minute hill.

**Key point:** Early high temperature buys exploration; late low temperature protects the best answer found. The walk shown is one fixed illustrative run, not an average.

### Visualization (canvas `c3`, 720×300)

Dual-panel chart: the fixed walk overlaid on the cost landscape (left) and the cooling schedule over the 12 moves (right), split by a vertical dashed divider at x=400.

- **Title (bold 15px, `#1a5276`, top center):** "A 12-Move Annealing Run: the Walk (left) and Its Cooling Schedule (right)".
- **Walk data (fixed, illustrative — no randomness):** positions after each step `[3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]` (steps 0–12); landscape costs as in c1, so step costs are `[48, 44, 42, 44, 48, 54, 58, 60, 56, 48, 38, 27, 20]`; temperature before each move `[12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1]`, and a final refused proposal 20 → 24 at T = 1.
- **Left panel (walk on landscape):** axis origin x=55, width 320, baseline y=245, chart height 180, y scale 0–75; landscape polyline mute `#6b7280` 2px; walk polyline violet `#4a3aa7` 3px with 4px dots through the 13 step positions; the five uphill segments (spots 5→6→7→8→9→10) overdrawn orange `#d95926` 4px; 6px green `#008300` dot at spot 15; orange bold 12px annotation "5 uphill hops accepted while hot" above the hill; x ticks at spots 0, 5, 10, 15, 20 (12px `#444`); caption 12px `#444` "fixed illustrative run".
- **Right panel (temperature):** axis origin x=430, width 250, same baseline/height; blue `#2a78d6` 3px stepped-down line through temperatures `[12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1]` over moves 1–12, y scale 0–13 with y labels 0/6/12 (12px `#6b7280`); x ticks at moves 1, 6, 12 (12px `#444`); x-axis label 12px `#6b7280` "move number"; magenta `#d55181` bold 12px annotation near the end, two lines: "T = 1: hop 20 → 24" / "refused (p ≈ 0.02)".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=400 from y=38 to h-12.

## Cooling Too Fast Is the Classic Mistake

**Tags:** `common mistake` (red), `where it's used` (blue), `rule of thumb` (green)

- **Quench** — cooling from 12 to 1 in two moves acts like greedy, freezing the cart at spot 5's 42 minutes.
- **No cooling** — holding T at 12 keeps accepting big climbs, so the cart is still bouncing near 44 at move 12.
- **Steady cooling** — dropping T by 1 per move let the walk escape first, then lock in at 20 minutes.
- **Where you meet it** — shift schedules, delivery routes, and feature subsets are full of shallow valleys.
- **No slope needed** — annealing only needs a cost number per candidate, so it works where gradients don't exist.

*Example (italic):* An impatient owner who turned strict after two days stayed at spot 5 forever; the patient one ended at spot 15.

**Common mistake:** Treating the cooling schedule as a detail. Cool too fast and you get greedy's answer back; the temperature must fall slowly enough to allow the escape climbs.

### Visualization (canvas `c4`, 720×300)

Dual-panel chart: three cooling schedules over 12 moves (left) and the final cost each schedule reaches (right), split by a vertical dashed divider at x=400.

- **Title (bold 15px, `#1a5276`, top center):** "Three Cooling Schedules, Three Endings (illustrative)".
- **Schedule data (over moves 1–12):** quench `[12, 6, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]` magenta `#d55181`; steady `[12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1]` blue `#2a78d6`; no cooling `[12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12]` yellow `#c98500`.
- **Final-cost data:** quench 42, steady 20, no cooling 44 (still bouncing).
- **Left panel (schedules):** axis origin x=55, width 320, baseline y=245, chart height 180, y scale 0–13 with y labels 0/6/12 (12px `#6b7280`); three 3px polylines in the colors above, each labeled bold 12px in its color at the right end ("quench", "steady", "no cooling"); x ticks at moves 1, 6, 12 (12px `#444`); x-axis label 12px `#6b7280` "move number".
- **Right panel (final cost bars):** axis origin x=430, width 250, same baseline/height, y scale 0–50 with gridlines `#e5e9ef` at 20/40 labeled 12px `#6b7280`; three vertical bars — quench 42 fill `rgba(213,81,129,0.5)`, steady 20 fill `rgba(42,120,214,0.5)`, no cooling 44 fill `rgba(201,133,0,0.5)` — each with its value bold 13px in the matching solid color above the bar and its name 12px `#444` below; green `#008300` bold 13px annotation "steady cooling finds 20" above the middle bar; caption 12px `#444` "illustrative single runs".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=400 from y=38 to h-12.

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Determinism:** no `Math.random()` anywhere; the annealing walk in c3 is the fixed literal sequence given above, and the only computed values are the deterministic `Math.exp(-d/T)` curves in c2.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
