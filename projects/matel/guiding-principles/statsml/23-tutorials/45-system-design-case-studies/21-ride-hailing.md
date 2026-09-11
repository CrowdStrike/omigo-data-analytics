# Ride-Hailing

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Ride-Hailing

**Subtitle:** A generic ride-hailing design exercise: matching moving supply to moving demand — index drivers by map cell, rank candidates by ETA, and let a per-cell price balance the two

## A Rider Taps Request in a City of Moving Cars

**Tags:** `core idea` (blue), `geospatial index` (green), `hex cells` (orange)

- **The setup** — a generic ride-hailing app: thousands of drivers roam a city; a rider taps request
- **The pings** — every driver's phone streams its location every 4 seconds, idle or on a trip
- **The cells** — the map splits into ~1 km buckets: geohash squares or H3-style hexes, both public ideas
- **The index** — each cell keeps the set of idle drivers currently inside it, updated on every ping
- **The lookup** — "who is near this rider" becomes "read the rider's cell plus its 6 neighbors"

*Example (italic):* A rider in the center cell triggers a read of 7 cells; the buckets return 9 idle drivers in about a millisecond.

**Key point:** Bucketing moving drivers into fixed map cells turns an impossible "scan every car" question into a handful of bucket reads — the cells stand still even though the supply doesn't.

### Visualization (canvas `c1`, 720×300)

Hexagonal cell map: the rider's cell plus its 6 neighbors highlighted as the queried set, each hex labeled with its idle-driver count; faded outer hexes suggest the rest of the grid.

- **Title (bold 15px, `#1a5276`, top center):** "One Request Reads 7 Cells, Not Every Car in the City".
- **Queried hexes:** flat-top hexagons, radius 46, fill `rgba(42,120,214,0.18)`, 2px `#2a78d6` border; centers at (330,165) center, (330,85) top, (399,125) upper-right, (399,205) lower-right, (330,245) bottom, (261,205) lower-left, (261,125) upper-left.
- **Driver counts (bold 14px `#1a5276`, at each hex center):** center 1, top 2, upper-right 0, lower-right 3, bottom 1, lower-left 2, upper-left 0 — total 9.
- **Faded hexes:** same geometry, 1px `#e5e9ef` border, no fill, centers at (468,85), (468,245), (192,85), (192,245); 12px `#6b7280` counts `[1, 0, 2, 1]` — visible but not queried.
- **Rider marker:** magenta `#d55181` filled dot radius 6 at (330,190) inside the center hex, bold 12px magenta label "rider" just below it.
- **Annotation (bold 13px green `#008300`, right side near x=520, y=140):** "7 cells read → 9 idle drivers found".
- **Caption (12px `#444`, bottom right):** "cell size and counts illustrative".

## Closest in Meters Is Slowest in Minutes

**Tags:** `worked example` (blue), `ETA ranking` (green)

- **The candidates** — the 7-cell query returns drivers; keep four for the hand-check: A, B, C, D
- **Straight-line** — A is closest at 0.4 km, then C at 0.7 km, B at 1.1 km, D at 1.5 km
- **The road** — A sits across a river with one bridge: 0.4 km as the crow flies, 9 min by car
- **The ETAs** — the routing engine gives B 3 min, D 5 min, C 6 min, A 9 min — the order flips
- **The pick** — dispatch B: third-farthest in meters, fastest in minutes

*Example (italic):* Driver B is nearly 3× farther than A in straight-line distance (1.1 km vs 0.4 km) yet arrives 6 minutes sooner (3 min vs 9 min).

**Key point:** Cheap cell lookups only shortlist candidates — the ranking that picks the winner uses ETA over the road network, never straight-line distance.

### Visualization (canvas `c2`, 720×300)

Grouped horizontal bars, one row per driver: straight-line distance (blue) vs routed ETA (orange), showing the rank flip between meters and minutes.

- **Title (bold 15px, `#1a5276`, top center):** "Closest in Meters Is Slowest in Minutes: Rank by ETA".
- **Legend (12px, under title at x=90, y=52):** blue swatch "straight-line km", orange swatch "ETA min".
- **Rows (top of each pair at y = 75, 130, 185, 240), row label bold 12px `#2c3e50` at x=20:** "Driver A", "Driver B", "Driver C", "Driver D".
- **Distance bars (top bar of each pair, 13px tall, from x=90):** fill `rgba(42,120,214,0.30)` with 2px `#2a78d6` edge, scale 160 px per km — widths A 64 (0.4 km), B 176 (1.1 km), C 112 (0.7 km), D 240 (1.5 km); 11px `#444` labels "0.4 km" / "1.1 km" / "0.7 km" / "1.5 km" at bar ends.
- **ETA bars (4px below the distance bar, 13px tall, from x=90):** solid orange `#d95926`, scale 28 px per min — widths A 252 (9 min), B 84 (3 min), C 168 (6 min), D 140 (5 min); B's ETA bar drawn solid green `#008300` instead; 11px labels "9 min" / "3 min" / "6 min" / "5 min" at bar ends.
- **Highlights:** bold 12px blue `#2a78d6` "closest (0.4 km)" right of Driver A's distance bar; bold 12px green `#008300` "fastest (3 min) — dispatched" right of Driver B's ETA bar.
- **Annotation (bold 13px magenta `#d55181`, top right near x=430, y=58):** "the crow flies over the river; the car takes the bridge".
- **Caption (12px `#444`, bottom right):** "distances and ETAs illustrative".

## A Firehose of Writes and a Price That Breathes

**Tags:** `why it matters` (blue), `write-heavy` (orange), `surge pricing` (green)

- **The volume** — 500,000 drivers × one ping per 4 s = 125,000 location writes/s in one region
- **Write-heavy** — updates dwarf queries, so cell buckets are overwritten in memory, not rebuilt trees
- **Both sides move** — riders open the app and move too; open requests are counted into the same cells
- **The imbalance** — a stadium cell shows 42 open requests against 6 idle drivers after a game
- **The signal** — a per-cell multiplier raises price where demand outruns supply, nudging drivers over

*Example (italic):* The stadium cell at 42 requests vs 6 idle drivers gets a 2.6× multiplier; balanced suburb cells nearby stay at 1.0×.

**Key point:** The same cell grid does three jobs — a write-heavy supply index, a demand counter, and the unit at which surge pricing balances the two sides of the marketplace.

### Visualization (canvas `c3`, 720×300)

Grouped vertical bars per cell: open requests (magenta) vs idle drivers (green), with the resulting surge multiplier printed above each pair.

- **Title (bold 15px, `#1a5276`, top center):** "Demand vs Idle Supply Per Cell Sets the Surge Multiplier".
- **Axes:** baseline 2px `#999` at y=245 from x=55 to x=660; y = count 0 to 45, gridlines `#e5e9ef` at counts 15/30/45 (4 px per unit, so y = 185/125/65), 12px `#444` y-tick labels at x=45.
- **Cells (pair centers at x = 105, 205, 305, 405, 505, 605), 12px `#444` labels under the baseline:** "airport", "stadium", "downtown", "suburb A", "suburb B", "riverside".
- **Demand bars (left of each pair, 26px wide):** solid magenta `#d55181`, heights from requests `[24, 42, 30, 8, 5, 11]`; 11px value labels above each bar.
- **Supply bars (right of each pair, 26px wide):** fill `rgba(0,131,0,0.35)` with 2px `#008300` edge, heights from idle drivers `[12, 6, 10, 14, 9, 11]`; 11px value labels above each bar.
- **Surge labels (bold 13px, centered above each pair at y=58):** "1.5×" orange `#d95926`, "2.6×" red `#e74c3c`, "1.8×" orange `#d95926`, then "1.0×" three times in mute `#6b7280`.
- **Legend (12px, top left at x=60, y=40):** magenta swatch "open requests", green swatch "idle drivers".
- **Annotation (bold 13px red `#e74c3c`, near x=205, y=88, by the stadium pair):** "42 vs 6 → 2.6×".
- **Caption (12px `#444`, bottom right):** "counts and multipliers illustrative".

## A Trip Is a State Machine, Not a Status Flag

**Tags:** `common mistake` (red), `state machine` (orange)

- **The lifecycle** — requested → matched → en route → completed, with cancel edges from early states
- **The mistake** — storing trip status as a free-form flag that any service can overwrite in any order
- **The race** — two dispatchers both read "requested" and each assigns a driver: two cars arrive
- **The fix** — transitions are compare-and-set: "matched" commits only if the state is still "requested"
- **The audit** — an explicit machine makes every trip replayable and every illegal hop rejectable

*Example (italic):* Dispatcher 2's "match driver Q" lands 80 ms after dispatcher 1's "match driver P"; the compare-and-set sees state = matched and rejects it — one car, not two.

**Common mistake:** Treating trip status as a field to overwrite. Concurrent matchers, retries, and late cancels all race on it — only atomic, whitelisted transitions keep one rider paired with exactly one driver.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: a status flag racing two match writes (both accepted, two cars) vs a compare-and-set state machine (second match rejected, clean lifecycle).

- **Title (bold 15px, `#1a5276`, top center):** "Two Matchers Race: Status Flag vs Compare-and-Set".
- **Box style:** 120px wide, 34px tall, 8px radius, 12px `#2c3e50` text; fills `rgba(42,120,214,0.15)` blue / `rgba(231,76,60,0.12)` red / `rgba(0,131,0,0.12)` green.
- **Row 1 (centered on y=95), label 12px `#444` at x=15:** "status flag"; blue box "requested" at x=110; two 3px `#2c3e50` arrows fanning to red boxes "matched: P" at (300, y=62) and "matched: Q" at (300, y=112) — both writes accepted; bold 12px red `#e74c3c` "✗ two cars dispatched" at x=470, y=95.
- **Row 2 (centered on y=205), label 12px `#444` at x=15:** "compare-and-set"; blue box "requested" at x=110, 3px arrow to green box "matched: P" at x=260, then arrows to green boxes "en route" at x=410 and "completed" at x=560.
- **Rejected write:** dashed 2px `#6b7280` (dash 4/3) arrow rising from (320, 268) into the "matched: P" box, 12px `#6b7280` label "match Q, 80 ms late" beside its tail; bold 12px red `#e74c3c` "rejected: state ≠ requested" at (330, 252).
- **Annotation (bold 13px orange `#d95926`, centered near y=290):** "a transition commits only if the current state is what it expects".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); driver counts, distances, ETAs, ping rates, demand/supply counts, and surge multipliers are invented and labeled illustrative; geohash and H3-style hex cells are cited only as publicly documented open-source ideas — the page is a generic design exercise, not a description of any real company's internal systems.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
