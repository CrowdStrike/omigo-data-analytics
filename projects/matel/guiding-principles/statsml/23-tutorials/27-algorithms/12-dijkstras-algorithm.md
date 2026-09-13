# Dijkstra's Algorithm

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Dijkstra's Algorithm

**Subtitle:** To find the cheapest route, grow a bubble outward from the start and always settle the unvisited spot with the smallest total cost — once a spot is the cheapest on the frontier, its answer is final

## The Cheapest Way to the Airport

**Tags:** `core idea` (blue), `expand the frontier` (green), `shortest path` (orange)

- **The trip** — you drive from Home to the Airport through a small town with six landmark spots
- **The roads** — every road has a drive time: Home–Cafe 2 min, Home–Bank 5, Cafe–Bank 1, Bank–Park 3, and so on
- **The frontier** — at any moment some spots have a best-known total from Home; the rest are still unexplored
- **The rule** — always expand the unvisited spot with the smallest total minutes from Home, never the nearest road
- **Why it works** — every road costs time, so no later detour can ever undercut the frontier's cheapest spot

*Example (italic):* The Bank sits at the end of a 5-minute road from Home, but Home→Cafe→Bank takes 2 + 1 = 3 minutes — so the Bank's real distance is 3, not 5.

**Key point:** Dijkstra grows a bubble of settled spots outward from the start, always swallowing the cheapest frontier spot next — that spot's distance can never improve again.

### Visualization (canvas `c1`, 720×300)

Node-link road map: six labeled circles connected by roads with minute labels, the cheapest Home→Airport route drawn thick green over the gray roads.

- **Title (bold 15px, `#1a5276`, top center):** "The Town Map: Six Spots, Road Times in Minutes".
- **Nodes:** circles radius 16, white fill, 2px stroke, bold 12px `#2c3e50` label centered inside; centers at Home (90, 165), Cafe (250, 80), Bank (295, 220), Park (465, 100), Mall (500, 240), Airport (645, 165); Home and Airport get 2px `#1a5276` strokes, the rest `#6b7280`.
- **Roads:** 2px `#6b7280` lines between node edges for the pairs Home–Cafe, Home–Bank, Cafe–Bank, Cafe–Park, Bank–Park, Bank–Mall, Park–Airport, Mall–Airport; each with a bold 12px `#444` minute label on a small white background square at its midpoint: `2, 5, 1, 6, 3, 4, 4, 5` respectively.
- **Cheapest route:** redraw Home→Cafe→Bank→Park→Airport as a 4px green `#008300` polyline beneath the node circles, so the winning roads (2, 1, 3, 4) stand out.
- **Annotation (bold 13px green `#008300`, near x=330, y=290):** "cheapest route: 2 + 1 + 3 + 4 = 10 min".
- **Caption (12px `#444`, top right):** "illustrative town — drive times invented".

## Settling Six Spots, One at a Time

**Tags:** `worked example` (blue), `settle order` (green)

- **Start** — Home settles at 0 min; its two roads put Cafe at 2 and Bank at 5 on the frontier
- **Step 1** — Cafe (2) is cheapest; via Cafe, Bank improves from 5 to 2 + 1 = 3 and Park appears at 2 + 6 = 8
- **Step 2** — Bank (3) settles; Park improves to 3 + 3 = 6 and Mall appears at 3 + 4 = 7
- **Step 3** — Park (6) settles; the Airport appears on the frontier at 6 + 4 = 10
- **Step 4** — Mall (7) settles; its Airport offer of 7 + 5 = 12 loses to the existing 10, nothing changes
- **Done** — Airport settles at 10; walking the winning roads backward gives Home→Cafe→Bank→Park→Airport

*Example (italic):* The direct Home–Bank road (5 min) never gets used — the Cafe detour beat it to 3 minutes at step 1, and settled spots are never reopened.

**Key point:** Each spot settles exactly once, in order of total distance: Home 0, Cafe 2, Bank 3, Park 6, Mall 7, Airport 10 — and 10 is the final answer.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart of the settle order: six rows, one per spot in the order settled, bar length = final minutes from Home, with the beaten first estimates shown as hollow gray markers.

- **Title (bold 15px, `#1a5276`, top center):** "Settle Order: Always the Cheapest Frontier Spot Next".
- **Axis:** minutes 0 to 12; x maps 0→150 and 12→690 (45 px per minute); light `#e5e9ef` vertical gridlines every 2 minutes with 12px `#444` labels "0"–"12" along y=262; row labels 12px `#444`, right-aligned at x=140.
- **Rows (top to bottom at y = 70, 103, 136, 169, 202, 235), 16px-tall bars, fill `rgba(42,120,214,0.35)`, 2px `#2a78d6` border:** "1. Home" length 0 (draw a 7px blue dot at x=150 instead), "2. Cafe" 2, "3. Bank" 3, "4. Park" 6, "5. Mall" 7, "6. Airport" 10 — the Airport bar filled `rgba(0,131,0,0.30)` with 2px `#008300` border; bold 12px value label ("0 min" … "10 min") just right of each bar end.
- **Beaten estimates:** hollow 6px `#6b7280` circles at 5 on the Bank row and 8 on the Park row, each with an 11px `#6b7280` label "was 5" / "was 8"; on the Airport row an 11px `#6b7280` label "Mall's 12 rejected" near x=600.
- **Annotation (bold 12px orange `#d95926`, two lines near x=430, y=52):** "picked in order of total minutes —" / "a settled spot is never revisited".
- **Caption (12px `#444`, bottom right):** "same numbers as the worked example — illustrative".

## Why Your GPS Doesn't Try Every Route

**Tags:** `where it's used` (blue), `one pass` (green), `scale` (orange)

- **Route explosion** — even this 6-spot town has 7 distinct Home→Airport routes; real maps have astronomically many
- **One pass** — Dijkstra settles each spot once, so its work grows with the map size, not the route count
- **Everywhere at once** — one run from Home yields the cheapest time to every spot, not just the Airport
- **GPS routing** — navigation apps run this idea (plus speedups) over maps with millions of intersections
- **Beyond maps** — the same rule routes internet packets, cheap flight connections, and game characters

*Example (italic):* Listing all 7 routes by hand gives 10, 12, 12, 12, 14, 16, 20 minutes — Dijkstra reached the same winner, 10, while settling just 6 spots and never listing a single full route.

**Key point:** Dijkstra never enumerates routes — it prices spots, one settle each, which is why the cheapest path in a million-intersection map is still computable in a blink.

### Visualization (canvas `c3`, 720×300)

Vertical bar chart of all seven Home→Airport routes and their total minutes, sorted cheapest first, with the winner in green — the brute-force list Dijkstra never had to build.

- **Title (bold 15px, `#1a5276`, top center):** "All Seven Routes, Priced by Hand — Dijkstra Skipped This List".
- **Axes:** origin x=70, baseline y=235, plot width 600, plot height 165; y = total minutes 0 to 22 with light `#e5e9ef` gridlines and 12px `#444` labels at 5, 10, 15, 20; seven bars 62px wide, evenly spaced starting x=78.
- **Bars (left to right):** totals `[10, 12, 12, 12, 14, 16, 20]`; first bar fill `rgba(0,131,0,0.30)` with 2px `#008300` border, the rest `rgba(42,120,214,0.35)` with 2px `#2a78d6` border; bold 13px total label above each bar top ("10" in green, others `#2a78d6`).
- **Route labels (11px `#444`, two lines, centered under each bar below the baseline):** "via Cafe, Bank, Park" / "(the winner)"; "via Cafe, Park"; "via Cafe, Bank, Mall"; "via Bank, Park"; "via Bank, Mall"; "via Bank, Cafe, Park"; "via Cafe, Park, Bank, Mall".
- **Annotation (bold 13px green `#008300`, near x=250, y=60):** "Dijkstra found the 10 without listing all seven".
- **Caption (12px `#444`, bottom right):** "route totals from the illustrative town map".

## Cheapest Next Road Is Not Cheapest Total

**Tags:** `common mistake` (red), `negative costs` (orange)

- **The mix-up** — the rule ranks frontier spots by total minutes from Home, not by the length of the last road
- **Proof on the map** — Bank's last road can be the 5-minute direct one, yet its settled total is 3 via the Cafe
- **Settled is final** — Dijkstra never reopens a settled spot; that shortcut is only safe when no road subtracts cost
- **The trap** — with roads Home→X at 2, Home→Y at 6, and Y→X at −5, Dijkstra settles X at 2, but via Y it is really 1
- **The fix** — when costs can be negative, use Bellman–Ford instead: slower, but it keeps rechecking

*Example (italic):* A courier app added "−4 minute" bonus lanes to reward quiet streets, and its Dijkstra router silently started returning routes that were not the cheapest.

**Common mistake:** Running Dijkstra on a graph with negative edge costs. Nothing crashes and no warning appears — the settled-is-final shortcut just quietly returns wrong answers.

### Visualization (canvas `c4`, 720×300)

Split panel: left, a tiny three-spot map with one negative road; right, two horizontal bars comparing Dijkstra's settled answer for X against the true cheapest cost via Y.

- **Title (bold 15px, `#1a5276`, top center):** "One Negative Road Breaks 'Settled Is Final'".
- **Left panel (x 20–330):** circles radius 16, white fill, bold 12px labels — Home (90, 165), Y (240, 85), X (240, 235); 2px `#6b7280` arrows Home→X labeled "2" and Home→Y labeled "6" (bold 12px `#444` on white midpoint squares); 3px magenta `#d55181` arrow Y→X labeled bold 12px magenta "−5".
- **Right panel bars:** minutes axis 0 to 7, x maps 0→430 and 7→690, light `#e5e9ef` gridlines at 2, 4, 6 with 12px `#444` labels at y=262; row 1 (y=110, 18px tall) "Dijkstra's answer for X" (12px `#444` label above the bar): length 2, fill `rgba(42,120,214,0.35)`, 2px `#2a78d6` border, bold 13px blue "2" at bar end; row 2 (y=190) "true cheapest, via Y": length 1, fill `rgba(0,131,0,0.30)`, 2px `#008300` border, bold 13px green "6 − 5 = 1" at bar end.
- **Annotation (bold 13px red `#e74c3c`, two lines near x=450, y=60):** "X settled at 2 and was never rechecked —" / "the −5 road made the real answer 1".
- **Caption (12px `#444`, bottom right):** "illustrative — negative costs are where Dijkstra stops applying".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates; small shared helpers for node circles (circle + centered label) and midpoint edge labels (white background square under the text) keep c1 and c4 consistent. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all node coordinates, road weights (`2, 5, 1, 6, 3, 4, 4, 5`), settle distances (`0, 2, 3, 6, 7, 10`), route totals (`10, 12, 12, 12, 14, 16, 20`), and the negative-edge trap (`2, 6, −5` giving 2 vs 1) are the hardcoded literals above — no randomness; chart numbers must stay identical to the text's worked example.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
