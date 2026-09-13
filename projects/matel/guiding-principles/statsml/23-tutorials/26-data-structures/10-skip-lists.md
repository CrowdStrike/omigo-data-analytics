# Skip Lists

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Skip Lists

**Subtitle:** A skip list is a sorted list with express lanes stacked on top — coin flips decide which items get promoted, and a search rides the fast lanes down to the answer

## A Metro Line with Express Trains

**Tags:** `core idea` (blue), `express lanes` (green), `sorted list` (orange)

- **The line** — a metro line has 12 stations at kilometer markers 3, 7, 12, 19, 25, 31, 38, 44, 50, 57, 63, 70
- **The local** — the local train stops at every one of the 12, so a far station means many stops
- **The express** — one lane up, an express stops at only 6 of them: 3, 12, 25, 38, 50, 63
- **The super-express** — the top lane stops at just 3: stations 3, 25, and 50
- **The ride** — stay on the fastest lane while it undershoots; drop a lane when the next stop overshoots
- **The structure** — that is a skip list: one sorted list with sparser copies stacked above it

*Example (italic):* To reach station 57 you ride the super-express to 50, see the express would jump past to 63, and take one local stop — never touching the ten stations before 50.

**Key point:** A skip list is a sorted linked list plus express lanes: each higher lane keeps about half the stops of the lane below, so long rides become a few big jumps.

### Visualization (canvas `c1`, 720×300)

Three-lane diagram: the full sorted line on the bottom, two thinner express lanes above, with dotted connectors showing that upper stops are copies of lower ones.

- **Title (bold 15px, `#1a5276`, top center):** "One Sorted Line, Three Lanes: Local Plus Two Express".
- **Lane rails:** horizontal 2px `#e5e9ef` lines at y=110 (top lane), y=170 (middle lane), y=230 (bottom lane), each from x=118 to x=700; lane labels 11px `#6b7280` left-aligned at x=8, vertically centered on each rail: "super-express", "express", "local".
- **Node x positions (shared by all lanes):** keys `[3, 7, 12, 19, 25, 31, 38, 44, 50, 57, 63, 70]` at x = `[125, 176, 227, 278, 329, 380, 431, 482, 533, 584, 635, 686]`.
- **Bottom lane (y=230):** all 12 keys as circles radius 12, white fill, 2px `#2a78d6` stroke, bold 11px `#1a5276` key number centered inside.
- **Middle lane (y=170):** circles only for keys 3, 12, 25, 38, 50, 63 (same x as below), same style but 2px `#199e70` stroke.
- **Top lane (y=110):** circles only for keys 3, 25, 50, same style but 2px `#4a3aa7` stroke.
- **Connectors:** vertical dotted 1px `#6b7280` lines joining each key's copies across lanes (e.g. 25 at y=110, 170, 230), drawn behind the circles.
- **Annotation (bold 12px orange `#d95926`, near x=470, y=68):** "each lane keeps about half the stops below".
- **Caption (12px `#444`, bottom right):** "kilometer markers illustrative".

## Riding to Station 57 in Four Looks

**Tags:** `worked example` (blue), `search path` (green)

- **Start** — board at station 3 on the top lane, whose only stops are 3, 25, and 50
- **Top lane** — 25 is not past 57, ride; 50 is not past 57, ride; the top lane ends, so drop down at 50
- **Middle lane** — the next express stop after 50 is 63, and 63 > 57, so drop again without moving
- **Bottom lane** — one local hop from 50 lands on 57: found it
- **The count** — the search looked at 25, 50, 63, 57 — four looks in total
- **The flat scan** — a local-only ride checks 3, 7, 12, 19, 25, 31, 38, 44, 50, 57: ten looks

*Example (italic):* Same station, two rides: the lanes needed 4 looks (25, 50, 63, 57); the local-only scan needed 10.

**Key point:** Ride each lane while the next stop is ≤ the target and drop a lane when it overshoots — here that turns 10 looks into 4.

### Visualization (canvas `c2`, 720×300)

The same three-lane diagram with the search path for station 57 drawn on top: green jump arrows along and down the lanes, one orange rejected probe toward 63, and the target circled.

- **Title (bold 15px, `#1a5276`, top center):** "Finding Station 57: Ride, Overshoot, Drop, Arrive".
- **Base diagram:** identical rails, node positions, keys, and connectors as `c1`, but all circle strokes muted to 1.5px `#6b7280` and key numbers 11px `#6b7280` (the path re-colors the nodes it touches).
- **Path nodes:** circles at 3 (top lane), 25 (top), 50 (top, middle, bottom), 57 (bottom) restroked 2.5px green `#008300` with bold 11px `#008300` numbers; probed node 63 (middle lane) restroked 2.5px orange `#d95926`.
- **Ride arrows:** 3px green `#008300` arrows with solid arrowheads: top lane 3→25 and 25→50 (along y=110); vertical drop 50 top→middle (x=533, y=110→170); vertical drop 50 middle→bottom (x=533, y=170→230); bottom lane 50→57 (along y=230).
- **Rejected probe:** 2px dashed (dash 5/4) orange `#d95926` arrow along the middle lane from 50 toward 63, ending in an orange arrowhead; bold 12px orange label just above it: "63 > 57 — drop".
- **Target:** halo circle radius 17, 2px green, around station 57 on the bottom lane; bold 12px green label "target" below it at y=262.
- **Annotation (bold 13px green `#008300`, near x=150, y=62):** "4 looks instead of 10".
- **Caption (12px `#444`, bottom right):** "looks: 25, 50, 63, 57 — a local-only scan checks 10 stations".

## Why Databases Keep Express Lanes

**Tags:** `where it's used` (blue), `speed` (green), `easy inserts` (orange)

- **Big lists hurt** — a plain scan of 1,000,000 items averages 500,000 looks; lanes cut it to about 20
- **Doubling is cheap** — doubling the list adds roughly one lane, so cost grows by about one look
- **The gap** — at 4,096 items a plain scan averages 2,048 looks; the lanes need about 12
- **Easy inserts** — a new item splices into a few lanes in place; nothing gets rebuilt or rebalanced
- **Sorted walks stay free** — the bottom lane is still the full sorted list, so range scans just walk it
- **Where it lives** — in-memory databases use skip lists for sorted sets, leaderboards, and indexes

*Example (italic):* A leaderboard with a million players finds any score in about 20 looks instead of a half-million-step scan.

**Key point:** Skip lists give binary-search speed on a linked list — and keep it while items are constantly inserted and removed.

### Visualization (canvas `c3`, 720×300)

Line chart of average looks per search as the list grows: the plain-scan line climbs steeply while the skip-list line hugs the floor.

- **Title (bold 15px, `#1a5276`, top center):** "Average Looks to Find One Item: Plain Scan vs Skip List".
- **Axes:** origin x=70, baseline y=245, plot width 590, plot height 185; x = list size with 5 evenly spaced category ticks at x = `[130, 260, 390, 520, 650]` labeled "16", "64", "256", "1,024", "4,096" (12px `#444`); y = looks, 0 at baseline to 2,048 at y=60, light `#e5e9ef` gridlines at 512, 1,024, 1,536, 2,048 with 12px `#444` labels on the left.
- **Plain-scan line:** orange `#d95926` 3px line through looks = `[8, 32, 128, 512, 2048]` at the 5 ticks (y scaled linearly, 2,048 at y=60); 5px orange dots; 12px orange value labels "8", "32", "128", "512", "2,048" beside each dot.
- **Skip-list line:** green `#008300` 3px line through looks = `[4, 6, 8, 10, 12]` at the same ticks (nearly flat along the baseline); 5px green dots; 12px green value labels "4", "6", "8", "10", "12" just above each dot.
- **Annotation (bold 13px green `#008300`, near x=400, y=205):** "the skip-list line barely rises".
- **Annotation (bold 12px orange `#d95926`, near x=545, y=95):** "plain scan: half the list".
- **Caption (12px `#444`, bottom right):** "average looks per search — illustrative".

## The Coin Flip, Not a Blueprint

**Tags:** `common mistake` (red), `randomness` (orange)

- **The worry** — it feels like someone must carefully plan which stations get express stops
- **The truth** — each new station flips a coin: heads promotes it one lane up, and it flips again
- **Halving on average** — the flips thin each lane to about half: here 12 local, 6 express, 3 super-express
- **No rebalancing** — unlike balanced search trees, nothing rotates or rebuilds; luck does the balancing
- **Bad luck fades** — a lopsided line is possible, but it becomes wildly unlikely as the list grows

*Example (italic):* Station 44 flipped tails on arrival, so it lives only in the local lane; station 25 flipped heads twice and reached the top.

**Common mistake:** Assuming the lanes need clever bookkeeping to stay balanced. The levels are pure coin flips — and that randomness is exactly why inserts stay cheap.

### Visualization (canvas `c4`, 720×300)

Bar chart of how many of the 12 stations appear in each lane, halving lane by lane, with the coin-flip promotion rule spelled out beside the bars.

- **Title (bold 15px, `#1a5276`, top center):** "Coin Flips Thin the Lanes: 12 → 6 → 3".
- **Axis:** horizontal 2px `#999` baseline at y=245 from x=90 to x=650; no y-axis labels (bars carry their own counts).
- **Bars (width 90, centered at x = 190, 370, 550):** station counts `[12, 6, 3]` drawn at 15px per station — bar tops at y=65, 155, 200; fills `rgba(42,120,214,0.35)` / `rgba(25,158,112,0.35)` / `rgba(74,58,167,0.35)` with 2px borders `#2a78d6` / `#199e70` / `#4a3aa7`.
- **Count labels:** bold 13px `#1a5276` centered above each bar: "12", "6", "3".
- **Lane labels:** 12px `#444` centered below the baseline at y=265: "local lane", "express lane", "super-express".
- **Halving arrows:** 2px dashed (dash 5/4) `#6b7280` arrows between neighboring bar tops (from x=235 to x=325, and x=415 to x=505), each with a 12px `#6b7280` label "× ½ per flip" above its midpoint.
- **Annotation (bold 12px violet `#4a3aa7`, near x=470, y=100):** two lines: "each heads promotes a station" / "one lane up — tails stops it".
- **Caption (12px `#444`, bottom right):** "12 stations halve to 6, then 3 — illustrative flip outcome".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all keys, lane memberships, node x positions, look counts, and bar values are the hardcoded arrays above (no randomness); `c1` and `c2` must share identical station coordinates so the search path overlays exactly; the 4-vs-10 look counts in the text must match `c2`, and the 12/6/3 lane counts must match `c4`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
