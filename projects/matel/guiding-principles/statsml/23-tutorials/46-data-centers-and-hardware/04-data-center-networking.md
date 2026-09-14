# Data Center Networking

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Data Center Networking

**Subtitle:** Inside a data center, servers talk to each other far more than they talk to users — so the network is built as a leaf-spine fabric where every rack is the same short distance from every other

## One Click, Eleven Megabytes of Internal Chatter

**Tags:** `core idea` (blue), `east-west vs north-south` (green), `traffic taxonomy` (orange)

- **The click** — a user loads a product page; the response leaving the building is about 100 KB
- **North-south** — traffic that enters or leaves the building: user requests and the replies to them
- **East-west** — traffic between servers inside: microservice calls, cache reads, DB replication
- **The fan-out** — that one page fires 12 internal service calls, 30 cache lookups, 3 replica writes
- **The ratio** — those internal hops move about 1.1 MB — roughly 11× the bytes the user ever sees

*Example (italic):* One 100 KB page response generates about 1.1 MB of server-to-server traffic before the first byte leaves the building — east-west dwarfs north-south by an order of magnitude.

**Key point:** Modern workloads are dominated by east-west traffic, so a data center network is designed around server-to-server bandwidth, not around the pipe to the internet.

### Visualization (canvas `c1`, 720×300)

Diagram of a building cross-section: one thin vertical north-south arrow at the front door vs a dense mesh of east-west arrows between racks, with a byte tally for each.

- **Title (bold 15px, `#1a5276`, top center):** "One Page Load: 100 KB Out the Door, 1.1 MB Inside the Building".
- **Building outline:** rounded rect from (40, 70) to (690, 265), 2px `#6b7280` border, fill `rgba(229,233,239,0.35)`, 12px `#6b7280` label "data center" at top-left inside.
- **Racks:** four blue `#2a78d6` rounded boxes 90×70 at x = 90, 250, 410, 570, y=150, fills `rgba(42,120,214,0.15)`, 12px `#2c3e50` labels "web", "services", "cache", "database".
- **North-south arrow:** single 3px green `#008300` double-headed vertical arrow from y=20 down through the wall to the "web" box; bold 13px green label "north-south 100 KB" beside it at (150, 45).
- **East-west arrows:** six 3px blue `#2a78d6` double-headed horizontal arrows between adjacent and skipping rack pairs at y = 165, 185, 205 (web↔services, services↔cache, cache↔database, web↔cache, services↔database, web↔database); bold 13px blue label "east-west 1.1 MB" centered at (365, 135).
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=250):** "12 service calls + 30 cache lookups + 3 replica writes = 11× the user-facing bytes".
- **Caption (12px `#444`, bottom right):** "byte counts illustrative".

## From a Tree With a Pinch to a Leaf-Spine Fabric

**Tags:** `worked example` (blue), `Clos fabric` (green), `ECMP` (orange)

- **The old tree** — three tiers: 8 top-of-rack switches feed 4 aggregation switches feed 2 core switches
- **The pinch** — every cross-rack byte from 8 racks squeezes through those 2 core boxes at the trunk
- **Leaf-spine** — flatten to 2 tiers: 8 leaves (one per rack), 4 spines, every leaf wired to every spine
- **Fixed distance** — any server to any other server in another rack is exactly leaf → spine → leaf
- **Hand-check** — rack 1 to rack 7 has 4 equal-cost paths (one per spine); ECMP hashes flows across all 4

*Example (italic):* Server A in rack 1 sends to server B in rack 7: A's leaf picks one of the 4 spines per flow, so 3 switch hops always — and 4 parallel highways instead of 1 shared trunk.

**Key point:** A leaf-spine (Clos) fabric makes every cross-rack pair the same short distance apart and turns the core bottleneck into many equal-cost paths that ECMP fills in parallel.

### Visualization (canvas `c2`, 720×300)

Side-by-side topology diagram: three-tier tree with a red pinch at the core (left) vs leaf-spine full mesh with 4 equal paths highlighted (right).

- **Title (bold 15px, `#1a5276`, top center):** "Three-Tier Tree (One Trunk) vs Leaf-Spine (Every Leaf to Every Spine)".
- **Left panel (x 20–345):** 12px `#6b7280` label "three-tier tree" at (30, 55). Two core boxes (40×24, red `#e74c3c` border, fill `rgba(231,76,60,0.12)`) at y=70 centered on x=130 and x=230; four aggregation boxes (36×22, `#6b7280`) at y=140, x = 70, 140, 210, 280; eight ToR boxes (28×20, blue `#2a78d6`) at y=215, x = 45 to 325 step 40. 1.5px `#999` lines: each ToR to its aggregation pair, each aggregation to both cores. Bold 12px red annotation at (180, 108): "all cross-rack traffic squeezes here".
- **Right panel (x 375–700):** 12px `#6b7280` label "leaf-spine fabric" at (385, 55). Four spine boxes (44×24, green `#008300` border, fill `rgba(0,131,0,0.12)`) at y=85, x centered 430, 505, 580, 655; eight leaf boxes (28×20, blue `#2a78d6`) at y=205, x = 395 to 675 step 40. 1px `rgba(107,114,128,0.5)` line from every leaf to every spine (32 lines). The 4 paths from leaf 1 up to each spine and down to leaf 7 drawn 2.5px green `#008300`. Bold 12px green annotation at (545, 250): "4 equal-cost paths, leaf-spine-leaf always".
- **Caption (12px `#444`, bottom right):** "8 racks, 4 spines — schematic".

## The Cost Knob and Where Software Feels It

**Tags:** `where it's used` (blue), `oversubscription` (green), `latency` (orange)

- **The knob** — a leaf has 48 × 25G server ports (1,200G down) but only 4 × 100G uplinks (400G up)
- **3:1 oversubscribed** — if all 48 servers blast cross-rack at once, each gets ~8.3G of its 25G NIC
- **Same rack is faster** — round trip inside a rack ~20 µs; cross-rack via a spine ~45 µs (illustrative)
- **It adds up** — 10,000 sequential cache calls take 0.2 s same-rack but 0.45 s cross-rack
- **Shuffles care** — a shuffle-heavy job moving data across racks lives on that 400G uplink budget

*Example (italic):* The same 10,000-call request path runs 0.2 s when the cache sits in the caller's rack and 0.45 s one spine hop away — placement is a latency decision.

**Key point:** Oversubscription at the leaf is the fabric's price dial — and it is why same-rack vs cross-rack placement shows up directly in your service's latency and your job's shuffle time.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart of round-trip time by distance, with a second annotation tying the 3:1 leaf oversubscription to per-server cross-rack bandwidth.

- **Title (bold 15px, `#1a5276`, top center):** "Where Your Packet Goes Decides How Long You Wait".
- **Axis:** vertical 2px `#999` baseline at x=210, bars extend right, max width 460; 12px `#444` row labels left-aligned at x=20.
- **Rows (bar height 22px, at y = 80, 135, 190):**
  - "same rack (one ToR hop) — 20 µs": green `#008300` bar width 40, 12px value label "20 µs" at bar end
  - "cross-rack (leaf-spine-leaf) — 45 µs": blue `#2a78d6` bar width 90, label "45 µs"
  - "cross-zone (another building) — 500 µs": orange `#d95926` bar width 460, label "500 µs"
- **Bar fills:** `rgba(0,131,0,0.30)`, `rgba(42,120,214,0.30)`, `rgba(217,89,38,0.30)` with 2px solid borders in the same hues.
- **Annotation (bold 13px violet `#4a3aa7`, at (230, 245)):** "3:1 leaf oversubscription: 48 servers share 400G up — ~8.3G each cross-rack vs 25G in-rack".
- **Caption (12px `#444`, bottom right):** "RTTs illustrative; oversubscription arithmetic exact".

## "The Fabric Is Flat, So Placement Doesn't Matter"

**Tags:** `common mistake` (red), `incast` (orange)

- **The claim** — equal hop counts get read as "the network is uniform; put anything anywhere"
- **Why it's wrong** — 3:1 oversubscription means cross-rack bandwidth is a shared, thinner pipe
- **Incast** — many senders answering one receiver at once overflow the last switch buffer together
- **The collapse** — a 64-way fan-in onto one older 10G port drops goodput from 9.4 to 1.9 Gb/s
- **The fix** — stagger fan-ins, cap concurrent senders, and keep chatty pairs rack-local when you can

*Example (italic):* A job that reads from 8 partitions sees a clean 9.3 Gb/s at the reducer; scaled to 64 partitions the same reducer gets 1.9 Gb/s — the fabric didn't change, the fan-in did.

**Common mistake:** Equal distance is not equal bandwidth. Leaf-spine removes the core pinch, but the leaf uplinks and the receiver's single port are still shared — incast and oversubscription live exactly there.

### Visualization (canvas `c4`, 720×300)

Line chart of receiver goodput vs number of simultaneous senders on a 10G port: flat, then a collapse once the switch buffer overflows.

- **Title (bold 15px, `#1a5276`, top center):** "Incast: Goodput at One 10G Port as Fan-In Grows".
- **Axes:** origin x=70, baseline y=245, plot width 590, plot height 180; x = concurrent senders, tick labels `[1, 2, 4, 8, 16, 32, 64]` evenly spaced (12px `#444`); y = goodput 0 to 10 Gb/s, gridlines `#e5e9ef` at 2.5/5/7.5, 12px `#444` labels.
- **Goodput line:** blue `#2a78d6` 3px line with 4px dots through senders `[1, 2, 4, 8, 16, 32, 64]`, goodput `[9.4, 9.4, 9.4, 9.3, 8.1, 4.2, 1.9]` Gb/s.
- **Buffer marker:** vertical dashed `#6b7280` (dash 4/3) line at the x-position of 16 senders, 12px `#6b7280` label "switch buffer overflows" at its top.
- **Annotation (bold 13px red `#e74c3c`, near the 64-sender point):** "64 senders: 1.9 Gb/s — retransmit timeouts eat the link".
- **Annotation (bold 12px green `#008300`, near the 8-sender point, y=90):** "8 senders: 9.3 Gb/s, still clean".
- **Caption (12px `#444`, bottom right):** "goodput values illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); page-load byte counts, RTTs, and incast goodput values are invented and labeled illustrative; the oversubscription arithmetic (48 × 25G = 1,200G down, 4 × 100G = 400G up, 3:1, ~8.3G per server) and the 10,000-call totals (0.2 s / 0.45 s) are exact consequences of the stated numbers.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
