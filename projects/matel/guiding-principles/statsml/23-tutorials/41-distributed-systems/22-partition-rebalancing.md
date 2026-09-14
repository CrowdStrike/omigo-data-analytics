# Partition Rebalancing

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Partition Rebalancing

**Subtitle:** When a new server joins a cluster, only a few fixed slices of the data move to it — the rest stay put and queries never stop

## A Fourth Server Joins the Cluster

**Tags:** `core idea` (blue), `fixed partitions` (green), `no downtime` (orange)

- **The table** — an online shop's orders table is split into 12 fixed partitions, P1 through P12
- **The cluster** — three nodes hold them: A has P1–P4, B has P5–P8, C has P9–P12, four each
- **The squeeze** — the shop grows, so a fourth node D is added to share the load
- **The move** — D takes exactly one partition from each node: P4 from A, P8 from B, P12 from C
- **The count** — only 3 of the 12 partitions move; the other 9 never leave their node
- **The trick** — partitions are fixed at creation; rebalancing reassigns whole partitions, never re-splits rows

*Example (italic):* After D joins, every node holds 3 partitions, and 75% of the shop's order data never crossed the network at all.

**Key point:** Partition rebalancing moves whole pre-cut slices of data between nodes to even out load — the partition count stays fixed, so adding a node only relocates the minimum few slices.

### Visualization (canvas `c1`, 720×300)

Before/after box diagram: left panel shows 3 nodes each stacking 4 partition boxes; right panel shows 4 nodes each stacking 3, with the three moved partitions highlighted on node D.

- **Title (bold 15px, `#1a5276`, top center):** "Node D Joins: Only 3 of 12 Partitions Move".
- **Left panel (before):** three columns headed "Node A", "Node B", "Node C" (bold 12px `#1a5276`) centered at x = 80, 165, 250, header y=70; each column stacks 4 partition boxes 70×26 at y = 85, 116, 147, 178; A holds `["P1","P2","P3","P4"]`, B `["P5","P6","P7","P8"]`, C `["P9","P10","P11","P12"]`; box fill `rgba(42,120,214,0.15)`, 1px `#2a78d6` border, 12px `#2c3e50` centered labels.
- **Divider:** bold 13px `#6b7280` arrow "→ D joins" centered at x=340, y=155.
- **Right panel (after):** four columns headed "Node A", "Node B", "Node C", "Node D" centered at x = 420, 500, 580, 660, header y=70; each stacks 3 boxes 70×26 at y = 85, 116, 147; A `["P1","P2","P3"]`, B `["P5","P6","P7"]`, C `["P9","P10","P11"]`, D `["P4","P8","P12"]`; stayed boxes keep the blue style, D's three boxes use fill `rgba(217,89,38,0.18)` with 2px `#d95926` border.
- **Annotation (bold 13px green `#008300`, centered near x=360, y=245):** "9 of 12 partitions never move — 75% of the data stays put".
- **Caption (12px `#444`, bottom right):** "partition layout illustrative; the 3-of-12 count is exact for this scheme".

## Copy First, Flip Ownership Second

**Tags:** `worked example` (blue), `copy-then-switch` (green)

- **Before/after** — partition counts per node go A 4→3, B 4→3, C 4→3, D 0→3; total stays 12
- **The copy** — D streams P4's 24 GB from A at 50 MB/s, reaching 100% caught up at minute 8
- **Old node serves** — during the whole copy, A keeps answering P4's 500 queries per second
- **The catch-up** — writes that land on A mid-copy are forwarded to D so its replica stays current
- **The flip** — at minute 8 the cluster metadata switches P4's owner to D in one atomic step
- **Hand-check** — at minute 4 the copy sits at 50% (12 of 24 GB), and A is still serving all 500 q/s

*Example (italic):* From minute 0 to 8 the copy runs 0% → 25% → 50% → 75% → 100% while P4's query rate holds at 500/s — shoppers never see a pause.

**Key point:** Rebalancing is copy-then-switch: the new node catches up in the background while the old owner keeps serving, and ownership flips only when the replica is complete.

### Visualization (canvas `c2`, 720×300)

Timeline chart of P4's move: copy progress rises 0→100% while the queries-served line stays flat, with a dashed marker where ownership flips from A to D.

- **Title (bold 15px, `#1a5276`, top center):** "Moving P4: Copy in the Background, Flip at Minute 8".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = minutes 0 to 10 with 12px `#444` tick labels every 2 minutes; y = percent 0 to 100, gridlines `#e5e9ef` at 25/50/75.
- **Copy line:** green `#008300` 3px line through minutes `[0, 2, 4, 6, 8]`, percent caught up `[0, 25, 50, 75, 100]`, then flat at 100 to minute 10; 12px green label "D catching up" near minute 3 above the line.
- **Queries line:** blue `#2a78d6` 3px line, flat at 90% height for minutes `[0, 2, 4, 6, 8, 10]`, values `[90, 90, 90, 90, 90, 90]` — labeled bold 12px blue "P4 queries: steady 500/s" near minute 1, y=70; segment after minute 8 drawn in aqua `#199e70` with 12px aqua label "now served by D" near minute 9.
- **Flip marker:** vertical dashed `#6b7280` (dash 4/3) line at minute 8, 12px `#6b7280` label "ownership flips A → D" at its top.
- **Annotation (bold 13px violet `#4a3aa7`, near minute 5, y=170):** "old owner serves every query until the copy is complete".
- **Caption (12px `#444`, bottom right):** "24 GB at 50 MB/s ≈ 8 min; query rate illustrative".

## Why Kafka and Elasticsearch Ship With Fixed Partitions

**Tags:** `where it's used` (blue), `hot shards` (orange), `scaling live` (green)

- **Streaming and storage** — Kafka topics and Elasticsearch indexes are pre-cut into fixed partitions for exactly this move
- **Scaling live** — clusters grow from 3 to 4 to 40 nodes without ever taking the data offline
- **The alternative** — assigning rows by `hash(key) mod N` means changing N reshuffles nearly every row
- **The math** — going 3→4 nodes, mod-N moves 75% of keys; fixed partitions move 25% (3 of 12)
- **Hot shards** — a partition getting outsized traffic can be moved alone to a quiet node, cooling the hotspot
- **Steady service** — because moves are few and copy-then-switch, rebalancing runs during business hours

*Example (italic):* Growing 4→5 nodes, a mod-N scheme would reshuffle 80% of all keys, while the fixed-partition cluster moves ≈1/5 — with 12 partitions, just 2 or 3 of them.

**Key point:** Fixed partitions turn "add a server" from a full-data reshuffle into a handful of whole-slice moves, which is why Kafka- and Elasticsearch-style systems can scale while serving traffic.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart comparing the share of data that moves when the cluster grows, under mod-N hashing vs fixed partitions, for two growth steps.

- **Title (bold 15px, `#1a5276`, top center):** "Share of Data Moved When the Cluster Grows".
- **Axis:** vertical 2px `#999` baseline at x=230, bars extend right, max width 440 = 100%; 12px `#444` scale labels "0%", "50%", "100%" along a bottom rule at y=255.
- **Rows (top to bottom at y = 75, 115, 165, 205), each with a left-aligned 12px `#444` label at x=20:**
  - "3→4 nodes, hash mod N": red `#e74c3c` bar width 330 (75%), 12px red value label "75% of keys move" at bar end
  - "3→4 nodes, fixed partitions": green `#008300` bar width 110 (25%), 12px green label "25% (3 of 12)"
  - "4→5 nodes, hash mod N": red bar width 352 (80%), 12px red label "80% of keys move"
  - "4→5 nodes, fixed partitions": green bar width 88 (20%), 12px green label "≈20% (2–3 of 12)"
- **Bar style:** 18px tall, mod-N bars fill `rgba(231,76,60,0.75)`, fixed-partition bars fill `rgba(0,131,0,0.75)`.
- **Annotation (bold 13px magenta `#d55181`, right side near y=250):** "fixed partitions move the minimum; mod-N reshuffles almost everything".
- **Caption (12px `#444`, bottom right):** "mod-N fractions exact for uniform keys; partition moves round to whole partitions".

## Twelve Partitions Forever, and the Rebalancing Storm

**Tags:** `common mistake` (red), `capacity ceiling` (orange), `network` (blue)

- **The cap** — a node can only hold whole partitions, so 12 partitions can never use more than 12 nodes
- **The mistake** — picking a partition count that fits today (12 for 3 nodes) and capping tomorrow's growth
- **Node 13 idles** — with 12 partitions, cluster throughput stops rising at 12 nodes; extra machines sit empty
- **The storm** — rebalancing too many partitions at once saturates the network and slows live queries
- **The throttle** — real systems move a few partitions at a time and cap copy bandwidth for this reason
- **The balance** — too many partitions has its own cost: per-partition overhead in memory and metadata

*Example (italic):* A cluster with 12 partitions grows fine to 12 nodes, but nodes 13 through 16 add zero throughput — the fix is re-splitting, which is the expensive full reshuffle rebalancing was meant to avoid.

**Common mistake:** Treating the partition count as a detail. It is the hard ceiling on how far the cluster can scale, and moving too many partitions at once turns a smooth rebalance into a network-saturating storm.

### Visualization (canvas `c4`, 720×300)

Step line of cluster throughput vs node count with 12 fixed partitions: linear growth up to 12 nodes, dead flat after, with the wasted-nodes zone shaded.

- **Title (bold 15px, `#1a5276`, top center):** "12 Partitions = a Hard Ceiling at 12 Nodes".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = nodes 1 to 16 with 12px `#444` tick labels at 1, 4, 8, 12, 16; y = relative throughput 0 to 12 (units "×"), gridlines `#e5e9ef` at 3/6/9/12.
- **Throughput line:** blue `#2a78d6` 3px line through nodes `[1, 2, 3, 4, 6, 8, 12, 13, 14, 16]`, throughput `[1, 2, 3, 4, 6, 8, 12, 12, 12, 12]` — straight rise, then flat.
- **Ceiling zone:** shade x from node 12 to node 16 above nothing — a light red band `rgba(231,76,60,0.08)` over the full plot height for nodes 12–16, topped by a dashed red `#e74c3c` (dash 4/3) horizontal line at throughput 12 labeled 12px red "ceiling: 12 partitions".
- **Marker:** 12px `#6b7280` label "node 13 adds nothing" with a short arrow pointing at the flat segment near node 13.
- **Annotation (bold 13px red `#e74c3c`, near node 8, y=90):** "extra nodes past 12 sit idle — pick the partition count for the cluster you'll have, not the one you have".
- **Caption (12px `#444`, bottom right):** "throughput in multiples of one node's capacity, illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the partition layout (A P1–P4, B P5–P8, C P9–P12; D takes P4/P8/P12), the copy timeline (24 GB at 50 MB/s, 0/25/50/75/100% at minutes 0–8, 500 q/s), and the throughput ceiling curve are invented and labeled illustrative; the moved-data fractions (75% vs 25% for 3→4 nodes, 80% vs ≈20% for 4→5) are exact for uniform keys under mod-N; fixed-partition moves round to whole partitions (2 or 3 of 12).
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
