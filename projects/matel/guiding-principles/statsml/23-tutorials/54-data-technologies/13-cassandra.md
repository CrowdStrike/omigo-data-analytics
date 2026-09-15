# Cassandra

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Cassandra

**Subtitle:** Cassandra has no primary node — any of its servers can accept a write, and you choose per query how many replicas must agree before "success"

## A Ring Where Every Node Takes Writes

**Tags:** `core idea` (blue), `leaderless` (green), `Dynamo + Bigtable` (orange)

- **The setup** — 6 servers form a ring storing readings from 5,000 temperature sensors
- **The origin** — born at Facebook: Dynamo's leaderless replication plus Bigtable's table model
- **No primary** — a sensor's write can land on any node; whichever receives it coordinates
- **The walk** — the coordinator hashes the partition key and sends copies to 3 replica nodes
- **No bottleneck** — with no single leader, no single node caps the cluster's write rate

*Example (italic):* Sensor s-17's 2:03pm reading arrives at node 2; node 2 hashes "s-17", finds replicas at nodes 2, 3, 4, and forwards copies — no leader was ever consulted.

**Key point:** Leaderless replication means every node accepts writes and forwards them to the replica set for that key — there is no primary whose failure stops writes.

### Visualization (canvas `c1`, 720×300)

Ring diagram: 6 node circles on a ring, a client write arriving at a coordinator, and replication arrows fanning out to the 3-node replica set.

- **Title (bold 15px, `#1a5276`, top center):** "One Write, Any Door: the Coordinator Fans Out to 3 Replicas".
- **Ring:** circle outline 2px `#e5e9ef`, center (400, 170), radius 100; 6 node circles radius 22 at angles 90°, 30°, 330°, 270°, 210°, 150° (node 1 at top, clockwise), labels "n1"–"n6" bold 13px `#2c3e50` centered.
- **Node fills:** replicas n2, n3, n4 fill `rgba(0,131,0,0.15)` with 2px `#008300` border; the rest fill `rgba(42,120,214,0.12)` with 2px `#2a78d6` border.
- **Client box:** rounded box 120×36 at (30, 60), fill `rgba(42,120,214,0.15)`, 12px text "sensor s-17 write"; 3px `#2a78d6` arrow from its right edge to node n2 (the coordinator).
- **Replication arrows:** 2px dashed (dash 5/3) `#008300` arrows from n2 to n3 and from n2 to n4; bold 12px green label "3 copies" near the n3 arrow midpoint.
- **Coordinator tag:** bold 12px `#1a5276` label "coordinator" just outside n2.
- **Annotation (bold 13px violet `#4a3aa7`, bottom left at (30, 270)):** "no primary — any node could have coordinated".
- **Caption (12px `#444`, bottom right):** "6-node ring, replication factor 3; sensor traffic illustrative".

## Losing a Node Mid-Write: ONE, QUORUM, ALL

**Tags:** `worked example` (blue), `tunable consistency` (green)

- **The failure** — at 2:03pm node 4 dies, so s-17's replica set {n2, n3, n4} has 2 nodes alive
- **The knob** — each write names a consistency level: how many replica acks count as success
- **ONE** — needs 1 ack of 3; n2 alone says yes; the others catch up later (eventual consistency)
- **QUORUM** — needs floor(3/2)+1 = 2 acks (exact); n2 and n3 answer, so it still succeeds
- **ALL** — needs 3 acks; n4 is dead, so the write times out and fails
- **Read repair** — when n4 returns, a QUORUM read spots its stale copy and writes the fix back

*Example (italic):* With node 4 down, s-17's 2:03pm reading succeeds at ONE and QUORUM but fails at ALL — the same cluster, three different answers, chosen per query.

**Key point:** Consistency is tunable per request: ONE is fastest, ALL is strictest, and a QUORUM read (2 of 3) always overlaps a QUORUM write in at least one replica — so it sees the newest value.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart: acks required by each consistency level vs the 2 replicas still alive, with a vertical "alive" threshold line deciding success or failure.

- **Title (bold 15px, `#1a5276`, top center):** "Node 4 Is Down: Which Consistency Levels Still Succeed?".
- **Axis:** baseline vertical 2px `#999` at x=190, bars extend right, scale 3 acks = 450px (150px per ack); x tick labels "1", "2", "3" (12px `#444`) below y=255.
- **Threshold line:** vertical dashed (dash 4/3) 2px `#d95926` at x=490 (2 acks), bold 12px `#d95926` label "2 replicas alive" at its top.
- **Rows (bar height 26px, at y = 85, 145, 205), each with left-aligned 12px `#444` label at x=20:**
  - "ONE — needs 1 ack": green `#008300` bar width 150, bold 12px green "succeeds" at bar end
  - "QUORUM — needs 2 acks": green `#008300` bar width 300, bold 12px green "succeeds" at bar end
  - "ALL — needs 3 acks": red `#e74c3c` bar width 450, bold 12px red "times out" at bar end
- **Bar fills:** success bars `rgba(0,131,0,0.30)` with 2px solid edge; failure bar `rgba(231,76,60,0.20)` with 2px solid edge.
- **Annotation (bold 13px `#1a5276`, near x=200, y=260):** "quorum of 3 = floor(3/2)+1 = 2 (exact)".
- **Caption (12px `#444`, bottom right):** "replication factor 3; ack counts exact".

## Built for Write Floods

**Tags:** `where it's used` (blue), `linear scale-out` (green)

- **The workload** — sensors, clickstreams, and message feeds write far more than they read
- **The scaling move** — add nodes and the ring re-splits the key range; no leader to re-elect
- **The numbers** — each node handles ~10k writes/s, so 6 nodes take 60k and 24 nodes take 240k
- **Near-linear** — because writes never funnel through a primary, throughput grows with nodes
- **The trade** — you give up joins and multi-row transactions to keep every write local and fast

*Example (italic):* Doubling the ring from 6 to 12 nodes roughly doubles capacity, 60k to 120k writes/s — no failover drill, no primary promotion, just more doors.

**Key point:** Leaderless design is what makes scale-out linear for write-heavy loads — every new node is another independent write path, not another follower of one leader.

### Visualization (canvas `c3`, 720×300)

Line chart of cluster write throughput vs node count, a straight line through hardcoded points showing linear scale-out.

- **Title (bold 15px, `#1a5276`, top center):** "More Nodes, More Doors: Writes/s Grows Linearly".
- **Axes:** origin x=70, baseline y=245, plot width 590, plot height 180; x = nodes 0 to 24 with 12px `#444` tick labels at 6/12/18/24; y = writes/s 0 to 240k, gridlines `#e5e9ef` at 60k/120k/180k with 12px `#444` labels "60k"–"240k".
- **Throughput line:** blue `#2a78d6` 3px line through nodes `[6, 12, 18, 24]`, writes/s `[60000, 120000, 180000, 240000]`, 5px radius solid dots at each point.
- **Point labels:** 12px `#2a78d6` "60k", "120k", "180k", "240k" above each dot.
- **Highlight:** vertical dashed `#6b7280` (dash 4/3) line at nodes=6, bold 12px `#6b7280` label "today's 6-node ring" at its top.
- **Annotation (bold 13px green `#008300`, near nodes=16, upper area):** "≈10k writes/s per node — no leader bottleneck".
- **Caption (12px `#444`, bottom right):** "throughput numbers illustrative".

## Design the Table Around the Query

**Tags:** `common mistake` (red), `partition key` (orange)

- **The confusion** — treating Cassandra like SQL: one normalized table per entity, joins later
- **No joins** — there is no join engine; a query can only walk one partition efficiently
- **The mistake** — partitioning readings by reading id scatters one sensor's data across all 6 nodes
- **The fix** — partition key (sensor_id, day): every reading for s-17 today lives on one replica set
- **The cost** — a new query pattern usually means a new table with the same data, written twice
- **The rule** — list your queries first, then design one table per query, not one per entity

*Example (italic):* "Last 100 readings for s-17" hits 1 partition on the (sensor_id, day) table but fans out to all 6 nodes on the by-reading-id table — same data, 6× the work.

**Common mistake:** Modeling entities instead of queries. Cassandra tables are shaped by the partition key you will filter on — get it wrong and every read becomes a cluster-wide scatter-gather.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: the same query against an entity-modeled table (scatter to all 6 nodes) vs a query-modeled table (one partition, one replica set).

- **Title (bold 15px, `#1a5276`, top center):** "Same Query, Two Table Designs: Scatter vs One Partition".
- **Row 1 (y=95), label 12px `#444` at x=20:** "by reading id"; blue `#2a78d6` rounded box at x=140 labeled "last 100 for s-17" (12px), 3px arrow to a red `#e74c3c` box at x=390 labeled "asks all 6 nodes" with bold 12px red "✗ 6-node scatter-gather".
- **Row 2 (y=205), label:** "by (sensor_id, day)"; blue box "last 100 for s-17", 3px arrow to a green `#008300` box at x=340 labeled "hash → 1 partition", then arrow to a green box at x=555 labeled "1 replica set answers" with bold 12px green "✓".
- **Box style:** 150–170px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=270):** "in Cassandra the query designs the table, not the entity".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays and coordinates above (no randomness); sensor counts, node throughput (10k/node, 60k–240k line) and traffic figures are invented and labeled illustrative; the quorum arithmetic (floor(3/2)+1 = 2) and the ONE/QUORUM/ALL ack counts (1/2/3 of replication factor 3) are exact Cassandra semantics.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
