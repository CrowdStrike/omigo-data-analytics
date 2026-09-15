# ZooKeeper

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** ZooKeeper

**Subtitle:** A tiny replicated filesystem with three tricks — ordering, watches, and nodes that vanish with their owner — from which distributed systems build locks, leaders, and discovery

## A Tiny Filesystem Everyone Can Watch

**Tags:** `core idea` (blue), `znodes` (green), `from Yahoo` (orange)

- **The tree** — ZooKeeper (built at Yahoo, now Apache) stores a small tree of named nodes called znodes
- **Like files** — each znode has a path (`/config/db-url`), a few bytes of data, and children
- **Replicated** — every server holds the whole tree; the ZAB protocol keeps all copies in one agreed order
- **Watches** — a client can ask "tell me once when this znode changes" instead of polling it
- **Ephemeral** — a znode created as ephemeral is auto-deleted the moment its creator's session dies
- **Sequential** — a znode can get a server-assigned, ever-increasing sequence number stapled to its name

*Example (italic):* Worker B crashes at 2:03pm; six seconds later its ephemeral znode `/workers/w-02` vanishes and every client watching it is notified — no heartbeat code written.

**Key point:** ZooKeeper is not a database — it is a small, strongly-ordered, watchable tree whose ephemeral nodes turn "this process is alive" into a fact the whole cluster can see.

### Visualization (canvas `c1`, 720×300)

Tree diagram of a znode namespace: persistent config nodes on the left, ephemeral worker nodes on the right, one ephemeral node vanishing and firing a watch.

- **Title (bold 15px, `#1a5276`, top center):** "The Znode Tree: Config That Stays, Workers That Vanish".
- **Root:** rounded box 44×26 centered at (360, 52), fill `rgba(42,120,214,0.15)`, 12px `#2c3e50` label "/".
- **Level 2 (y=115, boxes 110×30, 2px `#6b7280` connector lines from root):** "/config" centered at x=140, "/election" at x=360, "/workers" at x=580.
- **Under /config (y=185):** solid blue `#2a78d6` box 130×30 centered at x=140 labeled "db-url = pg-7" (12px), 11px `#6b7280` tag "persistent" beneath it.
- **Under /workers (y=185):** two dashed-border boxes 120×30: green `#008300` "w-01" centered at x=520, red `#e74c3c` "w-02" at x=655 with a 3px red ✗ drawn across it; 11px `#6b7280` tag "ephemeral" beneath each.
- **Watch arrow:** dashed orange `#d95926` (dash 4/3) 2px arrow from the w-02 box down-left to a 12px bold orange label "watch fires → client A notified" at (430, 255).
- **Annotation (bold 12px red `#e74c3c`, at (655, 232)):** "session died — node auto-deleted".
- **Caption (12px `#444`, bottom right):** "znode names illustrative".

## Leader Election With Numbered Ephemeral Znodes

**Tags:** `worked example` (blue), `leader election` (green)

- **The setup** — three replicas A, B, C each create an ephemeral sequential znode under `/election`
- **The names** — the server assigns numbers in arrival order: `n_0000000041`, `n_0000000042`, `n_0000000043`
- **The rule** — whoever holds the lowest number is leader, so A (41) leads; B and C just wait
- **The watch** — B watches znode 41 and C watches znode 42: each watches only its predecessor
- **The failover** — A's session dies at t=0; by t=+6s znode 41 is gone, B's watch fires, B sees it is lowest
- **Hand-check** — C's predecessor (42) still exists, so C never wakes; exactly one client is notified

*Example (italic):* A crashes at 2:03:00pm with a 6-second session timeout; at 2:03:06pm znode `n_0000000041` vanishes and B (holding `n_0000000042`) becomes leader — C sleeps through the whole thing.

**Key point:** No election message is ever sent — leadership is just "lowest surviving sequence number", and the ephemeral-node-plus-watch combination makes failover automatic and orderly.

### Visualization (canvas `c2`, 720×300)

Two-row before/after diagram: the `/election` znodes at t=0 (A leads) and at t=+6s (A's node gone, B leads), with watch arrows between predecessors.

- **Title (bold 15px, `#1a5276`, top center):** "Lowest Number Leads: One Death, One Watch, One New Leader".
- **Row 1 (boxes centered on y=105), label 12px `#444` at x=15:** "t=0"; three rounded boxes 155×40 centered at x=180, 385, 590: green `#008300` fill `rgba(0,131,0,0.12)` "n_0000000041 · A — LEADER", blue `#2a78d6` fill `rgba(42,120,214,0.15)` "n_0000000042 · B", blue "n_0000000043 · C" (12px `#2c3e50` text).
- **Row-1 watch arrows:** dashed orange `#d95926` (dash 4/3) 2px arrows from B's box to A's box and from C's box to B's box, each with an 11px orange "watch" label above the arrow midpoint.
- **Row 2 (boxes centered on y=215), label at x=15:** "t=+6s"; box 41 redrawn with dashed red `#e74c3c` border, fill `rgba(231,76,60,0.12)`, 3px red ✗ across it, label "n_0000000041 gone"; B's box now green with "n_0000000042 · B — LEADER"; C's box unchanged blue, its dashed orange watch arrow still pointing at B's box.
- **Annotation (bold 13px green `#008300`, at (385, 268)):** "only B's watch fires — C never wakes".
- **Caption (12px `#444`, bottom right):** "sequence numbers and 6s session timeout illustrative".

## The Coordination Layer Under Half of Big Data

**Tags:** `where it's used` (blue), `Kafka, HBase, Hadoop` (green)

- **Recipes** — the same primitives compose into locks, leader election, service discovery, config push
- **Kafka (pre-KRaft)** — brokers registered as ephemeral znodes; the controller was elected through ZooKeeper
- **HBase** — master election and live region-server tracking both ride on ephemeral znodes
- **Hadoop HA** — the standby NameNode takes over via a ZooKeeper-based failover controller
- **SolrCloud** — cluster state and shared config live in the znode tree, watched by every node
- **The quorum** — a 5-server ensemble commits once 3 of 5 agree, so it survives 2 failures (exact)

*Example (italic):* Four different systems — a message log, a key-value store, a filesystem, a search engine — all outsourced the same hard problem, "who is alive and who is in charge", to one small tree.

**Key point:** ZooKeeper's product insight is that consensus is hard and rare skills shouldn't be rebuilt per system — ship the agreed-upon tree once, and let everyone compose their coordination from it.

### Visualization (canvas `c3`, 720×300)

Three-column flow diagram: four primitives (left) compose into four recipes (middle) that power four real systems (right), with Kafka flagged as having since left.

- **Title (bold 15px, `#1a5276`, top center):** "Four Primitives → Four Recipes → Real Systems".
- **Column headers (bold 12px `#6b7280`, y=52):** "primitives" at x=115, "recipes" at x=360, "systems" at x=605 (centered).
- **Column 1 (blue `#2a78d6` rounded boxes 150×30, fill `rgba(42,120,214,0.15)`, centered at x=115, y=80/135/190/245, 12px text):** "ordered znode tree", "watches", "ephemeral nodes", "sequence numbers".
- **Column 2 (green `#008300` boxes 150×30, fill `rgba(0,131,0,0.12)`, centered at x=360, same y rows):** "leader election", "distributed lock", "service discovery", "config distribution".
- **Column 3 (boxes 170×30 centered at x=605, same y rows):** orange `#d95926` fill `rgba(217,89,38,0.12)` "Kafka (pre-KRaft)", then blue-filled "HBase", "Hadoop HA NameNode", "SolrCloud".
- **Arrows:** 2px `#6b7280` straight arrows column 1 → column 2 and column 2 → column 3, row to row.
- **Annotation (bold 12px orange `#d95926`, at (605, 62), just above the Kafka box):** "→ KRaft moved consensus in-broker".
- **Caption (12px `#444`, bottom right):** "recipe-to-system pairing simplified — most systems use several recipes".

## Watch Your Neighbor, Not the Leader

**Tags:** `common mistake` (red), `herd effect` (orange)

- **The naive plan** — every candidate watches the leader's znode so all learn instantly when it dies
- **The herd** — with 500 clients (illustrative), one leader death fires 499 watches at the same instant
- **The stampede** — all 499 re-read `/election` and re-check at once, hammering the ensemble it relies on
- **The fix** — each client watches only the znode one sequence number below its own, so 1 watch fires
- **The trend** — newer systems internalize consensus (Kafka's KRaft, Raft libraries) and drop ZooKeeper

*Example (italic):* With 500 candidates, watch-the-leader wakes 499 clients on a single death; watch-your-predecessor wakes exactly 1 — same failover, 499× less load at the worst moment.

**Common mistake:** Watching the leader directly. The official leader-election recipe watches the predecessor precisely to avoid the herd effect — and mistaking ZooKeeper for a general database, then hammering it, kills the coordination layer everything else stands on.

### Visualization (canvas `c4`, 720×300)

Horizontal bar chart: number of clients woken by one leader death under the two watch patterns, for a 500-client cluster.

- **Title (bold 15px, `#1a5276`, top center):** "Leader Dies With 500 Clients: Who Gets Woken Up?".
- **Axis:** vertical 2px `#999` baseline at x=250, bars extend right, max width 440; linear scale where 499 notifications = 440px.
- **Row 1 (bar centered on y=115), left-aligned 12px `#444` label at x=20:** "everyone watches the leader"; red `#e74c3c` bar 440px wide, 26px tall, bold 12px red value label "499 woken" at the bar end.
- **Row 2 (bar centered on y=195), label at x=20:** "each watches its predecessor"; green `#008300` bar 4px wide (floor so it stays visible), 26px tall, bold 12px green value label "1 woken" beside it.
- **Annotation (bold 13px magenta `#d55181`, centered near (470, 250)):** "499 simultaneous re-reads vs one — same failover either way".
- **Caption (12px `#444`, bottom right):** "500 clients illustrative; 499 vs 1 follows exactly from the watch pattern".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all boxes, arrows, and bars use the hardcoded coordinates and values above (no randomness); znode names, the 6s session timeout, and the 500-client count are invented and labeled illustrative; the 3-of-5 quorum (survives 2 failures) and the 499-vs-1 wakeup counts given the watch pattern are exact. ZooKeeper facts (Yahoo origin, ZAB, ephemeral/sequential znodes, one-shot watches, the predecessor-watch election recipe, use by Kafka pre-KRaft / HBase / Hadoop HA / SolrCloud, KRaft removal) are publicly documented.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
