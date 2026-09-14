# Akka Cluster & Sharding

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Akka Cluster & Sharding

**Subtitle:** The actor tree spans machines — 9,000 user session actors live on 3 nodes, and a sender never needs to know which node holds which actor

## One Actor System, Three Machines

**Tags:** `core idea` (blue), `membership & gossip` (green), `location transparency` (orange)

- **The app** — a web shop keeps one session actor per logged-in user: 9,000 users, 9,000 actors
- **Too big for one box** — suppose one machine can't carry the session load, so 3 nodes (A, B, C) share it
- **The cluster** — the nodes gossip their membership to each other; every node learns who is up
- **One tree** — the actors form a single logical system; a node is just where a branch happens to live
- **Location transparency** — code on node A sends to user 4217's actor the same way whether it sits on A or B

*Example (italic):* Node A handles user 4217's HTTP request, but her session actor lives on node B — the send looks identical either way, and the cluster delivers it.

**Key point:** A cluster stitches several machines into one actor system: gossip keeps a shared member list, and senders address actors by identity, never by machine.

### Visualization (canvas `c1`, 720×300)

Diagram of three node boxes joined by a gossip ring, with a message arrow crossing from node A to a session actor on node B.

- **Title (bold 15px, `#1a5276`, top center):** "Three Nodes, One Actor System: Gossip Below, Actors Above".
- **Node boxes:** three rounded rects 170×120, 8px radius, fill `rgba(42,120,214,0.12)`, 2px `#2a78d6` border, top-left corners at (55,90), (275,90), (495,90); bold 13px `#1a5276` labels "node A", "node B", "node C" centered at their tops.
- **Actors inside each box:** three small circles r=9, fill `rgba(0,131,0,0.25)`, 1.5px `#008300` stroke, in a row near the box bottom; 11px `#444` caption under each box "3,000 sessions" (nodes A, B, C alike).
- **Gossip ring:** dashed `#6b7280` (dash 5/4) 2px arcs connecting box bottoms A→B, B→C, C→A below y=230; 12px `#6b7280` label "gossip: member list, node health" centered at y=262.
- **Cross-node message:** 3px `#d95926` arrow from a point inside node A (x≈200, y≈150) to the middle actor circle in node B, bold 12px `#d95926` label "msg for user 4217" above the arrow midpoint.
- **Annotation (bold 13px green `#008300`, near x=560, y=70):** "sender never names a machine".
- **Caption (12px `#444`, bottom right):** "session counts illustrative".

## Routing User 4217: Entity, Shard, Node

**Tags:** `worked example` (blue), `shard coordinator` (green)

- **The entity id** — each session actor is an entity keyed by its user id, here 4217
- **The shard rule** — shard = userId mod 30, so user 4217 lands in shard 4217 % 30 = 17
- **The coordinator** — one elected coordinator owns the table: shards 0–9 on A, 10–19 on B, 20–29 on C
- **Hand-check** — shard 17 is in 10–19, so the message goes to node B; user 4230 → shard 0 → node A
- **On arrival** — node B's shard 17 region finds (or spawns) entity 4217 and hands it the message

*Example (italic):* A message tagged userId 4217 is hashed to shard 17, looked up to node B, and delivered to that one session actor — the sender did none of this by hand.

**Key point:** Sharding routes by entity id in two hops — id → shard by a fixed rule, shard → node by the coordinator's table — so 9,000 actors need only a 30-row map.

### Visualization (canvas `c2`, 720×300)

Left-to-right flow diagram of one message's routing hops, with the coordinator's shard table shown as the middle stage.

- **Title (bold 15px, `#1a5276`, top center):** "userId 4217 → shard 17 → node B → entity actor".
- **Stage boxes (all 40px tall, 8px radius, 12px `#2c3e50` text, centered vertically at y=150):** blue `rgba(42,120,214,0.15)` box 140px wide at x=30 labeled "msg: userId 4217"; violet `rgba(74,58,167,0.12)` box 150px wide at x=205 labeled "4217 % 30 = 17"; box 160px wide at x=390 with 1.5px `#c98500` border labeled "coordinator table"; green `rgba(0,131,0,0.12)` box 130px wide at x=580 labeled "entity 4217 on B".
- **Arrows:** 3px `#1a5276` arrows between consecutive boxes at y=150.
- **Table detail (under the coordinator box, 12px `#444`, three lines starting y=210):** "shards 0–9 → node A", "shards 10–19 → node B" (this line bold 12px `#008300`), "shards 20–29 → node C".
- **Second example (12px `#6b7280`, one line at y=60, left-aligned at x=30):** "same rule: userId 4230 → shard 0 → node A".
- **Annotation (bold 13px violet `#4a3aa7`, near x=390, y=95):** "9,000 actors, a 30-row map".
- **Caption (12px `#444`, bottom right):** "mod-30 rule exact, placement illustrative".

## When a Node Leaves, the Shards Move

**Tags:** `where it's used` (blue), `rebalancing` (green)

- **The event** — node C is drained for maintenance at 2:00pm; its 10 shards need a new home
- **The rebalance** — the coordinator reassigns shards 20–24 to node A and 25–29 to node B
- **The recovery** — entities restart on their new node at the next message, rebuilding state from persistence
- **The count** — A and B go from 3,000 sessions each to 4,500 each; the cluster serves all 9,000
- **Node join** — when C returns, the coordinator hands shards back the same way, no code change

*Example (italic):* User 4230's shard 0 never moved, but user 25's shard 25 quietly restarted on node B — both users keep shopping through the whole rebalance.

**Key point:** Rebalancing is why sharding beats fixed placement — the id-to-shard rule never changes, only the shard-to-node table, so nodes can come and go under live traffic.

### Visualization (canvas `c3`, 720×300)

Grouped bar chart of session actors per node before and after node C leaves, showing the load absorbed evenly by A and B.

- **Title (bold 15px, `#1a5276`, top center):** "Node C Leaves: 3,000 + 3,000 + 3,000 Becomes 4,500 + 4,500".
- **Axes:** origin x=70, baseline y=245, plot width 590, plot height 175; y = session actors 0 to 5,000, gridlines `#e5e9ef` at 1,250/2,500/3,750 with 12px `#444` labels; x = three node groups "node A", "node B", "node C" labeled 13px `#444` under the baseline.
- **Bars per group (width 55, gap 14):** "before" bar fill `rgba(42,120,214,0.35)` with 2px `#2a78d6` border at heights for `[3000, 3000, 3000]`; "after" bar solid `#008300` at heights for `[4500, 4500, 0]`.
- **Value labels:** 12px `#444` above each bar ("3,000", "4,500", "0").
- **Legend (top right, 12px):** blue swatch "before (3 nodes)", green swatch "after C leaves".
- **Empty-bar marker:** dashed `#e74c3c` (dash 4/3) outline where node C's after-bar would be, bold 12px `#e74c3c` label "shards 20–29 moved" beside it.
- **Annotation (bold 13px green `#008300`, near x=200, y=75):** "all 9,000 sessions still served".
- **Caption (12px `#444`, bottom right):** "counts illustrative, split exact for mod-30".

## Too Few Shards to Balance

**Tags:** `common mistake` (red), `shard count` (orange)

- **The mistake** — setting shard count equal to node count: 3 shards for 3 nodes "looks tidy"
- **Why it breaks** — shards are the unit of movement; 3 coarse shards cannot split across 2 nodes evenly
- **The numbers** — with 3 shards of 3,000, C leaving gives A two shards (6,000) and B one (3,000)
- **The fix** — with 30 shards, the same event gives A and B 15 shards each: 4,500 and 4,500
- **Rule of thumb** — pick roughly 10× the maximum node count; the shard table stays tiny either way

*Example (italic):* Same 9,000 users, same node loss — 3 shards leave node A carrying twice node B's load, while 30 shards split the loss exactly in half.

**Common mistake:** Confusing shards with nodes. A shard is a movable bundle of entities, not a machine — too few bundles and the coordinator has nothing fine-grained to rebalance with.

### Visualization (canvas `c4`, 720×300)

Side-by-side bar pairs comparing load on the two surviving nodes after C leaves, under a 3-shard versus a 30-shard configuration.

- **Title (bold 15px, `#1a5276`, top center):** "After C Leaves: 3 Shards Skew the Load, 30 Shards Split It".
- **Axes:** origin x=70, baseline y=245, plot width 590, plot height 175; y = session actors 0 to 7,000, gridlines `#e5e9ef` at 1,750/3,500/5,250 with 12px `#444` labels.
- **Left group (centered x≈220), 13px `#444` group label "3 shards" under baseline:** bars width 60 gap 20 for node A and node B at `[6000, 3000]`; node A bar solid `#e74c3c`, node B bar `rgba(42,120,214,0.35)` with 2px `#2a78d6` border; 12px labels "A: 6,000" and "B: 3,000" above.
- **Right group (centered x≈510), group label "30 shards":** same bar style, both bars solid `#008300` at `[4500, 4500]`; 12px labels "A: 4,500" and "B: 4,500" above.
- **Imbalance marker:** dashed `#6b7280` (dash 4/3) horizontal line at the 4,500 level across the left group, bold 12px `#e74c3c` label "A carries 2× B" above the left group at y=60.
- **Annotation (bold 13px green `#008300`, above the right group, y=95):** "fine-grained shards rebalance evenly".
- **Caption (12px `#444`, bottom right):** "9,000 total sessions, counts illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); session counts (9,000 total; 3,000/3,000/3,000 → 4,500/4,500; 6,000 vs 3,000 under 3 shards) are invented and labeled illustrative; the mod arithmetic (4217 % 30 = 17, 4230 % 30 = 0) and the even/uneven shard splits (10 per node, 15+15, 2+1) are exact consequences of the stated rules.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
