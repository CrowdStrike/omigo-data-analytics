# Neo4j & Graph Databases

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Neo4j & Graph Databases

**Subtitle:** When the question is about connections — who knows whom, what links to what — a graph database stores the joins up front instead of recomputing them on every query

## The Party List That Broke the Database

**Tags:** `core idea` (blue), `property graph` (green), `Cypher` (orange)

- **The party** — Ana invites her friends, their friends, and their friends: everyone within 3 hops
- **The graph** — each member is a node labeled `:Person` carrying properties like `name` and `city`
- **The relationship** — `KNOWS` is a typed, directed edge that holds its own properties (`since: 2019`)
- **The model** — nodes, typed relationships, and properties on both: the property-graph model
- **The query** — Cypher draws the pattern as ASCII art: `(a)-[:KNOWS]->(b)` reads "a knows b"

*Example (italic):* `MATCH (a:Person {name:'Ana'})-[:KNOWS*1..3]->(b) RETURN DISTINCT b` — Ana's whole 3-hop invite list in one line.

**Key point:** In a graph database the relationship is a first-class record — it has a type, a direction, and properties of its own, not just a foreign-key number sitting in someone else's row.

### Visualization (canvas `c1`, 720×300)

Diagram of the smallest possible property graph: two Person nodes, one KNOWS relationship, with property boxes attached to all three.

- **Title (bold 15px, `#1a5276`, top center):** "Nodes, Typed Relationships, and Properties on Both".
- **Ana node:** circle center (180,140) radius 44, fill `rgba(42,120,214,0.15)`, 3px `#2a78d6` stroke; bold 13px `#1a5276` two-line label ":Person" / "Ana" centered in the circle.
- **Bo node:** circle center (540,140) radius 44, same style, label ":Person" / "Bo".
- **Relationship:** 3px `#d95926` arrow from (224,140) to (496,140) with a filled triangular arrowhead at Bo's edge; bold 12px `#d95926` label ":KNOWS" centered above the shaft at (360,120).
- **Relationship property box:** rounded rect 130×26 centered at (360,172), fill `rgba(217,89,38,0.10)`, 1px `#d95926` border, 11px `#2c3e50` text "{ since: 2019 }".
- **Node property boxes:** rounded rects 150×40 centered at (180,232) and (540,232), fill `rgba(42,120,214,0.08)`, 1px `#2a78d6` border, 11px `#2c3e50` two-line text "name: 'Ana'" / "city: 'Lisbon'" and "name: 'Bo'" / "city: 'Porto'"; 1px dashed `#6b7280` connector from each circle's bottom to its box top.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=285):** "the edge is a record too — type, direction, properties".

## Three Hops by Hand: Self-Joins vs Pointer Chasing

**Tags:** `worked example` (blue), `index-free adjacency` (green)

- **The setup** — 1,000,000 members, each knowing 50 people on average (network sizes illustrative)
- **The counting** — 50 friends, 50×50 = 2,500 two-hop paths, 50³ = 125,000 three-hop paths (exact)
- **The relational way** — a `friendships` table of 50,000,000 rows, self-joined once per hop
- **The join cost** — every hop re-searches the whole table's index, once per path being extended
- **The graph way** — each node stores direct pointers to its neighbors; a hop is a pointer dereference
- **Index-free adjacency** — traversal cost scales with paths visited, not with total table size

*Example (italic):* On the illustrative benchmark, Ana's 3-hop invite list takes 30s as three self-joins but 0.4s as a graph traversal.

**Key point:** A join recomputes "who is connected" through a global index at query time; a graph stores the connection as a pointer at write time — so multi-hop queries don't degrade the way stacked self-joins do.

### Visualization (canvas `c2`, 720×300)

Grouped bar chart of query time by hop depth: relational self-join (orange, exploding) vs graph traversal (green, gently rising), 1 to 4 hops.

- **Title (bold 15px, `#1a5276`, top center):** "3-Hop Friends Query: Self-Joins Blow Up, Traversal Barely Notices".
- **Axes:** 2px `#999` baseline at y=245 from x=60 to x=660; group centers at x = 135, 285, 435, 585 with 12px `#444` tick labels "1 hop", "2 hops", "3 hops", "4 hops" at y=263.
- **Bars:** width 44; relational bar left edge at group center −49, graph bar left edge at center +5, both rising from y=245.
- **Relational bars (orange `#d95926`, solid):** heights `[24, 60, 130, 180]` px with 11px `#444` labels above: "0.02s", "0.4s", "30s", ">1000s"; the 4-hop bar gets an extra bold 12px red `#e74c3c` label "timed out" above its time label.
- **Graph bars (green `#008300`, solid):** heights `[16, 30, 58, 86]` px with 11px `#444` labels above: "0.01s", "0.05s", "0.4s", "2.1s".
- **Legend (12px `#444`, top left at x=70, y=52):** orange swatch "relational self-join", green swatch "graph traversal", stacked 18px apart.
- **Annotation (bold 13px green `#008300`, near x=300, y=85):** "traversal cost follows paths, not table size".
- **Caption (12px `#444`, bottom right):** "bar heights schematic (log-feel), times illustrative".

## Where the Joins Are the Point

**Tags:** `where it's used` (blue), `fraud & recommendations` (green)

- **Fraud rings** — accounts tied by shared phones and addresses form cycles visible only over many hops
- **Recommendations** — "people you may know" is exactly the 2-hop friends-of-friends query
- **Knowledge graphs** — entities plus typed facts, `(aspirin)-[:TREATS]->(headache)`, queried by pattern
- **The common thread** — all three ask about paths and shapes, not sums over columns
- **Depth scales** — hop 4 grows with the paths visited, not table size; the same depth kills a self-join

*Example (italic):* "Do these two accounts connect within a few hops of shared identifiers?" is one Cypher pattern — the same question as Ana's party list, wearing a fraud badge.

**Key point:** Reach for a graph database when the connections themselves are the product — ring detection, recommendations, and knowledge graphs are all "find this shape in the network" questions.

### Visualization (canvas `c3`, 720×300)

Ring diagram of a fraud ring: four account nodes alternating with four shared-identifier nodes around a circle, edges closing an 8-hop cycle.

- **Title (bold 15px, `#1a5276`, top center):** "A Fraud Ring: Four 'Strangers' Chained by Shared Phones and Addresses".
- **Ring layout:** 8 nodes on a circle centered (360,160), radius 80, at 45° steps clockwise from the top: (360,80), (417,103), (440,160), (417,217), (360,240), (303,217), (280,160), (303,103).
- **Account nodes (even positions 0,2,4,6):** circles radius 24, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` stroke, bold 11px `#1a5276` labels "Acct A", "Acct B", "Acct C", "Acct D".
- **Identifier nodes (odd positions 1,3,5,7):** rounded rects 78×24 centered on their points, fill `rgba(217,89,38,0.12)`, 2px `#d95926` stroke, 11px `#2c3e50` labels "555-0101", "9 Elm St", "555-0177", "4 Oak Av".
- **Edges:** 2px `#6b7280` straight lines joining each pair of neighboring ring nodes, closing the full cycle (8 edges).
- **Legend (12px `#444`, top left at x=20, y=60):** blue circle swatch "account node", orange box swatch "shared identifier", stacked 20px apart; 11px `#6b7280` note "edges: :USES_PHONE / :USES_ADDRESS" below at y=104.
- **Annotation (bold 13px magenta `#d55181`, bottom center near y=288):** "one 8-hop cycle pattern finds the whole ring".
- **Caption (12px `#444`, bottom right, y=268):** "identifiers fictitious, ring illustrative".

## Not a Faster Table for Everything

**Tags:** `common mistake` (red), `right tool` (orange)

- **The mistake** — treating a graph database as a faster drop-in for every relational workload
- **Start nodes need indexes** — index-free adjacency covers the hops; finding Ana still uses an index
- **Aggregates don't care** — "total orders per month" touches every record either way; no graph win
- **Shallow joins are fine** — a 1- or 2-hop join on indexed keys is fast in any relational database
- **The test** — count the hops in your slowest queries; graphs pay off at depth 3+ or variable depth

*Example (italic):* A team migrates its whole billing schema to a graph and finds invoice totals no faster — the one query that was slow, a 4-hop referral chain, was the only graph-shaped one.

**Common mistake:** Assuming index-free adjacency speeds up everything. It speeds up traversals — lookups still use indexes and aggregates still scan — so port the deep-hop queries and keep the ledgers relational.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram matching query shape to store: a deep traversal (graph wins) vs a whole-table aggregate (relational fine, graph no advantage).

- **Title (bold 15px, `#1a5276`, top center):** "Match the Query to the Store".
- **Row 1 (boxes centered on y=105), label 12px `#444` at x=20:** "3-hop invite list"; orange rounded box 200×40 centered at (270,105), fill `rgba(217,89,38,0.12)`, 12px `#2c3e50` text "relational: 3 self-joins — 30s" with bold 12px red `#e74c3c` "✗" at its right edge; green rounded box 210×40 centered at (530,105), fill `rgba(0,131,0,0.12)`, text "graph: 125,000 hops — 0.4s" with bold 12px green `#008300` "✓".
- **Row 2 (boxes centered on y=195), label at x=20:** "monthly order totals"; green rounded box 200×40 centered at (270,195), text "relational: one scan — fast" with green "✓"; gray rounded box 210×40 centered at (530,195), fill `rgba(107,114,128,0.12)`, 1px `#6b7280` border, text "graph: no advantage" with bold 12px `#6b7280` "—".
- **Box style:** 8px radius, 1px borders matching each fill's hue, 12px `#2c3e50` text centered.
- **Annotation (bold 13px orange `#d95926`, centered near y=262):** "index-free adjacency speeds hops — not lookups, not sums".
- **Caption (12px `#444`, bottom right, y=288):** "times illustrative, matching the hop benchmark above".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all coordinates and bar heights are the hardcoded values above (no randomness); path counts 50 / 2,500 / 125,000 are exact arithmetic given the 50-friends-per-member assumption; the member count, friend count, query times (0.02s / 0.4s / 30s / >1000s vs 0.01s / 0.05s / 0.4s / 2.1s), and fraud-ring identifiers are invented and labeled illustrative; the property-graph model, Cypher pattern syntax, and index-free adjacency are publicly documented Neo4j facts.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
