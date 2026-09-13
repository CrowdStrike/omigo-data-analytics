# Graph Databases

**Page type:** detail page (tutorial layout: `.card-section` blocks, each an h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Graph Databases

**Subtitle:** Store people and products as dots, friendships and purchases as lines between them — then "friends of friends who bought this" is a three-step walk, not a triple join

## Friends of Friends Who Bought the Lamp

**Tags:** `core idea` (blue), `running example` (green)

- **The question** — recommend lamp 88 to Ana if friends of her friends bought it
- **Nodes** — the dots: each person and each product is one node
- **Edges** — the lines: `FRIENDS_WITH` links people, `BOUGHT` links a person to a product
- **Traversal** — answering = walking edges: Ana → her friends → their friends → purchases
- **Edges are stored, not computed** — each node keeps direct pointers to its neighbors

*Example (italic):* In the drawing, the answer is just "what can I reach in 3 steps from Ana?" — Fay and Gus bought the lamp.

**Key point:** A graph database stores the connections themselves, so following a relationship is one pointer hop — not a search through a table.

### Visualization (canvas `c1`, 720×300)

Node-link diagram of the toy social network with the lamp purchase edges.

- **Title (bold 15px, `#1a5276`, top center):** "Nodes and Edges: Ana's Corner of the Network".
- **Shared node positions** (used by c1 and c2): Ana (90,150), Ben (230,75), Cara (230,150), Dev (230,228), Eli (390,55), Fay (390,125), Gus (390,190), Hana (390,250), lamp (560,155).
- **Friend edges** (grey `#b9c2cc`, width 1.5): Ana–Ben, Ana–Cara, Ana–Dev, Ben–Eli, Ben–Fay, Cara–Fay, Cara–Gus, Dev–Hana.
- **Bought edges** (gold `#c98500`, width 2.5): Fay–lamp, Gus–lamp.
- **Nodes:** filled circles radius 15 (Ana radius 17) with white bold 10px name labels centered. Ana blue `#2a78d6`; Ben/Cara/Dev aqua `#199e70`; Eli/Fay/Gus/Hana violet `#4a3aa7`.
- **Lamp node:** 52×40 rectangle centered at (560,155), fill `rgba(201,133,0,0.85)`, stroke `#c98500` width 2, white bold label "lamp 88".
- **Legend labels:** grey 11px "grey lines: FRIENDS_WITH" at (160,272); gold bold 11px "gold lines: BOUGHT" at (475,245).
- **Annotations:** blue bold 12px "start" above Ana; orange (`#d95926`) bold 13px bottom center: "the question: which purple nodes have a gold line to the lamp?".

## Walking the Three Hops by Hand

**Tags:** `worked example` (green)

- **Hop 1** — Ana's friends: {Ben, Cara, Dev} — 3 people
- **Hop 2** — their friends: Ben→{Eli, Fay}, Cara→{Fay, Gus}, Dev→{Hana}
- **Dedupe** — Fay appears twice; distinct friends-of-friends = {Eli, Fay, Gus, Hana} = 4
- **Hop 3** — follow each one's `BOUGHT` edges to lamp 88: Fay yes, Gus yes
- **Total work** — 1 + 3 + 4 = 8 people visited, answer = {Fay, Gus}

*Example (italic):* The walk never touches the other 10 million people in the network — only the 8 reachable ones.

**Key point:** Traversal cost depends on how many neighbors you actually visit, not on how big the whole network is.

### Visualization (canvas `c2`, 720×300)

Same node-link network as c1 with translucent hop bands and the walk highlighted.

- **Title (bold 15px, `#1a5276`, top center):** "The Walk: 3 Friends, 4 Friends-of-Friends, 2 Buyers".
- **Hop bands** (translucent rectangles, y 42, height 226): aqua `rgba(25,158,112,0.07)` x 185 width 90; violet `rgba(74,58,167,0.07)` x 345 width 90; gold `rgba(201,133,0,0.08)` x 510 width 105. Band labels bold 11px at y 56: aqua "hop 1: 3" (x 230), violet "hop 2: 4 distinct" (x 390), gold "hop 3: BOUGHT?" (x 562).
- **Edges:** all friend edges in aqua `#199e70` width 2; bought edges in green `#008300` width 3.
- **Nodes:** same positions as c1. Ana blue radius 17; Ben/Cara/Dev aqua; Eli and Hana greyed out `#8b95a1`; Fay and Gus green `#008300` radius 17. Lamp box as in c1.
- **Annotations:** violet 11px left-aligned at (415,112): "Fay reached twice — count once"; green bold 13px bottom center: "answer: {Fay, Gus} — 8 people visited, 10M ignored".

## The Same Question in SQL: a Triple Self-Join

**Tags:** `worked example` (green), `watch out` (orange)

- **Relational shape** — one `friendships(person, friend)` table, one `purchases` table
- **Hop = join** — each extra hop joins the friendships table to itself once more
- **The query** — `friendships f1 JOIN friendships f2 JOIN purchases p`
- **Fan-out** — at 150 friends each: 150 rows after f1, 22,500 after f2, then filter
- **The graph walk** — visited 8 people for the same answer on our toy network

*Example (italic):* Each join step multiplies rows by ~150 — hop 3 in SQL means ~3.4M intermediate rows.

**Key point:** SQL rebuilds every connection by matching ids at query time; the graph stored those matches as edges when the data was written.

### Visualization (canvas `c3`, 720×300)

Horizontal log-scale bar chart comparing rows handled per hop.

- **Title (bold 15px, `#1a5276`, top center):** "Rows Handled per Hop at 150 Friends Each (illustrative)".
- **Bars** (labels left at x 30, bars start x 250, plot width 380, top 60, row height 48, bar height 26, alpha 0.7, log10 scale with max log10(4,000,000)):
  - "hop 1: f1 rows" — value 150, label "150", violet `#4a3aa7`.
  - "hop 2: f1 JOIN f2" — value 22,500, label "22,500", violet `#4a3aa7`.
  - "hop 3: + purchases (~150 each)" — value 3,375,000, label "3.4M scanned to filter", red `#e74c3c`.
  - "graph walk, all 3 hops" — value 8, label "8 nodes visited", green `#008300`.
- Value labels bold 12px in bar color to the right of each bar; row labels 12px `#333`.
- **Footnote** (grey 11px below bars): "bar length is log scale — each hop multiplies SQL rows by ~150".
- **Takeaway** (orange `#d95926` bold 13px bottom center): "the join rebuilds edges by matching ids; the graph already stored them".

## When Relationships ARE the Data

**Tags:** `where it's used` (blue), `common mistake` (red)

- **Fraud rings** — accounts sharing a device, card, or address form telltale dense clusters
- **Recommendations** — "people near you in the graph bought..." is a 2-3 hop walk
- **Networks of things** — routes, supply chains, citations, org charts: paths are the question
- **The tell** — your SQL is mostly self-joins and "path", "hops", "connected to" words
- **The counter-tell** — filters and aggregates over attributes: plain SQL is simpler and faster

*Example (italic):* "Average order value by month" has no hops in it — a warehouse beats a graph there every time.

**Common mistake:** Picking a graph database because data "has relationships" — all data does. Pick it when your questions are about paths through them.

### Visualization (canvas `c4`, 720×300)

Split panel: accounts table on the left vs the same data as a graph star on the right.

- **Title (bold 15px, `#1a5276`, top center):** "A Fraud Ring Is Invisible in Rows, Obvious in a Graph".
- **Divider:** vertical dashed grey line (`#bdc3c7`, dash 4/3) at x 360 from y 38 to 262.
- **Left panel** — 5-row table starting at (45,60), rows 270×23, alternating white/`#f4f6f8` fill, `#b9c2cc` borders; header bold 12px `#1a5276`: "accounts table — each row looks fine alone". Rows (monospace 12px): acct 301/card ...9917, acct 302/card ...9917, acct 303/card ...9917, acct 304/card ...4410, acct 305/card ...9917. Card values in magenta `#d55181` when "...9917", grey otherwise. Footnote grey 11px: "the shared card hides in a value column".
- **Right panel** — star graph: central "card 9917" node as 60×28 magenta `#d55181` rectangle at (540,150), magenta edges (width 2) to four violet account circles (radius 15, white bold 9px labels) "301" (445,85), "302" (632,85), "303" (445,218), "305" (632,218); the odd account "304" as grey `#8b95a1` circle at (660,150) with a short grey edge. Caption magenta bold 12px at (540,258): "4 accounts, 1 card: a ring".
- **Takeaway** (orange bold 13px bottom center): "use a graph when the question is about paths and clusters — not when it's filters and averages".

## Regeneration instructions

- **Template:** tutorials topic-page layout (see `tutorials/CLAUDE.md` and `most-powerful-signals/07-social-graph-connections.html` skeleton). h1 (no index number) with 2px `#2980b9` bottom border, `.subtitle` grey one-liner, then four `.card-section` blocks: each has an `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) and a `table.layout` with `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row first, then a `<ul>` of one-line bullets each opening with `<b>bold term</b>` (bold terms colored `#1a5276`), one italic `.example` paragraph, one `.key-point` callout (background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem).
- **Tag pills:** inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px. Colors: blue `rgba(26,82,118,0.12)`/`#1a5276`; green `rgba(39,174,96,0.15)`/`#27ae60`; red `rgba(231,76,60,0.12)`/`#e74c3c`; orange `rgba(230,126,34,0.15)`/`#e67e22`. Inline `code` in ui-monospace on `#f4f6f8` background.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; canvases `width:100%` with `1px solid #e0e0e0` border, radius 4px.
- **Canvas:** each declared 720×300 intrinsic; scaled by `window.devicePixelRatio` via a shared `setup(id)` helper (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates).
- **Chart palette object `P`:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow/gold `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions (this page has no outbound links).
