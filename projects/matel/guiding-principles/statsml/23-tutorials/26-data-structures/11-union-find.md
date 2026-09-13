# Union-Find

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Union-Find

**Subtitle:** Keep track of which things belong to the same group by giving every group one leader — merging two groups is just re-aiming one arrow, so millions of merges cost almost nothing

## One Leader per Friend Circle

**Tags:** `core idea` (blue), `groups` (green), `leaders` (orange)

- **The party** — a host greets 8 guests one by one and wants to track who already knows whom
- **The facts** — knowledge arrives as pairs, one at a time: "Ana knows Ben", "Cam knows Dia"
- **The circles** — pairs chain together: if Ana–Ben and Ben–Cam, then all three are one circle
- **One leader** — each circle keeps one member as leader; "same circle?" becomes "same leader?"
- **Union-find** — find follows arrows to the leader; union merges two circles into one

*Example (italic):* By midnight the host has heard 6 facts about the 8 guests and can answer instantly that Ana and Dia share a circle — without replaying the introductions.

**Key point:** Union-find tracks who belongs together by giving every group exactly one leader — two members are in the same group precisely when their leaders match.

### Visualization (canvas `c1`, 720×300)

Two-panel before/after dot diagram: the same 8 guests drawn as labeled dots, first all separate (8 circles), then colored by circle after the 6 facts (3 circles).

- **Title (bold 15px, `#1a5276`, top center):** "8 Guests: from 8 Circles to 3".
- **Left panel (x 40–340):** bold 13px `#444` subtitle at top center of panel: "before the facts — 8 circles"; 8 guests as 9px-radius dots, fill `#e5e9ef`, 2px `#6b7280` stroke, laid out in two rows: top row y=140 at x = 70, 140, 210, 280 (Ana, Ben, Cam, Dia), bottom row y=215 at the same x values (Eli, Fay, Gus, Hana); 12px `#444` name labels centered below each dot.
- **Right panel (x 380–680):** bold 13px `#444` subtitle: "after 6 facts — 3 circles"; identical layout (x = 410, 480, 550, 620) but colored by circle: Ana, Ben, Cam, Dia filled blue `#2a78d6`; Eli, Fay, Gus filled green `#008300`; Hana filled mute `#6b7280`; 12px name labels below; a thin dashed rounded rectangle (dash 4/3, matching circle color) around each of the two multi-guest circles.
- **Divider:** vertical 1px `#e5e9ef` line at x=360 from y=60 to y=260; a short 2px `#6b7280` arrow crossing it at y=175.
- **Annotation (bold 12px orange `#d95926`, centered near y=282):** "the host's only question all night: 'are these two in the same circle already?'".
- **Caption (11px `#444`, bottom right):** "illustrative party — guests and facts invented".

## Six Facts, Three Circles

**Tags:** `worked example` (blue), `union & find` (green)

- **Start** — every guest is their own leader: 8 guests, 8 circles
- **Fact 1** — "Ana knows Ben": Ben's arrow now aims at Ana; circles drop from 8 to 7
- **Facts 2–5** — Cam–Dia, Ben–Cam, Eli–Fay, Fay–Gus merge on: 6, 5, 4, then 3 circles
- **find(Dia)** — follow the arrows Dia → Cam → Ana: two hops, leader Ana
- **Fact 6** — "Ana knows Dia": find gives leader Ana for both, nothing merges; still 3 circles

*Example (italic):* After all six facts the circles are {Ana, Ben, Cam, Dia}, {Eli, Fay, Gus}, and {Hana} — fact 6 changed nothing because find returned the same leader twice.

**Key point:** Union = point one circle's leader at the other's; find = follow arrows to the top. Six facts took 8 circles down to 3, and the redundant sixth fact was caught by comparing leaders.

### Visualization (canvas `c2`, 720×300)

Forest diagram of the parent arrows after all six facts: three trees whose roots are the leaders, with the find(Dia) hop path highlighted.

- **Title (bold 15px, `#1a5276`, top center):** "The Arrows After Six Facts — find(Dia) Climbs to Ana".
- **Nodes:** 9px-radius dots with 12px `#444` name labels beside them. Tree 1 (blue `#2a78d6` fill): Ana at (150, 95), Ben at (70, 190), Cam at (170, 190), Dia at (230, 258). Tree 2 (green `#008300` fill): Eli at (450, 95), Fay at (390, 190), Gus at (510, 190). Loner (mute `#6b7280` fill): Hana at (640, 95) with 11px `#6b7280` label "own leader" below.
- **Leader marks:** bold 12px `#1a5276` label "leader" directly above Ana and Eli.
- **Arrows (child → parent, 2px, small arrowheads):** Ben→Ana and Cam→Ana in blue `#2a78d6`; Fay→Eli and Gus→Eli in green `#008300`; the find(Dia) path Dia→Cam and Cam→Ana drawn 3px orange `#d95926` instead of blue.
- **Hop labels:** bold 12px orange "hop 1" beside the Dia→Cam arrow and "hop 2" beside the Cam→Ana arrow.
- **Annotation (bold 12px orange `#d95926`, near x=280, y=55, two lines):** "find(Dia): 2 hops → Ana" / "fact 6 (Ana–Dia): same leader, no merge".
- **Count strip (12px `#444`, bottom left at x=40, y=288):** "circles after each fact: 8 → 7 → 6 → 5 → 4 → 3 → 3".
- **Caption (11px `#444`, bottom right):** "illustrative — arrows follow 'point the second leader at the first'".

## Millions of Merges Without Re-Scanning

**Tags:** `where it's used` (blue), `speed` (orange)

- **Deduping** — customer records sharing an email or phone must collapse into one identity group
- **The naive way** — relabel the whole table after every match: ~1,000,000 steps at a million rows
- **Union-find way** — each match is one union: follow a few arrows, move one arrow — about 5 hops
- **Near-constant** — with two easy tricks, cost per merge grows so slowly it is flat in practice
- **Where it shows up** — record linkage, image segmentation, network connectivity, Kruskal's trees

*Example (italic):* Linking 1,000,000 customer records match by match costs about 5 pointer hops per merge with union-find, versus a million-row relabel each time the naive way.

**Key point:** Union-find turns "merge two groups" from a full re-scan into a handful of arrow hops — the per-merge cost stays near-constant no matter how big the data gets.

### Visualization (canvas `c3`, 720×300)

Horizontal paired-bar chart: at four dataset sizes, the cost of one merge done by full relabeling (orange) versus by union-find (green), bar lengths on a log scale so both fit.

- **Title (bold 15px, `#1a5276`, top center):** "Cost of One Merge: Relabel Everything vs Union-Find".
- **Rows:** four size groups with left-aligned 12px `#444` labels at x=20: "1,000 rows", "10,000 rows", "100,000 rows", "1,000,000 rows"; each group has two 14px-tall bars starting at x=150 — relabel bar at y = 68, 122, 176, 230 and union-find bar 18px below it.
- **Relabel bars (orange `#d95926`, fill `rgba(217,89,38,0.35)`, 2px stroke):** pixel widths 240, 320, 400, 480; 12px orange value labels at bar ends: "1,000 steps", "10,000 steps", "100,000 steps", "1,000,000 steps".
- **Union-find bars (green `#008300`, fill `rgba(0,131,0,0.35)`, 2px stroke):** pixel widths 38, 48, 48, 56; 12px green value labels at bar ends: "3 hops", "4 hops", "4 hops", "5 hops".
- **Legend (12px, top right near x=560, y=48):** orange swatch "relabel", green swatch "union-find".
- **Annotation (bold 12px green `#008300`, near x=430, y=262):** "merge cost barely grows — near-constant".
- **Caption (11px `#444`, bottom right):** "illustrative hop counts; bar lengths log-scaled".

## The Chain Trap

**Tags:** `common mistake` (red), `path compression` (orange)

- **The trap** — always aiming the second leader at the first can grow one long chain of arrows
- **Slow find** — in an 8-guest chain, the bottom guest needs 7 hops to reach the leader
- **Path compression** — after a find, re-aim every guest you passed straight at the leader
- **Union by size** — hang the smaller circle under the bigger one, so trees stay shallow
- **Together** — the two tricks flatten chains into 1-hop stars; that is where near-constant comes from

*Example (italic):* The same 8-guest circle costs 7 hops as a chain but only 1 hop after a single path-compressed find flattens it into a star.

**Common mistake:** Implementing union-find without path compression or union by size — it still gives right answers, but finds degrade toward walking the whole chain instead of a couple of hops.

### Visualization (canvas `c4`, 720×300)

Two-panel diagram: the same 8-guest circle as a worst-case vertical chain on the left (7 hops) and as a flattened star after path compression on the right (1 hop).

- **Title (bold 15px, `#1a5276`, top center):** "Same Circle, Two Shapes: Chain vs Star".
- **Left panel (centered x=180):** bold 13px `#444` subtitle "naive linking — a chain"; 8 nodes as 7px-radius dots, fill `#e5e9ef`, 2px `#6b7280` stroke, stacked at x=180, y = 70, 97, 124, 151, 178, 205, 232, 259; 2px `#6b7280` arrows from each node up to the one above; top node filled blue `#2a78d6` with bold 12px `#1a5276` label "leader" to its right; bottom node stroked 2px orange `#d95926` with bold 12px orange label "find here: 7 hops" to its right.
- **Right panel (leader at x=500, y=105):** bold 13px `#444` subtitle "after path compression — a star"; leader as 9px blue `#2a78d6` dot labeled "leader" (bold 12px `#1a5276`, above); 7 member nodes as 7px `#e5e9ef` dots along an arc at y=225, x = 380, 420, 460, 500, 540, 580, 620, each with a 2px green `#008300` arrow straight to the leader; bold 12px green label "find anywhere: 1 hop" at (500, 262), centered.
- **Divider:** vertical 1px `#e5e9ef` line at x=320 from y=60 to y=270.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=288):** "same 8 guests, same circle — 7 hops versus 1".
- **Caption (11px `#444`, bottom right):** "illustrative worst case".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all node positions, bar widths, hop counts, and the circle-count sequence 8 → 7 → 6 → 5 → 4 → 3 → 3 are the hardcoded literals above (no randomness); the guests, facts, and step counts are invented and labeled illustrative; chart numbers must match the text numbers exactly.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
