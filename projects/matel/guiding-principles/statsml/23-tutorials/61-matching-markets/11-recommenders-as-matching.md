# Recommenders as Matching

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Recommenders as Matching

**Subtitle:** A feed is a matching problem in disguise — users on one side, items with limited exposure on the other

**Running data (illustrative), used identically in every section — relevance scores per user–item pair:**

|       | Item A | Item B | Item C |
|-------|--------|--------|--------|
| Alice | 0.9    | 0.8    | 0.3    |
| Bob   | 0.9    | 0.4    | 0.6    |
| Carol | 0.9    | 0.5    | 0.7    |

Pure per-user top-1: everyone gets Item A — total relevance 0.9+0.9+0.9 = 2.7; Item A takes 3 impressions, Items B and C get zero.
Budget of 1 impression per item: best assignment is Alice–B (0.8), Bob–A (0.9), Carol–C (0.7) — total 2.4. The other five assignments total 2.3, 2.0, 2.0, 1.7, 1.6, so 2.4 is the maximum under the budget. Cost: (2.7−2.4)/2.7 ≈ 11% relevance.

## Who Sees What, Not What Ranks First

**Tags:** `core idea` (blue), `two-sided view` (green)

- **Ranking's question** — order all items for Alice; one user at a time, best score on top
- **Matching's question** — who sees what across all users, given each item can only be shown so much
- **Slots are capacity** — a feed shows k items per user; those k slots are the scarce resource
- **Crowding out** — an item shown to everyone eats slots that every other item needed
- **Going dark** — a new creator shown to no one never collects the data to prove itself

*Example (italic):* A feed with 3 slots per user and a million users is an allocation of 3 million impressions — something decides where they go.

**Key point:** A feed is a matching market in disguise: users bring slots, items bring exposure budgets, and the real decision is the allocation between the two sides.

### Visualization (canvas `c1`, 720×300)

A bipartite sketch: four user boxes on the left (one slot each), three item boxes on the right with exposure-budget labels, faint candidate edges between every pair.

- **Title (bold 15px, `#1a5276`, top center):** "A Feed as a Bipartite Market (illustrative)".
- **Left column header (bold 13px blue `#2a78d6`, centered at (170, 56)):** "USERS — each feed has k slots".
- **Right column header (bold 13px green `#008300`, centered at (555, 56)):** "ITEMS — each has an exposure budget".
- **Edges (drawn first, under the boxes):** 1px `rgba(107,114,128,0.3)` lines from (250, each user mid) to (470, each item mid) — all 12 user–item pairs.
- **User boxes:** x=90, width 160, height 34, at y = 72, 124, 176, 228 (mids 89, 141, 193, 245); fill `#fbfcfd`, 2px blue `#2a78d6` border; centered 12px `#2c3e50` text: "Alice — 1 slot", "Bob — 1 slot", "Carol — 1 slot", "Dana — 1 slot".
- **Item boxes:** x=470, width 190, height 44, at y = 84, 152, 220 (mids 106, 174, 242); fill `#fbfcfd`, 2px green `#008300` border; bold 12px green name at box top+18 ("Item A", "Item B", "Item C"), 11px `#6b7280` sub-line at top+34: "budget: 2 impressions", "budget: 1 impression", "budget: 1 impression".
- **Caption (bold 12px magenta `#d55181`, centered at y=284):** "4 user slots on the left, 4 units of budget on the right — allocation balances both sides".

## Everyone's Best Item Is the Same Item

**Tags:** `worked example` (blue), `exposure budget` (orange)

- **The scores** — Alice: A 0.9, B 0.8, C 0.3; Bob: A 0.9, B 0.4, C 0.6; Carol: A 0.9, B 0.5, C 0.7
- **Pure top-1** — everyone's best is Item A: total relevance 0.9+0.9+0.9 = 2.7, A takes 3 impressions
- **The starvation** — Items B and C get zero impressions, zero clicks, zero training data
- **Budget of 1 each** — best assignment is Alice–B (0.8), Bob–A (0.9), Carol–C (0.7): total 2.4
- **The price** — 2.4 vs 2.7 is an 11% relevance cost; the buy is signal for every item

*Example (italic):* Redo it by hand: only six assignments exist, and the other five total 2.3 or less — Alice–B, Bob–A, Carol–C is the unique maximum.

**Key point:** Under an exposure budget the optimum moves Alice to her second choice so no item goes dark — 11% of relevance buys a catalog where every item keeps earning data.

### Visualization (canvas `c2`, 720×300)

Two side-by-side bipartite panels on the running scores: per-user top-1 (three arrows converging on Item A) vs the budgeted matching (spread arrows), totals 2.7 vs 2.4 annotated.

- **Title (bold 15px, `#1a5276`, top center):** "Same Scores, Two Allocations (illustrative)".
- **Divider:** 1px `#e5e9ef` vertical line at x=360 from y=40 to y=250.
- **Left panel header (bold 13px orange `#d95926`, centered at (180, 52)):** "PER-USER TOP-1".
- **Right panel header (bold 13px green `#008300`, centered at (540, 52)):** "MATCHED — BUDGET 1 PER ITEM".
- **User boxes (both panels):** width 88, height 28, at y = 89, 151, 213 (mids 103, 165, 227); left panel x=44, right panel x=404; fill `#fbfcfd`, 1.5px `#2a78d6` border, centered 12px `#2c3e50` names "Alice", "Bob", "Carol".
- **Item boxes (both panels):** width 96, height 38, at y = 84, 146, 208 (mids 103, 165, 227); left panel x=228, right panel x=588; fill `#fbfcfd`; bold 12px name at top+15, 11px sub-line at top+30.
  - Left panel: Item A — 1.5px orange `#d95926` border, orange name, sub "3 impressions" (orange); Items B and C — 1.5px `#9aa2ad` border, mute `#6b7280` name and sub "0 impressions".
  - Right panel: all three 1.5px green `#008300` border, green names; subs 11px `#2c3e50`: Item A "Bob · 0.9", Item B "Alice · 0.8", Item C "Carol · 0.7".
- **Left arrows (orange `#d95926`, 2px, filled angled arrowheads):** all three onto Item A's left edge — Alice (132, 103)→(224, 97), Bob (132, 165)→(224, 103), Carol (132, 227)→(224, 109); bold 11px orange score labels "0.9" at (172, 94), (166, 128), (166, 162).
- **Right arrows (green `#008300`, 2px, filled angled arrowheads):** Alice (492, 103)→(584, 159) to Item B, Bob (492, 165)→(584, 109) to Item A, Carol (492, 227)→(584, 227) to Item C (scores shown in the item sub-lines, not on the arrows).
- **Totals:** bold 12px orange centered at (180, 266): "total relevance = 2.7"; bold 12px green centered at (540, 266): "total 0.8 + 0.9 + 0.7 = 2.4".
- **Caption (12px `#6b7280`, centered at y=290):** "cost: 11% of relevance — benefit: every item gets signal, every niche stays alive".

## Loops, Floors, and Exploration Slots

**Tags:** `where it's used` (blue), `feedback loop` (orange), `cold start` (green)

- **The loop** — shown more → clicked more → ranked higher → shown more; popularity feeds itself
- **Tail starvation** — the loop drains impressions from the catalog tail until it never surfaces
- **Creator platforms** — supply-side health turns into exposure floors and caps as hard constraints
- **Cold start** — exploration slots are capacity reserved for data collection, not a ranking tweak
- **The formal shape** — an assignment/transportation problem (or a large-scale relaxation) on scores
- **Division of labor** — the ML model produces the scores; the matching layer produces the allocation

*Example (italic):* A creator platform that guarantees every new upload some minimum of impressions is running a matching constraint, not a better model.

**Key point:** Production feeds bolt allocation machinery onto model scores — caps break the popularity loop, floors keep supply alive, and exploration is budgeted capacity.

### Visualization (canvas `c3`, 720×300)

The popularity feedback cycle as three boxes with a return path, and an orange exposure-cap valve breaking the return segment.

- **Title (bold 15px, `#1a5276`, top center):** "The Popularity Loop and the Exposure-Cap Valve".
- **Boxes (width 180, height 44, y=84):** "SHOWN MORE" at x=50 (2px blue `#2a78d6` border), "CLICKED MORE" at x=270 (2px green `#008300`), "RANKED HIGHER" at x=490 (2px violet `#4a3aa7`); fill `#fbfcfd`; bold 13px header in the border color, centered at box mid (y=111).
- **Forward arrows (2px mute `#6b7280`, filled arrowheads):** (234, 106)→(268, 106) and (454, 106)→(488, 106).
- **Return path (2px mute `#6b7280`):** down from (580, 128) to (580, 196), left along y=196 with a gap for the valve — segments (580, 196)→(378, 196) and (342, 196)→(140, 196) — then up from (140, 196) to (140, 134) ending in an upward filled arrowhead at (140, 132).
- **Valve (orange `#d95926` bowtie at (360, 196)):** left triangle (344, 187)/(344, 205)/(359, 196) and right triangle (376, 187)/(376, 205)/(361, 196), filled.
- **Valve label (bold 12px orange `#d95926`, centered at (360, 222)):** "EXPOSURE CAP — the valve that breaks the loop".
- **Note (bold 12px magenta `#d55181`, centered at y=252):** "without the cap: rich get richer, the catalog tail starves at zero".
- **Caption (12px `#6b7280`, centered at y=278):** "exploration slots do the same for cold start — capacity reserved for data collection".

## Better Scores Don't Fix Allocation

**Tags:** `common mistake` (red)

- **The mix-up** — learning-to-rank and relevance models improve the SCORES, not the allocation
- **Two layers** — scoring asks "how good is this pair?"; matching asks "who gets the scarce slots?"
- **The proof** — the worked example's scores never change; only the assignment rule moved
- **Perfect and broken** — a perfect ranker with greedy per-user top-k still piles exposure on one item
- **The tell** — if the fix for an exposure problem is "retrain the model", the wrong layer is being fixed

*Example (italic):* In the 3×3 example the ranker is flawless — the pile-up on Item A comes entirely from greedy per-user allocation.

**Common mistake:** Treating exposure concentration as a model-quality problem. Ranking quality lives in the scores; exposure lives in the allocation — a matching decision under capacity.

### Visualization (canvas `c4`, 720×300)

A three-box pipeline (ranking model → capacity & budgets → allocation) with a crossed-out red shortcut arc that tries to skip the capacity layer.

- **Title (bold 15px, `#1a5276`, top center):** "Scores Are the Input, Allocation Is the Decision".
- **Boxes (y=110, height 64, width 180):** "RANKING MODEL" at x=48, 2px blue `#2a78d6` border, bold 13px blue header at box top+26, sub-line 12px `#2c3e50` "relevance score per pair" at top+48; "CAPACITY & BUDGETS" at x=272, 2px orange `#d95926` border, sub-line "k slots, floors, caps"; "ALLOCATION" at x=496, 2px green `#008300` border, sub-line "who sees what". All fills `#fbfcfd`.
- **Pipeline arrows:** 2px mute `#6b7280` from (232, 142) to (268, 142) and from (456, 142) to (492, 142), filled arrowheads.
- **Shortcut arc:** 2px red `#e74c3c` quadratic curve from (138, 106) to (586, 106), control point (362, 30), red filled arrowhead at the right end; bold 14px red "✕" at (362, 52); label bold 12px red centered at (362, 74): "\"a better ranker will fix exposure\" — it won't".
- **Note (bold 13px magenta `#d55181`, centered at y=240):** "a perfect ranker still over-concentrates if allocation is greedy per-user".
- **Sub-caption (12px `#6b7280`, centered at y=262):** "learning-to-rank improves the scores; matching decides the allocation under capacity".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale(...)`; charts stored in an array, redrawn on debounced window resize; shared `arrow(ctx, x1, y1, x2, y2, color, width)` helper drawing a line plus an angle-aware filled arrowhead (used for straight, slanted, and upward arrows alike).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red `#e74c3c` only for the crossed-out shortcut in c4.
- **Data integrity:** hardcoded arrays only — score matrix `[[0.9,0.8,0.3],[0.9,0.4,0.6],[0.9,0.5,0.7]]` (rows Alice/Bob/Carol, columns Item A/B/C); top-1 total 2.7 (all three pairs at 0.9); budgeted optimum Alice–B (0.8), Bob–A (0.9), Carol–C (0.7) total 2.4; c1 budgets `[2, 1, 1]` with four one-slot users; invented numbers carry "(illustrative)" in chart titles. Platforms stay generic ("a feed", "a creator platform") — no brand names.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
