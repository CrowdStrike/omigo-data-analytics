# Representing Graphs

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Representing Graphs

**Subtitle:** A graph is just a record of who connects to whom — and you can write that record as a big yes/no grid (the adjacency matrix) or as each node's short list of neighbors (the adjacency list)

## One Friend Group, Two Notebooks

**Tags:** `core idea` (blue), `nodes & edges` (green), `two encodings` (orange)

- **The friend group** — five friends: Ana, Ben, Cara, Dev, Eli; a graph is just who is friends with whom
- **The edges** — 5 friendships: Ana–Ben, Ana–Cara, Ben–Cara, Cara–Dev, Dev–Eli — that is the whole map
- **Two notebooks** — the same map can be written as one big yes/no grid or as five short friends lists
- **The matrix** — a 5×5 grid: the cell at row Ana, column Ben holds a 1 because they are friends, else 0
- **The list** — one line per person naming only their friends: "Cara → Ana, Ben, Dev"

*Example (italic):* "Ana is friends with Ben" becomes a 1 in the grid's Ana-row Ben-column, and a "Ben" entry on Ana's list — the same fact in two notebooks.

**Key point:** A graph is nodes plus connections; the adjacency matrix and the adjacency list are two bookkeeping styles for exactly the same connections.

### Visualization (canvas `c1`, 720×300)

Single-panel node-link drawing of the five-friend graph: labeled circles joined by the 5 friendship lines, so the reader sees the object both notebooks will describe.

- **Title (bold 15px, `#1a5276`, top center):** "The Friend Group: 5 People, 5 Friendships".
- **Nodes (circles radius 22, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border, name centered inside in bold 13px `#1a5276`):** Ana at (160, 130), Ben at (310, 85), Cara at (310, 200), Dev at (470, 150), Eli at (600, 110).
- **Edges (3px `#6b7280` lines drawn under the nodes):** Ana–Ben, Ana–Cara, Ben–Cara, Cara–Dev, Dev–Eli — exactly the 5 pairs above, endpoints at the node centers.
- **Edge count badge:** bold 13px green `#008300` text near (120, 255): "5 friendships = 5 edges".
- **Annotation (bold 13px orange `#d95926`, near (470, 245), two lines):** "no line between Ana and Dev —" / "the matrix records this 0; the list just omits it".
- **Caption (12px `#444`, bottom right):** "illustrative — a made-up friend group".

## Writing Both Notebooks by Hand

**Tags:** `worked example` (blue), `count it yourself` (green)

- **The grid** — 5 people give 25 cells; each friendship fills two cells (Ana–Ben and Ben–Ana), so 10 hold a 1
- **Mostly zeros** — the other 15 cells hold 0; the grid spends most of its ink on non-friendships
- **The lists** — Ana: 2 entries, Ben: 2, Cara: 3, Dev: 2, Eli: 1 — 10 entries total, zero wasted lines
- **Same total** — both notebooks record each of the 5 friendships twice: 10 ones in one, 10 entries in the other
- **Symmetric** — friendship is mutual, so the grid mirrors across its diagonal; one-way "follows" would not

*Example (italic):* Cara's grid row reads 1, 1, 0, 1, 0 (Ana, Ben, herself, Dev, Eli) — the same three friends her list names outright.

**Key point:** 5 friendships become 10 filled cells in the matrix and 10 entries in the list — but the matrix also writes down the 15 zeros, and the list skips them.

### Visualization (canvas `c2`, 720×300)

Two-panel side-by-side: the full 5×5 adjacency matrix drawn as a grid on the left, the five adjacency lists written out on the right — the same 5 friendships encoded both ways.

- **Left panel title (bold 13px `#1a5276`, centered over the grid):** "Adjacency matrix — 25 cells, 10 ones".
- **Grid:** 5×5 cells of 30×30 px, top-left cell at (95, 75); 1px `#e5e9ef` cell borders; column initials "A B C D E" in 12px `#444` above the grid, row initials "A B C D E" at x=75 beside each row.
- **Filled cells (fill `rgba(0,131,0,0.30)`, centered "1" in bold 12px `#008300`):** the 10 cells (A,B), (A,C), (B,A), (B,C), (C,A), (C,B), (C,D), (D,C), (D,E), (E,D). All other 15 cells show a 12px `#6b7280` "0".
- **Right panel title (bold 13px `#1a5276`, centered near x=530):** "Adjacency list — 10 entries".
- **List rows (13px `#2c3e50`, name in bold `#1a5276`, left-aligned at x=420, y = 95, 125, 155, 185, 215):** "Ana → Ben, Cara" / "Ben → Ana, Cara" / "Cara → Ana, Ben, Dev" / "Dev → Cara, Eli" / "Eli → Dev".
- **Annotation (bold 12px orange `#d95926`, near (420, 255), two lines):** "each friendship appears twice —" / "10 ones here, 10 entries there".
- **Caption (12px `#444`, bottom right):** "illustrative — same five friendships as c1".

## A Town of 1,000 People

**Tags:** `where it's used` (blue), `sparse graphs` (green), `memory` (orange)

- **Scale it up** — 1,000 people means a 1,000×1,000 grid: 1,000,000 cells before a single friendship
- **Real friendships** — say each person has about 10 friends: 5,000 friendships → 10,000 list entries
- **The gap** — 10,000 entries vs 1,000,000 cells: the list is 100× smaller because friends are rare
- **Sparse graphs** — most real networks (roads, web links, follows) are sparse: nearly all pairs unconnected
- **Where it bites** — social feeds, road maps, and web crawls all walk graphs; the matrix wastes RAM on zeros

*Example (italic):* At 1,000 people with about 10 friends each, the matrix is 99% zeros — 990,000 cells that only say "not friends".

**Key point:** For sparse graphs — the usual case in the real world — the adjacency list stores only the real connections and wins on space by orders of magnitude.

### Visualization (canvas `c3`, 720×300)

Two-bar comparison for the 1,000-person town: matrix cells vs list entries, making the 100× storage gap visible.

- **Title (bold 15px, `#1a5276`, top center):** "1,000 People, ~10 Friends Each: Cells vs Entries".
- **Axes:** baseline 2px `#999` line at y=245 from x=100 to x=660; no y-axis scale (values labeled on the bars instead).
- **Bar 1 (matrix):** rectangle x=180, width=140, from y=65 down to the baseline (height 180); fill `rgba(217,89,38,0.35)`, 2px `#d95926` border; bold 13px `#d95926` value label "1,000,000 cells" centered above the bar; 12px `#444` label "adjacency matrix" centered below the baseline.
- **Bar 2 (list):** rectangle x=440, width=140, from y=237 down to the baseline (height 8 — minimum visible); fill `rgba(0,131,0,0.35)`, 2px `#008300` border; bold 13px `#008300` value label "10,000 entries" centered above the bar; 12px `#444` label "adjacency list" centered below the baseline.
- **Annotation (bold 13px green `#008300`, near (400, 130), two lines):** "the list is 100× smaller —" / "990,000 matrix cells are zeros".
- **Caption (12px `#444`, bottom right):** "illustrative — bar heights not to scale; true ratio is 100:1".

## So Is the List Always Better?

**Tags:** `common mistake` (red), `lookup speed` (orange)

- **The confusion** — after the space story, people conclude the matrix is obsolete; it is not
- **Yes/no in one look** — "is Ana friends with Dev?": the matrix checks 1 cell; the list scans Ana's 2 entries
- **Listing friends** — "who are Cara's friends?": the matrix scans her full row of 5; the list reads 3 entries
- **Dense graphs** — when most pairs ARE connected, the zeros vanish and the grid stops being wasteful
- **Matrix math** — squaring the matrix counts two-step paths (friends of friends) with plain arithmetic

*Example (italic):* "Is Ana friends with Dev?" — the grid answers in one cell look (row Ana, column Dev: 0), while Ana's list makes you scan both of her entries first.

**Common mistake:** Treating the adjacency list as the universally right answer. Dense graphs, constant-time edge checks, and matrix arithmetic all favor the grid — choose by how the graph will be used, not by habit.

### Visualization (canvas `c4`, 720×300)

Grouped bar chart of hand-countable lookup steps: two everyday questions on the x-axis, and for each, the number of looks the matrix needs vs the list — each notebook wins one.

- **Title (bold 15px, `#1a5276`, top center):** "Steps to Answer: Each Notebook Wins One Game".
- **Axes:** origin x=80, baseline y=245, plot width 560, plot height 175; y = steps 0 to 6 with light `#e5e9ef` gridlines and 12px `#444` tick labels at 0, 2, 4, 6.
- **Group 1 (centered near x=230), x-label 12px `#444` below the baseline:** "Is Ana friends with Dev?"; matrix bar (fill `rgba(217,89,38,0.35)`, 2px `#d95926` border, width 60) height = 1 step; list bar (fill `rgba(0,131,0,0.35)`, 2px `#008300` border, width 60) height = 2 steps; bold 13px value labels "1" and "2" above the bars.
- **Group 2 (centered near x=500), x-label:** "Who are Cara's friends?"; matrix bar height = 5 steps, list bar height = 3 steps; bold 13px value labels "5" and "3" above the bars.
- **Legend (12px, top right, y≈70):** orange swatch "matrix", green swatch "list".
- **Annotation (bold 12px violet `#4a3aa7`, near (300, 95), two lines):** "matrix: yes/no in one look —" / "list: reads friends with no zeros".
- **Caption (12px `#444`, bottom right):** "step counts from the 5-friend example in c1".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** every node position, filled-cell coordinate, list entry, bar value, and step count is hardcoded exactly as specified above (no randomness); the friend graph is the fixed 5-node / 5-edge set (Ana–Ben, Ana–Cara, Ben–Cara, Cara–Dev, Dev–Eli) reused across all four charts, and the town-of-1,000 numbers (1,000,000 cells / 10,000 entries / 990,000 zeros) match the section text.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
