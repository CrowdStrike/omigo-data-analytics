# Trees

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Trees

**Subtitle:** A tree stores items as a hierarchy of branch points — start at one root, ask "left or right?" at each step, and a well-balanced tree finds anything in a handful of questions

## Fifteen Books, One Question at a Time

**Tags:** `core idea` (blue), `hierarchy` (green), `root and leaves` (orange)

- **The shelf problem** — a librarian keeps 15 books by 15 authors and wants to find any one fast
- **One root** — she starts at the middle author, Hall; earlier names branch left, later names branch right
- **Parents and children** — every name points down to at most two names: a left child and a right child
- **Leaves** — the bottom names with no children (Adams, Chen, Evans, ..., Ortiz) are the leaves
- **Four levels** — 15 names stack into just 4 levels: 1 root, then 2, then 4, then 8 names

*Example (italic):* To find Ortiz she asks "before or after Hall?" then "before or after Lopez?" then Nolan — four questions and the book is in her hand.

**Key point:** A tree is data arranged as a hierarchy: one root on top, each item pointing to children — at most two in this page's binary search tree — and every lookup is a short walk of left/right questions.

### Visualization (canvas `c1`, 720×300)

Node-and-edge diagram of the full 15-author tree across 4 levels, with the 4-step search path to Ortiz highlighted in green.

- **Title (bold 15px, `#1a5276`, top center):** "The Librarian's Tree: 15 Authors, 4 Levels".
- **Node positions (center x, y), levels top to bottom:** level 1: Hall (360, 60); level 2: Diaz (180, 118), Lopez (540, 118); level 3: Baker (90, 176), Flores (270, 176), Jones (450, 176), Nolan (630, 176); level 4: Adams (45, 234), Chen (135, 234), Evans (225, 234), Gupta (315, 234), Ito (405, 234), Khan (495, 234), Mori (585, 234), Ortiz (675, 234).
- **Edges (drawn first):** 1.5px `#6b7280` straight lines between parent and child centers: Hall→Diaz, Hall→Lopez, Diaz→Baker, Diaz→Flores, Lopez→Jones, Lopez→Nolan, Baker→Adams, Baker→Chen, Flores→Evans, Flores→Gupta, Jones→Ito, Jones→Khan, Nolan→Mori, Nolan→Ortiz.
- **Node style:** rounded rect 56×22 centered on each position, fill `rgba(42,120,214,0.12)`, 1.5px `#2a78d6` border, bold 11px `#1a5276` name centered.
- **Search path (Hall, Lopez, Nolan, Ortiz):** fill `rgba(0,131,0,0.15)`, 2px `#008300` border, and a bold 11px `#008300` step number "1"–"4" just above each node's top-right corner; the three edges along the path drawn 2.5px `#008300`.
- **Level labels:** 11px `#6b7280` "level 1" … "level 4" at x=8, vertically aligned with each row.
- **Annotation (bold 12px green `#008300`, near x=70, y=95):** two lines: "any book found in" / "at most 4 questions".
- **Caption (12px `#444`, bottom right):** "illustrative — 15 made-up author names".

## Three Ways to Walk the Same Shelf

**Tags:** `worked example` (blue), `traversals` (green)

- **A traversal** — a fixed rule for visiting every name in the tree exactly once, no skips, no repeats
- **The small tree** — take the 7-name branch under Diaz: Baker and Flores below it, four leaves below them
- **In-order** — left child, then yourself, then right: Adams, Baker, Chen, Diaz, Evans, Flores, Gupta
- **Pre-order** — yourself first, then left, then right: Diaz, Baker, Adams, Chen, Flores, Evans, Gupta
- **Level-order** — read row by row, top to bottom: Diaz, Baker, Flores, Adams, Chen, Evans, Gupta
- **The magic** — in-order on a search tree comes out alphabetical with no sorting step at all

*Example (italic):* Cover the chart and walk the 7-name tree by hand with each rule — in-order really does produce Adams through Gupta in perfect A-to-G order.

**Key point:** Same 7 names, three different visit orders — and in-order reads a search tree in sorted order for free.

### Visualization (canvas `c2`, 720×300)

Left half: the 7-name subtree with orange in-order visit numbers on each node; right half: the three traversal outputs listed as labeled rows.

- **Title (bold 15px, `#1a5276`, top center):** "Three Walks of the Same 7 Names".
- **Tree node positions (center x, y):** Diaz (190, 75); Baker (100, 145), Flores (280, 145); Adams (55, 215), Chen (145, 215), Evans (235, 215), Gupta (325, 215).
- **Edges:** 1.5px `#6b7280` lines Diaz→Baker, Diaz→Flores, Baker→Adams, Baker→Chen, Flores→Evans, Flores→Gupta.
- **Node style:** rounded rect 54×22, fill `rgba(42,120,214,0.12)`, 1.5px `#2a78d6` border, bold 11px `#1a5276` name.
- **In-order badges:** 9px-radius circle filled `#d95926` at each node's top-left corner, bold 11px white number = in-order visit position: Adams 1, Baker 2, Chen 3, Diaz 4, Evans 5, Flores 6, Gupta 7.
- **Output rows (right half, label at x=390, sequence at x=478, y = 100 / 150 / 200):** bold 12px labels "in-order" (`#d95926`), "pre-order" (`#2a78d6`), "level-order" (`#199e70`); next to each, its 7-name sequence in 11px `#444` on two short lines if needed: "Adams, Baker, Chen, Diaz," / "Evans, Flores, Gupta" — then "Diaz, Baker, Adams, Chen," / "Flores, Evans, Gupta" — then "Diaz, Baker, Flores, Adams," / "Chen, Evans, Gupta".
- **Annotation (bold 12px orange `#d95926`, near x=390, y=250):** "in-order comes out alphabetical — A to G, no sort needed".
- **Caption (12px `#444`, bottom right):** "orange badges = in-order visit position".

## When the Tree Grows Sideways

**Tags:** `why it matters` (blue), `balance` (green), `worst case` (red)

- **Balanced** — the 15-name tree splits evenly at every branch, so any search takes at most 4 questions
- **The bad insert** — add the names in alphabetical order and each new name lands right of the last
- **A chain** — the "tree" degenerates into one 15-level diagonal line; finding Ortiz now takes 15 questions
- **It scales badly** — at 1,000 names a balanced tree needs about 10 questions; the chain can need 1,000
- **The fix** — self-balancing trees quietly rotate nodes as data arrives so the shape never goes sideways

*Example (italic):* Same 15 books, two shapes: the balanced tree answers in 4 questions, the chain in up to 15 — the data is identical, only the shape changed.

**Key point:** A tree only halves the search at each step when it is balanced — an unbalanced tree is just a slow list wearing a tree costume.

### Visualization (canvas `c3`, 720×300)

Two side-by-side dot diagrams of the same 15 items: a balanced 4-level tree on the left, a 15-node diagonal chain on the right, with worst-case question counts under each.

- **Title (bold 15px, `#1a5276`, top center):** "Same 15 Books, Two Shapes".
- **Left panel — balanced tree dots (5px radius, `#2a78d6`, edges 1px `#6b7280`):** root (180, 75); level 2 (110, 121), (250, 121); level 3 (75, 167), (145, 167), (215, 167), (285, 167); level 4 eight dots at x = 40, 80, 120, 160, 200, 240, 280, 320, y = 213; edges connect each parent to its two children.
- **Right panel — chain dots (5px radius, `#e74c3c`, 1.5px `#e74c3c` connecting segments):** 15 dots starting at (430, 68), each next dot at (+16, +12), ending at (654, 236).
- **Panel labels (bold 13px, centered under each panel at y=262):** left "balanced — worst case 4 questions" in `#008300`; right "alphabetical inserts — 15 questions" in `#e74c3c`.
- **Annotation (bold 12px `#1a5276`, near x=360, y=45, centered):** "at 1,000 books: 10 questions vs 1,000".
- **Caption (12px `#444`, bottom right):** "illustrative — each dot is one book".

## Twenty Levels Hold a Million Books

**Tags:** `common mistake` (red), `height vs size` (orange)

- **The confusion** — people hear "a million records" and picture a million-step search
- **Doubling** — each level holds twice the one above: 1, 2, 4, 8 ... so capacity explodes downward
- **Ten levels** — a balanced tree of 10 levels already holds 1,023 names; our 15 books used only 4
- **Twenty levels** — 20 levels hold 1,048,575 names: about a million books, still only 20 questions
- **Height vs size** — search cost in a balanced tree grows with the level count, not the item count

*Example (italic):* Doubling a single shelf 20 times blows past a million books, yet a lookup still walks just one path of 20 branch points from root to leaf.

**Common mistake:** Judging search cost by how many items there are instead of how many levels there are — in a balanced tree a thousand-fold more data adds only about 10 more questions.

### Visualization (canvas `c4`, 720×300)

Horizontal bar chart of tree capacity at 5, 10, 15, and 20 levels, with bar length proportional to level count (a log scale for the book counts) and exact capacities labeled.

- **Title (bold 15px, `#1a5276`, top center):** "How Many Books Fit in N Levels".
- **Rows (bar top-left y = 79, 124, 169, 214; bar height 22; bars start at x=150):** levels `[5, 10, 15, 20]`, capacities `[31, 1023, 32767, 1048575]`, bar widths `[125, 250, 375, 500]` px (25px per level).
- **Bar style:** fill `rgba(42,120,214,0.35)`, 1.5px `#2a78d6` border; row labels 12px `#444` at x=20, vertically centered on each bar: "5 levels", "10 levels", "15 levels", "20 levels".
- **Value labels (bold 12px `#1a5276`, 8px right of each bar end):** "31", "1,023", "32,767", "1,048,575".
- **Scale note (11px `#6b7280`, x=150, y=258):** "bar length = number of levels; book counts grow far faster".
- **Annotation (bold 13px green `#008300`, near x=440, y=100):** "every 5 extra levels ≈ 32× more books".
- **Caption (12px `#444`, bottom right):** "capacities are exact: 2^levels − 1".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`. Red `#e74c3c` appears only for the degenerate chain (genuine failure state).
- **Data:** all node positions, names, visit orders, dot coordinates, and capacities are the hardcoded literals above (no randomness); the 15 author names are invented ("illustrative"), the traversal orders and capacities 2^levels − 1 are exact and must match the text bullets (4 questions, 15 questions, 1,023, 1,048,575).
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
