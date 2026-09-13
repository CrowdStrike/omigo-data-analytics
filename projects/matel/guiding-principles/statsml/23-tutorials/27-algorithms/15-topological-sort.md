# Topological Sort

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Topological Sort

**Subtitle:** When tasks depend on each other, topological sort finds an order that never breaks a "must come before" arrow — the same trick a build tool, a spreadsheet, or a pipeline scheduler runs before doing anything

## Seven Brunch Tasks and the Arrows Between Them

**Tags:** `core idea` (blue), `dependencies` (green), `DAG` (orange)

- **The brunch** — seven Saturday tasks: shop, heat pan, set table, mix batter, brew coffee, fry, serve
- **The arrows** — some tasks must wait: you can't mix batter before shopping, or fry before mixing
- **The graph** — draw each task as a box and each "must come before" as an arrow: that's a DAG
- **The question** — you do one task at a time; which orders never put a task before its arrows?
- **The answer** — any such order is a topological sort: every arrow points forward in the list
- **No cycles** — it only works because no task waits on itself through a loop of arrows

*Example (italic):* Shop, heat pan, set table, mix batter, brew coffee, fry, serve — walk that list and every arrow lands on something later, so nothing is attempted too early.

**Key point:** A topological sort is a line-up of dependent tasks where every "must come before" arrow points forward — a legal to-do order for a dependency graph.

### Visualization (canvas `c1`, 720×300)

Single-panel node-and-arrow diagram: the seven brunch tasks as rounded boxes arranged left to right in four dependency layers, with the seven prerequisite arrows drawn between them.

- **Title (bold 15px, `#1a5276`, top center):** "The Brunch DAG: an Arrow Means 'Must Finish First'".
- **Nodes:** rounded rectangles 110×32 (6px radius), fill `rgba(42,120,214,0.12)`, 2px `#2a78d6` border, bold 12px `#1a5276` centered labels; centers at — "Shop" (110, 95), "Pan" (110, 170), "Table" (110, 245), "Mix" (300, 95), "Coffee" (300, 195), "Fry" (470, 130), "Serve" (630, 170).
- **Arrows:** 2px `#6b7280` lines with small solid arrowheads, box edge to box edge: Shop→Mix, Shop→Coffee, Pan→Fry, Mix→Fry, Coffee→Serve, Fry→Serve, Table→Serve.
- **Layer labels (11px `#6b7280`, y=48, above each column):** "no waiting", "needs shopping", "needs two", "needs three".
- **Annotation (bold 12px orange `#d95926`, two lines near x=430, y=250):** "no loops anywhere —" / "so a legal order must exist".
- **Caption (12px `#444`, bottom right):** "illustrative — one Saturday's brunch plan".

## Peeling Off Tasks With Zero Prerequisites

**Tags:** `worked example` (blue), `Kahn's algorithm` (green)

- **Count arrows in** — per task, count unfinished prerequisites: Shop 0, Pan 0, Table 0, Mix 1, Coffee 1, Fry 2, Serve 3
- **Take a zero** — any task with count 0 is safe to do now; start by doing Shop
- **Subtract** — finishing Shop drops Mix 1→0 and Coffee 1→0; two new tasks become safe
- **Repeat** — keep taking a zero and subtracting: Pan, Table, Mix, Coffee, then Fry drops Serve to 0
- **The output** — the pick order Shop, Pan, Table, Mix, Coffee, Fry, Serve is a topological sort
- **Stuck early?** — if no task has count 0 but tasks remain, the graph has a cycle: no order exists

*Example (italic):* Fry starts at 2 (waiting on Mix and Pan); after Pan it holds 1, after Mix it hits 0 and gets picked — you can redo the whole table by hand in a minute.

**Key point:** Kahn's algorithm is just bookkeeping: repeatedly do any task with 0 prerequisites left and subtract 1 from everything it was blocking.

### Visualization (canvas `c2`, 720×300)

Single-panel count grid: seven task rows by eight step columns, each cell showing the prerequisites still unfinished before that step, with the picked task's cell marked each round.

- **Title (bold 15px, `#1a5276`, top center):** "Prerequisite Counts, Step by Step — Pick a Zero, Subtract One".
- **Grid:** rows at y = 80, 105, 130, 155, 180, 205, 230 for tasks "Shop", "Pan", "Table", "Mix", "Coffee", "Fry", "Serve" (12px `#444` labels, right-aligned at x=115); columns centered at x = 165, 235, 305, 375, 445, 515, 585, 655 with 12px `#444` headers at y=60: "start", "1", "2", "3", "4", "5", "6", "7".
- **Cell values (12px, centered), column by column:** start `[0, 0, 0, 1, 1, 2, 3]`; after step 1 (Shop picked) `[✓, 0, 0, 0, 0, 2, 3]`; after 2 (Pan) `[✓, ✓, 0, 0, 0, 1, 3]`; after 3 (Table) `[✓, ✓, ✓, 0, 0, 1, 2]`; after 4 (Mix) `[✓, ✓, ✓, ✓, 0, 0, 2]`; after 5 (Coffee) `[✓, ✓, ✓, ✓, ✓, 0, 1]`; after 6 (Fry) `[✓, ✓, ✓, ✓, ✓, ✓, 0]`; after 7 (Serve) all `✓`.
- **Styling:** zeros bold blue `#2a78d6`; nonzero counts `#6b7280`; the cell picked that step drawn as a 22px circle filled `rgba(0,131,0,0.18)` with a bold green `#008300` check; light `#e5e9ef` gridlines between rows.
- **Pick order strip (bold 12px green `#008300`, y=262, centered):** "picked: Shop, Pan, Table, Mix, Coffee, Fry, Serve".
- **Annotation (bold 12px orange `#d95926`, near x=590, two lines at y=95 and y=122):** "Fry: 2 → 1 → 0" / "then it's safe".
- **Caption (11px `#444`, bottom right):** "counts = unfinished prerequisites, illustrative".

## Builds, Spreadsheets, and Pipeline Schedulers

**Tags:** `where it's used` (blue), `recalculation` (green), `pipelines` (orange)

- **Build tools** — a compiler must build a file's dependencies first; the build order is a topo sort
- **Spreadsheets** — B1 uses A1, C1 uses B1: the sheet topo-sorts cells before recalculating any
- **Pipeline DAGs** — schedulers run load before clean before train; the run plan is a topo sort
- **Package installs** — installing a library first installs everything it imports, in sorted order
- **Without it** — run steps in file order and a report can read a table that hasn't been built yet

*Example (italic):* A sheet holds A1 = 5, B1 = A1×4 = 20, C1 = B1×12 = 240, D1 = C1+60 = 300; change A1 to 6 and it recomputes B1 = 24, C1 = 288, D1 = 348 — always in dependency order.

**Key point:** Every "figure out what to run first" system — builds, spreadsheet recalc, DAG pipelines — has a topological sort at its core.

### Visualization (canvas `c3`, 720×300)

Single-panel spreadsheet chain: four formula cells drawn as boxes with dependency arrows, shown twice — old values on top, recalculated values after editing A1 below — with the recalculation order called out.

- **Title (bold 15px, `#1a5276`, top center):** "A Spreadsheet Recalculates in Topological Order".
- **Cell boxes (both rows):** four rectangles 130×52 (6px radius) centered at x = 130, 290, 450, 610; 2px `#2a78d6` border, fill `rgba(42,120,214,0.10)`; inside each: bold 12px `#1a5276` cell name + formula on line one, 13px `#2c3e50` value on line two.
- **Row 1 (boxes centered y=105), labeled 12px `#6b7280` "before, A1 = 5" at x=20, y=70:** "A1 = 5" value "5"; "B1 = A1×4" value "20"; "C1 = B1×12" value "240"; "D1 = C1+60" value "300".
- **Row 2 (boxes centered y=215), labeled 12px `#6b7280` "after editing A1 to 6" at x=20, y=180:** "A1 = 6" value bold green `#008300` "6"; "B1 = A1×4" value green "24"; "C1 = B1×12" value green "288"; "D1 = C1+60" value green "348".
- **Arrows:** 2px `#6b7280` arrows between neighboring boxes in each row (A1→B1→C1→D1); numbered 11px `#6b7280` badges "1", "2", "3" above row 2's arrows for the recalc order.
- **Annotation (bold 12px orange `#d95926`, centered near x=360, y=160):** "one edit, three recomputes — never a stale value read too early".
- **Caption (11px `#444`, bottom right):** "illustrative — a coffee-budget sheet".

## Many Right Answers, and the One Deal-Breaker

**Tags:** `common mistake` (red), `not unique` (orange), `cycles` (green)

- **Not unique** — the brunch has many legal orders; Shop, Mix, Pan, Fry, Table, Coffee, Serve also works
- **Ties are free** — Shop, Pan, and Table all start at 0 prerequisites; any of the three may go first
- **The test** — an order is valid exactly when no arrow points backward in the list, nothing more
- **The deal-breaker** — a cycle (A waits on B, B waits on A) means no valid order exists at all
- **Seen in the wild** — a spreadsheet reports it as "circular reference"; a build tool refuses to start

*Example (italic):* Table, Shop, Coffee, Pan, Mix, Fry, Serve is just as correct as the worked example's order — but Shop, Pan, Fry, Mix, Table, Coffee, Serve fries before mixing and is illegal.

**Common mistake:** Expecting "the" topological order. A DAG usually has many valid orders and any one is correct — the only unanswerable input is a graph with a cycle.

### Visualization (canvas `c4`, 720×300)

Single-panel order gallery: four candidate brunch orders drawn as rows of seven labeled chips, three valid and one broken, with the backward arrow that disqualifies the last row drawn in red.

- **Title (bold 15px, `#1a5276`, top center):** "Three Legal Orders, One Illegal One".
- **Rows (chip centers at y = 85, 130, 175, 235), each with a 12px label at x=20:** labels "valid" (green `#008300`) for rows 1–3, "illegal" (bold red `#e74c3c`) for row 4.
- **Chips:** seven rounded rectangles 78×28 (6px radius) per row, centered at x = 145, 225, 305, 385, 465, 545, 625; 12px centered labels; valid chips 1.5px `#27ae60` border with fill `rgba(39,174,96,0.10)`; row 4 chips 1.5px `#6b7280` border, except the offending "Fry" chip filled `rgba(231,76,60,0.15)` with 2px `#e74c3c` border and bold red label.
- **Row contents:** row 1 `["Shop", "Pan", "Table", "Mix", "Coffee", "Fry", "Serve"]`; row 2 `["Shop", "Mix", "Pan", "Fry", "Table", "Coffee", "Serve"]`; row 3 `["Table", "Shop", "Coffee", "Pan", "Mix", "Fry", "Serve"]`; row 4 `["Shop", "Pan", "Fry", "Mix", "Table", "Coffee", "Serve"]`.
- **Backward arrow:** curved 2px red `#e74c3c` arrow above row 4 from the "Mix" chip (x=385) back to the "Fry" chip (x=305), showing the Mix→Fry prerequisite pointing backward.
- **Annotation (bold 12px red `#e74c3c`, near x=470, y=210):** "fried before mixing — one backward arrow sinks the whole order".
- **Caption (11px `#444`, bottom right):** "any arrow pointing left = not a topological sort".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all node positions, prerequisite counts, cell values, and chip orders are the hardcoded literals above (no randomness); the c2 count columns must be exactly the arrays listed, and the spreadsheet values 5/20/240/300 and 6/24/288/348 must match the section text. Draw arrowheads as small filled triangles; red is used only for the illegal order and the backward arrow.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
