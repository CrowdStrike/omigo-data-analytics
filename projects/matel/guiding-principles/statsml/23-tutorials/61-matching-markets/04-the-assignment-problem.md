# The Assignment Problem

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** The Assignment Problem

**Subtitle:** When preferences become numbers, matching becomes picking one cell per row and column with the best total

**Running data (illustrative), used identically in every section — minutes for each driver to reach each zone:**

|       | Z1 | Z2 | Z3 | Z4 |
|-------|----|----|----|----|
| Alice | 1  | 2  | 6  | 5  |
| Bob   | 2  | 9  | 7  | 6  |
| Carol | 8  | 6  | 3  | 4  |
| Dan   | 9  | 7  | 4  | 3  |

Greedy assignment: Alice–Z1 (1), Carol–Z3 (3), Dan–Z4 (3), Bob forced into Z2 (9) — total 16.
Optimal assignment: Alice–Z2 (2), Bob–Z1 (2), Carol–Z3 (3), Dan–Z4 (3) — total 10.

## One Number Per Cell

**Tags:** `core idea` (blue), `cost matrix` (green)

- **The setup** — four delivery drivers (Alice, Bob, Carol, Dan) and four zones (Z1–Z4) to cover
- **The cell** — each cell holds one number: minutes for that driver to reach that zone
- **The collapse** — no preference lists here; both sides' feelings shrink into one cost per pair
- **The task** — give each driver exactly one zone, and each zone exactly one driver
- **The goal** — pick four cells, one per row and one per column, with the smallest total minutes

*Example (italic):* Alice reaches Z1 in 1 minute but Z3 in 6 — the matrix stores who is close to what.

**Key point:** An assignment is one cell per row and one per column; the assignment problem asks for the cheapest such pick out of all possible combinations.

### Visualization (canvas `c1`, 720×300)

The 4×4 cost matrix drawn as a grid with values, optimal cells circled green, side note with the optimal total.

- **Title (bold 15px, `#1a5276`, top center):** "Four Drivers, Four Zones — Minutes to Reach (illustrative)".
- **Grid:** origin (180, 64), cell 46×46, 1px `#e5e9ef` cell borders; values 13px `#2c3e50` centered in each cell; matrix rows Alice `[1,2,6,5]`, Bob `[2,9,7,6]`, Carol `[8,6,3,4]`, Dan `[9,7,4,3]`.
- **Column headers:** "Z1 Z2 Z3 Z4" bold 12px `#6b7280`, centered above each column at grid top − 10.
- **Row labels:** "Alice Bob Carol Dan" bold 12px `#2c3e50`, right-aligned at x=168, vertically centered per row.
- **Optimal cells circled:** green `#008300` circles (radius 16, 2.5px stroke) on Alice–Z2, Bob–Z1, Carol–Z3, Dan–Z4; those four values drawn bold 13px green instead of plain text color.
- **Side annotation (centered at x=545):** bold 13px green "optimal pick:" at y=120, 12px `#2c3e50` "one cell per row," at y=142 and "one per column" at y=160, bold 13px green "total 2+2+3+3 = 10" at y=188.
- **Caption (12px `#6b7280`, centered at y=284):** "an assignment picks exactly one cell in every row and every column".

## Greedy Grabs 16, the Optimum Costs 10

**Tags:** `worked example` (blue), `greedy trap` (orange)

- **Greedy** — grab the globally cheapest cell first: Alice–Z1 costs 1, the smallest number anywhere
- **Next picks** — Carol–Z3 (3) and Dan–Z4 (3) are the cheapest cells still open
- **The squeeze** — only Z2 remains for Bob, and Bob–Z2 costs 9: greedy total 1+3+3+9 = 16
- **Optimal** — Alice–Z2 (2), Bob–Z1 (2), Carol–Z3 (3), Dan–Z4 (3): total 10
- **The twist** — Alice gives up her personal best (1 → 2), and that one move saves Bob 7 minutes

*Example (italic):* The move that looks worst locally — Alice off her 1-minute zone — is exactly what wins globally.

**Key point:** Local best ≠ global best. Greedy locks in the flashiest cell and pays for it later; the optimum trades a 1-minute loss for a 7-minute gain.

### Visualization (canvas `c2`, 720×300)

Two side-by-side 4×4 matrices: greedy picks circled orange totaling 16, optimal picks circled green totaling 10, center annotation.

- **Title (bold 15px, `#1a5276`, top center):** "Greedy 16 vs Optimal 10 (illustrative)".
- **Both grids:** cell 38×38, 1px `#e5e9ef` borders, values 12px `#2c3e50`; left grid origin (70, 78), right grid origin (430, 78); column headers "Z1–Z4" 11px `#6b7280` at grid top − 8; row labels "Alice Bob Carol Dan" 11px `#2c3e50` right-aligned at origin x − 8.
- **Left panel header (bold 13px orange `#d95926`, centered at (146, 58)):** "GREEDY — total 16".
- **Left circles:** orange `#d95926` circles (radius 13, 2.5px) on Alice–Z1 (1), Bob–Z2 (9), Carol–Z3 (3), Dan–Z4 (3); those values bold orange.
- **Right panel header (bold 13px green `#008300`, centered at (506, 58)):** "OPTIMAL — total 10".
- **Right circles:** green `#008300` circles (same size) on Alice–Z2 (2), Bob–Z1 (2), Carol–Z3 (3), Dan–Z4 (3); those values bold green.
- **Center annotation (bold 13px magenta `#d55181`, centered at (360, 155), two lines):** "local best" / "≠ global best" (second line at y=173).
- **Under-grid notes (11px `#6b7280`, centered at y=248):** left at x=146 "grabs 1, 3, 3 … Bob is forced into 9"; right at x=506 "Alice takes 2, not 1 — Bob gets 2, not 9".
- **Caption (12px `#6b7280`, centered at y=284):** "greedy's first grab (Alice–Z1) is the very move that forces Bob into the 9-minute zone".

## The Hungarian Idea: Subtract Until Zeros Appear

**Tags:** `core idea` (blue), `Hungarian method` (green)

- **The trick** — subtract a constant from any row or column: every assignment's total drops equally
- **The consequence** — all totals shift together, so which assignment is best never moves
- **Row step** — subtract each row's minimum (1, 2, 3, 3): every row now contains a zero
- **Column step** — Z2 still has no zero, so subtract its column minimum (1) as well
- **Read it off** — zeros at Alice–Z2, Bob–Z1, Carol–Z3, Dan–Z4 form a full assignment: it is optimal
- **The receipt** — the subtractions sum to 1+2+3+3+1 = 10, exactly the optimal total

*Example (italic):* The full method runs in about n³ steps — unlike most combinatorial problems, this one is genuinely easy for computers.

**Key point:** Row and column subtractions never change which assignment wins; the moment a full assignment sits on zeros alone, it must be the optimal one.

### Visualization (canvas `c3`, 720×300)

Before/after matrices: the original with row minima marked, an arrow with the subtraction steps, and the fully reduced matrix with zeros highlighted and the zero-assignment circled.

- **Title (bold 15px, `#1a5276`, top center):** "Subtract Rows and Columns — Zeros Reveal the Answer (illustrative)".
- **Both grids:** cell 38×38, same style as c2; left grid origin (70, 78), right grid origin (430, 78); column headers 11px mute at top − 8, row labels 11px right-aligned at origin x − 8.
- **Left panel header (bold 13px blue `#2a78d6`, centered at (146, 58)):** "ORIGINAL".
- **Left grid values:** the running cost matrix; each row's minimum cell (Alice–Z1, Bob–Z1, Carol–Z3, Dan–Z4) drawn bold orange `#d95926`; row-minimum notes bold 12px orange "−1 −2 −3 −3" at x=238, one per row at the row's vertical center.
- **Right panel header (bold 13px green `#008300`, centered at (506, 58)):** "REDUCED — assign on zeros".
- **Right grid values:** rows Alice `[0,0,5,4]`, Bob `[0,6,5,4]`, Carol `[5,2,0,1]`, Dan `[6,3,1,0]` (after row minima and −1 from column Z2); every zero cell filled `rgba(25,158,112,0.16)`; assignment zeros Alice–Z2, Bob–Z1, Carol–Z3, Dan–Z4 circled green `#008300` (radius 13, 2.5px) with bold green values.
- **Center arrow:** 2px `#6b7280` line from (300, 155) to (398, 155) with filled arrowhead; labels bold 12px orange centered at (350, 135) "rows −1,−2,−3,−3" and at (350, 177) "then col Z2 −1".
- **Caption (12px `#6b7280`, centered at y=284):** "the zero-assignment is Alice–Z2, Bob–Z1, Carol–Z3, Dan–Z4 — the same optimal total of 10".

## Where One Matrix Runs the World

**Tags:** `where it's used` (blue), `dispatch & tracking` (green)

- **Couriers** — dispatch systems assign drivers to waiting orders by minimizing pickup minutes
- **Servers** — load balancers assign incoming requests to machines using per-pair latency costs
- **Video tracking** — matching detected objects to existing tracks between frames is an assignment
- **Peer review** — pairing reviewers to papers on expertise scores is the exact same matrix
- **Flip the sign** — with scores instead of costs, maximize instead of minimize — same problem

*Example (italic):* A multi-object tracker solves a fresh assignment problem on every video frame, dozens of times per second.

**Key point:** Whenever every pairing has a number and everyone needs exactly one partner, it is the assignment problem — and it is solvable fast, in polynomial time.

### Visualization (canvas `c4`, 720×300)

A four-row flow diagram: "rows × columns" box → arrow → "the number in each cell" box, one row per application.

- **Title (bold 15px, `#1a5276`, top center):** "One Formulation, Many Dispatchers".
- **Column headers (bold 12px, centered at y=52):** "rows × columns" in blue `#2a78d6` at x=200, "the number in each cell" in green `#008300` at x=540.
- **Rows (y = 64, 116, 168, 220; box height 40):** left boxes at x=60 width 280, right boxes at x=400 width 290; fill `#fbfcfd`; left border 2px blue, right border 2px green; centered 12px `#2c3e50` text:
  - "couriers × orders" → "cost: minutes to pickup"
  - "requests × servers" → "cost: latency per pair"
  - "detections × tracks (video)" → "cost: distance between frames"
  - "reviewers × papers" → "score: expertise — maximize"
- **Arrows:** 1.5px mute `#6b7280` from (348, row mid) to (392, row mid) with filled arrowhead per row.
- **Caption (bold 12px violet `#4a3aa7`, centered at y=284):** "solved exactly in about n³ steps — polynomial time, fast even when n is large".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale(...)`; charts stored in an array, redrawn on debounced window resize; shared `arrowHead(ctx, x, y, dir, color)` helper for filled right-pointing arrowheads; shared `drawGrid` helper for the 4×4 matrices (cell borders, values, headers, row labels).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`.
- **Data integrity:** hardcoded arrays only — cost matrix `[[1,2,6,5],[2,9,7,6],[8,6,3,4],[9,7,4,3]]`, reduced matrix `[[0,0,5,4],[0,6,5,4],[5,2,0,1],[6,3,1,0]]`, greedy picks `(0,0),(1,1),(2,2),(3,3)` total 16, optimal picks `(0,1),(1,0),(2,2),(3,3)` total 10; invented numbers carry "(illustrative)" in chart titles.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
