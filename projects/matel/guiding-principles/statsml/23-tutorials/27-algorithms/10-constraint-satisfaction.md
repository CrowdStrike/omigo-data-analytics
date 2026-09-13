# Constraint Satisfaction

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Constraint Satisfaction

**Subtitle:** Solving sudoku is not about being clever with numbers — it is crossing off what the rules forbid until only one option survives, and guessing only when crossing-off stalls

## One Sudoku Cell, Solved by Crossing Off

**Tags:** `core idea` (blue), `variables & domains` (green), `constraints` (orange)

- **The puzzle** — a 4×4 mini-sudoku with 7 given digits; every row, column, and 2×2 box needs 1–4
- **One cell** — take row 2, column 4: before any cleverness, its possible values are {1, 2, 3, 4}
- **Row rule** — row 2 already holds a 4 and a 1, so cross off 1 and 4 from the cell's list
- **Column rule** — column 4 already holds a 4 and a 3, so cross off 3 (the 4 is already gone)
- **One survivor** — only 2 remains, so the cell must be 2; no arithmetic happened, only elimination

*Example (italic):* The cell at row 2, column 4 starts with four candidates; the row kills 1 and 4, the column kills 3, and 2 is the last one standing.

**Key point:** This is a constraint satisfaction problem: variables (the empty cells), domains (the candidate lists), and constraints (no repeats in a row, column, or box). Solving means shrinking domains until each holds one value.

### Visualization (canvas `c1`, 720×300)

Dual-panel: the 4×4 puzzle grid with the target cell highlighted (left), and the target cell's candidate list being crossed off (right), split by a vertical dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "One Cell of a 4×4 Sudoku: Candidates {1,2,3,4} Reduced to One".
- **Puzzle data (row, col, value), 1-indexed:** givens `[[1,1,1],[1,4,4],[2,2,4],[2,3,1],[3,1,2],[3,4,3],[4,3,2]]`; all other 9 cells empty.
- **Left panel (grid):** 4×4 grid of 52px cells, top-left corner at x=70, y=55; thin 1px `#e5e9ef` cell borders, 2.5px ink `#1a5276` outer border and box borders (after columns 2 and rows 2); given digits bold 20px ink `#1a5276` centered in cells; row 2 and column 4 cells shaded `rgba(42,120,214,0.10)`; target cell (row 2, col 4) filled `rgba(201,133,0,0.25)` with a 2px yellow `#c98500` border and a bold 16px yellow "?" centered; caption 12px `#444` below the grid: "7 givens, 9 empty cells".
- **Right panel (elimination):** heading bold 13px `#2c3e50` at x=400, y=70: "candidates for the ? cell"; four 46px boxes in a row starting x=400, y=95, containing bold 20px digits 1, 2, 3, 4; digit 1 struck through with a 3px blue `#2a78d6` diagonal and labeled below in bold 12px blue "in row 2"; digit 3 struck with a 3px orange `#d95926` diagonal labeled "in col 4"; digit 4 struck with a 3px blue `#2a78d6` diagonal labeled "in row 2"; digit 2 circled with a 3px green `#008300` ring and labeled below in bold 13px green "only survivor".
- **Takeaway (bold 13px green `#008300`, x=400, y=245, two lines):** "one candidate left" / "→ the cell must be 2".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.

## Propagation: Let the Grid Fill Itself

**Tags:** `worked example` (blue), `propagation` (green)

- **The sweep** — visit every empty cell, cross off values already in its row, column, and box
- **Pass 1** — six of the nine empty cells drop to a single candidate and get filled at once
- **The ripple** — each fill shrinks its neighbours' lists; that is why it is called propagation
- **Pass 2** — the three cells that had two candidates each now drop to one, finishing the grid
- **No guessing** — this puzzle needs 0 guesses; two sweeps of pure elimination solve all 9 cells

*Example (italic):* The cell at row 1, column 2 keeps {2, 3} in pass 1, but once row 1 gains a 3 in pass 1, pass 2 leaves it only the 2.

**Key point:** Propagation is deduction done in bulk: every placement immediately tightens the choices everywhere it can see. Easy sudokus — and many real scheduling problems — collapse under propagation alone.

### Visualization (canvas `c2`, 720×300)

Three mini-grids left to right showing the same puzzle at start, after pass 1, and after pass 2, with arrows between them and fills color-coded by pass.

- **Title (bold 15px, `#1a5276`, top center):** "Two Propagation Passes Fill All 9 Empty Cells".
- **Grid data:** givens as in c1: `[[1,1,1],[1,4,4],[2,2,4],[2,3,1],[3,1,2],[3,4,3],[4,3,2]]`; pass-1 fills `[[1,3,3],[2,1,3],[2,4,2],[3,2,1],[3,3,4],[4,4,1]]`; pass-2 fills `[[1,2,2],[4,1,4],[4,2,3]]`.
- **Layout:** three 4×4 grids of 38px cells with top-left corners at x=45, x=285, x=525, all at y=70; each grid drawn with 1px `#e5e9ef` inner lines and 2px ink `#1a5276` outer/box borders; givens bold 16px ink `#1a5276` in every grid.
- **Grid 1 (start):** only the 7 givens; label bold 12px `#444` below: "start: 7 givens".
- **Grid 2 (after pass 1):** givens plus the 6 pass-1 fills in bold 16px green `#008300` on `rgba(0,131,0,0.10)` cells; label bold 12px green: "pass 1: +6 cells".
- **Grid 3 (after pass 2):** everything, with pass-2 fills in bold 16px orange `#d95926` on `rgba(217,89,38,0.12)` cells; label bold 12px orange: "pass 2: +3 cells — solved".
- **Arrows:** 2.5px `#6b7280` arrows between grids at y=145 (x 205→275 and x 445→515), each with bold 12px `#6b7280` label "sweep" above.
- **Takeaway (bold 13px green `#008300`, bottom center y=285):** "9 cells filled by elimination alone — 0 guesses".

## When Elimination Stalls: Guess, Then Backtrack

**Tags:** `worked example` (blue), `backtracking` (orange), `search tree` (green)

- **The stall** — on hard puzzles a sweep can end with every empty cell still holding 2+ candidates
- **The guess** — pick a small cell and try a value: row 1, column 2 holds {2, 3} at the start
- **Try 3 first** — placing 3 leaves row 1, column 3 with no legal candidate; the branch is dead
- **Backtrack** — undo the 3 completely, return to the choice point, and try the other value
- **Try 2** — placing 2 propagates cleanly and the remaining 8 cells fill themselves; solved

*Example (italic):* One wrong guess at row 1, column 2 is exposed after a single deduction step — the neighbouring cell's candidate list goes empty.

**Key point:** Backtracking is guess–propagate–check with a perfect undo. A contradiction is good news: it proves the guessed value wrong, which is itself a deduction about the puzzle.

### Visualization (canvas `c3`, 720×300)

A small search tree: one choice point with two branches, the left branch dying in a contradiction and the right branch reaching the solution, with a dashed "undo" arrow.

- **Title (bold 15px, `#1a5276`, top center):** "Backtracking Search Tree for the Cell r1c2 ∈ {2, 3}".
- **Root node:** rounded rect centered at (360, 80), ~250×44, fill `rgba(26,82,118,0.10)`, 2px ink `#1a5276` border; bold 13px ink text, two lines: "choice point" / "r1c2 could be 2 or 3".
- **Left branch:** 2.5px orange `#d95926` edge from root to a node centered at (185, 165) (rounded rect ~150×36, orange border, bold 13px orange text "try r1c2 = 3"); edge label bold 12px orange "guess #1" beside it.
- **Dead end:** 2.5px orange edge down to a node centered at (185, 245), ~250×44, fill `rgba(231,76,60,0.10)`, 2px red `#e74c3c` border; bold 12px red text, two lines: "r1c3 has no candidate left" / "contradiction — dead end"; a bold 16px red "✕" above the box corner.
- **Undo arrow:** dashed (dash 5/4) 2px red `#e74c3c` curved arrow from the dead-end node back up to the root, with bold 12px red label "undo, backtrack".
- **Right branch:** 2.5px green `#008300` edge from root to a node centered at (535, 165), ~150×36, green border, bold 13px green text "try r1c2 = 2"; edge label bold 12px green "guess #2".
- **Solution node:** 2.5px green edge down to a node centered at (535, 245), ~250×44, fill `rgba(0,131,0,0.10)`, 2px green border; bold 12px green text, two lines: "propagation fills the other 8 cells" / "solved ✓".
- **Caption (12px `#444`, bottom center y=290):** "wrong guesses die fast when propagation runs after every guess".

## Backtracking Is Not Brute Force

**Tags:** `common mistake` (red), `where it's used` (blue)

- **Blind count** — trying every digit in all 9 blanks means 4^9 = 262,144 full grids to check
- **With pruning** — backtracking abandons a branch the moment any candidate list goes empty
- **With propagation** — this puzzle needed just 9 forced placements and 0 guesses in total
- **Same machinery** — exam timetables, meeting rooms, and seating charts are cells with candidate lists
- **Compilers too** — register allocation assigns variables to registers under no-clash constraints

*Example (italic):* A scheduler placing 9 exams into 4 slots faces the same 262,144 blind combinations — and the same escape via propagation.

**Common mistake:** Calling backtracking "just brute force". Brute force checks complete grids one by one; backtracking prunes whole subtrees at the first broken constraint, and propagation prunes before guessing at all.

### Visualization (canvas `c4`, 720×300)

Horizontal bar chart on a log10 scale comparing the work done by three strategies on the same 9-blank puzzle.

- **Title (bold 15px, `#1a5276`, top center):** "Work to Solve the Same 9 Blanks (log scale)".
- **Data:** strategies and assignments examined: "blind enumeration" 262,144; "backtracking, no propagation" 350 (illustrative); "propagation first" 9. Bar lengths proportional to log10(value)/log10(262,144), i.e. 5.42, 2.54, and 0.95 units.
- **Layout:** bars start at x=250, max length 420px, 30px tall, at y=80, y=150, y=220; strategy labels bold 12px `#2c3e50` right-aligned at x=240; log10 tick marks along a 1px `#999` axis at y=262 labeled "1", "10", "100", "1k", "10k", "100k" in 11px `#6b7280` (evenly spaced, since the scale is log).
- **Bars:** blind enumeration fill `rgba(217,89,38,0.55)` with bold 13px orange `#d95926` value label "262,144 grids" at the bar end; backtracking fill `rgba(201,133,0,0.55)` with bold 13px yellow `#c98500` label "~350 (illustrative)"; propagation fill `rgba(0,131,0,0.55)` with bold 13px green `#008300` label "9 placements, 0 guesses".
- **Annotation (bold 13px green `#008300`, under the propagation bar):** "constraints do the searching for you".
- **Caption (12px `#444`, bottom center y=292):** "log scale: each tick is 10× more work".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
