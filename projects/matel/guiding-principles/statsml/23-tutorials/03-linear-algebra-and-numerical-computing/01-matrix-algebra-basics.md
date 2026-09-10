# Matrix Algebra Basics

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Matrix Algebra Basics

**Subtitle:** A matrix is a table of numbers with rules for combining tables — transpose flips it, the identity leaves it alone, the inverse undoes it, and order of multiplication matters

## A Smoothie Shop's Recipe Table

**Tags:** `core idea` (blue), `rows × columns` (green), `matrix × vector` (orange)

- **The shop** — a smoothie stand sells Berry Blast and Tropical, mixed from berries and bananas
- **The table** — Berry Blast: 2 cups berries + 1 banana; Tropical: 1 cup berries + 3 bananas
- **A matrix** — that recipe table written bare is the matrix R = [[2, 1], [1, 3]]; rows are drinks
- **The prices** — berries cost $3 per cup and bananas $1 each: the price vector c = [3, 1]
- **Row × column** — each drink's cost = walk its row, multiply by prices, add: 2·3 + 1·1 = 7
- **The product** — R·c = [7, 6]: Berry Blast costs $7 of ingredients, Tropical costs $6

*Example (italic):* Tropical's row is (1, 3), so its cost is 1·$3 + 3·$1 = $6 — one row-times-column dot product per drink.

**Key point:** A matrix is just a table with a combining rule: each output entry is one row of the left table dotted with one column of the right. Everything else in matrix algebra builds on that.

### Visualization (canvas `c1`, 720×300)

Flow diagram: the recipe grid R (left) times the price column c (middle) produces the cost column (right), with one row-times-column computation spelled out underneath.

- **Title (bold 15px, `#1a5276`, top center):** "Recipe Table × Price List = Cost per Smoothie".
- **Data:** R = `[[2, 1], [1, 3]]`, c = `[3, 1]`, result = `[7, 6]`; row labels "Berry Blast", "Tropical"; column labels "berries", "bananas".
- **R grid (left):** 2×2 grid of 56px cells, top-left at x=80, y=80; cell borders 2px ink `#1a5276`; entries bold 16px `#2c3e50` centered; column headers "berries"/"bananas" 12px `#6b7280` above; row labels 12px `#444` to the left; heading bold 13px blue `#2a78d6` "recipes R" above the grid.
- **c column (middle):** 1-wide, 2-tall grid of 56px cells at x=310, y=80, entries "$3", "$1" bold 16px; heading bold 13px yellow `#c98500` "prices c"; a bold 18px `#444` "×" centered at x=270, y=138.
- **Result column (right):** 1-wide, 2-tall grid at x=470, y=80, entries "$7", "$6" bold 16px green `#008300`; heading bold 13px green "cost R·c"; a bold 18px `#444` "=" at x=430, y=138.
- **Row-dot highlight:** first row of R filled `rgba(42,120,214,0.18)` and the c column filled `rgba(201,133,0,0.15)`; a blue 2px arrow from row 1 to the "$7" cell.
- **Worked line (bold 13px blue, centered at y=245):** "Berry Blast: 2·$3 + 1·$1 = $7 — one row dotted with one column".
- **Caption (12px `#444`, centered at y=278):** "rows = drinks, columns = ingredients; the product has one cost per row".

## Flipping the Table, and the Do-Nothing Matrix

**Tags:** `worked example` (blue), `transpose` (green), `identity` (orange)

- **Transpose** — Rᵀ flips rows and columns: drinks-by-ingredients becomes ingredients-by-drinks
- **The flip** — R = [[2, 1], [1, 3]] becomes Rᵀ = [[2, 1], [1, 3]] here — only because it is symmetric
- **A real flip** — the asymmetric pair: entry (row 1, col 2) = 1 swaps places with entry (row 2, col 1) = 1
- **Identity** — I = [[1, 0], [0, 1]] is the matrix version of the number 1: I·R = R and R·I = R
- **Check it** — row (1, 0) dotted with R's first column (2, 1) gives 1·2 + 0·1 = 2, unchanged
- **Why I exists** — you need a "multiply by 1" to even define what an inverse should produce

*Example (italic):* Multiplying the recipe table by I = [[1, 0], [0, 1]] re-computes every entry — 1·2 + 0·1 = 2, 1·1 + 0·3 = 1 — and hands back the same table.

**Key point:** Transpose swaps entry (i, j) with entry (j, i) — a mirror flip across the diagonal. The identity is the unique table that leaves every table it multiplies unchanged.

### Visualization (canvas `c2`, 720×300)

Three grids in a row: R with its diagonal marked, Rᵀ with the swapped off-diagonal pair highlighted, and I·R = R shown as a do-nothing pipeline.

- **Title (bold 15px, `#1a5276`, top center):** "Transpose Mirrors Across the Diagonal; the Identity Changes Nothing".
- **Data:** R = `[[2, 1], [1, 3]]`, Rᵀ = `[[2, 1], [1, 3]]`, I = `[[1, 0], [0, 1]]`.
- **R grid (left):** 2×2 grid of 52px cells at x=60, y=90; heading bold 13px blue `#2a78d6` "R"; dashed aqua `#199e70` diagonal line (dash 4/3) from the top-left cell corner to the bottom-right cell corner; off-diagonal entries (1 and 1) drawn bold 16px magenta `#d55181`, diagonal entries (2 and 3) bold 16px `#2c3e50`.
- **Flip arrow:** curved magenta 2px double-headed arrow between the two off-diagonal cells, label bold 12px magenta "(1,2) ↔ (2,1) swap" at y=225 under the grid.
- **Rᵀ grid (middle):** same styling at x=240, y=90; heading bold 13px aqua "Rᵀ (same here — R is symmetric)"; entries identical, off-diagonal pair again magenta.
- **Identity pipeline (right):** grid I at x=440, y=90 with entries bold 16px `#444`, heading bold 13px yellow `#c98500` "I"; a bold 16px "×" then a small 2×2 R grid at x=575, y=90 (44px cells, 14px entries, heading "R"); below both, bold 13px green `#008300` text "I·R = R" centered at x=555, y=235.
- **Takeaway (bold 13px green, bottom center at y=280):** "identity = multiply by 1: every row (1,0) or (0,1) just picks an entry back out".

## Socks Then Shoes: Why AB ≠ BA

**Tags:** `common mistake` (red), `order matters` (orange), `worked example` (blue)

- **Two actions** — D doubles the Berry Blast batch: D = [[2, 0], [0, 1]]; S swaps the two drinks: S = [[0, 1], [1, 0]]
- **Double, then swap** — S·D = [[0, 1], [2, 0]]: the doubled recipe ends up in the Tropical slot
- **Swap, then double** — D·S = [[0, 2], [1, 0]]: the doubling lands on what is now in slot one
- **Different tables** — S·D and D·S disagree in every nonzero entry, so AB = BA is not a law
- **Everyday version** — socks then shoes is not shoes then socks; each step acts on the last result
- **When it holds** — only special pairs commute, like anything with I or two diagonal matrices

*Example (italic):* Reading right to left, S·D means "double first, then swap" — flip the order and the 2 moves from row 2 column 1 to row 1 column 2.

**Common mistake:** Assuming matrix multiplication commutes like number multiplication. Matrices are actions, and doing action B on the result of A is not doing A on the result of B.

### Visualization (canvas `c3`, 720×300)

Two horizontal pipelines: top applies D then S, bottom applies S then D, each ending in its product matrix — the two ending grids differ and are cross-marked.

- **Title (bold 15px, `#1a5276`, top center):** "Double Then Swap vs Swap Then Double: Two Different Results".
- **Data:** D = `[[2, 0], [0, 1]]`, S = `[[0, 1], [1, 0]]`, S·D = `[[0, 1], [2, 0]]`, D·S = `[[0, 2], [1, 0]]`.
- **Top pipeline (y=70):** label bold 13px blue `#2a78d6` "double first, then swap (S·D)" at x=60, y=58; 2×2 grid of D (40px cells, entries bold 14px) at x=60; blue 2px arrow labeled "then S" 12px to a result grid of S·D at x=250; result entries bold 15px blue; the entry "2" at row 2, col 1 circled with a 2px blue ring.
- **Bottom pipeline (y=185):** label bold 13px orange `#d95926` "swap first, then double (D·S)" at x=60, y=173; grid of S at x=60; orange arrow labeled "then D"; result grid of D·S at x=250; the entry "2" at row 1, col 2 circled orange.
- **Comparison (right half):** both result grids repeated side by side at x=470 (top, blue frame) and x=590 (bottom, orange frame), 40px cells; a bold 20px red `#e74c3c` "≠" centered between them at x=555, y=140; heading bold 13px `#444` "same two actions, opposite order" above at y=70.
- **Takeaway (bold 13px red `#e74c3c`, bottom center at y=282):** "the 2 lands in a different cell — AB and BA are different matrices".

## Undoing the Recipe: the Inverse

**Tags:** `core idea` (blue), `inverse` (green), `where it's used` (orange)

- **The puzzle** — you see the ingredient costs $7 and $6 per drink but forgot the prices per unit
- **The undo** — R⁻¹ is the table with R⁻¹·R = I; multiplying by it reverses what R did
- **This R** — R⁻¹ = (1/5)·[[3, −1], [−1, 2]], where 5 = 2·3 − 1·1 is the determinant of R
- **Recover prices** — R⁻¹·[7, 6] = (1/5)·[3·7 − 1·6, −1·7 + 2·6] = (1/5)·[15, 5] = [3, 1] — the prices
- **Check** — R·R⁻¹ multiplies out to [[1, 0], [0, 1]] = I, the do-nothing matrix from before
- **No undo** — if the determinant is 0 the recipes are redundant and no inverse exists

*Example (italic):* From costs ($7, $6) alone, R⁻¹ hands back berries = $3 and bananas = $1 — solving two equations in two unknowns in one multiply.

**Key point:** The inverse turns "recipes × prices = costs" around into "prices = R⁻¹ × costs". It exists exactly when the determinant is nonzero — when the rows carry genuinely different information.

### Visualization (canvas `c4`, 720×300)

Round-trip diagram: prices go forward through R to costs, then back through R⁻¹ to the same prices, with the verification R·R⁻¹ = I in a panel on the right.

- **Title (bold 15px, `#1a5276`, top center):** "R Turns Prices into Costs; R⁻¹ Turns Them Back".
- **Data:** prices = `[3, 1]`, costs = `[7, 6]`, R = `[[2, 1], [1, 3]]`, R⁻¹ = (1/5)·`[[3, -1], [-1, 2]]`, det = 5.
- **Prices box (left):** rounded rect at x=60, y=105, 110×80, border 2px green `#008300`, fill `rgba(0,131,0,0.08)`; heading bold 13px green "prices"; entries "$3 berries" / "$1 banana" 12px `#2c3e50`.
- **Costs box (right of it):** rounded rect at x=320, y=105, 110×80, border 2px blue `#2a78d6`, fill `rgba(42,120,214,0.08)`; heading bold 13px blue "costs"; entries "$7 Berry" / "$6 Tropical" 12px.
- **Forward arrow (top):** blue 3px arrow from x=170 to x=320 at y=125, label bold 13px blue "× R" above.
- **Return arrow (bottom):** green 3px arrow from x=320 to x=170 at y=170, label bold 13px green "× R⁻¹ = (1/5)[[3,−1],[−1,2]]" below at y=192 in 12px.
- **Verification panel (right):** dashed `#bdc3c7` vertical divider (dash 4/3) at x=470 from y=40 to y=288; heading bold 13px `#444` "check the undo:" at x=495, y=70; lines 12px `#2c3e50` at x=495, spaced 24px: "R⁻¹·[7, 6]", "= (1/5)·[21−6, −7+12]", "= (1/5)·[15, 5] = [3, 1]"; below, bold 13px violet `#4a3aa7` "R·R⁻¹ = I" at y=200 and a small 2×2 identity grid (36px cells, entries bold 13px) at x=495, y=212.
- **Takeaway (bold 13px green, centered under the boxes at y=262, max width 400):** "round trip lands exactly on $3 and $1 — the inverse undoes the recipe table".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- All grids are drawn with 2px ink `#1a5276` cell borders and centered bold entries unless a spec above overrides color; matrix entries are hardcoded — no computation or randomness in the script.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
