# Longest Common Subsequence

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Longest Common Subsequence

**Subtitle:** Compare two versions of a list and ask what survived in the same order — a small grid of numbers finds that longest shared thread, and classic diff tools are built on it

## Two Grocery Lists, One Week Apart

**Tags:** `core idea` (blue), `shared order` (green), `diff` (orange)

- **Two lists** — Monday's grocery list has 5 items; Friday's rewrite has 4, and some items look familiar
- **The question** — what is the longest run of items that shows up in both lists, in the same order?
- **Gaps allowed** — eggs, apples, tea appear in both lists in that order, even with other items between
- **The answer** — no longer shared thread exists, so "eggs, apples, tea" (length 3) wins
- **The name** — this longest ordered overlap is called the longest common subsequence, or LCS

*Example (italic):* Monday: milk, eggs, bread, apples, tea. Friday: eggs, jam, apples, tea. The longest thread in both, in order: eggs, apples, tea.

**Key point:** The LCS is the longest list of items appearing in both versions in the same order — gaps are fine, reordering is not.

### Visualization (canvas `c1`, 720×300)

Two vertical lists of labeled boxes with green connector lines joining the items they share; the connectors never cross, which is exactly what "same order" means.

- **Title (bold 15px, `#1a5276`, top center):** "Two Lists, One Week Apart — What Survived in Order?".
- **Column headers (bold 13px `#1a5276`):** "Monday" centered at x=170, y=52; "Friday" centered at x=550, y=52.
- **Monday boxes:** rounded rects 110×28, centered x=170, centers at y = `[78, 116, 154, 192, 230]`, labels 13px = `["milk", "eggs", "bread", "apples", "tea"]`.
- **Friday boxes:** rounded rects 110×28, centered x=550, centers at y = `[88, 132, 176, 220]`, labels 13px = `["eggs", "jam", "apples", "tea"]`.
- **Kept items (eggs, apples, tea in both columns):** fill `rgba(0,131,0,0.12)`, 2px `#008300` border, text `#1a5276`.
- **Removed items (milk, bread):** fill `rgba(231,76,60,0.10)`, 1px `#e74c3c` border, a 2px `#e74c3c` strike line across the label.
- **Added item (jam):** fill `rgba(217,89,38,0.12)`, 1px `#d95926` border, 11px `#d95926` "+ new" tag at its right edge.
- **Connectors:** 2px `#008300` lines from the right edge of each kept Monday box to the left edge of its Friday twin: eggs (y 116→88), apples (y 192→176), tea (y 230→220).
- **Annotation (bold 13px green `#008300`, near x=360, y=150, two lines):** "3 lines, never crossing —" / "eggs, apples, tea: the LCS".
- **Caption (12px `#444`, bottom right):** "illustrative — two versions of one grocery list".

## Filling the Grid, Cell by Cell

**Tags:** `worked example` (blue), `grid filling` (green)

- **The grid** — one row per Monday item, one column per Friday item, plus an empty zero row and column
- **Cell meaning** — each cell holds the LCS length using only the items up to that row and column
- **Match rule** — items equal? take the diagonal neighbor and add 1 (eggs meets eggs: 0 + 1 = 1)
- **No-match rule** — items differ? copy the larger of the cell above and the cell to the left
- **The corner** — the bottom-right cell reads 3: the LCS of the two full lists is 3 items long
- **Traceback** — walking back through the diagonal +1 steps recovers eggs, apples, tea itself

*Example (italic):* At row "apples", column "apples" the diagonal neighbor holds 1, so the cell becomes 1 + 1 = 2 — two shared items so far.

**Key point:** Fill 20 cells with two tiny rules and the corner cell hands you the answer: LCS length 3.

### Visualization (canvas `c2`, 720×300)

The filled 6×5 dynamic-programming grid (zero row and column included), with the three match cells tinted green, dashed arrows tracing the answer back, and the corner cell holding the final 3.

- **Title (bold 15px, `#1a5276`, top center):** "The Grid: Each Cell = LCS Length So Far".
- **Geometry:** cells 80 wide × 34 tall; grid top-left at x=190, y=66; 5 columns, 6 rows (grid ends x=590, y=270); cell borders 1px `#e5e9ef`.
- **Column headers (12px `#444`, centered above each column at y=58):** `["∅", "eggs", "jam", "apples", "tea"]`.
- **Row labels (12px `#444`, right-aligned at x=182, one per row center):** `["∅", "milk", "eggs", "bread", "apples", "tea"]`.
- **Cell values (13px `#2c3e50`, centered), hardcoded matrix rows top to bottom:** `[[0,0,0,0,0],[0,0,0,0,0],[0,1,1,1,1],[0,1,1,1,1],[0,1,1,2,2],[0,1,1,2,3]]`.
- **Match cells:** (row "eggs", col "eggs"), (row "apples", col "apples"), (row "tea", col "tea") get fill `rgba(0,131,0,0.15)` and an 11px green `#008300` "↖+1" mark in the top-left corner of the cell.
- **Traceback:** 2px dashed (dash 5/3) `#d95926` arrows connecting the three match-cell centers, corner-most first: (tea,tea) → (apples,apples) → (eggs,eggs).
- **Corner cell (row "tea", col "tea"):** value drawn bold 15px `#1a5276`, cell outlined 2px `#1a5276`.
- **Annotation (bold 12px orange `#d95926`, near x=630, y=150, rotated 0, two lines):** "corner cell" / "= answer: 3".
- **Caption (12px `#444`, bottom left below row labels):** "match → diagonal + 1; else → max(above, left)".

## Why Diff Tools Do This

**Tags:** `where it's used` (blue), `diff & version control` (green), `speed` (orange)

- **Diff tools** — a classic "what changed?" view lines up the kept lines first, and the kept lines are the LCS
- **Removed** — anything on Monday's list but not in the LCS is printed with a "-" (milk, bread)
- **Added** — anything only on Friday's list is printed with a "+" (jam); kept lines stay unmarked
- **Why the grid** — brute force tests all 32 subsequences of the 5-item list; the grid fills just 20 cells
- **At scale** — two 1,000-line files need a million quick cells instead of astronomically many guesses
- **Beyond text** — spell-check suggestions, DNA alignment, and file sync reuse the exact same grid

*Example (italic):* Run a diff on the two grocery lists and it prints "- milk", "- bread", "+ jam" — the three unmarked lines are the LCS.

**Key point:** The smallest diff is a direct read-off of the LCS: kept = the LCS, and everything else is a - or a +.

### Visualization (canvas `c3`, 720×300)

A diff-output panel of the two grocery lists (minus, plus, and kept lines with tinted backgrounds), with a bracket showing the kept lines are exactly the LCS from the grid.

- **Title (bold 15px, `#1a5276`, top center):** "The Diff Is Just the LCS Plus Bookkeeping".
- **Diff lines (13px monospace, left-aligned at x=90, line centers at y = `[80, 112, 144, 176, 208, 240]`):** `["- milk", "  eggs", "- bread", "+ jam", "  apples", "  tea"]`.
- **Line backgrounds:** full-width 26px-tall bars from x=70 to x=330 behind each line — removed lines `rgba(231,76,60,0.08)` with text `#e74c3c`, added line `rgba(0,131,0,0.10)` with text `#008300`, kept lines white with text `#2c3e50`.
- **Bracket:** 2px `#008300` square bracket at x=345 spanning the three kept-line centers (y=112 to y=240, gapped past the - and + lines), with bold 13px green label to its right at x=360: "kept = the LCS: eggs, apples, tea".
- **Cost note (12px `#6b7280`, near x=430, y=225):** "20 grid cells found this; brute force checks 32 subsequences".
- **Caption (12px `#444`, bottom right):** "illustrative diff of the two lists".

## Subsequence Is Not Substring

**Tags:** `common mistake` (red), `gaps vs contiguous` (orange)

- **Substring** — a substring must be one unbroken block: "bread, apples, tea" sits contiguously in Monday's list
- **Subsequence** — a subsequence keeps left-to-right order but may skip: eggs, apples, tea jumps over bread
- **Different answers** — the longest common substring of these lists is "apples, tea" (length 2), not 3
- **Order still rules** — a subsequence may never reorder: "tea, eggs" appears in neither list's order
- **The mistake** — expecting a diff to keep only contiguous blocks; it happily keeps scattered lines

*Example (italic):* "eggs, apples, tea" is a common subsequence of length 3, but the longest common unbroken block is just "apples, tea" — length 2.

**Common mistake:** Confusing subsequence with substring. The LCS may scatter across each list with gaps in between; only the left-to-right order is sacred.

### Visualization (canvas `c4`, 720×300)

Two rows of the same five Monday items: the top row highlights the scattered subsequence with skip arcs, the bottom row shows the shorter unbroken block a substring is allowed to be.

- **Title (bold 15px, `#1a5276`, top center):** "Subsequence (Gaps OK) vs Substring (No Gaps)".
- **Item boxes:** both rows use rounded rects 100×30 with 13px labels, centers at x = `[110, 235, 360, 485, 610]`, labels = `["milk", "eggs", "bread", "apples", "tea"]`.
- **Row 1 (centers y=110), row label 12px `#444` at x=20, y=80:** "subsequence"; eggs, apples, tea boxes filled `rgba(0,131,0,0.15)` with 2px `#008300` borders; dashed 2px `#008300` skip arcs hopping box-top to box-top: eggs → apples (over bread) and apples → tea; bold 12px green label "length 3" at x=650, y=85.
- **Row 2 (centers y=210), row label at x=20, y=180:** "substring"; apples and tea boxes filled `rgba(217,89,38,0.15)` with 2px `#d95926` borders; solid 3px `#d95926` underline bar from the left edge of apples to the right edge of tea at y=232; bold 12px orange label "length 2" at x=650, y=185; other boxes plain white with `#e5e9ef` borders.
- **Annotation (bold 13px magenta `#d55181`, centered at x=360, y=272):** "same lists — subsequence 3, substring 2; gaps make the difference".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`. Red reserved for removed lines (a genuine deletion state).
- **Data:** the two lists, the 6×5 value matrix, all box centers, and all line positions are the hardcoded literal arrays above (no randomness); grid values are the true LCS dynamic-programming table for the two lists; text numbers (length 3, 20 cells, 32 subsequences, substring length 2) must match the charts.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
