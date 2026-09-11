# The Mutilated Chessboard

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** The Mutilated Chessboard

**Subtitle:** Sixty-two tiles and thirty-one two-tile mats look like a perfect match — but coloring the tiles and counting shows the fit is impossible before you try a single layout

## Two Missing Tiles on the Cafe Floor

**Tags:** `core idea` (blue), `the puzzle` (green), `perfect fit on paper` (orange)

- **The floor** — a cafe floor is a checkerboard of 64 tiles, 32 dark and 32 light
- **Two pillars** — builders remove two opposite corner tiles for pillars, leaving 62 tiles
- **The mats** — the owner buys 31 rubber mats, each covering exactly 2 side-by-side tiles
- **The hope** — 31 mats × 2 tiles = 62 tiles, so on paper it looks like a perfect fit
- **The struggle** — every layout ends with two stranded tiles that never sit next to each other

*Example (italic):* After an hour of shuffling mats, the owner is always left with two lonely light tiles on opposite sides of the room — never neighbors.

**Key point:** The arithmetic 31 × 2 = 62 matches perfectly, yet no arrangement ever works — the puzzle is explaining why without trying every layout.

### Visualization (canvas `c1`, 720×300)

Single-panel picture of the mutilated floor: an 8×8 checkerboard with the two opposite corners removed, one sample mat drawn covering two adjacent tiles, and the question posed at the right.

- **Title (bold 15px, `#1a5276`, top center):** "The Cafe Floor: 64 Tiles, 2 Removed, 31 Two-Tile Mats".
- **Board:** 8×8 grid, cell 24px, top-left corner of board at x=90, y=48 (board spans 192×192, bottom edge y=240); tile is dark `#1a5276` when (row+col) is even, light `#e5e9ef` otherwise; 1px `#fff` stroke between cells.
- **Removed corners:** top-left cell (row 0, col 0) and bottom-right cell (row 7, col 7) drawn white with a red `#e74c3c` 2px diagonal cross; 11px red label "removed" beside each (left of the top corner, right of the bottom corner).
- **Sample mat:** orange `#d95926` rounded rect (radius 5, 90% opacity) covering cells (row 3, cols 3–4); bold 11px white label centered on it: "1 mat".
- **Legend (x=340, y=185):** two 14px swatches (dark `#1a5276`, light `#e5e9ef` with `#6b7280` border) with 12px `#444` labels "dark tile" and "light tile".
- **Annotation (bold 13px `#1a5276`, x=340, y=95):** three lines: "62 tiles left." / "31 mats, 2 tiles each." / "Can they fit?".
- **Caption (12px `#444`, bottom right):** "each mat always covers two side-by-side tiles".

## Counting Colors Instead of Trying Mats

**Tags:** `worked example` (blue), `coloring argument` (green)

- **Color the floor** — the 64 tiles alternate like a chessboard: 32 dark and 32 light
- **Opposite corners** — walk the diagonal and count: opposite corners always share a color, here both dark
- **After removal** — 32 − 2 = 30 dark tiles remain, while all 32 light tiles remain
- **One mat's diet** — two side-by-side tiles always differ in color, so each mat covers 1 dark + 1 light
- **The shortfall** — 31 mats would need 31 dark tiles, but only 30 exist — the fit is impossible

*Example (italic):* Lay 30 mats and you have used 30 dark + 30 light tiles; the two leftovers are both light, and two light tiles are never adjacent — so mat 31 has nowhere to go.

**Key point:** 31 mats need 31 dark and 31 light tiles; the floor offers 30 dark and 32 light — that single count ends the search.

### Visualization (canvas `c2`, 720×300)

Single-panel bar chart: the remaining light and dark tile counts after removal, with a dashed demand line at 31 showing what 31 mats would require of each color.

- **Title (bold 15px, `#1a5276`, top center):** "After Removing Two Dark Corners: 32 Light vs 30 Dark".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = tile count 0 to 36 with light `#e5e9ef` gridlines at 10, 20, 30 and 12px `#444` tick labels; no x axis line beyond the baseline.
- **Bars (width 130):** "light tiles left" at x=150, height for value 32, fill `#e5e9ef` with 2px `#6b7280` border; "dark tiles left" at x=420, height for value 30, fill `#1a5276`; 12px `#444` category labels under the baseline; bold 14px value labels "32" and "30" just above each bar (dark bar's label in `#1a5276`).
- **Demand line:** horizontal dashed orange `#d95926` (dash 6/4) 2px line at value 31 across the plot; bold 12px orange label at its right end: "31 mats need 31 of EACH color".
- **Annotation (bold 13px red `#e74c3c`, right of the dark bar, near x=565, y=103/120):** two lines: "one dark tile short —" / "no layout can exist".
- **Caption (12px `#444`, bottom right):** "counts are exact: 64 − 2 dark corners = 30 dark + 32 light".

## One Clever Look Beats a Million Tries

**Tags:** `where it's used` (blue), `invariants` (green), `impossibility proofs` (orange)

- **Brute force** — a computer can try layouts for hours, and every run tops out at 30 mats placed
- **The invariant** — a fact no move can break: every mat, wherever it lies, eats 1 dark + 1 light tile
- **One-line proof** — 31 mats need 31 dark tiles, only 30 exist, so every possible layout fails at once
- **In interviews** — the classic test of whether you hunt for structure before writing a giant search
- **In practice** — matching problems (orders to couriers, mentors to mentees) fail the same way: count both sides first

*Example (italic):* An engineer burns a weekend on a scheduler that pairs 31 night shifts with 30 qualified staff — a two-number count would have declared it infeasible on Friday.

**Key point:** An invariant turns "I tried and failed" into "no one can ever succeed" — one count rules out every arrangement at once.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: six computer search runs, each stalling at or below 30 mats, against a dashed goal line at 31 that no run ever reaches.

- **Title (bold 15px, `#1a5276`, top center):** "Every Search Run Hits a Wall; One Count Explains It".
- **Axis:** x = mats placed 0 to 32, mapped from x=150 to x=670 (520px); 2px `#999` baseline at y=240; 12px `#444` tick labels "0", "8", "16", "24", "32" below the baseline; light `#e5e9ef` vertical gridlines at those ticks.
- **Rows (y = 70, 98, 126, 154, 182, 210, bar height 18):** left-aligned 12px `#444` labels "run 1" … "run 6" at x=95; bar values = `[28, 30, 29, 30, 27, 30]`, fill `rgba(42,120,214,0.35)` with 2px `#2a78d6` border; 12px `#2a78d6` value label at each bar's right end.
- **Goal line:** vertical dashed red `#e74c3c` (dash 4/3) 2px line at value 31 from y=55 to the baseline; bold 12px red label at its top: "goal: 31 — never reached".
- **Annotation (bold 13px green `#008300`, centered near y=272):** "coloring argument: 30 dark < 31 needed — done in one line".
- **Caption (12px `#444`, bottom right):** "run results illustrative; the 30-mat ceiling is provable".

## But 62 Is an Even Number!

**Tags:** `common mistake` (red), `necessary vs sufficient` (orange)

- **The trap** — "62 tiles, 31 mats, the numbers match" feels like a proof, but even is only step one
- **Necessary vs enough** — an odd tile count kills a tiling instantly; an even count promises nothing
- **Corner colors** — opposite corners share a color, so removing them unbalances the floor: 32 vs 30
- **The twin puzzle** — remove one dark and one light tile instead, and 31 mats always fit: 31 vs 31
- **Check colors first** — before hunting for arrangements, count what each piece must consume

*Example (italic):* Two cafes each lose two corner tiles: one loses two dark corners, the other a dark and a light — same 62 tiles, opposite answers.

**Common mistake:** Treating "the totals match" as a guarantee. Matching totals are necessary, not sufficient — the dark/light balance is the count that actually decides.

### Visualization (canvas `c4`, 720×300)

Two mini checkerboards side by side, both with 62 tiles: the left missing two same-color corners (impossible), the right missing two different-color corners (possible), with the color counts under each.

- **Title (bold 15px, `#1a5276`, top center):** "Both Floors Have 62 Tiles — Colors Decide".
- **Left board:** 8×8 grid, cell 16px, top-left at x=95, y=55 (board 128×128); dark `#1a5276` when (row+col) even, light `#e5e9ef`, 1px `#fff` strokes; removed cells (row 0, col 0) and (row 7, col 7) — both dark — drawn white with red `#e74c3c` 2px crosses.
- **Right board:** same style, top-left at x=430, y=55; removed cells (row 0, col 0) — dark — and (row 0, col 7) — light — drawn white with red crosses.
- **Verdict labels (bold 13px, centered under each board at y=215):** left in red `#e74c3c`: "30 dark, 32 light — impossible"; right in green `#008300`: "31 dark, 31 light — possible".
- **Sub-labels (12px `#444`, centered at y=235):** left: "two SAME-color corners removed"; right: "one dark + one light removed".
- **Annotation (bold 13px `#4a3aa7` violet, centered near y=272):** "even total = necessary; color balance = decisive".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Boards:** draw checkerboards with nested loops from the cell size and top-left coordinates above; tile color from (row+col) parity; no randomness anywhere. Tile counts (32/30, 31/31) and the search-run values `[28, 30, 29, 30, 27, 30]` are the hardcoded literals above; only the search-run values are invented and carry the "illustrative" caption.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
