# Space-Filling Curves

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Space-Filling Curves

**Subtitle:** Give every spot on a map one number by walking the blocks in a Z-shaped stroke — then a plain sorted list can answer "what's near me"

## Sixteen City Blocks, One Sorted List

**Tags:** `core idea` (blue), `Z-order` (green), `one key` (orange)

- **The app** — a food-truck app covers a small town cut into a 4×4 grid of sixteen blocks
- **The problem** — a phone can sort a list by one number, but a block has two: a column and a row
- **The trick** — walk every block with one Z-shaped pen stroke and number them 0 to 15 along the way
- **Z-order** — the stroke draws little Z's inside a bigger Z; the numbering it leaves is called Z-order
- **The payoff** — blocks with nearby keys are usually nearby on the map, so one sorted list does the job

*Example (italic):* Block (1, 1) gets key 3 and block (0, 1) gets key 2 — next to each other in the sorted list and next to each other on the map.

**Key point:** A space-filling curve turns a 2D location into one sortable number by numbering the blocks along a single stroke that visits them all.

### Visualization (canvas `c1`, 720×300)

Single-panel grid walk: the 4×4 town grid with each block showing its Z-order key, and the stroke drawn as one connected blue path from block 0 to block 15.

- **Title (bold 15px, `#1a5276`, top center):** "The Z Stroke: Sixteen Blocks Numbered 0–15 Along One Path".
- **Grid:** 4×4 cells of 52px, left edge x=250, top edge y=48 (grid spans x 250–458, y 48–256); cell borders 1px `#e5e9ef`; column 0–3 left to right, row 0–3 top to bottom.
- **Keys per cell (12px `#444`, top-left corner of each cell), row by row:** row 0: `0 1 4 5`; row 1: `2 3 6 7`; row 2: `8 9 12 13`; row 3: `10 11 14 15`.
- **Cell centers:** x = `[276, 328, 380, 432]` for columns 0–3, y = `[74, 126, 178, 230]` for rows 0–3.
- **Stroke:** blue `#2a78d6` 3px polyline through the cell centers in key order 0→15 (so 0=(col0,row0), 1=(col1,row0), 2=(col0,row1), 3=(col1,row1), 4=(col2,row0), ... 15=(col3,row3)).
- **Endpoints:** green `#008300` 7px dot at the key-0 center with 12px green label "start"; orange `#d95926` 7px dot at the key-15 center with 12px orange label "end".
- **Annotation (bold 12px orange `#d95926`, left side near x=60, y=140):** two lines: "little Z's inside" / "a bigger Z".
- **Caption (12px `#444`, bottom right):** "illustrative — a town cut into a 4×4 grid".

## Weaving Bits: Block (3, 2) Becomes Key 13

**Tags:** `worked example` (blue), `bit interleaving` (green)

- **The coordinates** — a truck parks at column 3, row 2; in binary that is x = 11 and y = 10
- **Weave** — take one bit from the row, one from the column, and alternate: y1 x1 y0 x0 = 1 1 0 1
- **The key** — binary 1101 is 13, exactly the number the Z stroke assigns to block (3, 2)
- **Check another** — the user's block (2, 3): x = 10, y = 11 weaves to 1110, which is key 14
- **Reverse** — un-weave 13 back into 11 and 10; the key stores both coordinates, nothing is lost

*Example (italic):* Column 3 is 11, row 2 is 10; weaving the bits gives 1101, which is 13 — no walking required, just two zips of a zipper.

**Key point:** Key = the bits of row and column interleaved: (3, 2) → 11 and 10 → 1101 → 13. Two coordinates in, one sortable number out.

### Visualization (canvas `c2`, 720×300)

Single-panel bit-weaving diagram: the two x bits and two y bits sit in colored boxes at the top, arrows drop each bit into its slot in the four-box key, and the result reads "= 13".

- **Title (bold 15px, `#1a5276`, top center):** "Weaving Bits: (3, 2) → 1101 → Key 13".
- **Source boxes (40×40, 2px border, bold 16px digit centered):** blue `#2a78d6` boxes at (200, 50) and (250, 50) holding "1" and "1", with 13px blue label "column x = 3" at x=110, y=75; green `#008300` boxes at (440, 50) and (490, 50) holding "1" and "0", with 13px green label "row y = 2" at x=560, y=75.
- **Key boxes (46×46, 2px border, bold 18px digit centered), left edges at x = `[270, 330, 390, 450]`, top y=170:** box 1 green holding "1", box 2 blue holding "1", box 3 green holding "0", box 4 blue holding "1"; 11px `#6b7280` slot labels under each box: "y1", "x1", "y0", "x0".
- **Arrows (2px, source color, simple arrowheads):** green (440,90)→(293,170); blue (200,90)→(353,170); green (490,90)→(413,170); blue (250,90)→(473,170).
- **Result:** bold 20px `#1a5276` text "= 13" at x=530, y=200.
- **Annotation (bold 12px violet `#4a3aa7`, centered near x=360, y=265):** "one bit from the row, one from the column, repeat".

## Why One Number Beats Two

**Tags:** `where it's used` (blue), `range scan` (green), `databases` (orange)

- **Databases sort** — indexes, files, and phone lists all order rows by a single key, never two at once
- **Range scan** — "trucks near me" becomes "keys 12 to 15", one contiguous slice of the sorted list
- **The quadrant** — keys 12–15 are exactly the four blocks of the user's corner of town
- **Found** — the trucks at keys 13 and 15 fall inside the slice; the trucks at keys 3 and 9 are skipped
- **Real systems** — map services and big databases use this trick (Z-order / Morton keys) for locations

*Example (italic):* The user in block key 14 asks for nearby trucks; scanning keys 12–15 returns the trucks at 13 and 15 without touching the other twelve blocks.

**Key point:** Sorting by Z-order key lets a plain one-column index answer a 2D "what is near me" question with one short range scan.

### Visualization (canvas `c3`, 720×300)

Two-part panel: the town grid on the left with the user's quadrant shaded and truck dots placed, and the sorted key line on the right with the same quadrant highlighted as one contiguous slice.

- **Title (bold 15px, `#1a5276`, top center):** "'Near Me' = One Slice of the Sorted List: Keys 12–15".
- **Left grid:** 4×4 cells of 40px, left edge x=70, top edge y=70 (grid spans x 70–230, y 70–230); 1px `#e5e9ef` borders; same key layout as c1 (11px `#444` key in each cell's top-left).
- **Quadrant shading:** cells with keys 12, 13, 14, 15 (columns 2–3, rows 2–3) filled `rgba(0,131,0,0.15)`.
- **Trucks:** orange `#d95926` 6px dots centered in the cells with keys `[3, 9, 13, 15]`.
- **User:** blue `#2a78d6` 7px dot in the key-14 cell with bold 12px blue label "you" beside it.
- **Key line:** horizontal 2px `#999` line at y=200 from x=320 to x=680; 16 ticks for keys 0–15 spaced 24px apart (key k at x = 320 + 24k); 11px `#444` labels "0"–"15" below the ticks.
- **Slice band:** green `rgba(0,131,0,0.18)` rectangle from key 12 (x=608) to key 15 (x=680), y 180–220, with bold 12px green label "scan 12–15" above it.
- **Dots on the line:** orange 6px dots above the ticks at keys 3, 9, 13, 15; blue 7px dot at key 14 with 12px blue label "you".
- **Annotation (bold 12px green `#008300`, near x=340, y=110):** two lines: "two trucks found," / "twelve blocks never touched".
- **Caption (12px `#444`, bottom right):** "illustrative — truck positions invented".

## Neighbors in the List, Strangers on the Map

**Tags:** `common mistake` (red), `boundary jump` (orange)

- **The jump** — keys 7 and 8 sit side by side in the list, but block (3, 1) to block (0, 2) is a long hop
- **Why** — each time the stroke finishes a quadrant it teleports to the start of the next one
- **The mistake** — assuming close keys always mean close blocks; true most of the time, not always
- **The fix** — real systems scan a slightly wider key range, or a few ranges, then check real distances
- **Still worth it** — a few extra distance checks beat comparing against every block in town

*Example (italic):* A naive app says the truck at key 8 is "right next to" the user at key 7 — on the map it is across town, 3 blocks west and 1 block down.

**Common mistake:** Treating key distance as map distance. Z-order keeps most neighbors together, but quadrant boundaries create jumps — always verify the real distance after the scan.

### Visualization (canvas `c4`, 720×300)

Two-part panel: the grid on the left with blocks 7 and 8 highlighted and a dashed arrow showing the long hop between them, and the key line on the right showing the same two keys touching.

- **Title (bold 15px, `#1a5276`, top center):** "The Catch: Keys 7 and 8 Touch in the List, Not on the Map".
- **Left grid:** 4×4 cells of 44px, left edge x=80, top edge y=60 (grid spans x 80–256, y 60–236); 1px `#e5e9ef` borders; same key layout as c1 (11px `#444` keys).
- **Highlights:** key-7 cell (column 3, row 1) filled `rgba(217,89,38,0.25)` with bold 13px `#d95926` "7" centered; key-8 cell (column 0, row 2) filled `rgba(213,81,129,0.25)` with bold 13px `#d55181` "8" centered.
- **Hop arrow:** red `#e74c3c` 2px dashed (dash 6/4) arrow from the key-7 cell center (234, 126) to the key-8 cell center (102, 170), with an arrowhead at the key-8 end.
- **Key line:** horizontal 2px `#999` line at y=150 from x=340 to x=680; 16 ticks for keys 0–15 spaced ~22.7px apart; 11px `#444` labels "0"–"15" below.
- **Dots on the line:** orange `#d95926` 7px dot at key 7 and magenta `#d55181` 7px dot at key 8, side by side, with bold 12px matching labels "7" and "8" above them.
- **Annotation (bold 12px red `#e74c3c`, near x=420, y=220):** two lines: "next-door keys," / "3 blocks west, 1 block down apart".
- **Caption (12px `#444`, bottom right):** "quadrant boundary = a teleport in the stroke".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all keys, cell layouts, truck positions, and bit values are the hardcoded literals above (no randomness); the 4×4 Z-order key layout is the true Morton order (bit-interleaved), and every key/coordinate pair in the text matches the charts — (3, 2) = 13, (2, 3) = 14, quadrant slice = keys 12–15, boundary jump = keys 7 and 8 at (3, 1) and (0, 2).
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
