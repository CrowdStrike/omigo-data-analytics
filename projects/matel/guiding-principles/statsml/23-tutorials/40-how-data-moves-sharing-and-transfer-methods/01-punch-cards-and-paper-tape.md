# Punch Cards & Paper Tape

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Punch Cards & Paper Tape

**Subtitle:** Data as holes in cardboard — a program was a box of cards, and sharing it meant handing the box to someone else

## A Program Was a Box of Cardboard

**Tags:** `core idea` (blue), `Hollerith cards` (green), `history` (orange)

- **The card** — a stiff cardboard rectangle with 80 columns; each column stores one character
- **Holes are the data** — hole = 1, no hole = 0; machines read them with metal brushes or light
- **One card, one line** — Alice's 600-line payroll program is a deck of exactly 600 cards
- **1890 origin** — Hollerith's card tabulators counted the US census years faster than hand tallies
- **Sharing** — no network existed; giving Bob the program meant handing him the physical box

*Example (italic):* Alice mails her payroll deck to a branch office — the data travels as 600 cards in a cardboard box.

**Key point:** A program or dataset was a physical object. Moving data meant moving cardboard, by hand or by post.

### Visualization (canvas `c1`, 720×300)

Zoomed face of an 80-column punch card (first 12 columns shown), with the text `PAY 12.50` punched in columns 1–9 using documented Hollerith/IBM 029 codes.

- **Title (bold 15px, `#1a5276`, top center):** "One Punch Card = One Line of Text (~80 bytes)".
- **Card shape:** manila rect `#fdf8ee`, 1.5px `#6b7280` border, x=60, y=44, 560×210, top-left corner clipped 18px (classic cut corner).
- **Printed characters** (cards printed the interpretation along the top edge): bold 13px `#1a5276` characters of `PAY 12.50` at y=66, one per column at colX(c) = 120 + c·40 for c = 0..11.
- **Row labels** (11px `#6b7280`, right-aligned at x=100): the 12 punch rows top to bottom: 12, 11, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 at rowY(i) = 88 + i·14; faint 1px `#e5e9ef` guide line across the card at each row.
- **Holes:** navy `#1a5276` filled rects 14×11 centered at (colX, rowY) for these documented 029 codes — P: rows 11,7 · A: rows 12,1 · Y: rows 0,8 · space: none · 1: row 1 · 2: row 2 · period: rows 12,3,8 · 5: row 5 · 0: row 0. Columns 10–12 left unpunched.
- **Zoom note (11px `#6b7280`, left, y=268):** "zoomed: first 12 of 80 columns".
- **Annotation (bold 13px green `#008300`, right, y=268):** "hole pattern = the character — no power needed to keep it".

## How Many Cards Is One Photo?

**Tags:** `worked example` (blue), `by-hand math` (green)

- **Card capacity** — 80 columns × 1 character each, so one card holds about 80 bytes
- **The photo** — a typical phone photo is about 3 MB, i.e. 3,000,000 bytes (illustrative)
- **Divide** — 3,000,000 ÷ 80 = 37,500 cards for one single photo
- **Box it** — cards shipped 2,000 to a box: 37,500 ÷ 2,000 ≈ 19 boxes
- **Stack it** — at ~0.18 mm per card the pile stands about 6.7 m tall
- **Weigh it** — at ~2.5 g per card the photo weighs roughly 90 kg as cardboard

*Example (italic):* One vacation snapshot, punched onto cards, arrives as 19 boxes on a hand truck.

**Key point:** Divide any file size by 80 to feel the era — every 80 bytes was one more card to punch, carry, and store.

### Visualization (canvas `c2`, 720×300)

Left: the arithmetic chain as four boxes with arrows. Right: the 6.7 m card stack drawn to scale next to a 1.7 m person (26 px per metre, baseline y=272).

- **Title (bold 15px, `#1a5276`, top center):** "One 3 MB Photo as Punch Cards (illustrative)".
- **Chain boxes** (x=40, 255×40, rounded 6px, ys = 56/118/180/242; tint fills with 2px borders in blue `#2a78d6`, green `#008300`, orange `#d95926`, violet `#4a3aa7`; 2px `#6b7280` down-arrows between): "photo ≈ 3,000,000 bytes" · "÷ 80 bytes per card = 37,500 cards" · "÷ 2,000 cards per box ≈ 19 boxes" · "× 0.18 mm per card ≈ 6.7 m stack". Text bold 12px `#2c3e50`, two lines where needed.
- **Ground line:** 1px `#6b7280` from x=380 to x=690 at y=272.
- **Card stack:** at x=430, width 70, from y=272 up to y=98 (174 px = 6.7 m at scale); drawn as 19 stacked segments (one per box), fill `rgba(201,133,0,0.25)`, 1px `#c98500` borders; dashed `#6b7280` height line to its left with 12px label "6.7 m".
- **Person:** stick figure at x=600, 44 px tall (1.7 m at scale): head circle r=7 at (600, 234), body/arms/legs 2px `#2a78d6` strokes down to y=272; 12px `#2c3e50` label "1.7 m person" below-right of ground line.
- **Annotation (bold 13px orange `#d95926`, centered over right half, y=60):** "one photo ≈ four people of cardboard".
- **Caption (11px `#6b7280`, bottom right):** "illustrative: 3 MB photo, 0.18 mm per card".

## Drop the Box, Then Sort It Back

**Tags:** `batch decks` (blue), `sequence numbers` (orange), `failure mode` (red)

- **Order is the program** — the machine runs cards in feed order; a shuffled deck is broken code
- **The accident** — one tripped operator turns a 600-card deck into random order on the floor
- **Columns 73–80** — FORTRAN reserved them for a sequence number; the compiler ignored them
- **The sorter** — a card-sorting machine re-orders a numbered deck mechanically in minutes
- **Batch decks** — jobs waited in trays; operators fed decks in, printouts came back hours later
- **Mailing data** — sharing across cities meant literally shipping card boxes or tape reels

*Example (italic):* Bob drops Alice's deck on the stairs; the numbers punched in columns 73–80 let a sorter rebuild it.

**Key point:** Columns 73–80 were insurance against gravity — numbering existed so order could be recovered after a drop.

### Visualization (canvas `c3`, 720×300)

Top: a card strip divided into the documented FORTRAN fixed-form column fields. Bottom: dropped-deck → card sorter → ordered-deck flow.

- **Title (bold 15px, `#1a5276`, top center):** "FORTRAN Card Fields — and the Undo for a Dropped Deck".
- **Card strip** (x=60 to x=660, y=54, height 54; widths proportional to 80 columns): cols 1–5 "label" fill `rgba(42,120,214,0.18)` (x 60–97.5) · col 6 "cont." fill `rgba(107,114,128,0.18)` (97.5–105) · cols 7–72 "statement" fill `rgba(0,131,0,0.15)` (105–600) · cols 73–80 "sequence #" fill `rgba(217,89,38,0.25)` (600–660). 1px `#6b7280` borders; 12px bold field labels above at y=48 ("1–5 label", "7–72 statement", "73–80 seq #"); 11px `#6b7280` note "col 6 = continuation" at y=122 with a short pointer line to the thin region.
- **Annotation (bold 13px orange `#d95926`, centered x=360, y=150):** "the compiler ignores 73–80 — they exist to survive a drop".
- **Flow row (y≈170–265):** left, 7 small white cards (34×22) at hardcoded positions/rotations around (130, 210) with 1.5px `#e74c3c` borders, bold 12px red `#e74c3c` label "dropped deck" at y=275; 2px `#6b7280` arrow to a violet-bordered box (x=280, y=185, 130×52, fill `rgba(74,58,167,0.10)`) labeled "card sorter" (bold 12px `#4a3aa7`) with sub-label "reads cols 73–80" (11px `#6b7280`); arrow to a neat deck at x=490: 8 aligned cards (70×9, 1px `#008300` borders, 11px vertical spacing... stacked with 10px offsets), bold 12px green `#008300` label "back in order" at y=275.
- **Caption (11px `#6b7280`, bottom right):** "layout: documented FORTRAN fixed form".

## Paper Tape, and the Fossils Still in Your Editor

**Tags:** `paper tape` (blue), `legacy` (green), `common confusion` (red)

- **Paper tape** — a continuous strip; each row of up to 8 holes across is one character
- **No drop hazard** — tape cannot be shuffled, but fixing one line means splicing the strip
- **"Patch"** — fixes were pasted over punched holes; the software term is widely traced to this
- **80-character lines** — terminal screens and code style limits copied the card's width
- **Fixed-width records** — mainframe files with 80-byte lines are card images; parsers still meet them

*Example (italic):* A "line too long (>80)" lint warning in 2026 traces back to a cardboard rectangle standardized in 1928.

**Key point:** The cardboard is gone but its shape is not — 80-character lines and fixed-width records are punch-card fossils.

### Visualization (canvas `c4`, 720×300)

Left: a horizontal 8-track paper tape segment with sprocket feed holes, spelling `DATA` in ASCII (one column of holes per character). Right: a vertical fossil timeline. Dashed `#e5e9ef` divider at x=330.

- **Title (bold 15px, `#1a5276`, top center):** "Paper Tape — and the 80-Column Fossil Trail".
- **Tape strip:** rect x=40, y=60, 270×150, fill `#fdf8ee`, 1.5px `#6b7280` border; 8 track rows at y = 70/86/102/118 and 150/166/182/198; sprocket feed holes as r=2 filled `#6b7280` dots at y=134 every 30px from x=55.
- **Characters:** `D A T A` at column x = 90/150/210/270; printed bold 12px `#1a5276` letters above the strip at y=52. Hole pattern per column from ASCII, bit b7 (top track) to b0 (bottom): D=68 → 0,1,0,0,0,1,0,0 · A=65 → 0,1,0,0,0,0,0,1 · T=84 → 0,1,0,1,0,1,0,0 · A repeats. Bit=1: r=5 filled `#1a5276` circle; bit=0: r=5 circle stroked 1px `#e5e9ef` (empty position).
- **Tape captions (11px `#6b7280`, x=175 centered):** "1 column of holes = 1 character (ASCII)" at y=228; "small centre dots = sprocket feed holes" at y=246.
- **Timeline:** 2px `#6b7280` vertical line at x=370 from y=52 to y=248; five entries at y = 60/105/150/195/240, each an r=5 dot (colors: blue `#2a78d6`, green `#008300`, yellow `#c98500`, aqua `#199e70`, magenta `#d55181`) plus bold 12px `#1a5276` era + 12px `#2c3e50` text starting x=385:
  - "1890 — Hollerith cards tabulate the US census"
  - "1928 — IBM's 80-column card becomes standard"
  - "1960s — FORTRAN decks: code in 1–72, seq in 73–80"
  - "1970s — 80×24 terminal screens copy the card width"
  - "today — 80-char lint limits, 80-byte records"
- **Annotation (bold 13px violet `#4a3aa7`, centered x=520, y=272):** "the 80-character line is the card's fossil".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red `#e74c3c` only for the genuine error state (the dropped deck).
- **Data:** all hole positions, chain numbers, stack/person heights, and timeline entries are the hardcoded values above (no randomness). The photo size (3 MB), card thickness (~0.18 mm), card weight (~2.5 g), and 2,000-cards-per-box figure drive invented totals labeled illustrative; the Hollerith 1890 census use, IBM 80-column format (1928), 029 punch codes, and FORTRAN fixed-form fields (1–5 / 6 / 7–72 / 73–80) are documented historical facts.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
