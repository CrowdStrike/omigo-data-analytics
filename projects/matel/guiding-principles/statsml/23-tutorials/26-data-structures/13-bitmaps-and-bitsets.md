# Bitmaps & Bitsets

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Bitmaps & Bitsets

**Subtitle:** A bitmap stores one yes/no fact per numbered slot as a single bit — a whole cinema hall fits in 8 bytes, and set questions become single machine instructions

## A Row of Light Switches

**Tags:** `core idea` (blue), `one bit per item` (green), `dense ids` (orange)

- **The hall** — a cinema has 64 numbered seats, and the box office only needs "taken or free" per seat
- **One bit each** — seat n maps to bit n: 1 means taken, 0 means free; all 64 answers fit in 8 bytes
- **No list needed** — instead of keeping a list of taken seat numbers, the position itself IS the seat
- **Reading** — checking seat 37 means looking at bit 37: no search, no scan, one direct look
- **The trade** — a bitmap stores yes/no only; anything richer per seat needs a different structure

*Example (italic):* The 7 pm showing is one 8-byte pattern: every taken seat flips its own switch to 1, and seat 37's answer sits waiting at position 37.

**Key point:** A bitmap is an array of yes/no bits indexed by item number — membership becomes "look at bit n", and 64 seats cost 8 bytes.

### Visualization (canvas `c1`, 720×300)

A single front row of 8 seats drawn as switch boxes, each holding its bit, with the direct-index arrow for one seat.

- **Title (bold 15px, `#1a5276`, top center):** "One Seat, One Bit: the Position Is the Seat Number".
- **Seat boxes:** 8 squares 56×56, left edges at x = `[104, 168, 232, 296, 360, 424, 488, 552]`, top y=95; 2px stroke; bits `[0, 1, 0, 0, 0, 1, 0, 1]` (seats 2, 6, 8 taken); taken boxes filled `rgba(217,89,38,0.25)` with 2px `#d95926` stroke, free boxes white with 2px `#2a78d6` stroke.
- **Bit digits:** bold 20px centered in each box — "1" in `#d95926` for taken, "0" in `#2a78d6` for free.
- **Seat labels:** 12px `#444` centered under each box at y=175: "seat 1" … "seat 8".
- **Index arrow:** 2px `#008300` arrow from the label "seat 6?" (bold 13px green, at x=452, y=225) straight up to the bottom of seat 6's box, with a filled arrowhead; beside it 12px green text "read bit 6 → 1, taken".
- **Byte brace:** 12px `#6b7280` text at x=360 centered, y=75: "one byte: 0 1 0 0 0 1 0 1".
- **Annotation (bold 13px `#199e70`, near x=360, y=45):** "no searching — the seat number is the address".
- **Caption (12px `#444`, bottom right):** "front row of 8 seats — the full hall is just 8 of these bytes".

## Selling Seat 5, Checking Seat 3, Counting the House

**Tags:** `worked example` (blue), `bit operations` (green)

- **Start** — the 7 pm row reads 0 1 0 0 0 1 0 1: seats 2, 6, and 8 are taken
- **Sell seat 5** — set bit 5 to 1: the row becomes 0 1 0 0 1 1 0 1; one flip, nothing else moves
- **Check seat 3** — bit 3 is 0, so seat 3 is free: one look answers it
- **Count the house** — count the 1s (a popcount): 0 1 0 0 1 1 0 1 holds four 1s — four tickets sold
- **Two showings** — the 9 pm row reads 0 0 1 0 1 1 0 0: seats 3, 5, and 6 are taken
- **AND them** — keep 1 only where both rows have 1: 0 0 0 0 1 1 0 0 — seats 5 and 6 taken in both

*Example (italic):* Line the rows up and multiply column by column: 01001101 AND 00101100 = 00001100 — the two double-booked seats fall out in one operation.

**Key point:** Set, check, count, and combine are single bit operations — the AND of two showings answers "taken in both" with no loops over seat lists.

### Visualization (canvas `c2`, 720×300)

Three stacked bit rows — the 7 pm row, the 9 pm row, and their AND — with the matching columns highlighted.

- **Title (bold 15px, `#1a5276`, top center):** "Two Showings ANDed: Seats 5 and 6 Are Taken in Both".
- **Column x centers:** 8 columns at x = `[132, 196, 260, 324, 388, 452, 516, 580]`; header row of seat numbers "1"–"8" in 12px `#444` at y=68.
- **Row layout:** three rows of 8 cells (44×40 each, centered on the column x): "7 pm" at y=85, "9 pm" at y=140, "AND" at y=210; row labels bold 13px right-aligned at x=95, vertically centered per row (`#2a78d6`, `#4a3aa7`, `#008300` respectively).
- **Bits:** 7 pm = `[0, 1, 0, 0, 1, 1, 0, 1]` (after selling seat 5), 9 pm = `[0, 0, 1, 0, 1, 1, 0, 0]`, AND = `[0, 0, 0, 0, 1, 1, 0, 0]`; each cell white with 1.5px `#6b7280` stroke, bit digit bold 16px (`#2c3e50` for 0, `#d95926` for 1).
- **Highlight:** columns 5 and 6 get a full-height rounded rectangle behind the cells, fill `rgba(0,131,0,0.10)` with 2px `#008300` stroke, spanning y=78 to y=255.
- **Operator row:** bold 14px `#6b7280` "AND ↓" centered at x=132−52, y=192 (left of the AND row, under the 9 pm row).
- **Annotation (bold 13px `#008300`, near x=470, y=283):** "only seats 5 and 6 survive the AND".
- **Caption (12px `#444`, bottom left at x=100, y=283):** "bit order: seat 1 on the left".

## A Million Users in 125 Kilobytes

**Tags:** `where it's used` (blue), `tiny memory` (green), `fast set math` (orange)

- **Daily actives** — one bit per user id answers "did user n visit today" for 1,000,000 users in 125 KB
- **The list way** — the same day as a list of 4-byte ids costs 400 KB at 100,000 actives, and it grows
- **Cross-day questions** — Monday AND Tuesday = active both days; OR = either day; all plain bit math
- **Database indexes** — bitmap indexes mark which rows hold each value, so WHERE clauses become ANDs
- **Hardware help** — CPUs count and combine 64 bits per instruction, so these questions fly

*Example (italic):* "Active Monday AND active Tuesday" over a million users is one AND of two 125 KB bitmaps — a few tens of thousands of machine words of work.

**Key point:** When items have dense integer ids, bitmaps turn set questions — who did both? who did either? how many? — into a handful of machine instructions.

### Visualization (canvas `c3`, 720×300)

Bar chart comparing memory for one day of active users stored as an id list versus a bitmap, at two activity levels.

- **Title (bold 15px, `#1a5276`, top center):** "Storing 'Who Was Active Today' for 1,000,000 Users".
- **Axes:** baseline 2px `#999` at y=240 from x=90 to x=660; y = kilobytes, 0 at baseline to 2,000 KB at y=70; light `#e5e9ef` gridlines at 500, 1,000, 1,500, 2,000 with 12px `#444` left labels "500", "1,000", "1,500", "2,000" and axis caption "KB" at x=52, y=62.
- **Bar groups:** two groups centered at x=250 ("100,000 active") and x=520 ("500,000 active"), 12px `#444` group labels at y=260; each group has two bars 70px wide, 16px apart.
- **Id-list bars:** heights for values `[400, 2000]` KB, fill `rgba(217,89,38,0.35)`, 2px `#d95926` stroke, bold 13px `#d95926` value labels above: "400 KB", "2,000 KB".
- **Bitmap bars:** both value 125 KB, fill `rgba(42,120,214,0.35)`, 2px `#2a78d6` stroke, bold 13px `#1a5276` labels "125 KB".
- **Flat-line note:** dashed 2px `#008300` horizontal line (dash 5/4) across both bitmap bar tops from x=180 to x=620; bold 13px `#008300` annotation near x=420, y=195: "the bitmap never grows — 1 bit per user, active or not".
- **Caption (12px `#444`, bottom right):** "id list = 4 bytes per active user — illustrative day".

## Bitmap or Bloom Filter?

**Tags:** `common mistake` (red), `exact vs probabilistic` (orange)

- **The mix-up** — both are arrays of bits, so they get confused; they answer different problems
- **Bitmap** — exact: bit n belongs to item n, answers are always right, but ids must be dense integers
- **Bloom filter** — hashes ANY key (words, URLs) to a few bit positions; can say yes when the truth is no
- **Sparse ids hurt** — seats 1–64 fit 64 bits exactly; random 64-bit ids would need an absurd bitmap
- **Pick by keys** — dense integer ids → bitmap; arbitrary keys plus tolerable false alarms → Bloom filter

*Example (italic):* Seats 1–64 fit a 64-bit bitmap exactly; remembering which of a billion possible URLs were crawled needs hashing — that is Bloom-filter territory.

**Common mistake:** Treating the two as interchangeable. A bitmap is an exact register indexed by item number; a Bloom filter is a probabilistic membership test for keys that have no usable number.

### Visualization (canvas `c4`, 720×300)

Two side-by-side panels contrasting the direct-index bitmap with the hashed Bloom filter.

- **Title (bold 15px, `#1a5276`, top center):** "Same Bits, Different Machines: Direct Index vs Hashed Probes".
- **Panels:** two rounded rectangles 300×195 at (35, 60) and (385, 60); left stroked 2px `#2a78d6`, right stroked 2px `#4a3aa7`; bold 13px panel titles inside at top center: "Bitmap" (`#2a78d6`), "Bloom filter" (`#4a3aa7`).
- **Left panel:** a row of 10 bit cells (22×22, starting x=55, y=115) with bits `[0, 0, 1, 0, 0, 0, 1, 0, 0, 0]`; label "seat 37" in bold 12px `#008300` at (110, 175) with ONE solid 2px green arrow to cell index 3 (its own slot); 11px `#444` cell caption "bit 37 — its own slot" at y=155; two property lines 12px `#444` centered at (185, 215) and (185, 232): "exact — never wrong", "needs dense integer ids".
- **Right panel:** same 10-cell row (starting x=405, y=115) with bits `[0, 1, 0, 0, 1, 0, 0, 0, 1, 0]`; label "key: 'url…'" in bold 12px `#d95926` at (450, 175) with THREE dashed 2px orange arrows (dash 4/3) fanning to cells 1, 4, and 8; 11px `#444` cell caption "3 hashed positions" at y=155; property lines 12px `#444` centered at (535, 215) and (535, 232): "any key type", "can false-positive".
- **Annotation (bold 12px `#e74c3c`, centered x=360, y=283):** "one arrow that is always right — or three guesses that are usually right".
- **Caption:** none beyond the annotation.

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the displayed CSS width × `devicePixelRatio` (sharp-rendering pattern) and scales the context; chart functions are pushed into a `__charts` array, run once, and re-run on window resize debounced 150 ms.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** every bit pattern, bar value, and coordinate is a hardcoded literal array as specced above (no randomness); the 7 pm / 9 pm / AND rows in `c2` must match the section-two bullets bit for bit (01001101, 00101100, 00001100), and the 125 KB / 400 KB / 2,000 KB figures must match the section-three text.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
