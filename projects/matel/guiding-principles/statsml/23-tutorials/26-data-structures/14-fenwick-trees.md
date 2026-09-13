# Fenwick Trees

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Fenwick Trees

**Subtitle:** A Fenwick tree stores overlapping partial sums inside one flat array, so running totals AND corrections to past values both finish in a few hops

## Two Ledgers, Two Opposite Headaches

**Tags:** `core idea` (blue), `running totals` (green), `updates too` (orange)

- **The shop** — a coffee shop logs sales for 8 days: 5, 3, 7, 2, 6, 4, 1, 8 cups
- **Question A** — the owner keeps asking "how many cups total through day k?"
- **Question B** — the owner also keeps fixing past days ("day 5 was actually 8, not 6")
- **Ledger 1** — a running-totals column answers A instantly, but one fix rewrites every later total
- **Ledger 2** — keeping raw days makes fixes trivial, but every total re-adds up to 8 numbers
- **The trick** — a Fenwick tree keeps overlapping partial sums so BOTH jobs take a few hops

*Example (italic):* Eight days is harmless either way — at a million days, one ledger makes every correction a rewrite and the other makes every total a marathon.

**Key point:** Precomputed totals are fast to read but slow to fix; raw values are fast to fix but slow to total — the Fenwick tree refuses to pick a side.

### Visualization (canvas `c1`, 720×300)

Bar chart of the 8 daily sales with the two naive ledgers' weaknesses called out.

- **Title (bold 15px, `#1a5276`, top center):** "Eight Days of Sales: 5, 3, 7, 2, 6, 4, 1, 8 Cups".
- **Axes:** baseline 2px `#999` at y=235 from x=80 to x=660; y = cups 0 to 8; light `#e5e9ef` gridlines at 2, 4, 6, 8 with 12px `#444` left labels.
- **Bars:** 8 bars 52px wide centered at x = `[120, 194, 268, 342, 416, 490, 564, 638]`, values `[5, 3, 7, 2, 6, 4, 1, 8]`, scaled ~20px per cup; fill `rgba(42,120,214,0.35)`, 2px `#2a78d6` stroke; bold 13px `#1a5276` value labels above each bar; 12px `#444` day labels "day 1"…"day 8" under the baseline at y=253.
- **Two callouts:** bold 12px `#d95926` at (200, 52): "totals ledger: one fix rewrites everything after it"; bold 12px `#4a3aa7` at (490, 74): "raw ledger: every total re-adds the whole row".
- **Caption (12px `#444`, bottom right):** "cup counts illustrative".

## Eight Cells That Each Guard a Stretch of Days

**Tags:** `worked example` (blue), `hand-checkable` (green)

- **The cells** — cell 1 stores day 1; cell 2 stores days 1–2; cell 3 stores day 3; cell 4 stores days 1–4
- **The rest** — cell 5 stores day 5; cell 6 stores days 5–6; cell 7 stores day 7; cell 8 stores days 1–8
- **The values** — from sales 5, 3, 7, 2, 6, 4, 1, 8 the cells hold 5, 8, 7, 17, 6, 10, 1, 36
- **Total through day 6** — cell 6 (days 5–6 = 10) + cell 4 (days 1–4 = 17) = 27: two looks, done
- **A correction** — day 5 becomes 8 (+2): only cells 5, 6, 8 cover day 5, so only they change
- **After the fix** — cells read 5, 8, 7, 17, 8, 12, 1, 38; the other five cells never moved

*Example (italic):* Check it by hand: days 1–6 are 5 + 3 + 7 + 2 + 6 + 4 = 27, exactly what cell 6 + cell 4 said in two looks.

**Key point:** Each cell guards a stretch whose length matches its index's lowest binary bit — so any prefix total, and any fix, touches only a handful of cells.

### Visualization (canvas `c2`, 720×300)

The 8 array cells drawn as brackets spanning the days they cover, with the two cells answering prefix(6) highlighted.

- **Title (bold 15px, `#1a5276`, top center):** "What Each Cell Covers — Total Through Day 6 = Cell 6 + Cell 4".
- **Day ruler:** 8 ticks at x = `[120, 194, 268, 342, 416, 490, 564, 638]` on a 2px `#999` line at y=245, 12px `#444` labels "d1"…"d8" at y=263.
- **Coverage brackets (rounded rectangles, 22px tall, labeled "cell n: value"):** cell 1 over d1 at y=205; cell 2 over d1–d2 at y=175; cell 3 over d3 at y=205; cell 4 over d1–d4 at y=145; cell 5 over d5 at y=205; cell 6 over d5–d6 at y=175; cell 7 over d7 at y=205; cell 8 over d1–d8 at y=115. Each bracket spans its days' tick range ±26px; default 1.5px `#6b7280` stroke, white fill, bold 11px `#2c3e50` label inside: "c1: 5", "c2: 8", "c3: 7", "c4: 17", "c5: 6", "c6: 10", "c7: 1", "c8: 36".
- **Highlights:** cell 6 and cell 4 brackets restroked 2.5px `#008300` with fill `rgba(0,131,0,0.10)` and label color `#008300`.
- **Sum arrows:** 2px `#008300` arrows from cell 6 and cell 4 brackets converging on a bold 14px green result box "17 + 10 = 27" (rounded rect, 2px `#008300` stroke) at (600, 60)–(700, 88).
- **Annotation (bold 12px `#008300`, near x=250, y=70):** "two looks instead of six additions".
- **Caption (12px `#444`, bottom right):** "bracket height = position only; width = days covered".

## Leaderboards That Never Stop Moving

**Tags:** `where it's used` (blue), `log n both ways` (green)

- **The pattern** — any stream of numbers that needs "total (or count) up to here" while values keep changing
- **Leaderboards** — "how many players score below 1,500?" is a prefix count that shifts with every game
- **Event counting** — "orders before 2 pm" under late-arriving fixes is the coffee-shop problem again
- **The cost** — both read and fix touch ~log₂(n) cells: about 20 at a million days, not 500,000
- **The bonus** — it is one flat array: no pointers, no tree objects, cache-friendly and tiny

*Example (italic):* A gaming leaderboard updates a score by fixing one value and re-asks "players below X" — Fenwick answers both in ~20 hops at a million players.

**Key point:** Whenever prefix totals and value updates interleave — sales, scores, event counts — the Fenwick tree makes both operations log-time in one array.

### Visualization (canvas `c3`, 720×300)

Two-line chart of work per operation as the data grows: naive rewrites climb linearly, Fenwick hops stay near the floor; plus the update path for the +2 fix.

- **Title (bold 15px, `#1a5276`, top center):** "Work per Operation as Days Pile Up".
- **Axes:** origin x=80, baseline y=240, plot width 400 (x=80 to 480); x = days with ticks at x = `[120, 200, 280, 360, 440]` labeled "16", "64", "256", "1,024", "4,096" (12px `#444`); y = cells touched 0 to 4,096, light `#e5e9ef` gridlines at 1,024, 2,048, 3,072, 4,096 with 12px `#444` left labels "1,024", "2,048", "3,072", "4,096".
- **Naive line:** orange `#d95926` 3px through `[16, 64, 256, 1024, 4096]` at the ticks; 5px dots; 12px orange label "rewrite the totals ledger" near (200, 120).
- **Fenwick line:** green `#008300` 3px through `[4, 6, 8, 10, 12]`; 5px dots with 12px green value labels above; bold 12px green label "Fenwick: ~log₂(n) hops" near (330, 210).
- **Right inset — update path:** vertical chain at x=590: three rounded cells "c5: 6→8", "c6: 10→12", "c8: 36→38" stacked at y=90, 150, 210 (90×34 each, 2px `#c98500` stroke, bold 12px `#c98500` text), joined by 2px `#c98500` arrows; bold 12px `#c98500` header "+2 to day 5 touches only" at (590, 66); 11px `#6b7280` footer "3 cells of 8" at (590, 258).
- **Caption (12px `#444`, bottom right):** "cells touched per operation — illustrative".

## Fenwick or Segment Tree?

**Tags:** `common mistake` (red), `right tool` (orange)

- **The mix-up** — both answer range questions in log time, so they get treated as the same thing
- **Fenwick** — one array of n cells, tiny code, but its native move is prefix sums from day 1
- **Ranges via prefixes** — days 3–6 = prefix(6) − prefix(2); subtraction works for sums, not for max
- **Segment tree** — ~2n nodes and more code, but answers any range question: sum, min, max, custom
- **Pick by question** — running totals and counts → Fenwick; range min/max or fancier math → segment tree

*Example (italic):* "Total sales days 3–6" is two Fenwick lookups subtracted; "hottest day between 3 and 6" cannot be subtracted — that one needs a segment tree.

**Common mistake:** Reaching for a segment tree when the question is only prefix sums — or forcing max/min into a Fenwick tree, whose subtraction trick only works for invertible operations like addition.

### Visualization (canvas `c4`, 720×300)

Side-by-side comparison panels: what each structure stores and which questions it answers.

- **Title (bold 15px, `#1a5276`, top center):** "Two Log-Time Tools, Different Native Questions".
- **Panels:** rounded rectangles 305×200 at (30, 55) and (385, 55); left stroked 2px `#008300` titled "Fenwick tree" (bold 13px `#008300`), right stroked 2px `#4a3aa7` titled "Segment tree" (bold 13px `#4a3aa7`); titles at panel top centers, y=77.
- **Left panel rows (12px `#2c3e50`, x centered at 182, y = 105/128/151/174):** "one flat array — n cells", "native move: prefix sums", "range sum = two prefixes subtracted", "min/max: no — can't subtract a max".  The last row colored `#e74c3c`.
- **Right panel rows (12px `#2c3e50`, x centered at 537, y = 105/128/151/174):** "a real tree — about 2n nodes", "native move: any range directly", "sum, min, max, custom combines", "cost: more memory, more code". The third row colored `#008300`.
- **Verdict strip:** bold 12px `#c98500` centered at (360, 240): "prefix totals under updates → Fenwick — anything richer per range → segment tree".
- **Footer (11px `#6b7280`, centered x=360, y=283):** "both: ~log₂(n) per operation".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the displayed CSS width × `devicePixelRatio` (sharp-rendering pattern) and scales the context; chart functions are pushed into a `__charts` array, run once, and re-run on window resize debounced 150 ms.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** the sales array `[5, 3, 7, 2, 6, 4, 1, 8]`, the cell values `[5, 8, 7, 17, 6, 10, 1, 36]`, the prefix(6) = 17 + 10 = 27 arithmetic, and the +2 update path (cells 5, 6, 8 → 8, 12, 38) are exact and MUST match between text and charts; the `c3` line values `[16, 64, 256, 1024, 4096]` vs `[4, 6, 8, 10, 12]` are illustrative and labeled so.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
