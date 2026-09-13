# Arrays vs Linked Lists

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Arrays vs Linked Lists

**Subtitle:** An array keeps its items side by side in numbered slots; a linked list scatters them and chains them with next-address notes — and because memory hands out neighbors for free, side by side almost always wins

## One Playlist, Two Ways to Store It

**Tags:** `core idea` (blue), `contiguous vs scattered` (green), `pointers` (orange)

- **The playlist** — a music player keeps a listener's 8 favorite songs (their IDs) in memory
- **The array way** — all 8 IDs sit side by side in one block, 4 bytes each, at addresses 100 to 128
- **The linked-list way** — each ID lands wherever space was free, plus a note holding the next one's address
- **Contiguous pays off** — in the array, song n's address is a formula: 100 + (n − 1) × 4, no searching
- **Scattered means chasing** — in the list, the only road to song 6 is following notes from song 1

*Example (italic):* Song 1 lives at address 100, so song 6 must live at 100 + 5 × 4 = 120 — one multiplication and the player is already there.

**Key point:** An array is one solid block where positions are arithmetic; a linked list is a scavenger hunt where each item merely tells you where the next one hides.

### Visualization (canvas `c1`, 720×300)

Two-band memory-layout diagram: the same 8 songs drawn once as a contiguous block of slots with addresses, and once as scattered boxes chained by arrows.

- **Title (bold 15px, `#1a5276`, top center):** "Same 8 Songs: One Solid Block vs a Scavenger Hunt".
- **Array band:** 12px `#444` label "array" at x=25, y=97; 8 adjacent boxes 55×34 starting at x=90, y=75 (touching edges), fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border; bold 13px `#1a5276` centered labels "S1"–"S8"; 11px `#6b7280` address labels "100", "104", "108", "112", "116", "120", "124", "128" centered under each box.
- **List band:** 12px `#444` label "linked list" at x=25, y=212; 8 boxes 48×28, fill `rgba(217,89,38,0.12)`, 2px `#d95926` border, bold 12px `#d95926` centered labels "S1"–"S8", at hardcoded top-left corners: S1 (95, 190), S2 (395, 250), S3 (215, 250), S4 (585, 195), S5 (300, 185), S6 (500, 250), S7 (160, 250)… use exactly `[[95,190],[395,250],[215,250],[585,195],[300,185],[500,250],[655,250],[455,185]]` for S1–S8.
- **Chain arrows:** 7 arrows S1→S2→…→S8, 2px `#d95926` lines with small filled arrowheads, drawn box-edge to box-edge; crossings are expected — they are the point.
- **Annotation (bold 12px `#d95926`, near x=430, y=165):** "each note stores only the NEXT address".
- **Caption (12px `#444`, bottom right):** "addresses illustrative — 4-byte song IDs".

## Reaching Song 6: One Jump vs Six Hops

**Tags:** `worked example` (blue), `lookup cost` (green)

- **The task** — the listener taps "play song 6"; how much work does each layout do?
- **Array math** — address = 100 + (6 − 1) × 4 = 120; go straight there: 1 step, same for any song
- **List walk** — start at song 1 and follow next-notes, visiting songs 1 through 6: 6 hops in total
- **Growing pain** — in a 1,000-song playlist, song 1,000 is still 1 array step but 1,000 list hops
- **The flip side** — squeezing a new song in after song 3: the list rewrites one note; the array must shift songs 4–8 over

*Example (italic):* To play song 6 the array does one address calculation; the list touches songs 1, 2, 3, 4, 5, 6 — six visits for one song.

**Key point:** Array lookup is a formula — 1 step wherever the item is; list lookup is a walk — as many hops as the position number.

### Visualization (canvas `c2`, 720×300)

Grouped bar chart: for each playlist position 1–8, the steps an array needs (always 1) next to the hops a linked list needs (equal to the position), with position 6 called out.

- **Title (bold 15px, `#1a5276`, top center):** "Steps to Reach Each Song: Array Stays Flat, List Climbs".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = positions 1–8, one group per position, 12px `#444` labels "song 1"–"song 8" below; y = steps 0 to 8, light `#e5e9ef` gridlines at 2, 4, 6, 8 with 12px `#444` labels.
- **Bars per group:** two bars 22px wide, 6px apart; array steps blue `#2a78d6` fill `rgba(42,120,214,0.55)`, values `[1, 1, 1, 1, 1, 1, 1, 1]`; list hops orange `#d95926` fill `rgba(217,89,38,0.55)`, values `[1, 2, 3, 4, 5, 6, 7, 8]`.
- **Callout at position 6:** both bars of the group get a 2px `#1a5276` outline; vertical dashed `#6b7280` (dash 4/3) guide from the group's center up to y=70.
- **Annotation (bold 13px `#1a5276`, near x=330, y=60):** "song 6: array 1 step, list 6 hops".
- **Legend (12px, top left inside plot):** blue swatch "array (1 step anywhere)", orange swatch "linked list (hops = position)".

## Why the Cache Makes Arrays Win

**Tags:** `where it's used` (blue), `cache lines` (green), `rule of thumb` (orange)

- **The chunk rule** — when the CPU asks memory for one byte, memory ships a whole 64-byte chunk (a cache line)
- **Free neighbors** — 64 bytes holds 16 four-byte song IDs, so fetching the array's song 1 delivers songs 1–16 at once
- **Scattered pays full price** — list nodes live far apart, so each of the same 16 songs is its own slow trip
- **The gap** — a cache hit costs about 1 ns, a memory fetch about 100 ns: 1 trip vs 16 is ~100 ns vs ~1,600 ns
- **Where you meet it** — NumPy arrays and pandas columns are contiguous blocks; that layout is much of their speed

*Example (italic):* Reading 16 songs takes the array 1 memory fetch (~100 ns) and the linked list 16 fetches (~1,600 ns) — same data, sixteen times the trips.

**Key point:** Memory is a delivery truck that always brings a 64-byte crate — arrays fill the crate with your next items, linked lists waste it on strangers.

### Visualization (canvas `c3`, 720×300)

Two-band fetch diagram: 16 contiguous array slots covered by a single cache-line bracket, above 16 scattered list nodes that each need their own fetch, with the time cost annotated.

- **Title (bold 15px, `#1a5276`, top center):** "One 64-Byte Fetch Grabs 16 Array Items — the List Needs 16 Fetches".
- **Array band:** 16 adjacent boxes 34×26 starting at x=90, y=85, fill `rgba(42,120,214,0.15)`, 1.5px `#2a78d6` border, 11px `#1a5276` centered labels "1"–"16"; one green `#008300` 2px square bracket spanning all 16 boxes from x=90 to x=634 drawn just above (y=72), bold 12px `#008300` label centered above it: "1 fetch — one 64-byte cache line".
- **List band:** 16 boxes 26×20, fill `rgba(217,89,38,0.12)`, 1.5px `#d95926` border, 11px `#d95926` centered labels "1"–"16", at x = 90 + i × 36 (i = 0…15) with hardcoded scattered y values `[180, 215, 195, 240, 185, 225, 200, 245, 190, 230, 180, 220, 205, 240, 185, 225]`; a short 1.5px `#6b7280` tick above each box (its own fetch); bold 12px `#d95926` label at x=90, y=165: "16 fetches — one slow trip per song".
- **Annotation (bold 13px `#d95926`, bottom center near y=285):** "~100 ns vs ~1,600 ns of waiting for memory".
- **Caption (12px `#444`, bottom right):** "illustrative — cache hit ~1 ns, memory fetch ~100 ns".

## "But Lists Insert Faster" — Mostly a Myth in Practice

**Tags:** `common mistake` (red), `big-O vs reality` (orange)

- **The textbook line** — list insert is O(1) and array insert is O(n), so lists sound like the editing champion
- **The catch** — that O(1) only counts rewiring one note; first you must WALK to the spot, and the walk cache-misses
- **Head-to-head** — inserting mid-way into 10,000 items: the array shifts 5,000 in one contiguous sweep, the list hops 5,000 scattered nodes
- **The clock** — at ~100 ns per scattered hop the list walk costs ~500 µs; the array's contiguous shift runs at copy speed, ~2 µs
- **When lists do win** — when you already hold a pointer to the spot, like a queue popping its own front

*Example (italic):* Both structures insert one song at position 5,000 of 10,000 — the "slow" array finishes in ~2 µs while the "fast" list is still walking at ~500 µs.

**Common mistake:** Reading big-O as a stopwatch. O(1) insert ignores the O(n) walk to find the spot, and it counts a cache-missing hop the same as a contiguous copy — on real hardware the array usually wins.

### Visualization (canvas `c4`, 720×300)

Two horizontal time bars on a shared microsecond axis: the array's mid-insert next to the linked list's, making the size of the practical gap unmissable.

- **Title (bold 15px, `#1a5276`, top center):** "Insert in the Middle of 10,000 Items: Theory Says List, the Clock Says Array".
- **Axis:** horizontal 2px `#999` line at y=245 from x=210 to x=690 (width 480), time 0 to 500 µs; 12px `#444` tick labels "0", "100", "200", "300", "400", "500 µs" every 100 µs; light `#e5e9ef` vertical gridlines at each tick.
- **Row 1 (bar centered y=110), 12px `#444` two-line label at x=20:** "array — shift 5,000 items" / "(one contiguous sweep)"; green `#008300` bar from 0 to 2 µs, 26px tall, fill `rgba(0,131,0,0.45)` — draw at least 3px wide so it stays visible; bold 12px `#008300` value label right of the bar: "≈2 µs".
- **Row 2 (bar centered y=180), label at x=20:** "linked list — walk 5,000" / "scattered nodes, then relink"; orange `#d95926` bar from 0 to 500 µs, 26px tall, fill `rgba(217,89,38,0.45)`; bold 12px `#d95926` value label just inside the bar's right end: "≈500 µs".
- **Annotation (bold 13px magenta `#d55181`, near x=300, y=70):** "finding the spot dominates — the 'slow' array wins by ~250×".
- **Caption (12px `#444`, bottom right):** "illustrative — ~100 ns per cache miss, contiguous copy at memcpy speed".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all box positions, bar values, and step counts are the hardcoded literal arrays above (no randomness); addresses follow 100 + (n − 1) × 4; timing numbers (1 ns, 100 ns, 1,600 ns, 2 µs, 500 µs) are illustrative and must match between text and charts.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
