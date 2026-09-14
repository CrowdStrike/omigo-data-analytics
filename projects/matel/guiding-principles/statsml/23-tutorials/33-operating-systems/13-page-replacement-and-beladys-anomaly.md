# Page Replacement & Belady's Anomaly

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Page Replacement & Belady's Anomaly

**Subtitle:** When memory is full something must be evicted — and under FIFO, giving the system MORE memory can make page faults go up, not down

## The Library Desk With Three Slots

**Tags:** `core idea` (blue), `caching` (green), `page fault` (orange)

- **The desk** — a librarian keeps only 3 books on the desk; the full collection lives in the stacks
- **A hit** — a reader asks for a book already on the desk; handing it over takes 5 seconds
- **A fault** — the book is not on the desk, so the librarian walks to the stacks: 180 seconds
- **The eviction** — the desk is full, so one book must go back before the new one can sit down
- **The policy** — FIFO returns whichever book has sat on the desk longest, ignoring how often it's read

*Example (italic):* A morning of 12 requests with 9 faults costs 27 minutes of walking to the stacks; the 3 desk hits cost 15 seconds combined.

**Key point:** Page replacement is this desk: RAM holds a few pages, disk holds the rest, and when memory is full an eviction policy decides which page leaves to make room.

### Visualization (canvas `c1`, 720×300)

Flow diagram of one request (hit path vs fault path) above a two-bar cost comparison of hit time vs fault time.

- **Title (bold 15px, `#1a5276`, top center):** "One Request: the 5-Second Hit vs the 180-Second Fault".
- **Flow row:** blue `#2a78d6` rounded box at (30, 70) labeled "reader asks for book" (12px `#2c3e50`); 3px `#6b7280` arrow to an ink `#1a5276` box at (250, 70) labeled "on the desk?"; green `#008300` 3px arrow up-right to a green-tinted box at (470, 45) labeled "hand it over — 5s"; orange `#d95926` 3px arrow down-right to an orange-tinted box at (470, 115) labeled "return oldest, walk to stacks — 180s".
- **Box style:** 150–190px wide, 36px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(0,131,0,0.12)` / `rgba(217,89,38,0.12)`, 12px `#2c3e50` text.
- **Cost bars (baseline x=170, rows at y=205 and y=240, 16px tall):** left-aligned 12px `#444` labels at x=20 "hit: 5s" and "fault: 180s"; green `#008300` bar width 12 (5s at 2.4 px/s); orange `#d95926` bar width 432 (180s at 2.4 px/s); 11px `#444` value labels at bar ends.
- **Annotation (bold 13px orange `#d95926`, near x=250, y=190):** "one fault costs 36 hits — evictions decide everything".
- **Caption (12px `#444`, bottom right):** "service times illustrative".

## Twelve Requests: Three Slots Beat Four

**Tags:** `worked example` (blue), `FIFO` (orange), `Belady's anomaly` (red)

- **The stream** — the day's requests arrive as books 1 2 3 4 1 2 5 1 2 3 4 5
- **Three slots** — FIFO faults 9 times; after request 7 the desk holds [1, 2, 5], so 1 and 2 hit
- **Four slots** — FIFO faults 10 times; request 7 (book 5) evicts book 1 right before requests 8–9 want 1 and 2
- **Hand-check** — 3 slots: hits at requests 8, 9, 12; 4 slots: hits at requests 5 and 6 only
- **The anomaly** — adding a slot raised faults from 9 to 10: more memory, more trips to the stacks

*Example (italic):* With 3 slots the librarian walks to the stacks 9 times; the bigger 4-slot desk sends her 10 times on the exact same request stream.

**Key point:** This is Belady's anomaly (1969): under FIFO replacement, adding memory frames can increase the number of page faults on the same reference string.

### Visualization (canvas `c2`, 720×300)

Cumulative fault count over the 12 requests, one line per desk size, showing the 4-slot line finishing above the 3-slot line.

- **Title (bold 15px, `#1a5276`, top center):** "Same 12 Requests: 3 Slots Fault 9 Times, 4 Slots Fault 10".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = request 1 to 12 with 12px `#444` tick labels "1 2 3 4 1 2 5 1 2 3 4 5" (the book asked for at each step); y = cumulative faults 0 to 12, gridlines `#e5e9ef` at 3/6/9.
- **3-slot line:** blue `#2a78d6` 3px line with 4px dots through requests `[1..12]`, cumulative faults `[1, 2, 3, 4, 5, 6, 7, 7, 7, 8, 9, 9]` — flat at requests 8, 9, 12 (the hits).
- **4-slot line:** orange `#d95926` 3px line with 4px dots, cumulative faults `[1, 2, 3, 4, 4, 4, 5, 6, 7, 8, 9, 10]` — flat only at requests 5 and 6, then faults every step.
- **Legend (12px, top left inside plot):** blue "3 slots", orange "4 slots".
- **Annotation (bold 13px red `#e74c3c`, near request 11, y=75):** "the bigger desk ends worse: 10 vs 9".
- **Caption (12px `#444`, bottom right):** "FIFO fault counts exact for this stream".

## Every Cache You Will Ever Size

**Tags:** `where it's used` (blue), `cache sizing` (green), `stack algorithms` (orange)

- **Everywhere** — the same choice runs OS memory, database buffer pools, CDN edges, and feature caches
- **The question** — "how much cache do we need?" quietly assumes more cache never hurts; FIFO breaks that
- **Stack algorithms** — LRU and optimal keep everything a smaller cache would keep, so faults only fall
- **FIFO is not one** — its queue order depends on cache size, so contents differ between sizes
- **Same stream** — LRU faults 12, 12, 10, 8, 5 across 1–5 slots on this string; it never rises

*Example (italic):* Growing the cache from 3 to 4 slots on this stream raises FIFO faults 9 → 10, while the same growth under LRU lowers them 10 → 8.

**Key point:** Only stack algorithms (LRU, optimal) guarantee a bigger cache is never worse; a FIFO-style cache benchmarked at one size proves nothing about another size.

### Visualization (canvas `c3`, 720×300)

Line chart of total faults vs number of desk slots for FIFO and LRU on the same 12-request stream, with the FIFO bump at 4 slots highlighted.

- **Title (bold 15px, `#1a5276`, top center):** "Faults vs Cache Size: FIFO Bumps Up, LRU Never Does".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = slots 1 to 5, 12px `#444` tick labels; y = faults 0 to 14, gridlines `#e5e9ef` at 4/8/12.
- **FIFO line:** orange `#d95926` 3px line with 5px dots through slots `[1, 2, 3, 4, 5]`, faults `[12, 12, 9, 10, 5]`; the dot at 4 slots drawn red `#e74c3c` and 7px with 12px red label "9 → 10".
- **LRU line:** green `#008300` 3px line with 5px dots, faults `[12, 12, 10, 8, 5]` — monotone non-increasing.
- **Legend (12px, top right inside plot):** orange "FIFO", green "LRU".
- **Annotation (bold 13px green `#008300`, near slots=2.6, y=180):** "LRU is a stack algorithm — more slots never hurt".
- **Caption (12px `#444`, bottom right):** "fault counts exact for the stream 1 2 3 4 1 2 5 1 2 3 4 5".

## "Just Add Memory" Is Not Always a Fix

**Tags:** `common mistake` (red), `eviction order` (orange)

- **The reflex** — a thrashing system gets more RAM; under FIFO the fault rate can rise instead of fall
- **Why it happens** — a bigger FIFO queue holds pages longer, so the "oldest" page can be the hottest one
- **Step 7 replay** — the 3-slot desk holds [1, 2, 5] so 1 and 2 hit; the 4-slot desk just evicted book 1
- **Recency wins** — LRU evicts the coldest book instead of the oldest, and the anomaly disappears
- **The lesson** — measure the fault rate at the actual new size; never extrapolate from one benchmark

*Example (italic):* An engineer doubles a FIFO cache after a single load test and ships; the anomaly is rare in practice but real, and only the policy — not the added size — rules it out.

**Common mistake:** Assuming cache misses fall monotonically as memory grows. That guarantee belongs to the eviction policy (stack algorithms like LRU), not to the memory you add.

### Visualization (canvas `c4`, 720×300)

Two-row snapshot of both desks just after request 7 (book 5 arrives), showing why the bigger desk evicted exactly the books needed next.

- **Title (bold 15px, `#1a5276`, top center):** "After Request 7: What Each Desk Kept, and What Comes Next".
- **Row 1 (y=95), label 12px `#444` at x=20:** "3 slots"; three blue `#2a78d6` rounded boxes at x=110/175/240 labeled "1", "2", "5" (bold 14px); mute `#6b7280` dashed outline box at x=320 labeled "4 sent back"; bold 12px green `#008300` text at x=440: "next asks: 1, 2 → both hits ✓".
- **Row 2 (y=205), label:** "4 slots"; four blue boxes at x=110/175/240/305 labeled "2", "3", "4", "5"; red `#e74c3c` dashed outline box at x=385 labeled "1 sent back"; bold 12px red text at x=470: "next asks: 1 → fault, 2 → fault ✗".
- **Box style:** 55px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` for held books, transparent with dashed 2px border for evicted books, 14px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=270):** "the bigger desk threw away exactly the books needed next".
- **Caption (12px `#444`, bottom right):** "desk contents exact for FIFO on this stream".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the reference string `1 2 3 4 1 2 5 1 2 3 4 5`, the cumulative FIFO fault arrays (`[1,2,3,4,5,6,7,7,7,8,9,9]` for 3 slots, `[1,2,3,4,4,4,5,6,7,8,9,10]` for 4), the faults-per-size arrays (FIFO `[12,12,9,10,5]`, LRU `[12,12,10,8,5]`), and the after-request-7 desk contents (`[1,2,5]` vs `[2,3,4,5]`) are exact simulation results for FIFO/LRU on that string; the 5s hit / 180s fault service times are invented and labeled illustrative.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
