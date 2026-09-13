# Heaps & Priority Queues

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Heaps & Priority Queues

**Subtitle:** A heap is a loosely ordered pile that keeps exactly one promise — here, in a min-heap, the smallest item is always on top (the mirror-image max-heap keeps the largest on top) — so "what's next?" is answered instantly no matter how the items arrived

## The Kitchen Board That Always Knows the Next Order

**Tags:** `core idea` (blue), `smallest on top` (green), `priority queue` (orange)

- **The kitchen** — a delivery kitchen has five open orders, each promised in some number of minutes
- **The question** — the cook only ever asks one thing: which order is due soonest right now?
- **The pile** — a heap is a loosely ordered pile with one promise: the smallest item sits on top
- **Priority queue** — the job description: accept items in any order, hand back the most urgent first
- **Cheap peek** — reading the soonest due time never means searching; it is always sitting on top

*Example (italic):* Orders due in 12, 7, 15, 3, and 9 minutes sit in the pile; the top says 3 without anyone scanning the board.

**Key point:** A heap keeps just enough order — smallest on top — so "what's next?" costs one glance, not one search.

### Visualization (canvas `c1`, 720×300)

Single-panel bar chart: the five open orders in arrival order with their minutes-until-due as bar heights, the soonest order highlighted green as the top of the heap.

- **Title (bold 15px, `#1a5276`, top center):** "Five Open Orders — the Heap's Top Is Always the Soonest".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 190; y axis = minutes until due, 0 to 16, light `#e5e9ef` gridlines at 4, 8, 12, 16 with 12px `#444` labels "4", "8", "12", "16 min".
- **Bars (width 70, centered at x = `[120, 240, 360, 480, 600]`, arrival order):** values `[12, 7, 15, 3, 9]` minutes, labels below the baseline 12px `#444`: "burger", "salad", "pizza", "soup", "noodles"; fill `rgba(42,120,214,0.35)`, 2px `#2a78d6` border — except "soup" (value 3): fill `rgba(0,131,0,0.35)`, 2px `#008300` border.
- **Value labels:** bold 13px above each bar in the bar's border color: "12", "7", "15", "3", "9".
- **Annotation (bold 13px green `#008300`, above the soup bar, arrow down to it):** "due soonest — always on top".
- **Caption (12px `#444`, bottom right):** "illustrative — one kitchen's open orders".

## Five Orders In, One Order Out — by Hand

**Tags:** `worked example` (blue), `sift up / sift down` (green)

- **Arrivals** — due times land one at a time in the order 12, 7, 15, 3, 9 minutes
- **Insert rule** — a new order starts at the bottom and swaps upward while smaller than its parent
- **After five inserts** — the tree reads 3 on top, then 7 and 15 below, then 12 and 9 underneath
- **Pop rule** — remove the top (3), lift the last item (9) to the top, swap it down past smaller children
- **After the pop** — 9 swaps with 7 and stops; the new top is 7, the next-soonest order
- **Cost** — each insert or pop walks one path of the tree: about log2(n) swaps, never all n items

*Example (italic):* Inserting 3 costs two swaps (past 12, then past 7); popping it costs one swap (9 sinks past 7).

**Key point:** After inserting 12, 7, 15, 3, 9 the heap is [3, 7, 15, 12, 9]; pop the 3 and one swap repairs it to [7, 9, 15, 12].

### Visualization (canvas `c2`, 720×300)

Two small trees side by side on one canvas: the heap after all five inserts (left) and the repaired heap after popping the smallest (right), with an arrow between them and each tree's flat array printed underneath.

- **Title (bold 15px, `#1a5276`, top center):** "Insert 12, 7, 15, 3, 9 — Then Pop the Smallest".
- **Node style:** circles radius 20, 2px `#1a5276` stroke, fill `rgba(42,120,214,0.15)`, values bold 14px `#1a5276` centered; edges 2px `#6b7280` lines drawn before nodes.
- **Left tree (after inserts):** root at (190, 95) value 3 — this node filled `rgba(0,131,0,0.25)` with 2px `#008300` stroke; children at (120, 165) value 7 and (260, 165) value 15; grandchildren at (85, 235) value 12 and (155, 235) value 9.
- **Right tree (after pop):** root at (530, 95) value 7; children at (460, 165) value 9 and (600, 165) value 15; one grandchild at (425, 235) value 12.
- **Arrow between trees:** 3px `#d95926` horizontal arrow from (310, 160) to (400, 160) with arrowhead; bold 12px `#d95926` two-line label above it: "pop 3;" / "9 sifts down".
- **Array captions:** 12px `#6b7280` centered under each tree at y=285: left "[3, 7, 15, 12, 9]", right "[7, 9, 15, 12]".
- **Annotation (bold 12px green `#008300`, next to the left root):** "smallest on top".
- **Caption (11px `#444`, bottom right):** "illustrative order times, minutes until due".

## Why Not Just Rescan the List Every Time?

**Tags:** `where it's used` (blue), `cost of a question` (orange)

- **The naive way** — keep a plain list and scan all n orders every time the cook asks what's next
- **The heap way** — pay about log2(n) swaps per insert or pop, and the top is always ready to read
- **At 4,096 orders** — a scan is 4,096 steps per question; the heap answers in about 12 swaps
- **Where it lives** — OS task schedulers, Dijkstra's shortest path, event simulators, top-k streams
- **Streaming top-k** — keep a small heap of the k best items seen so far; evict the top when beaten

*Example (italic):* Growing the kitchen from 8 to 4,096 open orders multiplies scan work by 512 but heap work only from 3 swaps to 12.

**Key point:** Scan cost grows with the list length (n steps); heap cost grows with the tree height (about log2 n) — at 4,096 items that is 4,096 versus 12.

### Visualization (canvas `c3`, 720×300)

Grouped bar chart on a logarithmic y axis: steps needed to find the next order at four list sizes, scan-the-list versus heap, showing the gap exploding as n grows.

- **Title (bold 15px, `#1a5276`, top center):** "Steps to Find the Next Order: Scan the List vs Heap".
- **Axes:** origin x=70, baseline y=245, plot width 580; y is log10 scale, pixel rule y = 245 − 50·log10(steps); light `#e5e9ef` gridlines at 10 (y=195), 100 (y=145), 1,000 (y=95) with 12px `#444` labels "10", "100", "1,000 steps".
- **Groups (centers at x = `[150, 290, 430, 570]`):** list sizes `[8, 64, 512, 4096]`, 12px `#444` labels below the baseline: "8 orders", "64", "512", "4,096".
- **Scan bars (left of each center, width 40, gap 8):** values `[8, 64, 512, 4096]` steps → heights `[45, 90, 135, 181]`px; fill `rgba(217,89,38,0.35)`, 2px `#d95926` border; bold 12px `#d95926` value labels above: "8", "64", "512", "4,096".
- **Heap bars (right of each center, width 40):** values `[3, 6, 9, 12]` swaps → heights `[24, 39, 48, 54]`px; fill `rgba(0,131,0,0.35)`, 2px `#008300` border; bold 12px `#008300` value labels above: "3", "6", "9", "12".
- **Legend (12px, top left inside plot):** orange swatch "scan whole list", green swatch "heap pop".
- **Annotation (bold 13px green `#008300`, upper right near x=560, y=70):** "4,096 orders: 12 swaps, not 4,096 steps".
- **Caption (11px `#444`, bottom right):** "y axis logarithmic; idealized step counts, illustrative".

## A Heap Is Not a Sorted List

**Tags:** `common mistake` (red), `array layout` (orange)

- **The array** — the heap [3, 7, 15, 12, 9] is stored flat, level by level, with no pointers at all
- **Family math** — the parent of slot i lives at slot (i−1)/2 rounded down; children at 2i+1 and 2i+2
- **Not sorted** — 15 sits before 12 and 9 in the array; only parent-child pairs are ordered
- **Only the top** — the single guarantee is slot 0 holds the smallest; slots 1 and 2 can go either way
- **The trap** — printing the array and expecting sorted output; sorted order needs n pops, not one look

*Example (italic):* [3, 7, 15, 12, 9] is a perfectly valid heap, yet read left to right it is nowhere near 3, 7, 9, 12, 15.

**Common mistake:** Expecting the heap array to be sorted. It only promises slot 0; to get the full sorted order you must pop n times — that repeated popping is exactly heapsort.

### Visualization (canvas `c4`, 720×300)

Two rows of value boxes on one canvas: the heap's flat array on top and the fully sorted order below, with the mismatching slots outlined so the "loose middle" is visible.

- **Title (bold 15px, `#1a5276`, top center):** "Same Five Numbers, Two Layouts: Heap Array vs Sorted".
- **Box style:** 80×50 rectangles with left edges at x = `[150, 250, 350, 450, 550]`, values bold 15px centered, 4px corner radius.
- **Row 1 (boxes at y=85):** 12px `#444` label at (40, 75): "the heap array (level by level)"; values `[3, 7, 15, 12, 9]`; slot 0 filled `rgba(0,131,0,0.15)` with 2px `#008300` border and bold 12px green label above: "guaranteed smallest"; slots 1–4 filled `rgba(42,120,214,0.15)` with 2px `#2a78d6` border; 11px `#6b7280` index labels under each box: "slot 0" … "slot 4".
- **Row 2 (boxes at y=200):** 12px `#444` label at (40, 190): "fully sorted order (what people expect)"; values `[3, 7, 9, 12, 15]`; fill `#f4f6f8`, 2px `#6b7280` border.
- **Mismatch marks:** dashed 2px `#d55181` outlines (dash 5/3) drawn 4px outside the row-1 boxes at slots 2 and 4, where the heap (15, 9) disagrees with the sorted order (9, 15).
- **Annotation (bold 13px magenta `#d55181`, right side near x=560, y=165):** "only slot 0 is promised — the middle is loose".
- **Caption (11px `#444`, bottom right):** "illustrative — same numbers as the worked example".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all bar values, node values, arrays, and step counts are the hardcoded literals above (no randomness); the worked-example numbers 12, 7, 15, 3, 9 and the arrays [3, 7, 15, 12, 9] and [7, 9, 15, 12] must appear identically in text and charts; c3 bar heights follow the stated log10 pixel rule.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
