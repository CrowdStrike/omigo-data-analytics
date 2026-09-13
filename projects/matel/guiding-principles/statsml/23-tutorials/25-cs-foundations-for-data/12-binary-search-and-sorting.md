# Binary Search & Sorting

**Page type:** detail page (tutorial card-sections, two-column layout: text left 50%, canvas right 50%, one table row per section)
**HTML title tag:** Binary Search & Sorting

**Subtitle:** Finding a word in a dictionary by opening the middle — halve the pages each look, and a million entries fall in 20 steps

## Finding "Marmalade" in a 1,000-Page Dictionary

**Tags:** core idea (blue), running example (green)

- **The task** — find "marmalade" in a 1,000-page dictionary with no thumb tabs
- **Nobody scans page 1** — you open the middle, see "N" words, and know M comes earlier
- **Halve** — every look keeps only the half of the pages that can still hold the word
- **10 looks** — 1,000 → 500 → 250 → 125 → 63 → 32 → 16 → 8 → 4 → 2 → 1
- **The name** — this move is binary search: check the middle, throw away half, repeat

*Example:* Open at "N", flip back to "K", forward to "M" — three looks already cut 1,000 pages to 125.

**Key point:** Binary search only works because the dictionary is sorted — on shuffled pages, nothing beats checking every page.

### Visualization (canvas `c1`, 720×300)

Halving staircase bar chart: pages still in play after each look.

- **Title (bold 15px, `#1a5276`, top center):** "Pages Still in Play After Each Look (1,000-page dictionary)"
- **Data:** 11 bars for looks 0–10 with values `[1000, 500, 250, 125, 63, 32, 16, 8, 4, 2, 1]`; each bar 40px wide, evenly spaced, value scale max 1000 over 172px height, min bar height 3px, baseline at y=238 with gray `#999` axis line.
- **Bar colors** (0.75 alpha fill): first bar blue `#2a78d6`, final bar (value 1) green `#008300`, all others aqua `#199e70`.
- **Labels:** value in 12px `#444` above each bar; look index (0–10) in gray `#6b7280` below each bar; x-axis caption "looks so far" in 12px `#444` centered below.
- **Annotations:** bold 13px orange `#d95926` at ~62% width: "10 looks: 1,000 pages narrowed to 1"; bold 12px green `#008300` at the right end: "found it".

## One ID Out of 1,000,000 in 20 Steps

**Tags:** worked example (green), rule of thumb (blue)

- **The setup** — a sorted list of 1,000,000 customer IDs; is ID 738204 in it?
- **Step 1** — check the middle (position 500,000): its ID is too small, keep the top half
- **Keep halving** — 1,000,000 → 500,000 → 250,000 → ... → 1: exactly 20 halvings
- **Scanning instead** — checking rows one by one averages 500,000 checks
- **The rule** — steps ≈ log₂(N): 1,000 needs 10, 1 million 20, 1 billion 30
- **Doubling is cheap** — twice the data costs ONE extra step, not twice the work

*Example:* On 1 billion sorted rows, a scan averages 500 million checks; binary search needs 30.

**Key point:** 20 checks vs 500,000 is a 25,000x gap — and it keeps widening as the data grows.

### Visualization (canvas `c2`, 720×300)

Grouped bar chart on a log-scale y-axis: checks needed, scan vs binary search.

- **Title (bold 15px, `#1a5276`, top center):** "Checks Needed: Scan Every Row vs Binary Search (log scale)"
- **Groups** (three, each with a magenta scan bar and a blue binary-search bar, 62px wide, 0.7 alpha fill; bar height = log10(value)/9 of 175px chart height, min 4px, baseline y=240):
  - "1 thousand rows" — scan 500 (label "500"), binary 10
  - "1 million rows" — scan 500,000 (label "500,000"), binary 20
  - "1 billion rows" — scan 500,000,000 (label "500 million"), binary 30
- **Y gridlines** (light `#e5e9ef`, labels 12px gray `#6b7280` right-aligned): 1, 1k, 1M, 1B; gray `#999` baseline.
- **Value labels:** bold 12px above each bar in the bar's color — magenta `#d55181` for scan, blue `#2a78d6` for binary search; group labels in 12px `#444` below the baseline.
- **Legend (right side):** magenta swatch "scan (average)", blue swatch "binary search" (12px, `#222`).
- **Annotation (right side, bold 13px orange `#d95926`, three lines):** "1M items:" / "20 steps," / "not 500,000"

## Sorting: Pay Once, Search Cheap Forever

**Tags:** core idea (blue), rule of thumb (blue)

- **The cost** — sorting 1,000,000 IDs takes about 20 million operations (N × log₂N)
- **Sounds expensive** — that is 40 unsorted scans' worth of work, paid one time
- **After that** — every search costs 20 steps instead of 500,000
- **Break-even** — past ~40 searches, sorting first was the cheaper plan; after that it's free speed
- **Everyday version** — contacts stay alphabetical for the same reason: sort once, look up daily

*Example:* 40 lookups on unsorted rows cost 20 million checks — the price of sorting once, after which each lookup is 20.

**Key point:** Sorting is an investment: N log N paid once buys log N per search for the rest of the data's life.

### Visualization (canvas `c3`, 720×300)

Line chart: cumulative total work vs number of searches run, with break-even point.

- **Title (bold 15px, `#1a5276`, top center):** "Total Work vs Number of Searches (1,000,000 rows)"
- **Axes:** x from 0 to 100 searches (ticks every 20, caption "searches run so far" in `#444`); y from 0 to 50M operations with gridlines every 10M (labels 0, 10M, 20M, 30M, 40M, 50M in gray `#6b7280`); gray `#999` L-shaped axis frame. Padding: top 48, bottom 52, left 80, right 165.
- **Lines (width 3):**
  - "never sort" — magenta `#d55181` straight line from (0, 0) to (100, 50,000,000): 500k operations per search.
  - "sort once" — blue `#2a78d6` nearly flat line from (0, 20,000,000) to (100, 20,002,000): 20M sort cost plus 20 per search.
- **Break-even marker:** dashed orange `#d95926` vertical line (dash 5/4, width 1.5) at x=40 up to the blue line, with a filled orange dot (radius 5) at (40, 20M) and bold 13px orange label "break-even: ~40 searches".
- **Legend (right side, 12px `#222`):** magenta swatch "never sort:" / "500k / search"; blue swatch "sort once (20M)," / "then 20 / search".

## Your Database Index Is This Exact Trick

**Tags:** where it's used (blue), watch out (orange)

- **CREATE INDEX** — the database builds a sorted structure (a B-tree) on that column: the pay-once sort
- **Indexed WHERE** — `WHERE customer_id = 738204` hops root → branch → leaf: 3 page reads
- **No index** — the same query reads all 10,000,000 rows off disk
- **Why inserts slow down** — each new row must be filed into the sorted structure, like a new dictionary word
- **When to index** — columns you filter on often earn the sort; ones you never search don't

*Example:* EXPLAIN says "full table scan, 10,000,000 rows" before the index and "index lookup, 3 reads" after.

**Key point:** An index is not magic — it is a maintained sorted copy of one column, so lookups can halve instead of scan.

### Visualization (canvas `c4`, 720×300)

Two-panel diagram (vertical dashed gray divider `#bdc3c7`, dash 4/3, at x=360): B-tree index lookup on the left, full table scan on the right.

- **Title (bold 15px, `#1a5276`, top center):** "Same Query, Two Plans: Index Hops vs Full Scan"
- **Left panel (B-tree):** three levels of boxes — one "root" box (78×26px) at top, three "branch" boxes in a middle row, four "leaf" boxes (66×26px) in a bottom row. Boxes on the lookup path (root → middle branch → third leaf) highlighted: fill `rgba(42,120,214,0.25)`, blue `#2a78d6` stroke width 2, bold navy `#1a5276` labels; other boxes `#f4f6f8` fill, `#b9c2cc` stroke, gray `#6b7280` labels. Blue arrows (width 2, filled arrowheads) connect root → middle branch → third leaf.
  - Below tree, bold 13px green `#008300`: "ID 738204 found: 3 page reads"; then 12px gray: "each hop halves-and-more the remaining rows".
- **Right panel (full scan):** label (bold 12px `#444`): "no index: read every block"; a 12×5 grid of small blocks (24×24px, fill `rgba(213,81,129,0.30)`, stroke `rgba(213,81,129,0.5)`); below, bold 14px magenta `#d55181`: "... 10,000,000 rows read".
- **Takeaway (bottom right, bold 13px orange `#d95926`):** "3 reads vs 10,000,000 — the index is a pre-paid sort"

## Regeneration instructions

- **Template:** tutorials topic-page layout (social-graph reference style): `<h1>` (no index number), `.subtitle`, then four `.card-section` blocks each with an `<h2>` and a `table.layout` with one `<tr>`: left `<td class="text-col">` (50%) holding `.tags` pills, a `<ul>` of one-line bullets with `<b>` lead terms, an italic `.example` line, and a `.key-point` callout; right `<td class="viz-col">` (50%) holding one canvas.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with 2px `#2980b9` bottom border; section h2 1.3rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem. Bullets 0.92rem, `li b` in `#1a5276`; inline `code` in ui-monospace on `#f4f6f8`. `.example` italic `#555` 0.9rem. `.key-point` background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem.
- **Tag pills:** `.tag` inline-block, 0.72rem bold, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Canvas:** intrinsic 720×300, CSS `width:100%`, 1px `#e0e0e0` border, radius 4px; scaled with `window.devicePixelRatio` via a shared `setup(id)` helper (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates).
- **Chart palette (`P` object):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- No nav bar, no back/home links, no cross-page links. In regenerated HTML any card links would use `.html` extensions.
