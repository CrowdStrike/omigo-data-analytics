# Memory Fragmentation

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Memory Fragmentation

**Subtitle:** A program can have plenty of free memory and still fail to allocate — the free space is chopped into small scattered pieces that no single request fits into

## The Coffee Counter With Six Empty Seats

**Tags:** `core idea` (blue), `free but unusable` (orange), `contiguous space` (green)

- **The counter** — a coffee shop has one straight counter with 20 seats, numbered 1 to 20
- **The rule** — a party must sit together in adjacent seats, no splitting across the room
- **The churn** — parties of 2, 3, and 4 come and go all morning, each leaving from wherever they sat
- **The gaps** — by 10am seats 3, 4, 9, 14, 15, and 20 are empty: 6 free seats in four scattered gaps
- **The failure** — a party of 4 walks in and is turned away: the largest run of adjacent free seats is 2

*Example (italic):* The barista counts 6 empty seats but still turns the party of 4 away — no 4 of those seats touch each other.

**Key point:** Memory fragmentation is the same failure inside a computer: after many allocations and frees, free memory ends up scattered in small holes, so a large request fails even though the total free space would cover it.

### Visualization (canvas `c1`, 720×300)

Seat-strip diagram of the 20-seat counter at 10am: occupied seats vs free seats, with the scattered free runs bracketed and the turned-away party annotated.

- **Title (bold 15px, `#1a5276`, top center):** "6 Seats Free, But a Party of 4 Cannot Sit Together".
- **Geometry:** 20 seat boxes in one row; seat i (1–20) at x = 50 + (i−1)×31, width 28px, y=125, height 55px, 3px corner radius; seat numbers 1–20 in 11px `#6b7280` centered under each box at y=198.
- **Occupied seats (hardcoded):** seats `[1, 2, 5, 6, 7, 8, 10, 11, 12, 13, 16, 17, 18, 19]`, fill `rgba(42,120,214,0.30)`, 1.5px `#2a78d6` border.
- **Free seats (hardcoded):** seats `[3, 4, 9, 14, 15, 20]`, fill `rgba(0,131,0,0.15)`, 1.5px `#008300` border, 11px `#008300` "free" label inside each.
- **Run brackets:** bold 12px `#008300` labels above the free runs at y=112: "run of 2" over seats 3–4, "1" over seat 9, "run of 2" over seats 14–15, "1" over seat 20.
- **Annotation (bold 13px orange `#d95926`, centered near y=250):** "party of 4 turned away — 6 seats free, largest run is only 2".
- **Caption (12px `#444`, bottom right):** "seat layout illustrative".

## Freeing 16 KB and Still Failing a 12 KB Request

**Tags:** `worked example` (blue), `allocate and free` (green)

- **The heap** — a program manages a 64 KB heap and allocates, in order: A 16 KB, B 8 KB, C 16 KB, D 8 KB, E 16 KB
- **Full house** — 16 + 8 + 16 + 8 + 16 = 64 KB, so the heap is completely full with zero waste
- **Two frees** — the program frees B and D: 8 + 8 = 16 KB free, but in two separate 8 KB holes
- **The request** — a 12 KB allocation arrives: 16 KB free in total, yet neither 8 KB hole can hold 12 KB
- **Hand-check** — 12 ≤ 16 (total passes) but 12 > 8 and 12 > 8 (each hole fails), so the allocation fails

*Example (italic):* The allocator reports 16 KB free, then returns out-of-memory on a 12 KB request — both statements are true at once.

**Key point:** An allocation needs one contiguous hole at least as big as the request; the sum of the holes is irrelevant, so 16 KB split as 8+8 cannot serve 12 KB.

### Visualization (canvas `c2`, 720×300)

Two horizontal memory strips: the heap fully allocated (top) and the heap after freeing B and D (bottom), with the failed 12 KB request drawn to scale against an 8 KB hole.

- **Title (bold 15px, `#1a5276`, top center):** "64 KB Heap: 16 KB Free in Two 8 KB Holes Fails a 12 KB Request".
- **Geometry:** both strips start at x=90, total width 600px mapped to 64 KB (9.375 px/KB), strip height 40px; row labels 12px `#444` at x=15: "all allocated" beside the top strip (y=85) and "B, D freed" beside the bottom strip (y=175).
- **Top strip blocks (hardcoded, left to right):** A 150px, B 75px, C 150px, D 75px, E 150px; A/C/E fill `rgba(42,120,214,0.30)` with 1.5px `#2a78d6` border, B/D fill `rgba(74,58,167,0.20)` with 1.5px `#4a3aa7` border; 12px `#2c3e50` labels "A 16 KB", "B 8 KB", "C 16 KB", "D 8 KB", "E 16 KB" centered in each block.
- **Bottom strip blocks:** same widths and positions; A/C/E as above; B and D become holes with fill `rgba(0,131,0,0.15)`, dashed 1.5px `#008300` border (dash 4/3), 12px `#008300` label "8 KB hole" in each.
- **Failed request:** dashed 2px `#d95926` rectangle (dash 5/3) 113px wide (12 KB to scale) drawn at y=235 starting at the left edge of hole B (x=240), 26px tall, bold 12px `#d95926` label "12 KB request — fits in neither hole" to its right.
- **Annotation (bold 13px green `#008300`, above the bottom strip near x=430, y=165):** "free total 16 KB, largest hole 8 KB".
- **Caption (12px `#444`, bottom right):** "block sizes exact, layout illustrative".

## Why Long-Running Jobs Slowly Eat RAM

**Tags:** `where it's used` (blue), `long-running process` (green), `restart fixes it` (orange)

- **The ratchet** — a data service's memory footprint climbs at every burst and never comes back down
- **The reason** — freed objects leave holes the allocator cannot return to the OS, so the footprint ratchets up
- **The numbers** — after 8 hours a job holds 7.5 GB from the OS while its live data is back down to 2 GB
- **GPU flavor** — deep-learning jobs hit "out of memory" on the GPU while the free-memory counter shows gigabytes spare
- **The blunt fix** — restarting the process hands back one clean contiguous block, which is why nightly restarts "cure" it
- **The real fix** — moving garbage collectors compact live objects together; C and C++ cannot move objects, so they live with the holes

*Example (italic):* A pandas job ratchets up to 7.5 GB over several bursts, drops its data to 2 GB, yet the process still holds 7.5 GB an hour later.

**Key point:** Fragmentation is why memory graphs of long-running processes ratchet upward — the holes between surviving objects are free to the program but unusable and unreturnable in bulk.

### Visualization (canvas `c3`, 720×300)

Line chart over an 8-hour shift: live data (sawtooth, falls after each burst) vs memory held from the OS (ratchets up and stays), the gap between them being fragmented waste.

- **Title (bold 15px, `#1a5276`, top center):** "The Memory Ratchet: Held RAM Never Follows Live Data Back Down".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = hours 0 to 8, 12px `#444` tick labels "0h"–"8h" every 2 hours; y = GB 0 to 8, gridlines `#e5e9ef` at 2/4/6 with 12px `#444` labels.
- **Live data line:** blue `#2a78d6` 3px line through hours `[0, 1, 2, 3, 4, 5, 6, 7, 8]`, GB `[2, 5, 3, 6, 3, 5, 2, 4, 2]` — sawtooth, always returns near 2.
- **Held memory line:** orange `#d95926` 3px line through the same hours, GB `[2, 5, 5, 6.5, 6.5, 7, 7, 7.5, 7.5]` — steps up, never steps down.
- **Gap shading:** fill `rgba(217,89,38,0.12)` between the two lines from hour 3 onward, bold 12px `#d95926` label "fragmented — free to the program, unusable in bulk" inside the gap near x = hour 6, y=115.
- **Labels:** bold 12px blue `#2a78d6` "live data" near (hour 7.3, y=190); bold 12px orange `#d95926` "held from OS" near (hour 7.3, y=70).
- **Annotation (bold 13px violet `#4a3aa7`, near x = hour 2, y=70):** "8h in: holds 7.5 GB to serve 2 GB".
- **Caption (12px `#444`, bottom right):** "GB values illustrative".

## External vs Internal: Two Different Wastes

**Tags:** `common mistake` (red), `two kinds` (orange), `free ≠ usable` (blue)

- **External** — waste *between* blocks: the scattered holes from the coffee counter and the 8 KB story
- **Internal** — waste *inside* a block: ask for 5 KB, the allocator hands out its 8 KB size class, 3 KB rides along unused
- **The tell** — external shows as "free memory but allocation fails"; internal shows as "usage higher than the data"
- **The mistake** — reading a free-memory counter and assuming any allocation up to that number will succeed
- **Not a leak** — a leak is memory never freed; fragmentation is memory freed but stranded, and leak-hunting tools won't find it

*Example (italic):* An engineer sees "free: 16 KB", requests 12 KB, gets out-of-memory, and files a leak bug — but nothing leaked; the free space is two 8 KB holes.

**Common mistake:** Treating "free memory" as one number. It is a shape: external fragmentation hides the waste between blocks, internal hides it inside them, and neither shows up in a simple free-bytes total.

### Visualization (canvas `c4`, 720×300)

Two labeled strips contrasting the wastes: external fragmentation as green holes between blue blocks, internal fragmentation as yellow slack inside one rounded-up block.

- **Title (bold 15px, `#1a5276`, top center):** "External Waste Sits Between Blocks; Internal Waste Hides Inside One".
- **Row 1 (strip y=85, height 40px), label 12px `#444` at x=15 (y=80):** "external"; strip from x=110 width 560; blue `rgba(42,120,214,0.30)` blocks (1.5px `#2a78d6` border) at offsets/widths `[0/120, 190/150, 410/150]` px, green `rgba(0,131,0,0.15)` holes (dashed 1.5px `#008300` border) at offsets/widths `[120/70, 340/70]` px with 11px `#008300` "hole" labels centered inside; bold 12px `#008300` caption under the strip at y=145: "free space chopped into holes between blocks".
- **Row 2 (strip y=185, height 40px), label:** "internal"; one block from x=110 width 320 (an 8 KB size class, 40 px/KB): left part width 200 fill `rgba(42,120,214,0.30)` labeled "used 5 KB" (12px `#2c3e50`), right part width 120 fill `rgba(201,133,0,0.20)` with 1.5px `#c98500` border labeled "slack 3 KB" (12px `#c98500`); bold 12px `#c98500` caption under the strip at y=245: "allocator rounded 5 KB up to its 8 KB size class".
- **Annotation (bold 13px magenta `#d55181`, centered near y=278):** "the free-bytes counter shows neither shape".
- **Caption (12px `#444`, bottom right):** "sizes illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the counter layout (free seats 3, 4, 9, 14, 15, 20 of 20), the heap story (64 KB as 16+8+16+8+16; free B and D → two 8 KB holes; 12 KB request fails), and the ratchet series (live `[2,5,3,6,3,5,2,4,2]` GB vs held `[2,5,5,6.5,6.5,7,7,7.5,7.5]` GB) are invented and labeled illustrative; the arithmetic (6 free seats with largest run 2; 8+8 = 16 ≥ 12 yet 12 > 8; 5 KB rounded to an 8 KB size class leaving 3 KB slack) is exact.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
