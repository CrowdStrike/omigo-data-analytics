# FlashAttention & IO-Aware Algorithms

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** FlashAttention & IO-Aware Algorithms

**Subtitle:** The fastest attention code does not do less math — it makes fewer trips to slow memory; the trick is to count the bytes moved, not the operations

## The Cook With a Tiny Counter

**Tags:** `core idea` (blue), `memory trips` (green), `running example` (orange)

- **The cook** — a cook has a tiny counter by the stove and a big pantry a thirty-second walk away
- **Two costs** — every dish costs chopping time at the counter plus walking time to the pantry
- **The surprise** — chopping takes 2 minutes, but fetching one item per step adds 18 minutes of walking
- **The fix** — carry one full basket over, cook everything that needs those items, then walk back once
- **GPUs too** — a GPU has tiny fast on-chip memory (the counter) and big slow main memory (the pantry)
- **IO-aware** — an IO-aware algorithm plans around the trips (bytes moved), not the chops (operations)

*Example (italic):* Two cooks follow the same 20-step recipe; the one who batches pantry trips finishes the dish in 5 minutes instead of 20 — identical steps, fewer walks.

**Key point:** An IO-aware algorithm is one designed to minimize data moved between slow and fast memory — because on modern hardware the moving, not the arithmetic, is usually what you wait for.

### Visualization (canvas `c1`, 720×300)

Two horizontal stacked bars on a shared minutes axis: the same dish cooked the fetch-per-step way and the batch-the-trips way, each bar split into chopping time and walking time.

- **Title (bold 15px, `#1a5276`, top center):** "One Dish, Two Recipes: Chopping vs Walking (minutes)".
- **Axis:** horizontal 2px `#999` line at y=245 from x=190 to x=670 (width 480), scale 0 to 20 minutes; 12px `#444` tick labels "0", "5", "10", "15", "20" below; light `#e5e9ef` vertical gridlines at each tick.
- **Row 1 (bar center y=100), left label 12px `#444` at x=20:** "old recipe — fetch per step"; 22px-tall stacked bar starting at x=190: blue `#2a78d6` chop segment 2 min, then orange `#d95926` walk segment 18 min (total 20); 12px bold labels above the bar: blue "chop 2" over the blue part, orange "walk 18" over the orange part.
- **Row 2 (bar center y=175), label:** "tile recipe — batch the trips"; stacked bar: blue chop 2 min, orange walk 3 min (total 5); bold labels "chop 2" and "walk 3" above.
- **Annotation (bold 13px green `#008300`, near x=430, y=195):** two lines: "same chopping, 4× fewer minutes —" / "the walks were the real cost".
- **Caption (12px `#444`, bottom right):** "illustrative — minutes invented for the example".

## Counting the Numbers Moved: 1,000 Tokens

**Tags:** `worked example` (blue), `bytes moved` (green)

- **The setup** — attention over 1,000 tokens, each described by 64 numbers: Q, K, V are 64,000 numbers each
- **The big table** — scoring every token against every token makes a 1,000 × 1,000 table: 1,000,000 numbers
- **Standard way** — writes and rereads that table twice (raw scores, then softmax): 4,000,000 moves
- **Flash way** — cuts Q, K, V into tiles that fit on-chip; the million-number table never leaves the chip
- **The bill** — standard moves about 4,256,000 numbers; FlashAttention moves about 256,000 — 16× less
- **Same math** — both do the same ~128 million multiply-adds and produce the exact same answer

*Example (italic):* For 1,000 tokens the score table alone holds 1,000,000 numbers — nearly four times bigger than all the inputs and outputs combined (256,000).

**Key point:** FlashAttention never parks the n×n score table in slow memory — traffic drops from about 4,256,000 numbers to about 256,000 while the arithmetic stays identical.

### Visualization (canvas `c2`, 720×300)

Two-bar comparison: numbers moved to and from slow memory for one attention pass over 1,000 tokens, standard versus FlashAttention.

- **Title (bold 15px, `#1a5276`, top center):** "Attention on 1,000 Tokens: Numbers Moved to Slow Memory".
- **Axes:** origin x=90, baseline y=245, plot width 560, plot height 185; y scale 0 to 4.5 million with light `#e5e9ef` gridlines at 1M, 2M, 3M, 4M and 12px `#444` labels "1M"–"4M" at left.
- **Bar 1 (center x=260, width 120):** value 4,256,000, fill `rgba(217,89,38,0.75)` with 2px `#d95926` border; bold 13px `#d95926` label "4,256,000" above the bar; 12px `#444` x-label "standard attention" below the baseline.
- **Bar 2 (center x=500, width 120):** value 256,000, fill `rgba(0,131,0,0.75)` with 2px `#008300` border; bold 13px `#008300` label "256,000" above; x-label "FlashAttention".
- **Annotation (bold 13px ink `#1a5276`, near x=380, y=105):** two lines: "about 16× less traffic —" / "same ~128M multiply-adds".
- **Caption (12px `#444`, bottom right):** "illustrative — counts from the 1,000-token worked example".

## Why GPUs Wait on Memory, Not Math

**Tags:** `where it's used` (blue), `memory-bound` (orange)

- **Fast at math** — a modern GPU can do hundreds of trillions of multiply-adds every second
- **Slow at fetching** — its big main memory delivers data roughly 10× slower than its on-chip memory
- **Memory-bound** — standard attention spends most of its wall-clock time waiting for the table to move
- **Real speedups** — counting bytes instead of ops gave attention 2–4× real speedups, zero accuracy loss
- **Longer context** — cheaper attention is one reason context windows grew from 2,000 to 100,000+ tokens
- **The habit** — when a kernel is mysteriously slow, check bytes moved before counting operations

*Example (italic):* An attention pass that took 140 ms on 8,000 tokens runs in 46 ms with FlashAttention — the math did not change, the trips did.

**Key point:** On memory-bound workloads the clock follows the bytes, not the FLOPs — which is why an algorithm with identical arithmetic can be 3× faster in practice.

### Visualization (canvas `c3`, 720×300)

Two-line chart: wall-clock time for one attention pass as the context length grows, standard attention versus FlashAttention.

- **Title (bold 15px, `#1a5276`, top center):** "Time per Attention Pass as Context Grows".
- **Axes:** origin x=70, baseline y=245, plot width 580, plot height 185; x = context length with four evenly spaced category ticks labeled "1,000", "2,000", "4,000", "8,000" (12px `#444`); y = 0 to 150 ms with light `#e5e9ef` gridlines at 50, 100, 150 labeled "50 ms", "100 ms", "150 ms" (12px `#444`).
- **Standard line:** orange `#d95926` 3px line with 5px dots through values `[2, 8, 33, 140]` ms at the four ticks; 12px orange label "standard" just above its last point.
- **Flash line:** green `#008300` 3px line with 5px dots through `[1, 3.4, 12, 46]` ms; 12px green label "FlashAttention" just below its last point.
- **Annotation (bold 13px green `#008300`, near x=430, y=145):** "3× faster at 8,000 tokens — 140 ms → 46 ms".
- **Caption (12px `#444`, bottom right):** "illustrative timings — shapes matter, not the exact ms".

## More Arithmetic, Yet Faster

**Tags:** `common mistake` (red), `FLOPs vs bytes` (orange)

- **The reflex** — "fewer operations means faster" is the first rule everyone learns, and here it misleads
- **More math** — FlashAttention actually does about 7% more arithmetic, recomputing values it never stored
- **Still faster** — recomputing a number on-chip is cheaper than a round trip to slow memory to fetch it
- **Measure both** — profile operations and bytes moved; whichever count is slower rules the runtime
- **The tell** — if speed barely improves when you shrink the math, the workload was memory-bound all along

*Example (italic):* Storing a value off-chip and reading it back later costs more time than rebuilding it from scratch on the chip — so FlashAttention deliberately rebuilds.

**Common mistake:** Ranking algorithms by operation counts (FLOPs) alone. FlashAttention does ~7% more math yet takes ~65% less time — on memory-bound work the bytes decide, not the FLOPs.

### Visualization (canvas `c4`, 720×300)

Grouped bar chart with everything scaled to standard attention = 100: one group for arithmetic done, one for time taken, showing flash paying a little more math to save a lot of time.

- **Title (bold 15px, `#1a5276`, top center):** "More Arithmetic, Less Time (standard = 100)".
- **Axes:** origin x=70, baseline y=245, plot width 580, plot height 185; y = 0 to 120 with light `#e5e9ef` gridlines at 25, 50, 75, 100 labeled 12px `#444`; dashed `#6b7280` (dash 4/3) reference line across the plot at 100 with 11px `#6b7280` label "standard = 100" at its right end.
- **Group 1 (centered x=240), 12px `#444` group label "multiply-adds" below the baseline:** two 70px-wide bars 12px apart — standard 100, fill `rgba(217,89,38,0.7)`, and flash 107, fill `rgba(0,131,0,0.7)`; bold 12px value labels "100" and "107" above the bars in matching colors.
- **Group 2 (centered x=500), label "time taken":** standard 100 (orange fill) and flash 35 (green fill); bold value labels "100" and "35".
- **Legend (top right, 12px):** orange square + "standard", green square + "FlashAttention".
- **Annotation (bold 13px violet `#4a3aa7`, near x=360, y=95):** two lines: "7% more math," / "65% less time".
- **Caption (12px `#444`, bottom right):** "illustrative — ratios, not benchmarks".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all bar values, line points, and minutes are the hardcoded literals above (no randomness); the 1,000-token traffic counts (4,256,000 vs 256,000), the timing arrays `[2, 8, 33, 140]` / `[1, 3.4, 12, 46]`, and the 100/107/35 ratios must match the text bullets exactly; every invented number keeps its "illustrative" caption.
- **Color convention across charts:** standard attention is always orange `#d95926`, FlashAttention always green `#008300`; blue `#2a78d6` reserved for the compute (chopping) segments in c1.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
