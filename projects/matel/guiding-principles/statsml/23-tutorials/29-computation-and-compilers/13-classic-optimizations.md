# Classic Optimizations

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Classic Optimizations

**Subtitle:** What -O2 actually does: the compiler rewrites your code into a faster version it can prove gives the same answer — folding constants, hoisting repeated work out of loops, and deleting code that can never run

## The Checkout Loop the Compiler Rewrites

**Tags:** `core idea` (blue), `constant folding` (green), `hoisting` (orange)

- **The loop** — a shop's checkout code adds up 1,000 item prices, each with 8% tax on top
- **What you wrote** — every item recomputes rate = 1 + 0.08, multiplies the price, adds to total
- **Constant folding** — the compiler computes 1 + 0.08 = 1.08 once, at compile time, not at runtime
- **Loop-invariant hoisting** — rate never changes inside the loop, so it moves above the loop
- **The promise** — -O2 only makes rewrites it can prove leave the receipt total unchanged

*Example (italic):* You wrote three steps per item; the compiled program runs two — and nobody can tell the difference from the receipt.

**Key point:** -O2 is not magic: it is a fixed list of small rewrites, each provably answer-preserving, applied in a fixed pipeline of passes.

### Visualization (canvas `c1`, 720×300)

Two-panel before/after diagram: the loop body as you wrote it (three step boxes) on the left, the -O2 version (one hoisted step above a two-step loop) on the right, with an arrow between them naming the two rewrites.

- **Title (bold 15px, `#1a5276`, top center):** "One Checkout Item: What You Wrote vs What -O2 Runs".
- **Left panel:** rounded 1px `#e5e9ef` box at x=40–330, y=60–260; header bold 13px `#2c3e50` at top: "you wrote — 3 steps per item"; three rounded step boxes (width 250, height 40, at y = 95, 145, 195, centered in panel) with 12px monospace text: box 1 `rate = 1 + 0.08` (fill `rgba(217,89,38,0.15)`, border 2px orange `#d95926`), box 2 `line = price * rate` (fill `rgba(42,120,214,0.12)`, border 2px blue `#2a78d6`), box 3 `total = total + line` (fill `rgba(42,120,214,0.12)`, border 2px blue `#2a78d6`).
- **Right panel:** rounded 1px `#e5e9ef` box at x=390–680, y=60–260; header bold 13px `#2c3e50`: "-O2 runs — 2 steps per item"; one green step box above the loop (width 250, height 34, y=92): 12px monospace `rate = 1.08  (once, before loop)` (fill `rgba(0,131,0,0.12)`, border 2px green `#008300`); below it a dashed `#6b7280` inner box labeled 11px `#6b7280` "loop" containing two blue step boxes (y = 160, 205): `line = price * 1.08` and `total = total + line` (same blue style as left).
- **Arrow:** 3px violet `#4a3aa7` arrow from x=335 to x=385 at y=160 with arrowhead.
- **Annotation (bold 13px violet `#4a3aa7`, two lines centered under the arrow at y≈280):** "fold the constant," / "hoist it out of the loop".

## Counting the Steps: 3,000 Down to 2,001

**Tags:** `worked example` (blue), `hand count` (green)

- **The count** — 1,000 items × 3 steps each = 3,000 steps, exactly as the source is written
- **After folding** — 1 + 0.08 becomes the literal 1.08; that addition never runs at all
- **After hoisting** — rate = 1.08 runs once before the loop instead of 1,000 times inside it
- **New total** — 1,000 items × 2 steps + 1 hoisted step = 2,001 steps; a third of the work gone
- **Check by hand** — trace 3 items yourself: 9 steps as written, 7 steps after -O2 (1 + 3×2)

*Example (italic):* For three items the original runs 9 steps; the optimized build runs 7 — same three line totals, same receipt, fewer steps.

**Key point:** 3,000 steps become 2,001: the multiply and the running add survive, and the 1,000 identical "1 + 0.08" additions simply vanish.

### Visualization (canvas `c2`, 720×300)

Two stacked horizontal bars on a shared step-count axis: the written version's 3,000 steps split into its three step types, above the -O2 version's 2,000 loop steps plus one hoisted step, showing the orange segment disappearing.

- **Title (bold 15px, `#1a5276`, top center):** "Steps to Ring Up 1,000 Items: 3,000 → 2,001".
- **Axis:** horizontal 2px `#999` line at y=240 from x=140 to x=660 (plot width 520 = 3,000 steps, so 1 step = 520/3000 px); tick labels "0", "500", "1,000", "1,500", "2,000", "2,500", "3,000" every 500 steps (12px `#444` below the axis); light `#e5e9ef` vertical gridlines at each tick from y=70 to the axis.
- **Row 1 (bar top y=100, height 34), left label 12px `#444` at x=20:** "you wrote"; three segments left to right: 1,000 steps orange `rgba(217,89,38,0.55)` labeled inside 11px white "1 + 0.08", 1,000 steps blue `rgba(42,120,214,0.55)` labeled "price × rate", 1,000 steps aqua `rgba(25,158,112,0.55)` labeled "total + line"; bold 13px `#2c3e50` total "3,000" just right of the bar.
- **Row 2 (bar top y=170, height 34), left label:** "-O2 build"; two segments: 1,000 steps blue and 1,000 steps aqua (same fills and inside labels); bold 13px `#2c3e50` total "2,001" just right of the bar; 11px green `#008300` note under that total: "incl. 1 hoisted step".
- **Annotation (bold 13px green `#008300`, at x≈380, y=82, above row 1's orange segment end):** "1,000 identical additions gone — one third of the work".

## Why the Same Code Runs Nearly 4× Faster

**Tags:** `where it's used` (blue), `classic passes` (green), `benchmarking` (orange)

- **The gap** — the same checkout loop over a million items: 9.2 ms at -O0, 2.4 ms at -O2, no source change
- **Dead-code elimination** — code that can never run, or results never used, is deleted outright
- **Inlining** — small functions get pasted in at the call site, killing the call overhead
- **Strength reduction** — pricey operations become cheap ones, like x × 2 turning into x + x
- **The benchmarking trap** — timing a debug (-O0) build tells you nothing about production speed
- **Fast libraries** — much of numpy's and BLAS's speed is exactly this: compiled, optimized inner loops

*Example (italic):* A data scientist times a C extension at -O0, reports 9.2 ms, and ships the conclusion — the production -O2 build runs it in 2.4 ms.

**Key point:** -O2 bundles dozens of small proven-safe rewrites; each saves a little, and inside a loop that runs a million times the savings multiply.

### Visualization (canvas `c3`, 720×300)

Two vertical bars comparing the runtime of the million-item checkout loop compiled at -O0 and at -O2, with the speedup called out.

- **Title (bold 15px, `#1a5276`, top center):** "Same Source, Two Builds: 1,000,000 Items".
- **Axes:** origin x=90, baseline y=250, plot height 190; y axis = runtime 0 to 10 ms (1 ms = 19 px), tick labels "0", "2", "4", "6", "8", "10 ms" every 2 ms (12px `#444`), light `#e5e9ef` gridlines across the plot at each tick; x axis just the baseline.
- **Bars (width 120):** "-O0 debug build" at x=200, value 9.2 ms (height 175 px), fill `rgba(42,120,214,0.35)`, border 2px blue `#2a78d6`; "-O2 release build" at x=440, value 2.4 ms (height 46 px), fill `rgba(0,131,0,0.30)`, border 2px green `#008300`; bar labels 12px `#444` under the baseline; bold 13px value labels "9.2 ms" and "2.4 ms" just above each bar in the bar's border color.
- **Annotation (bold 13px green `#008300`, two lines near x=560, y=110):** "same total to the cent —" / "3.8× faster".
- **Caption (12px `#444`, bottom right):** "illustrative timings — real ratios vary by code and machine".

## The Optimizer Never Changes the Answer — Almost

**Tags:** `common mistake` (red), `float order` (orange)

- **The fear** — people expect -O2 to change results; it refuses any rewrite it cannot prove safe
- **Float order** — (a + b) + c and a + (b + c) can differ in the last digits for floating point
- **The rewrite it skips** — total = 1.08 × sum(prices) needs reordering, so -O2 leaves your sum alone
- **-Ofast is different** — it trades that guarantee for speed; float sums may shift in the last digits
- **Debugging pain** — optimized code is reshuffled, which is why debuggers say "value optimized out"

*Example (italic):* The 9.2 ms build, the 2.4 ms -O2 build, and the 2.2 ms -O3 build all print the identical receipt — only the 1.9 ms -Ofast build may not.

**Common mistake:** Treating -O3 and -Ofast as "just more -O2". -O3 keeps the same-answer guarantee; -Ofast drops it, and float totals can quietly change.

### Visualization (canvas `c4`, 720×300)

Five vertical bars showing the checkout loop's runtime across optimization levels, with the one level that may change answers flagged in red.

- **Title (bold 15px, `#1a5276`, top center):** "Optimization Levels: Diminishing Returns, One Broken Promise".
- **Axes:** origin x=90, baseline y=250, plot height 190; y axis = runtime 0 to 10 ms (1 ms = 19 px), tick labels "0", "2", "4", "6", "8", "10 ms" every 2 ms (12px `#444`), light `#e5e9ef` gridlines at each tick.
- **Bars (width 80, at x = 110, 225, 340, 455, 570):** levels `["-O0", "-O1", "-O2", "-O3", "-Ofast"]` with runtimes `[9.2, 3.6, 2.4, 2.2, 1.9]` ms (heights 175, 68, 46, 42, 36 px); first four bars fill `rgba(42,120,214,0.35)` with 2px blue `#2a78d6` border, -Ofast bar fill `rgba(231,76,60,0.20)` with 2px red `#e74c3c` border; level labels 12px `#444` under the baseline, bold 13px value labels ("9.2", "3.6", "2.4", "2.2", "1.9") above each bar in the border color.
- **Guarantee bracket:** thin 1px `#6b7280` horizontal bracket over the first four bars at y=55 with 11px `#6b7280` label "same answer, guaranteed"; separate 11px red `#e74c3c` label "no guarantee" over -Ofast.
- **Annotation (bold 13px red `#e74c3c`, two lines near x=560, y=140):** "-Ofast may reorder float math —" / "the total can change".
- **Caption (12px `#444`, bottom right):** "illustrative timings — real ratios vary by code and machine".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all step counts (3 vs 2 per item; 3,000 vs 2,001; 9 vs 7 for three items) are exact consequences of the worked example and hardcoded; the millisecond timings `[9.2, 3.6, 2.4, 2.2, 1.9]` are invented, hardcoded literals labeled "illustrative" in their captions (no randomness anywhere). Text numbers must match chart numbers exactly.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
