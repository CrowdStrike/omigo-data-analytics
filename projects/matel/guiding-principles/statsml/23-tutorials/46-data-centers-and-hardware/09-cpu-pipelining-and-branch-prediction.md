# CPU Pipelining & Branch Prediction

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** CPU Pipelining & Branch Prediction

**Subtitle:** A CPU runs instructions like an assembly line and guesses which way every `if` will go — which is why the same loop over the same numbers runs several times faster when the array is sorted

## The Assembly Line Inside the CPU

**Tags:** `core idea` (blue), `pipeline` (green), `in-flight instructions` (orange)

- **The line** — one instruction passes through stages: fetch, decode, execute, memory, write back
- **The overlap** — while instruction 1 executes, instruction 2 decodes and instruction 3 is fetched
- **The payoff** — once the line is full, one instruction finishes every cycle, not one every 5
- **The depth** — real chips split work far finer: modern pipelines run ~15–20 stages deep
- **The catch** — the line only stays full if the CPU always knows which instruction comes next

*Example (italic):* Five instructions through a 5-stage line take 9 cycles overlapped instead of 25 one-at-a-time — nearly 3× the throughput from the same hardware.

**Key point:** Pipelining is an assembly line for instructions — many are in flight at once, so throughput depends on keeping the line fed with the correct next instruction every cycle.

### Visualization (canvas `c1`, 720×300)

Staircase pipeline diagram: 5 instructions (rows) flowing through 5 stages (colored cells) across cycles 1–9 on a shared cycle axis.

- **Title (bold 15px, `#1a5276`, top center):** "5 Instructions, 5 Stages: Done in 9 Cycles Instead of 25".
- **Layout:** cycle axis along the top, 9 columns starting at x=170, each 58px wide, 12px `#444` labels "cyc 1"–"cyc 9" at y=58; instruction rows I1–I5 at y = 80, 120, 160, 200, 240 with left-aligned 12px `#444` labels "I1"–"I5" at x=120.
- **Cells:** each instruction i (0-based) occupies cycles i+1..i+5 with stage cells in order F, D, E, M, W; cell 54×32px, 4px radius, 11px white bold stage letter centered; stage fills F blue `#2a78d6`, D aqua `#199e70`, E violet `#4a3aa7`, M yellow `#c98500`, W green `#008300` — the filled cells form a descending staircase.
- **Annotation (bold 13px green `#008300`, two centered lines in the empty top-right area at x=590, y=88 and y=106):** "from cycle 5 on," / "one finishes every cycle".
- **Caption (12px `#444`, bottom left at x=120, y=290):** "classic 5-stage teaching pipeline; real chips run 15–20 stages".

## The Loop That Runs 6× Faster on Sorted Data

**Tags:** `worked example` (blue), `the famous demo` (green)

- **The setup** — an array of 32,768 values in 0–255; the loop sums only the elements > 128
- **The branch** — `if (a[i] > 128)` decides each iteration; the CPU must guess it to keep the line full
- **Random order** — a random element is above 128 about half the time; the guess is a coin flip
- **Sorted order** — first every element fails the test, then every element passes: one pattern change
- **The result** — same array, same sum: random order 11.5s, sorted order 1.9s (illustrative timings)

*Example (italic):* Sorting the array first — extra work! — makes the summing loop about 6× faster, because the branch predictor goes from ~50% wrong to nearly always right.

**Key point:** The branch predictor learns from history — an all-false-then-all-true branch is trivially learnable, a 50/50 random one is unlearnable, and that alone changes the runtime severalfold.

### Visualization (canvas `c2`, 720×300)

Grouped bar chart: loop runtime for random vs sorted array, with each bar's mispredict rate labeled — same data, same work, different predictability.

- **Title (bold 15px, `#1a5276`, top center):** "Sum Elements > 128: Same Array, Sorted Runs ~6× Faster".
- **Axes:** origin x=70, baseline y=245, plot width 580, plot height 180; y = runtime seconds 0 to 12, gridlines `#e5e9ef` at 3/6/9 with 12px `#444` labels.
- **Bars (120px wide):** "random order" red `#e74c3c` bar centered at x=230, height for 11.5s, bold 13px red value label "11.5 s" above; "sorted order" green `#008300` bar centered at x=470, height for 1.9s, bold 13px green value label "1.9 s" above.
- **Sub-labels (12px `#444`, under each bar name):** "~50% branches mispredicted" under random; "<1% mispredicted" under sorted.
- **Annotation (bold 13px violet `#4a3aa7`, near x=490, y=100, clear of the tall red bar):** "the loop body is identical — only the guessability changed".
- **Caption (12px `#444`, bottom right):** "timings illustrative; pattern matches the classic Stack Overflow demo".

## What a Wrong Guess Costs

**Tags:** `why it matters` (blue), `pipeline flush` (red), `hot loops` (orange)

- **The flush** — a wrong guess means the line is full of wrong-path work: throw it all away and refetch
- **The bill** — a flush wastes roughly the pipeline depth, ~15–20 cycles; a right guess costs nothing
- **The math** — with a 15-cycle penalty, a branch costs about 1 + rate × 15 cycles on average
- **Hot loops** — a branch that runs a billion times turns each mispredict percent into real seconds
- **The tools** — branchless tricks (conditional moves, arithmetic masks) trade the guess for a tiny fixed cost

*Example (italic):* At a 15-cycle penalty, cutting a hot loop's mispredict rate from 50% to 0% drops the average branch cost from 8.5 cycles to 1 — the whole sorted-array speedup in one formula.

**Key point:** Correct predictions are free and wrong ones cost ~15–20 cycles, so in hot loops the mispredict rate — not the number of branches — is what you pay for.

### Visualization (canvas `c3`, 720×300)

Line chart: average cost per branch (cycles) as the mispredict rate rises, using cost = 1 + rate × 15.

- **Title (bold 15px, `#1a5276`, top center):** "Average Branch Cost = 1 + Mispredict Rate × 15 Cycles".
- **Axes:** origin x=70, baseline y=245, plot width 580, plot height 180; x = mispredict rate 0% to 50%, 12px `#444` tick labels at 0/10/20/30/40/50%; y = cycles per branch 0 to 9, gridlines `#e5e9ef` at 3/6 with 12px `#444` labels.
- **Cost line:** blue `#2a78d6` 3px line through rate points `[0, 5, 10, 25, 50]`% with costs `[1, 1.75, 2.5, 4.75, 8.5]` cycles; 5px solid blue dots at each point, 11px `#444` value labels above each dot.
- **Markers:** green `#008300` filled dot at (0%, 1) with bold 12px green label "sorted loop lives here"; red `#e74c3c` filled dot at (50%, 8.5) with bold 12px red label "random loop lives here".
- **Annotation (bold 13px orange `#d95926`, near x=25%, y=80):** "8.5× per branch — same loop, different data order".
- **Caption (12px `#444`, bottom right):** "15-cycle flush penalty, illustrative of a ~15-stage pipeline".

## It Is Not the Cache

**Tags:** `common mistake` (red), `predictability` (orange)

- **The confusion** — people credit the sorted speedup to memory caching; it is the same array either way
- **Same traffic** — sorted or random order, the loop touches the same 32,768 bytes sequentially
- **The real cause** — sorting changed the branch outcome sequence, not where the data lives
- **The lie in benchmarks** — a predictor learns loops of regular test data, then real inputs mispredict
- **The wrong fix** — adding a second `if` to "help" the CPU adds another branch to guess, not fewer

*Example (italic):* Replacing `if (a[i] > 128) sum += a[i]` with the branchless `sum += a[i] & -(a[i] > 128)` makes random and sorted run at the same speed — proof the branch, not the cache, was the cost.

**Common mistake:** Blaming memory whenever data order changes speed. Data order affects prediction as well as caching — if the same bytes are read either way, suspect the branch predictor first.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: a correctly predicted branch (work flows on) vs a mispredicted one (wrong-path work flushed, refetch after 15 wasted cycles).

- **Title (bold 15px, `#1a5276`, top center):** "Right Guess: Free. Wrong Guess: Flush and Refetch".
- **Row 1 (y=95), label 12px `#444` at x=20:** "predicted right"; blue `#2a78d6` rounded box at x=170 labeled "if (a[i] > 128)" (12px), 3px arrow to a green `#008300` box at x=400 labeled "guessed path was correct", then bold 12px green "✓ 0 cycles lost" at x=590.
- **Row 2 (y=205), label:** "predicted wrong"; blue box "if (a[i] > 128)", 3px arrow to a red `#e74c3c` box at x=370 labeled "15 wrong-path instructions flushed", then arrow to an orange `#d95926` box at x=580 labeled "refetch" with bold 12px red "✗ ~15 cycles wasted" beneath at y=250.
- **Box style:** 150–190px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(0,131,0,0.12)` / `rgba(231,76,60,0.12)` / `rgba(217,89,38,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px magenta `#d55181`, centered near y=280):** "both rows read the same memory — only the guess differs".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the 11.5s / 1.9s timings and ~50% / <1% mispredict rates are invented and labeled illustrative; the branch-cost line values (1 / 1.75 / 2.5 / 4.75 / 8.5 cycles at 0/5/10/25/50%) follow exactly from cost = 1 + rate × 15; the 5-stage/9-cycle pipeline counts are exact for the textbook pipeline.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
