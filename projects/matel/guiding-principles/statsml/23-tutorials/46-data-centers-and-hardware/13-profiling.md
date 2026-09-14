# Profiling

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Profiling

**Subtitle:** Before you optimize, measure where the time actually goes — a flame graph usually points somewhere nobody suspected

## The Pipeline Everyone Blamed on the Model

**Tags:** `core idea` (blue), `measure first` (green), `sampling profiler` (orange)

- **The pipeline** — a nightly churn-scoring job takes 50 seconds and the team wants it faster
- **The guess** — everyone blames the ML model: "scoring 2 million rows must be the slow part"
- **The sampler** — a sampling profiler interrupts the job 100 times/sec and records the call stack
- **Cheap and safe** — sampling adds ~1% overhead and is statistical, so it can run in production
- **The other kind** — instrumenting profilers time every call: exact, but the timing distorts the run
- **The verdict** — model scoring shows in 8% of samples; an unsuspected timestamp parser shows in 60%

*Example (italic):* In 5,000 samples over the 50-second run, `score_model` appears 400 times and `parse_timestamp` 3,000 times — intuition was off by a factor of seven.

**Key point:** Measure before optimizing — the slow part is rarely where you think, and a sampling profiler finds it for about 1% overhead.

### Visualization (canvas `c1`, 720×300)

Paired horizontal bar chart: the team's guessed share of runtime vs the measured share (from 5,000 samples) for four parts of the pipeline.

- **Title (bold 15px, `#1a5276`, top center):** "Guessed vs Measured: Where 50 Seconds Actually Go".
- **Layout:** four row groups at y = 70, 125, 180, 235; left-aligned 12px `#444` function labels at x=20 (`score_model`, `parse_timestamp`, `write_output`, `everything else`); bars start at x=180, max width 440 = 100%.
- **Bars per group (each 14px tall, 4px apart):** top bar = guessed share, fill `rgba(42,120,214,0.30)` with 2px `#2a78d6` edge; bottom bar = measured share, solid — green `#008300` for `parse_timestamp` (the real culprit), blue `#2a78d6` otherwise.
- **Values (guessed → measured, 11px labels at bar ends):** score_model 70% → 8%; parse_timestamp 5% → 60%; write_output 15% → 14%; everything else 10% → 18%. Pixel widths = percent × 4.4.
- **Legend (12px, top right):** hollow swatch "team's guess", solid swatch "profiler's 5,000 samples".
- **Annotation (bold 13px orange `#d95926`, near the parse_timestamp measured bar):** "the slow part nobody suspected".
- **Caption (12px `#444`, bottom right):** "guesses illustrative; measured shares match the flame graph below".

## Reading the Flame Graph

**Tags:** `worked example` (blue), `flame graph` (green)

- **Each box** — one function; the boxes stacked directly above it are the functions it called
- **Width is share** — a box's width is the fraction of samples it appeared in: 60% of samples, 60% of width
- **Not a timeline** — the x-axis merges identical stacks and carries no time order; only width means anything
- **Look for plateaus** — wide flat-topped boxes are where the CPU actually spends its time
- **Hand-check** — `parse_timestamp`: 3,000 of 5,000 samples → 60% of the width → 30 s of the 50 s run

*Example (italic):* `score_model` is a skinny 8%-wide box; `parse_timestamp` is a wide 60% plateau sitting three calls deep under `load_rows` → `parse_row`.

**Key point:** Read a flame graph by width, not position — the widest plateau is the biggest opportunity, wherever it sits in the stack and whatever it is named.

### Visualization (canvas `c2`, 720×300)

Flame graph of the 50-second pipeline run, drawn as stacked rounded boxes: call depth grows upward, box width = fraction of the 5,000 samples.

- **Title (bold 15px, `#1a5276`, top center):** "Flame Graph of the Pipeline: 5,000 Samples, Width = Share of Samples".
- **Geometry:** plot spans x=60 to x=660 (600px = 100%); rows are 28px tall with a 3px vertical gap; depth 0 at y=217, depth 1 at y=186, depth 2 at y=155, depth 3 at y=124. Box corner radius 3px, 1px white stroke between boxes, 12px `#2c3e50` centered labels (11px or omit label if the box is under 40px wide).
- **Depth 0:** `run_pipeline` — x=60, width 600 (100%), fill `rgba(42,120,214,0.20)`.
- **Depth 1 (left to right):** `load_rows` x=60 width 432 (72%) fill `rgba(42,120,214,0.30)`; `score_model` x=492 width 48 (8%) fill `rgba(0,131,0,0.30)`; `write_output` x=540 width 84 (14%) fill `rgba(74,58,167,0.25)`; `other` x=624 width 36 (6%) fill `#e5e9ef`.
- **Depth 2 (above load_rows only):** `read_file` x=60 width 48 (8%) fill `rgba(25,158,112,0.25)`; `parse_row` x=108 width 384 (64%) fill `rgba(201,133,0,0.25)`.
- **Depth 3 (above parse_row only):** `split_fields` x=108 width 24 (4%) fill `#e5e9ef`; `parse_timestamp` x=132 width 360 (60%) solid `#d95926` fill at 0.45 alpha with 2px `#d95926` edge.
- **Callouts:** bold 13px `#d95926` "60% — the plateau" with a short arrow to `parse_timestamp` from y≈95; bold 12px `#008300` "8% — the suspect" with an arrow down to `score_model` from y≈160 right side.
- **X-axis note (12px `#6b7280`, under the baseline at y=262, centered):** "x-axis = fraction of samples, NOT time order".
- **Caption (12px `#444`, bottom right):** "sample counts illustrative; percentages match the text".

## The Loop: Fix the Widest Thing, Then Look Again

**Tags:** `rule of thumb` (green), `where it's used` (blue)

- **One fix** — caching the parsed timestamp format cuts `parse_timestamp` from 30 s to 3 s
- **The payoff** — the whole pipeline drops from 50 s to 23 s by fixing code nobody had blamed
- **Re-profile** — the graph reshapes: `write_output` (7 s) is now the widest box at 30% of the new run
- **The ceiling** — fixing an 8% box can never save more than 8%; fixing the 60% box nearly halved the run
- **The loop** — profile → fix the widest thing → re-profile; stop when the widest box is cheap to leave alone

*Example (italic):* After the fix, the once-feared `score_model` is 4 s of 23 s — at most a 17% win — so the re-profiled graph says work on `write_output` next.

**Key point:** Never optimize twice off one profile — every fix reshapes the graph, so re-profile before choosing the next target.

### Visualization (canvas `c3`, 720×300)

Two horizontal stacked bars: total runtime before and after the one fix, segmented by function, on a shared seconds axis.

- **Title (bold 15px, `#1a5276`, top center):** "One Fix, Re-Profile: 50 s Becomes 23 s and the Widest Box Changes".
- **Axis:** seconds 0–50 mapped to x=110..710 (12px per second); light `#e5e9ef` gridlines with 12px `#444` tick labels at 0/10/20/30/40/50 s along y=255.
- **Bar rows (24px tall):** "before" at y=95 with a 12px `#444` label at x=20; "after" at y=175.
- **Before segments (left to right, widths at 12px/s):** `parse_timestamp` 30 s (360px, `#d95926` at 0.45 alpha), `write_output` 7 s (84px, `rgba(74,58,167,0.25)`), `score_model` 4 s (48px, `rgba(0,131,0,0.30)`), `read_file` 4 s (48px, `rgba(25,158,112,0.25)`), `everything else` 5 s (60px, `#e5e9ef`). Total 50 s.
- **After segments (same order and fills):** `parse_timestamp` 3 s (36px), `write_output` 7 s (84px), `score_model` 4 s (48px), `read_file` 4 s (48px), `everything else` 5 s (60px). Total 23 s, bar ends at x=386.
- **Segment labels:** 11px `#2c3e50` seconds inside each segment wide enough (≥40px); others get a thin leader line to a label above the bar.
- **Delta marker:** dashed `#6b7280` vertical line at the after-bar end (23 s) with bold 13px `#008300` label "−27 s from one cached format".
- **Annotation (bold 12px violet `#4a3aa7`, under the after bar near x=200):** "new widest: write_output (7 s of 23 s = 30%) — re-profile before the next fix".
- **Caption (12px `#444`, bottom right):** "seconds illustrative, consistent with the flame graph".

## Profiling the Wrong Dimension

**Tags:** `common mistake` (red), `CPU vs wall time` (orange)

- **Two clocks** — wall time is what the user waits; CPU time is when the processor is actually working
- **The symptom** — a "slow" 60-second export shows only 12 s of CPU work in the profiler
- **The gap** — for the other 48 s the program sits idle, waiting on a database across the network
- **The trap** — a CPU flame graph of that job looks innocent: waiting never appears in CPU samples
- **The fix** — use wall-clock (off-CPU) profiling for I/O-bound jobs, CPU profiling for compute-bound ones

*Example (italic):* Optimizing the export's CPU-heaviest function saves at most 12 of 60 seconds; batching its 4,800 one-row database calls (~10 ms each) saves 40.

**Common mistake:** Profiling CPU time on a program that is waiting, not working — a flat, innocent CPU profile on a slow job means look at I/O, not at code.

### Visualization (canvas `c4`, 720×300)

Two aligned timeline bars for the 60-second export: what the user experiences (full wall time) vs what a CPU profiler sees (busy slivers between long waits).

- **Title (bold 15px, `#1a5276`, top center):** "A 60-Second Job the CPU Profiler Calls Innocent".
- **Axis:** seconds 0–60 mapped to x=90..690 (10px per second); 12px `#444` tick labels at 0/15/30/45/60 s along y=250.
- **Bar 1 (y=100, 26px tall), label 12px `#444` at x=20 "wall time":** one solid bar x=90 width 600, fill `rgba(42,120,214,0.30)` with 2px `#2a78d6` edge, centered 12px label "user waits 60 s".
- **Bar 2 (y=175, 26px tall), label "CPU time":** six repeating pairs starting at x=90 — busy segment 2 s (20px, solid `#008300`) then wait segment 8 s (80px, `#e5e9ef` with 11px `#6b7280` label "wait" in the first two only). Busy total 12 s, wait total 48 s.
- **Brace annotation (bold 13px red `#e74c3c`, above bar 2 near x=400):** "48 s waiting on the database — invisible to CPU samples".
- **Side note (bold 12px `#008300`, right of bar 2 at x≈640, y=188):** "12 s busy".
- **Annotation (bold 13px orange `#d95926`, centered near y=270):** "profile the dimension that is actually slow".
- **Caption (12px `#444`, bottom right):** "timings illustrative; 6 bursts of 2 s CPU between 8 s waits".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all values are the hardcoded numbers above (no randomness); sample counts, seconds, and percentages are invented and labeled illustrative, but they must stay internally consistent: 5,000 samples, parse_timestamp 3,000 (60% / 30 s), score_model 400 (8% / 4 s), write_output 14% / 7 s, read_file 8% / 4 s, split_fields 4% / 2 s, other 6% / 3 s, totals 50 s before and 23 s after; the export job is 60 s wall = 12 s CPU + 48 s wait.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
