# Context Switching

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Context Switching

**Subtitle:** A CPU core only ever runs one process at a time — the OS makes a thousand feel simultaneous by switching between them fast, and every switch costs time

## The Morning Rush at One Espresso Machine

**Tags:** `core idea` (blue), `one core, many tasks` (green), `OS scheduler` (orange)

- **The shop** — one barista, one espresso machine, and a lunch rush of order tickets piling up
- **The juggle** — she works about 30 seconds on one drink, sets it aside, and picks up the next ticket
- **The switch** — each swap means noting where she stopped, clearing the counter, reading the new ticket
- **The illusion** — customers see every drink "in progress", yet the machine makes only one at a time
- **The computer** — a CPU core does the same: run one process, save its state, load the next one

*Example (italic):* At noon the barista has eight drinks "in progress", yet every second of espresso-machine time belongs to exactly one of them.

**Key point:** A context switch is the OS pausing one process, saving its state (registers, its place in the program), and restoring another's — one core fakes "many at once" by switching rapidly.

### Visualization (canvas `c1`, 720×300)

Two-row timeline for two orders (120s of work each): finishing one before the other vs juggling in 30s slices with a 5s switch cost between slices.

- **Title (bold 15px, `#1a5276`, top center):** "One Barista, Two Orders: Finishing vs Juggling".
- **Axes:** 2px `#999` time baseline at y=245 from x=60 to x=660 mapping 0–280s (600px / 280s); 12px `#444` tick labels every 60s below the line.
- **Row 1 (blocks 26px tall at y=85), 12px `#444` label "one at a time" at x=20:** blue `#2a78d6` block seconds 0–120 labeled "Order A" (12px white, centered), green `#008300` block 120–240 labeled "Order B"; 11px `#444` markers "A done 120s" and "B done 240s" above the block ends.
- **Row 2 (y=170), label "30s slices, 5s switch":** alternating blocks A,B,A,B... — blue A-slices at seconds `[0, 70, 140, 210]`, green B-slices at `[35, 105, 175, 245]`, each 30s wide; orange `#d95926` 5s slivers between every pair of slices (7 slivers), one labeled "5s switch" in 11px orange above; 11px markers "A done 240s" and "B done 275s".
- **Annotation (bold 13px orange `#d95926`, centered near y=140):** "juggling delayed A by 120s and finished 35s later overall".
- **Caption (12px `#444`, bottom right):** "drink and switch times illustrative".

## Four Lattes, Straight Through vs Round-Robin

**Tags:** `worked example` (blue), `hand-checkable` (green)

- **The setup** — four lattes, each needing 120s of actual machine work; putting a drink aside costs 5s
- **Straight through** — 4 × 120s = 480s total, and the first latte is ready at 120s
- **Round-robin** — 30s slices in order A B C D repeating: 16 slices with 15 switches between them
- **The overhead** — 15 switches × 5s = 75s, so the total time is 480 + 75 = 555s
- **The latency** — latte A's last slice is slice 13, so A is ready at 13×30 + 12×5 = 450s

*Example (italic):* Juggling made every latte later — the first one arrives at 450s instead of 120s, and the whole batch takes 555s instead of 480s.

**Key point:** Switching never adds capacity — the same 480s of work still happens, plus 75s of pure overhead; juggling only changes who waits, and here it makes everyone wait longer.

### Visualization (canvas `c2`, 720×300)

Horizontal stacked bar chart comparing total time and first-latte latency for the two schedules; work time in blue, switching overhead in orange.

- **Title (bold 15px, `#1a5276`, top center):** "Four Lattes: 480s of Work Either Way — Round-Robin Adds 75s".
- **Axis:** vertical 2px `#999` baseline at x=190, bars extend right, scale 470px / 600s; 12px `#444` x tick labels at 0/120/240/360/480/600 along y=250.
- **Row 1 (bar 26px tall at y=90), 12px `#444` label "one at a time — 480s" at x=20:** blue `#2a78d6` bar for 480s of work; dashed `#6b7280` (dash 4/3) vertical tick at the 120s position with 11px mute label "first latte 120s".
- **Row 2 (y=175), label "round-robin — 555s":** blue bar for 480s of work, then an appended orange `#d95926` segment for 75s labeled "75s switching" (11px, at the segment); dashed magenta `#d55181` vertical tick at the 450s position with 11px magenta label "first latte 450s".
- **Annotation (bold 13px magenta `#d55181`, near x=420, y=60):** "same work, later results — pure overhead plus terrible latency".
- **Caption (12px `#444`, bottom right):** "seconds exact for this toy example; the 5s switch cost is illustrative".

## Why 32 Workers Can Be Slower Than 4

**Tags:** `where you meet it` (blue), `parallel jobs` (green), `hidden cost` (orange)

- **The knob** — training jobs, data loaders, and web servers all ask you "how many workers?"
- **The ceiling** — a 4-core machine truly runs at most 4 processes at once; the rest wait their turn
- **The direct cost** — saving and restoring a process takes the OS a few microseconds per switch
- **The real cost** — the incoming process finds cold CPU caches, so its first stretch of work runs slow
- **The symptom** — CPU reads 100% busy while useful throughput falls: the machine is busy switching

*Example (italic):* A feature-extraction job set to 32 workers on a 4-core laptop finishes later than the same job set to 4 workers.

**Key point:** For CPU-bound work, match worker count to core count — beyond that, every extra worker adds context switches and cache evictions without adding any compute.

### Visualization (canvas `c3`, 720×300)

Line chart of job throughput vs worker count on a 4-core machine: rises to the core count, then decays as switching overhead grows.

- **Title (bold 15px, `#1a5276`, top center):** "Throughput on a 4-Core Machine: More Workers Is Not More Speed".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = worker counts `[1, 2, 4, 8, 16, 32, 256, 1000]` at 8 evenly spaced positions from x=90 to x=630 (12px `#444` labels); y = jobs/hour 0 to 400 with `#e5e9ef` gridlines at 100/200/300.
- **Throughput line:** blue `#2a78d6` 3px line with 4px dots through jobs/hour `[100, 195, 380, 360, 320, 240, 150, 60]`.
- **Peak marker:** green `#008300` 6px dot at the 4-worker point with bold 12px green label "peak at 4 workers = 4 cores"; vertical dashed `#6b7280` (dash 4/3) line at that x position.
- **Annotation (bold 13px orange `#d95926`, near the right end, y=110):** "1,000 workers: about 6× slower than 4".
- **Caption (12px `#444`, bottom right):** "throughput numbers illustrative; x spacing categorical, not to scale".

## Switching Is Not Parallelism

**Tags:** `common mistake` (red), `concurrency vs parallelism` (orange)

- **The confusion** — "the OS runs 1,000 processes" sounds parallel; it is 1,000 tasks sharing a few cores
- **Concurrency** — many tasks mid-flight taking turns on one core: the barista's crowded counter
- **Parallelism** — tasks running at the same instant, which needs more cores (more baristas, more machines)
- **The trap** — adding processes to a "slow" box adds switching, making it slower and inviting even more
- **The tell** — high CPU use, low output, and heavy "system time" point straight at switch overhead

*Example (italic):* Doubling the worker processes on a saturated 4-core machine doubled the juggling, not the espresso machines.

**Common mistake:** Believing more concurrent processes means more work done — on a fixed number of cores, extra processes only slice the same capacity thinner while a growing share of every second goes to switching.

### Visualization (canvas `c4`, 720×300)

Three stacked 100% bars splitting each CPU second into useful work vs switching overhead at 10, 100, and 1,000 runnable processes on the same machine.

- **Title (bold 15px, `#1a5276`, top center):** "Where the CPU Second Goes as Process Count Grows".
- **Axes:** baseline 2px `#999` at y=245; bars 90px wide centered at x=180/360/540, full bar height 180px = 100%; y gridlines `#e5e9ef` at 25/50/75% with 12px `#444` labels at x=50; 12px `#444` bar labels "10 processes" / "100 processes" / "1,000 processes" below the baseline.
- **Stacks (bottom = useful, top = switching):** useful work green `#008300` fill `rgba(0,131,0,0.30)` with 2px green top edge, shares `[98, 85, 40]`%; switching overhead orange `#d95926` solid, shares `[2, 15, 60]`%; bold 12px labels inside each segment ("98% useful", "2% switching", etc.), white on orange, `#2c3e50` on green.
- **Annotation (bold 13px orange `#d95926`, top area near y=52):** "at 1,000 processes, most of each second is spent switching, not working".
- **Caption (12px `#444`, bottom right):** "shares illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the worked-example arithmetic is exact for its stated inputs (4 lattes × 120s, 30s slices, 5s switch → 15 switches, 75s overhead, totals 480s vs 555s, first latte 120s vs 450s at slice 13); drink times, the 5s switch cost, the throughput curve `[100, 195, 380, 360, 320, 240, 150, 60]`, and the useful/switching shares `[98/2, 85/15, 40/60]` are invented and labeled illustrative.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
