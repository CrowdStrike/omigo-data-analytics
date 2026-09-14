# Processes vs Threads

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Processes vs Threads

**Subtitle:** Two ways to run work in parallel — separate kitchens that share nothing, or several cooks sharing one kitchen — trading isolation for cheap sharing

## Eight Cake Orders, Two Ways to Staff the Bakery

**Tags:** `core idea` (blue), `isolation` (green), `shared memory` (orange)

- **The bakery** — 8 cake orders land at once; the owner must decide how to run them in parallel
- **Option A: processes** — 8 separate kitchens, each with its own oven, pantry, and recipe book
- **Option B: threads** — one kitchen where 8 cooks share the same oven, pantry, and recipe book
- **The wall** — kitchens can't see into each other; cooks in one kitchen see every shelf instantly
- **The trade** — separate kitchens are safe but expensive; shared cooks are cheap but can collide

*Example (italic):* Cook 3 drops a mixer in a shared kitchen and every cook stops; in kitchen 3 of eight, the other seven never notice.

**Key point:** A process is a program with its own private memory; a thread is a worker inside a process sharing that memory — the whole difference is what is walled off and what is shared.

### Visualization (canvas `c1`, 720×300)

Side-by-side architecture diagram: two isolated process boxes with private memory on the left, one process box containing two threads over shared memory on the right.

- **Title (bold 15px, `#1a5276`, top center):** "Two Isolation Models: Private Memory vs One Shared Kitchen".
- **Left panel (x 30–345):** 13px bold `#2c3e50` label "processes" at (40, 60); two rounded boxes 130×150 at (55, 80) and (200, 80), 2px `#2a78d6` border, fill `rgba(42,120,214,0.10)`, each labeled 12px "process A" / "process B" with inner 110×40 boxes fill `rgba(42,120,214,0.25)` labeled "own memory" and an inner 110×30 box labeled "1 thread"; a vertical 3px `#1a5276` wall line at x=192 between them with 11px `#6b7280` label "no sharing".
- **Right panel (x 375–690):** 13px bold `#2c3e50` label "threads" at (385, 60); one rounded box 290×150 at (390, 80), 2px `#008300` border, fill `rgba(0,131,0,0.08)`, labeled "one process"; inside, a bottom 260×40 box fill `rgba(0,131,0,0.22)` labeled "shared memory" and two top 120×35 boxes fill `rgba(0,131,0,0.12)` labeled "thread 1" / "thread 2", each with a 2px `#008300` arrow down to the shared box.
- **Annotation (bold 13px orange `#d95926`, centered near y=270):** "processes wall memory off; threads all touch the same shelves".
- **Caption (12px `#444`, bottom right):** "schematic — two workers drawn per model for clarity".

## Counting the Memory Bill: 960 MB vs 184 MB

**Tags:** `worked example` (blue), `memory cost` (green)

- **The program** — the report worker's code, libraries, and loaded lookup tables total 120 MB
- **Per process** — worst case (each worker loads its own data): a full 120 MB copy per process
- **Per thread** — threads share the one 120 MB copy and add only a private 8 MB stack each
- **Process math** — 8 workers as processes: 8 × 120 = 960 MB
- **Thread math** — 8 workers as threads: 120 + 8 × 8 = 184 MB
- **Hand-check** — at 4 workers the same math gives 480 MB vs 152 MB; redo it on paper in seconds

*Example (italic):* Scaling from 1 to 8 workers costs 840 extra MB with processes but only 56 extra MB with threads — a 15× gap.

**Key point:** Threads are cheap because they share the program's memory; processes pay the full image again per worker — that is the price of the wall.

### Visualization (canvas `c2`, 720×300)

Grouped bar chart of total memory vs worker count: process model (blue bars) grows steeply, thread model (green bars) stays nearly flat.

- **Title (bold 15px, `#1a5276`, top center):** "Memory for N Workers: 120 MB Image + 8 MB per Thread Stack".
- **Axes:** origin x=70, baseline y=245, plot width 590, plot height 180; y = MB 0 to 1000, gridlines `#e5e9ef` at 250/500/750 with 12px `#444` labels; x = worker counts `[1, 2, 4, 8]` as four groups, 12px `#444` labels "1", "2", "4", "8 workers" centered under each group.
- **Process bars (blue `#2a78d6`, fill `rgba(42,120,214,0.35)`, 2px solid edge):** MB `[120, 240, 480, 960]`, 40px wide, left bar of each group.
- **Thread bars (green `#008300`, fill `rgba(0,131,0,0.30)`, 2px solid edge):** MB `[128, 136, 152, 184]`, 40px wide, right bar of each group with 8px gap.
- **Value labels:** 11px `#444` MB numbers on top of every bar.
- **Annotation (bold 13px green `#008300`, near the 8-worker group, y=90):** "8 threads: 184 MB — 8 processes: 960 MB".
- **Caption (12px `#444`, bottom right):** "image and stack sizes illustrative; the arithmetic is exact".

## Where Python Forces the Choice

**Tags:** `where it's used` (blue), `Python` (orange), `CPU vs I/O` (green)

- **The daily meeting** — a data scientist parallelizing a pandas job picks `threading` or `multiprocessing`
- **The GIL** — CPython lets only one thread run Python bytecode at a time, so CPU work can't share cores
- **CPU-bound** — a 4-worker feature computation: 1 worker 80s, 4 threads 79s, 4 processes 21s
- **I/O-bound** — threads shine when workers mostly wait: 100 API downloads overlap fine on one core
- **Crash blast radius** — one process segfaulting kills one job; one thread segfaulting kills all 4

*Example (italic):* Switching a CPU-bound scoring loop from 4 threads to 4 processes cuts the run from 79s to 21s — the threads were queueing behind the GIL.

**Key point:** Pick by workload: processes for CPU-bound work and crash isolation, threads for cheap sharing and I/O waiting — in CPython the GIL makes this choice sharp.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart of wall-clock time for the same CPU-bound job under three setups: 1 worker, 4 threads, 4 processes.

- **Title (bold 15px, `#1a5276`, top center):** "Same CPU-Bound Job, Three Setups: Only Processes Use 4 Cores".
- **Axis:** bars start at x=190, max width 460 mapping 0–80s; vertical 2px `#999` baseline at x=190; 12px `#444` second markers "0s", "20s", "40s", "60s", "80s" along y=260 with light `#e5e9ef` gridlines.
- **Rows (bar centers at y = 85, 145, 205; 26px tall; left-aligned 12px `#444` labels at x=20):**
  - "1 worker — 80s": mute `#6b7280` bar, fill `rgba(107,114,128,0.35)`, width 460
  - "4 threads — 79s": blue `#2a78d6` bar, fill `rgba(42,120,214,0.35)`, width 454
  - "4 processes — 21s": green `#008300` bar, fill `rgba(0,131,0,0.30)`, width 121
- **Value labels:** bold 12px matching-color time labels ("80s", "79s", "21s") just past each bar end.
- **Annotation (bold 13px magenta `#d55181`, near x=420, y=145):** "4 threads ≈ 1 worker: the GIL serializes CPU work".
- **Caption (12px `#444`, bottom right):** "timings illustrative — CPython, CPU-bound loop".

## Threads Share Everything — Including Bugs

**Tags:** `common mistake` (red), `race condition` (orange)

- **The mistake** — treating threads as "just lighter processes" and skipping locks on shared data
- **The setup** — two threads each add 1 to a shared counter 1,000 times; the expected total is 2,000
- **The collision** — both read 500, both write 501: one of the two increments silently vanishes
- **The runs** — five unlocked runs end at 2,000, 1,743, 1,918, 1,512, 1,379 — sometimes right, often not
- **The fix** — a lock around the increment (or a process per worker) makes every run end at 2,000

*Example (italic):* The counter bug never appears in the single-threaded test and passes 1 run in 5 by luck — the worst kind of failure to debug.

**Common mistake:** Assuming shared memory is safe because each step looks tiny. Read-modify-write on shared data needs a lock; processes dodge the bug entirely because nothing is shared by default.

### Visualization (canvas `c4`, 720×300)

Bar chart of the final counter value across five unlocked runs against the expected 2,000 line, plus one locked run that hits the target.

- **Title (bold 15px, `#1a5276`, top center):** "Two Threads, 1,000 Increments Each: Where Did the Counts Go?".
- **Axes:** origin x=70, baseline y=245, plot width 590, plot height 180; y = final counter 0 to 2,000, gridlines `#e5e9ef` at 500/1,000/1,500 with 12px `#444` labels; x = six bars 60px wide with 12px `#444` labels "run 1"–"run 5" and "locked".
- **Unlocked bars:** values `[2000, 1743, 1918, 1512, 1379]`; the 2,000 bar blue `#2a78d6` fill `rgba(42,120,214,0.35)`, the four short bars red `#e74c3c` fill `rgba(231,76,60,0.15)` with 2px red edges; bold 11px value labels on top.
- **Locked bar:** green `#008300` fill `rgba(0,131,0,0.30)`, value 2,000, bold 11px green label "2000".
- **Expected line:** dashed `#6b7280` (dash 5/4) horizontal line at y for 2,000, 12px `#6b7280` label "expected 2,000" at its left end.
- **Annotation (bold 13px red `#e74c3c`, near run 5, y=95):** "run 5 lost 621 increments — no crash, no error".
- **Caption (12px `#444`, bottom right):** "run outcomes illustrative; losses vary run to run".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); memory bars use image 120 MB + 8 MB per thread stack (illustrative sizes, exact arithmetic: process MB `[120, 240, 480, 960]`, thread MB `[128, 136, 152, 184]` at workers `[1, 2, 4, 8]`); CPU-bound timings 80s / 79s / 21s are illustrative; counter runs `[2000, 1743, 1918, 1512, 1379]` plus locked 2000 are illustrative.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
