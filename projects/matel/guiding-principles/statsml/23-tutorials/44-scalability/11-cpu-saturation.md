# CPU Saturation

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** CPU Saturation

**Subtitle:** CPU % says how busy the cores are; the run queue says how much work is waiting — and the waiting is what your users feel

## Busy Is Not the Same as Backed Up

**Tags:** `core idea` (blue), `run queue` (green), `load average` (orange)

- **The server** — an 8-core API server handles a morning traffic ramp starting at 9:00am
- **The gauge** — CPU utilization climbs 35% → 100% by 9:30 and then just sits pinned at 100%
- **The queue** — load average (runnable + disk-wait on Linux) climbs past 8: 10, 16, 24 by 10:00am
- **The meaning** — at load 8 all 8 cores are busy and nothing waits; at load 24, 16 threads are waiting
- **The blind spot** — utilization saturates at 100% and stops changing; the backlog does not stop growing

*Example (italic):* At 9:35 and at 10:00 the dashboard shows the same "CPU 100%", but load has gone from 10 to 24 — the second situation is far worse.

**Key point:** CPU saturation is measured by the run queue, not the utilization gauge — load average above the core count means work is waiting, and utilization can't show how much.

### Visualization (canvas `c1`, 720×300)

Dual-line timeline of the morning ramp: CPU % pins at 100 while load average keeps climbing past the 8-core line.

- **Title (bold 15px, `#1a5276`, top center):** "9:00–10:00am Ramp: CPU % Pins at 100, Load Keeps Climbing".
- **Axes:** origin x=60, baseline y=245, plot width 590, plot height 180; x = minutes after 9:00, tick labels "9:00"–"10:00" every 15 min (12px `#444`); left y = CPU % 0–100 (12px blue labels at 0/50/100); right y = load average 0–24 (12px orange labels at 0/8/16/24 at x=660); gridlines `#e5e9ef` at left-axis 25/50/75.
- **CPU % line:** blue `#2a78d6` 3px through minutes `[0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60]`, CPU % `[35, 50, 65, 80, 92, 98, 100, 100, 100, 100, 100, 100, 100]` — flat at 100 from minute 30.
- **Load line:** orange `#d95926` 3px through the same minute grid, load `[2.8, 4.0, 5.2, 6.4, 7.4, 7.9, 8.2, 10, 13, 16, 19, 22, 24]`, plotted against the right-hand 0–24 scale.
- **Core-count line:** horizontal dashed `#6b7280` (dash 4/3) at load 8 on the right scale, 12px `#6b7280` label "8 cores" at its left end.
- **Annotation (bold 13px orange `#d95926`, near minute 42, above the load line):** "CPU % stopped talking at 9:30 — the queue didn't".
- **Caption (12px `#444`, bottom right):** "traffic ramp illustrative".

## Load 24 on 8 Cores: the 2× Wait, by Hand

**Tags:** `worked example` (blue), `queueing math` (green)

- **Per-core queue** — load 24 on 8 cores means 24 / 8 = 3 runnable threads sharing each core
- **The share** — each thread gets 1/3 of a core, so 50ms of CPU work stretches to 150ms of wall time
- **The wait** — 150ms wall − 50ms running = 100ms waiting: every thread waits 2× its own run time
- **The ladder** — same 50ms burst: load 4 → 50ms, load 12 → 75ms, load 16 → 100ms, load 24 → 150ms
- **The rule** — when load ≥ cores, wall time ≈ run time × (load / cores); below the core count, wait ≈ 0

*Example (italic):* A request needing 50ms of CPU returns in 50ms at load 4 but 150ms at load 24 — the extra 100ms is pure run-queue waiting, not extra work.

**Key point:** Divide load average by core count: 1.0 means fully busy with no waiting; 3.0 means every runnable thread spends about two-thirds of its wall time waiting for a core.

### Visualization (canvas `c2`, 720×300)

Stacked horizontal bars: the same 50ms CPU burst at four load levels, split into run time (constant) and queue wait (growing).

- **Title (bold 15px, `#1a5276`, top center):** "The Same 50ms of Work at Four Load Levels (8 cores)".
- **Layout:** bars start at x=190, 1ms = 3px so max bar (150ms) is 450px; 2px `#999` vertical baseline at x=190; rows at y = 75, 120, 165, 210, bars 24px tall; left-aligned 12px `#444` row labels at x=20: "load 4 (0.5/core)", "load 8 (1.0/core)", "load 16 (2.0/core)", "load 24 (3.0/core)".
- **Run segment:** blue `#2a78d6` fill `rgba(42,120,214,0.30)` with 2px blue border, width 150px (50ms) on every row.
- **Wait segment:** appended after the run segment, solid orange `#d95926`, widths `[0, 0, 150, 300]`px for waits `[0, 0, 50, 100]`ms.
- **End labels (11px `#444` just past each bar):** "50ms", "50ms", "100ms", "150ms".
- **Legend (12px, top right under title):** blue swatch "running", orange swatch "waiting for a core".
- **Annotation (bold 13px orange `#d95926`, below the load-24 bar near y=245):** "at load 24, waiting = 2× running".
- **Caption (12px `#444`, bottom right):** "even time-slicing assumed; numbers illustrative".

## Why Latency Bends Long Before 100%

**Tags:** `where it's used` (blue), `tail latency` (green), `what to watch` (orange)

- **Bursty arrivals** — requests arrive in clumps, so brief queues form even when cores average 70% busy
- **The knee** — on the 8-core box, p50 latency is 63ms at 50% but 100ms at 80% and 210ms at 95%
- **Two states** — a thread on a core is CPU-bound; a runnable thread in the queue is waiting, not working
- **Better gauges** — watch load average ÷ cores and run-queue (scheduler) latency, not just "CPU %"
- **The signal** — rising scheduler delay with flat CPU % is the earliest clean sign of saturation

*Example (italic):* The 8-core server's p50 climbs from 63ms at 50% to 100ms at 80% — double the 50ms of actual CPU work, and the pain starts a full 20 points before the gauge reads 100.

**Key point:** Latency follows the queue, and queues grow steeply as utilization approaches the core count — so "CPU is only 80%" is exactly when to start worrying, not when to relax.

### Visualization (canvas `c3`, 720×300)

Hockey-stick curve: p50 request latency vs average CPU utilization on the 8-core server, with the knee marked near 80%.

- **Title (bold 15px, `#1a5276`, top center):** "Latency vs Utilization: the Knee Is Near 80%, Not 100%".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = average CPU utilization 10%–95%, 12px `#444` tick labels at 20/40/60/80/95; y = p50 latency 0–220ms, gridlines `#e5e9ef` at 50/100/150/200 with 12px labels.
- **Curve:** blue `#2a78d6` 3px line through utilization `[10, 20, 30, 40, 50, 60, 70, 80, 90, 95]`%, latency `[52, 54, 56, 59, 63, 70, 81, 100, 145, 210]`ms.
- **Service floor:** horizontal dashed `#6b7280` (dash 4/3) at 50ms, 12px `#6b7280` label "50ms of actual CPU work" near its left end.
- **Knee marker:** vertical dashed `#d95926` at 80%, filled orange dot on the curve at (80, 100).
- **Annotation (bold 13px orange `#d95926`, near x=55%, y=70):** "p50 doubles by 80% — everything above the floor is queue wait".
- **Caption (12px `#444`, bottom right):** "illustrative M/M/c-shaped curve".

## More Threads Don't Make More Cores

**Tags:** `common mistake` (red), `context switches` (orange)

- **The reflex** — "requests are queueing, add worker threads" — the team bumps the pool 8 → 64
- **The ceiling** — 8 cores can only ever run 8 CPU-bound threads at once; the rest just enter the queue
- **The overhead** — each extra runnable thread adds context switches and cache evictions on every slice
- **The numbers** — throughput: 3,400 req/s at 8 threads, 3,150 at 32, 2,300 at 128 — down 32% from peak
- **The fix** — for CPU-bound work, size the pool near the core count and shed or queue load upstream

*Example (italic):* Going from 8 to 128 worker threads on the 8-core box cut throughput from 3,400 to 2,300 req/s — the cores spent their slices switching instead of serving.

**Common mistake:** Treating a long run queue as a thread-shortage problem. If the work is CPU-bound, adding threads past the core count adds only waiting and context-switch overhead — throughput falls while load average climbs.

### Visualization (canvas `c4`, 720×300)

Throughput vs worker-thread count on the 8-core server: rises to the core count, then decays as context-switch overhead grows.

- **Title (bold 15px, `#1a5276`, top center):** "8 Cores: Throughput Peaks at 8 Threads, Then Decays".
- **Axes:** origin x=70, baseline y=245, plot width 580, plot height 180; x = worker threads at evenly spaced positions labeled `2, 4, 8, 16, 32, 64, 128` (12px `#444`); y = throughput 0–3,600 req/s, gridlines `#e5e9ef` at 1000/2000/3000 with 12px labels "1k"/"2k"/"3k".
- **Throughput line:** green `#008300` 3px line with 4px filled dots through threads `[2, 4, 8, 16, 32, 64, 128]`, req/s `[900, 1800, 3400, 3350, 3150, 2800, 2300]`.
- **Peak marker:** vertical dashed `#6b7280` (dash 4/3) at the 8-thread position, 12px `#6b7280` label "threads = cores" at its top.
- **Decay shading:** light red fill `rgba(231,76,60,0.08)` over the plot region right of the 8-thread line.
- **Annotation (bold 13px red `#e74c3c`, near the 64-thread position, y=100):** "past 8: same cores, more switching, −32% at 128".
- **Caption (12px `#444`, bottom right):** "CPU-bound workload, throughput illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the load/CPU ramp, latency curve, and throughput numbers are invented and labeled illustrative; the queueing arithmetic (load 24 / 8 cores = 3 per core → 50ms burst takes 150ms, waiting = 2× running) is exact and the text numbers must match the chart arrays.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
