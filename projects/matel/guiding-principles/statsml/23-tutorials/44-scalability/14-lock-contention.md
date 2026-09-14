# Lock Contention

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Lock Contention

**Subtitle:** When every thread must wait its turn on one lock, adding cores stops helping — Amdahl's law shows up as a production incident

## One Hot Row Every Checkout Touches

**Tags:** `core idea` (blue), `serialization` (orange), `hot lock` (red)

- **The sale** — a store runs a flash sale on one item; every checkout decrements its stock counter row
- **The lock** — the database locks that row per update, so checkouts pass through it one at a time
- **The queue** — 32 worker threads run in parallel, but at the counter they form a single-file line
- **The plateau** — throughput climbs with threads, flattens near 610 checkouts/s, then slips back
- **The definition** — lock contention: many threads serialize on one lock, so one shared section bounds the whole system

*Example (italic):* Going from 16 to 64 threads moves throughput from 610/s to 540/s — more threads, less work done.

**Key point:** A lock turns parallel work into serial work; past some concurrency, every extra thread only lengthens the line at the lock.

### Visualization (canvas `c1`, 720×300)

Line chart of checkout throughput vs number of worker threads: rises, plateaus, then falls as contention overhead grows.

- **Title (bold 15px, `#1a5276`, top center):** "More Threads, Same Counter: Throughput Plateaus, Then Falls".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = worker threads, evenly spaced tick labels `[1, 2, 4, 8, 16, 32, 64]` (12px `#444`); y = checkouts/s 0 to 700, gridlines `#e5e9ef` at 175/350/525.
- **Throughput line:** blue `#2a78d6` 3px line with 4px dots through threads `[1, 2, 4, 8, 16, 32, 64]`, checkouts/s `[95, 180, 330, 520, 610, 590, 540]`.
- **Peak marker:** vertical dashed `#6b7280` (dash 4/3) line at the 16-thread tick, 12px `#6b7280` label "peak 610/s" near its top.
- **Annotation (bold 13px red `#e74c3c`, above the right end of the line):** "64 threads do less than 16 — the lock is the bottleneck".
- **Caption (12px `#444`, bottom right):** "throughput numbers illustrative".

## Amdahl's Law by Hand: 5% Serial, 32 Cores

**Tags:** `worked example` (blue), `Amdahl's law` (green)

- **The split** — measure the checkout: 95% of the work runs in parallel, 5% happens under the counter lock
- **The formula** — speedup on N cores = 1 / (0.05 + 0.95/N); the serial 5% never shrinks
- **Hand-check** — N=32: 0.95/32 = 0.0297, plus 0.05 gives 0.0797, and 1/0.0797 ≈ 12.5× — not 32×
- **The ceiling** — as N → ∞ the parallel term vanishes: 1/0.05 = 20× is the hard cap, ever
- **Diminishing steps** — 32→64 cores buys 12.5×→15.4×; 64→128 buys only 15.4×→17.4×

*Example (italic):* With 5% of each checkout serialized on the lock, buying 32 cores instead of 1 yields at most ~12.5× the throughput.

**Key point:** Amdahl's law: speedup = 1/(s + (1−s)/N); a serial fraction s caps speedup at 1/s no matter how many cores you add.

### Visualization (canvas `c2`, 720×300)

Speedup-vs-cores curve for a 5% serial fraction, plotted against the ideal linear line and the 20× ceiling.

- **Title (bold 15px, `#1a5276`, top center):** "5% Serial Work Caps 32 Cores at ~12.5× (Amdahl's Law)".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = cores, evenly spaced tick labels `[1, 2, 4, 8, 16, 32, 64, 128]` (12px `#444`); y = speedup 0 to 32, gridlines `#e5e9ef` at 8/16/24, tick labels "8×"/"16×"/"24×"/"32×".
- **Ideal line:** gray `#6b7280` 2px dashed (dash 5/4) through cores `[1, 2, 4, 8, 16, 32]`, speedup `[1, 2, 4, 8, 16, 32]` (clips at the top of the plot), 12px `#6b7280` label "ideal linear" along it.
- **Amdahl curve:** blue `#2a78d6` 3px line with 4px dots through cores `[1, 2, 4, 8, 16, 32, 64, 128]`, speedup `[1.00, 1.90, 3.48, 5.93, 9.14, 12.55, 15.42, 17.41]`.
- **Ceiling line:** orange `#d95926` 2px dashed horizontal at speedup 20, bold 12px `#d95926` label "hard cap 20× = 1/0.05" above it at the right.
- **Point callout (bold 13px `#1a5276`, at the 32-core dot):** "32 cores → 12.5×".
- **Caption (12px `#444`, bottom right):** "speedup values exact from 1/(0.05+0.95/N)".

## The Incident: CPU Idle, Latency on Fire

**Tags:** `where it's used` (blue), `symptoms` (orange), `production` (red)

- **The page** — flash sale starts at 12:05; by 12:20 p99 checkout latency is 2,100 ms and climbing
- **The paradox** — CPU utilization *drops* from 70% to ~35%: threads are asleep waiting on the lock, not computing
- **The false fix** — on-call doubles the instance count; latency barely moves — the hot row is still one row
- **The tell** — throughput plateaus as concurrency rises, then falls as lock handoffs add pure overhead
- **The read** — idle CPU plus high latency plus a plateau is the signature of contention, not of undersized hardware

*Example (italic):* At 12:25 the dashboard shows 34% CPU and 2,400 ms p99 — the machine is bored and the users are furious.

**Key point:** Lock contention inverts the usual capacity signal: the system is slow *because* threads are waiting, so CPU looks healthy while latency explodes.

### Visualization (canvas `c3`, 720×300)

Dual-line incident timeline: p99 latency (left axis) climbs while CPU utilization (right axis) falls after the flash sale starts.

- **Title (bold 15px, `#1a5276`, top center):** "12:05 Flash Sale: Latency ×30 While CPU Goes Idle".
- **Axes:** origin x=60, baseline y=245, plot width 590, plot height 180; x = clock time "12:00" to "12:40", 12px `#444` tick labels every 10 min; left y = p99 ms 0 to 2500, gridlines `#e5e9ef` at 625/1250/1875, labels "0"/"1250"/"2500 ms" in 12px red `#e74c3c`; right y (labels at x=660) = CPU % 0 to 100, "0%"/"50%"/"100%" in 12px blue `#2a78d6`.
- **Latency line:** red `#e74c3c` 3px line through minutes `[0, 5, 10, 15, 20, 25, 30, 35, 40]`, p99 ms `[80, 120, 600, 1400, 2100, 2400, 2350, 2400, 2380]`.
- **CPU line:** blue `#2a78d6` 2px dashed (dash 6/4) through the same minutes, CPU % `[65, 70, 55, 45, 38, 34, 35, 34, 35]`.
- **Sale marker:** vertical dashed `#6b7280` (dash 4/3) line at minute 5, 12px `#6b7280` label "sale starts" at its top.
- **Annotations:** bold 13px red `#e74c3c` near (minute 27, upper area) "p99 2,400 ms"; bold 13px blue `#2a78d6` near (minute 27, lower area) "CPU 34% — threads blocked, not busy".
- **Caption (12px `#444`, bottom right):** "incident timeline illustrative".

## The Fix Is Less Lock, Not More Cores

**Tags:** `common mistake` (red), `fixes` (green)

- **The mistake** — throwing cores or replicas at a serialized section; Amdahl says the 5% still rules
- **Shrink it** — do only the decrement under the lock; move validation, logging, and pricing outside
- **Shard it** — split the one counter into 16 shard rows; a checkout picks one, contention drops 16-fold
- **Go lock-free** — an atomic decrement shrinks an in-memory counter's lock to one instruction
- **Accumulate locally** — each thread counts sales privately and flushes to the shared row every 100 ms

*Example (italic):* Sharding the stock counter into 16 rows takes 32-thread throughput from 590/s to about 4,800/s — no new hardware.

**Common mistake:** Scaling out a contended system. Adding cores raises N in 0.95/N but never touches the 0.05 — only shrinking or splitting the serial section moves the ceiling.

### Visualization (canvas `c4`, 720×300)

Horizontal bar chart: checkout throughput at 32 threads under four designs, from one global lock to per-thread accumulation.

- **Title (bold 15px, `#1a5276`, top center):** "Same 32 Threads, Four Designs: Shrink the Serial Section".
- **Layout:** bars start at x=230, max width 440, 26px tall; four rows at y = 70, 120, 170, 220, each with a right-aligned 12px `#444` label ending at x=220.
- **Rows (label — color, bar width px, 12px value label at bar end):**
  - "one lock, fat critical section" — red `#e74c3c`, width 25, "590/s"
  - "one lock, decrement only" — orange `#d95926`, width 90, "2,100/s"
  - "16 shard counters" — blue `#2a78d6`, width 205, "4,800/s"
  - "per-thread + 100 ms flush" — green `#008300`, width 380, "8,900/s"
- **Bar style:** solid fills at 0.85 alpha, 2px `#999` vertical baseline at x=230.
- **Annotation (bold 13px green `#008300`, above the bottom bar):** "15× the throughput — zero new cores".
- **Caption (12px `#444`, bottom right):** "throughput numbers illustrative; bar widths proportional".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the Amdahl speedup values in c2 are exact evaluations of 1/(0.05+0.95/N) and the 20× ceiling is exact (1/0.05); the throughput curves (c1, c4) and the incident timeline (c3) are invented and labeled illustrative; text numbers must match chart numbers.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
