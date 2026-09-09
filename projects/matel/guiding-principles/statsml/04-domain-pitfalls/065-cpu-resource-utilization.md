# CPU / Resource Utilization

**Page type:** detail page (one h2 + one-row obj-table per pitfall: text left 50%, canvas right 50%)
**HTML title tag:** CPU / Resource Utilization - Domain-Specific Pitfalls

**Subtitle:** Statistical pitfalls in compute metrics — the lies of averages, invisible overhead, and metrics that miss the actual bottleneck.

## Average CPU Hides Bimodality

**50% Average = 30s Fully Idle, Then 30s Pegged at 100%**

- **The pattern:** The server sits idle for 30 seconds, then runs pegged at 100% for 30 seconds.
- **The arithmetic:** The mean lands at exactly 50%, matching neither operational state.
- **Why it hurts:** Performance is terrible during the pegged phase and fine during the idle one.
- **What the mean omits:** A single average carries no information about the shape of usage.
- **What to track:** The full distribution — a bimodal load has no meaningful summary number.

### Visualization (canvas `canvas1`, 720×200 — HTML attribute height 300 but setup helper forces 720×200)

Bimodal frequency distribution of CPU utilization with a dashed "average" line through the empty middle.

- **Background:** light `#fafbfc`; L-shaped `#333` axes (width 1.5) from (60,20) down to (60,160) then right to (680,160).
- **X labels (13px `#333`, centered, 124px apart):** "0%", "20%", "40%", "60%", "80%", "100%".
- **Y-axis title:** rotated vertical "Frequency" at left.
- **Distribution:** sum of two Gaussians, peaks at 5% and 95% utilization (sigma 0.08, height 120px, +2 floor), plotted across the 620px width; filled `rgba(41,128,185,0.3)`, stroked `#2980b9` width 2.
- **Average line:** vertical dashed red `#e74c3c` (dash 6/4, width 2) at 50% (x = 60+310), from y=25 to y=155.
- **Average labels:** bold red 14px above the line: "Reported Avg: 50%"; red 12px below the axis: "(almost no samples here!)".
- **Peak labels (bold blue `#2980b9` 13px):** "Idle peak" near left peak (95,30), "Pegged peak" near right peak (640,30).

## Steal Time in VMs

**4 Cores Provisioned, 2.5 Delivered — Steal Time Takes the Rest**

- **The mechanism:** The hypervisor hands your cycles to noisy neighbors on the same host.
- **What you see:** The VM reports "CPU idle" while that time was actually taken away.
- **Dashboard blind spot:** The "steal%" metric is usually ignored or never plotted at all.
- **Real delivery:** A container that thinks it has 4 cores effectively gets about 2.5.
- **At scale:** Without steal-time awareness, capacity planning is based on fiction.

### Visualization (canvas `canvas2`, 720×200 — HTML attribute height 300 but setup helper forces 720×200)

Two stacked 100% bars comparing reported vs actual CPU breakdown, with legend.

- **Column titles (bold 14px `#333`, centered):** "What VM Reports" (x=200) and "Reality (with Steal)" (x=520), at y=20.
- **Bars:** each 140 wide × 140 tall starting at y=35, outlined `#2980b9` (width 1.5).
  - Left bar (x=130): user 30% green `#2ecc71`, system 20% orange `#f39c12`, idle 50% light gray `#ecf0f1`.
  - Right bar (x=450): user 30% green, system 20% orange, steal 25% red `#e74c3c`, actual idle 25% light gray.
- **Segment labels (bold 13px, white on colored segments, `#666` on idle):** left bar "30% user", "20% sys", "50% idle"; right bar "30% user", "20% sys", "25% steal", "25% idle".
- **Legend (x=610, 14×14 swatches, 13px `#333` text):** green "User", orange "System", red "Steal", light gray (with `#bbb` border) "Idle".
- **Annotations (red `#e74c3c`, centered under bars):** 12px "Looks fine!" under the left bar; bold 12px "Half your \"idle\" is stolen!" under the right bar.

## Context Switch Overhead Unmeasured

**10,000 Switches/sec Burns 5-10% That CPU% Never Shows**

- **What CPU% counts:** Only work time; the overhead lives in the gaps between measurements.
- **The hidden tax:** At 10,000 context switches per second you lose 5-10% of the CPU.
- **Where it goes:** Saving and restoring register state, cache flushes, TLB invalidations.
- **Thread-count effect:** More threads mean more switches, so the invisible tax grows.
- **The illusion:** CPU reports "100% utilized" while only ~92% is actually useful work.

### Visualization (canvas `canvas3`, 720×200 — HTML attribute height 300 but setup helper forces 720×200)

Horizontal execution timeline of colored work blocks separated by thin red context-switch gaps.

- **Title (bold 14px `#333`, top center):** "Zoomed CPU Timeline — Work Blocks + Context Switch Gaps".
- **Timeline:** row of blocks 50px tall starting at (30,40), outlined overall in `#333`; alternating work blocks in green `#2ecc71`, blue `#3498db`, purple `#9b59b6` with widths 45, 55, 35, 60, 40, 50, 55, 45, 38, 50, 42, each separated by a 4px-wide red `#e74c3c` context-switch sliver (stroked `#c0392b`) — 10 red slivers total.
- **Legend (14×14 swatches at x=30 from y=105, 12px `#333` text):** green "Thread A work", blue "Thread B work", purple "Thread C work", red "Context switch (invisible in CPU%)".
- **Right-aligned annotations at x=690 (red `#e74c3c`):** bold 14px "10 switches shown = ~8% of this timeline"; 13px lines "At 10,000/sec: 5-10% hidden overhead", "CPU reports: \"100% utilized\"", "Reality: ~92% useful work".

## NUMA Effects

**Identical 70% CPU, 40% Less Throughput on the Wrong NUMA Node**

- **The penalty:** Memory on the wrong NUMA node costs 2-3x the access latency.
- **What CPU% shows:** Local and remote access both read as the same 70% utilization.
- **The actual gap:** Throughput differs by 40% between two identical-looking runs.
- **Blind metric:** "CPU%" cannot say whether the memory access pattern is optimal.
- **What to measure:** Memory controller metrics and NUMA hit/miss ratios instead.

### Visualization (canvas `canvas4`, 720×200 — HTML attribute height 300 but setup helper forces 720×200)

Grouped bar comparison: identical CPU-utilization bars vs divergent throughput bars.

- **Title (bold 14px `#333`, top center):** "Same CPU Utilization (70%) — Different Throughput".
- **Left group (header bold 13px `#1a5276` "CPU Utilization" at x=160):** two identical blue `#2980b9` bars (60 wide, 70% of 130px max height) at x=90 and x=190, each labeled "70%" with sublabels "Local" and "Remote" (12px `#333`); a large bold gray `#666` "=" between them.
- **Arrow:** bold 20px `#333` "→" at x=320 between groups.
- **Right group (header bold 13px `#1a5276` "Actual Throughput" at x=520):** green `#27ae60` full-height bar (70 wide, 100%) at x=430 with white bold "100%" inside, sublabel "Local NUMA"; red `#e74c3c` bar at 60% height at x=540 with white bold "60%" inside, sublabel "Remote NUMA".
- **Annotation (bold red 13px, left-aligned at x=620):** "40% throughput loss" / "invisible in CPU%!".

## GC Pauses as Periodic Spikes

**Avg Latency 5ms, p99 200ms — One GC Pause Every 30 Seconds**

- **The cycle:** Java GC stops the world for 200ms roughly every 30 seconds.
- **The split:** Average latency reads 5ms while p99 sits at the full 200ms.
- **Why averages hide it:** Too rare to move the mean, yet it owns the entire tail.
- **Not schedulable:** The pattern is periodic but never perfectly predictable.
- **What to track:** Percentiles — mean-latency dashboards erase freezes users clearly feel.

### Visualization (canvas `canvas5`, 720×200 — HTML attribute height 300 but setup helper forces 720×200)

Latency time series: noisy 5ms baseline with three 200ms GC spikes, plus dashed avg and p99 reference lines.

- **Axes:** L-shaped `#333` axes (width 1.5) from (55,15) to (55,160) to (695,160). Y labels (12px `#333`, right-aligned): "200ms", "100ms", "5ms", "0ms". X labels (centered): "0s" (x=70), "30s" (x=230), "60s" (x=390), "90s" (x=550), plus axis title "Time" (x=380, y=192). Scale: y=160 is 0ms, linear to 200ms over 135px.
- **Baseline series:** blue `#2980b9` line (width 1.5), deterministic pseudo-random noise 3–9ms across x=60–690.
- **GC spikes:** three narrow red `#e74c3c` triangular spikes (width 3) at x=230, 390, 550, rising from 5ms to 200ms, each capped with a filled 4px-radius red dot and a red 11px "GC" label above.
- **Reference lines:** dashed green `#27ae60` horizontal line at 5ms; red `#e74c3c` horizontal line at 200ms (dash 6/4, width 1.5).
- **Labels (bold 13px, left-aligned at x=560):** green "Avg: 5ms (looks great!)" below the 5ms line; red "p99: 200ms (GC pause)" above the 200ms line.

## Cgroup Limits vs Actual Usage

**Usage Never Reaches the 2.0-Core Limit, Yet Throttling Starts at 1.9**

- **The setup:** Container limited to 2 CPU cores, actually consuming about 1.5.
- **The mechanism:** The limit itself changes behavior — the kernel throttles at 1.9 cores.
- **Invisible cost:** Throttle events never show up as "usage," only as slowdowns.
- **The paradox:** Usage never reports exceeding the limit, yet response times degrade.
- **What to watch:** CFS throttle counters; no CPU metric captures those spikes directly.

### Visualization (canvas `canvas6`, 720×200 — HTML attribute height 300 but setup helper forces 720×200)

Rising CPU-usage line approaching a dashed cgroup limit, with a shaded throttle zone.

- **Axes:** L-shaped `#333` axes from (55,15) to (55,165) to (695,165). Y labels (cores, 12px `#333`, right-aligned): "2.5", "2.0", "1.5", "1.0", "0.5", "0". X title "Time" centered at (380,192). Scale: y=165 is 0 cores, 2.5 cores spans 147px.
- **Limit line:** horizontal dashed red `#e74c3c` (dash 8/4, width 2) at 2.0 cores, labeled bold red 13px left-aligned at x=560: "Cgroup Limit: 2.0 cores".
- **Throttle zone:** band from 1.8 to 2.0 cores shaded `rgba(231,76,60,0.12)`, labeled bold red 12px at x=560: "THROTTLE ZONE".
- **Usage series:** blue `#2980b9` line (width 2.5) rising from ~0.8 cores toward a 1.95 plateau with small pseudo-random noise (±0.05), clamped to at most 1.97 cores; labeled bold blue 12px "Reported CPU Usage" at x=65 near 0.9 cores.
- **Throttle-event dots:** semi-transparent red `rgba(231,76,60,0.5)` 2px dots overlaid wherever usage exceeds 1.8 cores.
- **Annotation (dark red `#c0392b` 12px, centered at x=350 near 2.2 cores):** "Usage never exceeds limit..." / "but throttling causes invisible latency spikes".

## Regeneration instructions

- **Layout:** domains detail-page convention — h1, `.subtitle`, then per pitfall an unnumbered `<h2>` (1.4em `#1a5276`, 2px solid `#2980b9` bottom border) followed by a one-row `.obj-table`: left `<td>` (40%) with `.obj-title` (the bold one-line punchline) plus a `<ul>` of 4-5 labeled `<li>` bullets (`<strong>Label:</strong> phrase`), right `<td>` (60%, centered) with the canvas. Even rows background `#fafcfe`.
- **Page CSS:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; `.subtitle` `#666` 1.05em; `.obj-title` 1.05em weight 600 `#1a5276`; `ul` margin `8px 0 8px 20px`, 0.9em `#333`; `li` margin `4px 0`; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `strong` `#1a5276`; `.philosophy` callout style defined (background `#f0f4f8`, left border `4px solid #2980b9`) but unused on this page. No nav bar, no back/home links.
- **Canvas:** each `<canvas>` declared with `width="720" height="300"` attributes, but a shared `setupCanvas(id)` helper fixes the drawing size to 720×200 CSS pixels, sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`), and calls `ctx.scale` so drawing stays in logical coordinates; default font 17px system sans-serif.
- **Palette:** primary blue `#1a5276`, accent blue `#2980b9`, green `#27ae60` / `#2ecc71`, red `#e74c3c` (dark red `#c0392b`), orange `#f39c12`, purple `#9b59b6`, light gray `#ecf0f1`, gray text `#333`/`#666`.
- Card/grid links elsewhere point to this page as `domains/065-compute-utilization.html` in regenerated HTML.
