# GPU Cluster

**Page type:** detail page (one h2 + one-row obj-table per pitfall: text left 50%, canvas right 50%)
**HTML title tag:** GPU Cluster - Domain-Specific Pitfalls

**Subtitle:** Statistical pitfalls in GPU computing — occupancy lies, memory bottlenecks, and the tyranny of the slowest node.

## SM Occupancy ≠ Throughput

**95% Occupancy, 30% of Peak Throughput**

- **The mechanism:** Every warp is scheduled, yet most cycles wait on global memory fetches.
- **What the number means:** Occupancy counts warp residency, not useful arithmetic work.
- **The memory-bound tell:** High occupancy plus low arithmetic intensity means memory-bound.
- **Missing metric:** Without FLOPs per byte, occupancy alone is meaningless.
- **The dashboard lie:** Utilization reads "95% occupied" while 70% of peak is lost.

### Visualization (canvas `canvas1`, 720×200 — HTML attribute height 300 but setup helper forces 720×200)

Two-bar comparison: high occupancy vs low actual throughput, with a gap annotation.

- **Title (bold 17px `#1a5276`, top center):** "SM Occupancy vs Actual Throughput". Background `#f9fafb`.
- **Bars:** 120px wide, max height 130px, baseline y=175.
  - Left bar at x=180: 95%, green `#27ae60`; value "95%" bold green above; label "SM Occupancy" (`#333` 17px) below baseline.
  - Right bar at x=420: 30%, red `#e74c3c`; value "30%" bold red above; label "Actual Throughput" below baseline.
- **Gap markers:** two dashed red `#e74c3c` horizontal lines (dash 4/3) between the bars at the 95% and 30% heights; vertical dark-red `#c0392b` arrow (width 2) between them pointing down to the 30% level; bold dark-red 15px label "memory stalls" centered in the gap.

## Memory Bandwidth Bottleneck

**100 TFLOPS of Compute Fed by Only 2 TB/s of Memory**

- **The imbalance:** Compute capability is 100 TFLOPS; memory can only feed 2 TB/s.
- **Why more cores fail:** If the model is memory-bound, doubling compute does nothing.
- **Invisible in dashboards:** The bottleneck never appears in GPU utilization metrics.
- **Roofline rule:** FLOPs/byte below the compute-to-bandwidth ratio is always memory-limited.
- **Hardware is no fix:** The limit holds regardless of how many CUDA cores you add.

### Visualization (canvas `canvas2`, 720×200 — HTML attribute height 300 but setup helper forces 720×200)

Funnel/pipeline diagram: wide compute block narrowing into a thin memory-bandwidth pipe.

- **Title (bold 17px `#1a5276`, top center):** "Compute vs Memory Bandwidth Pipeline". Background `#f9fafb`.
- **Compute block:** green `#2ecc71` rectangle from x=60 to x=280, y=50 to y=170; white bold 17px centered text: "Compute" / "100 TFLOPS".
- **Funnel:** orange `#f39c12` trapezoid narrowing from the compute block's full height down to y=85–135 over 100px.
- **Bandwidth pipe:** red `#e74c3c` rectangle 140px wide (x=380–520), y=85–135; white bold 15px text: "Memory BW" / "2 TB/s"; a further 60px red output stub after it.
- **Flow arrows:** three dashed dark `#2c3e50` arrows (dash 5/3, width 2) exiting the output stub at y=95, 115, 135.
- **Labels:** bold dark-red `#c0392b` 15px "BOTTLENECK" centered below the pipe; gray `#555` 14px "Effective" / "Output" next to the arrows.

## Multi-tenant Interference

**Your Training Runs 15% Slower and No Metric Says Why**

- **The setup:** On a shared cluster your job runs alongside 3 other tenants.
- **What is shared:** NVLink bandwidth is pooled and the L2 cache is contended.
- **Who pays:** Memory contention on the shared cache degrades every tenant at once.
- **The symptom:** Training is 15% slower with no per-job metric showing an anomaly.
- **Why it hides:** Interference is systemic — isolated monitoring cannot see it.
- **What to change:** Only cluster-wide visibility can attribute the slowdown.

### Visualization (canvas `canvas3`, 720×200 — HTML attribute height 300 but setup helper forces 720×200)

Hub-and-spoke contention diagram: four tenant jobs connected to a shared-resource box.

- **Title (bold 17px `#1a5276`, top center):** "Multi-tenant GPU Resource Contention". Background `#f9fafb`.
- **Center hub:** orange `#f39c12` box 140×60 centered at (360,110); white bold 14px text: "Shared Resources" / "NVLink + L2 Cache".
- **Job boxes (100×36, white bold 14px labels):** "Your Job" blue `#2980b9` at (100,65); "Tenant B" gray `#7f8c8d` at (100,145); "Tenant C" gray at (580,65); "Tenant D" gray at (580,145).
- **Connections:** line from each job box edge to the hub edge; your job's line blue `#2980b9` width 2, tenants' lines light gray `#bdc3c7` width 1.
- **Annotation:** bold red `#e74c3c` 15px "-15% perf" at bottom-left (20,185), with a red elbow arrow (width 2) running up to point at the "Your Job" box.

## Checkpointing IO Storms

**Every 30 Minutes, Neighbours' IO Latency Jumps 100x**

- **The trigger:** A 1000-GPU training job checkpoints every 30 minutes.
- **The volume:** Each checkpoint writes 50GB to shared storage, all at once.
- **What breaks:** Storage saturates for the duration of the simultaneous write.
- **Collateral damage:** Other jobs on that storage see IO latency 100x normal.
- **Why it hides:** Periodic storms are invisible in per-GPU metrics.
- **Where to look:** Only storage-side monitoring shows them, and most teams never check it.

### Visualization (canvas `canvas4`, 720×200 — HTML attribute height 300 but setup helper forces 720×200)

IO-latency time series: flat ~1ms baseline with three square 100ms spikes at 30-minute intervals.

- **Title (bold 17px `#1a5276`, top center):** "IO Latency During Checkpoint Storms". Background `#f9fafb`.
- **Axes:** L-shaped `#333` axes, plot area from margins left 80 / right 40 / top 45 / bottom 40. Y labels (gray `#555` 13px, right-aligned): "100ms" (top), "50ms" (middle), "1ms" (bottom); rotated vertical y-axis title "IO Latency". X labels (centered): "0 min", "30 min" (33%), "60 min" (66%), "90 min" (100%).
- **Series:** blue `#2980b9` line (width 2) at the 1ms baseline, with three trapezoidal spikes to the 100ms level at 33%, 66%, and 99% of plot width (each spike ±15px wide, plateau ±5px).
- **Spike highlight bands:** 30px-wide vertical bands of `rgba(231,76,60,0.15)` behind each spike.
- **Annotations:** bold red `#e74c3c` 12px two-line label under spike 1: "other jobs" / "affected"; dark-red `#c0392b` 12px "checkpoint" under spike 2; bold red 14px "100x spike!" right of spike 3 near the top.

## Stragglers from Heterogeneous Hardware

**Utilization Reads 95%; Effective Throughput Is 70%**

- **The mix:** A100 and H100 GPUs sit in the same cluster and one job spans both.
- **The gap:** A100 nodes finish 30% slower, so they become the stragglers.
- **The hard constraint:** All-reduce waits for the slowest node in the group.
- **Net effect:** The whole job is limited by the weakest hardware in it.
- **The misleading metric:** "Cluster utilization" reads 95%, effective throughput only 70%.
- **Where time goes:** Fast nodes sit idle at synchronization barriers.

### Visualization (canvas `canvas5`, 720×200 — HTML attribute height 300 but setup helper forces 720×200)

Gantt-style timeline of four nodes: fast H100 rows idle-waiting at the all-reduce barrier set by slow A100 rows.

- **Title (bold 17px `#1a5276`, top center):** "Straggler Effect: H100 vs A100 in All-Reduce". Background `#f9fafb`.
- **Rows (22px-tall bars, 30px row pitch from y=50, timeline width 520px from x=100; row labels right-aligned `#333` 14px):** "H100 Node 1", "H100 Node 2", "A100 Node 1", "A100 Node 2".
  - H100 rows: green `#27ae60` compute block covering 0–60% of the timeline, then yellow `#f1c40f` idle/waiting block from 60% to 92%.
  - A100 rows: orange `#e67e22` compute block covering 0–90%, then tiny yellow sync wait from 90% to 92%.
- **Sync line:** vertical dashed red `#e74c3c` line (dash 4/3, width 2) at 92% of the timeline, spanning all rows; bold red 13px two-line label below: "all-reduce" / "sync".
- **Legend (14×14 swatches along the bottom, 13px `#333` text):** green "H100 compute", orange "A100 compute (30% slower)", yellow "Idle (waiting)".

## Thermal Throttling Under Sustained Load

**After 15 Minutes the GPU Settles at 85% of Its Benchmark Speed**

- **The opening:** Initial performance hits peak on a cold GPU.
- **The turn:** After 15 minutes of sustained compute the GPU throttles to 85% for heat.
- **The measurement error:** Cold-GPU benchmarks do not reflect hot-GPU production.
- **Misleading window:** The first 10 minutes of any test overstate what you will get.
- **The rule:** Steady-state performance is always lower than burst performance.
- **Why it persists:** Most benchmarks measure only the initial burst.

### Visualization (canvas `canvas6`, 720×200 — HTML attribute height 300 but setup helper forces 720×200)

Dual-curve time series: performance stepping down from 100% to 85% while temperature rises, with benchmark vs production window shading.

- **Title (bold 17px `#1a5276`, top center):** "Thermal Throttling: Cold Benchmark vs Hot Production". Background `#f9fafb`.
- **Axes:** L-shaped `#333` axes, margins left 70 / right 30 / top 45 / bottom 45. Y labels (gray `#555` 13px): "100%" (top), "85%" (at 35% down the plot), "0%" (bottom). X labels: "0", "5 min" (17%), "10 min" (33%), "15 min" (50%), "30 min" (80%), "Time" (100%).
- **Reference lines:** light-gray `#bdc3c7` dashed horizontals (dash 3/3) at the 100% and 85% levels.
- **Performance curve:** red `#e74c3c` line (width 3): flat at 100% until 33% of the width, quadratic ease-down to 85% between 33% and 50%, then flat at 85% to the end.
- **Temperature curve:** dashed orange `#f39c12` line (dash 4/3, width 2) rising with exponential saturation 1−exp(−3t) from 70% down-plot to 10% down-plot.
- **Window shading:** benchmark window (0–33%) shaded `rgba(39,174,96,0.12)` with bold green `#27ae60` 13px label "benchmark window"; production region (50%–100%) shaded `rgba(231,76,60,0.08)` with dark-red `#c0392b` label "production reality".
- **Legend (top right, 12px `#333`):** solid red line sample labeled "Performance"; dashed orange line sample labeled "Temperature".

## Regeneration instructions

- **Layout:** domains detail-page convention — h1, `.subtitle`, then per pitfall an unnumbered `<h2>` (1.4em `#1a5276`, 2px solid `#2980b9` bottom border) followed by a one-row `.obj-table`: left `<td>` (40%) with `.obj-title` (the bold one-line punchline) followed by a `<ul>` of labeled bullets (`<strong>Label:</strong> phrase`, each fitting one line), right `<td>` (60%, centered) with the canvas. Even rows background `#fafcfe`.
- **Page CSS:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; `.subtitle` `#666` 1.05em; `.obj-title` 1.05em weight 600 `#1a5276`; `ul` margin `8px 0 8px 20px`, 0.9em `#333`; `li` margin `4px 0`; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `strong` `#1a5276`; `.philosophy` callout style defined (background `#f0f4f8`, left border `4px solid #2980b9`) but unused on this page. No nav bar, no back/home links.
- **Canvas:** each `<canvas>` declared with `width="720" height="300"` attributes, but a shared `setupCanvas(id)` helper fixes the drawing size to 720×200 CSS pixels, sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`), and calls `ctx.scale` so drawing stays in logical coordinates; default font 17px system sans-serif.
- **Palette:** primary blue `#1a5276`, accent blue `#2980b9`, green `#27ae60` / `#2ecc71`, red `#e74c3c` (dark red `#c0392b`), orange `#e67e22` / `#f39c12`, yellow `#f1c40f`, gray `#7f8c8d` / `#bdc3c7`, gray text `#333`/`#555`.
- Card/grid links elsewhere point to this page as `domains/066-gpu-cluster.html` in regenerated HTML.
