# Data Center Network

**Page type:** detail page (one h2 + one-row obj-table per pitfall: text left 50%, canvas right 50%)
**HTML title tag:** Data Center Network - Domain-Specific Pitfalls

**Subtitle:** Statistical pitfalls in network telemetry — temporal aliasing, sampling blindness, and the deception of aggregate metrics.

## Microbursts Invisible at Polling Interval

**SNMP Says 30%; the Link Actually Hits 100% Every Second**

- **The burst:** 100% utilization for 200ms per second — drops and latency spikes are real.
- **Averaged away:** A 5-second poll window smears a 200ms saturation into a calm 30%.
- **Temporal aliasing:** The problem lives entirely between the measurements.
- **What to change:** Sub-second counters or drop counters, not the polled average.

### Visualization (canvas `canvas1`, 720×200 — HTML attribute height 300 but setup helper forces 720×200)

Time-series comparison: dense red microburst bars (reality) vs a flat blue SNMP sample line.

- **Background:** `#f9f9f9`-like light fill `#f9fbfd` over full 720×200; L-shaped axes in `#333` (width 1.5) from (60,20) down to (60,160) then right to (690,160).
- **Y-axis labels (gray `#555`, 12px, right-aligned at x=55):** "100%" (y=35), "50%" (y=92), "0%" (y=163).
- **X-axis labels (centered):** "0s", "5s", "10s", "15s", "20s", "25s" at 6 evenly spaced ticks (x = 60 + i×126, y=178).
- **Reality pattern:** 25 semi-transparent red bars, fill `rgba(231,76,60,0.3)`, one per second across the 630px plot width; each bar spans 20% of a second's width and full height (y=30 to 160), representing 100% bursts.
- **Reality label:** red `#e74c3c`, 13px, left-aligned at (80,25): "Reality: 100% bursts (200ms each)".
- **SNMP series:** horizontal blue `#2980b9` line (width 2) at 30% utilization (y = 160 − 0.3×140 = 118) connecting 6 sample points with filled 6px-radius blue dots at each tick.
- **SNMP label:** blue `#2980b9`, 14px, left-aligned at (400, y−10): "SNMP reports: 30%".
- **Title (bottom center, 17px, `#1a5276`):** "Temporal Aliasing: Bursts Between Samples".

## ECMP Hash Creates Uneven Load

**Four "Equal-Cost" Paths, One at 90% and Three at 20%**

- **Hash stickiness:** A flow's 5-tuple pins it to one path for its whole life.
- **Persistent imbalance:** Collisions concentrate heavy flows, so the skew never self-corrects.
- **The average lies:** (90+20+20+20)/4 = 37.5% while one link sits at failure threshold.
- **What to monitor:** Per-path maximum, never the fabric mean.

### Visualization (canvas `canvas2`, 720×200 — HTML attribute height 300 but setup helper forces 720×200)

Horizontal bar chart of per-path utilization with a dashed average line.

- **Data:** Path 1 = 90% (red `#e74c3c`), Path 2 = 20% (green `#27ae60`), Path 3 = 20% (green `#27ae60`), Path 4 = 20% (green `#27ae60`).
- **Layout:** bars 28px tall, 42px row pitch starting at y=20; bar track from x=120, max width 480px; light gray `#ecf0f1` background bar behind each colored utilization bar.
- **Labels:** path names right-aligned at x=110 in `#333` 14px; percentage value ("90%", "20%") in `#333` 13px just right of each bar end.
- **Average line:** vertical dashed purple `#8e44ad` line (dash 6/4, width 2) at 37% of bar width (x = 120 + 480×0.37), from y=15 to y=185; centered purple 14px label below: "Average = 37%".
- **Danger label:** red `#e74c3c` 12px left-aligned near end of Path 1 bar (x = 120 + 480×0.90 + 30, y=38): "NEAR FAILURE".
- **Title (bottom, 17px `#1a5276`, centered at w/2+50, y=192):** "Average Hides Imbalance".

## Elephant vs Mice Flows

**One Backup Job Starves 10,000 API Calls in a Shared Buffer**

- **The asymmetry:** A single 10Gbps flow occupies most of the buffer; thousands of small flows share the rest.
- **Aggregate looks fine:** Total throughput is healthy while mice tail latency collapses.
- **Illustrative gap:** Elephant 2ms vs mice 85ms on the same port.
- **Modeling rule:** Segment by flow type; an aggregate latency metric hides the victim class.

### Visualization (canvas `canvas3`, 720×200 — HTML attribute height 300 but setup helper forces 720×200)

Buffer-occupancy diagram plus latency legend and comparison bars.

- **Buffer box:** outlined rectangle `#2c3e50` (width 2) at (60,30), 400×120; label above in gray `#555` 13px centered: "Switch Buffer Space".
- **Elephant flow:** solid red `#e74c3c` block filling left 75% of buffer interior; white 15px centered text inside: "ELEPHANT FLOW" and "(1 backup job: 10Gbps)".
- **Mice flows:** 8 small blue `#3498db` rectangles (18×45) packed in the right 25% of the buffer, arranged 4 per row in 2 rows; dark 11px label "Mice" centered below that region.
- **Legend (right side, from x=500, y=30):** red 12×12 swatch + `#333` 14px text "Elephant: 2ms latency"; blue 12×12 swatch + text "Mice: 85ms latency!" 30px below.
- **Latency comparison bars:** short green `#27ae60` bar (20×16) labeled "Expected" (12px `#333`); long red `#e74c3c` bar (180×16) labeled "Actual mice tail latency".
- **Title (bottom center, 17px `#1a5276`):** "Buffer Starvation: Aggregate Hides Per-Flow Pain".

## Buffer Bloat

**Zero Packet Loss, 50ms Latency — Congestion With No Alarm**

- **The mechanism:** Deep buffers queue packets instead of dropping them.
- **Blind signal:** Loss-based congestion detection never fires, so the link reads "healthy."
- **Illustrative gap:** Latency sits at 50ms where the fabric budget is 0.1ms.
- **What to change:** Treat latency as the primary congestion signal; loss is a lagging one.

### Visualization (canvas `canvas4`, 720×200 — HTML attribute height 300 but setup helper forces 720×200)

Side-by-side mini line charts: flat packet loss vs rising latency.

- **Left chart (axes at x=60, y=40, 280×120, `#333` L-axes):** flat green `#27ae60` line (width 3) hugging 0% across the chart. Y labels "5%" (top) and "0%" (bottom) in gray `#555` 12px; x label "Time" centered below. Chart headers centered above in green: "Packet Loss" (15px) and `"Looks Healthy"` (13px).
- **Right chart (axes at x=400, y=40, 280×120):** rising red `#e74c3c` curve (width 3) following a power curve (exponent 1.5) from near-bottom-left to near-top-right. Y labels "50ms" (top) and "0.1ms" (bottom); x label "Time". Headers in red: "Latency" (15px) and `"Actual Problem"` (13px).
- **Title (bottom center, 17px `#1a5276`):** "Loss Metrics Deceive: Latency Reveals Truth".

## Fabric Upgrade Partial-State

**Half the Fleet on New Firmware Is Not a Steady State**

- **Mixed fabric:** Staged upgrades leave two firmware versions routing simultaneously.
- **Traffic shifts:** Protocols recalculate paths mid-window, so flows move for non-demand reasons.
- **Unrepresentative data:** Baselines built over the window describe the upgrade, not the network.
- **What to do:** Tag and exclude transitional periods from capacity and anomaly baselines.

### Visualization (canvas `canvas5`, 720×200 — HTML attribute height 300 but setup helper forces 720×200)

Three-zone timeline: steady signal, chaotic upgrade window (crossed out), new steady signal.

- **Timeline:** horizontal `#333` line (width 2) at y=110 from x=60 to x=680; zone boundaries at x=240 and x=460 marked by vertical dashed gray `#7f8c8d` lines (dash 4/4) from y=25 to y=150.
- **Zone 1 signal (x=60–240):** smooth green `#27ae60` sine wave (amplitude 8, centered at y=80, width 2).
- **Zone 2 signal (x=240–460):** noisy red `#e74c3c` jagged line (deterministic LCG noise, amplitude ±30, width 1.5); the whole zone is crossed out with a large red X (width 3) spanning (250,50)–(450,140) and (250,140)–(450,50).
- **Zone 3 signal (x=460–680):** smooth blue `#2980b9` sine wave (amplitude 6, centered at y=75, width 2).
- **Zone labels (14px, centered, two lines at y=25 and y=42):** green "Pre-Upgrade" / "(Steady State)"; red "During Upgrade" / "EXCLUDE"; blue "Post-Upgrade" / "(New Steady State)".
- **Time label:** gray `#555` 12px centered under middle zone at y=160: "Maintenance Window".
- **Title (bottom center, 17px `#1a5276`):** "Transitional Data Is Not Representative".

## Telemetry Sampling at 100Gbps

**1:1000 Sampling Means Rare Attacks Appear in No Sample**

- **Hard limit:** Mirroring 100% of 100Gbps for analysis is not feasible.
- **The math:** A 1-in-a-million-packet pattern has a 0.1% chance of landing in the sample.
- **Perverse scaling:** Faster links force sparser sampling, so security visibility degrades with speed.
- **What to add:** Full-fidelity counters or targeted capture for the patterns sampling cannot see.

### Visualization (canvas `canvas6`, 720×200 — HTML attribute height 300 but setup helper forces 720×200)

Declining detection-probability curve vs link speed, with a shaded blind zone.

- **Axes:** L-shaped `#333` axes at (80,25), plot area 560×130. Y labels "100%", "50%", "0%" (gray `#555` 12px, right-aligned); rotated vertical y-axis title "Detection Probability" at far left.
- **X labels (centered):** "1Gbps", "10Gbps", "40Gbps", "100Gbps", "400Gbps" evenly spaced; axis caption below: "Link Speed (at 1:1000 sampling)".
- **Curve:** red `#e74c3c` line (width 3), exponential decay prob = exp(−3.5·t)·0.85 + 0.05 for t in [0,1], falling from ~90% to ~5%.
- **Danger zone:** right half of plot shaded `rgba(231,76,60,0.1)`.
- **Annotations:** red `#e74c3c` 13px left-aligned near top-middle: "Rare event: ~0.001% of traffic"; dark red `#c0392b` 12px centered two-line note in lower-right: "Effectively blind" / "to rare attacks".
- **Title (bottom center, 17px `#1a5276`):** "Faster Network = Worse Security Visibility".

## Regeneration instructions

- **Layout:** domains detail-page convention — h1, `.subtitle`, then per pitfall an unnumbered `<h2>` (1.4em `#1a5276`, 2px solid `#2980b9` bottom border) followed by a one-row `.obj-table`: left `<td>` (40%) with `.obj-title` (one-line punchline) + a `<ul>` of labeled one-line bullets, right `<td>` (60%, centered) with the canvas. Even rows get background `#fafcfe`.
- **Page CSS:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; `.subtitle` `#666` 1.05em; `.obj-title` 1.05em weight 600 `#1a5276`; `ul` 0.9em `#333` with 20px left margin and `li` 4px vertical margin; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `strong` `#1a5276`; unused `.philosophy` callout style (background `#f0f4f8`, left border `4px solid #2980b9`) defined but no callout on page. No nav bar, no back/home links.
- **Canvas:** each `<canvas>` declared with `width="720" height="300"` attributes, but a shared `setupCanvas(id)` helper fixes the drawing size to 720×200 CSS pixels, sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`), and calls `ctx.scale` so drawing stays in logical coordinates; default font 17px system sans-serif.
- **Palette:** primary blue `#1a5276`, accent blue `#2980b9`, green `#27ae60`, red `#e74c3c`, purple `#8e44ad`, mice blue `#3498db`, gray text `#555`/`#333`.
- Card/grid links elsewhere point to this page as `domains/064-datacenter-network.html` in regenerated HTML.
