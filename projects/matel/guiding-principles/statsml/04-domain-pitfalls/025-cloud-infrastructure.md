# cloud provider Cloud Infrastructure — Domain Pitfalls

**Page type:** detail page (one h2 per pitfall, each followed by a two-column obj-table row: text left 50%, canvas right 50%)
**HTML title tag:** 25. cloud provider Cloud Infrastructure — Domain Pitfalls

**Subtitle:** Six critical data pitfalls when working with cloud provider services and infrastructure telemetry

## Callout (philosophy box)

cloud provider infrastructure generates massive volumes of operational data across regions, services, and accounts. Each layer introduces subtle data quality issues that compound when building analytics or ML pipelines on top of cloud telemetry.

## Multi-Region Data Residency

**Obj-title:** Data in eu-west-1 ≠ us-east-1

- Joining cross-region data introduces latency AND legal issues
- GDPR prevents certain cross-region flows (EU → US without adequacy decision)
- Same schema, same table name, fundamentally different legal status
- Cross-region replication lag creates temporal inconsistency

**Impact:** A join that works in dev (single region) silently violates compliance in production, or introduces 50-200ms latency per query that compounds in pipelines.

### Visualization (canvas `c1`, 720×300)

Diagram: two region boxes separated by a GDPR block barrier, with a blocked flow and an allowed aggregates-only flow.

- **EU box:** rectangle at (40,40) 200×200, fill `#eaf2f8`, stroke `#2980b9` width 2; bold title "eu-west-1" in `#1a5276`; contents in `#333` 14px: "Users Table", "Orders Table", "PII Data"; green (`#27ae60`) 12px label "GDPR Protected".
- **US box:** rectangle at (480,40) 200×200, fill `#fef9e7`, stroke `#f39c12` width 2; bold title "us-east-1"; contents: "Analytics Table", "ML Features", "Aggregates".
- **Barrier:** solid red (`#e74c3c`) rectangle at (310,50) 100×180 with white bold text "GDPR" / "BLOCK" and 11px "No PII transfer".
- **Blocked flow:** dashed red lines (dash 6/4, width 2) from EU box edge into the barrier and from the barrier to the US box (at y=100).
- **Allowed flow:** solid green (`#27ae60`) polyline routing under the barrier (from EU box at y=190 down to y=270, across, then up into the US box) ending in a green arrowhead; green 11px label below: "Aggregates only (no PII)".

## cloud monitoring Metric Sampling

**Obj-title:** Standard resolution = 5-min aggregation hides reality

- Spikes within 5-minute windows are invisible at standard resolution
- High-resolution metrics (1-min) cost 10x more
- Most dashboards show smoothed averages — presenting lies as truth
- P99 latency at 5-min resolution can miss 100% of actual tail events

**Impact:** You see "healthy" metrics while users experience timeouts. The data says everything is fine because the data resolution is too coarse to capture the problem.

### Visualization (canvas `c2`, 720×300)

Line chart: noisy per-second latency series vs the smoothed 5-minute average.

- **Axes:** x "Time (minutes)" with tick labels 0, 5, 10, 15, 20, 25, 30; rotated y label "Latency (ms)"; axes stroked `#333`. Plot area: left 70, right w−30, top 40, bottom h−50; y scale max 220.
- **Actual series (red `#e74c3c`, width 1, 60% alpha):** 180 points of base = 50 + 15·sin(i/20) plus seeded random noise (LCG seed 42, +0–10), with spikes: +120 for i 26–29, +150 for i 71–72, +100 for i 111–114, +130 for i 156–157.
- **Averaged series (blue `#2980b9`, width 3):** six points, each the mean of 30 consecutive actual points, plotted at bucket centers.
- **Legend (top):** red line sample labeled "Actual (per-second)"; blue thick line sample labeled "cloud monitoring 5-min avg (what you see)".
- **Annotation (bold red, top center of plot):** "← Spikes invisible in dashboard →".

## Cost Anomaly vs Usage Anomaly

**Obj-title:** Same cost spike, four different root causes

- More usage (legitimate growth)
- Pricing change (cloud provider updated rates)
- Reserved Instance expiration (fell back to on-demand)
- Marketplace charge (third-party subscription renewal)

**Impact:** All four look identical in billing data. Without root cause decomposition, you either over-alert (every cost change = incident) or miss real anomalies buried in expected changes.

### Visualization (canvas `c3`, 720×300)

Stacked bar chart of monthly cost decomposed by root cause.

- **Title (bold 14px `#1a5276`, top center):** "Same $12K spike — Four Different Root Causes".
- **Months and stacked values [Usage, Price Change, RI Expiry, Marketplace] in $:**
  - Jan: 5000, 1000, 0, 500
  - Feb: 5200, 1000, 0, 500
  - Mar (spike): 6000, 2000, 3000, 1000
  - Apr: 5500, 1000, 0, 500
  - May: 5300, 1000, 0, 500
- **Segment colors:** Usage `#3498db`, Price Change `#f39c12`, RI Expiry `#e74c3c`, Marketplace `#9b59b6`.
- **Axes:** y $0K–$12K in 5 gridline steps (labels "$0K"…"$12K" in `#666`, gridlines `#eee`); month labels below bars in `#333`, with "Mar (spike)" bold red `#e74c3c`. Bars 60% of slot width. Plot area: left 80, right w−40, top 50, bottom h−60.
- **Legend (below chart, horizontal row):** 12px color squares with labels Usage, Price Change, RI Expiry, Marketplace.

## Spot Instance Interruption

**Obj-title:** 2-minute warning, then your work is gone

- Workload must be checkpoint-able or progress is lost entirely
- Model training on spot: lost progress on interruption (hours of GPU time)
- Surviving jobs have survivorship bias in training time metrics
- Reported "average training time" excludes all failed/interrupted runs

**Impact:** Your training time estimates are biased downward — you only measure runs that survived. The true cost includes invisible wasted compute from interrupted runs.

### Visualization (canvas `c4`, 720×300)

Diagram of three training attempts as rising progress lines, two interrupted and one succeeding.

- **Axes:** x "Time (hours)", rotated y "Training Progress (%)"; axes stroked `#333`. Plot area: left 70, right w−40, top 45, bottom h−50.
- **Attempt 1:** red (`#e74c3c`, width 2.5) line from origin to (25% of width, 40% progress), ending in an 8px red circle with white "X"; annotations above it in red 12px: "Interrupted!" / "2-min warning".
- **Attempt 2:** orange (`#f39c12`, width 2.5) line from (30% width, 0) to (60% width, 60% progress), ending in a red circle with white "X"; annotation "Interrupted!".
- **Attempt 3:** green (`#27ae60`, width 2.5) line from (65% width, 0) to (95% width, 95% progress), ending in an 8px green circle with white "✓".
- **Wasted-work shading:** light red triangles (`rgba(231,76,60,0.1)`) under attempts 1 and 2.
- **Top-left annotations:** bold 13px `#1a5276` "Reported training time: 3h (survivor only)"; bold red `#e74c3c` "Actual total GPU time: 8h (incl. lost work)".

## Service Limit Throttling as Hidden Feature

**Obj-title:** API returns 429 → backoff → retry → success

- From data perspective: unexplained latency spike
- Cause is invisible unless you log throttle events separately
- SDK auto-retry hides the 429 from application-level metrics
- Throttling is intermittent — happens under load, invisible in tests

**Impact:** You see "slow API calls" in your latency data but the root cause (throttling) lives in a completely separate log stream that most teams never correlate.

### Visualization (canvas `c5`, 720×300)

Line chart of application latency with hidden 429 throttle events marked as red ticks below the axis.

- **Title (bold 14px `#1a5276`, top center):** "What You See (Latency) vs What Actually Happened (429 Throttle)".
- **Latency series (blue `#2980b9`, width 2):** 60 points; baseline 80 + random 0–20 (LCG seed 7); at throttle points (indices 12, 13, 28, 29, 30, 45, 46) latency jumps to 300 + random 0–200. Y scale max 550. Plot area: left 70, right w−40, top 50, bottom h−70.
- **Throttle events:** 6×20px red bars (`rgba(231,76,60,0.7)`) drawn just below the x-axis at each throttle index.
- **Legend (top-left inside plot):** blue "● Application latency (visible)"; red "■ HTTP 429 throttle events (hidden in separate log)".
- **Connector:** dashed gray (`#999`, dash 3/3) vertical line linking the spike at index 13 to its 429 bar, labeled "cause" in `#666` 11px.
- **Annotation (bold red, bottom center):** "429 events (only in CloudTrail, not app metrics)".
- **Rotated y label:** "Latency (ms)".

## Log Group Retention Mismatch

**Obj-title:** Different teams, different retention = asymmetric history

- Team A: 30-day retention. Team B: 90-day retention.
- Joining data across teams: systematically missing older data from Team A
- No error raised — the join succeeds, just with fewer rows
- Any trend analysis beyond 30 days is biased toward Team B's perspective

**Impact:** Historical analyses silently become single-source after the shorter retention window. No warning, no error — just progressively more biased results the further back you look.

### Visualization (canvas `c6`, 720×300)

Two horizontal timeline bars over a 90-day span showing asymmetric retention.

- **Title (bold 14px `#1a5276`, top center):** "Log Retention: Team A (30 days) vs Team B (90 days)".
- **Team B bar (y=90, 40px tall, full width):** solid green `#27ae60` with centered white bold text "Team B: Full 90-day history available"; label above in `#333` 12px: "Team B (90-day retention)".
- **Team A bar (y=160, 40px tall):** first 60 days filled `#fadbd8` with red (`#e74c3c`) diagonal hatch lines and bold red text "MISSING — No Error Raised"; last 30 days solid blue `#3498db` with white bold text "Team A: Only 30 days"; label above: "Team A (30-day retention)".
- **Day markers:** dashed gray (`#ccc`, dash 3/3) vertical guide lines at day 0, 30, 60, 90 labeled "90 days ago", "60 days ago", "30 days ago", "Today" in `#666` 11px along the bottom.
- **Warning annotation (bold red 13px, centered over the missing region near the top):** "← JOIN produces biased results (Team A data missing) →".

## Regeneration instructions

- **Layout:** domains detail-page template: h1, `.subtitle`, one `.philosophy` callout, then per pitfall an unnumbered `<h2>` (with an id slug; 1.4em `#1a5276`, 2px solid `#2980b9` bottom border) followed by a `.obj-table` with one `<tr>`: left `<td>` (45%) holding `.obj-title` + `<ul>` of bullets + an **Impact:** paragraph, right `<td>` (55%, centered) holding the canvas. Even rows background `#fafcfe`. No nav, no cross-page links.
- **Page CSS:** body system sans-serif, white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; `.subtitle` `#666` 1.05em; `ul` 0.9em `#333`; `strong` `#1a5276`; `.philosophy` background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em weight 600 `#1a5276`.
- **Canvas:** each canvas declares intrinsic `width="720" height="300"`; a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Charts drawn immediately in IIFEs. Base chart font 17px system sans-serif. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, mid blue `#2980b9`/`#3498db`, green `#27ae60`, red `#e74c3c`, orange `#f39c12`, purple `#9b59b6`, grays `#333`/`#666`.
- Card links elsewhere pointing to this page use the `.html` extension in regenerated HTML.
