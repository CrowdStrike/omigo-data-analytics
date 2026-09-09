# Observability / Monitoring

**Page type:** detail page (one h2 per pitfall, each followed by a two-column obj-table row: text left 50%, canvas right 50%)
**HTML title tag:** 28. Observability / Monitoring — Domain Pitfalls

**Subtitle:** Data pitfalls in monitoring systems, metrics, logs, and traces

## Callout (philosophy box)

Observability tools generate enormous data volumes, but the systems that collect, sample, and aggregate this data introduce subtle biases. What you see in your dashboards is never the full picture — it is a heavily processed summary shaped by engineering trade-offs.

## Trace Sampling Bias

**Obj-title:** Only 1% of traces kept — the sample is never neutral

- Head-based sampling: random 1% decision at trace start — keeps random subset regardless of outcome
- Tail-based sampling: decision after trace completes — keeps errors and slow requests preferentially
- Head-based: your "normal" sample ≠ actual normal (just random). Rare errors may be missed entirely
- Tail-based: error traces overrepresented. "99% of traces are errors" because you only kept errors
- Neither approach gives an unbiased view of system behavior

**Impact:** Conclusions drawn from sampled traces are systematically biased. You cannot compute accurate error rates, latency percentiles, or throughput from sampled data without correcting for sampling strategy.

### Visualization (canvas `c1`, 720×300)

Side-by-side dot grids comparing head-based vs tail-based sampling of 50 traces each.

- **Title (bold 17px `#1a5276`, top center):** "Trace Sampling: 100 Traces, 1% Kept".
- **Panels:** left labeled "Head-Based (Random 1%)", right labeled "Tail-Based (Errors/Slow Kept)" (14px `#555`). Each panel is a 10×5 grid of 8px-radius dots (32px column spacing, 22px row spacing); dots use a deterministic shuffled mix: 5 error (red `#e74c3c`), 5 slow (orange `#f39c12`), 40 normal (gray `#bbb`), shuffled with LCG seed 42.
- **Kept traces:** drawn larger (radius +2) with a `#2c3e50` 2px ring; unkept dots at 40% alpha. Left panel keeps a single random dot (index 7 — a normal trace, shown blue `#3498db`); right panel keeps every error and slow dot.
- **Legend (bottom-left):** dots labeled "Normal" (`#3498db`), "Error" (`#e74c3c`), "Slow" (`#f39c12`), and a ring labeled "= Kept in sample".
- **Bottom note (italic red, two lines, centered):** "Head-based: random, may miss all errors. Tail-based: overrepresents errors." / "Neither gives an unbiased picture of system behavior."

## Log Volume ≠ Problem Severity

**Obj-title:** One chatty microservice generates 90% of logs

- Service A: verbose debug logging, 90% of total log volume — mostly noise
- Service B: minimal logging, 0.1% of volume — but contains a critical database corruption error
- Log-based alerting systems: dominated by Service A's volume patterns
- Actual critical errors in quiet services: buried under noise
- Volume-based anomaly detection triggers on the wrong things

**Impact:** Log volume is not signal. Teams waste time investigating high-volume noise while critical low-volume errors go unnoticed for hours or days.

### Visualization (canvas `c2`, 720×300)

Bar chart of log volume share by service, with the critical error marked on the smallest bar.

- **Title (bold 17px `#1a5276`, top center):** "Log Volume by Service vs. Actual Severity".
- **Bars (80px wide, 20px gap, centered):** Auth Svc 2% `#3498db`; API GW 5% `#2ecc71`; Chat Svc 90% `#e67e22`; DB Proxy 1.5% `#9b59b6`; Payment 1.5% `#1abc9c`. Percent labels in white inside tall bars or in `#333` above short bars; service names below in `#333` 12px.
- **Axes:** y 0–100% with tick labels every 25% and light `#eee` gridlines.
- **Chatty annotation (italic orange `#e67e22`, over the Chat Svc bar):** "90% of all logs" / "(mostly noise)".
- **Critical marker (over the Payment bar):** red 24px "★" with bold red 11px labels "CRITICAL" / "ERROR HERE".
- **Bottom note (italic red, centered):** "Volume is not signal. Critical error buried in the quietest service."

## Metric Aggregation Hides Spikes

**Obj-title:** p50 = 20ms looks healthy while p99 = 2000ms means suffering

- Dashboard shows p50 latency = 20ms — "system is healthy"
- But p99 = 2000ms — 1% of users wait 100x longer
- At 1M requests/day, that is 10,000 users with terrible experience
- Average and median metrics hide the long tail entirely
- 5-minute aggregation windows can hide 30-second complete outages

**Impact:** The 1% of users with 100x worse experience are invisible in average metrics. Dashboard shows "healthy" while thousands of customers rage. SLOs must be percentile-based, not average-based.

### Visualization (canvas `c3`, 720×300)

Latency histogram with a long tail, marked with p50 and p99 lines.

- **Title (bold 17px `#1a5276`, top center):** "Latency Distribution: What Dashboards Hide".
- **Histogram buckets (range in ms → % of requests):** 5-10 → 5; 10-15 → 15; 15-20 → 35; 20-25 → 30; 25-35 → 8; 35-50 → 3; 50-100 → 2; 100-500 → 1; 500-1K → 0.5; 1K-2K → 0.3; 2000+ → 0.2. Y scale max 35.
- **Bar colors by bucket midpoint:** ≤25ms green `#27ae60`; ≤100ms orange `#f39c12`; >100ms red `#e74c3c`. X labels rotated ~30°, 10px `#666`.
- **Axes:** stroked `#999`; rotated y label "% of Requests"; x label "Latency (ms)".
- **p50 marker:** dashed blue (`#2980b9`, dash 5/3, width 2) vertical line at the third bucket, labeled bold "p50 = 20ms" and 11px `"Dashboard shows this"`.
- **p99 marker:** dashed red (`#e74c3c`) vertical line at the tenth bucket, labeled bold "p99 = 2000ms" and 11px `"Users experience this"`.
- **Annotation (italic red, bottom right):** "100x worse for 1% = 10,000 users/day".

## Alert Fatigue / Threshold Noise

**Obj-title:** 100 alerts/day means the team ignores them all

- Static threshold: CPU > 80% fires alert. But 80% is normal during batch jobs every night
- Result: 100+ alerts/day, all "expected" — team learns to ignore
- Real incident alert arrives: buried in noise, no one responds for 45 minutes
- Threshold doesn't account for day-of-week pattern: Monday is always "anomalous" vs weekend baseline
- Alert response rate drops from 90% to 5% over 3 months

**Impact:** Alert fatigue is the #1 cause of delayed incident response. When everything is an emergency, nothing is. Static thresholds in dynamic systems generate noise, not signal.

### Visualization (canvas `c4`, 720×300)

Split visual: alert dot grid on the left, declining response-rate curve on the right.

- **Title (bold 17px `#1a5276`, top center):** "Alert Fatigue: Response Rate Over Time".
- **Left grid:** labeled "100 daily alerts" (`#666` 12px); 60 dots in a 10×6 grid (18px spacing, 10px dot size); noise dots gray `#ccc` (every 7th one amber `#f39c12`) at 60% alpha; one critical alert (index 47) solid red `#e74c3c` with `#c0392b` ring; bold red annotation to the right: "← REAL INCIDENT" / "   (buried in noise)".
- **Right chart:** labeled "Team Response Rate"; axes stroked `#999`; y labels 100%/50%/0%; x labels "Week 1", "Month 1", "Month 3". Red (`#e74c3c`, width 3) declining curve through normalized points `[0,0.9] [0.1,0.85] [0.2,0.7] [0.3,0.5] [0.4,0.3] [0.5,0.2] [0.6,0.12] [0.7,0.08] [0.8,0.06] [0.9,0.05] [1.0,0.05]`; endpoint labels "90%" in green `#27ae60` and "5%" in red.
- **Bottom note (italic red, centered):** "When everything is an emergency, nothing is."

## Cardinality Explosion in Tags

**Obj-title:** Adding user_id as a label creates 10M time series

- 1 metric × 1 tag (region: 5 values) = 5 time series
- Add endpoint tag (100 values) = 500 time series
- Add user_id tag (10M values) = 5 billion time series — system OOM
- Every tag dimension multiplies storage and query cost
- Must choose: high-cardinality detail OR system stability — cannot have both

**Impact:** Cardinality explosion is the most common way to kill a monitoring system. Prometheus, observability platform, and every TSDB has hard limits. One bad label choice can take down your entire observability stack.

### Visualization (canvas `c5`, 720×300)

Log-scale line chart of time-series count exploding as tag dimensions are added.

- **Title (bold 17px `#1a5276`, top center):** "Cardinality Explosion: Time Series vs. Tag Dimensions".
- **Axes:** y log-scale labels 1, 100, 10K, 1M, 10M, 1B (evenly spaced, `#eee` gridlines); x categories (two-line labels): "base metric", "+ region (5)", "+ endpoint (100)", "+ status (5)", "+ user_id (10M)" at normalized positions 0.1/0.3/0.5/0.7/0.9.
- **Curve (red `#e74c3c`, width 3):** through normalized heights `[0.0, 0.14, 0.45, 0.6, 1.05]` (last clamped to top); 6px point dots — first four blue `#2980b9`, last red — with bold value labels "1", "5", "500", "2,500", "25 Billion".
- **OOM line:** dashed red (dash 8/4, width 2) horizontal line at 85% height, labeled bold "SYSTEM OOM" and 11px "Monitoring crashes here" (right-aligned).
- **Bottom annotation (italic red, centered):** "Each dimension MULTIPLIES total series count".

## Observability of Observability

**Obj-title:** Who watches the watchers? (Quis custodiet ipsos custodes?)

- Your monitoring system goes down
- You don't know your monitoring is down (because your monitoring is what tells you things are down)
- Therefore you don't know your app is down
- Meta-monitoring (monitoring your monitoring) is required but rarely implemented
- Even meta-monitoring needs to be monitored — infinite regress

**Impact:** The most dangerous failure mode: silent monitoring failure. Your dashboards show green (cached/stale) while production burns. External synthetic monitoring from a separate provider is the only reliable solution.

### Visualization (canvas `c6`, 720×300)

Nested-boxes diagram of the infinite regress of monitoring layers, with the monitor layer crossed out.

- **Title (bold 17px `#1a5276`, top center):** "Who Watches the Watchers?".
- **Outermost box (620×200, dashed gray `#bbb`, dash 5/5):** labeled above in bold `#999`: "??? (who monitors this?)".
- **Meta-Monitor box (480×150):** fill `#f0fff0`, stroke green `#27ae60`, corner label "Meta-Monitor" in green 13px.
- **Monitor box (320×100):** fill `#fff5f5`, stroke red `#e74c3c` width 3, corner label bold red "Monitor (DOWN!)"; a large red X (width 4, 60% alpha) drawn corner-to-corner across it.
- **App box (150×55, innermost):** fill `#f0f8ff`, stroke blue `#2980b9`, bold blue centered "Your App" with 11px "(maybe down?)".
- **Annotation (red 12px, below the app box):** "✖ cannot watch (down)".
- **Bottom note (italic red 11px, centered):** "Monitor down → No alerts → App down undetected → Customer impact".

## Regeneration instructions

- **Layout:** domains detail-page template: h1, `.subtitle`, one `.philosophy` callout, then per pitfall an unnumbered `<h2>` (with an id slug; 1.4em `#1a5276`, 2px solid `#2980b9` bottom border) followed by a `.obj-table` with one `<tr>`: left `<td>` (45%) holding `.obj-title` + `<ul>` of bullets + an **Impact:** paragraph, right `<td>` (55%, centered) holding the canvas. Even rows background `#fafcfe`. No nav, no cross-page links.
- **Page CSS:** body system sans-serif, white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; `.subtitle` `#666` 1.05em; `ul` 0.9em `#333`; `strong` `#1a5276`; `.philosophy` background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em weight 600 `#1a5276`.
- **Canvas:** each canvas declares intrinsic `width="720" height="300"`; a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Charts drawn immediately in IIFEs. Base chart font 17px system sans-serif. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, mid blue `#2980b9`/`#3498db`, green `#27ae60`/`#2ecc71`, red `#e74c3c`, orange `#f39c12`/`#e67e22`, purple `#9b59b6`, teal `#1abc9c`, grays `#555`/`#666`/`#999`/`#bbb`/`#ccc`.
- Card links elsewhere pointing to this page use the `.html` extension in regenerated HTML.
