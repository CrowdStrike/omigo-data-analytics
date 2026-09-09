# Elastic Computing / Auto-Scaling

**Page type:** detail page (one h2 per pitfall, each followed by a two-column obj-table row: text left ~40%, canvas right ~60%)
**HTML title tag:** 79. Elastic Computing / Auto-Scaling

**Subtitle:** Auto-scaling reacts in minutes while demand spikes in seconds. The non-representative data collected during scaling events — latency spikes, timeouts, degraded features — silently contaminates your pipeline.

## Scale-Up Lag — Traffic Arrives Before Capacity

**3-5 Minutes to Scale. Spike Lasts 30 Seconds.**

- **The sequence:** Detect, provision, boot, deploy, warm up — every scaling event costs 3-5 minutes.
- **The mismatch:** The spike itself lasts 30 seconds, so capacity lands after demand is gone.
- **The data impact:** During the lag latency spikes 10×, and real-time feature computations time out.
- **Corrupted values:** Timed-out computations return defaults or nulls, so latency-derived features lie.
- **The false signal:** The model learns "high latency → low conversion" from every scaling event.
- **Wrong attribution:** That correlation comes from infrastructure pressure, not from user behavior.

### Visualization (canvas `c1`, 720×240)

Two-line time-series chart: traffic spike vs delayed capacity response.

- **Background:** light gray plot background `#f9f9f9`; L-shaped axes in `#333`, width 1.5; margins left 50, right 60, top 30, bottom 40.
- **Traffic series (solid `#2471a3`, width 2.5):** 100 points over t=0..1; baseline value 20; for t in [0.18, 0.35] value = 20 + 75·sin((t−0.18)/0.17·π) (a sharp spike peaking at 95). Scale: value/100 of plot height.
- **Capacity series (dashed `#e74c3c`, dash 6/3, width 2.5):** baseline 30; for t in [0.30, 0.55] value = 30 + 50·(1 − e^(−3(t−0.30)/0.25)) (slow ramp-up); flat at 75 for t in (0.55, 0.75]; then declines 75 − 40·(t−0.75)/0.25 for t > 0.75.
- **Lag annotation:** thin gray `#666` horizontal line near the top from x at 25% plot width to 40% plot width, labeled centered above it in 10px gray: "3-5 min lag".
- **Legend (top right):** solid `#2471a3` line swatch labeled "Traffic"; dashed `#e74c3c` swatch labeled "Capacity", 11px.
- **Caption (bottom center, italic 11px `#555`):** "Capacity arrives after spike is over. Errors during gap corrupt data."

## Scale-Down Oscillation — Hysteresis as Noise Generator

**Scale Down → Latency Rises → Scale Up → Latency Drops → Scale Down → Repeat**

- **The cycle:** Scale down raises latency, which triggers scale up, which lowers latency again.
- **No settling point:** That lower latency then triggers scale down, and the system oscillates forever.
- **The data impact:** Time-series models read the 50ms/500ms oscillation as a genuine pattern.
- **Actually an artifact:** The period comes from control-system hysteresis, not from user demand.
- **The false seasonality:** The model learns "latency spikes every 10 minutes" and forecasts on it.
- **Silent breakage:** Change the hysteresis settings and the pattern vanishes; predictions go wrong.

### Visualization (canvas `c2`, 720×240)

Single-line damped oscillation chart.

- **Background:** `#f9f9f9`; L-shaped `#333` axes; margins left 50, right 30, top 30, bottom 40.
- **Series (solid `#e74c3c`, width 2.5):** 200 points over t=0..1; value = 50 + 40·sin(t·π·8)·e^(−0.5t) — a decaying square-wave-like oscillation (4 full cycles, amplitude shrinking from 40). Scale: value/100 of plot height.
- **Title (bold 12px `#1a5276`, centered near top):** "Latency oscillation from scale-up/scale-down cycling".
- **Caption (bottom center, italic 11px `#555`):** "Model sees periodic spikes. It's a control system artifact, not a real pattern."

## Cold Start Penalty — First Request Data Is Garbage

**New Instance: First Request Takes 2-10 Seconds. Subsequent: 50ms.**

- **The problem:** A cold instance serves its first request in 2-10 seconds versus 50ms once warm.
- **Two experiences:** Those are completely different products from the same code and same deploy.
- **The data contamination:** The latency distribution is bimodal, so averages describe nobody.
- **A/B confound:** A "winning" variant may simply have been served with fewer cold starts.
- **The model serving problem:** Cold-start weight loading returns the first prediction after the user left.
- **Permanent tail:** So roughly 5% of users always get NULL scores and the fallback experience.

### Visualization (canvas `c3`, 720×240)

Filled bimodal density curve of latency.

- **Background:** `#f9f9f9`; L-shaped `#333` axes; margins left 50, right 30, top 30, bottom 40.
- **Curve:** filled area with stroke `#27ae60` width 2 and fill `rgba(39,174,96,0.3)`; density over x=0..100: 0.95·gauss(x, mean 8, sd 3) + 0.05·gauss(x, mean 70, sd 8) — a tall narrow peak near the left and a small bump at ~70% width. Peak height scaled to 90% of plot height.
- **Labels:** bold 11px, centered: green `#27ae60` "95%: 50ms (warm)" at ~12% plot width near the top; red `#e74c3c` "5%: 3000ms (cold start)" at ~72% plot width slightly lower.
- **Caption (bottom center, italic 11px `#555`):** "Average: 195ms (meaningless). Distribution is bimodal. p50 and p99 tell different stories."

## Spot Eviction — Computation Interrupted Mid-Pipeline

**2-Minute Warning → Checkpoint → Lose 5 Min of Work → Reschedule**

- **The scenario:** A 2-minute eviction warning is not enough time to checkpoint 50GB of state.
- **Work lost:** The job restarts from the last full checkpoint, losing 30 minutes of compute.
- **The data pipeline impact:** An ETL job evicted mid-JOIN still writes its partial output to the table.
- **No error raised:** Downstream jobs read that partial table as complete and publish wrong aggregates.
- **The non-reproducibility:** Retried runs diverge through sampling and hash partitioning order.
- **Untraceable drift:** Two supposedly identical training runs then yield different model weights.

### Visualization (canvas `c4`, 720×240)

Horizontal event timeline of a spot eviction.

- **Background:** `#f9f9f9`.
- **Title (bold 12px `#1a5276`, top center):** "Spot instance eviction: work lost, partial data written".
- **Timeline:** horizontal `#333` line (width 2) across the canvas at y = top margin (40) + 50; margins left/right 30.
- **Events:** colored tick marks (vertical lines ±10px through the timeline) with small 9px labels below (multi-line where shown), at fractional x positions:
  - 0.10 — "Start job" — `#27ae60`
  - 0.40 — "Processing..." — `#3498db`
  - 0.60 — "⚠ 2min warning" — `#e67e22`
  - 0.65 — "Checkpoint" / "(partial)" — `#e67e22`
  - 0.70 — "EVICTED" — `#e74c3c`
  - 0.80 — "Reschedule" / "+ reload" — `#8e44ad`
  - 0.95 — "Resume" / "(30min lost)" — `#3498db`
- **Caption (bottom center, italic 11px `#555`):** "Partial data in output table. Downstream reads 50K rows instead of 100K. No error thrown."

## Cost ≠ Utilization — Competing Objectives in Data Collection

**10 Instances at 20% CPU: Expensive but Safe. 2 Instances at 90%: Cheap but Fragile.**

- **The tradeoff:** High utilization is cheap but leaves no headroom for the next unplanned burst.
- **Stale under load:** Saturated CPUs still emit "real-time" features, computed from stale inputs.
- **The data quality issue:** Features lag 30 seconds under load but only 1 second during training.
- **Skew from nowhere:** That gap is training-serving skew caused by infrastructure pressure alone.
- **The cost-driven data loss:** Event sampling, shorter retention, and slower pipelines all cut spend.
- **Degradation in disguise:** Each of those three cuts is also a data quality loss nobody logged.

### Visualization (canvas `c5`, 720×240)

Paired horizontal bars: cost saving (green) stacked over quality loss (red) for three cost optimizations.

- **Background:** `#f9f9f9`.
- **Title (bold 12px `#1a5276`, top center):** "Cost optimization = data quality degradation".
- **Rows** (labels right-aligned in 11px `#1a5276` to the left of the bars; left margin 220, right 30, top 45; bar row height 45 with 15px gap; each row has a green `#27ae60` top half-bar and a red `#e74c3c` bottom half-bar, both half the available width, with bold 10px white centered text):
  - "Sample events (keep 60%)" — green: "-40% cost" / red: "40% of events missing"
  - "Reduce retention (7d vs 30d)" — green: "-30% storage" / red: "Can't retrain on >7d data"
  - "Hourly pipeline (vs per-minute)" — green: "-50% compute" / red: "Features 60× staler"
- **Caption (bottom center, italic 11px `#555`):** "Every cost cut trades money for data quality. The tradeoff is hidden until model degrades."

## Capacity Planning From Non-Stationary Demand

**Last Year's Pattern ≠ This Year's. Until It Suddenly Is Again.**

- **The problem:** Capacity planning extrapolates history and assumes next year rhymes with last.
- **Never in the data:** Product launches, viral events, and competitor outages have no historical rows.
- **Under-provisioning:** An overwhelmed collection system starts dropping events under peak load.
- **Gaps where it matters:** Training data is therefore thinnest exactly at the loads the model must handle.
- **Over-provisioning:** Idle capacity invites right-sizing down to "barely enough" for normal traffic.
- **The quarterly loop:** That trimmed footprint fails at the first surprise spike, and the cycle restarts.

### Visualization (canvas `c6`, 720×240)

Expected vs actual demand lines with unexpected spikes.

- **Background:** `#f9f9f9`; L-shaped `#333` axes; margins left 50, right 30, top 30, bottom 40.
- **Expected series (dashed `#3498db`, dash 5/5, width 2):** 100 points; value = 40 + 15·sin(t·π·4) — smooth seasonal wave, two cycles.
- **Actual series (solid `#e74c3c`, width 2.5):** same seasonal base, plus +50 for t in (0.3, 0.4) (viral event) and +35 for t in (0.7, 0.75) (competitor outage). Scale: value/100 of plot height.
- **Annotations (bold 10px `#e74c3c`, centered):** "Viral event" at ~35% plot width near the top; "Competitor outage" at ~72% plot width slightly lower.
- **Legend (top right, 11px):** dashed `#3498db` swatch "Expected"; solid `#e74c3c` swatch "Actual".
- **Caption (bottom center, italic 11px `#555`):** "Under-provision during spikes → data collection drops events. Model never sees peak behavior."

## Regeneration instructions

- **Layout:** domains detail-page style — h1, `.subtitle` paragraph, then one `<h2>` per pitfall (unnumbered, with `border-bottom: 2px solid #2980b9`), each followed by a single-row `.obj-table`: left `<td>` (40%) with `.obj-title` div + `<ul>` of labeled one-sentence bullets, right `<td>` (60%, centered) with the canvas. Even table rows have background `#fafcfe`. No thead, no nav, no badges, no cross-page links.
- **Page CSS:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.4em `#1a5276`; `.subtitle` `#666` 1.05em; `ul` 0.9em `#333`; `strong` `#1a5276`; `.obj-table td` border `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em weight 600 `#1a5276`; `.philosophy` callout style available (background `#f0f4f8`, left border `4px solid #2980b9`) though unused on this page.
- **Canvas:** each declares intrinsic `width="720" height="240"`; a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, secondary blues `#2471a3`/`#3498db`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, gray text `#555`/`#666`/`#333`.
- Card/page links in regenerated HTML use `.html` extensions.
