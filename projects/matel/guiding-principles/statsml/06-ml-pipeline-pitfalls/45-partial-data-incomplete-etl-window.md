# Pitfall: Partial Data (Incomplete ETL Window)

**Page type:** detail page (three card-sections, each a two-column layout table: text left 45%, canvas right 55%)
**HTML title tag:** Partial Data (Incomplete ETL Window)

**Subtitle:** Analysis runs before all data arrives, creating systematically biased training sets.

## The Problem

Tags: `the trap` (red), `completeness` (blue)

- **Early run** — the training job queries day D before all of day D's data has arrived
- **No completeness check** — the job proceeds with whatever arrived and silently drops stragglers
- **Late events** — processing lag and timezone batching delay day-D events by hours every day
- **Systematic bias** — the same sources are missing every run, not a random sample of rows
- **Mixed completeness** — backfilled history is complete while recent days are partial
- **Serving mismatch** — the model trains on ~60% snapshots but scores complete data

*Example:* A fraud model trained at 3 AM misses 35% of EU transactions arriving on a 4-hour lag, and its EU false negative rate runs 2.8x higher.

**Impact:** Training data systematically underrepresents certain sources, so complete serving data arrives as a distribution shift.

### Visualization (canvas `c1`, 720×300)

Gantt-style timeline: four data-source arrival bars against an hourly axis, with a training-job marker cutting in too early.

- **Title (bold 14px `#1a5276`, top center):** "ETL Window: Training Job Runs Too Early".
- **Timeline:** horizontal gray `#999` axis from x=100 to x=650 at y=230, tick labels 00:00, 01:00, 02:00, 03:00, 04:00, 05:00 evenly spaced (10px `#666`).
- **Arrival bars (height 25, dashed outline = expected window, 60%-alpha solid fill = portion present at the cutoff):** Source A at y=70 streams 0.00→0.25 of the axis, green `#27ae60`; Source B at y=110 streams 0.05→0.35, green `#27ae60`; Source C at y=150 streams 0.30→0.55, orange `#e67e22`; Source D at y=190 streams 0.45→0.75, red `#e74c3c`. Each bar labeled with its source name (bold 11px `#2c3e50`) inside the left end, a "✓ HH:MM" completion marker (9px, bar color) above its right end — the clock time is computed from the axis fraction (5-hour axis), giving 01:15, 01:45, 02:45 and 03:45 rather than snapping to the nearest tick — and a computed "N% present" label (bold 10px, bar color) to the right of the expected window.
- **Training job marker:** vertical red 3px line at 40% of the axis (`trainFrac = 0.4`) from y=50 to y=230, labeled "TRAINING" / "JOB RUNS" in bold 12px red above it, with a red downward arrowhead at the base.
- **Computed completeness (no hardcoded statistics):** each source's share present is `clamp((trainFrac − start) / (end − start), 0, 1)`, giving Source A 100%, Source B 100%, Source C 40%, Source D 0%; the snapshot completeness is the mean of those four, 60%. Complete / partial / missing counts and the snapshot percentage are all derived in JS at render time and interpolated into the result box.
- **Right-side legend (10px bold, x=670):** "✓ Data available" in green (y=90), "⚠ Partial data" in orange (y=160), "✗ Missing data" in red (y=200).
- **Result box (100, 250, 550×40, white with red 2px border):** "RESULT:" in bold 11px red, then 11px with computed values: "Training sees 2 complete sources, 1 partial (40%), 1 missing (0%). Model learns from biased 60% snapshot." and "At serving time, all 4 sources present → distribution shift → accuracy degrades."

## Why It Happens

Tags: `root cause` (orange), `scheduling` (blue)

- **Clock triggers** — batch pipelines fire at a fixed wall-clock time, not on data readiness
- **Silent gaps** — a query over a partial window returns rows with no signal that some are missing
- **Timezone lag** — events from western regions land hours after their calendar day has closed
- **Variable latency** — upstream feeds deliver with unpredictable delay, so stragglers are routine
- **False confidence** — "query returned rows" is mistaken for "all data for this period is present"

*Example:* A 2am UTC pipeline consumes "yesterday" before US West Coast events from 11pm local arrive at 7am UTC, losing them from training for good.

**Root Cause:** Schedule-based triggers conflate "query returned results" with "all data for this period is present."

### Visualization (canvas `c2`, 720×300)

Timeline diagram: a fixed pipeline trigger versus events that keep arriving hours later.

- **Title (bold 14px `#1a5276`, top center):** "Schedule-Based Trigger vs Actual Data Arrival".
- **Timeline:** gray `#999` axis from x=80 to x=650 at y=160, tick labels 00:00, 02:00, 04:00, 06:00, 08:00, 10:00 evenly spaced (10px `#666`).
- **Query window:** shaded box `rgba(26,82,118,0.1)` with `#1a5276` dashed border (dash 5/3) covering the first 35% of the axis from y=45 down to the axis; label "QUERY WINDOW" in bold 11px `#1a5276` centered above it.
- **Pipeline trigger:** vertical red 3px line at 20% of the axis (02:00) from y=45 to the axis, labeled "PIPELINE" / "TRIGGERS" in bold 10px red above.
- **Event dots (radius 5, y jittered 80–130 by a seeded Park-Miller LCG — never `Math.random()`):** green `#27ae60` dots at axis fractions 0.05, 0.08, 0.1, 0.12, 0.15, 0.17 (captured before the trigger, jitter seed `20250945`); red `#e74c3c` dots at 0.45, 0.5, 0.55, 0.6, 0.64, 0.67, 0.7 (arrive after the pipeline ran, jitter seed `778213`). Both series share the same 80–130 band; the separate seeds keep their scatter independent and identical on every load.
- **Gap annotation:** orange `#e67e22` dashed double-arrow line (dash 4/3) at y=135 spanning from just right of the trigger to the last plotted late event (0.7 of the axis = 07:00), labeled "LATE EVENTS MISSED (up to 5 hours)" in bold 10px orange above it — the 5 is computed as `(maxLateFraction − 0.2) × 10 h`, matching the 02:00 trigger and the 07:00 arrival in the prose.
- **Legend (11px, below axis at x≈80), counts computed from the plotted dots:** green dot — "Events captured before trigger: 6 of 13 (46%)"; red dot — "Events lost after pipeline ran: 7 of 13 (54%)". 6 + 7 = 13 and the two percentages sum to 100.
- **Summary box (80, 240, 560×45, white with orange 2px border):** "PROBLEM:" in bold 11px orange, then 11px `#2c3e50`: "Pipeline at 2am UTC captures US East events but misses US West (UTC-8)." and "11pm PST events arrive at 7am UTC — 5 hours after cutoff. Permanently lost from training."

## The Correct Approach

Tags: `the fix` (green), `gating` (blue)

- **Completeness gate** — verify an explicit completeness SLA before any processing begins
- **Watermarks** — hold the run until event counts confirm all sources have flushed the window
- **Event-time windows** — window on event time, not processing time, so late rows land correctly
- **Grace periods** — keep the window open long enough for routine stragglers to arrive
- **Completeness tiers** — never mix complete backfilled history with still-arriving recent data
- **Timeout alerts** — if the gate never opens, alert loudly instead of proceeding silently

*Example:* The pipeline waits until observed events reach 95% of the trailing 7-day average for that hour, and late data is reprocessed in the next window.

**Fix:** Gate pipeline execution on completeness signals — high-watermarks and expected row counts — not wall-clock time.

### Visualization (canvas `c3`, 720×300)

Bar chart: event counts accumulating hour by hour toward a 95% watermark threshold that opens the gate.

- **Title (bold 14px `#1a5276`, top center):** "Watermark-Based Completeness Gate".
- **Axes:** chart area x 100–620, y 60–200; y labels 0%, 50%, 95%, 100% (10px `#666`, right-aligned); x labels T+0 through T+9h under each bar (9px `#666`).
- **Threshold line:** green `#27ae60` dashed 2px line (dash 6/4) at 95%, labeled "THRESHOLD (95% of 7-day avg)" in bold 10px green to the right of the chart.
- **Bars:** 10 accumulation bars with values `[0.3, 0.45, 0.55, 0.65, 0.72, 0.8, 0.85, 0.9, 0.95, 0.98]` of chart height; bars below threshold filled `rgba(230,126,34,0.4)` with `#e67e22` 1.5px stroke, bars at/above threshold filled `rgba(39,174,96,0.6)` with `#27ae60` stroke.
- **Gate indicators (above chart), positions computed from the data:** the open index is the first bar whose value reaches the 0.95 threshold — bar 9 (T+8h, 95%) — shown as "✅" with "GATE OPENS (95%)" in green and a green 2px arrow down to that bar's top; "⛔" with "GATE CLOSED (80%)" in bold red sits three bars earlier (T+5h). Both captions print the bar's own computed percentage.
- **Summary box (80, 230, 560×55, white with green 2px border):** "CORRECT:" in bold 11px green, then 11px `#2c3e50`: "Pipeline waits until event count >= 95% of trailing 7-day average — reached at T+8h." (threshold and reach-time both computed), "Gate opens only when watermark is met. If timeout reached, alert fires (don't proceed silently).", "Late data handled via reprocessing in next window — never permanently lost."

## Regeneration instructions

- **Layout:** three `.card-section` blocks ("The Problem", "Why It Happens", "The Correct Approach"), each with an h2 underlined by `2px solid #2980b9` and a `table.layout` (one `<tr>`): left `<td class="text-col">` (45%) holds `.tags` pills, a `<ul>` of labeled bullets, a `.example` paragraph, and a `.key-point` callout; right `<td class="viz-col">` (55%) holds one canvas.
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with `2px solid #2980b9` bottom border; `.subtitle` `#666` 0.95rem; `li b` in `#1a5276`; `ul` 0.92rem.
- **Tag pills:** `.tag` inline-block, 0.72rem bold, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Callouts:** `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem; `.example` italic `#555` 0.9rem.
- **Canvas:** intrinsic 720×300, CSS `width: 100%`, `1px solid #e0e0e0` border, 4px radius; scaled by `window.devicePixelRatio` via a shared `setup(id)` helper (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). A seeded Park-Miller LCG helper `lcg(seed)` sits directly after `setup(id)`; canvas 2's event-dot jitter draws from `lcg(20250945)` (on-time series) and `lcg(778213)` (late series) so every render is byte-identical. No canvas calls `Math.random()`.
- **Computed labels:** every percentage, count and reach-time printed on a canvas is derived in JS from the plotted values at render time — source completeness and the 60% snapshot in canvas 1, the captured/lost counts and maximum lag in canvas 2, and the gate percentages and T+8h reach-time in canvas 3.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`; accent `#2980b9`; text `#2c3e50`/`#666`.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
