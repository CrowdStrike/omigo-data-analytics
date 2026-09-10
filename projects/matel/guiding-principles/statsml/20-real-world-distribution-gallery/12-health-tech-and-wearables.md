# Health Tech & Wearables — Distribution Patterns

**Page type:** detail page (three-column obj-table layout: text left 38%, two canvases 31% each, one table per section)
**HTML title tag:** Health Tech & Wearables — Distribution Patterns

## Effect of Using Alarm on Sleep Data

**Pitfall label (uppercase, `#795548`):** ALARM CLOCK SHAPES YOUR SLEEP DATA

Weekdays cluster tightly around 6.5-7 hours; weekends split into two peaks — one still near 7 hours and one at 9-10 hours. The tight weekday cluster is consistent with an external constraint (the alarm) suppressing individual variation, and the weekend split with two groups: habitual wakers and catch-up sleepers.

- Weekday: packed into 6.5-7 hours — very little individual variation survives
- Weekend: two peaks — ~7-hour wakers vs. 9-10 hour sleepers
- The weekday tightness is consistent with an externally imposed wake time
- One reading of the weekend split: chronically under-slept catch-up vs. genuinely rested — the histogram can't confirm which

### Visualization (canvas `canvas1`, 420×340)

Overlaid histogram (shared `drawHistogram` utility): weekday vs weekend sleep duration.

- **Title (bold 13px, `#1a5276`, top center):** "Sleep Duration: Weekday (blue) vs Weekend (red)".
- **Data (seeded RNG mulberry32(42), Box-Muller normals):** weekday = 600 samples of `6.75 + N(0,1)*0.4`; weekend = 250 samples of `7.0 + N(0,1)*0.5` plus 150 samples of `9.5 + N(0,1)*0.6`.
- **Bins/range:** 35 bins, x from 4.5 to 12 (padded ±2% of range), tick labels 1 decimal at 5 evenly spaced positions; x-axis label "Hours of Sleep".
- **Primary series:** weekday bars fill `rgba(26,82,118,0.4)`, stroke `#1a5276`; overlay weekend bars fill `rgba(231,76,60,0.35)`, stroke `#e74c3c`.
- **Density line + SE band (standard for all histograms on this page):** Gaussian-smoothed counts (sigma 1.5 bins), line `#1e8449` width 2, 95% band `rgba(46,204,113,0.2)` using effective N clamped to [30, 200].
- **Axes:** L-shaped gray `#ccc` axes, margins top 40 / right 20 / bottom 45 / left 50; y labeled with max count and 0.
- **Legend (top right):** swatch "Weekday" `rgba(26,82,118,0.6)`, swatch "Weekend" `rgba(231,76,60,0.5)`.

### Visualization (canvas `canvas1b`, 400×340)

ECDF comparison line chart.

- **Title:** "ECDF: Alarm Constraint vs Natural Sleep".
- **Data:** ECDFs of the same weekday and weekend sample arrays; x range 4.5-12, y 0-1.
- **Shaded zone:** "alarm-constrained zone" from x=6 to x=7.5 — fill `rgba(26,82,118,0.08)`, dashed vertical borders `rgba(26,82,118,0.3)` (dash 4/3); bold 10px label "ALARM ZONE" centered at zone top in `#1a5276`.
- **Curves:** weekday ECDF `rgba(26,82,118,0.8)`, weekend ECDF `rgba(231,76,60,0.8)`, both width 2.5.
- **Annotation:** orange `#e67e22` arrow pointing at the steep weekday section near x=6.75, y≈35% height, with bold 10px label "Near-vertical = forced".
- **Legend (bottom right):** line swatches "Weekday" (blue) and "Weekend" (red).
- **Axes:** x ticks 1 decimal at 6 positions, label "Hours of Sleep"; y labels 0.0 / 0.5 / 1.0; margins top 40 / right 20 / bottom 45 / left 55.

## Resting Heart Rate — The Jump From Your Baseline Is the Signal

**Pitfall label (uppercase, `#2980b9`):** STEADY WITH SUDDEN SPIKES

Day-to-day resting heart rate is boringly consistent — a tight cluster around a personal baseline. That's what makes a sudden 5+ BPM shift stand out so sharply: in the simulated series here, a drift from 58 to the mid-60s is unmistakable against a baseline that normally moves 1-2 BPM. A jump from 58 to 65 in a fit person is a louder signal than a steady 72 in someone else.

- Day-to-day: very tight around the personal baseline
- A sudden 5+ BPM rise stands far outside normal variation — commonly interpreted as the body fighting something off
- The change from YOUR normal carries more signal than any fixed "healthy" number
- This is why per-user baselines beat population thresholds for anomaly detection

### Visualization (canvas `canvas2`, 420×340)

Overlaid histogram: healthy baseline vs illness-onset resting HR.

- **Title:** "Resting HR: Normal (blue) vs Illness Onset (red)".
- **Data:** baseline = 500 samples of `58 + N(0,1)*1.8`; illness = 150 samples of `65 + N(0,1)*2.2`.
- **Bins/range:** 35 bins, x 50-75 BPM, 0 decimals; x-axis label "BPM".
- **Series:** baseline bars fill `rgba(26,82,118,0.4)` stroke `#1a5276`; illness overlay fill `rgba(231,76,60,0.4)` stroke `#e74c3c`. Standard density line + SE band.
- **Legend:** "Healthy (58 BPM)" `rgba(26,82,118,0.6)`, "Illness (65 BPM)" `rgba(231,76,60,0.5)`.

### Visualization (canvas `canvas2b`, 400×340)

Time-series scatter with shift detection.

- **Title:** "HR Time-Series: Illness Onset at Day 30".
- **Data:** 45 daily points; days 0-29: `58 + N(0,1)*1.8`; days 30-44: `58 + (day-30)*0.8 + N(0,1)*2.2`. Y range 50-75.
- **Zones:** illness region (day ≥30) shaded `rgba(231,76,60,0.08)`; bold 10px labels at top — "HEALTHY BASELINE" in `rgba(39,174,96,0.7)` over the left zone, "ILLNESS ONSET" in `rgba(231,76,60,0.8)` over the right.
- **Reference lines:** dashed green `rgba(39,174,96,0.6)` mean line at 58 across the healthy zone (dash 5/3); dashed red `rgba(231,76,60,0.5)` threshold at 63 across full width (dash 3/3) labeled "+5 BPM alert" in `#e74c3c` 9px.
- **Points:** 4px circles — blue `rgba(26,82,118,0.7)` stroke `#1a5276` before day 30, red `rgba(231,76,60,0.75)` stroke `#e74c3c` after.
- **Annotation:** orange `#e67e22` upward arrow at day-30 x position below the axis with bold 10px label "SHIFT".
- **Axes:** x ticks "D0"…"D45" at 6 positions, label "Day"; y labels 50 / 62.5 / 75; margins top 40 / right 20 / bottom 45 / left 50.

## Daily Steps — Right-Skewed With a Goal Spike

**Pitfall label (uppercase, `#27ae60`):** SPIKE JUST PAST THE GOAL

Daily step counts form a right-skewed, single-peaked hill — many moderate days and a long tail of very active ones. The behavioral fingerprint sits just past 10,000: an excess spike at 10,000-10,500 paired with a deficit just below it, consistent with goal-driven users pushing to reach the round-number target and then stopping. Self-tracked data carries the tracker's incentives in its shape, not just the physiology.

- One right-skewed hill — a smooth spectrum of activity, not separate "sedentary" and "active" populations
- Excess bump at 10,000-10,500 — days pushed just past the default goal (goal-seeking heaping)
- Matching deficit just under 10,000 — days that would have ended near 9,500 got walked over the line
- The spike is a behavioral artifact of the goal setting, not a physiological mode

### Visualization (canvas `canvas3`, 420×340)

Single histogram: right-skewed daily steps with goal-seeking heaping.

- **Title:** "Daily Steps: Right-Skewed + Goal Spike Past 10k".
- **Data:** 700 samples via `sampleDay()` — log-normal `exp(8.75 + N(0,1)*0.5)` (median ~6,300 steps, long right tail), rejection-sampled to stay ≤18,000; goal-seeking heaping rule: samples in [8800, 10000) are, with probability 0.6, remapped to `10000 + U(0,1)*500` (mass moved from just below the goal to just past it).
- **Bins/range:** 40 bins, x 0-18000, 0 decimals; x-axis label "Steps per Day".
- **Bars:** fill `rgba(41,128,185,0.35)`, stroke `#1a5276`. Standard density line + SE band. No legend.
- **Annotation (drawn after the histogram, context keeps its dpr transform):** orange `#e67e22` width-2 arrow pointing down-right at the 10,250-step bin (x computed against the ±2%-padded 0-18000 range), with right-aligned bold 10px label "Goal spike just past 10k" to the arrow's upper left.

### Visualization (canvas `canvas3b`, 400×340)

Zoomed bar chart of the 9,000-11,000 range showing mass piling just past 10,000.

- **Title:** "Zoom 9k-11k: Mass Piles Just Past the Goal".
- **Data:** a fresh 6,000-sample draw from the same `sampleDay()` process; 100-step bins from 9,000 to 11,000 (20 bins), heights normalized to the max bin at 80% of plot height.
- **Bars:** bins below 10,000 fill `rgba(26,82,118,0.35)` stroke `#1a5276` (deficit side); bins at/above 10,000 fill `rgba(230,126,34,0.7)` stroke `#d35400` (excess side).
- **Goal line:** vertical dashed red `#e74c3c` line at 10,000 (dash 4/3, width 2) with bold 10px label "GOAL 10,000" centered above it.
- **Side labels (near top of plot, y offsets +32 / +44):** blue `#1a5276` bold 10px "DEFICIT" with 9px sub-line "days walked past the line" centered over x≈9,450; orange `#d35400` bold 10px "EXCESS" with 9px sub-line "stop right after the goal" centered over x≈10,550.
- **Axes:** x ticks at 9000 / 9500 / 10000 / 10500 / 11000, label "Steps per Day"; y labeled max count and 0; margins top 40 / right 20 / bottom 50 / left 45.

## Blood Glucose — The Shape Shifts Before the Threshold Trips

**Pitfall label (uppercase, `#e74c3c`):** SHAPE CHANGES = DISEASE PROGRESSION

In this simulated progression, healthy readings form a tight, roughly symmetric cluster around 80-120 mg/dL. As control degrades, a "shoulder" grows on the right side (post-meal spikes lingering); further along, the right side dominates. The interesting part: the shape change is visible while the mean is still on the healthy side of any single cutoff — one reason distribution shape can carry earlier signal than a threshold check.

- Healthy: tight, symmetric cluster around 80-120 mg/dL
- Pre-diabetic stage: a bump grows on the right (post-meal spikes linger)
- Diabetic stage: the right side dominates — high readings become the norm
- In this simulation, the shape morphs before the average crosses a diagnostic cutoff

### Visualization (canvas `canvas4`, 420×340)

Single histogram: pre-diabetic glucose with emerging right shoulder.

- **Title:** "Blood Glucose: Pre-Diabetic (Right Shoulder Emerging)".
- **Data:** 400 samples of `95 + N(0,1)*10` (main bell); 150 samples of `140 + N(0,1)*15` (right shoulder); 80 samples of `115 + N(0,1)*12` (transition zone).
- **Bins/range:** 40 bins, x 60-200 mg/dL, 0 decimals; x-axis label "mg/dL".
- **Bars:** fill `rgba(230,126,34,0.35)`, stroke `#e67e22`. Standard density line + SE band. No legend.

### Visualization (canvas `canvas4b`, 400×340)

Overlapping filled density curves for three disease stages.

- **Title:** "Shape Progression: Healthy → Pre-Diabetic → Diabetic".
- **Curves (Gaussian mixtures, x 60-220, drawn back-to-front so Healthy is on top):**
  - Healthy — fill `rgba(39,174,96,0.6)`, stroke `#1e8449`; single component mu=95, sigma=8, weight 1.0.
  - Pre-Diabetic — fill `rgba(230,126,34,0.6)`, stroke `#d35400`; components (mu=95, sigma=10, w=0.65) + (mu=140, sigma=15, w=0.35).
  - Diabetic — fill `rgba(231,76,60,0.6)`, stroke `#c0392b`; components (mu=100, sigma=12, w=0.3) + (mu=160, sigma=20, w=0.7).
- **Threshold lines (vertical dashed, dash 4/3, labels 9px below axis):** at 100 in `#27ae60` labeled "Normal <100 (fasting)"; at 126 in `#e74c3c` labeled "Diabetes ≥126 (fasting)".
- **Annotation:** dark `#333` horizontal arrow from x=95 to x=155 near the top (15% height) with bold 10px label "YEARS OF PROGRESSION" above it.
- **Legend (top right):** filled swatches for Healthy / Pre-Diabetic / Diabetic.
- **Axes:** x ticks at 5 positions (rounded), label "mg/dL"; margins top 40 / right 20 / bottom 50 / left 50.

## Workout Duration — Timer Spikes on Top of a Smooth Hill

**Pitfall label (uppercase, `#8e44ad`):** SPIKES AT CLASS TIMES

Sharp spikes at 30, 45, and 60 minutes sit on top of a smooth log-normal hill. The spikes line up with class lengths and app timers; the hill underneath is consistent with free-form exercisers stopping when they feel done. In this simulated mix, preset-length sessions are ~46%, free-form ~45%, and ~9% stop around 7 minutes.

- Spikes at 30, 45, 60 min — durations set by a clock, not by the body
- Smooth hill underneath — sessions that end whenever the exerciser decides
- Spike at 7 min — a "started and quit" mode that self-reports rarely mention
- Roughly half of the simulated sessions are externally timed

### Visualization (canvas `canvas5`, 420×340)

Single histogram: log-normal base with class-time spikes.

- **Title:** "Workout Duration: Log-Normal + Class Spikes (30, 45, 60 min)".
- **Data:** 300 samples of `exp(3.2 + N(0,1)*0.5)` kept if in (0, 120) — centered ~25 min; spikes: 120 samples of `30 + N(0,1)*1.5`, 90 samples of `45 + N(0,1)*1.5`, 100 samples of `60 + N(0,1)*2`; "gave up" spike: 60 samples of `7 + N(0,1)*1.2`.
- **Bins/range:** 50 bins, x 0-90 minutes, 0 decimals; x-axis label "Minutes".
- **Bars:** fill `rgba(142,68,173,0.35)`, stroke `#27ae60`. Standard density line + SE band. No legend.

### Visualization (canvas `canvas5b`, 400×340)

Waterfall bar chart of exercise motivation categories.

- **Title:** "Exercise Motivation Breakdown (Waterfall)".
- **Categories (cumulative waterfall, left to right; count label in white bold 12px centered on each bar, percentage in stroke color above each bar; two-line category label below axis):**

| Label | Count | % | Fill | Stroke |
|-------|-------|---|------|--------|
| Gave Up (~7 min) | 60 | 9% | rgba(231,76,60,0.75) | #c0392b |
| Free Exercise | 300 | 45% | rgba(142,68,173,0.7) | #7d3c98 |
| 30-min Class | 120 | 18% | rgba(41,128,185,0.7) | #1a5276 |
| 45-min Class | 90 | 13% | rgba(39,174,96,0.7) | #1e8449 |
| 60-min Class | 100 | 15% | rgba(230,126,34,0.7) | #d35400 |

- **Connectors:** dashed gray `#999` lines (dash 3/2) from each bar top to the next bar's base.
- **Annotations:** red `#e74c3c` curved arrow from the "Gave Up" bar with two-line bold 9px label "Hidden in / self-reports!"; blue `#1a5276` bracket spanning the three class bars labeled "EXTERNALLY TIMED (46%)" in bold 10px.
- **Y-axis:** labels total (670), half, and 0; margins top 40 / right 15 / bottom 65 / left 45.

## Body Temperature — A Wave Hiding Inside What Looks Like a Bell

**Pitfall label (uppercase, `#e67e22`):** HIDDEN WAVE LOOKS LIKE A BELL

Measure temperature once a day at a random time and the histogram looks like a bell curve around 98.6°F. Continuous monitoring reveals the same data is actually a daily wave — lowest around 4am, peaking in the late afternoon, with about a 1°F swing. One reading per day smashes that wave into a blob, and a fixed fever cutoff ignores where in the cycle you are.

- One reading per day → looks like random scatter around 98.6°F (you're sampling a wave at random phases)
- Continuous monitoring → a clear daily wave with ~1°F peak-to-trough swing
- Trough around 4am, peak in the late afternoon — the circadian rhythm made visible
- 99.5°F at 4am is more alarming than 99.5°F at 4pm — same number, different position in the cycle

### Visualization (canvas `canvas6`, 420×340)

Single histogram: single daily temperature measurements.

- **Title:** "Body Temp: Single Daily Measurement (Looks Gaussian)".
- **Data:** 500 samples — random hour in [0,24), temp = `98.6 - 0.5*cos((hour-4)/24 * 2π) + N(0,1)*0.3`.
- **Bins/range:** 40 bins, x 97.0-100.0 °F, 1 decimal; x-axis label "°F".
- **Bars:** fill `rgba(230,126,34,0.4)`, stroke `#d35400`. Standard density line + SE band. No legend.

### Visualization (canvas `canvas6b`, 400×340)

Circadian curve with scatter overlay.

- **Title:** "Continuous: Circadian Wave Revealed".
- **Curve:** true circadian wave `98.6 - 0.5*cos((h-4)/24 * 2π)` over hours 0-24, orange `#e67e22` width 3. Y range 97.5-99.5.
- **Scatter:** 80 noisy measurements (same formula + `N(0,1)*0.3`) as 3px dots in `rgba(26,82,118,0.5)`.
- **Annotations (bold 10px, `#e74c3c`):** "TROUGH 4am" near the 4am dip; "PEAK ~4pm" above the 4pm crest.
- **Reference line:** dashed gray `rgba(149,165,166,0.6)` horizontal line at 98.6 (dash 4/3) labeled `98.6 "normal"` in `#999` 9px.
- **Axes:** x ticks 12am / 4am / 8am / 12pm / 4pm / 8pm / 12am, label "Time of Day"; y ticks 97.5 to 99.5 by 0.5; margins top 40 / right 20 / bottom 50 / left 55.

## Stress Episodes Per Day — Most Days Nothing Happens

**Pitfall label (uppercase, `#16a085`):** MOSTLY ZERO, OCCASIONALLY SOMETHING

About two out of three simulated days register zero stress events. On the days something does register, it's typically 1-3 episodes; 5+ is rare. That makes the first modeling question binary — "did anything happen today?" — before any count matters. In the simulated weekly pattern, the non-zero days concentrate on workdays.

- ~2 in 3 days = zero events detected. Most days are just... fine.
- On days something triggers: usually 1-3 episodes
- The real detection challenge is binary: something vs. nothing (zero-inflation)
- 5+ events in one day = rare — an outlier worth flagging

### Visualization (canvas `canvas7`, 420×340)

Single histogram: zero-inflated Poisson counts.

- **Title:** "Stress Episodes/Day: Zero-Inflated Poisson".
- **Data:** 360 zeros plus 240 Poisson(λ=2) draws (Knuth algorithm).
- **Bins/range:** 12 bins, x -0.5 to 10.5, 0 decimals; x-axis label "Episodes per Day".
- **Bars:** fill `rgba(231,76,60,0.4)`, stroke `#c0392b`. Standard density line + SE band. No legend.

### Visualization (canvas `canvas7b`, 400×340)

Paired bar chart: weekday vs weekend stress episode distribution.

- **Title:** "Weekday vs Weekend Stress Pattern".
- **Data:** 8 simulated weeks — weekdays (Mon-Fri): 30% zero, otherwise Poisson(λ=2.5); weekends (Sat-Sun): 80% zero, otherwise Poisson(λ=1). Counts capped at 8 episodes, normalized to percent within each group.
- **Bars:** for each episode count 0-8, paired bars — weekday `rgba(231,76,60,0.7)`, weekend `rgba(39,174,96,0.7)`; bar width 35% of group width, 2px gap between pair.
- **Legend (top right):** swatches "Weekday" (red) and "Weekend" (green).
- **Axes:** x tick per episode count 0-8, label "Stress Episodes"; y labels max% and 0%; margins top 40 / right 20 / bottom 50 / left 50.

## App Engagement — Survive the Early Weeks and You Tend to Stay

**Pitfall label (uppercase, `#d35400`):** SURVIVE LONG ENOUGH AND YOU STICK

In this simulated cohort (Weibull with decreasing hazard), ~40% of users are gone within week 1 and ~70% by week 4. But the quit rate itself falls with time: the longer someone has lasted, the less likely they are to leave in the next week. Past ~6 weeks, weekly churn is a trickle — consistent with a habit having formed.

- ~40% gone in week 1 — the steepest part of the curve
- ~70% gone by week 4 — the drop is still fast but already slowing
- Past 6 weeks the curve flattens — remaining users rarely churn
- Decreasing hazard: having survived this long predicts surviving longer

### Visualization (canvas `canvas8`, 420×340)

Single histogram: time to app abandonment.

- **Title:** "App Abandonment Time (Weibull, k<1)".
- **Data:** 600 samples from Weibull inverse CDF `t = 3 * (-ln(1-u))^(1/0.7)` (k=0.7, λ=3 weeks).
- **Bins/range:** 30 bins, x 0-20 weeks, 0 decimals; x-axis label "Weeks Until Last Use".
- **Bars:** fill `rgba(142,68,173,0.4)`, stroke `#7d3c98`. Standard density line + SE band. No legend.

### Visualization (canvas `canvas8b`, 400×340)

Survival curve with milestones and habit zone.

- **Title:** "Survival Curve: Decreasing Hazard Rate".
- **Curve:** `S(t) = exp(-(t/3)^0.7)` over weeks 0-16, stroke `#7d3c98` width 3; area under the curve filled with a vertical gradient from `rgba(142,68,173,0.4)` (top) to `rgba(142,68,173,0.05)` (bottom).
- **Milestones (red `#e74c3c` dashed drop/leader lines dash 3/3, 4px dot on curve, bold 9px label):** week 1 "~40% gone"; week 4 "~70% gone"; week 6 "Habit zone →".
- **Habit zone:** region from week 6 to 16 shaded `rgba(39,174,96,0.08)`, with two-line bold 10px label "HABIT / FORMED" in `rgba(39,174,96,0.7)` near the top at week 11.
- **Axes:** x ticks "Wk 0" through "Wk 16" every 4 weeks, label "Weeks Since Install"; y labels 100% / 50% / 0%; margins top 40 / right 20 / bottom 50 / left 55.

## Sleep Score (Composite Metric, Personal Baseline Matters)

**Pitfall label (uppercase, `#c0392b`):** MISLEADING COMPOSITE

Unlike steps or heart rate (directly measured by sensor), sleep score is a computed abstraction — a proprietary blend of duration, stages, HR, and movement with opaque weights that are typically not clinically validated. It looks Gaussian at population level (mean ~75 here), but cross-user comparison is weak: in the illustration, Alice's typical range is 68-80 while Bob's is 52-72, so the same 72 means "bad night" for Alice and "great night" for Bob. The delta from your own baseline carries the signal, not the absolute number.

- NOT directly measured — unlike steps, HR, SpO2
- Proprietary composite: weights are opaque and typically not clinically validated
- Same score = different meaning per user (baselines vary widely)
- Most useful signal: personal delta from your own rolling mean

### Visualization (canvas `canvas9`, 420×340)

Single histogram: population-level sleep score.

- **Title:** "Sleep Score: Population Looks Gaussian (Misleading)".
- **Data:** 600 samples of `75 + N(0,1)*10`.
- **Bins/range:** 35 bins, x 40-100, 0 decimals; x-axis label "Sleep Score (0-100)".
- **Bars:** fill `rgba(41,128,185,0.4)`, stroke `#1a5276`. Standard density line + SE band. No legend.

### Visualization (canvas `canvas9b`, 400×340)

Two overlaid per-user Gaussian density curves with a shared score line.

- **Title:** "Same Score = Different Meaning".
- **Curves (x range 45-95):** Alice — Gaussian mu=74, sigma=3, fill `rgba(41,128,185,0.25)`, stroke `#2980b9` width 2; Bob — Gaussian mu=62, sigma=5, fill `rgba(231,76,60,0.25)`, stroke `#e74c3c` width 2 (Bob drawn first, Alice on top).
- **Score line:** vertical dashed `#333` line at x=72 (dash 4/3, width 2), labeled below the axis in bold 14px: "Score: 72".
- **Annotations:** blue `#2980b9` bold 10px 'Alice: "Bad night"' with 9px sub-line "(baseline 74, this is below)" to the right of Alice's curve; red `#e74c3c` bold 10px '"Great night": Bob' with 9px sub-line "(baseline 62, this is +10!)" to the left of Bob's curve.
- **Legend (top left):** swatches "Alice (μ=74)" `rgba(41,128,185,0.6)`, "Bob (μ=62)" `rgba(231,76,60,0.6)`.
- **Axes:** x ticks 50-90 by 10, label "Sleep Score"; margins top 40 / right 20 / bottom 50 / left 45.

## Regeneration instructions

- **Layout:** one `<table class="obj-table">` per section, each with a single `<tr>` of three `<td>`s — left (38%) holds `.pitfall-label` span + `<h3>` + paragraph + `<ul>`; middle (31%, centered) holds the primary 420×340 canvas; right (31%, centered) holds the insight 400×340 canvas.
- **Page style:** body system sans-serif, margin 20px, background `#f9f9f9`, text `#333`; h1 centered `#1a5276`; `.obj-table` full width, collapsed borders, cells `1px solid #2980b9` with 12px padding; h3 `#1a5276` 1.0em weight 700; paragraphs/bullets 14px, line-height 1.5-1.6; `.pitfall-label` inline-block bold 0.72em uppercase with 0.5px letter-spacing; `canvas { width: 100%; height: auto; }`.
- **Pitfall label colors:** assigned by a small script cycling `["#795548","#2980b9","#27ae60","#e74c3c","#8e44ad","#e67e22","#16a085","#d35400","#c0392b","#1abc9c"]` over all `.pitfall-label` elements in document order.
- **Data generation:** seeded RNG `mulberry32(42)` shared across all charts in document order (data values are reproducible only if generated in the same sequence); Box-Muller for normals.
- **Shared histogram utility:** `drawHistogram(canvasId, data, options)` — white background, bold 13px `#1a5276` title, gray `#ccc` L-axes, bars normalized to max count, optional overlay datasets, optional legend, x ticks at 6 positions with configurable decimals, y labeled max count and 0; every histogram also gets a Gaussian-smoothed density line `#1e8449` width 2 with a 95% SE band `rgba(46,204,113,0.2)`.
- **Canvas scaling:** all canvases declare intrinsic width/height attributes and set `max-width` to the intrinsic width, size the backing store to the displayed width (`getBoundingClientRect().width`, falling back to the intrinsic width) × `window.devicePixelRatio`, and `ctx.scale` by that combined factor.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`; secondary purples `#8e44ad`/`#7d3c98`, bar fills as rgba values listed per chart.
- No nav bar, no back/home links. In regenerated HTML any card links would use `.html` extensions (this page has none).
