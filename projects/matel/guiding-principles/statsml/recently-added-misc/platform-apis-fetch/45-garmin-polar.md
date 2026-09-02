# Garmin & Polar

**Page type:** detail page (two-column obj-table layout: text left 45%, payload/canvas right 55%, one row for the Overview section)
**HTML title tag:** Garmin & Polar — Platform APIs

**Subtitle:** Lets approved partners pull workouts and daily health data from users' Garmin watches and Polar devices.

**Verified badge:** Last verified: August 2026

## Overview

### What you can get

- Garmin: daily steps, sleep, stress and heart summaries, delivered to your server as devices sync
- Garmin: full workout recordings with per-second heart rate, speed, power and GPS (the original FIT file)
- Polar: training sessions with per-sample heart rate, speed, cadence and power, plus daily activity and nightly recovery
- Chest-strap heart data from Polar — the most accurate consumer heart signal available
- Body-composition readings from compatible scales

### Key point (callout)

**There is no self-serve sign-up, and Garmin pushes rather than letting you pull.** Both vendors approve integrations case by case, so the legal and commercial step comes before any engineering. Once approved, Garmin sends data to a webhook you host as devices sync; history arrives only through throttled, asynchronous "backfill" requests — out of order and mixed with live data.

### Watch out for

- Polar makes you "commit" a batch after fetching it — commit before your data is safely stored and it is gone for good, with no re-fetch
- Recording is not always one sample per second ("smart recording" skips samples), so counting samples to measure time is wrong
- Wrist optical heart rate and chest-strap readings are different instruments — do not pool them under one "heart rate" label
- Scores like Body Battery and Training Status are Garmin's own formulas and shift with software updates

### Payload example (right column)

Payload note: **What Garmin pushes to your webhook for each workout** — one summary plus a per-second samples array (trimmed)

```json
{ "activityDetails": [{
    "summary": {
      "activityType": "ROAD_BIKING",
      "durationInSeconds": 3612,
      "averageHeartRateInBeatsPerMinute": 148,
      "averagePowerInWatts": 197.4
    },
    "samples": [
      { "startTimeInSeconds": 1755851640,
        "heartRate": 141, "powerInWatts": 122 },
      { "startTimeInSeconds": 1755851641,
        "heartRate": 143, "powerInWatts": 268 }
    ]
}]}
```

Second payload note (chart caption): **Ten minutes of per-second power and cadence vs the single "average power" number in the summary.** The intervals exist only in the sample series.

### Visualization (canvas `powerCadenceCanvas`, responsive width × 380)

Dual-axis line chart: 600 seconds (10 minutes) of 1 Hz power (left axis) and cadence (right axis) with four shaded work-interval bands and a dashed summary-average-power reference line.

- **Data generation (deterministic, exact formulas):** LCG pseudo-random with seed 20260822: `seed = (seed·1103515245 + 12345) & 0x7fffffff; rnd = seed / 0x7fffffff`. Four hard-effort blocks in seconds: [60,150], [200,290], [340,430], [480,570]. Power per second t (0–600): base 300 W inside a block, 120 W outside; ramp factor `0.55 + 0.075·(t − blockStart)` for the first 6 s of each block; additive noise `(rnd−0.5)·46` inside blocks, `(rnd−0.5)·26` outside; clamped to [0, 400]. Cadence: smoothed toward target 93 rpm inside blocks / 72 rpm outside via `cad += (target − cad)·0.10 + (rnd−0.5)·1.1`, starting at 74, clamped to [40, 110]. Average power = mean of the power series.
- **Shaded bands:** the four block ranges filled `rgba(26,82,118,0.08)` full plot height.
- **Series styling:** power trace `#1a5276` width 1.1 (1 Hz); cadence trace `#e67e22` width 1.8 on the secondary (right) axis; summary average power as dashed `#e67e22` horizontal line (dash 4/4, width 1.5) labeled "summary average power (N W)" in orange 10.5px just above the line (N = rounded computed average).
- **Axes:** left y (power) 0–400 W, ticks every 50 in `#555`; right y (cadence) 40–110 rpm, ticks every 10 in `#e67e22`; x 0–600 s, ticks/gridlines every 60 in `#555`; gridlines `#e8e8e8`; axis lines `#2c3e50` on left, bottom, and right. Axis titles: "Elapsed time within activity (seconds)" (bottom center, `#2c3e50`); rotated left "Power (watts)" in `#1a5276`; rotated right "Cadence (rpm)" in `#e67e22`. Padding: left 56, right 56, top 52, bottom 68.
- **Title (bold 13px, `#1a5276`, left-aligned):** "1 Hz power and cadence from a FIT / activity-details sample series". Subtitle (11px, `#888`): "Shaded bands mark the four work intervals".
- **Legend (bottom left):** line samples with labels in `#555`: `#1a5276` solid (width 1.1) "power, 1 Hz samples"; `#e67e22` solid (width 1.8) "cadence, right axis"; `#e67e22` dashed (dash 4/4) "activity summary average".
- **Behavior:** redraws on window resize; width follows container.

## Official API References

- [Garmin Health API](https://developer.garmin.com/gc-developer-program/health-api/) — push-based wellness data: dailies, epochs, sleeps, stress, pulse ox, HRV, backfill
- [Polar AccessLink API](https://www.polar.com/accesslink-api/) — v3 REST reference: OAuth 2.0, transactions, exercises, samples, daily activity, nightly recharge

## Regeneration instructions

- **Layout:** single-page detail doc: h1, `.subtitle` paragraph, `.verified` badge, then `## Overview` (h2 with 2px `#2980b9` bottom border) containing one `.obj-table` (full width, one `<tr>`: left `<td>` 45% with `.section-label` headings + bullet lists + `.key-point` callout; right `<td>` 55% with `.payload-note` paragraphs, `<pre>` JSON payload, and the canvas), then `## Official API References` as a plain `<ul>` of external links.
- **Page CSS:** body system sans-serif (-apple-system stack), line-height 1.6, text `#2c3e50`, white background, padding 30px 40px; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; `.verified` badge inline-block, background `#eaf2f8`, border `1px solid #2980b9`, text `#1a5276`, padding 2px 10px, radius 4px, 0.8em; h2 1.3em `#1a5276` with `border-bottom: 2px solid #2980b9`; `.section-label` bold `#1a5276` block; `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 10px 14px, 0.93em; `.payload-note` 0.85em `#555`; `pre` background `#f4f4f4`, padding 14px, radius 6px, 0.82em; `code` background `#f4f4f4`; links `#1a5276`; li/p 0.93em. No nav bar, no back/home links.
- **Canvas:** `width: 100%` via CSS, height attribute 380; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates), redraw on resize.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`, band fill `rgba(26,82,118,0.08)`; grid `#e8e8e8`, axis `#2c3e50`, muted text `#555`/`#888`.
- In regenerated HTML, any card links use `.html` extensions (this page has only external links).
