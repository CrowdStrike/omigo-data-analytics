# Android Health Connect

**Page type:** detail page (two-column obj-table layout: text left 45%, payload/canvas right 55%, one row for the Overview section)
**HTML title tag:** Android Health Connect — Platform APIs

**Subtitle:** Lets an Android app read a user's health and fitness data — steps, heart rate, sleep — from their own phone, with their permission.

**Verified badge:** Last verified: August 2026

## Overview

### What you can get

- Steps, distance, and calories over time intervals
- Heart rate, blood pressure, glucose, and other vitals
- Sleep sessions, with stages when the writing app provides them
- Workouts with laps and routes
- Every record tagged with which app wrote it and how — sensor, session, or typed in by hand

### Key point (callout)

**Health Connect does not de-duplicate across apps.** If a phone pedometer, a wearable's app, and a fitness tracker all record the same walk, you read three overlapping step records, and a naive sum triple-counts. Reconciling by source is your job — and the inflation is worst for the users with the most devices, so it is not a harmless constant offset.

### Watch out for

- There is no server API. Only an app on the user's device can read, and the old Google Fit cloud API that once allowed server-side pulls is gone with no replacement.
- A denied permission returns empty results, indistinguishable from "no data". Refusal, non-wear, and a true zero all look the same.
- A fresh install sees only about the last 30 days unless the user grants an extra history permission.
- Background sync is at the mercy of battery managers, which vary wildly by phone maker. Data gaps of days are normal, and uninstall ends the series permanently.

### Payload example (right column)

Payload note: **Two overlapping step records for the same walk** — read on-device by your own app; no HTTP endpoint returns this. Same afternoon, two writers, nearly the same count. Summing them double-counts.

```json
[
  { "recordType": "StepsRecord", "count": 3120,
    "startTime": "2026-08-21T14:00:00Z",
    "endTime":   "2026-08-21T14:38:00Z",
    "metadata": {
      "dataOrigin": { "packageName": "com.fitbit.FitbitMobile" },
      "recordingMethod": "AUTOMATICALLY_RECORDED",
      "device": { "type": "DEVICE_TYPE_WATCH" } } },

  { "recordType": "StepsRecord", "count": 2984,
    "startTime": "2026-08-21T14:02:00Z",
    "endTime":   "2026-08-21T14:37:00Z",
    "metadata": {
      "dataOrigin": { "packageName": "com.google.android.apps.fitness" },
      "recordingMethod": "AUTOMATICALLY_RECORDED",
      "device": { "type": "DEVICE_TYPE_PHONE" } } }
]
```

### Visualization (canvas `stepsByOrigin`, responsive width × 380)

Stacked bar chart of daily step totals by data origin across one week, overlaid with a dashed de-duplicated total line.

- **X categories:** Mon, Tue, Wed, Thu, Fri, Sat, Sun.
- **Stacked series (bottom to top):**
  - `com.google.android.apps.fitness`, color `#1a5276`: `[7100, 6400, 8200, 5900, 7800, 11200, 4300]`
  - `com.fitbit.FitbitMobile`, color `#27ae60`: `[6800, 6100, 7900, 5600, 7400, 10800, 4100]`
  - `phone pedometer`, color `#e67e22`: `[3200, 2900, 4100, 2400, 3600, 5200, 1800]`
- **De-duplicated line** (one origin chosen per non-overlapping window), dashed red `#e74c3c` (dash 6/4, width 2) with 3px-radius red dots: `[7400, 6600, 8500, 6100, 8000, 11600, 4500]`.
- **Y axis:** 0 to 30,000 with gridlines/labels at 0, 5000, 10000, 15000, 20000, 25000, 30000 formatted as "0k"…"30k"; rotated y-axis title "steps per day" in `#2c3e50`. Gridlines `#e8e8e8`; axes `#2c3e50`. Naive stacked total labeled above each bar in `#888` (e.g. "17.1k" style k-format of the sum).
- **Padding:** top 58, right 20, bottom 92, left 58. Bar width min(46px, 52% of slot).
- **Title (bold 13px, `#1a5276`, centered):** "Daily step total by data origin — naive sum vs de-duplicated". Subtitle (italic 10.5px, `#888`, centered): "Health Connect does not reconcile overlapping records; the consumer must".
- **Annotation on the Saturday column** (index 5), drawn only if it fits inside the plot: a red `#e74c3c` vertical bracket (with end ticks) from the naive stacked top (27,200) down to the de-duplicated value (11,600), with bold red label "double counted" and italic gray `#888` sub-label "same walk, three writers" to its right.
- **Legend (bottom left, wraps to a second row if needed):** color swatches for the three package-name series plus a dashed red line entry "de-duplicated total (one origin per window)"; labels in `#555`.
- **Behavior:** redraws on window resize; width follows container.

## Official API References

- [Health Connect Developer Guide](https://developer.android.com/health-and-fitness/guides/health-connect) — data model, permissions, reading and writing records, differential sync
- [HealthConnectClient Reference](https://developer.android.com/reference/androidx/health/connect/client/HealthConnectClient) — the Jetpack client API surface, including `readRecords`, `aggregate`, and changes tokens

## Regeneration instructions

- **Layout:** single-page detail doc: h1, `.subtitle` paragraph, `.verified` badge, then `## Overview` (h2 with 2px `#2980b9` bottom border) containing one `.obj-table` (full width, one `<tr>`: left `<td>` 45% with `.section-label` headings + bullet lists + `.key-point` callout; right `<td>` 55% with `.payload-note` paragraph, `<pre>` JSON payload, and the canvas), then `## Official API References` as a plain `<ul>` of external links.
- **Page CSS:** body system sans-serif (-apple-system stack), line-height 1.6, text `#2c3e50`, white background, padding 30px 40px; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; `.verified` badge inline-block, background `#eaf2f8`, border `1px solid #2980b9`, text `#1a5276`, padding 2px 10px, radius 4px, 0.8em; h2 1.3em `#1a5276` with `border-bottom: 2px solid #2980b9`; `.section-label` bold `#1a5276` block; `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 10px 14px, 0.93em; `.payload-note` 0.85em `#555`; `pre` background `#f4f4f4`, padding 14px, radius 6px, 0.82em; `code` background `#f4f4f4`; links `#1a5276`; li/p 0.93em. No nav bar, no back/home links.
- **Canvas:** `width: 100%` via CSS, height attribute 380; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates), redraw on resize.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`; grid `#e8e8e8`, axis `#2c3e50`, muted text `#555`/`#888`.
- In regenerated HTML, any card links use `.html` extensions (this page has only external links).
