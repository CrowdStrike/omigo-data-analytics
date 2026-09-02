# Apple HealthKit

**Page type:** detail page (two-column obj-table layout: text left 45%, payload/canvas right 55%, one row for the Overview section)
**HTML title tag:** Apple HealthKit — Platform APIs

**Subtitle:** Lets an iPhone app read a user's health and fitness data — heart rate, steps, sleep, workouts — from their own device, with their permission.

**Verified badge:** Last verified: August 2026

## Overview

### What you can get

- Heart rate, steps, distance, calories, and other body measurements
- Sleep sessions, broken into stages on newer devices
- Workouts, routes, and Apple Watch ECG recordings
- Data written by other apps and devices, each sample tagged with its source

### Key point (callout)

**There is no server API.** No backend can pull a user's health data — reads happen only from an app running on that user's iPhone or Watch, with permission granted per data type. Ingestion happens only when your app runs, stops permanently on uninstall, and gaps are the normal case, not an incident.

### Watch out for

- A denied read permission does not error — it returns empty results, indistinguishable from "this user has no data". You cannot tell refusal from non-wear from a true zero.
- The phone, a Watch, and third-party apps can all record the same walk. Summing raw samples double-counts — and inflates most for users with the most devices.
- The same "heart rate" can come from a chest strap, an optical wrist sensor, or a typed-in entry. These are different measurements sharing one name — check the source metadata before pooling them.
- Sleep stages only exist from iOS 16 / watchOS 9 onward and only from apps that record them. Older or third-party data shows just "asleep" — a discontinuity that looks like a change in the user's sleep.

### Payload example (right column)

Payload note: **App-side serialization** — no API returns this; it is how your own app would flatten a sample it read on-device before uploading it to your backend. Note the source and device metadata.

```json
{
  "type": "HKQuantityTypeIdentifierHeartRate",
  "value": 58,
  "unit": "count/min",
  "startDate": "2026-08-21T02:14:30Z",
  "sourceRevision": {
    "source": { "name": "Apple Watch" },
    "productType": "Watch7,4",
    "version": "11.2"
  },
  "device": { "model": "Watch",
              "hardwareVersion": "Watch7,4" },
  "metadata": { "HKMetadataKeyHeartRateMotionContext": 1 }
}
```

Second payload note (chart caption): **Single-night hypnogram from `sleepAnalysis` samples** — each block is one interval sample, inferred from wrist sensors rather than a sleep lab.

### Visualization (canvas `hypnogram`, responsive width × 380)

Hypnogram: horizontal stage-band chart of one night's sleep-stage interval segments, 23:00 to 07:00 (480 minutes total).

- **Stage rows (top to bottom):** Awake, REM, Core, Deep. Stage colors: Awake `#e74c3c`, REM `#e67e22`, Core `#1a5276`, Deep `#27ae60`.
- **Segments** as `[startMin, endMin, stage]` measured in minutes from 23:00: `[0,18,Awake]`, `[18,45,Core]`, `[45,75,Deep]`, `[75,95,Core]`, `[95,108,REM]`, `[108,112,Awake]`, `[112,140,Core]`, `[140,170,Deep]`, `[170,190,Core]`, `[190,215,REM]`, `[215,220,Awake]`, `[220,250,Core]`, `[250,268,Deep]`, `[268,295,Core]`, `[295,325,REM]`, `[325,330,Awake]`, `[330,365,Core]`, `[365,378,Deep]`, `[378,405,Core]`, `[405,440,REM]`, `[440,448,Awake]`, `[448,466,Core]`, `[466,480,Awake]`.
- **Rendering:** each segment drawn as a filled block (height min(20px, 52% of row height)) centered on its stage row; consecutive segments joined by vertical step connectors in `rgba(26,82,118,0.35)`, width 1.5.
- **Axes:** left/bottom axes in `#2c3e50`; horizontal band gridlines and hourly vertical gridlines in `#e8e8e8`; hourly x labels as clock times ("23:00", "00:00", … "07:00") in `#555`; stage labels right-aligned left of the axis, each colored with its stage color. X-axis caption "clock time" in `#2c3e50`.
- **Padding:** top 58, right 18, bottom 76, left 62.
- **Title (bold 13px, `#1a5276`, centered):** "Sleep stage segments for one night, as stored in HealthKit". Subtitle (italic 10.5px, `#888`, centered): "inferred stages, not polysomnography — availability depends on OS version and writing app".
- **Legend (bottom, centered):** color swatches with stage names Awake / REM / Core / Deep, labels in `#555`.
- **Behavior:** redraws on window resize; width follows container.

## Official API References

- [HealthKit Framework Documentation](https://developer.apple.com/documentation/healthkit) — the full API reference: sample types, queries, and authorization
- [Health and Fitness — Apple Developer](https://developer.apple.com/health-fitness/) — the platform overview page for HealthKit, workouts, and related frameworks

## Regeneration instructions

- **Layout:** single-page detail doc: h1, `.subtitle` paragraph, `.verified` badge, then `## Overview` (h2 with 2px `#2980b9` bottom border) containing one `.obj-table` (full width, one `<tr>`: left `<td>` 45% with `.section-label` headings + bullet lists + `.key-point` callout; right `<td>` 55% with `.payload-note` paragraphs, `<pre>` JSON payload, and the canvas), then `## Official API References` as a plain `<ul>` of external links.
- **Page CSS:** body system sans-serif (-apple-system stack), line-height 1.6, text `#2c3e50`, white background, padding 30px 40px; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; `.verified` badge inline-block, background `#eaf2f8`, border `1px solid #2980b9`, text `#1a5276`, padding 2px 10px, radius 4px, 0.8em; h2 1.3em `#1a5276` with `border-bottom: 2px solid #2980b9`; `.section-label` bold `#1a5276` block; `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 10px 14px, 0.93em; `.payload-note` 0.85em `#555`; `pre` background `#f4f4f4`, padding 14px, radius 6px, 0.82em; `code` background `#f4f4f4`; links `#1a5276`; li/p 0.93em. No nav bar, no back/home links.
- **Canvas:** `width: 100%` via CSS, height attribute 380; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates), redraw on resize.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`; grid `#e8e8e8`, axis `#2c3e50`, muted text `#555`/`#888`.
- In regenerated HTML, any card links use `.html` extensions (this page has only external links).
