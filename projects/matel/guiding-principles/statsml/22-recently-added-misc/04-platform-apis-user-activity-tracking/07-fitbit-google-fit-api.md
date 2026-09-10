# Fitbit / Google Fit API

**Page type:** detail page (two-column obj-table layout: one single table row — all text sections stacked in the left 45% cell, payload + canvas in the right 55% cell)
**HTML title tag:** Fitbit / Google Fit API

**Subtitle:** Steps, heart rate (1-sec intraday), sleep stages, SpO2 — personal health telemetry accessible via OAuth2 APIs.

## What it provides

- Steps at 1-minute granularity
- Heart rate at 1-second resolution (intraday, personal apps only)
- Sleep stages — wake, light, deep, REM — with start/end timestamps per episode
- SpO2 nightly average (requires specific device models)
- Active Zone Minutes — earned only in Fat Burn, Cardio, and Peak heart-rate zones ("Out of Range" earns none)
- Calories burned, distance traveled

## Authentication

OAuth 2.0 with explicit user consent. Fitbit uses its own OAuth server. Google Health Connect uses an on-device API with no cloud round-trip for Android apps — data stays local unless the user grants a specific app access.

## Granularity

- Heart rate: 1-second resolution (intraday endpoint)
- Steps: 1-minute intervals
- Sleep stages: recorded at 30-second granularity, aggregated into stage episodes of variable length (episodes are often just a few minutes)

## Rate limit

150 requests per hour per user (Fitbit Web API).

## Business scenarios

- Insurance wellness programs offering premium discounts for step targets
- Corporate health challenges tracking team aggregate activity
- Clinical trial compliance monitoring (did the participant wear the device?)
- Sleep quality correlation with workplace productivity metrics

## Restrictions

**Key point (callout):** Intraday data (1-sec HR, 1-min steps) is available by default only to "Personal" app types on Fitbit; other app types must go through Fitbit's intraday access-request and approval process. Nightly SpO2 requires supported hardware (Sense/Sense 2, Versa 2/3/4, Charge 4/5+, Luxe, Inspire 3, Ionic, among others) — the supported list grows over time.

## Migration note

The Google Fit APIs were deprecated in May 2024 and sunset on June 30, 2025 — any Google Fit content here is historical, with Health Connect as the designated successor. The Fitbit Web API remains live but is being phased toward Health Connect: existing Fitbit apps continue working, while new integrations are steered to Health Connect, which also carries record types the Fitbit Web API never exposed, such as blood glucose. Cloud-to-cloud access is narrowing — the direction is on-device first, cloud export second.

## Right column: payload block

**Payload note (italic, small gray, above the pre):** Sample payload — documented Fitbit Web API response structure.

```
// ── illustrative payload; field names from Fitbit Web API docs, values are not real ──
// GET /1/user/-/activities/heart/date/today/1d/1sec.json
{
  "activities-heart-intraday": {
    "dataset": [
      { "time": "07:14:00", "value": 62 },
      { "time": "07:14:01", "value": 63 },
      { "time": "07:14:02", "value": 61 },
      { "time": "07:14:03", "value": 64 },
      { "time": "07:14:04", "value": 68 }
    ],
    "datasetInterval": 1,
    "datasetType": "second"
  },
  "activities-heart": [{
    "dateTime": "2026-08-22",
    "value": {
      "heartRateZones": [
        { "name": "Out of Range", "min": 30, "max": 91, "minutes": 820 },
        { "name": "Fat Burn", "min": 91, "max": 127, "minutes": 45 },
        { "name": "Cardio", "min": 127, "max": 154, "minutes": 22 },
        { "name": "Peak", "min": 154, "max": 220, "minutes": 8 }
      ],
      "restingHeartRate": 58
    }
  }]
}
```

**Chart caption (italic, small gray, above the canvas):** Illustrative: Heart rate intraday with zone bands

### Visualization (canvas `hrChart`, 720×360)

Line chart of heart rate over 24 hours with tinted background zone bands.

- **Zone bands (full plot width, translucent fills; zone name labels in `#666` 10px at each band's vertical center, left-aligned just inside the plot):**
  - Out of Range: 40–91 bpm, `rgba(52,152,219,0.12)`
  - Fat Burn: 91–127 bpm, `rgba(39,174,96,0.12)`
  - Cardio: 127–154 bpm, `rgba(230,126,34,0.12)`
  - Peak: 154–180 bpm, `rgba(231,76,60,0.12)`
- **Zone threshold lines:** dashed (dash 4/4, width 0.8) horizontal lines at 91 (`rgba(39,174,96,0.5)`), 127 (`rgba(230,126,34,0.5)`), 154 (`rgba(231,76,60,0.5)`).
- **Axes:** y from 40 to 180 bpm, labels at 40/60/80/100/120/140/160/180 (`#555` 11px, right-aligned) with `#eee` grid lines; x from 0 to 24 hours, labels "0:00" through "24:00" every 3 hours (`#555`, centered) with `#eee` vertical grid lines; L-shaped axes `#bbb`. Axis titles in `#333` 12px: "Time of Day" bottom center, "BPM" rotated vertical left. Padding: top 30, right 20, bottom 40, left 50.
- **Data (one point per hour, hour → bpm):** 0→56, 1→54, 2→52, 3→51, 4→53, 5→55, 6→62, 7→145, 8→108, 9→78, 10→74, 11→72, 12→76, 13→73, 14→71, 15→74, 16→82, 17→158, 18→118, 19→79, 20→72, 21→68, 22→63, 23→58.
- **Series:** line `#1a5276` 2.2px, round joins, with 3px-radius filled dots at every point in `#1a5276`.

## Official API References

- [Fitbit Web API Reference](https://dev.fitbit.com/build/reference/web-api/) — official documentation for OAuth 2.0, intraday endpoints, and rate limits
- [Android Health Connect](https://developer.android.com/health-and-fitness/guides/health-connect) — Google's official on-device health data API, the successor to the deprecated Google Fit APIs

## Regeneration instructions

- **Layout:** detail page with `.obj-table`: full-width `border-collapse: collapse` table containing a SINGLE `<tr>`. Left `<td>` (45%) stacks multiple `.obj-title` headings (What it provides / Authentication / Granularity / Rate limit / Business scenarios / Restrictions / Migration note), each followed by a `<ul>` or a 0.93em `<p>`; the Restrictions section body is a `.key-point` callout. Later `.obj-title`s carry `margin-top: 18px`. Right `<td>` (55%, no text-align:center on this page) holds a `.payload-note`, the `.payload` pre, an italic gray chart caption `<p>`, and the canvas. After the table, an `<h2>Official API References</h2>` with a plain `<ul>` of links.
- **Page style:** body system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold `#1a5276` 1.1em; `li` 0.93em; links `#1a5276`. `.payload-note` 0.82em `#666` italic. No nav bar, no back/home links.
- **Callout style:** `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em. `.payload` — same background/border, monospace (ui-monospace/Menlo) 0.78em, `white-space: pre`.
- **Canvas:** `display: block; margin: 0 auto`; explicit `width="720" height="360"` attributes; `setupCanvas(id, w, h)` sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`), fixes CSS width/height, and calls `ctx.scale` so drawing stays in logical coordinates.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, light blue `#3498db` (zone tint), gray text `#555`/`#666`/`#333`.
- In regenerated HTML, any card/nav links use `.html` extensions (this page has none; external doc links stay as-is).
