# Strava API

**Page type:** detail page (two-column obj-table layout: descriptive text left 45%, payload + canvas right 55%, two rows)
**HTML title tag:** Strava API

**Subtitle:** Activities, GPS streams, heart rate, power data, and segment efforts — the dominant platform for endurance sport tracking.

## What it provides

- Activity summaries (distance, time, elevation); calories appear on the detailed activity, not the summary
- GPS streams (per-second lat/lng/altitude/heartrate/cadence/power/temperature)
- Segment efforts (partly subscription-gated; leaderboards were removed from the public API in 2020)
- Athlete stats and profile data
- Clubs and club activities
- Routes (polyline, elevation, estimated moving time)

## Authentication

OAuth 2.0 with refresh tokens. Requires user authorization. Scopes: `read`, `read_all`, `activity:read`, `activity:read_all`, `activity:write`.

## Granularity

GPS streams are per-second (or per-recording-interval of the device, typically 1s). Activity summaries are per-activity. Segment efforts are per-attempt.

## Rate limit

Overall default: 200 requests per 15 minutes and 2,000 per day, plus a stricter read limit of 100 requests per 15 minutes and 1,000 per day. Both per application. Rate limit headers returned with every response.

## Payload (row 1, right column)

Monospace `.payload` block, verbatim:

```
// ── illustrative payload; field names from Strava Streams API docs, values are not real ──
// GET /api/v3/activities/{id}/streams?keys=latlng,altitude,heartrate&key_by_type=true
{
  "latlng": {
    "data": [
      [37.7749, -122.4194],
      [37.7750, -122.4192],
      [37.7752, -122.4189],
      [37.7755, -122.4185]
    ],
    "series_type": "distance",
    "resolution": "high"
  },
  "altitude": {
    "data": [12.4, 13.1, 14.8, 17.2],
    "series_type": "distance",
    "resolution": "high"
  },
  "heartrate": {
    "data": [142, 145, 148, 152],
    "series_type": "distance",
    "resolution": "high"
  },
  "distance": {
    "data": [0.0, 12.3, 24.8, 38.1],
    "series_type": "distance",
    "resolution": "high"
  }
}
```

### Visualization (canvas `c1`, 720×360)

Filled area chart: elevation profile of a hilly 15 km route.

- **Layout:** margins left 60, right 30, top 30, bottom 45.
- **Data (30 elevation points, meters, evenly spaced 0–15 km):** `[50, 65, 95, 140, 185, 230, 260, 278, 270, 250, 220, 185, 155, 130, 120, 125, 145, 175, 215, 260, 300, 330, 345, 350, 340, 310, 265, 210, 145, 80]`; y scale max 400 m.
- **Grid:** horizontal lines every 100 m (0–400) and vertical lines every 3 km (0–15), `#eee`, width 1.
- **Axes:** L-shaped left+bottom axes in `#1a5276`, width 1.5. Y labels "0m"–"400m" every 100 m, right-aligned, 11px `#666`; X labels "0 km"–"15 km" every 3 km, centered, 11px `#666`.
- **Axis titles:** rotated "Elevation (m)" at x=15 (12px `#1a5276`); "Distance (km)" centered below the x-axis (12px `#1a5276`).
- **Area:** polygon under the line closed to the baseline, fill `rgba(26,82,118,0.35)`; line on top in `#1a5276`, width 2.
- **Summit marker:** red dot (`#e74c3c`, radius 5) at data index 23 (value 350 m), with bold 11px red label "Summit: 350m" centered 12px above the dot.

**Caption (centered, 0.82em, `#666`):** Illustrative: Elevation profile from GPS stream

## Business scenarios

- Training load analytics and periodization tools
- Route recommendation engines
- Real estate (popular running routes increase property value claims)
- City planning (cycling infrastructure usage patterns)
- Insurance (activity verification for wellness programs)

## Restrictions

Strava enforces display guidelines — you cannot store or redistribute GPS data beyond what is needed for your integration. Bulk export of other users' data is prohibited. Heatmap data is aggregated and not available per-user via API. Free tier athletes have limited segment access. Privacy zones trim or hide streams and maps near users' homes, so GPS-based use cases see truncated traces.

## API changes

From October 2018 to October 2019, Strava overhauled API scopes and forced all apps to re-authorize users under the new, narrower scopes. In 2020 the segment leaderboards endpoint was removed from the public API. In November 2024, new API agreement terms prohibited displaying a user's data to other users and restricted use of Strava data for AI training — breaking many third-party apps.

**Key-point callout:** The 2018–2019 scope overhaul and the November 2024 agreement changes each broke many third-party training dashboards. Any integration must account for the possibility of further scope reduction.

(Row 2's right `<td>` is empty.)

## Official API References

- [Strava Developers](https://developers.strava.com/) — developer portal with getting-started, authentication, and rate limit docs
- [Strava API v3 Reference](https://developers.strava.com/docs/reference/) — endpoint reference for activities, streams, segments, and athletes

## Regeneration instructions

- **Layout:** platform-API detail page. h1 + `.subtitle`, then a `.obj-table` with two `<tr>` rows. Row 1: left `<td>` (45%) with "What it provides" (ul), Authentication, Granularity, Rate limit; right `<td>` (55%) with `.payload` block, canvas `c1`, and centered caption paragraph. Row 2: left `<td>` with Business scenarios (ul), Restrictions, API changes, and a `.key-point` callout; right `<td>` empty. After the table, `<h2>Official API References</h2>` with a `<ul>` of links.
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em, margin-bottom 24px. `ul` padding-left 20px; `li` 0.93em, margin-bottom 6px. Sub-headings after the first use inline `style="margin-top: 16px;"`. No nav bar, no back/home links.
- **Table style:** `.obj-table td` vertical-align top, padding 16px, border `1px solid #2980b9`; `.obj-title` bold `#1a5276` 1.1em, margin-bottom 8px.
- **Payload block:** `.payload` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px, ui-monospace/Menlo 0.78em, `white-space: pre`, `overflow-x: auto`, line-height 1.45.
- **Callout:** `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, margin 12px 0, 0.93em.
- **Links:** `a { color: #1a5276; }`. In regenerated HTML, any card/page links use `.html` extensions.
- **Canvas:** `display: block; margin: 0 auto`; intrinsic 720×360; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setupCanvas(id, w, h)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar/area fill `rgba(26,82,118,0.35)`.
