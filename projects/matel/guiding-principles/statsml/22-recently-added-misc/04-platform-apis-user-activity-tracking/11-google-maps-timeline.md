# Google Maps Timeline

**Page type:** detail page (two-column obj-table layout: descriptive text left 45%, payload + canvas right 55%, one row)
**HTML title tag:** Google Maps Timeline

**Subtitle:** Location visits, travel routes, dwell time, and place categories — a continuous record of where you have been, now transitioning to on-device storage.

## What It Provides

Place visits (with address, place name, category, coordinates), activity segments (travel between places with mode: walking/driving/cycling/transit), visit duration (arrival/departure timestamps), place confidence scores, semantic labels (home, work, gym, grocery store).

## Authentication

No API keys or OAuth — data comes via export only. Pre-migration: Google Takeout (JSON/KML). Post-migration: export initiated from the Maps app on the device (Settings → export). No official public API for Timeline data.

## Granularity

Visit-level, not raw GPS. Google infers discrete "place visits" from raw location signals. You see "arrived at Whole Foods at 5:42pm, left at 6:15pm" — not the continuous GPS trace. Travel segments show mode and route but not second-by-second position.

## Major Transition (2024–2025)

**Key-point callout:** Google moved Timeline data (2024) from cloud storage to on-device, with an optional end-to-end-encrypted cloud backup (not server-readable or API-accessible). After the migration: no web-based Timeline viewer (Maps desktop), and no Timeline data in web Takeout for migrated users — export must be initiated from the Maps app on the device. This dramatically reduces programmatic access.

## Business Scenarios

Travel pattern analysis, commute optimization, retail foot traffic studies, real estate location scoring, alibi verification, routine detection for elder care.

## Restrictions

No official API — only device-initiated export (JSON). On-device migration means historical data may be auto-deleted if not exported before the migration deadline; the default auto-delete window is now 3 months. Place inference is probabilistic (confidence scores range 0–100). Requires Location History (now "Timeline") to be enabled — it has always been opt-in and off by default; recent changes are the rename, the 2024 on-device migration, and the shorter default auto-delete.

## Payload Example

Monospace `.payload` block (right column), verbatim:

```
// ── illustrative payload; field names from the post-migration on-device export (Timeline.json), values are not real ──
// Maps app → Settings → Location → export Timeline data
{
  "semanticSegments": [{
    "startTime": "2026-08-22T17:42:00.000Z",
    "endTime": "2026-08-22T18:15:00.000Z",
    "visit": {
      "hierarchyLevel": 0,
      "probability": 0.82,
      "topCandidate": {
        "placeId": "ChIJIQBpAG2ahYAR_6128GcTUEo",
        "semanticType": "INFERRED_PLACE",
        "probability": 0.72,
        "placeLocation": {
          "latLng": "37.7490000°, -122.4194000°"
        }
      }
    }
  }]
}
// Legacy pre-migration Takeout used monthly "Semantic Location History"
// files with placeVisit / latitudeE7 fields instead.
```

## Dwell Time Distribution

### Visualization (canvas `dwellChart`, 720×380)

Horizontal bar chart: average daily minutes by place category.

- **Layout:** margins left 90, right 60, top 30, bottom 40; bar height 32, gap 10; x scale max 560 minutes.
- **Title (bold 13px, `#1a5276`, left-aligned at x=marginLeft, y=18):** "Average Daily Minutes by Place Category".
- **Data (label, minutes, bar color):**
  - Home — 540 — `#1a5276`
  - Work — 480 — `#1a5276`
  - Commute — 65 — `#e67e22`
  - Gym — 55 — `#27ae60`
  - Restaurant — 40 — `#27ae60`
  - Grocery — 25 — `#27ae60`
  - Shopping — 20 — `#27ae60`
  - Other — 35 — `rgba(26,82,118,0.35)`
- **Labels:** category name right-aligned left of each bar (12px `#2c3e50`); value label "540 min" etc. 8px right of each bar end (11px `#2c3e50`).
- **X-axis:** thin `#bbb` baseline below the bars, ticks every 120 from 0 to 540 with numeric labels (10px `#666`), and axis label "Minutes per day" centered below (11px `#666`).

**Caption (centered, 0.82em, `#666`):** Illustrative: Average daily dwell time by place category

## Official API References

- [Google Takeout](https://takeout.google.com/) — legacy export path; post-migration, Timeline data is not in web Takeout — export is initiated from the Maps app on the device (Settings → export). No public Timeline API
- [Google Maps Timeline Help](https://support.google.com/maps/answer/6258979) — official documentation on how Timeline works, storage, and export

## Regeneration instructions

- **Layout:** platform-API detail page. h1 + `.subtitle`, then a single `.obj-table` with one `<tr>`: left `<td>` (45%) holds `.obj-title` div headings ("What It Provides", "Authentication", "Granularity", "Major Transition (2024–2025)", "Business Scenarios", "Restrictions") with inline `style="margin-top: 18px;"` after the first, paragraphs and a `.key-point` callout; right `<td>` (55%) holds "Payload Example" title, `.payload` block, "Dwell Time Distribution" title, canvas, and centered caption paragraph. After the table, `<h2>Official API References</h2>` with a `<ul>` of links.
- **Page style:** `* { box-sizing: border-box; margin: 0; padding: 0; }`; body system sans-serif (-apple-system stack), white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`, margin-bottom 4px; `.subtitle` `#666` 1.05em, margin-bottom 24px. No nav bar, no back/home links.
- **Table style:** `.obj-table` full width, border-collapse; td vertical-align top, padding 16px, border `1px solid #2980b9`; `.obj-title` bold `#1a5276` 1.1em, margin-bottom 8px.
- **Payload block:** `.payload` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px, ui-monospace/Menlo 0.78em, `white-space: pre`, `overflow-x: auto`, line-height 1.45, margin 12px 0.
- **Callout:** `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, margin 12px 0, 0.93em.
- **Links:** `a { color: #1a5276; }`. In regenerated HTML, any card/page links use `.html` extensions.
- **Canvas:** `display: block; margin: 0 auto`; intrinsic 720×380; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setupCanvas(id, w, h)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`.
