# Personal Location History

**Page type:** detail page (two-column obj-table layout: text left 45%, code + canvas right 55%, one Overview row)
**HTML title tag:** Personal Location History — Platform APIs

**Subtitle:** A negative finding: no API on either platform returns where a person has been — Google moved location history onto the phone, and Apple never offered it at all.

**Verified badge:** Last verified: August 2026

## Overview

Left column:

**What you can get**

- Nothing queryable: there is no endpoint, and no permission to request, for a person's historical location trace
- Live position only — from your own app, while it runs, with the user's permission
- A manual Google Takeout export that a person can choose to hand you, containing raw GPS points and Google's inferred visits
- Aggregate, place-level proxies: busyness in first-party apps, published mobility datasets

**Key point (callout):** **A mobility study has exactly three honest options.** Collect prospectively with your own app (no history, real attrition), ask participants to donate Takeout exports (small, self-selected, retrospective), or use aggregates (no individual trajectories at all). They are not interchangeable — a per-person question cannot be answered with aggregate data without strong unstated assumptions.

**Watch out for**

- Takeout files contain Google's guesses — place visits and travel modes with confidence scores, not ground truth — and the same raw points can be relabelled differently in a later export
- Gaps in GPS traces track behaviour (battery saver, indoors, airplane mode) — they are not random and can't be safely interpolated
- Location plus time re-identifies people even with names removed; a few visits pin home and workplace

Right column:

**Google Takeout — the only retrospective route** (section-head)

Code block (pre, verbatim):

```
{ "semanticSegments": [
  { "startTime": "2026-03-14T09:05:00Z",
    "endTime":   "2026-03-14T09:52:00Z",
    "visit": { "topCandidate": {
      "placeId": "ChIJ...",
      "semanticType": "INFERRED_WORK",
      "probability": 0.86 } } }
    // ^ a model's guess, not an observation
] }

// What does NOT exist, on either platform:
// GET /timeline/v1/history          <- no such API
// GET /significantLocations         <- no such API
// CLLocationManager.locationHistory <- no such call
```

**Availability by location data source** (section-head above canvas)

### Visualization (canvas `availChart`, responsive width × 380)

Horizontal reachability band chart: six location data sources, each with a bar extending to one of four reachability tiers with a colored terminal dot and a note string.

- **Title (bold 13px, `#1a5276`, top left):** "Individual location history: how reachable is each source?". Below it (italic 10px, `#666`): "Only one row reaches the green column - and it is live position, not history."
- **Tiers (state index, label, color, x-position as fraction of plot width):** 0 "never exposed" `#e74c3c` at 0.08; 1 "on-device only" `#e74c3c` at 0.30; 2 "user export only" `#e67e22` at 0.58; 3 "queryable via API" `#27ae60` at 0.86. Tier header labels 9px in tier color above the plot; light `#eee` vertical separators midway between tier positions.
- **Rows (label, state, note):**
  - Server-side Google Timeline API — 0 — "no such API; server-side Timeline no longer the system of record"
  - Apple Significant Locations — 0 — "on-device, end-to-end encrypted, never exposed to third parties"
  - Google Takeout location export — 2 — "manual, per-user, consent-driven; cannot be polled"
  - On-device Timeline (user’s phone) — 1 — "visible to the user in the app; not readable by third parties"
  - Live CoreLocation / fused location — 3 — "your app, while running, with permission - not a history"
  - Aggregate place busyness — 1 — "first-party UI aggregate; not a public API field"
- **Row rendering:** label 11px right-aligned (`#27ae60` for the state-3 row, otherwise `#2c3e50`); band bar fill `rgba(26,82,118,0.35)` (height ≤18px) from the left edge to the tier x; terminal filled circle in the tier color (alpha 0.9, thin `rgba(0,0,0,0.12)` stroke); note text 9px `#777` beside the marker (flips to the left side if too close to the right edge); odd rows zebra `rgba(26,82,118,0.04)`. Plot border `#ddd`. Padding: top 56, right 22, bottom 66, left min(200, 40% of width).
- **Axis caption (bold 11px, `#1a5276`, centered below plot):** "increasing programmatic reachability →".
- **Legend (bottom left, 11px squares in tier colors, labels `#666`, order green→orange→red→red):** "queryable via API", "user export only", "on-device only", "never exposed".
- **Conclusion callout (italic 10px, `#e74c3c`, bottom right):** "no row delivers a queryable individual history: plan for consented collection or aggregate proxies".

## Official API References

- [Core Location — Apple Developer Documentation](https://developer.apple.com/documentation/corelocation) — live-position framework; the closest thing iOS offers, and it is not a history
- [Google Takeout](https://takeout.google.com/) — the user-initiated export that is the only route to a retrospective location trace

## Regeneration instructions

- **Layout:** platform-apis detail page. h1, `.subtitle` paragraph, `.verified` badge span, then `h2` "Overview" followed by a full-width `.obj-table` with one `<tr>`: left `<td>` (45%) with `.section-head` headings ("What you can get", "Watch out for"), bullet lists, and one `.key-point` callout; right `<td>` (55%) with `.section-head` labels above a `<pre>` code block and the canvas. Then `h2` "Official API References" with a link list.
- **Page CSS:** body system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; `.verified` badge inline-block, background `#eaf2f8`, border `1px solid #e0e0e0`, text `#1a5276`, padding 2px 10px, radius 4px, 0.8em; h2 1.3em `#1a5276` with `2px solid #2980b9` bottom border; obj-table cells `1px solid #e0e0e0` border, padding 16px; `.section-head` bold `#1a5276` 0.95em; `pre` background `#f8f9fa`, left border `3px solid #1a5276`, monospace 0.78em, padding 16px, radius 4px; `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 10px 14px, 0.93em; list items 0.93em; links `#1a5276`. No nav bar, no back/home links.
- **Canvas:** `<canvas id="availChart" height="380">`, CSS `display:block; width:100%`; drawing script measures `getBoundingClientRect()`, scales backing store by `window.devicePixelRatio` via `setTransform(dpr,0,0,dpr,0,0)`, fixes CSS height to 380px, and redraws on window resize.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`, gray text `#666`/`#555`/`#777`.
