# Google Maps Platform

**Page type:** detail page (two-column obj-table layout: text left 45%, code + canvas right 55%, one Overview row)
**HTML title tag:** Google Maps Platform — Platform APIs

**Subtitle:** Google's pay-per-call services for looking up places, addresses, and routes — facts about places, never about people.

**Verified badge:** Last verified: August 2026

## Overview

Left column:

**What you can get**

- Details about businesses and places: location, opening hours, rating, and a few reviews
- Address-to-coordinates conversion, and coordinates back to addresses
- Driving, walking, and cycling routes with travel times, optionally traffic-aware
- Travel-time tables between many origins and destinations at once
- Modelled environment data: air quality, rooftop solar potential, pollen

**Key point (callout):** **The "how busy is this place" chart is not in the API.** The busyness histogram you see in Google Maps is a feature of Google's own app, not a field you can query — and there is no per-person location data at all. Anything claiming to supply busyness is scraped, which the terms forbid.

**Watch out for**

- Costs multiply fast: a travel-time table of 300 stores by 5,000 customers is 1.5 million billable lookups in one loop
- You mostly can't store what comes back — the terms force you to re-query (and re-pay) for every wave of a time series
- The handful of reviews returned is a non-random sample of the full set; sentiment computed from them isn't comparable across places
- Asking for one extra "nice to have" field can move every call into a costlier billing tier

Right column:

**Place Details — what you get, and what isn't there** (section-head)

Code block (pre, verbatim):

```
GET .../v1/places/ChIJN1t_tDeuEmsRUsoyG83frY4

{
  "displayName": { "text": "Example Cafe" },
  "location": { "latitude": -33.8670,
                "longitude": 151.1957 },
  "rating": 4.3,
  "userRatingCount": 1842,
  "reviews": [ /* ~5 selected reviews —
                  no paging to the other 1837 */ ]

  /* fields that do NOT exist:
       "popularTimes"   <- Google's own UI only
       "visitorCount"   <- never exposed
     Absence here is the whole finding. */
}
```

**Relative cost weight by SKU family** (section-head above canvas)

### Visualization (canvas `skuChart`, responsive width × 380)

Horizontal bar chart of illustrative relative cost weight per 1000 calls for eight SKU families.

- **Title (bold 13px, `#1a5276`, top left):** "Relative cost weight per 1000 calls, by SKU family".
- **Honesty banner (italic 10px, `#e74c3c`, below title):** "ILLUSTRATIVE RELATIVE MAGNITUDES - NOT ACTUAL PRICES. Ordering only; read the current SKU table before budgeting."
- **Data (label, weight, stroke color):**
  - Geocoding — 1.0 — `#1a5276`
  - Places Nearby Search — 2.2 — `#1a5276`
  - Place Details (basic fields) — 1.6 — `#27ae60`
  - Place Details (advanced fields) — 3.4 — `#e67e22`
  - Routes / Directions — 1.4 — `#1a5276`
  - Distance Matrix (per element) — 0.7 — `#e74c3c`
  - Air Quality — 2.6 — `#8e44ad`
  - Solar (building insight) — 4.0 — `#8e44ad`
- **Scale:** x from 0 to max 4.4, 5 vertical gridlines (`#e8e8e8`), tick labels "0.0x"…"4.4x" in `#555`; L-shaped axis in `#2c3e50` (width 1.5). Padding: top 52, right 26, bottom 62, left min(196, 40% of width).
- **Bars:** fill `rgba(26,82,118,0.35)`, height ≤22px, each stroked 1.5px in its row color; value label ("1.0x" etc.) bold 10px in row color to the right of the bar; row labels 11px `#2c3e50` right-aligned left of axis; odd rows get zebra band `rgba(26,82,118,0.04)`.
- **X-axis label (bold 11px, `#1a5276`, centered):** "relative cost weight (illustrative, not quoted prices)".
- **Bottom-right callout (italic 10px, `#e74c3c`):** "lowest unit weight, highest total: matrix cost = origins x destinations".

## Official API References

- [Google Maps Platform documentation](https://developers.google.com/maps/documentation) — hub for all Maps Platform products, billing model, and key restrictions
- [Places API](https://developers.google.com/maps/documentation/places/web-service) — Place Details, Nearby Search, Text Search, field masks

## Regeneration instructions

- **Layout:** platform-apis detail page. h1, `.subtitle` paragraph, `.verified` badge span, then `h2` "Overview" followed by a full-width `.obj-table` with one `<tr>`: left `<td>` (45%) with `.section-head` headings ("What you can get", "Watch out for"), bullet lists, and one `.key-point` callout; right `<td>` (55%) with `.section-head` labels above a `<pre>` code block and the canvas. Then `h2` "Official API References" with a link list.
- **Page CSS:** body system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; `.verified` badge inline-block, background `#eaf2f8`, border `1px solid #e0e0e0`, text `#1a5276`, padding 2px 10px, radius 4px, 0.8em; h2 1.3em `#1a5276` with `2px solid #2980b9` bottom border; obj-table cells `1px solid #e0e0e0` border, padding 16px; `.section-head` bold `#1a5276` 0.95em; `pre` background `#f8f9fa`, left border `3px solid #1a5276`, monospace 0.78em, padding 16px, radius 4px; `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 10px 14px, 0.93em; list items 0.93em; links `#1a5276`. No nav bar, no back/home links.
- **Canvas:** `<canvas id="skuChart" height="380">`, CSS `display:block; width:100%`; drawing script measures `getBoundingClientRect()`, scales backing store by `window.devicePixelRatio` via `setTransform(dpr,0,0,dpr,0,0)`, fixes CSS height to 380px, and redraws on window resize.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, bar fill `rgba(26,82,118,0.35)`, gray text `#666`/`#555`.
