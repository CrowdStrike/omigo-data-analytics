# Uber / Lyft Data Download

**Page type:** detail page (single-row two-column obj-table: request/contents text left ~45%, payload sample + canvas right ~55%)
**HTML title tag:** Uber / Lyft Data Download

**Subtitle:** Personal trip history, fare breakdowns, and surge pricing data via GDPR/CCPA export

**Verified line (small gray text under subtitle):** Last verified: August 2026

## Section: How to Request

- **Uber:** Settings → Privacy → "Download Your Data" (processing takes up to 30 days)
- **Lyft:** Help → Account → "Request a copy of your personal data"

## Section: Delivery Timeline

- Uber: typically 3-7 days, can take up to 30 days
- Lyft: typically 7-14 days via email link
- Both deliver as ZIP archives with CSV/JSON files

## Section: What's Included

- Trip history with pickup/dropoff GPS coordinates
- Route taken (waypoints for completed trips)
- **Surge multiplier at time of request** (this bold phrase colored `#e67e22`) — reveals real-time pricing at that exact moment
- Fare breakdown (base, distance, time, fees, tips)
- Driver ratings given and received
- Wait time (request to pickup)
- Cancellations with timestamps and fees
- Payment methods used per trip
- Product type (UberX, Pool, Black, etc.)

**Key-point callout (blue left border):** **Why surge data matters:** The surge multiplier is a snapshot of real-time supply/demand pricing at that location and time. Aggregated across users, this reconstructs dynamic pricing surfaces.

## Section: What's Missing

**Missing callout (red left border `#e74c3c`, background `#fdf2f2`), bulleted list:**

- ETA model inputs (predicted arrival times, route alternatives)
- Driver matching algorithm scores
- Rider fraud/trust score
- Ride-pool optimization data (detour calculations)
- Demand forecasting features used for surge
- Driver location heatmaps at request time

## Right column: Payload sample

**Payload note (italic gray, above the code block):** Uber trip export record (fields marked * are documented; others inferred from export structure)

**Payload code block (monospace, `#f8f9fa` background, blue left border, verbatim including inline comments):**

```
{
  "trips": [
    {
      "request_time": "2025-11-14T17:42:08Z",      // *documented
      "dropoff_time": "2025-11-14T18:14:33Z",      // *documented
      "pickup_lat": 37.7749,                        // *documented
      "pickup_lng": -122.4194,                      // *documented
      "dropoff_lat": 37.8044,                       // *documented
      "dropoff_lng": -122.2712,                     // *documented
      "distance_miles": 12.4,                       // *documented
      "fare_amount": 34.82,                         // *documented
      "surge_multiplier": 1.8,                      // *documented
      "driver_rating": 5,                           // *documented
      "product_type": "UberX",                      // *documented
      "wait_time_seconds": 247,                     // inferred
      "route_polyline": "encoded_string...",        // inferred
      "payment_method": "Visa •••• 4021"            // *documented
    }
  ]
}
```

### Visualization (canvas `tripChart`, responsive width × 340)

Grouped bar chart: trip frequency by hour of day, weekday vs weekend, 24 hour groups with two bars each.

- **Title (bold 13px `#1a5276`, top center):** "Trip Frequency by Hour of Day".
- **Data (avg trips per hour, normalized 0-10 scale):**
  - Weekday (commute peaks at 7-9am and 5-7pm), hours 0-23: `[0.8, 0.4, 0.3, 0.2, 0.3, 0.9, 2.1, 5.8, 7.2, 4.5, 3.2, 3.0, 3.8, 3.5, 3.2, 3.8, 4.5, 7.8, 8.5, 6.2, 4.1, 3.0, 2.2, 1.4]`
  - Weekend (late night peaks at 10pm-2am, brunch peak 10am-1pm), hours 0-23: `[3.8, 3.2, 2.4, 1.2, 0.6, 0.4, 0.5, 0.8, 1.5, 2.8, 4.5, 5.2, 5.0, 4.2, 3.5, 3.0, 3.2, 3.8, 4.5, 5.0, 5.8, 6.5, 7.8, 6.2]`
- **Bars:** weekday bars filled `#1a5276`, weekend bars filled `#e67e22`; each hour group is chartWidth/24 wide, each bar 35% of group width with a small gap, weekend bar drawn 1px right of the weekday bar.
- **Y-axis:** 0 to 10, 6 tick labels at intervals of 2 (0, 2, 4, 6, 8, 10) in gray `#666` 10px; light `#eee` horizontal gridlines at each tick; rotated vertical label "Relative Trip Volume" in gray `#666` on the far left.
- **X-axis:** gray `#ccc` baseline; hour labels every 3 hours formatted "0:00", "3:00", … "21:00" in gray `#666` 9px, centered under their group.
- **Margins:** left 48, right 20, top 40, bottom 50.
- **Legend (bottom center):** `#1a5276` swatch + "Weekday", `#e67e22` swatch + "Weekend", labels in `#333` 11px.

## Regeneration instructions

- **Layout:** single `.obj-table` (full width, collapsed borders, `1px solid #e0e0e0` cell borders, 16px cell padding) with one `<tr>`: left `<td>` (45%) holds `.obj-title` headings + bullet lists + `.key-point` and `.missing` callouts; right `<td>` (55%, text-align center) holds `.payload-note`, `.payload` code block, and the canvas.
- **Page style:** body system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276` with 4px bottom margin; `.subtitle` `#666` 1.05em; `.verified` 0.85em `#888`; `.obj-title` bold `#1a5276` 1.1em (subsequent ones get `margin-top: 18px` inline); li 0.93em.
- **Callout styles:** `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em. `.missing` — background `#fdf2f2`, left border `3px solid #e74c3c`, same padding/size. `.payload` — background `#f8f9fa`, left border `3px solid #1a5276`, monospace (ui-monospace/Menlo) 0.78em, `white-space: pre`, left-aligned, line-height 1.45. `.payload-note` — 0.82em `#666` italic, left-aligned.
- **Canvas:** `style="width: 100%; height: 340px;"`; script reads `getBoundingClientRect()`, sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`), fixes CSS height to 340px, and calls `ctx.scale` so drawing stays in logical coordinates.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`; gray text `#666`/`#333`. No nav bar, no back/home links; in regenerated HTML any card links use `.html` extensions.
