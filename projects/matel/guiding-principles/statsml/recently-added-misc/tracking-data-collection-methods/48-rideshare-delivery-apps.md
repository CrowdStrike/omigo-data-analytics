# Tracking Data: Rideshare & Delivery Apps

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one row per section)
**HTML title tag:** Tracking Data: Rideshare & Delivery Apps

**Subtitle:** A trip record is an origin, a destination, and two timestamps. Repeated over months, the origin-destination pair is the identifying object — and it stays identifying after the rider ID is stripped.

## What is it?

Location access, granted for matching, scoped wider than the trip.

- **Why access is needed:** to match a vehicle or courier to an address
- **The permission granted** is usually broader than the trip itself
- **Outside an active trip:** whether location is sampled depends on the permission level chosen and the app's background configuration

### Visualization (canvas `c1`, 720×320)

Map-like schematic: five labeled location dots connected by dashed curved routes, plus a footer band stating what is absent.

- **Title (bold 17px, left-aligned at 20,26, ink `#1a5276`):** "Frequent origin–destination pairs — schematic". Subtitle line (12px, mute `#6b7280`, at 20,44): "Each endpoint is a separate place. The identifying object is the pair, not either end."
- **Locations (one hue per endpoint from the SERIES palette):** Home (110,150, blue `#2a78d6`), Work (370,88, green `#008300`), Gym (580,160, violet `#4a3aa7`), Bar (268,236, orange `#d95926`), Clinic (512,248, aqua `#199e70`).
- **Routes:** dashed (6/4, width 2) quadratic curves (control point at midpoint, offset −20px in y) between pairs Home→Work, Work→Gym, Home→Bar, Home→Clinic, Work→Home; each stroke tinted from its destination hue at 45% alpha.
- **Dots:** 10px filled circle in location hue with a 2px stroke, white 4px inner circle; bold 16px label in the location hue centered 18px above the dot.
- **Footer band:** rounded rect (radius 8) at (20,274) size 680×34, fill mute-tint at 9% alpha, 1.5px stroke in grid `#e5e9ef`; centered 13px mute text: "Absent: every trip not booked through the app — walking, driving, transit, a lift."

## What does it collect?

- **Pickup and dropoff coordinates** per trip, with accuracy estimates
- **Request, start and end timestamps**; route polyline for the completed trip
- **Fare, tip** and the price multiplier applied at request time
- **Order contents** on the delivery side — items, merchant, category
- **Quote outcome** — accepted, or the request abandoned
- **Location samples outside a trip**, where the granted permission allows it

**Stripping `rider_id` does not anonymise:** the identifying object is the origin–destination pair, not either endpoint, and a repeated late-night pair points at one household and one workplace.

**`pickup_rounded` looks like the fix and mostly is not:** two decimal places is roughly a one-kilometre cell, and coarsening each endpoint separately barely dents the joint uniqueness of the pair. Reducing precision on the components is not the same as reducing precision on the thing that identifies.

### Visualization (canvas `c2`, 720×320)

Booking-coverage heatmap: 8 time blocks × 7 days, hue per day column, alpha depth per booking density, plus a legend band.

- **Title (bold 17px, ink, at 20,26):** "Bookings by day and time block — schematic". Subtitle (12px mute, at 20,44): "Colour is the day; depth of colour is how many bookings fell in that block."
- **Grid geometry:** cell 80×26, origin (80,62). Columns = days Mon–Sun; rows = time blocks 6am, 9am, 12pm, 3pm, 6pm, 9pm, 12am, 3am.
- **Density data (rows = time blocks, cols = days, values 0–3):**
  - 6am: `[2, 2, 2, 2, 2, 0, 0]`
  - 9am: `[3, 3, 3, 3, 3, 0, 0]`
  - 12pm: `[1, 1, 1, 1, 1, 1, 1]`
  - 3pm: `[1, 1, 1, 1, 1, 1, 0]`
  - 6pm: `[3, 3, 3, 3, 3, 1, 1]`
  - 9pm: `[1, 1, 1, 1, 2, 3, 2]`
  - 12am: `[0, 0, 0, 0, 3, 3, 2]`
  - 3am: `[0, 0, 0, 0, 0, 3, 1]`
- **Coloring:** each day column takes one SERIES hue in order (Mon `#2a78d6`, Tue `#008300`, Wed `#4a3aa7`, Thu `#d95926`, Fri `#199e70`, Sat `#d55181`, Sun `#c98500`); density mapped to alpha `[0, 0.20, 0.50, 1]`. Zero cells filled with mute tint at 7% alpha (neutral, not a pale day hue). Cell strokes 0.5px in grid `#e5e9ef`.
- **Headers:** day names bold 15px in the column's own hue (header doubles as legend), 8px above the grid. Time labels right-aligned 15px in text `#2c3e50` left of the grid.
- **Legend band (y=288):** swatches with 12px mute labels — "no booking" (mute 7% tint), "fewer" (blue 20%), "more" (blue 50%), "most" (blue full).
- **Caveat (italic 12px mute at 80, legend+20):** "An empty block is a block with no booking, not a block with no travel."

Below the canvas (right column):

Sample payload — illustrative structure, not real captured data.

```
// Trip receipts and partner APIs expose pickup and
// dropoff points with timestamps. The internal field
// names below are generic reconstruction.
{
  // ── present in the trip record ──
  "trip_id":       "t-9f41…",
  "rider_id":      "u-2210…",
  "requested_at":  "2026-08-22T23:12:04Z",
  "pickup":        { "lat": 30.2672, "lon": -97.7431, "accuracy_m": 12 },
  "dropoff":       { "lat": 30.2849, "lon": -97.7341, "accuracy_m": 8 },
  "dropoff_ts":    "2026-08-22T23:26:51Z",
  "distance_km":   3.1,

  // ── inferred / plausible ──
  "pickup_label":     "home",     // most frequent late-night origin
  "surge_multiplier": 1.4,
  "pickup_rounded":   { "lat": 30.27, "lon": -97.74 },
  "route_polyline":   "…"
}
```

## Why is it collected?

**Stated purpose** (label pill, blue)

- **The trip record is the product:** matching needs both positions, the fare needs distance and duration, and arrival estimates need the route driven
- **Supply positioning:** historical origin-destination volume by time of day tells the platform where demand appears before it appears

**Additional consequence** (label pill, orange)

- **Willingness to pay becomes observable per account** — aggregate pricing comes from supply and demand, but the same record shows which individual accepted which multiplier
- **Labels emerge unprompted** — the modal late-night origin and weekday-morning destination name themselves

**Whether a per-rider price response happens is not established here** — that is a claim about an internal system. The structural point stands without it: the data required for legitimate aggregate pricing is the same data that makes individual price discrimination computable.

### Visualization (canvas `c3`, 720×320)

Line chart: a network-level surge multiplier path over one evening, with annotated cause spikes and a footer callout band.

- **Title (bold 17px ink at 20,26):** "Multiplier is computed on the network; the response is recorded per account". Subtitle (12px mute at 20,44): "Schematic — illustrative shape, not measured multipliers."
- **Plot area:** x=60, width = canvas−130, y=62, height 164. Max price scale 5.0.
- **Data points (t fraction of evening, multiplier):** `(0, 1.0), (0.1, 1.0), (0.2, 1.2), (0.3, 1.0), (0.4, 1.1), (0.48, 3.2 "large venue empties"), (0.55, 1.5), (0.6, 1.0), (0.7, 2.5 "weather turns"), (0.75, 1.8), (0.8, 1.2), (0.85, 1.0), (0.95, 4.5 "annual peak night"), (1.0, 3.8)`.
- **Y axis:** relative band labels only, no asserted multiplier values — gridlines with right-aligned 12px mute labels at 1.0 "base", 2.5 "raised", 4.5 "peak"; grid strokes `#e5e9ef`.
- **Base line:** dashed (4/3) green `#008300`, 1.5px, at multiplier 1.0, labeled "reference level" in 11px green above it.
- **Series line:** blue `#2a78d6`, 2.5px, connecting all points.
- **Cause annotations:** the three labeled spikes each get their own hue in SERIES order — violet `#4a3aa7` ("large venue empties"), aqua `#199e70` ("weather turns"), magenta `#d55181` ("annual peak night"): bold 12px centered label 12px above the point, a 1px leader line tinted at 60% alpha, and a 4.5px filled dot at the point.
- **X caption (12px mute, centered under plot):** "one evening →".
- **Footer callout:** rounded rect (radius 8) at (60,250) size 600×58, fill blue tint 7% alpha, stroke 1.5px ink `#1a5276`. Line 1 (bold 12px blue): "The curve is a function of aggregate supply and demand — it is not per-rider." Lines 2-3 (12px orange `#d95926`): "But which account accepted which point on it becomes a stored, per-account" / "observation. That is the capability the schema creates."

## Regeneration instructions

- **Layout:** tracking detail page `.obj-table` — full-width table, `border-collapse: collapse`, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` + `.lede` + bullets + `.key-point` callouts, right `<td>` (55%, `text-align: center`) holds the canvas (and, in the "What does it collect?" row, the `.payload-note` caption plus `<pre class="payload">` block below the canvas, both left-aligned).
- **Page style:** body system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold 1.1em `#1a5276`; li 0.93em with `li b` in `#1a5276` weight 600.
- **Callouts:** `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em, first `<strong>` in `#1a5276`.
- **Label pills:** `.lbl` inline-block uppercase 0.7em bold, padding 2px 7px, radius 3px; `.lbl-purpose` background `#eaf2fb` color `#1a5276`; `.lbl-effect` background `#fdf0e6` color `#a8501c`.
- **Payload block:** `.payload` — background `#f8f9fa`, left border `3px solid #1a5276`, monospace 0.78em, `white-space: pre`; `.payload-note` italic 0.82em `#666` immediately above.
- **Canvas:** declare intrinsic `width="720" height="320"`; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setupCanvas(id)` helper that reads the element's own width/height attributes. Shared helpers: `rr()` rounded-rect path and `tint(hex, alpha)` rgba derivation from palette hexes.
- **Chart palette (tracking pages):** categorical CVD-checked tokens — blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`; ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red is reserved for genuine alarm states, not in the series rotation. Page/site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- No nav bar, no back/home links. In regenerated HTML, any card links elsewhere use `.html` extensions.
