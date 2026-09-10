# Location & Geo Signals

**Page type:** detail page (most-powerful-signals compact style: per-section two-column layout table, text left 45% with tag pills / labeled bullets / example / key-point, canvas right 55%)
**HTML title tag:** Location & Geo Signals

**Subtitle:** Where you are reveals intent better than what you search — raw pings compound into speed, orientation, relationships, routines, and life events

## The Raw Location Stack

**Tags:** `signal` (blue), `rule of thumb` (blue)

- **Accuracy tiers** — BLE ~2 m, GPS ~5 m, WiFi ~25 m, cell ~500 m, IP ~5 km
- **Fusion** — Google/Apple built WiFi databases from phone-reported SSIDs + GPS
- **Beacons** — Target and Macy's piloted shelf-level offer networks
- **Battery** — continuous GPS drains 5-10%/hour; apps sample opportunistically
- **Rule of thumb** — cheaper signal, coarser fix; IP free but city-grade

*Example: Google Maps' blue dot fuses GPS, WiFi, cell, and motion dead-reckoning.*

**Key point:** Treating a 500 m cell-tower fix like a 5 m GPS fix places users in the wrong store, street, or city.

### Visualization (canvas `c1`, 720×300)

Horizontal bar chart on a log scale: accuracy radius by source.

- **Title (bold 14px `#1a5276`, top center):** "Accuracy Radius by Source (log scale, meters)".
- **Bars** (rows 40px pitch, label column at x=120, width = log10(m)/log10(10000) of chart width, min 8px):
  - "BLE beacon" 2 m — "~2 m — aisle level"
  - "GPS" 5 m — "~5 m — street level"
  - "WiFi" 25 m — "~25 m — building level"
  - "Cell tower" 500 m — "~500 m — neighborhood"
  - "IP address" 5000 m — "~5 km — city grade"
  First three rows fill `rgba(26,82,118,0.35)` stroke `#1a5276`; last two (coarse) fill `rgba(231,76,60,0.45)` stroke `#e74c3c`. Names right-aligned 12px `#2c3e50`; side labels 11px `#555`.
- **Caption (bottom center, bold 11px `#e67e22`):** "cheaper / lower-battery signal → coarser fix; phones fuse all layers continuously".

## Dwell Time = Visit

**Tags:** `mechanism` (blue), `failure mode` (red)

- **Visit rule** — geofence + 5-20 min dwell; short passes discarded
- **Foot traffic** — Placer.ai, SafeGraph, Foursquare panels; hedge funds trade on them
- **Ad attribution** — Google "store visits" links impression to physical visit
- **Popular times** — Google Maps busyness and local rankings = aggregated dwell
- **Thresholds** — 2-minute pass is traffic; 15 minutes is a customer

*Example: Foursquare turned check-ins into POI ground truth, then passive dwell replaced the check-in.*

**Failure mode:** GPS gives no floor number, so mall visits get misattributed to stores above, below, and next door.

### Visualization (canvas `c2`, 720×300)

Line chart: distance to store over time, with a geofence threshold and a shaded dwell window.

- **Title:** "Distance to Store Over Time — Dwell Window = Visit".
- **Data:** distance trace (m) `[800, 650, 500, 350, 200, 90, 30, 15, 12, 10, 14, 11, 9, 13, 12, 10, 15, 40, 150, 320, 500, 700]`, y max 850; padding top 42 / bottom 55 / left 65 / right 30. Rotated y label "distance to store (m)".
- **Series:** blue `#1a5276` line, width 3.
- **Geofence line:** dashed orange `#e67e22` horizontal line (dash 5/4) at 50 m, labeled 12px "geofence radius 50 m".
- **Dwell window:** `rgba(39,174,96,0.15)` shading between indices 6 and 16, labeled bold 13px green "dwell 20 min above threshold = VISIT".
- **Labels (12px `#555`):** "approach" (left), "depart" (right), "time →" below.
- **Caption (bottom center, bold 11px `#1a5276`):** "short pass-bys below the dwell threshold are discarded as drive-by traffic".

## Home / Work Inference

**Tags:** `signal` (blue), `privacy risk` (red)

- **Method** — 10pm-6am cluster = home; weekday 9-5 cluster = work
- **De-anonymization** — 4 spatio-temporal points identify ~95% of individuals
- **Incidents** — NYT re-identified Pentagon officials; Strava heatmaps exposed bases
- **Proxies** — home = income/demographics; work = employer/industry
- **Commute length** — feeds insurance mileage and delivery-zone eligibility

*Example: journalists bought broker pings and named an owner from one nighttime cluster.*

**Privacy risk:** The trajectory itself is the identifier — home + work + two errands re-identify almost everyone.

### Visualization (canvas `c3`, 720×300)

Map sketch: two jittered ping clusters (home and work) on a faint street grid.

- **Title:** "Pings Clustered by Time of Day → Home + Work Pair".
- **Background:** faint `#eee` street grid (7 vertical, 5 horizontal lines) inside padding top 40 / bottom 45 / left 50 / right 160.
- **Clusters (seeded pseudo-random jittered dots, 3.2px, elliptical spread):** home cluster at ~22% width / 68% height, 45 dots in `rgba(26,82,118,0.65)`, radius 38; work cluster at ~75% width / 28% height, 34 dots in `rgba(230,126,34,0.75)`, radius 30.
- **Commute path:** dashed gray `#bbb` quadratic curve between the clusters (dash 3/4), labeled 11px `#999` "commute".
- **Cluster labels (bold 13px):** "HOME (10pm–6am)" in `#1a5276`; "WORK (9–5, Mon–Fri)" in `#e67e22`.
- **Legend (right):** blue dot swatch "night pings", orange dot swatch "day pings"; below in bold 12px red: "home + work pair" / "≈ unique ID in a metro".
- **Caption (bottom center, bold 11px `#e74c3c`):** "4 spatio-temporal points uniquely identify ~95% of people in \"anonymized\" mobility data".

## Place Visits = Interest Graph

**Tags:** `signal` (blue), `abuse` (red)

- **POI join** — visits x Foursquare/Google Places categories = interest profile
- **Segments** — gym 3x/week = fitness; dealership = auto in-market; airports = traveler tier
- **Geo-conquesting** — Burger King's 1-cent Whopper inside McDonald's geofences
- **Sensitive inference** — clinics, worship, protests; brokers sold these segments
- **Recency ladder** — in-store beats last-week beats lives-nearby; bids follow

*Example: two dealership visits put a user in "auto in-market" ad segments within days.*

**How it's exploited:** The most predictive segments — health, faith, financial distress — are exactly the ones regulators keep finding for sale.

### Visualization (canvas `c4`, 720×300)

Annotated horizontal bars: 30 days of visits by category with inferred segments.

- **Title:** "One Device, 30 Days of Visits → Inferred Segments".
- **Rows (name, visits, side label, colors)** — scale max 20, rows 40px pitch, label column x=135:
  - "Coffee shops" 18 → "18 visits → daily commuter stop" — fill `rgba(26,82,118,0.35)` stroke `#1a5276`
  - "Gym" 13 → "13 visits → fitness buyer" — blue
  - "Clinic" 4 → "4 visits → sensitive — yet sellable" — fill `rgba(231,76,60,0.45)` stroke `#e74c3c`
  - "Car dealership" 3 → "3 visits → in-market: auto (high bid)" — fill `rgba(230,126,34,0.5)` stroke `#e67e22`
  - "Airport" 2 → "2 visits → business traveler" — blue
- **Caption (bottom center, bold 11px `#e67e22`):** "repeated dwell = demonstrated, costly behavior — stronger than any search query".

## Derived Signal: Speed from GPS Deltas

**Tags:** `derived signal` (blue), `defense` (green)

- **Driver scoring** — Uber/Lyft score harsh braking, acceleration, speeding from phones
- **Insurance telematics** — Progressive, Root, CMT rate hard brakes per 100 miles
- **Mode detection** — walk ~5, cycle ~15, drive 40+ km/h; transit = stops on route
- **ETA models** — per-segment speeds power Google/Waze traffic and delivery ETAs
- **Spoof detection** — teleportation or 300 km/h city "driving" betrays fakes

*Example: Pokémon GO bans city-to-city "walkers"; delivery apps flag teleporting couriers.*

**Key point:** One subtraction turns pings into behavior, context, and physical-integrity checks — the cheapest strong geo signal.

### Visualization (canvas `c5`, 720×300)

Speed-over-time trace with harsh-brake markers and a spoofed spike.

- **Title:** "Speed = Δposition / Δtime — Harsh Brakes and a Spoofed Jump".
- **Data:** speed (km/h) `[0, 12, 28, 42, 55, 58, 60, 24, 45, 57, 62, 60, 15, 40, 55, 58, 60, 175, 60, 55, 30, 10, 0]`, y max 190; harsh brakes at indices 7 and 12; spoof at index 17. Padding top 42 / bottom 50 / left 65 / right 30; rotated y label "speed (km/h)".
- **Plausibility band:** `rgba(39,174,96,0.10)` shading from 0 to 120 km/h; dashed red `#e74c3c` line (dash 5/4) at 120 labeled 11px "physically plausible ceiling (city) ~120 km/h".
- **Series:** blue `#1a5276` line, width 3.
- **Brake markers:** orange `#e67e22` 6px dots at the two brake points, labeled bold 12px "harsh brake (Δv > 12 km/h per sec)".
- **Spoof marker:** red `#e74c3c` 7px dot at the 175 spike, labeled bold 12px "175 km/h in a city = GPS spoof / teleport".
- **X label:** "time →" (12px `#555`).
- **Caption (bottom center, bold 11px `#1a5276`):** "one derivative → driver scoring, mode detection (walk ~5 / cycle ~15 / drive 40+), ETA, fraud".

## Derived Signal: Phone Angle & Orientation

**Tags:** `derived signal` (blue), `failure mode` (red)

- **Sensors** — accel/gyro/magnetometer at 50-100 Hz; historically no runtime permission
- **Mounted vs handheld** — mid-trip pickup flags driver distraction in telematics
- **Driver vs passenger** — entry side, pickup motion, typing-while-moving classify holder
- **Road quality** — crowd-sourced vibration spikes map potholes for cities
- **Crash detection** — deceleration + tumble + stop triggers Apple/Google emergency calls

*Example: CMT rates "phone in hand while moving" as distraction; cupholder rattle triggers disputes.*

**Failure mode:** Loose phones and bumpy roads fake distraction flags, and early crash detection auto-dialed 911 from roller coasters and ski slopes.

### Visualization (canvas `c6`, 720×300)

Split accelerometer trace: low-variance mounted half with a pothole spike, then high-variance handheld half.

- **Title:** "Accelerometer Variance — Mounted vs Handheld, Pothole Spike".
- **Layout:** padding top 42 / bottom 45 / left 65 / right 30; rotated y label "acceleration (z-axis)"; 160 samples around a midline.
- **First half:** green `#27ae60` noisy line, amplitude ±16px, one large upward pothole spike (−72px) at sample 40, labeled bold 12px orange "pothole spike" / "(crowd-sourced road quality)".
- **Second half:** red `#e74c3c` noisy line, amplitude ±72px.
- **Divider:** dashed gray vertical line at midpoint with an orange upward-pointing triangle and bold 11px orange label below: "phone picked up mid-trip".
- **Zone labels (bold 13px):** green "MOUNTED: low variance, vehicle-coupled"; red "HANDHELD while driving →" plus 12px "distraction flag".
- **Caption (bottom center, bold 11px `#1a5276`):** "50-100 Hz motion sensors: driver vs passenger, mounted vs handheld, road surface — no GPS needed".

## Derived Signal: Co-location

**Tags:** `derived signal` (blue), `privacy risk` (red)

- **Relationship labels** — office 9-5 = colleagues; nightly = household; evenings/weekends = partner
- **Household graphs** — ad-tech joins shared night location + IP; also friend suggestions
- **Contact tracing** — COVID Bluetooth proximity: co-location without absolute position
- **Fraud rings** — dozens of accounts pinging from one apartment = farm
- **Repeat rule** — one overlap is coincidence; 3+ different days = relationship edge

*Example: Life360 sells family co-location openly; SDKs build the same household graphs silently.*

**How it's exploited:** One consenting user becomes a sensor for everyone nearby — non-users still appear in proximity graphs.

### Visualization (canvas `c7`, 720×300)

Two horizontal day timelines (Device A, Device B) with red boxes around overlap windows.

- **Title:** "Two Devices, One Day — Overlap Windows = Relationship Edge".
- **Timelines (24h scaled to chart width, rows at y=70 and y=120, 26px tall, label column x=95):**
  - Device A segments: 0-8h home, 9-17h work A, 18-20h restaurant, 20.5-24h home.
  - Device B segments: 0-7h home, 8-16h work B, 18-20h restaurant, 20.5-24h home.
- **Segment colors:** home `#1a5276`, work A `#e67e22`, work B `#999`, restaurant `#27ae60`.
- **Overlap boxes:** red `#e74c3c` 2px outlines spanning both rows over 0-7h, 18-20h, 20.5-24h, labeled bold 10px red "household", "evening out", "household".
- **Hour ticks:** "0h", "6h", "12h", "18h", "24h" (11px `#555`).
- **Legend (row at y≈205):** swatches for "home", "work A", "work B", "restaurant", "co-location window".
- **Caption (bottom center, bold 11px `#1a5276`):** "same home every night = household; evenings + weekends = partner; office 9-5 only = colleagues".

## Derived Signal: Trajectory Patterns

**Tags:** `derived signal` (blue), `defense` (green)

- **Predictability** — next location ~90% predictable for typical commuters
- **Commute fingerprint** — route + departure times stable; learned in days
- **Account sharing** — two disjoint commute patterns = two people
- **Delivery fraud** — claimed route vs GPS mismatch flags fake deliveries
- **Anomaly triggers** — banks decline contradicting swipes; geofences fire automations, offers

*Example: a bank seeing your phone in Chicago declines the Miami "card-present" swipe.*

**Key point:** Once the routine baseline is learned, every deviation becomes a classified event — second user, fraud, trip, or emergency.

### Visualization (canvas `c8`, 720×300)

Scatter of departure times by weekday with a baseline band and two anomalies.

- **Title:** "Departure Time by Weekday — Regular Commute vs Anomaly".
- **Data:** departure hours over 20 weekdays `[8.1, 8.3, 8.2, 8.0, 8.25, 8.15, 8.3, 8.1, 8.2, 8.35, 8.05, 8.2, 8.3, 8.15, 13.5, 8.2, 8.1, 13.8, 8.25, 8.2]`; anomalies at indices 14 and 17; y range 7–15. Padding top 42 / bottom 50 / left 65 / right 30; rotated y label "departure hour".
- **Baseline band:** `rgba(39,174,96,0.15)` shading over 8.0–8.4, labeled bold 12px green "personal baseline: 8:00-8:25 window (learned in ~1 week)".
- **Points:** blue `#1a5276` 4.5px dots; anomalies red `#e74c3c` 7px dots with thin red arrows up to a centered bold 12px red label "1:30pm departures = second user? shared account?".
- **X label:** "weekdays →" (12px `#555`).
- **Caption (bottom center, bold 11px `#1a5276`):** "next-location predictability ~90% for commuters — deviations from the baseline are the signal".

## Life-Event Detection from Cluster Shifts

**Tags:** `signal` (blue), `best practice` (green)

- **Relocation** — nighttime cluster moves = mover, the most valuable retail segment
- **Job change** — new weekday cluster; commute change feeds insurance mileage
- **Travel** — long home-cluster absence triggers roaming and security upsells
- **Churn prediction** — courier trajectory leaving the delivery zone predicts churn
- **Persistence rule** — new cluster must dominate 2-4 weeks before relabeling

*Example: a two-week beach rental looks like a move; day-3 "new mover" coupons hit vacationers.*

**Detection rule:** The threshold is temporal persistence, not distance — a 12 km shift for 2 days is a trip; 4 weeks is a new home.

### Visualization (canvas `c9`, 720×300)

Step-change line: distance of nightly cluster from old home over 16 weeks.

- **Title:** "Distance of Nightly Cluster from Old Home (km, 16 weeks)".
- **Data:** `[0.1, 0.2, 0.1, 0.15, 0.1, 6.0, 0.1, 0.2, 12.1, 12.0, 12.2, 12.1, 12.0, 12.1, 12.2, 12.0]`, y max 14. Padding top 42 / bottom 50 / left 65 / right 30; rotated y label "km from old home".
- **Series:** blue `#1a5276` line (width 2) with 5px dots; the week-5 vacation blip dot in orange `#e67e22`.
- **Confirmed-move region:** `rgba(39,174,96,0.15)` shading from ~week 10.5 onward.
- **Annotations (bold 12px):** orange "vacation blip — ignore" at week 5; red "step change: nightly cluster moved 12 km" near week 8; green "persisted 2+ weeks → label as MOVE" inside the shaded region.
- **X label:** "weeks →" (12px `#555`).
- **Caption (bottom center, bold 11px `#1a5276`):** "change-point on cluster centroids: persistence, not distance, separates trips from life events".

## Surge Pricing & Location-Based Price Discrimination

**Tags:** `mechanism` (blue), `gaming` (orange)

- **Surge mechanics** — Uber/Lyft demand/supply ratio sets block-level, per-minute multipliers
- **Predictable peaks** — bar close, concerts, rain; riders walk out of polygons
- **Price discrimination** — WSJ found Staples pricing by distance to competitor stores
- **Delivery fees** — vary with density and supply, correlating with wealth
- **Geo-compliance** — licensing, gambling, tax rules inherit location error modes

*Example: Uber drivers mass-logged-off near airports to fake scarcity and trigger surge.*

**Gaming vector:** When price depends on measured local state, the measurement itself becomes the attack surface for both sides.

### Visualization (canvas `c10`, 720×300)

Bars + line: hourly demand/supply ratio with the surge multiplier tracking it.

- **Title:** "Surge Multiplier Tracks Local Demand / Supply, Hour by Hour".
- **Data (24 hourly demand/supply ratios):** `[0.4, 0.3, 0.2, 0.2, 0.3, 0.6, 1.1, 1.6, 1.4, 0.9, 0.8, 0.9, 1.0, 0.9, 0.8, 0.9, 1.2, 1.7, 1.5, 1.2, 1.1, 1.4, 2.4, 1.2]`, y max 2.6. Padding top 42 / bottom 50 / left 65 / right 150.
- **Bars:** `rgba(26,82,118,0.35)` per-hour ratio bars.
- **Surge line:** orange `#e67e22` (width 3) plotting max(1.0, ratio).
- **Reference:** dashed gray `#bbb` line at 1.0 labeled "1.0x".
- **Annotations:** bold 12px red "2.4x at bar close" at hour 22; 11px blue "am rush" (hour 7) and "pm rush" (hour 17).
- **Hour labels:** "0h", "6h", "12h", "18h", "24h".
- **Legend (right):** blue-tint swatch "demand/supply", orange swatch "surge multiplier".
- **Caption (bottom center, bold 11px `#e67e22`):** "predictable peaks invite gaming: riders walk out of the polygon, drivers manufacture scarcity".

## Accuracy Failure Modes, Spoofing & Countermeasures

**Tags:** `failure mode` (red), `defense` (green)

- **Urban canyons** — GPS multipath gives 50 m+ errors downtown
- **Indoors** — no floor number; a "mall visit" spans 40 stacked stores
- **IP geolocation** — wrong-city common; MaxMind default sent traffic to a Kansas farm
- **VPN / carrier NAT** — whole carrier regions exit through one IP block
- **Spoofing defense** — cross-check speed physics, WiFi/cell consistency, mock-location flags

*Example: Pokémon GO layers speed checks, mock-location detection, and behavioral signals — no single check suffices.*

**Design rule:** Always carry the accuracy radius with the coordinate and never trust one location source alone.

### Visualization (canvas `c11`, 720×300)

Horizontal bar chart on a log scale: typical position error by environment.

- **Title:** "Typical Position Error by Environment (log scale, meters)".
- **Bars** (rows 40px pitch, label column x=155, width = log10(m)/log10(50000) of chart width, min 8px):
  - "GPS, open sky" 4 m — "~4 m" — good (fill `rgba(39,174,96,0.45)` stroke `#27ae60`)
  - "GPS, suburb" 8 m — "~8 m" — good
  - "Indoors (WiFi only)" 30 m — "~30 m + no floor number" — bad (fill `rgba(231,76,60,0.45)` stroke `#e74c3c`)
  - "GPS, urban canyon" 50 m — "~50 m — multipath off towers" — bad
  - "IP geolocation" 20000 m — "~20 km — wrong city is common" — bad
- **Caption (bottom center, bold 11px `#e74c3c`):** "always carry the accuracy radius with the coordinate — and cross-check sources against spoofing".

## Regeneration instructions

- **Layout:** one `.card-section` per section, each containing an `<h2>` (1.3rem `#1a5276`, bottom border `2px solid #2980b9`) and a `table.layout` with a single `<tr>`: left `td.text-col` (45%) holding `.tags` pills, `<ul>` bullets, `p.example`, `.key-point`; right `td.viz-col` (55%) with one `<canvas width="720" height="300">` styled `width:100%`, border `1px solid #e0e0e0`, radius 4px.
- **Page style:** body system-ui sans, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with bottom border `2px solid #2980b9`; `.subtitle` `#666` 0.95rem; `ul` 0.92rem; `li b` in `#1a5276`.
- **Tag pills:** `.tag` inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Key-point callout:** background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem. `.example` italic `#555` 0.9rem.
- **Canvas:** shared `setup(id)` helper scaling by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates); one IIFE per chart. Cluster/noise charts use a seeded LCG pseudo-random generator for reproducible jitter. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`.
- No nav bar, no back/home links. In regenerated HTML any links use `.html` extensions.
