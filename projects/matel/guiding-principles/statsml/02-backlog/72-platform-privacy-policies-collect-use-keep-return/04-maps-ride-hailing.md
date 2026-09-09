# Maps / Ride-Hailing

**Page type:** detail page (obj-table layout: one row per section, text left 45%, canvas right 55% centered)
**HTML title tag:** Maps / Ride-Hailing — Collect, Use, Keep, Return

**Subtitle:** Continuous location history reveals home, work, routines — and every deviation from them.

**Disclaimer (callout):** **Disclaimer:** Generalized, illustrative synthesis of common practices for this platform category. No company names, no reference to any specific product's policy. Real policies vary and change constantly — read the actual policy for the actual product.

## What gets collected

- **Declared:** destination searches, saved places ("home", "work"), trip requests, ratings and tips, payment details.
- **Incidental:** continuous GPS trail — often in the background, not just during navigation; timestamps of every ping; speed and accelerometer readings during trips; nearby WiFi and Bluetooth signals; device model, IP, battery level.
- **Inferred:** home and work addresses you never typed; commute schedule and its deviations; visits to gyms, clinics, places of worship; relationships from repeated shared pickups and destinations.

> **Key point:** Most surprising: WiFi and Bluetooth scans can place you indoors even with GPS off — and the visited-places timeline can be reconstructed for years, not days.

### Visualization (canvas `c1`, 720×400)

Grouped horizontal bar chart: assumed vs realistic collection extent per category. (This variant has no numeric value labels at bar ends.)

- **Title (bold 13px `#1a5276`, centered):** "What people assume is collected vs realistic extent (illustrative)"
- **Legend:** swatch `rgba(26,82,118,0.35)` labeled "assumed"; swatch `rgba(231,76,60,0.55)` labeled "realistic extent" (11px `#2c3e50`).
- **Rows** (label, assumed a, realistic b; 0–100 scale, right-aligned 11px labels at x=215, bars start x=225, max width 430, bar height 11, 3px between the pair, group gap 40, start y=52):
  - Destination searches: 80 / 90
  - Trip pickup/dropoff history: 65 / 95
  - Continuous background location: 25 / 85
  - Speed / accelerometer in trips: 10 / 70
  - WiFi / Bluetooth surroundings: 5 / 65
  - Inferred home & work: 15 / 90
  - Gym / clinic / worship visits: 10 / 80
  - Relationship inference: 5 / 55
- **Footer caption (gray `#999` 10px, centered):** "Numbers are illustrative — they show the shape of the gap, not measured values."

## How it gets used

- **Provide the service:** routing, driver dispatch, ETA computation.
- **Rank / recommend:** place suggestions, "popular near you", predicted destinations before you type.
- **Pricing:** demand forecasting and dynamic pricing built from everyone's trip patterns.
- **Ad targeting / measurement:** places visited become audience segments; store-visit attribution proves an ad "worked".
- **Model training:** traffic models, arrival-time predictors, road-network updates from your movement.
- **Sharing:** partners, aggregators, and legal-process requests scoped by place and time window.

### Visualization (canvas `c2`, 720×360)

Flow diagram: source boxes on the left funnel into a central orange hub, which fans out to colored use boxes on the right. Source boxes: `#1a5276` stroke with 12%-alpha fill, bold 11px centered labels. Arrows into the hub are gray `#bbb`; arrows out of the hub are drawn in each use box's own color, all 1.5px with filled arrowheads.

- **Title (bold 13px `#1a5276`, centered):** "From raw signals to uses"
- **Source boxes** (x=25, 150×36, `#1a5276`): "Location trail" (y=55), "Trip history" (y=120), "Sensor telemetry" (y=185), "Searches & saves" (y=250)
- **Hub box** (x=275, y=130, 160×80, orange `#e67e22`, 2px stroke, 12%-alpha fill): bold 12px "Movement profile" plus 10px `#7d5a29` subtitle "places · routines · segments"
- **Use boxes** (x=510, 190×34, label / color / y):
  - "Routing & ETA", `#27ae60`, y=45
  - "Pricing & demand models", `#2980b9`, y=95
  - "Place recommendations", `#2980b9`, y=145
  - "Ad targeting & attribution", `#e74c3c`, y=195
  - "Model training", `#8e44ad`, y=245
  - "Partner / legal sharing", `#e67e22`, y=295

## How long it's kept

- **Active account:** location timeline and trip history typically persist for the life of the account unless manually purged.
- **After deletion:** a grace/backup tail — commonly weeks to months — before purging begins.
- **Trip receipts:** financial records outlive deletion by years under tax and accounting law.
- **Safety / fraud records:** incident and dispute data held indefinitely "as required by law".
- **Aggregated / de-identified movement data:** no retention limit at all — the longest retention applies to copies stripped of direct identifiers, not the originals, which get the shorter windows. The catch: location traces are notoriously re-identifiable even without a name attached.

### Visualization (canvas `c3`, 720×340)

Horizontal retention-timeline bars, one per data category, with a dashed "account deleted" vertical marker and a horizontal axis line at the bottom.

- **Title (bold 13px `#1a5276`, centered):** "Retention by data category (illustrative)"
- **Plot:** bars start at x=220, axis width 460, first row y=45, bar height 18, gap 18; bars filled at 45% alpha of their color with a 1px solid stroke of the same color; rows at fraction 1.0 get a solid arrowhead (= indefinite) and their note right-aligned inside the bar; other notes sit in gray `#666` 10px to the right of the bar; right-aligned 11px `#2c3e50` row labels.
- **Rows** (label, fraction, color, note):
  - Raw location pings, 0.55, `#2980b9`, "deletion + backup tail"
  - Trip history, 0.62, `#2980b9`, "tail before purge"
  - Search & saved places, 0.55, `#2980b9`, (no note)
  - Trip receipts (financial), 0.85, `#e67e22`, "tax law: years"
  - Safety / fraud records, 1.0, `#e74c3c`, "indefinite"
  - De-identified aggregates, 1.0, `#e74c3c`, "indefinite"
- **Marker:** dashed (5/4) red `#e74c3c` 2px vertical line at 45% of axis width, bold 11px red label below: "account deleted".
- **Axis:** thin gray `#999` horizontal line under the bars; gray `#666` 10px labels "account opens" (left) and "indefinite →" (right).

## What you get back

- **Included:** trip list with pickup/dropoff, saved places, search history, ratings, profile and settings.
- **Excluded:** the raw location ping stream, inferred home/work labels, visit-pattern segments, WiFi/Bluetooth scan data, fraud and safety scores, copies already shared with partners, internal access logs.

> **Key point:** The asymmetry: the export returns what you gave, not what was derived. The inferred layer — routines, places, relationships — is the most valuable part and it stays behind.

### Visualization (canvas `c4`, 720×340)

Two side-by-side panels comparing export contents vs retained data. Panels have 8%-alpha fill + 2px stroke of panel color, bold 13px colored title, 11px `#2c3e50` item lines at 22px spacing.

- **Title (bold 13px `#1a5276`, centered):** "The data export: what comes back vs what stays behind"
- **Left panel (x=30, y=40, 320×240, green `#27ae60`) "IN THE EXPORT":** Trip list (pickup / dropoff) / Saved places & labels / Search history / Ratings & tips / Profile & settings
- **Right panel (x=375, y=40, 320×275, red `#e74c3c`) "EXISTS BUT NOT RETURNED":** Raw location ping stream / Inferred home / work labels / Visit-pattern & routine segments / WiFi / Bluetooth scan data / Fraud & safety scores / Copies shared with partners / Internal access logs
- **Footer caption (gray `#999` 10px, centered):** "You get your inputs back. The derived movement profile stays with the platform."

## Regeneration instructions

- **Template/layout:** platform-privacy-policies detail page. h1, `.subtitle`, one `.disclaimer` callout, then a single `.obj-table` (full-width, border-collapse) with four `<tr>` rows — one per section (collected / used / kept / returned). Left `<td>` (45%) holds `.obj-title` + `<ul>` bullets + optional `.key-point` box; right `<td>` (55%, text-align center) holds the canvas.
- **Page CSS:** body -apple-system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; subtitle `#666` 1.05em; table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold `#1a5276` 1.1em; `li` 0.93em; `.key-point` background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em; `.disclaimer` background `#fdf3e7`, left border `3px solid #e67e22`, padding 10px 14px, 0.9em, text `#7d5a29`. No nav bar, no back/home links.
- **Palette:** `#1a5276` primary blue, `#2980b9` secondary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange, `#8e44ad` purple, gray `#999`/`#666`.
- **Canvas:** declare intrinsic `width`/`height` attributes per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setupCanvas(id, w, h)` helper.
- In regenerated HTML, any card links use `.html` extensions.
