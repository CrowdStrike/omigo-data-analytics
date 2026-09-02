# Live Sensor & Broadcast Feeds — Public Because the Signal Is in the Air

**Page type:** detail page (four titled sections, each a two-column row: text left 45%, canvas right 55%)
**HTML title tag:** Live Sensor & Broadcast Feeds — Public Because the Signal Is in the Air

**Subtitle:** Some data is public not by legal filing or policy choice but by physics: aircraft, ships, transit systems, and the planet itself broadcast unencrypted signals for safety and coordination, and anyone with a receiver can catch, aggregate, and archive them.

**Intro callout (blue-left-border box):** Broadcast protocols were designed so that nearby operators — pilots, ship captains, dispatchers — could see each other and avoid collisions. Encryption would defeat the purpose: the whole point is that any receiver can decode the message. Cheap software-defined radios and hobbyist receiver networks turned these local safety broadcasts into permanent global datasets, with no single custodian who could ever take them back.

## 1. Aircraft — ADS-B, the sky's open ledger

Every aircraft continuously broadcasts its identity, position, altitude, and velocity — unencrypted, by design, so other aircraft and ground stations can see it.

- **The broadcast:** ADS-B messages carry the airframe's unique ICAO address, GPS position, altitude, heading, and speed, transmitted about once per second on 1090 MHz.
- **The receivers:** a $30 software-defined radio decodes it; hobbyists run thousands of home antennas feeding networks like FlightRadar24 and ADS-B Exchange, which stitch local receptions into global, gap-free tracks.
- **Derived products:** celebrity-jet trackers, corporate-deal inference from tail-number movements, emissions estimates per flight — all built from a collision-avoidance signal.
- **The block that doesn't block:** cooperative aggregators honor blocked-tail-number lists (LADD); ADS-B Exchange refuses all blocks on principle — the signal is in the air, and no list can recall it.

Key point: Opting out is impossible at the source: the aircraft must broadcast to fly in controlled airspace. Any "privacy" is a policy choice made by each aggregator, and it takes only one non-cooperating aggregator to void it.

### Visualization (canvas `c1`, 720×400)

Fan-out/fan-in diagram: one aircraft broadcast reaching a row of hobbyist antennas, merging into an aggregator box.

- **Title (bold 14px `#1a5276`, centered, y=22):** "One broadcast, thousands of listeners, one permanent track"
- **Aircraft box:** 220×48 centered at x=360, top y=48, fill `rgba(26,82,118,0.12)`, 2px `#1a5276` border. Bold 12px `#1a5276` centered: "AIRCRAFT — ADS-B out, 1090 MHz"; 11px `#666`: "ICAO id · position · altitude · velocity — unencrypted".
- **Signal fan:** three concentric arcs (radii 40, 60, 80) in `rgba(26,82,118,0.35)` (width 1.5) below the aircraft box, spanning ~0.15π to 0.85π.
- **Antenna row (y=190):** five antenna masts in green `#27ae60` (2px vertical mast with a V-shaped top), each with a thin `#bbb` connector line from the aircraft, and a 10px `#666` label below: x=90 "home antenna", x=240 "rooftop SDR", x=390 "hobbyist Pi", x=540 "airport spotter", x=660 "feeder site". Under the row, 11px `#999` centered: "each receiver hears only the aircraft within ~250 km of it".
- **Merge:** thin `#bbb` lines from each antenna converging to the aggregator box.
- **Aggregator box:** 340×42 centered at x=360, top y=288, fill `rgba(39,174,96,0.10)`, 2px `#27ae60` border. Bold 12px `#27ae60` centered: "AGGREGATOR — FlightRadar24, ADS-B Exchange"; 11px `#666`: "local receptions stitched into one continuous global track".
- **Caption (12px `#999`, centered, y = h−14):** "The aircraft cannot choose its audience — anyone under the signal is a receiver"

## 2. Ships — AIS, where silence itself is a signal

Ships broadcast identity, position, heading, speed, and declared destination over AIS so nearby vessels can avoid collisions. Aggregators turned it into a global shipping map.

- **Coastal receivers:** volunteer and commercial antennas feed MarineTraffic-style trackers covering ports and shipping lanes in real time.
- **Satellite AIS:** constellations pick up the same broadcasts mid-ocean, extending coverage far past any coastline — the ship cannot choose who is listening.
- **"Dark ships":** switching AIS off to hide is itself detectable — a track that goes silent near a sanctioned port, then reappears with a changed draft, is exactly what sanctions-enforcement analysts look for.
- **Spoofing arms race:** because absence is suspicious, evaders now inject fake positions instead — which analysts catch by cross-checking against satellite imagery.

Key point: A safety protocol became a surveillance dataset — and its absence became a signal too. Once everyone broadcasts, not broadcasting is information.

### Visualization (canvas `c2`, 720×380)

Layered coverage diagram: satellite AIS boxes at top, coastline with coastal receivers at bottom, and a ship's dotted track in the middle with a flagged dark-period gap.

- **Title (bold 14px `#1a5276`, centered, y=22):** "AIS coverage layers — and the gap that gives a ship away"
- **Satellite layer (y=60):** three 52×24 boxes labeled "SAT" (bold 10px `#8e44ad`) at x=180, 360, 540, fill `rgba(142,68,173,0.12)`, 1.5px `#8e44ad` borders. Left-aligned 11px `#8e44ad` note: "satellite AIS — hears mid-ocean, past any coastline".
- **Coastline (y=300):** 1.5px `#999` horizontal line from x=30 to x=690, labeled "coastline" in 10px `#999`. Three green `#27ae60` antenna symbols on the line at x=110, 340, 600. Below, 11px `#27ae60`: "coastal receivers — ports and shipping lanes".
- **Ship track (around y=190):** filled `#1a5276` dots (radius 4, spaced 22px) in two segments — x=60 to 280 (gently descending) and x=470 to 660 (gently rising); 11px labels "AIS on" (at x=150) and "AIS on again" (at x=565).
- **Dark period:** dashed (6/5) 2px `#e74c3c` line bridging the gap between segments; a `#e74c3c` rectangle outlining the gap region; bold 12px `#e74c3c` centered above: "DARK PERIOD"; below in 11px `#666`: "36 h of silence near a sanctioned port —" / "flagged by enforcement analysts".
- **Caption (12px `#999`, centered, y = h−14):** "Turning the transponder off does not hide the ship — it highlights it"

## 3. Transit & city — feeds published to be consumed

Cities sit between the two worlds: transit data is deliberately published, but in broadcast style — one open feed, unknown consumers.

- **GTFS:** the de-facto standard for schedules, stops, and routes; transit agencies worldwide publish it so any trip planner can ingest it.
- **GTFS-realtime:** live vehicle positions, delays, and service alerts — every "where is my bus" app is a thin client over the agency's own feed.
- **One feed, many products:** the same stream powers trip planners, arrival boards, accessibility routers, and academic studies of system reliability — the agency never sees who consumes it.
- **Cameras and webcams:** public traffic cameras and open webcams stream continuously; anyone can archive them, and computer vision turns "watchable" into "queryable".

Key point: Publishing one open feed outsources the entire application layer: the agency maintains the data, strangers build every interface. The cost is that downstream use — including archival and analysis the agency never intended — is uncontrollable by construction.

### Visualization (canvas `c3`, 720×340)

Fan-out diagram: one agency feed box on the left connecting to five downstream consumer boxes on the right.

- **Title (bold 14px `#1a5276`, centered, y=22):** "One published feed, an entire ecosystem of unknown consumers"
- **Agency box:** 200×110 at (40, 110), fill `rgba(26,82,118,0.12)`, 2px `#1a5276` border. Bold 12px `#1a5276` "TRANSIT AGENCY"; 11px `#555` lines: "GTFS: routes, stops, schedules" / "GTFS-realtime: vehicle positions," / "delays, service alerts"; 11px `#999`: "one open URL, no signup".
- **Connectors:** 1.5px `#bbb` lines from the agency box's right edge to each downstream box.
- **Downstream boxes (each 290×40 at x=400, white fill, 2px border in box color; bold 12px label in box color, 11px `#666` subline):**
  - y=52 "Trip-planner apps" `#27ae60` — "routing over the schedule"
  - y=106 "\"Where is my bus\" apps" `#27ae60` — "thin client on realtime feed"
  - y=160 "Station arrival boards" `#e67e22` — "same feed, rendered on-site"
  - y=214 "Researchers & archives" `#8e44ad` — "reliability studies, history kept forever"
  - y=268 "Uses nobody predicted" `#e74c3c` — "the agency never sees who consumes it"
- **Caption (12px `#999`, centered, y = h−14):** "The agency maintains the data; strangers build every interface on top of it"

## 4. Earth's instruments — the planet on a public feed

The physical world itself is instrumented, and the readings go public within minutes of the event.

- **Earthquakes:** USGS publishes detected events — location, magnitude, depth — on public feeds within minutes of the shaking.
- **Lightning:** global detection networks triangulate every strike from radio emissions; the strike announces itself, the network just listens.
- **Rivers and air:** gauge heights and streamflow update near-continuously; air-quality data mixes official monitors with crowdsensed networks like PurpleAir, where individuals volunteer their backyard sensors into a public map.
- **Archived by everyone:** each feed is mirrored by researchers, apps, and hobbyists the moment it publishes — the dataset exists in thousands of copies from day one.

Key point: For broadcast and sensor data there is no take-back: once a signal is emitted and archived by third parties, deletion is impossible — not because anyone refuses, but because there was never a single custodian to delete from.

### Visualization (canvas `c4`, 720×360)

Event-to-feed timeline (earthquake example) plus a row of three other-feed boxes below.

- **Title (bold 14px `#1a5276`, centered, y=22):** "From physical event to public dataset in minutes"
- **Timeline:** 2px `#999` line at y=130 from x=50 to x=670 with a filled right-arrowhead.
- **Steps (filled dot radius 7 in step color on the line; labels alternate above/below with thin `#ccc` connectors; label bold 12px in step color, subline 11px `#666`, time tag 10px `#999` on the opposite side of the line):**
  - x=90, "t = 0", "Earthquake" — "ground shakes" — `#e74c3c` (above)
  - x=235, "+ seconds", "Seismometers trigger" — "network detects waves" — `#e67e22` (below)
  - x=385, "+ 1-2 min", "Location + magnitude" — "auto-computed solution" — `#1a5276` (above)
  - x=535, "+ minutes", "USGS public feed" — "apps, alerts, mirrors" — `#27ae60` (below)
  - x=650, "forever", "Archived" — "thousands of copies" — `#8e44ad` (above)
- **Other feeds row (each 195×62 at y=230, white fill, 2px border in box color; bold 12px label, subline in 11px `#666` split across two lines at the word midpoint):**
  - x=60 "Lightning networks" `#e67e22` — "every strike triangulated from its own radio burst"
  - x=275 "River gauges" `#1a5276` — "stage + streamflow, updated continuously"
  - x=490 "Air quality" `#27ae60` — "official monitors + crowdsensed PurpleAir"
- **Caption (12px `#999`, centered, y = h−14):** "Same pattern everywhere: the event emits, the network hears, the feed publishes — no custodian in between"

## Regeneration instructions

- **Template:** backlog multi-section detail page. After h1, `.subtitle`, and `.intro` callout, one `.lang-section` per numbered section: `h2` (1.3rem `#1a5276`, `border-bottom: 2px solid #2980b9`, 4px padding-bottom), then a `table.layout` with one `<tr>`: left `<td class="text-col">` (45%) with lead paragraph, bullets, `.key-point`; right `<td class="viz-col">` (55%) with the canvas. Section h2 headings carry the "1.–4." numbers shown above.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with `border-bottom: 2px solid #2980b9`; `.subtitle` `#666` 0.95rem; `.intro` — background `#f0f4f8`, `border-left: 3px solid #2980b9`, padding 8px 12px, 0.9rem, 32px margin-bottom; `.key-point` — background `#f8f9fa`, `border-left: 3px solid #e74c3c`, padding 8px 12px, 0.9rem; `ul` 0.92rem; canvases `width: 100%`, `border: 1px solid #e0e0e0`, radius 4px; `.lang-section` 40px margin-bottom.
- **Canvases:** intrinsic width 720, heights 400/380/340/360 as specified; shared `setupCanvas(id, h)` helper scales by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). Canvas fonts use `-apple-system, sans-serif`.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`; grays `#555`/`#666`/`#888`/`#999`/`#bbb`/`#ccc`; translucent fills `rgba(26,82,118,0.12)`, `rgba(26,82,118,0.35)`, `rgba(39,174,96,0.10)`, `rgba(142,68,173,0.12)`.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
