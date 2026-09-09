# Imagery & Mapping — The Surface of the Planet, Browsable

**Page type:** detail page (two-column layout table per section: text left 45%, canvas right 55%, one `.lang-section` per topic)
**HTML title tag:** Imagery & Mapping — The Surface of the Planet, Browsable

**Subtitle:** Street-level, aerial, and satellite imagery plus open map data make every address and every acre visually inspectable by anyone — a permanent, dated, searchable photographic record of the physical world.

**Intro callout:** Most public records are text: names, dates, amounts. Imagery is different — it shows what a place actually looks like, and it is captured systematically rather than on request. Corporate camera fleets photograph every public street, government satellite programs image every acre every few days, and volunteers maintain an editable map of record. Together they form a visual layer that any other address-keyed record can be draped over.

## 1. Street-level — every public street, photographed and re-photographed

Camera fleets drive public roads on a repeating cycle, so the same address accumulates a photographic history:

- **Google Street View** — the archetype: fleet cars photograph public streets, faces and license plates are auto-blurred, panoramas published for nearly every road in covered countries.
- **The equivalents** — Apple Look Around, Yandex Panoramas in Russia/CIS, Baidu and Tencent street maps in China, Kakao Road View in Korea. Coverage follows the local market leader, not one global provider.
- **Historical slider** — providers keep prior capture passes, so one address can be viewed across a decade: renovations, new fences, cars in the driveway, businesses opening and closing.
- **Legal basis** — the photos are taken from public roads; no permission from the property owner is needed or asked.

**Key point:** Street-level imagery is a time series, not a snapshot. The slider turns "what does this address look like" into "what has this address looked like, year by year" — a change log nobody at the address opted into.

### Visualization (canvas `c1`, 720×340)

Time-lapse strip diagram: four photo panels of the same address across capture passes, connected by arrows.

- **Title (bold 14px `#1a5276`, top center):** "One address, four capture passes — the historical imagery slider".
- **Panels:** 4 panels, each 155px wide × 190px tall, starting at left=35, top=52, 15px gaps, arrows in `#bbb` between consecutive panels at mid-height. Each panel: white fill, 2px colored border, light-blue sky band `rgba(26,82,118,0.10)` across the top 46px, and a simple line-drawn house sketch (`#666`, 1.5px): rectangle body 80px wide (height 40px for 2011, 62px from 2015 onward — second floor added), triangular roof, door 16×22, ground line. A purple (`#8e44ad`) picket fence (4 short vertical strokes) appears on the 2019 and 2023 panels only. A solid colored year tab (24px tall, white bold 12px year text) spans the panel bottom.
- **Panel data (year, border/tab color, bullet notes below each panel in 10px `#666`):**
  - 2011, `#1a5276`: "original facade" / "old storefront sign" / "street trees young"
  - 2015, `#27ae60`: "second floor added" / "new business name" / "driveway repaved"
  - 2019, `#e67e22`: "fence installed" / "solar panels visible" / "shop now vacant"
  - 2023, `#e74c3c`: "full renovation" / "new tenants" / "EV charger out front"
- **Caption (12px `#999`, bottom center):** "Nobody at the address requested any of these captures — the fleet just came back".

## 2. Satellite — open by program, sub-meter by purchase

Overhead imagery moved from state secret to public utility in one lifetime:

- **Landsat** — a 50+ year archive of the whole planet; the full archive became free to download in 2008, and usage exploded overnight.
- **ESA Sentinel / Copernicus** — fresh multi-band imagery of everywhere on Earth every few days, free and open by program mandate; the backbone of crop, deforestation, and flood monitoring.
- **Commercial sub-meter** — Maxar, Planet, Airbus sell imagery sharp enough to count cars and read construction progress; any journalist or analyst can task a satellite over a coordinate.
- **Open-source intelligence** — OSINT analysts track troop movements, sanctions-busting tankers, and factory activity from open and commercial feeds, publishing findings that once required an intelligence agency.

**Key point:** Resolution that was classified military capability decades ago is now a free download. The barrier is no longer access to the pixels — it is knowing where to look and what a change means.

### Visualization (canvas `c2`, 720×340)

Timeline with rising bars: access level increasing across four eras.

- **Title (bold 14px `#1a5276`, top center):** "Overhead imagery: from state secret to free global download".
- **Timeline axis:** horizontal `#999` line at y=230 from x=60 to x=680 with arrowhead at right end; tick and bold 12px year label under each era.
- **Bars:** at each era x-position, a rising bar 28px wide, fill `rgba(26,82,118,0.35)`, 2px stroke in the era color, height = access level; era title in bold 12px era color above the bar; 4 detail lines in 10px `#666` below the axis.
- **Eras (x, year, title, color, bar height, detail lines):**
  - x=105, "1960s", "Classified", `#e74c3c`, 70px: "spy satellites (CORONA)" / "film capsules dropped" / "from orbit; existence" / "itself was secret"
  - x=250, "1972", "Government program", `#e67e22`, 100px: "Landsat launches:" / "civilian earth imaging," / "imagery sold per scene" / "to agencies + researchers"
  - x=395, "2008", "Free archive", `#27ae60`, 130px: "entire Landsat archive" / "opened at no cost;" / "downloads jump from" / "thousands to millions"
  - x=540, "2014+", "Open + commercial", `#1a5276`, 160px: "Sentinel: everywhere," / "every few days, free;" / "sub-meter commercial" / "shots purchasable by anyone"
- **Y-axis label (rotated vertical, 11px `#999`, left side):** "who can see it".
- **Caption (12px `#999`, bottom center):** "OSINT analysts now track wars and supply chains from feeds anyone can open".

## 3. Open map data — the crowdsourced map of record

Alongside corporate imagery sits an openly licensed map anyone can edit and everyone can download:

- **OpenStreetMap** — roads, buildings, points of interest, land use; editable by anyone, and every edit's full history is public, including who changed what and when.
- **Crowdsourced street photos** — Mapillary and KartaView let anyone upload dashcam or phone imagery, filling streets the corporate fleets skip and refreshing them more often.
- **Government layers folded in** — cadastral parcel boundaries, elevation models, and address registries released as open data get traced or imported into the shared map.
- **Downstream everywhere** — delivery apps, humanitarian response, and research pipelines all build on the same open base, so an edit propagates far beyond the map site itself.

**Key point:** The map is assembled, not issued: independent layers from volunteers, companies, and governments stack on shared coordinates. No single custodian controls it — and no single custodian can retract it.

### Visualization (canvas `c3`, 720×400)

Layer-stack diagram: five labeled parallelogram map layers on the left, connectors converging into one compressed merged layer stack on the right.

- **Title (bold 14px `#1a5276`, top center):** "Independent open layers stack into one map".
- **Left layers:** parallelograms (260px wide, 38px tall, slanted 40px), fill `rgba(26,82,118,0.08)`, 2px stroke in layer color, at x=50; bold 12px layer name in layer color plus 10px `#666` source line; `#bbb` connector lines from each layer to the merged map at (415,200).
  - "Crowd street photos" / "Mapillary / KartaView volunteers" — `#8e44ad`, y=60
  - "Points of interest" / "OSM editors: shops, schools, clinics" — `#e67e22`, y=125
  - "Buildings + parcels" / "traced imagery + open cadastre imports" — `#e74c3c`, y=190
  - "Roads + paths" / "OSM base network, full edit history public" — `#27ae60`, y=255
  - "Terrain + elevation" / "government open elevation models" — `#1a5276`, y=320
- **Right merged map:** at x=420, width 260, five compressed parallelograms stacked 22px apart starting at y=150, each stroked in the corresponding layer color (reversed order), top layer filled `rgba(26,82,118,0.15)`, others `rgba(26,82,118,0.06)`. Heading above in bold 13px `#1a5276`: "The shared open map"; two 11px `#666` lines below: "same coordinates, no single custodian" / "downloadable in full, by anyone".
- **Caption (12px `#999`, bottom center):** "Every edit is public and versioned — the map has a changelog like source code".

## 4. What imagery joins to — the visual join key

An address in imagery links visual context to every other public record keyed by that address:

- **Records gain a face** — a deed, a permit, a business registration, a court filing: each becomes concrete when you can see the building it refers to and how it changed.
- **Geoguessing** — photos with no metadata are routinely located from context clues alone: signage language, road markings, vegetation, sun angle. Communities do it competitively in minutes.
- **Privacy asymmetry** — owners can request blurring of their house, but the request itself is visible, and the unblurred archive persists at the provider; removal changes the display, not the record.
- **Cross-checking** — imagery verifies or contradicts text records: a "vacant" parcel with a building on it, a permit-free extension visible on the slider.

**Key point:** Imagery is the join key that turns text records into physical-world knowledge. It answers "what does this place actually look like" — a question no registry field answers — and once joined, every record inherits that answer.

### Visualization (canvas `c4`, 720×400)

Hub-and-spoke diagram: central street-level photo panel connected to six surrounding record-type boxes.

- **Title (bold 14px `#1a5276`, top center):** "Imagery as the join key: one photo, every address-keyed record".
- **Center panel:** 170×130 rectangle centered at (360, 205), fill `rgba(26,82,118,0.12)`, 2.5px `#1a5276` stroke, containing a small line-drawn house (60×34 body plus roof), bold 12px label "STREET-LEVEL PHOTO", and two 10px `#666` lines: "one address, seen" / "and dated".
- **Record boxes:** 190×48 white rectangles with 2px colored stroke, bold 12px name in the color plus 10px `#666` subtitle; `#bbb` connector lines from each to the center.
  - "Property records" / "deed, assessment, permits" — `#27ae60`, at (60,60)
  - "Business filings" / "registration at this address" — `#e67e22`, at (470,60)
  - "Court + liens" / "filings naming the address" — `#e74c3c`, at (40,185)
  - "Map data" / "parcel, POI, elevation" — `#8e44ad`, at (490,185)
  - "Listings + photos" / "interior shots, price history" — `#1a5276`, at (60,310)
  - "Satellite history" / "the lot, year by year" — `#1a5276`, at (470,310)
- **Caption (12px `#999`, bottom center):** "Blur-on-request hides the display, not the archive — and no text field answers \"what does it look like\"".

## Regeneration instructions

- **Layout:** backlog detail page (kusto-style 2-col): h1, `.subtitle`, `.intro` callout, then one `.lang-section` per numbered topic. Each section: `<h2>` with 2px `#2980b9` bottom border, then a `table.layout` (border-collapse, full width) with one row: `td.text-col` (45%) holding an intro sentence, a `<ul>` of labeled bullets (bold lead terms), and a `.key-point` div; `td.viz-col` (55%) holding the canvas. No index number in the h1.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; `.intro` background `#f0f4f8` with 3px `#2980b9` left border; `.key-point` background `#f8f9fa` with 3px `#e74c3c` left border; ul 0.92rem; `strong` inherits bullet color. Canvases `width: 100%`, 1px `#e0e0e0` border, 4px radius.
- **Canvases:** intrinsic width 720, heights as given per chart (340/340/400/400); shared `setupCanvas(id, h)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple accent `#8e44ad`, bar fill `rgba(26,82,118,0.35)`, gray text `#666`/`#999`.
- In regenerated HTML, any card links use `.html` extensions (this page has none).
