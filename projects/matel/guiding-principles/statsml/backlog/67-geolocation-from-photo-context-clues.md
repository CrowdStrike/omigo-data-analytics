# Geolocation from Photo Context Clues

**Page type:** detail page (backlog-style two-column layout: numbered h2 sections, text left ~45%, canvas right ~55%)
**HTML title tag:** Geolocation from Photo Context Clues

**Subtitle:** Extracting geospatial signal from unstructured visual data — a pure inference problem where every pixel encodes location constraints.

## Intro callout

Every pixel encodes a location constraint: sun angle, vegetation, architecture, signage, star field. No single clue is precise, but the clues are largely independent, so their intersection shrinks "anywhere on Earth" to a neighbourhood. Stripping EXIF removes the easy answer, not this hard one.

## 1. What is it?

Determining where a photo was taken using only what is visible in the image — no EXIF metadata, no GPS tag, no filename hints. Every element encodes geography: sun angle constrains latitude, vegetation narrows climate zone, architecture pins country, and signage identifies city.

It works by elimination. Each visual clue rules out large swathes of the globe. Stack enough of them and the candidate region shrinks from "anywhere on Earth" to a neighbourhood.

**Key point callout:** This is also a privacy problem. Stripping EXIF from a photo removes the easy answer — it does not remove the hard answer. A trained analyst (or model) can still geolocate from what the image shows.

### Visualization (canvas `c1`, 720×300)

Annotated schematic "photo frame" showing clue regions inside a stylized scene.

- **Frame:** light gray `#eee` rectangle 600×210 at (60,15), stroked `#1a5276` width 2.
- **Sky band:** `rgba(135,206,235,0.4)` strip across the top 70px; upper-left 180×70 corner overlaid dark night sky `rgba(20,30,60,0.7)`.
- **Stars:** nine white 2px dots at (85,30), (100,50), (120,35), (140,55), (110,65), (150,28), (170,45), (130,22), (95,42); a faint white (`rgba(255,255,255,0.4)`) polyline connecting (85,30)→(120,35)→(140,55)→(100,50)→(85,30) suggests a constellation.
- **Sun:** orange `#e67e22` filled circle radius 20 at (520,50).
- **Pole and shadow:** vertical `#555` line (width 3) from (350,200) to (350,120); shadow line `rgba(0,0,0,0.3)` (width 2) from (350,200) to (280,210).
- **Vegetation zone:** `rgba(39,174,96,0.3)` rectangle 200×73 at (62,150).
- **Buildings:** `rgba(26,82,118,0.2)` rectangles 80×130 at (420,90) and 60×110 at (520,110).
- **Sign:** white 60×30 box at (150,100) with `#1a5276` border, centered bold 10px text "R. 25" in `#1a5276`.
- **Annotation callouts (bold 11px, left-aligned):** "☀ Sun angle → latitude" orange `#e67e22` at (545,45); "Sky haze → altitude/coast" `#1a5276` at (250,80); "★ Stars → lat + time" white at (65,80); "Vegetation → climate zone" green `#27ae60` at (65,147); "Buildings → country" `#1a5276` at (420,85); "Shadow → time + bearing" `#666` at (260,220); "Sign → language/region" red `#e74c3c` at (135,140).

## 2. What signals are available?

- **Sun & shadows:** Shadow direction → compass bearing. Shadow length → sun elevation → latitude band (given date). Combined → specific latitude window.
- **Sky & atmosphere:** Haze, cloud type, light colour temperature. Coastal, desert, and high-altitude skies look different.
- **Vegetation:** Grass species, tree shape, leaf type, seasonal state. Brown grass = dry climate or dormant winter. Palm trees ≠ birch trees.
- **Architecture:** Roof pitch, building material, window style, power line design, road markings, curb shape. Strong regional fingerprint.
- **Signage & text:** Script family, font style, traffic sign convention, commercial branding. Even partial text constrains language.
- **Terrain:** Rock colour, soil type, mountain profile, coastline shape. Red laterite soil is not gray glacial clay.
- **Star placement:** The visible star field is a unique fingerprint for latitude + date + time. Polaris altitude = latitude. Constellation rotation encodes time of year. A clear night sky is one of the strongest single-image signals available.

**Key point callout:** No single signal is precise. Sun angle alone gives a latitude band thousands of kilometres wide. But signals are largely independent, so their intersection is much smaller than any one constraint.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart of elimination power per signal.

- **Title (bold 13px, `#1a5276`, top center):** "% of Earth's surface eliminated by each signal (illustrative)".
- **Data (label, %, bar color — drawn at 60% alpha with a solid 1px border in the same color):**
  - "Star field" 92% — `#2c3e50`
  - "Signage/text" 95% — `#e74c3c`
  - "Architecture" 90% — `#1a5276`
  - "Vegetation" 82% — `#27ae60`
  - "Sun position" 75% — `#e67e22`
  - "Terrain/soil" 70% — `#8e44ad`
  - "Sky/atmosphere" 60% — `#2980b9`
- **Layout:** rows 22px tall with 6px gaps starting at y=32; right-aligned 13px labels ending at x=130; bar track background `#f0f0f0` 380px = 100%; bold 12px percentage label ("92%", …) right of each bar in `#2c3e50`.
- **Note (11px `#999`, bottom center):** "Each signal independently eliminates most of the planet".

## 3. How does narrowing work?

Each clue rules out places that don't match. The answer is whatever geography survives all the filters at once.

**Signal 1 (sun angle):** Eliminates wrong hemisphere, narrows to ±15° latitude band.

**+ Signal 2 (vegetation):** Eliminates wrong climate zones within that band. Desert vs. temperate vs. tropical.

**+ Signal 3 (architecture):** Narrows to a country or small group of countries.

**+ Signal 4 (signage):** Pins to a language region, often a single country.

**+ Signal 5 (star field):** Visible constellations + their rotation angle encode latitude and date/time. Among the tightest single-image constraints.

**+ Signal 6 (specific landmarks):** If visible, resolves to a city or street.

**Key point callout:** This is constraint satisfaction, not pattern matching. Each signal is a filter. The answer is what survives all filters — not what matches one filter best.

### Visualization (canvas `c3`, 720×300)

Vertical funnel of centered bars shrinking in width, one per accumulated signal.

- **Title (bold 13px, `#1a5276`, top center):** "Candidate region shrinks with each signal (illustrative)".
- **Stages (label left, area right, bar width in px, centered horizontally; rows 26px tall + 3px gap starting y=32):**
  - "Start" — "510M km²" — width 580
  - "+Sun angle" — "~85M km²" — width 400
  - "+Vegetation" — "~15M km²" — width 260
  - "+Architecture" — "~2M km²" — width 140
  - "+Star field" — "~200K km²" — width 80
  - "+Signage" — "~50K km²" — width 50
  - "+Landmarks" — "~5 km²" — width 12
- **Bar style:** horizontal gradient `rgba(26,82,118,0.15)` → `rgba(26,82,118,0.35)` (center) → `rgba(26,82,118,0.15)`, stroked `#1a5276` width 1. Stage labels 12px `#2c3e50` right-aligned left of each bar; area values 12px `#666` right of each bar.
- **Arrow:** vertical red `#e74c3c` line (width 2) at cx+310 running the funnel's height, ending in a filled red arrowhead, with 11px centered label "narrowing" below.

## 4. Resolution limits & failure modes

Best case (rich urban scene with signage): street-level. Worst case (featureless desert or ocean): hemisphere only.

**Common failures:**

- Signals conflict — vegetation says tropics, architecture says temperate (greenhouse, transplanted species)
- Globalised infrastructure — same road markings used in multiple countries
- Indoor photos — almost no geospatial signal unless window view or unique object visible
- Overconfidence — mistaking "consistent with" for "uniquely identifies"

**Key point callout:** Applications: GeoGuessr competitive play, OSINT investigations, journalism source verification, missing persons cases, wildlife photo provenance. The same skill that makes a game entertaining also makes "anonymous" photo sharing less anonymous than assumed.

### Visualization (canvas `c4`, 720×300)

Range plot: achievable resolution per scene type on an ordinal scale.

- **Title (bold 13px, `#1a5276`, top center):** "Resolution achievable by scene type (illustrative)".
- **Scale:** horizontal `#ccc` line at y=60 from x=100 spanning 520px, with `#999` ticks and 10px `#666` labels evenly spaced: "Hemisphere", "Continent", "Country", "Region", "City", "Street" (positions 0–5).
- **Scene rows (28px apart starting y=95; each a 16px-tall range bar at 50% alpha with 1.5px solid border and 4px filled endpoint dots in the row color; min/max in scale units 0–5):**
  - "Open ocean/desert" 0–1 — `#e74c3c`
  - "Rural landscape" 0.5–2.5 — `#e67e22`
  - "Suburban scene" 2–4 — `#f39c12`
  - "Urban, no text" 2.5–4 — `#27ae60`
  - "Urban with signage" 3.5–5 — `#1a5276`
- **Row labels:** 12px `#2c3e50`, right-aligned left of the scale start.
- **Note (11px `#999`, bottom center):** "Range shows typical best to worst case for each scene type".

## Regeneration instructions

- **Layout:** backlog-style detail page. h1, `.subtitle`, one `.intro` callout, then one `.lang-section` per numbered h2; inside each, `table.layout` with a single row: left `td.text-col` (45%) holding paragraphs/bullets and a `.key-point` callout, right `td.viz-col` (55%) holding the canvas.
- **Page CSS:** body system sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with `border-bottom: 2px solid #2980b9`; section h2 1.3rem `#1a5276` with the same 2px `#2980b9` bottom border; subtitle `#666` 0.95rem; bullets 0.92rem.
- **Callouts:** `.intro` — background `#f0f4f8`, left border `3px solid #2980b9`, padding 8px 12px, 0.9rem. `.key-point` — background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem.
- **Canvas:** each declared 720×300 with `width: 100%`, border `1px solid #e0e0e0`, radius 4px; scaled via a shared `setupCanvas(id)` helper using `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates).
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, amber `#f39c12`, bar fill `rgba(26,82,118,0.35)`. No nav bar, no back/home links. (Any links in regenerated HTML use `.html` extensions.)
