# Wildfire & Flood Spread

**Page type:** detail page (four titled sections, each a two-column row: text left 50%, canvas right 50%)
**HTML title tag:** Wildfire & Flood Spread — Fire Over Fuel, Water Over Terrain

**Subtitle:** Both are spread processes over a landscape grid — fire propagates by fuel, wind, and slope; water routes downhill over elevation data — and both inherit their biggest uncertainty from the weather forecast upstream.

**Intro callout (blue-left-border box):** Wildfire and flood models share a shape: a physical spread rule applied cell by cell over terrain, driven by a weather forecast, feeding an evacuation decision with a hard deadline. The fire rule is combustion physics over a fuel map; the flood rule is water flowing downhill over an elevation grid. In both cases the model is only the middle of a chain — the weather forecast feeds it, and a go/no-go evacuation call consumes it.

## 1. Fire behavior: fuel, wind, slope

Operational fire models descend from the Rothermel line of spread-rate models: given fuel type, fuel moisture, wind speed, and slope, compute how fast the fire line advances.

- **Rothermel line:** spread rate is computed from fuel, moisture, wind, and slope.
- **Uphill acceleration:** flames preheat the fuel above them, so fire runs upslope.
- **Wind alignment:** spread is fastest where wind and slope push the same way.
- **Elliptical growth:** the perimeter grows as an ellipse stretched downwind.
- **Huygens wavefront:** each perimeter point spawns its own small ellipse.
- **Stale inputs:** fuel maps and moisture estimates lag the real landscape.
- **The output:** perimeter positions by the hour for crews and planners.

Key point: The physics is local and simple — rate of spread per cell — but the inputs are perpetually stale: the fuel map was surveyed years ago, and the moisture estimate is an interpolation. The model is exact arithmetic on approximate ground truth.

### Visualization (canvas `c1`, 720×360)

Terrain side-profile with fire perimeter positions at successive hours, spacing widening on the uphill/downwind side, with a wind arrow and slope annotation.

- **Title (bold 14px `#1a5276`, centered, y=22):** "Fire runs uphill and downwind — hourly perimeter positions spread apart"
- **Terrain profile:** filled polygon under a polyline from (50, 300) → (300, 272) → (670, 132), closed via (670, 320) and (50, 320); fill `rgba(26,82,118,0.12)`, top edge stroked 2px `#999`. Terrain height is linear on each segment: y(x) = 300 − 0.112×(x−50) for x ≤ 300, then y(x) = 272 − 0.3784×(x−300).
- **Flame markers:** filled `#e74c3c` triangles (base 12 wide, 16 tall, apex up) sitting on the terrain at x = 120, 190, 280, 400, 560 — triangle base center at (x, terrain(x)), apex at (x, terrain(x) − 16). 10px `#666` centered hour labels "0h", "1h", "2h", "3h", "4h" at each x, y = terrain(x) + 16.
- **Wind arrow:** 2.5px `#2980b9` horizontal arrow from (140, 70) to (290, 70) with a filled arrowhead; bold 11px `#2980b9` left-aligned label "wind" at (300, 74).
- **Slope annotation:** bold 11px `#e67e22` two-line note left-aligned at (330, 190) / (330, 206): "flames preheat the fuel upslope —" / "equal hours, widening distance"; thin 1px `#e67e22` connector from (455, 212) to the terrain near (480, 236).
- **Caption (12px `#999`, centered, y = h−14):** "Same fire, same hours — the uphill, downwind side covers more ground every hour"

## 2. Coupled to weather — and to itself

The fire model's largest input is the weather forecast, and its largest omission is the fire's own effect on that weather.

- **Inherited uncertainty:** a wind shift redraws the entire spread map.
- **Forecast dependence:** the fire forecast is only as good as the weather feed.
- **Ember spotting:** lofted embers ignite new fires far ahead of the front.
- **Broken assumption:** spotting breaks the smooth-wavefront picture entirely.
- **Fire-made weather:** pyroconvective plumes generate their own winds.
- **Omitted feedback:** most operational models leave that coupling out.
- **Ensembles:** many wind scenarios yield burn-probability maps, not one line.

Key point: A single predicted perimeter is a statement about one wind forecast; an ensemble of wind scenarios turns the same model into a burn-probability map — the honest product when the dominant input is itself uncertain.

### Visualization (canvas `c2`, 720×380)

Top-down map with two fire-spread fans from the same ignition under two wind scenarios, plus a spot-fire dot beyond the front; the shaded union reads as the burn-probability zone.

- **Title (bold 14px `#1a5276`, centered, y=22):** "Two wind scenarios, two futures — the union is the burn-probability zone"
- **Map frame:** rect (40, 44) 640×280, fill `#fdfdfd`, 1px `#ccc` border.
- **Fire fans:** two ellipses drawn with `ctx.ellipse`, both anchored at the ignition point (150, 190). Scenario A (wind toward upper right): center (341, 131), rx=200, ry=72, rotation −0.30 rad, fill `rgba(230,126,34,0.30)`, stroke 1.5px `#e67e22`. Scenario B (wind toward lower right): center (344, 238), rx=200, ry=72, rotation +0.25 rad, fill `rgba(231,76,60,0.25)`, stroke 1.5px `#e74c3c`. The alpha overlap near the ignition renders darker — that region burns in both scenarios.
- **Ignition:** filled `#555` dot radius 5 at (150, 190); 10px `#555` centered label "ignition" at (150, 212).
- **Wind legend (top right, left-aligned labels at x=560):** 2px `#e67e22` arrow from (520, 66) to (552, 56) with arrowhead + 10px `#666` "wind scenario A" at (560, 62); 2px `#e74c3c` arrow from (520, 90) to (552, 98) with arrowhead + 10px `#666` "wind scenario B" at (560, 94).
- **Spot fire:** filled `#e74c3c` dot radius 4 at (588, 96); dashed (4/3) 1px `#8e44ad` connector from the scenario-A front near (520, 105) to (584, 98); bold 10px `#8e44ad` right-aligned two-line label at (640, 130) / (640, 143): "spot fire — ember ignition" / "beyond the modeled front".
- **Union note:** 11px `#666` centered at (360, 344): "darker overlap burns under both winds — the union is the planning zone".
- **Caption (12px `#999`, centered, y = h−14):** "One model, an ensemble of winds — probability contours instead of a single confident perimeter"

## 3. Floods: routing water over a grid

Flood forecasting is a two-stage chain: rainfall-runoff hydrology converts forecast rain into river inflow, then hydraulic models route that water over elevation grids to map inundation.

- **Two-stage chain:** rainfall-runoff hydrology feeds hydraulic routing.
- **Runoff models:** forecast rain becomes river inflow via the catchment model.
- **Hydraulic routing:** water is routed downhill over a digital elevation grid.
- **The output:** inundation depth, cell by cell, across the floodplain.
- **Terrain dominates:** small elevation errors flip neighborhoods wet or dry.
- **"100-year" misread:** it means 1% chance in any year, not once a century.
- **Back-to-back floods:** independent 1% draws can land in consecutive years.

Key point: The hydraulics is mature; the risk lives in the inputs. Elevation data quality decides which houses the map shows wet, and the "100-year flood" label misleads anyone who reads it as a schedule rather than a 1%-per-year probability.

### Visualization (canvas `c3`, 720×380)

Valley cross-section with river channel, stacked water levels labeled with annual exceedance probabilities, houses on the floodplain, and a note that levels assume current terrain.

- **Title (bold 14px `#1a5276`, centered, y=22):** "One valley, three water levels — each labeled by annual probability, not by schedule"
- **Terrain cross-section:** filled polygon under the polyline (50, 120) → (160, 240) → (300, 255) → (320, 300) → (400, 300) → (420, 255) → (560, 240) → (670, 110), closed via (670, 340) and (50, 340); fill `#f8f9fa`, top edge stroked 2px `#555`.
- **Normal water:** filled `rgba(26,82,118,0.35)` polygon inside the channel: (312, 282) → (408, 282) → (400, 300) → (320, 300); 10px `#2980b9` centered label "normal flow" at (360, 318).
- **AEP levels (dashed horizontal lines, each with a left-aligned label at x=575 in its color, 11px bold):** 10%/yr — dashed (5/4) 1.5px `#2980b9` line at y=250 from x=302 to x=418, label "10% per year" at (575, 254); 1%/yr — dashed (5/4) 1.5px `#e67e22` line at y=226 from x=252 to x=498, label "1% per year (\"100-yr\")" at (575, 230); 0.2%/yr — dashed (5/4) 1.5px `#e74c3c` line at y=206 from x=210 to x=545, label "0.2% per year" at (575, 210).
- **Houses:** two house glyphs on the floodplain — walls 24×16 rects with 1.5px `#555` border and white fill at (218, 226) and (486, 226); roofs filled `#999` triangles from (214, 226) to (246, 226) peaking at (230, 212), and from (482, 226) to (514, 226) peaking at (498, 212).
- **House annotation:** bold 11px `#e74c3c` centered at (360, 62): "both houses sit inside the 1%-per-year zone"; thin 1px `#e74c3c` connectors from (280, 68) to (230, 208) and from (440, 68) to (498, 208).
- **Terrain note:** 11px `#666` centered at (360, 352): "levels assume today's terrain data — small elevation errors flip wet and dry".
- **Caption (12px `#999`, centered, y = h−14):** "A 1%-per-year flood can arrive twice in a decade — the label is a probability, not a promise"

## 4. From model to evacuation

The deliverable is not a map; it is a decision with a deadline — evacuate or hold — and the model earns its keep only if its warning outruns the evacuation itself.

- **The real output:** a decision with a deadline, not a prettier map.
- **Trigger points:** pre-committed thresholds turn forecasts into orders.
- **Fire example:** the fire crossing line X issues the evacuation automatically.
- **Flood example:** river stage Y triggers the siren, not another meeting.
- **Lead-time test:** model warning time must exceed total evacuation time.
- **Decoration risk:** a perfect forecast that arrives late is worth nothing.
- **Asymmetric costs:** false alarms are expensive; misses are catastrophic.

Key point: Trigger points move the decision out of the panic window: the argument about when to evacuate happens calmly, in advance, and the model's job narrows to one question — will the trigger be crossed, and how long before impact?

### Visualization (canvas `c4`, 720×340)

Horizontal timeline from model warning to impact, with stacked bars for decision, notification, and evacuation travel time set against the available lead time, and the margin annotated.

- **Title (bold 14px `#1a5276`, centered, y=22):** "The only test that matters: warning lead time minus evacuation time"
- **Trigger note:** bold 11px `#8e44ad` left-aligned at (60, 58): "trigger point crossed (fire line X / river stage Y)"; dashed (4/3) 1px `#8e44ad` vertical line at x=60 from y=64 to y=125.
- **Endpoints:** bold 11px `#1a5276` left-aligned "model warning issued" at (60, 96); bold 11px `#e74c3c` right-aligned "impact" at (660, 96); dashed (4/3) 1.5px `#e74c3c` vertical line at x=660 from y=100 to y=225.
- **Lead-time bar:** rect (60, 108) 600×26, fill `rgba(26,82,118,0.12)`, 1.5px `#1a5276` border; bold 11px `#1a5276` centered label "available lead time" at (360, 125).
- **Needed-time stack:** three abutting rects at y=163, height 26 — decision: x=60–210, fill `rgba(26,82,118,0.35)`; notification: x=210–360, fill `#e67e22`; evacuation travel: x=360–590, fill `#e74c3c`. 10px `#555` centered labels below at y=205: "decision time" at (135, 205), "notification" at (285, 205), "evacuation travel" at (475, 205).
- **Margin:** 2px `#27ae60` double-headed horizontal arrow at y=176 from x=592 to x=658; bold 11px `#27ae60` right-aligned two-line label at (658, 232) / (658, 248): "margin — the model's" / "real deliverable".
- **Failure note:** 11px `#666` centered at (360, 280): "if the stack outgrows the lead time, the forecast is decoration".
- **Caption (12px `#999`, centered, y = h−14):** "Pre-committed triggers spend the lead time on moving people, not on holding meetings"

## Regeneration instructions

- **Template:** backlog multi-section detail page. After h1, `.subtitle`, and `.intro` callout, one `.lang-section` per numbered section: `h2` (1.3rem `#1a5276`, `border-bottom: 2px solid #2980b9`, 4px padding-bottom), then a `table.layout` with one `<tr>`: left `<td class="text-col">` (50%) with lead paragraph, bullets, `.key-point`; right `<td class="viz-col">` (50%) with the canvas. Section h2 headings carry the "1.–4." numbers shown above.
- **Bullet style:** each bullet is a bold label + one short complete sentence that fits on one line at normal page width — no wrapping; split dense content into more bullets rather than longer ones. Bullet `<strong>` labels are colored `#1a5276` (via `li strong { color: #1a5276; }`).
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with `border-bottom: 2px solid #2980b9`; `.subtitle` `#666` 0.95rem; `.intro` — background `#f0f4f8`, `border-left: 3px solid #2980b9`, padding 8px 12px, 0.9rem, 32px margin-bottom; `.key-point` — background `#f8f9fa`, `border-left: 3px solid #e74c3c`, padding 8px 12px, 0.9rem; `ul` 0.92rem; canvases `width: 100%`, `border: 1px solid #e0e0e0`, radius 4px; `.lang-section` 40px margin-bottom.
- **Canvases:** intrinsic width 720, heights 360/380/380/340 as specified; shared `setupCanvas(id, h)` helper scales by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). Canvas fonts use `-apple-system, sans-serif`. Every canvas gets a bold 14px `#1a5276` centered title at y=22 and a 12px `#999` centered caption at y = h−14.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, accent `#2980b9`; grays `#555`/`#666`/`#999`/`#bbb`/`#ccc`; translucent fills `rgba(26,82,118,0.12)`, `rgba(26,82,118,0.35)`, `rgba(230,126,34,0.30)`, `rgba(231,76,60,0.25)`.
- No nav bar, no back/home links, no cross-references to other pages.
