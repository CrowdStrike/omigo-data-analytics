# Space & Satellites

**Page type:** detail page (one h2 per pitfall, each followed by a two-column obj-table row: labeled bullets left ~40%, canvas right ~60%)
**HTML title tag:** Space & Satellites — Domain-Specific Pitfalls

**Subtitle:** Domain-specific pitfalls in satellite data analytics.

## Downlink Bandwidth Limits

**Obj-title:** Downlink Bandwidth Limits

- **The bottleneck:** A satellite collects ~100 TB of raw data per orbit, far beyond what it can send down.
- **Downlink cap:** Only ~1 TB moves per ground-station pass, so the link, not the sensor, sets the limit.
- **99% discarded:** Data is dropped or aggressively compressed on-board before it ever leaves the spacecraft.
- **Stale logic:** The prioritization rules doing that filtering were uploaded days or weeks earlier.
- **Blind decisions:** The keep/discard logic runs with no knowledge of current intelligence needs.
- **No fleet awareness:** It also cannot know what other satellites in the constellation already collected.
- **The result:** Analysts get a narrow, pre-filtered slice — never the full picture the sensor captured.

### Visualization (canvas `canvas1`, 720×200)

Two horizontal proportional bars: data collected vs data transmitted.

- **Background:** `#f8f9fa`. **Title (17px `#1a5276`, centered at y=22):** "Data Collected vs. Data Transmitted Per Orbit".
- **Collected bar:** at (80,50), 600×45px, fill red `#e74c3c`, stroke `#c0392b`, with centered white 17px label "Collected: ~100 TB/orbit".
- **Transmitted bar:** below at (80,115), width 1% of 600px (min 8px)×45px, fill green `#27ae60`, stroke `#1e8449`, with 14px `#222` label to its right: "Transmitted: ~1 TB/pass".
- **Annotation (17px red, centered below bars):** "99% of data LOST — never reaches ground".
- **Gap indicator:** gray `#7f8c8d` dashed (4/3) horizontal line between the bars spanning the full collected-bar width.
- **Legend (14px, far left):** red square "Lost", green square "Sent" (labels `#222`).

## Orbital Mechanics Constrain Collection

**Obj-title:** Orbital Mechanics Constrain Collection

- **Sparse passes:** A LEO satellite crosses over a given point only once every several hours to days.
- **Brief window:** Each pass gives an imaging window of seconds to minutes, then the target is gone.
- **No real-time:** Continuous monitoring is fundamentally impossible for most of the Earth most of the time.
- **Revisit range:** 12 hours for agile high-priority constellations, 5+ days for single-satellite systems.
- **Patchwork:** Analysts fill the gaps with interpolation and fusion between the momentary glimpses.
- **Narrative risk:** The stitched story reads as continuous observation, which it never was.

### Visualization (canvas `canvas2`, 720×200)

Timeline of a 48-hour window with brief imaging-pass spikes over a no-coverage background.

- **Background:** `#f8f9fa`. **Title (17px `#1a5276`, centered at y=22):** "Satellite Revisit Windows Over 48 Hours".
- **Timeline:** horizontal `#2c3e50` 2px axis at y=100 from x=60 to x=690, with tick marks and 14px `#555` labels every 6 hours: `0h, 6h, 12h, ..., 48h`.
- **Gap background:** band `rgba(231,76,60,0.1)` from y=45 to y=95 spanning the full timeline (no-coverage region).
- **Pass spikes:** narrow green `#27ae60` vertical bars (~0.4h wide, plus small downward triangle markers on top) at hours `2, 8, 14.5, 21, 27, 33.5, 40, 46.5`.
- **Labels (14px, left-aligned at bottom):** "Green spikes = imaging window (~2-5 min each)" in green; "Red background = no coverage (hours of gap between passes)" in red.
- **Gap annotation (14px `#7f8c8d`, centered near hour 5, above the band):** "~6 hr gap".

## Radiation-Induced Data Corruption

**Obj-title:** Radiation-Induced Data Corruption

- **Bit flips:** Cosmic rays and solar particle events corrupt roughly 1 in 10^6 pixels per image frame.
- **Looks like noise:** Corruption is spatially and temporally random, tied to no sensor pattern.
- **Significant in bulk:** Across large mosaics the corrupted-pixel count is statistically significant.
- **Too sparse to spot:** Per frame it stays sparse enough to resemble ordinary sensor noise.
- **Not calibratable:** Being random in space and time, it cannot be calibrated out of the imagery.
- **ECC limits:** Error-correcting codes catch most multi-bit errors; single-bit flips in data buses slip through.
- **ML risk:** Downstream models may learn these artifacts as features instead of ignoring them.
- **Sparse terrain worst:** The risk peaks in rarely-observed terrain where training data is sparse.

### Visualization (canvas `canvas3`, 720×200)

Simulated grayscale image grid with randomly scattered corrupted pixels highlighted in red.

- **Background:** `#f8f9fa`. **Title (17px `#1a5276`, centered at y=22):** "Radiation-Induced Pixel Corruption (Simulated Image Grid)".
- **Pixel grid:** 85 columns × 20 rows of 7px cells starting at (60,38). Each normal cell is grayscale `rgb(v,v,v)` with v = 120 + 60·sin(c·0.1)·cos(r·0.15) + 30·rand, clamped to 50-255 (seeded LCG, seed 42) — simulating terrain texture.
- **Corrupted pixels:** ~1.2% of cells (seeded LCG, seed 7) filled solid red `#e74c3c` with a bright red `#ff0000` 1px outline.
- **Legend (14px, bottom):** red square + "= Corrupted pixel (bit flip from cosmic ray)" in `#222`; right-aligned gray `#888` note "~1 in 10^6 pixels per frame".

## Ground Truth Inaccessible

**Obj-title:** Ground Truth Inaccessible

- **No site visits:** Detections from 500+ km altitude typically cannot be physically verified on the ground.
- **Resolution limit:** At 0.3-5 m/pixel, "tank or truck?" cannot be answered from the imagery alone.
- **Category ambiguity:** Nor can "refugee camp or construction site?" — both read as gray rectangles.
- **Proxy truth:** Verification needs separate intelligence sources: field agents, drone overflights.
- **Other sources:** Cooperative reporting and social media signals stand in where no agent can be sent.
- **Metrics are estimates:** Model accuracy numbers therefore inherit the error of that proxy truth.
- **Long-lived bias:** Systematic biases can persist undetected for years with nothing to catch them.

### Visualization (canvas `canvas4`, 720×200)

Four side-by-side panels showing the same object at increasing altitude and decreasing clarity.

- **Background:** `#f8f9fa`. **Title (17px `#1a5276`, centered at y=22):** "Object Recognition vs. Altitude — Resolution Degradation".
- **Panels:** four 145×110px boxes (fill `#ecf0f1`, border `#2980b9` 1px), centered horizontally with 15px gaps, top y=42:
  1. "1 km (drone)" / "(5 cm/px)" — clarity 1.0: crisp dark truck silhouette (`#2c3e50` body, cab, `#555` wheels); caption "Clearly a truck".
  2. "10 km (aircraft)" / "(30 cm/px)" — clarity 0.6: blurry semi-transparent rectangles (`rgba(44,62,80,0.5)` and 0.3); caption "Vehicle type unclear".
  3. "200 km (LEO)" / "(1 m/px)" — clarity 0.3: concentric translucent blobs `rgba(44,62,80,0.25)`; caption "Something there?".
  4. "500 km (typical sat)" / "(3 m/px)" — clarity 0.1: barely visible smear (circles at alpha 0.12 and 0.2); caption "Blob — tank or truck?".
- **Panel labels:** altitude + resolution in 12px `#1a5276` below each panel; caption in 11px red `#e74c3c` below that.
- **Degradation arrow:** red `#e74c3c` 2px right-pointing arrow along the bottom spanning panels 2-4, with centered 11px red label "Increasing ambiguity".

## Atmospheric/Weather Interference

**Obj-title:** Atmospheric/Weather Interference

- **Cloud blackout:** Optical sensors are blocked completely by cloud cover — no partial signal gets through.
- **Monsoon gap:** Monsoon season leaves 3+ continuous months essentially uncollectable for optical.
- **SAR isn't equivalent:** Radar penetrates clouds but measures backscatter intensity, not spectral reflectance.
- **Seasonal bias:** Models trained on "available" data over-represent dry-season conditions.
- **Wet season missing:** Those same models under-represent the wet-season reality they rarely observe.
- **Coverage floor:** Southeast Asia, the Amazon basin, and Central Africa see optical coverage on 20-30% of days.
- **Skewed trends:** Trend estimates then describe the sampled clear days, not the actual year.

### Visualization (canvas `canvas5`, 720×200)

Monthly bar chart of optical data completeness for a tropical region, with a monsoon bracket.

- **Background:** `#f8f9fa`. **Title (17px `#1a5276`, centered at y=22):** "Optical Data Completeness — Tropical Region (12 Months)".
- **Chart area:** x=70, y=40, 600×120px, axes `#2c3e50` 1px; horizontal gridlines `#eee` with right-aligned 12px `#555` y-labels at 0%, 25%, 50%, 75%, 100%.
- **Data (completeness % by month):** Jan 72, Feb 68, Mar 55, Apr 40, May 25, Jun 8, Jul 5, Aug 6, Sep 12, Oct 45, Nov 65, Dec 70.
- **Bar colors by value:** <15% red `#e74c3c`; 15-39% orange `#e67e22`; ≥40% green `#27ae60`. Bars outlined `rgba(0,0,0,0.2)`; month labels 11px `#333` below.
- **Monsoon bracket:** red `#e74c3c` 2px bracket under May-Sep with centered 14px red caption "Monsoon Season — near-total optical data loss".

## Constellation Coordination

**Obj-title:** Constellation Coordination

- **Heterogeneous fleet:** Constellations of 100+ satellites differ in sensors and spectral bands.
- **Orbital spread:** Orbital parameters, satellite ages, and calibration states differ across the fleet too.
- **Stitching inputs:** Sun angles, radiometric responses, and ground sample distances vary pass to pass.
- **Constructed view:** The "consistent global view" comes from heavy post-processing, not direct observation.
- **False changes:** Cross-calibration drift between satellites can masquerade as real change on the ground.
- **Seam artifacts:** Swath-boundary mosaicking artifacts trigger false-positive change detections.

### Visualization (canvas `canvas6`, 720×200)

Mosaic of eight adjacent satellite swath strips with visibly different brightness calibration and dashed seam lines.

- **Background:** `#f8f9fa`. **Title (17px `#1a5276`, centered at y=22):** "Satellite Constellation — Cross-Calibration Inconsistency".
- **Strips:** 8 vertical strips, 75px wide × 110px tall, starting y=42, horizontally centered. Each filled with a 4px-block terrain-like texture: base value 130 + 30·sin((py+px)·0.05) plus a per-strip calibration offset and seeded noise (seed 101), rendered as warm-tinted grays `rgb(v, 0.95v, 0.85v)`, clamped 30-255. Strip borders blue `#2980b9` 1.5px.
- **Calibration offsets (DN):** Sat-A `0`, Sat-B `+25`, Sat-C `-10`, Sat-D `+40`, Sat-E `-20`, Sat-F `+15`, Sat-G `-30`, Sat-H `+5` — labeled below each strip: satellite name in 10px `#1a5276`, offset (e.g. "+25 DN") in red `#e74c3c`.
- **Seams:** dashed red `rgba(231,76,60,0.7)` 2px vertical lines (dash 3/3) at every boundary between strips.
- **Legend (14px, bottom):** "Red dashed lines = swath boundaries with calibration discontinuities" in red, left-aligned; "DN = digital number offset" in `#555`, right-aligned.

## Regeneration instructions

- **Layout:** standard domains detail page. h1, `.subtitle` paragraph, then one `h2` per pitfall (six total) followed by a `.obj-table` (full-width, border-collapse) with a single `<tr>`: left `<td>` (40%) holding `.obj-title` div + a `<ul>` of labeled bullets (each `<li>` starting with a bold `<strong>` label), right `<td>` (60%, centered) holding the canvas. No nav bar, no back/home links, no thead, no badges.
- **Page CSS:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6. h1 1.8em `#1a5276`. h2 1.4em `#1a5276` with 2px bottom border `#2980b9`, padding-bottom 8px, margin 40px 0 15px. `.subtitle` `#666` 1.05em. `ul` 0.9em `#333`. `.obj-table td` border `1px solid #e0e0e0`, padding 20px 24px; even rows background `#fafcfe`. `.obj-title` 1.05em weight 600 `#1a5276`. `strong` `#1a5276`. `.philosophy` style defined but unused.
- **Canvases:** each 720×200 with inline `style="width:720px;height:200px"`; shared `setupCanvas(id)` sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`), and calls `ctx.scale` so drawing stays in logical coordinates. Chart fonts: 17px system for titles (`CHART_FONT`), 14px for legends (`CHART_FONT_SMALL`). Textured charts use seeded LCG generators (multiplier 1664525, increment 1013904223) for determinism.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, secondary blue `#2980b9`, dark red `#c0392b`, dark green `#1e8449`, axis dark `#2c3e50`, gray text `#555`/`#666`/`#888`.
- In regenerated HTML, any card links use `.html` extensions (this page has none).
