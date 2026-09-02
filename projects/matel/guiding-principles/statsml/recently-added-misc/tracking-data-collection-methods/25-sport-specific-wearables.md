# Tracking Data: Sport-Specific Wearables

**Page type:** grid page (card navigation grid, 3 columns; one header canvas above the grid, numbered cards linking to per-sport detail pages in `sport-specific-wearables/`)
**HTML title tag:** Tracking Data: Sport-Specific Wearables

**Subtitle:** Each sport puts a sensor at the one point the sport is played with — a body part, or the equipment itself. Every device below measures at that single point; the athlete around it is computed.

**Philosophy callout:** **One point stands in for the whole athlete:** a fitness band samples one wrist all day; a sport sensor samples the one point the sport pays for — glove, vest, sleeve, crank arm or ball. Swing, sprint, torque and stroke are computed from that point plus a model of the body around it — and each sport's device fails, and misleads, in its own way.

## Header visualization (canvas `c0`, 720×380, above the grid)

Picture story: one back-view mannequin, five sensor positions — a hue-coded dot at each placement, dashed leader line, label in the sport's hue.

- **Header strip:** tinted ink band `rgba(26,82,118,0.07)` full width, 28px tall; bold 15px ink `#1a5276` centered title "One body — where each sensor sits" (no item count in the title — the card set below may grow).
- **Mannequin (back view, centered at x=330):** head circle (330,86) r20; neck (330,106)→(330,116); shoulder line (270,122)→(390,122); torso trapezoid (272,122)(388,122)(372,232)(288,232), fill 0.06-alpha ink tint, 2px ink stroke; arms as 2px ink polylines (272,124)→(252,190)→(246,250) and (388,124)→(408,190)→(414,250) with small hand circles r6 at (246,256) and (414,256); legs (302,232)→(296,345) and (358,232)→(364,345); ground line 1px `#e5e9ef` at y=350.
- **Five markers** (6px dot in the sport hue, dashed 1.5px leader dash 3/3 in the same hue, 13.5px label in the same hue, second line 11.5px mute):
  - **American football** — orange `#d95926`: two dots on the shoulders (282,120) and (378,120); leader from (378,120) to (490,74); label left-aligned at (496,70) "American football" / "radio tag, one per shoulder pad".
  - **Soccer** — blue `#2a78d6`: dot between the shoulder blades (330,146); leader to (170,84); label right-aligned at (164,80) "Soccer" / "GNSS pod in a vest pocket".
  - **Baseball** — violet `#4a3aa7`: dot on the right forearm (410,196); leader to (520,178); label left-aligned (526,174) "Baseball (pitching)" / "inertial sleeve over the elbow".
  - **Swimming** — aqua `#199e70`: dot on the left wrist (248,240); leader to (130,220); label right-aligned (124,216) "Swimming" / "watch-style unit on the wrist".
  - **Golf** — magenta `#d55181`: dot on the right hand (414,256); leader to (520,296); label left-aligned (526,292) "Golf" / "sensor on the glove or club".
  - **Motorsport** — violet `#4a3aa7` (hue shared with baseball; positions and labels keep them distinct): dot on the left hand (246,256); leader to (146,262); label right-aligned (140,258) "Motorsport" / "pulse sensor in the glove".
- **Equipment strip (bottom-left, for the sensors that sit in the tool rather than on the body):** 11.5px mute italic heading left-aligned at (40,306) "in the equipment, not on the body:"; then two entries — 4px yellow `#c98500` dot at (46,320) with 12px yellow label at (54,324) "cycling & rowing — crank / oarlock"; 4px green `#008300` dot at (46,336) with 12px green label at (54,340) "racket, bat & ball — in the tool itself".
- **Footer band:** tinted ink band `rgba(26,82,118,0.06)`, 34px tall at the bottom; 14px `#2c3e50` centered text "Each sensor measures motion at the point it is strapped to. Everything about the athlete is computed from that point."

## Cards (single `.grid`, 3 columns; card numbers match the file index numbers)

| # | Label (color) | Title | Link | Description | Topics |
|---|---|---|---|---|---|
| 1 | GLOVE SENSOR (`#d55181`) | Golf | [sport-specific-wearables/01-golf.md](sport-specific-wearables/01-golf.md) | A sensor on the glove or shaft measures hand motion; the club-head speed the golfer reads is extrapolated down an assumed shaft. | swing-sensor, extrapolation, measured-vs-derived |
| 2 | GNSS VEST (`#2a78d6`) | Soccer | [sport-specific-wearables/02-soccer.md](sport-specific-wearables/02-soccer.md) | A pod between the shoulder blades logs a trail of position fixes; a "sprint" is any fix above a speed cutoff someone configured. | gnss-vest, thresholds, load |
| 3 | RADIO TAG (`#d95926`) | American Football | [sport-specific-wearables/03-american-football.md](sport-specific-wearables/03-american-football.md) | The shoulder-pad tag transmits pings and holds no data; stadium antennas solve position from arrival times, and keep the record. | radio-tag, beacon, stadium-held |
| 4 | ELBOW SLEEVE (`#4a3aa7`) | Baseball Pitching | [sport-specific-wearables/04-baseball-pitching.md](sport-specific-wearables/04-baseball-pitching.md) | An inertial sleeve senses forearm motion; the elbow torque that sets pitch limits and rest days is a limb-model output. | inertial-sleeve, model-output, workload |
| 5 | WRIST UNIT (`#199e70`) | Swimming | [sport-specific-wearables/05-swimming.md](sport-specific-wearables/05-swimming.md) | Water blocks radio and satellite, so one wrist unit dead-reckons the swim; a soft wall touch silently merges two laps. | wrist-imu, dead-reckoning, structural-nulls |
| 6 | STRAIN GAUGE (`#c98500`) | Cycling & Rowing Power | [sport-specific-wearables/06-cycling-rowing-power.md](sport-specific-wearables/06-cycling-rowing-power.md) | A strain gauge in the crank or oarlock measures force mechanically — the rare number that is measured, not inferred, though often at one leg only. | strain-gauge, measured-directly, single-sided |
| 7 | EQUIPMENT SENSOR (`#008300`) | Racket, Bat & Ball | [sport-specific-wearables/07-racket-bat-ball-sensors.md](sport-specific-wearables/07-racket-bat-ball-sensors.md) | Nothing is worn — the sensor lives in the racket, bat or ball. The tool is tracked; the athlete holding it is an inference. | equipment-mounted, classifier, shared-equipment |
| 8 | BIOMETRIC GLOVE (`#4a3aa7`) | Motorsport Biometrics | [sport-specific-wearables/08-motorsport-biometrics.md](sport-specific-wearables/08-motorsport-biometrics.md) | An optical pulse sensor stitched into the racing glove, radioed out for crash triage — also a stress record of an employee at work. | vitals, safety-telemetry, custody |

## Regeneration instructions

- **Layout:** grid page — h1, `.subtitle`, `.philosophy` callout, centered header canvas `c0`, then one `.grid` (3 columns) with one `.card` anchor per row of the cards table above. No TOC, no section headers, no nav bar, no back/home links.
- **Card structure:** `<a class="card" href="...">` containing `<div class="card-label" style="color:LABEL_COLOR">LABEL</div>`, `<h3>N. Title</h3>` (unpadded index number matching the file index), `<p>description</p>`, then `<div class="topics">` with one `<span class="topic-tag">` per topic.
- **Page CSS:** same as the parent hub grid (`../03-tracking-data-collection-methods.html`): body system sans-serif, white background, text `#2a2a2a`, padding 40px 20px; h1 1.8em `#1a5276`; `.subtitle` `#666` 1.05em; `.philosophy` background `#f0f4f8`, left border `4px solid #2980b9`; `.grid` 3 columns gap 16px (2 at ≤800px, 1 at ≤500px); `.card` background `#f8fafb`, border `1px solid #e0e0e0`, radius 8px, hover shadow + `#2980b9` border; `.card h3` `#1a5276` 1.0em; `.card p` 0.85em `#555`; `.topic-tag` background `#eef4f8`, border `1px solid #cdd`, 0.7em; `.card-label` 0.72em bold uppercase.
- **Canvas:** `c0` declares intrinsic `width="720" height="380"`; `setupCanvas(id)` sizes the backing store to rendered width × `window.devicePixelRatio` (display capped via `style.maxWidth`) and `ctx.scale`s so drawing stays in logical coordinates; canvas centered in a wrapper div, `width: 100%`, `max-width: 720px`.
- **Palette:** `P = { blue: #2a78d6, green: #008300, magenta: #d55181, yellow: #c98500, aqua: #199e70, orange: #d95926, violet: #4a3aa7, ink: #1a5276, text: #2c3e50, mute: #6b7280, grid: #e5e9ef }`; sport hues fixed page-wide: golf magenta, soccer blue, football orange, baseball violet, swimming aqua. Red not used.
- **Links:** the table above links to `.md` siblings; in the regenerated HTML every card `href` uses the `.html` extension instead.
