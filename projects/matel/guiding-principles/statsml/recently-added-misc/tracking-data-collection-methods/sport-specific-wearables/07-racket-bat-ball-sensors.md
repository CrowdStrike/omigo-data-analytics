# Sport Wearables: Racket, Bat & Ball Sensors

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one row per section; picture-story canvases)
**HTML title tag:** Sport Wearables: Racket, Bat &amp; Ball Sensors

**Subtitle:** Here nothing is worn at all — the sensor lives in the equipment. The tool is tracked; the athlete, and the ball off the strings, are inferences.

Sport hue: green `#008300` (the equipment sensor). Because green is the sport hue on this page, measured annotations use aqua `#199e70` instead; derived annotations orange `#d95926` as usual.

## What is it?

Lede: The sensor lives in the equipment; nothing is worn.

- **Fitted:** in the racket butt cap, a bat sticker, or the ball's core
- **Measures:** handle acceleration, rotation and vibration; a smart ball measures its own spin
- **Derived:** stroke type, impact location on the strings, ball speed and spin estimates

Key-point callout: **The athlete is an inference from the tool:** every number describes the equipment; assigning it to a person assumes who was holding it.

### Visualization (canvas `c1`, 720×360)

Two-panel picture story: a tennis racket sensing at the handle, and a cricket smart ball sensing at its own core.

- **Header strip:** tinted ink band `rgba(26,82,118,0.07)` full width, 28px tall; bold 15px ink `#1a5276` centered title "The tool is tracked; the athlete is an inference".
- **Ground line:** 1px `#e5e9ef` at y=316, x 50→440 (left panel only).
- **Tennis player (left, side view, ink 2px):** legs (200,314)→(222,240) and (240,314)→(222,240); torso (222,240)→(232,175); head circle (238,160) r12, fill `tint(ink,0.06)`; arm (230,180)→(285,190).
- **Racket:** handle 2.5px ink line (285,190)→(310,152); head — ellipse centered (325,128), radiusX 20, radiusY 30, rotated ~-35°, ink 2px stroke, fill `tint(ink,0.05)`; three faint string lines across the ellipse in `tint(ink,0.25)`.
- **Sensor marker** — green: 6px dot at the butt cap (283,192); dashed green 1.5px leader (dash 3/3) to (160,120); bold 13.5px green label right-aligned (154,116) "sensor in the butt cap" / 11.5px mute "nothing on the player".
- **Measured annotation** — aqua: bold 13px left-aligned (56,250) "measured: handle motion" / 12px aqua "+ the shake of each impact".
- **Derived annotation** — orange: bold 13px left-aligned (360,86) "inferred: impact point, ball speed" / 12px orange "— from the shake, not from sight".
- **Cricket smart ball (right):** circle r40 at (560,200), ink 2px stroke, fill `tint(ink,0.05)`; seam — two parallel arcs across the middle in `tint(ink,0.5)` 1.5px; green 6px dot at the center (560,200); dashed green leader to (560,120); bold 13.5px green label centered (560,112) "smart ball — electronics at the core"; aqua 12.5px centered (560,262) "its own spin, measured directly"; 11.5px mute centered (560,278) "the one object here that measures itself".
- **Footer band:** tinted ink band `rgba(26,82,118,0.06)`, 34px; 14px `#2c3e50` centered "The racket infers the ball from vibration; the smart ball carries its own sensor. Neither ever measures the player."

## What does it collect?

- **Per-stroke inertial burst** and a vibration signature from the handle
- **Derived:** stroke type (a classifier), impact location, a ball-speed estimate
- **Session totals per equipment**, synced to whichever account paired the sensor
- **Precision:** impact location is estimated from vibration patterns, not seen — precise-looking heat maps blur near the frame edges, and a mishit can classify as clean

Key-point callout: **Precise-looking, camera-free:** the string-bed map has the resolution of a classifier, not of anything that watched the ball.

### Visualization (canvas `c2`, 720×320)

Picture story: the inferred string-bed heat map beside the vibration trace it came from.

- **Header strip:** tinted band, 28px; bold 15px ink centered title "The heat map has the classifier's resolution, not a camera's".
- **Racket face (left):** large ellipse centered (185,165), radiusX 78, radiusY 105, ink 2px stroke; string grid — 5 vertical + 6 horizontal lines in `tint(ink,0.18)` clipped to the ellipse; short handle stub (185,270)→(185,296) 2.5px ink.
- **Impact blobs (schematic, inferred):** filled circles — (185,150) r26 `tint(green,0.45)`; (170,185) r18 `tint(green,0.30)`; (205,130) r13 `tint(green,0.20)`; near-frame blob at (232,205) r15 `tint(orange,0.40)` with dashed orange 1.5px ring r19 and bold 12px orange label left-aligned (262,208) "near-frame hits blur" / 11.5px orange (262,222) "— and can classify as clean".
- **Caption under the racket (11.5px mute, centered at (185,306)):** "schematic — inferred zones, not observations".
- **Vibration trace (right):** panel x 380→690; baseline y=170; hardcoded burst (26 samples, schematic units −1…1): `[0.0, 0.0, 0.1, -0.1, 0.9, -0.8, 0.7, -0.6, 0.55, -0.45, 0.4, -0.32, 0.26, -0.2, 0.16, -0.12, 0.1, -0.08, 0.06, -0.05, 0.03, -0.02, 0.02, -0.01, 0.0, 0.0]` scaled to ±70px, aqua 2px polyline; 12.5px aqua label centered (535,74) "the shake the map is inferred from"; arrow — dashed orange 1.5px from (380,170) to (270,180) with arrowhead, bold 12px orange centered (330,148) "→ becomes the map".
- **Footer band:** 34px; 14px `#2c3e50` centered "A mishit near the throat can classify as clean — the map never saw the ball."

### Sample payload (right column, under canvas `c2`)

Caption (`.payload-note`, italic): "Sample payload — illustrative structure, not real captured data."

```
// Reconstruction. No racket-sensor vendor publishes its
// sync schema; the measured/inferred split is the point.
{
  "paired_account": "u_2209…",   // whoever paired the sensor
  "session_id":     "s_0341",
  "stroke_id":      417,

  // ── measured at the handle ──
  "accel_g":  [[0.2,0.1,1.0], [6.4,2.2,3.8], "…"],
  "gyro_dps": [[15,8,4], [820,340,190], "…"],
  "vibration": { "peak_hz": 118, "decay_ms": 60 },

  // ── inferred / classified ──
  "stroke":         "forehand",  // classifier output
  "impact_zone":    "upper-mid", // from vibration, not seen
  "ball_speed_est": 84,          // inferred from handle, not tracked
  "player":         null         // nothing identifies the hands
}
```

## Why is it collected?

Label pill: STATED PURPOSE

- **Technique feedback** and session counts
- **Equipment fitting** — matching a racket to a swing

Label pill: ADDITIONAL CONSEQUENCE

- **The record follows the paired account, not the hands** — a shared family racket or a coaching bag writes several people into one history
- **Junior players' sessions** sync to a parent's or an academy's vendor account

Key-point callout: **Unit of observation, in miniature:** the measurement unit is a racket; the decision unit is a player; nothing in the record bridges them.

### Visualization (canvas `c3`, 720×320)

Picture story: one racket handed between two players, both feeding a single account timeline.

- **Header strip:** tinted band, 28px; bold 15px ink centered title "One racket, two players, one history".
- **Ground line:** 1px `#e5e9ef` at y=262, x 40→330.
- **Player A (adult, ink 2px):** legs (80,260)→(95,205) and (110,260)→(95,205); torso (95,205)→(95,140); head circle (95,126) r13 fill `tint(ink,0.06)`; arm (95,155)→(150,175); 12px `#2c3e50` label centered (95,282) "player A".
- **Player B (junior, smaller):** legs (255,260)→(245,218) and (233,260)→(245,218); torso (245,218)→(245,170); head circle (245,158) r11 fill `tint(ink,0.06)`; arm (245,180)→(196,178); 12px `#2c3e50` label centered (245,282) "player B".
- **The racket between them:** handle 2.5px ink line (150,175)→(196,178); small head ellipse centered (173,160), radiusX 12, radiusY 17, ink 1.5px; green 5px sensor dot at (152,176); 11.5px mute label centered (173,204) two lines "one racket," / "handed across".
- **Flow to the account:** dashed green 1.5px lines from the sensor dot (152,176) curving via control (330,120) and from (196,178) via control (330,230), both ending with arrowheads at (420,168); solid green 2px line (420,168)→(470,168).
- **Account timeline (right):** rounded rect (470,120)–(690,216), `tint(green,0.06)` fill, green 1.5px stroke; bold 13px green centered (580,142) "one paired account"; inside, a timeline bar (490,170)→(670,170) 2px `#2c3e50` with alternating tick marks — 6 blue `#2a78d6` and 5 violet `#4a3aa7` ticks (7px tall, interleaved along the bar); 11.5px mute centered (580,196) "sessions from both hands, one stream".
- **Caption** — bold 12px orange centered (580,242): "no field says who was holding it".
- **Footer band:** 34px; 14px `#2c3e50` centered "A 'career total' on a shared racket is a household statistic, not a person's."

## Regeneration instructions

- **Layout:** tracking detail page — `<table class="obj-table">`, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` heading, `.lede`, bullets with bold lead terms (`li b` in `#1a5276`), `.key-point` callouts, `.lbl` label pills (last row); right `<td>` (55%, text-align center) holds the canvas, and for the "What does it collect?" row also the `.payload-note` caption plus `<pre class="payload">` block (left-aligned).
- **Page CSS:** identical to the sibling sport pages (see `01-golf.html`): body system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; td borders `1px solid #2980b9`, padding 16px; `.key-point` background `#f8f9fa`, left border `3px solid #1a5276`; `.lbl-purpose` `#eaf2fb`/`#1a5276`, `.lbl-effect` `#fdf0e6`/`#a8501c`; `.payload` monospace block with ink left border. No nav bar, no back/home links.
- **Canvas:** intrinsic `width`/`height` attributes (c1 720×360, c2/c3 720×320); shared `setupCanvas(id)` sizes the backing store to rendered width × `window.devicePixelRatio` and `ctx.scale`s to logical coordinates. Helpers: `tint(hex,a)`, `band()` header/footer strips, `arrowHead()`, `dashedLeader()`.
- **Palette:** `P = { blue: #2a78d6, green: #008300, magenta: #d55181, yellow: #c98500, aqua: #199e70, orange: #d95926, violet: #4a3aa7, ink: #1a5276, text: #2c3e50, mute: #6b7280, grid: #e5e9ef }`; sport hue green, measured annotations aqua (green is taken on this page), derived orange; red unused. All chart data hardcoded literal arrays; invented zones and numbers labeled schematic/illustrative.
- In regenerated HTML, any card links elsewhere use `.html` extensions (this page has none).
