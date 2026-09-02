# Sport Wearables: Golf

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, three rows: What is it? / What does it collect? / Why is it collected?)
**HTML title tag:** Sport Wearables: Golf

**Subtitle:** The sensor rides the hands — on a glove or clipped to the shaft. The club-head number the golfer reads is extrapolated down an assumed shaft, not measured at the head.

## What is it?

Lede: A small inertial sensor that rides the golfer's hands and reconstructs the swing.

- **Worn:** a small sensor on the glove, or clipped to the club shaft
- **Measures:** hand acceleration and rotation through the swing
- **Derived:** tempo, club-head speed and swing path — extrapolated down the shaft

**Speed at the wrong end:** the club head moves far faster than the hands, so head speed is scaled up through an assumed shaft model — a glove sensor and a shaft sensor disagree about the same swing.

### Visualization (canvas `c1`, 720×360)

Annotated side-view figure of a golfer mid-swing (facing left), with the sensor dot on the hands and the swing arc traced by the club head — the picture separates where the measurement happens from where the reported number applies.

- **Header strip:** tinted ink band `rgba(26,82,118,0.07)` full width, 28px tall; bold 15px ink `#1a5276` centered title "Golf — the sensor rides the hands, the number describes the club head".
- **Ground line:** 1px `#e5e9ef` at y=316, x 60→660.
- **Golfer** (ink `#1a5276` 2px strokes, near x=400): head circle (408,112) r13, fill 0.06-alpha ink tint; spine (404,125)→(392,196); hips dot r4 at (392,198); legs (392,198)→(376,255)→(372,314) and (392,198)→(410,258)→(416,314); arms one polyline shoulder (402,132)→(360,168)→(330,196); hands circle r7 at (328,198), fill 0.06-alpha ink tint.
- **Club:** 2.5px ink line hands (328,198)→club head (250,296); club head = filled ink circle r5.
- **Club-head arc:** dashed magenta `#d55181` 2px (dash 6/4), centered on the hands (328,198), radius 124, swept from top-of-backswing (upper right, start angle −0.62 rad — pulled slightly past the seed's ≈(430,100) so the arc clears the golfer's head) through the bottom to the finish (upper left, angle of point ≈(212,120)). Three 4px magenta dots ON the arc (positions computed from the angles at r=124: top ≈(429,126), impact (328,322), finish ≈(225,129)) with 12px mute labels: "top of backswing" (left-aligned (437,122)), "impact" (bottom dot, label left-aligned (340,321), just below the ground line), "finish" (right-aligned (217,124)).
- **Sensor marker** — magenta: 6px dot on the hands (328,198); dashed 1.5px magenta leader (dash 3/3) to (560,150); bold 13.5px magenta label left-aligned (566,146) "swing sensor", second line 11.5px mute (566,162) "on the glove (or the shaft)".
- **Measured annotation** — green `#008300`, left-aligned: bold 13px at (470,210) "measured: hand acceleration"; 12px green at (470,226) "+ rotation, 3 axes each".
- **Derived annotation** — orange `#d95926`, left-aligned near the club head: bold 13px at (66,286) "derived: club-head speed"; 12px orange at (66,302) "scaled down an assumed shaft". (Moved from the seed position (80,250), which the swing arc crossed.)
- **Footer band:** tinted ink band `rgba(26,82,118,0.06)`, 34px tall at the bottom; 14px `#2c3e50` centered text "Club-head speed is not measured at the head — it is extrapolated from hand motion through a shaft model."

## What does it collect?

- **Per-swing inertial samples** — acceleration and rotation, three axes each, at a high vendor-set rate
- **Derived per swing:** tempo ratio, club-head speed, swing path
- **Club selection** — user-entered, not sensed
- **Shot location**, where a GPS watch or tag is paired
- **Precision:** the unit samples hundreds of readings a second, but head-speed precision is set by the shaft model, not the sensor — two mounts on one swing can disagree by more than the quoted margin

**The club field is typed, not sensed:** club-by-club distance averages inherit every swing logged against the wrong club, and nothing in the motion data can correct the label.

### Visualization (canvas `c2`, 720×320)

One swing as the sensor sees it: the raw acceleration burst (hardcoded literal array, illustrative) drawn as a green trace, collapsing through an arrow into the three orange numbers the golfer actually reads.

- **Header strip:** tinted ink band, 28px; bold 15px ink centered title "One swing: a second of samples becomes three numbers".
- **Trace plot** (x 56→444, top y=56, baseline y=246, y-scale 0→35 g): 48-sample hand-acceleration magnitude array, hardcoded — flat address (≈1 g), slow backswing rise to ≈3 g, dip to ≈1.2 g at the top, sharp downswing rise to 34 g at impact, decay through the follow-through. Green `#008300` 2px line over a 0.10-alpha green area fill; baseline 1px `#e5e9ef`.
- **Measured label** — bold 12.5px green, left-aligned (58,50): "measured: the raw burst".
- **Phase markers** — magenta 4px dots on the trace at the top-of-backswing dip (index 24) and the impact peak (index 31); 11.5px mute labels "top" (above the dip) and "impact" (left-aligned beside the peak).
- **Phase axis labels** — 11px mute at y=262 under the trace: "address", "backswing", "downswing", "follow-through" at the matching sample positions.
- **Arrow** — ink 2px line (452,150)→(492,150) with a filled triangle head; 11px mute label centered (472,140) "vendor model".
- **Derived chips** — heading bold 12.5px orange centered (599,56) "derived by the model"; three rounded rects x=508, 182×48, at y=66/126/186, 1.5px orange stroke, 0.06-alpha orange fill; each holds a bold 15px orange value centered at chip top+20 and an 11px mute caption at chip top+37: "tempo 3.1 : 1" / "backswing : downswing"; "club-head 92.4 mph" / "extrapolated, not measured"; "swing path −2.3°" / "from rotation at the hands".
- **Footer band:** tinted ink band, 34px; 14px `#2c3e50` centered text "Seconds of motion samples go in; the golfer sees three numbers come out. Illustrative trace."

### Payload (below canvas `c2`)

Caption (`.payload-note`, italic): "Sample payload — illustrative structure, not real captured data."

```
// Reconstruction. No swing-sensor vendor publishes its
// sync schema; the measured/derived split is the point.
{
  "account_id": "u_8813…",
  "swing_id":   "sw_0207",
  "mount":      "glove",          // or "shaft"
  // ── measured by the inertial unit ──
  "sample_hz":  "high, vendor-set",
  "accel_g":    [[0.1,0.0,1.0], [2.4,0.8,3.1], "…"],
  "gyro_dps":   [[12,4,8],      [640,210,155], "…"],
  // ── derived by the vendor's swing model ──
  "tempo_ratio":     3.1,        // backswing : downswing
  "clubhead_mph":    92.4,       // extrapolated, not measured
  "swing_path_deg":  -2.3,
  "club":            "7i"        // user-entered, not sensed
}
```

## Why is it collected?

**Stated purpose** (label pill `.lbl-purpose`)

- **Technique feedback** — tempo and swing path, swing by swing on the range
- **Distance averages** — club-by-club carry built from paired shot locations

**Additional consequence** (label pill `.lbl-effect`)

- **Every practice swing syncs** to a vendor account under a consumer agreement
- **A multi-year physical record** — swing speed over years tracks a person's physical capability, and handicap-relevant performance data sits off-course

**A capability time series:** the stated purpose needs only the current session, but the account typically keeps every one — year over year, a club-head-speed series is effectively a fitness measurement held by a vendor.

### Visualization (canvas `c3`, 720×320)

Two mounts reading the same swing: a glove-mounted and a shaft-mounted sensor on identical schematic clubs, each reporting its own club-head speed — the number depends on where the sensor sat and how far its model had to extrapolate.

- **Header strip:** tinted ink band, 28px; bold 15px ink centered title "Two mounts, one swing — two club-head speeds".
- **Center label:** bold 13px ink centered (360,48) "same swing".
- **Left panel — glove mount:** title bold 13.5px ink centered (170,68) "glove mount". Club: 2.5px ink line hands (230,100)→head (110,240); hands circle r7 ink stroke, 0.06-alpha ink fill; head filled ink circle r5. Sensor: 6px magenta dot on the hands; dashed magenta leader (dash 3/3) to (280,78); bold 12.5px magenta label left-aligned (286,74) "sensor on the glove". Measured: bold 12px green left-aligned (245,112) "measured here". Extrapolated span: dashed orange 2px (dash 3/3) parallel to the shaft, offset left, (216,104)→(96,244); 11.5px orange label right-aligned (150,160) "extrapolated". Readout: bold 16px magenta centered (185,268) "club-head speed: 92.4 mph".
- **Right panel — shaft mount:** title bold 13.5px ink centered (530,68) "shaft mount". Same club geometry shifted right: hands (590,100)→head (470,240). Sensor: 6px magenta dot partway down the shaft at (542,156); dashed magenta leader to (452,140); bold 12.5px magenta label right-aligned (446,138) "sensor on the shaft". Measured: bold 12px green left-aligned (554,168) "measured here". Extrapolated span (shorter): dashed orange 2px (532,162)→(458,246); 11.5px orange label right-aligned (480,212) "extrapolated". Readout: bold 16px magenta centered (545,268) "club-head speed: 95.8 mph".
- **Footer band:** tinted ink band, 34px; 14px `#2c3e50` centered text "Schematic, illustrative values — the number depends on where the sensor sat and which model extrapolated."

## Regeneration instructions

- **Layout:** tracking-page `.obj-table`: full-width bordered table, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` + `.lede` + bullets + `.key-point` callouts + `.lbl` pills, right `<td>` (55%, centered) holds the canvas; the "What does it collect?" row also carries the `.payload-note` caption and `.payload` pre block under its canvas (both left-aligned).
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276` "Sport Wearables: Golf" (no index number); subtitle `#666` 1.05em; table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold `#1a5276` 1.1em; `li b` `#1a5276` weight 600, li 0.93em. No nav bar, no back/home links, no links to other pages.
- **Callouts:** `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em, leading `<strong>` in `#1a5276`.
- **Label pills:** `.lbl` inline-block uppercase 0.7em bold, padding 2px 7px, radius 3px; `.lbl-purpose` background `#eaf2fb` color `#1a5276`; `.lbl-effect` background `#fdf0e6` color `#a8501c`.
- **Payload:** `.payload` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px, ui-monospace 0.78em, `white-space: pre`, line-height 1.45; `.payload-note` 0.82em `#666` italic.
- **Canvas:** each declares its intrinsic size in `width`/`height` attributes (c1 720×360, c2 and c3 720×320); a shared `setupCanvas(id)` reads the element's attributes, sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Helpers: `tint(hex, a)` for translucent fills from palette tokens, `rr()` rounded-rect. Every canvas carries the tinted ink header band (28px, title bold 15px ink centered) and footer band (34px, 14px `#2c3e50` centered). Charts hardcode literal data arrays (no Math.random); the c2 burst is a 48-value literal array and is labeled illustrative; c3 speeds are labeled schematic/illustrative.
- **Sport hue:** magenta `#d55181` marks the sensor and its readouts throughout; measured annotations green `#008300`; derived annotations orange `#d95926`.
- **Palette (declared once as tokens):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`; ink `#1a5276` (headings/axes only), text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red is reserved for genuine alarm states and is not used on this page.
- In regenerated HTML, any card links use `.html` extensions (this page has none).
