# Sport Wearables: Motorsport Biometrics

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, three rows: What is it? / What does it collect? / Why is it collected?)
**HTML title tag:** Sport Wearables: Motorsport Biometrics

**Subtitle:** An optical pulse sensor stitched into the racing glove, radioed out of the car — collected so medics can read a driver who cannot answer the radio. The same stream is also a stress record of an employee at work.

## What is it?

Lede: A pulse sensor stitched into the racing glove, radioed out of the car.

- **Worn:** an optical pulse-oximetry sensor stitched into the glove, used in some top single-seater series
- **Measures:** pulse rate and a blood-oxygen estimate, optically at the finger
- **Derived:** alert thresholds for the medical crew

**Collected for triage:** the stated consumer of this stream is a medical crew at a crash scene — a purpose far narrower than the data, which is a continuous vitals record of a named employee at work.

### Visualization (canvas `c1`, 720×360)

Annotated side-view open-cockpit scene: a simple single-seater silhouette on a ground line, driver's helmet and torso visible, arm to the wheel; a violet sensor dot on the glove at the wheel with a dashed leader to its label; a dashed violet radio arc from the car to a small pit-wall receiver mast at the right. Hue code: violet = the glove sensor and its radio stream, green = measured annotation, orange = derived annotation, ink = car and figure outlines.

- **Header strip:** tinted ink band `rgba(26,82,118,0.07)` full width, 28px tall; bold 15px ink `#1a5276` centered title "Motorsport — a pulse sensor in the glove, radioed off the car".
- **Ground line:** 1px mute `#6b7280` from (60,272) to (660,272).
- **Wheels:** two ink circles r18 at (160,254) and (390,254), fill 0.12-alpha ink tint, ink 1.5px stroke, drawn before the body.
- **Car body** (ink 1.5px stroke, 0.06-alpha ink fill), closed path: (100,262) nose tip → (160,246) nose top → (218,242) cockpit front → (230,252) cockpit dip front → (302,252) cockpit dip rear → (316,234) roll-hoop front → (338,234) roll-hoop top → (420,250) engine cover → (442,250) → (442,264) → (100,264) → close.
- **Rear wing:** ink 2px line (428,226)→(470,226); strut (450,226)→(446,250).
- **Driver:** helmet circle r11 at (272,230), 0.15-alpha ink tint fill, ink 1.5px stroke; torso line (276,240)→(288,252); arm line (268,238)→(242,248) to the wheel.
- **Steering wheel:** ink 2px line (238,238)→(234,254).
- **Glove sensor dot** — violet `#4a3aa7`: 5.5px dot at (240,247).
- **Sensor label** violet, bold 13px centered (168,150): "optical sensor, stitched into the glove"; dashed violet leader (dash 3/3, 1.25px) from (168,158) to (236,240).
- **Radio arc** — dashed violet (dash 3/3, 1.5px) quadratic curve from (334,228), control (470,110), to (604,192); violet arrowhead at (606,193), angle 0.52 rad; bold 12.5px violet label centered (470,142): "radioed off the car".
- **Pit wall + receiver mast** (right): rect (570,238) 96×34, 0.05-alpha ink fill, ink 1.25px stroke; violet mast on top — 2.5px stem (610,196)→(610,238), 1.5px v-fork (605,189)-(610,196)-(615,189), 4px violet dot at (610,196); mute 11.5px label centered (618,292): "pit-wall medical receiver".
- **Measured annotation** — green `#008300`, 12px centered (250,296): "pulse rate + blood oxygen, optically at the finger"; dashed green leader (dash 3/3, 1.25px) from (240,253) to (248,284).
- **Derived annotation** — orange `#d95926`, 12px centered (250,312): "alert states — derived thresholds".
- **Footer band:** tinted ink band `rgba(26,82,118,0.06)`, 34px tall at the bottom; 14px `#2c3e50` centered text "Built so a medical crew can read the driver when the driver cannot answer."

## What does it collect?

- **Pulse rate and a blood-oxygen estimate**, sampled continuously through the session
- **Timestamps aligned with car telemetry** — vitals can be read against g-loads and lap position
- **Derived alert flags** from thresholds set for the medical crew
- **Precision:** an optical sensor on a hand gripping a vibrating wheel under multi-g loads — motion artifact is the precision floor, and the sensor's own confidence varies lap by lap

**Keyed to the car:** because the vitals share timestamps with car telemetry, every heart-rate spike acquires a corner, a lap and a g-load — the stream reads as a story, not a number.

### Visualization (canvas `c2`, 720×320)

One lap, schematic: a violet pulse-rate trace over grey g-load shading, with one stretch drawn dashed with hollow dots where the trace goes low-confidence through the highest-vibration section. All values hardcoded illustrative arrays — no generated data.

- **Header strip:** tinted ink band, 28px; bold 15px ink centered title "One lap — the pulse trace loses confidence where the g-load shakes the hand".
- **Plot geometry:** 30 samples, x(i) = 70 + i·620/29 (x from 70 to 690).
- **Pulse trace** — violet: BPM array `[126,128,131,136,143,150,156,160,158,152,147,144,148,151,149,153,150,146,142,139,143,149,155,159,161,157,151,146,141,138]`; y(v) = 196 − (v−120)·2.6. Solid violet 2px segments; segments touching indices 12–16 drawn dashed (dash 3/3). Dots r2.5 at every sample: filled violet where confident, hollow (white fill, violet 1.5px stroke) at indices 12–16.
- **G-load shading** — grey: g array `[0.6,1.2,2.4,3.6,3.9,2.8,1.5,0.8,1.4,2.6,3.4,3.8,4.2,4.4,4.3,4.1,3.2,1.8,0.9,0.7,1.6,2.9,3.7,3.5,2.2,1.1,0.8,1.8,2.7,1.4]`; filled area from baseline y=252 up to y(g) = 252 − g·16, fill 0.18-alpha mute tint, no stroke. The g peak (indices 12–16) sits under the low-confidence stretch by construction.
- **Trace label** violet, bold 12px left-aligned (74,58): "pulse rate — illustrative".
- **Shading label** mute, 11.5px left-aligned (74,236): "g-load — schematic shading".
- **Low-confidence annotation** — italic 11.5px mute centered (369,84): "low confidence — motion artifact"; dashed mute leader (dash 3/3, 1px) from (369,90) to (369,114).
- **Baseline axis:** 1px mute line (70,252)→(690,252); mute 11px centered label (380,270): "lap distance →".
- **Footer band:** tinted ink band, 34px; 14px `#2c3e50` centered text "The gaps are motion artifact, not physiology — the sensor's confidence varies lap by lap."

### Payload (below canvas `c2`)

Caption (`.payload-note`, italic): "Sample payload — illustrative structure, not real captured data."

```
// Reconstruction. Field names are generic; biometric
// glove schemas are not public.
{
  // ── measured at the finger ──
  "pulse_bpm": [148, 151, 149, 153],
  "spo2_pct": [97.2, 96.8, null, 97.0],   // null — motion artifact
  "signal_quality": ["good", "good", "poor", "good"],

  // ── derived / aligned downstream ──
  "alert_state": "none",                   // thresholds, not measured
  "lap": 23,
  "telemetry_key": "car-12/lap-23/t+41.2s",
  "driver": { "id": "DRV-12", "name": "…" }
                // joined from an entry list, not sensed
}
```

## Why is it collected?

**Stated purpose** (label pill `.lbl-purpose`)

- **Crash triage** — severity assessment before extraction
- **Medical response decisions** at the scene and on the way to care

**Additional consequence** (label pill `.lbl-effect`)

- **A stress and fitness record of a named employee** — heart-rate response to pressure moments is performance-relevant information a team could act on
- **Custody arrangements differ by series** between medical delegates and teams

**The sensor cannot enforce its purpose:** data collected under a safety rationale acquires performance uses simply by existing — nothing in the glove limits who reads the stream.

### Visualization (canvas `c3`, 720×320)

Custody split picture: the glove's stream leaves the driver for the car telemetry stream, then forks — a solid green line to a medical delegate node ("triage — stated") and a dashed blue line to a team node ("analysis — possible"). The driver figure at the far left holds neither copy.

- **Header strip:** tinted ink band, 28px; bold 15px ink centered title "Custody split — one stream, a stated reader and a possible one".
- **Driver** (far left) — ink stick figure: head circle r9 at (72,118), body (72,127)→(72,172), left arm (72,138)→(56,158), right arm (72,138)→(92,150) reaching toward the stream, legs (72,172)→(58,200) and (72,172)→(86,200); violet 4.5px glove dot at (92,150) with one dashed violet arc r13 (0.5 alpha) centered on it; mute 11.5px label centered, two lines (72,222) "the driver" / (72,237) "holds neither copy".
- **Stream node** (violet): rounded rect (170,126) 146×46, 1.5px violet stroke, 0.07-alpha violet fill; bold 12.5px violet centered (243,146) "car telemetry stream"; 11px text centered (243,162) "vitals + lap-time keys"; 11px violet centered (243,188) "leaves the car by radio".
- **Medical delegate node** (green): rounded rect (490,54) 180×50, green stroke, 0.07-alpha green fill; bold 12.5px green centered (580,74) "medical delegate"; 11px text centered (580,90) "severity before extraction".
- **Team node** (blue `#2a78d6`): rounded rect (490,204) 180×50, blue stroke, 0.07-alpha blue fill; bold 12.5px blue centered (580,224) "the team"; 11px text centered (580,240) "stress & fitness record".
- **Arrows:** dashed violet 1.5px (dash 3/3) from the glove (98,150) to the stream node (168,147), violet arrowhead, violet 11px label centered (132,132) "vitals"; solid green 2px from (316,144) to (488,82), green arrowhead at (490,81), green 11px label centered (398,96) "triage — stated"; dashed blue 2px (dash 3/3) from (316,158) to (488,226), blue arrowhead at (490,227), italic blue 11px label centered (398,208) "analysis — possible".
- **Note:** italic 11px mute centered (400,268) "custody arrangements differ by series".
- **Footer band:** tinted ink band, 34px; 14px `#2c3e50` centered text "The glove cannot enforce its stated purpose — the same stream is triage to one reader, performance to another."

## Regeneration instructions

- **Layout:** tracking-page `.obj-table`: full-width bordered table, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` + `.lede` + bullets + `.key-point` callouts + `.lbl` pills, right `<td>` (55%, centered) holds the canvas; the "What does it collect?" row also carries the `.payload-note` caption and `.payload` pre block under its canvas (both left-aligned).
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; subtitle `#666` 1.05em; table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold `#1a5276` 1.1em; `li b` `#1a5276` weight 600, li 0.93em. No nav bar, no back/home links, no links to other pages.
- **Callouts:** `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em, leading `<strong>` in `#1a5276`.
- **Label pills:** `.lbl` inline-block uppercase 0.7em bold, padding 2px 7px, radius 3px; `.lbl-purpose` background `#eaf2fb` color `#1a5276`; `.lbl-effect` background `#fdf0e6` color `#a8501c`.
- **Payload:** `.payload` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px, ui-monospace 0.78em, `white-space: pre`, line-height 1.45; `.payload-note` 0.82em `#666` italic.
- **Canvas:** each declares its intrinsic size in `width`/`height` attributes (c1 720×360, c2 and c3 720×320); a shared `setupCanvas(id)` reads the element's attributes, sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Helpers: `tint(hex, a)` for translucent fills from palette tokens, `rr()` rounded-rect, `arrowHead()` for one-way arrows. Every canvas carries the tinted ink header band (28px) and footer band (34px). Charts hardcode literal coordinate and value arrays (no Math.random); the c2 pulse and g-load arrays are labeled "illustrative" / "schematic" on the canvas itself.
- **Hue roles on this page:** violet `#4a3aa7` = the glove sensor and its radio stream (sport hue); green `#008300` = measured annotations and the medical delegate; orange `#d95926` = derived annotations; blue `#2a78d6` = the team node in c3; ink `#1a5276` headings, bands, car and figure outlines only; mute `#6b7280` axes and neutral notes. Red is reserved for genuine alarm states and is not used on this page.
- **Naming rule:** no series, sanctioning body, glove supplier or product is named — generic terms only ("some top single-seater series", "the glove supplier", "medical delegate"). No unsourced specifics: adoption is hedged "in some series", custody is hedged "arrangements differ by series", sensor confidence is hedged "varies lap by lap" with no numbers asserted.
- **Palette (declared once as tokens):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`; ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`.
- In regenerated HTML, any card links use `.html` extensions (this page has none).
