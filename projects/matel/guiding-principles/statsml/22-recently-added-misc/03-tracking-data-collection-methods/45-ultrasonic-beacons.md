# Tracking Data: Ultrasonic Beacons

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, three rows: What is it? / What does it collect? / Why is it collected?)
**HTML title tag:** Tracking Data: Ultrasonic Beacons

**Subtitle:** A high-frequency audio watermark that a library inside a host app can correlate against microphone input. A detection is a classifier output compared to a threshold, not a direct observation.

## What is it?

Lede: A matched filter over microphone input, not a recording.

- **The pattern** sits near the top of or just above the range most adults hear — commonly around 18kHz and up
- **Where it plays:** embedded in broadcast audio, or through a venue's speakers
- **The listener:** a library inside an app that holds microphone permission correlates incoming audio against patterns it already knows
- **What it computes** is a correlation score; whether that score counts as a detection depends on a threshold the operator sets

**Range is short but not sharply bounded:** ultrasound attenuates quickly and reflects, so it passes through a doorway and can bleed through a thin wall.

**"Inaudible" is a population claim:** hearing at these frequencies varies strongly with age, and younger ears often do perceive tones that older ears do not.

### Visualization (canvas `c1`, 720×320)

Scene diagram: a TV emitting an ultrasonic watermark across a room to a phone's microphone, in three labeled stages.

- **Header strip:** tinted ink band `rgba(26,82,118,0.07)` full width, 28px tall; bold 15px ink `#1a5276` centered title "An identifier travels from an emitter, through the room, to a listener".
- **Stage captions** (bold 12px, centered, y=46), walking SERIES so each stage is identifiable: "1 · emitter" at x=150 in blue `#2a78d6`; "2 · airborne signal" at x=380 in green `#008300`; "3 · listener" at x=557 in violet `#4a3aa7`.
- **TV set:** dark `#2c3e50` frame (80,62) 140×100 with blue `#2a78d6` screen inset; stand rectangles below. White centered screen text: bold 15px "TV AD" and 13px "PLAYING".
- **Ultrasonic waves:** eight concentric green `#008300` arcs (1.5px) from (220,112), radii 55 to 230, angle −0.4 to 0.4 rad, alpha fading 0.9→0.2. Labels above the waves: green 14px "~18kHz watermark" at (380,68); mute 13px "attenuates fast, but reflects and passes doorways" at (380,85).
- **Phone:** dark rounded rectangle (530,72) 55×100 radius 8 with `#e5e9ef` screen inset; violet `#4a3aa7` mic circle (radius 9) at (557,107) with white "MIC" text; violet bold 14px label below: "Host app holding" / "mic permission".
- **Footer band:** tinted ink band `rgba(26,82,118,0.06)`, 34px tall at the bottom; 14px `#2c3e50` centered text "The emitter puts an identifier into the room; a listening device may correlate against it".

## What does it collect?

- **Which beacon pattern** was matched, and its correlation score
- **Dwell** — how long the score stayed above threshold
- **Timestamp** of the detection
- **The host app's** advertising or device identifier
- **Which app** carried the library, and what the mic permission was granted for
- **Nothing below threshold** — non-detections are usually not logged

**A detection is a classifier output:** `correlation` compared against `detect_threshold`. Both error types are available — a beacon bleeding through a wall can clear the bar and be recorded as a visit, and a noisy room can push a genuine one under it. Downstream, the record reads as a fact.

**It runs on a grant given for something else:** note `mic_permission_for` — the microphone was granted for another purpose.

### Visualization (canvas `c2`, 720×320)

Bar chart of correlation scores for five scenarios against an operator-set threshold, coloured by whether the log and the room agree.

- **Header strip:** tinted ink band, 28px; bold 15px ink centered title "A detection is a score crossing a threshold (schematic)".
- **Axes:** y-axis at x=70 from baseline y=214 up 150px, labeled "0" and "1.0" (12px mute); rotated y-axis title "correlation score"; x-axis 580px wide.
- **Threshold line:** dashed orange `#d95926` (dash 6/4, 2px) at score 0.65, labeled in bold orange 12px: "detect_threshold (operator sets this)" — the one operator-set number on the chart.
- **Bars** (52px wide, one per case slot, fill = 0.28-alpha tint of outcome hue, 2px stroke; bold 12px outcome marker "logged" / "not logged" above the bar in the outcome hue; 12px two-line case label below the axis). Hardcoded illustrative scores; colour encodes an agreement code shared with the page's outcome legend (red not used — a scoring disagreement is not an alarm):
  - "in the room, quiet" — score 0.88, truth: present → logged, agree = green `#008300`
  - "in the room, noisy" — score 0.52, truth: present → not logged, missing = violet `#4a3aa7`
  - "through an open doorway" — score 0.71, truth: absent → logged, extra = magenta `#d55181`
  - "neighbour's TV, thin wall" — score 0.68, truth: absent → logged, extra = magenta `#d55181`
  - "two rooms away" — score 0.29, truth: absent → not logged, agree = green `#008300`
- **Legend** (12px swatch row at y≈262): green "log matches the room"; violet "present, under threshold"; magenta "logged, not in the venue".
- **Footer band:** tinted ink band, 30px; 13px `#2c3e50` centered text "Two of the logged bars are not visits, and one real presence is missing — neither is countable from the log".

### Payload (below canvas `c2`)

Caption (`.payload-note`, italic): "Sample payload — illustrative structure, not real captured data."

```
// There is no public API for this. The whole block is
// reconstruction from how audio watermarking works: a
// library inside a host app correlates microphone input
// against a pattern it already knows.
{
  // ── inferred / plausible ──
  "beacon_id":         "bcn-4471",
  "carrier_hz":        18400,
  "correlation":       0.71,
  "detect_threshold":  0.65,     // below this, nothing is logged
  "dwell_ms":          9400,
  "detected_at":       "2026-08-22T20:31:44Z",
  "advertising_id":    "3f2a…",
  "host_app":          "…",
  "mic_permission_for": "voice_search",
  "audio_retained":    false     // score kept, samples discarded
}
```

## Why is it collected?

**Stated purpose** (label pill `.lbl-purpose`)

- **Cross-device attribution** — a real and otherwise hard problem: a TV and a phone share no identifier, so no join key exists
- **The watermark supplies one** by putting the identifier into the room

**Additional consequence** (label pill `.lbl-effect`)

- **The same match is a presence assertion** — a venue beacon detection functions as a check-in
- **Two devices, one beacon, one window** functions as a co-location claim, from an audio correlation rather than a location permission

**Only the hits are logged, so there is no denominator:** one match is enough to credit an ad, so the design records what cleared the threshold and nothing else. A presence rate needs the non-detections to divide by, and they were never written down. Downstream the field still reads as an observed visit.

### Visualization (canvas `c3`, 720×320)

Five-step attribution chain from ad play to stored visit, with grouping bands splitting physical events from model outputs.

- **Header strip:** tinted ink band, 28px; bold 15px ink centered title "The join key travels through the air — and turns into a fact at the last step".
- **Five step circles** (radius 35, centers at x = 62 + i×146, y=112; fill = 0.22-alpha tint of the step hue, 2.5px stroke; 18px icon inside; bold 14px label lines below; 12px mute assertion line under the label), walking SERIES so each link is identifiable:
  - blue `#2a78d6`, 📺: "Ad plays" — asserts "audio output" (physical)
  - green `#008300`, 🔊: "Watermark / in audio" — asserts "pattern emitted" (physical)
  - violet `#4a3aa7`, 📱: "Mic input / correlated" — asserts "score computed" (physical)
  - orange `#d95926`, ≥: "Score above / threshold" — asserts "classifier fires" (derived)
  - aqua `#199e70`, ✓: "Stored as / a visit" — asserts "reads as observed" (derived)
- **Arrows** between circles at center height, each 2px in the hue of the receiving stage, with a small filled arrowhead.
- **Grouping bands** (y=236, height 22, split between step 3 and step 4): aqua-tinted `rgba(25,158,112,0.14)` band labeled in bold aqua 12px "physical event" over the first three steps; orange-tinted `rgba(217,89,38,0.14)` band labeled in orange "model output" over the last two.
- **Footer band:** tinted ink band, 30px; 13px `#2c3e50` centered text "The model output enters the warehouse in the same column shape as a measurement".

## Regeneration instructions

- **Layout:** tracking-page `.obj-table`: full-width bordered table, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` + `.lede` + bullets + `.key-point` callouts + `.lbl` pills, right `<td>` (55%, centered) holds the canvas; the "What does it collect?" row also carries the `.payload-note` caption and `.payload` pre block under its canvas (both left-aligned).
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; subtitle `#666` 1.05em; table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold `#1a5276` 1.1em; `li b` `#1a5276` weight 600, li 0.93em. No nav bar, no back/home links.
- **Callouts:** `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em, leading `<strong>` in `#1a5276`.
- **Label pills:** `.lbl` inline-block uppercase 0.7em bold, padding 2px 7px, radius 3px; `.lbl-purpose` background `#eaf2fb` color `#1a5276`; `.lbl-effect` background `#fdf0e6` color `#a8501c`.
- **Payload:** `.payload` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px, ui-monospace 0.78em, `white-space: pre`, line-height 1.45; `.payload-note` 0.82em `#666` italic.
- **Canvas:** each declares intrinsic `width="720" height="320"`; a shared `setupCanvas(id)` reads the element's attributes, sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Helpers: `tint(hex, a)` for translucent fills from palette tokens, `rr()` rounded-rect, and a shared agreement-code map `AGREE = { agree: green, missing: violet, extra: magenta }` so the same case reads the same colour across charts. Charts hardcode literal data arrays (no Math.random).
- **Palette (declared once as tokens):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`; ink `#1a5276` (headings/axes only), text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red is reserved for genuine alarm states and is not used on this page. Site-wide accent palette: #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange.
- In regenerated HTML, any card links use `.html` extensions (this page has none).
