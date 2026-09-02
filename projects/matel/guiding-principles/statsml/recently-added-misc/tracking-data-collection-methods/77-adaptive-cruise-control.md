# Tracking Data: Adaptive Cruise Control

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one row per section)
**HTML title tag:** Tracking Data: Adaptive Cruise Control

**Subtitle:** A radar behind the front grille times its own radio echoes to hold a chosen gap to the car ahead — the car speeds up and slows down on its own while the driver keeps steering.

## Section 1: What is it?

**Lede:** Cruise control that watches the car ahead and keeps a steady gap, instead of just holding one speed.

- **Hardware:** a radar behind the front grille sends out radio pulses and times how long each echo takes to bounce back — the delay tells it how far away the car ahead is
- **One bounded task:** it works the accelerator and brakes on its own to hold the gap; the driver still steers and stays responsible for the drive

**Key point — The gap is a time, not a distance:** the setting is how many seconds you trail the car ahead, not meters. The same 1.8-second setting is about 30 m at 60 km/h and about 65 m at 130 km/h — the actual distance stretches with speed on its own.

### Visualization (canvas `c1`, 720×320)

Side-view schematic: two cars on a road, a radar at the nose of the following car, a cone to the car ahead, the outgoing pulse and the returning timed echo.

- **Road:** 2px `#2c3e50` horizontal line at y=250 from x=40 to x=680.
- **Your car (left, blue `#2a78d6`):** body rounded rect (rr, radius 10) 150×40 at (80,205); cab rounded rect 80×32 radius 8 at (110,178); two wheel circles radius 13 fill `#2c3e50` centered (115,250) and (195,250). Label (11px mute `#6b7280`, centered above cab, y=168): "your car".
- **Car ahead (right, aqua `#199e70`):** same shapes shifted — body at (500,205), cab at (530,178), wheels at (535,250) and (615,250). Label (11px mute, centered above cab, y=168): "car ahead".
- **Radar unit:** small magenta `#d55181` rounded rect 8×16 at the nose of your car, (228,214).
- **Radar cone:** filled triangle from the radar face (236,222) opening to the lead car's rear — vertices (236,222), (500,200), (500,244) — magenta at alpha 0.10, plus 1px magenta alpha-0.3 edges.
- **Outgoing pulse:** three magenta 1.5px arcs centered on the radar face (236,222), radii 26/44/62, spanning −0.5 to +0.5 radians. Label (11px magenta, centered, y=170 at x=300): "radio pulse out".
- **Returning echo:** dashed (4/3) 1.5px orange `#d95926` line at y=232 from x=490 back to x=252, arrowhead at the left end. Label (11px orange, centered above the arrow at x=368, y=224): "echo back — round trip timed".
- **Gap dimension:** 1px mute line at y=282 from x=236 to x=500 with 6px vertical end ticks; label (11px mute, centered at x=368, y=300): "gap = half the round-trip time × radio speed".
- **Caption (14px `#2c3e50`, bottom center, y=314):** "The radar times its own echo — the delay is the distance to the car ahead. Schematic."

## Section 2: What does it collect?

- **Gap and closing speed** — how far the car ahead is, and how fast that gap is shrinking, both read from the returning echo
- **Driver choices** — the cruise speed you dialed in and the gap setting you picked
- **Overrides** — every time you press the brake or floor the accelerator over the system, with a timestamp

**Key point — Closing speed is measured, not guessed:** when the gap is changing, the echo comes back at a slightly shifted pitch (the Doppler effect). One pulse gives the closing rate directly — no comparing of snapshots, no model estimate.

### Visualization (canvas `c2`, 720×320)

One minute of the gap held around the set-point, then a cut-in and a driver brake override.

- **Title (bold 14px `#1a5276`, top center, y=22):** "Sixty seconds of gap-keeping, then the driver takes over".
- **Plot area:** x=70 to x=670 spans 0–60 s; y=260 (0 m) to y=60 (60 m).
- **Gridlines:** 1px `#e5e9ef` at 0/20/40/60 m with right-aligned 11px mute labels ("0 m" … "60 m") at x=62; x ticks at 0/15/30/45/60 s with 11px mute labels ("0 s" … "60 s") at y=278.
- **Set-gap line:** dashed (4/3) 1.5px `#6b7280` horizontal line at 50 m; left-aligned 11px mute label at (x=72, y=80): "set gap ≈ 50 m (1.8 s at 100 km/h)".
- **Gap trace (blue `#2a78d6`, 2.5px polyline), points (t s, gap m):** (0,50), (4,52), (8,49), (12,51), (16,50), (20,48), (24,51), (28,50), (32,49), (35,50), (36,34), (37,26), (38,22), (40,24), (44,30), (48,38), (52,45), (56,49), (60,50).
- **Cut-in marker:** right-aligned 11px mute label "car cuts in" at (X(36)−8, y=150).
- **Override marker:** vertical dashed (4/3) 1.5px orange `#d95926` line at t=38 from y=60 to y=260; orange dot radius 4 on the trace at (38 s, 22 m); left-aligned bold 11px orange label at (X(38)+8, y=70): "driver brakes — override logged".
- **Caption (italic 12px `#2c3e50`, bottom center, y=306):** "The system nudges the gap back to the set-point; the brake tap ends its control and is recorded. Illustrative trace."

**Payload note (below canvas, italic, left-aligned):** Sample payload — illustrative structure, not real captured data.

**Payload block (monospace, left border `#1a5276`):**

```
// ACC vendor schemas are not published.
// Whole block is reconstruction; field names generic.
// ── inferred / plausible ──
{
  "vehicle_id": "veh-30157",
  "ts":         "2026-08-22T14:31:07Z",

  // measured from the radar echo
  "gap_m":             48.2,
  "closing_speed_kmh": -3.5,     // negative = gap growing
  "lead_vehicle":      true,

  // the driver's own inputs
  "set_speed_kmh":   110,
  "gap_setting_s":   1.8,        // seconds, not meters
  "driver_override": "brake",
  "acc_state":       "disengaged"
}
```

## Section 3: Why is it collected?

**Label (purpose pill):** Stated purpose

- **Holding the gap** — speeding up and slowing down for you so the distance to the car ahead stays steady
- **Highway comfort** — less pedal work in cruising, and in stop-and-go traffic on some systems

**Label (effect pill):** Additional consequence

- **The settings travel with the telematics** — on connected cars, the set speed next to the posted limit, plus every override and disengagement, can be logged or uploaded with other driving data on some systems

**Key point — Not moving means invisible:** many radar-only systems throw away echoes from anything that is not itself moving, so signs and guard rails never trigger braking — and on older or radar-only systems, a car stopped in your lane can be discarded by that same filter.

### Visualization (canvas `c3`, 720×320)

Dot plot: objects placed by their own speed, with the stationary band the radar filters out — and the stopped car that lands in it.

- **Title (bold 14px `#1a5276`, top center, y=22):** "Which echoes the radar keeps".
- **Axis:** x=110 to x=650 spans 0–100 km/h of the object's own speed; baseline 1px `#e5e9ef` at y=252; ticks at 0/25/50/75/100 with 11px mute labels ("0 km/h" … "100 km/h") at y=270; axis label (11px mute, centered, y=290): "how fast the object itself is moving".
- **Filter band:** rect from x=96 to X(6 km/h), y=48 to y=252, fill mute `#6b7280` alpha 0.12. Header (bold 11px mute, centered over the band, y=44): "filtered as clutter".
- **Kept zone header (bold 11px green `#008300`, centered over the rest, y=44):** "kept and tracked".
- **Objects (dot radius 5 at (X(speed), row y), label 11px to the right of the dot at +12):**
  - road sign — speed 0, y=88, mute dot, mute label "road sign"
  - guard rail — speed 0, y=124, mute dot, mute label "guard rail"
  - stopped car in your lane — speed 0, y=160, **red `#e74c3c` dot radius 6** (genuine alarm state), bold red label "stopped car in your lane — same bin as the signs"
  - slow truck ahead — speed 60, y=196, blue `#2a78d6` dot, blue label right-aligned at −12: "slow truck ahead"
  - car ahead — speed 95, y=232, blue dot, blue label right-aligned at −12: "car ahead"
- **Caption (italic 11px mute, two centered lines, y=296 and y=312):** "Radar-only systems usually drop echoes that are not moving — a stopped car can land in the discard bin." / "Schematic — newer camera-plus-radar systems handle this case better."

## Regeneration instructions

- **Layout:** tracking-page `.obj-table`: full-width `border-collapse: collapse` table, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` + optional `.lede` + bullets + `.key-point` callouts + `.lbl` pills, right `<td>` (55%, `text-align: center`) holds the canvas — and in the "What does it collect?" row, the `.payload-note` and `.payload` `<pre>` below the canvas (both left-aligned).
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold 1.1em `#1a5276`; `li` 0.93em with `li b` in `#1a5276` weight 600. No nav bar, no back/home links.
- **Callouts:** `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em; leading `<strong>` in `#1a5276`.
- **Pills:** `.lbl` inline-block uppercase 0.7em bold, padding 2px 7px, radius 3px; `.lbl-purpose` background `#eaf2fb` color `#1a5276`; `.lbl-effect` background `#fdf0e6` color `#a8501c`.
- **Payload:** `.payload` — background `#f8f9fa`, left border `3px solid #1a5276`, ui-monospace 0.78em, `white-space: pre`; `.payload-note` italic 0.82em `#666` immediately above.
- **Canvas:** each declares intrinsic `width="720" height="320"`; a shared `setupCanvas(id)` reads those attributes, sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Shared helpers: `tint(hex, alpha)` for translucent fills and `rr()` rounded-rect path.
- **Palette (tracking pages):** categorical tokens blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`; ink `#1a5276` (headings/axes only), text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red `#e74c3c` appears once, on the stopped-car dot in c3 — a genuine alarm state, its only permitted use. (Site-wide palette reference: #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange.)
- In regenerated HTML, any card links use `.html` extensions.
