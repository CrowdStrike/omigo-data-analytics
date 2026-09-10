# Tracking Data: Tracked Motion Controllers

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one row per section)
**HTML title tag:** Tracking Data: Tracked Motion Controllers

**Subtitle:** A handheld controller senses motion with an IMU that drifts; a visual system pins it to a place in the room. The pose you see is a fusion of the two — an estimate, not a reading.

## What is it?

Two instruments fused: fast-but-drifting inertial sensing, slow-but-absolute visual fixes.

- **IMU in the hand:** an accelerometer and gyroscope sample rotation and acceleration hundreds of times per second, but integrating them into a position accumulates drift within seconds
- **A visual system supplies the fix:** an early console remote watched infrared beacons near the TV (the Wii's "sensor bar" is just two IR clusters — the camera is in the remote); later systems track a glowing orb or an LED constellation on the controller with cameras, or sweep the room with laser base stations
- **Fusion:** the pose that games consume is the IMU stream continuously corrected by visual fixes — neither instrument alone produces it

Key-point callout: **The pose is a fusion estimate, not a reading:** when the cameras lose sight of the controller, the pose silently degrades to dead reckoning. The numbers keep arriving at the same rate — with a growing error that nothing in the stream prints.

### Visualization (canvas `c1`, 720×320)

Living-room scene: a TV with a camera above it, a player holding a VR-style controller in each hand — the raised one inside the camera's tracking cone, the lowered one outside it.

- **Title (bold 14px `#1a5276`, centered):** "The camera sees the orb; the IMU feels the motion".
- **Floor line:** `#e5e9ef` 1px horizontal at y≈285.
- **TV (left):** screen rect (70, 80) 200×120, stroke `#2a78d6` 2px, fill `#f0f4f8`; stand: two short legs to the floor. Camera above the TV center: small magenta `#d55181` rect ~26×14 at y≈62 with a white lens circle and blue pupil dot; label "camera" (12px `#6b7280`) to its left.
- **On the TV screen:** a rounded chip (stroke `#199e70`) with two monospace-style lines, `right: visual` and `left: inertial` (11px aqua); beneath it (10px `#6b7280`): "pose = IMU stream + camera fixes".
- **Person (right):** stick figure standing on the floor line — head circle r=16 at (560, 110), torso line to (560, 220), two legs to (540, 283) and (580, 283); right arm raised toward the TV: shoulder (560, 150) → elbow (505, 130) → wrist (472, 116); left arm hanging to (592, 190).
- **Controllers (one per hand):** each a short grip line (`#2c3e50` 3px) ending in a glowing LED orb — aqua `#199e70` disc r=7 over a soft halo `rgba(25,158,112,0.18)` r=14. Raised hand: grip to (458, 102), orb at (452, 95). Lowered hand: grip to (602, 202), orb at (607, 209).
- **Camera cone:** dashed magenta lines (dash 3/3) from the camera lens to just above and below the raised orb, with a faint magenta fill (`rgba(213,81,129,0.06)`) between them; label along the top edge (11px `#6b7280`): "tracking cone".
- **Annotations (11px `#6b7280`, centered under the raised arm):** "LED orb — what the camera watches" and "IMU inside the grip — fast, drifts".
- **Lowered-controller label (11px `#6b7280`, left-aligned, two lines beside it):** "outside the cone —" / "dead reckoning".
- **Bottom caption (12px `#6b7280`, centered):** "Schematic — the pose the game consumes is the IMU stream pinned down by camera fixes."

## What does it collect?

- **6DoF pose** — position and orientation of each controller, up to hundreds of samples per second
- **Raw IMU samples** — angular velocity and linear acceleration
- **Button, trigger, and grip states** with timestamps
- **Tracking status** per sample — visually confirmed vs dead reckoning
- **The play space:** room-scale tracking requires a mapped boundary — and newer headsets go further, depth-scanning the room into a 3D mesh of walls and furniture so virtual content can be drawn against the real environment

Key-point callouts:

- **Motion-capture grade by accident:** a 6DoF hand trail at a hundred-plus samples per second is the same data a mocap studio produces — reach, height, handedness, and tremor are all visible in it.
- **Consumed, mostly not kept:** the pose stream drives the game and is discarded; what persists is session-level activity. Retention is a design choice — the sensor does not make it.

### Visualization (canvas `c2`, 720×320)

Two sample streams merging into one: dense IMU ticks and sparse camera fixes fusing into the pose stream.

- **Title (bold 14px `#1a5276`, centered):** "Two sample rates, one output stream".
- **Row 1 (y≈90), "IMU · hundreds of Hz":** a dense row of small blue `#2a78d6` ticks (~60 ticks across x=150…650), label left-aligned at x=20 (13px `#2c3e50`), sub-label "measures change, drifts" (11px `#6b7280`).
- **Row 2 (y≈150), "cameras · tens of Hz":** sparse aqua `#199e70` dots (r=4) every ~10th position on the same span, label + sub-label "measures place, lags".
- **Merge arrows:** two short orange `#d95926` arrows from rows 1 and 2 converging to row 3.
- **Row 3 (y≈230), "fused pose · what the game sees":** medium-density violet `#4a3aa7` ticks, sub-label "an estimate whose error depends on how recent the last fix is".
- **Bottom caption (12px `#6b7280`, centered):** "Schematic — when the controller leaves the cameras' view, row 2 goes silent and row 3 keeps flowing."

### Sample payload (right column, under canvas `c2`)

Caption (italic): "Sample payload — illustrative structure, not real captured data."

```
// Field names are placeholders — controller protocols are
// not published as public schemas. Reconstruction; the
// measured / estimated split is the part worth reading.
{
  "controller": "left",
  "t_us": 81234567,

  // ── measured by sensors ──
  "gyro_dps":  [1.2, -0.4, 0.1],
  "accel_g":   [0.02, -0.98, 0.04],
  "buttons":   { "trigger": 0.0, "grip": 0.42 },

  // ── estimated — fusion output, not a sensor reading ──
  "pose": {
    "pos_m": [0.31, 1.42, -0.28],   // relative to the mapped play space
    "rot_q": [0.71, 0.01, -0.70, 0.02],
    "tracking": "visual"            // or "inertial_only" — error grows
  }
}
```

## Why is it collected?

Label pill: STATED PURPOSE

- **Low-latency pointing** — a swing has to land in the same frame it happens
- **Play-space safety** — the mapped boundary warns before a wall does

Label pill: ADDITIONAL CONSEQUENCE

- The mapped boundary is a **rough floor plan of a private room**, created as a side effect of tracking a game controller
- Where pose streams are retained — telemetry, research modes — they carry the **body geometry and motor patterns** of the player

Key-point callout: **Every position is relative to that day's play space:** the origin moves when the system is recalibrated or the furniture changes, so coordinates are not comparable across sessions, let alone across households. A study that pools raw controller positions is comparing room setups and mounting heights along with people.

### Visualization (canvas `c3`, 720×320)

The same swing recorded in two sessions whose play-space origins differ — identical shape, shifted coordinates.

- **Title (bold 14px `#1a5276`, centered):** "One player, one swing, two calibrations"; sub-line (12px `#6b7280`): "the arc is identical; the coordinates are not".
- **Plot area:** a simple x/z floor view, axes `#e5e9ef` with origin marks.
- **Arc A (blue `#2a78d6`, 2.5px):** a swing arc from hardcoded points `[[0.2,0.1],[0.32,0.22],[0.45,0.3],[0.6,0.34],[0.75,0.3],[0.88,0.22],[1.0,0.1]]` mapped into the left-center of the plot; origin cross labeled "session 1 origin" (11px blue).
- **Arc B (orange `#d95926`, 2.5px):** the same seven points offset by (+0.35, +0.18) — drawn shifted right/up; origin cross labeled "session 2 origin (recalibrated)" (11px orange).
- **Offset marker:** dashed aqua `#199e70` line connecting the two arcs' peak points, labeled bold 11px aqua "same motion, shifted frame".
- **Bottom caption (italic 11px `#6b7280`, centered):** "Illustrative coordinates — positions are relative to a mapped play space, not to the Earth."

## Regeneration instructions

- **Layout:** tracking detail page — `<table class="obj-table">`, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` heading, optional `.lede`, bullets with bold lead terms (`li b` in `#1a5276`), `.key-point` callouts, `.lbl` label pills; right `<td>` (55%, text-align center) holds the canvas, and for the "What does it collect?" row also the `.payload-note` caption plus `<pre class="payload">` block (left-aligned).
- **Page CSS:** body system sans-serif (-apple-system stack), white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; `.obj-title` bold 1.1em `#1a5276`; td borders `1px solid #2980b9`, padding 16px; li 0.93em; `.key-point` background `#f8f9fa`, left border `3px solid #1a5276`, 0.93em, `strong:first-child` in `#1a5276`; `.lede` 0.95em; `.lbl` pills 0.7em bold uppercase radius 3px — `.lbl-purpose` `#eaf2fb`/`#1a5276`, `.lbl-effect` `#fdf0e6`/`#a8501c`; `.payload` background `#f8f9fa`, left border `3px solid #1a5276`, ui-monospace 0.78em, white-space pre; `.payload-note` 0.82em italic `#666`. No nav bar, no back/home links.
- **Canvas:** each canvas declares intrinsic `width`/`height` attributes; a shared `setupCanvas(id)` helper multiplies the backing store by `window.devicePixelRatio`, fixes CSS size, and calls `ctx.scale(dpr, dpr)`. Charts use hardcoded literal arrays — never `Math.random()`.
- **Palette:** charts use the validated categorical palette `P = { blue: #2a78d6, green: #008300, magenta: #d55181, yellow: #c98500, aqua: #199e70, orange: #d95926, violet: #4a3aa7, ink: #1a5276, text: #2c3e50, mute: #6b7280, grid: #e5e9ef }`; red is deliberately not in the rotation (reserved for genuine alarm states).
- In regenerated HTML, any card links elsewhere use `.html` extensions.
