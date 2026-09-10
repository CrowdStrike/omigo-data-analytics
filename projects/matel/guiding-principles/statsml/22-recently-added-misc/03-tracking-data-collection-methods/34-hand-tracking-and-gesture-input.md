# Tracking Data: Hand Tracking & Gesture Input

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one row per section)
**HTML title tag:** Tracking Data: Hand Tracking & Gesture Input

**Subtitle:** Cameras fit a skeleton to the hand and a classifier turns poses into commands. The gesture is a model output — and how a person gestures is close to a signature.

## What is it?

A camera estimates hand joints; a classifier decides what you meant.

- **Mechanism:** headset or depth cameras fit a skeleton of roughly two dozen joints per hand to each frame — no hardware on the hand itself
- **A second stage classifies:** joint trajectories are matched against a gesture vocabulary — pinch, palm, point — and only a matched event becomes a command
- **Dwell to confirm:** for consequential actions some systems require a static gesture held for several seconds, trading speed for a lower false-activation rate
- **Where it appears:** XR headsets, camera-based consoles, some laptops and cars

Key-point callout: **Two models deep before anything happens:** the skeleton is a model fit to pixels, and the gesture is a classifier on the skeleton. A "command" is the second model agreeing with the first — each stage has its own error rate, and the user only ever sees the combined one.

### Visualization (canvas `c1`, 720×320)

Living-room scene: a person stands in front of a TV with a camera above it; the camera's cone covers their raised hand, a skeleton is fit to the hand, and the recognized command appears on screen with a dwell progress bar.

- **Title (bold 14px `#1a5276`, centered):** "Nothing on the hand: the camera watches, a model decides".
- **Floor line:** `#e5e9ef` 1px horizontal at y≈285.
- **TV (left):** screen rect (70, 80) 200×120, stroke `#2a78d6` 2px, fill `#f0f4f8`; stand: two short legs to the floor. Camera above the TV center: small magenta `#d55181` rect ~26×14 at y≈62 with a white lens circle and blue pupil dot; label "camera" (12px `#6b7280`) to its left.
- **On the TV screen:** a rounded chip (stroke `#199e70`) with monospace-style text `gesture: pinch  ·  0.87` (12px aqua); below it a dwell progress bar — track `#e5e9ef` 120×10, fill aqua to 68% — labeled beneath (10px `#6b7280`): "hold to confirm · 3.4s of 5s".
- **Person (right):** stick figure standing on the floor line — head circle r=16 at (560, 110), torso line to (560, 220), two legs to (540, 283) and (580, 283), left arm hanging to (592, 190); right arm raised toward the TV: shoulder (560, 150) → elbow (505, 130) → wrist (462, 112).
- **Tracked hand:** small skeleton at the wrist point — five short finger polylines fanning from (462, 112), joints as blue `#2a78d6` dots (r=2.5), fingertips magenta; thumb and index tips nearly touching (the pinch).
- **Camera cone:** dashed magenta lines (dash 3/3) from the camera lens to just above and below the raised hand, with a faint magenta fill (`rgba(213,81,129,0.06)`) between them; label along the top edge (11px `#6b7280`): "tracking cone".
- **Annotation (12px `#6b7280`, near the hand):** "skeleton fit per frame — ~2 dozen joints".
- **Bottom caption (12px `#6b7280`, centered):** "Schematic — the person holds nothing; the cone and the model are the entire input chain."

## What does it collect?

- **Joint positions per frame** — a full hand skeleton at the display frame rate
- **Gesture events** with a confidence score and timestamp
- **Pinch strength** as a continuous value, where the platform exposes it
- **Dwell progress** toward a hold-to-confirm threshold
- **Handedness and hand geometry**, needed to fit the skeleton at all
- **Derived: gesture dynamics** — the speed, amplitude, and timing with which one person forms each gesture

Key-point callouts:

- **The event is cheap, the stream is not:** platforms hand apps the pinch event freely but gate the continuous skeleton stream behind a permission — a skeleton stream is behavioral data, the same shape as keystroke timing.
- **A vocabulary is a threshold set:** every gesture in the set carries a false-accept and a false-reject rate, and widening the vocabulary raises confusions between neighboring gestures.

### Visualization (canvas `c2`, 720×320)

Continuous estimate vs discrete commands: a pinch-strength trace with a threshold, and the event marks it produces.

- **Title (bold 14px `#1a5276`, centered):** "The command channel is a threshold on a continuous estimate".
- **Trace:** pinch-strength (0–1) over ~30 hardcoded samples spanning x=70…650, plotted as a blue `#2a78d6` 2px line in a strip y≈70–190. Data: `[0.05,0.06,0.08,0.12,0.3,0.62,0.88,0.93,0.9,0.7,0.4,0.15,0.08,0.1,0.35,0.55,0.58,0.52,0.4,0.2,0.1,0.07,0.3,0.7,0.9,0.94,0.9,0.6,0.25,0.08]`.
- **Threshold line:** dashed violet `#4a3aa7` horizontal at 0.75, labeled right in bold 12px violet "classifier threshold".
- **Event marks:** aqua `#199e70` tick + dot under each crossing above threshold (samples ~6–8 and ~24–26) labeled "pinch"; the middle bump (~0.58 peak) gets a `#6b7280` label "near miss — no event".
- **Bottom caption (12px `#6b7280`, centered):** "Illustrative values. Two pinches were intended; the middle one was real to the user and invisible to the log."

### Sample payload (right column, under canvas `c2`)

Caption (italic): "Sample payload — illustrative structure, not real captured data."

```
// Joint set follows the documented OpenXR hand-tracking
// extension (26 joints per hand); values are not real.
{
  "t_us": 45120843,
  "hand": "right",
  "is_tracked": true,

  // ── documented joint stream (26 joints; three shown) ──
  "joints": {
    "wrist":     { "pos_m": [0.31, 1.02, -0.24], "radius_m": 0.021 },
    "thumb_tip": { "pos_m": [0.27, 1.06, -0.29], "radius_m": 0.009 },
    "index_tip": { "pos_m": [0.26, 1.07, -0.30], "radius_m": 0.008 }
  },

  // ── inferred / plausible — classifier output ──
  "gesture":        "pinch",
  "confidence":     0.87,
  "pinch_strength": 0.92,
  "dwell_ms":       3400,     // held toward a 5000 ms confirm
  "dwell_target":   "erase_drawing"
}
```

## Why is it collected?

Label pill: STATED PURPOSE

- **Controller-free input** — hands work without holding, pairing, or charging anything
- **Hold-to-confirm gestures** guard destructive actions, because a twitch should not delete a file

Label pill: ADDITIONAL CONSEQUENCE

- The stream that recognizes a pinch also captures **hand size, tremor, and timing** — studied as a behavioral biometric, enough to help distinguish people
- Whether gesture dynamics are retained beyond the frame is a **policy choice**; recognition itself does not require it

Key-point callout: **Dwell time is an error dial, not a courtesy:** the hold-for-seconds confirm exists because gesture recognition has a false-activation rate that a physical button does not. The dwell length is a threshold someone tuned — shorten it and accidental confirms rise, lengthen it and users abandon the action. A comparison of "task completion time" across systems with different dwell settings compares configurations, not users.

### Visualization (canvas `c3`, 720×320)

Two error curves against dwell duration, with the chosen operating point marked.

- **Title (bold 14px `#1a5276`, centered):** "Why the confirm gesture is held for seconds"; sub-line (12px `#6b7280`): "two failure modes move in opposite directions as the hold lengthens".
- **Axes:** x = required hold duration 0–6 s (labels 0…6 at even ticks, 11px `#6b7280`); y unlabeled "rate" axis, y maps 0–1 between baseline y≈240 and top y≈70; axis lines `#e5e9ef`.
- **Curve A (orange `#d95926`, 2.5px), "accidental confirms":** falls from high to near zero — hardcoded points at each 0.5 s: `[0.9,0.62,0.4,0.25,0.15,0.09,0.055,0.035,0.022,0.015,0.01,0.008,0.006]`.
- **Curve B (blue `#2a78d6`, 2.5px), "given up mid-hold":** rises slowly then steeply: `[0.0,0.005,0.01,0.02,0.03,0.05,0.07,0.10,0.14,0.19,0.26,0.35,0.46]`.
- **Operating point:** vertical dashed aqua `#199e70` line at x=5 s labeled bold 12px aqua "a vendor's chosen dwell"; both curve labels sit at their right ends in bold 12px of their colors.
- **Bottom caption (italic 11px `#6b7280`, centered):** "Illustrative curves — the trade-off shape is the point, not the numbers."

## Regeneration instructions

- **Layout:** tracking detail page — `<table class="obj-table">`, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` heading, optional `.lede`, bullets with bold lead terms (`li b` in `#1a5276`), `.key-point` callouts, `.lbl` label pills; right `<td>` (55%, text-align center) holds the canvas, and for the "What does it collect?" row also the `.payload-note` caption plus `<pre class="payload">` block (left-aligned).
- **Page CSS:** body system sans-serif (-apple-system stack), white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; `.obj-title` bold 1.1em `#1a5276`; td borders `1px solid #2980b9`, padding 16px; li 0.93em; `.key-point` background `#f8f9fa`, left border `3px solid #1a5276`, 0.93em, `strong:first-child` in `#1a5276`; `.lede` 0.95em; `.lbl` pills 0.7em bold uppercase radius 3px — `.lbl-purpose` `#eaf2fb`/`#1a5276`, `.lbl-effect` `#fdf0e6`/`#a8501c`; `.payload` background `#f8f9fa`, left border `3px solid #1a5276`, ui-monospace 0.78em, white-space pre; `.payload-note` 0.82em italic `#666`. No nav bar, no back/home links.
- **Canvas:** each canvas declares intrinsic `width`/`height` attributes; a shared `setupCanvas(id)` helper multiplies the backing store by `window.devicePixelRatio`, fixes CSS size, and calls `ctx.scale(dpr, dpr)`. Charts use hardcoded literal arrays — never `Math.random()`.
- **Palette:** charts use the validated categorical palette `P = { blue: #2a78d6, green: #008300, magenta: #d55181, yellow: #c98500, aqua: #199e70, orange: #d95926, violet: #4a3aa7, ink: #1a5276, text: #2c3e50, mute: #6b7280, grid: #e5e9ef }`; red is deliberately not in the rotation (reserved for genuine alarm states).
- In regenerated HTML, any card links elsewhere use `.html` extensions.
