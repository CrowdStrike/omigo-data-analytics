# Tracking Data: Fitness Game Peripherals

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one row per section)
**HTML title tag:** Tracking Data: Fitness Game Peripherals

**Subtitle:** A game controller that measures exertion — a flexed ring, a leg strap, a motion sensor. It builds a workout history like a fitness product, governed like a game.

## What is it?

Exercise hardware sold as a game accessory.

- **Hardware:** a rigid ring with a strain sensor that measures press and pull force (the Ring-Con bundled with a well-known fitness game is the best-known example), plus a motion sensor strapped to the leg or held in the hand
- **The game is the interface:** force, reps, and pace drive gameplay, and sampling ends when play does
- **Self-reported inputs:** the game asks for weight and age to estimate effort

Key-point callout: **A fitness record outside the fitness category:** the same rep counts and calorie estimates a health app would sync sit in a game save, under a game's terms of service — the "is this health data?" question never gets asked, because nothing about the product says health.

### Visualization (canvas `c1`, 720×320)

Living-room scene: a player stands in front of a TV squeezing a ring controller with both hands, a motion sensor strapped to one leg; the game on screen counts reps and shows a calorie estimate.

- **Title (bold 14px `#1a5276`, centered):** "A workout, measured by a game controller".
- **Floor line:** `#e5e9ef` 1px horizontal at y≈285.
- **TV (left):** screen rect (70, 80) 200×120, stroke `#2a78d6` 2px, fill `#f0f4f8`; stand: two short legs to the floor.
- **On the TV screen:** two rounded chips (stroke `#199e70`) with monospace-style text `ring press: 45 reps` and `kcal: 96.42` (11px aqua); beneath them (10px `#6b7280`): "weight? age? — typed in once".
- **Person (right):** side-view stick figure facing the TV — head circle r=16 at (555, 115), torso line to (555, 215), two legs to (535, 283) and (575, 283); one arm up from the shoulder (~555, 150) to the ring's top grip, one arm down to its bottom grip.
- **Ring controller:** orange `#d95926` 3px circle r=32 centered (492, 165), held vertically in front of the chest; short thick grip pads (`#2c3e50`, 8px arc segments) at its top and bottom where the hands hold it.
- **Squeeze arrows:** two small magenta `#d55181` vertical arrows pressing inward at the ring's top and bottom grips; label above (11px `#6b7280`, centered): "strain sensor measures press & pull".
- **Leg strap:** small rect on the left leg, aqua `#199e70` border with faint aqua fill; label near the floor (11px `#6b7280`, centered): "leg strap — motion sensor".
- **Wireless link:** dashed violet `#4a3aa7` curve from the ring to the TV's right edge; label above it (11px `#6b7280`): "force & motion, sent to the game".
- **Bottom caption (12px `#6b7280`, centered):** "Schematic — the ring and strap sample only while the game runs; the history lives in a game save."

## What does it collect?

- **Press and pull force** from the strain sensor, sampled during exercises
- **Body motion** from the strapped or held controller — knee lifts, squats, pace
- **Reps and holds**, counted by thresholds on those signals
- **Session log** — exercises done, duration, streaks, difficulty setting
- **Self-reported weight and age**, feeding the effort estimate
- **Derived: calorie and distance estimates** per session, from a model

Key-point callouts:

- **A rep is a threshold decision:** a shallow squat counts or does not depending on a cutoff someone tuned for fun — rep totals are gameplay numbers wearing exercise units.
- **The series measures play, not fitness:** sampling exists only while the game is on. A missing week means the person did not play, not that they did not move — reading the log as an activity history confuses engagement with exercise.

### Visualization (canvas `c2`, 720×320)

Coverage comparison over one month: a wrist wearable's continuous record vs a game peripheral's session-only record.

- **Title (bold 14px `#1a5276`, centered):** "Two records of the same month".
- **Row 1 (y≈100), "wrist wearable":** a continuous horizontal band x=170…660, height 22, fill `rgba(42,120,214,0.35)` with a `#2a78d6` 1px border; sub-label (11px `#6b7280`): "samples whether or not anything happens".
- **Row 2 (y≈170), "fitness game peripheral":** short filled blocks (fill `rgba(217,89,38,0.55)`) only on play days — day indices `[1,2,4,7,8,9,15,16,22,28]` of 30, each block ~day-width, on a faint `#e5e9ef` baseline track; sub-label: "samples only during play sessions".
- **Day ticks:** light ticks along x with labels "day 1" and "day 30" at the ends (10px `#6b7280`).
- **Annotation:** a `#6b7280` bracket over days 17–21 of row 2 labeled "gap = didn't play, not didn't move" (11px).
- **Bottom caption (12px `#6b7280`, centered):** "Schematic month — the gaps in row 2 are structural, not behavioral."

### Sample payload (right column, under canvas `c2`)

Caption (italic): "Sample payload — illustrative structure, not real captured data."

```
// Field names are placeholders — game save formats are not
// public schemas. Reconstruction; the self-reported /
// measured / derived split is the part worth reading.
{
  "profile": "player_2",
  "session": {
    "date": "2026-08-25",
    "duration_min": 24,

    // ── measured during play ──
    "ring_force_peak": 142,       // strain sensor, internal units
    "reps": { "squat": 30, "ring_press": 45, "knee_lift": 60 },

    // ── self-reported ──
    "weight_kg": 71,
    "age": 34,

    // ── derived — model output ──
    "kcal_est": 96.42,
    "distance_km_est": 1.8
  },
  "streak_days": 12,
  "difficulty": 21
}
```

## Why is it collected?

Label pill: STATED PURPOSE

- **Gameplay input** — force and reps are the controls, not a side channel
- **Progress and streaks** attach a fitness habit to a game loop

Label pill: ADDITIONAL CONSEQUENCE

- Years of dated exercise sessions, weight entries and effort estimates accumulate in an **account-tied log governed by game terms**, not health policy
- The calorie figure is a **model output applied to a self-report** — estimated effort scaled by an entered weight

Key-point callout: **Precision theater:** an effort estimate built on a self-reported weight and a tuned rep threshold is displayed to two decimal places. The number is not wrong so much as unaccompanied — no error bar, and no way for the player to tell 96.42 from "roughly a hundred".

### Visualization (canvas `c3`, 720×320)

The self-report drives the "measurement": the same session's calorie estimate under two weight entries.

- **Title (bold 14px `#1a5276`, centered):** "Same workout, different weight entry"; sub-line (12px `#6b7280`): "the estimate inherits whatever the player typed".
- **Two horizontal bars** (height 34, starting x=250, gray `#e5e9ef` track to x=640): "weight entered: 62 kg" → bar filled `rgba(42,120,214,0.55)` to 84.1, value label "84.10 kcal" bold 13px `#2a78d6`; "weight entered: 80 kg" → bar filled `rgba(217,89,38,0.55)` proportionally longer to 108.5, value label "108.50 kcal" bold 13px `#d95926`. Bar labels left-aligned at x=60 (13px `#2c3e50`).
- **Bracket:** aqua `#199e70` bracket spanning the two bar ends, labeled bold 11px aqua "~30% swing from a typed number".
- **Note (11px `#6b7280`, centered, under bars):** "both shown to two decimals; neither carries an error bar".
- **Bottom caption (italic 11px `#6b7280`, centered):** "Illustrative values — the sensitivity to the self-report is the point, not the exact kcal."

## Regeneration instructions

- **Layout:** tracking detail page — `<table class="obj-table">`, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` heading, optional `.lede`, bullets with bold lead terms (`li b` in `#1a5276`), `.key-point` callouts, `.lbl` label pills; right `<td>` (55%, text-align center) holds the canvas, and for the "What does it collect?" row also the `.payload-note` caption plus `<pre class="payload">` block (left-aligned).
- **Page CSS:** body system sans-serif (-apple-system stack), white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; `.obj-title` bold 1.1em `#1a5276`; td borders `1px solid #2980b9`, padding 16px; li 0.93em; `.key-point` background `#f8f9fa`, left border `3px solid #1a5276`, 0.93em, `strong:first-child` in `#1a5276`; `.lede` 0.95em; `.lbl` pills 0.7em bold uppercase radius 3px — `.lbl-purpose` `#eaf2fb`/`#1a5276`, `.lbl-effect` `#fdf0e6`/`#a8501c`; `.payload` background `#f8f9fa`, left border `3px solid #1a5276`, ui-monospace 0.78em, white-space pre; `.payload-note` 0.82em italic `#666`. No nav bar, no back/home links.
- **Canvas:** each canvas declares intrinsic `width`/`height` attributes; a shared `setupCanvas(id)` helper multiplies the backing store by `window.devicePixelRatio`, fixes CSS size, and calls `ctx.scale(dpr, dpr)`. Charts use hardcoded literal arrays — never `Math.random()`.
- **Palette:** charts use the validated categorical palette `P = { blue: #2a78d6, green: #008300, magenta: #d55181, yellow: #c98500, aqua: #199e70, orange: #d95926, violet: #4a3aa7, ink: #1a5276, text: #2c3e50, mute: #6b7280, grid: #e5e9ef }`; red is deliberately not in the rotation (reserved for genuine alarm states).
- In regenerated HTML, any card links elsewhere use `.html` extensions.
