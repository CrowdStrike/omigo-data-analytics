# Tracking Data: Sports Athlete Tracking

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one row per section; second row contains an inner 4-column map-table)
**HTML title tag:** Tracking Data: Sports Athlete Tracking

**Subtitle:** Different sports measure athletes with different devices, because each sport breaks a different measurement method. What is measured directly — and what has to be computed instead — changes with the sport.

## What is it?

A measuring device chosen to fit the sport.

- **On a field:** a vest worn high on the upper back, holding a satellite receiver (GNSS) plus accelerometer, gyroscope and magnetometer
- **Elsewhere:** in a pool, at depth, on a bike or on an indoor court, that vest gets no signal or answers the wrong question
- **The physics chooses:** a satellite receiver needs sky, radio does not travel through water, a camera needs line of sight, a strain gauge needs equipment to sit inside

Key-point callout: **The sport determines the sensor:** each sport eliminates a different measurement method, so the directly measured quantity differs from sport to sport. Two squads in two sports do not hold comparable data even when both report a column called "distance" or "load".

### Visualization (canvas `c1`, 720×320)

Environment × method compatibility matrix.

- **Title (bold 16px `#2a78d6`, centered):** "The environment rules methods out before anyone picks a supplier".
- **Columns (bold 13px `#2a78d6`):** "Open field", "Indoor hall", "In / under water".
- **Rows (13px `#2c3e50`, left-aligned, alternate rows striped `rgba(42,120,214,0.06)`):**
  - Satellite positioning (GNSS): ✓ ✗ ✗
  - Local radio anchors: ✓ ✓ ✗
  - Multi-camera optical: ✓ ✓ ~ (with note beside the ~: "above water only")
  - Worn inertial unit: ✓ ✓ ✓
  - Doppler radar (tracks the ball): ✓ ✓ ✗
  - Pressure transducer (depth): ✗ ✗ ✓
  - Strain gauge (force): ✓ ✓ ✓
- **Marks:** ✓ green `#008300` bold 15px; ~ orange `#d95926`; ✗ gray `#95a5a6`. Layout: labels at x=8, grid starts x=200, column width 165, row height 23, top y=50; thin blue rules above and below the grid.
- **Footer lines (13px `#6b7280`, left-aligned):** "✓ works here      ~ limited      ✗ does not work here" and "The mechanical sensors also need a piece of equipment to sit inside, which not every sport has."

## What does it collect?

Inner map-table (headers: Sport / Device / Measured directly / Derived from it):

| Sport | Device | Measured directly | Derived from it |
|---|---|---|---|
| Field sports — soccer, rugby, Australian rules, American football | Upper-back vest: GNSS receiver plus inertial unit | Position fixes; triaxial acceleration, rotation, heading | Distance, speed, sprint counts, composite "load" |
| Indoor court sports | Local positioning system with fixed anchors (radio ranging), or multi-camera optical tracking with nothing worn at all | Range to each anchor, or pixel positions per frame | Court position, then the same movement metrics |
| Ball-flight sports — baseball pitching, golf, tennis, cricket | Doppler radar, high-frame-rate multi-camera systems, or both | Ball trajectory — the object tracked is the ball, not the athlete | Spin rate, launch angle, release point |
| Swimming | Worn inertial unit plus lap detection | Acceleration and rotation; wall-touch events | Stroke rate and count, lap splits, turn time |
| Diving and freediving | Pressure transducer (depth gauge) | Water pressure — depth is not measured | Depth, computed by assuming a water density — fresh and salt water give different depths for the same pressure |
| Cycling and rowing | Strain gauge in the crank, the oar, or the oarlock | Force, measured mechanically | Power and work — the rare case where the quantity of interest is measured rather than inferred from motion |

Key-point callouts:

- **Measured and computed share one flat record:** the accuracy estimate is frequently dropped downstream, so a position fix and a threshold-dependent sprint count arrive as two ordinary columns, read with equal trust.
- **"Load" is not comparable:** a weighted sum of accelerometer output with the supplier's own weights and no shared definition. Changing supplier breaks the historical series, and a season-over-season comparison stops meaning anything while still rendering as a chart.

### Visualization (canvas `c2`, 720×320)

Three-panel schematic: differentiation amplifies noise (one movement, three quantities).

- **Titles (centered):** bold 16px `#2a78d6` "Each differentiation amplifies the noise underneath"; sub-line 13px `#6b7280` "Schematic, no scale. One movement, three quantities, one unit."
- **Panels** (210px wide, x=20/250/480, y from 48 to 168, fill `rgba(42,120,214,0.06)`, stroke `rgba(42,120,214,0.4)`), each with a blue `#2a78d6` 1.6px polyline plus 1.6px dots over its hardcoded normalized series:
  - Position (24 pts, smooth rising): `[0.10, 0.13, 0.18, 0.21, 0.27, 0.31, 0.38, 0.43, 0.48, 0.56, 0.59, 0.67, 0.70, 0.76, 0.79, 0.83, 0.84, 0.88, 0.90, 0.93, 0.94, 0.95, 0.97, 0.98]`
  - Speed (23 pts, jagged): `[0.42, 0.55, 0.38, 0.61, 0.44, 0.68, 0.50, 0.52, 0.74, 0.35, 0.71, 0.33, 0.58, 0.36, 0.46, 0.22, 0.44, 0.28, 0.35, 0.20, 0.24, 0.30, 0.21]`
  - Acceleration (22 pts, very jagged): `[0.62, 0.28, 0.78, 0.24, 0.82, 0.20, 0.55, 0.88, 0.14, 0.90, 0.12, 0.75, 0.22, 0.66, 0.16, 0.72, 0.26, 0.60, 0.30, 0.48, 0.58, 0.34]`
- **Panel labels (centered under each):** title bold 15px `#2a78d6` ("Position" / "Speed" / "Acceleration"), sub 13px `#2c3e50` ("measured" / "first derivative" / "second derivative"), note bold 13px orange `#d95926` ("error as measured" / "error amplified" / "error amplified again").
- **Chevrons:** orange 2px right-pointing chevrons between panels at x≈235 and x≈465.
- **Bottom caption (13px `#6b7280`, centered):** "All three leave the unit in one record and are read with the same confidence."

### Sample payload (right column, under canvas `c2`)

Caption (italic): "Sample payload — illustrative structure, not real captured data."

```
{
  // Field names are placeholders. No vendor wire format is
  // public, so the record below is a reconstruction of shape.
  "session_id": "S-4471…",
  "athlete_id": "ATH-0192",              // a named employee

  // ── measured / sampled by the unit ──
  "sample_rate":  "<as configured on the unit>",
  "fix":          { "lat": "<deg>", "lon": "<deg>",
                    "accuracy_est": "<receiver estimate>" },
  "accel_g":      ["<ax>", "<ay>", "<az>"],
  "gyro_dps":     ["<gx>", "<gy>", "<gz>"],

  // ── inferred / computed downstream ──
  "distance_total":      "<sum of position deltas>",
  "distance_high_speed": "<same sum, above a chosen speed>",
  "sprint_count":        "<crossings of a chosen threshold>",
  "peak_accel":          "<second derivative of position>",
  "load_score":          "<vendor weighted sum, formula private>"
}
```

## Why is it collected?

Label pill: STATED PURPOSE

- **Managing training load** — how hard this session was next to the last one, for a squad wearing the same unit on the same settings
- **Timing a return** from injury, and planning sessions and rotation

Label pill: ADDITIONAL CONSEQUENCE

- The same record supports **contract valuation, selection, and insurance or medical judgements** about a named employee
- It **outlives the session** and often the athlete's time at the club — and its thresholds were tuned for one squad, so the numbers do not carry meaning away from it

Key-point callout: **Survivorship in injury prediction:** a row exists only for a session that was played, so the training set is built from players durable enough to be recorded. The sessions an athlete was too injured to attend are the missing rows.

### Visualization (canvas `c3`, 720×320)

Bar chart: the players a risk model most needs contribute the fewest rows.

- **Titles (centered):** bold 13px `#1a5276` "Sessions in the training set, by how much of the season a player missed"; sub-line 12px `#6b7280` "a row exists only for a session that was played".
- **Data (illustrative, six players ordered by days unavailable):** A 0 days out / 92 rows; B 6 / 84; C 21 / 68; D 48 / 44; E 79 / 21; F 112 / 7.
- **Bars:** baseline y=224, max height 138 scaled to 100 rows, bar width 54, first bar centered at x=132, step 96. Bars with rows < 30 (E, F) highlighted orange (`rgba(217,89,38,0.45)` fill, `#d95926` stroke and value label); the rest blue (`rgba(42,120,214,0.30)` fill, `#2a78d6` stroke). Row count in bold 12px above each bar; below the baseline: "player A"…"player F" (bold 12px `#2c3e50`) and "N days out" (12px `#6b7280`).
- **X annotation (12px `#6b7280`, centered):** "more of the season missed  →".
- **Captions (centered):** italic 12px `#2c3e50` "The sessions a model would learn most from are the ones nobody was fit enough to record."; italic 11px `#6b7280` "Illustrative — the shape of the imbalance, not a real squad."

## Regeneration instructions

- **Layout:** tracking detail page — `<table class="obj-table">`, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` heading, optional `.lede`, bullets with bold lead terms (`li b` in `#1a5276`), `.key-point` callouts, `.lbl` label pills; the "What does it collect?" row additionally holds an inner `<table class="map-table">` (4 columns, left-aligned) in the left cell; right `<td>` (55%, text-align center) holds the canvas, and for row 2 also the `.payload-note` caption plus `<pre class="payload">` block (left-aligned; angle brackets HTML-escaped as `&lt;`/`&gt;`).
- **Page CSS:** body system sans-serif (-apple-system stack), white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; `.obj-title` bold 1.1em `#1a5276`; td borders `1px solid #2980b9`, padding 16px; li 0.93em; `.key-point` background `#f8f9fa`, left border `3px solid #1a5276`, 0.93em, `strong:first-child` in `#1a5276`; `.lede` 0.95em; `.lbl` pills 0.7em bold uppercase radius 3px — `.lbl-purpose` `#eaf2fb`/`#1a5276`, `.lbl-effect` `#fdf0e6`/`#a8501c`; `.payload` background `#f8f9fa`, left border `3px solid #1a5276`, ui-monospace 0.78em, white-space pre; `.payload-note` 0.82em italic `#666`; `.map-table` 0.82em, th background `#f8f9fa` color `#1a5276`, cell borders `1px solid #d5dbdb`, first column bold `#1a5276` 22% width. No nav bar, no back/home links.
- **Canvas:** each canvas declares intrinsic `width`/`height` attributes; a shared `setupCanvas(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Rounded-rect helper `rr()` available.
- **Palette:** charts use the validated categorical palette `P = { blue: #2a78d6, green: #008300, magenta: #d55181, yellow: #c98500, aqua: #199e70, orange: #d95926, violet: #4a3aa7, ink: #1a5276, text: #2c3e50, mute: #6b7280, grid: #e5e9ef }`; red is deliberately not in the rotation (reserved for genuine alarm states). Site palette anchors: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- In regenerated HTML, any card links elsewhere use `.html` extensions.
