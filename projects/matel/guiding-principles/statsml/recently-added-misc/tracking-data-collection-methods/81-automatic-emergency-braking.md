# Tracking Data: Automatic Emergency Braking

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one row per section)
**HTML title tag:** Tracking Data: Automatic Emergency Braking

**Subtitle:** A radar and a camera on the front of the car keep counting the seconds left before it would hit what is ahead — and if the driver does nothing in time, the car brakes by itself.

## Section 1: What is it?

**Lede:** The one driving assist whose last step is the car acting, not suggesting.

- **Sensing:** a radar and a camera behind the windshield together keep estimating the seconds left before the car would touch what is ahead (time to collision)
- **Escalation:** when that number falls below one cutoff the car warns, below the next it pre-tightens the brakes, below the last it brakes by itself — but only if the driver has not reacted
- **Weaker in the dark:** in independent testing, stopping for pedestrians at night is usually markedly weaker than in daylight — how much varies by model and test program

**Key point — Acting, not suggesting:** lane warnings beep and blind-spot lights glow, and it stays the driver's move. This is the one assist where the final step is the car pressing the brake itself, and regulation increasingly requires it on new cars.

### Visualization (canvas `c1`, 720×320)

Line chart: the countdown-to-impact value falling over time, crossing three cutoff lines — warn, pre-tighten, brake. Red is used only for the final self-braking stage (genuine alarm state).

- **Title (bold 14px `#1a5276`, top center, y=22):** "The countdown, and the three cutoffs".
- **Plot area:** x=80 to x=660 spans 0–6 s of elapsed time; y=60 (5 s left) to y=260 (0 s left).
- **Gridlines:** 1px `#e5e9ef` horizontal lines at 0–5 s left, with right-aligned 11px mute labels "0 s" … "5 s" at x=72.
- **Axis notes (11px mute):** "seconds left before impact (time to collision)" left-aligned at (80, 44); "time passing →" centered below the plot at y=286.
- **Countdown line (blue `#2a78d6`, 2.5px):** points (t s, seconds left): (0, 5.0), (0.8, 4.3), (1.6, 3.6), (2.4, 2.9), (3.2, 2.3), (4.0, 1.7), (4.8, 1.15), (5.6, 0.65).
- **Cutoff lines (dashed 4/3, 1.5px, horizontal across the plot):** warn at 2.6 s in orange `#d95926`; pre-tighten at 1.6 s in violet `#4a3aa7`; brake at 0.9 s in red `#c0392b`.
- **Cutoff labels (bold 11px, same hue as line, right-aligned at x=656, 6px above each line):** "warn the driver — 2.6 s", "pre-tighten brakes — 1.6 s", "car brakes itself — 0.9 s".
- **Crossing dots (radius 4.5, filled, cutoff hue):** where the countdown crosses each cutoff — (2.8 s, 2.6), (4.15 s, 1.6), (5.2 s, 0.9).
- **Caption (italic 12px `#2c3e50`, bottom center, y=308):** "The countdown falls; each cutoff triggers the next step if the driver has not reacted. Illustrative."

## Section 2: What does it collect?

- **Countdown and events** — the seconds-left estimate runs continuously; each warning and each self-braking moment is logged with speed and brake pressure just before and after
- **Driver reaction** — whether the driver braked or steered in response, how late — or that nothing happened at all
- **Crash recorder** — a black box in the car (event data recorder) preserves the last seconds of speed, braking, and steering before and after a triggering event

**Key point — A dial, not a fact:** set the system eager and it can stop for shadows and overpasses (phantom braking, reported on some systems); set it cautious and it can miss a pedestrian stepping out late. No setting removes both mistakes — only which one happens more often.

### Visualization (canvas `c2`, 720×320)

Grouped bar chart: two error types per sensitivity setting, three settings — one error falls as the other rises.

- **Title (bold 14px `#1a5276`, top center, y=22):** "Every setting trades one mistake for the other".
- **Legend (11px mute, two swatches centered under the title at y=44):** orange tint swatch "stops for nothing (false alarm)", violet tint swatch "fails to stop (miss)".
- **Axes:** baseline y=250 from x=80 to x=660; 1px `#e5e9ef` gridlines at 0/5/10 with right-aligned 11px mute labels at x=72; 11px mute note "out of 100 staged test runs (illustrative)" left-aligned at (80, 64); scale 15px per unit.
- **Groups** centered at x=190 (eager), x=370 (balanced), x=550 (cautious); two bars per group, width 44, gap 12 between the pair.
- **Values (illustrative):** eager — false alarms 9, misses 2; balanced — false alarms 4, misses 5; cautious — false alarms 1, misses 11.
- **Bars:** false alarms in orange `#d95926` alpha 0.4 fill with 1.5px orange stroke; misses in violet `#4a3aa7` alpha 0.4 fill with 1.5px violet stroke; 11px mute value above each bar.
- **Group labels (bold 12px `#2c3e50`, centered at y=270):** "eager", "balanced", "cautious".
- **Caption (italic 12px `#2c3e50`, bottom center, y=304):** "Turning the dial down on one error turns the other up. Illustrative numbers."

**Payload note (below canvas, italic, left-aligned):** Sample payload — illustrative structure, not real captured data.

**Payload block (monospace, left border `#1a5276`):**

```
// AEB event schemas are not published.
// Whole block is reconstruction; field names generic.
// ── inferred / plausible ──
{
  "vehicle_id": "veh-30172",
  "ts":         "2026-08-19T21:14:07Z",

  // measured by radar + camera
  "ttc_s":            0.9,           // seconds left at trigger
  "object_class":     "pedestrian",
  "speed_kmh_before": 58,

  // what the system did
  "event":              "aeb_brake", // aeb_warning | aeb_prefill | aeb_brake
  "brake_pressure_pct": 100,
  "speed_kmh_after":    21,

  // what the driver did
  "driver_brake_input": false,
  "driver_steer_input": false,

  // preserved by the crash recorder
  "edr_snapshot_retained": true      // ~5 s before / after
}
```

## Section 3: Why is it collected?

**Label (purpose pill):** Stated purpose

- **Avoiding or softening front crashes** — the car can start braking sooner than a startled human; regulators increasingly require the system on new cars

**Label (effect pill):** Additional consequence

- **The record travels** — activation events and recorder snapshots reach crash investigators, and on connected cars they can also reach the maker and, on some systems, insurers

**Key point — Read only after something happens:** nobody opens the log until a crash. Then the braking record — produced by cutoffs the driver never saw — arrives as evidence about what the driver did and did not do.

### Visualization (canvas `c3`, 720×320)

Retention strip with zoom: a 30-minute drive as a long band that is mostly never stored, with a sliver around the event kept by the crash recorder, expanded below to show the recorded seconds.

- **Title (bold 14px `#1a5276`, top center, y=22):** "What the crash recorder keeps from a 30-minute drive".
- **Top strip:** x=60 to x=680, y=58, height 30; fill mute `#6b7280` alpha 0.18; left-aligned 11px mute label above at y=50: "the drive — discarded, never stored". Kept sliver: 6px-wide blue `#2a78d6` alpha 0.8 rect at 73% along the strip (event at minute ~22).
- **Connector lines (1px mute alpha 0.5):** from the sliver's bottom corners down to the top corners of the zoom band.
- **Zoom band:** x=140 to x=600, y=150, height 84; fill blue alpha 0.12 with 1.5px blue border; bold 11px blue label above at y=142: "kept: ~10 seconds of speed, braking, steering".
- **Inside the zoom band:** dashed (4/3) 1.5px red `#c0392b` vertical line at the band's center (the self-braking moment, genuine alarm state), with bold 11px red label "car brakes itself" left-aligned just right of the line inside the band's top (x=midX+6, y=zoneY+13); a speed trace (2px `#2c3e50`, 11px mute "speed" label at its left end): flat at high speed (y=175) from the band's left edge to center, then falling to low speed (y=218) by x=560, flat to the right edge.
- **Tick labels (11px mute, below the band at y=250):** "-5 s" at x=140, "0" at x=370, "+5 s" at x=600.
- **Caption (italic 11px mute, bottom center, y=300):** "Illustrative — window lengths vary by system; the strip shows the crash recorder only, not other data the car may send."

## Regeneration instructions

- **Layout:** tracking-page `.obj-table`: full-width `border-collapse: collapse` table, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` + optional `.lede` + bullets + `.key-point` callouts + `.lbl` pills, right `<td>` (55%, `text-align: center`) holds the canvas — and in the "What does it collect?" row, the `.payload-note` and `.payload` `<pre>` below the canvas (both left-aligned).
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold 1.1em `#1a5276`; `li` 0.93em with `li b` in `#1a5276` weight 600. No nav bar, no back/home links.
- **Callouts:** `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em; leading `<strong>` in `#1a5276`.
- **Pills:** `.lbl` inline-block uppercase 0.7em bold, padding 2px 7px, radius 3px; `.lbl-purpose` background `#eaf2fb` color `#1a5276`; `.lbl-effect` background `#fdf0e6` color `#a8501c`.
- **Payload:** `.payload` — background `#f8f9fa`, left border `3px solid #1a5276`, ui-monospace 0.78em, `white-space: pre`; `.payload-note` italic 0.82em `#666` immediately above.
- **Canvas:** each declares intrinsic `width="720" height="320"`; a shared `setupCanvas(id)` reads those attributes, sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Shared helpers: `tint(hex, alpha)` for translucent fills and `rr()` rounded-rect path.
- **Palette (tracking pages):** categorical tokens blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`; ink `#1a5276` (headings/axes only), text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red `#c0392b` appears only for the self-braking stage — a genuine alarm state.
- In regenerated HTML, any card links use `.html` extensions.
