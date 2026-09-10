# Tracking Data: Fleet Data Capture & Shadow Mode

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one row per section)
**HTML title tag:** Tracking Data: Fleet Data Capture & Shadow Mode

**Subtitle:** Customer cars with outward-facing cameras mail selected moments of the road back to the maker, while a trainee version of the driving software rides along and is graded against the driver — without ever touching the wheel.

## Section 1: What is it?

**Lede:** A fleet of everyday cars working as data collectors for the software that drives them.

- **Mailing moments home:** cars upload short recordings of moments the maker asked for — hard brakes, moments the driving assist gave up, or scenes matching a description engineers pushed out ("send clips of construction zones")
- **A trainee graded on paper:** a newer version of the software rides along, makes its own decisions on paper, and is compared with what the driver actually did — its hands never touch the wheel (shadow mode)

**Key point — The scale flip:** any one car almost never sees the rare thing; a fleet of a million cars usually sees it every day. Fleet capture turns the rarest road moments into a steady feed.

### Visualization (canvas `c1`, 720×320)

A drive as a long strip of continuous time; three short flagged slices lift off and flow up to a maker archive box — the kept vs discarded proportion is visible.

- **Title (bold 14px ink `#1a5276`, top center, y=22):** "One drive: what leaves the car".
- **Timeline strip:** x=60 to x=680 spans a 40-minute drive (15.5px/min); y=210, height 30. Base fill blue `#2a78d6` alpha 0.22 = the drive that stays in the car.
- **Flagged slices** (start min, duration min): (8, 0.5), (21, 0.4), (33, 0.6) — drawn over the strip in orange `#d95926` alpha 0.6.
- **Minute ticks:** 11px mute `#6b7280` labels "0 min", "10", "20", "30", "40" under the strip at y=262.
- **Archive box:** rounded rect (rr, radius 8) at (300,48), 140×40; fill violet `#4a3aa7` alpha 0.12, 1.5px violet stroke; bold 11px violet label "maker's archive" centered inside.
- **Upload curves:** from the top center of each flagged slice, a 1.5px quadratic curve in orange alpha 0.6 rising to the archive box bottom edge (control point midway, pulled toward the box); a 3px orange dot where each curve meets the box.
- **Proportion labels (11px mute):** left-aligned under the strip at y=280: "≈ 40 min driven — stays in the car, overwritten"; right-aligned at x=680, y=280: "≈ 1.5 min uploaded".
- **Caption (italic 12px `#2c3e50`, bottom center, y=308):** "Almost the whole drive never leaves the car; three flagged slices do. Illustrative drive."

## Section 2: What does it collect?

- **Short sensor clips** — a few seconds of video, radar echoes, and laser dot measurements (lidar) around each flagged moment, with location and time
- **The trigger reason** — which rule fired: a hard brake, the assist giving up, the trainee disagreeing with the driver, or a scene engineers asked for
- **Two decisions side by side** — what the trainee software would have done vs what the driver actually did

**Key point — The trigger list decides what the dataset sees:** the fleet only mails home moments someone told it to look for, so the training data inherits the blind spots of that list (selection bias, in plain words). A hazard nobody thought to flag stays invisible at any fleet size.

### Visualization (canvas `c2`, 720×320)

Grouped bars per scene type: how often each scene happens on the road vs how often it lands in the dataset — listed types pass through almost fully, the unlisted hazard arrives as zero.

- **Title (bold 14px ink, top center, y=22):** "What the road shows vs what the dataset sees".
- **Plot area:** x=80 to x=660, baseline y=252; value scale 0.85px per count (max 170 ≈ 145px tall).
- **Categories** (centered at x = 155, 295, 435, 575), 11px mute labels under the baseline: "hard brakes", "construction zones", "cut-ins", "unlisted hazard".
- **Counts (moments in one fleet-week, illustrative, hardcoded):** on the road [140, 90, 170, 12]; in the dataset [126, 81, 153, 0].
- **Bars:** two per category, width 34, 6px gap, centered on the category x. "On the road" in blue alpha 0.35 with 1.5px blue stroke; "in the dataset" in aqua `#199e70` alpha 0.45 with 1.5px aqua stroke. 10px mute count value above each bar.
- **Zero marker:** where the unlisted-hazard dataset bar would stand, a bold 11px orange `#d95926` label "0 — not on the trigger list" just above the baseline.
- **Legend (11px mute, top right):** blue swatch "happens on the road", aqua swatch "arrives in the dataset".
- **Caption (italic 12px `#2c3e50`, bottom center, y=308):** "The dataset can only contain what the trigger list asks for. Illustrative counts."

**Payload note (below canvas, italic, left-aligned):** Sample payload — illustrative structure, not real captured data.

**Payload block (monospace, left border `#1a5276`):**

```
// Fleet-capture schemas are not published.
// Whole block is reconstruction; field names generic.
// ── inferred / plausible ──
{
  "vehicle_id": "veh-30172",
  "ts":         "2026-08-24T08:12:07Z",
  "trigger":    "shadow_disagreement",
  "clip_s":     12,

  // sensors bundled in the clip
  "sensors":    ["camera_front", "camera_sides", "radar"],

  // the graded comparison
  "shadow_decision": "brake",
  "driver_action":   "steer_around",

  "location":   { "lat": 37.79, "lon": -122.41 },   // rounded
  "uploaded":   true,
  "kept_in_car": false
}
```

## Section 3: Why is it collected?

**Label (purpose pill):** Stated purpose

- **Finding failures** — moments where the assist gave up or guessed wrong become the next software version's homework
- **Training better software** — rare scenes ordered by description arrive by the thousand instead of being hunted for by test drivers

**Label (effect pill):** Additional consequence

- **The street did not sign up** — clips record pedestrians, other drivers, license plates, storefronts; the owner agreed to terms, the people in frame did not

**Key point — An archive of public moments:** what accumulates at the maker is a searchable library of street scenes — on some systems reachable by legal requests, with how long it is kept decided by the maker, not the people in frame.

### Visualization (canvas `c3`, 720×320)

Pictogram for one captured clip: the people recorded in the frame vs the accounts that agreed to any terms.

- **Title (bold 14px ink, top center, y=22):** "One 12-second clip: recorded vs agreed".
- **Row 1 — recorded in the frame:** bold 11px mute label "people and drivers in the frame" at x=60, y=78; nine person icons in blue `#2a78d6` starting at x=80, spaced 58px, centered on y=118 — each a head circle radius 8 at (x,108) plus a rounded-rect body 20×26 (radius 8) at (x−10,120); bold 22px blue count "9" right-aligned at x=660, y=125.
- **Row 2 — agreed to the terms:** bold 11px mute label "accounts that agreed to the terms" at x=60, y=188; one person icon in green `#008300`, same construction, centered on y=228 at x=80; bold 22px green count "1" right-aligned at x=660, y=235; 11px mute note "(the car's owner)" left of the count at x=610, right-aligned.
- **Divider:** 1px grid `#e5e9ef` horizontal line at y=165 from x=60 to x=660.
- **Caption (italic 12px `#2c3e50`, bottom center, y=300):** "Everyone in frame is recorded; one account consented. Illustrative scene."

## Regeneration instructions

- **Layout:** tracking-page `.obj-table`: full-width `border-collapse: collapse` table, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` + optional `.lede` + bullets + `.key-point` callouts + `.lbl` pills, right `<td>` (55%, `text-align: center`) holds the canvas — and in the "What does it collect?" row, the `.payload-note` and `.payload` `<pre>` below the canvas (both left-aligned).
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold 1.1em `#1a5276`; `li` 0.93em with `li b` in `#1a5276` weight 600. No nav bar, no back/home links.
- **Callouts:** `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em; leading `<strong>` in `#1a5276`.
- **Pills:** `.lbl` inline-block uppercase 0.7em bold, padding 2px 7px, radius 3px; `.lbl-purpose` background `#eaf2fb` color `#1a5276`; `.lbl-effect` background `#fdf0e6` color `#a8501c`.
- **Payload:** `.payload` — background `#f8f9fa`, left border `3px solid #1a5276`, ui-monospace 0.78em, `white-space: pre`; `.payload-note` italic 0.82em `#666` immediately above.
- **Canvas:** each declares intrinsic `width="720" height="320"`; a shared `setupCanvas(id)` reads those attributes, sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Shared helpers: `tint(hex, alpha)` for translucent fills and `rr()` rounded-rect path.
- **Palette (tracking pages):** categorical tokens blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`; ink `#1a5276` (headings/axes only), text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red is reserved for genuine alarm states and not used here. (Site-wide palette reference: #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange.)
- In regenerated HTML, any card links use `.html` extensions.
