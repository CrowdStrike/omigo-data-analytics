# Tracking Data: Short Video Behavioral Tracking

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one row per section)
**HTML title tag:** Tracking Data: Short Video Behavioral Tracking

**Subtitle:** Short-video feeds record how long each video was on screen, where it was scrubbed or replayed, and how the swipe away was performed.

## Section 1: What is it?

The feed does not count views, it times them.

- **Instrumented player** reports milliseconds on screen
- **Loops and scrubs** — whether it repeated, where it was scrubbed back to
- **Exit gesture** — how the swipe away was performed
- **Why so fine:** with no follows, likes, or search, watch time is all a fresh feed has

**Timing is not attention:** the player reports that a video was on screen and unpaused, not that anyone was looking. A video playing to a face-down phone gives the same watch time as one someone studied.

### Visualization (canvas `c1`, 720×320)

Annotated phone-player diagram: a video player mock with seven measurement callouts, colored by signal kind. Hue encodes the kind of engagement signal: blue `#2a78d6` = dwell, green `#008300` = playback control, violet `#4a3aa7` = gesture, orange `#d95926` = device state, aqua `#199e70` = pacing between videos. The screen fill and play triangle are a device mock, not categorical.

- **Phone frame:** rounded rect (radius 14) at x=40, y=10, 180×220, ink `#1a5276` stroke 2px; dark screen `#1a1a2e` (164×180 at x=48, y=35); abstract video content block in blue at 45% alpha (140×80 at x=60, y=80); white play triangle at 80% alpha.
- **Annotations** (each: dotted 2/2 line from x=212 to x=240 in the kind's hue, a 3px dot at x=240, and 14px text in the hue at x=250; y positions 35–215 at 30px pitch):
  1. "Time on screen: 4.2s of a 15s video" (dwell, blue)
  2. "Scrub position on replay: 0:03" (playback, green)
  3. "Swipe gesture duration: 140ms" (gesture, violet)
  4. "Paused: 800ms" (playback, green)
  5. "Orientation change: yes" (device, orange)
  6. "Sound on: yes" (device, orange)
  7. "Gap to next video: 1.4s" (pacing, aqua)
- **Right side label** (14px, centered at x=606): muted "Every field here is a timing" / "or a flag from the player." then magenta `#d55181` "None of them observes" / "whether anyone was watching."
- **Footer band** (y=250, 70px, ink tint alpha 0.05): bold 13px `#2c3e50` "Kinds of signal the player emits:" then legend squares at x=40/160/300/420/560 — "dwell" (blue), "playback control" (green), "gesture" (violet), "device state" (orange), "pacing" (aqua).

## Section 2: What does it collect?

- **Time on screen** per video, in milliseconds
- **Loop count**, and the scrub position when replayed
- **Swipe gesture** duration and direction
- **Orientation changes**
- **Gap** between one video ending and the next starting
- **Sound state** — on or off
- **Two timestamps** — client and server receipt
- **Precision:** fields carry milliseconds, but timing is usually only as fine as the player's reporting cadence, and client clocks drift — the server timestamp is there to catch that

**Completion is derived, and above 1 here:** a loop pushes watch time past video length. An "average completion" metric is unbounded above, so a few heavy rewatchers dominate the mean.

**Clipping does not fix it:** capping the ratio at 1 removes the rewatch signal rather than repairing the metric.

### Visualization (canvas `c2`, 720×320)

Area/line chart: playback occupancy across one video, split into three colored phases.

- **Title (bold 16px, ink, centered, y=20):** "Time on screen per second of one video (schematic)".
- **Axes:** left margin 80, right 50, top 46, bottom 84; ink 1px L-shaped axes. Y-axis label (rotated, 14px ink): "Time on screen". X-axis label (14px ink, centered): "Position in the video (seconds)". X ticks every 3s from 0s to 15s (13px muted labels) with vertical grid-gray lines.
- **Data (occupancy 0–1 per second, 15 points):** `[0.3, 0.5, 0.8, 0.9, 0.95, 0.7, 0.4, 0.3, 0.6, 0.85, 0.9, 0.88, 0.5, 0.3, 0.1]`.
- **Phases** (fill = hue tint alpha 0.32 under the curve; line 2.5px in the hue; dashed 2/3 vertical boundary tick at each phase start):
  1. indices 0–7, blue `#2a78d6`, "first pass"
  2. indices 7–12, violet `#4a3aa7`, "segment replayed"
  3. indices 12–14, mute `#6b7280`, "swiped away"
- **Annotations (bold 13px, above the plot):** blue "longest on screen" at index 4; violet "segment replayed" at index 10; muted "swiped away" at index 13.
- **Footer band** (32px, ink tint alpha 0.05, below the axis): legend squares at x=80/250/460 — "first pass" (blue), "segment replayed" (violet), "swiped away" (mute).

### Payload (below canvas c2)

Payload note (italic, above the block): "Sample payload — illustrative structure, not real captured data."

```
// No public API for feed telemetry — the whole block
// is reconstruction from plausible fields.
// ── inferred / plausible ──
{
  "event":          "video_impression_end",
  "video_id":       "v_88231…",
  "video_len_ms":   14200,
  "watch_ms":       31600,      // exceeds length: rewatched
  "loops":          2,
  "completion":     2.23,       // derived: watch_ms / video_len_ms
  "max_scrub_ms":   9800,
  "paused":         false,
  "exit_action":    "swipe_next",
  "swipe_ms":       140,        // gesture duration, not dwell
  "sound_on":       true,
  "ts_client":      "2026-08-22T22:07:41.905Z",
  "ts_server":      "2026-08-22T22:07:44.118Z"
}
```

## Section 3: Why is it collected?

**Stated purpose** (label pill)

- **Ranking the feed** — with no follow graph and no query, behaviour is all there is, and a view flag is too coarse: two seconds and fourteen are both "a view"
- **Bot detection** — timing is the fastest signal for scripted playback

**Additional consequence** (label pill)

- **Daily rhythm** from timestamps across sessions, and **predicted-interest labels** from response to categories
- Those are **model outputs with error rates** — but a stored label reads like a measured field once it lands in a table

**A closed loop:** watch time is only observed on videos the ranker chose to show, so the training data is a sample the model selected for itself. Low-ranked content is rarely shown, so it rarely gathers the evidence that would revise its rank.

### Visualization (canvas `c3`, 720×320)

Curve chart: how much evidence each rank bucket ever accumulates. The curve is the standard-error shape err(n) = 1/√n over a log x-axis of impressions; the ranker decides the impression count, so low-ranked buckets sit where the curve is flat and more exposure would change the estimate most. Illustrative placements.

- **Title (bold 14px, ink, centered, y=24):** "How sharp the watch-time estimate can get". **Subtitle (12px, muted, y=42):** "precision improves with the square root of the number of times a video was shown".
- **Axes:** plot from x0=96 to x1=600, baseline y=232, top y=74; ink 1px axes. X is log10 impressions from 10 to 100,000 with decade ticks labeled "10", "100", "1,000", "10,000", "100,000" and grid-gray vertical lines; below: "times the video was shown  →". Rotated y-axis label (12px ink): "how wide the estimate stays". Y scales err(n) normalized to err(10) at the top.
- **Curve:** blue `#2a78d6`, 2.5px, err(10^l) sampled at l steps of 0.05 from 1 to 5.
- **Bucket markers** (5.5px dots on the curve, dashed 3/3 drop lines at 0.7 alpha of the hue; bold 12px label plus 12px "shown about N" line, placed left or right of the dot):
  1. l=1.4 — "videos it ranks low", "shown about 25 times", orange `#d95926`, label right
  2. l=3.0 — "the middle of the feed", "shown about 1,000 times", violet `#4a3aa7`, label right
  3. l=4.8 — "videos it ranks high", "shown about 60,000 times", aqua `#199e70`, label left
- **Captions (centered):** italic 12px `#2c3e50` at h−26: "The bucket whose estimate would move most is the one given the fewest chances to move it." Italic 11px muted at h−9: "Illustrative — the curve is the standard-error shape; the three placements show the spread."

## Regeneration instructions

- **Layout:** tracking-page `.obj-table`: full-width table, `border-collapse: collapse`, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` + `.lede` + bullets + `.key-point` callouts, right `<td>` (55%, centered) holds the canvas (and, in the "What does it collect?" row, the `.payload-note` + `.payload` pre block below the canvas, left-aligned).
- **Page style:** body system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold 1.1em `#1a5276`; `li` 0.93em with `li b` in `#1a5276` weight 600. No nav bar, no back/home links.
- **Callouts:** `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em; leading `<strong>` in `#1a5276`.
- **Label pills:** `.lbl` inline-block uppercase 0.7em weight 700, letter-spacing 0.05em, padding 2px 7px, radius 3px; `.lbl-purpose` background `#eaf2fb` color `#1a5276`; `.lbl-effect` background `#fdf0e6` color `#a8501c`.
- **Payload block:** `.payload` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px, ui-monospace 0.78em, `white-space: pre`, line-height 1.45; `.payload-note` 0.82em `#666` italic.
- **Canvas:** declare intrinsic `width`/`height` attributes per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setupCanvas(id)` helper that reads the element's own width/height attributes. Include a rounded-rect path helper and a `tint(hex, alpha)` helper for translucent fills.
- **Palette (tracking pages):** categorical tokens blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`; ink `#1a5276` (headings/axes only), text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red is deliberately not in the rotation (reserved for alarm states). Project-wide palette reference: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- In regenerated HTML, any card links use `.html` extensions.
