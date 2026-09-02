# Tracking Data: Voice Assistants: Wake Word Detection

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one row per section)
**HTML title tag:** Tracking Data: Voice Assistants: Wake Word Detection

**Subtitle:** Alexa, Siri, and Google Assistant keep the microphone active to detect a wake word. False triggers upload speech the user did not intend to send.

## What is it?

A small detector on the device listens for one phrase.

- **Rolling buffer:** audio is held briefly and discarded continuously
- **Nothing leaves** the device until the detector fires
- **On a match** — "Alexa" or "Hey Siri" — streaming to vendor servers begins
- **Pre-roll:** a second or two of buffered audio from before the trigger goes too

**It is a classifier:** so it has a false positive rate. A misfire uploads whatever was being said — which is how unintended speech reaches the vendor.

### Visualization (canvas `c1`, 720×320)

Schematic: a smart-speaker cylinder at center with concentric listening-radius circles, radiating sound-wave lines, and side labels.

- **Center:** (360,120). Concentric circles, outer to inner — radius 110 "Always listening" magenta `#d55181`; radius 85 "Background capture" orange `#d95926`; radius 60 "Wake word zone" `rgba(42,120,214,0.35)`; radius 35 "Speaker" blue `#2a78d6`. Each circle filled in its color at 12% global alpha and stroked 1.5px in the color; the two outer circles use a dashed stroke (4/3).
- **Speaker cylinder:** blue at 90% alpha — ellipse (18×6) at cy−15, body rect 36×30, ellipse at cy+15; white 16px "MIC" text at center.
- **Sound waves:** 8 radial line segments at 45° steps from radius 40 to 55, 2px; waves at 90°, 225°, and 315° in orange (unintended), the rest in blue.
- **Right-side labels (14px):** "Accidental capture" in red `#e74c3c` at (cx+115, 50); "Background noise" in orange at (cx+90, 75); "Wake word trigger" in blue at (cx+65, 100).
- **Legend (bold 14px orange at (cx+115, 195)):** "Orange = unintended wake".
- **Caption (15px mute `#6b7280`, centered at y=225):** "Microphone never turns off — it just waits for a trigger word".

## What does it collect?

- **Audio** of intentional voice commands
- **Audio from false triggers** the user did not intend to send
- **Speaker identity,** where voice profiles are enabled per household member
- **Background sound** present in the clip
- **Timestamps** of each interaction
- **Transcript** and its confidence score

**Two records, not one:** audio and the transcript derived from it are separate. Retention and deletion controls for the two are not necessarily the same, and the specifics vary by vendor and change over time.

**Selection effect:** the log is the classifier's positives, not the true wake events — misfires are in it, quiet or accented real requests are missing. Any rate computed from it is conditioned on detection, so lowering the threshold appears to increase usage.

**Pre-roll:** the clip starts before the trigger, so a false positive captures speech never addressed to the device.

### Visualization (canvas `c2`, 720×320)

Dual-curve tradeoff chart: false accepts vs missed wake words across a detector threshold sweep, with a crossover marker.

- **Plot area:** x=70, y=25, width 600, height 155.
- **Data (11 points, permissive → strict threshold):**
  - False accepts (orange `#d95926`): `[0.95, 0.82, 0.66, 0.50, 0.37, 0.26, 0.18, 0.12, 0.07, 0.04, 0.02]`
  - Missed wake words (blue `#2a78d6`): `[0.01, 0.02, 0.04, 0.07, 0.12, 0.18, 0.27, 0.40, 0.56, 0.74, 0.92]`
- **Axes:** blue 1.5px L-shaped axes; y gridlines at 0.00/0.25/0.50/0.75/1.00 with right-aligned 14px mute labels and 0.5px grid `#e5e9ef` lines.
- **Curves:** 2px polylines, one point per threshold step.
- **Crossover marker:** dashed (3/3) 1px magenta `#d55181` vertical line at the 6th point (index 5).
- **X labels (14px mute):** "permissive threshold" (left-aligned under plot), "strict threshold" (right-aligned).
- **Legend (14px, small color-bar swatches at top-left of plot):** orange "False accepts (unintended uploads)"; blue "Missed wake words (device ignores you)".
- **Caption (15px mute, centered at y=218):** "Schematic. No threshold removes both errors — tightening one raises the other."

Below the canvas (right column):

Sample payload — illustrative structure, not real captured data.

```
// Vendor internals are not published — the whole block
// is reconstruction from plausible fields.
// ── inferred / plausible ──
{
  "event":            "wake_word_detected",
  "device_id":        "A2X9…",
  "wake_word":        "configured phrase",
  "detect_score":     0.63,       // threshold crossed, not certainty
  "threshold":        0.60,
  "preroll_ms":       1500,       // audio kept from before the trigger
  "clip_ms":          4200,
  "transcript":       "set a timer for ten minutes",
  "asr_confidence":   0.88,
  "intent":           "timer.create",
  "intent_resolved":  true,
  "speaker_profile":  null,       // no voice match
  "ts":               "2026-08-22T07:14:03Z"
}
```

## Why is it collected?

**Stated purpose** (label pill, blue)

- **Process the request** that was spoken
- **Improve recognition accuracy** — measured by scoring the machine transcript against a human one, so sampled clips are transcribed by people

**Additional consequence** (label pill, orange)

- A **normal step** in supervised model development; review programs, and whether they are **opt-in or opt-out, differ by vendor** and have changed over time
- The same rows read as a **record of household activity** — waking hours, days away, interests from past topics

**A quiet log is not a quiet house:** a row exists only where a speaker was installed and only when the detector fired, both chosen for a responsive product rather than to sample a home. Rooms without a device and hours nobody spoke contribute nothing, so a gap means no triggers, not no activity.

### Visualization (canvas `c3`, 720×320)

Two-band comparison: an illustrative household-activity area curve over 24 hours above tick marks for the rows the log actually holds, with a highlighted afternoon gap.

- **Title (bold 14px ink `#1a5276`, left at 22,22):** "One day in one room, and the rows the log holds for it". Subtitle (12px mute at 22,38): "A row is written only when the detector fires."
- **Activity data (24 hourly values, 0–1 scale):** `[0.05, 0.03, 0.02, 0.02, 0.03, 0.10, 0.35, 0.60, 0.55, 0.35, 0.30, 0.32, 0.40, 0.45, 0.42, 0.40, 0.44, 0.55, 0.75, 0.85, 0.80, 0.60, 0.35, 0.15]`.
- **Trigger rows (hours of day):** `[6.6, 7.2, 7.9, 8.4, 12.1, 18.3, 19.0, 19.7, 21.2, 22.4]`. Gap band from hour 12.4 to 18.1.
- **Geometry:** padL 56, padR 28; activity band baseline y=132, height 74; tick band at y=172, height 26.
- **Gap highlight:** `rgba(217,89,38,0.10)` rect spanning both bands over hours 12.4–18.1; bold 12px orange `#d95926` label "no rows here" centered above it.
- **Activity band:** closed area filled `rgba(25,158,112,0.22)` with 2px aqua `#199e70` outline, one vertex per hour at hour+0.5; grid baseline. Labels: bold 12px aqua "people in the room", 11px mute "illustrative".
- **Log band:** bold 12px blue `#2a78d6` label "rows in the log"; 3px blue vertical ticks at each trigger hour rising from the band baseline; grid baseline under the band.
- **Hour labels (12px mute, centered):** "0:00", "6:00", "12:00", "18:00", "0:00" at hours 0/6/12/18/24.
- **Captions (centered, italic):** 12px text `#2c3e50` "The afternoon reads as an empty house because nobody had anything to ask." (h−24); 11px mute "Schematic — the mismatch between the two rows is the point, not the values." (h−8).

## Regeneration instructions

- **Layout:** tracking detail page `.obj-table` — full-width table, `border-collapse: collapse`, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` + `.lede` + bullets + `.key-point` callouts, right `<td>` (55%, `text-align: center`) holds the canvas (and, in the "What does it collect?" row, the `.payload-note` caption plus `<pre class="payload">` block below the canvas, both left-aligned).
- **Page style:** body system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold 1.1em `#1a5276`; li 0.93em with `li b` in `#1a5276` weight 600.
- **Callouts:** `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em, first `<strong>` in `#1a5276`.
- **Label pills:** `.lbl` inline-block uppercase 0.7em bold, padding 2px 7px, radius 3px; `.lbl-purpose` background `#eaf2fb` color `#1a5276`; `.lbl-effect` background `#fdf0e6` color `#a8501c`.
- **Payload block:** `.payload` — background `#f8f9fa`, left border `3px solid #1a5276`, monospace 0.78em, `white-space: pre`; `.payload-note` italic 0.82em `#666` immediately above.
- **Canvas:** declare intrinsic `width="720" height="320"`; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setupCanvas(id)` helper that reads the element's own width/height attributes. Shared helpers: `rr()` rounded-rect path and `tint(hex, alpha)` rgba derivation from palette hexes.
- **Chart palette (tracking pages):** categorical CVD-checked tokens — blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`; ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red `#e74c3c` is reserved for genuine alarm states (used once here for "Accidental capture"). Page/site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- No nav bar, no back/home links. In regenerated HTML, any card links elsewhere use `.html` extensions.
