# Tracking Data: Prescribed Medical Monitors

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one row per section)
**HTML title tag:** Tracking Data: Prescribed Medical Monitors

**Subtitle:** A clinician prescribes a recorder for a bounded window — a night, a day, two weeks — and reads the result. Unlike a fitness band, these measure the signal itself; the catch is whether the window contained the thing being looked for.

## What is it?

A recorder a clinician prescribes for a bounded window, then reads.

- **Holter monitor / adhesive ECG patch:** chest electrodes recording the heart's electrical activity continuously, from a day to a couple of weeks
- **Home sleep study kit:** airflow, breathing effort and blood oxygen for one night; the lab version (polysomnography) adds brain activity
- **Ambulatory blood-pressure monitor:** an arm cuff that inflates on a schedule across 24 hours, awake and asleep
- **Event / loop recorders:** capture short snippets only when the patient or an algorithm triggers them
- **Professional continuous glucose monitor:** a sensor patch worn for one to two weeks, logging interstitial glucose typically every 5 to 15 minutes; in blinded mode the patient never sees the readings — the clinician reads them at the end

Key-point callout: **Measured, not modeled:** the electrodes record the heart's electrical signal itself, and a sleep lab measures brain activity rather than inferring stages from wrist motion. A clinician reads the result — not a hidden scoring model.

### Visualization (canvas `c1`, 720×360) — "The basic set — worn on the body, each for its own window"

Picture story: one schematic figure wearing the basic set, hue-coded dots and dashed leader lines (dash 3/3) to labels; a timeline strip of each device's window underneath. Header band (28px) and footer band (34px).

- **Header band:** rect 0,0 → 720×28 fill `rgba(26,82,118,0.07)`; bold 15px `#1a5276` centered at (360,19): "The basic set — worn on the body, each for its own window".
- **Footer band:** rect 0,326 → 720×34 fill `rgba(26,82,118,0.06)`; 14px `#2c3e50` centered at (360,347): "Schematic figure. Drawn together here; in practice each is prescribed on its own."
- **Figure (ink `#1a5276`, 2px stroke, fills `tint(ink,0.05)`):** head circle center (190,58) r=14; torso rounded rect (150,76) 80×114 r=18; left arm line (152,92)→(126,180) with hand circle (125,186) r=5; right arm line (228,92)→(254,180) with hand circle (255,186) r=5; legs (172,190)→(172,230) and (208,190)→(208,230).
- **Devices on the figure:**
  - ECG patch (magenta `#d55181`): rounded rect (176,108) 22×30 r=5, fill `tint(magenta,0.25)`, stroke 2px; electrode dots r=3.5 at (170,104), (202,110), (186,146).
  - Sleep-kit cannula (blue `#2a78d6`): line (178,68)→(202,68) 1.5px across the face, dot r=3.5 at (190,68).
  - Finger clip (violet `#4a3aa7`): rounded rect (249,179) 13×13 r=3 over the right hand, fill `tint(violet,0.3)`, stroke 1.5px.
  - BP cuff (aqua `#199e70`): rounded rect (130,112) 24×32 r=4 on the left upper arm, fill `tint(aqua,0.25)`, stroke 2px.
- **Labels (left-aligned at x=400, bold 13px in device hue, sub-line 12px `#6b7280` 15px below), dashed leaders 1.5px dash [3,3] in the same hue:**
  - Blue at (400,60): "home sleep kit — nasal airflow sensor"; sub: "one night; the lab version adds brain activity". Leader (206,68)→(394,57).
  - Magenta at (400,105): "Holter monitor / adhesive ECG patch"; sub: "electrodes — the heart's electrical signal itself". Leader (200,122)→(394,102).
  - Aqua at (400,150): "ambulatory blood-pressure cuff"; sub: "inflates on a schedule, day and night". Leader (156,128)→(394,147).
  - Violet at (400,195): "finger clip — blood oxygen"; sub: "worn as part of the sleep kit". Leader (263,185)→(394,192).
- **Timeline strip (windows, schematic):** heading 12px `#6b7280` left-aligned at (60,252): "recording window (schematic — not to scale)". Three rows, row label right-aligned 12.5px at x=180, bar rounded rect from x=190 height 9 r=4 fill `tint(hue,0.5)` stroke hue 1px, duration text 12px in hue 8px past the bar end; text baselines at bar y+8:
  - y=262, blue, "sleep study", bar width 45, "one night"
  - y=283, aqua, "BP monitor", bar width 70, "24 hours"
  - y=304, magenta, "Holter / patch", bar width 420, "a day to a couple of weeks"

## What does it collect?

- **ECG trace:** the raw electrical waveform, recorded continuously; lead-off intervals are marked as gaps, not smoothed over
- **Sleep signals:** airflow, chest and abdominal effort, blood oxygen and position; brain activity in the lab version
- **BP series:** cuff readings on a schedule — commonly every 15–30 minutes by day, less often at night
- **Symptom diary:** the patient notes what they felt and when; joining diary entries to the trace is a manual step

Key-point callouts:

- **The window is the weakness:** a 24-hour recording samples one day of a varying life. A symptom that did not occur in the window produces a clean recording — which is not the same as a clean heart.
- **The instrument changes the night:** first nights on a sleep study are commonly described in sleep medicine as unrepresentative — wires, an unfamiliar mask, the knowledge of being recorded.

### Visualization (canvas `c2`, 720×320) — "One month of symptoms, one day of recording"

The missed-window story: a month-long life strip with rare symptom episodes as orange ticks, and a 24-hour recording window that lands between two of them.

- **Header band:** rect 0,0 → 720×28 fill `rgba(26,82,118,0.07)`; bold 15px `#1a5276` centered at (360,19): "One month of symptoms, one day of recording".
- **Footer band:** rect 0,286 → 720×34 fill `rgba(26,82,118,0.06)`; 14px `#2c3e50` centered at (360,307): "The recording is clean. The month was not. Illustrative — episode timing is schematic."
- **Month strip:** rect x=50..670, y=138..166 (620×28), fill `tint(blue,0.07)`, stroke `#e5e9ef` 1px; day ticks for d=1..29 at x = 50 + d·(620/30), vertical `#e5e9ef` 1px lines y=138..166. Corner labels 11px `#6b7280`: "day 1" left-aligned at (50,182), "day 30" right-aligned at (670,182).
- **Symptom episodes (orange `#d95926`):** vertical bars 4px wide, y=128..176, centered at days 4.5, 11.2, 19.6, 27.3 → x ≈ 143, 281, 455, 614.
- **Episode label:** bold 13px orange centered at (360,66): "symptom episodes — brief, rare, unscheduled"; dashed orange leaders [3,3] from (300,72)→(281,124) and (420,72)→(455,124).
- **Recording window (magenta):** rect x=352 w=21, y=130..174, fill `tint(magenta,0.18)`, stroke `#d55181` 2px — one day's width, landing between the day-11 and day-19 episodes.
- **Window labels:** dashed magenta leader (360,208)→(361,178); bold 13px magenta centered at (360,222): "the 24-hour Holter window"; 12.5px green `#008300` centered at (360,240): "recording result: clean — no episode occurred while worn"; 12.5px italic orange centered at (360,260): "both neighbouring episodes fell outside the window".

### Sample payload (right column, under canvas `c2`)

Caption (italic): "Sample payload — illustrative structure, not real captured data."

```
// No monitor vendor publishes its report schema, so this
// block is reconstruction — the measured/interpreted split
// is the part worth reading.
{
  "study": { "type": "ambulatory_ecg_patch", "wear_hours": 46.5 },

  // ── measured — electrodes on the skin ──
  "leads": { "count": 1, "sample_rate_hz": 250 },
  "beats_recorded": 198420,
  "artifact_intervals": [
    { "start": "day1 14:02", "end": "day1 14:09",
      "reason": "lead-off" }
  ],  // gaps are marked as gaps, unlike consumer bands

  // ── interpreted — read by a clinician / verified algorithm ──
  "findings": {
    "ectopic_beats": 143,   // counted by a reviewing clinician
    "pauses_gt_2s": 0,
    "afib_episodes": 0      // none within the worn window
  },
  "symptom_diary": [
    { "time": "day2 07:40", "note": "palpitations on stairs",
      "trace_correlate": "sinus tachycardia" }  // joined manually
  ]
}
```

## Why is it collected?

Label pill: STATED PURPOSE

- **Answer one clinical question:** is there an arrhythmia, an apnea, a sustained pressure problem — within the prescribed window
- **Correct a biased reading:** the ambulatory cuff exists because clinic readings run high for some patients in the clinic itself

Label pill: ADDITIONAL CONSEQUENCE

- The result lands in a **medical record**, interpreted and stored under clinical rules — a different holder and lifetime than a vendor's fitness account
- The prescription bounds the collection: **the recorder stops when the window ends**, rather than accumulating indefinitely

Key-point callout: **A device built against an observer effect:** the measurement setting was biasing the measurement, so a device was built to take the setting out — readings on a schedule across an ordinary day, including the night-time dip a clinic visit can never observe.

### Visualization (canvas `c3`, 720×320) — "Why the ambulatory cuff exists"

The observer-effect inversion: one elevated clinic dot vs the same person's 24-hour ambulatory profile (illustrative hardcoded arrays), with the night-time dip the clinic never sees.

- **Header band:** rect 0,0 → 720×28 fill `rgba(26,82,118,0.07)`; bold 15px `#1a5276` centered at (360,19): "Why the ambulatory cuff exists — one clinic reading vs the day it came from".
- **Footer band:** rect 0,286 → 720×34 fill `rgba(26,82,118,0.06)`; 14px `#2c3e50` centered at (360,307): "Illustrative values. The elevated reading reflects the setting, not the day."
- **Plot frame:** padL=70, padR=80 → x from 70 to 640; top=48, base=246. y maps systolic 90–170: `yAt(v) = 246 − (v−90)/80 · 198`. Gridlines `#e5e9ef` 1px at 100, 120, 140, 160 with 11px `#6b7280` labels right-aligned at x=62; axis baseline `#e5e9ef` at y=246. X labels 11px `#6b7280` centered at hours 0, 6, 12, 18, 24 (y=262): "12am", "6am", "12pm", "6pm", "12am"; `xAt(hr) = 70 + hr/24 · 570`.
- **Night band (violet):** rect from xAt(0) to xAt(6), y=48..246, fill `tint(violet,0.06)`. Label centered at (141,72) bold 12.5px violet: "night-time dip"; sub 12px violet at (141,87): "a clinic visit can never see this".
- **Elevated-range line (schematic):** dashed [4,4] 1.5px `#6b7280` at yAt(140); label 11px `#6b7280` left-aligned at (646, yAt(140)+4): "elevated (schematic)".
- **Ambulatory series (aqua `#199e70`, 2.5px line, dots r=3 at even hours):** 25 hourly systolic values (illustrative): `[104,102,101,100,101,103,108,116,124,127,128,127,126,128,126,125,127,128,126,122,118,113,109,106,104]`. Label bold 12.5px aqua centered at (500,130): "worn cuff — readings across the whole day".
- **Clinic dot (orange `#d95926`):** single filled dot r=5.5 at (xAt(10), yAt(152)) ≈ (307,93), ringed by a 1.5px orange circle r=9. Label bold 13px orange left-aligned at (330,64): "clinic reading — same person, that morning"; dashed orange leader [3,3] from (326,68)→(313,86).

## Regeneration instructions

- **Layout:** tracking detail page — `<table class="obj-table">`, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` heading, optional `.lede`, bullets with bold lead terms (`li b` in `#1a5276`), `.key-point` callouts, `.lbl` label pills; right `<td>` (55%, text-align center) holds the canvas, and for the "What does it collect?" row also the `.payload-note` caption plus `<pre class="payload">` block (left-aligned).
- **Page CSS:** body system sans-serif (-apple-system stack), white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; `.obj-title` bold 1.1em `#1a5276`; td borders `1px solid #2980b9`, padding 16px; li 0.93em; `.key-point` background `#f8f9fa`, left border `3px solid #1a5276`, 0.93em, `strong:first-child` in `#1a5276`; `.lede` 0.95em; `.lbl` pills 0.7em bold uppercase radius 3px — `.lbl-purpose` `#eaf2fb`/`#1a5276`, `.lbl-effect` `#fdf0e6`/`#a8501c`; `.payload` background `#f8f9fa`, left border `3px solid #1a5276`, ui-monospace 0.78em, white-space pre; `.payload-note` 0.82em italic `#666`. No nav bar, no back/home links.
- **Canvas:** each canvas declares intrinsic `width`/`height` attributes; a shared `setupCanvas(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Helpers: `tint(hex, a)` for translucent fills from palette hexes, rounded-rect helper `rr()`, and `band(ctx, w, h, title, footer)` drawing the 28px header band (`rgba(26,82,118,0.07)`, bold 15px `#1a5276` centered) and 34px footer band (`rgba(26,82,118,0.06)`, 14px `#2c3e50` centered) used on every canvas. Dashed leader lines use dash [3,3] at 1.5px in the label's hue.
- **Palette:** charts use the validated categorical palette `P = { blue: #2a78d6, green: #008300, magenta: #d55181, yellow: #c98500, aqua: #199e70, orange: #d95926, violet: #4a3aa7, ink: #1a5276, text: #2c3e50, mute: #6b7280, grid: #e5e9ef }`; red is deliberately not in the rotation (reserved for genuine alarm states). Hue roles on this page: ECG/Holter magenta, sleep signals blue/violet, BP cuff aqua, measured/clean annotations green, missed-window and clinic-bias emphasis orange.
- In regenerated HTML, any card links elsewhere use `.html` extensions.
