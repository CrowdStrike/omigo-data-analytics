# Tracking Data: Biometric Templates and Matching

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one row per section)
**HTML title tag:** Tracking Data: Biometric Templates and Matching

**Subtitle:** A biometric system does not recognise anyone. It computes a similarity score and compares it to a threshold. Everything else — the match, the identification, the accuracy claim — follows from that one comparison.

## What is it?

One pipeline, whatever the trait — finger, face, iris, voice, gait.

- **Sensor** takes a raw sample — an image, a print, a waveform
- **Template:** software extracts features and produces a list of numbers
- **What is stored** is the template; most systems discard the raw sample
- **Enrollment** is the first pass — the trait is presented once, a template computed, and filed against a record
- **Every later use** repeats the pipeline and compares the new template to the stored one

Key-point callouts:

- **A template is not a password hash:** two readings of the same finger never produce identical templates, because pressure, moisture, angle and sensor noise shift the numbers. A password check asks "is this equal?" and gets yes or no. A biometric check has no equality to test, so it asks "how close is this?" and gets a number.
- **Someone decides which numbers count:** that decision is a threshold, set by whoever configures the system — not by the sensor and not by the person being measured.

### Visualization (canvas `c1`, 720×320)

Pipeline schematic: raw sample → feature extraction → template, run twice on the same finger, converging on one similarity score.

- **Title (bold 16px `#2a78d6`, centered):** "Two captures of the same finger".
- **Two lanes** (y=72 and y=132), each: "raw sample" rounded box (104×36, fill `rgba(42,120,214,0.35)`, blue stroke) with sub-label "capture 1" / "capture 2" → blue arrow → orange `#d95926` "feature extraction" box (108×36, white bold text) → blue arrow → white "template" box (216×36, blue stroke) containing the vector in 13px monospace: capture 1 `[ 0.81  0.14  0.63  0.27  0.55 … ]`, capture 2 `[ 0.79  0.17  0.61  0.31  0.52 … ]`.
- **Brace + arrow** joining the two template boxes into a result box (152×80 at x=554, fill `rgba(42,120,214,0.15)`, blue 2px stroke) reading: "similarity" (bold 15px blue), "0.61" (bold 17px monospace blue), and "not \"equal / not equal\"" (13px `#6b7280`).
- **Bottom lines (centered):** 14px `#6b7280` "A password hash would be byte-identical or rejected. A template is neither."; bold 14px `#2a78d6` "Same finger, same person, different numbers — so the test is a distance, not an equality."; 13px `#6b7280` "Schematic. Vector values shown for illustration only."

## What does it collect?

- **Template** — the feature vector — plus the enrollment record it is filed against
- **Modality and capture device** that produced it
- **Timestamp** of the attempt, and often the reader's location
- **Similarity score** for the comparison
- **Threshold in force** at the time, and the resulting decision
- **Quality score** for the sample, and a liveness or presentation-attack result
- **Candidate list** in a search, ranked, with a score per candidate

Key-point callouts:

- **The decision is configuration, not measurement:** score and threshold are separate fields. Raise the threshold to 0.65 and this same captured sample becomes a non-match, with nothing about the person or the sensor having changed — and the decision is re-derivable after the fact from the stored score.
- **Every listed candidate cleared the bar:** a one-to-one check has a single comparison to clear it. A search can return several, and something further down the stack has to pick.

### Visualization (canvas `c2`, 720×320)

Overlapping genuine / impostor score distributions with a movable threshold.

- **Title (bold 16px `#2a78d6`, centered):** "Every decision is a threshold on a score".
- **Data (hardcoded schematic shapes, 30 bins each, no random generation):**
  - impostor: `[0, 1, 3, 7, 14, 24, 37, 52, 66, 74, 75, 69, 58, 45, 33, 23, 15, 10, 6, 4, 2, 1, 1, 0, 0, 0, 0, 0, 0, 0]`
  - genuine: `[0, 0, 0, 0, 0, 0, 0, 1, 1, 2, 3, 5, 8, 12, 18, 26, 36, 47, 58, 66, 70, 68, 60, 48, 35, 22, 12, 5, 2, 1]`
  - threshold at bin index 16.
- **Plot:** x=58, width 604, baseline y=172, height 118, value scale max 80. Impostor curve stroked blue `#2a78d6` 2px with body fill `rgba(42,120,214,0.18)`; genuine curve stroked green `#008300` with body fill `rgba(0,131,0,0.16)`. Error areas: impostor right of threshold filled `rgba(231,76,60,0.55)` (false accepts); genuine left of threshold filled `rgba(217,89,38,0.55)` (false rejects). Blue baseline.
- **Threshold:** vertical dashed blue line (dash 5/4), labeled "threshold" in bold 14px above.
- **Curve labels (bold 14px):** "impostor comparisons" (blue, left) and "genuine comparisons" (green, right).
- **Error labels (13px):** "false accepts" in red `#e74c3c` right of the threshold; "false rejects" in orange `#d95926` left of it.
- **Axis end labels (13px `#6b7280`):** "less similar" left, "more similar" right.
- **Trade annotation (bold 13px blue, centered around the threshold):** "← move it left: fewer false rejects, more false accepts" and "move it right: the reverse →".
- **Bottom caption (13px `#6b7280`, centered):** "Schematic. Both distributions are hardcoded shapes, not measured data. No threshold removes both areas."

### Sample payload (right column, under canvas `c2`)

Caption (italic): "Sample payload — illustrative structure, not real captured data."

```
// One match attempt. Field names are placeholders,
// not any real vendor's schema.
{
  "attempt_id": "ma_7c41…",

  // ── things systems of this kind genuinely record ──
  "enrollment_id":  "enr_9f2a…",   // the record, not the raw sample
  "modality":       "fingerprint",
  "capture_device": "reader_A17",
  "ts":             "2026-08-22T09:14:03Z",
  "similarity":     0.61,          // measurement
  "threshold":      0.55,          // configuration
  "decision":       "match",

  // ── inferred / plausible ──
  "sample_quality": 0.72,
  "liveness":       { "result": "pass", "score": 0.88 },
  "candidates": [                  // present in 1:N search
    { "enrollment_id": "enr_9f2a…", "similarity": 0.61 },
    { "enrollment_id": "enr_3b70…", "similarity": 0.58 },
    { "enrollment_id": "enr_c119…", "similarity": 0.56 }
  ]
}
```

## Why is it collected?

Label pill: STATED PURPOSE

- **To check one claim:** "is this the person on this record?" — unlocking a phone, clearing a turnstile
- One comparison, and the person is **cooperating**

Label pill: ADDITIONAL CONSEQUENCE

- The same equipment answers a **different question** — "who is this?", asked against a whole database
- Every record in that database gets **its own chance to match by accident**

Calc callout (monospace, `#f8f9fa` background, `#1a5276` left border):

```
Illustrative numbers, chosen to show the shape:

odds of a wrong match  = 1 in 1,000,000 per comparison
database               = 5,000,000 records
wrong matches expected = 5 per search

The same equipment against 10,000 records gives 0.01.
Nothing changed but the size of the database.
```

Key-point callout: **Most of the matches can be wrong even when the equipment is very accurate:** if the person being sought is rarely in the database, the few real matches are swamped by accidental ones. An accuracy figure earned by checking one claim does not survive being reused for a search.

### Visualization (canvas `c3`, 720×320)

Bar chart: wrong matches grow with the database, while the accuracy figure does not.

- **Titles (centered):** bold 13px `#1a5276` "Wrong matches expected per search"; sub-line 12px `#6b7280` "same equipment, same accuracy - only the database grows".
- **Bars (illustrative, matching the worked example):** "one check" ≈ 0 (label "about 0"), "10,000" = 0.01, "100,000" = 0.1, "1 million" = 1, "5 million" = 5. Baseline y=210, max height 128 scaled to 5, bar width 62, first bar centered at x=74, step 116; minimum drawn bar height 2px.
- **Colors:** last bar (5 million) highlighted orange — fill `rgba(217,89,38,0.5)`, stroke `#d95926`; the rest blue — fill `rgba(42,120,214,0.32)`, stroke `#2a78d6`. Value labels bold 13px above bars in the bar's hue; category labels 12px `#2c3e50` below.
- **X annotation (12px `#6b7280`, centered):** "records searched".
- **Captions (centered):** italic 12px `#2c3e50` "The accuracy claim is earned on the left and quoted on the right."; italic 11px `#6b7280` "Illustrative - the numbers show the shape, not a tested device."

## Regeneration instructions

- **Layout:** tracking detail page — `<table class="obj-table">`, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` heading, optional `.lede`, bullets with bold lead terms (`li b` in `#1a5276`), `.key-point` callouts, `.lbl` label pills, and (row 3) a `.calc` monospace callout; right `<td>` (55%, text-align center) holds the canvas, and for the "What does it collect?" row also the `.payload-note` caption plus `<pre class="payload">` block (left-aligned).
- **Page CSS:** body system sans-serif (-apple-system stack), white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; `.obj-title` bold 1.1em `#1a5276`; td borders `1px solid #2980b9`, padding 16px; li 0.93em; `.key-point` background `#f8f9fa`, left border `3px solid #1a5276`, 0.93em, `strong:first-child` in `#1a5276`; `.lede` 0.95em; `.lbl` pills 0.7em bold uppercase radius 3px — `.lbl-purpose` `#eaf2fb`/`#1a5276`, `.lbl-effect` `#fdf0e6`/`#a8501c`; `.calc` background `#f8f9fa`, left border `3px solid #1a5276`, ui-monospace 0.88em; `.payload` background `#f8f9fa`, left border `3px solid #1a5276`, ui-monospace 0.78em, white-space pre; `.payload-note` 0.82em italic `#666`. No nav bar, no back/home links.
- **Canvas:** each canvas declares intrinsic `width`/`height` attributes; a shared `setupCanvas(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Helpers: `tint(hex, a)`, rounded-rect `rr()`, and an `arrow(ctx, x1, y, x2, color)` horizontal-arrow helper.
- **Palette:** charts use the validated categorical palette `P = { blue: #2a78d6, green: #008300, magenta: #d55181, yellow: #c98500, aqua: #199e70, orange: #d95926, violet: #4a3aa7, ink: #1a5276, text: #2c3e50, mute: #6b7280, grid: #e5e9ef }`; red `#e74c3c` reserved for genuine error states (used here only for false accepts). Site palette anchors: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- In regenerated HTML, any card links elsewhere use `.html` extensions.
