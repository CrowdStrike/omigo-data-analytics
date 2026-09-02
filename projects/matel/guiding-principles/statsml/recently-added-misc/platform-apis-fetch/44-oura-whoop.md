# Oura & Whoop

**Page type:** detail page (two-column obj-table layout: text left 45%, payload/canvas right 55%, one row for the Overview section)
**HTML title tag:** Oura & Whoop — Platform APIs

**Subtitle:** Lets you read sleep, recovery and readiness data from a user's Oura ring or Whoop strap with their permission.

**Verified badge:** Last verified: August 2026

## Overview

### What you can get

- Oura: daily readiness, sleep and activity scores, plus nightly heart rate, HRV, temperature and blood-oxygen readings
- Whoop: a recovery score and strain score for each sleep-to-wake cycle, plus resting heart rate and HRV
- Detailed sleep records with time spent in each sleep stage
- Workouts with heart-rate summaries
- Oura user notes via the enhanced-tag feed (the original plain tag feed is deprecated)

### Key point (callout)

**The headline scores are the vendors' own secret formulas, not measurements.** Both companies have shipped updates that moved everyone's readiness or recovery numbers with no change in the people wearing the devices. For any serious analysis, use the underlying measured components — heart rate, HRV, sleep-stage minutes — instead of the scores.

### Watch out for

- Whoop organises data by sleep-to-wake "cycles", not calendar days — joining it to daily data from anywhere else misaligns silently
- Vendors recompute history: a record you fetched yesterday can be different today, with no version marker
- A night without the device produces no record at all — absence, not a zero — and those missing nights are exactly the disrupted ones you care about
- Whoop developer access requires registration and review; plan for it before designing a study around it

### Payload example (right column)

Payload note: **A Whoop recovery record** — the score is a model output; the heart-rate and HRV fields are its measured inputs

```json
{
  "records": [
    {
      "cycle_id": 93845721,
      "score_state": "SCORED",
      "score": {
        "recovery_score": 63,
        "resting_heart_rate": 51,
        "hrv_rmssd_milli": 62.4,
        "spo2_percentage": 96.3,
        "skin_temp_celsius": 33.8
      }
    }
  ]
}
```

Second payload note (chart caption): **90 days of one person's composite score vs their own HRV.** The score steps down at a software release; the physiology does not move.

### Visualization (canvas `scoreInstability`, responsive width × 380)

Dual-line time series over 90 days: vendor composite score (with an artificial software-release step) vs nightly HRV rescaled onto the same 0–100 axis (no step).

- **Data generation (deterministic, exact formulas):** N = 90 days; BREAK = day index 55 (score-model update); STEP = −9 score points. Pseudo-random helper `rnd(i, salt) = (frac(sin((i+1)·12.9898 + salt·78.233)·43758.5453) − 0.5)·2` in [−1, 1]. Nightly HRV RMSSD (ms), mild weekly rhythm, no step: `hrv[i] = 58 + 3.0·sin(i/7·2π) + 2.2·sin(i/29) + 4.6·rnd(i,1)`. Composite score, driven by the same physiology plus a pure software step: `score[i] = 70 + (hrv[i]−58)·1.15 + 2.6·rnd(i,2) + (i ≥ 55 ? −9 : 0)`. HRV plotted rescaled onto the score axis using the pre-break linear map (no step introduced): `hrvPlot[i] = 70 + (hrv[i]−58)·1.15`.
- **Series styling:** composite score line `#e67e22` width 2 (visible step); rescaled HRV line `#27ae60` width 1.6 (no step).
- **Update marker:** vertical dashed red `#e74c3c` line (dash 6/4, width 1.5) at day 55, with bold red 10.5px label "app/firmware score-model update" to its right near the top.
- **Pre/post means:** flat thick (width 5) reference segments in `rgba(26,82,118,0.35)`: pre-break mean of score over days 8–54 drawn from day 8 to 54; post-break mean over days 55–89 drawn from day 55 to 89. Labels in `#555` 10.5px: "mean X.X" above the pre segment and "mean Y.Y  (Δ Z.Z)" below the post segment (values computed from the data).
- **Axes:** y from 0 to 100, gridlines `#e8e8e8` every 10, labels every 20 in `#555`; x ticks/gridlines every 15 days (0–90) in `#555`; axes `#2c3e50`. Axis titles: "Day" (bottom center) and rotated "Score (0–100 scale)" (left), `#2c3e50` 11px. Padding: left 50, right 16, top 48, bottom 80.
- **Title (bold 13px, `#1a5276`, left-aligned):** "Composite score instability: the score moved, the physiology did not". Subtitle (11px, `#555`): "90 nights, one subject. A level shift with no physiological correlate is a software release, not a finding."
- **Legend (bottom left):** line samples with labels in `#555`: `#e67e22` solid "vendor composite score"; `#27ae60` solid "nightly HRV RMSSD (rescaled to same axis)"; `#e74c3c` dashed "score-model release".
- **Footnote (italic 10.5px, red `#e74c3c`, bottom left):** "A changepoint test on the purple series finds a real break. It is a release note, not a physiological event."
- **Behavior:** redraws on window resize; width follows container.

## Official API References

- [Oura API Documentation](https://cloud.ouraring.com/docs) — API v2 usercollection endpoints, OAuth 2.0, personal access tokens and webhooks
- [Whoop Developer Documentation](https://developer.whoop.com/docs) — OAuth 2.0 scopes, cycle/recovery/sleep/workout endpoints, pagination and webhooks

## Regeneration instructions

- **Layout:** single-page detail doc: h1, `.subtitle` paragraph, `.verified` badge, then `## Overview` (h2 with 2px `#2980b9` bottom border) containing one `.obj-table` (full width, one `<tr>`: left `<td>` 45% with `.section-label` headings + bullet lists + `.key-point` callout; right `<td>` 55% with `.payload-note` paragraphs, `<pre>` JSON payload, and the canvas), then `## Official API References` as a plain `<ul>` of external links.
- **Page CSS:** body system sans-serif (-apple-system stack), line-height 1.6, text `#2c3e50`, white background, padding 30px 40px; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; `.verified` badge inline-block, background `#eaf2f8`, border `1px solid #2980b9`, text `#1a5276`, padding 2px 10px, radius 4px, 0.8em; h2 1.3em `#1a5276` with `border-bottom: 2px solid #2980b9`; `.section-label` bold `#1a5276` block; `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 10px 14px, 0.93em; `.payload-note` 0.85em `#555`; `pre` background `#f4f4f4`, padding 14px, radius 6px, 0.82em; `code` background `#f4f4f4`; links `#1a5276`; li/p 0.93em. No nav bar, no back/home links.
- **Canvas:** `width: 100%` via CSS, height attribute 380; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates), redraw on resize.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`; grid `#e8e8e8`, axis `#2c3e50`, muted text `#555`/`#888`.
- In regenerated HTML, any card links use `.html` extensions (this page has only external links).
