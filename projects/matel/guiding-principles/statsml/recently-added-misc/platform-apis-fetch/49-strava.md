# Strava

**Page type:** detail page (two-column obj-table layout: text left 45%, payload + canvas right 55%, single Overview row, followed by an API-references list)
**HTML title tag:** Strava — Platform APIs

**Subtitle:** Lets you pull an athlete's uploaded workouts — runs, rides, GPS tracks and sensor series — with their permission.

**Verified badge:** Last verified: August 2026

## Overview

**What you can get** (section label, left column)

- A list of an athlete's activities with distance, time, elevation and averages
- The sensor series behind each activity: GPS track, heart rate, power, cadence, altitude
- Segments (user-created route sections) and each athlete's attempts on them
- Athlete profile, training zones and gear mileage
- Webhook pings when activities are created, edited or deleted

**Key-point callout (red left border):**

**Strava measured almost none of this.** It is an aggregator over hundreds of device types — watches, phones, bike computers, indoor trainers — each with its own accuracy, plus Strava's own recomputed layers on top. Treat the recording device as part of the data's definition, and remember that athletes can edit or delete history at any time.

**Watch out for** (section label, left column)

- Rate limits are per app and shared across all your users: 200 requests per 15 minutes and 2,000 per day overall — but reads are capped tighter at 100 per 15 minutes and 1,000 per day, and that read limit is the binding one for most apps
- Privacy settings silently remove activities and clip GPS tracks near homes — nothing tells you what was withheld
- "Power" may be measured by a sensor or estimated by Strava; one boolean flag (`device_watts`) is all that distinguishes them
- The developer agreement forbids much of what analysts want — aggregation, model training, broad storage — so legal availability is far narrower than technical availability

**Payload note (right column):** **An activity's streams** — parallel arrays joined by position. The time gaps are uneven, and the null is a sensor dropout, not zero.

Code block (`pre`), verbatim:

```
{
  "time": {
    "data": [0, 1, 2, 3, 5, 8, 14, 22, 31, 45]
  },
  "heartrate": {
    "data": [96, 98, 101, 104, 109,
             118, 130, 141, 148, 152]
  },
  "watts": {
    "data": [0, 104, 162, 188, 205,
             231, null, 252, 268, 281]
  }
}
```

**Chart caption (payload-note above canvas):** **A 30-second average computed on the time axis vs by array position** — they diverge wherever recording is uneven.

### Visualization (canvas `stravaStreamChart`, responsive width × 380)

Line chart over a 300-second heart-rate stream: raw irregular samples as dots, plus two trailing-window averages — one over a 30-second time window (correct, green solid) and one over 30 array elements (wrong, red dashed) — diverging inside a shaded variable-rate recording band.

- **Data model (deterministic, computed):**
  - Heart rate: `98 + 46/(1+exp(-(t-140)/22)) - 14/(1+exp(-(t-235)/9)) + 3.2*sin(t/11) + 1.6*sin(t/3.7)` bpm (rise through the middle, then partial recovery, with wobble).
  - Sample times: dense 1 Hz for 0–100 s; variable-rate ("smart recording") 100–200 s using cumulative gaps `[4, 7, 3, 9, 12, 5, 15, 8, 6, 11, 4, 13]` s; dense 1 Hz again 200–300 s.
  - Correct series (green): trailing mean over samples whose time offsets fall within 30 seconds.
  - Wrong series (red dashed): trailing mean over the last 30 array elements, as if spacing were constant.
- **Layout:** height 380; padding left 56, right 18, top 54, bottom 74. x maps 0–300 s; y maps 90–160 bpm.
- **Title (top center):** bold 13px `#1a5276` "An index-based window is not a time-based window"; italic 10px `#888` sub-line "Illustrative stream shape. Strava streams are index-aligned with no absolute timestamps."
- **Variable-rate band:** 100–200 s filled `rgba(26,82,118,0.12)` with `rgba(26,82,118,0.35)` vertical boundary lines; centered bold 10px `#1a5276` label near the top: "variable-rate recording".
- **Gridlines:** `#e8e8e8` horizontal every 10 bpm from 90 to 160, with `#555` 11px right-aligned tick labels.
- **Raw samples:** 1.7px-radius `#1a5276` dots at their true time offsets.
- **Series lines:** width 2.1, drawn point-to-point at true offsets — time-window mean solid `#27ae60`; index-window mean dashed (`[6,4]`) `#e74c3c`.
- **Axes:** `#2c3e50` L-shape; x ticks every 50 s labeled 0–300 in `#555` 11px.
- **Axis titles (bold 11px `#1a5276`):** x "Offset from start_date (seconds) — the time stream, not the array index" centered below; y "Heart rate (bpm)" rotated −90° at left.
- **Legend (bottom row, 10px, `#555` labels):** blue dot swatch "heartrate samples (irregular offsets)"; solid green line swatch "30-second window (time axis)"; dashed red line swatch "30-sample window (by index)".
- Redraws on window resize.

## Official API References

- [Strava Developers](https://developers.strava.com/) — portal root: API agreement, app registration and program updates
- [Rate Limits](https://developers.strava.com/docs/rate-limits/) — per-application overall and read-specific 15-minute and daily quotas, and the usage response headers

## Regeneration instructions

- **Layout:** platform-APIs detail page. h1, `.subtitle` paragraph, `.verified` badge span, then `h2` "Overview" with a single-row `.obj-table` (left `<td>` 45%: section labels + bullet lists + one `.key-point` callout; right `<td>` 55%: `.payload-note` + `<pre>` JSON + chart note + `<canvas>`), then `h2` "Official API References" with a link list.
- **Page CSS:** body system sans-serif (-apple-system stack), line-height 1.6, text `#2c3e50`, white background, padding 30px 40px. h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; `.verified` inline badge — background `#eaf2f8`, border `1px solid #2980b9`, text `#1a5276`, padding 2px 10px, radius 4px, 0.8em. h2 1.3em `#1a5276` with `border-bottom: 2px solid #2980b9`. `pre` background `#f4f4f4`, padding 14px, radius 6px, 0.82em. `.key-point` background `#f8f9fa`, `border-left: 3px solid #e74c3c`, padding 10px 14px, 0.93em. `.section-label` bold `#1a5276` block. `.payload-note` 0.85em `#555`. `li`/`p` 0.93em; links `#1a5276`; `code` background `#f4f4f4`. No nav bar, no back/home links.
- **Canvas:** `<canvas id="stravaStreamChart" height="380">`, CSS `display:block; width:100%`; drawing code reads `getBoundingClientRect().width`, sets backing store to `rect.width * dpr` / `380 * dpr` using `window.devicePixelRatio`, fixes CSS height to 380px, `ctx.scale` back to logical coordinates, and re-renders on `resize`.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, band fill `rgba(26,82,118,0.12)`; grid `#e8e8e8`; text `#555`/`#2c3e50`/`#888`.
- In regenerated HTML, any card/page links use `.html` extensions.
