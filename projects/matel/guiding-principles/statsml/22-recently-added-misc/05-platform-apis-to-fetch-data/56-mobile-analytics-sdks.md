# Mobile Analytics SDKs

**Page type:** detail page (two-column obj-table layout: text left 45%, code + canvas right 55%, one Overview row)
**HTML title tag:** Mobile Analytics SDKs — Platform APIs

**Subtitle:** Analytics tools (Firebase/GA4, Amplitude, Mixpanel) that record what users do inside your own app — you choose what to track, and you can export every raw event.

**Verified badge:** Last verified: August 2026

## Overview

Left column:

**What you can get**

- Every tap, screen view, and custom event you chose to record, per user, inside your app
- Ready-made dashboard numbers: funnels, retention, active users
- A full raw-event export (for example Firebase into BigQuery) for your own modelling
- Your own user ID attached to events, so journeys can be stitched across devices

**Key point (callout):** **The dashboard and the raw export will disagree — legitimately.** Dashboards sample, round, and hide small groups; the raw export has everything but keeps filling in for days. Pick one as the source of truth per question and expect a stable gap between them — alert only when the gap changes.

**Watch out for**

- No history before you start: an event you didn't track in March cannot be recovered in April, ever
- Nothing outside your own app — no competitor apps, no device-wide behaviour
- An event that silently stops firing (a client bug) looks exactly like users changing behaviour — monitor per-event volume
- Mixpanel's JQL query language is deprecated — do not build new work on it

Right column:

**GA4 aggregate API** — the number is an estimate, and it says so

Code block (pre, verbatim):

```
"metadata": {
  "samplingMetadatas": [
    { "samplesReadCount":  "1120004",
      "samplingSpaceSize": "4870219" }
    // ^ only ~23% of events were read;
    //   the reported total is an ESTIMATE
  ],
  "dataLossFromOtherRow": true,
  "subjectToThresholding": true
}

// A raw BigQuery COUNT(*) over the same
// slice will NOT match this figure.
// Neither number is wrong — they are
// different estimators.
```

**Retention cohorts: raw export vs sampled/thresholded aggregate API** (label above canvas)

### Visualization (canvas `fidelityCanvas`, responsive width × 380)

Multi-series retention line chart: four cohort retention curves over days since first_open, showing large-cohort agreement between raw and aggregate data, and small-cohort aggregate data disappearing past a threshold.

- **Title (bold 13px, `#1a5276`, top center):** "Same cohorts, two datasets". Below it (italic 10px, `#888`): "illustrative curves — the point is the divergence pattern, not the values".
- **X-axis:** days since first_open, values `[0, 1, 3, 7, 14, 21, 30, 45, 60]`, linear scale 0–60, ticks labeled "d0"…"d60" in `#666`.
- **Y-axis:** retained share 0–100%, gridlines (`#eee`) and labels at every 20% in `#666`. Axes stroked `#999`. Padding: top 62, right 22, bottom 76, left 56.
- **Series (line width 2, dots radius 3.2):**
  - Large cohort — raw export: solid `#1a5276`, values `[1.00, 0.462, 0.311, 0.238, 0.181, 0.156, 0.134, 0.115, 0.103]`.
  - Large cohort — aggregate API (sampled): dashed 5/4 `#27ae60`, values `[1.00, 0.455, 0.318, 0.231, 0.187, 0.149, 0.139, 0.109, 0.098]`.
  - Small cohort — raw export: solid `#8e44ad`, values `[1.00, 0.404, 0.263, 0.192, 0.141, 0.118, 0.096, 0.078, 0.066]`.
  - Small cohort — aggregate API (thresholded): dashed 5/4 `#e67e22`, values `[1.00, 0.398, 0.271, 0.205, null, null, null, null, null]` — line stops at day 7.
- **Suppression region:** from x=day 7 to the right edge, fill `rgba(231,76,60,0.06)`; vertical dashed 4/4 line at day 7 in `#e74c3c` (width 1.5). Annotations at top of region: bold 10px `#e74c3c` "small cohort drops below threshold", then italic 9px `#c0392b` two lines: "aggregate API returns nothing here —" / "the raw export still has every event".
- **Y-axis label (rotated, bold 11px, `#1a5276`):** "retained share of cohort".
- **Legend (two rows above bottom caption, line swatches, labels in `#2c3e50`):** row 1: "Large cohort — raw export" (`#1a5276`, solid), "Large cohort — aggregate API (sampled)" (`#27ae60`, dashed); row 2: "Small cohort — raw export" (`#8e44ad`, solid), "Small cohort — aggregate API (thresholded)" (`#e67e22`, dashed).
- **Bottom caption (italic 10px, `#666`, center):** "reconciling by declaring one series \"wrong\" discards the actual finding: they answer different questions".

## Official API References

- [Firebase Analytics documentation](https://firebase.google.com/docs/analytics) — SDK setup, automatically collected events, BigQuery export
- [GA4 Data API (v1)](https://developers.google.com/analytics/devguides/reporting/data/v1) — runReport and related methods, response metadata

## Regeneration instructions

- **Layout:** platform-apis detail page. h1, `.subtitle` paragraph, `.verified` badge span, then `h2` "Overview" followed by a full-width `.obj-table` with one `<tr>`: left `<td>` (45%) holds `.section-label` bold headings ("What you can get", "Watch out for"), bullet lists, and one `.key-point` callout; right `<td>` (55%) holds small gray-bold intro paragraphs (0.85em, `#555`) above a `<pre>` code block and the canvas. Then `h2` "Official API References" with a link list. Note: on this page the obj-table cells have padding 16px and no visible cell borders; heading class is `.section-label` (bold, `#1a5276`, display block).
- **Page CSS:** body system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; `.verified` badge inline-block, background `#eaf2f8`, border `1px solid #2980b9`, text `#1a5276`, padding 2px 10px, radius 4px, 0.8em; h2 1.3em `#1a5276` with `2px solid #2980b9` bottom border; `pre` background `#f4f4f4`, padding 14px, radius 6px, 0.82em; `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 10px 14px, 0.93em; list items 0.93em; links `#1a5276`. No nav bar, no back/home links.
- **Canvas:** `<canvas id="fidelityCanvas" height="380">`, CSS `display:block; width:100%`; drawing script measures `getBoundingClientRect()`, scales backing store by `window.devicePixelRatio`, fixes CSS height to 380px, and redraws on window resize.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, bar fill `rgba(26,82,118,0.35)`, gray text `#666`/`#555`.
