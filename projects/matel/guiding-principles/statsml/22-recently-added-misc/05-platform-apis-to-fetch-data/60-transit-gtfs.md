# Transit GTFS

**Page type:** detail page (two-column obj-table layout: text left 45%, code + canvas right 55%, one Overview row)
**HTML title tag:** Transit GTFS — Platform APIs

**Subtitle:** The open format public-transit agencies use to publish their timetables (a zip of spreadsheet files) and live vehicle updates (a streaming feed).

**Verified badge:** Last verified: August 2026

## Overview

Left column:

**What you can get**

- Full timetables for an agency: stops, routes, trips, and scheduled times
- Live predicted arrivals, delays, and cancellations
- Real-time vehicle positions, sometimes with crowding levels
- Service alerts for disruptions and closures

**Key point (callout):** **Live feeds are not archived.** The real-time feed is a snapshot that overwrites itself; yesterday's data is simply gone unless you were recording it. Any study of actual (not scheduled) service must have its own capture pipeline running before the questions are asked.

**Watch out for**

- Feed quality varies wildly by agency: some refresh every few seconds, others every few minutes, and optional fields may be empty or missing
- Comparing agencies without adjusting for refresh rate manufactures differences — a slow feed simply cannot see short delays
- Scheduled times can exceed 24:00 (overnight trips belong to the previous service day), and IDs are only unique within one feed — naive parsing and naive merging both fail silently
- There is no central registry: finding and maintaining feed URLs is manual work that decays

Right column:

**stop_times.txt — the timetable itself, with its classic trap** (section-head)

Code block (pre, verbatim):

```
trip_id,arrival_time,departure_time,stop_id,stop_sequence
T_4471_wk,06:12:00,06:12:00,S_1102,1
T_4471_wk,06:14:30,06:15:00,S_1108,2
T_4471_wk,06:19:00,06:19:00,S_1121,3
T_9930_wk,24:48:00,24:48:00,S_1102,1
T_9930_wk,25:03:00,25:03:00,S_1121,3

# rows 4-5: times past 24:00:00 belong to the
# PRIOR service day. Parsing these as clock
# times drops all overnight service.
```

**Realtime refresh interval — illustrative spread** (section-head above canvas)

### Visualization (canvas `cadenceChart`, responsive width × 380)

Horizontal bar chart on a log x-scale: GTFS-Realtime refresh interval for 14 illustrative agencies, each bar annotated with interval and message-type coverage.

- **Title (bold 13px, `#1a5276`, top left):** "GTFS-Realtime refresh interval spread across agencies". Below it (bold italic 10px, `#e74c3c`): "ILLUSTRATIVE — shape of the observed spread, not measured values from named agencies".
- **Data (agency, seconds, message coverage label):**
  - Agency A — 5s — TU+VP+SA
  - Agency B — 10s — TU+VP+SA
  - Agency C — 15s — TU+VP
  - Agency D — 15s — VP only
  - Agency E — 20s — TU+VP
  - Agency F — 30s — TU+VP+SA
  - Agency G — 30s — TU only
  - Agency H — 45s — TU+VP
  - Agency I — 60s — VP only
  - Agency J — 60s — TU+VP
  - Agency K — 90s — TU only
  - Agency L — 120s — TU+VP
  - Agency M — 180s — SA only
  - Agency N — 300s — TU only
- **Bar colors by interval:** ≤20s green `#27ae60`; ≤60s orange `#e67e22`; >60s red `#e74c3c`. Bars ≤15px tall, drawn from the y-axis to the log-scaled x of the interval. Beside each bar (monospace 9.5px, `#666`): "<sec>s  <msgs>". Agency labels 10.5px `#2c3e50` right-aligned.
- **X-axis:** log10 scale from 4s to 400s; gridlines (`#e8e8e8`) and tick labels (`#555`) at 5s, 10s, 30s, 60s, 120s, 300s. L-shaped axis in `#2c3e50` (width 1.5). Padding: top 54, right min(92, 18% of width), bottom 52, left 78.
- **X-axis label (bold 11px, `#1a5276`, centered):** "feed refresh interval (log scale)".
- **Annotation at 60s:** vertical dashed 5/4 purple `#8e44ad` line (width 1.5); italic 9.5px `#8e44ad` two-line text to its right: "at 60s, any delay event shorter" / "than 60s is unobservable".
- **Bottom-right note (italic 10px, `#e74c3c`):** "pooling these rows without normalizing cadence manufactures effects".

## Official API References

- [gtfs.org](https://gtfs.org/) — the canonical GTFS specification site (Schedule and Realtime references, best practices)
- [GTFS-Realtime overview — Google Transit](https://developers.google.com/transit/gtfs-realtime) — TripUpdate, VehiclePosition and ServiceAlert reference

## Regeneration instructions

- **Layout:** platform-apis detail page. h1, `.subtitle` paragraph, `.verified` badge span, then `h2` "Overview" followed by a full-width `.obj-table` with one `<tr>`: left `<td>` (45%) with `.section-head` headings ("What you can get", "Watch out for"), bullet lists, and one `.key-point` callout; right `<td>` (55%) with `.section-head` labels above a `<pre>` code block and the canvas. Then `h2` "Official API References" with a link list.
- **Page CSS:** body system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; `.verified` badge inline-block, background `#eaf2f8`, border `1px solid #e0e0e0`, text `#1a5276`, padding 2px 10px, radius 4px, 0.8em; h2 1.3em `#1a5276` with `2px solid #2980b9` bottom border; obj-table cells `1px solid #e0e0e0` border, padding 16px; `.section-head` bold `#1a5276` 0.95em; `pre` background `#f8f9fa`, left border `3px solid #1a5276`, monospace 0.78em, padding 16px, radius 4px; `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 10px 14px, 0.93em; list items 0.93em; links `#1a5276`. No nav bar, no back/home links.
- **Canvas:** `<canvas id="cadenceChart" height="380">`, CSS `display:block; width:100%`; drawing script measures `getBoundingClientRect()`, scales backing store by `window.devicePixelRatio` via `setTransform(dpr,0,0,dpr,0,0)`, fixes CSS height to 380px, and redraws on window resize.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, bar fill `rgba(26,82,118,0.35)`, gray text `#666`/`#555`.
