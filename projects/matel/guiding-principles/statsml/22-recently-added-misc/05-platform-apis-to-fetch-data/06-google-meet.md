# Google Meet

**Page type:** detail page (two-column obj-table: text left 45%, code sample + canvas right 55%, one row)
**HTML title tag:** Google Meet — Platform APIs

**Subtitle:** After a Google Meet call ends, look up who was in it, when they joined and left, and any recording or transcript it produced.

**Verified badge:** Last verified: August 2026

## Overview

### What you can get

- A record of each meeting that took place
- Who attended, with every separate connection they made
- Recordings (stored in Google Drive) and transcripts
- Transcript text broken down by utterance and speaker
- Meeting links you can create and configure

**Key-point callout:** **A person is not a connection.** Each attendee can have several sessions — a network drop, a browser refresh, or a second device each adds one. The attendee's overall start-to-end span overstates their presence (it includes the gaps), while adding up sessions can double-count two devices. Real attended time means merging each person's intervals — neither shortcut is right.

### Watch out for

- Meeting records expire after about 30 days — extract and store them promptly, there is no recovery later
- Recordings and transcripts exist only if switched on during the call; nothing can be generated after the fact
- Dial-in and anonymous guests have no resolvable identity, so they cannot be matched across meetings
- No live data and no connection-quality stats — this API only looks backward at finished meetings

### Code sample (right column)

Heading: **One attendee, two sessions**

```
"participantSessions": [
  { "startTime": "2026-08-18T09:58:44Z",
    "endTime":   "2026-08-18T10:21:07Z" },
  { "startTime": "2026-08-18T10:23:15Z",
    "endTime":   "2026-08-18T10:59:02Z" }
]
// one person, two connections — the 2-minute gap is
// invisible if you only read the participant row
```

### Visualization (canvas `meetSessions`, responsive width × 380)

Gantt-style timeline: one row per participant, showing participantSessions bars against the conference window (minutes 0–62).

- **Title (bold 13px `#1a5276`, top center):** "One conferenceRecord: participants vs. their participantSessions"
- **Subtitle (italic 10.5px `#666`):** "thin grey line = earliestStartTime..latestEndTime;  solid bars = actual sessions"
- **Conference envelope:** dashed `#1a5276` horizontal line (dash 3/3) at y=50 spanning minute 0 to 62, labeled "conferenceRecord.startTime" (left-aligned at start) and "endTime" (right-aligned at end) in 9.5px `#1a5276`.
- **X-axis:** minutes 0–62 mapped across chart width (left offset = min(150, 26% width)+10, right margin 24); light `#eee` gridlines with `#888` labels "0 min", "15 min", "30 min", "45 min", "60 min" below the rows.
- **Rows** (top y=84, row height 42, bar height 13). Each row: right-aligned label (11px `#2c3e50`) with idType below it (italic 9px; `#888` for signedinUser, `#8e44ad` for phoneUser, `#e67e22` for anonymousUser); a thin `#888` span line with end ticks from earliest start to latest end; solid session bars in participant color at 0.85 alpha (overlapping second sessions offset down 7px and 5px shorter); italic 9px `#666` note starting at the span's left edge below the bar.
  - Amit Jaiswal — signedinUser, color `#1a5276`, sessions [0, 22.4] and [24.5, 60.3], note "reconnect — 2 sessions, 2.1 min gap"
  - R. Menon — signedinUser, color `#1a5276`, sessions [4.5, 31.0] and [28.0, 59.0] (overlapping), note "two devices — sessions overlap"
  - Guest — anonymousUser, color `#e67e22`, session [5.3, 58.9], note "no directory identity"
  - +1 555-***-**19 — phoneUser, color `#8e44ad`, sessions [12.8, 20.1] and [33.4, 46.2], note "dial-in dropped and redialled"
  - K. Rao — signedinUser, color `#1a5276`, session [40.0, 41.2], note "joined late, left after 1 min"
- **Caption (italic 10px `#e74c3c`, centered below rows):** "Counting participants gives 5. Counting sessions gives 8. Neither is attended time — merge the intervals."

## Official API References

- [Google Meet REST API](https://developers.google.com/meet/api) — documentation root for the Meet API v2
- [Meet REST API overview](https://developers.google.com/meet/api/guides/overview) — spaces vs conferenceRecords data model and capabilities

## Regeneration instructions

- **Layout:** single page: h1, `.subtitle`, `.verified` badge, then `h2` "Overview" followed by a one-row `.obj-table` (left `<td>` 45% with `.section-title` headings, bullet lists, and a `.key-point` callout; right `<td>` 55% with a `.section-title`, a `<pre>` code sample, and the canvas), then `h2` "Official API References" with a link list.
- **Page style:** body system sans-serif, `#2c3e50` text, white background, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; h2 1.3em `#1a5276` with 2px `#2980b9` bottom border; `.verified` inline badge — background `#eaf2f8`, border 1px `#2980b9`, color `#1a5276`, 0.8em, radius 4px, padding 2px 10px; `.section-title` bold `#1a5276` 1.05em; `pre` background `#f4f4f4`, padding 14px, radius 6px, 0.82em; `.key-point` background `#f8f9fa`, left border 3px `#e74c3c`, padding 10px 14px, 0.93em; li 0.93em; links `#1a5276`. No nav bar, no back/home links.
- **Canvas:** `<canvas id="meetSessions" height="380">`, CSS `width: 100%`; redraws on window resize using `getBoundingClientRect()` width; sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and `ctx.scale` back to logical coordinates.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, grays `#666`/`#888`/`#2c3e50`.
- In regenerated HTML, any card/page links use `.html` extensions.
