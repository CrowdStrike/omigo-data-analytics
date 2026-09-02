# Zoom

**Page type:** detail page (two-column obj-table layout: text left 45%, code sample + canvas right 55%, one Overview row; second h2 section with reference links)
**HTML title tag:** Zoom — Platform APIs

**Subtitle:** Pull who attended each Zoom meeting and for how long, plus recordings, transcripts and connection-quality stats.

**Verified badge:** Last verified: August 2026

## Overview

### Left column

**What you can get**

- Meetings as scheduled, and as they actually happened
- Per-person join and leave times for each meeting
- Cloud recordings, and transcripts if they were enabled beforehand
- Connection-quality stats per participant
- Webinar registration and attendance

**Key-point callout:** **The rejoin trap.** Attendance data has one row per *connection*, not per person. Someone who dropped and rejoined appears twice, and joining from two devices double-counts. Merge each person's time intervals before counting heads or hours, or every attendance number comes out inflated.

**Watch out for**

- Quality stats and deleted recordings expire much faster than the meeting record itself — pull them promptly or lose them
- Transcripts exist only if the setting was on *before* the meeting started; nothing can be transcribed after the fact
- Some data needs a higher-tier Zoom plan — the same code can work in one account and fail in another
- Guests often have no reliable identity, so the same person cannot be matched across meetings

### Right column

**Section title:** Participant report — same person, two rows

Code block (pre, JSON):

```
"participants": [
  { "id": "u8Kd0Ql3...", "name": "Amit Jaiswal",
    "join_time": "2026-08-18T09:58:44Z",
    "leave_time": "2026-08-18T10:21:07Z" },
  { "id": "u8Kd0Ql3...", "name": "Amit Jaiswal",
    "join_time": "2026-08-18T10:23:15Z",
    "leave_time": "2026-08-18T10:59:02Z" },
  { "id": "", "name": "iPhone", "user_email": "",
    "join_time": "2026-08-18T10:11:33Z",
    "leave_time": "2026-08-18T10:44:58Z" }
]
// rows 1-2: one person who rejoined
// row 3: a guest with no identity at all
```

### Visualization (canvas `zoomWindows`, responsive width × 380)

Horizontal bar chart: approximate retrievable history in days per Zoom API surface, with each row showing an endpoint sub-label, a note beside the bar, and an optional orange "gated:" annotation; open-ended bars get a ragged white right edge.

- **Data (label, endpoint sub-label, value in days on a 0–190 scale, bar color, open-ended flag, note, gate):**
  - Scheduled meetings — `/users/{id}/meetings`, 175, `#27ae60`, open, "persists while meeting exists", no gate
  - Past meeting instance — `/past_meetings/{uuid}`, 175, `#27ae60`, open, "keyed by UUID, long-lived", no gate
  - Report: participants — `/report/meetings/{id}/participants`, 175, `#1a5276`, closed, "~1 month per request, paged", gated: "reporting plan"
  - Report: usage rollups — `/report/users, /report/daily`, 175, `#1a5276`, closed, "~1 month per request, paged", gated: "reporting plan"
  - Dashboard metrics — `/metrics/meetings?type=past`, 120, `#e67e22`, closed, "few months, plan-dependent", gated: "Business/Edu/Ent — not Pro"
  - Dashboard QoS detail — `/metrics/.../participants/qos`, 12, `#e74c3c`, closed, "short window after meeting ends", gated: "Business/Edu/Ent — not Pro"
  - Cloud recording files — `/users/{id}/recordings`, 175, `#8e44ad`, open, "admin auto-delete setting decides", gated: "cloud recording licence"
  - Deleted recordings — `Trash before purge`, 30, `#e74c3c`, closed, "limited grace period, then gone", no gate
  - Audio transcript — `TRANSCRIPT file in recording`, 0, `#e74c3c`, closed, "exists only if enabled beforehand", gated: "setting ON before meeting"
- **Title (centered, bold 13px `#1a5276`):** "How far back each Zoom surface can still answer"
- **Subtitle (centered, italic 10.5px `#666`):** "approximate retrievable history in days; open-ended bars are ragged at the right edge"
- **Layout:** label column min(180, 30% of width) — label right-aligned 11px `#2c3e50`, endpoint sub-label italic 9px `#888` below it; bars start at labelW+10, chart width extends to width−170; bar height 17, gap 17, first row at y=62; bars filled at alpha 0.85 (zero-value bars solid red, min width 3px); open-ended bars get a white zigzag cut at the right edge. Notes 9.5px right of each bar in `#666` (red for zero-value rows); gate annotations italic orange 9px on a second line: "gated: <gate>".
- **Gridlines:** light `#eee` verticals at 0, 30, 90, 180 days, tick labels "0d", "30d", "90d", "180d" in `#888` below.
- **Bottom caption (centered, italic 10px `#666`):** "Red rows expire faster than the meeting record itself — or never exist unless a setting was on in advance."
- Canvas redraws on window resize.

## Official API References

- [Zoom Developer Platform docs](https://developers.zoom.us/docs/) — top-level documentation portal
- [Zoom API overview](https://developers.zoom.us/docs/api/) — REST API families, references, and usage guides

## Regeneration instructions

- **Layout:** single detail page: h1, `.subtitle` paragraph, `.verified` badge span, `h2` "Overview" with a full-width `.obj-table` (one `<tr>`; left `<td>` 45% text, right `<td>` 55% code + canvas), then `h2` "Official API References" with a plain `<ul>` of links. No nav bar, no back/home links.
- **Page CSS:** body system sans-serif (-apple-system stack), line-height 1.6, text `#2c3e50`, white background, padding 30px 40px; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; `.verified` badge — background `#eaf2f8`, border `1px solid #e0e0e0`, text `#1a5276`, padding 2px 10px, radius 4px, 0.8em; h2 1.3em `#1a5276` with `border-bottom: 2px solid #2980b9`; obj-table cells padding 16px, top-aligned (no cell borders); `.section-title` bold `#1a5276` 1.05em; li 0.93em; links `#1a5276`.
- **Code block:** `pre` — background `#f4f4f4`, padding 14px, radius 6px, 0.82em, left-aligned, horizontal overflow scroll.
- **Callout:** `.key-point` — background `#f8f9fa`, `border-left: 3px solid #e74c3c`, padding 10px 14px, 0.93em.
- **Canvas:** `display:block; width:100%`, `height` attribute 380; sized from `getBoundingClientRect().width`, scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates), redrawn on resize.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, gray text `#666`/`#888`.
- In regenerated HTML, any card/page links use `.html` extensions.
