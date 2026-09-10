# Cisco Webex

**Page type:** detail page (two-column obj-table layout: text left 45%, caption + canvas right 55%, one Overview row; second h2 section with reference links)
**HTML title tag:** Cisco Webex — Platform APIs

**Subtitle:** Read Webex messages, meetings, recordings and meeting-quality data — how much you can see depends entirely on who you sign in as.

**Verified badge:** Last verified: August 2026

## Overview

### Left column

**What you can get**

- Messages and shared files from group spaces
- Meetings, who attended them, and recordings
- Meeting transcripts, where they were produced
- Network-quality data per meeting participant
- Room-hardware status and workspace sensor readings (people count, noise, air quality)

**Key-point callout:** **A bot is not an observer.** In group spaces a bot only receives messages that @mention it, so a "read all our chat" pipeline built on a bot sees a tiny, biased sample. Reading everyone's messages requires a special Compliance Officer role — and with the wrong identity the API returns an empty list, not an error, so a broken pipeline looks perfectly healthy.

**Watch out for**

- Meeting-quality data requires a paid add-on (Pro Pack); without it the data simply is not available
- Real-time notifications carry only IDs, not text — if a message is deleted before you fetch it, the content is gone for good
- Recurring meetings: attendance, recordings and quality data attach to individual occurrences, not the series
- Deleted messages vanish without a trace; edits keep no history of the original

### Right column

**Caption (italic, 0.85em, `#666`):** What each way of signing in can actually see, and what unlocks the rest.

### Visualization (canvas `webexGates`, responsive width × 380)

Horizontal bar chart: how much of the org each Webex API surface exposes (reach 0–100, "single caller" to "whole org"), each bar colored by what unlocks it, drawn over a faint full-width track showing what is withheld, with a gate annotation right of each bar.

- **Data (label, endpoint sub-label, reach 0–100, bar color, gate annotation):**
  - Messages (bot token) — `/v1/messages`, 8, `#e74c3c`, "@mentions only in group spaces"
  - Messages (user OAuth) — `/v1/messages`, 26, `#e67e22`, "only that user’s own spaces"
  - Messages (compliance) — `spark-compliance:*`, 100, `#8e44ad`, "Compliance Officer role required"
  - Rooms / memberships — `/v1/rooms`, 30, `#e67e22`, "caller’s spaces; org view needs compliance"
  - Meetings (host) — `/v1/meetings`, 24, `#e67e22`, "own meetings; series ≠ instance"
  - Meetings (site admin) — `/v1/meetings?siteUrl=`, 90, `#1a5276`, "Webex site admin"
  - Recordings (host) — `/v1/recordings`, 22, `#e67e22`, "own recordings only"
  - Recordings (admin) — `/v1/admin/recordings`, 92, `#1a5276`, "org admin / compliance officer"
  - Meeting Qualities — `/v1/meeting/qualities`, 88, `#8e44ad`, "requires Pro Pack (paid add-on)"
  - Devices / Workspaces — `/v1/workspaceMetrics`, 78, `#1a5276`, "org admin + sensor must exist"
  - Admin Audit Events — `/v1/adminAudit/events`, 95, `#27ae60`, "org admin; config changes only"
- **Title (centered, bold 13px `#1a5276`):** "How much of the org each Webex surface actually exposes"
- **Subtitle (centered, italic 10.5px `#666`):** "bar length is illustrative, not measured; the label states what gates the remainder"
- **Layout:** label column min(178, 30% of width) — label right-aligned 10.5px `#2c3e50`, endpoint sub-label italic 8.5px `#888` below it; bars start at labelW+10, chart width extends to width−200; bar height 15, gap 14, first row at y=62. Each row first gets a faint full-width track (`rgba(26,82,118,0.35)` at alpha 0.18), then the reach bar in its color at alpha 0.88 (min width 3px). Gate annotation 9px right of the track, in `#e74c3c` for the red row, otherwise `#666`.
- **Gridlines:** light `#eee` verticals at 0, 50, 100; axis end labels in `#888` 9.5px below the grid: "single caller" (left-aligned at 0) and "whole org" (right-aligned at 100); the 50 line is unlabeled.
- **Bottom caption (centered, italic 10px `#666`):** "Under-reach returns an empty list, not an error — the pipeline looks healthy while measuring a biased sample."
- Canvas redraws on window resize.

## Official API References

- [Webex for Developers docs](https://developer.webex.com/docs) — top-level documentation portal
- [Compliance](https://developer.webex.com/docs/compliance) — Compliance Officer role and org-wide message access

## Regeneration instructions

- **Layout:** single detail page: h1, `.subtitle` paragraph, `.verified` badge span, `h2` "Overview" with a full-width `.obj-table` (one `<tr>`; left `<td>` 45% text, right `<td>` 55% italic caption + canvas — no code block on this page), then `h2` "Official API References" with a plain `<ul>` of links. No nav bar, no back/home links.
- **Page CSS:** body system sans-serif (-apple-system stack), line-height 1.6, text `#2c3e50`, white background, padding 30px 40px; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; `.verified` badge — background `#eaf2f8`, border `1px solid #e0e0e0`, text `#1a5276`, padding 2px 10px, radius 4px, 0.8em; h2 1.3em `#1a5276` with `border-bottom: 2px solid #2980b9`; obj-table cells padding 16px, top-aligned (no cell borders); `.section-title` bold `#1a5276` 1.05em; li 0.93em; links `#1a5276`; right-column caption uses inline style `font-size:0.85em; color:#666; font-style:italic`.
- **Callout:** `.key-point` — background `#f8f9fa`, `border-left: 3px solid #e74c3c`, padding 10px 14px, 0.93em.
- **Canvas:** `display:block; width:100%`, `height` attribute 380; sized from `getBoundingClientRect().width`, scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates), redrawn on resize.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, track fill `rgba(26,82,118,0.35)`, gray text `#666`/`#888`.
- In regenerated HTML, any card/page links use `.html` extensions.
