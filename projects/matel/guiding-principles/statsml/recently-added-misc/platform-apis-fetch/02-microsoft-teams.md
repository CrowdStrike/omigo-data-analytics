# Microsoft Teams

**Page type:** detail page (two-column obj-table layout: text left 45%, code sample + canvas right 55%, one Overview row; second h2 section with reference links)
**HTML title tag:** Microsoft Teams — Platform APIs

**Subtitle:** Read Teams chats, channel messages, meeting attendance, call records and who is online — all through Microsoft's Graph API.

**Verified badge:** Last verified: August 2026

## Overview

### Left column

**What you can get**

- Channel and chat messages, with reactions and mentions
- Meeting details, plus who attended and for how long
- Meeting recordings and transcripts, where licensing allows
- Call records — connection quality and duration, not what was said
- Whether a user is available right now

**Key-point callout:** **Reading message content across the whole organisation needs Microsoft's explicit approval of your app** — not just an admin clicking consent. Bulk message access is a "protected API": Microsoft reviews the application, and usage is metered and billed. Treat the approval lead time as a project dependency, not a formality.

**Watch out for**

- Call records keep only about 30 days of history, and listing them requires a start-time filter — collect continuously if you need history
- "Who is online" is only right now — there is no history of past presence
- Deleted messages keep a placeholder but lose their text; edits leave no record of the original wording
- How much message history exists depends on each organisation's retention settings — identical code returns different depths in different tenants

### Right column

**Section title:** Two messages: one normal, one deleted

Code block (pre, JSON):

```
{
  "value": [
    {
      "id": "1755861242113",
      "createdDateTime": "2026-08-18T09:14:02Z",
      "from": { "user": { "displayName": "Amit Jaiswal" } },
      "body": { "content": "Rolling the sync window back to T-2h" },
      "reactions": [ { "reactionType": "like" } ]
    },
    {
      "id": "1755859001004",
      "deletedDateTime": "2026-08-18T10:02:11Z",
      "body": { "content": "" }
    }
  ]
}
// deleted: the row stays, the text is gone for good
```

### Visualization (canvas `teamsWindowChart`, responsive width × 380)

Horizontal bar chart: queryable history per Graph surface (bar length) with a colored gate stripe (top and bottom 4px edges of each bar) encoding what permission level unlocks it.

- **Data (label, value on 0–100 indicative scale, note, gate color):**
  - Channel messages — 100, "retention-policy governed", gate `#8e44ad`
  - Chat messages — 100, "retention-policy governed", gate `#8e44ad`
  - Meeting transcripts — 100, "licence + retention gated", gate `#e67e22`
  - Attendance reports — 100, "per meeting, if generated", gate `#e67e22`
  - PSTN call logs — 90, "date range capped per call", gate `#e67e22`
  - callRecords (CDR) — 30, "30 days, then gone", gate `#e67e22`
  - Presence — 0, "point-in-time only — no series", gate `#e74c3c`
  - Code comment key: GATE_PROTECTED purple = Microsoft app-review required (protected APIs); GATE_ADMIN orange = app-only permission + admin consent (or PowerShell policy); GATE_STANDARD green = ordinary delegated/app permission; GATE_NONE red = no history exists to query.
- **Title (centered, bold 14px `#1a5276`):** "Queryable history vs. what it takes to be allowed to read it"
- **Subtitle (centered, italic 10px `#666`):** "bar length is indicative, not a scale of days; the colour is the part that stops projects"
- **Layout:** label column min(150, 30% of width) right-aligned bold 11px `#2c3e50`; bars start at labelW+12, chart width extends to width−170; bar height 20, gap 16, first bar at y=60; bar body fill `rgba(26,82,118,0.35)` with 4px-thick gate-colored stripes along top and bottom edges; bars at max value get a dashed gate-colored ">" continuation chevron at the right end. Note text 10px `#666` left of nothing / right of each bar.
- **Gridlines:** light `#eee` verticals at 0, 30, 60, 90 on the 0–100 scale, tick labels "0d", "30d", "60d", "90d" in `#888` below the grid.
- **Legend (two columns below grid, 10px `#666` labels, 10×10 swatches):** `#8e44ad` "protected API — app review"; `#e67e22` "app-only + admin consent"; `#27ae60` "ordinary permission"; `#e74c3c` "no history exists".
- **Bottom caption (centered, italic 10px `#666`):** "Nothing here is retrievable retroactively: the red bar never had history, the 30-day bar loses it daily."
- Canvas redraws on window resize.

## Official API References

- [Teams API in Microsoft Graph](https://learn.microsoft.com/en-us/graph/teams-concept-overview) — the Teams-specific resource families and how they fit together
- [Protected APIs in Microsoft Teams](https://learn.microsoft.com/en-us/graph/teams-protected-apis) — the approval process gating message content at tenant scale

## Regeneration instructions

- **Layout:** single detail page: h1, `.subtitle` paragraph, `.verified` badge span, `h2` "Overview" with a full-width `.obj-table` (one `<tr>`; left `<td>` 45% text, right `<td>` 55% code + canvas), then `h2` "Official API References" with a plain `<ul>` of links. No nav bar, no back/home links.
- **Page CSS:** body system sans-serif (-apple-system stack), line-height 1.6, text `#2c3e50`, white background, padding 30px 40px; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; `.verified` badge — background `#eaf2f8`, border `1px solid #e0e0e0`, text `#1a5276`, padding 2px 10px, radius 4px, 0.8em; h2 1.3em `#1a5276` with `border-bottom: 2px solid #2980b9`; obj-table cells padding 16px, top-aligned (no cell borders on this page); `.section-title` bold `#1a5276` 1.05em; li 0.93em; links `#1a5276`.
- **Code block:** `pre` — background `#f4f4f4`, padding 14px, radius 6px, 0.82em, left-aligned, horizontal overflow scroll.
- **Callout:** `.key-point` — background `#f8f9fa`, `border-left: 3px solid #e74c3c`, padding 10px 14px, 0.93em.
- **Canvas:** `display:block; width:100%`, `height` attribute 380; sized from `getBoundingClientRect().width`, scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates), redrawn on resize.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, bar fill `rgba(26,82,118,0.35)`, gray text `#666`/`#888`.
- In regenerated HTML, any card/page links use `.html` extensions.
