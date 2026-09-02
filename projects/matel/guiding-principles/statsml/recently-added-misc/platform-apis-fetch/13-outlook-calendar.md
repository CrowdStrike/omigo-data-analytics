# Outlook Calendar

**Page type:** detail page (platform-API layout: h1 + subtitle + "Last verified" badge, one two-column obj-table row — text left 45%, example JSON + canvas right 55% — then an "Official API References" link list)
**HTML title tag:** Outlook Calendar — Platform APIs

**Subtitle:** Read meetings, invitee responses, and room availability in Microsoft 365 calendars through Microsoft Graph.

**Verified badge:** Last verified: August 2026

## Section: What you can get (left column)

- Meetings with organizer, invitees, responses, and online-meeting links
- Recurring meetings expanded into individual occurrences over a date window
- Busy/free availability and working hours for a set of people
- Meeting-room directories with capacity and location details
- Suggested meeting times that fit everyone's availability

### Key-point callout

**The plain event listing does not expand recurring meetings.** A weekly standup running for two years appears exactly once, so meeting-hours and room-utilisation numbers built on it are wrong by an order of magnitude. Use the calendar-view query over a date range, which expands each series into its occurrences. The failure is silent — the totals look plausible.

## Section: Watch out for (left column)

- An outside person's calendar that looks empty or free is usually a permissions result, not an empty calendar
- RSVP data is unreliable for external invitees, and a room's "accepted" only means a booking policy said yes — nobody's actual attendance is recorded anywhere
- The organizer's copy and each invitee's copy of a meeting are separate objects that can disagree; mixing them double-counts
- Deleted occurrences vanish without a trace, so last week's calendar cannot be reconstructed after the fact

## Section: Example: availability lookup for an external contact (right column)

Code block (`pre`, JSON):

```json
{
  "scheduleId": "p@partner.example",
  "availabilityView": "000000000000000000",
  "scheduleItems": [],
  "error": {
    "message": "Unable to retrieve free/busy information.",
    "responseCode": "unknown"
  }
}
```

Caption (italic, gray, 0.85em): All zeros ("free all day") next to an error object means "not permitted to look", not "free all day".

### Visualization (canvas `calEndpointChart`, responsive width × 380)

Capability matrix (grid of glyphs): which calendar endpoint answers which question. Values: 2 = yes (filled circle), 1 = partial/policy-dependent (half-filled circle with outline), 0 = no (X mark).

- **Title (bold 14px `#1a5276`, top center):** "Which calendar endpoint answers which question"
- **Subtitle (italic 10px `#666`):** "green = yes, orange = partial or policy-dependent, red = no"
- **Columns (capabilities, headers wrapped to two 9.5px `#2c3e50` lines above the grid):** "expands recurrence", "returns subject + body", "attendee response status", "cross-tenant free/busy", "suggests new slots", "room capacity metadata"
- **Rows (endpoints, right-aligned bold 10.5px monospace `#1a5276` labels; values in column order):**
  - `/events` — [0, 2, 2, 0, 0, 0]
  - `/calendarView` — [2, 2, 2, 0, 0, 0]
  - `/events/{id}/instances` — [2, 2, 2, 0, 0, 0]
  - `/getSchedule` — [2, 1, 0, 1, 0, 0]
  - `findMeetingTimes` — [2, 0, 1, 1, 2, 1]
  - `/places` — [0, 0, 0, 0, 0, 2]
- **Glyph colors by value:** 2 → `#27ae60` filled 8px-radius circle; 1 → `#e67e22` half-filled circle (right half filled, full outline, 1.5px stroke); 0 → `#e74c3c` X (two 1.5px diagonal strokes, 10px span).
- **Geometry:** label column min(150, 30% of width); grid starts at y=118; row height 34px; column width = chart width / 6; `#eee` gridlines between columns and rows.
- **Column highlight:** the first column ("expands recurrence") gets a `rgba(26,82,118,0.35)` fill at 0.35 globalAlpha behind the glyphs — it is the trap column.
- **Footer annotations (centered below grid):** italic 10.5px `#e74c3c`: "Column 1 is the trap: only /events fails it, and /events is the endpoint people reach for first."; italic 10px `#666`: "No single endpoint fills a row. Every real calendar pipeline joins at least three of them."
- Redraws on window resize; backing store scaled by devicePixelRatio; fixed 380px CSS height.

## Section: Official API References

- [Outlook Calendar API Overview](https://learn.microsoft.com/en-us/graph/outlook-calendar-concept-overview) — Microsoft Graph calendar surface concepts
- [List calendarView](https://learn.microsoft.com/en-us/graph/api/user-list-calendarview) — server-expanded occurrences over a bounded range

## Regeneration instructions

- **Layout:** platform-APIs detail page. Body: h1, `.subtitle` paragraph, `.verified` badge span, one `table.obj-table` with a single `<tr>`; left `<td>` (45%) holds `.section-title` headings ("What you can get", "Watch out for") with `<ul>` lists and one `.key-point` callout between them; right `<td>` (55%) holds a `.section-title` example heading, a `<pre>` JSON block with an inline-styled italic gray caption `<p>`, and the canvas. After the table: `<h2>Official API References</h2>` with a `<ul>` of external links.
- **Page CSS:** body system sans-serif (-apple-system stack), line-height 1.6, text `#2c3e50`, padding 30px 40px, white background. h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; `.verified` badge — background `#eaf2f8`, border `1px solid #e0e0e0`, text `#1a5276`, padding 2px 10px, radius 4px, 0.8em. h2 1.3em `#1a5276` with `border-bottom: 2px solid #2980b9`. `table.obj-table` full width, collapsed borders, td padding 16px, first td 45% / last td 55%. `pre` background `#f4f4f4`, padding 14px, radius 6px, 0.82em. `.key-point` background `#f8f9fa`, `border-left: 3px solid #e74c3c`, padding 10px 14px, 0.93em. `.section-title` bold `#1a5276` 1.05em. `li` 0.93em. Links `#1a5276`. Endpoint row labels use `ui-monospace, Menlo, monospace`. No nav bar, no back/home links.
- **Canvas:** `<canvas id="calEndpointChart" height="380">`, CSS `display:block; width:100%`; script sizes backing store to cell width × 380 times `window.devicePixelRatio`, `ctx.scale` back to logical coordinates, redraws on resize.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, highlight fill `rgba(26,82,118,0.35)`, grays `#666`/`#eee`.
- In regenerated HTML, any card/grid links pointing to this page use the `.html` extension.
