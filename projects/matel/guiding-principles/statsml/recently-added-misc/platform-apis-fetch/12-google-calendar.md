# Google Calendar

**Page type:** detail page (platform-API layout: h1 + subtitle + "Last verified" badge, one two-column obj-table row — text left 45%, example JSON + canvas right 55% — then an "Official API References" link list)
**HTML title tag:** Google Calendar — Platform APIs

**Subtitle:** Read and manage calendar events, invitees, and busy/free availability in Google Calendar.

**Verified badge:** Last verified: August 2026

## Section: What you can get (left column)

- Events with times, titles, locations, and video-call links
- The invitee list for each meeting, with each person's RSVP
- Recurring meetings, expandable into their individual occurrences
- Busy/free time blocks for scheduling, without revealing event details
- Change notifications and "only what changed" sync

### Key-point callout

**Recurring meetings come back as one row unless you ask for expansion.** A weekly standup running all year is returned as a single event, so meeting-load numbers built on the default listing come out dramatically low while still looking plausible. Since recurring meetings are the bulk of most calendars, this is the single most common error in calendar analytics.

## Section: Watch out for (left column)

- RSVP is not attendance — "accepted" can be automatic, and outside invitees often stay "no reply" forever
- What you see of other people's calendars depends on their sharing settings; an unreadable calendar can look free all day
- All-day events end on an exclusive date — naive duration math adds a day to every one
- Sync tokens expire, forcing a full re-read of the calendar

## Section: Example: busy/free lookup for two people (right column)

Code block (`pre`, JSON):

```json
{
  "calendars": {
    "dan@example.com": {
      "busy": [
        { "start": "2026-08-24T09:00:00Z", "end": "2026-08-24T09:25:00Z" },
        { "start": "2026-08-24T13:00:00Z", "end": "2026-08-24T14:00:00Z" }
      ]
    },
    "external.consultant@othercorp.com": {
      "busy": [],
      "errors": [ { "domain": "global", "reason": "notFound" } ]
    }
  }
}
```

Caption (gray, 0.88em): The second calendar is not free — it is unreadable. An empty busy list next to an error must never be scored as availability.

### Visualization (canvas `endpointMatrix`, responsive width × 380)

Grouped horizontal bar matrix: which Calendar endpoint returns which property. Values: 2 = yes (full bar), 1 = partial/conditional (half bar), 0 = no (3px red stub).

- **Title (bold 13px `#1a5276`, top center):** "What each Calendar endpoint actually returns"
- **Subtitle (italic 10px `#888`):** "full bar = yes  /  half bar = conditional  /  stub = no"
- **Properties (row groups, right-aligned bold 10px `#2c3e50` labels):** "Expands recurrence", "Returns event title", "Returns attendee list", "Returns responseStatus", "Cross-domain visibility", "Tells you what changed"
- **Endpoints (one thin bar per endpoint within each group; name / color / values in property order):**
  - `events.list (default)` — `#e67e22` — [0, 2, 2, 2, 1, 0]
  - `events.list singleEvents=true` — `#1a5276` — [2, 2, 2, 2, 1, 0]
  - `events.instances` — `#27ae60` — [2, 2, 2, 2, 1, 0]
  - `freebusy.query` — `#8e44ad` — [2, 0, 0, 0, 2, 0]
  - `events.watch notification` — `#e74c3c` — [0, 0, 0, 0, 0, 1]
- **Geometry:** label column min(196, 38% of width); chart area to its right; bar width = (value/2) × chart width, bar height min(7, computed from group height), 2.2px spacing; value 0 drawn as a 3px-wide `#e74c3c` stub. Half bars (value 1) get an 8px `#666` annotation to their right: "ACL / re-fetch dependent".
- **Gridlines:** vertical `#eee` lines at 0, 1/2, and full chart width; horizontal `#eee` separators between property groups; `#888` baseline under the last group.
- **Legend (bottom, wrapping):** 11×8px color swatch + endpoint name in 9.5px `#2c3e50` for each of the five endpoints.
- **Footer annotation (italic 9.5px `#e74c3c`, left-aligned at chart x):** "No endpoint returns attendance. responseStatus is an RSVP; Meet join data lives in the Admin SDK Reports API."
- Redraws on window resize; backing store scaled by devicePixelRatio with `setTransform` reset then `ctx.scale` back to logical coordinates; fixed 380px CSS height.

## Section: Official API References

- [Google Calendar API Documentation](https://developers.google.com/calendar/api) — API home with guides and concepts
- [Calendar API v3 Reference](https://developers.google.com/calendar/api/v3/reference) — events, calendarList, freebusy, settings resources

## Regeneration instructions

- **Layout:** platform-APIs detail page. Body: h1, `.subtitle` paragraph, `.verified` badge span, one `table.obj-table` with a single `<tr>`; left `<td>` (45%) holds `.section-title` headings ("What you can get", "Watch out for") with `<ul>` lists and one `.key-point` callout between them; right `<td>` (55%) holds a `.section-title` example heading, a `<pre>` JSON block with an inline-styled gray caption `<p>`, and the canvas. After the table: `<h2>Official API References</h2>` with a `<ul>` of external links.
- **Page CSS:** body system sans-serif (-apple-system stack), line-height 1.6, text `#2c3e50`, padding 30px 40px, white background. h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; `.verified` badge — background `#eaf2f8`, border `1px solid #e0e0e0`, text `#1a5276`, padding 2px 10px, radius 4px, 0.8em. h2 1.3em `#1a5276` with `border-bottom: 2px solid #2980b9`. `table.obj-table` full width, collapsed borders, td padding 16px, first td 45% / last td 55%. `pre` background `#f4f4f4`, padding 14px, radius 6px, 0.82em. `.key-point` background `#f8f9fa`, `border-left: 3px solid #e74c3c`, padding 10px 14px, 0.93em. `.section-title` bold `#1a5276` 1.05em. `li` 0.93em. Links `#1a5276`. No nav bar, no back/home links.
- **Canvas:** `<canvas id="endpointMatrix" height="380">`, CSS `display:block; width:100%`; script sizes backing store to cell width × 380 times `window.devicePixelRatio` and redraws on resize.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, grays `#666`/`#888`/`#eee`.
- In regenerated HTML, any card/grid links pointing to this page use the `.html` extension.
