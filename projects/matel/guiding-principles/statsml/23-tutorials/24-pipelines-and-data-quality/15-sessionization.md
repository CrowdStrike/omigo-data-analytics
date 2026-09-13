# Sessionization

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Sessionization

**Subtitle:** Cutting one user's stream of clicks into separate visits — a new session starts wherever the gap between two events exceeds an inactivity timeout, classically 30 minutes

## One Shopper, Fourteen Clicks, How Many Visits?

**Tags:** `core idea` (blue), `event streams` (green), `30-minute rule` (orange)

- **The stream** — a shopper's day on a retail site logs 14 events, from 08:12 to 22:03
- **The morning** — 08:12 home, 08:14 search, 08:19 product page, 08:23 add to cart, then silence
- **The lunch check** — 12:41 open cart, 12:44 product page, 12:52 read reviews, then silence again
- **The evening buy** — seven events from 21:05 to 22:03, ending in a checkout
- **The cut** — wherever two consecutive events are more than 30 minutes apart, a new visit begins

*Example (italic):* The 4h 18m silence after 08:23 and the 8h 13m silence after 12:52 cut the day into three visits: 4, 3, and 7 events.

**Key point:** A session is a run of one user's events in which no gap between consecutive events exceeds the inactivity timeout — the log stores clicks, and sessionization invents the "visit".

### Visualization (canvas `c1`, 720×300)

Single horizontal timeline of the shopper's day: 14 event dots, three shaded session bands, and the two long gaps labeled.

- **Title (bold 15px, `#1a5276`, top center):** "One Day, 14 Events — the 30-Minute Rule Cuts 3 Sessions".
- **Axis:** horizontal 2px `#999` timeline at y=170, from x=60 to x=660, spanning 08:00 to 22:30 (870 minutes, linear); 12px `#444` tick labels at 08:00 / 12:00 / 16:00 / 20:00.
- **Event dots:** 6px radius `#2a78d6` circles at minutes-after-08:00 `[12, 14, 19, 23, 281, 284, 292, 785, 789, 798, 827, 832, 838, 843]` (08:12–08:23, 12:41–12:52, 21:05–22:03).
- **Session bands:** three rounded rectangles behind the dots (y=150 to y=190), fill `rgba(0,131,0,0.12)` with 2px `#008300` border, spanning 08:12–08:23, 12:41–12:52, 21:05–22:03; bold 12px `#008300` labels above each band: "session 1 — 4 events", "session 2 — 3 events", "session 3 — 7 events".
- **Gap labels (bold 12px `#d95926`, centered above the timeline in each silence):** "gap 4h 18m" between bands 1–2, "gap 8h 13m" between bands 2–3, each with a thin dashed `#d95926` horizontal bracket.
- **Annotation (bold 13px `#4a3aa7`, near top left under title):** "any gap > 30 min starts a new session".
- **Caption (12px `#444`, bottom right):** "timestamps illustrative".

## Hand-Cutting the Stream: 13 Gaps Against a 30-Minute Ruler

**Tags:** `worked example` (blue), `do it by hand` (green)

- **The gaps** — 14 events give 13 gaps, in minutes: 2, 5, 4, 258, 3, 8, 493, 4, 9, 29, 5, 6, 5
- **The rule** — cut where gap > 30 min: only 258 and 493 qualify, so 2 cuts make 3 sessions
- **Hand-check** — sessions = cuts + 1; here 2 + 1 = 3, holding 4, 3, and 7 events
- **Session length** — last event minus first: 11 min, 11 min, and 58 min for the three sessions
- **The near miss** — the 29-minute gap at 21:18→21:47 survives by a single minute

*Example (italic):* With the 30-minute ruler the evening visit stays whole at 58 minutes; a 25-minute ruler would cut it in two at the 29-minute gap.

**Key point:** Sessionization is one pass over sorted timestamps per user — compare each gap to the timeout, count the cuts — so anyone can redo the split by hand and audit a pipeline's session table.

### Visualization (canvas `c2`, 720×300)

Bar chart of the 13 inter-event gaps with a dashed line at the 30-minute timeout; the two gaps above the line are the session cuts.

- **Title (bold 15px, `#1a5276`, top center):** "13 Gaps, One Threshold: Only Two Gaps Exceed 30 Minutes".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = gap minutes 0 to 60 with gridlines `#e5e9ef` at 15/30/45 and 12px `#444` labels; x = gap index 1–13, 12px `#444` labels under each bar.
- **Bars:** 13 bars, 32px wide, 14px spacing, heights from the literal array `[2, 5, 4, 258, 3, 8, 493, 4, 9, 29, 5, 6, 5]`; gaps ≤ 30 filled `rgba(42,120,214,0.35)` with 11px `#444` value labels on top.
- **Overflow bars:** gaps 258 and 493 exceed the y-scale — draw them full plot height in solid `#d95926` with a small break mark (two white diagonal slashes) near the top and bold 12px `#d95926` labels "258" and "493" above.
- **Threshold line:** dashed `#e74c3c` (dash 6/4) horizontal line at y for 30 min, bold 12px `#e74c3c` label "timeout = 30 min" at its right end.
- **Near-miss highlight:** the 29-minute bar filled `#c98500` with bold 12px `#c98500` callout above it: "29 min — survives by 1 minute".
- **Caption (12px `#444`, bottom right):** "gap minutes illustrative".

## Move the Timeout, Move Every Metric

**Tags:** `where it's used` (blue), `metric sensitivity` (orange)

- **The knob** — the 30-minute timeout is an old web-analytics convention, not a law of nature
- **Same day, new counts** — this one stream yields 7, 4, 4, 3, 3, or 2 sessions as the timeout moves
- **Events per session** — the mirror image: 2.0 events at a 5-min timeout, 7.0 at a 300-min timeout
- **Dashboard trap** — "sessions per user rose 30%" can be a timeout change, not a behavior change
- **Tool mismatch** — two teams with different defaults will never reconcile their session counts

*Example (italic):* At a 10-minute timeout the 29-minute evening gap becomes a cut, so the same 14 clicks report 4 sessions instead of 3 — a 33% jump with zero change in behavior.

**Key point:** Session count, session length, and events per session are all functions of the timeout — always report the timeout next to the metric, and never compare session numbers computed under different timeouts.

### Visualization (canvas `c3`, 720×300)

Bar chart of session count versus timeout choice for the same 14-event day, with events-per-session values labeled above each bar.

- **Title (bold 15px, `#1a5276`, top center):** "Same 14 Events: the Timeout Alone Sets the Session Count".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = sessions 0 to 8, gridlines `#e5e9ef` at 2/4/6 with 12px `#444` labels; x = six bars labeled "5 min", "10 min", "15 min", "30 min", "60 min", "300 min" (12px `#444`).
- **Bars:** 60px wide, ~36px spacing, heights from `[7, 4, 4, 3, 3, 2]` sessions, fill `rgba(42,120,214,0.35)` with 2px `#2a78d6` border; the "30 min" bar filled solid `#2a78d6` with bold 12px `#2a78d6` label "the classic default" above its column.
- **Events-per-session labels (bold 12px `#008300`, above each bar):** "2.0", "3.5", "3.5", "4.7", "4.7", "7.0" events/session (14 events divided by the bar's session count).
- **Annotation (bold 13px `#d55181`, upper right of plot):** "7 sessions or 2 — same shopper, same clicks".
- **Caption (12px `#444`, bottom right):** "counts derived from the 13 illustrative gaps; events/session = 14 ÷ sessions".

## The Midnight Cut That Invents a Session

**Tags:** `common mistake` (red), `day boundaries` (orange), `edge cases` (green)

- **The night visit** — the same shopper another evening: 6 events at 23:38, 23:44, 23:57, 00:04, 00:11, 00:16
- **The truth** — every gap is ≤ 13 minutes, so this is one 38-minute session crossing midnight
- **The daily job** — a pipeline that processes each day's file alone clips the visit at 00:00
- **The damage** — one visit becomes two sessions (3 events Friday, 3 Saturday), counts up, lengths down
- **Timezone twist** — cutting at UTC midnight moves the split to mid-evening for many local users
- **Lone events** — one-event sessions get length 0 by last-minus-first; report them separately

*Example (italic):* The daily job reports two sessions of 19 and 12 minutes where the shopper really made one 38-minute visit — night-owl users inflate the count every single day.

**Common mistake:** Sessionizing inside daily partitions without stitching across midnight — session counts inflate, session lengths shrink, and the bias lands hardest on late-night users and on timezones far from the partition clock.

### Visualization (canvas `c4`, 720×300)

Two-row timeline comparing the true cross-midnight session with what a day-partitioned job reports, split at a midnight line.

- **Title (bold 15px, `#1a5276`, top center):** "One 38-Minute Visit, Cut in Two by the Daily Partition".
- **Shared axis:** horizontal 2px `#999` timeline at y=250, x=60 to x=660 spanning 23:30 to 00:20 (50 minutes, linear); 12px `#444` tick labels at 23:30 / 23:45 / 00:00 / 00:15.
- **Midnight line:** vertical dashed `#e74c3c` (dash 6/4) line at the 00:00 position from y=55 to y=250, bold 12px `#e74c3c` label "00:00 partition cut" at its top.
- **Event dots (both rows):** 6px radius `#2a78d6` circles at minutes-after-23:30 `[8, 14, 27, 34, 41, 46]` (23:38, 23:44, 23:57, 00:04, 00:11, 00:16).
- **Row 1 (band y=90 to y=130), 12px `#444` label "stitched (true)" at x=20:** one rounded band fill `rgba(0,131,0,0.12)`, 2px `#008300` border, spanning 23:38–00:16; bold 12px `#008300` label "1 session — 38 min, 6 events" above it.
- **Row 2 (band y=180 to y=220), label "daily job":** two rounded bands fill `rgba(231,76,60,0.12)`, 2px `#e74c3c` border, spanning 23:38–23:57 and 00:04–00:16; bold 12px `#e74c3c` labels "session A — 19 min, 3 events" and "session B — 12 min, 3 events" above them.
- **Annotation (bold 13px `#d95926`, centered near y=270):** "same clicks: +1 session, −7 minutes of measured visit time".
- **Caption (12px `#444`, bottom right):** "timestamps illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the shopper's timestamps and gaps are invented and labeled illustrative; derived numbers are exact given those gaps — sessions per timeout `[7, 4, 4, 3, 3, 2]` come from counting gaps strictly greater than each timeout, and events/session is 14 divided by the session count.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
