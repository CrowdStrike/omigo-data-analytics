# PagerDuty & On-Call

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** PagerDuty & On-Call

**Subtitle:** On-call is a designed system, not a heroic habit — alerts route to whoever holds the rotation, unanswered pages climb an escalation chain, and every step is timestamped

## The 2am Alert Knows Who to Wake

**Tags:** `core idea` (blue), `alert routing` (green), `PagerDuty` (orange)

- **The alert** — at 2:14am a monitor sees database query latency cross its threshold and fires
- **The service** — the alert lands on the "orders-db" service, PagerDuty's unit of ownership
- **The policy** — the service's escalation policy says who gets paged first and what happens if they don't answer
- **The schedule** — the policy points at an on-call schedule: a rotation that names one responder per week
- **The page** — this week that name is Priya, so her phone — not a mailing list — buzzes at 2:14am

*Example (italic):* Nobody decides who to call at 2am; the rotation decided a month ago, and the alert simply follows the route to Priya.

**Key point:** Routing turns "who do we call?" into configuration — monitor to service, service to escalation policy, policy to schedule, schedule to exactly one human.

### Visualization (canvas `c1`, 720×300)

Flow diagram of one alert's route through the routing chain, with the weekly rotation strip below showing why the page lands on Priya.

- **Title (bold 15px, `#1a5276`, top center):** "One Alert, One Route: Monitor → Service → Policy → Schedule → Human".
- **Top row (box centers at y=110):** five rounded boxes left to right at x = 15, 160, 305, 450, 595, each 115px wide, 46px tall, 8px radius, 12px `#2c3e50` text, joined by 3px `#6b7280` arrows: "db latency alert 2:14am" (fill `rgba(231,76,60,0.12)`, border `#e74c3c`), "service: orders-db" (fill `rgba(42,120,214,0.15)`, border `#2a78d6`), "escalation policy" (fill `rgba(42,120,214,0.15)`, border `#2a78d6`), "on-call schedule" (fill `rgba(42,120,214,0.15)`, border `#2a78d6`), "Priya's phone" (fill `rgba(0,131,0,0.12)`, border `#008300`).
- **Rotation strip (y=195, 34px tall):** three adjacent week blocks each 200px wide starting at x=60, 1px `#6b7280` borders, 12px `#444` labels: "Aug 17–23: Sam", "Aug 24–30: Priya" (fill `rgba(0,131,0,0.15)`, bold label), "Aug 31–Sep 6: Ken"; dashed `#6b7280` (dash 4/3) vertical line from the schedule box down into the middle block.
- **Annotation (bold 13px green `#008300`, below the middle block at y=258):** "2:14am on Aug 26 falls in Priya's week".
- **Caption (12px `#444`, bottom right):** "names, times, and rotation illustrative".

## Five Minutes to Acknowledge, Then the Chain Climbs

**Tags:** `worked example` (blue), `escalation` (green)

- **Level 1** — 2:14am: PagerDuty pages Priya (primary on-call) and starts a 5-minute ack timer
- **No answer** — Priya sleeps through it; at 2:19am the timer expires and the page escalates
- **Level 2** — 2:19am: Marco (secondary on-call) is paged; he acknowledges at 2:21am
- **Chain stops** — the acknowledgment halts escalation; level 3 (the manager) is never paged
- **Resolution** — Marco restarts the stuck query pool and resolves the incident at 2:52am
- **The record** — fired 2:14, acked 2:21, resolved 2:52: time-to-ack 7 min, time-to-resolve 38 min

*Example (italic):* The alert needed two pages and 7 minutes to reach a human who answered — and the system logged every step without anyone taking notes.

**Key point:** Escalation makes silence an event: an unacknowledged page is not lost, it climbs to the next level on a timer until someone takes ownership.

### Visualization (canvas `c2`, 720×300)

Three-lane escalation timeline for the 2:14am incident: primary paged and silent, secondary paged and acknowledging, manager never reached.

- **Title (bold 15px, `#1a5276`, top center):** "2:14am Incident: the Page Climbs Until Someone Answers".
- **Axes:** origin x=110, baseline y=245, plot width 560; x = clock time 2:14 to 2:24, 12px `#444` tick labels every 2 minutes ("2:14"…"2:24"), gridlines `#e5e9ef` at each tick.
- **Lanes (horizontal, 12px `#444` labels at x=15):** "L1 Priya" at y=90, "L2 Marco" at y=150, "L3 manager" at y=210.
- **Lane 1:** blue `#2a78d6` 8px dot at 2:14 labeled 12px "paged"; red `#e74c3c` 4px segment from 2:14 to 2:19 with bold 12px red label "no ack — 5-min timeout" above it.
- **Escalation arrow:** 3px `#d95926` arrow from (2:19, lane 1) down to (2:19, lane 2), bold 12px orange `#d95926` label "escalate".
- **Lane 2:** blue 8px dot at 2:19 labeled "paged"; green `#008300` 10px dot at 2:21 with bold 13px green label "acknowledged 2:21".
- **Lane 3:** dashed `#6b7280` (dash 4/3) 2px line across the lane, 12px `#6b7280` label "never paged — chain stopped at ack".
- **Annotation (bold 13px violet `#4a3aa7`, upper right near y=60):** "time to acknowledge: 7 min".
- **Caption (12px `#444`, bottom right):** "timestamps illustrative; resolved 2:52am (off scale)".

## The Timestamps Write the Postmortem

**Tags:** `where it's used` (blue), `MTTA / MTTR` (green)

- **Everything logged** — fired, paged, escalated, acked, resolved: each transition carries a timestamp
- **Two metrics** — time-to-acknowledge and time-to-resolve fall out of the log for free
- **The review** — the postmortem timeline is read from the incident record, not from memory
- **The pattern** — comparing incidents shows where time goes: waking someone vs fixing the thing
- **The outlier** — a 63-minute ack means the paging chain failed, whatever the fix time was

*Example (italic):* In last quarter's review, the failed-backup incident stood out instantly — 63 minutes just to get an acknowledgment, against 2 to 21 for every other incident.

**Key point:** Because the pager is a system, its history is data — slow acks point at the rotation and escalation design, slow fixes point at the runbooks and the software.

### Visualization (canvas `c3`, 720×300)

Horizontal stacked bar chart of four incidents: time-to-acknowledge (blue segment) then time from ack to resolve (green segment), in minutes.

- **Title (bold 15px, `#1a5276`, top center):** "Where the Minutes Went: Ack Time vs Fix Time, Four Incidents".
- **Axis:** horizontal 2px `#999` baseline at x=230, bars extend right, scale 4px per minute (max width 368 at 92 min); 11px `#666` scale note "4 px = 1 min" under the title.
- **Rows (bar centers at y = 70, 120, 170, 220), each with a left-aligned 12px `#444` label at x=20:**
  - "disk full — 41 min total": blue `#2a78d6` segment width 8 (ack 2 min), green `#008300` segment width 156 (fix 39 min)
  - "db latency (2am page) — 38 min": blue segment width 28 (ack 7 min), green segment width 124 (fix 31 min)
  - "cache errors — 55 min": blue segment width 84 (ack 21 min), green segment width 136 (fix 34 min)
  - "failed backup — 92 min": red `#e74c3c` segment width 252 (ack 63 min) with bold 12px red label "63 min to ack — paging failed", green segment width 116 (fix 29 min)
- **Bar style:** 16px tall segments, blue fill `rgba(42,120,214,0.85)`, green fill `rgba(0,131,0,0.75)`, 11px `#444` minute labels inside or at segment ends.
- **Legend (12px, upper right):** blue swatch "time to acknowledge", green swatch "ack to resolve".
- **Annotation (bold 13px magenta `#d55181`, bottom center near y=265):** "slow acks blame the pager design, not the responder".
- **Caption (12px `#444`, bottom right):** "minutes illustrative".

## When Every Page Cries Wolf

**Tags:** `common mistake` (red), `alert fatigue` (orange)

- **The drift** — every incident adds "one more alert, just in case", and the page volume creeps up
- **Non-actionable** — most new pages need no action: transient blips, duplicates, someone else's problem
- **The training** — responders learn the honest lesson that most pages are safe to ignore
- **The decay** — ack speed collapses as volume grows; the real page waits behind a wall of noise
- **The fix** — delete or downgrade non-actionable alerts; a page should mean "a human must act now"

*Example (italic):* At 38 pages a week the team acked 92% within 5 minutes; by 150 pages a week only 27% — the pager still worked, the humans had tuned it out.

**Common mistake:** Treating more alerts as more safety. Every non-actionable page trains responders to ignore the pager, so alert volume past what humans can triage makes outages longer, not shorter.

### Visualization (canvas `c4`, 720×300)

Dual-line chart over eight weeks: pages per week climbing (red, left axis) while the share acked within 5 minutes collapses (blue, right axis).

- **Title (bold 15px, `#1a5276`, top center):** "Alert Fatigue: More Pages, Slower Humans".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = weeks 1 to 8, 12px `#444` tick labels "wk 1"…"wk 8"; left y = pages per week 0 to 160, gridlines `#e5e9ef` at 40/80/120; right y (labels at x=670, 12px `#2a78d6`) = % acked within 5 min, 0 to 100.
- **Pages line:** red `#e74c3c` 3px line with 5px dots through weeks `[1, 2, 3, 4, 5, 6, 7, 8]`, pages `[38, 55, 74, 92, 110, 128, 141, 150]`.
- **Ack line:** blue `#2a78d6` 3px line with 5px dots, same weeks, percent acked within 5 min `[92, 88, 79, 66, 54, 41, 33, 27]` mapped to the right axis.
- **Labels:** bold 12px red "pages/week" near the red line at week 3; bold 12px blue "% acked in 5 min" near the blue line at week 3.
- **Annotation (bold 13px `#e74c3c`, right side near week 7, y=70):** "150 pages/week → 27% acked in 5 min".
- **Caption (12px `#444`, bottom right):** "weekly counts illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); names, clock times, incident minutes, and weekly page counts are invented and labeled illustrative; the routing chain (monitor → service → escalation policy → schedule → responder), timeout-driven escalation, and acknowledgment/resolution timestamping are publicly documented PagerDuty behavior. Time-to-ack 7 min and time-to-resolve 38 min follow exactly from the stated timestamps (2:14 → 2:21 → 2:52).
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
