# Polling vs Push

**Page type:** detail page (tutorial card-sections: h2 per section, two-column table.layout with text left 45% / canvas right 55%; one section uses a 3-col layout3 38/31/31 with two canvases)
**HTML title tag:** Polling vs Push

**Subtitle:** Two ways to learn something happened: keep asking "anything new?" on a timer, or have the system tell you the moment it happens

## Two Ways to Hear About a New Order

Tags: `core idea` (blue pill), `running example` (green pill)

- **The event** — an order lands at 10:04:23
- **Polling** — your script asks "any new orders?" every 60 s: at 10:04:00, 10:05:00, …
- **Polling result** — the 10:04:00 check saw nothing; you learn at 10:05:00, 37 s late
- **Push** — the order system sends you a message; you learn at 10:04:23, right away
- **Who does the work** — polling: the asker keeps checking; push: the source announces

*Example (italic):* Polling is refreshing your inbox by hand; push is the new-mail notification.

**Key point:** With polling you find out at the next check, so news is up to one full interval stale; with push you find out when it happens.

### Visualization (canvas `c1`, 720×300)

Two horizontal timelines: polling vs push for the 10:04:23 order.

- **Title (bold 15px ink `#1a5276`, top center):** "Order Lands at 10:04:23 — When Do You Find Out?"
- **Time axis:** 10:03:00 (0 s) to 10:06:00 (180 s), padL=120; light-gray vertical gridlines with gray tick labels "10:03", "10:04", "10:05", "10:06" at 0/60/120/180 s.
- **Two lanes** (thin gray horizontal lines, bold colored right-aligned labels): "Polling" (orange `#d95926`) at y=110; "Push" (green `#008300`) at y=205 (drawn at 210 for the lane line).
- **Order marker:** magenta `#d55181` dashed vertical line (dash 5/4) at 83 s, bold magenta 12px label above: "order lands 10:04:23".
- **Polling lane:** 6px dots at each 60 s check; checks before the order (10:03, 10:04) drawn gray with 12px label "check: nothing"; the 10:05 check (120 s) drawn orange labeled "found it". Orange staleness bracket from 83 s to 120 s below the lane with bold orange 13px label "37 s stale".
- **Push lane:** green 6px dot right at the event (~84 s), bold green 12px label "message delivered 10:04:23 — you already know" and bold green 13px "~0 s stale".

## Counting the Cost of One Quiet Day

Tags: `worked example` (green pill), `trade-off` (orange pill)

Layout note: this section uses the 3-col `table.layout3` — text column 38%, two viz columns 31% each (canvases `c2a` and `c2b`).

- **The shop** — 30 orders arrive over a day, spread across 24 hours
- **Polling every 60 s** — 60 × 24 = 1,440 checks in the day
- **Empty checks** — at most 30 checks find news, so ≥1,410 checks (~98%) find nothing
- **Staleness** — each order waits for the next tick: ~30 s late on average, up to 60 s
- **Push instead** — exactly 30 messages, one per order, each arriving in under a second

*Example (italic):* 1,440 questions to learn 30 facts, each up to a minute old — or 30 messages with none of the waiting.

**Key point:** Polling cost scales with the clock (checks per day); push cost scales with reality (events per day) — and push is fresher anyway.

### Visualization (canvas `c2a`, 420×300)

Three-bar chart: checks made vs checks with news.

- **Title (bold 14px ink, top center):** "One Day of Asking: 1,440 Checks"
- **Bars** (width 84, baseline y=232 with thin gray line, chart height 168, scale max 1,500, min bar height 4px; bold colored value above each bar, two-line dark 12px label below):

| x | label | value | color |
|---|-------|-------|-------|
| 60 | polling checks | 1,440 | orange `#d95926` |
| 170 | checks with news | 30 | aqua `#199e70` |
| 280 | push messages | 30 | green `#008300` |

- **Bottom annotation (bold magenta 12px, centered, y=285):** "~98% of checks found nothing"

### Visualization (canvas `c2b`, 400×300)

Three-bar chart: how late you learn about each order.

- **Title (bold 14px ink, top center):** "How Late Is the News?"
- **Bars** (width 80, baseline y=232, chart height 168, scale max 65 s, min bar height 4px):

| x | label | value | value label | color |
|---|-------|-------|-------------|-------|
| 55 | poll: worst case | 60 | 60 s | orange `#d95926` |
| 165 | poll: average | 30 | 30 s | yellow `#c98500` |
| 275 | push | 1 | <1 s | green `#008300` |

- **Bottom annotation (bold green 12px, centered, y=285):** "push: fresh AND cheaper here"

## Where a Data Scientist Meets This Every Day

Tags: `why it matters` (blue pill), `where it's used` (green pill)

- **Hourly cron ETL** — polling: the 09:00 job picks up an 08:01 order 59 minutes later
- **Streaming / CDC / webhooks** — push: the same order reaches the pipeline in seconds
- **Your dashboard** — "as of when?" is set by this choice, not by the chart library
- **Your features** — a fraud model scoring on hour-old data misses the fraud happening now
- **Pick by need** — daily report: hourly polling is fine; fraud alerts: only push is fine

*Example (italic):* "The numbers look wrong" at 09:30 is often just an 09:00 polling job being honest about its schedule.

**Key point:** Before debugging a "stale" metric, ask how the data moves: on a timer (polling) or on arrival (push) — the staleness is usually the design, not a bug.

### Visualization (canvas `c3`, 720×300)

Two-lane timeline: hourly batch vs streaming — when an 08:01 order reaches the dashboard.

- **Title (bold 15px ink, top center):** "An 08:01 Order, Two Pipelines: Cron ETL vs Streaming"
- **Time axis:** 08:00 (0 min) to 09:30 (90 min), padL=150; light-gray vertical gridlines labeled "08:00", "08:30", "09:00", "09:30".
- **Lanes:** "Hourly cron (poll)" (orange) at y=105; "Streaming (push)" (green) at y=205.
- **Order marker:** magenta dashed vertical line at 08:01 with bold magenta 12px label "order at 08:01".
- **Cron lane:** orange 6px dot at 09:00 labeled bold 12px above "09:00 job runs — order finally loaded"; translucent orange waiting bar `rgba(217,89,38,0.25)` from 08:01 to 09:00 (16px tall) with bold orange 13px label below "waits 59 minutes".
- **Streaming lane:** green 6px dot at ~08:01.5, bold green 12px "in the pipeline within seconds" and bold green 13px "dashboard already fresh at 08:02".
- **Bottom annotation (gray 12px, centered, y=288):** "same order, same shop — the freshness gap is the pipeline design"

## The Common Confusion: Fast Polling Is Still Polling

Tags: `common mistake` (red pill), `trade-off` (orange pill)

- **The temptation** — "just poll every second, it'll feel like push"
- **The bill** — 60 s → 1,440 checks; 10 s → 8,640; 1 s → 86,400 — for the same 30 orders
- **Still stale** — 1 s polling is still up to 1 s late; the lag shrinks, never disappears
- **Push isn't free either** — a missed message is gone; polling self-heals at the next check
- **Common compromise** — push for speed, plus a slow poll as a safety net to catch misses

*Example (italic):* Shortening the interval walks the cost curve up a cliff while freshness crawls toward what push gives you at 30 messages a day.

**Key point:** Faster polling buys freshness with load — it approaches push but never reaches it, because asking on a timer can't know when things happen.

### Visualization (canvas `c4`, 720×300)

Log-scale line chart: checks per day vs polling interval, push as a reference line.

- **Title (bold 15px ink, top center):** "Polling Faster: The Cost Cliff on the Way to \"Feels Like Push\""
- **Axes:** x categorical polling intervals (evenly spaced), axis caption "polling interval"; y log10 scale from 10 to 100,000 with gridlines/labels at 10, 100, 1k, 10k, 100k; rotated gray y-axis label "checks or messages per day (log scale)". Padding: top 54, bottom 60, left 90, right 40.
- **Polling line** (orange `#d95926`, width 3, 5px dots, bold 12px value labels above points):

| interval | checks/day |
|----------|-----------|
| 60 s | 1,440 |
| 30 s | 2,880 |
| 10 s | 8,640 |
| 5 s | 17,280 |
| 1 s | 86,400 |

- **Push reference:** green dashed horizontal line (dash 6/4) at 30/day, bold green 13px label above-left: "push: 30 messages/day, ~0 s stale".
- **Annotation (bold magenta 13px, centered near the last point, below it):** "86,400 checks and still up to 1 s late"

## Regeneration instructions

- **Template/layout:** tutorials detail page. `<h1>` + `.subtitle`, then four `.card-section` blocks each with `<h2>` (1.3rem `#1a5276`, 2px solid `#2980b9` bottom border). Sections 1, 3, 4 use `table.layout` (`td.text-col` 45% / `td.viz-col` 55%); section 2 uses `table.layout3` (`td.text-col` 38%, two `td.viz-col` 31%). All tables width 100%, border-collapse, td padding 12px, vertical-align top.
- **Text column structure:** `.tags` pill row first (pills 0.72rem bold, 2px 10px padding, 10px radius: blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`); then a `<ul>` (0.92rem) of one-line bullets each opening with `<b>` in `#1a5276`; one italic `.example` line (0.9rem `#555`); one `.key-point` callout (background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem).
- **Page CSS:** body system-ui sans, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem. Canvases have `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Chart palette (JS object P):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- **Canvas:** `c1`, `c3`, `c4` are 720×300; `c2a` is 420×300 and `c2b` is 400×300. Scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id, w, h)` helper that defaults to 720×300. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- In regenerated HTML, any card/page links use `.html` extensions.
