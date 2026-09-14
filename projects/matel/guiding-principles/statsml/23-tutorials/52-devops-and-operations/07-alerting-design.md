# Alerting Design

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Alerting Design

**Subtitle:** Page a human only when users are hurting — alert on symptoms like errors and latency, not causes like high CPU, a rule from Google's SRE book

## The CPU Alert That Never Meant Anything

**Tags:** `core idea` (blue), `symptoms not causes` (green), `Google SRE` (orange)

- **The store** — an online store's checkout service, on-call rotation, a pager, and a month of alerts
- **The cause alerts** — "CPU > 80%" fired 31 times, "disk > 80%" 16, "memory > 90%" 12; users noticed nothing
- **The real outage** — day 22, a TLS certificate expired: a cause nobody had written an alert for
- **The symptom alert** — "checkout error rate > 1%" paged within 2 minutes, because users saw failures
- **The source** — Google's published Site Reliability Engineering book codified this: page on symptoms, not causes

*Example (italic):* Fifty-nine cause alerts in a month carried zero user impact; the one outage arrived through an unalerted cause and was caught only by the symptom page.

**Key point:** A symptom is something a customer would notice (errors, latency); a cause is a machine state that may or may not matter. Causes are endless and you can't enumerate them all — symptoms cover whatever cause comes next.

### Visualization (canvas `c1`, 720×300)

Horizontal bar chart: one month of alerts at the store — times fired vs times they meant real user impact.

- **Title (bold 15px, `#1a5276`, top center):** "One Month of Alerts: Fires vs Actual User Impact".
- **Axis:** horizontal 2px `#999` baseline at x=230, bars extend right, max width 430 mapped to 31 fires; 12px `#444` row labels left-aligned at x=20.
- **Rows (top to bottom at y = 70, 115, 160, 205):**
  - "CPU > 80% (cause)": blue `#2a78d6` bar for 31 fires (width 430), overlay green `#008300` bar width 0 with 11px `#444` label "0 outages"
  - "disk > 80% (cause)": blue bar for 16 fires (width 222), label "0 outages"
  - "memory > 90% (cause)": blue bar for 12 fires (width 166), label "0 outages"
  - "checkout error rate (symptom)": green `#008300` bar for 1 fire (width 14), bold 12px green label "1 fire — the real outage (cert expiry)"
- **Bar style:** 16px tall, cause bars fill `rgba(42,120,214,0.30)`, symptom bar solid green; 11px count labels at bar ends ("31", "16", "12", "1").
- **Annotation (bold 13px red `#e74c3c`, near y=250, centered):** "the outage came from a cause with no alert — the symptom caught it".
- **Caption (12px `#444`, bottom right):** "counts illustrative".

## Sorting One Week of Alerts: Page, Ticket, or Dashboard

**Tags:** `worked example` (blue), `alert taxonomy` (green)

- **The week** — one on-call week produces 12 alerts; the design question is what each one deserves
- **Page** — wake a human NOW: urgent, actionable, user-visible — checkout errors at 4%, p99 latency 8s
- **Ticket** — needs action this week, not tonight: disk fills in ~9 days, certificate expires in 20 days
- **Log/dashboard** — context only, no notification: one pod restart, a cache hit-rate dip, a nightly GC pause
- **Hand-check** — of the 12 alerts, only 2 pass the page test; 4 become tickets, 6 stay on the dashboard

*Example (italic):* "Disk fills in 9 days" at 3am is a ticket — waking someone buys nothing that a morning fix doesn't; "checkout errors at 4%" is a page because customers are failing right now.

**Key point:** Three tiers, one test — a page must be urgent, actionable, and user-visible all at once; anything failing one of the three drops a tier.

### Visualization (canvas `c2`, 720×300)

Three-column sorting diagram: the week's 12 alerts flowing into page / ticket / dashboard buckets, with the qualifying test above each column.

- **Title (bold 15px, `#1a5276`, top center):** "12 Alerts, One Week: Only 2 Deserve to Wake Anyone".
- **Columns (headers bold 13px at y=54, centered at x = 140, 360, 580):** red `#e74c3c` "PAGE — now", orange `#d95926` "TICKET — this week", mute `#6b7280` "DASHBOARD — context".
- **Test line (11px `#6b7280`, under each header at y=68):** "urgent + actionable + user-visible" / "actionable, not urgent" / "neither — just information".
- **Boxes:** rounded rects 190px wide, 26px tall, 6px radius, 12px `#2c3e50` text, stacked from y=80 with 6px gaps:
  - Page column (fill `rgba(231,76,60,0.12)`, border `#e74c3c`): "checkout error rate 4%", "p99 latency 8s"
  - Ticket column (fill `rgba(217,89,38,0.12)`, border `#d95926`): "disk full in ~9 days", "cert expires in 20 days", "backup failed, retry OK", "queue depth trending up"
  - Dashboard column (fill `rgba(107,114,128,0.10)`, border `#6b7280`): "single pod restart", "cache hit rate dip", "nightly GC pause", "CPU 82% for 5 min", "one slow query", "deploy completed"
- **Count labels (bold 13px, column color, under each stack):** "2 pages", "4 tickets", "6 dashboard-only".
- **Annotation (bold 12px violet `#4a3aa7`, bottom center y=285):** "every page must be worth waking a human".
- **Caption (12px `#444`, bottom right):** "alert mix illustrative".

## Alerting on the Error Budget, Not the Raw Number

**Tags:** `rule of thumb` (blue), `SLO burn rate` (green), `absence alerts` (orange)

- **The budget** — a 99.9% success SLO over 30 days allows 0.1% failures: 43.2 minutes of full downtime
- **Burn rate** — error rate divided by the 0.1% allowance; alert when the budget is burning fast, not on raw values
- **Fast burn** — 1-hour window at burn rate 14.4 (error rate 1.44%) spends 2% of the month's budget per hour: page
- **Slow burn** — 6-hour window at burn rate 6 (error rate 0.6%) spends 5% per 6 hours: catches the quiet simmer
- **Absence too** — a pipeline that silently stops emits no errors at all; alert on missing heartbeats
- **Runbooks** — every alert links a runbook: what it means, what to check, how to mitigate

*Example (italic):* A 30-minute spike at 3% trips the 1-hour window within the half-hour, while an 8-hour simmer at 0.7% never touches it — only the 6-hour window sees the slow bleed.

**Key point:** Multi-window burn-rate alerts tie paging to the SLO — you get woken exactly when the error budget is being spent too fast, whether in one loud spike or a slow leak.

### Visualization (canvas `c3`, 720×300)

Line chart of 24 hours of error rate with two incidents — a fast spike and a slow simmer — against the two burn-rate thresholds.

- **Title (bold 15px, `#1a5276`, top center):** "Two Windows Catch Two Shapes: the Spike and the Simmer".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = hours 0 to 24 with 12px `#444` tick labels every 6h ("0h"–"24h"); y = error rate 0% to 3.5%, gridlines `#e5e9ef` at 1.0/2.0/3.0 with 12px labels.
- **Error-rate line:** blue `#2a78d6` 3px line through hours `[0, 2, 4, 4.2, 4.7, 5, 8, 11, 12, 13, 16, 19, 20, 21, 24]`, rates `[0.05, 0.06, 0.05, 3.0, 3.0, 0.08, 0.05, 0.06, 0.7, 0.72, 0.68, 0.71, 0.09, 0.05, 0.06]` — a sharp 3.0% spike near hour 4, a flat 0.7% simmer from hour 12 to 20.
- **Fast threshold:** red `#e74c3c` dashed (dash 6/4) horizontal line at 1.44%, 12px red label "1h window: 1.44% (burn 14.4)" at its right end.
- **Slow threshold:** orange `#d95926` dashed horizontal line at 0.6%, 12px orange label "6h window: 0.6% (burn 6)".
- **Annotations:** bold 12px red "spike: fast window pages within the half-hour" near (hour 5, y=70); bold 12px orange "simmer: only the slow window sees it" near (hour 15, y=150).
- **Caption (12px `#444`, bottom right):** "incident shapes illustrative; thresholds from a 99.9% / 30-day SLO".

## The Fatigue Spiral: How Noise Causes the Outage

**Tags:** `common mistake` (red), `alert fatigue` (orange), `hygiene loop` (green)

- **The spiral** — noisy alerts get acknowledged reflexively, so the one real page gets the same reflexive ack
- **The training** — every non-actionable page teaches responders that the pager is safe to ignore
- **The doctrine** — standard SRE rule: every page must be actionable and require human judgment, else automate the response
- **The peak** — at the worst week the team took 9 pages a shift and acked 70% of them with no action taken
- **The hygiene loop** — review every page weekly; delete or fix the noisy ones; track pages-per-shift as team health
- **The payoff** — six weeks of weekly reviews cut the load to 2 pages a shift and 10% no-action acks

*Example (italic):* The week the team hit 9 pages a shift, a genuine checkout outage was acked and slept through — it looked exactly like the other 8.

**Common mistake:** Treating alert noise as an annoyance instead of a defect. A page that needs no action isn't harmless — it is actively training the on-call to ignore the next real one; fix it, automate it, or delete it.

### Visualization (canvas `c4`, 720×300)

Dual-line chart over 12 on-call weeks: pages per shift and the share acked without action, before and after weekly page reviews begin.

- **Title (bold 15px, `#1a5276`, top center):** "Pages per Shift as a Health Metric: the Review Loop Pays Off".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = weeks 1 to 12 with 12px `#444` tick labels every 2 weeks; left y = pages per shift 0 to 10, gridlines `#e5e9ef` at 2.5/5/7.5.
- **Pages line:** blue `#2a78d6` 3px line through weeks `[1..12]`, pages per shift `[3, 4, 6, 7, 8, 9, 9, 6, 4, 3, 2, 2]`.
- **No-action line:** red `#e74c3c` 2px line, share acked without action `[20, 30, 45, 55, 65, 70, 70, 50, 35, 25, 15, 10]` percent, scaled to the same plot (100% = top), 12px red label "% acked, no action" near its week-5 point.
- **Review marker:** vertical dashed `#6b7280` (dash 4/3) line at week 7, 12px `#6b7280` label "weekly page review starts" at its top.
- **Annotations:** bold 12px red "peak: 9 pages/shift, 70% ignored" near week 6, y=60; bold 13px green `#008300` "2 pages/shift — every page means something again" near week 10, y=180.
- **Caption (12px `#444`, bottom right):** "weekly counts illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); alert counts, incident shapes, and weekly page loads are invented and labeled illustrative; the SLO numbers are exact — 99.9% over 30 days allows 43.2 minutes (0.001 × 30 × 24 × 60), burn 14.4 over 1h = 1.44% error rate spending 2% of budget per hour, burn 6 over 6h = 0.6% spending 5% per 6 hours (the standard multi-window values from the published SRE material).
- Credit the alerting philosophy to Google's published Site Reliability Engineering book in the first section's bullets, as written above.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
