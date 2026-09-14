# Agile, Scrum, Kanban

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Agile, Scrum, Kanban

**Subtitle:** The methods explained by their mechanisms: agile bets that plans decay fast, Scrum builds feedback loops on a fixed cadence, and Kanban's WIP limits cap wait time by Little's law

## The Bet: Plans Decay Faster Than You Think

**Tags:** `core idea` (blue), `2001 manifesto` (green), `feedback loop` (orange)

- **The plan** — a team writes a 12-month spec in January and executes it head-down, open loop
- **The decay** — by June half the assumptions are stale: users, competitors, and data all moved
- **The bet** — ship small increments and steer from real feedback instead of executing the plan blind
- **The manifesto** — the 2001 Agile Manifesto named it: "responding to change over following a plan"
- **The loop** — every few weeks working software meets real users, and the course gets corrected

*Example (italic):* The open-loop team lands 90 gap-points off target after 12 months; the team steering monthly never drifts past 10.

**Key point:** Agile is not a ritual set — it is a control-loop bet: since plans decay, shorten the interval between building something and learning whether it was right.

### Visualization (canvas `c1`, 720×300)

Line chart of "gap between the plan and what users actually need" over 12 months: an open-loop plan drifting steadily up vs a monthly-feedback sawtooth staying low.

- **Title (bold 15px, `#1a5276`, top center):** "Open-Loop Plan vs Monthly Steering: the Gap Over 12 Months".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = months 0 to 12 with 12px `#444` tick labels every 3 months; y = gap index 0 to 100, gridlines `#e5e9ef` at 25/50/75, 12px `#444` labels.
- **Open-loop line:** red `#e74c3c` 3px line through months `[0, 2, 4, 6, 8, 10, 12]`, gap `[0, 10, 22, 38, 55, 74, 90]` — smooth accelerating drift.
- **Steered line:** green `#008300` 2.5px sawtooth through month/gap pairs — months `[0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10, 10, 11, 11, 12]`, gaps `[0, 8, 2, 9, 2, 8, 3, 9, 2, 8, 2, 9, 3, 8, 2, 9, 2, 8, 3, 9, 2, 8, 2, 9]` — each rise is a month of drift, each vertical drop is a feedback correction.
- **Line labels:** bold 12px red "execute the plan, open loop" near (month 9, gap 70); bold 12px green "ship + steer monthly" near (month 6, gap 16).
- **Annotation (bold 13px `#1a5276`, near month 3, y=75):** "the bet: feedback beats prediction".
- **Caption (12px `#444`, bottom right):** "gap index illustrative".

## Scrum: Every Ceremony Is a Feedback Loop

**Tags:** `worked example` (blue), `cadence` (green), `Scrum` (orange)

- **The sprint** — a fixed 2-week timebox forces integration and feedback on a calendar, not "when ready"
- **Planning** — the team commits to a scope for the sprint: the loop gets a setpoint to steer against
- **Daily standup** — a 24-hour loop to surface blockers early, not a status report read to a manager
- **Review** — working software in front of stakeholders at day 10: the product feedback loop
- **Retrospective** — the team inspects how it worked, not what it built: the process feedback loop
- **The fit** — Scrum suits feature teams that benefit from a shared cadence and a demo rhythm

*Example (italic):* A blocker raised at Tuesday's standup costs one day; the same blocker surfacing at the day-10 review costs nine.

**Key point:** Each ceremony exists to close one specific feedback loop — and the sprint's fixed length is what makes every loop fire on schedule instead of "eventually".

### Visualization (canvas `c2`, 720×300)

Timeline diagram of one 2-week sprint (working days d1–d10) with the four ceremonies drawn as loops of different periods closing back onto the timeline.

- **Title (bold 15px, `#1a5276`, top center):** "One 2-Week Sprint: Four Loops at Three Frequencies".
- **Timeline:** 2px `#999` horizontal arrow at y=185 from x=60 to x=660; day ticks every 60px starting x=80, 12px `#444` labels "d1".."d10" below at y=205.
- **Planning box:** blue `#2a78d6` rounded box (110×34, 8px radius, fill `rgba(42,120,214,0.15)`) at (70, 130), 12px `#2c3e50` text "planning: commit scope", short 2px arrow down to the d1 tick.
- **Standup loop:** small aqua `#199e70` 2px arcs above the timeline between each pair of adjacent day ticks (d1–d2 through d9–d10, arc height 18px); one bold 12px aqua label "standup: surface blockers — daily loop" centered at y=155 above days d4–d6.
- **Review box:** green `#008300` rounded box (120×34, fill `rgba(0,131,0,0.12)`) at (540, 60), text "review: product feedback"; dashed green 2px arc (dash 5/4) from the d10 tick up over the whole timeline back to the d1 tick, apex y=52, 12px green label "feeds next planning" near the apex.
- **Retro box:** violet `#4a3aa7` rounded box (120×34, fill `rgba(74,58,167,0.12)`) at (540, 235), text "retro: process feedback"; dashed violet 2px arc below the timeline from d10 back to d1, apex y=272.
- **Annotation (bold 13px orange `#d95926`, near x=90, y=262):** "no loop closed, no reason to meet".
- **Caption (12px `#444`, bottom right):** "2-week sprint, 10 working days".

## Kanban: the WIP Limit Is a Latency Control

**Tags:** `worked example` (blue), `Little's law` (green), `flow` (orange)

- **The board** — no sprints: columns (to do / doing / done) and a hard cap on cards allowed in "doing"
- **Little's law** — items in progress = arrival rate × cycle time, so cycle time = WIP ÷ throughput
- **The walk** — a team finishes 5 tickets/week; with 30 tickets in flight, cycle time = 30 ÷ 5 = 6 weeks
- **The cap** — limit WIP to 10 and cycle time = 10 ÷ 5 = 2 weeks: same throughput, answers 3× sooner
- **The meaning** — the WIP limit is a latency control, not bureaucracy: finish before starting more
- **The fit** — Kanban suits interrupt-driven flow work: ops queues, data requests, support tickets

*Example (italic):* A data request joining a 30-item board waits 6 weeks; on a 10-item board it ships in 2 — throughput is 5 per week either way.

**Key point:** At fixed throughput, WIP and cycle time are the same quantity in different units (Little's law) — capping the cards in progress is what shortens the wait, not working faster.

### Visualization (canvas `c3`, 720×300)

Vertical bar chart of cycle time at four WIP limits, throughput held at 5 tickets/week — the bars are Little's law computed exactly.

- **Title (bold 15px, `#1a5276`, top center):** "Same 5 Tickets/Week — WIP Alone Sets the Wait".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = cycle time 0 to 6 weeks, gridlines `#e5e9ef` at 2/4, 12px `#444` labels "2 wk"/"4 wk"; no y label above 6.
- **Bars (90px wide, centered at x = 135, 285, 435, 585), heights from cycle time = WIP ÷ 5:**
  - "WIP 5": green `#008300` fill `rgba(0,131,0,0.30)`, cycle time 1 week (height 30px)
  - "WIP 10": green, cycle time 2 weeks (height 60px)
  - "WIP 20": orange `#d95926` fill `rgba(217,89,38,0.30)`, cycle time 4 weeks (height 120px)
  - "WIP 30": red `#e74c3c` fill `rgba(231,76,60,0.25)`, cycle time 6 weeks (height 180px)
- **Bar labels:** bold 13px matching-color value labels "1 wk" / "2 wk" / "4 wk" / "6 wk" centered above each bar; 12px `#444` WIP labels below the baseline at y=265.
- **Annotation (bold 13px `#1a5276`, upper left near x=110, y=70):** "cycle time = WIP ÷ throughput".
- **Caption (12px `#444`, bottom right):** "Little's law — numbers exact".

## Cargo Cult Ceremonies and Corrupted Metrics

**Tags:** `common mistake` (red), `Goodhart` (orange)

- **The cargo cult** — "we do standups, therefore we're agile": ceremonies kept, feedback loops dropped
- **The tell** — standup becomes status theater, review demos slideware, retro actions never land
- **Velocity** — story points per sprint help the team forecast its own next sprint, nothing more
- **Goodhart** — grade teams on velocity and estimates inflate on cue: the measure stops measuring
- **Burndown** — same trap: a planning chart, honest only while no one's review depends on its slope

*Example (italic):* A team graded on velocity "improves" from 21 to 58 points per sprint over six quarters while shipping the same 11–12 features.

**Common mistake:** Adopting the rituals without the loops they exist to close, then managing by the numbers the rituals produce — velocity is a planning aid, and it corrupts the moment it becomes a performance score.

### Visualization (canvas `c4`, 720×300)

Two-line chart over six quarters: reported velocity climbing after it becomes a KPI, features actually shipped staying flat.

- **Title (bold 15px, `#1a5276`, top center):** "Velocity as a KPI: Points Inflate, Output Doesn't".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = quarters Q1 to Q6, 12px `#444` tick labels evenly spaced; y = 0 to 60, gridlines `#e5e9ef` at 15/30/45.
- **Velocity line:** magenta `#d55181` 3px line through quarters `[1, 2, 3, 4, 5, 6]`, points `[21, 24, 29, 37, 46, 58]` — accelerating after Q2.
- **Shipped line:** blue `#2a78d6` 3px line through the same quarters, features `[11, 12, 11, 12, 11, 12]` — flat.
- **KPI marker:** vertical dashed `#6b7280` (dash 4/3) line at Q2, 12px `#6b7280` label "velocity becomes a KPI" at its top.
- **Line labels:** bold 12px magenta "reported story points" near (Q5, 50); bold 12px blue "features shipped" near (Q4, 18).
- **Annotation (bold 13px red `#e74c3c`, near Q4, y=70):** "Goodhart: the measure became the target".
- **Caption (12px `#444`, bottom right):** "counts illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the c3 cycle times follow Little's law exactly (WIP 5/10/20/30 at 5 tickets/week gives 1/2/4/6 weeks — label them exact); the c1 gap index and the c4 velocity/features counts are invented and labeled illustrative; text numbers (5/week, 30→6 weeks, 10→2 weeks, 21→58 points, 11–12 features) must match the charts.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
