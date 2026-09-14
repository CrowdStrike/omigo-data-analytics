# Cron & Scheduled Jobs

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Cron & Scheduled Jobs

**Subtitle:** Cron is a tiny 1975 Unix program that wakes up once a minute, compares the clock to a table of schedules, and runs whatever matches — the same five-field schedule line still sits under every modern orchestrator

## The 6:30am Sales Report Nobody Sends by Hand

**Tags:** `core idea` (blue), `the daemon` (green), `crontab` (orange)

- **The shop** — a coffee shop owner wants yesterday's sales summary in her inbox at 6:30 every weekday
- **The old way** — an employee arrives early, runs the report script by hand, and forgets on busy days
- **The daemon** — cron is a background program that wakes up once every minute and checks the clock
- **The table** — each user keeps a crontab: one line per job, a five-field schedule plus the command to run
- **The match** — when the current minute matches a line's schedule, cron runs that line's command
- **The scale** — in one day cron wakes 1,440 times and, for this shop, finds 26 matches to fire

*Example (italic):* At 6:30 on Tuesday cron's minute-check matches the report line, runs `sales_report.sh`, and the owner's email arrives before she unlocks the door.

**Key point:** Cron is just a clock-watcher plus a table — wake every minute, compare the time to each crontab line, run the commands that match, go back to sleep.

### Visualization (canvas `c1`, 720×300)

Timeline strip of one day at the coffee shop: three job rows with tick marks at every firing time, over a shared 0–24h axis.

- **Title (bold 15px, `#1a5276`, top center):** "One Day of Cron at the Coffee Shop: 1,440 Checks, 26 Matches".
- **Axes:** origin x=170, baseline y=245, plot width 500, plot height 170; x = hour of day 0 to 24 with 12px `#444` tick labels at 0/6/12/18/24; light gridlines `#e5e9ef` at those hours.
- **Rows (y = 90, 150, 210), each with a right-aligned 12px `#444` label at x=160:**
  - "hourly till sync (0 * * * *)": blue `#2a78d6` 2px tick marks at every hour `[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23]` — 24 ticks, 14px tall
  - "2:00 backup (0 2 * * *)": aqua `#199e70` single 3px tick at hour `[2]`, 18px tall
  - "sales report (30 6 * * 1-5)": green `#008300` single 3px tick at hour `[6.5]`, 18px tall, with bold 12px green label "6:30 fire" above it
- **Wake strip:** thin mute `#6b7280` dotted line along y=250 labeled 11px `#6b7280` "cron wakes every minute — 1,440 times/day" at x=180.
- **Annotation (bold 13px violet `#4a3aa7`, near x=15h, y=60):** "24 + 1 + 1 = 26 job runs today".
- **Caption (12px `#444`, bottom right):** "shop schedule illustrative".

## Reading the Five Fields: 30 6 * * 1-5

**Tags:** `worked example` (blue), `schedule syntax` (green)

- **Five slots** — a schedule line reads minute, hour, day-of-month, month, day-of-week, in that order
- **The line** — `30 6 * * 1-5` means minute 30, hour 6, any day of month, any month, Monday–Friday
- **The star** — `*` means "every value", so the two stars ignore the calendar date entirely
- **Hand-check** — Mon–Fri is 5 days, one firing per day at 6:30, so the line fires 5 times per week
- **The year** — 5 firings × 52 weeks ≈ 260 report emails per year, weekends silently skipped
- **Small tweaks** — change `30 6` to `0 22` and the same job runs at 10pm; `1-5` to `6` means Saturday only

*Example (italic):* On Saturday at 6:30 cron wakes, sees day-of-week 6 is outside 1-5, matches nothing, and no report is sent — about 260 of the year's 365 mornings fire.

**Key point:** A cron line is five clock filters ANDed together — the job runs at every minute of the year that passes all five, which you can count by hand: 5 per week, about 260 per year.

### Visualization (canvas `c2`, 720×300)

Field-breakdown diagram: the five fields of `30 6 * * 1-5` as labeled boxes, above a Mon–Sun week strip showing which days fire.

- **Title (bold 15px, `#1a5276`, top center):** "30 6 * * 1-5 — Five Filters, Five Firings a Week".
- **Field boxes (row at y=80):** five rounded boxes 96px wide, 44px tall, 8px radius, starting x=90 with 20px gaps; fills `rgba(42,120,214,0.15)` with 2px `#2a78d6` borders; bold 16px `#1a5276` value centered ("30", "6", "*", "*", "1-5") and 11px `#6b7280` role label beneath each box ("minute", "hour", "day of month", "month", "day of week").
- **Week strip (row at y=185):** seven squares 70px wide, 44px tall starting x=100 with 12px gaps, labeled 12px "Mon".."Sun"; Mon–Fri filled `rgba(0,131,0,0.20)` with 2px `#008300` border and bold 12px green "6:30" inside; Sat/Sun filled `rgba(107,114,128,0.12)` with 1px `#6b7280` border and 12px `#6b7280` "skip" inside.
- **Connector:** 2px `#6b7280` arrow from the "1-5" box down to the week strip.
- **Annotation (bold 13px green `#008300`, bottom center y=265):** "5 runs/week × 52 weeks ≈ 260 runs/year".
- **Caption (12px `#444`, bottom right):** "weekly count exact; yearly count approximate".

## The 1975 Scheduler Under Every Orchestrator

**Tags:** `where it's used` (blue), `history` (orange), `data pipelines` (green)

- **The origin** — cron shipped with Unix in 1975 as a few hundred lines that read one system table
- **The rewrite** — the 1987 multi-user rewrite gave every user a crontab and is still what Linux runs
- **The pipelines** — nightly ETL jobs, model retrains, and report refreshes are mostly cron lines at heart
- **The orchestrators** — workflow schedulers and container platforms accept the same five-field strings
- **Why it survived** — a schedule as five text fields is easy to store, diff, review, and reason about
- **The habit** — a data scientist who can read `30 6 * * 1-5` can read the schedule of nearly any pipeline

*Example (italic):* A 2025 pipeline definition that says its nightly run is `0 2 * * *` is using, character for character, the syntax a 1975 machine-room operator would recognize.

**Key point:** Fifty years of tooling — from a lab minicomputer to container platforms — kept the same five-field schedule language, so learning it once pays off in every scheduler you meet.

### Visualization (canvas `c3`, 720×300)

Horizontal timeline 1975–2025 with era boxes, all connected to one shared schedule string to show the syntax never changed.

- **Title (bold 15px, `#1a5276`, top center):** "50 Years, Same Five Fields".
- **Axis:** 2px `#999` horizontal timeline at y=150 from x=60 to x=660; 12px `#444` year ticks at 1975, 1987, 2005, 2015, 2025 spaced evenly at x = 60, 210, 360, 510, 660.
- **Era boxes (46px tall, 8px radius, 12px `#2c3e50` text, alternating above y=95 and below y=175 the line):**
  - 1975: blue `rgba(42,120,214,0.15)` border `#2a78d6`, "cron in Unix"
  - 1987: aqua `rgba(25,158,112,0.15)` border `#199e70`, "per-user crontabs"
  - 2005: violet `rgba(74,58,167,0.12)` border `#4a3aa7`, "config-managed cron fleets"
  - 2015: orange `rgba(217,89,38,0.12)` border `#d95926`, "orchestrators & container jobs"
  - 2025: green `rgba(0,131,0,0.12)` border `#008300`, "your pipeline schedule"
- **Shared string:** bold 14px monospace `#1a5276` "30 6 * * 1-5" centered in a rounded pill at (360, 262), thin dashed `#6b7280` connectors from each era box down to the pill.
- **Annotation (bold 13px magenta `#d55181`, near x=480, y=60):** "new engines, unchanged schedule language".
- **Caption (12px `#444`, bottom right):** "era grouping schematic, years approximate".

## Cron Fires on Time, Not on Completion

**Tags:** `common mistake` (red), `overlap` (orange), `missed runs` (green)

- **The blind spot** — cron never checks whether the previous run finished before starting the next one
- **The pile-up** — a job scheduled every 10 minutes that takes 25 minutes stacks up overlapping copies
- **Hand-check** — starts at minute 0, 10, 20, 30, 40; each runs 25 min, so at minute 40 three copies overlap
- **No catch-up** — if the machine is off or asleep at 6:30, the run is silently skipped, not queued
- **The guard** — real deployments add a lock file so a new copy exits if the old one still holds the lock
- **The symptom** — duplicated report emails or a database hammered by three copies of the same query

*Example (italic):* The shop's inventory sync is set to `*/10 * * * *` but takes 25 minutes on delivery day — by minute 40 the copies started at 20, 30, and 40 are all running at once.

**Common mistake:** Assuming cron runs are one-at-a-time and guaranteed. Cron only fires at matching minutes — it never waits for the last run, never retries, and never backfills a run the machine slept through.

### Visualization (canvas `c4`, 720×300)

Gantt-style overlap chart: bars for each start of a `*/10` job that takes 25 minutes, with a vertical probe at minute 40 crossing three bars.

- **Title (bold 15px, `#1a5276`, top center):** "Every 10 Minutes, But Each Run Takes 25: the Pile-Up".
- **Axes:** origin x=110, baseline y=245, plot width 540, plot height 175; x = minutes 0 to 70 with 12px `#444` tick labels every 10; gridlines `#e5e9ef` at each tick.
- **Bars (18px tall, one row per run at y = 70, 105, 140, 175, 210), left-aligned 12px `#444` labels "run 1".."run 5" at x=60:** each bar spans start to start+25 in minutes — starts `[0, 10, 20, 30, 40]`, ends `[25, 35, 45, 55, 65]`; runs 1–2 fill `rgba(42,120,214,0.30)` border `#2a78d6`; runs 3–5 fill `rgba(231,76,60,0.18)` border `#e74c3c` (the three that overlap at the probe).
- **Probe:** vertical dashed red `#e74c3c` (dash 4/3) line at minute 40 from y=55 to y=245, bold 13px red label "minute 40: 3 copies running" at its top.
- **Annotation (bold 12px orange `#d95926`, near x=55min, y=250):** "cron fired on schedule every time — it just never asked if the last run was done".
- **Caption (12px `#444`, bottom right):** "job duration illustrative; start times exact for */10".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the coffee-shop schedule (26 firings/day = 24 hourly + 1 backup + 1 report), the 5/week and roughly-260/year counts for `30 6 * * 1-5`, and the `*/10` overlap starts `[0,10,20,30,40]` / ends `[25,35,45,55,65]` with 3 concurrent copies at minute 40 are exact for the stated schedules; the shop jobs and the 25-minute duration are invented and labeled illustrative; timeline years (1975, 1987) reflect real cron history, later era years approximate.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
