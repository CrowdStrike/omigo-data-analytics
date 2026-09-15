# Grafana

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Grafana

**Subtitle:** Grafana is the dashboard that owns no data — it queries Prometheus, Loki, databases and clouds live, and puts their answers on one shared time axis

## The Dashboard That Owns No Data

**Tags:** `core idea` (blue), `query federation` (green), `open source` (orange)

- **The tool** — Grafana is an open-source dashboarding UI, born in 2014 as a fork of Kibana
- **No storage** — it keeps no metrics or logs of its own; every panel runs a live query on a backend
- **The plugins** — data-source plugins speak Prometheus, Loki, Elasticsearch, SQL databases, CloudWatch
- **The panel** — each panel holds one query and one chart type; a dashboard is a grid of panels
- **The JSON** — the whole dashboard is a single JSON document, so it can live in version control

*Example (italic):* One dashboard shows CPU from Prometheus, slow queries from PostgreSQL, and spend from CloudWatch — Grafana stored none of those numbers.

**Key point:** Grafana is a query-federation layer with charts on top — it renders other systems' data side by side and owns none of it.

### Visualization (canvas `c1`, 720×300)

Hub-and-spoke flow diagram: one Grafana box on the left sending query arrows to five backend boxes on the right; results flow back, nothing is copied.

- **Title (bold 15px, `#1a5276`, top center):** "One Grafana, Five Backends: Panels Query, Backends Answer".
- **Grafana box:** rounded box at x=60, y=130, 160×50, 8px radius, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border, bold 13px `#1a5276` label "Grafana (no data stored)".
- **Backend boxes (right column at x=500, 180×34 each, 8px radius, fill `rgba(0,131,0,0.10)`, 1.5px `#008300` border, 12px `#2c3e50` text), tops at y = 48, 100, 152, 204, 256:** "Prometheus (metrics)", "Loki (logs)", "Elasticsearch", "PostgreSQL", "CloudWatch".
- **Arrows:** 2px `#6b7280` lines from the Grafana box right edge (x=220, y=155) fanning to each backend box left edge, small solid arrowheads at the backend end.
- **Arrow labels (11px `#6b7280`, midway along each arrow):** "PromQL", "LogQL", "Lucene", "SQL", "CloudWatch API".
- **Annotation (bold 13px violet `#4a3aa7`, at x=240, y=285):** "queries go out, results come back — no data is copied".

## One Incident, Two Backends, One Time Axis

**Tags:** `worked example` (blue), `incident` (red), `correlation` (green)

- **The incident** — a checkout latency alert fires at 14:34; on-call opens the service dashboard
- **Panel one** — request rate from Prometheus: steady near 1,200 req/s, then a cliff down to 450
- **Panel two** — error logs from Loki: 3 errors/min jumps to 520/min in the same minute
- **The axis** — both panels share one time axis, so the eye lines the two cliffs up instantly
- **The culprit** — a deploy marker sits at 14:32, two minutes before the alert; rollback at 14:44
- **Hand-check** — errors go 3 → 520 per minute between the 14:32 and 14:35 samples; the deploy sits between them

*Example (italic):* The 14:32 deploy marker lines up with requests falling 1,200 → 450 req/s and errors rising 3 → 520/min; the 14:44 rollback restores both.

**Key point:** The dashboard answers "what changed?" by putting Prometheus numbers and Loki logs on the same time axis — the deploy marker does the rest.

### Visualization (canvas `c2`, 720×300)

Two stacked mini-panels sharing one x axis, imitating an incident dashboard: request-rate line (Prometheus) on top, error-log bars (Loki) below, with the deploy marker cutting through both.

- **Title (bold 15px, `#1a5276`, top center):** "The Incident Dashboard: Two Backends, One Time Axis".
- **Shared x axis:** x from 60 to 660 (width 600) mapping 14:20 to 14:50; 12px `#444` tick labels "14:20", "14:26", "14:32", "14:38", "14:44", "14:50" under the lower panel at y=268.
- **Top panel (requests, Prometheus):** plot area y 48–138; y = req/s 0 to 1300, gridline `#e5e9ef` at 650; 11px `#6b7280` panel label "req/s — Prometheus" at (x=62, y=44).
- **Request line:** blue `#2a78d6` 3px line through minutes after 14:20 `[0, 3, 6, 9, 12, 15, 18, 21, 24, 27, 30]`, req/s `[1180, 1210, 1190, 1205, 1195, 620, 480, 450, 510, 1150, 1190]` — cliff after minute 12, recovery after minute 24.
- **Bottom panel (errors, Loki):** plot area y 160–250; y = errors/min 0 to 600, gridline at 300; 11px `#6b7280` panel label "errors/min — Loki" at (x=62, y=156).
- **Error bars:** red `#e74c3c` fill `rgba(231,76,60,0.55)` bars 12px wide at the same minute grid, errors/min `[2, 3, 2, 4, 3, 520, 490, 455, 410, 12, 4]`.
- **Deploy marker:** vertical dashed `#6b7280` (dash 4/3) line at minute 12 (14:32) spanning y 48–250, bold 12px `#6b7280` label "deploy" at its top.
- **Rollback marker:** vertical dashed `#008300` (dash 4/3) line at minute 24 (14:44), 12px `#008300` label "rollback" at its top.
- **Annotation (bold 13px red `#e74c3c`, near minute 17, y=170):** "3 → 520 errors/min right after the deploy".
- **Caption (12px `#444`, bottom right):** "rates illustrative".

## Why On-Call Opens Grafana First

**Tags:** `where it's used` (blue), `on-call` (orange), `single pane` (green)

- **First reflex** — during an incident, the service dashboard is the first tab the on-call opens
- **Single pane** — metrics, logs, and traces from backends that never talk to each other sit in one view
- **Template variables** — one dashboard with a `$service` dropdown replaces a copy per service
- **Alerting** — Grafana evaluates queries on a schedule and pages when a threshold breaks
- **JSON in git** — dashboards are JSON documents, so review, rollback, and provisioning work like code

*Example (italic):* With one correlated view the on-call spots the bad deploy in 4 minutes; hopping between four tools and matching timestamps by hand takes 26 (illustrative).

**Key point:** The value is not the charts — it is one shared time axis laid over systems that cannot be joined any other way.

### Visualization (canvas `c3`, 720×300)

Horizontal stacked bar chart: minutes from alert to diagnosis, one dashboard versus four separate tools, broken into stages.

- **Title (bold 15px, `#1a5276`, top center):** "Time to Spot the Bad Deploy: One Pane vs Four Tabs".
- **Layout:** row labels 12px `#444` left-aligned at x=20; bars start at x=230, scale 15px per minute, bars 26px tall.
- **Row 1 (y=105), label "one dashboard — 4 min":** stacked segments: blue `#2a78d6` width 15 (open the view, 1 min), orange `#d95926` width 15 (find the right panel, 1 min), magenta `#d55181` width 30 (line up the signals, 2 min); bold 12px `#008300` total label "4 min" at the bar end.
- **Row 2 (y=185), label "four separate tools — 26 min":** stacked segments: blue width 15 (1 min), orange width 120 (find the right view in each tool, 8 min), magenta width 255 (match timestamps by hand, 17 min); bold 12px `#e74c3c` total label "26 min" at the bar end.
- **Legend (11px `#444`, at y=240, x=230):** blue swatch "open", orange swatch "find", magenta swatch "correlate" — 12px squares, 6px gap.
- **Annotation (bold 13px violet `#4a3aa7`, near x=240, y=70):** "the shared time axis does the correlating for you".
- **Caption (12px `#444`, bottom right):** "minutes illustrative".

## Dashboard Sprawl: 340 Dashboards, Few Trusted

**Tags:** `common mistake` (red), `ops culture` (orange)

- **The sprawl** — every project spins up dashboards; nobody deletes them when the project ends
- **The count** — an org audit finds 340 dashboards; 213 have not been opened in over 90 days
- **The trust** — during an incident, on-call must guess which of six "checkout" dashboards is current
- **No owner** — stale panels query renamed metrics and render empty, or worse, silently wrong
- **The fix** — a few owned, reviewed, runbook-linked dashboards beat hundreds of orphans

*Example (italic):* Six dashboards match a search for "checkout"; three render empty panels, two disagree with each other, and the on-call loses ten minutes picking one (illustrative).

**Common mistake:** Treating dashboards as free. An unowned dashboard is negative documentation — during an incident a wrong chart costs more than no chart.

### Visualization (canvas `c4`, 720×300)

Vertical bar chart: 340 dashboards bucketed by when they were last viewed — a small trusted head and a long stale tail.

- **Title (bold 15px, `#1a5276`, top center):** "340 Dashboards by Last View: Most Are Stale".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = dashboards 0 to 240, gridlines `#e5e9ef` at 60/120/180 with 12px `#444` labels.
- **Bars (90px wide, centered at x = 135, 285, 435, 585), heights from counts `[24, 41, 62, 213]`:**
  - "viewed <7 days": green `#008300` fill `rgba(0,131,0,0.35)`, count 24
  - "7–30 days": blue `#2a78d6` fill `rgba(42,120,214,0.30)`, count 41
  - "30–90 days": orange `#d95926` fill `rgba(217,89,38,0.30)`, count 62
  - "over 90 days": red `#e74c3c` fill `rgba(231,76,60,0.35)`, count 213
- **Labels:** bold 13px matching-color count above each bar; 12px `#444` bucket labels below the baseline.
- **Annotation (bold 13px red `#e74c3c`, near x=380, y=70):** "213 of 340 untouched for a quarter — dead weight during an incident".
- **Caption (12px `#444`, bottom right):** "counts illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); request rates, error rates, diagnosis minutes, and dashboard counts are invented and labeled illustrative; the facts (2014 Kibana fork, data-source plugin model, dashboards as JSON, template variables, alerting) are publicly documented Grafana behavior.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
