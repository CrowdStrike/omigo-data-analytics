# Prometheus

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Prometheus

**Subtitle:** Prometheus stores every metric as a set of labeled time series it pulls from your services — PromQL turns the raw counters into rates, sums, and alerts

## Four Series From One Metric Name

**Tags:** `core idea` (blue), `pull model` (green), `labels` (orange)

- **The setup** — a shop runs two services, checkout and search; each exposes a plain-text /metrics page
- **The pull** — Prometheus scrapes each /metrics endpoint on a schedule (every 15s); services never push
- **The metric** — one name, `http_requests_total`, plus a label set: service and status
- **The series** — each unique label combination is its own time series: 2 services × 2 statuses = 4 series
- **The sample** — every scrape appends one timestamped value per series to Prometheus's local store

*Example (italic):* At 2:00:00pm the checkout scrape returns `http_requests_total{service="checkout",status="200"} 184203` — one sample landing in one of the 4 series.

**Key point:** A Prometheus metric is a name plus a label set — and the label set, not the name, defines the series. Four label combinations means four independent streams of samples.

### Visualization (canvas `c1`, 720×300)

Flow diagram of the pull model: Prometheus on the right scraping two service boxes on the left, with the four resulting time series listed underneath as scrape-output lines.

- **Title (bold 15px, `#1a5276`, top center):** "One Scrape Loop, Four Time Series".
- **Prometheus box:** rounded box at x=490, y=80, 180×70, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border, bold 13px `#1a5276` label "Prometheus" with 12px `#444` sub-label "scrapes + stores".
- **Target boxes:** two rounded boxes at x=70, 180×44, fills `rgba(0,131,0,0.12)`, 2px `#008300` border — y=68 labeled "checkout :8080/metrics" (12px), y=132 labeled "search :8080/metrics".
- **Arrows:** 3px `#6b7280` arrows FROM the Prometheus box's left edge TO each target box's right edge (pull direction), shared 12px `#6b7280` label "GET /metrics every 15s" centered between them at y≈105.
- **Series lines (monospace 12px, x=70, at y = 210, 230, 250, 270), each preceded by a 10×10 colored square:** blue `#2a78d6` `{service="checkout",status="200"} 184203`, magenta `#d55181` `{service="checkout",status="500"} 1290`, green `#008300` `{service="search",status="200"} 412876`, orange `#d95926` `{service="search",status="500"} 3430`.
- **Annotation (bold 13px violet `#4a3aa7`, right side near x=470, y=240):** "4 label combos → 4 series".
- **Caption (12px `#444`, bottom right):** "counter values illustrative".

## Turning a Counter Into Requests per Second

**Tags:** `worked example` (blue), `PromQL` (green)

- **The counter** — `http_requests_total` only ever goes up; the raw value (1,290) means nothing by itself
- **The window** — `rate(http_requests_total[5m])` looks at the last 5 minutes of samples per series
- **The math** — checkout's 500s rose 1,200 → 1,290 over 5 min: 90 / 300s = 0.3 req/s (exact)
- **Per series** — the same query gives search's 500s: 3,400 → 3,430, so 30 / 300s = 0.1 req/s
- **The aggregation** — `sum by (status)`: 200s (illustrative) 30 + 50 = 80, 500s 0.3 + 0.1 = 0.4 req/s

*Example (italic):* `rate(http_requests_total{status="500"}[5m])` returns two values — 0.3 for checkout and 0.1 for search — because rate() works on each series separately.

**Key point:** rate() turns an ever-growing counter into a per-second speed (a slope), and `sum by (label)` collapses the series along whichever labels you leave out.

### Visualization (canvas `c2`, 720×300)

Line chart of the raw checkout-500 counter climbing over 5 minutes, with a slope triangle showing how rate() reads the rise as 0.3 req/s.

- **Title (bold 15px, `#1a5276`, top center):** "rate() Is a Slope: 90 More Errors in 5 Minutes = 0.3/s".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = time "2:00" to "2:05" with 12px `#444` tick labels every 1 minute; y = counter value 1,150 to 1,350, gridlines `#e5e9ef` at 1,200 / 1,250 / 1,300 with 12px `#444` labels.
- **Counter line:** magenta `#d55181` 3px line with 4px dots through minutes `[0, 1, 2, 3, 4, 5]`, values `[1200, 1218, 1236, 1254, 1272, 1290]`.
- **Slope triangle:** dashed `#6b7280` (dash 4/3) horizontal line from (min 0, 1200) to (min 5, 1200) labeled "300 s" (12px `#6b7280`, below), dashed vertical line from (min 5, 1200) up to (min 5, 1290) labeled "Δ 90" (12px `#6b7280`, right).
- **Annotation (bold 13px green `#008300`, near min 1.5, y=95):** "rate = 90 / 300s = 0.3 req/s (exact)".
- **Caption (12px `#444`, bottom right):** "counter values illustrative; the division is exact".

## From Borgmon to the Pager

**Tags:** `where it's used` (blue), `alerting` (green), `history` (orange)

- **The origin** — built at SoundCloud starting in 2012, openly inspired by Google's internal Borgmon
- **The pedigree** — the second project accepted into the CNCF, right after Kubernetes
- **The rule** — an alerting rule is just PromQL: checkout's error ratio > 5% sustained "for 10m"
- **The handoff** — firing alerts go to Alertmanager, which groups, silences, and routes them to a pager
- **The habit** — dashboards and alerts watch rate() and ratios, never raw counter values

*Example (italic):* Checkout's error ratio crosses 5% at 2:22pm; it stays above for the full 10-minute hold, so at 2:32pm Alertmanager pages on-call (times illustrative).

**Key point:** One query language drives both graphs and alerts — an alert is a PromQL expression that has been true for long enough, handed to Alertmanager for delivery.

### Visualization (canvas `c3`, 720×300)

Timeline of checkout's error ratio during an incident: the PromQL threshold, the 10-minute "pending" hold, and the moment the alert fires.

- **Title (bold 15px, `#1a5276`, top center):** "Alert Rule: error ratio > 5% for 10m → page".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = time "2:00" to "3:00" with 12px `#444` tick labels every 10 minutes; y = error ratio 0% to 10%, gridlines `#e5e9ef` at 2.5 / 5 / 7.5 with 12px `#444` labels.
- **Ratio line:** blue `#2a78d6` 3px line through minutes `[0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60]`, ratio % `[1.0, 1.1, 0.9, 1.2, 3.3, 7.5, 9.2, 8.8, 6.1, 3.0, 1.4, 1.0, 0.9]`.
- **Threshold:** orange `#d95926` dashed (dash 6/4) horizontal 2px line at 5%, 12px orange label "threshold 5%" at its left end.
- **Pending band:** fill `rgba(201,133,0,0.15)` between minute 22 and minute 32 spanning the plot height, 12px `#c98500` label "pending (for 10m)" at its top.
- **Firing marker:** red `#e74c3c` vertical 2px line at minute 32, bold 13px red label "FIRING → Alertmanager pages" to its right at y=70.
- **Caption (12px `#444`, bottom right):** "error ratios illustrative".

## The user_id Label That Melts the Server

**Tags:** `common mistake` (red), `cardinality` (orange)

- **The temptation** — "let's add a user_id label so we can graph one customer's requests"
- **The multiplication** — labels multiply: every new label value spawns a series per existing series
- **The math** — 800 series × 500,000 users = 400,000,000 series (exact, given those counts)
- **The cost** — each series is its own stream in memory and in the index; millions melt the server
- **The fix** — keep labels low-cardinality (service, status, endpoint); per-user detail belongs in logs

*Example (italic):* One engineer's one-line label change takes the shop from 800 series to 400 million — Prometheus runs out of memory before the next deploy (counts illustrative).

**Common mistake:** Treating labels as free annotation. Every distinct label value creates a whole new time series — unbounded values like user IDs, emails, or request IDs belong in logs or traces, never in labels.

### Visualization (canvas `c4`, 720×300)

Horizontal bar chart of series counts as labels are added, ending with the user_id explosion; bar widths are hardcoded pixels giving a log feel.

- **Title (bold 15px, `#1a5276`, top center):** "The Cardinality Trap: One user_id Label, 500,000× the Series".
- **Axis:** vertical 2px `#999` baseline at x=250, bars extend right, max width 430.
- **Rows (top to bottom at y = 70, 120, 170, 220), each with a left-aligned 12px `#444` label at x=20:**
  - "service × status (2×2) — 4 series": blue `#2a78d6` bar width 30
  - "+ endpoint (×20) — 80 series": blue bar width 95
  - "+ instance (×10) — 800 series": blue bar width 145
  - "+ user_id (×500,000) — 400,000,000 series": red `#e74c3c` bar width 430, bold 12px red label "server runs out of memory" above the bar's right end
- **Bar style:** 14px tall, blue bars fill `rgba(42,120,214,0.30)` with 2px `#2a78d6` edge, red bar solid; 11px `#444` count labels at bar ends where the row label doesn't already carry the count.
- **Annotation (bold 13px magenta `#d55181`, centered near y=265):** "cardinality is the product of every label's values".
- **Caption (12px `#444`, bottom right):** "pixel widths log-feel schematic; multiplications exact, label counts illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); counter values, error ratios, and label counts are invented and labeled illustrative; the derived arithmetic (90/300s = 0.3 req/s, 30/300s = 0.1 req/s, the sums 80 and 0.4 req/s, and the cardinality products 4 / 80 / 800 / 400,000,000) is exact given those inputs. Historical facts (SoundCloud origin, Borgmon inspiration, second CNCF project after Kubernetes) are publicly documented.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
