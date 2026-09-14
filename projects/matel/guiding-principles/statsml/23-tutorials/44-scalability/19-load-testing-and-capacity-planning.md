# Load Testing & Capacity Planning

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Load Testing & Capacity Planning

**Subtitle:** Ramp realistic traffic against a production-like copy until latency hockey-sticks — the knee is your real ceiling, and load testing tells you which resource sets it

## Ramping Traffic at the Checkout Service

**Tags:** `core idea` (blue), `the knee` (green), `test types` (orange)

- **The service** — a checkout API must survive a holiday sale forecast to peak at 3,000 orders/s
- **The ramp** — a load tool steps traffic 100 → 1,200 req/s at one production-like instance
- **The knee** — up to ~800 req/s throughput tracks offered load; past it, it flattens near 820
- **The hockey stick** — p95 latency holds 80–140ms below the knee, then jumps 260 → 520 → 2,300ms
- **The spike test** — jump instantly to peak load to see whether the service recovers or falls over
- **The soak test** — hold a steady load for hours to catch slow leaks (memory, file handles)

*Example (italic):* At 700 req/s the checkout answers in 140ms; at 900 req/s it serves only 815/s and p95 latency hits 520ms.

**Key point:** The knee — where throughput stops growing and latency hockey-sticks — is the system's real ceiling; finding that one number before users do is what a load test is for.

### Visualization (canvas `c1`, 720×300)

Dual-axis line chart of the ramp: throughput (left axis) flattens at the knee while p95 latency (right axis) hockey-sticks, on a shared offered-load x-axis.

- **Title (bold 15px, `#1a5276`, top center):** "The Ramp Finds the Knee: Throughput Flattens, Latency Hockey-Sticks".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = offered load 0 to 1,200 req/s, 12px `#444` tick labels every 300; left y = throughput 0 to 1,200 req/s (blue axis labels), gridlines `#e5e9ef` at 300/600/900; right y = p95 latency 0 to 2,400ms (red axis labels at x=675).
- **Throughput line:** blue `#2a78d6` 3px line through offered `[100, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 1100, 1200]`, served `[100, 200, 300, 400, 500, 600, 700, 790, 815, 820, 818, 815]` — tracks the diagonal, then flattens near 820.
- **Latency line:** red `#e74c3c` 3px line through the same offered-load grid, p95 ms `[80, 80, 82, 85, 90, 105, 140, 260, 520, 900, 1500, 2300]` — flat, then a cliff upward.
- **Knee marker:** vertical dashed `#6b7280` (dash 4/3) line at offered load 800, bold 12px `#6b7280` label "the knee ≈ 800 req/s" at its top.
- **Annotation (bold 13px violet `#4a3aa7`, near x=950, y=80):** "past the knee, more load buys latency, not throughput".
- **Caption (12px `#444`, bottom right):** "single instance, numbers illustrative".

## Which Resource Runs Out First

**Tags:** `worked example` (blue), `lowest ceiling` (green)

- **The guess** — the team bet CPU would max out first; the dashboards said otherwise
- **The readings** — at the 800 req/s knee: CPU 55%, memory 40%, disk I/O 30%, network 20%
- **The culprit** — the database connection pool: all 100 connections busy, requests queue behind it
- **One lowest ceiling** — one resource typically runs out first at a given load mix, and it sets the knee
- **Raise and repeat** — with a 200-connection pool the knee moves to ~1,150 req/s, where CPU at 90% takes over

*Example (italic):* Doubling the pool to 200 connections lifts the knee from 800 to ~1,150 req/s — then CPU becomes the next lowest ceiling.

**Key point:** A load test doesn't just measure the ceiling's height — it names the resource that sets it, and that resource is rarely the one you guessed.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart of resource utilization measured at the 800 req/s knee: four comfortable resources and one saturated pool.

- **Title (bold 15px, `#1a5276`, top center):** "Utilization at the Knee (800 req/s): One Resource at 100%".
- **Axis:** vertical 2px `#999` baseline at x=230, bars extend right, 100% = width 440 (so 100% ends at x=670); dashed `#e5e9ef` guide lines at 50% (x=450) and 100% (x=670) with 11px `#6b7280` labels "50%" / "100%" below the last row.
- **Rows (top to bottom at y = 62, 103, 144, 185, 226), each with a left-aligned 12px `#444` label at x=20, bars 16px tall:**
  - "CPU": fill `rgba(42,120,214,0.30)`, width 242, 12px `#444` label "55%" at bar end
  - "memory": fill `rgba(42,120,214,0.30)`, width 176, label "40%"
  - "disk I/O": fill `rgba(42,120,214,0.30)`, width 132, label "30%"
  - "network": fill `rgba(42,120,214,0.30)`, width 88, label "20%"
  - "DB connections (100 of 100)": solid red `#e74c3c`, width 440, bold 12px red label "100% — the ceiling" at bar end
- **Annotation (bold 13px magenta `#d55181`, centered near y=275):** "the guess was CPU — the ceiling was the 100-connection pool".
- **Caption (12px `#444`, bottom right):** "readings illustrative".

## From One Instance's Ceiling to a Fleet Size

**Tags:** `where it's used` (blue), `capacity planning` (green), `rule of thumb` (orange)

- **The forecast** — marketing predicts the holiday sale peaks at 3,000 orders/s
- **The headroom rule** — plan capacity for 2× the forecast peak: 6,000 req/s
- **The utilization cap** — run each instance at ≤70% of its measured 800 ceiling: 560 req/s per instance
- **The fleet size** — 6,000 / 560 = 10.7, so 11 instances, giving 6,160 req/s of safe capacity
- **Why the cap** — near the ceiling, queueing makes latency fragile; the last 30% is not usable headroom

*Example (italic):* 11 instances × 560 req/s = 6,160 req/s — double the forecast peak, with every instance under 70% busy.

**Key point:** Capacity planning is arithmetic on three numbers — measured per-instance ceiling, forecast peak, and a headroom rule — and the load test is what supplies the first one.

### Visualization (canvas `c3`, 720×300)

Vertical bar chart building the fleet size: forecast peak, the 2× headroom target, and what 11 instances at 70% utilization actually provide.

- **Title (bold 15px, `#1a5276`, top center):** "Sizing the Fleet: 11 = ceil(6,000 / 560)".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = req/s 0 to 7,000, gridlines `#e5e9ef` at 2,000/4,000/6,000 with 12px `#444` labels.
- **Bars (120px wide, centered at x = 170, 370, 570), each with a 12px `#444` label below the baseline and a bold 12px value label above the bar top:**
  - "forecast peak": blue `#2a78d6` fill `rgba(42,120,214,0.30)` with 2px blue top edge, height for 3,000, value label "3,000"
  - "plan for 2× peak": orange `#d95926` fill `rgba(217,89,38,0.25)` with 2px orange top edge, height for 6,000, value label "6,000"
  - "11 instances @ 560": green `#008300` fill `rgba(0,131,0,0.25)` with 2px green top edge, height for 6,160, value label "6,160" — drawn as 11 stacked segments (1px white gaps) of 560 each to show the per-instance blocks
- **Target line:** horizontal dashed `#6b7280` (dash 4/3) line across the plot at 6,000, 11px `#6b7280` label "headroom target" at its left end.
- **Annotation (bold 13px violet `#4a3aa7`, near x=430, y=70):** "560 = 70% of the measured 800 ceiling".
- **Caption (12px `#444`, bottom right):** "forecast illustrative, arithmetic exact".

## Traffic That's Too Polite to Break Anything

**Tags:** `common mistake` (red), `realistic traffic` (orange)

- **Too uniform** — perfectly even synthetic arrivals never queue the way real bursty traffic does
- **No think time** — real shoppers browse and pause, holding sessions open; back-to-back requests hide that concurrency
- **Cache-friendly keys** — replaying the same 50 product IDs gives ~97% cache hits; a real sale spreads across thousands
- **The false ceiling** — the polite test shows a knee near 2,200 req/s; realistic traffic knees at 800
- **Production-like or nothing** — a half-size database or a pre-warmed empty cache invalidates the measured ceiling

*Example (italic):* The team nearly sized the fleet off a 2,200 req/s ceiling — realistic keys cut it to 800, a 2.75× overestimate.

**Common mistake:** Trusting a ceiling measured with polite traffic — uniform arrivals, no think time, hot cached keys. The test must mismodel nothing that the knee depends on, or the first real spike finds the true ceiling for you.

### Visualization (canvas `c4`, 720×300)

Two p95 latency curves on a shared offered-load axis: the cache-friendly replay knees far to the right of realistic traffic — a false ceiling.

- **Title (bold 15px, `#1a5276`, top center):** "Polite Test vs Real Traffic: a 2,200 Knee That Is Really 800".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = offered load 0 to 2,400 req/s, 12px `#444` tick labels every 600; y = p95 latency 0 to 1,000ms, gridlines `#e5e9ef` at 250/500/750.
- **Polite-test line:** blue `#2a78d6` 3px line through offered `[200, 400, 600, 800, 1000, 1400, 1800, 2200, 2400]`, p95 ms `[60, 62, 65, 70, 78, 95, 130, 300, 900]` — stays flat until ~2,200, 12px blue label "same 50 hot keys" near x=1,500 above the line.
- **Realistic line:** red `#e74c3c` 3px line through offered `[200, 400, 600, 800, 1000, 1200]`, p95 ms `[75, 85, 120, 260, 700, 980]` — hockey-sticks at 800 and exits the top of the plot, 12px red label "realistic keys + bursts" near x=650 above the line.
- **Knee markers:** vertical dashed `#e74c3c` (dash 4/3) line at 800 with bold 12px red label "real knee: 800"; vertical dashed `#6b7280` line at 2,200 with 12px `#6b7280` label "false knee: 2,200".
- **Annotation (bold 13px orange `#d95926`, near x=1,200, y=60):** "97% cache hits made the ceiling look 2.75× higher".
- **Caption (12px `#444`, bottom right):** "curves illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); throughput/latency curves, utilization readings, and the 3,000 req/s forecast are invented and labeled illustrative; the capacity arithmetic (560 = 0.70 × 800; 6,000 / 560 = 10.7 → 11; 11 × 560 = 6,160) is exact and the text numbers match the chart numbers.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
