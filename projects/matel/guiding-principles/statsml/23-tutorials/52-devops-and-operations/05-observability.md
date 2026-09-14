# Observability

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Observability

**Subtitle:** Metrics, logs, and traces each answer a different question about a failing system — IS something wrong, WHAT exactly happened, and WHERE

## Three Questions About One Slow Checkout

**Tags:** `core idea` (blue), `three pillars` (green), `metrics` (orange)

- **The shop** — an online store handles 12,000 checkout requests per minute across a dozen services
- **The spike** — at 14:07 the checkout p99 latency jumps from about 320ms to 2,400ms
- **Metrics** — numeric time series, pre-aggregated and cheap at scale; they answer "IS something wrong?"
- **The limit** — aggregation destroys detail: the p99 line says slow, never WHICH requests are slow
- **Logs** — structured event records with all the detail; they answer "WHAT exactly happened?"
- **Traces** — one request's path across services, timed span by span; they answer "WHERE did it happen?"

*Example (italic):* The p99 dashboard fires an alert at 14:07 — it proves checkout is sick, but points at zero of the 12,000 requests per minute causing it.

**Key point:** Each pillar answers one question — metrics detect (IS), traces locate (WHERE), logs explain (WHAT) — and no single pillar can do the other two jobs.

### Visualization (canvas `c1`, 720×300)

Line chart of checkout p99 latency over 20 minutes with an alert threshold: flat around 320ms, then a step up to ~2,400ms at 14:07.

- **Title (bold 15px, `#1a5276`, top center):** "The Metric Says WRONG, Not WHICH: Checkout p99 at 14:07".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = time "14:00" to "14:20", 12px `#444` tick labels every 4 minutes; y = p99 latency 0 to 2,500ms, gridlines `#e5e9ef` at 500/1000/1500/2000, 12px `#444` labels.
- **p99 line:** blue `#2a78d6` 3px line through minutes `[0, 2, 4, 6, 7, 8, 10, 12, 14, 16, 18, 20]`, p99 ms `[310, 322, 318, 305, 2400, 2350, 2410, 2380, 2320, 2390, 2360, 2400]` — vertical step at minute 7.
- **Threshold line:** red `#e74c3c` dashed (dash 6/4) horizontal line at 800ms, 12px red label "alert threshold 800ms" at its left end.
- **Alert marker:** red `#e74c3c` filled circle radius 5 at (minute 7, 2400), bold 12px red label "alert fires 14:07" beside it.
- **Annotation (bold 13px `#d95926`, near minute 13, y=95):** "12,000 req/min behind this one line — zero named".
- **Caption (12px `#444`, bottom right):** "latency values illustrative".

## Following Request 7f3a Through the Stack

**Tags:** `worked example` (blue), `traces` (green), `correlation ID` (orange)

- **Step 1: metric** — the 14:07 alert says checkout is slow; it cannot say more, so we go to traces
- **Step 2: trace** — filtering traces for checkout requests over 800ms, every slow one crosses payment-svc
- **The waterfall** — `7f3a` totals 2,380ms: cart-svc 60ms, payment-svc 2,100ms, inventory-svc 80ms, gateway 140ms
- **Step 3: logs** — the payment span's logs, joined by trace ID `7f3a`, name the cause in one line
- **The line** — "pool exhausted: waited 2,000ms for a connection; query took 100ms" — 2,000 + 100 ≈ the 2,100ms span
- **The glue** — the same correlation ID stamped on the trace and every log line is what makes the join possible

*Example (italic):* Metric (14:07 alert) → trace (payment-svc span is 2,100 of 2,380ms) → log (connection pool exhausted): three hops from "something is wrong" to the exact cause.

**Key point:** The pillars work as a funnel — alert on a METRIC, narrow to the guilty service with a TRACE, read the WHAT in that span's LOGS — and correlation IDs are the thread through all three.

### Visualization (canvas `c2`, 720×300)

Trace waterfall for request `7f3a`: horizontal span bars on a shared 0–2,400ms time axis, the payment-svc span in red dominating the width.

- **Title (bold 15px, `#1a5276`, top center):** "Trace 7f3a: One Span Owns 2,100 of 2,380ms".
- **Axis:** horizontal 2px `#999` time axis at y=250 from x=170 to x=690, ticks and 12px `#444` labels at 0 / 600 / 1200 / 1800 / 2400ms; 1ms = (520/2400)px.
- **Rows (bar height 24px, 8px radius, left-aligned 12px `#2c3e50` service labels at x=20):**
  - y=80 "api-gateway": blue `rgba(42,120,214,0.30)` bar spanning 0–2,380ms, 2px `#2a78d6` border, 11px label "2,380ms" at bar end
  - y=120 "cart-svc": aqua `rgba(25,158,112,0.30)` bar spanning 40–100ms, 11px label "60ms"
  - y=160 "payment-svc": red `rgba(231,76,60,0.25)` bar spanning 120–2,220ms, 2px `#e74c3c` border, bold 12px red label "2,100ms — pool exhausted" centered inside
  - y=200 "inventory-svc": green `rgba(0,131,0,0.25)` bar spanning 2,240–2,320ms, 11px label "80ms"
- **Log callout:** dashed `#6b7280` (dash 4/3) leader line from the payment bar down to a rounded box at (x=250, y=262), 11px `#2c3e50` monospace text: `trace=7f3a "waited 2,000ms for connection; query 100ms"`.
- **Annotation (bold 13px green `#008300`, near x=520, y=60):** "WHERE found — now read that span's logs".
- **Caption (12px `#444`, bottom right):** "span timings illustrative".

## Monitoring Watches Known Failures; Observability Asks New Questions

**Tags:** `where it's used` (blue), `high cardinality` (green)

- **Monitoring** — pre-built dashboards and alerts for failure modes you already predicted (CPU, error rate)
- **Observability** — being able to ask a question you never predicted, after the incident starts
- **The new question** — "is the pool exhaustion only hitting one payment provider?" — no dashboard exists for it
- **High cardinality** — answering it needs per-request fields like provider, user tier, region on every event
- **Why metrics can't** — a metric with 2 million user IDs as a label explodes; traces and logs carry them fine
- **Structured logging** — key=value logs (not free text) are what make those fields queryable at all

*Example (italic):* Querying traces by `payment_provider` shows 96% of slow checkouts use provider B — a question nobody had thought to dashboard before 14:07.

**Key point:** Monitoring answers questions you wrote down in advance; observability is the ability to ask new ones — and high-cardinality, structured telemetry is what makes new questions answerable.

### Visualization (canvas `c3`, 720×300)

Two-row flow diagram: monitoring (known question, pre-built dashboard) vs observability (new question, high-cardinality query), as rounded boxes with arrows.

- **Title (bold 15px, `#1a5276`, top center):** "Known Questions vs New Questions".
- **Row 1 (boxes centered on y=100), label 12px `#444` at x=20:** "monitoring"; blue `#2a78d6` rounded box at x=150 labeled "known failure mode: 'error rate high?'" (12px), 3px arrow to a blue box at x=400 labeled "pre-built dashboard", arrow to a green `#008300` box at x=600 labeled "answer in seconds".
- **Row 2 (boxes centered on y=210), label:** "observability"; violet `#4a3aa7` rounded box at x=150 labeled "new question: 'only provider B?'", 3px arrow to a violet box at x=400 labeled "query raw traces by provider", arrow to a green box at x=600 labeled "96% provider B".
- **Box style:** 150–180px wide, 44px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(74,58,167,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text, two lines allowed.
- **Divider:** 1px `#e5e9ef` horizontal line at y=155.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=275):** "the second row only works if per-request fields were recorded before you needed them".
- **Caption (12px `#444`, bottom right):** "provider split illustrative".

## Telemetry Is a Data Pipeline With Its Own Bill

**Tags:** `common mistake` (red), `cost` (orange)

- **The mistake** — treating telemetry as free exhaust; it is a data pipeline you build, ship, store, and pay for
- **The shape** — for the same day of traffic: metrics ~4 GB, sampled traces ~60 GB, raw logs ~1,200 GB (illustrative)
- **Why logs dominate** — every request writes many detailed events; detail is exactly what makes logs expensive
- **Sampling** — keeping 1% of traces (and 100% of errors) preserves the WHERE while cutting volume 100×
- **Retention tiers** — hot search for 7 days, warm for 30, cold archive for 365 — most log bytes are never read
- **The trap** — cutting logs blindly to save money, then losing the WHAT during the next incident

*Example (italic):* At 1,200 GB of logs per day, one chatty debug statement left on in checkout can add more daily volume than every metric in the company combined.

**Common mistake:** Instrumenting everything at full volume forever. Telemetry needs the same volume/retention engineering as any data pipeline — sample traces, tier log storage, and keep metrics for the long haul because they are the cheap pillar.

### Visualization (canvas `c4`, 720×300)

Horizontal bar chart: daily telemetry volume by pillar for the same traffic, with log-feel bar widths and a retention-tier note on the logs bar.

- **Title (bold 15px, `#1a5276`, top center):** "One Day of Traffic, Three Very Different Bills".
- **Axis:** horizontal 2px `#999` baseline at x=210, bars extend right, max width 460; log-feel achieved by hardcoded pixel widths, not a real log axis.
- **Rows (top to bottom at y = 80, 140, 200), each with a left-aligned 12px `#444` label at x=20:**
  - "metrics — 4 GB/day": green `#008300` bar width 60, 11px label "4 GB" at bar end
  - "traces (1% sampled) — 60 GB/day": blue `#2a78d6` bar width 170, 11px label "60 GB"; 11px `#6b7280` note under the bar "100% would be ~6,000 GB"
  - "logs — 1,200 GB/day": orange `#d95926` bar width 460, bold 12px `#d95926` label "1,200 GB"; three 11px `#6b7280` tier ticks along the bar at widths 110 / 260 / 460 labeled "hot 7d / warm 30d / cold 365d"
- **Bar style:** 22px tall, fills at 0.30 alpha of each color with a 2px solid border in the same color.
- **Annotation (bold 13px red `#e74c3c`, near x=420, y=55):** "aggregation is why metrics are cheap — and why they lack detail".
- **Caption (12px `#444`, bottom right):** "volumes illustrative; pixel widths schematic".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); latency values, span timings, provider split, and telemetry volumes are invented and labeled illustrative; the arithmetic ties are load-bearing — the 2,000ms wait + 100ms query matches the 2,100ms payment span, and the 1% trace sample matches the 60 GB vs ~6,000 GB note.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
