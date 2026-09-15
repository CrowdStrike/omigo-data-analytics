# OpenTelemetry

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** OpenTelemetry

**Subtitle:** One standard API for traces, metrics, and logs — instrument your service once, then point the telemetry at any backend you like

## One Checkout Service, Three Signals, One SDK

**Tags:** `core idea` (blue), `three signals` (green), `OTLP` (orange)

- **The service** — a checkout service handles 1,200 requests/min and needs to be observable
- **Traces** — each request emits one span here, part of a trace across cart, payment, and inventory
- **Metrics** — counters and latencies roll up into 40 metric series (requests, errors, p99)
- **Logs** — the same requests produce about 3,000 log lines/min, tied to their trace IDs
- **One SDK** — a single OpenTelemetry SDK produces all three signals through one API per language
- **One wire** — everything leaves the process in one format, the OTLP protocol, vendor-neutral

*Example (italic):* The checkout team adds the OTel SDK once; auto-instrumentation for their HTTP and DB libraries emits traces, metrics, and logs with no per-vendor agent.

**Key point:** OpenTelemetry standardizes how telemetry is produced — one API/SDK and one wire format (OTLP) for all three signals, independent of where the data ends up.

### Visualization (canvas `c1`, 720×300)

Left-to-right flow diagram: the checkout service's three signals funneling through one OTel SDK, over OTLP, into a Collector, then out to interchangeable backends.

- **Title (bold 15px, `#1a5276`, top center):** "Three Signals, One SDK, One Wire Format".
- **Service box:** blue `#2a78d6` rounded box at x=30, y=95, 150×110, fill `rgba(42,120,214,0.15)`, 8px radius, bold 13px `#2c3e50` label "checkout service" at top; inside it three 12px signal chips stacked at y=135/160/185: "spans 1,200/min" (blue `#2a78d6`), "metrics 40 series" (green `#008300`), "logs 3,000/min" (orange `#d95926`).
- **SDK box:** violet `#4a3aa7` rounded box at x=250, y=120, 130×60, fill `rgba(74,58,167,0.12)`, 12px label "OTel SDK (one API)".
- **Collector box:** ink `#1a5276` rounded box at x=460, y=120, 130×60, fill `rgba(26,82,118,0.12)`, 12px label "OTel Collector".
- **Arrows:** 3px `#6b7280` arrows service→SDK and SDK→Collector; the SDK→Collector arrow carries a bold 12px violet `#4a3aa7` label "OTLP" above it.
- **Backends:** three small 100×34 green `#008300` boxes at x=608, y=85/135/185, fill `rgba(0,131,0,0.12)`, 12px labels "backend A", "backend B", "backend C"; thin 2px `#6b7280` arrows from the Collector to each.
- **Annotation (bold 13px green `#008300`, centered near y=275):** "any OTLP backend can receive this — the service does not care which".
- **Caption (12px `#444`, bottom right):** "signal volumes illustrative".

## Inside the Collector: Receive, Process, Export

**Tags:** `worked example` (blue), `pipeline` (green)

- **Receive** — the Collector's OTLP receiver takes in the service's 1,200 spans/min
- **Process** — a filter processor drops health-check spans: 180 of 1,200 (15%) are `/healthz` noise
- **Batch** — a batch processor groups the remaining 1,020 spans into batches of 100 before sending
- **Export** — two exporters fan out: the same 1,020 spans/min go to backend A and to backend B
- **Hand-check** — 1,200 in − 180 dropped = 1,020 out; fan-out copies, so each backend gets 1,020

*Example (italic):* In one minute the Collector receives 1,200 spans, drops 180 health checks, and ships 1,020 spans to each of two backends — all from a YAML pipeline, no code.

**Key point:** The Collector is a standalone pipeline — receivers in, processors in the middle, exporters out — so filtering, batching, and routing live in config, outside the service.

### Visualization (canvas `c2`, 720×300)

Horizontal pipeline diagram with span counts at each stage: receiver, filter, batcher, then a fan-out to two backend boxes.

- **Title (bold 15px, `#1a5276`, top center):** "One Minute Through the Collector: 1,200 In, 1,020 Out to Each Backend".
- **Stage boxes (rounded, 8px radius, 130×56, top edge y=120):** "OTLP receiver" blue `#2a78d6` fill `rgba(42,120,214,0.15)` at x=30; "filter: drop /healthz" orange `#d95926` fill `rgba(217,89,38,0.12)` at x=210; "batch (100/batch)" violet `#4a3aa7` fill `rgba(74,58,167,0.12)` at x=390.
- **Backend boxes:** green `#008300` fill `rgba(0,131,0,0.12)`, 130×44, at x=570, y=90 labeled "backend A" and x=570, y=176 labeled "backend B" (12px `#2c3e50` text).
- **Arrows:** 3px `#6b7280` arrows between stages with bold 12px labels above: "1,200 spans/min" before the filter, "1,020" after it, then two arrows from the batch box to the backends each labeled "1,020".
- **Drop marker:** bold 12px red `#e74c3c` label "−180 dropped (15%)" below the filter box at y=205 with a short 2px red tick down from the box.
- **Annotation (bold 13px green `#008300`, centered near y=262):** "routing and filtering are config, not code".
- **Caption (12px `#444`, bottom right):** "span counts illustrative".

## Switching Vendors Without Re-Instrumenting

**Tags:** `where it's used` (blue), `vendor lock-in` (green), `2019 merger` (orange)

- **The old world** — every observability vendor shipped its own agent; instrumentation was vendor code
- **The trap** — switching vendors meant re-instrumenting every service, so nobody switched
- **The merger** — OpenTracing and OpenCensus, two rival standards, merged into OpenTelemetry in 2019
- **The math** — 40 services × 3 days each to re-instrument = 120 engineer-days per vendor switch
- **The OTel way** — next quarter the team swaps backend A for backend B by editing one exporter line
- **Zero code change** — the checkout service is not redeployed; the Collector just points elsewhere

*Example (italic):* In Q3 telemetry flows to backend A; in Q4 a one-line exporter edit sends the same 1,020 spans/min to backend B — the 40 services never notice.

**Key point:** OTel moves the vendor decision from code to config — instrumentation is written once against the standard, and the exporter, not the service, knows the backend.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: engineer-days to change observability vendors, proprietary agents vs OpenTelemetry, for a 40-service fleet.

- **Title (bold 15px, `#1a5276`, top center):** "Cost of a Vendor Switch: 40 Services, Proprietary Agents vs OTel".
- **Axis:** vertical 2px `#999` baseline at x=250, bars extend right, max width 420 representing 120 engineer-days (3.5 px/day).
- **Rows (bar tops at y = 70, 120, 170, 220), each with a left-aligned 12px `#444` label at x=20:**
  - "install vendor agent, per vendor": blue `#2a78d6` bar width 420 (120 days)
  - "switch vendors, proprietary": red `#e74c3c` bar width 420 (120 days), 12px red end label "re-instrument all 40"
  - "install OTel once": blue `#2a78d6` bar width 420 (120 days)
  - "switch vendors with OTel": green `#008300` bar width 4 (1 day), bold 12px green end label "edit one exporter line"
- **Bar style:** 16px tall, blue bars fill `rgba(42,120,214,0.30)`, red and green bars solid, 11px `#444` day-count labels at bar ends ("120 days", "120 days", "120 days", "1 day").
- **Annotation (bold 13px magenta `#d55181`, right side near y=255):** "pay the instrumentation cost once — never again per vendor".
- **Caption (12px `#444`, bottom right):** "engineer-days illustrative; 2019 merger date exact".

## OTel Is Not a Backend

**Tags:** `common mistake` (red), `pipeline vs storage` (orange)

- **The confusion** — teams adopt OTel and then ask where their dashboards are; OTel has none
- **What OTel is** — the API, the SDKs, auto-instrumentation, the OTLP protocol, and the Collector
- **What it is not** — storage, querying, dashboards, and alerting all belong to the backend you pick
- **The symptom** — a Collector exporting to nowhere: 1,020 spans/min received, nothing to look at
- **The fix** — OTel produces and routes the telemetry; some backend must still store and show it

*Example (italic):* A team deploys the SDK and Collector, opens no backend account, and wonders why 1,020 spans/min yield zero dashboards — the pipe works, but it empties into nothing.

**Common mistake:** Conflating instrumentation with the backend. OpenTelemetry standardizes how telemetry is produced and shipped — you still choose, run, or buy the system that stores and visualizes it.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: a pipeline exporting to nothing (dead end) vs the same pipeline exporting to a real backend, with a bracket marking where OTel's job ends.

- **Title (bold 15px, `#1a5276`, top center):** "OTel Ends at the Exporter — a Backend Still Has to Exist".
- **Row 1 (box tops y=85), label 12px `#444` at x=20:** "no backend"; blue `#2a78d6` rounded box at x=130 labeled "SDK + Collector, 1,020 spans/min" (12px), 3px arrow to a red `#e74c3c` dashed-border box at x=430 labeled "exporter → nowhere" with bold 12px red "✗ no dashboards, data lost".
- **Row 2 (box tops y=185), label:** "with backend"; identical blue box "SDK + Collector, 1,020 spans/min", 3px arrow to a green `#008300` box at x=430 labeled "backend stores + queries" with bold 12px green "✓ dashboards, alerts".
- **Box style:** 190–210px wide, 44px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Boundary bracket:** vertical dashed `#6b7280` (dash 4/3) line at x=390 from y=60 to y=250, 12px `#6b7280` label at its top: "OTel's job ends here".
- **Annotation (bold 13px orange `#d95926`, centered near y=270):** "OTel is the plumbing, not the reservoir".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all values are the hardcoded numbers above (no randomness); request/span/log volumes, the 15% health-check drop (180 of 1,200 → 1,020), and the 40 services × 3 days = 120 engineer-days switch cost are invented and labeled illustrative; the OpenTracing + OpenCensus merger into OpenTelemetry in 2019, the three-signal scope (traces, metrics, logs), OTLP, and the Collector's receive/process/export pipeline are documented facts, labeled exact where dated.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
