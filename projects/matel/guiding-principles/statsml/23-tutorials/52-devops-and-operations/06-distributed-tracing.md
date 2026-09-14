# Distributed Tracing

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Distributed Tracing

**Subtitle:** One trace ID rides along with a request across every service it touches, so the whole journey reassembles into a timed tree — and the slow part points at itself

## One Slow Checkout, Twelve Suspects

**Tags:** `core idea` (blue), `trace ID` (green), `spans` (orange)

- **The symptom** — checkout takes 900ms, but the request fans out across a gateway and five services
- **The trace ID** — the gateway mints one random ID (`7f3a…`) the moment the request enters at the edge
- **Propagation** — every downstream call carries the ID in a request header, so all hops stamp the same ID
- **Spans** — each service records a span: a named, timed operation with a pointer to its parent span
- **The tree** — a collector groups spans by trace ID and rebuilds the request as one parent-child tree

*Example (italic):* Payment's span says trace=7f3a, parent=checkout; the collector files it under the same tree as the gateway's span, no guessing.

**Key point:** A distributed trace is just spans that share one ID — each service times only its own work, and the shared ID lets one tool reassemble the full journey.

### Visualization (canvas `c1`, 720×300)

Flow diagram: one checkout request crossing five services, with the same trace ID label carried over every arrow (context propagation).

- **Title (bold 15px, `#1a5276`, top center):** "One Request, One ID: trace 7f3a Rides Every Hop".
- **Boxes (rounded 8px radius, 40px tall, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border, 12px `#2c3e50` centered text):** "gateway" at (40,130) 110px wide; "auth" at (200,130) 90px wide; "inventory" at (350,65) 110px wide; "pricing" at (350,195) 110px wide; "payment" at (520,130) 100px wide.
- **Mint marker:** bold 12px `#008300` label "mints trace 7f3a" above the gateway box at (40,118).
- **Arrows (3px `#6b7280`, solid, arrowheads):** gateway→auth; auth→inventory and auth→pricing (fan-out, drawn from auth's right edge up/down); inventory→payment and pricing→payment (fan-in).
- **Header tags:** on each arrow midpoint, an 11px `#4a3aa7` pill-style label "7f3a" (violet text, fill `rgba(74,58,167,0.10)`, 8px radius) — five identical tags total.
- **Child hint:** dashed 2px `#d95926` arrow from payment box down to a small box "database" at (520,215) 100px wide, fill `rgba(217,89,38,0.12)`, showing the tree goes deeper than services.
- **Annotation (bold 13px `#008300`, bottom center near y=280):** "same ID in every header — the tree reassembles itself".

## Where the 900ms Actually Went

**Tags:** `worked example` (blue), `waterfall` (green)

- **The waterfall** — spans drawn as bars on one shared time axis, indented under their parent spans
- **The serial spine** — gateway 40ms, then auth 60ms: the first 100ms is plain one-after-another work
- **The parallel pair** — inventory (120ms) and pricing (80ms) start together, so the row costs only 120ms
- **The long bar** — payment runs 220–820ms, and inside it one database span covers 550 of the 600ms
- **The finish** — an 80ms order-confirmation span closes the request at exactly 900ms
- **The verdict** — one `UPDATE ledger` call is 61% of the whole checkout, found by looking, not grepping

*Example (italic):* 40 + 60 + 120 + 600 + 80 = 900ms — and 550ms of the payment's 600 sit inside a single database span.

**Key point:** The waterfall answers "where did the time go" by geometry — the widest bar at the deepest indent is the culprit, visible in seconds instead of an hour of cross-referencing logs.

### Visualization (canvas `c2`, 720×300)

Waterfall (Gantt-style) chart of the 900ms checkout trace: eight span bars on one time axis, indented by depth, with the 550ms database span standing out.

- **Title (bold 15px, `#1a5276`, top center):** "The 900ms Checkout, Decomposed Span by Span".
- **Time axis:** baseline 2px `#999` at y=252 from x=170 to x=690; x maps ms 0–900 to px via `x = 170 + ms*(520/900)`; tick labels "0ms" / "300ms" / "600ms" / "900ms" (12px `#444`) with vertical gridlines `#e5e9ef` full height of the row area.
- **Rows (bar height 18px, one row per span, tops at y = 56, 80, 104, 128, 152, 176, 200, 224); left-aligned 11px `#444` span-name labels at x=20, indented +12px per tree depth:**
  - depth 0 "POST /checkout": start 0, duration 900 — outline-only bar, 2px `#1a5276`, no fill
  - depth 1 "gateway.route": start 0, duration 40 — fill `#2a78d6`
  - depth 1 "auth.verify": start 40, duration 60 — fill `#199e70`
  - depth 1 "inventory.check": start 100, duration 120 — fill `#008300`
  - depth 1 "pricing.quote": start 100, duration 80 — fill `#c98500`
  - depth 1 "payment.charge": start 220, duration 600 — fill `#d95926`
  - depth 2 "db: UPDATE ledger": start 250, duration 550 — fill `#e74c3c`
  - depth 1 "order.confirm": start 820, duration 80 — fill `#4a3aa7`
- **Duration labels:** 11px `#444` "40ms", "60ms", "120ms", "80ms", "600ms", "550ms", "80ms" just right of each bar end (root bar labeled "900ms total" inside its right end, 11px `#1a5276`).
- **Annotation (bold 13px `#e74c3c`, in the empty area right of the auth/inventory rows, centered near x=480, y=120):** "one DB span = 550ms — 61% of the checkout".
- **Caption (12px `#444`, bottom right):** "timings illustrative".

## What the Tree Shows That Logs Never Will

**Tags:** `where it's used` (blue), `hidden structure` (green)

- **Serial vs parallel** — the tree shows inventory and pricing overlap; run serially they'd draw stairs
- **The stairs smell** — a staircase of sibling spans with no data dependency is free latency to reclaim
- **Retry storms** — three identical child spans under one parent, evenly spaced, is a retry loop in plain sight
- **N+1 calls** — 40 tiny identical DB spans under one query span expose per-row calls that should be one batch
- **Dependency maps** — aggregating parent-child span edges over real traffic draws the true service graph

*Example (italic):* Running inventory and pricing one after the other would push checkout from 900ms to 980ms — the stairs cost 80ms.

**Key point:** Logs tell you what each service did alone; only a trace shows the shape between services — overlap, repeats, and fan-out you can restructure for free latency wins.

### Visualization (canvas `c3`, 720×300)

Two stacked mini-waterfalls of the same checkout: top panel a serial (staircase) layout ending at 980ms, bottom panel the actual parallel layout ending at 900ms.

- **Title (bold 15px, `#1a5276`, top center):** "Same Spans, Two Shapes: Stairs Cost 80ms".
- **Shared scale:** x maps ms 0–1000 to px via `x = 60 + ms*0.6`; bar height 16px; ticks "0" / "500ms" / "1000ms" at 12px `#444` on a 2px `#999` baseline at y=262; panel labels bold 12px `#1a5276` at x=60: "serial — 980ms" at y=52, "parallel (actual) — 900ms" at y=158.
- **Panel A rows (tops at y = 60, 78, 96, 114), serial staircase:**
  - "gateway+auth": start 0, duration 100 — fill `#6b7280`
  - "inventory": start 100, duration 120 — fill `#008300`
  - "pricing": start 220, duration 80 — fill `#c98500`
  - "payment → confirm": start 300, duration 680 — fill `#d95926` (payment 600 + confirm 80 drawn as one bar, 11px label "600+80")
- **Panel B rows (tops at y = 166, 184, 202, 220), parallel pair:**
  - "gateway+auth": start 0, duration 100 — fill `#6b7280`
  - "inventory": start 100, duration 120 — fill `#008300`
  - "pricing": start 100, duration 80 — fill `#c98500` (same row band as inventory's neighbor, visibly overlapping in time)
  - "payment → confirm": start 220, duration 680 — fill `#d95926`
- **End markers:** vertical dashed `#e74c3c` line (dash 4/3) at 980ms with 11px red "980ms"; vertical dashed `#008300` line at 900ms with 11px green "900ms".
- **Annotation (bold 13px `#008300`, right side near y=240):** "the overlap is free — 80ms back".
- **Caption (12px `#444`, bottom right):** "timings illustrative, same spans as the trace above".

## Sampling, and the Place Traces Break

**Tags:** `common mistake` (red), `sampling` (orange)

- **The cost** — tracing every request at 10,000 req/min is too much data, and almost all traces are boring
- **Head sampling** — decide at the edge (keep 1% at random): cheap, but blind to how the request ends
- **Tail sampling** — decide after the trace completes: keep the slow and errored ones, drop happy paths
- **The irony** — the traces worth keeping are exactly the ones you can only recognize at the tail
- **One standard** — OpenTelemetry auto-instruments common frameworks, so most spans now come for free
- **Queue break** — context dies at a message queue unless the trace ID is copied into the message itself

*Example (italic):* Of 10,000 checkouts, 150 are slow and 50 error; 1% head sampling keeps about 2 of those 200 — tail sampling keeps all 200.

**Common mistake:** Publishing to a queue without copying the trace context into the message — the consumer starts a fresh trace, and the tree silently splits into two orphans at exactly the async hop you most needed to see.

### Visualization (canvas `c4`, 720×300)

Horizontal bar chart: of the 200 interesting traces (slow or error) in one minute of traffic, how many each sampling strategy actually captures.

- **Title (bold 15px, `#1a5276`, top center):** "10,000 Requests, 200 Worth Keeping: Head vs Tail Sampling".
- **Axis:** horizontal 2px `#999` baseline at x=240, bars extend right, x maps count 0–200 to width 0–440px (`w = count*2.2`); ticks "0" / "100" / "200" at 12px `#444` below y=252.
- **Rows (bar height 20px, tops at y = 80, 140, 200), each with a left-aligned 12px `#444` label at x=20:**
  - "slow or error traces in the minute": outline-only bar, 2px `#1a5276`, count 200 (width 440), 11px `#1a5276` label "200 of 10,000" right-aligned inside the bar end
  - "kept by head sampling (1% random)": solid `#e74c3c` bar, count 2 (width 4), bold 12px `#e74c3c` label "≈2 — the culprit is probably gone" right of the bar
  - "kept by tail sampling (slow/error rule)": solid `#008300` bar, count 200 (width 440), 11px white label "all 200" right-aligned inside the bar end
- **Annotation (bold 13px `#4a3aa7`, centered near y=270):** "similar storage bill, opposite evidence".
- **Caption (12px `#444`, bottom right):** "10,000 req/min, counts illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all span starts/durations and sampling counts are the hardcoded values above (no randomness); every number is invented and labeled illustrative. Consistency checks the text and charts must both satisfy: 40+60+120+600+80 = 900ms; the db span is 550 of payment's 600ms and 550/900 ≈ 61%; the serial variant is 40+60+120+80+600+80 = 980ms (80ms worse); 150 slow + 50 error = 200 interesting of 10,000, and 1% head sampling keeps ≈2 of them.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
