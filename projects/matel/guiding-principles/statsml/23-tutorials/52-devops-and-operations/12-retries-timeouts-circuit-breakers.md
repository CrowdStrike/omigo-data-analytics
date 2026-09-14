# Retries, Timeouts, Circuit Breakers

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Retries, Timeouts, Circuit Breakers

**Subtitle:** The defensive trio every service-to-service call needs — a timeout so it can't hang, retries so blips don't become errors, and a circuit breaker so outages fail fast — each one fixing the previous one's failure mode

## The Call That Never Comes Back

**Tags:** `core idea` (blue), `timeouts` (green), `resource exhaustion` (orange)

- **The call** — a checkout service calls a payment service on every order; it usually answers in 120ms
- **The hang** — one payment instance stops answering; without a timeout the call just waits forever
- **The pool** — every hung call holds one of checkout's 200 threads, and hung calls never give them back
- **The freeze** — all 200 threads stuck means checkout serves no one — a slow dependency froze it
- **The sizing** — the deadline comes from payment's real latency: p99 is 400ms, so timeout 800ms, not 30s

*Example (italic):* Hung calls arrive at just over 3 per second; within a minute all 200 checkout threads are stuck waiting on payments, and the entire checkout service stops answering anyone.

**Key point:** Every remote call gets a deadline sized from the dependency's measured latency distribution (roughly 2× its p99) — a call that cannot end is a thread you never get back, and a default 30s timeout is 75× too long for a 400ms dependency.

### Visualization (canvas `c1`, 720×300)

Line chart of checkout's busy threads over the 60 seconds after one payment instance hangs: no-timeout line climbs to the pool limit, with-timeout line stays flat.

- **Title (bold 15px, `#1a5276`, top center):** "One Hung Dependency: No Timeout Drains the Whole Thread Pool".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = seconds 0 to 60 with 12px `#444` tick labels every 10s; y = busy threads 0 to 200, gridlines `#e5e9ef` at 50/100/150.
- **Pool limit line:** horizontal dashed `#6b7280` (dash 4/3) line at y for 200 threads, 12px `#6b7280` label "pool limit 200 — service frozen" above it at the left.
- **No-timeout line:** red `#e74c3c` 3px line through seconds `[0, 10, 20, 30, 40, 50, 60]`, busy threads `[12, 45, 80, 118, 152, 185, 200]` — steady climb to the limit.
- **With-timeout line:** blue `#2a78d6` 3px line through the same seconds, busy threads `[12, 16, 14, 18, 15, 17, 16]` — flat; 12px blue label "800ms timeout" at its right end.
- **Annotation (bold 13px red `#e74c3c`, near x=35s, y=95):** "hung calls never return threads".
- **Caption (12px `#444`, bottom right):** "thread counts illustrative; pool size 200 exact for this example".

## Three Layers, Three Tries, 27× the Traffic

**Tags:** `worked example` (blue), `retry amplification` (red), `backoff` (green)

- **The blip** — a retried call often works: a dropped packet or one bad instance clears on try two
- **The multiplication** — each layer makes 3 attempts, and every attempt fans out to the layer below
- **The arithmetic** — app 3 tries × gateway 3 × checkout 3 = 3 × 3 × 3 = 27 requests hit payments (exact)
- **The storm** — payments was already struggling; naive immediate retries turned 1 request into 27
- **The fix** — exponential backoff (wait 100ms, 200ms, 400ms) plus random jitter, and a 10% retry budget

*Example (italic):* One user tap becomes 27 payment attempts during a wobble — 3 tries at the app, each spawning 3 at the gateway, each spawning 3 at checkout: 3 × 3 × 3 = 27, exactly.

**Key point:** Retries turn transient failures into successes, but every retry is added load on a struggling dependency — space attempts with exponential backoff, add jitter so retries don't arrive in synchronized waves, and cap total retries at a budget (e.g. 10% of normal traffic).

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart of request counts per layer during a wobble: 1 request at the top multiplies into 27 at the bottom.

- **Title (bold 15px, `#1a5276`, top center):** "Retry Amplification: 3 Layers × 3 Attempts = 27× at the Bottom".
- **Layout:** 4 rows at y = 70, 115, 160, 205; left-aligned 12px `#444` row labels at x=20; bars start at x=210, 16px tall, width = 16px per request, 12px bold count labels at bar ends.
- **Rows (top to bottom):**
  - "mobile app — 1 request": blue `#2a78d6` bar width 16, label "1"
  - "API gateway — 3 requests": blue `#2a78d6` bar width 48, label "3"
  - "checkout — 9 requests": orange `#d95926` bar width 144, label "9"
  - "payment service — 27 requests": red `#e74c3c` bar width 432, bold red label "27"
- **Annotation (bold 13px red `#e74c3c`, centered near y=255):** "27× exact: 3 × 3 × 3 — the bottom layer takes the storm".
- **Caption (12px `#444`, bottom right):** "request counts exact; bar width 16px per request".

## The Breaker's Three States

**Tags:** `worked example` (blue), `state machine` (green), `fallbacks` (orange)

- **Truly down** — payments is dead for 30 minutes; even polite retries now just burn latency and load
- **CLOSED** — the normal state: calls flow through while the breaker counts recent failures
- **OPEN** — 50% of the last 20 calls failed: the breaker trips and calls fail fast, no timeout wait
- **HALF-OPEN** — after a 30s cool-off, the breaker lets one probe call through to test recovery
- **The verdict** — probe succeeds → back to CLOSED; probe fails → back to OPEN for another 30s
- **The fallback** — while OPEN, serve cached data or a default: "order received, we'll confirm by email"

*Example (italic):* At 10:00:00 payments dies; by 10:00:08 the breaker has seen 10 failures in its last 20 calls and opens — for the rest of the outage checkout answers in about 1ms with a fallback instead of waiting the full 800ms timeout on every doomed call.

**Key point:** The circuit breaker is a state machine — closed → open → half-open — that stops paying the timeout on a dependency it already knows is down and periodically probes for recovery; the fallback is what you serve while it's open.

### Visualization (canvas `c3`, 720×300)

State-machine diagram: three rounded state boxes with labeled transition arrows forming the closed → open → half-open cycle.

- **Title (bold 15px, `#1a5276`, top center):** "The Breaker State Machine: Closed → Open → Half-Open".
- **Boxes (170×50, 8px radius, 13px bold `#2c3e50` state name + 11px sub-line):**
  - CLOSED: green border `#008300`, fill `rgba(0,131,0,0.12)`, at x=60, y=125; sub-line "calls flow, failures counted"
  - OPEN: red border `#e74c3c`, fill `rgba(231,76,60,0.12)`, at x=480, y=60; sub-line "fail fast in ~1ms"
  - HALF-OPEN: orange border `#d95926`, fill `rgba(217,89,38,0.12)`, at x=480, y=200; sub-line "one probe allowed"
- **Arrows (3px, arrowheads, 12px `#444` labels midway):**
  - CLOSED → OPEN (red `#e74c3c`, arcing over the top): label "≥50% of last 20 calls fail"
  - OPEN → HALF-OPEN (mute `#6b7280`, down the right side): label "30s cool-off elapses"
  - HALF-OPEN → CLOSED (green `#008300`, back along the bottom-left): label "probe succeeds"
  - HALF-OPEN → OPEN (dashed red `#e74c3c`, dash 4/3, short arrow up): label "probe fails"
- **Annotation (bold 13px orange `#d95926`, centered near y=280):** "while OPEN, serve the fallback — cached data or a graceful default".
- **Caption (12px `#444`, bottom right):** "thresholds (50% of 20 calls, 30s cool-off) exact for this example".

## Remove One and Its Disaster Returns

**Tags:** `common mistake` (red), `composition` (blue), `idempotency` (orange)

- **The composition** — timeout bounds each attempt; retry-with-backoff handles blips; breaker handles outages
- **No timeout** — a retry can never fire because attempt one never ends; the thread-freeze returns
- **No retries** — every network blip becomes a user-visible error; a one-packet hiccup fails a real order
- **No breaker** — in a real outage every call still waits 800ms and retries pile 27× load on a corpse
- **The mistake** — retrying non-idempotent calls: a timed-out charge may have actually succeeded

*Example (italic):* A charge times out after 800ms but had already landed on the payment side; the blind retry bills the card a second time — the retry needed an idempotency key, not just backoff.

**Common mistake:** Treating the three as alternatives. They are layers: remove timeouts and hangs freeze the service, remove retries and every blip becomes an error, remove the breaker and outages become 27× retry storms — and never retry a write without an idempotency key, or one purchase becomes two charges.

### Visualization (canvas `c4`, 720×300)

Three-row flow diagram: each row removes one defense and shows the specific disaster that comes back, as cause box → arrow → red consequence box.

- **Title (bold 15px, `#1a5276`, top center):** "Remove One Defense and Its Disaster Comes Back".
- **Rows (y = 70, 140, 210), each: 12px `#444` label at x=20, blue cause box at x=185, 3px arrow, red consequence box at x=440:**
  - "remove timeout": blue box "one slow dependency" → red box "hung threads — service freezes"
  - "remove retries": blue box "one network blip" → red box "real order fails for the user"
  - "remove breaker": blue box "30-min outage" → red box "800ms waits + 27× retry load"
- **Box style:** 190–210px wide, 40px tall, 8px radius, cause fill `rgba(42,120,214,0.15)` with `#2a78d6` border, consequence fill `rgba(231,76,60,0.12)` with `#e74c3c` border, 12px `#2c3e50` text.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=278):** "each defense covers the failure mode the other two cannot".
- **Caption (12px `#444`, bottom right):** "27× exact from the amplification arithmetic; other figures from this page's example".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); thread counts and outage timings are invented and labeled illustrative; the amplification counts 1 / 3 / 9 / 27 are exact (3 × 3 × 3 = 27); backoff waits 100/200/400ms, timeout 800ms, breaker thresholds (50% of last 20 calls, 30s cool-off) are exact for this page's example and must match between text and charts.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
