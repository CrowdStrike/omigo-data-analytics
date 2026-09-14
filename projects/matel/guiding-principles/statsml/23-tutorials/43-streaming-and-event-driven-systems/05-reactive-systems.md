# Reactive Systems

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Reactive Systems

**Subtitle:** Message-driven backends stay responsive under a traffic spike — components talk through queues, so overload is shed at the door instead of hanging every caller

## The 10am Ticket On-Sale That Melts a Server

**Tags:** `core idea` (blue), `message-driven` (green), `traffic spike` (orange)

- **The on-sale** — a ticket-booking backend idles at 200 requests/s; at 10:01am a tour goes on sale and 2,000 requests/s arrive
- **The old design** — thread-per-request: each booking holds a thread while it waits ~500 ms on the payment gateway
- **The collapse** — all 200 threads block at once, new requests pile up on sockets, and p95 latency climbs to 30 s
- **The reactive design** — requests become messages on a bounded queue; stateless workers pull them at their own pace
- **The name** — systems built this way are called reactive: responsive, resilient, elastic, and message-driven (the Reactive Manifesto's four traits)

*Example (italic):* At 10:04am the thread-blocking service answers in 28 s (mostly timeouts) while the message-driven one still answers in about 1 s.

**Key point:** A reactive system decouples components with asynchronous messages, so a spike fills a queue it can measure and shed — instead of silently freezing every thread in the building.

### Visualization (canvas `c1`, 720×300)

Timeline chart of p95 response time through the on-sale spike: thread-blocking service (latency cliff) vs message-driven service (small bump), on a shared time axis.

- **Title (bold 15px, `#1a5276`, top center):** "10:01am On-Sale: Thread-Blocking Hits 30 s, Message-Driven Stays Near 1 s".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = time "10:00" to "10:10" with 12px `#444` tick labels every 2 minutes; y = p95 response seconds 0 to 32, gridlines `#e5e9ef` at 8/16/24 with 12px `#444` labels.
- **Thread-blocking line:** red `#e74c3c` 3px line through minutes `[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]`, seconds `[0.4, 0.5, 3, 12, 28, 30, 30, 30, 29, 28, 26]` — steep climb after the spike, pinned at the 30 s client timeout.
- **Message-driven line:** green `#008300` 3px line through the same minutes, seconds `[0.4, 0.5, 0.9, 1.4, 1.1, 0.8, 0.6, 0.5, 0.5, 0.4, 0.4]` — small bump, quick recovery.
- **Spike marker:** vertical dashed `#6b7280` (dash 4/3) line at minute 1, 12px `#6b7280` label "on-sale opens — 2,000 req/s" at its top.
- **Annotation (bold 13px green `#008300`, near minute 6, y=95):** "queue + backpressure keep answers near 1 s".
- **Caption (12px `#444`, bottom right):** "latencies illustrative".

## The Thread Math: 200 Threads, 400 Bookings a Second

**Tags:** `worked example` (blue), `backpressure` (green)

- **Capacity** — each request blocks a thread for 0.5 s, so 200 threads serve at most 200 ÷ 0.5 = 400 requests/s
- **The gap** — the spike delivers 2,000 requests/s: 1,600/s more than the blocking service can ever finish
- **Message version** — 8 workers each process 50 messages/s, the same 400/s baseline, but nothing holds a thread while waiting
- **Bounded queue** — the queue caps at 5,000 messages; once full, extra requests get a 5 ms "you're in line, retry shortly" reply
- **Hand-check** — at 8 workers, 400/s of the 2,000/s are served and the other 1,600/s are shed fast instead of hanging 30 s

*Example (italic):* Same hardware, same 400/s baseline throughput — the blocking service turns the extra 1,600/s into 30 s hangs, the reactive one into 5 ms retry replies.

**Key point:** Backpressure means the overloaded side says "not now" cheaply and immediately; the load doesn't vanish, but it stops taking healthy requests down with it.

### Visualization (canvas `c2`, 720×300)

Vertical bar chart comparing the incoming spike rate with each design's throughput, with the thread-math annotation spelled out.

- **Title (bold 15px, `#1a5276`, top center):** "2,000 req/s Arrive — Who Can Serve Them?".
- **Axes:** baseline 2px `#999` at y=245, plot from x=80 to x=680; y = requests/s 0 to 2,000, gridlines `#e5e9ef` at 500/1000/1500 with 12px `#444` labels at x=70 right-aligned.
- **Bars (each 110px wide, value labels bold 13px above bar tops, category labels 12px `#444` below baseline):**
  - x=130: "incoming spike" — fill `rgba(42,120,214,0.30)` with 2px `#2a78d6` edge, height for 2,000, label "2,000/s".
  - x=310: "thread-per-request" — solid red `#e74c3c`, height for 400, label "400/s served"; bold 12px red note "rest hang until timeout" above it at y≈130.
  - x=490: "message-driven, 8 workers" — solid green `#008300`, height for 400, label "400/s served"; bold 12px green note "rest shed in 5 ms" above it at y≈130.
- **Annotation (bold 13px violet `#4a3aa7`, top left near x=90, y=60):** "200 threads ÷ 0.5 s block = 400/s — the math nobody repealed".
- **Caption (12px `#444`, bottom right):** "rates illustrative; 8 workers × 50 msg/s = 400/s".

## Elastic Under Load: Watch the Queue, Scale the Workers

**Tags:** `where it's used` (blue), `elastic` (green), `resilient` (orange)

- **Load made visible** — queue depth is a number an autoscaler can read; blocked threads are invisible until the crash
- **Elasticity** — workers scale 8 → 40 during the spike; 40 × 50 = 2,000 messages/s matches the incoming rate
- **Failure isolation** — if the payment gateway dies, its messages wait in the queue; browsing and seat maps keep working
- **Where you meet it** — Kafka consumer groups, actor systems like Erlang or Akka, serverless functions scaling on queue depth
- **The trade** — every hop becomes eventually-consistent and asynchronous, which is harder to trace and reason about

*Example (italic):* The queue rides its 5,000 cap at 10:02–10:03am and is back to 4,900 by 10:04am; by then 40 workers match the spike, and the backlog drains to zero by 10:10am.

**Key point:** Message-driven is the mechanism, the other three traits are the payoff — the queue makes load measurable (elastic), failures local (resilient), and latency bounded (responsive).

### Visualization (canvas `c3`, 720×300)

Dual-line chart over the spike: queue depth (left axis, area) and worker count (right axis, step line), showing the scale-up catching the backlog.

- **Title (bold 15px, `#1a5276`, top center):** "Queue Depth Triggers the Scale-Up: 8 → 40 Workers".
- **Axes:** origin x=60, baseline y=245, plot width 580, plot height 180; x = "10:00" to "10:10", 12px `#444` ticks every 2 minutes; left y = queued messages 0 to 5,000, gridlines `#e5e9ef` at 1,250/2,500/3,750; right y (labels at x=655, 12px `#d95926`) = workers 0 to 40.
- **Queue cap line:** dashed `#e74c3c` (dash 5/4) horizontal line at 5,000 with 12px red label "cap 5,000 — overflow shed" at its left end.
- **Queue depth:** blue fill `rgba(42,120,214,0.25)` under a 2px `#2a78d6` line through minutes `[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]`, depth `[0, 4900, 5000, 5000, 4900, 3800, 2200, 900, 300, 100, 0]`.
- **Worker count:** orange `#d95926` 3px step line, same minutes, workers `[8, 8, 16, 32, 40, 40, 40, 40, 16, 8, 8]`.
- **Annotation (bold 13px green `#008300`, near minute 7, y=100):** "40 × 50/s = 2,000/s — backlog drains".
- **Caption (12px `#444`, bottom right):** "depths and worker counts illustrative".

## Async Alone Is Not Reactive

**Tags:** `common mistake` (red), `unbounded queue` (orange)

- **The confusion** — bolting a message queue onto the design and calling it reactive, with no bound and no backpressure
- **Unbounded mailbox** — at a 1,600/s deficit the queue grows by ~96,000 messages per minute, quietly
- **The new crash** — the failure moves from thread exhaustion to memory exhaustion; same outage, worse diagnosis
- **Stale work** — even before crashing, a huge backlog means serving 10-minute-old booking requests nobody still wants
- **Hidden blocking** — one synchronous call inside an "async" worker re-creates the thread-per-request problem in miniature

*Example (italic):* Unbounded, the queue holds ~384,000 messages by 10:05am and the worker process dies of memory; bounded at 5,000, it never exceeds its cap.

**Common mistake:** Treating the queue as infinite. A queue without a bound doesn't remove overload — it hides it until memory runs out; backpressure means the system must be able to say no.

### Visualization (canvas `c4`, 720×300)

Line chart of queue depth for the same spike under two policies: unbounded (runaway growth ending in a crash) vs bounded-with-shedding (flat at the cap).

- **Title (bold 15px, `#1a5276`, top center):** "Same Spike, Two Queues: Unbounded Grows Until Memory Dies".
- **Axes:** origin x=70, baseline y=245, plot width 590, plot height 180; x = "10:00" to "10:06", 12px `#444` ticks every minute; y = queued messages in thousands, 0 to 400, gridlines `#e5e9ef` at 100/200/300 with 12px `#444` labels "100k"/"200k"/"300k".
- **Unbounded line:** red `#e74c3c` 3px line through minutes `[0, 1, 2, 3, 4, 5]`, thousands `[0, 0, 96, 192, 288, 384]` — linear climb at the 1,600/s deficit, ending in a bold 16px red "✗" at the last point with bold 12px red label "worker OOMs at 10:05".
- **Bounded line:** green `#008300` 3px line, same minutes, thousands `[0, 0, 5, 5, 5, 5]` — hugs the baseline; bold 12px green label "bounded at 5k, overflow shed" with a short green arrow pointing to the line near minute 4.
- **Annotation (bold 13px orange `#d95926`, centered near y=70):** "no backpressure = same outage, new symptom".
- **Caption (12px `#444`, bottom right):** "1,600/s deficit × 60 s ≈ 96k msgs/min; depths illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); request rates, latencies, queue depths, and worker counts are invented and labeled illustrative; the arithmetic identities (200 ÷ 0.5 = 400, 8 × 50 = 400, 40 × 50 = 2,000, 1,600 × 60 = 96,000) are exact and the chart values must keep them consistent.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
