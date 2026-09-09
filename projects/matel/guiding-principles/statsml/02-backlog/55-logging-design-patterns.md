# Logging Design Patterns

**Page type:** detail page (backlog-style sections: 2-col text/viz layout table per section; BACKLOG status badge next to h1)
**HTML title tag:** Logging Design Patterns

**Status badge (in h1):** BACKLOG

**Subtitle:** Why traditional class-based loggers fail in distributed data pipelines — and what patterns actually work.

**Intro callout:** Class-based loggers answer "where in the code?" — but distributed pipelines need "where in the data flow?". This page collects the failure modes of traditional logging in pipelines, distributed systems, and actor systems, and the patterns that replace it.

## 1. The Problem with Class-Based Loggers

Log4J-style loggers are designed around a simple model: `Logger.getLogger(ClassName.class)`. This assumes:

- A class has one purpose → one log context.
- The class name tells you what's happening.
- Logs go to a local filesystem.

All three assumptions break in modern data systems:

- **Shared code, multiple pipelines** — The same transformation class is reused across different pipelines. `JsonParser.class` tells you nothing about *which* data flow triggered the log.
- **Pipeline identity ≠ class identity** — What matters is the DAG context: which pipeline, which stage, which partition, which retry.
- **Dynamic composition** — Pipelines are composed at runtime from reusable operators. The class hierarchy doesn't reflect the execution graph.

**Key point:** Class-based loggers answer "where in the code?" but not "where in the data flow?" — and in pipelines, the second question is the one that matters.

### Visualization (canvas `c1`, 720×340)

Fan-in diagram: three pipelines converge on one shared class whose log output loses pipeline identity.

- **Title (bold 13px, `#1a5276`, top center):** "Same Class, Different Pipelines — Logger Identity Problem".
- **Center box:** 120×50 px centered at (w/2, h/2−10); fill `rgba(26,82,118,0.1)`, 2px stroke `#1a5276`, bold 11px label "JsonParser".
- **Incoming pipelines (left, 2px lines at 0.6 alpha converging on the box's left edge, labels 11px at x=10):**
  - "Orders Pipeline" — green `#27ae60`, at y = center−70
  - "Events Pipeline" — orange `#e67e22`, at y = center
  - "Logs Pipeline" — purple `#8e44ad`, at y = center+70
- **Output (right):** gray `#999` 2px line from the box's right edge to x = w−100; two red `#e74c3c` 11px text lines at x = w−190: "Log: JsonParser.class" and "Which pipeline? 🤷".
- **Bottom caption (11px, `#888`, centered):** "Class name is constant — pipeline context is lost".

## 2. Distributed Systems: Delayed & Fragmented

In distributed systems, logs are fundamentally different from local application logs:

- **Clock skew** — Nodes disagree on time. Log ordering requires logical clocks or vector timestamps, not wall-clock.
- **Network delay** — Logs arrive out of order at the aggregator. A "first" error may appear after its downstream effects.
- **Partial visibility** — At any moment, your aggregated view is incomplete. Late-arriving logs change the story retroactively.
- **Volume asymmetry** — One noisy node can drown the signal from the node where the root cause lives.

**Philosophy callout:** **Core tension:** Distributed logging needs global correlation but has only local causality. Correlation IDs (trace IDs, span IDs) bridge this gap — but only if propagated correctly through every hop.

### Visualization (canvas `c2`, 720×340)

Three-node timeline diagram of a distributed request with out-of-order log arrival.

- **Title (bold 13px, `#1a5276`, top center):** "Distributed Logs: Out-of-Order Arrival".
- **Timelines:** three horizontal 1px `#ccc` lines for "Node A" (y=70), "Node B" (y=150), "Node C" (y=230); node names right-aligned 11px `#2c3e50`; time axis from x=80 spanning width−140.
- **Events** (5px dots with 9px labels above, positioned at fraction t of the timeline):

| Node | t | Label | Color |
|---|---|---|---|
| A | 0.10 | req start | `#27ae60` |
| A | 0.30 | call B | `#2980b9` |
| B | 0.35 | recv | `#2980b9` |
| B | 0.50 | call C | `#e67e22` |
| C | 0.55 | recv | `#e67e22` |
| C | 0.70 | ERROR | `#e74c3c` |
| B | 0.75 | timeout | `#e74c3c` |
| A | 0.90 | 500 | `#e74c3c` |

- **Propagation arrows:** dashed (3/3) 1px gray `#999` lines connecting event pairs: call B→recv, recv→call C(→recv), ERROR→timeout, timeout→500 (pairs [1,3], [3,4], [5,6], [6,7]).
- **Bottom captions (11px, `#888`, centered):** "Aggregator sees: 500 → timeout → ERROR (reverse causal order)" and "Correlation ID is the only way to reconstruct the story".

## 3. Actor Systems: No Filesystem, Only Messages

Actor-based systems (Akka, Erlang/OTP, Orleans) break the logging model further:

- **No shared state** — Actors don't share memory or files. Each actor is an island.
- **Everything is a message** — Logging itself must go through the message bus. High-throughput logging can back-pressure the system it's observing.
- **Supervision trees** — Errors propagate via supervision, not stack traces. The "cause" lives in a different actor's mailbox.
- **Lifecycle opacity** — Actors spawn, die, restart. The logger that started a trace may not exist when the trace completes.

**Key point:** In actor systems, the log IS a message — subject to the same backpressure, ordering guarantees (or lack thereof), and delivery semantics as the data it's trying to observe.

### Visualization (canvas `c3`, 720×300)

Actor-graph diagram: three actors sending log messages to a dedicated log actor.

- **Title (bold 13px, `#1a5276`, top center):** "Actor Systems: Logging via Message Bus".
- **Actors:** circles of radius 28, fill = color at ~13% alpha (`color + '22'`), 2px stroke in color, 10px centered label in color:
  - "Actor A" at (100, 100), green `#27ae60`
  - "Actor B" at (300, 80), blue `#2980b9`
  - "Actor C" at (500, 120), orange `#e67e22`
  - "Log Actor" at (350, 230), purple `#8e44ad`
- **Log messages:** dashed (4/3) 1.5px lines at 0.5 alpha from each of A/B/C (bottom edge) to the Log Actor (top edge), colored per source actor.
- **Backpressure annotation (10px, red `#e74c3c`, centered at (350, 260)):** "⚠ backpressure".
- **Bottom caption (11px, `#888`, centered):** "Log messages compete with data messages on the same bus".

## 4. Stack Traces: The Unreadable Wall

Large stack traces are a symptom of logging design failure:

- **Framework noise** — 80% of frames are framework internals (Spring, Netty, Kafka consumer loops). The signal is 2-3 lines buried in 200.
- **Async boundaries** — Stack traces in async/reactive code are meaningless. The "call stack" doesn't represent the causal chain.
- **Retry amplification** — One root failure generates N retries × M layers of wrapping. The same error appears dozens of times with slightly different stacks.

**What works instead:**

- Structured events over free-text messages
- Causal context (trace/span) over positional context (line number)
- Error classification (type + first occurrence) over stack dump repetition
- Correlation-first search over timestamp-first scroll

**Philosophy callout:** **Design principle:** A log line should answer "what happened in this data flow?" not "what line of code ran?" The code is in git. The runtime context is only in the log.

### Visualization (canvas `c4`, 720×340)

Rendered mock stack trace with signal lines highlighted against framework noise.

- **Title (bold 13px, `#1a5276`, top center):** "Stack Trace: Signal Buried in Framework Noise".
- **Stack lines** (9px monospace, left-aligned at x=30, starting y=45, 13px line height; noise lines in `#999`, signal lines in green `#27ae60` on a `rgba(39,174,96,0.12)` highlight band spanning the row):
  1. `at o.s.w.s.FrameworkServlet.service(FrameworkServlet:897)` — noise
  2. `at o.s.w.s.FrameworkServlet.service(FrameworkServlet:883)` — noise
  3. `at javax.servlet.http.HttpServlet.service(HttpServlet:750)` — noise
  4. `at o.a.c.c.ApplicationFilterChain.doFilter(AppFilter:166)` — noise
  5. `at o.s.w.f.OncePerRequestFilter.doFilter(OncePerReq:113)` — noise
  6. `at com.app.pipeline.OrderParser.transform(OrderParser:42)` — **signal**
  7. `at com.app.pipeline.JsonParser.parse(JsonParser:118)` — **signal**
  8. `at o.a.k.c.internals.Fetcher.fetchRecords(Fetcher:583)` — noise
  9. `at o.a.k.c.internals.Fetcher.fetch(Fetcher:492)` — noise
  10. `at o.a.k.c.KafkaConsumer.poll(KafkaConsumer:1206)` — noise
  11. `at o.a.k.c.KafkaConsumer.poll(KafkaConsumer:1187)` — noise
  12. `at o.s.k.l.KafkaMessageListener.run(KafkaListener:88)` — noise
  13. `at java.lang.Thread.run(Thread:748)` — noise
- **Signal annotation (right edge, green):** short 2px arrow line at the first signal row plus bold 10px right-aligned labels "← signal" and "(2 lines)".
- **Bottom captions (11px, centered):** red `#e74c3c`: "2 useful lines / 13 total = 15% signal"; gray `#888`: "Multiply by retries × layers × nodes = unreadable noise wall".

## Regeneration instructions

- **Template/layout:** backlog detail page. Body: h1 with inline `.status` badge, `.subtitle`, `.intro` callout, then four `.lang-section` divs each with an h2 (1.3rem `#1a5276`, 2px `#2980b9` bottom border) and a `table.layout` with one `<tr>`: left `td.text-col` (45%) with paragraphs, `<ul>` bullets and a `.key-point` or `.philosophy` callout; right `td.viz-col` (55%) with one canvas.
- **Page CSS:** body system-ui/-apple-system sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem. `.intro`/`.philosophy`: background `#f0f4f8`, left border 3px `#2980b9`, 0.9rem. `.key-point`: background `#f8f9fa`, left border 3px `#e74c3c`, 0.9rem. `.status` badge: background `#fef9e7`, border 1px `#f39c12`, text `#b7950b`. `code`: background `#e8f0f8`, color `#1a5276`. Canvases: 1px `#e0e0e0` border, 4px radius, `width: 100%`.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, secondary blue `#2980b9`, purple `#8e44ad`, gray `#999`/`#888`.
- **Canvas:** each canvas declares intrinsic `width`/`height` attributes as specified; a shared `setupCanvas(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates.
- No nav bar, no back/home links. This page has no outbound card links; any regenerated links elsewhere use `.html` extensions.
