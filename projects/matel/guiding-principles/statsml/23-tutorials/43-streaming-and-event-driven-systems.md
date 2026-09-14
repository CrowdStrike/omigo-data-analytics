# Streaming & Event-Driven Systems

**Page type:** grid page (tutorials category grid: single flat 4-column nav-grid of cards with topic tags)
**HTML title tag:** Streaming & Event-Driven Systems

**Subtitle:** How data that never stops arriving gets processed — append-only logs, windows, state — and how services stay responsive by reacting to events instead of waiting on calls.

## Cards

Each card links to a topic page under `streaming/`. The card shows a colored uppercase subcategory label (`.card-num`), a numbered title, a one-line description, and 2-4 topic tag pills. All cards sit in one flat `.nav-grid`; the colored labels carry the grouping.

| # | Category | Title | Link | Description | Topic tags |
|---|----------|-------|------|-------------|------------|
| 1 | STREAMS & PROCESSING | Message Queues & Kafka Concepts | [43-streaming-and-event-driven-systems/01-message-queues-and-kafka-concepts.md](43-streaming-and-event-driven-systems/01-message-queues-and-kafka-concepts.md) | A topic is a named append-only log split into partitions — consumers read at their own pace and can rewind, because reading never deletes anything. | append-only log, partitions, consumer offsets |
| 2 | STREAMS & PROCESSING | Windowing & Watermarks | [43-streaming-and-event-driven-systems/02-windowing-and-watermarks.md](43-streaming-and-event-driven-systems/02-windowing-and-watermarks.md) | A stream never ends, so you can never "count all the orders" — windows cut it into finite buckets, and watermarks decide when a bucket is safe to close. | tumbling windows, event time, late events |
| 3 | STREAMS & PROCESSING | Stateful Stream Processing | [43-streaming-and-event-driven-systems/03-stateful-stream-processing.md](43-streaming-and-event-driven-systems/03-stateful-stream-processing.md) | A running count per customer lives inside the stream engine as state — checkpoints snapshot it with the stream position, so a crash recovers to exactly where it was. | running state, checkpoints, exactly-once |
| 4 | STREAMS & PROCESSING | Stream Joins | [43-streaming-and-event-driven-systems/04-stream-joins.md](43-streaming-and-event-driven-systems/04-stream-joins.md) | Joining two streams that never finish — you can't wait for "all" rows, so every join becomes a bet about how far apart matching events can be. | join window, buffering, clicks to ads |
| 5 | EVENT-DRIVEN ARCHITECTURE | Reactive Systems | [43-streaming-and-event-driven-systems/05-reactive-systems.md](43-streaming-and-event-driven-systems/05-reactive-systems.md) | Message-driven backends stay responsive under a traffic spike — components talk through queues, so overload is shed at the door instead of hanging every caller. | backpressure, message-driven, traffic spikes |
| 6 | EVENT-DRIVEN ARCHITECTURE | Event-Driven vs Request-Driven | [43-streaming-and-event-driven-systems/06-event-driven-vs-request-driven.md](43-streaming-and-event-driven-systems/06-event-driven-vs-request-driven.md) | Two ways for services to talk: call someone and wait for the answer, or announce what happened and let whoever cares react later. | sync vs async, decoupling, publish-subscribe |

## Regeneration instructions

To rebuild the HTML from this spec:

- **Template:** tutorials category grid. Single page: h1, `.subtitle` paragraph, then one flat `.nav-grid` of `.nav-card` anchors (no h2 section headings).
- **Layout:** `.nav-grid` is CSS grid, `repeat(4, 1fr)`, 16px gap, 15px top margin; responsive: 3 columns below 1400px, 2 below 1100px, 1 below 600px.
- **Links:** the tables above link to the `.md` versions for markdown navigation; in the regenerated HTML, each card's `href` is the same path with an `.html` extension instead.
- **Card structure:** `<a class="nav-card" href="...">` containing `<div class="card-num">SUBCATEGORY LABEL</div>`, `<h3>N. Topic Title</h3>` (unpadded index number matching the 2-digit zero-padded file index), `<p>description</p>`, then `<div class="topics">` of `<span class="topic-tag">` pills.
- **Category label colors:** applied by a small script mapping `.card-num` text to color — "STREAMS & PROCESSING" `#2980b9`, "EVENT-DRIVEN ARCHITECTURE" `#27ae60`; the CSS default for `.card-num` is `#2980b9`, 0.75em bold, 4px bottom margin.
- **Card style:** background `#ffffff`, border `1px solid #d8d8d8`, radius 10px, padding 20px, shadow `0 2px 4px rgba(0,0,0,0.05)`, transition on border-color/transform; hover: border `#2980b9`, `translateY(-2px)`. h3 `#1a3a4a` 1em with 6px bottom margin; description `#555` 0.85em. Topic tags: background `#f0f0f0`, border `1px solid #ccc`, radius 4px, padding 2px 6px, 0.7em `#666`, in a flex-wrap row with 4px gap and 8px top margin.
- **Page style:** body system sans-serif, background `#f5f5f0`, text `#2a2a2a`, padding 40px, line-height 1.6; universal `* { margin:0; padding:0; box-sizing:border-box }` reset; h1 1.8em `#2980b9` with 10px bottom margin; subtitle `#666` 1.05em with 30px bottom margin. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22` (page accents here use `#2980b9`, `#27ae60`).
- **Canvases:** none on this page; any canvases elsewhere in this series use `window.devicePixelRatio` scaling.
