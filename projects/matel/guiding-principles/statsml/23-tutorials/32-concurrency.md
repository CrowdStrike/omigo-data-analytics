# Concurrency

**Page type:** grid page (tutorials category grid: single flat 4-column nav-grid of cards with topic tags)
**HTML title tag:** Concurrency

**Subtitle:** How programs do many things at once — the races, deadlocks, and hardware quirks that come with sharing, and the classic patterns for coordinating work safely.

## Cards

Each card links to a topic page under `concurrency/`. The card shows a colored uppercase subcategory label (`.card-num`), a numbered title, a one-line description, and 2-4 topic tag pills. All cards sit in one flat `.nav-grid`; the colored labels carry the grouping.

| # | Category | Title | Link | Description | Topic tags |
|---|----------|-------|------|-------------|------------|
| 1 | FOUNDATIONS | Concurrency vs Parallelism | [32-concurrency/01-concurrency-vs-parallelism.md](32-concurrency/01-concurrency-vs-parallelism.md) | One worker juggling many tasks versus many workers each doing a task at the same instant — dealing with many vs doing many at once. | juggling vs doing, threads, cores |
| 2 | LOCKS & SHARED STATE | Race Conditions & Mutexes | [32-concurrency/02-race-conditions-and-mutexes.md](32-concurrency/02-race-conditions-and-mutexes.md) | Two threads update one shared number at the same time and one update silently vanishes — a mutex makes them take turns. | lost update, shared state, take turns |
| 3 | LOCKS & SHARED STATE | Deadlock | [32-concurrency/03-deadlock.md](32-concurrency/03-deadlock.md) | Two threads each hold one lock and wait for the other's — like two baristas each gripping one machine, both waiting forever. | circular wait, lock ordering, frozen forever |
| 4 | LOCKS & SHARED STATE | Lock-Free Thinking | [32-concurrency/04-lock-free-thinking.md](32-concurrency/04-lock-free-thinking.md) | Compare-and-swap writes a new value only if the old one still holds — one uninterruptible step, so threads coordinate without ever taking a lock. | compare-and-swap, atomic step, retry loop |
| 5 | LOCKS & SHARED STATE | Concurrent Hash Maps | [32-concurrency/05-concurrent-hash-maps.md](32-concurrency/05-concurrent-hash-maps.md) | One big lock makes every thread queue behind one shared map — a lock per bucket keeps the safety but lets threads wait only when they truly collide. | lock striping, per-bucket locks, check-then-act |
| 6 | HARDWARE REALITY | The Hardware Memory Model | [32-concurrency/06-the-hardware-memory-model.md](32-concurrency/06-the-hardware-memory-model.md) | Each CPU core drafts its writes in a private buffer first, so two threads can watch the same memory and see writes land in different orders. | store buffer, reordering, memory fences |
| 7 | HARDWARE REALITY | Cache Coherence & False Sharing | [32-concurrency/07-cache-coherence-and-false-sharing.md](32-concurrency/07-cache-coherence-and-false-sharing.md) | Cores copy memory in 64-byte lines and only one may write a line at a time — two cores updating different variables on the same line still fight over it. | cache lines, line ping-pong, padding |
| 8 | CLASSIC PROBLEMS | Producer-Consumer | [32-concurrency/08-producer-consumer.md](32-concurrency/08-producer-consumer.md) | One side makes work, the other side does it, and a fixed-size buffer between them forces the fast side to wait for the slow side. | bounded buffer, backpressure, queues |
| 9 | CLASSIC PROBLEMS | Dining Philosophers | [32-concurrency/09-dining-philosophers.md](32-concurrency/09-dining-philosophers.md) | Five identical diners each grab their left fork at the same moment and all starve — deadlock caused not by a bug, but by perfect symmetry. | perfect symmetry, starvation, fork ordering |
| 10 | CLASSIC PROBLEMS | Readers-Writers | [32-concurrency/10-readers-writers.md](32-concurrency/10-readers-writers.md) | Many people can read the same data at the same time, but a writer needs it alone — shared reads, exclusive writes. | read-write lock, shared reads, exclusive write |
| 11 | MESSAGE PASSING | The Actor Model & Erlang | [32-concurrency/11-the-actor-model-and-erlang.md](32-concurrency/11-the-actor-model-and-erlang.md) | Every worker keeps its own private state and talks only by sending messages — share nothing, and ten machines look the same as one. | share nothing, mailboxes, distribution |

## Regeneration instructions

To rebuild the HTML from this spec:

- **Template:** tutorials category grid. Single page: h1, `.subtitle` paragraph, then one flat `.nav-grid` of `.nav-card` anchors (no h2 section headings).
- **Layout:** `.nav-grid` is CSS grid, `repeat(4, 1fr)`, 16px gap, 15px top margin; responsive: 3 columns below 1400px, 2 below 1100px, 1 below 600px.
- **Links:** the tables above link to the `.md` versions for markdown navigation; in the regenerated HTML, each card's `href` is the same path with an `.html` extension instead.
- **Card structure:** `<a class="nav-card" href="...">` containing `<div class="card-num">SUBCATEGORY LABEL</div>`, `<h3>N. Topic Title</h3>` (unpadded index number matching the 2-digit zero-padded file index), `<p>description</p>`, then `<div class="topics">` of `<span class="topic-tag">` pills.
- **Category label colors:** applied by a small script mapping `.card-num` text to color — "FOUNDATIONS" `#2980b9`, "LOCKS & SHARED STATE" `#27ae60`, "HARDWARE REALITY" `#8e44ad`, "CLASSIC PROBLEMS" `#e67e22`, "MESSAGE PASSING" `#c0392b`; the CSS default for `.card-num` is `#2980b9`, 0.75em bold, 4px bottom margin.
- **Card style:** background `#ffffff`, border `1px solid #d8d8d8`, radius 10px, padding 20px, shadow `0 2px 4px rgba(0,0,0,0.05)`, transition on border-color/transform; hover: border `#2980b9`, `translateY(-2px)`. h3 `#1a3a4a` 1em with 6px bottom margin; description `#555` 0.85em. Topic tags: background `#f0f0f0`, border `1px solid #ccc`, radius 4px, padding 2px 6px, 0.7em `#666`, in a flex-wrap row with 4px gap and 8px top margin.
- **Page style:** body system sans-serif, background `#f5f5f0`, text `#2a2a2a`, padding 40px, line-height 1.6; universal `* { margin:0; padding:0; box-sizing:border-box }` reset; h1 1.8em `#2980b9` with 10px bottom margin; subtitle `#666` 1.05em with 30px bottom margin. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22` (page accents here use `#2980b9`, `#27ae60`, `#8e44ad`, `#e67e22`, `#c0392b`).
- **Canvases:** none on this page; any canvases elsewhere in this series use `window.devicePixelRatio` scaling.
