# Scalability

**Page type:** grid page (tutorials category grid: single flat 4-column nav-grid of cards with topic tags)
**HTML title tag:** Scalability

**Subtitle:** Where systems break under load — the queueing math that predicts it, the resources that run out first, and the caching and capacity work that buys headroom.

## Cards

Each card links to a topic page under `scalability/`. The card shows a colored uppercase subcategory label (`.card-num`), a numbered title, a one-line description, and 2-4 topic tag pills. All cards sit in one flat `.nav-grid`; the colored labels carry the grouping.

| # | Category | Title | Link | Description | Topic tags |
|---|----------|-------|------|-------------|------------|
| 1 | PERFORMANCE MATH | Little's Law | [44-scalability/01-littles-law.md](44-scalability/01-littles-law.md) | items = rate × time — one identity that sizes every queue, from a coffee shop line to a server's thread pool. | queueing, L = λW, sizing pools |
| 2 | PERFORMANCE MATH | The M/M/1 Utilization Curve | [44-scalability/02-the-m-m-1-utilization-curve.md](44-scalability/02-the-m-m-1-utilization-curve.md) | Why systems melt at 80% busy, not 100% — average wait grows like ρ/(1−ρ), and that curve is a cliff, not a ramp. | utilization, wait time, the cliff |
| 3 | PERFORMANCE MATH | Tail Latency | [44-scalability/03-tail-latency.md](44-scalability/03-tail-latency.md) | The 99th percentile matters more than the average — a page fanning out to 100 backend calls meets the slowest 1% almost every time. | p99, fan-out, slowest 1% |
| 4 | PERFORMANCE MATH | Availability Math | [44-scalability/04-availability-math.md](44-scalability/04-availability-math.md) | "Four nines" sounds like a slogan until you convert it to minutes — and every service you call in a chain multiplies your downtime. | nines, downtime budget, chained services |
| 5 | RESOURCE EXHAUSTION | Running Out of File Descriptors | [44-scalability/05-running-out-of-file-descriptors.md](44-scalability/05-running-out-of-file-descriptors.md) | Every open file, socket, and pipe costs one descriptor from a per-process budget — and the classic default budget is only 1,024. | open files, sockets, ulimit |
| 6 | RESOURCE EXHAUSTION | Ephemeral Port Exhaustion | [44-scalability/06-ephemeral-port-exhaustion.md](44-scalability/06-ephemeral-port-exhaustion.md) | Every outbound connection borrows a local port that stays reserved after closing — open one per request and you run out of numbers. | outbound connections, TIME_WAIT, port range |
| 7 | RESOURCE EXHAUSTION | Out of Memory & the OOM Killer | [44-scalability/07-out-of-memory-and-the-oom-killer.md](44-scalability/07-out-of-memory-and-the-oom-killer.md) | Linux promises programs more memory than it has — when the bill comes due, the kernel kills one process, and it is rarely the guilty one. | overcommit, kernel, killed processes |
| 8 | RESOURCE EXHAUSTION | Memory Leaks & GC Pauses | [44-scalability/08-memory-leaks-and-gc-pauses.md](44-scalability/08-memory-leaks-and-gc-pauses.md) | A garbage collector can only free objects nobody references — a cache that never evicts keeps the heap ratcheting up until the process dies. | heap growth, garbage collection, pauses |
| 9 | RESOURCE EXHAUSTION | Database Connection Exhaustion | [44-scalability/09-database-connection-exhaustion.md](44-scalability/09-database-connection-exhaustion.md) | Every open database connection costs the server real memory and a worker — so services lend out a small pool of warm connections instead. | connection pool, warm connections, handshake cost |
| 10 | RESOURCE EXHAUSTION | Thread-Pool Starvation | [44-scalability/10-thread-pool-starvation.md](44-scalability/10-thread-pool-starvation.md) | One slow dependency can freeze an entire service — the worker pool fills up with threads that are all doing nothing but waiting. | worker pool, slow dependency, waiting threads |
| 11 | RESOURCE EXHAUSTION | CPU Saturation | [44-scalability/11-cpu-saturation.md](44-scalability/11-cpu-saturation.md) | CPU % says how busy the cores are; the run queue says how much work is waiting — and the waiting is what your users feel. | run queue, load average, busy vs backed up |
| 12 | BOTTLENECKS | Cache Breakdown | [44-scalability/12-cache-breakdown.md](44-scalability/12-cache-breakdown.md) | A cache absorbing 95% of reads shields the database — so a small hit-rate dip, an expiring hot key, or a cold restart multiplies backend load. | hit rate, load amplification, cold start |
| 13 | BOTTLENECKS | Hot Keys & Hot Shards | [44-scalability/13-hot-keys-and-hot-shards.md](44-scalability/13-hot-keys-and-hot-shards.md) | Hash sharding spreads keys perfectly evenly — but load follows the celebrities, so one hot account can melt its shard while the rest sit idle. | sharding, skewed load, celebrity keys |
| 14 | BOTTLENECKS | Lock Contention | [44-scalability/14-lock-contention.md](44-scalability/14-lock-contention.md) | When every thread must wait its turn on one lock, adding cores stops helping — Amdahl's law shows up as a production incident. | hot row, Amdahl's law, serial section |
| 15 | BOTTLENECKS | Disk Limits | [44-scalability/15-disk-limits.md](44-scalability/15-disk-limits.md) | A disk can stop you three different ways — full of bytes, out of I/O operations, or out of inodes — and "disk is fine" needs three checks. | bytes, IOPS, inodes |
| 16 | BOTTLENECKS | Bandwidth & Network Limits | [44-scalability/16-bandwidth-and-network-limits.md](44-scalability/16-bandwidth-and-network-limits.md) | A 10 Gbit/s network card moves at most 1.25 GB per second — a job that shuffles terabytes has a time floor set by the wire, not the CPU. | wire speed, throughput floor, data shuffles |
| 17 | CACHING & CAPACITY | Cache Eviction | [44-scalability/17-cache-eviction.md](44-scalability/17-cache-eviction.md) | A full cache must forget something — LRU, LFU, and TTL each forget differently, and the choice decides your hit rate. | LRU, LFU, TTL |
| 18 | CACHING & CAPACITY | Cache Invalidation & Write Policies | [44-scalability/18-cache-invalidation-and-write-policies.md](44-scalability/18-cache-invalidation-and-write-policies.md) | A cache is a copy, and every copy can go stale — the write policy decides how long the cache is allowed to lie about the source of truth. | stale copies, write-through, write-back |
| 19 | CACHING & CAPACITY | Load Testing & Capacity Planning | [44-scalability/19-load-testing-and-capacity-planning.md](44-scalability/19-load-testing-and-capacity-planning.md) | Ramp realistic traffic against a production-like copy until latency hockey-sticks — the knee is your real ceiling, and the test names the resource that sets it. | traffic ramp, latency knee, capacity ceiling |

## Regeneration instructions

To rebuild the HTML from this spec:

- **Template:** tutorials category grid. Single page: h1, `.subtitle` paragraph, then one flat `.nav-grid` of `.nav-card` anchors (no h2 section headings).
- **Layout:** `.nav-grid` is CSS grid, `repeat(4, 1fr)`, 16px gap, 15px top margin; responsive: 3 columns below 1400px, 2 below 1100px, 1 below 600px.
- **Links:** the tables above link to the `.md` versions for markdown navigation; in the regenerated HTML, each card's `href` is the same path with an `.html` extension instead.
- **Card structure:** `<a class="nav-card" href="...">` containing `<div class="card-num">SUBCATEGORY LABEL</div>`, `<h3>N. Topic Title</h3>` (unpadded index number matching the 2-digit zero-padded file index), `<p>description</p>`, then `<div class="topics">` of `<span class="topic-tag">` pills.
- **Category label colors:** applied by a small script mapping `.card-num` text to color — "PERFORMANCE MATH" `#2980b9`, "RESOURCE EXHAUSTION" `#27ae60`, "BOTTLENECKS" `#8e44ad`, "CACHING & CAPACITY" `#e67e22`; the CSS default for `.card-num` is `#2980b9`, 0.75em bold, 4px bottom margin.
- **Card style:** background `#ffffff`, border `1px solid #d8d8d8`, radius 10px, padding 20px, shadow `0 2px 4px rgba(0,0,0,0.05)`, transition on border-color/transform; hover: border `#2980b9`, `translateY(-2px)`. h3 `#1a3a4a` 1em with 6px bottom margin; description `#555` 0.85em. Topic tags: background `#f0f0f0`, border `1px solid #ccc`, radius 4px, padding 2px 6px, 0.7em `#666`, in a flex-wrap row with 4px gap and 8px top margin.
- **Page style:** body system sans-serif, background `#f5f5f0`, text `#2a2a2a`, padding 40px, line-height 1.6; universal `* { margin:0; padding:0; box-sizing:border-box }` reset; h1 1.8em `#2980b9` with 10px bottom margin; subtitle `#666` 1.05em with 30px bottom margin. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22` (page accents here use `#2980b9`, `#27ae60`, `#8e44ad`, `#e67e22`).
- **Canvases:** none on this page; any canvases elsewhere in this series use `window.devicePixelRatio` scaling.
