# Database Connection Exhaustion

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Database Connection Exhaustion

**Subtitle:** Every open database connection costs the server real memory and a worker process — so services keep a small pool of warm connections and lend them out instead of opening one per request

## The Checkout That Pays for a Handshake Every Time

**Tags:** `core idea` (blue), `why pool` (green), `Postgres` (orange)

- **The service** — an orders service hits Postgres once per checkout to insert the order row
- **The naive way** — open a fresh connection for every request, run the query, close it
- **The setup tax** — TCP handshake 1 ms + auth 4 ms + session setup 3 ms before the 2 ms query
- **The server side** — Postgres forks one backend process per connection; connect storms flood it
- **The pool** — keep a few warm connections open; a request borrows one (~0.1 ms) and returns it

*Example (italic):* Connection-per-request spends 10 ms per checkout, 8 ms of it pure setup; a pooled checkout spends 2.1 ms — the query plus a borrow.

**Key point:** A connection pool exists because opening a connection is expensive on both ends — latency for the client, a whole process and its memory for the database.

### Visualization (canvas `c1`, 720×300)

Horizontal stacked-bar comparison of one request's latency: fresh connection per request (four segments) vs borrowing from a pool (two segments), on a shared millisecond scale.

- **Title (bold 15px, `#1a5276`, top center):** "One Checkout: 8 ms of Setup for a 2 ms Query".
- **Layout:** bars start at x=190, scale 50 px per ms (10 ms = 500 px), bar height 34; row labels 12px `#444` right-aligned at x=180; a light 2px `#999` baseline under each bar; ms axis ticks 12px `#444` at 0/2/4/6/8/10 ms along y=255.
- **Row 1 (y=95), label "new connection each time":** segments TCP `#2a78d6` 1 ms, auth `#d95926` 4 ms, session setup `#c98500` 3 ms, query `#008300` 2 ms; 11px white segment labels ("TCP 1", "auth 4", "setup 3", "query 2"); bold 12px `#e74c3c` total "10 ms" at bar end.
- **Row 2 (y=185), label "borrow from pool":** segments borrow `#6b7280` 0.1 ms (drawn 6 px min-width), query `#008300` 2 ms; bold 12px `#008300` total "2.1 ms" at bar end.
- **Annotation (bold 13px `#d95926`, near x=420, y=150):** "80% of the slow bar is setup, not the query".
- **Caption (12px `#444`, bottom right):** "timings illustrative; segment order is the real connect sequence".

## Sizing the Pool with Little's Law

**Tags:** `worked example` (blue), `Little's Law` (green)

- **The law** — connections busy on average = queries per second × seconds each query holds one
- **The numbers** — the orders service runs 200 queries/s and each query takes 20 ms (0.020 s)
- **Hand-check** — 200 × 0.020 = 4 connections busy on average; that is the whole calculation
- **The headroom** — bursts run ~3× the average, so a pool of 12–15 covers spikes comfortably
- **The scaling** — the need grows linearly: double the traffic or the query time, double the pool

*Example (italic):* At 200 queries/s × 20 ms the service needs 4 busy connections on average; at 800 queries/s the same math says 16.

**Key point:** Pool size is not a guess — Little's Law (arrival rate × holding time) gives the average number of connections in use, and a modest multiple of it covers bursts.

### Visualization (canvas `c2`, 720×300)

Line chart of connections needed vs load for a fixed 20 ms query time, with the worked-example point highlighted.

- **Title (bold 15px, `#1a5276`, top center):** "Little's Law: queries/s × query time = connections busy".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = load 0 to 800 queries/s, 12px `#444` tick labels at 0/200/400/600/800; y = busy connections 0 to 20, gridlines `#e5e9ef` at 5/10/15.
- **Line:** blue `#2a78d6` 3px through points at queries/s `[50, 100, 200, 400, 800]`, busy connections `[1, 2, 4, 8, 16]`; 5px `#2a78d6` dots at each point with 12px value labels above.
- **Worked-example marker:** the (200, 4) point drawn as an 8px green `#008300` ring, dashed `#6b7280` (dash 4/3) drop lines to both axes, bold 12px green label "200 q/s × 20 ms = 4" beside it.
- **Annotation (bold 13px violet `#4a3aa7`, near x=520 q/s, upper area):** "double the traffic, double the connections".
- **Caption (12px `#444`, bottom right):** "20 ms query time held fixed; points exact from the formula".

## Too Small Queues, Too Big Thrashes

**Tags:** `where it goes wrong` (blue), `saturation` (red), `max_connections` (orange)

- **Too small** — with only 4 connections at 200 q/s the pool runs 100% busy; any burst waits in line
- **The queue** — requests block at the pool, not the database; p99 latency explodes near saturation
- **Too big** — hundreds of open connections mean hundreds of Postgres backend processes competing
- **The thrash** — past the sweet spot, CPU switches and lock contention make every query slower
- **The ceiling** — Postgres refuses logins beyond `max_connections`, which defaults to 100

*Example (italic):* Growing the pool from 16 to 256 drops illustrative throughput from 9,600 to 4,800 queries/s — half the work with sixteen times the connections.

**Key point:** Throughput rises with pool size only until the database's cores are busy; after that more connections just add queueing and contention inside the database itself.

### Visualization (canvas `c3`, 720×300)

Curve of database throughput vs pool size: rises, peaks, then falls, with the region past the default `max_connections` shaded as refused territory.

- **Title (bold 15px, `#1a5276`, top center):** "More Connections Is Not More Throughput".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = pool size, categorical evenly-spaced ticks `[2, 4, 8, 16, 32, 64, 128, 256]` (12px `#444`); y = queries/s 0 to 10,000, gridlines `#e5e9ef` at 2,500/5,000/7,500, labels "2.5k/5k/7.5k/10k".
- **Curve:** blue `#2a78d6` 3px line through pool sizes `[2, 4, 8, 16, 32, 64, 128, 256]`, throughput `[1900, 3800, 7400, 9600, 9800, 9100, 7200, 4800]`; 5px dots at each point.
- **Sweet spot:** green `#008300` 8px ring on the (32, 9800) point, bold 12px green label "sweet spot" above it.
- **Refusal zone:** vertical dashed red `#e74c3c` (dash 4/3) line between the 64 and 128 ticks marking pool size 100, 12px red label "max_connections = 100 (Postgres default)" at its top; light red fill `rgba(231,76,60,0.06)` from that line to the right edge.
- **Annotation (bold 13px `#d95926`, near the 256 tick, y=120):** "256 connections: half the throughput of 32".
- **Caption (12px `#444`, bottom right):** "throughput illustrative; 128 and 256 assume max_connections raised above the 100 default".

## One Pool Per Instance Still Multiplies

**Tags:** `common mistake` (red), `autoscaling` (orange)

- **The setup** — each orders-service instance carries its own pool of 20 warm connections
- **The math** — 5 instances × 20 = 100 connections: exactly at the Postgres default, and it works
- **The spike** — the autoscaler adds instances under load: 10 instances want 200, 25 want 500
- **The failure** — connection 101 onward is refused; new instances crash-loop at startup
- **The fixes** — shrink per-instance pools, cap instances, or share one server-side pooler

*Example (italic):* A pool of 20 looked tiny per instance, but at 25 autoscaled instances the fleet demands 500 connections against a limit of 100.

**Common mistake:** Sizing the pool per instance and forgetting the multiplication — total demand is pool size × instance count, and the autoscaler changes the instance count without asking the database.

### Visualization (canvas `c4`, 720×300)

Bar chart of total connections demanded as the instance count scales, against a dashed line at the database's default connection limit.

- **Title (bold 15px, `#1a5276`, top center):** "Pool of 20 × Instances: the Fleet Blows the Limit".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = total connections 0 to 500, gridlines `#e5e9ef` at 100/200/300/400; x = three bar groups centered at x = 180 / 360 / 540 with 12px `#444` labels "5 instances", "10 instances", "25 instances".
- **Bars (90px wide):** totals `[100, 200, 500]` (scale 0.36 px per connection: heights 36 / 72 / 180); the portion of each bar at or below 100 filled `rgba(42,120,214,0.30)` with 2px `#2a78d6` edge, the portion above 100 filled `rgba(231,76,60,0.25)` with 2px `#e74c3c` edge; bold 12px value labels on top ("100" in `#2a78d6`, "200" and "500" in `#e74c3c`).
- **Limit line:** horizontal dashed red `#e74c3c` (dash 4/3) across the plot at the 100-connection height, 12px red label "max_connections = 100 (Postgres default)" above its left end.
- **Annotation (bold 13px `#e74c3c`, near the 25-instances bar, y=90):** "400 connection attempts refused".
- **Caption (12px `#444`, bottom right):** "pool size 20 per instance; counts exact from the multiplication".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); connect timings, throughput curve, and burst multiples are invented and labeled illustrative; Little's Law points (200 × 0.020 = 4, etc.), the fleet totals (100 / 200 / 500), and the Postgres default `max_connections = 100` are exact.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
