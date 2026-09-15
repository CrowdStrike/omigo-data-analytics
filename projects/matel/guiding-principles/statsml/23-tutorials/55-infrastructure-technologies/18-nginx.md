# Nginx

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Nginx

**Subtitle:** Nginx answers ten thousand simultaneous connections with a handful of workers — each one an event loop that sleeps until a socket has something to say

## Ten Thousand Connections, Four Workers

**Tags:** `core idea` (blue), `C10K` (green), `event loop` (orange)

- **The problem** — the late-90s "C10K problem": how can one server hold 10,000 open connections?
- **The old model** — Apache's classic design gave every connection its own thread or process
- **The cost** — 10,000 mostly-idle connections still meant 10,000 stacks parked in memory
- **The answer** — Igor Sysoev released nginx in 2004: a few workers, each running an event loop
- **The trick** — epoll (Linux) / kqueue (BSD) wake a worker only for sockets that have data ready
- **The split** — 4 workers × ~2,500 connections each; an idle socket costs a few kilobytes

*Example (italic):* A chat tab keeps its connection open for minutes between messages — nginx pays a few kilobytes for it; a thread-per-connection server pays a whole thread.

**Key point:** An event-driven server multiplexes thousands of connections per worker and only spends CPU on sockets that are actually ready — idle connections are nearly free.

### Visualization (canvas `c1`, 720×300)

Side-by-side schematic: thread-per-connection (a wall of thread boxes, one per connection) vs event-driven (4 worker boxes, each fanning out to thousands of sockets).

- **Title (bold 15px, `#1a5276`, top center):** "Same 10,000 Connections: 10,000 Threads vs 4 Event Loops".
- **Left panel (x 40–340):** 12px `#444` label "thread per connection" at (40, 62); grid of 80 small rects (12×8 px, 4px gaps, fill `rgba(42,120,214,0.30)`, 1px `#2a78d6` border), 10 columns × 8 rows starting at (60, 78); 13px `#6b7280` "⋯" centered at (190, 232); bold 12px red `#e74c3c` label "× 10,000 threads, busy or not" at (60, 258).
- **Right panel (x 380–690):** 12px `#444` label "event-driven (nginx)" at (380, 62); four rounded boxes (130×32, 8px radius, fill `rgba(0,131,0,0.12)`, 2px `#008300` border) at x=400, y = 78 / 130 / 182 / 234, labels 12px `#2c3e50` "worker 1 — epoll" … "worker 4 — epoll"; from each box's right edge, 3 thin 1px `#6b7280` fan lines to x=660 with an 11px `#6b7280` "≈2,500 sockets" label at (560, box center − 20) on the top box only.
- **Annotation (bold 13px green `#008300`, at (400, 285)):** "4 loops sleep until a socket is ready".
- **Caption (12px `#444`, bottom right):** "counts illustrative; C10K = 10,000 concurrent connections".

## The Memory Bill at 10,000 Idle Connections

**Tags:** `worked example` (blue), `memory` (green)

- **The stacks** — give each thread a 1 MB stack (illustrative): 10,000 threads ≈ 10,000 MB ≈ 10 GB
- **The workers** — 4 nginx workers at ~10 MB each (illustrative) ≈ 40 MB of process memory
- **The sockets** — nginx's docs: 10,000 inactive keep-alive connections cost about 2.5 MB
- **The total** — ~10,000 MB vs ~43 MB: about 230× less memory for the same open connections
- **The scaling** — thread memory grows with every connection; event-loop memory barely moves

*Example (italic):* Doubling to 20,000 idle connections adds another ~10 GB of thread stacks on the old model, but only ~2.5 MB of bookkeeping to nginx.

**Key point:** Under thread-per-connection, memory is a straight line through the connection count; under an event loop it is nearly flat — that difference is what solved C10K.

### Visualization (canvas `c2`, 720×300)

Line chart of memory vs open (mostly idle) connections: thread-per-connection climbs linearly to 10,000 MB; event-driven stays flat near 43 MB.

- **Title (bold 15px, `#1a5276`, top center):** "Memory vs Idle Connections: Straight Line vs Flat Line".
- **Axes:** origin x=70, baseline y=245, plot width 590, plot height 180; x = connections 0 to 10,000 with 12px `#444` tick labels at 0 / 2,500 / 5,000 / 7,500 / 10,000; y = memory in MB 0 to 10,000, gridlines `#e5e9ef` at 2,500 / 5,000 / 7,500, 12px `#444` y labels with "MB" unit ("2,500 MB" etc.).
- **Thread line:** red `#e74c3c` 3px line through connections `[0, 2500, 5000, 7500, 10000]`, MB `[0, 2500, 5000, 7500, 10000]` (1 MB per thread).
- **Event line:** green `#008300` 3px line through the same connection grid, MB `[40, 41, 41, 42, 43]` — visually hugging the baseline.
- **Labels:** bold 12px red "10,000 threads ≈ 10,000 MB" near (x≈6,800 conns, y≈75); bold 12px green "4 workers + sockets ≈ 43 MB" near (x≈5,200 conns, y≈225).
- **Annotation (bold 13px violet `#4a3aa7`, near x=2,000 conns, y=55):** "≈230× less memory at 10,000 connections".
- **Caption (12px `#444`, bottom right):** "1 MB/thread and 10 MB/worker illustrative; 2.5 MB per 10k idle connections from nginx docs; arithmetic exact".

## The Web's Front Door

**Tags:** `where it's used` (blue), `reverse proxy` (green)

- **Static files** — serves images, CSS, and JS straight from disk without waking the app servers
- **Reverse proxy** — forwards /api requests to backends and shields them from slow clients
- **Load balancer** — spreads traffic across upstream servers (round-robin, least-conn, ip-hash)
- **TLS terminator** — decrypts HTTPS once at the door so backends can speak plain HTTP inside
- **The crown** — by 2021 usage surveys, nginx had overtaken Apache as the most-used web server
- **The config** — behavior is declared per URL prefix in `location` blocks inside `nginx.conf`

*Example (italic):* `location /static/` serves files from disk; `location /api/` proxies to an upstream pool of 3 app servers — one config file, four jobs.

**Key point:** Nginx sits in front of nearly everything: one process at the edge that terminates TLS, serves static assets, and balances the rest across app servers.

### Visualization (canvas `c3`, 720×300)

Front-door flow diagram: many clients on the left, one nginx box in the middle listing its roles, disk and three app servers on the right.

- **Title (bold 15px, `#1a5276`, top center):** "One Front Door: Terminate, Serve, Balance".
- **Clients (left):** three rounded boxes (110×30, fill `rgba(42,120,214,0.15)`, 1px `#2a78d6` border) at x=30, y = 85 / 145 / 205, labels 12px `#2c3e50` "browser", "mobile app", "API client"; 12px `#6b7280` "HTTPS" beside the arrows at x≈160.
- **Nginx (center):** rounded box (170×130, 8px radius, fill `rgba(26,82,118,0.10)`, 2px `#1a5276` border) at x=240, y=95; bold 13px `#1a5276` "nginx" at its top; inside, three 11px `#2c3e50` lines "terminate TLS", "serve /static/ from disk", "proxy /api/ upstream".
- **Arrows:** 3px `#6b7280` arrows from each client box to the nginx box's left edge; from its right edge, one 3px green `#008300` arrow up to a disk box and three 3px blue `#2a78d6` arrows to the app boxes.
- **Right column:** rounded box (130×30, fill `rgba(0,131,0,0.12)`, 1px `#008300` border) at x=540, y=70 labeled "disk: /static"; three boxes (130×30, fill `rgba(42,120,214,0.15)`, 1px `#2a78d6` border) at x=540, y = 130 / 175 / 220 labeled "app server 1/2/3", each with an 11px `#6b7280` share label "34%" / "33%" / "33%" at its right.
- **Annotation (bold 13px green `#008300`, at (240, 265)):** "backends never see TLS or slow clients".
- **Caption (12px `#444`, bottom right):** "round-robin shares illustrative".

## One Blocking Call Freezes Thousands

**Tags:** `common mistake` (red), `event loop` (orange)

- **One thread** — each worker's event loop is a single thread; handlers must return quickly
- **The stall** — a handler that blocks for 2 seconds parks the entire loop, not just itself
- **The blast radius** — the worker's other ~2,500 connections all wait behind that one call
- **The fix** — hand slow work to upstream servers, thread pools (`aio threads`), or a job queue
- **The smell** — latency spikes on unrelated URLs whenever one slow endpoint gets traffic

*Example (italic):* One handler waits 2 s on a locked database row; 2,499 bystander connections on that worker freeze for the same 2 s and never learn why.

**Common mistake:** Putting slow, blocking work inside the event loop. The loop's superpower — one thread serving thousands — becomes its weakness the moment that thread stops: one stalled handler stalls every connection it multiplexes.

### Visualization (canvas `c4`, 720×300)

Timeline chart of requests served per second on two workers: worker 1 hits a 2-second blocking call and flatlines to 0; worker 2 cruises untouched.

- **Title (bold 15px, `#1a5276`, top center):** "A 2-Second Blocking Call Flatlines the Whole Worker".
- **Axes:** origin x=70, baseline y=245, plot width 590, plot height 180; x = time 0s to 10s with 12px `#444` tick labels every 2s; y = requests/sec 0 to 4,000, gridlines `#e5e9ef` at 1,000 / 2,000 / 3,000.
- **Worker 1 line:** red `#e74c3c` 3px line through seconds `[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]`, req/s `[3000, 3010, 2990, 3005, 0, 0, 3900, 3600, 3080, 3010, 3000]` — cliff to 0 at 4s, burst above 3,000 as the backlog drains, then normal.
- **Worker 2 line:** green `#008300` 3px line through the same second grid, req/s `[2995, 3005, 3000, 2990, 3010, 3000, 2995, 3005, 3000, 2990, 3000]` — flat.
- **Stall marker:** vertical dashed `#6b7280` (dash 4/3) line at 4s, 12px `#6b7280` label "blocking call starts" at its top.
- **Annotation (bold 13px red `#e74c3c`, near 5s, y=90):** "2 s stall × 2,500 waiting connections".
- **Caption (12px `#444`, bottom right):** "throughput numbers illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); memory-per-thread, worker sizes, load shares, and throughput traces are invented and labeled illustrative; the 2.5 MB per 10,000 idle keep-alive connections figure is from nginx's own documentation, and the 10,000 MB / 43 MB ≈ 230× arithmetic is exact given the stated assumptions. C10K, Igor Sysoev, the 2004 release, epoll/kqueue, and nginx overtaking Apache in 2021 usage surveys are documented facts.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
