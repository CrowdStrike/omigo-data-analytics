# HTTP/2 & HTTP/3

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** HTTP/2 & HTTP/3

**Subtitle:** HTTP/2 lets one connection carry many requests at once; HTTP/3 goes further and abandons TCP itself, so one lost packet can no longer stall the whole page

## Twenty Assets, Six Checkout Lanes

**Tags:** `core idea` (blue), `multiplexing` (green), `HTTP/2` (orange)

- **The page** — a coffee shop's online menu needs 20 assets: the HTML, 14 drink photos, 3 styles, 2 scripts
- **The old rule** — HTTP/1.1 answers one request at a time per connection; the rest wait in line
- **Six lanes** — browsers open about 6 connections per site, so the 20 assets queue 3–4 deep
- **The 2015 fix** — HTTP/2 slices every request into frames and interleaves them on ONE connection
- **Multiplexing** — all 20 requests are in flight together; no asset waits behind an unrelated one
- **Bonus** — HTTP/2 also compresses repetitive headers (HPACK), shaving bytes off every request

*Example (italic):* The 20th drink photo no longer waits behind three earlier photos on its lane — its request goes out in the same instant as the first.

**Key point:** HTTP/2's multiplexing turns six short queues into one shared pipe: every request departs immediately, and responses come back interleaved as frames.

### Visualization (canvas `c1`, 720×300)

Two-panel lane diagram: HTTP/1.1's six connections with queued asset boxes (left) vs HTTP/2's single connection carrying 20 interleaved frames (right).

- **Title (bold 15px, `#1a5276`, top center):** "One Page, 20 Assets: Six Queues vs One Multiplexed Pipe".
- **Left panel:** bold 13px `#1a5276` label "HTTP/1.1 — six connections" at (40, 52); six lanes as 1px `#e5e9ef` horizontal lines at y = `[75, 108, 141, 174, 207, 240]` from x=40 to x=330; on each lane, queued asset boxes 26px wide, 22px tall, 6px gap, starting x=44, fill `rgba(42,120,214,0.30)` with 1px `#2a78d6` border; lane loads (boxes per lane, top to bottom) = `[4, 4, 3, 3, 3, 3]` — total 20; 11px `#6b7280` label "waits" beside the last box of the first lane.
- **Right panel:** bold 13px `#1a5276` label "HTTP/2 — one connection" at (400, 52); one thick lane (3px `#1a5276` line) at y=155 from x=400 to x=705; 20 interleaved frames as 12px-wide, 24px-tall boxes at x = 402 + i×15 for i in 0..19, fills cycling `['#2a78d6', '#008300', '#d95926', '#4a3aa7', '#199e70']` at 0.55 alpha; 11px `#6b7280` label "frames from all 20 assets interleaved" centered under the lane at y=195.
- **Annotation (bold 13px green `#008300`, right panel, y=245):** "all 20 requests in flight at once".
- **Caption (12px `#444`, bottom right):** "asset counts exact, box layout schematic".

## Counting the Waves: 400 ms Becomes 100 ms

**Tags:** `worked example` (blue), `load-time math` (green)

- **The setup** — say every asset takes 100 ms to fetch (request out, bytes back), and nothing else runs
- **HTTP/1.1 math** — 20 assets over 6 lanes = waves of 6, 6, 6, 2 → 4 waves × 100 ms = 400 ms
- **HTTP/2 math** — all 20 requests leave at once on one connection → 1 wave × 100 ms = 100 ms
- **Hand-check** — lane 1 fetches assets 1, 7, 13, 19 back to back: 4 × 100 ms confirms the 400 ms
- **Fine print** — real downloads also share bandwidth, so 100 ms is a floor; the queuing delay is what vanishes

*Example (italic):* The menu page drops from 400 ms to 100 ms with zero extra bandwidth — the 300 ms saved was pure waiting in line.

**Key point:** With per-asset time fixed at 100 ms, HTTP/1.1's cost is (waves × 100 ms) and HTTP/2's is one wave — the speedup equals the queue depth, ceil(20/6) = 4×.

### Visualization (canvas `c2`, 720×300)

Gantt-style waterfall: six HTTP/1.1 lanes each running sequential 100 ms fetches, and below them one HTTP/2 lane finishing everything in the first 100 ms.

- **Title (bold 15px, `#1a5276`, top center):** "Every Asset Takes 100 ms: 4 Waves vs 1 Wave".
- **Axes:** time axis from x=170 (0 ms) to x=650 (400 ms), so 1.2 px per ms; 2px `#999` baseline at y=250; 12px `#444` tick labels "0", "100", "200", "300", "400 ms" every 100 ms with vertical `#e5e9ef` gridlines from y=45 to y=250.
- **HTTP/1.1 rows:** left-aligned 12px `#444` lane labels "lane 1"…"lane 6" at x=100, rows at y = `[60, 82, 104, 126, 148, 170]`; bars 14px tall, fill `rgba(42,120,214,0.30)`, 1px `#2a78d6` border, one bar per 100 ms fetch; bars per lane (start ms) = lane 1: `[0, 100, 200, 300]`, lane 2: `[0, 100, 200, 300]`, lanes 3–6: `[0, 100, 200]` each — 4+4+3+3+3+3 = 20 bars.
- **HTTP/2 row:** 12px `#444` label "HTTP/2" at x=100, y=215; one solid green `#008300` bar from 0 to 100 ms, 18px tall, with bold 12px white centered label "20 streams".
- **Finish markers:** vertical dashed `#6b7280` (dash 4/3) lines at 100 ms and 400 ms; bold 12px green `#008300` label "HTTP/2 done: 100 ms" by the first, bold 12px orange `#d95926` label "HTTP/1.1 done: 400 ms" by the second.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=280):** "queuing, not bandwidth, cost the 300 ms".
- **Caption (12px `#444`, bottom right):** "100 ms per asset illustrative".

## One Lost Packet on the Bus Ride Home

**Tags:** `where it's used` (blue), `head-of-line blocking` (red), `HTTP/3 / QUIC` (orange)

- **The catch** — TCP delivers bytes strictly in order, so ONE lost packet stalls all 20 streams at once
- **Head-of-line blocking** — HTTP/2 fixed the queue at the HTTP layer but inherited TCP's stall below it
- **Lossy networks** — on a bus's flaky cellular link, 1–2% packet loss makes those stalls constant
- **The 2022 fix** — HTTP/3 replaces TCP with QUIC, a new transport protocol built on top of UDP
- **Independent streams** — QUIC retransmits per stream: photo #7's lost packet delays only photo #7
- **Faster starts** — QUIC folds the TLS handshake into its own, saving a round trip on new connections

*Example (italic):* At 2% packet loss the menu page takes ~650 ms over HTTP/2-on-TCP but ~170 ms over HTTP/3 in this illustration — the gap is all TCP stalls.

**Key point:** HTTP/2 removed queuing inside HTTP but not inside TCP; HTTP/3 abandons TCP for QUIC so that a lost packet blocks one stream instead of all twenty.

### Visualization (canvas `c3`, 720×300)

Line chart of page load time vs packet loss rate: HTTP/2 over TCP climbs steeply as every loss stalls the whole connection; HTTP/3 over QUIC climbs gently.

- **Title (bold 15px, `#1a5276`, top center):** "Page Load Time as the Network Gets Lossy".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = packet loss 0% to 2% with 12px `#444` tick labels every 0.5%; y = load time 0 to 700 ms, gridlines `#e5e9ef` at 175/350/525 with 12px `#444` labels.
- **HTTP/2-on-TCP line:** orange `#d95926` 3px line through loss `[0, 0.5, 1, 1.5, 2]` (%), load `[100, 180, 300, 460, 650]` (ms), 4px dots; 12px orange label "HTTP/2 (TCP)" near the last point.
- **HTTP/3-on-QUIC line:** green `#008300` 3px line through the same loss grid, load `[100, 115, 130, 150, 170]` (ms), 4px dots; 12px green label "HTTP/3 (QUIC)" near its last point.
- **Annotation (bold 13px orange `#d95926`, near x=1%, y=80):** "one lost packet stalls all 20 TCP streams".
- **Caption (12px `#444`, bottom right):** "load times illustrative; 0% column matches the 100 ms worked example".

## "UDP Means Unreliable" — Not for QUIC

**Tags:** `common mistake` (red), `what QUIC rebuilds` (orange)

- **The worry** — UDP drops packets without apology, so HTTP/3 sounds like a reliability downgrade
- **The reality** — QUIC rebuilds acknowledgements, retransmission, and congestion control above UDP
- **Per stream** — delivery order is guaranteed inside each stream, just not across unrelated streams
- **Built-in TLS** — QUIC bakes in TLS 1.3 encryption; there is no plaintext flavor of HTTP/3
- **Why UDP then** — UDP is simply the packet doorway every OS and firewall already lets through
- **The mistake** — benchmarking HTTP/3 on a flawless office network and concluding it "does nothing"

*Example (italic):* On the 0%-loss column of the chart above, HTTP/2 and HTTP/3 tie at 100 ms — QUIC's win only appears once packets start going missing.

**Common mistake:** Treating "runs on UDP" as "unreliable." QUIC re-implements everything TCP guaranteed — delivery, ordering, congestion control — but scoped per stream, which is exactly the point.

### Visualization (canvas `c4`, 720×300)

Side-by-side protocol stack diagram: the HTTP/2 stack (TCP guarantees ordering for the whole connection) vs the HTTP/3 stack (QUIC rebuilds guarantees per stream over bare UDP).

- **Title (bold 15px, `#1a5276`, top center):** "What QUIC Rebuilds Above UDP".
- **Column labels (bold 13px `#1a5276`):** "HTTP/2 stack" centered over x=110–310 at y=55; "HTTP/3 stack" centered over x=430–630 at y=55.
- **Left column (boxes 200px wide at x=110, 50px tall, 8px radius, y = 70 / 132 / 194, 12px `#2c3e50` centered text):** "HTTP/2 — requests as frames" fill `rgba(42,120,214,0.15)` border `#2a78d6`; "TLS — encryption (separate layer)" fill `rgba(74,58,167,0.12)` border `#4a3aa7`; "TCP — reliability + ordering for the WHOLE connection" fill `rgba(217,89,38,0.12)` border `#d95926`.
- **Right column (same geometry at x=430):** "HTTP/3 — requests as streams" fill `rgba(42,120,214,0.15)` border `#2a78d6`; "QUIC — reliability, per-stream ordering, TLS 1.3 built in" fill `rgba(0,131,0,0.12)` border `#008300`; "UDP — just addressed packets" fill `rgba(107,114,128,0.12)` border `#6b7280`.
- **Annotation (bold 13px green `#008300`, centered near y=280):** "reliability didn't disappear — it moved into QUIC, per stream".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the 100 ms per-asset time and the loss-vs-load-time curves (`[100, 180, 300, 460, 650]` TCP / `[100, 115, 130, 150, 170]` QUIC) are invented and labeled illustrative; the 20-asset count, 6-connections-per-site browser habit, wave arithmetic (6+6+6+2 → 4 × 100 ms = 400 ms vs 1 × 100 ms), and protocol facts (HTTP/2 multiplexing + HPACK, 2015; HTTP/3 over QUIC over UDP with built-in TLS 1.3, 2022) are exact.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
