# DNS

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** DNS

**Subtitle:** Before your browser can fetch a page it must turn a name like www.example.com into a numeric address — DNS is the phone book that answers, one question at a time, all the way from the root

## From a Name to a Number

**Tags:** `core idea` (blue), `name lookup` (green), `resolution chain` (orange)

- **The click** — a laptop in a coffee shop types www.example.com; the network only routes numbers
- **The need** — the browser must learn the server's IP address, 203.0.113.7, before sending anything
- **The stub** — the laptop doesn't know it; it asks its configured resolver, "what is www.example.com?"
- **The resolver** — the resolver doesn't know either, but it knows where the 13 root name servers live
- **The chain** — root points to the .com servers, .com points to example.com's servers, they answer
- **The delegation** — no single machine holds the whole phone book; each level names the next

*Example (italic):* The resolver asks root ("ask .com"), then .com ("ask ns1.example.com"), then ns1 answers "www is 203.0.113.7" — three questions, one address.

**Key point:** DNS is a distributed directory: nobody stores every name, but every name is reachable by walking root → TLD → authoritative, each hop delegating to the next.

### Visualization (canvas `c1`, 720×300)

Flow diagram of one full lookup: laptop asks the resolver, the resolver walks root → .com TLD → authoritative, then returns the IP.

- **Title (bold 15px, `#1a5276`, top center):** "One Lookup for www.example.com, Traced to the Root".
- **Left boxes:** blue `#2a78d6` rounded box "laptop" at (x=30, y=140, 110×44); ink `#1a5276` box "resolver" at (x=210, y=140, 130×44).
- **Right boxes (stacked):** aqua `#199e70` box "root server" at (x=480, y=52, 180×40); violet `#4a3aa7` box ".com TLD server" at (x=480, y=124, 180×40); green `#008300` box "ns1.example.com (authoritative)" at (x=480, y=196, 180×40).
- **Box style:** 8px radius, fills at 0.12 alpha of each stroke color, 2px stroke, 12px `#2c3e50` labels centered.
- **Arrows (2px `#6b7280`, numbered bold 12px in each hop color):** 1 laptop→resolver "what is www.example.com?"; 2 resolver→root, reply italic 11px "ask .com"; 3 resolver→TLD, reply "ask ns1.example.com"; 4 resolver→authoritative, reply "203.0.113.7"; 5 resolver→laptop bold green "203.0.113.7".
- **Annotation (bold 13px `#4a3aa7`, bottom center near y=282):** "no server knows the answer — each one names who to ask next".
- **Caption (12px `#444`, bottom right):** "IP from the documentation range, illustrative".

## Timing the Three Questions

**Tags:** `worked example` (blue), `latency` (green)

- **Question 1** — resolver to a root server and back: 24 ms to learn "the .com servers know"
- **Question 2** — resolver to a .com TLD server: 18 ms to learn "ns1.example.com is authoritative"
- **Question 3** — resolver to ns1.example.com: 21 ms to get the answer, "www is 203.0.113.7"
- **Hand-check** — the cold lookup costs 24 + 18 + 21 = 63 ms before the first byte of the page moves
- **The repeat** — the resolver caches the answer, so the next lookup is served locally in 1 ms
- **The TTL** — the record carries a time-to-live of 300 s; the cache may reuse it for 5 minutes

*Example (italic):* The laptop's first visit pays 63 ms of DNS before the page even starts loading; a second visit ten seconds later pays 1 ms.

**Key point:** A cold lookup is the sum of its hops — 24 + 18 + 21 = 63 ms here — and caching collapses every repeat within the TTL to a single local answer.

### Visualization (canvas `c2`, 720×300)

Waterfall chart of the traced lookup: three sequential hop bars stacking left to right, a total bar, and a tiny cached-repeat bar for contrast.

- **Title (bold 15px, `#1a5276`, top center):** "Cold Lookup Waterfall: 24 + 18 + 21 = 63 ms".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = milliseconds 0 to 70, 12px `#444` tick labels every 10 ms, gridlines `#e5e9ef`.
- **Rows (14px tall bars, left-aligned 12px `#444` labels above each bar):** at y=80 "root — who runs .com?": aqua `#199e70` bar from 0 to 24 ms; at y=125 "TLD — who runs example.com?": violet `#4a3aa7` bar from 24 to 42 ms; at y=170 "authoritative — what is www?": green `#008300` bar from 42 to 63 ms; at y=215 "cached repeat": blue `#2a78d6` bar from 0 to 1 ms.
- **Bar value labels:** 11px, each bar's ms at its right end ("24 ms", "18 ms", "21 ms", "1 ms").
- **Total marker:** vertical dashed `#6b7280` (dash 4/3) line at 63 ms, bold 12px ink `#1a5276` label "63 ms total" at its top.
- **Annotation (bold 13px blue `#2a78d6`, near x=15 ms, y=235):** "the cache turns 63 ms into 1 ms".
- **Caption (12px `#444`, bottom right):** "hop timings illustrative; the sum is exact".

## Where Engineers Feel DNS Every Day

**Tags:** `where it's used` (blue), `caching` (green), `outages` (orange)

- **Page speed** — DNS is the first hop of every request; a slow resolver taxes every page load
- **Service discovery** — backends find databases and APIs by name, so DNS sits inside every call path
- **Outage shape** — when DNS fails, healthy servers look "down" because nobody can find them
- **The cache math** — with a 300 s TTL, a lookup every minute pays the full trip only at minutes 0, 5, 10
- **Telemetry** — 12 of 15 lookups in that window are 1 ms cache hits; only 3 cost the full 63 ms
- **Data angle** — latency logs show a spiky bimodal shape (1 ms vs 63 ms), not a smooth average

*Example (italic):* A dashboard averaging those 15 lookups reports 13.4 ms, a number no single lookup ever took — the real story is 12 hits at 1 ms and 3 misses at 63 ms.

**Key point:** DNS meets the engineer as latency and as failure: cache TTLs make lookup times bimodal, and a DNS outage takes down services whose servers are perfectly healthy.

### Visualization (canvas `c3`, 720×300)

Stem-and-dot timeline of lookup latency for one lookup per minute over 15 minutes with a 300 s TTL: tall spikes at cache expiry, flat 1 ms hits between.

- **Title (bold 15px, `#1a5276`, top center):** "One Lookup per Minute, TTL 300 s: Only the Expiries Are Slow".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = minutes 0 to 15, 12px `#444` tick labels every 5 min; y = latency 0 to 70 ms, gridlines `#e5e9ef` at 20/40/60.
- **Data:** minutes `[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14]`, latency ms `[63,1,1,1,1,63,1,1,1,1,63,1,1,1,1]`.
- **Stems:** 2px vertical lines from baseline to each value — orange `#d95926` for the three 63 ms misses, blue `#2a78d6` for the 1 ms hits; 5px radius dots on top in the same colors.
- **TTL braces:** thin `#6b7280` horizontal bracket from minute 0 to 5 at y=70 with 11px mute label "one TTL (300 s)".
- **Annotation (bold 13px green `#008300`, near x=7 min, y=100):** "12 of 15 lookups answered from cache in 1 ms".
- **Caption (12px `#444`, bottom right):** "latencies illustrative, TTL exact".

## A DNS Change Is Not a Broadcast

**Tags:** `common mistake` (red), `TTL` (orange), `propagation` (blue)

- **The myth** — people say a DNS change "propagates", as if the new address were pushed everywhere
- **The reality** — nothing is pushed; each resolver keeps its cached copy until its own TTL runs out
- **The math** — with a 3600 s TTL and evenly aged caches, the old IP fades linearly over 60 minutes
- **Hand-check** — 15 min after the change 75% of resolvers still answer old; at 30 min, 50%; at 45 min, 25%
- **The trap** — lowering the TTL at the same moment as the change is too late; old caches never see it
- **The fix** — drop the TTL to something short a day before the move, then change the record

*Example (italic):* A shop moves its server and edits the record at noon with a 3600 s TTL — at 12:30 half its customers are still knocking on the old machine's door.

**Common mistake:** Expecting a DNS edit to take effect instantly. Caches expire on their own schedules, so both addresses are live for up to one full TTL — plan the cutover, don't flip it.

### Visualization (canvas `c4`, 720×300)

Two crossing lines over the hour after a record change: share of resolvers still serving the old IP (falling) vs the new IP (rising), crossing at 30 minutes.

- **Title (bold 15px, `#1a5276`, top center):** "After the Change: Old and New IP Share Over One TTL (3600 s)".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = minutes since change 0 to 60, 12px `#444` tick labels every 15 min; y = % of resolvers 0 to 100, gridlines `#e5e9ef` at 25/50/75.
- **Old IP line:** orange `#d95926` 3px line through minutes `[0, 15, 30, 45, 60]`, percent `[100, 75, 50, 25, 0]`, 12px orange label "still serving old IP" near its upper left.
- **New IP line:** green `#008300` 3px line through the same minutes, percent `[0, 25, 50, 75, 100]`, 12px green label "serving new IP" near its lower left.
- **Crossover marker:** vertical dashed `#6b7280` (dash 4/3) line at minute 30, 4px radius ink `#1a5276` dot at the 50/50 crossing.
- **Annotation (bold 13px magenta `#d55181`, near x=32 min, y=70):** "half of users still hit the old server 30 min in".
- **Caption (12px `#444`, bottom right):** "uniform cache ages assumed — decay shape illustrative, TTL exact".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); hop timings (24/18/21 ms, sum 63), per-minute latencies `[63,1,1,1,1,63,1,1,1,1,63,1,1,1,1]`, and old/new IP shares `[100,75,50,25,0]` / `[0,25,50,75,100]` are invented and labeled illustrative; the resolution chain (root → TLD → authoritative), the 13 root server identities, the TTL values (300 s, 3600 s), and the documentation IP 203.0.113.7 are factual conventions.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
