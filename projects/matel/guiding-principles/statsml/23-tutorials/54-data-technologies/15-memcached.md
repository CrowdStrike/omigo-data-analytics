# Memcached

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Memcached

**Subtitle:** Memcached is a hash table you reach over the network — get, set, delete on borrowed RAM, and deliberately nothing more

## The 80 ms Query and the 1 ms Hash Table

**Tags:** `core idea` (blue), `caching` (green), `2003` (orange)

- **The slow page** — an article page runs an 80 ms database query on every single view (illustrative)
- **The cache server** — memcached holds `article:1042 → page data` in RAM and answers a get in 1 ms
- **Three verbs** — the whole API is essentially get(key), set(key, value, ttl), delete(key)
- **The origin** — Brad Fitzpatrick wrote it in 2003 to take load off LiveJournal's database
- **Deliberately minimal** — no disk, no queries, no replication: a multithreaded hash table over TCP

*Example (italic):* The app asks memcached for `article:1042` first; only when the key is missing does it run the 80 ms SQL query, then set() the result for the next visitor.

**Key point:** Memcached is a networked in-memory hash table, nothing more — the caching behavior is the cache-aside pattern your app writes around those three verbs.

### Visualization (canvas `c1`, 720×300)

Two-row flow diagram of the cache-aside pattern: the hit path (memcached answers, database untouched) vs the miss path (query the database, then set the key).

- **Title (bold 15px, `#1a5276`, top center):** "Cache-Aside: Ask the Hash Table First, the Database Only on a Miss".
- **Row 1 (y=95), label 12px `#444` at x=20:** "hit — 9 of 10 views"; blue `#2a78d6` rounded box at x=145 labeled "get(article:1042)" (12px), 3px arrow to a green `#008300` box at x=350 labeled "memcached: found — 1 ms", then bold 12px green "✓ database never touched" at x=545.
- **Row 2 (y=205), label:** "miss — 1 of 10"; blue box at x=145 labeled "get → (nothing)", 3px arrow to an orange `#d95926` box at x=330 labeled "SQL query — 80 ms", arrow to a blue box at x=525 labeled "set(key, row, 60s)".
- **Box style:** 150–170px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(0,131,0,0.12)` / `rgba(217,89,38,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=270):** "the app owns the pattern — memcached only knows get, set, delete".
- **Caption (12px `#444`, bottom right):** "latencies illustrative".

## What a 90% Hit Ratio Buys You

**Tags:** `worked example` (blue), `hit ratio math` (green)

- **The two costs** — a hit costs the 1 ms get; a miss costs the get plus the query: 1 + 80 = 81 ms
- **The formula** — average latency = h × 1 + (1 − h) × 81, where h is the hit ratio (exact)
- **At 90% hits** — 0.9 × 1 + 0.1 × 81 = 9.0 ms average, down from 81 ms with a cold cache
- **Database relief** — at 1,000 views/s, the database now runs 100 queries/s instead of 1,000
- **The last mile** — pushing 90% to 99% hits cuts the average again, from 9.0 ms to 1.8 ms
- **The TTL** — set(key, value, 60) caps staleness at 60 s; expiry is just a miss you scheduled

*Example (italic):* At a 90% hit ratio, 9 of every 10 visitors get the article in 1 ms and 1 pays the full 81 ms — averaging 9.0 ms per view.

**Key point:** Misses dominate the math — average latency ≈ miss ratio × query cost — so each extra point of hit ratio near the top cuts a larger fraction of the remaining latency.

### Visualization (canvas `c2`, 720×300)

Bar chart of average page latency at four hit ratios, computed from the section's formula.

- **Title (bold 15px, `#1a5276`, top center):** "Average Page Latency vs Hit Ratio: Misses Dominate the Math".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = milliseconds 0 to 90, gridlines `#e5e9ef` at 20/40/60/80 with 12px `#444` labels at x=50 right-aligned.
- **Bars (80px wide, centered at x = 135, 285, 435, 585):** hit ratios `["0% hits", "50%", "90%", "99%"]`, average ms `[81, 41, 9.0, 1.8]`, pixel heights `[162, 82, 18, 4]`; fills red `#e74c3c`, orange `#d95926`, blue `#2a78d6`, green `#008300`.
- **Labels:** bold 13px value labels "81 ms" / "41 ms" / "9.0 ms" / "1.8 ms" above each bar in the bar's color; 12px `#444` hit-ratio labels below the baseline.
- **Annotation (bold 13px green `#008300`, near x=420, y=95):** "90% hits: 81 ms → 9.0 ms average".
- **Caption (12px `#444`, bottom right):** "avg = h×1 + (1−h)×81 — arithmetic exact, latencies illustrative".

## Spreading Keys Across a Fleet

**Tags:** `where it's used` (blue), `consistent hashing` (green), `scale` (orange)

- **Dumb servers** — a memcached node knows nothing about the others; there is no cluster protocol
- **Client-side sharding** — the client library hashes each key to pick which node stores it
- **Consistent hashing** — keys sit on a hash ring, so removing a node remaps only that node's share
- **The naive trap** — hash(key) % N remaps nearly every key when N changes, emptying the cache at once
- **Facebook's fleet** — its published 2013 paper describes memcached serving over a billion reads/s
- **Why it scales** — servers that never talk to each other have no coordination cost; add RAM by adding boxes

*Example (italic):* With 4 nodes, `article:1042` hashes to node C; when C dies, only C's quarter of the keys spread over A, B, and D — the other three quarters never move.

**Key point:** Memcached scales by staying dumb — nodes are independent, the client's consistent hash spreads the keys, and losing a node costs only that node's share of hits.

### Visualization (canvas `c3`, 720×300)

Grouped bar chart of each node's share of cached keys: four healthy nodes vs the moment node C dies, showing that only C's share redistributes.

- **Title (bold 15px, `#1a5276`, top center):** "Consistent Hashing: Losing Node C Moves Only Node C's Keys".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = share of keys 0 to 40%, gridlines `#e5e9ef` at 10/20/30 with 12px `#444` labels.
- **Left group ("4 nodes healthy", 12px `#444` group label below at x=195):** bars 40px wide at x = 100, 150, 200, 250 for nodes A/B/C/D, shares `[25, 25, 25, 25]` %, pixel heights `[112, 112, 112, 112]`; fills blue `#2a78d6`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`.
- **Right group ("node C dies", group label at x=505):** bars at x = 410, 460, 510, 560 for A/B/C/D, shares `[33, 34, 0, 33]` %, pixel heights `[148, 153, 0, 148]`; C drawn as a dashed red `#e74c3c` outline (dash 4/3) 40×112 where its bar used to be, with bold 12px red "✗" centered inside.
- **Labels:** 12px node letters "A B C D" below the baseline under each bar; bold 12px share labels ("25%", "33%", "34%") above each bar in the bar's color.
- **Annotation (bold 12px orange `#d95926`, near x=430, y=70):** "C's 25% redistributes — 75% of keys never move".
- **Caption (12px `#444`, bottom right):** "shares illustrative; with modulo hashing nearly all keys would move".

## A Cache Is Not a Database

**Tags:** `common mistake` (red), `vs Redis` (orange)

- **The mistake** — storing shopping carts only in memcached, treating a set() like a database write
- **LRU eviction** — when RAM fills, the least-recently-used entries are silently deleted to make room
- **No persistence** — a restart or crash empties the node; there is no disk to reload from
- **No replication** — no second copy exists; a lost node is only safe if the database holds the truth
- **The Redis contrast** — Redis adds data structures, persistence, and replication; memcached refuses them
- **Neither is wrong** — a pure look-aside cache suits memcached; a data-structure server is Redis's job

*Example (italic):* A user's cart written only to memcached vanishes during a traffic spike when LRU evicts it to make room for hot article keys.

**Common mistake:** Putting the only copy of data in memcached. Eviction and restarts delete keys without warning — every key must be rebuildable from the database, or it does not belong there.

### Visualization (canvas `c4`, 720×300)

Two-column comparison diagram: what memcached deliberately leaves out, row by row against what Redis provides — framed as different promises, not a ranking.

- **Title (bold 15px, `#1a5276`, top center):** "Memcached Stays a Cache; Redis Grows Into a Data-Structure Server".
- **Column headers (bold 13px, y=62):** "memcached" in blue `#2a78d6` centered at x=190; "Redis" in violet `#4a3aa7` centered at x=530.
- **Rows (box tops at y = 80, 125, 170, 215), each row one rounded box per column, 260px wide, 38px tall, 8px radius, 12px `#2c3e50` text centered:**
  - memcached boxes at x=60, fill `rgba(42,120,214,0.15)`: "get / set / delete only", "LRU eviction, TTLs", "no disk — restart starts empty", "no replication, no failover"
  - Redis boxes at x=400, fill `rgba(74,58,167,0.12)`: "lists, sets, sorted sets, hashes", "optional persistence to disk", "replication and failover", "pub/sub, scripting, streams"
- **Annotation (bold 12px aqua `#199e70`, centered at x=360, y=285):** "both keep data in RAM — they differ on everything promised beyond that".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the 1 ms / 80 ms latencies, 1,000 views/s, and node key-shares are invented and labeled illustrative; the hit-ratio arithmetic (avg = h×1 + (1−h)×81, giving 81 / 41 / 9.0 / 1.8 ms) is exact on those illustrative inputs; the 2003 LiveJournal origin and Facebook's billion-reads-per-second figure come from public documentation (the 2013 "Scaling Memcache at Facebook" paper).
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
