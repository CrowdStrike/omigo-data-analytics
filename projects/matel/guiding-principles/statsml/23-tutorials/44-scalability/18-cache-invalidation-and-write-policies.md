# Cache Invalidation & Write Policies

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Cache Invalidation & Write Policies

**Subtitle:** A cache is a copy, and every copy can go stale — the write policy decides how long the cache is allowed to lie about the source of truth

## The Mug That Stayed at $30

**Tags:** `core idea` (blue), `stale data` (red), `source of truth` (green)

- **The setup** — a store caches product prices in memory so the product page never waits on the database
- **The change** — at 2:00pm a sale drops the ceramic mug from $30 to $24 in the database
- **The lie** — the cache still holds its old copy, so every shopper keeps seeing $30
- **The expiry** — the cached entry was stored with a 5-minute TTL, so it dies on its own at 2:05pm
- **The problem** — for 5 minutes the cache and the database disagree, and the cache wins every read

*Example (italic):* From 2:00 to 2:05pm the database says $24 while every product-page view serves the cached $30 — shoppers who add the mug see the wrong price.

**Key point:** Staleness is the gap between the source of truth changing and the cached copy catching up — write policies and invalidation exist to shrink or bound that gap.

### Visualization (canvas `c1`, 720×300)

Step-line timeline: database price vs cached price around the 2:00pm change, with the disagreement window shaded.

- **Title (bold 15px, `#1a5276`, top center):** "Price Drops to $24 at 2:00 — the Cache Says $30 for 5 More Minutes".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = clock time 1:58 to 2:08, 12px `#444` tick labels every 2 minutes ("1:58"…"2:08"); y = price $20 to $32, gridlines `#e5e9ef` at 24 and 28 with 12px `#444` labels "$24" / "$28".
- **Database line:** green `#008300` 3px step line: $30 from 1:58 to 2:00, vertical drop at 2:00, $24 from 2:00 to 2:08; 12px green label "database (truth)" above its right end.
- **Cache line:** red `#e74c3c` 3px dashed (dash 6/4) step line: $30 from 1:58 to 2:05, vertical drop at 2:05, $24 from 2:05 to 2:08; 12px red label "cache copy" above its $30 segment.
- **Stale window:** rectangle from 2:00 to 2:05 between the two lines, fill `rgba(231,76,60,0.10)`; bold 13px red `#e74c3c` label centered in it: "stale window: 5 min".
- **TTL marker:** vertical dashed `#6b7280` (dash 4/3) line at 2:05, 12px `#6b7280` label "TTL expires" at its top.
- **Caption (12px `#444`, bottom right):** "times and prices illustrative".

## Three Ways to Write the New Price

**Tags:** `worked example` (blue), `write policies` (green)

- **The costs** — in this store a database write takes 10 ms and a cache write takes 2 ms
- **Write-through** — write cache and database together: 10 + 2 = 12 ms, but the cache is never stale
- **Write-back** — write only the cache (2 ms) and flush to the database within 30 s: fast but risky
- **The risk** — if the cache node dies before the flush, the $24 exists nowhere; the truth reverts to $30
- **Cache-aside** — write the database (10 ms), then delete the cache key; the next read repopulates $24
- **Hand-check** — write-through 12 ms, write-back 2 ms, cache-aside 10 ms + a 1 ms delete = 11 ms

*Example (italic):* The same $24 update costs 12 ms write-through, 2 ms write-back, and 11 ms cache-aside — write-back is 6× faster only because it postpones the database's copy.

**Key point:** Every policy trades among three things — write latency, staleness, and durability; write-through pays latency, write-back risks loss, cache-aside (the most common) accepts a brief miss.

### Visualization (canvas `c2`, 720×300)

Three-row flow diagram, one row per write policy, showing where the $24 goes first and what each arrow costs.

- **Title (bold 15px, `#1a5276`, top center):** "Same $24 Update, Three Write Policies".
- **Rows (centered at y = 85, 160, 235), each with a bold 12px `#1a5276` policy label at x=20:** "write-through", "write-back", "cache-aside".
- **Box style:** 110px wide, 36px tall, 8px radius, 12px `#2c3e50` centered text; app boxes fill `rgba(42,120,214,0.15)` with `#2a78d6` border, cache/db boxes fill `rgba(0,131,0,0.12)` with `#008300` border; arrows 3px `#6b7280` with 11px `#444` labels above them.
- **Row 1 (write-through):** box "app $24" at x=150 → arrow "2 ms" → box "cache $24" at x=310 → arrow "10 ms" → box "db $24" at x=470; bold 12px green `#008300` label at x=600: "12 ms, never stale".
- **Row 2 (write-back):** box "app $24" at x=150 → arrow "2 ms" → box "cache $24" at x=310 → dashed (dash 5/4) arrow "flush ≤30 s later" → box "db $30→$24" at x=470; bold 12px orange `#d95926` label at x=600: "lost if cache dies first".
- **Row 3 (cache-aside):** box "app $24" at x=150 → arrow "10 ms" → box "db $24" at x=310 → arrow "1 ms delete" → box "cache (empty)" at x=470 with red `#e74c3c` border and fill `rgba(231,76,60,0.08)`; bold 12px blue `#2a78d6` label at x=600: "next read repopulates".
- **Caption (12px `#444`, bottom right):** "latencies illustrative".

## The Race That Pins a Stale Price

**Tags:** `race condition` (red), `cache-aside` (blue), `TTL` (orange)

- **The trigger** — cache-aside repopulates on a miss, and a repopulate can overlap an update
- **Reader A** — at t=0 ms a page view misses the cache and starts reading the price from the database
- **Writer B** — at t=3 ms the sale lands: database becomes $24, and at t=5 ms B deletes the cache key
- **The stale write** — at t=8 ms A's read returns the old $30, and at t=10 ms A writes $30 into the cache
- **The pin** — the delete already happened, so the stale $30 sits in the cache until the 300 s TTL kills it
- **The safety net** — TTL cannot prevent this race, but it bounds the damage to at most one TTL period

*Example (italic):* B's delete at t=5 ms removes nothing useful; A's $30 arrives 5 ms later and stays pinned for up to 300 seconds — the TTL, not the invalidation, ends the lie.

**Key point:** In cache-aside, a read that started before an update can repopulate the cache with pre-update data after the invalidation ran — TTL is the only bound on how long that stale value survives.

### Visualization (canvas `c3`, 720×300)

Two-lane sequence timeline (reader A, writer B) over 0–12 ms, with a cache-state bar underneath showing the pinned stale value.

- **Title (bold 15px, `#1a5276`, top center):** "The Read-Repopulate Race: Old $30 Lands After the Delete".
- **Time axis:** horizontal 2px `#999` line at y=245 from x=60 to x=660 mapping 0–12 ms, 12px `#444` tick labels at 0/3/5/8/10/12 ms.
- **Lanes:** thin 1px `#e5e9ef` guide lines at y=100 (bold 12px `#2a78d6` label "reader A" at x=20) and y=165 (bold 12px `#d95926` label "writer B" at x=20).
- **Reader A events (blue `#2a78d6` 7px dots on y=100, 12px labels above):** t=0 "cache miss, read db"; t=8 "db returns $30"; t=10 red `#e74c3c` dot "writes $30 to cache".
- **Writer B events (orange `#d95926` 7px dots on y=165, 12px labels below):** t=3 "db ← $24"; t=5 "delete cache key".
- **Cache-state bar (14px tall at y=210, spanning x=60 to x=660):** fill `#e5e9ef` with 11px `#6b7280` label "empty" from 0 to 10 ms; solid red `#e74c3c` from 10 to 12 ms with bold 11px white label "$30 stale"; red arrow off the right edge with 12px red label "…pinned until TTL (300 s)".
- **Annotation (bold 13px red `#e74c3c`, near x=330, y=60):** "the delete ran — but the stale write came later".
- **Caption (12px `#444`, bottom right):** "timings illustrative".

## The Invalidation Message That Never Arrives

**Tags:** `common mistake` (red), `TTL` (orange), `two hard things` (blue)

- **The design** — a price-change event tells every cache node to drop the mug's key immediately
- **The mistake** — trusting that event so much that entries are stored with no TTL at all
- **The failure** — the message queue drops one event, or the worker that applies it crashes mid-batch
- **The result** — a cache entry nothing will ever invalidate: $30 served forever while the truth is $24
- **The joke** — "there are only two hard things in computer science: cache invalidation and naming things"
- **The fix** — keep event-driven invalidation for speed, keep TTL as the backstop that bounds staleness

*Example (italic):* One lost message with no TTL means the $30 mug outlives the sale, the quarter, and the on-call engineer who eventually greps the cache by hand.

**Common mistake:** Treating invalidation events as guaranteed delivery. Any missed message, crashed worker, or partitioned node turns "invalidate on change" into "never invalidate" — only a TTL converts unbounded staleness into a fixed worst case.

### Visualization (canvas `c4`, 720×300)

Horizontal bar chart: worst-case staleness of the $30 price under three invalidation setups.

- **Title (bold 15px, `#1a5276`, top center):** "Worst-Case Staleness: What Bounds the Lie?".
- **Axis:** vertical 2px `#999` baseline at x=250, bars extend right, max width 440; widths are hardcoded pixels for log-feel, not a real scale.
- **Rows (bar centers at y = 90, 155, 220), each with a left-aligned 12px `#444` label ending at x=240:**
  - "event delivered (normal case)": green `#008300` bar width 12, 11px green label "≈1 s" at bar end
  - "event lost, 5-min TTL backstop": orange `#d95926` bar width 220, 11px orange label "300 s" at bar end
  - "event lost, no TTL": red `#e74c3c` bar width 400 ending in a 12px-long arrowhead past x=650, bold 12px red label "unbounded — stale forever"
- **Bar style:** 18px tall, solid fills, 4px corner radius.
- **Annotation (bold 13px magenta `#d55181`, centered near y=265):** "TTL turns 'forever' into 'at most 5 minutes'".
- **Caption (12px `#444`, bottom right):** "durations illustrative; pixel widths schematic".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded values above (no randomness); prices ($30/$24), latencies (10 ms db, 2 ms cache, 1 ms delete), TTLs (5 min / 300 s), race timestamps (0/3/5/8/10/12 ms), and staleness bars are invented and labeled illustrative; write-through = 12 ms and cache-aside = 11 ms in the text must match the c2 arrow labels.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
