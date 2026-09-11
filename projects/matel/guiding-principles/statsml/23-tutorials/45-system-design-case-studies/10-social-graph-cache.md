# Social Graph Cache

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Social Graph Cache

**Subtitle:** Rendering one social-network page takes hundreds of tiny graph reads, so virtually all of them must come from memory — the published design puts huge memcached fleets in front of sharded MySQL, and the cache tier becomes the real serving system

## One Page Load, Five Hundred Cache Reads

**Tags:** `core idea` (blue), `read amplification` (green), `NSDI 2013` (orange)

- **The page** — one feed render needs friend lists, like counts, comments, and permission checks per story
- **The amplification** — the NSDI 2013 paper measures an average of 521 distinct memcache fetches per page
- **The tail** — at the 95th percentile a single page load fetches 1,740 distinct items, over 3× the average
- **The shape** — each read is tiny and cheap alone; the page is expensive because there are hundreds of them
- **The design** — memcached fleets sit in front of sharded MySQL and answer over a billion requests per second

*Example (italic):* Opening one popular page fires 521 cache reads on average — if even a few of those had to wait on disk-backed MySQL, the page could not render in time.

**Key point:** The social graph amplifies one page view into hundreds of small dependent reads, so the only workable design serves essentially every read from an in-memory cache tier.

### Visualization (canvas `c1`, 720×300)

Horizontal bar chart: how one page request amplifies into memcache fetches — the request itself, the average page, and the 95th-percentile page; bar widths proportional to fetch counts.

- **Title (bold 15px, `#1a5276`, top center):** "Read Amplification: One Page Request Becomes Hundreds of Fetches".
- **Axis:** horizontal 2px `#999` baseline at x=250, bars extend right, max width 420; widths proportional to values (the 1-fetch bar drawn 5px so it stays visible).
- **Rows (top to bottom at y = 85, 150, 215), each with a left-aligned 12px `#444` label at x=20:**
  - "the request — 1 page load": blue `#2a78d6` bar width 5, 11px label "1"
  - "average popular page": blue bar width 126, bold 12px label "521 memcache fetches"
  - "95th-percentile page": orange `#d95926` bar width 420, bold 12px orange label "1,740 fetches"
- **Bar style:** 16px tall, blue bars fill `rgba(42,120,214,0.30)` with solid 2px border, orange bar solid.
- **Annotation (bold 13px magenta `#d55181`, right side near y=260):** "every one of these must be a memory hit".
- **Caption (12px `#444`, bottom right):** "521 avg / 1,740 p95 as published (NSDI 2013)".

## Look-Aside Caching, Deletes, and Leases

**Tags:** `worked example` (blue), `cache invalidation` (green), `leases` (orange)

- **The read path** — the web server gets a key from memcache; on a miss it reads MySQL and sets the value back
- **The write path** — the web server writes MySQL, then deletes the key rather than updating it (idempotent)
- **The race** — a slow miss-reader can set a pre-write value after the delete, freezing stale data in cache
- **The lease** — a miss returns a 64-bit token; a set is accepted only if it is valid, and a delete voids it
- **The herd** — a token is issued at most once per key every 10 seconds, so one reader per miss reaches MySQL
- **The payoff** — the paper reports leases cut one stampede's peak database load from 17K to 1.3K queries/s

*Example (italic):* A hot key is invalidated and thousands of readers miss at once — without leases MySQL takes a 17,000 queries/s spike; with leases one reader refills the key and the peak stays near 1,300 queries/s.

**Key point:** Look-aside caching with delete-on-write is simple, and leases patch its two failure modes at once — stale sets from racing readers and thundering herds on hot keys.

### Visualization (canvas `c2`, 720×300)

Timeline chart of database queries per second while a hot key is invalidated at t=20s: without leases (spike to 17K) vs with leases (stays near 1.3K), on a shared time axis.

- **Title (bold 15px, `#1a5276`, top center):** "Hot-Key Invalidation: Leases Cut the DB Spike from 17K/s to 1.3K/s".
- **Axes:** origin x=70, baseline y=245, plot width 590, plot height 180; x = seconds 0 to 60 with 12px `#444` tick labels every 15s ("0s"–"60s"); y = DB queries/s 0 to 18,000, gridlines `#e5e9ef` at 4,500/9,000/13,500 with 12px `#444` labels "4.5K"/"9K"/"13.5K".
- **No-lease line:** red `#e74c3c` 3px line through seconds `[0, 10, 20, 25, 30, 40, 50, 60]`, queries/s `[900, 950, 17000, 12000, 6000, 2500, 1200, 950]` — vertical spike at the invalidation.
- **Lease line:** green `#008300` 3px line through the same seconds, queries/s `[900, 950, 1300, 1250, 1100, 1000, 950, 940]` — nearly flat.
- **Invalidation marker:** vertical dashed `#6b7280` (dash 4/3) line at second 20, 12px `#6b7280` label "key deleted" at its top.
- **Labels:** bold 12px red "no leases — 17K/s peak" near (x≈27s, y=70); bold 12px green "leases — 1.3K/s peak" near (x≈42s, y=215).
- **Annotation (bold 13px violet `#4a3aa7`, near x=45s, y=110):** "one token holder refills; everyone else waits or reads slightly stale".
- **Caption (12px `#444`, bottom right):** "17K/s and 1.3K/s peaks as published; curve shapes illustrative".

## TAO and the Primary-Region Invalidation Stream

**Tags:** `where it's used` (blue), `graph cache` (green), `replication` (orange)

- **The graph API** — TAO replaces raw key-value use with objects (nodes) and associations (ordered typed edges)
- **The calls** — assoc_get, assoc_count, and assoc_range answer "newest 50 comments on this post" natively
- **The win** — a graph-aware cache updates one edge in a cached list instead of invalidating a serialized blob
- **The workload** — the TAO paper measures 99.8% reads to 0.2% writes, at over a billion reads per second
- **Regions** — every region runs a full cache tier; MySQL masters live in the primary region, replicas elsewhere
- **The stream** — mcsqueal tails the master's commit log and pipes deletes to every region, riding replication

*Example (italic):* A like written in the primary region commits to master MySQL, replicates to the replica region's MySQL, and the invalidation stream then deletes the stale cached count there — so a cache refill never reads data older than its own invalidation.

**Key point:** At this scale the cache stops being a dumb key-value layer: TAO makes it graph-aware, and invalidations travel with the database replication stream so every region's cache converges.

### Visualization (canvas `c3`, 720×300)

Two-row architecture flow: the primary region (web → cache → MySQL master) and a replica region (web → cache → MySQL replica), with the replication stream and the mcsqueal invalidation deletes flowing between them.

- **Title (bold 15px, `#1a5276`, top center):** "Writes Commit in the Primary Region; Deletes Ride the Replication Stream".
- **Row 1 (boxes centered on y=95), label 12px `#444` at x=15:** "primary region"; blue `#2a78d6` rounded box at x=130 labeled "web tier" (12px), 3px arrow to a green `#008300` box at x=300 labeled "memcache / TAO", 3px arrow to a blue box at x=510 labeled "MySQL master".
- **Row 2 (boxes centered on y=215), label:** "replica region"; blue box "web tier" at x=130, arrow to green box "memcache / TAO" at x=300, arrow to blue box "MySQL replica" at x=510.
- **Replication arrow:** solid 3px blue `#2a78d6` vertical arrow from "MySQL master" down to "MySQL replica", 12px blue label "SQL replication" at its right.
- **Invalidation arrow:** dashed 2px orange `#d95926` (dash 5/4) arrow from "MySQL replica" left to the replica region's "memcache / TAO" box, bold 12px orange label "mcsqueal deletes" beneath it.
- **Box style:** 130–160px wide, 42px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px magenta `#d55181`, centered near y=280):** "invalidations follow the data — a cache never refills from a DB older than the delete".
- **Caption (12px `#444`, bottom right):** "topology per the published papers, simplified".

## The Cache Tier Is the Serving System

**Tags:** `common mistake` (red), `capacity` (orange)

- **The trap** — treating memcache as a bolt-on speed-up for a database that could survive on its own
- **The math** — at TAO's published 96.4% hit rate, MySQL is sized for 3.6% of a billion reads/s, or 36M/s
- **The cliff** — hit rate falling from 96.4% to 92.8% doubles the miss traffic MySQL absorbs, to 72M reads/s
- **The gutter** — the paper reserves about 1% of cache servers as a Gutter pool to absorb a dead server's keys
- **The lesson** — at extreme read amplification the cache IS the serving system; MySQL is the durable store

*Example (italic):* At 1 billion reads/s and a 96.4% hit rate, MySQL sees 36M reads/s; a dip to a 92.8% hit rate doubles that to 72M/s within seconds — and with no cache at all the full 1,000M/s would land on a tier built for 36M.

**Common mistake:** Sizing the database as if the cache were optional. Once the hit rate is in the high 90s, the database has been capacity-planned for the misses only — a modest hit-rate drop is a multiplicative DB overload, which is why the papers spend their pages on leases, Gutter pools, and invalidation streams rather than on MySQL.

### Visualization (canvas `c4`, 720×300)

Horizontal bar chart: read traffic landing on MySQL at three cache states, out of 1 billion reads/s; bar widths proportional to the read rates.

- **Title (bold 15px, `#1a5276`, top center):** "MySQL Load out of 1B Reads/s: the Database Is Sized for the Misses".
- **Axis:** horizontal 2px `#999` baseline at x=250, bars extend right, max width 420; widths proportional to values.
- **Rows (top to bottom at y = 85, 150, 215), each with a left-aligned 12px `#444` label at x=20:**
  - "96.4% hit rate (published)": green `#008300` bar width 15, 11px label "36M reads/s"
  - "92.8% hit rate (a small dip)": orange `#d95926` bar width 30, bold 12px orange label "72M reads/s — 2× the load"
  - "cache gone entirely": red `#e74c3c` bar width 420, bold 12px red label "1,000M reads/s — 28× capacity"
- **Bar style:** 16px tall, green bar fill `rgba(0,131,0,0.30)` with 2px border, orange and red bars solid.
- **Annotation (bold 13px violet `#4a3aa7`, right side near y=260):** "a 3.6-point hit-rate drop doubles the database's work".
- **Caption (12px `#444`, bottom right):** "96.4% hit rate published (TAO); 1B reads/s published; other rows are arithmetic, illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness). Published figures come from the widely cited memcache-scaling and social-graph-store papers of the early 2010s: 521 avg / 1,740 p95 fetches per popular page, over a billion requests/s, lease tokens at most once per key per 10 seconds, 17K/s → 1.3K/s stampede peak, 99.8%/0.2% read/write mix, 96.4% hit rate, ~1% Gutter pool. Everything else (spike curve shapes, the 92.8% dip scenario, 36M/72M/1,000M arithmetic) is illustrative and labeled so. Stick to the published papers — no claims about current internal systems.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
