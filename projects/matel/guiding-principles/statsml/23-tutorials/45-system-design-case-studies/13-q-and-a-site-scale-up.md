# Q&A Site Scale-Up

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Q&A Site Scale-Up

**Subtitle:** One of the busiest Q&A sites on the web ran on about two dozen well-tuned machines — the famous public counterexample to reflexive horizontal scaling

## The Site That Refused to Scale Out

**Tags:** `core idea` (blue), `scale-up` (green), `public numbers` (orange)

- **The site** — the site served on the order of 2 billion page views a month, a top-50 site by traffic
- **The fleet** — ~11 web servers, 4 SQL Servers, 2 Redis, 3 Elasticsearch, 3 tag engines: ~two dozen boxes
- **The blog** — their engineers published the architecture and live performance numbers for years, in public
- **The twist** — the web tier idled at roughly 5–15% CPU; a fraction of the fleet could carry the whole load
- **The point** — no microservices, no sharding, no thousand-node cluster — a few big machines, heavily tuned

*Example (italic):* The 2016 architecture post showed question pages rendering in about 18 ms on web servers loafing near 10% CPU.

**Key point:** One of the busiest sites on the web fit on roughly two dozen machines because the team sized the system from measured numbers, not from an assumption of hyperscale.

### Visualization (canvas `c1`, 720×300)

Horizontal bar chart: server count per role in the publicly-blogged 2016-era stack — the whole "real work" fleet on one axis.

- **Title (bold 15px, `#1a5276`, top center):** "The Whole Fleet: ~23 Boxes Serve ~2 Billion Page Views a Month".
- **Axis:** vertical 2px `#999` baseline at x=230, bars extend right, scale 32px per server; left-aligned 12px `#444` role labels at x=20.
- **Rows (top to bottom at y = 70, 105, 140, 175, 210), bars 16px tall:**
  - "Web servers (IIS)": blue `#2a78d6` fill `rgba(42,120,214,0.35)` bar width 352 (11 servers)
  - "SQL Servers": green `#008300` fill `rgba(0,131,0,0.30)` bar width 128 (4)
  - "Elasticsearch": aqua `#199e70` fill `rgba(25,158,112,0.30)` bar width 96 (3)
  - "Tag engines": violet `#4a3aa7` fill `rgba(74,58,167,0.25)` bar width 96 (3)
  - "Redis": orange `#d95926` fill `rgba(217,89,38,0.30)` bar width 64 (2)
- **Count labels:** bold 12px, same hue as each bar, at each bar's right end ("11", "4", "3", "3", "2").
- **Annotation (bold 13px ink `#1a5276`, right side near y=250):** "no sharding, no microservices — 23 machines total".
- **Caption (12px `#444`, bottom right):** "roles and counts from their 2016 architecture post; bar widths schematic".

## Doing the Arithmetic on One Whiteboard Line

**Tags:** `worked example` (blue), `back-of-envelope` (green)

- **The load** — the 2016 post reported roughly 66 million page loads per day across the whole network
- **Per server** — 66M ÷ 11 web servers ≈ 6 million pages per server per day
- **Per second** — 6M ÷ 86,400 seconds ≈ 70 pages per second on each server
- **Per render** — at ~18 ms per page, that is ~1.3 pages actually in flight on a server at any instant
- **Hand-check** — a modern multi-core box can render dozens of pages at once; 1.3 is barely a warm-up

*Example (italic):* 66,000,000 ÷ 11 ÷ 86,400 × 0.018 s ≈ 1.3 concurrent renders — one division chain shows why the CPUs sit near idle.

**Key point:** Requests per second times seconds per request gives concurrent work per box — do this arithmetic before buying anything, because it often says you already have 10× the capacity you need.

### Visualization (canvas `c2`, 720×300)

Left-to-right funnel of the division chain as four rounded boxes with arrows, plus a utilization bar underneath showing the resulting CPU headroom.

- **Title (bold 15px, `#1a5276`, top center):** "From 66M Pages/Day to 1.3 Pages In Flight".
- **Funnel row (boxes centered on y=120):** four rounded boxes, 150px wide × 52px tall, 8px radius, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border, 12px `#2c3e50` two-line text, at x = 30, 205, 380, 555; 3px `#6b7280` arrows between them:
  - "66M page loads / day"
  - "÷ 11 servers = 6M / day each"
  - "÷ 86,400 s ≈ 70 / sec"
  - "× 18 ms ≈ 1.3 in flight"
- **Utilization bar (y=215, x=60 to x=660, 22px tall):** track fill `#e5e9ef` with 1px `#ccc` border; filled segment green `#008300` width 60px (10% of 600) with bold 12px green label "web tier ~5–15% CPU" to its right; 12px `#444` label "measured utilization" at x=60, y=205.
- **Annotation (bold 13px violet `#4a3aa7`, near x=440, y=260):** "the arithmetic predicted the idle CPUs — no guessing needed".
- **Caption (12px `#444`, bottom right):** "traffic and render times from their blog; concurrency derived, bar position illustrative".

## Why One Big Box Kept Winning

**Tags:** `where it's used` (blue), `caching` (green), `RAM` (orange)

- **Read-heavy** — the overwhelming share of traffic is people reading questions, not writing them
- **Cache everything** — Redis and in-process caches absorb repeat reads before they ever touch SQL
- **Tuned code** — hot paths were profiled relentlessly; the ~18 ms render was a designed budget, not luck
- **RAM outran data** — server RAM got cheap faster than the database grew, so the hot set stayed in memory
- **Fewer parts** — one debuggable process per tier beats chasing a request across 40 distributed services

*Example (italic):* A question read by 100,000 people was written once — caching turns 100,000 potential database hits into roughly one.

**Key point:** Scale-up worked because the workload was read-heavy, the reads were cacheable, and the working set fit in one box's RAM — verify those three properties in your own numbers before reaching for a distributed design.

### Visualization (canvas `c3`, 720×300)

Two-line chart over time: database size vs the RAM you could put in one SQL box — the RAM line grows steeper, so the hot working set never escapes memory.

- **Title (bold 15px, `#1a5276`, top center):** "RAM Got Cheap Faster Than the Data Grew".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = years 2010 to 2016 with 12px `#444` tick labels at `[2010, 2012, 2014, 2016]`; y = terabytes 0 to 2, gridlines `#e5e9ef` at 0.5/1.0/1.5 with 12px `#444` labels "0.5 TB"/"1 TB"/"1.5 TB".
- **Data line:** blue `#2a78d6` 3px line with 4px dots through years `[2010, 2012, 2014, 2016]`, TB `[0.3, 0.7, 1.2, 2.0]`; bold 12px blue label "database size" near its right end.
- **RAM line:** green `#008300` 3px line with 4px dots through the same years, TB `[0.1, 0.4, 0.8, 1.5]`; bold 12px green label "RAM in one SQL box" near its right end.
- **Annotation (bold 13px green `#008300`, near x=2013, y=70):** "RAM grew ~15×, data grew ~7× — the hot set stayed in memory".
- **Caption (12px `#444`, bottom right):** "TB values illustrative; the trend matches their blogged hardware upgrades".

## When One Big Box Really Does Run Out

**Tags:** `common mistake` (red), `limits` (orange)

- **The mistake** — copying thousand-instance architectures because famous companies blog about them
- **Write volume** — a single primary tops out; sustained writes beyond one box genuinely force sharding
- **Data size** — when the working set outgrows the biggest RAM you can buy, caching stops saving you
- **Availability** — one box is one failure domain; strict uptime targets need real redundancy and failover
- **The order** — measure first, tune second, buy RAM third, and distribute only when a hard limit is hit

*Example (italic):* A team measuring 200 requests per second builds a sharded 12-service platform — solving a problem its own numbers say it does not have.

**Common mistake:** Treating "distributed" as the default posture. Scale-out is the correct response to a measured limit — write rate, data size, or an availability target one box cannot meet — never a starting assumption.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: the reflexive scale-out path (complexity with no measured need) vs the measure-first path (small fleet, huge headroom).

- **Title (bold 15px, `#1a5276`, top center):** "Same 200 req/s, Two Architectures".
- **Row 1 (boxes centered on y=105), label 12px `#444` at x=20:** "reflexive scale-out"; blue `#2a78d6` rounded box at x=170 labeled "measured: 200 req/s" (12px), 3px arrow to a red `#e74c3c` box at x=420 labeled "12 services, 3 DBs, message bus" with bold 12px red "✗ every bug is a distributed trace" beneath it.
- **Row 2 (boxes centered on y=215), label:** "measure first"; blue box at x=170 "measured: 200 req/s", 3px arrow to a green `#008300` box at x=380 labeled "profile + buy RAM", then arrow to a green box at x=560 labeled "2 boxes, 50× headroom" with bold 12px green "✓".
- **Box style:** 150–180px wide, 44px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text, 2px borders in each box's hue.
- **Annotation (bold 13px orange `#d95926`, centered near y=275):** "distribute at a measured limit — write rate, data size, or availability — not by reflex".
- **Caption (12px `#444`, bottom right):** "req/s and service counts illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); server counts (11/4/3/3/2), ~66M page loads/day, ~18 ms renders, and 5–15% web CPU come from a large Q&amp;A site's publicly-blogged 2016-era architecture posts; the derived chain (6M/day, 70/sec, 1.3 in flight) is arithmetic on those figures; TB growth curves and the 200 req/s comparison are invented and labeled illustrative.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
