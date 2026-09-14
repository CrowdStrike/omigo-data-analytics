# Consistent Hashing

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Consistent Hashing

**Subtitle:** Place servers and keys on the same ring and a new server steals only its neighbors' keys — hash(key) mod N reshuffles almost everything

## The Cache That Melted When Server 4 Arrived

**Tags:** `core idea` (blue), `caching` (green), `mod-N trap` (red)

- **The cluster** — a web store caches 900,000 product pages across 3 cache servers
- **The rule** — each key lives on server hash(key) mod 3; every lookup goes straight to it
- **The growth** — traffic doubles, so ops adds a 4th server and switches to mod 4
- **The remap** — 675,000 of 900,000 keys (75%) now hash to a different server: all cache misses
- **The meltdown** — hit rate falls from 96% to 24% and the database absorbs the missing reads
- **The ring** — consistent hashing would have moved only 225,000 keys (25%), one server's share

*Example (italic):* The 4th server was added to reduce load — for the next several minutes it multiplied the database's read load ~19-fold instead.

**Key point:** Consistent hashing maps keys and servers onto the same ring; a key belongs to the next server clockwise, so a new server claims keys only from its immediate neighborhood.

### Visualization (canvas `c1`, 720×300)

Timeline chart comparing cache hit rate through the scale-up: mod-N rehash (hit rate collapses) vs consistent-hash ring (small dip), on a shared time axis.

- **Title (bold 15px, `#1a5276`, top center):** "Adding the 4th Server: Mod-N Empties the Cache, the Ring Barely Dips".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = time "0 min" to "10 min" with 12px `#444` tick labels every 2 minutes; y = cache hit rate 0 to 100%, gridlines `#e5e9ef` at 25/50/75 with 12px `#444` labels.
- **Mod-N line:** orange `#d95926` 3px line through minutes `[0, 2, 3, 3.2, 4, 5, 6, 8, 10]`, hit rate `[96, 96, 95, 24, 38, 52, 64, 80, 90]` — vertical cliff to 24% at minute 3 (75% of keys now cold), slow recovery as the cache re-warms.
- **Ring line:** green `#008300` 3px line through the same minute grid, hit rate `[96, 96, 95, 72, 80, 88, 92, 95, 96]` — shallow dip to 72% (only 25% of keys move).
- **Scale-up marker:** vertical dashed `#6b7280` (dash 4/3) line at minute 3, 12px `#6b7280` label "server 4 added" at its top.
- **Annotation (bold 13px orange `#d95926`, near minute 5, y=200):** "mod 4 moved 675,000 of 900,000 keys".
- **Caption (12px `#444`, bottom right):** "hit rates illustrative".

## Eight Keys and Four Servers on a 100-Slot Ring

**Tags:** `worked example` (blue), `hash ring` (green)

- **The ring** — slots 0–99 arranged in a circle; server A sits at slot 10, B at 45, C at 80
- **The rule** — a key hashed to slot p walks clockwise to the first server it meets
- **Eight keys** — hashes 5, 18, 30, 40, 52, 70, 85, 95 → A gets 3 keys, B gets 3, C gets 2
- **Add D at 60** — D now owns slots 46–60; only the key at 52 moves, from C to D
- **Hand-count** — 1 of 8 keys moved; mod-4 rehashing the same 8 hashes would move 7 of 8
- **Virtual nodes** — giving each server many slots evens out how much of the ring each one owns

*Example (italic):* Adding D at slot 60 moves only the key at 52 (C → D); the other 7 keys keep their server and stay cached.

**Key point:** The only keys that move are those in the arc between the new server and its counter-clockwise neighbor — every other key is untouched.

### Visualization (canvas `c2`, 720×300)

Hash-ring diagram: a circle with three servers and eight keys placed by slot number, then server D inserted at slot 60 claiming one key from C.

- **Title (bold 15px, `#1a5276`, top center):** "Adding Server D at Slot 60: One Key Moves, Seven Stay Put".
- **Ring:** circle centered at (200, 168), radius 105, 2.5px `#e5e9ef` stroke; slot 0 at 12 o'clock, clockwise, slot p at angle −90° + 3.6°·p; small `#6b7280` ticks at slots 0/25/50/75 with 11px `#6b7280` slot-number labels just outside.
- **Servers (9px dots on the ring, bold 12px labels outside):** A blue `#2a78d6` at slot 10, B green `#008300` at slot 45, C violet `#4a3aa7` at slot 80, D orange `#d95926` at slot 60 drawn with a 2px dashed orange halo and label "D (new)".
- **Claimed arc:** 4px orange `#d95926` arc along the ring from slot 46 to slot 60.
- **Keys:** 5px `#2c3e50` dots just inside the ring at slots `[5, 18, 30, 40, 52, 70, 85, 95]`, each with an 11px `#6b7280` slot-number label; the dot at slot 52 drawn magenta `#d55181` with a short 2px magenta arrow toward D and bold 12px magenta label "52: C → D".
- **Right panel (12px `#444`, starting x=445, y=110, line height 22):** "before: A 3 · B 3 · C 2" and "after: A 3 · B 3 · C 1 · D 1".
- **Annotation (bold 13px green `#008300`, x=445, y=180):** "1 of 8 keys moves — mod-4 would move 7 of 8".
- **Caption (12px `#444`, bottom right):** "slot positions illustrative".

## Every System That Adds and Removes Machines

**Tags:** `where it's used` (blue), `scaling` (green), `sharding` (orange)

- **Distributed caches** — memcached-style clients hash keys onto a server ring so deploys keep caches warm
- **Sharded stores** — Dynamo-style databases place data and nodes on a ring so nodes can join live
- **CDNs** — the technique was invented (1997) to spread requests across cache fleets that change hourly
- **Load balancers** — ring-hashing a user id keeps sessions sticky while backends come and go
- **The arithmetic** — going from N to N+1 servers, mod-N moves N/(N+1) of keys; a ring moves 1/(N+1)

*Example (italic):* Growing a fleet from 10 to 11 servers remaps 90.9% of keys under mod-N but only 9.1% on a ring.

**Key point:** Any system where the server count changes while data must stay findable — scale-up, scale-down, or crash — needs key placement that barely depends on N.

### Visualization (canvas `c3`, 720×300)

Horizontal grouped bar chart: share of keys that move under three fleet changes, mod-N rehash vs consistent-hash ring.

- **Title (bold 15px, `#1a5276`, top center):** "Share of Keys That Move When the Fleet Changes".
- **Axis:** vertical 2px `#999` baseline at x=180, bars extend right, max width 420 = 100% of keys; no x gridlines, 11px value labels at bar ends.
- **Rows (row centers at y = 85, 155, 225, left-aligned 12px `#444` labels at x=20):** "3 → 4 servers", "10 → 11 servers", "10 → 9 (one dies)"; each row has an orange `#d95926` mod-N bar above a green `#008300` ring bar, 14px tall with a 6px gap.
- **Mod-N bar values:** `[75, 90.9, 90]` % moved → pixel widths `[315, 382, 378]`.
- **Ring bar values:** `[25, 9.1, 10]` % moved → pixel widths `[105, 38, 42]`.
- **Legend (12px, top right):** orange swatch "mod-N rehash", green swatch "consistent-hash ring".
- **Annotation (bold 13px violet `#4a3aa7`, right side near y=260):** "the ring moves only what mod-N would keep".
- **Caption (12px `#444`, bottom right):** "percentages are exact expected fractions".

## Two Ways to Get Burned: Mod-N, and Too Few Virtual Nodes

**Tags:** `common mistake` (red), `hot spots` (orange)

- **The mod-N trap** — hash(key) mod N bakes the fleet size into every key's home; any scaling changes N
- **The blast radius** — one added server invalidates most of the cache and the database eats the misses
- **One slot is luck** — with a single ring slot each, our four servers own 30, 35, 20, and 15% of the ring
- **Hot spot** — B serves more than twice D's traffic; a busy server can melt while its neighbors idle
- **Virtual nodes** — giving each server 150 slots slices the ring finely: loads land at 26/25/24/25%
- **Failure bonus** — a dead server's 150 small arcs scatter over all survivors, not one unlucky neighbor

*Example (italic):* With one slot each, B carries 35% of keys and D just 15%; with 150 virtual nodes each, every server lands within 1% of the fair 25%.

**Common mistake:** Choosing the ring over mod-N is only half the job — a ring with one slot per server trades the remap problem for a hot-spot problem. Virtual nodes are part of the design, not an optimization.

### Visualization (canvas `c4`, 720×300)

Grouped bar chart: share of the keyspace owned by each of the four servers, with one ring slot each vs 150 virtual nodes each.

- **Title (bold 15px, `#1a5276`, top center):** "One Slot per Server vs 150 Virtual Nodes: Load Evens Out".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = share of keys 0 to 40%, gridlines `#e5e9ef` at 10/20/30 with 12px `#444` labels; x = server groups A, B, C, D centered at x = 150, 290, 430, 570 with 13px `#444` labels below the baseline.
- **One-slot bars (orange `#d95926`, 42px wide, left of each group center):** shares `[30, 35, 20, 15]` % — the arc lengths from the worked example's ring (A owns 81–10, B 11–45, C 61–80, D 46–60).
- **Vnode bars (green `#008300`, 42px wide, right of each group center):** shares `[26, 25, 24, 25]` %.
- **Value labels:** 12px `#2c3e50` percentages above every bar.
- **Fair-share line:** horizontal dashed `#6b7280` (dash 4/3) line at 25%, 12px `#6b7280` label "fair share 25%" at its left end.
- **Legend (12px, top right):** orange swatch "1 slot each", green swatch "150 virtual nodes each".
- **Annotation (bold 13px green `#008300`, top center near y=75):** "150 slots each: every server within 1% of fair".
- **Caption (12px `#444`, bottom right):** "load shares illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); hit-rate curves, ring slot positions (servers 10/45/80/60, keys 5/18/30/40/52/70/85/95), and per-server load shares (30/35/20/15 vs 26/25/24/25) are invented and labeled illustrative; the keys-moved percentages (75 vs 25, 90.9 vs 9.1, 90 vs 10) are the exact expected fractions for mod-N vs ring rehashing, and 675,000 / 225,000 are 75% / 25% of the 900,000-key cache.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
