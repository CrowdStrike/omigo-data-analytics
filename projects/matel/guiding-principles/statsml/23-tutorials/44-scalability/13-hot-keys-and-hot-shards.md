# Hot Keys & Hot Shards

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Hot Keys & Hot Shards

**Subtitle:** Hash sharding spreads keys perfectly evenly — but load follows the celebrities, so one hot account can melt its shard while fifteen others sit idle

## The Celebrity Who Melted Shard 7

**Tags:** `core idea` (blue), `skewed load` (orange), `sharding` (green)

- **The app** — a social app stores 10 million accounts across 16 shards, hashed by account id
- **The balance** — the hash is fair: every shard holds about 625,000 accounts, no favorites
- **The celebrity** — one star account draws 320,000 of the app's 800,000 reads/sec — 40% of all traffic
- **The hot shard** — every read of that one account hashes to shard 7, which now serves 350,000 reads/sec
- **The idle rest** — the other 15 shards each handle about 30,000 reads/sec — a 12x gap

*Example (italic):* Shard 7 pages the on-call at 350,000 reads/sec while shard 8, one slot over, coasts at 30,000 — same hardware, same key count.

**Key point:** Hash sharding balances the number of keys, not the load per key — when traffic follows a power law, one hot key concentrates a huge share of the work on a single shard.

### Visualization (canvas `c1`, 720×300)

Vertical bar chart of reads/sec for all 16 shards: fifteen short blue bars and one towering red bar at shard 7.

- **Title (bold 15px, `#1a5276`, top center):** "Reads/sec per Shard: One Celebrity, One Fire".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = shards 0–15, 12px `#444` tick labels under every bar; y = reads/sec 0 to 400k, gridlines `#e5e9ef` at 100k/200k/300k with 12px `#444` labels "100k"/"200k"/"300k" at x=52 right-aligned.
- **Bars:** 16 slots of 37.5px, bars 26px wide centered in each slot; reads/sec `[30000, 30000, 30000, 30000, 30000, 30000, 30000, 350000, 30000, 30000, 30000, 30000, 30000, 30000, 30000, 30000]`; shard 7 solid red `#e74c3c`, all others fill `rgba(42,120,214,0.35)` with 1px `#2a78d6` edge.
- **Value labels:** bold 12px red "350k" above shard 7's bar; 11px `#6b7280` "30k" above shard 0's bar only.
- **Annotation (bold 13px red `#e74c3c`, near x=340, y=80, arrowed to shard 7's bar):** "one account = 40% of all reads".
- **Caption (12px `#444`, bottom right):** "traffic numbers illustrative".

## Even Keys, Lopsided Load

**Tags:** `worked example` (blue), `power law` (orange)

- **Keys per shard** — 10,000,000 accounts / 16 shards = 625,000 accounts each; the hash nails this
- **Load without the star** — 800,000 − 320,000 = 480,000 reads/sec spread evenly: 30,000 per shard
- **Load on shard 7** — its fair 30,000 plus the star's 320,000 = 350,000 reads/sec
- **Hand-check** — 15 × 30,000 + 350,000 = 800,000 reads/sec, the whole app's traffic accounted for
- **The ratio** — shard 7 runs 350,000 / 30,000 ≈ 12x hotter than its neighbors, with identical key counts

*Example (italic):* Every shard stores the same 625,000 accounts' worth of keys, yet shard 7 does 44% of the cluster's total read work.

**Key point:** Balanced keys and balanced load are different claims — the hash guarantees the first, and a power-law traffic distribution quietly breaks the second.

### Visualization (canvas `c2`, 720×300)

Two horizontal stacked bars on a shared 600px width: top bar = keys per shard (16 equal segments), bottom bar = reads per shard (one giant segment), making the even/skewed contrast visual.

- **Title (bold 15px, `#1a5276`, top center):** "Same Keys per Shard, Very Different Work per Shard".
- **Layout:** both bars start at x=80, full width 600, height 44px; top bar at y=80 with left 12px `#444` label "keys" at x=20 (vertically centered); bottom bar at y=185 with label "reads/sec".
- **Keys bar (top):** 16 equal segments of 37.5px (625k accounts each), fill `rgba(42,120,214,0.30)`, 1px white separators; 11px `#6b7280` "625k each" centered inside; segment 7 gets a 2px `#1a5276` outline to mark it.
- **Reads bar (bottom):** segment widths proportional to load out of 800k over 600px — seven segments of 22.5px (shards 0–6, 30k each, fill `rgba(42,120,214,0.30)`), one segment of 262.5px (shard 7, 350k, solid red `#e74c3c` with bold 12px white centered label "shard 7 — 350k"), eight segments of 22.5px (shards 8–15, fill `rgba(42,120,214,0.30)`); 1px white separators.
- **Connector:** dashed `#6b7280` (dash 4/3) lines linking segment 7's edges on the top bar to the red segment's edges on the bottom bar, showing one thin key slice exploding into a fat load slice.
- **Annotation (bold 13px red `#e74c3c`, centered near y=255):** "1/16 of the keys, 44% of the reads".
- **Caption (12px `#444`, bottom right):** "widths to scale, numbers illustrative".

## You Pay for the Hottest Shard

**Tags:** `why it matters` (blue), `cost` (orange), `capacity planning` (green)

- **The rule** — every shard must be sized for its own peak, so the fleet is sized for shard 7
- **The provision** — shard 7 needs headroom above 350,000, so each shard is bought at 400,000 reads/sec
- **The bill** — 16 shards × 400,000 = 6,400,000 reads/sec of capacity to serve 800,000 — 12.5% used
- **The utilization gap** — shard 7 runs at 87.5% of capacity while the other 15 idle at 7.5%
- **Finding it** — per-shard CPU graphs show the fire; per-key metrics (top-K counters) name the arsonist

*Example (italic):* The cluster is provisioned 8x over its average load, and the finance dashboard calls it "waste" — it is actually the price of one hot key.

**Key point:** With a hot shard you provision the whole fleet for the hottest member, not the average — the skew of one key sets the hardware bill for all sixteen shards.

### Visualization (canvas `c3`, 720×300)

Capacity-vs-load bar chart: 16 slots, each with a light full-height "capacity bought" bar to 400k and a solid "load served" overlay, exposing the idle headroom everywhere except shard 7.

- **Title (bold 15px, `#1a5276`, top center):** "Provisioned for the Hottest: 6.4M Capacity, 800k Load".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = shards 0–15 with 12px `#444` tick labels; y = reads/sec 0 to 400k, gridlines `#e5e9ef` at 100k/200k/300k, 12px labels at x=52.
- **Capacity bars:** 16 bars, 26px wide in 37.5px slots, all full height to 400k, fill `rgba(107,114,128,0.15)` with 1px `#e5e9ef` edge.
- **Load overlays:** solid bars inside each capacity bar, same widths, heights for `[30000, 30000, 30000, 30000, 30000, 30000, 30000, 350000, 30000, 30000, 30000, 30000, 30000, 30000, 30000, 30000]`; shard 7 red `#e74c3c`, others blue `rgba(42,120,214,0.45)`.
- **Capacity line:** dashed `#6b7280` (dash 4/3) horizontal line across the plot at the 400k level, 12px `#6b7280` label "capacity: 400k per shard" above it at the right end.
- **Labels:** bold 12px red "87.5% used" above shard 7; bold 12px `#6b7280` "7.5% used" above shard 2.
- **Annotation (bold 13px violet `#4a3aa7`, near x=430, y=120):** "fleet runs at 12.5% average utilization".
- **Caption (12px `#444`, bottom right):** "capacity and load illustrative".

## Splitting the Key, Not the Cluster

**Tags:** `common mistake` (red), `fixes` (green)

- **The reflex** — "add more shards" fails: hashing still sends every read of one key to one shard
- **Key salting** — split the star into 8 sub-keys (`star#0`…`star#7`); each read picks one at random
- **After the split** — the star's 320,000 reads/sec spread as 40,000 each across up to 8 shards
- **The new peak** — hottest shard drops from 350,000 to 70,000; per-shard provision drops 400k → 100k
- **Other fixes** — front hot items with a dedicated cache; give whale tenants their own isolated shard
- **The cost** — salting works best for read-heavy keys; writes must fan out or reconcile across sub-keys

*Example (italic):* Going from 16 to 64 shards leaves the hottest shard at 327,500 reads/sec — salting the one hot key into 8 sub-keys cuts it to 70,000.

**Common mistake:** Treating a hot key as a capacity problem. Resharding moves the celebrity to a different shard but never splits them — the fix is to spread the key itself (salting), absorb it (cache), or wall it off (isolation).

### Visualization (canvas `c4`, 720×300)

Two side-by-side mini bar panels: left = 16 shards before salting (one 350k spike), right = 16 shards after salting the star key into 8 sub-keys (peak 70k), on a shared y scale.

- **Title (bold 15px, `#1a5276`, top center):** "Salting One Hot Key into 8 Sub-Keys: Peak 350k → 70k".
- **Left panel:** origin x=60, baseline y=245, plot width 280, plot height 180; y = 0 to 400k with gridlines `#e5e9ef` at 100k/200k/300k and 12px `#444` labels at x=52; bold 13px `#2c3e50` panel label "before" centered at y=262; 16 bars of 14px in 17.5px slots, reads `[30000, 30000, 30000, 30000, 30000, 30000, 30000, 350000, 30000, 30000, 30000, 30000, 30000, 30000, 30000, 30000]`, shard 7 red `#e74c3c`, others `rgba(42,120,214,0.35)`; bold 12px red "350k" above the spike.
- **Right panel:** origin x=400, baseline y=245, plot width 280, plot height 180, same y scale and gridlines (no repeated y labels); bold 13px `#2c3e50` panel label "after salting" centered at y=262; 16 bars, reads `[70000, 30000, 70000, 30000, 70000, 30000, 70000, 30000, 70000, 30000, 70000, 30000, 70000, 30000, 70000, 30000]` — the 8 sub-keys land on even-numbered shards (illustrative), each adding 40,000 to its base 30,000; sub-key shards green `#008300`, others `rgba(42,120,214,0.35)`; bold 12px green "70k" above shard 0's bar.
- **Divider:** vertical 1px `#e5e9ef` line at x=350 from y=55 to y=245.
- **Annotation (bold 13px green `#008300`, right panel near x=470, y=90):** "provision 100k/shard instead of 400k".
- **Caption (12px `#444`, bottom right):** "sub-key placement illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); account counts, reads/sec, capacity, and sub-key placement are invented and labeled illustrative; the arithmetic is internally consistent (15×30k + 350k = 800k; 320k/8 = 40k per sub-key; 30k + 40k = 70k; 16×400k = 6.4M; 800k/6.4M = 12.5%; 350k/400k = 87.5%; 30k/400k = 7.5%).
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
