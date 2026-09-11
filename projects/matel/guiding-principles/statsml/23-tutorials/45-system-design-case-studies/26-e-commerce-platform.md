# E-commerce Platform

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** E-commerce Platform

**Subtitle:** Hosting a million merchants means someone, somewhere, is having a flash sale every minute — so the platform treats extreme spikes as the steady state and designs for isolation, not just capacity (publicly blogged architecture concepts)

## Everyone Else's Flash Sale

**Tags:** `core idea` (blue), `multi-tenancy` (green), `spiky load` (orange)

- **The platform** — one codebase and fleet serve over a million merchant stores at once
- **The drop** — a sneaker brand posts a limited release and 80,000 shoppers hit checkout in a minute
- **The rotation** — with a million stores, some merchant's viral moment is always happening right now
- **The steady state** — platform load is never calm: it is a baseline plus a spike that keeps moving
- **The real risk** — one merchant's storm slowing every neighboring store on the same machines

*Example (italic):* At 2:12pm it's a sneaker drop, at 2:27pm a TV-famous sauce brand, at 2:42pm a K-pop merch store — the spike never stops, it just changes owner.

**Key point:** At platform scale, flash sales are not rare events to survive — they are the permanent workload, so the design question shifts from "how big is the peak" to "whose peak hurts whom".

### Visualization (canvas `c1`, 720×300)

Line chart of one hour of platform-wide checkout traffic: a modest baseline punctuated by three successive merchant spikes, each labeled with a different (generic) merchant.

- **Title (bold 15px, `#1a5276`, top center):** "One Hour on the Platform: the Spike Is Always Somewhere".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = minutes 0 to 60 with 12px `#444` tick labels every 15 min ("2:00pm"–"3:00pm"); y = checkouts per second (thousands) 0 to 100, gridlines `#e5e9ef` at 25/50/75.
- **Traffic line:** blue `#2a78d6` 3px line through minutes `[0, 5, 10, 12, 14, 20, 25, 27, 29, 35, 40, 42, 44, 50, 55, 60]`, thousands of checkouts/s `[20, 21, 22, 78, 24, 21, 20, 70, 23, 20, 22, 88, 25, 21, 20, 22]` — flat baseline near 20k with sharp spikes at minutes 12, 27, 42.
- **Spike labels (bold 12px, above each peak):** orange `#d95926` "sneaker drop" at minute 12, magenta `#d55181` "sauce brand" at minute 27, violet `#4a3aa7` "merch drop" at minute 42.
- **Baseline marker:** dashed `#6b7280` (dash 4/3) horizontal line at y for 20k, 12px `#6b7280` label "everyday baseline" at its left end.
- **Annotation (bold 13px green `#008300`, centered at x=220, y=70):** "a million stores = a flash sale every minute".
- **Caption (12px `#444`, bottom right):** "traffic shape illustrative".

## Pods: A Self-Contained Slice per Group of Merchants

**Tags:** `worked example` (blue), `pods / shards` (green), `blast radius` (orange)

- **The pod** — a self-contained slice of the platform: a set of merchants with their own database and workers
- **The routing layer** — every request first looks up shop → pod, then goes only to that pod's resources
- **The math** — 1,000,000 merchants across 100 pods ≈ 10,000 merchants per pod (illustrative split)
- **Hand-check** — a viral drop lands on one pod, so at most ~1% of merchants share the blast radius
- **The escape hatch** — a persistently hot merchant can be moved to a quieter pod, or isolated onto its own

*Example (italic):* When the sneaker drop hammers pod 7, the 990,000 merchants on the other 99 pods never touch pod 7's database — their day is unchanged.

**Key point:** Pods turn one giant shared system into many small independent ones — load and failures stop at the pod boundary, and the routing layer's shop→pod map makes moving a hot merchant a data change, not a redesign.

### Visualization (canvas `c2`, 720×300)

Flow diagram: requests enter a routing layer that maps shop → pod, fanning out to three pod boxes (each "merchants + DB + workers"); a hot merchant is shown being moved out of a busy pod into a dedicated one.

- **Title (bold 15px, `#1a5276`, top center):** "Routing Layer Maps Shop → Pod; Each Pod Stands Alone".
- **Routing box:** rounded box at x=40, y=125, 160px wide, 52px tall, 8px radius, fill `rgba(42,120,214,0.15)`, 12px `#2c3e50` two-line text "routing layer / shop → pod lookup".
- **Pod boxes (right column):** three rounded boxes 190px wide, 52px tall at x=300, y = `[45, 125, 205]`, fill `rgba(0,131,0,0.12)`, 12px text "pod 1 · merchants + DB + workers", "pod 7 · merchants + DB + workers", "pod 42 · merchants + DB + workers"; pod 7's border drawn 2px orange `#d95926` with bold 12px orange label "hot" at its top-right corner.
- **Arrows:** 3px `#2a78d6` arrows from the routing box's right edge (x=200) to each pod box's left edge.
- **Isolation box:** rounded box at x=545, y=125, 150px wide, 52px tall, fill `rgba(230,126,34,0.15)`, 12px text "dedicated pod"; dashed 2px `#d95926` arrow from pod 7 to it with bold 12px orange label "move hot shop" above the arrow.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=282):** "failures and load stop at the pod boundary".
- **Caption (12px `#444`, bottom right):** "pod count and layout schematic".

## BFCM: the Annual Stress Test, with a Waiting Room

**Tags:** `why it matters` (blue), `throttling` (orange), `BFCM` (green)

- **The exam** — Black Friday–Cyber Monday is the platform's annual stress test, stats published publicly after
- **The scale** — the platform reported roughly $9.3B in 2023 BFCM sales, peaking above $4M in sales per minute
- **The storm** — even inside one pod, a big drop can send far more checkouts than inventory or capacity allows
- **The throttle** — platform-level throttling caps checkout flow at a rate the pod can sustain
- **The queue** — excess shoppers go to a documented waiting room and are admitted in order as capacity frees

*Example (italic):* A drop sends 12,000 checkout attempts per minute at a pod that sustains 4,000; 8,000 shoppers per minute join the waiting room instead of crashing the store (rates illustrative).

**Key point:** Throttling plus a queue converts an impossible spike into a sustained maximum — shoppers wait a few minutes in line, but checkout never falls over, for that merchant or for neighbors.

### Visualization (canvas `c3`, 720×300)

Line chart of a 30-minute drop: checkout attempts spike far above a flat throttle cap; the shaded gap between the curves is the waiting room.

- **Title (bold 15px, `#1a5276`, top center):** "Checkout Storm: Attempts Spike, Throughput Holds, the Gap Waits in Line".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = minutes after drop 0 to 30, 12px `#444` tick labels every 10 min; y = thousands per minute 0 to 14, gridlines `#e5e9ef` at 4/8/12.
- **Attempts line:** magenta `#d55181` 3px line through minutes `[0, 2, 4, 6, 8, 10, 12, 15, 20, 25, 30]`, thousands/min `[1, 2, 12, 10, 8, 6, 5, 4, 3, 2, 1]` — sharp spike to 12k at minute 4, decaying after.
- **Throughput line:** green `#008300` 3px line, same minute grid, thousands/min `[1, 2, 4, 4, 4, 4, 4, 4, 3, 2, 1]` — flat cap at 4k while attempts exceed it, tracking attempts otherwise.
- **Waiting-room fill:** shaded region between the two lines wherever attempts exceed throughput, fill `rgba(230,126,34,0.25)`, bold 12px orange `#d95926` label "waiting room" centered inside near minute 6.
- **Cap marker:** dashed `#6b7280` (dash 4/3) horizontal line at 4k, 12px `#6b7280` label "throttle cap 4k/min" at its right end.
- **Annotation (bold 13px green `#008300`, near minute 20, y=75):** "checkout never falls over — the line does the absorbing".
- **Caption (12px `#444`, bottom right):** "rates illustrative; BFCM totals as publicly reported by the platform".

## More Servers Is Not Isolation

**Tags:** `common mistake` (red), `noisy neighbor` (orange)

- **The mistake** — answering "flash sales hurt neighbors" with "add more machines to the shared pool"
- **Shared pool** — one hot merchant's queries still contend for the same database, cache, and workers
- **The spillover** — during a viral drop, innocent neighbor stores see checkout latency blow up too
- **The pod answer** — with per-pod databases and workers, the drop saturates its pod and nothing else
- **The lesson** — multi-tenancy turns capacity planning into isolation design: partition first, then size

*Example (italic):* In a shared pool, three bystander stores see p95 checkout latency jump from ~120ms to ~2s during someone else's drop; in a podded design they stay near 130ms (illustrative).

**Common mistake:** Sizing the platform for the sum of all peaks while leaving tenants sharing everything — the question is never just "is there enough capacity" but "whose spike can reach whose customers".

### Visualization (canvas `c4`, 720×300)

Grouped bar chart: p95 checkout latency of three bystander merchants during a neighbor's viral drop — shared pool vs podded design, with a quiet-day baseline marker.

- **Title (bold 15px, `#1a5276`, top center):** "Bystander Latency During Someone Else's Drop: Shared vs Podded".
- **Axes:** origin x=70, baseline y=245, plot width 590, plot height 180; y = p95 checkout latency (ms) 0 to 2,500, gridlines `#e5e9ef` at 500/1000/1500/2000 with 12px `#444` labels; x = three groups centered at x = `[180, 370, 560]`, 12px `#444` labels "store B", "store C", "store D" below the baseline.
- **Bars per group (two, 44px wide, 10px gap):** shared-pool bar red `rgba(231,76,60,0.75)` then podded bar green `#008300`; shared heights for ms values `[1800, 2100, 1950]`, podded for `[130, 125, 140]`; 11px `#444` ms value labels above each bar.
- **Baseline marker:** dashed `#6b7280` (dash 4/3) horizontal line at 120ms, 12px `#6b7280` label "quiet-day p95 ≈ 120ms" at its left end.
- **Legend (top right, 12px):** red swatch "shared pool", green swatch "podded".
- **Annotation (bold 13px magenta `#d55181`, centered near y=70):** "same total hardware — only the partitioning changed".
- **Caption (12px `#444`, bottom right):** "latencies illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); traffic rates, pod counts, waiting-room rates, and latencies are invented and labeled illustrative; the BFCM figures (~$9.3B total, >$4M/min peak, 2023) are the platform's publicly reported numbers and appear only in text, not in a chart.
- **Content scope:** stick to publicly blogged e-commerce-platform concepts — pods/shards as self-contained merchant slices, a shop→pod routing layer, checkout throttling with a waiting room, and BFCM as the published annual stress test; merchant names in examples are generic inventions.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
