# Separation of Storage and Compute

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Separation of Storage and Compute

**Subtitle:** Keep the data in cheap object storage and rent compute only while a job runs — the cluster becomes disposable, the data does not

## The Orders Job That Kept a Cluster Alive

**Tags:** `core idea` (blue), `S3 + ephemeral clusters` (green), `architecture` (orange)

- **The job** — a nightly analytics job scans a 2 TB orders dataset and takes 2 hours on 20 nodes
- **The old way** — classic Hadoop stores the orders data on the workers' own disks (HDFS)
- **The trap** — turn those 20 nodes off and the data goes with them, so the cluster runs 24/7
- **The shift** — move the 2 TB to object storage (S3); the workers now hold no data at all
- **The payoff** — spin up 20 nodes at 1am, run the 2-hour job, kill the cluster; the data stays put

*Example (italic):* At 3am the cluster is deleted; the orders dataset sits untouched in S3, waiting for tomorrow's 20 fresh nodes.

**Key point:** Coupled architecture ties data lifetime to cluster lifetime; separating them makes compute ephemeral and storage permanent — this is the definition of storage/compute separation.

### Visualization (canvas `c1`, 720×300)

Side-by-side architecture diagram: coupled Hadoop cluster (data on worker disks, always on) vs separated (S3 below, disposable cluster above), with a vertical divider.

- **Title (bold 15px, `#1a5276`, top center):** "Coupled: Data Lives on the Workers — Separated: Data Lives in S3".
- **Divider:** vertical 1px `#e5e9ef` line at x=360; 13px bold `#2c3e50` sub-labels "coupled (classic Hadoop)" at x=180 y=55 and "separated (S3 + ephemeral)" at x=540 y=55, both centered.
- **Left half:** three worker boxes (90×70, 8px radius, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border) at x=45/145/245, y=90; each contains 12px `#2c3e50` text "worker" and a small inner disk box (60×22, fill `rgba(212,81,129,0.18)`, 1px `#d55181` border) labeled "orders shard" (11px). Below at y=215, bold 12px red `#e74c3c` centered at x=180: "kill a node = lose its shard — runs 24/7".
- **Right half:** one wide S3 box (280×46, x=410, y=200, fill `rgba(0,131,0,0.12)`, 2px `#008300` border) labeled "S3: orders/ 2 TB" (bold 12px `#008300`); three worker boxes (80×55, fill `rgba(42,120,214,0.15)`, 2px dashed `#2a78d6` border) at x=415/510/605, y=90, each labeled "worker" (12px) with no disk inside; three 2px `#6b7280` arrows from each worker down to the S3 box.
- **Annotation (bold 13px green `#008300`, centered x=550, y=175):** "cluster is disposable, data is not".
- **Caption (12px `#444`, bottom right):** "diagram schematic, illustrative".

## Pricing the Two Architectures

**Tags:** `worked example` (blue), `cost model` (green)

- **The rate** — each node costs $0.50/hr, so the 20-node cluster costs $10 for every hour it is up
- **Coupled** — the cluster must stay up to keep the data: 720 hr/month × $10 = $7,200/month
- **Separated compute** — 2 hr/night × 30 nights × $10 = $600/month, paid only while the job runs
- **Separated storage** — 2 TB in S3 at $0.023/GB-month ≈ $47/month, paid whether or not compute runs
- **Hand-check** — $600 + $47 = $647 vs $7,200: the same nightly job for roughly 1/11th the bill

*Example (italic):* The job itself is identical both nights — the $6,553/month difference is purely the idle hours the coupled cluster spends babysitting its own disks.

**Key point:** Separation changes the cost model from "pay to keep data alive" to "pay for storage always (cheap) plus compute only while running (metered)".

### Visualization (canvas `c2`, 720×300)

Vertical bar chart of monthly cost: one tall coupled bar vs one short stacked separated bar (compute + storage), shared dollar axis.

- **Title (bold 15px, `#1a5276`, top center):** "Same Nightly Job, Two Bills: $7,200 vs $647 per Month".
- **Axes:** origin x=70, baseline y=250, plot width 580, plot height 190; y = $0 to $8,000 with gridlines `#e5e9ef` and 12px `#444` labels at 2,000/4,000/6,000/8,000.
- **Coupled bar:** centered x=230, width 120, height scaled to $7,200 (≈171px), fill `rgba(231,76,60,0.30)`, 2px `#e74c3c` border; bold 13px `#e74c3c` value label "$7,200" above; 12px `#444` label "coupled — cluster up 24/7" below baseline.
- **Separated bar (stacked):** centered x=490, width 120; bottom segment $600 (≈14px, fill `rgba(42,120,214,0.35)`, 1px `#2a78d6` border) labeled "compute $600" (11px `#2a78d6`, to the right of the segment); top segment $47 (≈2px, solid `#008300`) labeled "storage $47" (11px `#008300`, to the right, staggered 6px above the segment top to clear the compute label); bold 13px `#008300` value label "$647" above; 12px `#444` label "separated — 2 hr/night" below baseline.
- **Annotation (bold 13px violet `#4a3aa7`, centered x=360, y=95):** "~11× cheaper: idle hours were the real cost".
- **Caption (12px `#444`, bottom right):** "rates illustrative: $0.50/node-hr, $0.023/GB-mo".

## One Copy of Data, Many Engines

**Tags:** `where it's used` (blue), `shared data` (green)

- **One source** — the 2 TB orders dataset in S3 is the single copy every tool reads
- **Many readers** — the nightly Spark job, a SQL warehouse, an ML training run, and ad-hoc notebooks
- **No copies** — in the coupled world each engine needed the data loaded into its own cluster's disks
- **Independent scaling** — the ML team runs 40 nodes for an hour; the SQL team runs 4 all day; neither waits
- **Independent failure** — killing the notebook cluster cannot corrupt or delete the shared orders data

*Example (italic):* On Monday the ML team spins up 40 nodes against the same S3 path the 20-node nightly job used on Sunday — no export, no copy, no coordination.

**Key point:** Because storage outlives any cluster, multiple engines of different sizes and lifetimes can share one authoritative copy of the data — the pattern behind modern lakehouse stacks.

### Visualization (canvas `c3`, 720×300)

Hub-and-spoke diagram: central S3 dataset box with four engine boxes around it, each spoke labeled with that engine's cluster size and lifetime.

- **Title (bold 15px, `#1a5276`, top center):** "Four Engines, One Copy: s3://lake/orders/".
- **Hub:** rounded box 200×54 centered at (360, 165), fill `rgba(0,131,0,0.12)`, 2px `#008300` border, bold 13px `#008300` two-line text "S3: orders/" / "one 2 TB copy".
- **Spoke boxes (140×44, 8px radius, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border, 12px `#2c3e50` text, each connected to the hub by a 2px `#6b7280` line):**
  - "Spark nightly job" at (110, 75) with 11px `#6b7280` spoke label "20 nodes, 2 hr"
  - "SQL warehouse" at (610, 75) with spoke label "4 nodes, all day"
  - "ML training" at (110, 250) with spoke label "40 nodes, 1 hr"
  - "ad-hoc notebooks" at (610, 250) with spoke label "2 nodes, on demand"
- **Annotation (bold 13px violet `#4a3aa7`, centered x=360, y=280):** "engines come and go — the data never moves".
- **Caption (12px `#444`, bottom right):** "cluster sizes illustrative".

## Not Free: Network Reads vs Data Locality

**Tags:** `common mistake` (red), `trade-off` (orange)

- **The confusion** — treating S3 like a local disk: every read now crosses the network
- **Locality lost** — Hadoop's trick was moving compute to the data; separation moves data to the compute
- **The numbers** — the 2 TB scan runs in 8 min on local disks, 12 min cold over the network
- **The mitigation** — a local SSD/RAM cache on the workers brings warm re-reads back to about 8.5 min
- **The mistake** — benchmarking only the cold first read and concluding separation "doesn't scale"

*Example (italic):* The nightly job's first pass over the orders data pays the 12-minute network price; its second and third passes hit the cache and run at near-local speed.

**Common mistake:** Assuming separation costs nothing. The first network read is genuinely slower than a local-disk read — the architecture works because caching makes repeat reads cheap, not because the network penalty disappeared.

### Visualization (canvas `c4`, 720×300)

Horizontal bar chart: time to scan the 2 TB orders dataset under three read paths — local disk, cold network read from S3, network read with warm cache.

- **Title (bold 15px, `#1a5276`, top center):** "Scanning 2 TB: Local Disk vs Cold S3 vs Cached S3".
- **Axis:** vertical 2px `#999` baseline at x=230, bars extend right, max width 420 scaled to 14 minutes; 12px `#444` tick labels "0 / 5 / 10 min" along a light `#e5e9ef` grid.
- **Rows (bar height 26px, at y = 85, 150, 215, each with a right-aligned 12px `#444` label ending at x=220):**
  - "local disk (coupled)": green `#008300` bar to 8 min (width 240), 12px value label "8 min" at bar end
  - "S3 cold read (separated)": red `#e74c3c` bar to 12 min (width 360), value label "12 min"
  - "S3 + warm cache": blue `#2a78d6` bar to 8.5 min (width 255), value label "8.5 min"
- **Bracket:** thin dashed `#6b7280` vertical line (dash 4/3) at the 8-min mark spanning all rows, 11px `#6b7280` label "local-disk baseline" at its top.
- **Annotation (bold 13px orange `#d95926`, right side near y=260):** "pay the network price once, then cache".
- **Caption (12px `#444`, bottom right):** "scan times illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all values are the hardcoded numbers above (no randomness); node rate ($0.50/hr), S3 price ($0.023/GB-mo), cluster sizes, and scan times (8 / 12 / 8.5 min) are invented and labeled illustrative; the cost arithmetic ($10/hr × 720 = $7,200; $10 × 2 × 30 = $600; $600 + $47 = $647) must stay internally consistent between text and charts.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
