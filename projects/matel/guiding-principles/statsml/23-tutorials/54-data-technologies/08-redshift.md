# Redshift

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Redshift

**Subtitle:** Amazon's 2013 warehouse splits one giant table across many machines so a single SQL query runs on all of them at once — appliance-class power priced like a cloud service

## Four Machines Pretending to Be One Database

**Tags:** `core idea` (blue), `MPP` (green), `cloud warehouse` (orange)

- **The table** — a retailer's orders table holds 800 million rows, too big for one database server
- **The cluster** — Redshift spreads it over 4 compute nodes, each keeping 200 million rows on local disk
- **The leader** — a leader node takes your SQL, plans it, and ships compiled steps to every compute node
- **Columnar** — each node stores data by column, so a 2-column query never reads the other 18 columns
- **The lineage** — AWS built it on ParAccel's MPP engine and launched the service in February 2013

*Example (italic):* You type one SELECT at the leader; four machines scan in parallel and the leader stitches four partial answers into one result set.

**Key point:** MPP — massively parallel processing — means the table is pre-split across nodes and every query runs on all of them at once; the cluster answers as if it were one database.

### Visualization (canvas `c1`, 720×300)

Architecture diagram: a SQL client box on top, a leader node below it, and four compute node boxes along the bottom, each holding an equal slice of the orders table.

- **Title (bold 15px, `#1a5276`, top center):** "One Leader, Four Compute Nodes, One Table Split Four Ways".
- **Client box:** rounded rect x=285, y=42, width 150, height 34, fill `#f4f5f7`, 2px `#6b7280` border, 8px radius; 12px `#2c3e50` label "SQL client" centered.
- **Leader box:** rounded rect x=260, y=100, width 200, height 44, fill `rgba(74,58,167,0.12)`, 2px `#4a3aa7` border, 8px radius; bold 13px `#4a3aa7` label "leader node" at y=118, 11px `#6b7280` sub-label "plans + merges, stores no data" at y=134.
- **Compute boxes (y=190, each 150 wide × 58 tall, 8px radius, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border):** at x=30, 200, 370, 540; bold 12px `#1a5276` top line "node 1".."node 4", 12px `#2c3e50` second line "200M rows" in each.
- **Arrows:** 2px `#6b7280` arrow from client bottom to leader top; 3px `#2a78d6` arrows from leader bottom fanning to the top center of each compute box.
- **Annotation (bold 13px green `#008300`, right side near y=165):** "one query → four parallel scans".
- **Caption (12px `#444`, bottom right):** "row counts illustrative; leader/compute split is Redshift's documented architecture".

## One Query Fans Out Across 800 Million Rows

**Tags:** `worked example` (blue), `parallel scan` (green)

- **The query** — total revenue by month over all 800M rows: one SELECT with a GROUP BY month
- **The fan-out** — the leader sends the scan to all 4 nodes; each scans its own 200M rows simultaneously
- **Columnar saving** — the query touches 2 of 20 columns, so each node reads 1/10 of its bytes (exact)
- **The rate** — at an illustrative 1 million rows per second per node, each node finishes in 200 seconds
- **The merge** — the leader adds 4 partial month-totals; wall time ≈ 200s instead of 800s on one machine

*Example (italic):* The same scan on one server takes ~800 seconds; the 4-node cluster returns in ~200 — the speedup equals the node count when rows split evenly (4× exact at the stated rate).

**Key point:** Parallel scan time is rows-per-node divided by scan rate — the query costs one node's share of the work, and the leader's merge of 12 monthly subtotals is nearly free.

### Visualization (canvas `c2`, 720×300)

Gantt-style timeline: one long bar for a single-server scan of 800M rows, then four short parallel bars for the cluster nodes, all on a shared seconds axis.

- **Title (bold 15px, `#1a5276`, top center):** "The Same 800M-Row Scan: One Machine vs Four in Parallel".
- **Axes:** baseline y=260, plot from x=170 to x=650 (0.6 px per second); x = seconds 0 to 800 with 12px `#444` tick labels at 0/200/400/600/800; light gridlines `#e5e9ef` at each tick from y=55 to y=260; 12px `#444` row labels left-aligned at x=15.
- **Row "single server" (bar center y=85, 24px tall):** orange fill `rgba(217,89,38,0.25)`, 2px `#d95926` border, from x=170 to x=650 (0–800s); bold 12px `#d95926` label "800M rows — 800s" centered inside.
- **Rows "node 1".."node 4" (bar centers y=140, 175, 210, 245, each 22px tall):** green fill `rgba(0,131,0,0.30)`, 2px `#008300` border, each from x=170 to x=290 (0–200s); 11px `#008300` label "200M — 200s" at each bar's right end.
- **Finish marker:** vertical dashed `#6b7280` (dash 4/3) line at x=290, bold 12px `#008300` label "cluster done at 200s" just right of it near y=125.
- **Annotation (bold 13px violet `#4a3aa7`, left-aligned at x=330, two lines y=178/196):** "4 nodes at once → 4× faster," / "and only 2 of 20 columns ever read".
- **Caption (12px `#444`, bottom right):** "1M rows/s per node illustrative; 4× speedup exact at that rate".

## The $25,000 Terabyte Becomes a $1,000 Terabyte

**Tags:** `where it's used` (blue), `cloud economics` (green), `history` (orange)

- **The incumbent** — in 2012 warehousing meant on-prem MPP appliances at roughly $19,000–25,000 per TB per year
- **The launch pitch** — Redshift on-demand worked out near $3,723/TB/yr, and under $1,000 with a 3-year reservation
- **No forklift** — a cluster starts from a web console in minutes instead of a months-long hardware purchase
- **The chores** — you size the cluster, pick distribution and sort keys, and run VACUUM after deletes
- **The evolution** — Spectrum (2017) let queries reach into S3; RA3 nodes (2019) split storage from compute

*Example (italic):* AWS's launch math: a warehouse that had cost a mid-size company millions up front became a few thousand dollars a month entered on a form.

**Key point:** Redshift's disruption was economic — appliance-class columnar MPP at a small fraction of appliance prices — but the classic model's price is cluster management: sizing nodes, keying tables, running VACUUM, chores later serverless designs removed.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: cost of one terabyte-year of warehouse in 2013 under three options, with the on-prem appliance as the giant bar.

- **Title (bold 15px, `#1a5276`, top center):** "Price of a Terabyte-Year of Warehouse at Redshift's 2013 Launch".
- **Axis:** vertical 2px `#999` baseline at x=240, bars extend right, max width 440, widths proportional to dollars; 12px `#444` two-line row labels left-aligned at x=20.
- **Rows (bar centers at y = 90, 155, 220, bars 26px tall, bold 12px value labels at bar ends):**
  - "on-prem MPP appliance / (AWS's 2012 comparison)": red `#e74c3c` fill `rgba(231,76,60,0.25)`, 2px `#e74c3c` border, width 440, label "$25,000/TB/yr" in `#e74c3c`
  - "Redshift on-demand / ($0.85/hr XL node, 2 TB)": blue `#2a78d6` fill `rgba(42,120,214,0.30)`, width 66, label "$3,723/TB/yr" in `#2a78d6`
  - "Redshift 3-yr reserved": green `#008300` fill `rgba(0,131,0,0.30)`, width 18, label "under $1,000/TB/yr" in `#008300`
- **Annotation (bold 13px magenta `#d55181`, near x=330, y=250):** "reserved price ≈ 1/25th of the appliance".
- **Caption (12px `#444`, bottom right):** "figures from AWS's 2012–13 launch materials; bar widths proportional to $/TB/yr".

## The Distribution Key That Sent Everything to Node 2

**Tags:** `common mistake` (red), `data skew` (orange)

- **The choice** — someone sets DISTKEY(country) so every row for a country lives on the same node
- **The skew** — one country accounts for 440M of the 800M orders; that node holds 440M rows, the other three ~120M each
- **The stall** — the revenue query now waits on the fat node: 440s, while three nodes sit idle after ~120s
- **The rule** — a query finishes when its slowest node finishes; skew turns a parallel scan back into a serial one
- **The fix** — distribute on a high-cardinality even key (order id, or EVEN) and save KEY distribution for join columns

*Example (italic):* Same cluster, same query: EVEN distribution returns in 200s, DISTKEY(country) in 440s — 2.2× slower with zero hardware change.

**Common mistake:** Picking the distribution key by what is convenient to filter or group on instead of how evenly it spreads rows. A lopsided or low-cardinality key piles data onto one node, and the whole cluster waits for that node.

### Visualization (canvas `c4`, 720×300)

Grouped vertical bar chart: rows per node under EVEN distribution (four equal bars) vs DISTKEY(country) (one giant bar, three small ones), with finish times.

- **Title (bold 15px, `#1a5276`, top center):** "Rows per Node: EVEN vs DISTKEY(country) — the Cluster Waits for Node 2".
- **Axes:** baseline 2px `#999` at y=245, plot height 180 (0.4 px per million rows); y gridlines `#e5e9ef` at 100M/200M/300M/400M (y = 205/165/125/85) with 11px `#6b7280` labels at x=38; group labels bold 12px `#444` centered under each group at y=268: "EVEN" and "DISTKEY(country)".
- **EVEN bars (green fill `rgba(0,131,0,0.30)`, 2px `#008300` border, 34px wide, at x = 80, 125, 170, 215):** all height 80 (200M); 11px `#008300` label "200M" atop each.
- **Skewed bars (34px wide, at x = 400, 445, 490, 535):** node 1 height 48 (120M) blue fill `rgba(42,120,214,0.30)` with 2px `#2a78d6` border; node 2 height 176 (440M) red fill `rgba(231,76,60,0.25)` with 2px `#e74c3c` border and bold 11px `#e74c3c` label "440M" atop; nodes 3–4 height 48 (120M) blue; 11px `#2a78d6` "120M" atop each blue bar.
- **Time labels:** bold 12px `#008300` "all done in 200s" centered over the EVEN group near y=140; bold 12px `#e74c3c` "query waits 440s" beside the red bar near (x=575, y=80).
- **Annotation (bold 13px orange `#d95926`, centered near x=360, y=55):** "query time = the tallest bar".
- **Caption (12px `#444`, bottom right):** "row counts and 1M rows/s rate illustrative; per-node math exact".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all values are the hardcoded numbers above (no randomness). Documented Redshift facts: ParAccel-based engine, February 2013 launch, leader/compute-node cluster architecture, columnar storage, distribution styles (KEY/EVEN/ALL), sort keys and VACUUM maintenance, Redshift Spectrum (2017), RA3 managed-storage nodes (2019), and the launch-era pricing ($19,000–25,000/TB/yr on-prem comparison, ~$3,723/TB/yr on-demand, under $1,000/TB/yr 3-yr reserved). Invented and labeled illustrative: the 800M-row orders table, 4-node cluster, 20 columns, the 1M rows/s scan rate, and the 440M/120M skew split. Derived arithmetic (200M per node, 200s vs 800s, 1/10 of bytes for 2 of 20 columns, 440s vs 200s = 2.2×, reserved ≈ 1/25th of $25,000) is exact given those inputs.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
