# Hadoop

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Hadoop

**Subtitle:** Hadoop turned a rack of cheap machines into one giant disk and one giant computer — split the file into replicated blocks, then send the program to the data instead of the data to the program

## Ten Machines, One Giant Log

**Tags:** `core idea` (blue), `data locality` (green), `HDFS` (orange)

- **The log** — a 1 TB web-server log to word-count, too big for one disk to scan quickly (illustrative)
- **The split** — HDFS chops it into 128 MB blocks: 8,192 blocks for the terabyte
- **The spread** — each block is copied onto 3 of the 10 machines, so two dead machines lose nothing
- **The old move** — pulling 1 TB to one compute node at 1 Gbps takes ~2.2 hours before any counting
- **The inversion** — Hadoop ships the 1 MB counting program to all 10 machines instead

*Example (italic):* Machine 4's mappers process ~820 of the 8,192 blocks from its local copies, and the network carries only the little program and the results.

**Key point:** HDFS makes ten commodity machines act as one giant replicated disk, and MapReduce sends the computation to wherever the blocks already sit — that is data locality.

### Visualization (canvas `c1`, 720×300)

Two-row flow diagram contrasting the two directions of movement: shipping the terabyte to the compute (slow) vs shipping the tiny program to the data (fast).

- **Title (bold 15px, `#1a5276`, top center):** "Move the Computation, Not the Terabyte".
- **Row 1 (boxes centered on y=95), label 12px `#444` at x=20:** "the old way"; blue `#2a78d6` rounded box at x=140, 170px wide × 44px tall, label "cluster disks — 1 TB log" (12px `#2c3e50`); thick 6px red `#e74c3c` arrow to a red-bordered box (fill `rgba(231,76,60,0.12)`) at x=460, 190×44, label "one compute node"; bold 12px red label above the arrow: "1 TB over the wire ≈ 2.2 hr at 1 Gbps".
- **Row 2 (y=205), label:** "the Hadoop way"; green `#008300` rounded box at x=140, 170×44, label "job jar — 1 MB"; three thin 2px `#008300` arrows fanning right to three stacked boxes at x=440, each 220px wide × 26px tall at y=170 / 205 / 240, fills `rgba(0,131,0,0.12)`, 12px labels "machine 1 — maps its ~100 GB", "machine 2 — maps its ~100 GB", "… machines 3–10 likewise" (third box dashed 2px `#6b7280` border, `#6b7280` text); bold 12px green label under the jar box: "10 MB shipped total".
- **Box style:** 8px radius, 2px borders matching fill hue, text centered.
- **Caption (12px `#444`, bottom right):** "sizes and 1 Gbps illustrative; transfer time exact for those rates".

## Counting "error" with Map, Shuffle, Reduce

**Tags:** `worked example` (blue), `map/shuffle/reduce` (green)

- **The map** — each machine scans only its local blocks and emits per-word counts like (error, 312)
- **The shuffle** — every (error, n) pair from all 10 machines is routed to the same reducer
- **The reduce** — that reducer just adds ten partial counts: 312 + 287 + 305 + … = 3,005
- **Hand-check** — no machine ever reads another machine's blocks; only tiny (word, count) pairs move
- **The failure** — machine 7 dies mid-job; its blocks have replicas, so its map tasks rerun elsewhere

*Example (italic):* The reducer for "error" receives ten numbers — 312, 287, 305, 298, 291, 320, 284, 309, 296, 303 — and writes one line of output: error 3,005.

**Key point:** MapReduce is three moves — map where the data lives, shuffle pairs by key so equal words meet, reduce by summing — and the shuffle is the only step that touches the network.

### Visualization (canvas `c2`, 720×300)

Three-stage flow diagram: mapper boxes on the left with their local partial counts, shuffle lines crossing in the middle grouped by word, two reducer boxes on the right with the grand totals.

- **Title (bold 15px, `#1a5276`, top center):** "Word Count: Map Locally, Shuffle by Key, Reduce to Totals".
- **Mapper boxes (left column, x=50, 200px wide × 40px tall, 8px radius, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border, 12px `#2c3e50` two-line labels), tops at y = 55, 105, 155:** "machine 1" / "(error, 312) (warn, 45)"; "machine 2" / "(error, 287) (warn, 51)"; "machine 3" / "(error, 305) (warn, 48)".
- **Fourth box (x=50, y=210, 200×34):** dashed 2px `#6b7280` border, fill `#f4f5f7`, 12px `#6b7280` label "machines 4–10 …".
- **Reducer boxes (right column, x=500, 200px wide × 44px tall):** at y=80 fill `rgba(0,131,0,0.12)`, 2px `#008300` border, bold 12px `#008300` label "reducer A" with second line "error → 3,005"; at y=180 fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border, bold 12px `#2a78d6` label "reducer B" with second line "warn → 472".
- **Shuffle lines:** 2px violet `#4a3aa7` lines from the right edge of all four mapper boxes to reducer A's left edge; 2px aqua `#199e70` lines from the same four boxes to reducer B's left edge.
- **Annotation (bold 12px violet `#4a3aa7`, centered near x=370, y=42):** "shuffle: same word → same reducer".
- **Caption (12px `#444`, bottom right):** "counts illustrative; the totals are the exact sums of the ten partials".

## The Ecosystem It Spawned — and What Replaced It

**Tags:** `where it's used` (blue), `ecosystem` (green), `history` (orange)

- **The papers** — Google published GFS (2003) and MapReduce (2004); Hadoop is their open-source clone
- **The layers** — Hive added SQL, Pig added scripting, HBase added key lookups — all on the same HDFS files
- **YARN** — Hadoop 2 (2013) split resource management out, so engines other than MapReduce could share the cluster
- **The displacement** — Spark took the compute (memory between steps, not disk); object storage took the storage
- **The legacy** — data locality, shuffle-by-key, and schema-on-read live on in Spark, warehouses, and lakehouses

*Example (italic):* A 2026 Spark job reading Parquet from object storage still runs map-side work, a shuffle, and reducers — the MapReduce shape with the Hadoop machinery swapped out.

**Key point:** The Hadoop software is now the legacy layer, but its two ideas — cheap replicated blocks across commodity machines, and code shipped to data — remain the blueprint of every modern data platform.

### Visualization (canvas `c3`, 720×300)

Horizontal timeline from 2003 to 2020 with alternating up/down milestone stems, tracing the arc from the Google papers to the Spark and object-storage era.

- **Title (bold 15px, `#1a5276`, top center):** "From Google Papers to Legacy Layer in Fifteen Years".
- **Baseline:** 3px `#1a5276` horizontal line at y=170 from x=60 to x=680; 12px `#444` year ticks below it at 2005/2010/2015/2020 (x = 133, 315, 497, 680).
- **Milestones (dot 6px radius on the baseline, 2px stem, bold 12px two-line label at the stem's end; up-stems end at y=95, down-stems at y=245):**
  - x=60, up, blue `#2a78d6`: "2003 — GFS paper"
  - x=96, down, blue `#2a78d6`: "2004 — MapReduce paper"
  - x=169, up, green `#008300`: "2006 — Hadoop at Yahoo"
  - x=242, down, green `#008300`: "2008 — Hive, Pig, HBase"
  - x=424, up, orange `#d95926`: "2013 — YARN (Hadoop 2)"
  - x=460, down, violet `#4a3aa7`: "2014 — Spark top-level"
  - x=642, up, magenta `#d55181`: "2019 — object storage default"
- **Annotation (bold 13px `#1a5276`, near x=430, y=280):** "the papers were public; the whole ecosystem grew from them".
- **Caption (12px `#444`, bottom right):** "years are published/announced dates".

## Not a Database, Not for Small Jobs

**Tags:** `common mistake` (red), `overhead` (orange)

- **The reflex** — teams call 20 GB "big data" and reach for the cluster, but it fits one machine's RAM
- **The overhead** — a MapReduce job spends minutes on JVM launch and scheduling before any real work
- **The crossover** — illustrative: the laptop wins below a few GB; the cluster wins ~10× at a terabyte
- **Not a database** — HDFS has no indexes and no transactions; every query is a full scan of the blocks
- **Small files** — the NameNode keeps every block's metadata in RAM; millions of tiny files choke it

*Example (italic):* Counting errors in a 1 GB slice takes 2 minutes on a laptop and 4 minutes on the ten-machine cluster; at 1 TB the laptop needs ~2,000 minutes and the cluster ~205.

**Common mistake:** Reaching for the cluster because the data is called "big". Hadoop's economics begin where one machine ends — below that, the scheduling and shuffle overhead is all cost and no benefit.

### Visualization (canvas `c4`, 720×300)

Grouped bar chart of job runtime at four data sizes: single laptop vs the 10-node cluster, showing the cluster losing on small inputs and winning at the terabyte.

- **Title (bold 15px, `#1a5276`, top center):** "Laptop vs 10-Node Cluster: Overhead Rules Small Jobs".
- **Axes:** 2px `#999` baseline at y=245 from x=70 to x=680; four group centers at x = 140, 290, 440, 590 with 12px `#444` labels below the baseline: "1 GB", "10 GB", "100 GB", "1 TB"; no y gridlines (heights are hand-log-scaled).
- **Bars (34px wide, laptop bar left of center, cluster bar right, 4px gap):** laptop fill `rgba(42,120,214,0.30)` with 2px `#2a78d6` border; cluster fill `rgba(217,89,38,0.25)` with 2px `#d95926` border.
- **Heights (pixels up from the baseline, log-scaled by hand) and 11px `#444` value labels above each bar:** laptop minutes `[2, 20, 200, 2000]` → heights `[30, 75, 120, 165]`, labels "2 min", "20 min", "200 min", "2,000 min"; cluster minutes `[4, 6, 25, 205]` → heights `[43, 51, 80, 121]`, labels "4 min", "6 min", "25 min", "205 min".
- **Legend (12px, top left near x=80, y=55):** blue swatch "laptop", orange swatch "10-node cluster".
- **Annotation (bold 13px red `#e74c3c`, above the 1 GB group, y=80):** "cluster loses on small data".
- **Annotation (bold 13px green `#008300`, above the 1 TB group, y=60):** "≈10× faster at a terabyte".
- **Caption (12px `#444`, bottom right):** "minutes illustrative; bar heights log-scaled by hand, not linear".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all values are the hardcoded numbers above (no randomness). Documented Hadoop facts: open-source implementation of Google's GFS (2003) and MapReduce (2004) papers, 128 MB default HDFS block size, 3× replication, data locality, map/shuffle/reduce, YARN arriving with Hadoop 2 (2013), Hive/Pig/HBase circa 2008, Spark becoming an Apache top-level project (2014), NameNode holding block metadata in RAM. Invented and labeled illustrative: the 1 TB log, the 10-machine cluster, the ten "error" partials (312, 287, 305, 298, 291, 320, 284, 309, 296, 303), the "warn" partials (45, 51, 48 shown; machines 4–10 sum to 328 by construction so the total is 472), the 1 Gbps link, and all runtimes in c4. Exact arithmetic: 8,192 blocks = 1 TiB / 128 MiB, ~2.2 hr = 8×10^12 bits / 1 Gbps (decimal units), error total 3,005 and warn total 472 as sums of the partials.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
