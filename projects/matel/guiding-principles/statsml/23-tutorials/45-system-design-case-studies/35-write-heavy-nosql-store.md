# Write-Heavy NoSQL Store

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Write-Heavy NoSQL Store

**Subtitle:** A device-telemetry service takes about 50 writes for every read — that one ratio picks the storage engine, because a store that never edits a row in place can append instead of seek

## Every Device Writes, Almost Nobody Reads

**Tags:** `core idea` (blue), `the write profile` (green), `sequential vs random` (orange)

- **The load** — 1.2 million field sensors each post one small reading every 10 seconds: 120,000 writes/s
- **The ratio** — dashboards ask for 2,400 reads/s, so the profile is 120,000 &divide; 2,400 = 50 writes per read
- **The read shape** — a read is almost always "this device, last hour", a scan of neighbouring rows
- **In-place is fatal** — editing a row where it sits costs one random seek; at 8 ms that is 125 writes/s per disk
- **Appending is not** — writes that only ever go on the end of a file stream out with no seek between them

*Example (italic):* Illustrative Example — the same bytes are either a few hundred seeking disks or a fraction of one streaming disk; only the access pattern differs.

**Key point:** Write-heavy design starts from one measured ratio. At 50 writes per read you optimise the write path first and accept a slower read, because sequential writing beats random seeks by orders of magnitude.

### Visualization (canvas `c1`, 720&times;300)

Two panels: a bar pair for the measured request profile (left) and a track diagram contrasting scattered random writes with one contiguous append (right).

- **Title (bold 15px, `#1a5276`, top center):** "The Profile: 120,000 Writes/s, 2,400 Reads/s".
- **Divider:** vertical dashed `#6b7280` (dash 4/3) at x=350, y=46 to y=280.
- **Left panel axes:** origin x=95, baseline y=250, plot height 170, max 120,000; gridlines `#e5e9ef` at 40,000/80,000/120,000 with 12px `#444` right-aligned labels "40k"/"80k"/"120k" at x=88; 1px `#999` baseline from x=95 to x=330.
- **Bars (60px wide):** writes centered x=150, value 120,000, fill `rgba(42,120,214,0.30)`, 2px `#2a78d6` border, bold 12px `#2a78d6` label "120,000/s" above; reads centered x=265, value 2,400, fill `rgba(0,131,0,0.25)`, 2px `#008300` border, bold 12px `#008300` label "2,400/s" above; 12px `#444` category labels "writes" and "reads" under the baseline.
- **Left annotation (bold 13px magenta `#d55181`, centered x=210, y=72):** ratio computed at render as `writes / reads` &rarr; "ratio = 50 : 1".
- **Right panel label (bold 13px `#1a5276`, centered x=540, y=66):** "cost of one write on one disk".
- **Random track (y=125):** 2px `#6b7280` horizontal line x=390 to x=690; 8 write marks as 8&times;8 `#e74c3c` squares at x = 402, 448, 470, 528, 560, 605, 641, 678; between consecutive marks a thin 1px `rgba(231,76,60,0.55)` arc bulging 12px above the line (the seek).
- **Random labels:** bold 12px `#e74c3c` left-aligned at (390, 104): "in-place: one 8 ms seek per write"; 12px `#444` at (390, 155), computed at render as `1 / 0.008` &rarr; "&rarr; 125 writes/s per disk".
- **Append track (y=218):** filled rect x=390 to x=690, height 14, fill `rgba(0,131,0,0.25)`, 2px `#008300` border, no gaps.
- **Append labels:** bold 12px `#008300` left-aligned at (390, 200): "append: one contiguous run"; 12px `#444` at (390, 252): "&rarr; no seek between writes".
- **Caption (12px `#444`, bottom right):** "Illustrative Example — 8 ms random seek assumed".

## Buffer in Memory, Log for Safety, Flush, Then Merge

**Tags:** `worked example` (blue), `write path` (green), `append-only` (orange)

- **Batching first** — clients send many rows per request, so the node handles requests, not one round trip per row
- **The commit log** — each batch is appended once to a sequential log on disk, and that append is the durability
- **The memtable** — the same rows also go into a sorted in-memory buffer, so recent data is served from RAM
- **The flush** — when the buffer is full it is written out whole as one immutable sorted file; nothing is edited later
- **The merge** — a background thread compacts those files into fewer, larger ones, arranged in growing levels

*Example (italic):* A batch of readings lands at 09:00, is appended to the log, sits in the memtable, and later becomes part of a sorted file on disk — no row was ever overwritten where it lay.

**Key point:** The write path has exactly one seek-free shape: append the batch to a log, sort it in memory, flush whole files, and let a background thread do the reorganising nobody is waiting on.

### Visualization (canvas `c2`, 720&times;300)

Conceptual flow diagram split into a MEMORY band and a DISK band: batched writes fan into the commit log and the memtable, the memtable flushes to an immutable sorted file, and compaction feeds four growing levels. No numeric annotations.

- **Title (bold 15px, `#1a5276`, top center):** "One Write Path: Log It, Sort It, Flush It, Merge It Later".
- **Regions:** horizontal dashed `#6b7280` (dash 4/3) divider at y=152 from x=25 to x=700; 11px `#6b7280` labels "MEMORY" at (28, 50) and "DISK" at (28, 170).
- **Batch box:** rounded rect x=25 y=76 w=130 h=58, r=8, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border; bold 12px `#2a78d6` "batched writes" at y=100 and 12px `#2c3e50` "arriving" at y=118, centered.
- **Memtable box:** rounded rect x=205 y=70 w=155 h=70, r=8, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border; bold 12px `#2a78d6` "sorted memtable" at y=98, 12px `#2c3e50` "kept in key order" at y=118.
- **Arrow batch &rarr; memtable:** 3px `#2a78d6` from (155, 105) to (200, 105).
- **Commit log box:** rounded rect x=25 y=182 w=150 h=48, r=8, fill `rgba(74,58,167,0.12)`, 2px `#4a3aa7` border; bold 12px `#4a3aa7` "commit log — append" centered.
- **Arrow batch &rarr; log:** 3px `#4a3aa7` from (90, 134) to (90, 178); 12px `#4a3aa7` label "durability" left-aligned at (98, 168).
- **Flush arrow:** 3px `#d95926` from (282, 140) to (282, 186); bold 12px `#d95926` label "flush" left-aligned at (292, 168).
- **Immutable file box:** rounded rect x=205 y=190 w=145 h=58, r=8, fill `rgba(0,131,0,0.12)`, 2px `#008300` border; bold 12px `#008300` "immutable file" at y=214, 12px `#2c3e50` "sorted, never edited" at y=234.
- **Compaction arrow:** 3px `#d95926` from (350, 219) to (425, 219); bold 12px `#d95926` "compaction" centered at (388, 205).
- **Level bars (x=435, height 16, fill `rgba(25,158,112,0.30)`, 2px `#199e70` border):** widths 30 at y=178, 60 at y=200, 110 at y=222, 200 at y=244; 12px `#2c3e50` labels "L1"/"L2"/"L3"/"L4" 8px right of each bar's end.
- **Level annotation (bold 12px `#199e70`, left-aligned at (435, 168)):** "each level larger than the one above".
- **Caption (12px `#444`, bottom right):** "Illustrative Example — schematic, not to scale".

## What You Pay For a Seek-Free Write

**Tags:** `the tradeoff` (orange), `read cost` (blue), `write amplification` (red)

- **Reads get harder** — a key can sit in the memtable or in any file, so one read may check several places
- **The blunting tools** — a per-file key range and a bloom filter let a read skip most files without opening them
- **Scans stay cheap** — each file is sorted, so "this device, last hour" is still a short run of neighbouring rows
- **Write amplification** — leveled compaction rewrites each byte a few dozen times before it settles at the bottom
- **The real trade** — the extra disk work is background work, moved off the path the client is waiting on

*Example (italic):* Illustrative Example — a store with a commit log, a flush, and four merge levels rewrites roughly 42 bytes of disk traffic for each byte the client sent once.

**Key point:** Append-only writing does not make work vanish, it relocates it. You accept a read that consults several files and a lot of background rewriting, and in exchange no client write ever waits on a seek.

### Visualization (canvas `c3`, 720&times;300)

Conceptual before/after: the top row is an in-place store answering a read from one page; the bottom row is the append-only store consulting the memtable and several sorted files. One computed figure for amplification.

- **Title (bold 15px, `#1a5276`, top center):** "The Read Consults Several Places, Not One Page".
- **Row 1 (in-place, cy=95):** 12px `#444` left-aligned label "in-place store" at (25, 62); blue `#2a78d6` rounded box (140&times;40, r=8, fill `rgba(42,120,214,0.15)`, 2px border, 12px `#2c3e50` text) at x=120 labeled "read key k"; 3px `#2a78d6` arrow from (260, 95) to (330, 95); one green box (150&times;40, fill `rgba(0,131,0,0.12)`, 2px `#008300`) at x=330 labeled "one page"; bold 12px `#008300` left-aligned at (500, 99): "1 place to look".
- **Row 2 (append-only, cy=190):** 12px `#444` left-aligned label "append-only store" at (25, 152); blue box (140&times;40) at x=120, cy=190 labeled "read key k"; five 2px `#d95926` arrows fanning from (262, 190) to the left edge of the five boxes below at their vertical centres.
- **Five target boxes (95&times;26, r=6, 2px border, 11px `#2c3e50` centered text):** x=340, top edges y = 128, 158, 188, 218, 248 — the first fill `rgba(42,120,214,0.15)` border `#2a78d6` labeled "memtable"; the four below fill `rgba(0,131,0,0.12)` border `#008300` labeled "L0 file", "L1 file", "L2 file", "L3 file".
- **Row 2 annotation (bold 12px `#d95926`, left-aligned at (455, 172)):** "several places to look"; 12px `#444` at (455, 194): "range + bloom filter"; 12px `#444` at (455, 214): "skip most of them".
- **Amplification annotation (bold 13px magenta `#d55181`, left-aligned at (25, 288)):** computed at render from `1 + 1 + fanout * levels` with fanout 10 and levels 4 &rarr; "1 byte in, about 42 bytes of disk writes out".
- **Caption (12px `#444`, bottom right):** "Illustrative Example — fan-out 10 over 4 levels".

## What Goes Wrong: One Hot Partition Key

**Tags:** `common mistake` (red), `hotspot` (orange), `background work` (green)

- **The tempting key** — partition by timestamp: readings land in time order, so range scans look cheap
- **What happens** — a key that only increases sends every write in this second to the one partition owning "now"
- **The arithmetic** — that node takes 120,000 of 120,000 writes/s = 100%, while 11 of 12 nodes take none
- **The fix** — prefix the key with a bucket such as hash of device id, then timestamp inside the bucket
- **After the fix** — the same load spreads to 10,000 writes/s per node, 8.3% each, and a fleet scan reads 12 buckets
- **The other two** — if compaction falls behind, reads touch a growing backlog of files; and a delete is itself a write

*Example (italic):* Adding a bucket prefix changed no hardware and no client code — the same load went from 100% on one node to 8.3% on each of twelve.

**Common mistake:** Choosing a time-ordered partition key in a write-heavy store. Sequential keys serialise every write onto the partition that owns "now", so the cluster's write capacity collapses to one node's — and deletes, being appended tombstones, grow the store before they shrink it.

### Visualization (canvas `c4`, 720&times;300)

Two 12-bar panels on a shared scale: writes per node under a timestamp key (one bar at full height) and under a bucket-prefixed key (twelve short equal bars).

- **Title (bold 15px, `#1a5276`, top center):** "Same 120,000 Writes/s, Two Partition Keys".
- **Divider:** vertical dashed `#6b7280` (dash 4/3) at x=360, y=46 to y=282.
- **Shared scale:** baseline y=252, plot height 165, max 120,000; bars 18px wide, pitch 24.
- **Left panel:** bars start x=50 (12 bars, ending x=338); values `[120000,0,0,0,0,0,0,0,0,0,0,0]`; the non-zero bar fill `rgba(231,76,60,0.30)`, 2px `#e74c3c` border; zero nodes drawn as an 18&times;3 `#e5e9ef` stub on the baseline.
- **Left labels:** panel title bold 12px `#e74c3c` centered (200, 66) "key = timestamp"; bold 12px `#e74c3c` above the tall bar, computed at render as its share of the total &rarr; "100%"; 11px `#444` node ticks "n1", "n4", "n8", "n12" under bars 1, 4, 8, 12.
- **Right panel:** bars start x=402 (12 bars, ending x=690); values all 10,000 (computed at render as total &divide; 12); fill `rgba(0,131,0,0.25)`, 2px `#008300` border.
- **Right labels:** panel title bold 12px `#008300` centered (546, 66) "key = bucket, then timestamp"; bold 12px `#008300` centered (546, 150), computed at render &rarr; "8.3% each"; 11px `#444` node ticks "n1", "n4", "n8", "n12".
- **Annotation (bold 13px violet `#4a3aa7`, centered at (360, 282)):** "one node saturated vs twelve nodes at one twelfth".
- **Caption (12px `#444`, bottom right):** "Illustrative Example — 12 nodes, even hash assumed".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Density target:** four sections, five to six bullets each, roughly twenty bullets on the page; at most one arithmetic illustration per section. The page is concept-driven — charts are flows, before/afters, and shapes rather than numeric ledgers.
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720&times;300; shared `setup(id)` helper sizes the backing store to the rendered width &times; `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms). Helpers `roundRect()` and `arrow()` as in the sibling pages.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data and arithmetic (no randomness anywhere; each figure below is derived in JS from the base constants and printed by the chart, so labels and prose cannot drift):**
  - Base constants: 1,200,000 devices, 10 s reporting interval, 2,400 reads/s, 8 ms random seek, 12 nodes, compaction fan-out 10 over 4 levels.
  - Derived: 1,200,000 &divide; 10 = 120,000 writes/s; 120,000 &divide; 2,400 = 50 : 1; 1 &divide; 0.008 = 125 writes/s per seeking disk.
  - Amplification: 1 (commit log) + 1 (flush) + 10 &times; 4 (levels) = 42&times;, printed as one figure — no stacked ledger.
  - Partitioning: timestamp key `[120000,0,…]` &rarr; 120,000 &divide; 120,000 = 100% on one node; bucketed key 120,000 &divide; 12 = 10,000 per node = 8.3% each.
  - All device counts, disk rates, seek time, node count, and fan-out are invented and labeled "Illustrative Example" in the chart captions; the arithmetic between them is exact.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
