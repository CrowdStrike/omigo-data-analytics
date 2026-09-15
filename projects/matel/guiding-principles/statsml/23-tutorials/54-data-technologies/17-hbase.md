# HBase

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** HBase

**Subtitle:** The open-source Bigtable — one giant table kept sorted by row key and sliced into regions across servers; the design that stored Facebook Messenger's messages for years

## One Giant Table, Sorted by Row Key

**Tags:** `core idea` (blue), `wide column` (green), `Bigtable` (orange)

- **The app** — a chat service keeps every message in one HBase table, one row per message
- **The row key** — each row is named `userId#timestamp`, like `00042#20260826140301`
- **Always sorted** — HBase keeps rows in lexicographic row-key order, like a phone book
- **Column families** — an `m:` family holds `m:body`, `m:sender`; rows are sparse, blanks cost zero bytes
- **Regions** — the sorted key range is cut into slices called regions, each served by one server

*Example (italic):* User 00042's 2:03pm message gets key `00042#20260826140301` and lands shoulder to shoulder with that user's other messages.

**Key point:** HBase is one huge sorted map from row key to sparse columns — the open-source build of Google's Bigtable paper, on HDFS; sorting keeps related keys together, and cutting the sort order into regions is what makes it scale.

### Visualization (canvas `c1`, 720×300)

Diagram of the sorted key space as a horizontal bar split into four regions, with arrows down to the region servers that host them.

- **Title (bold 15px, `#1a5276`, top center):** "The Sorted Key Space, Cut Into 4 Regions on 3 Servers".
- **Key-space bar:** rectangle at x=60, y=80, width 600, height 40; split into 4 equal segments (150px each) filled `rgba(42,120,214,0.25)`, `rgba(0,131,0,0.20)`, `rgba(217,89,38,0.20)`, `rgba(74,58,167,0.18)` with 2px `#1a5276` segment borders; bold 12px `#2c3e50` labels centered in each: "region 1", "region 2", "region 3", "region 4".
- **Boundary keys (12px `#444`, below the bar at y=138):** "00000", "02500", "05000", "07500", "end" at x = 60, 210, 360, 510, 660.
- **Sort arrow (12px `#6b7280`, above bar at y=70):** "row keys sorted lexicographically →" left-aligned at x=60.
- **Server boxes:** three rounded boxes 150×40 at y=210, x = 90, 300, 510, fill `rgba(26,82,118,0.10)`, 12px `#2c3e50` labels "region server A", "region server B", "region server C".
- **Arrows:** 2px `#6b7280` lines: region 1 → server A, region 2 → server B, regions 3 and 4 → server C.
- **Annotation (bold 12px violet `#4a3aa7`, right side near y=185):** "regions split and move between servers to balance load".
- **Caption (12px `#444`, bottom right):** "boundaries illustrative".

## One User's Day Is One Contiguous Scan

**Tags:** `worked example` (blue), `range scan` (green)

- **The ask** — load user 00042's messages for Aug 26: every key starting `00042#20260826`
- **The scan** — start at row `00042#20260826`, read forward, stop before `00042#20260827` — one range
- **The count** — 38 messages that day (illustrative), all adjacent in sort order, all in one region
- **Sparse cells** — every row has `m:body`; only 5 of the 38 carry an `m:attach` column
- **Per-row consistency** — a read of any single row always sees its latest write, guaranteed
- **Padding matters** — unpadded `42#...` would sort after `100#...`; zero-padding keeps a user's rows together

*Example (italic):* The day's conversation view is one sequential scan of 38 adjacent rows — not 38 scattered lookups across the cluster.

**Key point:** Because rows are sorted, one user's messages sit contiguously — a whole day is one cheap sequential read, which is exactly why the key leads with the user, not the time.

### Visualization (canvas `c2`, 720×300)

Rendered slice of the sorted table: 8 rows with their row keys and sparse columns, the scan window for user 00042 highlighted.

- **Title (bold 15px, `#1a5276`, top center):** "8 Sorted Rows: the Scan Grabs a Contiguous Band".
- **Column headers (bold 12px `#1a5276`, y=68):** "row key" at x=70, "m:sender" at x=340, "m:body" at x=450, "m:attach" at x=590; 1px `#e5e9ef` rule under headers at y=76.
- **Rows (12px `#2c3e50`, 26px apart, first baseline y=100), keys at x=70 in monospace:** `00041#20260826235804` / bram / "see you then" / (blank), `00041#20260826235957` / kate / "night!" / (blank), `00042#20260826080312` / lena / "coffee at 9?" / (blank), `00042#20260826080455` / self / "yes, same place" / (blank), `00042#20260826081210` / lena / "here's the spot" / pin.png, `00042#20260826234501` / self / "done for today" / (blank), `00043#20260826000012` / arun / "invoice sent" / (blank), `00043#20260826000230` / arun / "check your mail" / (blank).
- **Ellipsis row:** 12px `#6b7280` "... 33 more 00042 rows ..." centered between the 5th and 6th data rows (shift rows 6–8 down 26px to make space).
- **Scan band:** rows 3 through 6 plus the ellipsis (all `00042#20260826...` keys) sit on a `rgba(0,131,0,0.10)` background rectangle spanning x=60 to x=660; 2px `#008300` bracket on its left edge.
- **Band label (bold 12px green `#008300`, inside the band at x=540, at the y level of the 4th data row):** "scan: 38 rows".
- **Sparse note (bold 12px orange `#d95926`, below table near y=286):** "blank cells are truly absent — sparse rows store nothing for them".
- **Caption (12px `#444`, bottom right):** "keys and messages illustrative".

## Fast Writes: the LSM Tree Under the Hood

**Tags:** `where it's used` (blue), `LSM tree` (green), `Messenger` (orange)

- **The write path** — a new message is appended to a log on disk, then added to a sorted in-RAM buffer (the memstore)
- **The flush** — when the memstore fills (128 MB by default), it's written out as an immutable sorted file (an HFile)
- **Compaction** — background merges fold many small HFiles into fewer big ones so reads touch fewer files
- **Sequential only** — HBase never updates a disk block in place; every write is an append or a merge
- **Messenger** — Facebook Messenger stored its message history in HBase for years (publicly documented)

*Example (italic):* At 50,000 messages/sec (illustrative), every write is an append; a read of one row merges the memstore with a handful of HFiles.

**Key point:** LSM-tree storage — memstore, HFiles, compactions — turns random small writes into sequential appends, which is why write-heavy stores like a messaging backlog fit HBase so well.

### Visualization (canvas `c3`, 720×300)

Left-to-right flow diagram of the LSM write path: incoming write → WAL + memstore → flush to HFiles → compaction into one big HFile.

- **Title (bold 15px, `#1a5276`, top center):** "The Write Path: Memstore → HFiles → Compaction".
- **Write box:** rounded box 120×40 at x=30, y=130, fill `rgba(26,82,118,0.10)`, 12px `#2c3e50` label "new message".
- **WAL box:** rounded box 140×40 at x=200, y=65, fill `rgba(217,89,38,0.15)`, 12px `#2c3e50` label "WAL (disk log)"; 2px `#6b7280` arrow from write box, 11px `#6b7280` "append" on the arrow.
- **Memstore box:** rounded box 140×40 at x=200, y=185, fill `rgba(42,120,214,0.20)`, 12px `#2c3e50` label "memstore (RAM)"; 2px `#6b7280` arrow from write box, 11px `#6b7280` "sorted insert" on the arrow.
- **HFile boxes:** three rounded boxes 110×32 stacked at x=420, y = 70, 130, 190, fill `rgba(0,131,0,0.12)`, 2px `#008300` borders, 12px `#2c3e50` labels "HFile 1", "HFile 2", "HFile 3"; 2px `#008300` arrow from memstore to HFile 3 with bold 11px green "flush at 128 MB".
- **Compacted box:** rounded box 110×54 at x=580, y=118, fill `rgba(0,131,0,0.20)`, 2px `#008300` border, bold 12px `#2c3e50` label "big HFile"; three 2px `#6b7280` arrows converging from the small HFiles, 11px `#6b7280` "compaction" beneath.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=272):** "no in-place disk updates — writes are appends, reads merge memstore + HFiles".
- **Caption (12px `#444`, bottom right):** "128 MB flush threshold is the HBase default (exact)".

## The Hotspot: Monotonic Keys Pile Onto One Region

**Tags:** `common mistake` (red), `row-key design` (orange)

- **The trap** — key the table `timestamp#userId` and every new message has the largest key so far
- **One region** — sorted order sends all 60,000 writes/min to the tail region on one server
- **Idle fleet** — the other servers hold cold history and do nothing; adding servers doesn't help
- **The fix** — lead with the user: `userId#timestamp` spreads live writes across all regions
- **Other fixes** — a salt prefix or a reversed timestamp does the same for purely time-keyed data
- **The cost** — user-first keys give up one global "latest messages" scan; queries go per user

*Example (italic):* Timestamp-first, region server 1 absorbs all 60,000 writes/min; user-first, the four servers take 15,200 / 14,900 / 15,100 / 14,800.

**Common mistake:** Choosing a row key that grows monotonically. Sorted storage turns "newest keys" into "one server" — the row key, not the cluster size, decides whether writes spread.

### Visualization (canvas `c4`, 720×300)

Grouped bar chart: writes per minute landing on each of 4 region servers, timestamp-first key (red) vs user-first key (green).

- **Title (bold 15px, `#1a5276`, top center):** "60,000 Writes/min: One Hot Server vs Four Warm Ones".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = writes/min 0 to 60,000, gridlines `#e5e9ef` at 15,000/30,000/45,000 with 12px `#444` labels "15k"/"30k"/"45k"; x = four groups centered at x = 150, 290, 430, 570 labeled "RS 1"–"RS 4" (12px `#444`).
- **Timestamp-first bars (left of each center, 40px wide, red `#e74c3c` fill `rgba(231,76,60,0.35)` with 2px red border):** values `[60000, 0, 0, 0]` at 3 px per 1,000 writes (180, 0, 0, 0 px tall); zero bars drawn as a 2px red tick on the baseline.
- **User-first bars (right of each center, 40px wide, green `#008300` fill `rgba(0,131,0,0.30)` with 2px green border):** values `[15200, 14900, 15100, 14800]` (46, 45, 45, 44 px tall).
- **Value labels:** 11px `#444` above each nonzero bar ("60,000", "15,200", "14,900", "15,100", "14,800").
- **Legend (12px, top right at x≈500, y=55):** red swatch "timestamp#user", green swatch "user#timestamp".
- **Annotation (bold 13px red `#e74c3c`, near x=200, y=80):** "sequential keys: RS 1 takes 100% of the load".
- **Caption (12px `#444`, bottom right):** "write rates illustrative; both key schemes total 60,000/min".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); message counts, senders, write rates, and per-server splits are invented and labeled illustrative; the 128 MB memstore flush default and the sorted-order / zero-padding facts are exact; HBase-as-open-source-Bigtable-on-HDFS, sorted/sparse/column-family semantics, per-row strong consistency, region splitting, LSM writes (memstore + HFiles + compactions), sequential-key hotspotting, and Facebook Messenger's use of HBase are publicly documented.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
