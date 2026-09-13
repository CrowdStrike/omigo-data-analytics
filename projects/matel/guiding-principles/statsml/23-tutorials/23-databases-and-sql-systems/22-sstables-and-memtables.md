# SSTables & Memtables

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** SSTables & Memtables

**Subtitle:** Every LSM storage engine is two structures: a sorted in-memory notebook for fresh writes (the memtable) and immutable sorted files it flushes to disk (SSTables)

## The Notebook at the Counter and the Filed Pages

**Tags:** `core idea` (blue), `sorted writes` (green), `LSM engine` (orange)

- **The ledger** — a coffee shop tracks loyalty points per customer; every update is one small write
- **The memtable** — fresh writes land in one sorted in-memory page: an update is just an insert
- **Sorted on arrival** — leo: 31 arrives before ava: 12, yet the page keeps ava above leo
- **The flush** — when the page hits its limit (4 entries here), it is written to disk in one pass
- **The SSTable** — the flushed file is a Sorted String Table: sorted by key, never edited again
- **A fresh page** — the instant one page is filed, an empty memtable starts taking new writes

*Example (italic):* At 9am the memtable holds ava: 12 and leo: 31; two more writes and the whole page would flush to disk as SSTable-3.

**Key point:** A memtable is the sorted in-memory buffer where every write lands; an SSTable is that buffer frozen into an immutable sorted file on disk.

### Visualization (canvas `c1`, 720×300)

Flow diagram: incoming writes entering a sorted memtable in a MEMORY region, with a flush arrow crossing a dashed divider into a DISK region holding two existing SSTable boxes.

- **Title (bold 15px, `#1a5276`, top center):** "Fresh Writes Fill a Sorted Memtable; Full Memtables Flush as SSTables".
- **Regions:** horizontal dashed `#6b7280` divider (dash 4/3) at y=170 from x=30 to x=690; 11px `#6b7280` labels "MEMORY" at (35, 52) and "DISK" at (35, 188).
- **Memtable box:** rounded rect x=60 y=62 w=240 h=92, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border; header bold 12px `#2a78d6` "memtable (sorted, 2/4)"; rows 12px `#2c3e50` "ava: 12" and "leo: 31".
- **Incoming writes:** 12px `#2c3e50` labels "write leo: 31 (9:01)" at (400, 82) and "write ava: 12 (9:04)" at (400, 112), each with a 2px `#199e70` arrow pointing left into the memtable box; note bold 12px `#199e70` at (400, 142): "sorted on insert — ava files above leo".
- **Flush arrow:** 3px `#d95926` arrow from the memtable's bottom edge (x=180, y=154) down to (x=180, y=192), bold 12px `#d95926` label beside it: "flush when full — one sequential write".
- **Disk boxes:** SSTable-2 rounded rect x=60 y=198 w=200 h=84, fill `rgba(0,131,0,0.12)`, 2px `#008300` border, header bold 12px `#008300` "SSTable-2 (newer)", rows 12px "ben: ⊗ deleted", "kai: 18", "maya: 57"; SSTable-1 rounded rect x=300 y=198 w=200 h=84, fill `rgba(74,58,167,0.12)`, 2px `#4a3aa7` border, header bold 12px `#4a3aa7` "SSTable-1 (older)", rows "ben: 22", "maya: 40", "noah: 15".
- **Annotation (bold 13px violet `#4a3aa7`, at x=520, y=240):** "files are never edited after the flush".
- **Caption (12px `#444`, bottom right):** "entries and flush size illustrative".

## Looking Up Maya, Then Zipping Two Files Together

**Tags:** `worked example` (blue), `read path` (green), `merge` (orange)

- **The lookup** — a read for maya checks the memtable first: only ava and leo live there, so it misses
- **Newest file next** — SSTable-2 holds maya: 57; the read returns 57 and stops searching
- **Never reached** — SSTable-1 also stores maya: 40, but the older copy is never consulted
- **A deeper read** — noah misses the memtable and SSTable-2, then hits in SSTable-1 at 15
- **The merge** — compaction walks SSTable-1 and SSTable-2 key by key: ben, kai, maya, noah
- **Who survives** — maya keeps 57 over 40; ben's tombstone erases both ben rows: 6 rows in, 3 out

*Example (italic):* Read maya → memtable miss → SSTable-2 hit (57) → stop; the later merge writes exactly kai: 18, maya: 57, noah: 15.

**Key point:** Reads check the memtable, then SSTables newest-to-oldest, stopping at the first hit; compaction is a sorted-list merge where the newest version of each key wins.

### Visualization (canvas `c2`, 720×300)

Read-path diagram: the three stores as boxes across the top, with two lookup rows beneath showing per-store miss/hit results for maya and noah.

- **Title (bold 15px, `#1a5276`, top center):** "Read Path: Memtable First, Then Newest File, Stop at First Hit".
- **Store boxes (y=55, h=84, w=190, rounded):** memtable at x=40, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border, header bold 12px `#2a78d6` "memtable", rows 12px `#2c3e50` "ava: 12", "leo: 31"; SSTable-2 at x=265, fill `rgba(0,131,0,0.12)`, 2px `#008300` border, header bold 12px `#008300` "SSTable-2 (newest)", rows "ben: ⊗", "kai: 18", "maya: 57"; SSTable-1 at x=490, fill `rgba(74,58,167,0.12)`, 2px `#4a3aa7` border, header bold 12px `#4a3aa7` "SSTable-1 (oldest)", rows "ben: 22", "maya: 40", "noah: 15".
- **Row 1 (y=185), label bold 12px `#2c3e50` at x=40:** "read maya →"; under each box center (x=135, 360, 585): 12px `#6b7280` "✗ miss" at x=135; bold 12px `#008300` "✓ hit: 57 — stop" at x=360; 12px `#6b7280` "not checked (40 ignored)" at x=585.
- **Row 2 (y=220), label bold 12px `#2c3e50` at x=40:** "read noah →"; 12px `#6b7280` "✗ miss" at x=135; 12px `#6b7280` "✗ miss" at x=360; bold 12px `#008300` "✓ hit: 15" at x=585.
- **Arrows:** thin 2px `#6b7280` arrows left-to-right between the columns along each lookup row, stopping at the hit column.
- **Annotation (bold 13px green `#008300`, centered near y=262):** "the newest copy shadows maya: 40 — the old file is never read".
- **Caption (12px `#444`, bottom right):** "points illustrative".

## Why Write-Heavy Stores Are Built This Way

**Tags:** `where it's used` (blue), `fast writes` (green), `tombstones` (orange)

- **Built for writes** — every write is an append: a memtable insert plus a sequential log write, no seek
- **The engines** — Cassandra, RocksDB, and LevelDB all store data this way; the design is an LSM tree
- **Deletes are writes** — you cannot erase a row from an immutable file, so a delete writes a tombstone
- **The tombstone** — ben's delete is a tiny "ben: deleted" record that shadows every older ben on read
- **The trade** — writes get cheap, reads may touch several files; bloom filters skip files lacking the key

*Example (italic):* An illustrative points ledger sustains about 100k appended writes/s where an update-in-place design manages about 12k.

**Key point:** LSM engines turn every insert, update, and delete into a cheap sequential append — which is why write-heavy stores stand on memtables and SSTables.

### Visualization (canvas `c3`, 720×300)

Vertical bar chart comparing illustrative write throughput: update-in-place storage vs LSM-style append-only storage.

- **Title (bold 15px, `#1a5276`, top center):** "Why LSM: Every Write Is a Sequential Append".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = writes per second 0 to 120k, gridlines `#e5e9ef` at 30k/60k/90k with 12px `#444` labels "30k"/"60k"/"90k"; 2px `#999` baseline.
- **Bars (120px wide):** "update-in-place (seek + rewrite page)" centered at x=210, value 12k, fill `rgba(42,120,214,0.30)`, 2px `#2a78d6` border, bold 12px `#2a78d6` value label "12k" above the bar; "LSM (memtable + append)" centered at x=510, value 100k, fill `rgba(0,131,0,0.25)`, 2px `#008300` border, bold 12px `#008300` value label "100k" above the bar; 12px `#444` category labels under the baseline.
- **Annotation (bold 13px green `#008300`, near x=430, y=70):** "appends never seek — ~8× the write throughput".
- **Caption (12px `#444`, bottom right):** "throughput numbers illustrative".

## Updates Don't Edit Files — They Shadow Them

**Tags:** `common mistake` (red), `immutable files` (orange), `space` (blue)

- **No edits** — updating maya from 40 to 57 never touches the old file; a new row shadows the old one
- **Shadow, not overwrite** — the disk holds both maya: 40 and maya: 57; reads only ever see 57
- **Space until compaction** — dead versions and tombstones sit on disk until a merge drops them
- **The surprise** — deleting rows can make the store temporarily bigger: each delete adds a tombstone
- **The cleanup** — the merge from the worked example shrinks 6 stored rows to the 3 live ones

*Example (italic):* Before compaction the shop's ledger stores 6 rows for 3 live customers; after compaction, exactly 3 rows remain.

**Common mistake:** Expecting an update or delete to change the bytes on disk. SSTables are immutable — new versions shadow old ones, and the dead rows occupy space until compaction reclaims them.

### Visualization (canvas `c4`, 720×300)

Before/after compaction diagram: two source SSTables on the left with shadowed rows struck through, merging via arrows into one compacted SSTable on the right.

- **Title (bold 15px, `#1a5276`, top center):** "Compaction: Two Files Merge, Shadowed Rows Drop Out".
- **Left boxes (w=210, h=92, rounded):** SSTable-1 at x=50 y=58, fill `rgba(74,58,167,0.12)`, 2px `#4a3aa7` border, header bold 12px `#4a3aa7` "SSTable-1 (older)", rows 12px: "ben: 22" and "maya: 40" in strikethrough `#6b7280` with 11px `#6b7280` "(shadowed)" suffix, "noah: 15" in `#2c3e50`; SSTable-2 at x=50 y=172, fill `rgba(0,131,0,0.12)`, 2px `#008300` border, header bold 12px `#008300` "SSTable-2 (newer)", rows: "ben: ⊗ tombstone" in strikethrough `#6b7280`, "kai: 18" and "maya: 57" in `#2c3e50`.
- **Merge arrows:** 3px `#d95926` arrows from the right edges of both left boxes (y=104 and y=218) converging to the merged box's left edge at (430, 160); bold 12px `#d95926` label "key-by-key merge" at (300, 150).
- **Merged box:** rounded rect x=430 y=112 w=230 h=96, fill `rgba(25,158,112,0.12)`, 2px `#199e70` border, header bold 12px `#199e70` "merged SSTable", rows 12px `#2c3e50` "kai: 18", "maya: 57", "noah: 15".
- **Annotation (bold 13px magenta `#d55181`, near x=430, y=252):** "6 rows stored, 3 live — the rest was shadow space".
- **Caption (12px `#444`, bottom right):** "row values illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all values are the hardcoded literals above (no randomness); the ledger is memtable `{ava: 12, leo: 31}` (flush limit 4), SSTable-2 `{ben: tombstone, kai: 18, maya: 57}`, SSTable-1 `{ben: 22, maya: 40, noah: 15}`; the compaction output is `{kai: 18, maya: 57, noah: 15}` (6 rows in, 3 out); loyalty points, flush size, and the 12k vs 100k writes/s bars are invented and labeled illustrative.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
