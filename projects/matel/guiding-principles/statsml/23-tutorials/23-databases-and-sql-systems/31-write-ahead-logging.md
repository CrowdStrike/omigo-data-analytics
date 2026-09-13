# Write-Ahead Logging

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Write-Ahead Logging

**Subtitle:** Before touching the data, the database writes down what it is about to do — so a power cut mid-write loses nothing that was promised

## The $200 Transfer and the Pulled Plug

**Tags:** `core idea` (blue), `durability` (green), `crash safety` (orange)

- **The transfer** — Priya moves $200 from checking ($500) to savings ($100) at 10:00:00
- **Log first** — the database appends the changes to an append-only log file before touching any data
- **Ack after fsync** — only once the log record is forced to disk does the app hear "committed"
- **Pages later** — the actual account pages are rewritten lazily, whenever the database gets to them
- **The plug** — power dies after the checking page is written but before the savings page is
- **Safe anyway** — on restart the log still holds the full story, so the committed transfer survives

*Example (italic):* The plug is pulled 50 ms in: the log has all three records, the checking page shows $300, the savings page still shows $100 — yet no money is lost.

**Key point:** Write-ahead logging means every change is described in a sequential log that is forced to disk BEFORE the data pages — commit is safe the moment the log is, not when the data is.

### Visualization (canvas `c1`, 720×300)

Two-lane timeline of the transfer's first 80 ms: the log lane fills up fast and durably; the data-page lane lags behind and is cut off by the power failure.

- **Title (bold 15px, `#1a5276`, top center):** "The Log Is Done at 22 ms; the Data Pages Never Finish".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = milliseconds 0 to 80 with 12px `#444` tick labels every 20 ms; two lane guide lines `#e5e9ef` at y=110 ("log file — on disk", 12px `#6b7280` label at x=62) and y=190 ("data pages — on disk", same style).
- **Log lane boxes (28px tall, 8px radius, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border, 11px `#2c3e50` text), centered on y=110:** "LSN 101 checking 500→300" at x for 8 ms; "LSN 102 savings 100→300" at 15 ms; "LSN 103 COMMIT ✓ fsync" at 22 ms with 2px `#008300` border instead.
- **Data lane boxes, centered on y=190:** green-bordered box (fill `rgba(0,131,0,0.12)`, 2px `#008300`) "checking page = $300" at 40 ms; dashed 2px `#6b7280` outline box (no fill) "savings page write" at 66 ms with 11px `#6b7280` label "never happens" beneath it.
- **Power-cut marker:** vertical dashed red `#e74c3c` (dash 5/4) 3px line at 50 ms from y=55 to y=245, bold 12px red label "power cut" at its top.
- **Annotation (bold 13px green `#008300`, near x=25 ms, y=70):** "commit already durable at 22 ms".
- **Caption (12px `#444`, bottom right):** "timings illustrative".

## Replaying the Log: Redo or Skip, LSN by LSN

**Tags:** `worked example` (blue), `recovery` (green)

- **Restart scan** — recovery reads the log forward and finds LSN 101, 102, 103 for the transfer
- **Page stamps** — every data page stores the LSN of the last change already applied to it
- **LSN 101** — the checking page carries page-LSN 101; 101 ≥ 101, change already there: skip
- **LSN 102** — the savings page carries page-LSN 87 (stale); 87 < 102: redo, set savings to $300
- **LSN 103** — a COMMIT record, so the transfer counts; without it, both changes would roll back
- **Idempotent** — crash during recovery? Replay again; the LSN compare makes redo safe to repeat

*Example (italic):* Recovery redoes exactly one record — LSN 102, savings 100→300 — and skips LSN 101 because the checking page already carries stamp 101.

**Key point:** The redo rule is one comparison: apply a log record only if its LSN is greater than the page's stamped LSN — that single check makes replay correct and repeatable.

### Visualization (canvas `c2`, 720×300)

Three-row decision diagram: each log record on the left, the page's stamped LSN in the middle, and the recovery verdict on the right.

- **Title (bold 15px, `#1a5276`, top center):** "Recovery After the Crash: One Redo, One Skip, One Commit".
- **Column headers (bold 12px `#6b7280`, y=58):** "log record" at x=70, "page LSN on disk" at x=310, "verdict" at x=530.
- **Rows at y = 95, 155, 215; boxes 34px tall, 8px radius, 12px `#2c3e50` text:**
  - Row 1: blue box (fill `rgba(42,120,214,0.15)`, 2px `#2a78d6`) at x=70 "LSN 101 checking 500→300"; mid box (fill `rgba(107,114,128,0.10)`, 1px `#6b7280`) at x=310 "page LSN 101"; verdict box at x=530 (fill `rgba(107,114,128,0.12)`, 2px `#6b7280`) bold "SKIP — 101 ≥ 101".
  - Row 2: blue box "LSN 102 savings 100→300"; mid box "page LSN 87"; verdict box (fill `rgba(0,131,0,0.12)`, 2px `#008300`) bold green "REDO — 87 < 102".
  - Row 3: blue box "LSN 103 COMMIT"; mid box "—"; verdict box (fill `rgba(74,58,167,0.12)`, 2px `#4a3aa7`) bold violet "KEEP — transfer counts".
- **Arrows:** 2px `#6b7280` horizontal arrows between the boxes in each row.
- **Annotation (bold 13px green `#008300`, centered near y=262):** "one redo rebuilds the savings page: $100 → $300".
- **Caption (12px `#444`, bottom right):** "LSNs illustrative".

## One Log, Many Jobs: Recovery, Replicas, Restores

**Tags:** `where it's used` (blue), `replication` (green), `CDC` (orange)

- **Every serious DB** — Postgres's WAL, MySQL's redo log, SQLite's WAL mode: same idea, different names
- **Filesystems too** — journaling filesystems log metadata changes before touching the real structures
- **Replication** — replicas stay in sync by replaying the primary's log stream, not by copying files
- **Point-in-time restore** — a backup plus the log since then rebuilds the database as of any second
- **CDC** — change-data-capture taps the same log to feed caches, search indexes, and warehouses
- **Speed bonus** — one sequential append is far cheaper than scattering random page writes at commit

*Example (italic):* A replica running 300 ms behind the primary is just another database replaying the same log 300 ms later.

**Key point:** The log outgrows crash safety — it becomes the canonical stream of every change, and recovery, replicas, restores, and pipelines are all just different subscribers to it.

### Visualization (canvas `c3`, 720×300)

Hub-and-spoke flow: the WAL as one source box on the left, fanning out to four consumer boxes on the right, each replaying the same records.

- **Title (bold 15px, `#1a5276`, top center):** "One Sequential Log, Four Subscribers".
- **Source box:** rounded box at x=50, y=115, 170px wide, 70px tall, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border; bold 13px `#1a5276` label "WAL" and 11px `#2c3e50` lines "LSN 101, 102, 103…" and "append-only" inside.
- **Consumer boxes (170px wide, 38px tall, 8px radius, 12px `#2c3e50` text), left edge x=470, centered at y = 70, 125, 180, 235:**
  - "crash recovery — redo replay" (fill `rgba(0,131,0,0.12)`, 2px `#008300`)
  - "replica — 300 ms behind" (fill `rgba(25,158,112,0.12)`, 2px `#199e70`)
  - "point-in-time restore" (fill `rgba(201,133,0,0.12)`, 2px `#c98500`)
  - "CDC → warehouse, caches" (fill `rgba(74,58,167,0.12)`, 2px `#4a3aa7`)
- **Arrows:** four 2px `#6b7280` arrows from the source box's right edge (x=220, y=150) fanning to each consumer box's left edge, small filled triangle heads.
- **Annotation (bold 13px magenta `#d55181`, centered near x=345, y=272):** "everything downstream is just replaying the log".
- **Caption (12px `#444`, bottom right):** "lag and consumers illustrative".

## write() Is Not on Disk: the fsync Trap

**Tags:** `common mistake` (red), `fsync` (orange), `log is truth` (blue)

- **The trap** — a successful `write()` only means the OS cache has the bytes, not the disk itself
- **fsync** — the database must call fsync on the log and wait before acknowledging any commit
- **Lying disks** — some drives ack from a volatile onboard cache; power loss can still eat those writes
- **Log is truth** — data files are a cache of the log; a torn or stale page is rebuilt from it
- **The mistake** — disabling fsync for a benchmark win, then losing acknowledged commits in a crash

*Example (italic):* With fsync off, a "committed" transfer can sit only in RAM for 30 seconds — a power cut in that window silently erases an acknowledged commit.

**Common mistake:** Believing `write()` returning means durable. Only an fsync'd log record survives the pulled plug — every layer above the disk is volatile, and the data files are just a cache.

### Visualization (canvas `c4`, 720×300)

Layered durability diagram: three stacked storage layers with `write()` and `fsync()` arrows, and a red volatile zone showing what a power cut erases.

- **Title (bold 15px, `#1a5276`, top center):** "Where the Bytes Actually Are When the Plug Is Pulled".
- **Layers (centered boxes 420px wide, left edge x=150, 44px tall, 8px radius, 12px `#2c3e50` text):**
  - y=75: "application buffer (RAM)" — fill `rgba(42,120,214,0.15)`, 2px `#2a78d6`
  - y=145: "OS page cache (RAM)" — fill `rgba(201,133,0,0.12)`, 2px `#c98500`
  - y=215: "disk — durable" — fill `rgba(0,131,0,0.12)`, 2px `#008300`, bold label
- **Arrows (3px, labeled 12px):** blue `#2a78d6` arrow from layer 1 to layer 2 labeled "write() returns here"; green `#008300` arrow from layer 2 to layer 3 labeled "fsync() returns here".
- **Volatile zone:** dashed red `#e74c3c` (dash 5/4) 2px rectangle enclosing layers 1 and 2 (x=140 to x=580, y=55 to y=200), bold 12px red label "vanishes on power cut" at its top-right corner.
- **Annotation (bold 13px red `#e74c3c`, right side near x=590, y=232):** "only this layer survives".
- **Caption (12px `#444`, bottom right):** "layers schematic; drive caches vary".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all values are the hardcoded literals above (no randomness); the transfer amounts ($500→$300 checking, $100→$300 savings, $200 moved), LSNs (101/102/103, stale page-LSN 87), and timeline milliseconds (log records at 8/15/22 ms, checking page at 40 ms, power cut at 50 ms, savings write at 66 ms) are invented and labeled illustrative; the redo rule (apply iff record LSN > page LSN) and the write()/fsync() durability boundary are real semantics.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
