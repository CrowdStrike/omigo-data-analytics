# What a Filesystem Does

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** What a Filesystem Does

**Subtitle:** A disk is just millions of numbered storage blocks — the filesystem is the bookkeeping layer that turns them into named files inside folders, and keeps its notes safe through a power cut

## Saving the Coffee Shop's Sales File

**Tags:** `core idea` (blue), `files & folders` (green), `inodes` (orange)

- **The save** — a coffee shop's laptop saves sales-aug.csv, a 10 KB spreadsheet of the month's sales
- **The disk** — the drive is only millions of numbered 4,096-byte blocks; it has no idea what a file is
- **The blocks** — the filesystem picks three free blocks (#7, #12, #31) and copies the bytes into them
- **The inode** — an index card (inode 52) records the size, owner, dates, and the list of those blocks
- **The directory** — the /reports folder is itself a small file: a name list, "sales-aug.csv → inode 52"

*Example (italic):* Opening the file runs the chain backwards — /reports gives inode 52, inode 52 gives blocks 7, 12, 31, and the blocks give back the spreadsheet.

**Key point:** A filesystem is the bookkeeping layer between names and blocks: directories map names to inodes, and inodes map to the blocks that hold the actual bytes.

### Visualization (canvas `c1`, 720×300)

Three-hop flow diagram: directory entry box, then inode card box, then a numbered disk-block grid with the file's three blocks highlighted.

- **Title (bold 15px, `#1a5276`, top center):** "One Name, Three Hops: from sales-aug.csv to Bytes on Disk".
- **Directory box:** rounded box at x=25, y=110, 175×64, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border; 12px `#2c3e50` lines "/reports directory" and "sales-aug.csv → inode 52".
- **Inode box:** rounded box at x=250, y=95, 175×95, fill `rgba(74,58,167,0.12)`, 2px `#4a3aa7` border; 12px lines "inode 52", "size 10,240 B", "blocks: 7, 12, 31".
- **Arrows:** 3px `#1a5276` arrows directory→inode and inode→grid.
- **Block grid:** 8 cols × 4 rows of 26px cells (2px gap) starting at x=475, y=85, numbered 0–31 in 11px; empty cells stroked `#e5e9ef`, blocks 7, 12, 31 filled green `#008300` with white 11px numbers; 12px `#6b7280` label "disk: numbered 4,096-byte blocks" beneath the grid.
- **Annotation (bold 13px blue `#2a78d6`, near x=30, y=260):** "the disk stores bytes; the filesystem stores the map".
- **Caption (12px `#6b7280`, bottom right):** "block and inode numbers illustrative".

## Two Full Blocks and a Half-Empty One

**Tags:** `worked example` (blue), `block math` (green)

- **The math** — 10,240 bytes ÷ 4,096 bytes per block = 2.5, and blocks are all-or-nothing, so 3 blocks
- **The fill** — blocks 7 and 12 carry 4,096 bytes each; block 31 carries only the last 2,048
- **The waste** — block 31's unused 2,048 bytes stay reserved; no other file may borrow them
- **The lookup** — reading the file costs 1 directory read + 1 inode read + 3 block reads = 5 disk reads
- **On disk** — the real footprint is 3 × 4,096 = 12,288 bytes, not the 10,240 the shop wrote

*Example (italic):* Redo it by hand: 10,240 = 4,096 + 4,096 + 2,048 — two full blocks, one half-full, 2,048 bytes of paid-for emptiness.

**Key point:** File sizes round up to whole blocks — a file's disk footprint is its byte count rounded up to the next multiple of 4,096.

### Visualization (canvas `c2`, 720×300)

Vertical bar chart of the three blocks' fill levels, with block 31's empty half drawn as a dashed reserved slab.

- **Title (bold 15px, `#1a5276`, top center):** "10,240 Bytes Into 4,096-Byte Blocks: the Last Block Is Half Empty".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = bytes 0 to 4,096, gridlines `#e5e9ef` at 1,024 / 2,048 / 3,072 with 12px `#6b7280` labels; x = bar labels "block 7", "block 12", "block 31" in 12px `#444`.
- **Used bars:** width 110, centers x = 190, 360, 530; used bytes `[4096, 4096, 2048]`, fill `rgba(42,120,214,0.35)`, 2px `#2a78d6` border; bold 12px `#2c3e50` value labels "4,096", "4,096", "2,048" atop each used segment.
- **Reserved slab:** on block 31 only, the remaining `2048` bytes drawn from y of 2,048 up to 4,096 as fill `rgba(107,114,128,0.15)` with 1px dashed `#6b7280` border and 11px `#6b7280` label "reserved, empty".
- **Annotation (bold 13px orange `#d95926`, near block 31, y=70):** "footprint 12,288 B for a 10,240 B file".
- **Caption (12px `#444`, bottom right):** "byte counts exact for a 10,240-byte file".

## A Hundred Thousand Receipts, One File Each

**Tags:** `where it's used` (blue), `tiny files` (orange)

- **The export** — the shop writes every receipt as its own file: 100,000 files of 120 bytes each
- **The data** — 100,000 × 120 bytes = 12 MB of actual sales numbers
- **The footprint** — each file claims a whole 4,096-byte block: 100,000 × 4,096 B ≈ 410 MB of disk
- **The inodes** — each file burns an inode; ext4-style filesystems fix the supply at format time
- **The fix** — one combined CSV holds the same rows in ~2,930 blocks: back to 12 MB on disk

*Example (italic):* A pipeline that writes one tiny file per record can exhaust the inode table while the drive still reports hundreds of free gigabytes.

**Key point:** Data scientists meet the filesystem the day they write millions of tiny files — block rounding and inode limits, not raw bytes, decide when the disk is full.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart comparing the data's true size against its on-disk cost as 100,000 separate files versus one CSV.

- **Title (bold 15px, `#1a5276`, top center):** "100,000 Receipt Files: 12 MB of Data Costs 410 MB of Disk".
- **Layout:** left-aligned 12px `#444` row labels at x=20; bars start at x=230, max width 440, 18px tall; scale 440px = 410 MB.
- **Rows (top to bottom at y = 90, 150, 210):**
  - "data inside the files — 12 MB": green `#008300` bar width 13
  - "disk used, one file each — 410 MB": orange `#d95926` bar width 440
  - "same rows as one CSV — 12 MB": blue `#2a78d6` bar width 13
- **Value labels:** 11px `#2c3e50` at each bar's right end ("12 MB", "410 MB", "12 MB").
- **Annotation (bold 13px magenta `#d55181`, right side near y=255):** "34× overhead — a whole 4,096-byte block per tiny file".
- **Caption (12px `#444`, bottom right):** "receipt counts illustrative; block math exact (100,000 × 4,096 B ≈ 410 MB)".

## A Save Is Four Writes, Not One

**Tags:** `common mistake` (red), `journaling` (orange), `crash safety` (green)

- **The mistake** — treating "save" as one atomic step; it is really 4 separate writes to the disk
- **The four** — the data blocks, inode 52, the /reports directory entry, and the free-block map
- **The crash** — power dies after write 2 of 4: the bytes exist but no name points at them
- **The journal** — a journaling filesystem first writes one note covering the 3 bookkeeping changes
- **The replay** — on reboot it reads the journal: finished notes replay, unfinished notes are discarded
- **The guarantee** — the bookkeeping lands all-or-nothing; the data blocks are not replayed

*Example (italic):* The laptop loses power mid-save at 9:41pm; on reboot the replay leaves the bookkeeping consistent — but sales-aug.csv's contents can still be half-written unless the app used a temp-file rename or fsync.

**Common mistake:** Assuming the journal protects file contents too. By default it covers only the bookkeeping — journaling data as well is a slow optional mode, so apps rely on fsync and temp-file renames.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: a power cut mid-save without a journal (orphaned file) versus with a journal (replayed, consistent).

- **Title (bold 15px, `#1a5276`, top center):** "Power Cut Mid-Save: the Journal Keeps the Bookkeeping Consistent".
- **Row 1 (y=95), label 12px `#444` at x=20:** "no journal"; blue `#2a78d6` rounded box at x=125 labeled "writes 1–2: blocks + inode" (12px); vertical dashed `#e74c3c` line (dash 4/3) at x=350 with 11px red label "power cut"; red `#e74c3c` box at x=395, 300px wide, labeled "writes 3–4 never happen — bytes with no name" with bold 12px red "✗ orphaned file".
- **Row 2 (y=205), label:** "with journal"; green `#008300` box at x=125 labeled "step 0: journal the 3 bookkeeping changes"; 3px arrow to blue box at x=330 labeled "apply writes 1–4"; same dashed red "power cut" line at x=510; green box at x=545 labeled "reboot: replay note" with bold 12px green "✓ consistent".
- **Box style:** 150–300px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text, 3px `#1a5276` arrows between boxes.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=270):** "the journal makes the bookkeeping all-or-nothing".
- **Caption (12px `#444`, bottom right):** "write ordering schematic; step count exact for this save".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); block numbers (7, 12, 31), inode 52, and the receipt scenario are invented and labeled illustrative; the block math is exact: 10,240 B = 4,096 + 4,096 + 2,048 (3 blocks, footprint 12,288 B, block fills `[4096, 4096, 2048]`), and 100,000 × 4,096 B ≈ 410 MB vs 100,000 × 120 B = 12 MB (~34× overhead, one CSV ≈ 2,930 blocks).
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
