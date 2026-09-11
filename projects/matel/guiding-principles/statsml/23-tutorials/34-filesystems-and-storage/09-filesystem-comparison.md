# Filesystem Comparison

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Filesystem Comparison

**Subtitle:** Six filesystems on one scorecard — journaling, checksums, snapshots, file-size limits, and which computers can even read the drive

## One Drive, Three Computers

**Tags:** `core idea` (blue), `shared drive` (green), `compatibility` (orange)

- **The drive** — a family buys one 2 TB external drive to share between a Windows PC, a Mac, and a Linux laptop
- **One format** — a drive is formatted with ONE filesystem, and each operating system speaks its own favorites
- **The first file** — the file to store is a 6 GB vacation video, already over FAT32's 4 GB per-file limit
- **The candidates** — FAT32, exFAT, NTFS, ext4, APFS, and ZFS could each format the drive
- **The filesystem** — the on-disk bookkeeping that turns raw sectors into named files and folders

*Example (italic):* Formatted as APFS, the drive works only on the Mac; formatted as exFAT, all three machines read and write it — so exFAT wins this particular job.

**Key point:** Choosing a filesystem is a compatibility-and-features trade-off — the "best" one is whichever row of the scorecard matches the job, not the newest name.

### Visualization (canvas `c1`, 720×300)

OS-compatibility grid: 6 filesystem rows × 3 OS columns (Windows, macOS, Linux), each cell colored by support level.

- **Title (bold 15px, `#1a5276`, top center):** "Who Can Read the Drive? Six Filesystems × Three Operating Systems".
- **Geometry:** row labels 12px `#2c3e50` left-aligned at x=20, rows top-to-bottom `["FAT32","exFAT","NTFS","ext4","APFS","ZFS"]` at y = `[78, 112, 146, 180, 214, 248]`; column headers bold 12px `#1a5276` `["Windows","macOS","Linux"]` centered at x = `[280, 460, 620]` on y=56; cells 150px wide, 26px tall, 4px radius, left edges at x = `[205, 385, 545]`.
- **Cell colors:** full read/write = fill `rgba(0,131,0,0.22)` with 12px `#008300` label; partial (read-only or extra install) = fill `rgba(201,133,0,0.20)` with 12px `#c98500` label; no support = fill `rgba(107,114,128,0.12)` with 12px `#6b7280` label "no".
- **Cell labels per row (Windows / macOS / Linux):** FAT32 `["r/w","r/w","r/w"]` (green/green/green); exFAT `["r/w","r/w","r/w 5.4+"]` (green/green/green); NTFS `["r/w","read-only","r/w 5.15+"]` (green/yellow/green); ext4 `["no","no","r/w"]` (gray/gray/green); APFS `["no","r/w","no"]` (gray/green/gray); ZFS `["no","no","install"]` (gray/gray/yellow).
- **Annotation (bold 13px blue `#2a78d6`, below the grid near y=285, left of center):** "only two rows are all-green — and FAT32 can't hold the 6 GB video".
- **Caption (12px `#444`, bottom right):** "support levels per current OS docs; Linux kernel versions shown".

## The Scorecard: Journals, Checksums, Snapshots, Size Limits

**Tags:** `worked example` (blue), `feature matrix` (green)

- **Journaling** — NTFS and ext4 log metadata changes before applying them; APFS and ZFS use copy-on-write instead
- **Data checksums** — only ZFS checksums every data block; ext4 and APFS checksum their metadata only
- **Snapshots** — APFS and ZFS can freeze a point-in-time view of the volume built in; the other four cannot
- **Max file** — FAT32 caps at 4 GB; NTFS/ext4 16 TB (default format); APFS 8 EB; exFAT and ZFS 16 EB
- **Hand-check** — the 6 GB video fits on five of the six rows; only the FAT32 size cell blocks it

*Example (italic):* Reading the exFAT row left to right: no journal, no data checksums, no snapshots, 16 EB files — maximum reach across machines, minimum built-in protection.

**Key point:** Read the matrix by rows for "what does this filesystem give me" and by columns for "who has this feature" — every cell is a documented spec, not a benchmark.

### Visualization (canvas `c2`, 720×300)

Feature-matrix grid: 6 filesystem rows × 4 feature columns rendered as colored cells with short text labels.

- **Title (bold 15px, `#1a5276`, top center):** "The Family Scorecard: Safety Features and Size Limits".
- **Geometry:** row labels 12px `#2c3e50` at x=20, rows `["FAT32","exFAT","NTFS","ext4","APFS","ZFS"]` at y = `[78, 112, 146, 180, 214, 248]`; column headers bold 12px `#1a5276` `["journal/CoW","data checksums","snapshots","max file"]` centered at x = `[245, 380, 510, 635]` on y=56; cells 120px wide, 26px tall, 4px radius, left edges at x = `[185, 320, 450, 580]`.
- **Cell colors:** feature present = fill `rgba(0,131,0,0.22)`, 12px `#008300` label; partial = fill `rgba(201,133,0,0.20)`, 12px `#c98500` label; absent = fill `rgba(107,114,128,0.12)`, 12px `#6b7280` label "no"; max-file cells are neutral fill `rgba(42,120,214,0.12)` with 12px `#2a78d6` size text, except FAT32's which is `rgba(201,133,0,0.20)` with `#c98500` "4 GB".
- **Cell labels per row (journal / checksums / snapshots / max file):** FAT32 `["no","no","no","4 GB"]`; exFAT `["no","no","no","16 EB"]`; NTFS `["metadata","no","no","16 TB"]` (green/gray/gray/blue); ext4 `["metadata","metadata only","no","16 TB"]` (green/yellow/gray/blue); APFS `["CoW","metadata only","yes","8 EB"]` (green/yellow/green/blue); ZFS `["CoW","full data","yes","16 EB"]` (green/green/green/blue).
- **Annotation (bold 13px magenta `#d55181`, below the grid near y=285, centered):** "ZFS checks every safety box; exFAT checks none — it wins on reach, not protection".
- **Caption (12px `#444`, bottom right):** "cells reflect documented specs; NTFS 16 TB is the default-format limit".

## Where the Wrong Row Bites a Data Scientist

**Tags:** `where it's used` (blue), `big files` (green), `data safety` (orange)

- **The 4 GB wall** — a parquet dump or raw video over 4 GB simply refuses to copy onto a FAT32 drive
- **Camera cards** — SD cards ship as FAT32 or exFAT, which is why many cameras split long videos at 4 GB
- **Silent bit rot** — without data checksums, a flipped bit in an old archive is served back as if correct
- **Power cuts** — a journal or copy-on-write keeps the filesystem's own bookkeeping consistent after a crash
- **Long archives** — ZFS's full data checksums plus periodic scrubs make it a classic storage-server choice

*Example (italic):* A 20 GB training-data file copies fine onto exFAT, NTFS, ext4, APFS, or ZFS, but errors out immediately on FAT32.

**Key point:** The size column decides whether today's copy succeeds; the journal and checksum columns decide whether the data is still trustworthy years from now.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart of max file size per filesystem on a log-feel scale (hardcoded pixel widths, not a real log axis), with a dashed marker for the 6 GB video.

- **Title (bold 15px, `#1a5276`, top center):** "Max File Size: Only One Bar Stops Before Your Video".
- **Axis:** vertical 2px `#999` baseline at x=200, bars extend right, max width 460; left-aligned 12px `#444` row labels at x=20.
- **Rows (top to bottom at y = `[70, 105, 140, 175, 210, 245]`), bars 16px tall:**
  - "FAT32 — 4 GB": yellow `#c98500` bar width 40
  - "NTFS — 16 TB": blue `#2a78d6` bar width 170
  - "ext4 — 16 TB": blue `#2a78d6` bar width 170
  - "APFS — 8 EB": aqua `#199e70` bar width 300
  - "exFAT — 16 EB": green `#008300` bar width 320
  - "ZFS — 16 EB": green `#008300` bar width 320
- **Bar labels:** 11px `#444` size text ("4 GB", "16 TB", "16 TB", "8 EB", "16 EB", "16 EB") just past each bar end.
- **Video marker:** vertical dashed `#6b7280` (dash 4/3) line at x=255 from y=55 to y=260, 12px `#6b7280` label "the 6 GB video" at its top — right of FAT32's bar end, left of all others.
- **Annotation (bold 13px orange `#d95926`, near x=380, y=95):** "the 4 GB wall is the only limit you'll actually hit".
- **Caption (12px `#444`, bottom right):** "bar widths log-feel schematic; size limits are documented specs".

## Biggest Max-File Number Isn't the Safest Choice

**Tags:** `common mistake` (red), `journaling` (orange)

- **The mistake** — ranking filesystems by the max-file column and crowning exFAT the most advanced
- **What's missing** — exFAT has no journal and no data checksums; an unplug mid-write can corrupt its file table
- **The journal's job** — ext4 replays its log after a crash and mounts back to a consistent state in seconds
- **Checksums vs journal** — a journal protects the bookkeeping; only data checksums catch bit rot in the files
- **The right read** — first keep rows that clear the compatibility need, then pick the most safety cells

*Example (italic):* A drive yanked mid-copy leaves exFAT's file table half-written and a repair tool guessing; the same yank on ext4 is fixed by journal replay at the next mount.

**Common mistake:** Treating one column as the ranking. exFAT's 16 EB limit sits in the same row as three "no" safety cells — big files and safe files are different columns.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: power cut mid-write on exFAT (no journal, corruption risk) vs ext4 (journal replay, consistent), shown as boxes with arrows.

- **Title (bold 15px, `#1a5276`, top center):** "Power Cut Mid-Write: No Journal vs Journal".
- **Row 1 (boxes centered on y=105), label 12px `#444` at x=20:** "exFAT"; blue `#2a78d6` rounded box at x=130 labeled "writing file + updating table" (12px), 3px arrow to a yellow `#c98500` box at x=350 labeled "power cut: table half-written", arrow to a red `#e74c3c` box at x=560 labeled "repair tool guesses" with bold 12px red "✗ file may be lost".
- **Row 2 (boxes centered on y=215), label:** "ext4"; blue box at x=130 labeled "journal entry written first", 3px arrow to a yellow box at x=350 labeled "power cut mid-write", arrow to a green `#008300` box at x=560 labeled "journal replay on mount" with bold 12px green "✓ consistent in seconds".
- **Box style:** 150–170px wide, 44px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(201,133,0,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text, two-line wrap allowed.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=280):** "the max-file-size column says nothing about surviving a power cut".
- **Caption (12px `#444`, bottom right):** "flow schematic; journal protects metadata, not file contents".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all cell labels, row/column positions, and bar widths are the hardcoded arrays above (no randomness); filesystem facts (journaling, checksums, snapshots, max file sizes 4 GB / 16 TB / 16 TB / 8 EB / 16 EB / 16 EB, OS support levels) are documented public specs; the family drive, the 6 GB video, and the 20 GB training file are invented framing, and the log-feel bar widths in `c3` are schematic and labeled so.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
