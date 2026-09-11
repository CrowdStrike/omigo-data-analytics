# exFAT

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** exFAT

**Subtitle:** Microsoft stretched the old FAT design in 2006 so flash cards could hold giant files — and it became the one format every gadget can read

## The 12 GB Wedding Video That Wouldn't Fit

**Tags:** `core idea` (blue), `4 GB ceiling` (orange), `FAT stretched` (green)

- **The shoot** — a videographer records a wedding ceremony as one 12 GB video file on an SD card
- **The ceiling** — FAT32, the old card format, cannot store any file larger than 4 GB, full stop
- **The workaround** — FAT32 cameras silently split long recordings into awkward ~4 GB chunks
- **The stretch** — exFAT (2006) keeps FAT's simple table-of-clusters design but widens the size fields
- **The result** — the same card, formatted exFAT, holds the 12 GB video as one ordinary file

*Example (italic):* On FAT32 the ceremony arrives as four ~4 GB fragments to stitch together; on exFAT it is a single 12 GB file that plays straight through.

**Key point:** exFAT is FAT modernized for flash storage — same simple design, but the 4 GB file-size ceiling is gone, which is why big video files were its whole reason to exist.

### Visualization (canvas `c1`, 720×300)

Vertical bar chart of four real-world file sizes against a dashed horizontal line at FAT32's 4 GB ceiling; bars that clear the line are the files FAT32 cannot hold.

- **Title (bold 15px, `#1a5276`, top center):** "Files vs the FAT32 Ceiling: What Forced exFAT to Exist".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = file size 0 to 24 GB, gridlines `#e5e9ef` at 8 and 16 with 12px `#444` labels "8 GB" / "16 GB"; x = four bars, 90px wide, centered at x = 140, 290, 440, 590, 12px `#444` labels under each.
- **Bars (labels, sizes GB):** `["10-min clip", "phone backup", "wedding video", "4K timelapse"]`, sizes `[1.1, 3.8, 12, 22]`; bars at or under 4 GB blue `#2a78d6`, bars over 4 GB magenta `#d55181`; 12px value labels ("1.1 GB" ... "22 GB") above each bar top.
- **Ceiling line:** dashed `#6b7280` (dash 5/4) horizontal line at the 4 GB height across the plot, bold 12px `#6b7280` label "FAT32 limit: 4 GB" above its left end.
- **Annotation (bold 13px green `#008300`, upper right near y=48):** "on exFAT all four fit as single files".
- **Caption (12px `#444`, bottom right):** "file sizes illustrative".

## Counting Clusters on a 128 GB Card

**Tags:** `worked example` (blue), `clusters` (green), `slack space` (orange)

- **The cluster** — exFAT hands out space in fixed blocks called clusters; this card uses 128 KB ones
- **The video** — 12 GB is 12 × 1,024 = 12,288 MB; each 128 KB cluster is 0.125 MB
- **Hand-check** — 12,288 ÷ 0.125 = 98,304 clusters, and the video fills them edge to edge: 0% waste
- **The tiny file** — a 4 KB recipe note still occupies one whole 128 KB cluster; 124 KB sits empty
- **The pile** — 1,000 such notes hold ~4 MB of data but claim 1,000 × 128 KB = 125 MB of disk
- **The slack** — that is about 97% wasted space, the price of clusters sized for giant video files

*Example (italic):* The 12 GB video uses exactly 98,304 clusters with no waste, while 1,000 tiny 4 KB notes burn 125 MB of card to store 4 MB of text.

**Key point:** exFAT allows huge clusters (up to 32 MB) so big files stay fast and the allocation table stays small — great for video, wasteful for folders full of tiny files.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart with two workloads: data actually stored vs space claimed on disk, showing zero slack for the big video and massive slack for the pile of tiny files.

- **Title (bold 15px, `#1a5276`, top center):** "128 KB Clusters: Perfect for One Big File, Wasteful for 1,000 Tiny Ones".
- **Axis:** vertical 2px `#999` baseline at x=250, bars extend right, max width 420; pixel widths schematic (not a shared linear scale), each pair labeled with its true MB.
- **Row 1 (data bar y=80, on-disk bar y=110), left-aligned 12px `#444` labels at x=20:** "12 GB video — 98,304 clusters"; data bar blue `#2a78d6` width 420 labeled "data 12,288 MB" (11px, at bar end); on-disk bar green `#008300` width 420 labeled "on disk 12,288 MB — 0% slack" (11px bold green).
- **Row 2 (data bar y=185, on-disk bar y=215), label at x=20:** "1,000 notes × 4 KB each"; data bar blue width 8 labeled "data 4 MB"; on-disk bar orange `#d95926` width 250 labeled "on disk 125 MB — 97% slack" (11px bold orange `#d95926`).
- **Bar style:** 16px tall, data bars fill `rgba(42,120,214,0.30)` with 1px `#2a78d6` border, on-disk bars solid.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=270):** "big clusters love big files and punish small ones".
- **Caption (12px `#444`, bottom right):** "cluster math exact; bar widths schematic".

## The One Format Every Gadget Can Read

**Tags:** `where it's used` (blue), `interoperability` (green), `SD standard` (orange)

- **The floor** — exFAT is the least-common-denominator format that Windows, macOS, and Linux all write
- **The rivals** — NTFS, APFS, and ext4 are each at home on one platform, mostly foreign on the rest
- **The mandate** — since 2009 the SDXC card standard (cards over 32 GB) specifies exFAT as its format
- **The opening** — Microsoft published the exFAT spec in 2019; Linux gained native support in kernel 5.4
- **The habit** — cameras, drones, game consoles, and TVs ship exFAT because every buyer's computer reads it

*Example (italic):* The videographer's card moves from camera to a Mac for editing to a client's Windows laptop, and every stop reads it without installing anything.

**Key point:** exFAT wins not by being the best filesystem but by being the interoperability floor — the one format that a card can be formatted with and still work everywhere.

### Visualization (canvas `c3`, 720×300)

Support matrix: five filesystems (rows) against five device types (columns), with full read+write, read-only, and unsupported cells; the exFAT row is the only all-green one.

- **Title (bold 15px, `#1a5276`, top center):** "Who Can Use Which Format: exFAT Is the Only Full Row".
- **Grid geometry:** column headers 12px bold `#444` at y=60 over columns centered at x = 220, 320, 420, 520, 620: `["Windows", "macOS", "Linux", "Camera", "TV/console"]`; row labels 12px bold `#2c3e50` left-aligned at x=20 for rows at y = 95, 135, 175, 215, 255: `["NTFS", "APFS", "ext4", "FAT32", "exFAT"]`.
- **Cell symbols (16px bold, centered per cell):** full read+write = "✓" green `#008300`; read-only = "R" yellow `#c98500`; unsupported = "–" mute `#6b7280`.
- **Matrix values (rows top to bottom):** NTFS `["✓", "R", "✓", "–", "–"]`; APFS `["–", "✓", "–", "–", "–"]`; ext4 `["–", "–", "✓", "–", "–"]`; FAT32 `["✓", "✓", "✓", "✓", "✓"]` with 11px `#c98500` note "4 GB file cap" right of the row; exFAT `["✓", "✓", "✓", "✓", "✓"]`.
- **Row highlight:** light green band `rgba(0,131,0,0.08)` behind the exFAT row (x=15 to 690, 30px tall); thin `#e5e9ef` gridlines between rows.
- **Legend (12px, y=285, left):** "✓ read+write   R read-only   – unsupported".
- **Annotation (bold 13px green `#008300`, right of the exFAT row):** "the floor everyone shares".
- **Caption (12px `#444`, bottom right):** "default OS support, no extra drivers".

## No Journal Means No Undo

**Tags:** `common mistake` (red), `no journal` (orange)

- **The confusion** — people treat exFAT like a rugged modern filesystem; it skips a key safety feature
- **The journal** — NTFS and ext4 log each change before making it, so a crash can be replayed or rolled back
- **exFAT's gap** — it has no journal; a write updates the bitmap, the data, and the directory in separate steps
- **The yank** — pull the card or lose power mid-write and the steps disagree: orphaned clusters, wrong sizes
- **The habit** — always eject safely; treat exFAT cards as transport, not as the only copy of anything

*Example (italic):* The videographer pulls the card while the last clip is still flushing; the bitmap says the clusters are used, but no directory entry points at them — the clip is gone.

**Common mistake:** Assuming exFAT protects data like NTFS or ext4 does. It was kept journal-free to stay simple and flash-friendly, so an interrupted write has no automatic repair — the safe-eject step is doing real work.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: the same power cut during a write, replayed on a journaled filesystem (recovers) and on exFAT (leaves orphaned clusters).

- **Title (bold 15px, `#1a5276`, top center):** "Power Cut Mid-Write: Journal Replays, exFAT Shrugs".
- **Row 1 (boxes centered on y=105), label 12px `#444` at x=20:** "journaled (NTFS/ext4)"; blue `#2a78d6` rounded box at x=185 labeled "1. log intent in journal" (12px), 3px arrow to a blue box at x=390 labeled "2. write data + metadata", arrow to a green `#008300` box at x=580 labeled "reboot: replay journal" with bold 12px green "✓ consistent" beneath it.
- **Row 2 (boxes centered on y=215), label at x=20:** "exFAT (no journal)"; blue box at x=185 labeled "1. mark clusters in bitmap", 3px arrow to a blue box at x=390 labeled "2. write data...", then a red `#e74c3c` box at x=580 labeled "3. directory entry never written" with bold 12px red "✗ orphaned clusters" beneath it.
- **Power-cut marker:** vertical dashed `#6b7280` (dash 4/3) line at x=490 spanning both rows, bold 12px `#6b7280` label "power cut" at its top (y=45).
- **Box style:** 150–175px wide, 42px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(0,131,0,0.12)` / `rgba(231,76,60,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=280):** "eject only after the light stops blinking".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the c1 file sizes `[1.1, 3.8, 12, 22]` GB and the c2 workload are invented and labeled illustrative/schematic; the cluster arithmetic (12,288 MB ÷ 0.125 MB = 98,304 clusters; 1,000 × 128 KB = 125 MB for ~4 MB of data ≈ 97% slack) is exact; the FAT32 4 GB limit, the 2006/2009/2019 dates, the SDXC mandate, kernel 5.4 support, and the c3 support matrix are documented public facts.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
