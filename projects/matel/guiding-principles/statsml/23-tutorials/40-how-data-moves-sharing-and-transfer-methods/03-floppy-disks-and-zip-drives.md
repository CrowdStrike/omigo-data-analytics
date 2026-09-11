# Floppy Disks & Zip Drives

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Floppy Disks &amp; Zip Drives

**Subtitle:** Before networks were everywhere, files traveled by hand on a plastic square — and its 1.44 MB limit taught everyone what a megabyte was

## Sharing a File Meant Walking It Across the Room

**Tags:** `core idea` (blue), `physical media` (orange), `hand-to-hand` (green)

- **The disk was the network** — before attachments, sharing a file meant handing someone a disk
- **5.25-inch floppy** — the bendy square of the early PC era; the common version held 360 KB
- **3.5-inch floppy** — the rigid pocket square of the 1990s; the HD version held 1.44 MB
- **Zip drive** — a 1994 cartridge holding 100 MB, roughly 70 floppies in one plastic shell
- **One disk, one task** — a term paper, a spreadsheet, or a small program fit; not much else

*Example (italic):* Alice copies her 900 KB report onto a floppy, walks it to Bob's desk, and Bob copies it off — that was file sharing.

**Key point:** The floppy made data physical — a file was a thing you could hold, label with a pen, hand over, and lose in a drawer.

### Visualization (canvas `c1`, 720×300)

Horizontal bar chart on a log-scale axis: the capacity ladder from 5.25" floppy to Zip 250.

- **Title (bold 15px, ink `#1a5276`, top center):** "The Size Ladder: Five Steps From 360 KB to 250 MB".
- **Axis:** horizontal log10(KB) scale, value 10 KB (log 1) at x=180 to 1 GB (log 6) at x=690; baseline `#999` at y=252; vertical gridlines `#e5e9ef` with 12px `#444` tick labels "10 KB", "100 KB", "1 MB", "10 MB", "100 MB", "1 GB".
- **Bars (height 26px, one per row, y centers at 68/106/144/182/220), row labels 12px `#2c3e50` right-aligned at x=172:** 5.25" floppy = 360 KB (blue `#2a78d6`), 3.5" DD = 720 KB (aqua `#199e70`), 3.5" HD = 1,440 KB (green `#008300`), Zip 100 = 100,000 KB (orange `#d95926`), Zip 250 = 250,000 KB (violet `#4a3aa7`). Bar length = (log10(KB) − 1) / 5 of the axis span, from x=180.
- **Value labels (bold 12px, bar color, just right of each bar end):** "360 KB", "720 KB", "1.44 MB", "100 MB", "250 MB".
- **Annotation (bold 13px orange `#d95926`, near x=430, y=205):** "one Zip cartridge ≈ 70 floppies".
- **Caption (11px `#6b7280`, bottom right):** "log scale — each gridline is 10x; capacities as marketed".

## Insert Disk 7 of 26: The Arithmetic of Spanning

**Tags:** `worked example` (green), `redo it by hand` (blue)

- **The rule** — disks = file size ÷ 1.44 MB, rounded up; the last disk rides mostly empty
- **4 MB photo** — 4 ÷ 1.44 = 2.8, so 3 floppies; one 100 MB Zip disk swallows it whole
- **45 MB install** — 45 ÷ 1.44 = 31.3, so 32 floppies, fed in one at a time, in order
- **700 MB movie** — 700 ÷ 1.44 = 486.1, so 487 floppies; on Zip disks, just 7
- **Windows 95** — really shipped on 13 floppies, using a squeezed 1.68 MB disk format
- **Spanning risk** — one file split across disks; lose or scratch disk 19 and the set is dead

*Example (italic):* Backing up a 45 MB drive meant sitting by the machine, swapping 32 numbered disks each time it asked "insert next disk".

**Key point:** Rounding up — ceiling division — is the same arithmetic behind memory pages, network packets, and upload chunks today.

### Visualization (canvas `c2`, 720×300)

Grouped bar chart on a log-scale y-axis: disks needed per file, floppy vs Zip, for the three worked-example files.

- **Title (bold 15px, ink `#1a5276`, top center):** "Disks Needed to Carry One File (log scale)".
- **Axes:** origin x=80, baseline y=240, plot width 560, plot height 175; y = log10(count) from 1 to 1000, horizontal gridlines `#e5e9ef` at counts 1, 10, 100, 1000 with 12px `#444` labels; three x groups centered at x=185, x=360, x=535 with 13px `#444` labels "4 MB photo", "45 MB install", "700 MB movie".
- **Bars (54px wide, 16px gap within a group):** 1.44 MB floppy (blue `#2a78d6`) counts `[3, 32, 487]`; Zip 100 MB (orange `#d95926`) counts `[1, 1, 7]`. Bar height = (log10(count) + 0.15) / 3.15 of plot height, so a count of 1 draws a visible stub.
- **Value labels (bold 13px, bar color, just above each bar):** "3", "32", "487" and "1", "1", "7".
- **Legend (12px, top right):** blue swatch "1.44 MB floppy", orange swatch "Zip 100 MB".
- **Annotation (bold 13px green `#008300`, centered near x=360, y=62):** "one movie: 487 floppies — or 7 Zip disks".
- **Caption (11px `#6b7280`, bottom right):** "counts = size ÷ capacity, rounded up".

## Where a Data Scientist Still Meets the Floppy

**Tags:** `why it matters` (orange), `units` (blue), `chunking` (green)

- **Units became intuition** — a generation learned what a megabyte is by running out of them
- **Chunking lives on** — multi-disk spanning is the ancestor of multipart uploads and file splits
- **The gap kills media** — files grew faster than disks, so each medium was outgrown and retired
- **Sneakernet survives** — for huge datasets, shipping drives by truck still beats the network
- **Capacity question** — "does it fit on the disk?" is the original storage-planning exercise

*Example (italic):* A 100 GB dataset uploaded in 2 GB chunks follows the exact logic of disk 1-of-26 spanning — split, number, reassemble.

**Key point:** The floppy died of a growth mismatch — data outgrows every medium eventually, and planning for that gap is part of the job.

### Visualization (canvas `c3`, 720×300)

Two lines on a log-scale y-axis over the years 1982–2000: disk capacity (step line) vs the typical file people wanted to share (rising line) — the file line crosses the disk line and the medium dies.

- **Title (bold 15px, ink `#1a5276`, top center):** "Files Outgrew the Disk (illustrative)".
- **Axes:** origin x=80, baseline y=240, plot width 540, plot height 175; x = years 1982 to 2000, 12px `#444` tick labels at 1982, 1988, 1994, 2000; y = log10(KB) from 1 to 6, horizontal gridlines `#e5e9ef` at 10 KB, 1 MB, 100 MB with 12px `#444` labels.
- **Disk capacity step line (blue `#2a78d6`, 3px):** through (1982, 360 KB) → (1987, 360) → step up → (1987, 1,440) → (1994, 1,440) → step up → (1994, 100,000) → (2000, 100,000). Steps drawn as vertical segments.
- **Typical shared file line (orange `#d95926`, 3px, 4px dots):** points (1982, 15 KB document), (1988, 120 KB image), (1993, 1,000 KB scan), (1996, 4,000 KB photo), (2000, 700,000 KB movie) — plot values only, names appear in the legend caption below.
- **Legend (12px, top left inside plot):** blue swatch "disk capacity (floppy → Zip)", orange swatch "typical file people shared".
- **Annotation (bold 13px red `#e74c3c`, near x=470, y=95):** "file passes disk — the medium dies".
- **Caption (11px `#6b7280`, bottom right):** "illustrative sizes; capacities as marketed".

## Two Leftovers: The Save Icon and the Fuzzy Megabyte

**Tags:** `common confusion` (red), `fun fact` (green)

- **The save icon** — the 3.5-inch floppy still means "save" to people who never touched one
- **1.44 MB is neither** — the disk holds 1,474,560 bytes: not 1.44 million, not 1.44 binary MB
- **A hybrid unit** — 1,440 KB of 1,024 bytes each, then divided by 1,000: two unit systems mixed
- **Zip vs .zip** — the Zip drive and the .zip compressed file format are unrelated namesakes
- **Formatted space** — the file table takes a slice, so an empty disk shows about 1.38 MB free

*Example (italic):* Bob wonders why his brand-new "1.44 MB" disk shows 1.38 MB free — three different megabytes are in play at once.

**Key point:** The 1000-vs-1024 confusion that still muddies drive sizes and memory specs started here — when a capacity looks a few percent off, suspect the units first.

### Visualization (canvas `c4`, 720×300)

Two-panel: left, a drawn 3.5" floppy pictogram (the save icon); right, three close bars on a zoomed byte axis showing what "1.44 MB" could mean vs what the disk actually holds.

- **Title (bold 15px, ink `#1a5276`, top center):** "Three Different '1.44 MB'".
- **Divider:** dashed `#bdc3c7` vertical line at x=235 from y=40 to y=282.
- **Left panel (pictogram centered at x=120):** floppy body = rounded rect x=55 y=62 w=130 h=130, fill `rgba(42,120,214,0.15)`, 3px blue `#2a78d6` border, top-right corner clipped; metal shutter = rect x=90 y=62 w=60 h=38 fill `#6b7280` with a white window slot; label area = white rect x=75 y=130 w=90 h=48 with 2px `#199e70` border and two light `#e5e9ef` rule lines. Caption below (bold 12px ink, centered at x=120, y=222): "this shape still means 'save'"; second line (11px `#6b7280`, y=240): "on toolbars, decades after the disk".
- **Right panel bars:** origin x=300, baseline y=240, plot width 380, plot height 165; y axis zoomed from 1,400,000 to 1,520,000 bytes (bar height = (bytes − 1,400,000) / 120,000 of plot height); three bars 84px wide centered at x=360, x=485, x=610.
- **Bars and labels (13px `#2c3e50` two-line labels below baseline):** "1.44 million bytes" = 1,440,000 (yellow `#c98500`); "actual capacity" = 1,474,560 (green `#008300`); "1.44 binary MB" = 1,509,949 (violet `#4a3aa7`).
- **Value labels (bold 12px, bar color, just above each bar):** "1,440,000", "1,474,560", "1,509,949".
- **Annotation (bold 12px ink `#1a5276`, centered over the bars at y=58):** "'1.44 MB' = 1,474,560 ÷ (1000 × 1024) — a mixed unit".
- **Caption (11px `#6b7280`, under the baseline, right):** "y-axis starts at 1,400,000 bytes to show the gap".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label "Key point:").
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home/cross-page links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array, run once, and re-run on window resize (debounced 150ms).
- **Chart palette object:** `const P = { blue:'#2a78d6', green:'#008300', magenta:'#d55181', yellow:'#c98500', aqua:'#199e70', orange:'#d95926', violet:'#4a3aa7', ink:'#1a5276', text:'#2c3e50', mute:'#6b7280', grid:'#e5e9ef' }`. Navy ink for titles and axes; red `#e74c3c` only for the "medium dies" alarm annotation in c3.
- **Data:** all chart values are the hardcoded arrays above — no randomness. Disk capacities (360 KB, 720 KB, 1,440 KB, 100 MB, 250 MB), the byte counts (1,440,000 / 1,474,560 / 1,509,949), and the Windows 95 13-disk fact are documented; the disks-needed counts are exact ceiling divisions of the stated file sizes; c3's "typical file" sizes are invented and the chart is titled "(illustrative)".
- Chart numbers must match the text bullets exactly (3 / 32 / 487 floppies; 1 / 1 / 7 Zip disks; 1,474,560 bytes; ~1.38 MB free).
