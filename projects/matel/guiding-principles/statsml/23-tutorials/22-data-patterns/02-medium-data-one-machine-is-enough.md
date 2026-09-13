# Medium Data: One Machine Is Enough

**Page type:** detail page (tutorial topic page: `.card-section` blocks, each h2 + two-column `table.layout` — text left 50%, canvas right 50%)
**HTML title tag:** Medium Data: One Machine Is Enough

**Subtitle:** Too big for a spreadsheet but comfortable on a single machine — a database and some patience go a long way

## The Orders Table Outgrows the Spreadsheet

**Tags:** `core idea` (blue), `running example` (green)

- **A retailer grows** — its orders table reaches 200 million rows, about 60 GB on disk
- **Spreadsheet fails** — Excel stops at 1,048,576 rows; this table is ~190x past that
- **RAM fails too** — 60 GB does not fit in the machine's 16 GB of memory at once
- **Disk is fine** — the machine's 2 TB drive holds 60 GB using only 3% of its space
- **That is medium data** — bigger than memory, far smaller than one machine's disk

*Example:* The analyst who tried to open the 60 GB orders file in Excel watched it freeze; a database on the same laptop queried it fine.

**Key point:** Medium data is the zone between memory and disk — one ordinary machine with a database handles it comfortably.

### Visualization (canvas `c1`, 720×300)

Log-scale size axis showing where the 60 GB orders table sits between spreadsheet limit, RAM, and one disk.

- **Title (bold 16px, `#1a5276`, top center):** "Where the 60 GB Orders Table Sits".
- **Axis:** horizontal line at y=190 from x=60 to x=680 (`#6b7280`, width 2). Position is log10(bytes) mapped from logMin=8 to logMax=12.5 (100 MB to ~3 TB).
- **Vertical tick marks** (each a colored line from y=130 to y=200, bold 13px label above at y=118, 12px note below at y=218):
  - `spreadsheet limit` at 3e8 bytes, note "~1M rows", orange `#d95926`
  - `RAM` at 1.6e10 bytes, note "16 GB", violet `#4a3aa7`
  - `one disk` at 2e12 bytes, note "2 TB", blue `#2a78d6`
- **Orders marker:** green `#008300` downward triangle at 6e10 bytes just above the axis, labeled "orders table: 60 GB" in bold 13px above it.
- **Annotations (centered below axis):** green bold 13px "too big for a spreadsheet or RAM — tiny next to one disk"; muted `#6b7280` 12px "size on a log scale — each step to the right is a big jump".

## The Phone Book Trick: Why an Index Works

**Tags:** `worked example` (green), `core idea` (blue)

- **The question** — find customer 48213's orders among 200 million rows
- **Dumb way** — read every row, like reading a 1,000-page phone book cover to cover
- **Phone book way** — open the middle, see which half holds the name, repeat
- **Count the flips** — 1,000 → 500 → 250 → 125 → 63 → 32 → 16 → 8 → 4 → 2 → 1: ten flips
- **Why ten** — each flip halves the pages, and 2¹⁰ = 1,024, just over 1,000
- **An index is that** — a sorted lookup the database keeps so it can jump, not scan

*Example:* Full scan: about 5 minutes of disk reading; indexed lookup: 12 rows fetched in a blink.

**Key point:** Sorting plus halving turns "read everything" into "a handful of jumps" — that trick is what makes one machine enough.

### Visualization (canvas `c2`, 720×300)

Two-panel comparison: full cover-to-cover read vs a halving staircase, split by a dashed vertical divider at x=320 (`#e5e9ef`, dash 4/3).

- **Title (bold 16px, `#1a5276`, top center):** "Finding One Name in a 1,000-Page Phone Book".
- **Left panel:** orange `#d95926` bar (x=60, y=70, 200×34) with bold 13px label above "read all 1,000 pages" and white bold 13px text inside "1,000 page reads". Below in muted 12px: "like a full table scan:" / "200,000,000 rows read"; then orange bold 13px "~5 minutes of disk reading".
- **Right panel:** halving staircase bar chart in green `#008300`; data `[1000, 500, 250, 125, 63, 32, 16, 8, 4, 2, 1]`, 11 bars starting at x=360, total width 320, baseline y=232, chart height 150, bar height proportional to value/1000 (min 3px). Bold 13px label above: "halve the pages each flip". Value labels 12px on bars 1000, 500, 250 and "...1" on the last bar. Thin muted baseline; muted 12px x-label "flip 1 . . . flip 10"; green bold 13px caption "10 flips, because 2¹⁰ = 1,024".
- **Bottom center (blue `#2a78d6` bold 13px):** "an index = the database's phone book".

## Where the Analyst Meets Medium Data

**Tags:** `where it's used` (blue), `two wrong turns` (orange)

- **Very common zone** — order histories, clickstreams, patient records often land here
- **Wrong turn 1** — forcing it into a spreadsheet: crashes, or silently truncated rows
- **Wrong turn 2** — jumping to a cluster of machines: weeks of setup nobody needed
- **Right turn** — one database on one machine; lookups in a blink, big reports in minutes
- **Patience is a tool** — a 20-minute nightly report on one machine is a fine design

*Example:* The monthly revenue report scans all 200 million rows in about 5 minutes — once a month, that is nothing.

**Key point:** Most "big data" problems in companies are medium data — a database plus patience beats a cluster plus complexity.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart of query times on a log scale.

- **Title (bold 16px, `#1a5276`, top center):** "One Database, One Machine, 200 Million Rows".
- **Layout:** bars start at x=265, right padding 95, first row top y=62, row height 50, bar height 28. Length is log10(seconds) mapped from logMin=−2.5 to logMax=3 (0.003 s to 1000 s).
- **Rows** (label right-aligned left of bar in bold 13px `#2c3e50`; time note in bar color right of bar):
  - "one customer's orders (index)" — 0.01 s, note "0.01 s", green `#008300`
  - "one month's revenue (partition)" — 9 s, note "9 s", blue `#2a78d6`
  - "full-history report (scan)" — 300 s, note "5 min", violet `#4a3aa7`
- **Annotations (centered):** blue bold 13px "even the slowest query is a coffee break, not a cluster"; muted 12px near bottom "illustrative times, log scale".

## The Confusion: "Bigger Than Memory" Is Not "Big Data"

**Tags:** `common mistake` (red), `rule of thumb` (blue)

- **The reflex** — "60 GB won't fit in RAM, we need a cluster" — no, you need a disk
- **Databases stream** — they read the table from disk in chunks, never all at once
- **The real ceiling** — one machine's disk: a 2 TB drive is 125x the 16 GB of RAM
- **Room to grow** — at 12 GB of new orders a year, 60 GB takes decades to fill 2 TB
- **The test** — data bigger than one machine's disk is big; below that, it is medium

*Example:* The 60 GB table that "needed a cluster" fit on the laptop's own drive 33 times over.

**Key point:** Memory is a speed limit, not a size limit — the single-machine boundary is the disk, and disks are huge.

### Visualization (canvas `c4`, 720×300)

True-proportion area diagram: the orders table and RAM drawn as slices inside one disk rectangle.

- **Title (bold 16px, `#1a5276`, top center):** "The \"Too Big\" Table Inside One Machine's Disk".
- **Disk rectangle:** x=70, y=70, 580×120; fill `#f4f7fa`, stroke blue `#2a78d6` width 2; bold 13px blue label above-left: "one disk: 2 TB (2,000 GB)".
- **Slices inside (full height, widths to true proportion of 2000 GB):** violet `#4a3aa7` RAM slice, 16/2000 of width (~4.6px); green `#008300` orders slice, 60/2000 of width (~17.4px), placed 4px right of RAM slice.
- **Labels below:** violet bold 12px "RAM-sized: 16 GB"; green bold 12px "orders table: 60 GB (3% of the disk)" with a thin green leader line from the orders slice down to the label.
- **Annotations (centered, bottom):** green bold 14px "bigger than memory, 33x smaller than the disk — a database streams it in chunks"; muted 12px "slice widths drawn to true proportion".

## Regeneration instructions

- **Layout:** tutorial topic page. `<h1>` (no index number) with 2px `#2980b9` bottom border, `.subtitle` paragraph, then four `.card-section` blocks. Each section: `<h2>` (1.3rem, `#1a5276`, 2px `#2980b9` bottom border) followed by `table.layout` (one `<tr>`; left `td.text-col` 50% width, right `td.viz-col` 50% width, cells padded 12px, no cell borders).
- **Text column structure:** `.tags` row of colored pill spans (`.tag` — 0.72rem, weight 600, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`); then a `<ul>` (0.92rem) of one-line bullets each starting with `<b>` in `#1a5276`; one italic `.example` paragraph (`#555`, 0.9rem); one `.key-point` callout (background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem) opening with `<strong>Key point:</strong>`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; universal `* { margin:0; padding:0; box-sizing:border-box; }`; h1 2rem `#1a5276`; subtitle `#666` 0.95rem. No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
- **Canvas:** each canvas declared `width="720" height="300"`, CSS `width:100%`, border `1px solid #e0e0e0`, radius 4px; a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
