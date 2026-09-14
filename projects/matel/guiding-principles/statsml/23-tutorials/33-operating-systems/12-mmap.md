# mmap

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** mmap

**Subtitle:** mmap tells the OS to make a file appear as ordinary memory — you index into it like an array, and 4 KB pages load from disk only when you touch them

## The 8 GB Orders File That Opens in a Blink

**Tags:** `core idea` (blue), `zero copy` (green), `page fault` (orange)

- **The file** — a coffee-shop chain's order log holds 125 million orders of 64 bytes each: 8 GB
- **The old way** — read() copies bytes from disk into a kernel buffer, then again into your buffer
- **The map** — mmap() makes the file appear at an address, so orders[n] is just a memory access
- **Nothing loads yet** — the map is created instantly; not one byte leaves the disk at mmap time
- **First touch** — reading an unloaded mapped byte triggers a page fault; the OS pulls in one 4 KB page

*Example (italic):* The program calls mmap on the 8 GB log and gets an address back in under a millisecond — no order has been read from disk yet.

**Key point:** mmap doesn't copy a file into memory — it makes the file's bytes addressable, and the OS fetches 4 KB pages lazily the first time each one is touched.

### Visualization (canvas `c1`, 720×300)

Two-row flow diagram comparing the read() path (two copies of the data) with the mmap path (the file appears directly in the address space, loaded on touch).

- **Title (bold 15px, `#1a5276`, top center):** "read() Copies Twice; mmap() Just Points".
- **Row 1 (y=95), label 12px `#444` at x=20:** "read()"; blue `#2a78d6` rounded box at x=110 labeled "orders file (disk)" (12px), 3px arrow to a yellow `#c98500` box at x=310 labeled "kernel buffer — copy 1", 3px arrow to an orange `#d95926` box at x=520 labeled "your buffer — copy 2", with bold 12px orange `#d95926` "8 GB moved twice" beneath the row.
- **Row 2 (y=205), label:** "mmap()"; blue box at x=110 "orders file (disk)", dashed 2px `#6b7280` arrow (dash 5/4) labeled "page fault on first touch" (11px `#6b7280`) to a green `#008300` box at x=430 labeled "your address space — file appears here".
- **Box style:** 150–180px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(201,133,0,0.12)` / `rgba(217,89,38,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px green `#008300`, near x=430, y=265):** "touch a byte, load one 4 KB page".
- **Caption (12px `#444`, bottom right):** "box sizes schematic, file size illustrative".

## Finding Order #500,000 by Arithmetic

**Tags:** `worked example` (blue), `hand check` (green)

- **Fixed records** — every order is exactly 64 bytes, so order n starts at byte n × 64
- **The target** — order 500,000 starts at byte 500,000 × 64 = 32,000,000
- **The page** — 32,000,000 ÷ 4096 = page 7812 remainder 2048: byte 2048 inside page 7812
- **The fault** — first touch of page 7812 reads that 4 KB page (plus a few pre-fetched neighbors)
- **Ten lookups** — 10 random orders touch at most 10 pages: 40 KB read instead of 8 GB
- **The reread** — asking for order 500,000 again is a plain RAM access; the page is already in

*Example (italic):* To price-check order 500,000, the program reads orders[500000] — the OS loads page 7812 and the other 1,953,124 pages of the file stay on disk.

**Key point:** Cost follows pages touched, not file size — 10 random 64-byte reads cost 10 page faults, 40 KB of I/O against an 8 GB file.

### Visualization (canvas `c2`, 720×300)

Schematic page strip with one page highlighted, the lookup arithmetic written out, and two bars comparing bytes read.

- **Title (bold 15px, `#1a5276`, top center):** "Order 500,000 → Byte 32,000,000 → Page 7812, Offset 2048".
- **Page strip (y=70, height 34):** 25 cells 22px wide starting at x=60 (total width 550), white fill, 1px `#e5e9ef` borders; cell index 9 filled `rgba(0,131,0,0.25)` with 2px `#008300` border and bold 12px `#008300` label "page 7812" above it; 11px `#6b7280` labels "page 0" under the first cell and "page 1,953,124" under the last.
- **Arithmetic lines (13px `#2c3e50`, x=60):** at y=150 "byte = 500,000 × 64 = 32,000,000"; at y=172 "page = 32,000,000 ÷ 4096 = 7812 rem 2048".
- **Bars (rows at y=215 and y=248, 14px tall, starting x=230, left-aligned 12px `#444` labels at x=20):** "read whole file — 8 GB": blue fill `rgba(42,120,214,0.30)` width 440; "mmap, 10 lookups — 40 KB": solid green `#008300` width 4 with bold 12px green label "40 KB" at its end. Pixel widths schematic, not to scale.
- **Annotation (bold 13px violet `#4a3aa7`, right side near y=190):** "one page in, 1,953,124 stay on disk".
- **Caption (12px `#444`, bottom right):** "strip and bar widths schematic; arithmetic exact".

## Big Files on Small Laptops

**Tags:** `where it's used` (blue), `data science` (green), `shared memory` (orange)

- **numpy** — np.memmap opens the 8 GB log as an array on a 16 GB laptop without loading it
- **Databases** — SQLite and LMDB serve queries straight out of mapped file pages
- **Columnar files** — Arrow and Parquet readers map files so each slice loads page by page
- **Sharing** — two processes mapping the same file share one physical copy of each page
- **Programs themselves** — the OS starts every executable and library by mmap-ing it

*Example (italic):* A dashboard process and a batch job both map the orders log; the hot pages exist once in RAM, not twice.

**Key point:** Whenever a tool opens a huge file "instantly" and memory grows only as you scan it, mmap is usually what's underneath.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: resident memory for three ways of using the same 8 GB orders file, against a 16 GB RAM budget line.

- **Title (bold 15px, `#1a5276`, top center):** "Same 8 GB File, Three Memory Footprints (16 GB Laptop)".
- **Layout:** bars start at x=230, left-aligned 12px `#444` labels at x=20, scale 1 GB = 25px; vertical dashed 2px `#6b7280` (dash 4/3) budget line at x=630 with 12px `#6b7280` label "16 GB RAM" at its top.
- **Rows (y = 90, 150, 210, bars 16px tall):**
  - "read all into memory — 8 GB": orange `#d95926` bar width 200, 11px width label "8 GB" at end
  - "mmap + scan 10% of rows — 0.8 GB": blue `#2a78d6` bar width 20, label "0.8 GB"
  - "mmap + 10 lookups — 40 KB": green `#008300` bar width 2 (minimum visible), bold label "40 KB"
- **Annotation (bold 13px green `#008300`, centered near y=260):** "memory follows pages touched, not file size".
- **Caption (12px `#444`, bottom right):** "footprints illustrative".

## Mapped Is Not Loaded, Written Is Not Saved

**Tags:** `common mistake` (red), `durability` (orange)

- **The scare** — top shows the process "using" 8 GB of virtual memory; resident is just the touched pages
- **Mapped ≠ loaded** — the map is a promise; RAM fills one 4 KB page at a time as bytes are touched
- **Written ≠ saved** — storing into a mapped page changes RAM and marks the page dirty
- **The flush** — the disk copy updates only when the OS writes back — or when you call msync
- **The crash** — power loss before the flush loses the "written" order; the file keeps its old bytes

*Example (italic):* The program stamps order 500,000 as refunded and crashes a second later — on reboot the file still says paid, because the dirty page never flushed.

**Common mistake:** Treating a store into a mapped file as durable. The write lands in a RAM page marked dirty; until write-back or an explicit msync, the file on disk still holds the old bytes.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: a write through the map that crashes before flushing (lost) vs one followed by msync (durable).

- **Title (bold 15px, `#1a5276`, top center):** "A Write Through mmap Is Not on Disk Yet".
- **Row 1 (y=95), label 12px `#444` at x=20:** "no msync"; blue `#2a78d6` rounded box at x=110 labeled "store: order 500,000 → refunded" (12px), 3px arrow to a yellow `#c98500` box at x=330 labeled "dirty page in RAM", 3px arrow to a red `#e74c3c` box at x=530 labeled "crash — disk still says paid" with bold 12px red "✗ write lost" beneath it.
- **Row 2 (y=205), label:** "with msync"; blue box at x=110 "store: order 500,000 → refunded", 3px arrow to a green `#008300` box at x=330 labeled "msync flushes the page", 3px arrow to a green box at x=530 labeled "disk says refunded" with bold 12px green "✓ durable".
- **Box style:** 150–190px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(201,133,0,0.12)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=270):** "the store is instant; durability is a separate step".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all values are the hardcoded figures above (no randomness); the 8 GB / 125 million × 64-byte orders file, the 10-lookup 40 KB total, and the three memory footprints are invented and labeled illustrative/schematic; the lookup arithmetic is exact (500,000 × 64 = 32,000,000; 32,000,000 = 4096 × 7812 + 2048; 8,000,000,000 ÷ 4096 = 1,953,125 pages, so 1,953,124 pages remain untouched).
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
