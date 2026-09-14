# Virtual Memory

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Virtual Memory

**Subtitle:** The OS gives every process its own private map of memory, so every program is written as if it owns all the RAM — the hardware translates behind the scenes

## Every Program Thinks It Starts at Address Zero

**Tags:** `core idea` (blue), `private address space` (green), `the OS translates` (orange)

- **Two apps** — a photo editor and a music player both run on the same 8-frame stick of RAM
- **The illusion** — each app sees its own private addresses 0, 1, 2, 3... starting from zero
- **The map** — the OS keeps a page table per process: "your page 0 is really frame 2 of RAM"
- **No collisions** — both apps can use "address 0" at once because each maps to a different frame
- **The overflow** — a page nobody touched lately can even live on disk until it is needed again

*Example (italic):* The photo editor writes to its page 0 and lands in RAM frame 2; the music player writes to its page 0 and lands in frame 4 — neither ever sees the other's data.

**Key point:** Virtual memory is a per-process map from pretend addresses to real RAM frames — every program gets the same simple view (I start at zero and own everything) while the OS juggles the real hardware.

### Visualization (canvas `c1`, 720×300)

Mapping diagram: two small per-process address-space columns on the left, one shared physical RAM column of 8 frames on the right, arrows showing where each virtual page really lives.

- **Title (bold 15px, `#1a5276`, top center):** "Two Processes, One RAM: the Page Table Is the Map".
- **Left columns:** photo editor at x=70, music player at x=250; each a stack of 4 boxes (110px wide, 34px tall, starting y=80, 4px gap) labeled "page 0"–"page 3" in 12px `#2c3e50`; photo-editor boxes fill `rgba(42,120,214,0.20)` with 2px `#2a78d6` border, music-player boxes fill `rgba(0,131,0,0.18)` with 2px `#008300` border; bold 13px column labels above each stack.
- **Right column:** physical RAM at x=520, a stack of 8 boxes (130px wide, 22px tall, starting y=60, 3px gap) labeled "frame 0"–"frame 7" in 11px `#6b7280`, empty frames fill `#f4f6f8` with 1px `#e5e9ef` border.
- **Mappings (2px arrows, hardcoded):** photo editor pages `[0, 1, 2, 3]` map to frames `[2, 5, 0, 7]` (blue `#2a78d6` arrows, landing frames filled `rgba(42,120,214,0.20)`); music player pages `[0, 1, 2]` map to frames `[4, 1, 6]` (green `#008300` arrows, landing frames filled `rgba(0,131,0,0.18)`).
- **Disk box:** small gray rounded box at x=380, y=255 labeled "disk" (12px `#6b7280`); dashed 2px `#6b7280` arrow (dash 4/3) from music player page 3 to it, 11px `#6b7280` label "paged out".
- **Annotation (bold 13px violet `#4a3aa7`, near x=380, y=45):** "both apps use 'page 0' — different frames, no clash".
- **Caption (12px `#444`, bottom right):** "8 frames and page layout illustrative".

## Translating Address 9,300 by Hand

**Tags:** `worked example` (blue), `page table` (green)

- **Page size** — memory moves in 4,096-byte pages, so an address splits into (page number, offset)
- **The split** — virtual address 9,300 ÷ 4,096 = page 2, remainder 1,108 → offset 1,108 within the page
- **The lookup** — the photo editor's page table says virtual page 2 lives in physical frame 7
- **The rebuild** — physical address = 7 × 4,096 + 1,108 = 28,672 + 1,108 = 29,780
- **Offset untouched** — translation swaps only the page number; the offset 1,108 rides along unchanged

*Example (italic):* The app asks for byte 9,300; the hardware quietly reads byte 29,780 of real RAM — the app never learns the real number.

**Key point:** Every memory access is a two-step arithmetic trick: split the address into page + offset, replace the page number using the page table, keep the offset — 9,300 in, 29,780 out.

### Visualization (canvas `c2`, 720×300)

Left-to-right flow diagram of one translation: virtual address box splits into page/offset, the page table row rewrites the page number, boxes recombine into the physical address.

- **Title (bold 15px, `#1a5276`, top center):** "One Lookup: Virtual 9,300 Becomes Physical 29,780".
- **Stage 1 (x=40, y=110):** blue `#2a78d6` rounded box (150×44, fill `rgba(42,120,214,0.15)`) labeled "virtual addr 9,300" in 13px `#2c3e50`.
- **Stage 2 (x=250, two boxes):** split via two 2px `#6b7280` arrows into "page 2" box (110×40, y=70, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border) and "offset 1,108" box (110×40, y=160, fill `#f4f6f8`, 1px `#e5e9ef` border); 11px `#6b7280` math labels beside each: "9300 ÷ 4096" and "remainder".
- **Stage 3 (x=420, y=70):** green `#008300` rounded box (130×40, fill `rgba(0,131,0,0.12)`) labeled "page table: 2 → 7" in 12px, fed by an arrow from the page-2 box.
- **Stage 4 (x=590, y=110):** green box (150×44, fill `rgba(0,131,0,0.12)`, 2px `#008300` border) labeled "physical 29,780" in bold 13px, fed by arrows from the page-table box and the untouched offset box; 11px `#6b7280` math label "7×4096 + 1,108" beneath.
- **Offset path:** the offset box's arrow to stage 4 is dashed `#199e70` (dash 4/3) with 11px aqua `#199e70` label "offset passes through unchanged".
- **Annotation (bold 13px orange `#d95926`, centered near y=270):** "the app said 9,300 — the RAM heard 29,780".
- **Caption (12px `#444`, bottom right):** "4,096-byte pages; all arithmetic exact".

## Why a Big DataFrame "Fits" — Until It Doesn't

**Tags:** `where it's used` (blue), `paging` (green), `thrashing` (red)

- **The gift** — memory-mapped files let a 30 GB dataset open on a 16 GB laptop; pages load on demand
- **The scan** — while the working set fits in RAM, each pass over the data stays fast
- **The cliff** — past physical RAM the OS evicts pages you still need; every access becomes a disk trip
- **The numbers** — a pass at 14 GB takes 29 s; at 18 GB the same pass takes 210 s, at 22 GB, 540 s
- **The symptom** — the process isn't "broken", the disk light is solid: that slowdown is thrashing

*Example (italic):* A data scientist's 18 GB join runs 7× slower than the 14 GB version — not 1.3× slower — because 2 GB of pages keep bouncing between RAM and disk.

**Key point:** Virtual memory degrades gracefully until the working set exceeds physical RAM, then falls off a cliff — the 16 GB line on the chart is the real capacity of the machine, whatever the address space claims.

### Visualization (canvas `c3`, 720×300)

Line chart: time for one full pass over the dataset vs dataset size on a 16 GB-RAM machine, with a vertical marker at physical RAM and a cliff beyond it.

- **Title (bold 15px, `#1a5276`, top center):** "One Pass Over the Data: Fast Until the Working Set Outgrows RAM".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = dataset size 0 to 24 GB with 12px `#444` tick labels every 4 GB; y = seconds per pass 0 to 600, gridlines `#e5e9ef` at 150/300/450.
- **Pass-time line:** blue `#2a78d6` 3px line with 4px dots through dataset sizes `[2, 6, 10, 14, 16, 18, 22]` GB, seconds `[4, 12, 20, 29, 48, 210, 540]` — near-linear to 14 GB, cliff after 16 GB.
- **RAM marker:** vertical dashed `#6b7280` (dash 4/3) line at 16 GB, 12px `#6b7280` label "physical RAM = 16 GB" at its top.
- **Point labels:** 12px `#2c3e50` values "29 s" above the 14 GB point and "210 s" beside the 18 GB point.
- **Annotation (bold 13px red `#e74c3c`, near x=19 GB, y=90):** "past RAM, every page fault is a disk trip".
- **Caption (12px `#444`, bottom right):** "pass times illustrative".

## Virtual Size Is Not Memory Used

**Tags:** `common mistake` (red), `VIRT vs RES` (orange)

- **The confusion** — `top` shows the process at VIRT 12.4 GB and people cry memory leak
- **VIRT** — address space the process has *reserved*: mapped files, arenas, pages never touched
- **RES** — pages actually sitting in physical RAM right now: here it is only 1.3 GB
- **Cheap promises** — reserving pages costs almost nothing until a page is first written to
- **What to watch** — rising RES (plus swap activity) means pressure; a big VIRT alone means nothing

*Example (italic):* A worker that memory-maps a 10 GB file shows VIRT 12.4 GB, RES 1.3 GB, swap 0.3 GB — it is actually holding about a tenth of what the scary column says.

**Common mistake:** Reading VIRT as memory consumed. Virtual size counts promises, not occupancy — judge a process by resident pages and swap traffic, or you will "fix" leaks that never existed.

### Visualization (canvas `c4`, 720×300)

Horizontal bar chart for one process: the big VIRT bar vs the small RES and swap bars, on a shared GB scale.

- **Title (bold 15px, `#1a5276`, top center):** "Same Process, Three Numbers: Only One Is RAM".
- **Axis:** vertical 2px `#999` baseline at x=190, bars extend right, max width 460 = 12.4 GB scale; light `#e5e9ef` gridlines at 4 / 8 / 12 GB with 11px `#6b7280` labels along the bottom.
- **Rows (top to bottom at y = 85, 150, 215), each with a left-aligned 12px `#444` label at x=20:**
  - "VIRT — reserved 12.4 GB": blue bar fill `rgba(42,120,214,0.30)`, 2px `#2a78d6` border, width 460
  - "RES — in RAM 1.3 GB": solid green `#008300` bar, width 48
  - "swap — paged out 0.3 GB": solid orange `#d95926` bar, width 11
- **Bar style:** 26px tall, 11px `#2c3e50` GB value labels just past each bar's right end.
- **Overlay note:** dashed `#6b7280` bracket (dash 4/3) over the empty right portion of the VIRT bar with 11px `#6b7280` label "mostly never-touched mappings".
- **Annotation (bold 13px magenta `#d55181`, near x=420, y=150):** "the 'leak' was a promise, not a payment".
- **Caption (12px `#444`, bottom right):** "sizes illustrative; one mmap-heavy worker".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the address translation is exact arithmetic with 4,096-byte pages (9,300 → page 2, offset 1,108; frame 7 → 29,780); the frame mappings ([2,5,0,7] and [4,1,6] + one page on disk), pass times ([4,12,20,29,48,210,540] s at [2,6,10,14,16,18,22] GB on 16 GB RAM), and VIRT/RES/swap sizes (12.4 / 1.3 / 0.3 GB) are invented and labeled illustrative.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
