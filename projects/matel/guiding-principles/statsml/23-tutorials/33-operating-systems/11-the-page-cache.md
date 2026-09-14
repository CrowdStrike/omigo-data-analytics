# The Page Cache

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** The Page Cache

**Subtitle:** The OS keeps recently read file pages in spare RAM — which is why the second read of the same file feels instant

## The Second Read of the Orders File

**Tags:** `core idea` (blue), `page cache` (green), `RAM vs disk` (orange)

- **The file** — an analyst opens a coffee shop's 2 GB orders file to total last month's sales
- **First read** — the script grinds for 20.5 seconds while the disk hands over the data
- **The rerun** — she fixes a typo, runs the exact same script again, and it finishes in 0.2 seconds
- **The secret** — the OS quietly kept the file's pages in unused RAM after the first read
- **Second read** — the data comes straight from that RAM copy; the disk is never asked

*Example (italic):* Same script, same 2 GB file, same laptop — 20.5 seconds the first time, 0.2 seconds the second, and she changed nothing about the code.

**Key point:** The page cache is the OS using otherwise-idle RAM to keep copies of recently read file pages, so repeat reads skip the disk entirely.

### Visualization (canvas `c1`, 720×300)

Two-row flow diagram: the first read's path (disk → page cache → script, slow) vs the second read's path (page cache → script, instant).

- **Title (bold 15px, `#1a5276`, top center):** "First Read Fills the Cache, Second Read Skips the Disk".
- **Row 1 (y=95), label 12px `#444` at x=20:** "first read — 20.5 s"; blue `#2a78d6` rounded box at x=170 labeled "disk (orders file)" (12px), 3px arrow to an aqua `#199e70` box at x=370 labeled "page cache (RAM)", 3px arrow to a violet `#4a3aa7` box at x=570 labeled "script".
- **Row 2 (y=205), label:** "second read — 0.2 s"; aqua box at x=370 labeled "page cache (RAM)", 3px arrow to a violet box at x=570 labeled "script"; a mute `#6b7280` dashed (dash 4/3) box outline at x=170 labeled "disk — never asked" in 12px `#6b7280`.
- **Box style:** 150–170px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(25,158,112,0.15)` / `rgba(74,58,167,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px green `#008300`, centered near y=270):** "the OS kept a copy — the second read never touches the disk".
- **Caption (12px `#444`, bottom right):** "timings illustrative".

## 20.5 Seconds Cold, 0.2 Seconds Warm

**Tags:** `worked example` (blue), `hand math` (green)

- **The sizes** — the orders file is 2,048 MB; the disk streams about 100 MB/s; RAM about 10,240 MB/s
- **Cold read** — 2,048 MB ÷ 100 MB/s = 20.5 seconds when every byte must come off the disk
- **Warm read** — 2,048 MB ÷ 10,240 MB/s = 0.2 seconds when every byte is already cached in RAM
- **The pages** — the cache works in 4 KB pages, so the file occupies 2,048 MB ÷ 4 KB = 524,288 pages
- **The ratio** — 20.5 s ÷ 0.2 s ≈ 100× faster, from nothing but the OS remembering the file

*Example (italic):* You can redo it on paper: 2,048 ÷ 100 = 20.48 ≈ 20.5 s cold, and 2,048 ÷ 10,240 = 0.2 s warm — a ~100× speedup.

**Key point:** The gap is just disk speed vs RAM speed: the same 2,048 MB takes 20.5 s at 100 MB/s and 0.2 s at 10,240 MB/s.

### Visualization (canvas `c2`, 720×300)

Line chart of cumulative MB delivered over time: the cold read crawls to 2,048 MB in 20.5 s while the warm read is a near-vertical line finishing at 0.2 s.

- **Title (bold 15px, `#1a5276`, top center):** "Same 2,048 MB: 20.5 s From Disk, 0.2 s From Cache".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = seconds 0 to 21 with 12px `#444` tick labels at 0/5/10/15/20; y = MB delivered 0 to 2,048, gridlines `#e5e9ef` at 512/1024/1536, 12px `#444` labels.
- **Cold line:** blue `#2a78d6` 3px line through seconds `[0, 5, 10, 15, 20.5]`, MB `[0, 500, 1000, 1500, 2048]`, bold 12px blue label "cold: from disk" near (13 s, 1150 MB).
- **Warm line:** green `#008300` 3px line through seconds `[0, 0.2]`, MB `[0, 2048]` — nearly vertical at the left edge, bold 12px green label "warm: from cache, 0.2 s" near (1.5 s, 1950 MB).
- **Annotation (bold 13px violet `#4a3aa7`, near x=8 s, y=90):** "~100× faster — same file, same code".
- **Caption (12px `#444`, bottom right):** "100 MB/s disk and 10,240 MB/s RAM illustrative; division exact".

## Why Your Benchmark Lies the Second Time

**Tags:** `where it's used` (blue), `benchmarking` (orange), `data science` (green)

- **The habit** — a data scientist times a load script five times and reports the typical run
- **The runs** — the timings come back 21.0 s, then 0.9 s, 0.9 s, 1.0 s, 0.9 s
- **The trap** — runs 2–5 are warm-cache runs; only run 1 shows what a fresh machine will do
- **Production pain** — a nightly job on a rebooted server always runs cold and takes the 21 s path
- **Honest timing** — report cold and warm separately, or flush the cache between runs to measure cold

*Example (italic):* Reporting "about 0.9 s" from runs 2–5 hides the 21.0 s cold start the scheduler will actually see at 3am.

**Key point:** Any timing you run twice on the same data is measuring the page cache the second time — decide whether cold or warm is the number you actually need.

### Visualization (canvas `c3`, 720×300)

Bar chart of the five timed runs: run 1 towers at 21.0 s, runs 2–5 sit near 1 s, with a bracket marking the warm runs.

- **Title (bold 15px, `#1a5276`, top center):** "Five Timings of the Same Script: One Cold, Four Warm".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = five bars labeled "run 1"–"run 5" (12px `#444`); y = seconds 0 to 22, gridlines `#e5e9ef` at 5/10/15/20 with 12px `#444` labels.
- **Bars:** widths 70px, centers evenly spaced; run 1 orange `#d95926` at height for 21.0 s; runs 2–5 green `#008300` fill `rgba(0,131,0,0.30)` with 2px green outline at heights for `[0.9, 0.9, 1.0, 0.9]` s; 12px `#2c3e50` value labels ("21.0 s", "0.9 s", "0.9 s", "1.0 s", "0.9 s") above each bar.
- **Bracket:** mute `#6b7280` horizontal bracket over runs 2–5 near y=200 with 12px `#6b7280` label "warm cache".
- **Annotation (bold 13px orange `#d95926`, near run 1's bar top, y=55):** "run 1 is what a rebooted server sees".
- **Caption (12px `#444`, bottom right):** "run timings illustrative".

## Full RAM Is Not a Memory Leak

**Tags:** `common mistake` (red), `free vs available` (orange)

- **The scare** — a 16 GB laptop shows 14 GB "used" and the analyst hunts for a memory leak
- **The split** — apps really hold 5 GB; the page cache holds 9 GB; only 2 GB is truly untouched
- **The rule** — cached pages are evictable: the OS hands them back the instant a program asks
- **Available** — the honest headroom is free + cache = 2 + 9 = 11 GB, not the scary 2 GB
- **The mistake** — "fixing" it by rebooting or dropping caches, which only buys 21-second cold reads

*Example (italic):* The monitoring dashboard screams 87% memory used, yet a new 8 GB process starts fine — 9 GB of that "used" was reclaimable cache.

**Common mistake:** Reading "free" memory as headroom. Free RAM is wasted RAM, so a healthy OS keeps it full of cache — the number that matters is available, not free.

### Visualization (canvas `c4`, 720×300)

Two stacked horizontal bars on the same 16 GB scale: what the naive "used" number shows vs what is actually available once cache is counted as reclaimable.

- **Title (bold 15px, `#1a5276`, top center):** "16 GB Machine: 'Used' Looks Scary, 'Available' Is Fine".
- **Scale:** bars start at x=170, full 16 GB spans 480px (30px per GB), 22px tall, 4px radius; row labels 12px `#444` at x=20; GB axis ticks at 0/4/8/12/16 along y=250 in 12px `#444`.
- **Row 1 (y=100), label "naive view":** violet `#4a3aa7` segment 150px labeled "apps 5 GB", blue `#2a78d6` segment `rgba(42,120,214,0.30)` 270px labeled "cache 9 GB", grid-gray `#e5e9ef` segment 60px labeled "free 2 GB"; bold 12px red `#e74c3c` note above the bar: "reads as 14/16 GB used (87%)".
- **Row 2 (y=190), label "honest view":** violet segment 150px labeled "apps 5 GB", green `#008300` segment `rgba(0,131,0,0.25)` 330px labeled "available 11 GB (free 2 + cache 9)".
- **Segment text:** 12px `#2c3e50`, centered in each segment (outside with leader if too narrow).
- **Annotation (bold 13px green `#008300`, near x=430, y=245):** "cache is given back the moment an app asks".
- **Caption (12px `#444`, bottom right):** "GB split illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the 100 MB/s disk, 10,240 MB/s RAM, run timings (21.0 / 0.9 / 0.9 / 1.0 / 0.9 s) and the 5/9/2 GB RAM split are invented and labeled illustrative; the divisions (2,048÷100 = 20.5 s, 2,048÷10,240 = 0.2 s, 2 + 9 = 11 GB available, 524,288 pages of 4 KB) are exact arithmetic on those invented inputs.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
