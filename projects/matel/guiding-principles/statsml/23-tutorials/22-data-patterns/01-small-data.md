# Small Data

**Page type:** detail page (tutorial card-sections, two-column layout: text left 50%, canvas right 50%, one table row per section)
**HTML title tag:** Small Data

**Subtitle:** Data that fits in a spreadsheet or a laptop's memory — where the simplest tools are also the best ones

## Three Years of a Bakery, One Tiny File

**Tags:** core idea (blue), running example (green)

- **One row a day** — a bakery logs date, croissants sold, and revenue in a spreadsheet
- **Three years later** — that is 1,095 rows; the file is about 50 KB on disk
- **The laptop** — has 16 GB of memory; the whole bakery history uses 0.0003% of it
- **Everything at once** — the machine holds every row in memory with room to spare
- **That is small data** — the dataset fits comfortably; no special tools are needed

*Example:* The baker scrolls the whole three-year history top to bottom in about four seconds.

**Key point:** If the whole dataset fits in a spreadsheet or your laptop's memory, it is small data — and most datasets are.

### Visualization (canvas `c1`, 720×300)

Horizontal bar comparison on a log-scale axis: bakery file size vs laptop memory.

- **Title (bold 16px, `#1a5276`, top center):** "The Whole Bakery History vs the Laptop's Memory"
- **Axis:** horizontal log scale in bytes from 10³ (1 KB) to 10¹¹ (100 GB); tick labels 1 KB, 1 MB, 1 GB, 100 GB; axis caption "size (log scale)"; gray `#6b7280` axis line and labels. Bars start at x=175.
- **Bars (30px tall, solid fill, bold 13px labels — row label right-aligned left of bar in `#2c3e50`, size note in bar color right of bar end):**
  - "bakery file (1,095 rows)" — 50,000 bytes, note "50 KB", green `#008300`
  - "laptop memory (RAM)" — 16,000,000,000 bytes, note "16 GB", blue `#2a78d6`
- **Annotations:** bold 13px green above the bakery bar: "3 years of business = 0.0003% of memory"; 12px gray below: "(bar lengths on a log scale — the true gap is ~300,000x)".

## One Week of Croissants, Averaged by Hand

**Tags:** worked example (green), core idea (blue)

- **The rows** — Mon to Sun sales: 42, 51, 38, 45, 60, 88, 95 croissants
- **Add them** — 42+51+38+45+60+88+95 = 419 croissants for the week
- **Divide** — 419 ÷ 7 = 59.9 croissants per day on average
- **Look closer** — weekdays average 47.2, the weekend averages 91.5 — nearly 2x
- **No code needed** — one SUM and one AVERAGE cell; the pattern is visible by eye

*Example:* Seven numbers, one sum, one divide — and the "bake more on weekends" decision falls out.

**Key point:** With small data you can redo every calculation by hand — which also means you can catch every mistake.

### Visualization (canvas `c2`, 720×300)

Daily bar chart of one week's croissant sales with an average line and weekend highlight.

- **Title (bold 16px, `#1a5276`, top center):** "Croissants Sold, One Week (sum 419, average 59.9/day)"
- **Data:** days Mon–Sun with values `[42, 51, 38, 45, 60, 88, 95]`; y scale max 100; bars 58px wide; gray baseline axis. Padding: top 55, bottom 55, left 60, right 24.
- **Bar colors:** weekdays (Mon–Fri) blue `#2a78d6`; weekend (Sat, Sun) green `#008300`. Bold 12px value label above each bar in `#2c3e50`; 12px day label below.
- **Average line:** dashed violet `#4a3aa7` horizontal line (dash 6/4, width 2) at 59.9, labeled bold 12px violet "average 59.9".
- **Annotations:** bold 13px green near top right: "weekend ~2x weekdays (91.5 vs 47.2)"; 12px gray bottom center: "seven numbers you can add up yourself".

## Why Reaching for Heavy Tools Backfires

**Tags:** where it's used (blue), cost of tools (orange)

- **Most data is small** — surveys, experiments, store logs, lab results, monthly reports
- **Fast answers** — a spreadsheet answers the bakery question in about 30 seconds
- **Eyeball every row** — small data lets you spot the typo where 95 was entered as 950
- **Heavy tools cost days** — setting up a compute cluster for 1,095 rows wastes a week
- **Match tool to size** — the simplest tool that holds the data is usually the best one

*Example:* A new hire proposed a cluster for the bakery file; the owner answered it in Excel first.

**Key point:** Complexity is a cost you pay, not a badge — small data earns you the fastest, simplest tools.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart on a log-scale axis: time to first answer by tool.

- **Title (bold 16px, `#1a5276`, top center):** "Time to Answer \"What Is Our Daily Average?\" (1,095 rows)"
- **Axis:** horizontal log scale in seconds from 1 to 10⁶; bars start at x=210, rows 52px apart.
- **Bars (30px tall, solid fill; row label bold 13px right-aligned in `#2c3e50`, time note in bar color right of bar end):**
  - "spreadsheet" — 30 seconds, note "30 seconds", green `#008300`
  - "small script on a laptop" — 300 seconds, note "5 minutes", blue `#2a78d6`
  - "set up a compute cluster" — 432,000 seconds, note "~5 days", orange `#d95926`
- **Annotations:** bold 13px orange centered below the bars: "same answer, ~14,000x longer to get it"; 12px gray at the bottom: "illustrative times, log scale — the ordering is the point".

## The Confusion: "Lots of Rows" Is Not "Big"

**Tags:** common mistake (red), rule of thumb (blue)

- **Rows mislead** — a million rows sounds huge, but size is bytes, not row count
- **Do the math** — 1,000,000 orders at ~80 bytes each is 0.08 GB; RAM holds 16 GB
- **Even 10 million** — sensor readings at ~120 bytes each is 1.2 GB — still fits easily
- **The test** — compare the file size to your memory, not the row count to your intuition
- **Small is not trivial** — the value of data has nothing to do with its size

*Example:* The "huge" million-row orders file was 0.08 GB — half of one percent of the laptop's RAM.

**Key point:** Measure data in bytes against your machine's memory — millions of rows are usually still small data.

### Visualization (canvas `c4`, 720×300)

Vertical bar chart on a log-scale y-axis: dataset sizes vs the 16 GB RAM line.

- **Title (bold 16px, `#1a5276`, top center):** "Row Count vs Actual Size in Memory"
- **Axes:** y = size on log scale in GB from 10⁻⁵ (10 KB) to 10² (100 GB), rotated y-axis label "size (log scale)" in 12px gray; gray L-shaped axis frame. Padding: top 55, bottom 60, left 70, right 24. Bars 120px wide, evenly gapped.
- **RAM line:** dashed magenta `#d55181` horizontal line (dash 6/4, width 2) at 16 GB, labeled bold 13px magenta "laptop RAM: 16 GB".
- **Bars (solid fill; bold 12px size note above bar in `#2c3e50`; 12px label below baseline; row count in 12px gray beneath it):**
  - "bakery sales" — 1,095 rows, 0.00005 GB, note "50 KB", green `#008300`
  - "orders table" — 1,000,000 rows, 0.08 GB, note "0.08 GB", blue `#2a78d6`
  - "sensor readings" — 10,000,000 rows, 1.2 GB, note "1.2 GB", aqua `#199e70`
- **Annotation (bold 13px blue, top center):** "millions of rows, still far below the RAM line"

## Regeneration instructions

- **Template:** tutorials topic-page layout (social-graph reference style): `<h1>` (no index number), `.subtitle`, then four `.card-section` blocks each with an `<h2>` and a `table.layout` with one `<tr>`: left `<td class="text-col">` (50%) holding `.tags` pills, a `<ul>` of one-line bullets with `<b>` lead terms, an italic `.example` line, and a `.key-point` callout; right `<td class="viz-col">` (50%) holding one canvas.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with 2px `#2980b9` bottom border; section h2 1.3rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem. Bullets 0.92rem, `li b` in `#1a5276`. `.example` italic `#555` 0.9rem. `.key-point` background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem.
- **Tag pills:** `.tag` inline-block, 0.72rem bold, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Canvas:** intrinsic 720×300, CSS `width:100%`, 1px `#e0e0e0` border, radius 4px; scaled with `window.devicePixelRatio` via a shared `setup(id)` helper (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates).
- **Chart palette (`P` object):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- No nav bar, no back/home links, no cross-page links. In regenerated HTML any card links would use `.html` extensions.
