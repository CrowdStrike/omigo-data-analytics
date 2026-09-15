# pandas (& Polars)

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** pandas (& Polars)

**Subtitle:** A DataFrame is a table with named columns and labeled rows that you transform whole, not cell by cell — pandas made it every data scientist's first tool, and Polars is its faster Arrow-native challenger

## The Orders Table Becomes a DataFrame

**Tags:** `core idea` (blue), `DataFrame` (green), `pandas` (orange)

- **The table** — an online shop's orders: 12 rows, three columns: order_id, region, revenue
- **The DataFrame** — pandas holds that table in memory with named columns and a row index
- **One dtype per column** — order_id is integers, region is text, revenue is floats
- **Whole-column verbs** — you say "sum revenue", never "loop over rows and add cell by cell"
- **The origin** — Wes McKinney started pandas in 2008; the name comes from "panel data"

*Example (italic):* `df["revenue"].sum()` adds all 12 order revenues in one line — 1500 total — with no loop written by you.

**Key point:** A DataFrame is a labeled in-memory table; you operate on named columns as whole units, and the library handles the row-by-row work for you.

### Visualization (canvas `c1`, 720×300)

Drawn table graphic of the orders DataFrame: header row plus six data rows and an ellipsis row, with callout labels naming the index, the columns, and the one-dtype rule.

- **Title (bold 15px, `#1a5276`, top center):** "A DataFrame: the Orders Table with Labels on Everything".
- **Table geometry:** header row at y=62, row height 26, seven body rows below (six data + one ellipsis); columns: index (x=70, width 50), order_id (x=120, width 110), region (x=230, width 130), revenue (x=360, width 110); 1px `#e5e9ef` cell borders.
- **Header row:** fill `rgba(26,82,118,0.12)`, bold 12px `#1a5276` text "order_id", "region", "revenue"; index header cell blank.
- **Body rows (12px `#2c3e50`, index cells 12px `#6b7280`):** rows `0..5` = `[1001, "North", 120]`, `[1002, "West", 210]`, `[1003, "South", 90]`, `[1004, "North", 80]`, `[1005, "West", 140]`, `[1006, "South", 60]`; ellipsis row shows "…", "…", "…", "… 6 more rows".
- **Callouts (bold 12px, right side x=500):** blue `#2a78d6` "named columns, one dtype each" with a thin arrow to the header at y=75; green `#008300` "row index (labels)" with arrow to the index column at y=130; violet `#4a3aa7` "you address columns by name: df['revenue']" at y=215.
- **Caption (12px `#444`, bottom right):** "12-row shop orders table, values illustrative".

## Splitting Revenue by Region

**Tags:** `worked example` (blue), `groupby` (green)

- **The question** — total revenue per region across the 12 orders
- **Split** — groupby("region") partitions the rows: North gets 4 rows, South 3, West 5
- **Apply** — sum revenue inside each group: North 120+80+200+100, South 90+60+150, West 210+140+50+180+120
- **Combine** — the three sums come back as one small table: North 500, South 300, West 700
- **Hand-check** — 500 + 300 + 700 = 1500, matching the whole column's sum

*Example (italic):* `df.groupby("region")["revenue"].sum()` returns exactly three rows — North 500, South 300, West 700 — from the 12-row table.

**Key point:** Split-apply-combine is the DataFrame's signature move: partition rows by a key, apply one function per group, and glue the results into a new table.

### Visualization (canvas `c2`, 720×300)

Three-stage flow diagram: the 12-row table on the left splits into three group boxes in the middle (values listed), arrows labeled "sum" lead to a result table on the right.

- **Title (bold 15px, `#1a5276`, top center):** "groupby('region'): Split 12 Rows, Sum Each Group, Combine to 3".
- **Left box (x=25, y=70, 130×170):** rounded 8px, fill `rgba(26,82,118,0.10)`, bold 12px `#1a5276` header "orders (12 rows)", 11px `#6b7280` note "region, revenue" below it.
- **Split arrows:** three 2px `#6b7280` arrows fanning from the left box's right edge to the three group boxes.
- **Group boxes (x=210, width 200, height 48, at y=62 / y=126 / y=190):** rounded 8px; North fill `rgba(42,120,214,0.15)` with 12px text "North: 120, 80, 200, 100"; South fill `rgba(25,158,112,0.15)` with "South: 90, 60, 150"; West fill `rgba(217,89,38,0.15)` with "West: 210, 140, 50, 180, 120"; bold 12px region name colored `#2a78d6` / `#199e70` / `#d95926`.
- **Apply arrows:** 2px `#6b7280` arrow from each group box to the result box, each with an 11px `#6b7280` label "sum".
- **Result box (x=490, y=90, 190×120):** rounded 8px, fill `rgba(0,131,0,0.10)`, bold 12px `#008300` header "revenue by region"; three 13px `#2c3e50` lines "North 500", "South 300", "West 700".
- **Annotation (bold 12px violet `#4a3aa7`, centered near y=275):** "500 + 300 + 700 = 1500 — every order counted exactly once".

## Eager pandas, Lazy Polars

**Tags:** `where it's used` (blue), `lazy execution` (green), `Polars` (orange)

- **Eager pandas** — every line runs immediately and materializes a full intermediate copy in memory
- **Single core** — classic pandas runs one thread; the pain shows up as tables reach millions of rows
- **Lazy Polars** — Polars (Rust, Apache Arrow, 2020) records a query plan and runs nothing until collect()
- **The optimizer** — the plan is rewritten first: read only region and revenue, push the filter into the scan
- **The payoff** — on a 100M-row orders file: pandas ~38 s and ~15 GB vs Polars ~4 s and ~2 GB (illustrative)

*Example (italic):* The same "revenue over 100, grouped by region" query is ~9× faster in lazy Polars because it never loads the six unused columns (illustrative).

**Key point:** pandas executes step by step with full copies; Polars builds the whole query first, optimizes it, and runs it multi-threaded — same DataFrame idea, different execution model.

### Visualization (canvas `c3`, 720×300)

Two-row pipeline diagram on the same query: eager pandas materializing every step vs lazy Polars optimizing the plan before one execution.

- **Title (bold 15px, `#1a5276`, top center):** "Same Query, Two Engines: Eager Step-by-Step vs Lazy Plan-then-Run".
- **Row 1 (boxes centered on y=95), label 12px `#444` at x=20:** "pandas (eager)"; three blue `#2a78d6` rounded boxes at x=140 / x=330 / x=520, each 165×44, fill `rgba(42,120,214,0.15)`, 12px text "read all 8 cols (15 GB)", "filter >100 (full copy)", "groupby sum (full copy)"; 3px `#6b7280` arrows between them; bold 12px red `#e74c3c` annotation "3 materialized tables, 1 core — 38 s" at x=140, y=145.
- **Row 2 (boxes centered on y=210), label:** "Polars (lazy)"; violet `#4a3aa7` rounded box at x=140, 165×44, fill `rgba(74,58,167,0.12)`, text "build plan (runs nothing)"; green `#008300` box at x=330, 165×44, fill `rgba(0,131,0,0.12)`, text "optimize: 2 cols, filter in scan"; green box at x=520, 165×44, text "collect() on 8 cores"; 3px `#6b7280` arrows; bold 12px green annotation "one pass, 2 GB — 4 s" at x=520, y=260.
- **Box style:** 8px radius, 12px `#2c3e50` text, 1.5px borders in the box's line color.
- **Caption (12px `#444`, bottom right):** "times, sizes, and core counts illustrative — 100M-row example".

## Looping Over Rows Defeats the Point

**Tags:** `common mistake` (red), `vectorization` (orange)

- **The habit** — newcomers write a Python for-loop over rows to add up revenue per region
- **The cost** — each row crossing the Python interpreter is slow; the loop redoes what groupby does in C
- **The scale** — on 10M orders (illustrative): row loop ~120 s, pandas groupby ~0.9 s, Polars ~0.3 s
- **The tell** — code using iterrows() or apply() row-by-row is usually a groupby or column op in disguise
- **Polars twist** — calling collect() after every single step throws away the lazy optimizer's whole advantage

*Example (italic):* The 12-row example hides it, but at 10M rows the row loop takes ~120 s while `groupby` gives the same three sums in under a second (illustrative).

**Common mistake:** Treating a DataFrame like a list of rows. The speed lives in whole-column operations — write the loop and you get the answer, but over a hundred times slower.

### Visualization (canvas `c4`, 720×300)

Horizontal bar chart: runtime of the same revenue-per-region computation on 10M rows, done three ways.

- **Title (bold 15px, `#1a5276`, top center):** "Same Answer, Three Speeds: 10M Rows Grouped by Region".
- **Axis:** vertical 2px `#999` baseline at x=230, bars extend right, max width 440; bar widths are hardcoded schematic pixels, not a linear scale.
- **Rows (bars 18px tall, centered on y = 90, 155, 220, each with a right-aligned 12px `#444` label ending at x=220):**
  - "Python row loop — 120 s": red `#e74c3c` bar width 440, bold 12px red label "~130× slower" at the bar end
  - "pandas groupby — 0.9 s": blue `#2a78d6` bar width 70, 11px `#444` width label "0.9 s" at the bar end
  - "Polars lazy — 0.3 s": green `#008300` bar width 30, 11px `#444` width label "0.3 s" at the bar end
- **Bar style:** solid fills, 4px corner radius optional flat rectangles acceptable.
- **Annotation (bold 13px magenta `#d55181`, right side near y=260):** "the answer is identical — North 500-scale sums, just 130× later".
- **Caption (12px `#444`, bottom right):** "runtimes illustrative, bar widths schematic".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all values are the hardcoded numbers above (no randomness); the 12-order table and its group sums are invented but internally exact (North 120+80+200+100=500, South 90+60+150=300, West 210+140+50+180+120=700, total 1500); all runtimes, memory sizes, and speedup factors (38 s / 4 s, 15 GB / 2 GB, 120 s / 0.9 s / 0.3 s) are invented and labeled illustrative; pandas 2008 / Wes McKinney and Polars Rust / Arrow / 2020 are documented facts.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
