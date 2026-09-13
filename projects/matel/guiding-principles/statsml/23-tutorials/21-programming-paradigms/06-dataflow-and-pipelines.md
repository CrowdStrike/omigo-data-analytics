# Dataflow & Pipelines

**Page type:** detail page (tutorial card-sections: h2 + two-column table.layout, text left 50%, canvas/code right 50%)
**HTML title tag:** Dataflow & Pipelines

**Subtitle:** An orders report drawn as boxes and arrows — the data moves through fixed stages, and the row count on each arrow tells you the pipe is healthy

## An Orders Report, Drawn as Boxes and Arrows

**Tags:** `core idea` (blue pill), `running example` (green pill)

- **The job** — turn 10,000 raw orders into a 12-row city sales report
- **The boxes** — four stages: clean, join customers, aggregate, report
- **The arrows** — data flows left to right; each arrow carries a row count
- **Data moves** — orders pass through the stages like water through pipes
- **Code stands still** — each box is a small function that never changes mid-run

*Example:* 10,000 raw orders enter on the left; a 12-row city report comes out on the right.

**Key point:** A pipeline is a drawing you can point at — each box does one thing, and every arrow is a place you can count rows.

### Visualization (canvas `c1`, 720×300)

Left-to-right pipeline diagram with row counts labeled on the arrows and a side table feeding the join.

- **Title (bold 16px, `#1a5276`, top center):** "The Orders Pipeline — Row Counts Ride on the Arrows".
- **Five stage boxes** in a row at y=110 (96×56, last box 66 wide), two-line labels where noted:
  1. "raw / orders" — fill `rgba(42,120,214,0.15)`, stroke blue `#2a78d6`
  2. "clean" — fill `rgba(0,131,0,0.12)`, stroke green `#008300`
  3. "join / customers" — fill `rgba(74,58,167,0.12)`, stroke violet `#4a3aa7`
  4. "aggregate / by city" — fill `rgba(217,89,38,0.12)`, stroke orange `#d95926`
  5. "report" — fill `rgba(213,81,129,0.12)`, stroke magenta `#d55181`
- **Edges:** mute `#6b7280` arrows between boxes with bold ink 12px count labels above: "10,000 rows", "9,600 rows", "9,600 rows", "12".
- **Side table:** a yellow `#c98500` box below the join (96×44, fill `rgba(201,133,0,0.12)`) labeled bold "customers" / "500 rows", with a yellow upward arrow feeding into the join box.
- **Captions (left side, centered at x=190):** bold magenta 14px "data moves left to right — the code never moves"; mute 12px "each box: one small function".

## Follow 10,000 Rows Through the Pipe

**Tags:** `worked example` (green pill)

- **Enter** — 10,000 raw order rows from the orders file
- **Clean drops 400** — 250 rows missing a price, 150 with negative quantity
- **Join keeps 9,600** — each order looks up its customer's city in a 500-row table
- **Unmatched flagged** — 300 of the 9,600 orders find no customer; city becomes "unknown"
- **Aggregate collapses** — 9,600 order rows group into 12 totals (11 cities + "unknown")

*Example:* Sanity check by hand: 10,000 − 250 − 150 = 9,600 rows into the join.

**Key point:** Every count is predictable before the run — 10,000 in, 9,600 after clean, 12 out. Surprises live in the gaps.

Code payload (`pre.code` monospace block below the canvas, verbatim):

```
report = (
    load("orders.csv")        # 10,000 rows
    .pipe(clean)              #  9,600 rows (400 dropped)
    .pipe(join_customers)     #  9,600 rows (300 unmatched)
    .pipe(aggregate_by_city)  #     12 rows
)
```

### Visualization (canvas `c2`, 720×300)

Split panel: row-count funnel bars (left) and a two-bar breakdown of the 400 dropped rows (right).

- **Title (bold 16px, `#1a5276`):** "Row Counts at Every Stage".
- **Divider:** vertical dashed grid-gray `#e5e9ef` (4/3) at x=460.
- **Left funnel** (horizontal bars 26px tall from x=145, max width 240 scaled to 10,000; labels right-aligned, bold value labels to the right of each bar):
  - "raw orders" — 10,000, blue `#2a78d6`
  - "after clean" — 9,600, green `#008300`
  - "after join" — 9,600, violet `#4a3aa7`
  - "after aggregate" — 12 (minimum 5px), orange `#d95926`
  - Mute caption below: "join changes no counts here — it adds columns".
- **Right breakdown**, header bold ink 13px: "The 400 rows clean removed". Two vertical bars (60px wide, baseline y=210, scaled to 250 over 120px): "missing price" 250 orange `#d95926`, "negative qty" 150 magenta `#d55181`, bold value labels above each bar. Gray baseline. Captions: bold green "250 + 150 = 400 — the drop is explained"; mute "10,000 - 400 = 9,600".

## Each Stage Is Testable Alone

**Tags:** `where it's used` (blue pill), `best practice` (green pill)

- **Small tests** — feed `clean()` five handmade rows and check what survives
- **Counts as alarms** — yesterday the join emitted 9,600 rows; today it emits 19,200
- **Localized bug** — a doubled count after the join points at exactly one box
- **The usual culprit** — duplicate customer rows make the join produce two rows per order
- **Rerun one box** — fix the customers table, rerun from the join, not from scratch

*Example:* 19,200 = 9,600 × 2 — the count itself says "something matched twice".

**Key point:** Row counts on arrows are free assertions — check them every run and a broken stage names itself.

### Visualization (canvas `c3`, 720×300)

Paired horizontal bar chart: yesterday's vs today's row counts per stage, with the doubled join count in alarm red.

- **Title (bold 16px, `#1a5276`):** "The Day the Join Doubled — Counts Catch It".
- **Data:** stages "raw orders", "after clean", "after join", "after aggregate"; expected (yesterday) `[10,000, 9,600, 9,600, 12]`; today `[10,000, 9,600, 19,200, 12]`.
- **Bars:** per stage two thin bars (12px each, from x=150, max width 380 scaled to 19,200; minimum 4px): top bar yesterday in `rgba(42,120,214,0.45)` with blue 11px value label; bottom bar today in `rgba(0,131,0,0.5)` with green label when matching, or solid red `#e74c3c` with bold red label where today ≠ expected (the join row: "19,200").
- **Legend (top right):** blue swatch "yesterday", green swatch "today".
- **Bottom captions (centered):** bold red 14px "19,200 = 9,600 x 2: duplicate customer rows fanned out the join"; mute 12px "the broken box names itself — fix customers, rerun from the join".

## One Big Script Is Not a Pipeline

**Tags:** `common mistake` (red pill)

- **The confusion** — "my script already runs top to bottom, so it's a pipeline"
- **Tangled state** — a 400-line script where any line can touch any variable
- **No seams** — you cannot test the middle of it without running all of it
- **Pipeline seams** — named inputs and outputs between stages are the testing points
- **Same work** — both produce the report; only one can be checked box by box

*Example:* In the script, a typo at line 60 shows up as a wrong number at line 390 — with no arrow to count in between.

**Common confusion (key-point callout):** Running in order is not the point — the point is that each stage has a named input and output you can inspect.

### Visualization (canvas `c4`, 720×300)

Split panel: tangled variable graph inside one big script box vs a clean three-box chain with named, inspectable seams.

- **Title (bold 16px, `#1a5276`):** "One 400-Line Script vs Four Boxes".
- **Divider:** vertical dashed grid-gray `#e5e9ef` (4/3) at x=360.
- **Left panel (center x=180), header bold orange `#d95926`:** "the script: everything touches everything". One big box 240×160 (fill `#fdf6f0`, stroke orange) containing six white circle nodes (radius 14, orange stroke) labeled "df", "tmp", "df2", "x", "out", "df" — note "df" appears twice — connected by 10 semi-transparent orange `rgba(217,89,38,0.5)` tangled links. Captions: bold red `#e74c3c` "no seam to test at — run all 400 lines or nothing"; mute "variable \"df\" means 3 different things".
- **Right panel (center x=545), header bold green `#008300`:** "the pipeline: named seams between boxes". Three chained boxes 76×40 (fill `#f8f9fa`): "clean" (green), "join" (violet `#4a3aa7`), "aggregate" (orange), joined by mute arrows. Between boxes, dashed aqua `#199e70` seam markers dropping down to bold aqua 11px labels: "orders_clean" / "inspect here" and "orders_joined" / "inspect here". Captions: bold green "test any box alone with 5 handmade rows"; mute "same work as the script — but checkable box by box".

## Regeneration instructions

- **Layout:** tutorial detail page — h1 + `.subtitle`, then four `.card-section` blocks; each has an `<h2>` (1.3rem `#1a5276`, 2px solid `#2980b9` bottom border) and a `table.layout` with `td.text-col` (50%) and `td.viz-col` (50%). Text column: `.tags` pill row, `<ul>` of one-line bullets each opening with `<b>` (bold term in `#1a5276`), an italic `.example` line, and a `.key-point` callout (background `#f8f9fa`, left border 3px solid `#e74c3c`). Viz column: one canvas per section; section 2 also has a `pre.code` block under the canvas (background `#f8f9fa`, left border 3px solid `#1a5276`, monospace 0.78rem).
- **Tag pills:** 0.72rem bold, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`. Inline `code` in monospace on `#f4f6f8`.
- **Page CSS:** body system-ui sans, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` underline; `.subtitle` `#666` 0.95rem. Canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300 (read from width/height attributes), scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette (JS object P):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- In regenerated HTML, any card/page links use `.html` extensions (this page has none).
