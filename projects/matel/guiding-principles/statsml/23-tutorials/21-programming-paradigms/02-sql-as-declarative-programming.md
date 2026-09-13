# SQL as Declarative Programming

**Page type:** detail page (tutorial card-sections: h2 + two-column table.layout, text left 50%, canvas/code right 50%)
**HTML title tag:** SQL as Declarative Programming

**Subtitle:** You describe the rows you want — the database's planner invents the loops, picks the indexes, and chooses the join order for you

## You Never Say "Loop Over Rows"

**Tags:** `core idea` (blue pill), `running example` (green pill)

- **The ask** — top 5 customers by spend, again: one SQL statement, zero loops
- **What SQL states** — which table, which columns, what order, how many rows
- **What SQL omits** — how to visit rows, what to cache, when to sort, who works in parallel
- **The planner** — the database reads your query and writes a step-by-step plan itself
- **Its choices** — full scan or index, join order, hash vs sort, number of workers

*Example:* You said "top 5 by spend"; the planner decided to walk an index backwards and stop after 5 rows.

**Key point:** A SQL query is a description of the result, not a script — the loops still happen, but the planner writes them, not you.

Code payload (`.payload` monospace block below the canvas, verbatim):

```
SELECT name, spend
FROM customers
ORDER BY spend DESC
LIMIT 5;
-- no for-loop, no sort call, no index name:
-- the plan is the database's job
```

### Visualization (canvas `c1`, 720×300)

Flow diagram: query → planner → executor, with the planner's choices as dashed-linked bubbles.

- **Title (bold 15px, `#1a5276`, top center):** "From Description to Plan: What Happens to Your Query".
- **Query box** at x=30, y=96, 170×68 — fill `rgba(0,131,0,0.08)`, stroke green `#008300`; three monospace 11px lines centered: "SELECT name, spend" / "ORDER BY spend DESC" / "LIMIT 5"; bold green 12px label "your WHAT" above the box.
- **Arrow** (gray `#6b7280`) from x=200 to x=250 at mid-height y=130.
- **Planner box** at x=250, 180×68 — fill `#eef4fb`, stroke blue `#2a78d6`; bold blue 13px "query planner", mute 12px "writes the HOW".
- **Choice bubbles** (fill `rgba(74,58,167,0.08)`, stroke violet `#4a3aa7`, bold violet 12px text, 160×24, connected to the planner box by violet 3/3 dashed lines): "index or full scan?" centered at (340, 52) above; "how many workers?" at (340, 226) below; "which join order?" at (535, 60) upper right.
- **Arrow** (gray) from x=430 to x=480 at y=130.
- **Executor box** at x=480, 210×68 — fill `#fdf0e6`, stroke orange `#d95926`; bold orange 13px "executor runs the plan", dark 12px "returns exactly 5 rows".
- **Bottom caption (bold blue 13px, centered, y=278):** "you wrote zero loops — the planner wrote them all".

## One Query, Two Plans: 1,000,000 Rows vs 5

**Tags:** `worked example` (green pill)

- **The table** — `customers` now has 1,000,000 rows (illustrative timings)
- **Plan A, no index** — read all 1,000,000 rows, sort them all, keep 5: about 2.0 s
- **Plan B, with index** — the index already stores spend in order; read 5 entries: 0.02 s
- **Same query text** — not one character of the SQL changed between A and B
- **The ratio** — 2.0 s / 0.02 s = 100x faster, from reading 5 rows instead of 1,000,000

*Example:* An index on spend is like a phone book already sorted by spend — the top 5 sit right at the front.

**Key point:** The 100x came from the plan, not the query — declarative code lets the engine swap a terrible plan for a great one behind your back.

Code payload (`.payload` block below the canvas, verbatim):

```
CREATE INDEX idx_spend ON customers (spend);
-- the SELECT above is untouched;
-- the planner now picks Plan B on its own
```

### Visualization (canvas `c2`, 720×300)

Side-by-side plan comparison: two vertical step chains split by a dashed divider.

- **Title (bold 15px, `#1a5276`):** "Same Query, Two Plans (illustrative timings)".
- **Divider:** vertical dashed gray line `#bdc3c7` (dash 4/3) at x=360 from y=38 to bottom.
- **Left column (center x=185), title bold orange `#d95926`:** "Plan A — no index". Three stacked boxes 250×32 (fill `#f6f8fa`, last box `rgba(0,131,0,0.08)`, stroke orange) connected by downward orange arrows: "read all 1,000,000 rows" → "sort all 1,000,000 by spend" → "keep the first 5". Time label below, bold orange 14px: "about 2.0 s".
- **Right column (center x=540), title bold green `#008300`:** "Plan B — index on spend". Boxes: "open index: already sorted" → "read the top 5 entries" → "done — 999,995 rows untouched". Time label bold green: "about 0.02 s".
- **Bottom caption (bold magenta `#d55181` 13px, centered, y=288):** "100x faster — the SQL text is identical in both".

## Why This Matters When Your Query Is Slow

**Tags:** `where it's used` (blue pill), `rule of thumb` (green pill)

- **Debug the plan, not the text** — a slow query usually has a bad plan, not bad SQL
- **EXPLAIN shows it** — `EXPLAIN` prints the plan the planner chose; read it first
- **Joins too** — joining 3 tables has 6 possible orders; the planner picks the cheap one
- **Parallelism is free** — the planner can split a big scan over 4 workers; a loop can't
- **Stats drive choices** — the planner estimates row counts; stale stats mean bad plans

*Example:* The same dashboard query ran 100x faster on Monday because a DBA added an index on Sunday.

**Key point:** When SQL is slow, ask "what plan did it pick and why" — rewriting the query is the last resort, not the first.

### Visualization (canvas `c3`, 720×300)

Two before/after bar-pair charts split by a dashed divider at x=360.

- **Title (bold 15px, `#1a5276`):** "Sunday: CREATE INDEX. Monday: Same Query, New Plan".
- **Left chart (center x=185), header bold 13px ink:** "Query time (illustrative)". Two bars 75px wide on baseline y=232, height scaled to max 2.2 s over 148px: 2.0 s orange `#d95926`, 0.02 s green `#008300`; bold value labels "2 s" / "0.02 s" above bars, labels "before index" / "after index" below. Thin gray `#999` baseline from x=60 to x=325. Bold green caption at y=268: "100x — not one word of SQL changed".
- **Right chart (center x=540), header:** "Rows the engine had to read". Bars scaled to max 1,100,000: 1,000,000 orange, 5 green (minimum 4px); value labels "1,000,000" / "5", x-labels "before index" / "after index". Gray baseline x=415–680. Bold blue `#2a78d6` caption at y=268: "the work fell, so the time fell".

## The Common Confusion: SQL Is Not Read Top to Bottom

**Tags:** `common mistake` (red pill)

- **Written order** — SELECT, FROM, WHERE, ORDER BY, LIMIT is how you type it
- **Logical order** — FROM first, then WHERE, then SELECT, then ORDER BY, then LIMIT
- **Why it bites** — a column alias made in SELECT can't be used in WHERE: WHERE runs first
- **Not a script** — the planner may reorder further, as long as the result is identical
- **LIMIT is last** — rows are filtered and ordered before the cut to 5 happens

*Example:* `SELECT spend*1.1 AS bumped ... WHERE bumped > 100` fails — WHERE ran before the alias existed.

**Key point:** SQL text is a description, so it has no "line 1 runs first" — learn the logical order once and the alias errors stop surprising you.

### Visualization (canvas `c4`, 720×300)

Two-column mapping diagram with crossing colored arrows: written clause order vs logical execution order.

- **Title (bold 15px, `#1a5276`):** "The Order You Write vs the Order It Means".
- **Column headers (bold mute `#6b7280` 13px):** "written order" over the left column (x=110, box width 130), "logical order" over the right column (x=480).
- **Left column boxes** (130×28, fill `#f6f8fa`, monospace bold 12px, one per row starting y=62, row height 40), text and stroke colors: SELECT violet `#4a3aa7`, FROM blue `#2a78d6`, WHERE aqua `#199e70`, ORDER BY yellow `#c98500`, LIMIT magenta `#d55181`.
- **Right column boxes** (same style): "1. FROM" blue, "2. WHERE" aqua, "3. SELECT" violet, "4. ORDER BY" yellow, "5. LIMIT" magenta.
- **Arrows:** one per written clause, colored to match, from left box to its logical rank on the right — SELECT→row 3, FROM→row 1, WHERE→row 2, ORDER BY→row 4, LIMIT→row 5 (so the top three arrows cross).
- **Bottom caption (y=278):** bold magenta "SELECT runs 3rd, not 1st —" followed by plain dark text "so WHERE cannot see an alias SELECT makes".

## Regeneration instructions

- **Layout:** tutorial detail page — h1 + `.subtitle`, then four `.card-section` blocks; each has an `<h2>` (1.3rem `#1a5276`, 2px solid `#2980b9` bottom border) and a `table.layout` with `td.text-col` (50%) and `td.viz-col` (50%). Text column: `.tags` pill row, a `<ul>` of one-line bullets each opening with `<b>` (bold term in `#1a5276`), an italic `.example` line, and a `.key-point` callout (background `#f8f9fa`, left border 3px solid `#e74c3c`). Viz column: one canvas per section; sections 1 and 2 also have a `.payload` `<pre>` under the canvas (background `#f8f9fa`, left border 3px solid `#1a5276`, monospace 0.78em).
- **Tag pills:** 0.72rem bold, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`. Inline `code` in monospace on `#f4f6f8`.
- **Page CSS:** body system-ui sans, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` underline; `.subtitle` `#666` 0.95rem. Canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300, scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette (JS object P):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- In regenerated HTML, any card/page links use `.html` extensions (this page has none).
