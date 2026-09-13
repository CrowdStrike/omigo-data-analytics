# How SQL Becomes Execution

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** How SQL Becomes Execution

**Subtitle:** You write what you want, never how — the database parses your sentence, plans the steps, and picks the cheapest way to run them, exactly like a compiler in disguise

## One Question, Four Steps Inside the Database

**Tags:** `core idea` (blue), `parse, plan, optimize` (green), `compiler in disguise` (orange)

- **The question** — a coffee shop owner asks: how many drinks over $4 did we sell, by drink type?
- **The SQL** — `SELECT drink, COUNT(*) FROM orders WHERE price > 4 GROUP BY drink` says what, never how
- **Parse** — the database checks the grammar and turns the one-line text into a tree of clauses
- **Plan** — the tree becomes concrete steps: read rows, filter on price, group by drink, count
- **Optimize** — several step orderings give the same answer; the cheapest guess wins
- **Execute** — only now does anything touch the 10,000-row orders table and return 5 drink rows

*Example (italic):* The owner types one sentence; the database quietly writes the program that answers it — the same job a compiler does with source code.

**Key point:** SQL is a description of the answer, not a recipe — parse → plan → optimize is the database compiling your description into a recipe it can run.

### Visualization (canvas `c1`, 720×300)

Horizontal pipeline diagram: the query sentence at the top feeding four stage boxes (parse, plan, optimize, execute) joined by arrows, with a one-line caption under each stage.

- **Title (bold 15px, `#1a5276`, top center):** "From One Line of SQL to a Running Program".
- **Query text (bold 12px monospace, `#1a5276`, centered at y=58):** `SELECT drink, COUNT(*) FROM orders WHERE price > 4 GROUP BY drink`.
- **Stage boxes (150×56, rounded 6px, top edge y=105) at x = 30, 205, 380, 555:** first three fill `rgba(42,120,214,0.12)` with 2px `#2a78d6` border, fourth fill `rgba(0,131,0,0.12)` with 2px `#008300` border. Two centered lines per box: bold 13px `#1a5276` stage name ("PARSE", "PLAN", "OPTIMIZE", "EXECUTE") and 12px `#2c3e50` gloss ("text → tree", "tree → steps", "pick cheapest", "run the steps").
- **Arrows:** three 2px `#6b7280` right-pointing arrows between consecutive boxes at y=133, arrowheads 6px.
- **Stage captions (11px `#6b7280`, centered under each box at y=182):** "grammar check", "read, filter, group, count", "index vs full scan", "returns 5 drink rows".
- **Annotation (bold 13px violet `#4a3aa7`, centered at y=235):** "the same pipeline a compiler uses — source in, machine steps out".
- **Caption (12px `#444`, bottom right):** "illustrative — the coffee shop's 10,000-row orders table, used throughout this page".

## Two Plans, One Answer: Scan or Index

**Tags:** `worked example` (blue), `cost model` (green)

- **The table** — this month's orders table holds 10,000 rows, and 2,000 of them have price > $4
- **Plan A: full scan** — read all 10,000 rows and keep the 2,000 matches: cost 10,000 row reads
- **Plan B: index** — jump straight to the 2,000 matches, but each jump needs a lookup: 2,000 × 2 = 4,000
- **The pick** — 4,000 is less than 10,000, so the optimizer chooses the index plan for this query
- **The crossover** — if more than 50% of rows matched, the "clever" index plan would cost more than scanning

*Example (italic):* For "price > $0.50" nearly every row matches, so the plain full scan beats the index — clever is not always cheaper.

**Key point:** The optimizer does arithmetic you can redo by hand: index cost 2 × 2,000 = 4,000 vs scan cost 10,000 — it picks by numbers, not habit.

### Visualization (canvas `c2`, 720×300)

Single-panel line chart of plan cost against the share of rows matching the filter: the full scan is a flat line, the index plan a rising line, with this query's 20% match marked and the 50% crossover dotted.

- **Title (bold 15px, `#1a5276`, top center):** "Two Plans, One Query: Cost vs Share of Matching Rows".
- **Axes:** origin x=70, baseline y=245, plot width 580, plot height 180; x = share of rows matching, 0% to 100%, 12px `#444` tick labels every 20%; y = cost in row reads, 0 to 20,000, light `#e5e9ef` gridlines at 5,000 / 10,000 / 15,000 with 12px `#444` labels "5k", "10k", "15k", "20k".
- **Scan line:** green `#008300` 3px horizontal line at cost 10,000 across the full width; 12px green label "full scan — always 10,000" above its left end.
- **Index line:** blue `#2a78d6` 3px line through hardcoded points at match share `[0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]` (%), cost `[0, 2000, 4000, 6000, 8000, 10000, 12000, 14000, 16000, 18000, 20000]`; 12px blue label "index — 2 × matched rows" near x=75%, below the line.
- **This-query marker:** vertical dashed orange `#d95926` (dash 4/3) line at 20% from the baseline up to the index line; orange 7px dot at (20%, 4,000).
- **Crossover marker:** mute `#6b7280` 6px dot at (50%, 10,000) with 12px `#6b7280` label "crossover at 50%" to its right.
- **Annotation (bold 13px orange `#d95926`, near x=25%, y=95):** two lines: "this query: 20% match" / "index 4,000 vs scan 10,000".
- **Caption (12px `#444`, bottom right):** "illustrative cost model: one index match counted as 2 row reads".

## When the Cost Guess Goes Stale

**Tags:** `where it's used` (blue), `statistics` (green), `slow queries` (orange)

- **The guess** — the optimizer never counts matches up front; it estimates from stored table statistics
- **Stale stats** — prices rose in June, and now 8,000 rows are over $4, but old stats still say 2,000
- **Wrong pick** — it chooses the index expecting cost 4,000; the true cost is 8,000 × 2 = 16,000
- **Worse than plain** — a full scan at 10,000 would have beaten the "optimized" plan by 6,000 reads
- **The fix** — refreshing statistics (ANALYZE) is how a slow query gets fast without changing one character of SQL
- **Index blockers** — wrapping the column in a function, like ROUND(price) > 4, hides the index entirely

*Example (italic):* The same query ran fine all spring and crawled in June — nothing in the SQL changed, only the data under it, and refreshed stats fixed it.

**Key point:** Query speed rests on the optimizer's estimates, so when a query slows down for no visible reason, suspect the guesses before the SQL.

### Visualization (canvas `c3`, 720×300)

Three vertical bars comparing the optimizer's estimated cost, the actual cost after the data changed, and the full-scan cost it passed over.

- **Title (bold 15px, `#1a5276`, top center):** "Stale Statistics: the Estimate vs What Actually Happened".
- **Axes:** baseline y=245 from x=70 to x=680; y = cost in row reads, 0 to 18,000, light `#e5e9ef` gridlines at 4,000 / 8,000 / 12,000 / 16,000 with 12px `#444` labels "4k", "8k", "12k", "16k".
- **Bars (120px wide, drawn up from the baseline), centers at x = 180, 380, 580:** estimate bar height for 4,000 in blue `rgba(42,120,214,0.35)` with 2px `#2a78d6` border; actual bar for 16,000 in orange `rgba(217,89,38,0.35)` with 2px `#d95926` border; full-scan bar for 10,000 in green `rgba(0,131,0,0.25)` with 2px `#008300` border.
- **Value labels (bold 13px, matching bar color, centered above each bar):** "4,000", "16,000", "10,000".
- **Bar labels (12px `#444`, two centered lines below the baseline):** "optimizer's estimate" / "(old stats: 2,000 match)"; "actual run" / "(really 8,000 match)"; "plain full scan" / "(never wrong by much)".
- **Annotation (bold 13px orange `#d95926`, centered near y=70):** "the 'optimized' plan cost 16,000 — 6,000 more than just scanning".
- **Caption (12px `#444`, bottom right):** "illustrative — same query, June data".

## SELECT Runs Last, Not First

**Tags:** `common mistake` (red), `execution order` (orange)

- **The trap** — SQL reads top-to-bottom, but the database does not run it top-to-bottom
- **Real order** — FROM runs first, then WHERE, then GROUP BY, and SELECT nearly last
- **Why it bites** — a nickname created in SELECT (an alias) does not exist yet when WHERE runs
- **The error** — `SELECT price*0.9 AS sale_price ... WHERE sale_price > 4` fails: WHERE ran first
- **Mental model** — read any query starting at FROM; the SELECT line is only the final packaging step

*Example (italic):* The owner filters on a discount column invented in SELECT and gets "unknown column" — the filter ran before the column was born.

**Common mistake:** Treating the written order as the running order — WHERE cannot see SELECT's aliases because filtering happens before the output columns are built.

### Visualization (canvas `c4`, 720×300)

Two-column mapping diagram: the query's four clauses in written order on the left, the four execution steps on the right, with crossing arrows showing how the database reorders them.

- **Title (bold 15px, `#1a5276`, top center):** "The Order You Write vs the Order It Runs".
- **Column headers (bold 13px `#1a5276`):** "written order" centered at x=170, y=68; "execution order" centered at x=550, y=68.
- **Left boxes (220×34, rounded 5px, fill `rgba(42,120,214,0.12)`, 1.5px `#2a78d6` border, 12px monospace `#2c3e50` text) at x=60, top edges y = 80, 124, 168, 212:** "SELECT drink, COUNT(*)", "FROM orders", "WHERE price > 4", "GROUP BY drink".
- **Right boxes (220×34, same style but fill `rgba(0,131,0,0.12)`, 1.5px `#008300` border) at x=440, same y positions:** "1. FROM orders", "2. WHERE price > 4", "3. GROUP BY drink", "4. SELECT drink, COUNT(*)".
- **Arrows (2px `#6b7280`, arrowheads 6px, from each left box's right edge to its matching right box's left edge):** FROM (y=141) → step 1 (y=97); WHERE (y=185) → step 2 (y=141); GROUP BY (y=229) → step 3 (y=185); the SELECT arrow (y=97 → step 4 at y=229) drawn 3px in orange `#d95926` so the longest reorder stands out.
- **Annotation (bold 13px orange `#d95926`, centered at y=283):** "SELECT is written first but runs last — WHERE can't see its aliases".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all box positions, line points, and bar heights are the hardcoded literal values above (no randomness); the c2 cost lines follow the stated model exactly (index = 2 × matched rows, scan = 10,000 flat), and the invented row counts carry "illustrative" captions.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
