# Query Optimizers

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Query Optimizers

**Subtitle:** The SQL you write says what you want, not how to fetch it — the database's optimizer prices several routes to the same answer and quietly runs the cheapest one

## One Question, Two Ways to Answer It

**Tags:** `core idea` (blue), `what not how` (green), `cost estimates` (orange)

- **The question** — an online bakery's analyst asks its database: show Maya's orders over $50
- **Two routes** — it can read all 500,000 order rows, or jump via a customer index to Maya's 120
- **The optimizer** — a planner inside the database that prices each route before running anything
- **Cost guess** — full scan estimated at 2.0 s, index route at 0.003 s; it silently takes the cheap one
- **The rewrite** — the SQL you typed is a wish; the plan that actually runs may look nothing like it

*Example (italic):* Like a taxi ride: you give the address, the driver picks the streets — your query is the address, the optimizer is the driver.

**Key point:** A query declares what you want; the optimizer decides how to get it, comparing estimated costs and running only the cheapest plan.

### Visualization (canvas `c1`, 720×300)

Left-to-right flow diagram: the query box feeds an optimizer box, which branches to two candidate plan boxes with estimated costs; the cheap plan is highlighted as the one chosen.

- **Title (bold 15px, `#1a5276`, top center):** "One Question, Two Routes — the Optimizer Picks".
- **Query box:** rounded rect at x=25, y=115, 175×75, 2px ink `#1a5276` border, fill `rgba(26,82,118,0.06)`; 12px `#2c3e50` text lines centered: "orders WHERE", "customer = 'Maya'", "AND total > $50".
- **Optimizer box:** rounded rect at x=265, y=122, 135×60, 2px violet `#4a3aa7` border, fill `rgba(74,58,167,0.08)`; bold 13px violet label "optimizer"; 2px `#6b7280` arrow with arrowhead from the query box into it.
- **Plan A box (rejected):** rounded rect at x=470, y=55, 225×72, 2px `#6b7280` border; bold 12px `#6b7280` line "Plan A — read every row", 12px lines "500,000 rows", "est. 2.0 s"; 2px `#6b7280` arrow from optimizer to it.
- **Plan B box (chosen):** rounded rect at x=470, y=180, 225×72, 3px green `#008300` border, fill `rgba(0,131,0,0.08)`; bold 12px green line "Plan B — customer index", 12px lines "120 rows", "est. 0.003 s"; 3px green arrow from optimizer to it; bold 14px green check mark "✓ chosen" at its right edge.
- **Annotation (bold 12px green `#008300`, near x=250, y=235):** two lines: "same answer either way —" / "it takes the cheap route on its own".
- **Caption (12px `#444`, bottom right):** "costs illustrative — a bakery orders table".

## Counting the Rows Each Route Touches

**Tags:** `worked example` (blue), `selectivity` (green)

- **Two filters** — customer = 'Maya' keeps 120 of 500,000 rows; total > $50 keeps 1 row in 5
- **Picky first** — the optimizer starts with the pickier filter: index straight to Maya's 120 rows
- **Then the rest** — checking total > $50 on those 120 leaves 120 × 1/5 = 24 final rows
- **The dumb order** — total-first touches all 500,000 rows and still leaves 100,000 to sift for Maya
- **Same answer** — both orders return the identical 24 rows; only the work differs: 120 vs 500,000
- **Hand check** — 500,000 / 120 ≈ 4,000× fewer rows touched, and 120 × 1/5 = 24 either way

*Example (italic):* Filter order changed nothing about the answer and almost everything about the bill — 24 rows out either way, but 500,000 rows touched versus 120.

**Key point:** Selectivity sets the order: apply the filter that throws away the most rows first — 120 × 1/5 = 24 final rows, checkable by hand.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart: four bars showing rows touched at each step of the two filter orders, making the 4,000× gap between scan-first and index-first visible at a glance.

- **Title (bold 15px, `#1a5276`, top center):** "Rows Touched by Each Filter Order (same 24 answers out)".
- **Layout:** bars start at x=225 and extend right, max width 455 (so 500,000 rows → 455px; linear scale; any bar under 3px is drawn at 3px minimum so it stays visible); bar height 18px; step labels left-aligned 12px `#444` at x=20.
- **Group label (bold 12px `#d95926` at x=20, y=68):** "Plan A — total > $50 first (full scan)".
- **Plan A bars:** y=85 "step 1: check total on all rows" value 500,000, fill `rgba(217,89,38,0.55)`; y=120 "step 2: sift those for Maya" value 100,000, fill `rgba(217,89,38,0.35)`; bold 12px `#d95926` value labels "500,000" and "100,000" just past each bar end.
- **Group label (bold 12px `#008300` at x=20, y=168):** "Plan B — Maya first (customer index)".
- **Plan B bars:** y=185 "step 1: index jump to Maya's rows" value 120, fill `rgba(0,131,0,0.55)`; y=220 "step 2: check total on those" value 120, fill `rgba(0,131,0,0.35)`; bold 12px green value labels "120" and "120" just past each bar end (drawn at the 3px minimum width).
- **Annotation (bold 13px ink `#1a5276`, near x=400, y=160):** "same 24 rows out — about 4,000× fewer rows touched".
- **Caption (12px `#444`, bottom right):** "illustrative — 500,000-row orders table".

## Join Order and Why Your Query Isn't Run as Written

**Tags:** `where it's used` (blue), `join order` (green), `EXPLAIN` (orange)

- **Join order** — "customers with a refunded order": start from 2,000 refunds, not 500,000 orders
- **The savings** — small-first builds 4,000 intermediate rows; big-first builds 502,000 — 125× more
- **Everywhere** — Spark, warehouses, and dataframe engines all reorder work; SQL is not run top-down
- **EXPLAIN** — every database will print the plan it chose; read it before blaming your query
- **Fast then slow** — the same query can switch plans overnight as tables grow or stats refresh

*Example (italic):* An analyst spent a week reordering her JOIN clauses; EXPLAIN showed the optimizer had been quietly using the same plan the whole time.

**Key point:** The optimizer reorders joins to keep intermediate results small — 4,000 rows built instead of 502,000 for the identical three-table answer.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart in two groups: intermediate rows built at each join step under the big-table-first order versus the small-table-first order, same three tables and same final answer.

- **Title (bold 15px, `#1a5276`, top center):** "Two Join Orders, Same Answer: Rows Built Along the Way".
- **Layout:** bars start at x=255, max width 425 (500,000 rows → 425px, linear; bars under 3px drawn at 3px minimum); bar height 18px; step labels left-aligned 12px `#444` at x=20.
- **Group label (bold 12px `#d95926` at x=20, y=66):** "big first — total built: 502,000".
- **Big-first bars:** y=83 "step 1: orders ⋈ customers" value 500,000, fill `rgba(217,89,38,0.55)`; y=118 "step 2: result ⋈ refunds" value 2,000, fill `rgba(217,89,38,0.35)`; bold 12px `#d95926` value labels "500,000" and "2,000" past the bar ends.
- **Group label (bold 12px `#008300` at x=20, y=170):** "small first — total built: 4,000".
- **Small-first bars:** y=187 "step 1: refunds ⋈ orders" value 2,000, fill `rgba(0,131,0,0.55)`; y=222 "step 2: result ⋈ customers" value 2,000, fill `rgba(0,131,0,0.35)`; bold 12px green value labels "2,000" and "2,000".
- **Annotation (bold 13px green `#008300`, near x=400, y=155):** "start small: 125× less work, identical answer".
- **Caption (12px `#444`, bottom right):** "illustrative — orders 500,000, customers 10,000, refunds 2,000".

## It Guesses From Statistics, Not From Your Data

**Tags:** `common mistake` (red), `stale statistics` (orange)

- **The map** — the optimizer never reads your rows to plan; it reads statistics collected earlier
- **Stale stats** — stats gathered in July say about 100 December orders; by January there are 60,000
- **Wrong route** — expecting 100 rows it picks the index hop; 60,000 hops take 9.0 s, one scan 1.2 s
- **The fix** — refresh statistics (ANALYZE) after big loads; the optimizer is only as good as its map
- **Not magic** — that was a bad estimate, not a bad database; the plan fit the world it believed in

*Example (italic):* After the holiday data load, one ANALYZE turned the 9-second report back into a 1.2-second one — without changing a character of the query.

**Common mistake:** Assuming the optimizer sees your data. It sees statistics about your data — feed it a stale summary and it will confidently pick the slow plan.

### Visualization (canvas `c4`, 720×300)

Two-panel bar chart: left panel compares the optimizer's estimated row count against reality after a big load; right panel shows the runtime of the plan it picked versus the plan it should have picked.

- **Title (bold 15px, `#1a5276`, top center):** "Stale Statistics: a Confident Wrong Turn".
- **Left panel (x 60–330, baseline y=245, panel subtitle 12px `#444` centered below title):** "December rows: estimate vs actual"; two vertical bars 70px wide — "estimated" at x=95, value 100, blue `#2a78d6` fill `rgba(42,120,214,0.55)`; "actual" at x=215, value 60,000, orange fill `rgba(217,89,38,0.55)`; heights linear with 60,000 → 170px (the 100-row bar lands under 1px, drawn at 3px minimum); bold 12px value labels "100" (blue) and "60,000" (orange) above each bar; 12px `#444` name labels below the baseline.
- **Right panel (x 400–680, baseline y=245, panel subtitle 12px `#444`):** "runtime of the chosen plan"; two vertical bars 70px wide — "picked: index hops" at x=435, value 9.0 s, orange `#d95926` fill `rgba(217,89,38,0.55)`, height 170px; "best: one scan" at x=560, value 1.2 s, green fill `rgba(0,131,0,0.55)`, height 23px (linear, 9.0 s → 170px); bold 12px value labels "9.0 s" and "1.2 s" above the bars; 12px `#444` name labels below the baseline.
- **Divider:** 1px `#e5e9ef` vertical line at x=365 from y=55 to y=260.
- **Annotation (bold 13px violet `#4a3aa7`, centered near x=365, y=285):** "old map, wrong route — refresh stats after big loads".
- **Caption (12px `#444`, top right):** "illustrative — stats last refreshed in July".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all bar values, box labels, and costs are the hardcoded literals above (no randomness); row-count bars use linear scale with a 3px minimum bar width/height so tiny values stay visible; the text's numbers (500,000 / 120 / 24 / 100,000 / 2,000 / 4,000 / 502,000 / 100 / 60,000 / 9.0 s / 1.2 s) must match the charts exactly.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
