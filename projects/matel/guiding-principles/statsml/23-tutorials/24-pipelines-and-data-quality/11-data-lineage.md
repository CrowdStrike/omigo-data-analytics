# Data Lineage

**Page type:** detail page (tutorial card-sections: one h2 per section, two-column table layout — text left 50%, canvas right 50%)
**HTML title tag:** Data Lineage

**Subtitle:** Every number on a dashboard has ancestors — the tables, jobs, and transformations that fed it. Lineage is the recorded family tree, so you can trace a wrong number back and know what breaks when a source changes.

## Tracing the Revenue Tile Back to Its Parents

Tags: `core idea` (blue), `running example` (green), `provenance` (blue)

- **The tile** — the CEO dashboard says yesterday's revenue was $1.117M; finance expected ~$1.145M
- **One step up** — the tile reads one table: daily_revenue, written by the job build_revenue
- **Two steps up** — that job reads three sources: orders, fx_rates, and refunds
- **The culprit** — orders and refunds updated at 04:10; fx_rates still holds Monday's euro rate
- **The definition** — that upstream chain, recorded so it can be walked, is data lineage

*Example:* Without the map: a morning of guessing. With it: three hops from wrong tile to stale table in minutes.

**Key point:** A number is only as trustworthy as its least trustworthy ancestor — and you can't check ancestors you can't name.

### Visualization (canvas `c1`, 720×300)

Flow diagram: the lineage graph behind the revenue tile with the stale source highlighted.

- **Title (bold 16px, `#1a5276`, top center):** "The Family Tree of One Dashboard Number".
- **Nodes (rounded label boxes 150×44 unless noted, light fill `#f8f9fa`, colored 2px border, bold label + gray sublabel):**
  - Left column of three sources: "orders / updated 04:10" (blue `#2a78d6`), "fx_rates / updated Mon (!)" (red `#e74c3c`, fill `rgba(231,76,60,0.08)`), "refunds / updated 04:10" (blue).
  - Middle: "build_revenue / nightly job" (violet `#4a3aa7`).
  - Right: "daily_revenue / table" (aqua `#199e70`, 170 wide).
  - Below right: "revenue tile: $1.117M / CEO dashboard" (orange `#d95926`, fill `#fdf6ee`, 170×50).
- **Arrows:** three source→job arrows (blue, blue, blue; the fx_rates arrow is red and thicker, width 3), job→table arrow (violet), table→tile arrow (aqua, vertical).
- **Trace-back path:** dashed red polyline (dash 5/4, width 1.5) from the tile back left and up to fx_rates; bold red 13px label: "trace back: tile → table → job → the one stale parent".
- **Caption (gray 12px, bottom center):** "three hops, walked in minutes because every edge was recorded".

## Recomputing the Tile by Hand: Where $28k Went Missing

Tags: `worked example` (green), `trace it yourself` (orange)

- **The recipe** — revenue = US sales + euro sales × rate + UK sales × rate − refunds
- **The inputs** — $600k US, €400k euro-zone, £100k UK, $30k refunds (illustrative)
- **Correct rates** — EUR 1.12, GBP 1.27: 600 + 448 + 127 − 30 = $1,145k
- **Stale rate** — Monday's EUR 1.05 gives 400 × 1.05 = $420k: total drops to $1,117k
- **The match** — the gap is exactly 400 × (1.12 − 1.05) = $28k — the smoking gun is arithmetic

*Example:* The $28k shortfall equals one input's error precisely — that is how a trace gets confirmed, not just suspected.

**Key point:** Lineage tells you which inputs to check; recomputing the number from those inputs tells you which one is guilty.

### Visualization (canvas `c2`, 720×300)

Comparison table drawn on canvas: the recompute with correct rates vs the stale EUR rate, side by side.

- **Title (bold 16px, `#1a5276`, top center):** "Recomputing Revenue With Each Rate Set (all figures illustrative, $k)".
- **Column headers (bold 13px):** "correct rates" in green `#008300` at x≈420, "stale EUR rate" in red `#e74c3c` at x≈560.
- **Rows (38px tall, alternating white/`#f8f9fa` with `#e5e9ef` borders):**
  - "US sales" — 600 vs 600
  - "euro sales €400k ×" — 448 vs 420 (rate note gray: "EUR 1.12 vs stale 1.05"; the differing 420 shown bold red)
  - "UK sales £100k ×" — 127 vs 127 (rate note: "GBP 1.27 both runs")
  - "refunds" — −30 vs −30
- **Totals row (under a 2px `#1a5276` rule):** "total" — "$1,145k" in green vs "$1,117k" in red.
- **Caption (bold orange `#d95926` 14px, bottom center):** "gap = 400 × (1.12 − 1.05) = $28k — exactly one input's error".

## The Other Direction: "If We Change This Table, What Breaks?"

Tags: `impact analysis` (blue), `where it's used` (blue), `what goes wrong` (red)

- **The request** — the orders team wants to rename the column "amount" to "gross_amount"
- **Downstream** — lineage shows orders feeds 3 jobs, which feed 12 dashboards and 2 ML models
- **With the map** — one query lists every consumer; owners get warned before the rename ships
- **Without it** — the rename ships Friday; broken tiles are discovered one angry user at a time
- **Same graph** — tracing back is debugging; walking forward is impact analysis

*Example:* "Who reads this table?" asked in Slack found 2 of the 17 downstream nodes; the lineage graph found all 17.

**Key point:** Lineage answers "what breaks?" before the change. Incidents answer it after. Same information, very different price.

### Visualization (canvas `c3`, 720×300)

Fan-out flow diagram: everything downstream of the orders table.

- **Title (bold 16px, `#1a5276`, top center):** "Walking the Graph Forward: Everything Downstream of \"orders\"".
- **Source node:** "orders / rename \"amount\"?" box (130×44, blue `#2a78d6` border, fill `rgba(42,120,214,0.08)`) at the left.
- **Middle:** three job boxes (140×44, violet `#4a3aa7` border): "build_revenue", "build_funnel", "train_features"; blue arrows from orders to each.
- **Right:** three offset stacks of small tiles (130×34 white cards with colored borders, stacked with 5px offsets) with violet arrows from the jobs: 4-deep orange `#d95926` stack labeled "7 finance dashboards"; 3-deep aqua `#199e70` stack labeled "5 product dashboards"; 2-deep magenta `#d55181` stack labeled "2 ML models".
- **Annotations (left-aligned, lower left):** bold red 13px "one rename → 17 downstream nodes"; gray 12px two lines: "lineage lists them before the change;" / "without it, users find them after".

## The Hand-Drawn Diagram Is Not Lineage

Tags: `common confusion` (red), `rule of thumb` (orange)

- **The confusion** — a diagram drawn once in a wiki is a snapshot; pipelines change weekly
- **The decay** — six months later the wiki shows 9 dependencies; the real graph has 17
- **The fix** — derive lineage from the code itself: parse the SQL and job configs on every deploy
- **Granularity** — table-level says "orders feeds this tile"; column-level says WHICH columns do
- **The payoff** — column-level shows renaming an unused note column breaks nothing at all

*Example:* The team froze all changes to orders for a month — column-level lineage showed only 4 of its 25 columns mattered.

**Key point:** Lineage you maintain by hand is documentation; lineage extracted from the code is a measurement. Only the second one stays true.

### Visualization (canvas `c4`, 720×300)

Two-panel diagram: table-level vs column-level lineage, split by a dashed vertical divider at x=360.

- **Title (bold 16px, `#1a5276`, top center):** "Table-Level Warns About Everything; Column-Level Tells the Truth".
- **Left panel ("table-level view", bold `#1a5276` header):** one big "orders / 25 columns" box (130×110, blue `#2a78d6`) wired by a thick blue arrow to a "revenue tile" box (100×40, orange `#d95926`). Bold red 12px two-line note below: "ANY change to orders" / "raises an alarm — 25/25 columns".
- **Right panel ("column-level view", bold `#1a5276` header):** a vertical list of six column chips (110×26): "amount", "currency", "order_date", "store_id" used (green `#008300` border, fill `rgba(0,131,0,0.08)`, bold, each with a thin green arrow to the tile) and "note", "… 20 more" unused (gray `#6b7280` border, fill `#f8f9fa`, no arrows). "revenue tile" box (100×40, orange) at the right. Bold green 12px two-line note below: "only 4 of 25 columns feed the tile —" / "renaming \"note\" breaks nothing".
- **Bottom-left caption (bold orange 13px):** "over-alarming lineage gets ignored — precision is what keeps it useful".

## Regeneration instructions

- **Layout:** tutorial detail page. `<h1>` + `.subtitle`, then 4 `.card-section` blocks, each an `<h2>` (1.3rem, `#1a5276`, bottom border `2px solid #2980b9`) followed by `table.layout` with one `<tr>`: left `td.text-col` (50%) holding `.tags` pills + `<ul>` bullets + italic `.example` + `.key-point` callout; right `td.viz-col` (50%) holding one 720×300 canvas.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with bottom border `2px solid #2980b9`; `.subtitle` `#666` 0.95rem; canvases `width:100%`, border `1px solid #e0e0e0`, radius 4px; `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem; `.example` italic `#555` 0.9rem; `<li><b>` bold terms in `#1a5276`. No nav bar, no back/home links.
- **Tag pills:** `.tag` inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px — blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`, red `#e74c3c`. Overall doc palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- **Canvas:** intrinsic `width`/`height` attributes per chart; a shared `setup(id)` helper scales by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates); shared `box()` and `arrow()` helpers draw labeled node boxes and arrowheads. All data hardcoded (no `Math.random()`). Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- No cross-page links; in regenerated HTML any card links would use `.html` extensions.
