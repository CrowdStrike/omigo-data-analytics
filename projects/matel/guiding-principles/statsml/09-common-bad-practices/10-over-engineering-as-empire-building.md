# Over-Engineering as Empire Building

**Page type:** detail page (two-column obj-table layout: text left ~40%, canvases right ~60%, one row per section)
**HTML title tag:** Over-Engineering as Empire Building — Common Bad Practices

**Subtitle:** Complexity as Job Security — Problem needs 200 lines of SQL. You build a distributed system requiring a team of 4.

## Section 1: The Practice

- Business need: query a table, aggregate, produce a report.
- What gets built: custom ingestion pipeline, 3 microservices, a monitoring stack, a custom orchestrator, a data lake, a cache layer.
- Now you "need" a team of 4 to maintain it. Complexity = headcount = budget = seniority = promotion.

### Visualization (canvas `c1`, 720×340)

Simple-solution box vs an explosion diagram of interconnected components, both producing the same report.

- **Left (simple, green):** rectangle 185×70 at (15, 35), fill `rgba(39,174,96,0.1)`, stroke `#27ae60` width 2. Text centered at x=107 in `#27ae60`: bold 16px "The actual problem:", then 15px "200 lines of SQL" / "1 cron job" — all text fits inside the box. Green width-2 arrow (with filled arrowhead) pointing right from the box edge (x 205→250), labeled left-aligned 15px "report" at x=258.
- **Right (complex, red):** 8 boxes 100×40, fill `rgba(231,76,60,0.08)`, stroke `#e74c3c` width 1.5, red 16px centered labels; centers: `Ingestion Pipeline` (380,40 — two lines), `Orchestrator` (530,40), `Service A` (660,40), `Service B` (380,130), `Service C` (530,130), `Monitoring` (660,130), `Data Lake` (430,220), `Cache Layer` (590,220).
- **Mesh connections:** thin `rgba(231,76,60,0.3)` width-1 lines between box pairs (index pairs): [0,1],[0,3],[1,2],[1,4],[2,5],[3,4],[3,6],[4,5],[4,7],[5,7],[6,7].
- **Output arrow from complex:** red width-2 vertical arrow from (590, 260) down to (590, 285) with filled arrowhead, red 16px label "same report" at (590, 298).
- **Bottom label (italic 15px `#666`, centered, y = h−15):** "Same result. Left: 1 person, 1 day. Right: 4 people, 6 months."

## Section 2: The Incentive Problem

- Person A ships a 200-line solution and moves on to the next problem. The work registers as small scope.
- Person B builds a multi-service platform that needs a team of four to maintain. The work registers as large scope and team growth.
- Evaluation systems often measure visible scope, not problem-solved-per-line — so the incentive points away from simplicity.

### Visualization (canvas `c2`, 720×300)

Scatter plot of built solution complexity vs actual requirement complexity; points far above the diagonal are over-engineered.

- **Title (bold 16px `#1a5276`, centered, y=20):** "Built Complexity vs Actual Requirement Complexity".
- **Plot area:** left axis at x=70, bottom axis at y=235, plot spans x 70→690 and y 50→235; axis lines `#999` width 1. Both scales 0→10; mapping `px(x) = 70 + 62·x`, `py(y) = 235 − 18.5·y`. Ticks at 0,2,4,6,8,10 on both axes — 13px `#666` labels, x ticks centered at y=252, y ticks right-aligned at x=62. Axis titles 14px `#666`: "Actual requirement complexity" centered at y=272; "Built solution complexity" rotated −90° at x=18, vertically centered on the plot.
- **Diagonal (dashed `#999`, pattern [5,4], width 1):** from data (0,0) to (10,10); italic 13px `#27ae60` label "built ≈ needed" at pixel (560, 118), centered.
- **Matched projects (green `#27ae60` filled circles, r=6):** data points (1,1.2), (2,2.2), (3,3.4), (4,3.8), (5,5.3), (6,5.8), (7,7.2) — hugging the diagonal.
- **Over-engineered projects (red `#e74c3c` filled circles, r=7):** data points (1,6.5), (1.5,8.2), (2,7.4), (2.5,9.4), (3,8.6), (4,9.0) — a cluster far above the diagonal (simple problems, distributed-platform solutions).
- **Insight annotation (bold 15px `#e74c3c`, centered at pixel x=470, two lines y=180/199):** "far above the diagonal" / "= over-engineered"; thin red width-1.5 arrow (with filled arrowhead) from (350, 172) to (280, 105) pointing into the red cluster.
- **Bottom label (italic 13px `#666`, centered, y = h−8):** "Every project above the line needs a bigger team than the problem does."

## Section 3: Variant — "Future-Proofing"

- "We might need to scale to 100x current load someday!" Build for 100x now (at 100x cost and complexity). Someday never comes. But the complexity justifies the team size TODAY.

### Visualization (canvas `c3`, 720×260)

Two horizontal scale bars: actual load vs what was built.

- **Title (bold 16px `#1a5276`, centered, y=25):** "Current Load vs. What Was Built".
- **Actual bar (y=50):** tiny green `#27ae60` bar — 1% of a 500px scale (5px) at x=100, 25px tall, green stroke; green 16px left-aligned label "Actual: 1x" just right of the bar.
- **Built-for bar (y=90):** full-width bar 500×25 at x=100, fill `rgba(231,76,60,0.3)`, stroke `#e74c3c` width 1.5; red 16px right-aligned label inside the bar's right end: "Built for: 100x (\"someday\")".
- **Bottom label (italic 14px `#666`, centered, y = h−10):** "\"Someday\" never comes. But the complexity justifies the team size TODAY."

## Section 4: Variant — Resume-Driven Development

- Choose technologies that look good on YOUR resume, not technologies appropriate for the problem.
- Kubernetes for a cron job. Kafka for 10 events/second. Spark for 1GB of data.

**Why it persists:** Engineering orgs promote people who "build systems" and "grow teams," not people who solve problems simply. Simple solution = small scope = no promotion. Complex solution = big scope = team growth = promotion.

**The tell:** Ask "what happens if we replace this with a SQL query and a cron job?" If defensive reaction, the complexity serves the builder's career, not the business. Also: compare system complexity to actual throughput/scale requirements.

### Visualization (canvas `c4`, 720×260)

Utilization bar chart: provisioned capacity vs actual peak usage across five systems — every bar stuck in single digits.

- **Title (bold 16px `#1a5276`, centered, y=20):** "Provisioned Capacity vs Actual Peak Utilization".
- **Plot area:** baseline (x-axis) at y=205, top of scale at y=45 (0→100%, so 1.6px per %); left axis at x=70, plot spans to x=690; axis lines `#999` width 1. Y gridlines at 25/50/75/100% in `#eee` width 1 with 12px `#999` right-aligned labels at x=62 ("25%", "50%", "75%", "100%").
- **Bars (width 70, centered at x = 132, 256, 380, 504, 628):** for each system, a full-height column outline (fill `rgba(231,76,60,0.08)`, stroke `rgba(231,76,60,0.35)` width 1, from y=45 to baseline) = provisioned capacity, plus a solid red `#e74c3c` bar from the baseline up = actual peak utilization. Systems and values: `Kafka cluster` 2%, `K8s cluster` 4%, `Spark cluster` 3%, `Redis cache` 6%, `Data lake` 5%.
- **Value labels:** bold 14px `#e74c3c` centered 6px above each solid bar ("2%", "4%", "3%", "6%", "5%"). System names 13px `#333` centered at y=222.
- **Target line:** dashed `#27ae60` (pattern [6,4], width 1.5) horizontal at 70% (y=93); 13px `#27ae60` label "healthy utilization target (70%)" left-aligned at (75, 87).
- **Insight annotation (bold 15px `#e74c3c`, centered at w/2, y=135):** "Over 90% of every system sits idle".
- **Bottom label (italic 13px `#666`, centered, y = h−8):** "Capacity provisioned for imagined scale; actual peak never leaves single digits."

### Visualization (canvas `c5`, 720×300)

Cumulative cost line chart over 24 months: the simple solution flatlines after shipping; the over-engineered platform keeps climbing from maintenance burden.

- **Title (bold 16px `#1a5276`, centered, y=20):** "Cumulative Cost Over 24 Months".
- **Plot area:** left axis at x=65, bottom axis at y=240, plot spans x 65→600 and y 45→240; axis lines `#999` width 1. X scale 0→24 months, `px(m) = 65 + (535/24)·m`, ticks at 0,6,12,18,24 (13px `#666`, centered, y=258), axis title 13px `#666` "Months" centered at y=276. Y scale 0→700 ($K), `py(v) = 240 − (195/700)·v`, gridlines at 200/400/600 in `#eee` width 1 with 12px `#999` right-aligned labels at x=58 ("$200K", "$400K", "$600K").
- **Simple solution line (green `#27ae60`, width 2.5):** cumulative cost in $K at month m: 0 at m=0, then `15 + 1.5·(m−1)` for m=1..24 ($15K build in month 1, $1.5K/month maintenance; $49.5K at month 24). Green filled dot r=5 at (1, 15) with 13px `#27ae60` label "simple ships (month 1)" left-aligned at (95, 222); inline series label italic 13px `#27ae60` "simple solution (SQL + cron)" left-aligned at (330, 214). End labels left-aligned at x=606: bold 14px `#27ae60` "$49.5K" at y=224, 12px "simple" at y=238.
- **Over-engineered line (red `#e74c3c`, width 2.5):** cumulative cost in $K: `40·m` for m=0..6 (6-month build at $40K/month, $240K at launch), then `240 + 25·(m−6)` for m=7..24 (4-engineer maintenance at $25K/month; $690K at month 24). Red filled dot r=5 at (6, 240) with 13px `#e74c3c` label "platform ships (month 6)" left-aligned at (207, 168); inline series label italic 13px `#e74c3c` "over-engineered platform" left-aligned at (300, 188). End labels left-aligned at x=606: bold 14px `#e74c3c` "$690K" at y=52, 12px "over-engineered" at y=66.
- **Insight annotation (bold 15px `#e74c3c`, centered at (340, 78)):** "14x the cost by month 24 — and still climbing".
- **Bottom label (italic 13px `#666`, centered, y = h−6):** "The build cost is visible up front; the maintenance burden compounds forever."

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table, four `<tr>` rows (The Practice / The Incentive Problem / Variant — "Future-Proofing" / Variant — Resume-Driven Development); left `<td>` (40%) holds `.obj-title` + bullets/paragraphs, right `<td>` (60%, centered) holds the canvas(es) — row 4 stacks `c4` and `c5`.
- **Page style:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; `.subtitle` `#666` 1.0em; `p` `#333` 0.95em; `ul` 0.9em `#333`, `li` margin 6px 0; table cell borders `1px solid #e0e0e0`, padding 20px 24px, vertical-align middle; `.obj-title` 1.05em, weight 600, `#1a5276`; `strong` in `#1a5276`. No nav bar, no back/home links.
- **Canvas:** declare intrinsic `width`/`height` attributes as given per chart; `canvas { display: block; margin: 0 auto; width: 100%; height: auto; }`. Sharp-rendering pattern: shared `setup(id)` helper stores the logical size in `data-w`/`data-h` on first call, sizes the backing store to rendered CSS width × `devicePixelRatio` (`scale = (getBoundingClientRect().width / w) * dpr`), and `ctx.scale(scale, scale)`. Chart draw functions are pushed into a `__charts` array, run once on load, and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray text `#666`/`#333`.
