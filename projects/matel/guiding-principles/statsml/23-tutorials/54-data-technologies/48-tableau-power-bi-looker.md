# Tableau, Power BI, Looker

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Tableau, Power BI, Looker

**Subtitle:** The three big BI tools all turn warehouse tables into dashboards — they differ in where the metric definition lives: in the analyst's workbook, in a shared dataset, or in version-controlled code

## One Dashboard, Three Philosophies

**Tags:** `core idea` (blue), `BI layer` (green), `three tools` (orange)

- **The ask** — an online retailer wants one dashboard: quarterly revenue broken down by region
- **Tableau** — the analyst drags Region and Revenue onto shelves; VizQL turns the gesture into a query and a chart
- **Power BI** — Microsoft's BI, bundle-priced into the Office ecosystem; metrics are DAX measures in a shared dataset
- **Looker** — revenue is defined once as code in a LookML model; Looker writes SQL and pushes it to the warehouse
- **The real split** — same bars either way; what differs is where "revenue =" lives and who is allowed to change it

*Example (italic):* Three analysts get the same request; one drags fields, one writes a DAX measure, one commits a LookML file — all three ship the same four bars.

**Key point:** A BI layer sits between the warehouse and the reader, turning tables into charts — the three tools embody three philosophies: analyst-driven exploration, ecosystem integration, and governed definitions-as-code.

### Visualization (canvas `c1`, 720×300)

Three-lane flow diagram, one lane per tool: warehouse box → the box where "revenue" is defined (highlighted) → dashboard box, so the reader sees the definition move rightward-to-leftward across philosophies.

- **Title (bold 15px, `#1a5276`, top center):** "Same Dashboard, Three Homes for the Definition of Revenue".
- **Lanes (rows at y = 85, 155, 225), each with a bold 12px `#1a5276` lane label at x=20:** "Tableau", "Power BI", "Looker".
- **Row 1 (Tableau):** box at x=110 "warehouse / extract", 3px arrow, box at x=300 "workbook: SUM(Amount) on the shelf" with 3px orange `#d95926` border (the definition lives here), arrow, box at x=560 "chart".
- **Row 2 (Power BI):** box at x=110 "warehouse / import", arrow, box at x=300 "dataset: DAX measure Revenue" with 3px blue `#2a78d6` border, arrow, box at x=560 "report".
- **Row 3 (Looker):** box at x=110 "LookML model (code, in git)" with 3px green `#008300` border, arrow labeled 11px `#6b7280` "generates SQL", box at x=300 "warehouse runs the query", arrow, box at x=560 "dashboard".
- **Box style:** 150–170px wide, 40px tall, 8px radius, fill `rgba(42,120,214,0.12)`, 12px `#2c3e50` text; highlighted definition boxes add the colored border above.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=270):** "the colored box is where 'revenue =' lives — workbook, dataset, or code".

## The Same Four Bars, Built Three Ways

**Tags:** `worked example` (blue), `revenue by region` (green)

- **The data** — an orders table, one row per order, with two columns that matter: region and amount
- **The numbers** — quarterly revenue: North $4.2M, South $3.1M, East $2.6M, West $1.8M (illustrative)
- **Tableau** — drag Region to Columns and SUM(Amount) to Rows; VizQL compiles the shelves into the query
- **Power BI** — write `Revenue = SUM(Orders[Amount])` once in DAX, then reuse the measure in any visual
- **Looker** — commit `measure: revenue { type: sum sql: ${amount} ;; }`; it emits GROUP BY region SQL
- **Hand-check** — total is 4.2 + 3.1 + 2.6 + 1.8 = $11.7M in all three tools, because the input rows are identical

*Example (italic):* All three tools render the identical four bars — North $4.2M tallest, West $1.8M shortest — and all three totals land on $11.7M.

**Key point:** The output is the same chart from the same table; what you are really choosing is the authoring model — gestures, a formula language, or a committed code file.

### Visualization (canvas `c2`, 720×300)

Vertical bar chart of quarterly revenue by region — the one chart all three tools produce — with the three authoring paths named beneath the plot.

- **Title (bold 15px, `#1a5276`, top center):** "Revenue by Region: the Chart All Three Tools Draw".
- **Axes:** origin x=60, baseline y=225, plot width 600, plot height 160; y = revenue $0 to $5M, gridlines `#e5e9ef` with 12px `#444` labels at $1M/$2M/$3M/$4M.
- **Bars:** four bars centered at x = 150, 290, 430, 570, each 90px wide, fill `rgba(42,120,214,0.35)` with 2px `#2a78d6` border; regions `["North", "South", "East", "West"]`, revenue $M `[4.2, 3.1, 2.6, 1.8]`; bold 13px `#1a5276` value labels "$4.2M" etc. above each bar; 12px `#444` region labels below the baseline.
- **Annotation (bold 13px green `#008300`, upper right near x=430, y=70):** "total $11.7M — identical in all three tools".
- **Footer strip (11px `#6b7280`, three labels evenly spaced at y=262):** "Tableau: shelves → VizQL", "Power BI: DAX measure", "Looker: LookML → SQL".
- **Caption (12px `#444`, bottom right):** "revenue figures illustrative; the $11.7M total is the exact sum of the four bars".

## Where the Definition Lives Decides Who Controls It

**Tags:** `where it's used` (blue), `trade-offs` (green)

- **Analyst-driven** — Tableau optimizes for exploration speed: see the data, drag, iterate, publish
- **Ecosystem** — Power BI wins on price and reach; it ships alongside Office, so every desk already has it
- **Governed** — Looker centralizes: one LookML definition, version-controlled, and every dashboard inherits it
- **No extracts** — Looker pushes queries to the warehouse, so numbers update when the warehouse does
- **The trade** — freedom to explore pulls against consistency of definitions; each tool picks a side
- **You meet this** — the day your model's output lands in a dashboard and someone asks which number is official

*Example (italic):* A data scientist ships a churn score; in Tableau every analyst can slice it their own way, while in Looker the score's definition is a code review away from anyone changing it.

**Key point:** Pick by what you fear more — slow analysts (favor Tableau's exploration), procurement friction (favor Power BI's bundle), or inconsistent metrics (favor Looker's governed layer).

### Visualization (canvas `c3`, 720×300)

Spectrum diagram: one horizontal axis from analyst freedom to central governance, with the three tools placed as labeled dots and a one-line descriptor under each.

- **Title (bold 15px, `#1a5276`, top center):** "Three Philosophies on One Axis".
- **Axis:** 3px `#999` horizontal line at y=160 from x=80 to x=640, small arrowheads at both ends; bold 13px `#2c3e50` end labels "analyst freedom" at x=80 (anchored left, y=130) and "central governance" at x=640 (anchored right, y=130).
- **Markers (14px radius filled dots on the axis):** Tableau at x=150 orange `#d95926`; Power BI at x=360 blue `#2a78d6`; Looker at x=580 green `#008300`; bold 13px name labels in the dot's color 24px above each dot.
- **Descriptors (12px `#444`, centered 28px below each dot):** "drag-and-drop exploration (VizQL)" under Tableau; "Office ecosystem, DAX datasets" under Power BI; "metrics as code (LookML)" under Looker.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=255):** "further right = fewer people can redefine a metric, and fewer versions of it exist".
- **Caption (12px `#444`, bottom right):** "positions schematic".

## Three Dashboards, Three Revenues

**Tags:** `common mistake` (red), `governance` (orange)

- **The setup** — three teams each build their own "revenue" dashboard from the same orders table
- **Sales** — counts gross bookings and reports $11.7M for the quarter
- **Finance** — subtracts $0.5M of refunds and reports $11.2M
- **Ops** — subtracts refunds and $0.8M of tax and reports $10.4M
- **The meeting** — the CFO sees three "revenue" numbers for one quarter, and trust in every dashboard drops
- **The fix** — one shared definition in a semantic layer, not a fourth dashboard averaging the other three

*Example (italic):* All three dashboards are internally correct — 11.7, then 11.7 − 0.5 = 11.2, then 11.2 − 0.8 = 10.4 — yet the company cannot say what revenue was.

**Common mistake:** Treating dashboard-level formulas as harmless. Every copy of a metric definition is a place for it to drift — the governance problem Looker's LookML (and semantic layers generally) exists to solve.

### Visualization (canvas `c4`, 720×300)

Horizontal bar chart: the same quarter's "revenue" as reported by three dashboards, with the subtracted pieces called out so the reader sees the drift is definitional, not an error.

- **Title (bold 15px, `#1a5276`, top center):** "One Quarter, Three 'Revenues'".
- **Axis:** bars start at x=230 and extend right, max width 440 for the largest value; left-aligned 12px `#444` row labels at x=20.
- **Rows (top to bottom at y = 85, 145, 205), bar widths proportional to value:**
  - "Sales dash — gross": blue `#2a78d6` bar width 440, bold 12px label "$11.7M" at the bar end
  - "Finance dash — minus refunds": aqua `#199e70` bar width 421, label "$11.2M", small 11px red `#e74c3c` note "− $0.5M refunds" just right of the label
  - "Ops dash — minus refunds + tax": orange `#d95926` bar width 391, label "$10.4M", small 11px red note "− $0.8M tax"
- **Bar style:** 22px tall, 30% opacity fills with 2px solid borders in the bar's color.
- **Annotation (bold 13px magenta `#d55181`, centered near y=255):** "all three are 'correct' — the definitions differ, and nobody agreed on one".
- **Caption (12px `#444`, bottom right):** "dollar amounts illustrative; the subtractions (11.7 − 0.5 = 11.2, 11.2 − 0.8 = 10.4) are exact".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); regional revenue (4.2 / 3.1 / 2.6 / 1.8 $M) and the refund/tax deltas (0.5 / 0.8 $M) are invented and labeled illustrative; the totals and subtractions (11.7, 11.2, 10.4) are exact arithmetic on those inputs. Product facts (VizQL, DAX, LookML, Office bundling, warehouse-pushed queries) are publicly documented; do not invent undocumented product behavior.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
