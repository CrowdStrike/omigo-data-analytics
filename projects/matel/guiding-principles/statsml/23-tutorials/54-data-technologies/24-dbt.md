# dbt

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** dbt

**Subtitle:** dbt turns SQL SELECT statements into a versioned, tested pipeline — transformations live in files, build in dependency order, and fail loudly when the data is bad

## A SELECT Statement That Lives in a File

**Tags:** `core idea` (blue), `ELT` (green), `SQL` (orange)

- **The raw table** — an online shop loads raw order rows into the warehouse untouched, every night
- **The model** — a dbt model is one file holding one SELECT; running it builds a table or view
- **stg_orders** — a staging model renames columns, casts types, and drops cancelled test orders
- **daily_revenue** — a second model SELECTs from stg_orders and sums amount per order date
- **ref()** — daily_revenue writes `ref('stg_orders')`, not a table name; the refs form a dependency DAG
- **In-warehouse** — dbt compiles the files to SQL the warehouse itself executes — ELT, not ETL

*Example (italic):* One command, `dbt run`, walks the graph and builds stg_orders before daily_revenue — nobody remembers the order by hand.

**Key point:** dbt moves no data itself — it organizes the SELECT statements that transform data already in the warehouse, and ref() tells it which SELECT must run first.

### Visualization (canvas `c1`, 720×300)

Left-to-right flow diagram of the running example: raw source table feeding two dbt models, with the E+L / T split bracketed above.

- **Title (bold 15px, `#1a5276`, top center):** "ELT in a dbt Project: Load Raw First, Transform With SELECT Files".
- **Boxes (46px tall, 8px radius, 12px `#2c3e50` text, centered vertically at y=155):** gray box at x=50 w=160 labeled "raw_orders (source)" fill `rgba(107,114,128,0.15)` border 2px `#6b7280`; blue box at x=290 w=160 labeled "stg_orders (model)" fill `rgba(42,120,214,0.15)` border 2px `#2a78d6`; green box at x=530 w=160 labeled "daily_revenue (model)" fill `rgba(0,131,0,0.12)` border 2px `#008300`.
- **Arrows:** 3px `#2c3e50` arrows between the boxes; the second arrow carries a bold 12px violet `#4a3aa7` label "ref('stg_orders')" above it.
- **Brackets (12px `#6b7280`, y=90):** "E + L — loader tool" spanning the gray box; "T — dbt (SELECT in files)" spanning the two model boxes, each with a thin `#6b7280` bracket line.
- **Sub-labels (11px `#6b7280`, under each box at y=215):** "loaded as-is", "rename, cast, filter", "SUM(amount) per day".
- **Annotation (bold 13px `#1a5276`, centered near y=260):** "every arrow dbt knows about comes from a ref() call".

## The Tuesday the Test Said No

**Tags:** `worked example` (blue), `tests` (green)

- **The load** — Tuesday night's export lands 250 new raw orders; a bug leaves 7 with a NULL amount
- **The test** — one YAML line under stg_orders declares `not_null` on amount; `dbt test` runs it as a query
- **The catch** — the test returns 7 failing rows, so the build halts before daily_revenue rebuilds
- **The damage avoided** — SUM skips NULLs, so Tuesday would read $2,890 instead of the true $3,065
- **Hand-check** — 7 lost orders × $25 average = $175 missing, exactly the $3,065 − $2,890 gap
- **The morning after** — the export is fixed, `dbt run` rebuilds the chain, Tuesday posts $3,065

*Example (italic):* The dashboard never shows the wrong $2,890 — it keeps Monday's good data until the 7 NULLs are fixed and the run goes green.

**Key point:** dbt tests (`not_null`, `unique`, `relationships`) are declared in YAML next to the model and run as queries after each build — bad data fails the pipeline instead of quietly reaching the report.

### Visualization (canvas `c2`, 720×300)

Bar chart of daily_revenue for the week, with Tuesday drawn twice: the blocked NULL-damaged total in red beside the corrected total in green.

- **Title (bold 15px, `#1a5276`, top center):** "not_null Caught 7 NULLs Before Tuesday Shipped Wrong".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = revenue $0 to $4,500, gridlines `#e5e9ef` at 1500/3000/4500 with 12px `#444` labels "$1,500"/"$3,000"/"$4,500".
- **Bars:** days `["Mon","Tue","Wed","Thu","Fri","Sat","Sun"]`, values `[3120, 3065, 2980, 3140, 3410, 4230, 3955]`; single bars width 50, left edge x = 80 + i*82, fill `rgba(42,120,214,0.30)` with 2px `#2a78d6` border — except Tuesday's slot holds two 24px bars: left bar 2890 fill `rgba(231,76,60,0.20)` with dashed 2px `#e74c3c` border (blocked), right bar 3065 fill `rgba(0,131,0,0.25)` with 2px `#008300` border (shipped).
- **Labels:** 12px `#444` day names centered under each slot at y=262; 12px `#2c3e50` value labels above each bar top; 11px red `#e74c3c` "blocked" and 11px green `#008300` "shipped" above Tuesday's pair.
- **Annotation (bold 13px green `#008300`, near x=340, y=60):** "7 NULLs = $175 that never went missing".
- **Caption (12px `#444`, bottom right):** "daily amounts illustrative; 3,065 − 2,890 = 175 = 7 × $25 is exact".

## Why a New Job Title Appeared

**Tags:** `where it's used` (blue), `lineage` (green)

- **Version control** — models are text files, so every change is a commit a teammate reviews first
- **Docs for free** — `dbt docs generate` builds a browsable site from YAML descriptions plus the DAG
- **Lineage** — click daily_revenue and the graph shows what feeds it and what breaks if it changes
- **The role** — this workflow named the "analytics engineer": SQL work run with software discipline
- **The warehouse works** — dbt only compiles and submits SQL; Snowflake or BigQuery execute it

*Example (italic):* A new analyst traces a dashboard number to daily_revenue, then to stg_orders, then to the raw source — in the generated docs, not by asking around.

**Key point:** dbt's contribution is not new SQL — it wraps SQL in the practices software teams already trust: git, code review, automated tests, and documentation generated from the project itself.

### Visualization (canvas `c3`, 720×300)

Lineage graph as dbt's generated docs would draw it: two sources, two staging models, two marts, one dashboard, every edge from a ref() call.

- **Title (bold 15px, `#1a5276`, top center):** "The Lineage Graph dbt Draws For You".
- **Columns (headers 12px `#6b7280` at y=60):** "sources" at x=95, "staging" at x=315, "marts" at x=530.
- **Boxes (140px wide, 36px tall, 8px radius, 12px `#2c3e50` text):** gray `rgba(107,114,128,0.15)` border 2px `#6b7280`: "raw_orders" at (40,85), "raw_customers" at (40,170); blue `rgba(42,120,214,0.15)` border 2px `#2a78d6`: "stg_orders" at (255,85), "stg_customers" at (255,170); green `rgba(0,131,0,0.12)` border 2px `#008300`: "daily_revenue" at (470,85), "customer_orders" at (470,170); violet `rgba(74,58,167,0.12)` border 2px `#4a3aa7`: "dashboard" at (470,240).
- **Edges (2px `#2c3e50` arrows):** raw_orders→stg_orders, raw_customers→stg_customers, stg_orders→daily_revenue, stg_orders→customer_orders, stg_customers→customer_orders, daily_revenue→dashboard.
- **Annotation (bold 13px violet `#4a3aa7`, near x=60, y=265):** "built from ref() calls — no one drew this by hand".
- **Sub-label (11px `#6b7280`, under stg_orders at y=132):** "2 models depend on this".

## A Thousand Models Nobody Owns

**Tags:** `common mistake` (red), `ownership` (orange)

- **Cheap to add** — a model is one file; a busy team can add hundreds a year without noticing
- **Copy-paste sprawl** — daily_revenue_v2, daily_revenue_final, and _tmp variants pile up untouched
- **The bill** — the nightly run rebuilds everything, so run time grows with models nobody reads
- **Fear of deleting** — with no owner listed, nobody knows which models a dashboard still uses
- **The discipline** — owners in YAML, naming conventions, and pruning dead models keep the DAG honest

*Example (italic):* Two years in, the project holds 1,000 models but only 240 have a listed owner — the other 760 survive because deleting them scares everyone.

**Common mistake:** Treating models as free. Every ref() edge is a maintenance promise; a thousand-model DAG without owners turns the tool built for clarity into a new swamp.

### Visualization (canvas `c4`, 720×300)

Two-line growth chart over eight quarters: total models climbing to 1,000 while models with a listed owner flatten near 240, the gap shaded as the unowned swamp.

- **Title (bold 15px, `#1a5276`, top center):** "Model Count Grows; Ownership Doesn't".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = quarters "Q1"–"Q8" with 12px `#444` tick labels every quarter; y = models 0 to 1,000, gridlines `#e5e9ef` at 250/500/750/1000.
- **Total-models line:** red `#e74c3c` 3px line through quarters `[1,2,3,4,5,6,7,8]`, counts `[40, 120, 310, 480, 620, 760, 890, 1000]`, 12px red label "total models" near its right end.
- **Owned-models line:** blue `#2a78d6` 3px line through the same quarters, counts `[40, 95, 150, 180, 205, 220, 232, 240]`, 12px blue label "with a listed owner" near its right end.
- **Gap shading:** fill `rgba(231,76,60,0.10)` between the two lines from Q2 to Q8.
- **Annotation (bold 13px red `#e74c3c`, near x=430, y=90):** "760 models nobody owns".
- **Caption (12px `#444`, bottom right):** "counts illustrative; 1,000 − 240 = 760 is exact".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); row counts (250 / 7), daily revenues `[3120, 3065, 2980, 3140, 3410, 4230, 3955]`, and model counts `[40, 120, 310, 480, 620, 760, 890, 1000]` vs `[40, 95, 150, 180, 205, 220, 232, 240]` are invented and labeled illustrative; 3,065 − 2,890 = 175 = 7 × $25 and 1,000 − 240 = 760 are exact arithmetic on those illustrative figures.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
