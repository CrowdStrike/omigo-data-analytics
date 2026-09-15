# Superset

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Superset

**Subtitle:** Apache Superset is an open-source BI layer that owns no data — it connects to the databases you already have and turns tables into shared charts, dashboards, and filters

## The Daily Revenue Table Nobody Can See

**Tags:** `core idea` (blue), `open-source BI` (green), `Apache Superset` (orange)

- **The table** — a retailer's warehouse holds `daily_revenue`: one row per day per region, plain SQL rows
- **The askers** — sales, finance, and ops each ping the analyst every week for the same numbers
- **The BI layer** — Superset connects to that database and turns the table into charts anyone can open
- **Owns no data** — Superset stores connections, charts, and dashboards; the rows stay in your database
- **Any SQL database** — it speaks SQLAlchemy, so Postgres, MySQL, Snowflake and dozens more all plug in
- **Born at Airbnb** — started in 2015 by the creator of Airflow, later donated to the Apache Software Foundation

*Example (italic):* The analyst connects Superset to the warehouse once; by that afternoon, sales opens a live revenue dashboard instead of emailing for a CSV.

**Key point:** Superset is a BI layer over databases you already run — every chart is a query sent to your database via SQLAlchemy, and no rows are ever copied into Superset itself.

### Visualization (canvas `c1`, 720×300)

Flow diagram: three databases on the left feed one Superset box in the middle via SQLAlchemy, which serves dashboards to many viewers on the right.

- **Title (bold 15px, `#1a5276`, top center):** "A BI Layer Owns No Data: Charts Are Queries, Not Copies".
- **Left column (x=30, boxes 130px wide, 36px tall, y = 75, 140, 205):** three rounded boxes labeled "PostgreSQL", "MySQL", "Snowflake" (12px `#2c3e50`), fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border.
- **Middle box (x=280, y=105, 180px wide, 80px tall):** violet `#4a3aa7` 2px border, fill `rgba(74,58,167,0.10)`, bold 13px label "Superset" with 11px `#6b7280` sublabel "datasets · charts · dashboards".
- **Right column (x=560, boxes 120px wide, 32px tall, y = 80, 135, 190):** three green-bordered `#008300` boxes labeled "sales", "finance", "ops" (12px), fill `rgba(0,131,0,0.10)`.
- **Arrows:** 2px `#6b7280` arrows from each database box to the Superset box, labeled once "SQLAlchemy" (11px `#6b7280`, above the middle arrow); 2px arrows from Superset to each viewer box, labeled once "dashboards" (11px `#6b7280`).
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=265):** "no rows are copied — every chart re-queries the source".
- **Caption (12px `#444`, bottom right):** "architecture schematic".

## Turning daily_revenue Into a Shared Dashboard

**Tags:** `worked example` (blue), `SQL Lab` (green)

- **The query** — in SQL Lab, the analyst runs `SELECT day, region, revenue FROM daily_revenue` ad hoc
- **The dataset** — she saves it as a dataset; Superset now knows its columns and their types
- **The metric** — she defines `total_revenue = SUM(revenue)` once on the dataset, not inside any chart
- **The chart** — a bar chart of total_revenue by day: $11,800 Tue low up to $18,600 Sat peak (illustrative)
- **The filter** — a region filter on the dashboard; picking "West" re-runs every chart's query filtered

*Example (italic):* The week's dailies sum to exactly $100,500, and Sat's $18,600 peak is 58% above Tue's $11,800 low (exact arithmetic on the illustrative dailies).

**Key point:** Charts are defined over datasets and metrics, not raw SQL — define SUM(revenue) once, reuse it in every chart, and let dashboard filters re-run the queries live.

### Visualization (canvas `c2`, 720×300)

Vertical bar chart of the running example's week of daily revenue, weekend bars highlighted, metric definition shown as a label.

- **Title (bold 15px, `#1a5276`, top center):** "Chart: total_revenue by Day — One Metric, Defined Once".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = revenue $0 to $20,000, gridlines `#e5e9ef` at 5,000 / 10,000 / 15,000 with 12px `#444` labels "$5k" / "$10k" / "$15k".
- **Bars:** 7 bars, 56px wide, evenly spaced starting x=85; day labels "Mon"–"Sun" (12px `#444`) under each; revenues `[12400, 11800, 13100, 12900, 14200, 18600, 17500]`; Mon–Fri fill `rgba(42,120,214,0.35)` with 2px `#2a78d6` top edge, Sat–Sun fill `rgba(0,131,0,0.30)` with 2px `#008300` top edge.
- **Value labels:** 11px `#2c3e50` above each bar: "12.4k", "11.8k", "13.1k", "12.9k", "14.2k", "18.6k", "17.5k".
- **Metric label (12px `#6b7280`, top left under the title, x=60):** "metric: total_revenue = SUM(revenue)".
- **Annotation (bold 13px green `#008300`, near the Sat bar, y=55):** "weekend peak $18,600".
- **Caption (12px `#444`, bottom right):** "revenues illustrative; weekly total $100,500 is their exact sum".

## Why Teams Pick a BI Layer With No Per-Seat Fees

**Tags:** `where it's used` (blue), `semantic layer` (green), `licensing` (orange)

- **One definition** — the semantic layer holds metrics and dimensions per dataset; every chart reuses them
- **No metric drift** — without it, three teams write three SUM queries and report three different totals
- **Open source** — Apache-licensed, so 5 or 500 viewers cost the same: your server, not per-seat fees
- **The alternative** — Tableau and Looker license per user, which quietly caps who gets a login
- **The cache** — Superset caches chart results in front of the database, so repeat viewers skip the query
- **Self-serve** — analysts publish datasets; colleagues who don't write SQL build their own charts on top

*Example (italic):* When finance asks "does your revenue number include refunds?", the answer lives in one metric definition on the dataset — not in 14 people's private spreadsheets.

**Key point:** A shared semantic layer plus no per-seat licensing is the pitch: the whole company reads one definition of revenue on one dashboard, and adding a viewer costs nothing.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: yearly BI cost for a 55-person team (5 builders + 50 viewers) under per-seat licensing vs Superset's flat infrastructure cost.

- **Title (bold 15px, `#1a5276`, top center):** "Yearly Cost, 5 Builders + 50 Viewers: Per-Seat vs Open Source".
- **Axis:** bars start at x=250, extend right, max width 440; left-aligned 12px `#444` row labels at x=20.
- **Rows (top to bottom at y = 80, 125, 170, 225):**
  - "seat BI: 5 builders × $75/mo": blue `#2a78d6` bar, fill `rgba(42,120,214,0.30)`, width 147 (= $4,500)
  - "seat BI: 50 viewers × $15/mo": blue bar, width 293 (= $9,000)
  - "seat BI total": solid blue bar, width 440 (= $13,500), bold 12px `#2a78d6` end label "$13,500/yr"
  - "Superset: one server, any viewers": solid green `#008300` bar, width 39 (= $1,200), bold 12px `#008300` end label "$1,200/yr"
- **Bar style:** 16px tall, 11px `#444` dollar labels at the ends of the first two bars ("$4,500", "$9,000"); pixel widths scale linearly at 440px = $13,500.
- **Annotation (bold 13px magenta `#d55181`, right side near y=260):** "viewer #51 costs $0 — the seat meter never runs".
- **Caption (12px `#444`, bottom right):** "seat prices and server cost illustrative".

## Fourteen Dashboards, Three Different Revenues

**Tags:** `common mistake` (red), `metric drift` (orange)

- **The multiplication** — cloning a dashboard is one click, so 14 revenue dashboards appear in six months
- **The drift** — each clone tweaks its inline SQL: one subtracts refunds, one also drops test orders
- **The meeting** — three VPs quote $100,500, $96,900, and $95,400 for the same week's revenue
- **The cause** — metrics written inside each chart's SQL instead of once on the shared dataset
- **The fix** — define total_revenue on the dataset; clones then inherit the definition instead of forking it

*Example (italic):* The week's $3,600 of refunds and $1,500 of test orders explain the whole $5,100 spread — every dashboard was "right" by its own private definition.

**Common mistake:** Treating dashboards as free copies. Every cloned chart with its own inline SQL forks the metric definition — keep metrics on the dataset so 14 dashboards still report one number.

### Visualization (canvas `c4`, 720×300)

Vertical bar chart: the same week's revenue as reported by three cloned dashboards, each with a slightly different inline definition.

- **Title (bold 15px, `#1a5276`, top center):** "One Week, Three Dashboards, Three 'Revenues'".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y runs $90,000 (baseline) to $102,000 (top), gridlines `#e5e9ef` at 94,000 / 98,000 with 12px `#444` labels "$94k" / "$98k"; 12px `#444` label "$90k" at the baseline.
- **Bars:** 3 bars, 120px wide, at x = 120, 300, 480; values `[100500, 96900, 95400]` scaled at 15px per $1,000 above the $90k baseline (heights 157.5 / 103.5 / 81); bar A fill `rgba(42,120,214,0.35)` with 2px `#2a78d6` top edge, bar B fill `rgba(217,89,38,0.25)` with 2px `#d95926` top edge, bar C fill `rgba(231,76,60,0.20)` with 2px `#e74c3c` top edge.
- **Value labels:** bold 12px above each bar in the bar's edge color: "$100,500", "$96,900", "$95,400".
- **Bar labels (11px `#444`, two lines under each bar):** "dashboard A / SUM(revenue)", "dashboard B / minus refunds", "dashboard C / minus refunds & test orders".
- **Annotation (bold 13px red `#e74c3c`, upper right near y=70):** "$5,100 spread — which one goes in the board deck?".
- **Fix note (bold 12px green `#008300`, centered near y=272):** "fix: one total_revenue metric on the dataset".
- **Caption (12px `#444`, bottom right):** "values illustrative; y-axis starts at $90k to show the spread".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); daily revenues, seat prices, server cost, refunds ($3,600), and test orders ($1,500) are invented and labeled illustrative; the weekly total $100,500, the 58% Sat-over-Tue ratio, the $4,500 / $9,000 / $13,500 yearly seat costs, and the $96,900 / $95,400 / $5,100-spread figures are exact arithmetic on those illustrative inputs. Product facts (Airbnb origin by the creator of Airflow, Apache Software Foundation, SQLAlchemy connectivity, SQL Lab, dataset metrics, result caching) are publicly documented Superset behavior.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
