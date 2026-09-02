# 9. SQL on New Engines

**Page type:** detail page (kusto-style 2-col text/viz layout: intro callout, numbered h2 sections each with text left 45% / canvas or code right 55%)
**HTML title tag:** 9. SQL on New Engines

**Subtitle:** The language became the stable layer while engines were replaced underneath

**Intro callout:** Compatibility with SQL grammar is the main adoption strategy for new systems. The cost of a new language is retraining everyone. So innovate the engine, keep the interface. SQL is the stable API — engines are the implementation detail.

## 1. How It Works

- Modern cloud-native query engines (Trino/Presto, BigQuery, Snowflake, DuckDB, Databricks SQL) accept standard SQL but execute it on radically different architectures
- Serverless, separated storage/compute, federated across sources, or embedded in-process
- Same SQL grammar, different execution architectures underneath
- The language is the stable contract — the engine is replaceable

**Key point:** The SQL layer stayed fixed while the entire execution architecture underneath was swapped out, repeatedly.

### Visualization (canvas `c1`, 720×300)

Horizontal box-and-arrow pipeline flow.

- **Title (bold 14px, top center, `#1a5276`):** "Query Execution Pipeline".
- **Boxes:** 6 equal-width boxes in a row (gap 24px, left margin 10px, height 56px, top y=118), labels (with line breaks): "SQL text", "Parser", "Logical / plan" (two lines), "Optimiser" (solid fill `#1a5276`, white text), "Physical / plan" (two lines), "Pluggable / engine" (two lines, solid fill `#27ae60`, white text).
- **Non-solid boxes:** fill `rgba(26,82,118,0.35)`, stroke `#1a5276` 1px, text `#222` 13px system-ui.
- **Arrows:** between consecutive boxes, `#444`, line width 1.5, small filled triangular arrowheads.
- **Caption (13px `#444`, centered ~52px below boxes):** "Trino · BigQuery · Snowflake · DuckDB — same SQL layer, different execution underneath".

## 2. Where It Fits

- **Strengths:** Zero retraining cost — anyone who knows SQL is immediately productive; ecosystem compatibility (BI tools, JDBC/ODBC drivers, notebooks all work)
- **Strengths:** New engines inherit decades of SQL optimization research and compete on performance, not syntax
- **Weaknesses:** Vendor-specific extensions creep back (BigQuery's STRUCT syntax, Snowflake's VARIANT handling); performance differences hidden behind identical syntax
- **Weaknesses:** Migration still has edge cases (date functions, NULL handling, type coercion); cost models differ dramatically
- **Use case:** Cloud data warehouses, federated queries across heterogeneous sources, serverless analytics for variable workloads, lakehouse architectures, embedded analytics (DuckDB)

*Illustration: monthly revenue by region from the query's output — identical results whichever engine ran it.*

### Code block (in viz column, above canvas `c2`)

```
-- The same analytical query runs on all three engines:
-- BigQuery, Snowflake, Trino — same SQL, different execution

SELECT
    region,
    product_category,
    DATE_TRUNC('month', order_date) AS month,
    SUM(revenue) AS total_revenue,
    COUNT(DISTINCT customer_id) AS unique_customers
FROM orders
JOIN products ON orders.product_id = products.id
WHERE order_date >= '2024-01-01'
GROUP BY region, product_category, DATE_TRUNC('month', order_date)
ORDER BY total_revenue DESC;

-- Engine differences are invisible at this level:
-- BigQuery: serverless, slot-based execution
-- Snowflake: warehouse auto-scaling
-- Trino: federated across S3 + PostgreSQL + Kafka
```

### Visualization (canvas `c2`, 720×300)

Two-series line chart of monthly revenue.

- **Title (bold 14px, top center, `#1a5276`):** "Query Result — Monthly Revenue by Region (electronics)".
- **Plot area:** left=80, right=570, top=55, bottom=245; axes stroked `#999` 1px (left y-axis and bottom x-axis).
- **X-axis:** months `['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun']`, evenly spaced, labels 13px `#222` below axis.
- **Y-axis:** 0 to 3.0 ($M scale); labels "$0M", "$1M", "$2M", "$3M" in 11px `#888` right-aligned left of axis; horizontal gridlines `#eee` at $1M–$3M.
- **Series 1 (North America):** `[1.8, 2.0, 2.1, 2.3, 2.4, 2.6]` in `#1a5276`, line width 3, 4px-radius filled dots at each point.
- **Series 2 (EMEA):** `[1.1, 1.2, 1.3, 1.3, 1.5, 1.6]` in `#27ae60`, same style.
- **Legend (top right, x = w−125):** 12×12 color swatches with 13px `#222` labels: `#1a5276` "North America", `#27ae60` "EMEA".

## Takeaway (key-point callout, full width at page bottom)

**Takeaway:** Innovate the engine, keep the interface — SQL is the stable API and the engine is the implementation detail.

## Regeneration instructions

- **Layout:** backlog detail page. h1 with 2px `#2980b9` bottom border, `.subtitle`, then an `.intro` callout (background `#f0f4f8`, left border 3px solid `#2980b9`, padding 8px 12px, 0.9rem). Each numbered section is a `.topic-section` (margin-bottom 40px) with an h2 (1.3rem `#1a5276`, 2px `#2980b9` bottom border) and a `table.layout` (width 100%, border-collapse, td padding 12px): left `td.text-col` 45% holds bullets + `.key-point`, right `td.viz-col` 55% holds the canvas (section 2 also has a `<pre>` code block above its canvas). A final full-width `.key-point` takeaway sits after the sections. The h1 carries the index number "9." matching the file index.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `.key-point` background `#f8f9fa` with left border 3px solid `#e74c3c`, 0.9rem; `.example` italic `#555` 0.9rem; ul 0.92rem; `<pre>` background `#f4f4f4`, padding 12px 16px, radius 4px, 0.85rem. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300, CSS `width: 100%` with 1px `#e0e0e0` border and 4px radius; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar/box fill `rgba(26,82,118,0.35)`.
- Any card links in regenerated HTML use `.html` extensions.
