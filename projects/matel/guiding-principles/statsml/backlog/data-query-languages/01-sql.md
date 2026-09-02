# 1. SQL

**Page type:** detail page (backlog kusto-style 2-col text/viz layout: text left 45%, viz right 55%, one layout table per topic section)
**HTML title tag:** 1. SQL

**Subtitle:** State the result, let the optimiser choose the method

**Intro callout:** The pivotal idea — separate the question from the method. Because the query doesn't name an access path, the engine can change indexes, join order, and storage layout without rewriting queries.

## 1. How It Works

- A declarative language for querying and manipulating data in relational database management systems, based on relational algebra and tuple relational calculus
- Set-based operations on relations — you describe **what** data you want, not how to retrieve it
- The database engine decides execution strategy: access paths, indexes, join order
- Because queries never name an access path, storage layout can change without rewriting a single query

**Key point:** The query is a statement about the result, not a program about the steps — the optimiser owns the method.

### Visualization (canvas `c1`, 720×300)

Horizontal box-and-arrow pipeline flow diagram.

- **Title (bold 14px, centered, `#1a5276`):** "Query Execution Pipeline"
- **Boxes (6, in a row):** "SQL text", "Parser", "Logical\nplan", "Optimiser", "Physical\nplan", "Single-node\nexecutor". Boxes 56px tall at y=118, equal widths filling 720px minus 10px side margins with 24px gaps.
  - Default box style: fill `rgba(26,82,118,0.35)`, stroke `#1a5276` width 1, label text `#222` 13px.
  - "Optimiser" box: solid fill `#1a5276`, white label.
  - "Single-node executor" box: solid fill `#e67e22`, white label.
- **Arrows between boxes:** `#444` line width 1.5 with small filled triangle heads.
- **Caption (13px `#444`, centered below boxes):** "Engine chooses indexes, join order, and access paths — the query never names them"

## 2. Where It Fits

- **Strengths:** 50 years of optimization research behind every query plan; massive tooling ecosystem and a portable skill across hundreds of products
- **Strengths:** Joins, aggregations, subqueries, and window functions in a composable grammar
- **Weaknesses:** Awkward for time-series analysis without extensions; poor native nested/hierarchical data support
- **Weaknesses:** Vendor dialect fragmentation (MySQL vs PostgreSQL vs Oracle); procedural logic requires CTEs or window functions that read unnaturally
- **Use case:** Relational databases, data warehouses, analytics platforms, any structured tabular data — the default choice unless a specific constraint disqualifies it

*Illustration: departments with more than 10 orders since 2024-01-01, sorted by average order value.*

Code block (`pre`, in the viz column above the canvas):

```
SELECT d.name, COUNT(*) AS order_count, AVG(o.total) AS avg_order
FROM departments d
JOIN employees e ON e.dept_id = d.id
JOIN orders o ON o.employee_id = e.id
WHERE o.created_at >= '2024-01-01'
GROUP BY d.name
HAVING COUNT(*) > 10
ORDER BY avg_order DESC;
```

### Visualization (canvas `c2`, 720×300)

Vertical bar chart of the example query result.

- **Title (bold 14px, centered, `#1a5276`):** "Query Result — Avg Order Value by Department (sorted DESC)"
- **Data:** departments `['Enterprise', 'Consulting', 'Retail', 'Support']`, avg order values `[412, 355, 289, 198]`, order counts `[124, 86, 342, 57]`.
- **Bars:** width 96px, gap 60px, centered horizontally; baseline y=240, max bar height 180px scaled to max value 450. Fill `rgba(26,82,118,0.35)`, stroke `#1a5276` width 1.
- **Labels:** value above each bar as "$412" etc. in bold 13px `#1a5276`; department name below baseline in 13px `#222`; "<count> orders" (e.g. "124 orders") below that in `#444`.
- **Baseline:** thin `#999` horizontal line extending 20px past the bars on each side.

## Takeaway callout

**Takeaway:** Separating the question from the method let the engine evolve for fifty years without breaking a single query.

## Regeneration instructions

- **Layout:** backlog kusto-style detail page; the h1 carries the index number "1. SQL" matching the file index. Structure: h1, `.subtitle`, `.intro` callout, then one `.topic-section` per section (h2 with `2px solid #2980b9` bottom border), each containing a `table.layout` with one `<tr>`: left `td.text-col` (45%) with bullets/key-point/example, right `td.viz-col` (55%) with optional `pre` and a canvas. A final standalone `.key-point` takeaway after the sections.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with `2px solid #2980b9` bottom border; subtitle `#666` 0.95rem; `.intro` background `#f0f4f8`, left border `3px solid #2980b9`, padding 8px 12px, 0.9rem; `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem; `.example` italic `#555` 0.9rem; ul 0.92rem; `pre` background `#f4f4f4`, padding 12px 16px, radius 4px, 0.85rem. Canvas: `width: 100%`, border `1px solid #e0e0e0`, radius 4px. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange, bar fill `rgba(26,82,118,0.35)`, gray text `#444`/`#222`/`#999`.
- In regenerated HTML, any card/page links use .html extensions (this page has none).
