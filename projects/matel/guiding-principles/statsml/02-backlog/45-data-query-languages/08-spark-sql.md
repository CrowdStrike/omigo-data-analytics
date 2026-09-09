# 8. Spark SQL

**Page type:** detail page (two-column layout table per section: text left 45%, viz right 55%; intro callout; closing key-point)
**HTML title tag:** 8. Spark SQL

**Subtitle:** Declarative queries compiled to distributed plans — SQL that scales

## Intro callout

The correction to MapReduce. Keep a high-level declarative language, compile it to distributed work. Catalyst optimizer builds logical plans, optimizes them, and emits physical stages across the cluster — the programmer never specifies how.

## 1. How It Works

- A module in Apache Spark providing a SQL interface and DataFrame API, unified under one optimizer
- Catalyst builds logical plans, applies rule-based and cost-based optimization, then generates physical plans distributed across executors
- In-memory processing eliminates MapReduce's disk overhead between stages
- The same engine serves SQL, DataFrames, streaming, and ML

**Key-point callout:** **Key point:** Catalyst decides the distributed execution — the programmer states the query, never the stages.

### Visualization (canvas `c1`, 720×300)

Box-and-arrow pipeline (shared `drawFlow` helper): 6 equal-width boxes in a row at y=118, height 56, 24px gaps, spanning the width from x=10; `#444` arrows with triangular heads between boxes.

- **Title (bold 14px, `#1a5276`, top center, y=32):** "Query Execution Pipeline"
- **Boxes (13px text, multi-line where noted):**
  1. "SQL /" / "DataFrame" — fill `rgba(26,82,118,0.35)`, stroke `#1a5276`, text `#222`
  2. "Parser" — same light style
  3. "Logical" / "plan" — same light style
  4. "Catalyst" / "optimiser" — solid `#1a5276`, white text
  5. "Physical" / "plan" — light style
  6. "Distributed" / "stages" — solid `#e67e22`, white text
- **Caption (13px `#444`, centered below boxes):** "In-memory stages across cluster executors — no disk I/O between stages"

## 2. Where It Fits

- **Strengths:** Fast — in-memory processing avoids disk I/O between stages; a real, extensible optimizer (Catalyst) with rule-based and cost-based passes
- **Strengths:** Unified API — same engine for SQL, DataFrames, streaming (Structured Streaming), and ML; large community and ecosystem
- **Weaknesses:** JVM memory management complexity (GC pauses, OOM on skewed data); cluster resource tuning is non-trivial (executor memory, cores, partitions)
- **Weaknesses:** Shuffle-heavy operations still slow; small-data overhead adds latency for simple queries
- **Use case:** Large-scale ETL on data lakes, interactive analytics on petabyte datasets, streaming pipelines with exactly-once semantics, ML feature engineering at scale

*Illustration: completed transactions since 2024-01-01 aggregated per category — average vs 95th-percentile amount.*

**Code block (in viz column, above canvas `c2`):**

```
-- Spark SQL with Catalyst optimization
EXPLAIN EXTENDED
SELECT
    category,
    COUNT(*) as purchase_count,
    AVG(amount) as avg_amount,
    PERCENTILE_APPROX(amount, 0.95) as p95_amount
FROM transactions
WHERE event_date >= '2024-01-01'
  AND status = 'completed'
GROUP BY category
HAVING COUNT(*) > 100;

-- Catalyst produces:
-- 1. Logical Plan (parsed)
-- 2. Analyzed Plan (resolved references)
-- 3. Optimized Plan (predicate pushdown, column pruning)
-- 4. Physical Plan (hash aggregate, columnar scan)
```

### Visualization (canvas `c2`, 720×300)

Grouped vertical bar chart: avg vs p95 amount per category, with row counts and a legend.

- **Title (bold 14px, `#1a5276`, top center, y=28):** "Query Result — Avg vs P95 Amount by Category"
- **Data:** categories `['electronics', 'home', 'apparel', 'grocery']`, avg amounts `[86, 54, 41, 27]`, p95 amounts `[412, 265, 158, 74]`, row counts `[3120, 5480, 8930, 21400]`.
- **Scale:** y max 450; baseline y=240, max bar height 180px; group width 100px (two half-width bars per group), 48px gaps, group block centered then shifted 40px left.
- **Colors:** avg bar filled `rgba(26,82,118,0.35)` stroked `#1a5276`; p95 bar solid `#e67e22`.
- **Value labels:** "$N" above each bar — avg in 13px `#1a5276`, p95 in bold 13px `#e67e22` ($86/$412, $54/$265, $41/$158, $27/$74).
- **Category labels:** 13px `#222` 20px below baseline; "N rows" (3120 rows, 5480 rows, 8930 rows, 21400 rows) 13px `#444` 38px below baseline.
- **Baseline:** thin `#999` line extending 20px past the group block on both sides.
- **Legend (top right, x = width−125, 12×12 swatches at y=55 and y=75):** light-blue swatch with `#1a5276` stroke labeled "avg_amount"; orange `#e67e22` swatch labeled "p95_amount"; labels 13px `#222`.

## Closing key-point

**Takeaway:** Keep the declarative language, replace the execution — Catalyst decides how, the programmer never does.

## Regeneration instructions

- **Template/layout:** data-query-languages detail page. h1 with 2px `#2980b9` bottom border, `.subtitle`, one `.intro` callout, then one `.topic-section` per numbered section (h2 with 2px `#2980b9` bottom border), each containing a `table.layout` with one row: left `td.text-col` (45%) for bullets/key-point/example, right `td.viz-col` (55%) for optional `<pre>` block and canvas. A standalone `.key-point` div at the bottom holds the takeaway. Canvas `c1` is drawn with a reusable `drawFlow(id, title, boxes, caption)` box-and-arrow helper.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; `.subtitle` `#666` 0.95rem; `.intro` background `#f0f4f8`, left border `3px solid #2980b9`, padding 8px 12px, 0.9rem; `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem; `.example` italic `#555` 0.9rem; ul 0.92rem; `pre` background `#f4f4f4`, padding 12px 16px, radius 4px, 0.85rem.
- **Canvas:** intrinsic 720×300, CSS `width: 100%`, border `1px solid #e0e0e0`, radius 4px; scaled with `window.devicePixelRatio` via a shared `setup(id)` helper (backing store = rendered width × dpr, `ctx.scale` back to logical coordinates).
- **Palette:** `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange, bar fill `rgba(26,82,118,0.35)`, text `#222`/`#444`.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
