# 7. HiveQL

**Page type:** detail page (two-column layout table per section: text left 45%, viz right 55%; intro callout; closing key-point)
**HTML title tag:** 7. HiveQL

**Subtitle:** SQL-like grammar over MapReduce — brought SQL users back to Hadoop

## Intro callout

Don't force SQL users to learn MapReduce. Give them familiar grammar, compile it to distributed jobs underneath. The retraining cost of a new language is the real barrier to adoption — not technical limitations.

## 1. How It Works

- A SQL-like query language for Apache Hive that compiles queries into MapReduce (later Tez/Spark) jobs over data stored in HDFS
- Developed at Facebook (2009) to make Hadoop accessible to analysts
- Schema-on-read — data is stored as files, schema is applied at query time via a metastore
- Familiar grammar eliminates retraining — analysts never see the distributed jobs underneath

**Key-point callout:** **Key point:** The grammar is the adoption strategy — SQL syntax on top, distributed batch jobs underneath.

### Visualization (canvas `c1`, 720×300)

Box-and-arrow pipeline (shared `drawFlow` helper): 6 equal-width boxes in a row at y=118, height 56, 24px gaps, spanning the width from x=10; `#444` arrows with triangular heads between boxes.

- **Title (bold 14px, `#1a5276`, top center, y=32):** "Query Execution Pipeline"
- **Boxes (13px text, multi-line where noted):**
  1. "HiveQL text" — fill `rgba(26,82,118,0.35)`, stroke `#1a5276`, text `#222`
  2. "Parser" — same light style
  3. "Logical" / "plan" — same light style
  4. "Optimiser" — solid `#1a5276`, white text
  5. "Physical" / "plan" — light style
  6. "MapReduce /" / "Tez jobs" — solid `#e67e22`, white text
- **Caption (13px `#444`, centered below boxes):** "Batch jobs over HDFS files — schema applied at read time via the metastore"

## 2. Where It Fits

- **Strengths:** Familiar SQL syntax for analysts who refused to write Java; schema-on-read flexibility; UDFs for custom logic
- **Strengths:** Partitioning and bucketing for performance on massive HDFS datasets (petabyte scale); the Hive Metastore became a shared catalog
- **Weaknesses:** High latency — batch-oriented, minutes for simple queries; no real-time or interactive queries
- **Weaknesses:** Limited SQL compliance (no UPDATE/DELETE originally); metastore adds operational complexity; query optimization was primitive initially
- **Use case:** Data warehouse queries and batch reporting/ETL on Hadoop clusters; ad-hoc analysis by SQL-trained analysts needing HDFS data without writing MapReduce

*Illustration: event counts by type for one day's business hours, reading only the pruned partitions.*

**Code block (in viz column, above canvas `c2`):**

```
-- Partitioned table for efficient time-range queries
CREATE EXTERNAL TABLE web_events (
    user_id STRING,
    event_type STRING,
    page_url STRING,
    duration_sec INT
)
PARTITIONED BY (dt STRING, hour INT)
STORED AS PARQUET
LOCATION '/data/warehouse/web_events';

-- Query with partition pruning
SELECT event_type, COUNT(*) as event_count,
       AVG(duration_sec) as avg_duration
FROM web_events
WHERE dt = '2024-03-15' AND hour BETWEEN 9 AND 17
GROUP BY event_type
HAVING COUNT(*) > 1000
ORDER BY event_count DESC;
```

### Visualization (canvas `c2`, 720×300)

Vertical bar chart: event counts by type with average duration labels.

- **Title (bold 14px, `#1a5276`, top center, y=28):** "Query Result — Event Count by Type, 2024-03-15 hours 9-17 (sorted DESC)"
- **Data:** labels `['page_view', 'click', 'search', 'add_to_cart', 'purchase']`, counts `[48200, 21400, 9800, 3600, 1400]`, avg durations `[34, 12, 48, 21, 95]` (seconds).
- **Scale:** y max 52000; baseline y=240, max bar height 180px; bars 84px wide, 48px gaps, group centered.
- **Colors:** bars filled `rgba(26,82,118,0.35)`, stroked `#1a5276` 1px.
- **Labels:** count above each bar as bold 13px `#1a5276` in "N.Nk" format (48.2k, 21.4k, 9.8k, 3.6k, 1.4k); event type 13px `#222` 20px below baseline; "avg Ns" (avg 34s, avg 12s, avg 48s, avg 21s, avg 95s) 13px `#444` 38px below baseline.
- **Baseline:** thin `#999` line extending 20px past the bar group on both sides.

## Closing key-point

**Takeaway:** Familiar grammar over a new engine — retraining cost, not technical limitation, is the real barrier to adoption.

## Regeneration instructions

- **Template/layout:** data-query-languages detail page. h1 with 2px `#2980b9` bottom border, `.subtitle`, one `.intro` callout, then one `.topic-section` per numbered section (h2 with 2px `#2980b9` bottom border), each containing a `table.layout` with one row: left `td.text-col` (45%) for bullets/key-point/example, right `td.viz-col` (55%) for optional `<pre>` block and canvas. A standalone `.key-point` div at the bottom holds the takeaway. Canvas `c1` is drawn with a reusable `drawFlow(id, title, boxes, caption)` box-and-arrow helper.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; `.subtitle` `#666` 0.95rem; `.intro` background `#f0f4f8`, left border `3px solid #2980b9`, padding 8px 12px, 0.9rem; `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem; `.example` italic `#555` 0.9rem; ul 0.92rem; `pre` background `#f4f4f4`, padding 12px 16px, radius 4px, 0.85rem.
- **Canvas:** intrinsic 720×300, CSS `width: 100%`, border `1px solid #e0e0e0`, radius 4px; scaled with `window.devicePixelRatio` via a shared `setup(id)` helper (backing store = rendered width × dpr, `ctx.scale` back to logical coordinates).
- **Palette:** `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange, bar fill `rgba(26,82,118,0.35)`, text `#222`/`#444`.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
