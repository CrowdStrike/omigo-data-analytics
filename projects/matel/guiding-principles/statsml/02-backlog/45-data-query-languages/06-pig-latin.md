# 6. Pig Latin

**Page type:** detail page (two-column layout table per section: text left 45%, viz right 55%; intro callout; closing key-point)
**HTML title tag:** 6. Pig Latin

**Subtitle:** Dataflow DAGs for Hadoop — the transitional language between MapReduce and SQL-on-cluster.

## Intro callout

**Core trade-off:** Higher-level than MapReduce but lower than SQL. You describe dataflow steps and the system compiles to MapReduce jobs. Proved the concept — then was replaced by exactly what it proved.

**Bottom-up vs top-down:** SQL is top-down — you declare the final result and the optimizer works backward to find the path. Pig Latin is bottom-up — you build the result step by step, each line transforming the previous output. Like Splunk SPL, you think in a pipeline: load → filter → group → project → store. The programmer sees the data flow; in SQL, only the optimizer does.

## 1. How It Works

A dataflow language developed at Yahoo (2008) that compiles procedural data transformation scripts into sequences of MapReduce jobs on Hadoop.

- **Operators:** you write LOAD, FILTER, GROUP, FOREACH, JOIN, STORE — the compiler plans the MapReduce stages
- **Explicit data flow, implicit parallelism:** the compiler sees the full DAG and can merge stages where possible
- **Pipeline thinking:** each statement transforms the previous output, so you see the data at every stage

**Key-point callout:** **Why it mattered:** proved that a higher-level abstraction over MapReduce was viable and necessary — paving the way for SQL-on-Hadoop.

### Visualization (canvas `c1`, 720×300)

Two-part diagram: compilation flow on top, top-down vs bottom-up paradigm comparison below.

- **Title (bold 14px, `#1a5276`, top center, y=25):** "Pig Latin Compilation Flow"
- **Top row (3 boxes, 190×52 at y=45, 45px gap, centered; two-line 13px white text; `#444` arrows between):**
  1. "Pig Latin script" / "LOAD · FILTER · GROUP · STORE" — blue `#1a5276`
  2. "Dataflow DAG" / "relational operators" — orange `#e67e22`
  3. "Compiled MapReduce jobs" / "run on Hadoop cluster" — green `#27ae60`
- **Paradigm headings (bold 13px, y=150):** "Top-down (SQL)" in `#1a5276` centered at x=185; "Bottom-up (Pig)" in `#e67e22` centered at x=548.
- **SQL side (row at y=168, height 36):** solid blue box "SELECT …" (95px wide at x=20) → arrow → dashed-border box "optimizer (hidden)" (125px wide at x=142, fill `rgba(26,82,118,0.12)`, stroke `#1a5276` dash 4/3, 12px `#1a5276` text) → arrow → solid green box "Result" (62px wide at x=288).
- **Pig side:** 5 chained orange `#e67e22` boxes (56px wide, 14px gaps, starting x=380, 11px white text) labeled "LOAD", "FILTER", "GROUP", "FOREACH", "STORE", with arrows between.
- **Captions (12px `#444`, y=228):** "you never see intermediate steps" under the SQL side; "you see data at every stage" under the Pig side.
- **Bottom annotation (bold 13px red `#e74c3c`, center, y=270):** "The transitional layer — higher than MapReduce, lower than SQL"

## 2. Where It Fits

- **Strength:** much easier than raw MapReduce for multi-step pipelines — a ~200-line Java job became ~10 lines of Pig
- **Strength:** handles nested data (bags, tuples, maps) naturally; extensible via User Defined Functions (UDFs)
- **Weakness:** no optimizer in early versions — execution order was literal; another language to learn (neither SQL nor Java)
- **Weakness:** batch only, no interactive queries; made redundant by HiveQL and then Spark SQL — effectively abandoned by 2017
- **Use case (historical, 2008-2014):** Hadoop ETL pipelines at Yahoo, letting analysts process web-scale data without raw Java MapReduce — retired once SQL itself could compile to distributed plans

*Example: process web server access logs to find the URLs with the most 500-errors.*

**Code block (in viz column, above canvas `c2`):**

```
-- Load web server logs
logs = LOAD '/data/access_logs'
    USING PigStorage('\t')
    AS (ip:chararray, ts:chararray,
        method:chararray, url:chararray,
        status:int, bytes:int);

-- Keep only server errors
errors = FILTER logs BY status >= 500;

-- Group by URL
by_url = GROUP errors BY url;

-- Count errors per URL
counts = FOREACH by_url GENERATE
    group AS url,
    COUNT(errors) AS error_count;

-- Top offenders
ranked = ORDER counts BY error_count DESC;
top20 = LIMIT ranked 20;

-- Write output
STORE top20 INTO '/output/error_hotspots';
```

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart: 500-errors per URL, top 5, sorted descending.

- **Title (bold 14px, `#1a5276`, top center, y=25):** "Example Output — 500-Errors per URL (top of error_hotspots)"
- **Data:** urls `['/api/checkout', '/api/search', '/login', '/api/cart', '/home']`, counts `[143, 97, 58, 41, 12]`.
- **Scale:** max 150; label column 150px, right padding 70px; bars 30px tall with 12px gaps, starting at y=50.
- **Colors:** bars `rgba(26,82,118,0.35)`; url labels right-aligned 13px `#222` left of bars; count values bold 13px `#222` right of each bar.
- **Baseline:** vertical thin `#999` line at x=150 spanning the bars.
- **Caption (12px `#444`, bottom center, 10px from bottom):** "rows written by STORE top20 INTO '/output/error_hotspots'"

## Closing key-point

**The meta-point:** Pig Latin proved a higher-level abstraction over MapReduce was viable — and once SQL itself compiled to distributed plans, the hand-visible pipeline lost to the optimiser-driven declarative approach it had paved the way for.

## Regeneration instructions

- **Template/layout:** data-query-languages detail page. h1 with 2px `#2980b9` bottom border, `.subtitle`, one `.intro-callout` (two paragraphs separated by `<br><br>`), then one `.section` per numbered section (h2 with 2px `#2980b9` bottom border), each containing a `table.layout` with one row: left `td.text-col` (45%) for paragraph/bullets/key-point/example, right `td.viz-col` (55%) for optional `<pre><code>` block and canvas. A standalone `.key-point` div at the bottom holds the meta-point.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; `.subtitle` `#666` 0.95rem; `.intro-callout` background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.95rem; `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem; `.example` italic `#555` 0.9rem; ul 0.92rem; `pre` background `#f4f4f4`, padding 12px, radius 4px, 0.85em.
- **Canvas:** intrinsic 720×300, CSS `width: 100%`, border `1px solid #e0e0e0`, radius 4px; scaled with `window.devicePixelRatio` via a shared `setup(id)` helper (backing store = rendered width × dpr, `ctx.scale` back to logical coordinates).
- **Palette:** `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange, bar fill `rgba(26,82,118,0.35)`, text `#222`/`#444`.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
