# Data Query Languages

**Page type:** grid page (nav-card grid, auto-fit columns min 300px, cards with category label + topic tags)
**HTML title tag:** Data Query Languages — Discussion Backlog

**Subtitle:** Each language kind embeds a different assumption — top-down declaration vs bottom-up pipeline, optimiser-driven vs hand-planned, condition-based vs resemblance-based.

## Cards

Each card links to a detail page under `data-query-languages/`. The card shows a colored uppercase category label (`.card-num`), a numbered title, a one-to-two sentence description, and a row of topic tags.

| # | Category | Title | Link | Description | Topic tags |
|---|----------|-------|------|-------------|------------|
| 1 | DECLARATIVE | SQL | [data-query-languages/01-sql.md](data-query-languages/01-sql.md) | Top-down, declarative. State the result, let the optimiser find the path. Outlived every engine underneath it. | top-down, set-based, ANSI standard |
| 2 | PIPELINE | Splunk SPL | [data-query-languages/02-splunk-spl.md](data-query-languages/02-splunk-spl.md) | Bottom-up, pipe-based. Narrow a stream stage by stage — each operator transforms the previous output. | bottom-up, time-ordered, schema-on-read |
| 3 | PIPELINE | Kusto KQL | [data-query-languages/03-kusto-kql.md](data-query-languages/03-kusto-kql.md) | Azure's pipe-forward language for telemetry at scale. SPL's paradigm with stricter typing and a real optimiser. | bottom-up, pipe-forward, telemetry |
| 4 | PIPELINE | LogScale (Humio) | [data-query-languages/04-logscale.md](data-query-languages/04-logscale.md) | Real-time log search with streaming aggregation. The bet: brute-force search on compressed data beats maintaining indexes. | bottom-up, streaming, no pre-indexing |
| 5 | HAND-PLANNED | MapReduce | [data-query-languages/05-mapreduce.md](data-query-languages/05-mapreduce.md) | Hand-planned distributed execution. You are the optimiser. The one clear reversal — proved wrong. | bottom-up, hand-planned, retired |
| 6 | DATAFLOW | Pig Latin | [data-query-languages/06-pig-latin.md](data-query-languages/06-pig-latin.md) | Bottom-up dataflow DAGs. Transitional — proved SQL should compile to distributed plans, then was replaced by exactly that. | bottom-up, dataflow, retired |
| 7 | DECLARATIVE | HiveQL | [data-query-languages/07-hiveql.md](data-query-languages/07-hiveql.md) | SQL-like grammar compiled to MapReduce jobs — brought SQL users back to Hadoop. Retraining cost, not technology, is the adoption barrier. | top-down, hadoop, transitional |
| 8 | DECLARATIVE | Spark SQL | [data-query-languages/08-spark-sql.md](data-query-languages/08-spark-sql.md) | The correction to MapReduce: declarative queries compiled to distributed plans by the Catalyst optimiser. | top-down, catalyst, distributed |
| 9 | DECLARATIVE | SQL on New Engines (Trino, BigQuery) | [data-query-languages/09-modern-sql-engines.md](data-query-languages/09-modern-sql-engines.md) | The language became the stable layer while engines were replaced underneath. SQL is the API; engines are implementation detail. | top-down, cloud, stable interface |
| 10 | RESEMBLANCE | Vector Similarity Search | [data-query-languages/10-vector-similarity.md](data-query-languages/10-vector-similarity.md) | Retrieval by resemblance, not by condition. Closest matches — no guarantee they qualify. | embeddings, ANN, RAG |
| 11 | GENERATED | NL → Generated SQL | [data-query-languages/11-nl-to-sql.md](data-query-languages/11-nl-to-sql.md) | Plain language as input — still compiles to SQL. Failure mode: runs cleanly, answers a different question. | LLM, text-to-SQL, generated |

## Regeneration instructions

- **Template:** nav-grid style. Single page: h1, `.subtitle` paragraph, then one `.nav-grid` of `.nav-card` anchors. No callout box.
- **Layout:** `.nav-grid` is CSS grid, `repeat(auto-fit, minmax(300px, 1fr))`, 16px gap.
- **Links:** the table above links to `.md` versions for markdown navigation; in the regenerated HTML, each card's `href` is the same path with an `.html` extension (relative, e.g. `data-query-languages/01-sql.html`).
- **Card structure:** `<a class="nav-card" href="...">` containing `<div class="card-num" style="color:CATEGORY_COLOR">CATEGORY</div>`, `<h3>N. Title</h3>` (unpadded index matching the file index), `<p>description</p>`, then `<div class="topics">` with one `<span class="topic-tag">` per tag.
- **Category label colors:** DECLARATIVE `#1a5276`; PIPELINE `#e67e22`; HAND-PLANNED and DATAFLOW `#95a5a6`; RESEMBLANCE `#8e44ad`; GENERATED `#27ae60`.
- **Card style:** background `#ffffff`, border `1px solid #d8d8d8`, radius 10px, padding 20px, shadow `0 2px 4px rgba(0,0,0,0.05)`; hover: border `#2980b9`, `translateY(-2px)`. `.card-num` 0.75em bold; h3 `#1a3a4a` 1em; description 0.85em `#555`.
- **Topic tags:** background `#f0f0f0`, border `1px solid #ccc`, radius 4px, padding 2px 6px, 0.7em `#666`; tags wrap in a flex row with 4px gap.
- **Page CSS:** body -apple-system/Segoe UI/Roboto sans-serif, background `#f5f5f0`, text `#2a2a2a`, padding 40px, line-height 1.6; h1 1.8em `#2980b9`; subtitle `#666` 1.05em. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`. No canvases on this page; detail pages' canvases use `window.devicePixelRatio` scaling.
