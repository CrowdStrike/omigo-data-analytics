# Data Processing Paradigms — Runtime Awareness and Execution Modes

**Page type:** detail page (backlog-style: intro callout, numbered h2 sections, two-column layout table with text left ~45% and canvas right ~55%)
**HTML title tag:** Data Processing Paradigms — Discussion Backlog

**Subtitle:** Hard constraints that execution paradigms impose on statistical profiling

**Intro callout:** Statistical profiling pipelines execute within specific computation paradigms that impose hard constraints on what's possible. A pipeline assuming all data fits in memory will silently produce wrong results on distributed data.

## 1. Execution Paradigms

- **Single-machine notebook** (pandas, R): All in RAM. Full random access. Exact statistics. Hard ceiling ~10-50GB
- **MapReduce / batch** (Spark, Hadoop): Partitioned across nodes. No global ordering. Aggregations need shuffle. Not all stats are reducible
- **Streaming** (Kafka, Flink, Beam): Data arrives continuously. No "full dataset." Approximate statistics (HyperLogLog, t-digest)
- **Actor-model** (Akka, Erlang, Ray): No shared state. Results async/out-of-order. Must be combinable (monoid structure)
- **Mini-batch** (online learning): Small chunks sequential. Running stats (Welford's). Exponential decay. Profiles evolve

### Visualization (canvas `c1`, 720×300)

Capabilities matrix grid (paradigms × properties) with colored check/cross/tilde symbols, on light gray `#f8f9fa` background.

- **Title (bold 15px, `#1a5276`, top center):** "Execution Paradigm Capabilities Matrix"
- **Rows (two-line labels, right-aligned 13px `#2c3e50`):** "Notebook (pandas/R)", "MapReduce (Spark)", "Streaming (Flink)", "Actor-model (Ray)", "Mini-batch (online)"
- **Columns (two-line bold 13px `#1a5276` headers):** "Random Access", "Exact Stats", "Fault Tolerant", "Scalable", "Ordered"
- **Grid** starts at x=140, y=75 approx; cells 100×45. Cell values (1 = green check `#27ae60` on `rgba(39,174,96,0.15)`; 0 = red cross `#e74c3c` on `rgba(231,76,60,0.1)`; 0.5 = orange tilde `#e67e22` on `rgba(230,126,34,0.12)`; symbols bold 19px; `#ddd` cell borders):
  - Notebook: `[1, 1, 0, 0, 1]`
  - MapReduce: `[0, 0.5, 1, 1, 0]`
  - Streaming: `[0, 0, 1, 1, 0.5]`
  - Actor: `[0, 0.5, 1, 1, 0]`
  - Mini-batch: `[0.5, 0, 0.5, 0.5, 1]`
- **Legend (13px, y=278):** green "✓ = Full support" at x=200, orange "~ = Partial/approximate" at x=350, red "✗ = Not available" at x=530.

## 2. Storage and Eventual Consistency

- **Object storage (S3/GCS/Blob):** Object storage, not FS. No append. Eventual consistency on LIST
- **HDFS:** Block-level distribution. Sequential reads fast, random impossible
- **Long-term archival:** Retrieval minutes-hours. Must plan one-pass computation
- **Delta Lake / Iceberg / Hudi:** ACID on object storage. Time travel. Schema evolution

Second bullet list:

- Write not immediately visible to all readers
- LIST operations stale — may miss files or see deleted ones
- Cross-partition writes not atomic

**Eventual consistency is the silent profiling killer** (red-bordered key-point callout): Histogram may have missing bins, undercounted tails, phantom spikes.

### Visualization (canvas `c2`, 720×300)

Timeline diagram with a highlighted danger zone and impact bullet list, on light gray `#f8f9fa` background.

- **Title (bold 15px, `#1a5276`, top center):** "Eventual Consistency: The Danger Zone for Profiling"
- **Timeline:** horizontal `#2c3e50` width-2 line at y=100 from x=60 to x=660 with filled arrowhead; 13px `#555` label "time →" near the right end.
- **Events** (colored tick marks ±15px with bold 13px two-line labels above): x=150 green `#27ae60` "T1: Write completes"; x=350 orange `#e67e22` "T2: LIST returns stale results"; x=550 green `#27ae60` "T3: Data finally visible to all".
- **Danger zone:** rectangle (150, y+20, 400×50), fill `rgba(231,76,60,0.12)`, dashed `#e74c3c` border (dash 4/3, width 1.5); bold 15px red centered "DANGER ZONE" and 13px red "Profiling here = wrong results (missing data, stale counts)".
- **Impact list** below (13px, "Impact on profiling:" in `#2c3e50` at (60,200), then red `#e74c3c` bullets indented at x=80):
  - Histogram: missing bins from unread partitions
  - Count: underreported N (stale LIST)
  - Percentiles: biased by missing tail data
  - Shape detection: phantom spikes from partial reads

## 3. What Pipeline Needs to Know

- **Reducibility check:** Is this stat computable in one distributed pass?
- **Ordering assumptions:** Does this test need ordered data?
- **Memory bounds:** Does this need full distribution materialized?
- **Consistency guarantee:** Read-after-write? Eventual? Max staleness?
- **Fault tolerance:** Can computation resume from checkpoint?

**Key Questions** (red-bordered key-point callout):
(1) Auto-detect runtime or explicit declaration?
(2) Minimum window for valid streaming profile?
(3) "Requires exact" vs "has approximate variant"?
(4) Compatibility matrix: test x paradigm x validity?
(5) "Wait and retry" vs "proceed with CI"?
(6) Lineage interaction with distributed execution?

### Visualization (canvas `c3`, 720×300)

Two-column reducible vs non-reducible statistics comparison, on light gray `#f8f9fa` background.

- **Title (bold 15px, `#1a5276`, top center):** "Statistic Reducibility: What Can Be Computed Distributed?"
- **Left column** (x=50, y=60, 250×155, fill `rgba(39,174,96,0.08)`, stroke `#27ae60` width 1.5), bold green 14px header "REDUCIBLE (one pass OK)"; 13px items, each with green ✓ then `#2c3e50` text: "mean (sum/count)", "count", "min / max", "variance (Welford's)", "sum", "boolean any/all".
- **Right column** (x=400, y=60, 250×155, fill `rgba(231,76,60,0.08)`, stroke `#e74c3c` width 1.5), bold red 14px header "NON-REDUCIBLE (needs collect)"; 13px items, each with red ✗ then `#2c3e50` text: "median", "mode", "KDE (kernel density)", "exact percentiles", "KS test statistic", "histogram (exact bins)".
- **Side annotation** (orange `#e67e22`): short arrow to the right of the non-reducible column with 12px stacked labels "Need" / "collect-" / "to-driver".

## Regeneration instructions

- **Layout:** backlog detail-page style. `<h1>` (2rem, `#1a5276`, 2px solid `#2980b9` bottom border), `.subtitle` (`#666`, 0.95rem), `.intro` callout (background `#f0f4f8`, left border `3px solid #2980b9`, padding 8px 12px, 0.9rem). Each section is a `.lang-section` (40px bottom margin) with an `<h2>` (1.3rem `#1a5276`, 2px solid `#2980b9` bottom border) and a `table.layout` with one `<tr>`: left `td.text-col` (45%) with bullets/callouts, right `td.viz-col` (55%) with the canvas.
- **Callouts:** `.key-point` — background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; `ul` 0.92rem with 20px left margin; canvases `width: 100%` with `1px solid #e0e0e0` border, 4px radius.
- **Palette:** `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- **Canvas:** declare intrinsic `width`/`height` attributes as given; scale by `window.devicePixelRatio` via a shared `setupCanvas(id)` helper (`ctx.scale` back to logical coordinates).
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
