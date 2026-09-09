# 5. MapReduce

**Page type:** detail page (two-column layout table per section: text left 45%, viz right 55%; intro callout; closing key-point)
**HTML title tag:** 5. MapReduce

**Subtitle:** The one reversal — scale required giving up the optimiser

## Intro callout

Distribute work by writing explicit map and reduce functions. You become the optimizer. The lesson learned: this approach was wrong — expressing intent declaratively is more valuable than controlling execution manually.

## 1. How It Works

A programming model (Google, 2004) for processing large datasets across clusters of commodity machines.

- **Two functions:** the programmer writes map() to emit key-value pairs and reduce() to aggregate values by key
- **Framework does the rest:** partitions, shuffles, sorts, distributes, and schedules work across nodes
- **Fault tolerance:** deterministic re-execution of failed tasks

**Key-point callout:** **You are the optimizer:** the framework automates distribution, but there is no query planner — every stage of the computation is hand-planned by the programmer.

### Visualization (canvas `c1`, 720×300)

Box-and-arrow diagram: MapReduce execution flow with fan-out/fan-in stages.

- **Title (bold 14px, `#1a5276`, top center, y=25):** "MapReduce Execution Flow"
- **Layout:** 5 columns, box width 110px, 40px gap, centered horizontally.
- **Stages (13px white text in boxes):**
  1. "Input" / "splits" — blue `#1a5276` box, 110×56 at y=125
  2. Three parallel "map()" boxes — blue `#1a5276`, 110×40 at y=75, 135, 195
  3. "Shuffle" / "& sort" — orange `#e67e22` box, 110×56 at y=125
  4. Two parallel "reduce()" boxes — blue `#1a5276`, 110×44 at y=100, 160
  5. "Output" / "HDFS" — green `#27ae60` box, 110×56 at y=125
- **Arrows (`#444`, 1.5px, triangular heads):** fan-out from Input to each map(); fan-in from each map() to Shuffle; fan-out from Shuffle to each reduce(); fan-in from each reduce() to Output. Mid y=153.
- **Stage captions (12px `#444`, centered above each column at y=62):** "split input", "parallel map", "by key", "parallel reduce", "write output"
- **Bottom annotation (bold 13px red `#e74c3c`, center, y=285):** "You hand-plan every stage — you are the optimiser"

## 2. Where It Fits

- **Strength:** horizontal scaling on commodity hardware (thousands of nodes), proven at Google/Yahoo scale for batch workloads
- **Strength:** simple mental model — just two functions — with fault tolerance through deterministic re-execution
- **Weakness:** you are the optimizer — no query planner, no automatic join strategies
- **Weakness:** verbose Java boilerplate, batch-only high latency, no iterative processing (each step writes to disk), expensive intermediate shuffle
- **Use case (historical):** early Hadoop batch ETL (2004-2012), web indexing at Google, log processing, large-scale text analysis — largely replaced by higher-level frameworks, but the distribution model persists underneath

*Example: the canonical word count — map emits (word, 1) for every word, reduce sums the counts per word.*

**Code block (in viz column, above canvas `c2`):**

```
// Word Count — the canonical MapReduce example

map(key: filename, value: file_contents):
    for each word in file_contents:
        emit(word, 1)

reduce(key: word, values: list_of_counts):
    emit(word, sum(values))

// Framework handles: splitting input, distributing map tasks,
// shuffling by key, sorting, distributing reduce tasks,
// writing output, retrying failures
```

### Visualization (canvas `c2`, 720×300)

Vertical bar chart: word count output, one bar per key.

- **Title (bold 14px, `#1a5276`, top center, y=25):** "Word Count Output — emit(word, sum(values))"
- **Data:** words `['the', 'data', 'map', 'reduce', 'key', 'value']`, counts `[42, 27, 19, 15, 11, 8]`.
- **Scale:** max 45; padding top 50, bottom 55, left 60, right 40; slot width = plot width / 6, bar width 55% of slot.
- **Colors:** all bars `rgba(26,82,118,0.35)`; count value bold 13px `#222` above each bar; word label 13px `#222` below (32px from bottom).
- **Baseline:** thin `#999` line across the plot bottom.
- **Caption (12px `#444`, bottom center, 10px from bottom):** "one bar per key after the shuffle groups all (word, 1) pairs"

## Closing key-point

**The meta-point:** MapReduce automated distribution but made the programmer the optimiser — and hand-planned execution ultimately lost to declarative languages where the optimiser plans the path.

## Regeneration instructions

- **Template/layout:** data-query-languages detail page. h1 with 2px `#2980b9` bottom border, `.subtitle`, one `.intro-callout`, then one `.section` per numbered section (h2 with 2px `#2980b9` bottom border), each containing a `table.layout` with one row: left `td.text-col` (45%) for paragraph/bullets/key-point/example, right `td.viz-col` (55%) for optional `<pre><code>` block and canvas. A standalone `.key-point` div at the bottom holds the meta-point.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; `.subtitle` `#666` 0.95rem; `.intro-callout` background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.95rem; `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem; `.example` italic `#555` 0.9rem; ul 0.92rem; `pre` background `#f4f4f4`, padding 12px, radius 4px, 0.85em.
- **Canvas:** intrinsic 720×300, CSS `width: 100%`, border `1px solid #e0e0e0`, radius 4px; scaled with `window.devicePixelRatio` via a shared `setup(id)` helper (backing store = rendered width × dpr, `ctx.scale` back to logical coordinates).
- **Palette:** `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange, bar fill `rgba(26,82,118,0.35)`, text `#222`/`#444`.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
