# Scala

**Page type:** detail page (kusto-style two-column layout: text column left 45% with bullets, key-point callout and tag pills; viz column right 55% with one canvas plus a code block)
**HTML title tag:** Scala

**Subtitle:** OOP + FP on the JVM. Built for Spark, actors, and DSLs.

## Text column

- OOP + FP fusion — everything is an object, everything is a function
- Actor-based concurrency via Akka — message passing, no shared state
- Spark's native language — distributed data processing at scale
- Type system powerful enough to build internal DSLs

**Key point (red-left-border callout):** **Trade-off:** Complex type system and declining community in exchange for expressiveness and JVM interop.

**Tags (pills):** JVM (blue), functional (blue), Spark (green), Akka (blue), complex types (orange), declining (red)

## Viz column

### Visualization (canvas `pipeline`, 720×220)

Flow diagram: Spark DAG of lazy transformations as five rounded boxes connected left-to-right by arrows.

- **Title (bold 13px, top center at y=25, `#1a5276`):** "Spark DAG — Lazy Transformations".
- **Boxes:** width 90, height 44, at y=80, corner radius 6, white bold 12px centered labels:
  - "Source" at x=60, fill `#1a5276`
  - "map()" at x=200, fill `#27ae60`, stage label "Stage 1"
  - "filter()" at x=340, fill `#27ae60`, stage label "Stage 2"
  - "reduce()" at x=480, fill `#27ae60`, stage label "Stage 3"
  - "Sink" at x=620, fill `#1a5276`
- **Stage labels:** 11px `#666`, centered 20px below each staged box.
- **Arrows:** gray `#7f8c8d` lines (width 2) between consecutive boxes at vertical center, each ending in a filled triangular arrowhead.
- **Bottom annotation (10px `#95a5a6`, centered at y=190):** "RDD / DataFrame partitions flow lazily until action triggers execution".

### Code block

Below the canvas, `<pre><code>` (background `#f8f9fa`, padding 12px, radius 4px, 0.82rem, margin-top 12px, `overflow-x: auto`):

```scala
// Spark: distributed data pipeline
val sales = spark.read.parquet("s3://data/sales")

val result = sales
  .filter($"amount" > 100)
  .groupBy($"region")
  .agg(sum($"amount").as("total"))
  .orderBy($"total".desc)

// Pattern matching — exhaustive, type-safe
def describe(x: Any): String = x match {
  case i: Int if i > 0 => "positive"
  case s: String       => s"text: $s"
  case _               => "unknown"
}
```

## Regeneration instructions

- **Layout:** single `table.layout` with one `<tr>`: left `td.text-col` (45%) holds the bullet list, `.key-point` callout, and `.tags` pill row; right `td.viz-col` (55%) holds the canvas then the code block. Cell padding 12px, `vertical-align: top`.
- **Page style:** body system-ui/-apple-system sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with `border-bottom: 2px solid #2980b9` and 8px padding-bottom; subtitle `#666` 0.95rem, margin-bottom 32px; `ul` 0.92rem with 20px left margin; no nav bar, no back/home links.
- **Key-point style:** `.key-point` — background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, margin-top 12px, 0.9rem.
- **Tag pill style:** `.tags` flex-wrap with 8px gap, margin-top 20px; `.tag` padding 4px 10px, radius 4px, 0.78rem, weight 600. Colors: `.tag-green` background `#eafaf1` text `#1e8449`; `.tag-red` background `#fdedec` text `#c0392b`; `.tag-blue` background `#eaf2f8` text `#1a5276`; `.tag-orange` background `#fef9e7` text `#b7540c`.
- **Canvas:** `<canvas id="pipeline" height="220">`, CSS `width: 100%`, border `1px solid #e0e0e0`, radius 4px; backing store 720×220 scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, arrow gray `#7f8c8d`, annotation gray `#95a5a6`.
- In regenerated HTML, any card links use `.html` extensions.
