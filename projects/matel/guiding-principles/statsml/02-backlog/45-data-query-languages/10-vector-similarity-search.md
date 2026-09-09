# 10. Vector Similarity Search

**Page type:** detail page (kusto-style 2-col text/viz layout: intro callout, numbered h2 sections each with text left 45% / canvas or code right 55%)
**HTML title tag:** 10. Vector Similarity Search

**Subtitle:** Retrieval by resemblance rather than by stated condition

**Intro callout:** Not a query language in the traditional sense. You don't state conditions — you provide an example and get back the closest matches. No guarantee any of them actually qualify for your intent. Different in kind from declarative querying.

## 1. How It Works

- **Encode:** Data is encoded as high-dimensional vectors (embeddings) using embedding models
- **Retrieve:** Queries find nearest neighbors by geometric distance — cosine similarity, Euclidean (L2), or dot product
- **No conditions:** No boolean predicates — similarity is a continuous score, not a binary match
- **Speed trade-off:** Approximate nearest neighbor (ANN) indexes trade precision for speed

**Key point:** Retrieval is by resemblance, not predicate — the closest matches carry no guarantee that any of them satisfy your condition.

### Visualization (canvas `c1`, 720×300)

2D scatter of an embedding space with a query point and a nearest-neighbor radius circle.

- **Title (bold 14px, top center, `#1a5276`):** "Embedding Space: Nearest ≠ Qualifying".
- **Query point:** orange (`#e67e22`) diamond (10px half-diagonal) at (240, 170), labeled "query" in 13px orange to its right.
- **Top-k radius circle:** dashed (dash 5/4) orange circle, radius 88, line width 1.5, centered on the query; label "top-k radius" in 13px `#444` just above the circle's top-right.
- **Background points (not retrieved):** 14 dots, 5px radius, fill `rgba(26,82,118,0.35)`, at coordinates: (70,80), (110,230), (150,120), (430,75), (490,210), (560,110), (600,250), (650,80), (670,170), (400,265), (560,60), (130,265), (340,60), (490,140).
- **Neighbors inside circle that fit intent (green `#27ae60`, 6px dots):** (205,125), (295,140), (190,200).
- **Neighbors inside circle that are off-intent (red `#e74c3c`, 6px dots):** (285,215), (235,240).
- **Annotation:** red line (width 1.2) from (291,218) to (380,258); bold 13px red text at (386,263): "close, but satisfies no condition".
- **Legend (left-aligned at x=470, 12×12 swatches, 13px `#222` labels):** green "retrieved & fits intent"; red "retrieved, off-intent"; `rgba(26,82,118,0.35)` "not retrieved"; below the legend, 13px `#444` text: "resemblance, not predicate".

## 2. Where It Fits

- **Strength:** Handles semantic similarity — meaning, not just keywords — on unstructured data (text, images, audio)
- **Strength:** Enables RAG; finds relationships keyword search misses; scales with ANN indexes (HNSW, IVF)
- **Weakness:** No precision guarantee — closest does not mean correct or relevant; distance thresholds are arbitrary
- **Weakness:** Embedding model selection/maintenance, index rebuild costs, no native boolean logic (needs hybrid search), results hard to explain or debug
- **Use case:** Semantic search, RAG pipelines for LLM grounding, recommendations, near-duplicate detection, image similarity, anomaly detection by distance from cluster centers

*Example: the work boot and ballet flat score high on similarity, yet no score marks where "relevant" ends.*

### Code block (in viz column, above canvas `c2`)

```
-- Pseudocode: vector similarity search

-- 1. Encode the query
query_vector = embed("shoes comfortable for walking long distances")

-- 2. Search nearest neighbors (cosine similarity)
SELECT product_id, product_name, description,
       cosine_similarity(embedding, query_vector) AS score
FROM products
ORDER BY score DESC
LIMIT 10;

-- Results:
-- 0.92  "Trail Runner Pro - all-day comfort"
-- 0.89  "Cushioned Walking Shoe - arch support"
-- 0.87  "Marathon Training Flat - lightweight"
-- 0.84  "Steel-toe Work Boot - padded insole"  ← relevant?
-- 0.81  "Ballet Flat - memory foam"             ← relevant?
-- No threshold tells you where "relevant" ends
```

### Visualization (canvas `c2`, 720×300)

Bar chart of top-k similarity scores with an arbitrary threshold line.

- **Title (bold 14px, top center, `#1a5276`):** "Top-k Scores: Where Does \"Relevant\" End?".
- **Plot area:** padding top 45, bottom 55, left 65, right 215; axes stroked `#999` 1px; y-axis labeled "1.0" at top and "0.7" at bottom (13px `#444`); y scale from 0.70 to 1.0.
- **Bars:** width 56px, gap 32px, starting 22px right of the y-axis. Labels / scores / colors:
  - "Trail Runner" 0.92 — `rgba(26,82,118,0.35)` (relevant)
  - "Walking Shoe" 0.89 — `rgba(26,82,118,0.35)` (relevant)
  - "Marathon Flat" 0.87 — `rgba(26,82,118,0.35)` (relevant)
  - "Work Boot" 0.84 — `#e74c3c` (not relevant)
  - "Ballet Flat" 0.81 — `#e74c3c` (not relevant)
- **Value labels:** score to 2 decimals in bold 13px `#222` centered above each bar; category labels in 11px `#222` below the axis.
- **Threshold line:** horizontal dashed (dash 6/4) orange (`#e67e22`) line at score 0.85 across the plot, width 1.5; bold 13px orange label to its right: "0.85 cutoff? arbitrary".
- **Side annotations (right of plot):** bold 13px red: "high score, off-intent"; below it, 13px `#444`: "no score marks relevance".

## Takeaway (key-point callout, full width at page bottom)

**The takeaway:** Vector search retrieves what resembles your example, not what satisfies your condition — the closest match carries no guarantee it qualifies for your intent.

## Regeneration instructions

- **Layout:** backlog detail page. h1 with 2px `#2980b9` bottom border, `.subtitle`, then an `.intro` callout (background `#f0f4f8`, left border 4px solid `#2980b9`, padding 12px 16px, 0.95rem). Each numbered section is a `.bias-section` (margin-bottom 40px) with an h2 (1.3rem `#1a5276`, 2px `#2980b9` bottom border) and a `table.layout` (width 100%, td padding 12px): left `td.text-col` 45% holds bullets + `.key-point` + `.example`, right `td.viz-col` 55% holds the canvas (section 2 also has a `<pre>` code block above its canvas). A final full-width `.key-point` takeaway sits after the sections. The h1 carries the index number "10." matching the file index.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `.key-point` background `#f8f9fa` with left border 3px solid `#e74c3c`, 0.9rem; `.example` italic `#555` 0.9rem; ul 0.92rem; `<pre>` background `#f8f9fa`, 1px `#e0e0e0` border, radius 4px, padding 12px, 0.85rem, 'SF Mono'/Consolas monospace. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300, CSS `width: 100%` with 1px `#e0e0e0` border and 4px radius; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`.
- Any card links in regenerated HTML use `.html` extensions.
