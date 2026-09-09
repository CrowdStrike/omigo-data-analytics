# Data Lineage — Provenance Tracking Through the Pipeline

**Page type:** detail page (backlog-style: intro callout, numbered h2 sections, two-column layout table with text left ~45% and canvas right ~55%)
**HTML title tag:** Data Lineage — Provenance Tracking Through the Pipeline — Discussion Backlog

**Subtitle:** Recording how every derived result was produced

**Intro callout:** Every derived result should carry a record of HOW it was produced — which raw data, which transforms, which parameters, which code version. Without lineage, results are unauditable black boxes.

## 1. What Lineage Captures

- **Source tracking:** Which rows, columns, time window contributed? Record filter predicate, not just "n=3200"
- **Transform chain:** Raw → type detection → bucket strategy → shape classification → enrichment → verdict
- **Parameter provenance:** Hard-coded? Data-driven? User override? Affects trust level
- **Branching decisions:** At each fork, WHY that branch was chosen
- **Aggregation lineage:** Which features contributed what weight to final answer

**Why this matters** (red-bordered key-point callout): Multi-candidate validation records which candidates tried, passed, selected. Reproducibility: same data + lineage = same result. Debugging bad verdicts: trace back to first incorrect step. Temporal validity: timestamps enable staleness detection. Audit trail: proof chain for regulated domains.

### Visualization (canvas `c1`, 720×300)

Left-to-right transform DAG with colored language-zone backgrounds, on light gray `#f8f9fa` background.

- **Title (bold 15px, `#1a5276`, top center):** "Transform DAG with Language Zone Boundaries"
- **Zone backgrounds** (y=30, height 200, 13px `#555` labels centered below at y=240): SQL at x=15 width 160 fill `rgba(39,174,96,0.08)`; DataFrame at x=180 width 160 fill `rgba(41,128,185,0.08)`; Python at x=345 width 180 fill `rgba(230,126,34,0.08)`; Pipeline at x=530 width 170 fill `rgba(142,68,173,0.08)`. Dashed vertical `#bdc3c7` boundaries (dash 4/3) at x=175, 340, 525.
- **DAG nodes** (rounded rects, radius 5, white bold 13px labels): "Raw Table" (40,70 100×35, `#27ae60`), "Ref Table" (40,140 100×35, `#27ae60`), "JOIN / GROUP BY" (195,100 120×35, `#2980b9`), "Python Transform" (365,70 130×35, `#e67e22`), "Feature Eng." (365,140 130×35, `#e67e22`), "Shape Classify" (550,70 120×35, `#8e44ad`), "Enrichment" (550,140 120×35, `#8e44ad`), "Verdict" (620,195 70×30, `#1a5276`).
- **Edges** (gray `#7f8c8d` lines width 1.5, `#555` filled arrowheads): Raw Table→JOIN, Ref Table→JOIN, JOIN→Python Transform, JOIN→Feature Eng., Python Transform→Shape Classify, Feature Eng.→Enrichment, Shape Classify→Verdict, Enrichment→Verdict.

## 2. Lineage Across Boundaries

- **SQL transforms:** GROUP BY, CASE WHEN, window functions — no 1:1 column mapping
- **Joins:** Fan-out (1:N) creates rows from nowhere. Multi-hop chains
- **Mixed-mode pipelines:** SQL → Python → SQL → model
- **Lossy transforms:** Aggregations, sampling, dedup destroy information

### Visualization (canvas `c2`, 720×300)

JOIN fan-out diagram: two rendered data tables connected by fan-out lines, on light gray `#f8f9fa` background.

- **Title (bold 15px, `#1a5276`, top center):** "JOIN Fan-Out: Row Provenance Problem"
- **Left table** at x=60, y=45, rows 160×25, labeled above (bold 14px `#1a5276`): "orders (3 rows)". Monospace 10px rows: header `order_id | customer` (blue `#1a5276` header row, white text), then `1001 Alice`, `1002 Bob`, `1003 Alice` (alternating `#eaf2f8`/white, `#bdc3c7` borders).
- **Right table** at x=430, y=35, rows 220×22, labeled above: "items (7 rows after JOIN)". Header `order_id | item | amount`, then rows: `1001 Widget $50`, `1001 Gadget $30`, `1001 Cable $10`, `1002 Widget $50`, `1002 Screen $200`, `1003 Gadget $30`, `1003 Cable $10`.
- **Fan-out lines** (orange `#e67e22`, width 1.5): order 1001 row → item rows 1-3; order 1002 row → item rows 4-5; order 1003 row → item rows 6-7. Bold orange 14px label "1:N fan-out" at (330, 90).
- **Bottom annotations (centered):** bold red 14px at y=250: "After GROUP BY: which source rows contributed?"; 14px `#555` at y=270: "Lineage = ??? (aggregation destroys per-row provenance)"

## 3. Native Code Opacity

- **Native code:** pandas apply(), for-loops — opaque to static analysis

**Key Questions** (red-bordered key-point callout):
(1) Minimum viable lineage?
(2) Stochastic components?
(3) Human-readable vs compact?
(4) Caching interaction?
(5) Storage scale?
(6) SQL: parse query plans or instrument?
(7) Joins: per-row or per-column?
(8) Native code: annotations or tracing?

### Visualization (canvas `c3`, 720×300)

Three-column traceability spectrum, each column with colored header, dark code block, and tinted verdict box; light gray `#f8f9fa` background.

- **Title (bold 15px, `#1a5276`, top center):** "Code Traceability Spectrum"
- **Columns** (200px wide; header bar 25px tall at y=35 with white bold 13px label; code block `#2c3e50` background 80px tall with `#ecf0f1` 9px monospace text; verdict box at y=148, 55px tall, tinted at 0.1 alpha of column color with 13px colored text):
  - x=50, green `#27ae60`, header "SQL (Traceable)", code: `SELECT dept,` / `  AVG(salary)` / `FROM employees` / `GROUP BY dept`, verdict: "Lineage: FULL" / "Input cols known" / "Output type inferred"
  - x=280, red `#e74c3c`, header "Python Lambda (Opaque)", code: `df["x"] = df.apply(` / `  lambda r: magic(r),` / `  axis=1` / `)`, verdict: "Lineage: NONE" / "Black box function" / "Cannot infer types"
  - x=510, orange `#e67e22`, header "Annotated (Hybrid)", code: `@lineage(` / `  inputs=["age","income"],` / `  output_type="float",` / `  desc="risk_score"` / `)` / `def compute(r): ...`, verdict: "Lineage: DECLARED" / "Human annotation" / "Trust but verify"

## Regeneration instructions

- **Layout:** backlog detail-page style. `<h1>` (2rem, `#1a5276`, 2px solid `#2980b9` bottom border), `.subtitle` (`#666`, 0.95rem), `.intro` callout (background `#f0f4f8`, left border `3px solid #2980b9`, padding 8px 12px, 0.9rem). Each section is a `.lang-section` (40px bottom margin) with an `<h2>` (1.3rem `#1a5276`, 2px solid `#2980b9` bottom border) and a `table.layout` with one `<tr>`: left `td.text-col` (45%) with bullets/callouts, right `td.viz-col` (55%) with the canvas.
- **Callouts:** `.key-point` — background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem.
- **Inline code:** `code` — background `#e8f0f8`, padding 2px 6px, radius 3px, 0.85em, `#1a5276`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; `ul` 0.92rem with 20px left margin; canvases `width: 100%` with `1px solid #e0e0e0` border, 4px radius.
- **Palette:** `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange, purple `#8e44ad` accent for pipeline nodes.
- **Canvas:** declare intrinsic `width`/`height` attributes as given; scale by `window.devicePixelRatio` via a shared `setupCanvas(id)` helper (`ctx.scale` back to logical coordinates); a `roundRect` helper draws rounded rectangles for DAG nodes.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
