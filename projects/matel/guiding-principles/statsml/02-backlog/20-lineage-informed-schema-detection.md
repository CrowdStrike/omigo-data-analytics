# Lineage-Informed Schema Detection for Semantic Validation

**Page type:** detail page (backlog-style: intro callout, numbered h2 sections, two-column layout table with text left ~45% and canvas right ~55%)
**HTML title tag:** Lineage-Informed Schema Detection for Semantic Validation — Discussion Backlog

**Subtitle:** Using provenance to infer type, range, and semantic role

**Intro callout:** Data lineage tells you what data MEANS, not just where it came from. When you know a column was produced by a JOIN, aggregation, or code transform, you can infer schema, type, valid range, and semantic role far more accurately.

## 1. How Lineage Improves Schema Detection

- **Derived column semantics:** `COUNT(*) GROUP BY dept` → non-negative integer, bounded by source size
- **Join-inherited constraints:** Customer age still 0-120 after join, order amount still positive
- **Transform-aware types:** `log(income)` → float. `age > 65` → boolean. `DATEDIFF` → non-negative int with unit
- **Aggregation-collapsed cardinality:** GROUP BY state → ≤50 unique values
- **Ratio/proportion detection:** positive_count / total_count → known [0,1]
- **Feature engineering provenance:** salary/hours = "dollars per hour", weight/height² = BMI

### Visualization (canvas `c2`, 720×300)

Rendered data table of feature constraints derived from lineage, on light gray `#f8f9fa` background.

- **Title (bold 15px, `#1a5276`, top center):** "Feature Constraints Derived from Lineage"
- **Table** starting at y=40, rows 30px tall, full-width 690px starting x=15. Header row solid `#1a5276` with white bold 13px labels: "Feature", "Lineage", "Constraint", "Split Rule" (column x positions 20, 130, 330, 540). Body rows alternate white/`#eaf2f8` with `#ddd` borders; Feature column 10px monospace `#1a5276`, Lineage column 9px monospace `#555`, Constraint column 13px sans-serif green `#27ae60`, Split Rule column 13px sans-serif orange `#e67e22`.
- **Rows (Feature | Lineage | Constraint | Split Rule):**
  - `dept_count` | `COUNT(*) GROUP BY` | non-neg int, ≤ N | integer splits only
  - `avg_salary` | `AVG(salary)` | float, ≥ 0 | any float, non-neg
  - `is_active` | `age > 65` | boolean {0,1} | only split at 0.5
  - `bmi` | `weight / height²` | float [10, 80] | bounded by formula
  - `tenure_days` | `DATEDIFF(now, start)` | non-neg int | integer, ≥ 0
  - `pct_positive` | `pos_ct / total_ct` | float [0, 1] | cannot exceed 1.0

## 2. How Better Schema Validates Decision Trees

- **Type-aware split constraints:** ratio [0,1] → can't split outside range. Count → integer only
- **Propagated NULL semantics:** Structural NULLs (no match) vs missing-data NULLs — different meanings
- **Cross-branch dependency:** total_purchases AND avg_purchase_amount both from purchases[] → hidden dependency
- **Temporal ordering:** account_age < 1yr AND lifetime_transactions > 10000 → temporal mismatch

### Visualization (canvas `c3`, 720×300)

Shared-source dependency diagram: one source node fanning out to three derived features, plus warning box and bad-branch example; light gray `#f8f9fa` background.

- **Title (bold 15px, `#1a5276`, top center):** "Shared Source Detection — Hidden Feature Dependencies"
- **Source node:** orange `#e67e22` rounded rect (300,45 120×35, radius 5), white bold 14px label "purchases[]".
- **Three derived feature nodes** (blue `#2980b9` rounded rects 130×40 at y=120, white 13px two-line labels): "total_purchases (COUNT)" at x=120, "avg_amount (AVG)" at x=310, "max_purchase (MAX)" at x=500. Orange `#e67e22` width-2 lines connect source bottom-center to each node top-center.
- **Warning box** (100,185 520×28, fill `rgba(231,76,60,0.08)`, stroke `#e74c3c` width 1.5), bold red 14px centered text: "WARNING: SHARED SOURCE — these features are NOT independent!"
- **Example text:** left-aligned 13px `#2c3e50` at (100,240): "Example bad tree branch:"; 10px monospace red at (100,258): `IF total_purchases > 5 AND avg_amount < 20 AND max_purchase > 500`; centered 12px `#555` at y=280: "All three conditions constrain the SAME underlying array — not three independent signals"

## 3. Implementation Layers

- **Layer 1:** Lineage capture (transform DAG)
- **Layer 2:** Schema inference (type, range, cardinality, unit, nullability, granularity)
- **Layer 3:** Constraint propagation (forward through DAG)
- **Layer 4:** Validation rules (model inputs, tree splits, branch paths, feature interactions)

**Key Questions** (red-bordered key-point callout):
(1) Auto vs human annotation?
(2) Opaque code → infer from observed?
(3) Schema drift handling?
(4) Hard rejection vs soft warning?
(5) Replace or supplement doc 25 metadata?
(6) Ensemble compatibility?

### Visualization (canvas `c1`, 720×300)

Three-layer inference flow diagram plus without/with comparison panels, on light gray `#f8f9fa` background.

- **Title (bold 15px, `#1a5276`, top center):** "Three-Layer Inference: Lineage → Schema → Validation"
- **Flow boxes** (rounded rects radius 6 at y=45, height 80; white bold 14px title + white 9px monospace item lines; connected by gray `#7f8c8d` width-2 arrows with `#555` arrowheads):
  - "Lineage Record" (x=30, 180 wide, green `#27ae60`): `source: employees`, `transform: GROUP BY dept`, `params: COUNT(*)`
  - "Inferred Schema" (x=260, 180 wide, blue `#2980b9`): `type: non-neg integer`, `range: [0, max_employees]`, `granularity: per-dept`
  - "Validation Rules" (x=490, 200 wide, purple `#8e44ad`): `split must be integer`, `value cannot be negative`, `bounded by source size`
- **"Without Lineage: GUESSES" panel** (30,155 330×55, fill `rgba(231,76,60,0.08)`, stroke `#e74c3c`), bold red 14px heading, then 13px `#2c3e50` lines: "dept_count looks numeric → allow float splits, negative values" and "No idea it came from COUNT(*) → no semantic constraints".
- **"With Lineage: KNOWS" panel** (380,155 320×55, fill `rgba(39,174,96,0.08)`, stroke `#27ae60`), bold green 14px heading, then 13px lines: "dept_count = COUNT(*) → integer, non-negative, bounded" and "Split at 3.7 flagged as invalid. Value -2 flagged as impossible".
- **Bottom annotation (13px `#555`, centered at y=265):** "Lineage transforms type detection from observation-only to knowledge-informed"

## Regeneration instructions

- **Layout:** backlog detail-page style. `<h1>` (2rem, `#1a5276`, 2px solid `#2980b9` bottom border), `.subtitle` (`#666`, 0.95rem), `.intro` callout (background `#f0f4f8`, left border `3px solid #2980b9`, padding 8px 12px, 0.9rem). Each section is a `.lang-section` (40px bottom margin) with an `<h2>` (1.3rem `#1a5276`, 2px solid `#2980b9` bottom border) and a `table.layout` with one `<tr>`: left `td.text-col` (45%) with bullets/callouts, right `td.viz-col` (55%) with the canvas.
- **Callouts:** `.key-point` — background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem.
- **Inline code:** `code` — background `#e8f0f8`, padding 2px 6px, radius 3px, 0.85em, `#1a5276`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; `ul` 0.92rem with 20px left margin; canvases `width: 100%` with `1px solid #e0e0e0` border, 4px radius.
- **Palette:** `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange, purple `#8e44ad` accent.
- **Canvas:** declare intrinsic `width`/`height` attributes as given; scale by `window.devicePixelRatio` via a shared `setupCanvas(id)` helper (`ctx.scale` back to logical coordinates); a `roundRect` helper draws rounded rectangles. Note canvas ids: section 1 uses `c2`, section 2 uses `c3`, section 3 uses `c1`.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
