# Semi-Structured Datasets with Nested Columns

**Page type:** detail page (backlog kusto-style two-column layout: text left 45%, canvas right 55%, one table row per section)
**HTML title tag:** Semi-Structured Datasets with Nested Columns — Discussion Backlog

**Subtitle:** Understanding nested structure before flattening it into features

**Intro callout:** Real-world data isn't flat tables. JSON logs, medical records, e-commerce events all have nested structure that must be understood before profiling.

## 1. Types of Nesting

- **JSON objects:** `{"address": {"city": "NYC", "zip": "10001", "coords": {"lat": 40.7, "lng": -74.0}}}`
- **Arrays of varying length:** `{"purchases": [{"item": "A", "amt": 50}, {"item": "B", "amt": 120}]}`
- **Mixed depth:** Some rows 3 levels deep, others 1
- **Repeated nested groups:** Patient has multiple diagnoses with date + code + severity

### Visualization (canvas `c1`, 720×340)

JSON tree diagram of a patient record with typed, color-coded nodes on a `#f8f9fa` background.

- **Title (bold 16px monospace, `#1a5276`, top left):** "JSON Tree: patient_record"
- **Nodes** (each drawn as white boxes with a colored 1.5px outline and colored text; root in bold 14px monospace, all others 10px monospace):
  - depth 0: `patient_record` at (160,60), color `#1a5276`
  - depth 1: `demographics` (80,130) `#27ae60`; `lab_results[]` (260,130) `#e67e22`; `medications[]` (420,130) `#e67e22`
  - depth 2: `age` (30,200), `gender` (100,200) both `#27ae60`; `address{}` (170,200) `#2980b9`; `date` (200,200), `test_name` (280,200), `value` (360,200), `drug_name` (380,200), `dosage` (460,200), `start_date` (540,200) all `#27ae60`
  - depth 3: `city` (120,270), `zip` (200,270) `#27ae60`; `coords{}` (280,270) `#2980b9`
  - depth 4: `lat` (250,320), `lng` (320,320) `#27ae60`
- **Edges** (light gray `#bdc3c7`, width 1.5): root→{demographics, lab_results[], medications[]}; demographics→{age, gender, address{}}; lab_results[]→{date, test_name, value}; medications[]→{drug_name, dosage, start_date}; address{}→{city, zip, coords{}}; coords{}→{lat, lng}.
- **Legend (top right at ~x=520, 14px, swatch squares):** green `#27ae60` "Leaf (scalar)"; blue `#2980b9` "Object (nested)"; orange `#e67e22` "Array (repeated)". Legend text in `#2c3e50`.
- **Depth labels (12px `#95a5a6`, right-aligned at x=700):** "depth 0" through "depth 4" at the corresponding row heights.

## 2. Challenges and Approach

- Flattening destroys context
- Array aggregation is a modeling choice (count? max? mean? last?)
- Sparsity from flattening (70% missing = "not applicable"?)
- Type detection at every level needed
- Cardinality explosion from one-hot on nested objects

Second bullet list:

- **Schema inference first**
- **Semantic grouping** (nested fields under same parent = feature GROUP)
- **Array summarization candidates** (multiple summaries per array field)
- **Depth-aware type detection**
- **Missingness as signal**

**Key Questions:**
(1) When to flatten vs keep structure?
(2) Predefined vs learned summarization?
(3) Effective n for sparse nested fields?
(4) Auto-detect keys vs features?
(5) Schema drift in nested structures?

### Visualization (canvas `c2`, 720×300)

Rendered table of derived flattened features, on a `#f8f9fa` background.

- **Title (bold 16px, `#1a5276`, top left):** "Flattened Output: Derived Features from Nested Structure"
- **Header row:** solid `#1a5276` band with white bold 14px column names: Feature, Source Path, Strategy, Type, Example (column x positions 20, 150, 340, 470, 560; row height 38).
- **Data rows** (zebra striping: white / `#eaf2f8`; Feature and Source Path in 10px monospace — Feature `#2c3e50`, path `#555`; Strategy in 13px sans-serif colored `#e67e22` when it contains "array", otherwise `#27ae60`; Type in `#1a5276`; Example in `#2c3e50`):

| Feature | Source Path | Strategy | Type | Example |
|---|---|---|---|---|
| age | demographics.age | direct | int | 42 |
| lab_count | lab_results[] | array → count | int | 7 |
| cholesterol_last | lab_results[].value | array → latest | float | 215.3 |
| cholesterol_trend | lab_results[].value | array → slope | float | -2.1 |
| med_count | medications[] | array → count | int | 3 |
| max_dosage | medications[].dosage | array → max | float | 500.0 |

- **Annotation:** dashed red (`#e74c3c`, dash 4/3, width 1.5) vertical line in the Strategy column spanning the cholesterol_last/cholesterol_trend rows, with 12px red text beside it: "Same source, different aggregations".

## Regeneration instructions

- **Template/layout:** backlog kusto-style detail page. h1 with 2px `#2980b9` bottom border, `.subtitle`, `.intro` callout (background `#f0f4f8`, left border `3px solid #2980b9`, padding 8px 12px, 0.9rem), then one `.lang-section` per numbered section: h2 (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with a single `<tr>` — left `td.text-col` (45%) holds bullets/key-point (Section 2 has two consecutive `<ul>` lists), right `td.viz-col` (55%) holds the canvas. No index number in the h1.
- **Inline code style:** `code` — background `#e8f0f8`, padding 2px 6px, radius 3px, 0.85em, color `#1a5276`.
- **Key-point callout:** `.key-point` — background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; ul 0.92rem with 4px li spacing; canvases `width: 100%` with `1px solid #e0e0e0` border, radius 4px. No nav bar, no back/home links.
- **Palette:** `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange, accents `#2980b9`, grays `#95a5a6`/`#bdc3c7`, zebra `#eaf2f8`.
- **Canvas:** intrinsic width/height attributes as given (c1 720×340, c2 720×300); scale via `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) in a shared `setupCanvas(id)` helper that reads the element's declared width/height.
- Card links elsewhere pointing to this page use the `.html` extension in regenerated HTML.
