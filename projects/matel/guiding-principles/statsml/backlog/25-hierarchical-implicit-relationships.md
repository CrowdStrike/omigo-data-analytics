# Hierarchical Data with Implicit Relationships

**Page type:** detail page (backlog kusto-style 2-col layout: text left 45%, canvas right 55%, one `.card-section` per numbered section)
**HTML title tag:** Hierarchical Data with Implicit Relationships

**Subtitle:** Many datasets contain columns that form implicit hierarchies — parent/child, part/whole, containment — with no explicit foreign key or tree structure. The pipeline must detect these relationships and understand their consequences for profiling and modeling.

**Status badge:** TO DISCUSS

## 1. Containment Hierarchies

**Containment** — each level is fully determined by the level below it, but nothing in the schema declares that:

- **Geographic:** city → state → country → region
- **Organizational:** employee → team → department → division
- **Temporal:** day → week → month → quarter → year
- **Product:** SKU → product line → category → department

**Key point (red-accent callout):** **Detection signal:** if column A has N unique values and column B has M with M ≪ N, and every value of A maps to exactly one value of B — that functional dependency *is* the hierarchy.

*Example: 4,200 cities collapse into 50 states, 12 countries, 4 regions — a cardinality funnel with no join key anywhere.*

### Visualization (canvas `c1`, 720×300)

Two-panel: nested containment boxes (left) and log-scaled cardinality funnel bars (right).

- **Title (bold 14px, `#1a5276`, centered, y=25):** "Containment: Levels Implied, Never Declared".
- **Levels data:** region = APAC (n=4), country = India (n=12), state = Maharashtra (n=50), city = Pune (n=4200).
- **Left panel:** four nested rectangles starting at (30,52) size 300×200, each inner box inset by 26px per level; fill `rgba(26,82,118,0.06)`, stroke `#1a5276` width 1.5; level name (10px `#1a5276`) at top-left of each box. A vertical dashed orange (`#e67e22`, dash 4/3) line at x=200 from y=248 up to y=66, labeled "implicit parent link" (9px orange, at 206,262).
- **Right panel:** header "unique values per level" (11px `#666` at 390,50). Four horizontal bars from x=390, one per level, city at top: width = 24 + log(n+1)/log(5000)·250, height 18, fill `rgba(26,82,118,0.35)`, stroke `#1a5276`; level's first word above each bar (10px `#2c3e50`), count value (`#1a5276`) at bar end. Values: 4200, 50, 12, 4.
- **Caption (11px `#888`, bottom center):** "Each child maps to exactly one parent — a functional dependency, not a foreign key".

## 2. Derived and Computed Hierarchies

**Derived hierarchies** — the parent column is a deterministic function of the child, so both carry the same information:

- **Aggregation:** line_item_total is qty × price; order_total is the sum of line items
- **Bucketing:** age_group derived from age; income_bracket from income
- **Encoding:** first 3 digits of ZIP = region; ICD code prefix = disease category
- **Composite keys:** account_id embeds branch_code + customer_seq

**Key point (red-accent callout):** **Why it hurts:** parent and child in the same model produce perfect multicollinearity. Detect via prefix/substring patterns, exact functional relationships, or suspiciously high correlation with discrete steps.

### Visualization (canvas `c2`, 720×300)

Five source→derived chip rows with operation-labeled dashed arrows and a collinearity badge.

- **Title (bold 14px, `#1a5276`, centered, y=25):** "Derived Parents: Same Information, Twice".
- **Rows (one per 44px starting y=52):**
  | Source chip (blue) | Operation (orange arrow label) | Derived chip (green) |
  |---|---|---|
  | qty × price | multiply | line_item_total |
  | line_item_total | sum by order | order_total |
  | age = 34 | bucket | age_group = 30–39 |
  | zip = 10013 | prefix(3) | region = 100xx |
  | account_id | split | branch_code + seq |
- **Chips:** source chip 170×24 at x=24, fill `rgba(26,82,118,0.10)`, stroke `#1a5276`, text `#1a5276` 10px; derived chip 180×24 at x=306, fill `rgba(39,174,96,0.10)`, stroke/text `#27ae60`. Dashed orange (`#e67e22`, dash 4/3) arrow between them with operation label above.
- **Badge (per row, red `#e74c3c` 10px, at x=500):** "R² = 1.00  (exact)".
- **Caption (11px `#888`, bottom center):** "Both columns in one model → perfect multicollinearity, unstable coefficients".

## 3. Semantic Hierarchies (Hardest)

**Semantic hierarchies** are real relations that live entirely outside the data:

- **Taxonomic:** "laptop" is-a "computer" is-a "electronics" — stored as flat category strings
- **Part-whole:** engine is part of car, but the dataset only has `component` and `system` columns
- **Role-based:** a manager is an employee who supervises other employees (self-referential)

**Key point (red-accent callout):** Cannot be detected from data distributions at all — requires domain metadata, naming conventions, or an explicit ontology.

**Open question (orange-accent callout):** **Open question:** how much should the pipeline attempt without external knowledge?

### Visualization (canvas `c3`, 720×300)

Two-panel: flat stored column values (left) vs the latent taxonomy tree (right, dashed = not in the data).

- **Title (bold 14px, `#1a5276`, centered, y=25):** "Flat Strings vs the Taxonomy They Belong To".
- **Left panel:** header "stored column: category" (11px `#666`). Five stacked chips 130×26 at x=26 (one per 34px from y=62): laptop, desktop, monitor, sofa, table — fill `rgba(26,82,118,0.10)`, stroke/text `#1a5276`.
- **Right panel tree:** root node "department (?)" at (480,70); mid nodes "electronics (?)" at (400,145) and "furniture (?)" at (590,145) — all three drawn as dashed orange (`#e67e22`) boxes. Leaf nodes solid blue (`#1a5276`) boxes: laptop (330,232), desktop (400,232), monitor (470,232) under electronics; sofa (560,232), table (640,232) under furniture. All tree edges dashed orange (dash 4/3).
- **Divider:** vertical `#ccc` line at x=210 from y=45 to 265. Footer labels: "in the data" (gray `#999`, left) and "dashed = exists only in an ontology" (orange, at x=240).
- **Caption (11px `#888`, bottom center):** "No distribution reveals is-a or part-of — metadata or ontology required".

## 4. Implicit Graph Structure

**Graph relations flattened into columns** — the edges are in the rows, not the schema:

- **Referral chains:** user_id + referred_by_user_id = a hidden tree
- **Supply chain:** supplier → manufacturer → distributor → retailer, encoded as separate entity columns
- **Approval flows:** created_by → reviewed_by → approved_by = a DAG

**Key point (red-accent callout):** Profiling each column independently misses the structure entirely — the distribution of `referred_by` tells you nothing about depth, fan-out, or cycles.

**Open question (orange-accent callout):** **Open question:** what does "feature profiling" even mean for a column whose value is an edge rather than an attribute?

### Visualization (canvas `c4`, 720×300)

Two-panel: flat two-column table (left) transformed via a "self-join" dashed arrow into a reconstructed referral tree (right).

- **Title (bold 14px, `#1a5276`, centered, y=25):** "Two Columns, One Hidden Tree".
- **Left table (starting 30,55, cells 95×30, header row darker):** columns `user_id` / `referred_by`; rows: u1/—, u2/u1, u3/u1, u4/u2, u5/u2. Header fill `rgba(26,82,118,0.18)`, body `rgba(26,82,118,0.05)`, borders/text `#1a5276`.
- **Transform arrow:** dashed orange (`#e67e22`, dash 5/4) from (230,150) to (300,150), labeled "self-join" (9px, above).
- **Right tree:** green circular nodes (radius 13, fill `rgba(39,174,96,0.18)`, stroke/text `#27ae60`) at u1(500,80), u2(425,155), u3(590,155), u4(380,230), u5(468,230); solid blue `#1a5276` edges u1→u2, u1→u3, u2→u4, u2→u5.
- **Footer labels:** "flat rows" (`#999`, at 30,260); "depth, fan-out, root count — none of it in per-column stats" (`#27ae60`, at 330,260).
- **Caption (11px `#888`, bottom center):** "Supply chains and approval flows flatten the same way — as edges in columns".

## 5. Context-Scoped Identifiers

**Motivating example.** A student's roll number (say 15) is meaningful only within their class. Roll 15 in Class 3-A is a completely different student from roll 15 in Class 3-B.

- It **looks numeric** — the type classifier says `int_num`
- It **looks low-cardinality** — values 1–50 repeat in every class
- Profiled alone it is near-uniform, therefore meaningless
- It resolves to an entity only when paired with its **scoping column** (class)

**Same pattern elsewhere:** seat number within a flight, apartment number within a building, port number within a host IP, question number within an exam, employee ID scoped to a subsidiary or branch, version number within a software package.

**Key point (red-accent callout):** **Heuristic:** if unique(col) is low but unique(col, scope_col) ≈ nrows, the column is scoped. Names like *_number or *_id with low cardinality relative to row count are candidates. This is a compound key disguised as a single column — the pipeline sees a number, the domain sees an identifier that needs context.

### Visualization (canvas `c5`, 720×380)

Two class boxes with colliding roll numbers (top) plus a distinct-count bar test (bottom).

- **Title (bold 14px, `#1a5276`, centered, y=25):** "Roll 15 Is Not One Student".
- **Class boxes:** "Class 3-A" at x=30 and "Class 3-B" at x=400, each 290×110 from y=48, fill `rgba(26,82,118,0.05)`, stroke `#1a5276`. Inside each, five roll-number cells 46×30 for values 13, 14, 15, 16, 17; cell 15 highlighted — fill `rgba(231,76,60,0.15)`, stroke `#e74c3c` width 1.8, bold red text. Sub-labels: "roll_no values" (`#999`) and "roll 15 → Priya" (Class 3-A) / "roll 15 → Priya (different person)" (Class 3-B) in red.
- **Collision link:** dashed red (`#e74c3c`, dash 4/3) line between the two 15-cells (y≈97), labeled "same value" above and "≠ same entity" below (9px red, centered at x=359).
- **Bottom bar test:** header "distinct-count test" (11px `#666` at 30,192). Three horizontal bars from x=200, scale max 600 over 420px, fill `rgba(26,82,118,0.35)`:
  | Label | Value | Stroke color |
  |---|---|---|
  | unique(roll_no) | 50 | `#e74c3c` |
  | unique(class) | 12 | `#e67e22` |
  | unique(roll_no, class) | 600 | `#27ae60` |
- **Reference line:** vertical dashed `#999` line at the 600 mark, labeled "nrows = 600" (9px `#999`).
- **Caption (11px `#888`, bottom center):** "Low alone, row-unique when paired with scope → a compound key wearing a number".

## 6. Statistical Consequences

Ignoring the hierarchy breaks the assumptions profiling and tests rely on:

- **Redundant information:** city + state + zip all encode overlapping location signal → inflated importance
- **Simpson's paradox:** aggregating across hierarchy levels hides or reverses effects
- **Non-independence:** observations within the same group (same department, same region) are correlated — violates the i.i.d. assumption
- **Ecological fallacy:** group-level statistics do not apply to individuals within the group

**Key point (red-accent callout):** **The diagnostic:** measure how much variance lives between groups versus within them. A high between-group share means the level itself carries the signal — that is the case for a multilevel/mixed-effects model instead of flat feature engineering.

### Visualization (canvas `c6`, 720×380)

Two-panel: Simpson's paradox scatter (left) and variance decomposition stacked bars (right).

- **Title (bold 14px, `#1a5276`, centered, y=25):** "Pooling Across Levels Reverses the Effect".
- **Left panel** (plot area 66,66 size 268×232, sub-header "within-group vs pooled slope"): three clusters of 6 blue dots (`rgba(26,82,118,0.35)` fill, `#1a5276` stroke, radius 3.2). Cluster g (0–2) baseline at x=0.10+0.30g, y=0.62−0.28g (normalized coords); points step +0.05 in x, +0.045 in y with small deterministic jitter — each cluster trends **upward**. Green (`#27ae60`, width 2) within-group trend line over each cluster, labeled "group 1/2/3". Red dashed (`#e74c3c`, dash 6/4, width 2) pooled line from (0.05, 0.82) to (0.98, 0.20) — trending **down** — labeled "pooled slope −". Axis labels: "x (child-level measure)", rotated "y (outcome)".
- **Right panel** (plot area 400,66 size 290×232, sub-header "variance share by level"): stacked bars (52px wide) for three features — sales by store (between=70%), temp by region (between=88%), clicks by user (between=22%). Bottom segment fill `rgba(26,82,118,0.35)` stroke `#1a5276` (between groups), top segment fill `rgba(230,126,34,0.25)` stroke `#e67e22` (within groups). Bold blue label on each bar: "ICC 0.70" / "ICC 0.88" / "ICC 0.22". Gridlines every 25% with % labels. Legend: "between groups" (blue), "within groups" (orange).
- **Caption (11px `#888`, bottom center):** "High between-group share → model the level; do not pool it away".

## 7. Modeling Consequences

The same structure damages the model in four distinct ways:

- **Multicollinearity:** including both levels of a hierarchy inflates coefficient variance
- **Leakage:** child-level aggregates leak parent-level labels; a random row split puts siblings from one group on both sides of the split
- **Cardinality explosion:** one-hot encoding the leaf level of a deep hierarchy creates thousands of sparse features
- **Wrong granularity:** a model trained at the wrong level predicts at a granularity that does not match the decision context

**Key point (red-accent callout):** **Rule:** whenever a hierarchy is present, split by group, never by row. Group-level splits also give an honest estimate of performance on unseen groups.

### Visualization (canvas `c7`, 720×380)

Two panels comparing random row split vs group-level split across three group boxes of four rows each.

- **Title (bold 14px, `#1a5276`, centered, y=25):** "Random Row Split vs Group-Level Split".
- **Left panel** (x=24, header "random row split"): three "group 1/2/3" boxes (96×190, stroke `#1a5276`), each holding 4 row chips (76×28); train chips fill `rgba(26,82,118,0.35)`/text `#1a5276` labeled "train row", test chips fill `rgba(230,126,34,0.25)`/text `#e67e22` labeled "test row". Assignment (1=test): group1 `[0,1,0,1]`, group2 `[1,0,0,1]`, group3 `[0,1,1,0]` — every group mixed. Verdict below dashed red line: bold "BROKEN" (`#e74c3c`), "leakage: every group appears in train and test" (`#2c3e50`), "group mean seen in training inflates the score" (red).
- **Right panel** (x=380, header "group-level split"): same structure, assignment group1 `[0,0,0,0]`, group2 `[0,0,0,0]`, group3 `[1,1,1,1]` — whole groups on one side. Verdict below dashed green line: bold "OK" (`#27ae60`), "clean: no group crosses the boundary", "score generalizes to unseen groups" (green).
- **Divider:** vertical `#ccc` line at x=356.
- **Caption (11px `#888`, bottom center):** "Siblings share a parent-level label — splitting rows leaks it straight into training".

## 8. Detection and Pipeline Response

What is actually feasible to automate — functional dependency mining, cardinality ratios, prefix/substring patterns?

- Once detected, should the pipeline recommend a level, or present all levels with warnings?
- How does this interact with the type classifier? A hierarchy column may be `cat` at leaf and `cat` at root with very different cardinality profiles.
- Should hierarchical features get a special encoding strategy — target encoding at group level, embeddings that respect the hierarchy?
- Mixed-level datasets: some rows at leaf level, others at intermediate nodes — how do we detect and handle that?
- When should the pipeline suggest multilevel/hierarchical models instead of flat feature engineering?

**Open question (orange-accent callout):** **Unresolved:** whether the pipeline picks a level for the user or surfaces every level with warnings. Picking is convenient but silently discards signal; surfacing everything pushes the judgement back to the analyst.

### Visualization (canvas `c8`, 720×300)

Four detection-test → pipeline-response mapping rows (blue test chip → arrow → green response chip).

- **Title (bold 14px, `#1a5276`, centered, y=25):** "Detection Test → Pipeline Response".
- **Rows (one per 56px starting y=50; left chip 320×38 fill `rgba(26,82,118,0.08)` stroke/text `#1a5276`; gray `#999` arrow; right chip 316×38 fill `rgba(39,174,96,0.08)` stroke/text `#27ae60`):**
  | Detection test | Pipeline response |
  |---|---|
  | unique(A) ≫ unique(B), A maps 1:1 into B | containment level — keep one level, flag the rest |
  | exact formula, prefix or substring match | drop the derived twin, flag collinearity |
  | unique(col) low, unique(col, scope) ≈ nrows | compound key — do not profile as numeric |
  | high ICC / group-level variance share | suggest mixed-effects model, split by group |
- **Caption (11px `#888`, bottom center):** "Semantic hierarchies have no test on this list — they need external metadata".

## Regeneration instructions

- **Layout:** backlog detail page. Body → h1 → `.subtitle` → `.status` badge ("TO DISCUSS") → one `.card-section` per numbered section, each an `<h2>` plus a `table.layout` with one `<tr>`: left `td.text-col` (45%) for paragraphs/bullets/`.key-point`/`.questions`/`.example`, right `td.viz-col` (55%) for the canvas.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with `border-bottom: 2px solid #2980b9`. `.subtitle` `#666` 0.95rem. `.status` inline-block pill: background `#e8f0f8`, color `#1a5276`, padding 3px 10px, radius 12px, 0.85em bold. h2 1.3rem `#1a5276` with 2px `#2980b9` bottom border. `.key-point` background `#f8f9fa`, `border-left: 3px solid #e74c3c`. `.questions` background `#f8f9fa`, `border-left: 3px solid #e67e22`. `.example` italic `#555` 0.9rem. `strong` in `#1a5276`; `code` background `#e8f0f8`, color `#1a5276`. Canvas: `width: 100%`, border `1px solid #e0e0e0`, radius 4px. No nav bar, no back/home links, no index number in h1.
- **Palette:** primary blue `#1a5276`, accent blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`.
- **Canvas:** intrinsic width/height attributes per chart (c1–c4 and c8 are 720×300; c5–c7 are 720×380); a `setup(id)` helper (with inline equivalents for 380-tall canvases) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- Regenerated HTML has no card links (detail page); any links elsewhere use `.html` extensions.
