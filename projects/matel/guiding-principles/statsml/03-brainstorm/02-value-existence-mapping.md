# Value Existence Mapping

**Page type:** detail page (TOC box + numbered h2 sections, each a two-column obj-table row: text left 45%, canvas right 55%)
**HTML title tag:** Value Existence Mapping

**Subtitle:** Identify gaps in data to split into possible sub-populations — or at least quantify where data exists and where it doesn't.

## Table of Contents

1. Core Idea: Gaps Split Data into Sub-Populations (#core)
2. The Mapping Process (#process)
3. Five Real-World Patterns (#patterns)
4. Pipeline Connection (#pipeline)

## 1. Core Idea: Gaps Split Data into Sub-Populations

**Find Gaps → Split → Analyze Each Part Independently**

- Real data has structural gaps — empty regions where no values exist
- A gap means the data likely contains separate sub-populations that should NOT be analyzed together
- Mixing clusters produces wrong means, wrong shapes, wrong test results
- **The gap itself may be the strongest classifier** — cluster membership separates classes

**Example:** Troponin levels: cluster 1 [0.01-0.04] = 97% neg, gap [0.04-0.40] = empty, cluster 2 [0.40-12.0] = 96% pos. No statistical test needed — the gap IS the decision boundary.

**Quantifying gaps:** Width of gap, density ratio on each side, whether 3σ of either cluster can reach across. These determine if a gap is structural (real sub-population boundary) or just sparse sampling.

### Visualization (canvas `c1`, 720×240)

Scatter plot of two dot clusters with a highlighted empty gap between them and a dashed "mean" line falling inside the gap.

- **Title (bold 17px `#1a5276`, top-left at x=40, y=22):** "Data Has Two Clusters — Mean Falls in Empty Gap".
- **Margins:** left 60, right 30, top 45, bottom 35.
- **Left cluster:** 80 random dots (seeded PRNG, seed 42, multiplicative LCG 16807 mod 2147483647), x spanning the first 30% of plot width, y spanning full plot height, 3px radius circles filled `rgba(41,128,185,0.5)`.
- **Right cluster:** 60 random dots, x spanning 65%–95% of plot width, same y span, filled `rgba(39,174,96,0.5)`.
- **Gap region:** rectangle from 33% to 63% of plot width, full plot height, filled `rgba(231,76,60,0.08)`; centered red (`#e74c3c`) 17px label at gap center: "GAP — no data here".
- **Mean line:** vertical dashed red line (`#e74c3c`, width 2, dash 5/3) at 48% of plot width, full plot height; bold red 17px label to its right near the top: "← \"mean\" is fiction".

## 2. The Mapping Process

**Sort → Find Gaps → Define Clusters → Detect Spikes**

- **Sort & compute gaps:** Gap between each consecutive pair. Most are small (within-cluster). Structural gaps are 10× the median gap.
- **Identify structural gaps:** Empty regions where no data was observed. Mark cluster boundaries.
- **Define clusters:** Each contiguous region between gaps. Record: n, range, density, class distribution.
- **Detect point masses:** Single values with >10× expected frequency → separate from continuous distribution.

**Output per cluster:** range [min, max], n, density, pos/neg counts. Gaps and point masses recorded separately.

### Visualization (canvas `c2`, 720×280)

Horizontal 4-step process flow of rounded boxes connected by arrows.

- **Title (bold 17px `#1a5276`, top-left):** "Mapping Process: Sort → Gaps → Clusters → Spikes".
- **Steps (150×55px rounded boxes, radius 6, 15px gaps, centered horizontally and vertically):**
  1. "Sort values / compute gaps" — color `#7f8c8d`
  2. "Find structural / gaps (10× median)" — color `#e74c3c`
  3. "Define clusters / (contiguous regions)" — color `#2980b9`
  4. "Detect spikes / (>10× expected freq)" — color `#8e44ad`
- Each box: fill at 12% alpha of its color, 2px stroke of the color, two-line bold 17px centered label in the color.
- **Arrows:** gray `#999` line + small triangle head between consecutive boxes.
- **Footer (17px `#555`, bottom center):** "Output: list of clusters + gaps + point masses → feeds into shape detection per cluster".

## 3. Five Real-World Patterns

### A: Single Dense Cluster + Outlier Tail

95% of data in a tight range, scattered extremes beyond.

- If tail has high class purity and n≥30 → it's signal, split and model separately
- If tail is n<10 → probably noise or data errors, merge back

**Examples:** Hospital charges, income, lot area.

### Visualization (canvas `cA`, 720×280)

Histogram with dense body and sparse tail.

- **Title (bold 17px `#1a5276`, left):** "A: Dense Cluster + Outlier Tail (income, hospital charges)".
- **Bins (20):** `[2, 8, 25, 50, 80, 65, 35, 15, 5, 2, 1, 1, 0, 0, 1, 0, 0, 0, 0, 1]`, scale max 82. Margins: left 40, right 20, top 25, bottom 25.
- **Bar colors:** first 10 bins `rgba(41,128,185,0.4)`, remaining bins `rgba(231,76,60,0.4)`.
- **Annotation (17px `#e74c3c`, right-aligned near top-right):** "tail: signal or noise?".

### B: Multiple Dense Clusters with Gaps

Data exists in 2-3 tight groups with truly empty ranges between them.

- Each cluster is likely a different subpopulation
- Cluster membership alone may be the strongest classifier
- Gap splitting (Doc 16) handles this pattern

**Examples:** Hormone levels, enzyme biomarkers, blood cell counts.

### Visualization (canvas `cB`, 720×280)

Histogram with two clusters and an empty gap.

- **Title (bold 17px `#1a5276`, left):** "B: Two Clusters with Gap (enzyme levels, hormones)".
- **Bins (20):** `[3, 12, 30, 45, 35, 15, 4, 0, 0, 0, 0, 0, 5, 18, 38, 50, 40, 20, 8, 2]`, scale max 52. Same margins as A.
- **Bar colors:** bins 0-6 `rgba(41,128,185,0.4)`, bins 12+ `rgba(39,174,96,0.4)`; zero bins not drawn.
- **Annotation (17px `#e74c3c`, centered mid-plot at bin 9.5):** "GAP".

### C: Dense at One End, Sparse at Other

Heavy concentration at one boundary, exponentially thinning out.

- Equal-width bins compress 80% into bin 1 — useless
- Need adaptive bins (narrow where dense, wide where sparse)

**Examples:** Days since last login, claim amounts, page views, file sizes.

### Visualization (canvas `cC`, 720×280)

Decaying histogram.

- **Title (bold 17px `#1a5276`, left):** "C: Dense at One End, Sparse at Other (days since login, claims)".
- **Bins (20):** `[80, 50, 30, 18, 12, 8, 5, 4, 3, 2, 2, 1, 1, 1, 1, 0, 0, 0, 0, 0]`, scale max 82. Bars `rgba(41,128,185,0.4)`.
- **Annotation (17px `#e67e22`, right-aligned near top-right):** "need adaptive bins (not equal-width)".

### D: Uniform Coverage (No Gaps)

Values span full range without structural gaps. Density varies but no empty zones.

- Standard histogram binning works fine
- No gap splitting needed — proceed directly to shape detection

**Examples:** Age, temperature, test scores, year built.

### Visualization (canvas `cD`, 720×280)

Flat histogram.

- **Title (bold 17px `#1a5276`, left):** "D: Uniform Coverage — No Gaps (age, temperature, test scores)".
- **Bins (20):** `[12, 14, 11, 13, 15, 12, 14, 11, 13, 12, 14, 13, 11, 15, 12, 14, 13, 11, 12, 14]`, scale max 18. Bars `rgba(39,174,96,0.4)`.
- **Annotation (17px `#27ae60`, right-aligned near top-right):** "simplest case → direct to shape detection".

### E: Point Masses (Spikes) + Continuous

Specific values have disproportionate frequency — typically 0, max, or defaults.

- Point masses encode categorical info ("unused", "maxed out", "doesn't have this")
- Separate them first, then profile the continuous remainder independently
- The spike itself may be highly predictive (e.g., capital_gain=0 vs >0)

**Examples:** Capital gains (95% zero), 2nd floor SF, insurance deductible used.

### Visualization (canvas `cE`, 720×280)

Histogram with a tall spike in bin 0 plus a low continuous hump.

- **Title (bold 17px `#1a5276`, left):** "E: Point Mass (spike at 0) + Continuous Remainder".
- **Bins (20):** `[75, 0, 2, 5, 10, 15, 20, 18, 12, 8, 5, 3, 2, 1, 1, 0, 0, 0, 0, 0]`, scale max 78.
- **Bar colors:** bin 0 `rgba(142,68,173,0.5)`, all others `rgba(41,128,185,0.4)`.
- **Annotations:** left-aligned purple (`#8e44ad`) 17px label "spike" near the spike; right-aligned gray (`#666`) label near top-right: "separate spike → binary feature + profile remainder".

## 4. Pipeline Connection

**Value Map → Shapes Are Detected Per Cluster**

- **Pattern A/C:** Split tail if significant → profile main body and tail separately
- **Pattern B:** Each cluster gets its own shape detection, model fitting, range testing
- **Pattern D:** Direct to shape detection (Step 3) — simplest case
- **Pattern E:** Point mass → binary feature. Continuous remainder → shape detection.

**Key insight:** Shape detection on the WRONG population (mixing clusters) gives wrong shape. Value mapping ensures each shape analysis runs on a coherent, connected region of the data.

### Visualization (canvas `c3`, 720×240)

Routing table of 5 colored rounded rows mapping each pattern to its action.

- **Title (bold 17px `#1a5276`, left):** "Value Map Determines What Shape Detection Sees".
- **Rows (full-width minus 40px margins, 30px tall, 37px pitch starting y=45; fill at 8% alpha of row color, 1.5px stroke of row color, radius 4; bold colored pattern label at x=60, dark `#333` action text starting at x=260 prefixed "→  "):**
  1. "A (cluster + tail)" → "Split tail if n≥30 → profile each" — `#2980b9`
  2. "B (multiple clusters)" → "Each cluster → own shape + model + range test" — `#27ae60`
  3. "C (dense at one end)" → "Adaptive bins → shape detection" — `#e67e22`
  4. "D (uniform coverage)" → "Direct to shape detection (simplest)" — `#27ae60`
  5. "E (spikes + continuous)" → "Spike → binary feature. Remainder → shape detection." — `#8e44ad`

## Callout (philosophy box)

**Principle:** Never analyze a distribution before knowing where data exists. A mean across two clusters is a fiction. A normality test across a gap is nonsense. Map first, then analyze within each coherent region.

## Regeneration instructions

- **Layout:** single long page. h1, `.subtitle`, a `.toc` box (background `#f8fafb`, border `1px solid #e0e0e0`, padding 20px 30px, radius 4px, bold "Table of Contents" heading + ordered list of anchor links `#core`, `#process`, `#patterns`, `#pipeline`), then numbered h2 sections. Each content block is an `.obj-table`: full-width table, one `<tr>` per block; left `<td>` (45%) holds `.obj-title` + bullets/paragraphs, right `<td>` (55%, centered) holds the canvas. Section 3 uses five separate one-row obj-tables (patterns A–E). Page ends with a `.philosophy` callout.
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.4em `#1a5276` with `border-bottom: 2px solid #2980b9`, padding-bottom 8px; subtitle `#666` 1.05em; `strong` in `#1a5276`; table cell borders `1px solid #e0e0e0`, padding 20px 24px, even rows background `#fafcfe`; `.obj-title` 1.05em, weight 600, `#1a5276`; `.philosophy` background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em. No nav bar, no back/home links.
- **Canvas:** declare intrinsic `width`/`height` attributes as given per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart text uses 17px -apple-system. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, grays `#666`/`#333`/`#999`.
- In regenerated HTML, any card/page links use `.html` extensions.
