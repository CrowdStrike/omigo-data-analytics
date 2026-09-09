# Feature Type Detection

**Page type:** detail page (two-column `.obj-table` with header row: type/characteristics text left 40%, canvas right 60%, one row per feature type; followed by an output-schema table and a footer callout)
**HTML title tag:** Feature Type Detection

**Subtitle:** Before analyzing any distribution, determine feature type. Type dictates bucketing, shape detection, and applicable tests.

**Table header row:** "Type & Characteristics" | "Visualization"

## Row 1: CATEGORICAL — Few distinct values, no meaningful numeric ordering

Badge: CATEGORICAL (purple `#6a1b9a`)

- **What it is:** Labels, not measurements. Numeric encoding doesn't represent magnitude
- **Examples:** Gender [0,1], Blood Type [A,B,AB,O], Zip Code, Error Codes
- **Bucketing:** Each unique value is its own bucket. No ranges
- **Key test:** If a single category has >90% one class, that value alone is a classifier
- **Detection clue:** Pos/neg ratio varies wildly and non-monotonically across ordered values

### Visualization (canvas `canvas-cat`, 720×200)

Bar chart: positive-class rate per category, non-monotonic.

- **Title (bold 14px `#1a5276`):** "Categorical: Pos% per category (non-monotonic = categorical)".
- **Data:** categories and pos rates — Gender 52%, Blood\nType 80%, Dept 15%, Error\nCode 90%, Region 35%.
- **Bars:** equal-width across the plot area (padding: top 30, bottom 40, left 60, right 30); bar color by rate: `#27ae60` if >60%, `#e74c3c` if <30%, otherwise `rgba(26,82,118,0.35)`. Percentage label (12px `#222`) above each bar; category label (11px `#555`) below.
- **Axis:** horizontal `#999` baseline only.
- **Annotation (bold 12px `#6a1b9a`, right of plot center):** "← No trend: ordering is meaningless".

## Row 2: DISCRETE NUMERIC — Limited distinct values, numeric ordering matters

Badge: DISCRETE NUMERIC (blue `#1565c0`)

- **What it is:** Numeric and ordered, but only specific values appear. Gaps are inherent, not missing data
- **Examples:** Pregnancies [0-8], Pain scale [1-10], Tumor stage [1-4], Prior surgeries [0-3]
- **Bucketing:** Can group adjacent values into ranges (1-3, 4-6, 7+)
- **Key test:** "Pregnancies 0-2" vs "5+" might separate classes
- **Cannot interpolate:** There is no value 2.5

### Visualization (canvas `canvas-disc`, 720×200)

Grouped bar chart: paired neg/pos counts per integer value.

- **Title (bold 14px `#1a5276`):** "Discrete: Prior Surgeries — pos vs neg per value".
- **Data (value: neg, pos):** 0: 180, 20; 1: 120, 45; 2: 60, 70; 3: 25, 85; 4: 8, 40. Scale max count = 200.
- **Bars:** pairs 22px wide, neg bar `#e74c3c` left, pos bar `#27ae60` right (4px apart); value labels (12px `#555`) under each pair; padding right 140 for legend.
- **Legend (top right):** red swatch "Neg class", green swatch "Pos class".
- **Annotation (bold 12px `#1a5276`, below axis):** "← more neg    more pos →".

## Row 3: CONTINUOUS — Many distinct values across a range

Badge: CONTINUOUS (green `#2e7d32`)

- **What it is:** Any number within a range. Each observation might be unique
- **Examples:** Blood glucose [67-313 mg/dL], BMI [16-46], Reaction time [212-1502ms]
- **Bucketing:** Must create buckets by choosing boundaries. Shape within ranges determines valid tests
- **Trap:** Empty ranges aren't "zero" — they're "no data." Don't interpolate
- **Detection:** High unique ratio (U/N > 0.5 typically)

### Visualization (canvas `canvas-cont`, 720×200)

Strip scatter plot: blood glucose values on x from 60 to 400, points jittered vertically, with a highlighted empty gap band.

- **Data (seeded LCG RNG, seed 42):** neg points in red `rgba(192,57,43,0.5)` — 60 points around 75–125 and 20 points around 130–160; pos points in green `rgba(30,132,73,0.5)` — 40 points around 180–250 and 25 points around 280–350. Dots radius 2.5.
- **Gap band:** x range 160–180 shaded `rgba(230,126,34,0.12)`, labeled "gap" in bold 11px `#e67e22`.
- **Axes:** horizontal `#999` baseline; x labels 60, 150, 250, 400 (11px `#555`).
- **Title (bold 14px `#1a5276`):** "Continuous: Blood glucose — data clusters with gap".

## Row 4: EDGE CASE — Encoded Categoricals That Look Numeric

Badge: EDGE CASE (orange `#e65100`)

- **Example:** "region_code" [1-12], N=5000, U=12, R=0.0024
- **Naive mistake:** Bucket as 1-4, 5-8, 9-12 (treats as discrete numeric)
- **Problem:** Region 3 has 80% pos, Region 7 has 90% neg — bucketing destroys signal
- **Detection:** Pos/neg ratio non-monotonic across ordered values
- **Fix:** Treat each value independently — the "ordering" is meaningless

### Visualization (canvas `canvas-edge1`, 720×200)

Bar chart: pos rate per region code 1–12, jumping non-monotonically.

- **Title (bold 14px `#1a5276`):** "Region Code: Pos% jumps non-monotonically → categorical!".
- **Data (pos rates, regions 1–12):** `[0.30, 0.45, 0.80, 0.20, 0.75, 0.15, 0.10, 0.90, 0.35, 0.55, 0.25, 0.70]`.
- **Bars:** 12 bars, 4px gaps; color by rate: `#27ae60` if >60%, `#e74c3c` if <30%, otherwise `rgba(26,82,118,0.35)`; region number (10px `#555`) below each bar.
- **Reference line:** dashed `#bbb` (dash 3/3) horizontal line at 50%, labeled "50%" in 10px `#999` at left.
- **Annotation (bold 12px `#e65100`, below axis):** "Bucketing 1-4, 5-8, 9-12 would destroy signal".

## Row 5: EDGE CASE — Continuous Feature with Cluster Gaps

Badge: EDGE CASE (orange `#e65100`)

- **Example:** "enzyme_level" — values in [0.1-2.5] and [8.0-15.0], nothing between
- **Stats:** N=2000, U=1847, R=0.92 → clearly continuous
- **Implication:** Gap 2.5-8.0 is meaningful — two sub-populations
- **Treatment:** Profile each cluster independently. The gap itself might be the separator

### Visualization (canvas `canvas-edge2`, 720×200)

Strip scatter plot: two clusters on x scale 0–16 separated by a highlighted empty gap.

- **Title (bold 14px `#1a5276`):** "Enzyme Level: Two clusters, empty gap = two sub-populations".
- **Data (seeded LCG RNG, seed 77):** cluster 1 at 0.1–2.5 — 80 points, first 55 red `rgba(192,57,43,0.5)`, rest green `rgba(30,132,73,0.5)`; cluster 2 at 8.0–15.0 — 70 points, first 15 red, rest green. Dots radius 2.5, jittered vertically.
- **Gap:** x range 2.5–8.0 shaded `rgba(230,126,34,0.08)` with dashed `rgba(230,126,34,0.4)` outline (dash 4/4), labeled "EMPTY GAP" in bold 12px `#e65100` at its center.
- **Axes:** horizontal `#999` baseline; x labels 0, 2.5, 8.0, 15.0 (11px `#555`).

## Row 6: EDGE CASE — Discrete with Heavy Concentration

Badge: EDGE CASE (orange `#e65100`)

- **Example:** "num_claims" — N=10000, U=8, R=0.0008
- **Distribution:** 0→75%, 1→15%, 2→6%, 3→2.5%, 4→1%, 5+→0.5%
- **Challenge:** 75% at value 0. Tests must account for extreme imbalance
- **Treatment:** Value 0 is its own bucket. Values 3+ grouped for sufficient samples

### Visualization (canvas `canvas-edge3`, 720×200)

Bar chart: percentage of records per claim count, dominated by zero.

- **Title (bold 14px `#1a5276`):** "Num Claims: 75% at zero — extreme imbalance".
- **Data (value: pct):** "0": 75, "1": 15, "2": 6, "3": 2.5, "4": 1, "5+": 0.5. Scale max = 80%.
- **Bars:** value 0 bar `#e74c3c`; values 1–2 `rgba(26,82,118,0.35)`; values 3, 4, 5+ `#e67e22`. Pct label (11px `#222`) above each bar, value label (12px `#555`) below.
- **Annotation (bold 12px `#e65100`, below axis):** "Value 0 = own bucket      Values 3+ = group for n".

## Output of Type Detection

(h2 heading styled `#1a5276` with 2px `#2980b9` bottom border, followed by a three-column `.output-table`)

| Field | Description | Example |
|-------|-------------|---------|
| `type` | categorical / discrete / continuous | continuous |
| `unique_count` | Number of distinct values | 847 |
| `unique_ratio` | U / N | 0.847 |
| `value_range` | [min, max] | [67.3, 312.7] |
| `has_gaps` | Are there empty regions? | true |
| `top_values` | Most common values + counts | {98.6: 23, 99.0: 18, ...} |
| `ordering_valid` | Does numeric order carry meaning? | true |

## Callout (philosophy box)

**Next:** This profile feeds into Step 2 (Value Existence Mapping) where we figure out exactly where data lives on the number line. [Step 2 →](02-value-existence-mapping.md)

## Regeneration instructions

- **Layout:** detail page built around a full-width `.obj-table` with a `<thead>` row ("Type & Characteristics" | "Visualization") and one `<tr>` per feature type. Left `<td>` (40%): a `.metric-domain` colored badge span, a bold `.metric-title` line, and a `.metric-desc` `<ul>` of labeled bullets. Right `<td>` (60%): the canvas. After the table: an h2 "Output of Type Detection" and the `.output-table`, then a `.philosophy` footer callout whose "Step 2 →" link points to `06-profiling-value-mapping.html` in regenerated HTML (`.md` sibling in this spec).
- **Page CSS:** body -apple-system sans-serif, background `#fafafa`, text `#1a1a1a`, 15px, padding 20px 10px. h1 `#1a5276`; `.subtitle` `#333` 1.1em. `.obj-table` th background `#1a5276` white text; all cell borders 1px solid `#2980b9`, padding 14px 16px; even rows background `#f0f8ff`. `.metric-title` bold 1.1em `#1a5276`. Badge colors: `.domain-cat` `#6a1b9a`, `.domain-disc` `#1565c0`, `.domain-cont` `#2e7d32`, `.domain-edge` `#e65100` (white text, 2px 8px padding, 3px radius, 0.85em). `.output-table`: th background `#e8eef3` `#1a5276` text with 2px `#bbb` bottom border; td 1px `#e0e0e0` bottom border; `code` on `#eee` background. `.philosophy`: background `#f0f4f8`, 4px `#2980b9` left border, padding 12px 16px.
- **Canvases:** all six are 720×200 CSS pixels (`display: block`); a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Common chart padding: top 30, bottom 40, left 60, right 30 (right 140 on the discrete chart for its legend).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`; scatter dots `rgba(192,57,43,0.5)` (neg) and `rgba(30,132,73,0.5)` (pos); badge orange annotations `#e65100`, categorical annotation purple `#6a1b9a`.
