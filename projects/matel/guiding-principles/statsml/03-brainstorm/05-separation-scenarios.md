# Separation Scenarios

**Page type:** detail page (TOC box + numbered h2 sections, each a two-column obj-table row with verdict badge: text left 45%, canvas right 55%; plus a summary table)
**HTML title tag:** Separation Scenarios

**Subtitle:** How pos and neg class distributions relate — 8 patterns that cover the real world.

## Table of Contents

1. Cluster Separation (Gap IS the Classifier) (#a)
2. Shifted Centers (Overlap with Shift) (#b)
3. Twin Peaks Map to Classes (#c)
4. Spike vs Spread (One-Directional) (#d)
5. Sparse Tail Signal (#e)
6. No Separation (Noise) (#f)
7. Monotonic Discrete Trend (#g)
8. Categorical Dominance (#h)
9. Decision Summary (#summary)

## 1. Cluster Separation — Gap IS the Classifier

**Verdict badge:** USE — Full Coverage (green pill, `.v-use`)

**Gap Between Clusters Naturally Separates Classes**

- From profiling: multiple clusters with empty gaps between them
- Different classes dominate different clusters
- The gap itself is the decision boundary — no statistical test needed

**Example:** Troponin I levels. Cluster 1 [0.01-0.04 ng/mL]: 97% neg (n=980). Gap [0.04-0.40]: no data. Cluster 2 [0.40-12.0]: 96% pos (n=470). Classify by cluster membership.

### Visualization (canvas `ca`, 720×240)

Two-cluster histogram with a shaded gap.

- **Title (bold 17px `#1a5276`, left):** "Troponin I — Gap = Decision Boundary".
- **Margins:** left 50, right 30, top 35, bottom 35; scale max 130.
- **Left cluster (35% of plot width):** bins `[5, 20, 45, 80, 120, 95, 60, 30, 10, 3]`, bars `rgba(231, 76, 60, 0.5)`.
- **Gap (next 20% of width):** filled `rgba(200,200,200,0.15)` with dashed `#999` outline (dash 4/3); centered 17px `#666` label "GAP — no data".
- **Right cluster (remaining 45%):** bins `[3, 15, 40, 70, 100, 85, 55, 25, 8]`, bars `rgba(39, 174, 96, 0.5)`.
- **Bottom labels (bold 17px, centered under each cluster):** red `#e74c3c` "97% neg"; green `#27ae60` "96% pos".

## 2. Shifted Centers — Overlap with Shift

**Verdict badge:** USE — Tails Beyond Overlap (green pill, `.v-use`)

**Both Classes Same Range, But Shifted Distributions**

- No gap, single cluster. But class distributions have different centers.
- Signal is in the tails: low end is dominated by neg, high end by pos
- Middle overlap zone is inconclusive — can't classify reliably there

**Example:** Fasting glucose. Neg median=92, Pos median=165. Below 100: 98% neg (n=665). Above 130: 92% pos (n=160). Zone [100-130]: ambiguous.

### Visualization (canvas `cb`, 720×240)

Two overlaid shifted histograms with a highlighted overlap zone.

- **Title (bold 17px `#1a5276`, left):** "Fasting Glucose — Shifted Distributions, Overlap in Middle".
- **Margins:** left 50, right 30, top 35, bottom 35; 20 bins; scale max 170.
- **Neg bins (left-shifted, `rgba(231, 76, 60, 0.4)`):** `[10, 30, 70, 130, 160, 140, 90, 50, 20, 8, 3, 1, 0, 0, 0, 0, 0, 0, 0, 0]`.
- **Pos bins (right-shifted, `rgba(39, 174, 96, 0.4)`, drawn over the same axis):** `[0, 0, 0, 1, 3, 5, 8, 12, 18, 25, 35, 40, 38, 30, 20, 12, 6, 3, 2, 1]`.
- **Overlap zone:** rectangle from bin 6 to bin 10, full plot height, filled `rgba(243, 156, 18, 0.12)`; centered orange `#e67e22` 17px label "overlap" near the top.
- **Bottom labels (bold 17px):** red "neg (98%)" at bin 2; green "pos (92%)" at bin 15.

## 3. Twin Peaks Map to Classes

**Verdict badge:** USE — Peak Membership (green pill, `.v-use`)

**Bimodal Feature Where Each Peak = One Class**

- Shape detection found bimodal (two peaks with valley)
- Left peak dominated by one class, right peak by the other
- Valley region is mixed — don't classify there

**Example:** Hemoglobin in anemia screening. Left peak [7-11]: 91% pos (anemic, n=350). Right peak [12-17]: 94% neg (n=500). Valley [11-12]: mixed, skip. Coverage: 85%.

### Visualization (canvas `cc`, 720×240)

Bimodal histogram with color-coded peaks and a marked valley.

- **Title (bold 17px `#1a5276`, left):** "Hemoglobin — Bimodal, Each Peak = One Class".
- **Margins:** left 50, right 30, top 35, bottom 35; scale max 115.
- **Bins (concatenated):** left peak `[5, 15, 35, 60, 80, 70, 45, 20, 8]` in `rgba(39, 174, 96, 0.5)`; valley `[5, 4, 5]` in `rgba(243, 156, 18, 0.4)`; right peak `[8, 25, 55, 90, 110, 100, 65, 30, 12, 4]` in `rgba(231, 76, 60, 0.5)`.
- **Valley annotation:** vertical dashed orange line (`#e67e22`, 1.5px, dash 4/3) at valley center; orange 17px label "valley" near the top.
- **Bottom labels (bold 17px):** green "91% pos (anemic)" under the left peak (bin 4); red "94% neg (healthy)" under the right peak (bin 16).

## 4. Spike vs Spread — One-Directional

**Verdict badge:** PARTIAL — Only Deviations Classify (amber pill, `.v-partial`)

**One Class is a Spike, Other is Spread Out**

- Neg class concentrates at one value (e.g., normal body temp 98.6°F)
- Pos class is spread across a wider range
- Deviation FROM spike = pos signal. Being AT spike = inconclusive (not neg!)
- **Asymmetric power:** can confirm pos, cannot rule out pos

**Example:** Body temperature. Normal cluster at 98.4-98.8 (97% neg). Temp ≥99.0: enrichment 5x for pos. But 55% of infections have normal temp — only deviators get classified.

### Visualization (canvas `cd`, 720×240)

Overlaid spike (neg) vs spread (pos) histograms.

- **Title (bold 17px `#1a5276`, left):** "Body Temperature — Spike at Normal, Spread = Infection Signal".
- **Margins:** left 50, right 30, top 35, bottom 35; 20 bins; scale max 185.
- **Neg bins (spike at bins 8-9, `rgba(231, 76, 60, 0.4)`):** `[1, 2, 3, 5, 8, 12, 20, 40, 180, 150, 30, 8, 3, 1, 0, 0, 0, 0, 0, 0]`.
- **Pos bins (spread, `rgba(39, 174, 96, 0.4)`):** `[1, 2, 3, 4, 5, 6, 8, 10, 12, 10, 8, 7, 6, 5, 4, 3, 2, 2, 1, 1]`.
- **Annotations:** bold green 17px right-aligned "≥99°F → pos signal (5x)" near top-right; orange 17px "spike at 98.6 = inconclusive" near the bottom at bin 10.

## 5. Sparse Tail Signal

**Verdict badge:** PARTIAL — Small n in Tail (amber pill, `.v-partial`)

**Main Body Overlaps, Extreme Tail is One Class**

- Central distribution: both classes look the same
- Far tail (top 5-10% of values): dominated by one class
- Strong signal but low coverage — only extreme values classify
- CI may be wide due to small n in the tail

**Example:** CRP levels. Below 5: 82% neg (weak). CRP 20-100: 87% pos (n=138). CRP >100: 94% pos but only n=32. The tail is definitive, the body is noise.

### Visualization (canvas `ce`, 720×240)

Decaying histogram with gray mixed body and green definitive tail.

- **Title (bold 17px `#1a5276`, left):** "CRP — Main Body Overlaps, Tail is Definitive".
- **Margins:** left 50, right 30, top 35, bottom 35; scale max 125.
- **Bins (20):** `[80, 120, 100, 70, 40, 25, 15, 10, 8, 6, 5, 4, 3, 3, 2, 2, 2, 1, 1, 1]`; bins 0-7 `rgba(150,150,150,0.35)`, bins 8+ `rgba(39, 174, 96, 0.5)`.
- **Tail boundary:** vertical dashed green line (`#27ae60`, 1.5px, dash 4/3) at bin 8.
- **Bottom labels:** gray `#666` 17px "mixed (no signal)" at bin 4; bold green 17px "CRP > 20: 87-94% pos" at bin 14.

## 6. No Separation — Noise

**Verdict badge:** REJECT — No Signal (red pill, `.v-reject`)

**Both Classes Have Identical Distribution**

- Every bucket at every width: enrichment ~1.0 (matches base rate)
- KS test p > 0.05 — can't reject "same distribution"
- No tail, no peak, no bucket shows class difference
- Discard entirely — this feature is noise

**Example:** Patient ID last 2 digits. Uniform for both classes. No bucket at any width achieves enrichment > 1.2x. Feature carries zero information about the target.

### Visualization (canvas `cf`, 720×240)

Paired side-by-side bars, both classes nearly identical uniform.

- **Title (bold 17px `#1a5276`, left):** "Random Feature — Both Classes Identical".
- **Margins:** left 50, right 30, top 35, bottom 35; 20 bin pairs, each bin split into two half-width bars: neg height `40 + 5·sin(i·0.7)` in `rgba(231, 76, 60, 0.35)`, pos height `40 + 5·cos(i·0.9)` in `rgba(39, 174, 96, 0.35)`, scale max 50.
- **Bottom label (bold 17px `#c0392b`, centered):** "No bucket at any width shows enrichment > 1.2x → REJECT".

## 7. Monotonic Discrete Trend

**Verdict badge:** USE — Threshold Selection (green pill, `.v-use`)

**Ordered Discrete Values Correlate with Class**

- Integer/discrete feature where pos_ratio increases (or decreases) steadily with value
- No single value is overwhelmingly one class, but the trend is clear
- Pick a threshold where enrichment crosses significance

**Example:** Number of risk factors (0-6). Pos rate climbs: 3%→8%→15%→28%→45%→62%→80%. Threshold ≥4: enrichment 3.25x, n=600. Clear monotonic signal.

### Visualization (canvas `cg`, 720×240)

Bar chart with overlaid trend line for pos rate by risk-factor count.

- **Title (bold 17px `#1a5276`, left):** "Risk Factors (0-6) — Monotonic Enrichment Trend".
- **Margins:** left 55, right 30, top 35, bottom 45; y scale 0-100%.
- **Values (x label / pos %):** 0→3%, 1→8%, 2→15%, 3→28%, 4→45%, 5→62%, 6→80%.
- **Trend line:** orange `#e67e22`, 3px, through bar-center points.
- **Bar colors:** pct < 16 → `rgba(41,128,185,0.4)`; 16 ≤ pct < 30 → `rgba(200,200,200,0.4)`; pct ≥ 30 → `rgba(39,174,96,0.4)`. Bold 17px `#333` percent labels above bars; gray `#666` x-value labels below.
- **Base rate line:** horizontal dashed red (`#e74c3c`, 1px, dash 5/3) at 16%, right-aligned red 17px label "base rate 16%".

## 8. Categorical Dominance

**Verdict badge:** USE — Per Category (green pill, `.v-use`)

**Specific Categories Are Class Markers**

- No ordering assumed — each category checked independently
- Some categories are strong pos markers, others strong neg markers
- Ambiguous categories (enrichment 0.7-1.5x) are not used for classification

**Example:** Chest pain type. "typical_angina": 72% pos, enrichment 4.5x (n=500). "asymptomatic": 95% neg, enrichment 0.3x (n=1500). Two categories classify, two are ambiguous.

### Visualization (canvas `ch`, 720×240)

Category bar chart of pos rate with per-bar n labels.

- **Title (bold 17px `#1a5276`, left):** "Chest Pain Type — Categories as Class Markers".
- **Margins:** left 55, right 30, top 35, bottom 50; y scale 0-100%; bars at 50% alpha with bold "N% pos" labels above and two-line category name + "n=N" below in gray `#666`.
- **Categories:** "Typical Angina" 72% pos, n=500, `#1e8449`; "Atypical Angina" 35% pos, n=400, `#f4d03f`; "Non-Anginal" 10% pos, n=600, `#85c1e9`; "Asymptomatic" 5% pos, n=1500, `#2980b9`.
- **Base rate line:** horizontal dashed red (`#e74c3c`, 1px, dash 5/3) at 16%.

## 9. Decision Summary

| Scenario | Signal Source | Decision | Coverage |
|----------|---------------|----------|----------|
| A: Cluster gap | Gap = boundary | USE (green `#1e8449`) | Full |
| B: Shifted centers | Tails beyond overlap | USE (partial) (green `#1e8449`) | Varies |
| C: Twin peaks | Peak membership | USE (green `#1e8449`) | High (excl. valley) |
| D: Spike vs spread | Deviation from spike | PARTIAL (one-dir) (amber `#b57d00`) | Low |
| E: Sparse tail | Extreme values | PARTIAL (limited) (amber `#b57d00`) | Low |
| F: No separation | None | REJECT (red `#c0392b`) | N/A |
| G: Monotonic discrete | Ordered threshold | USE (green `#1e8449`) | High |
| H: Categorical | Specific category values | USE (per cat) (green `#1e8449`) | Varies |

## Callout (philosophy box)

**Key insight:** Most features are NOT scenario A (perfect separation). Real value comes from partial classifiers that each cover part of the population with high confidence. Multiple partial classifiers combined = full coverage.

## Regeneration instructions

- **Layout:** single long page. h1, `.subtitle`, a `.toc` box (background `#f8fafb`, border `1px solid #e0e0e0`, padding 20px 30px, radius 4px, bold "Table of Contents" heading + ordered anchor list `#a`–`#h`, `#summary`), then eight numbered h2 sections, each a one-row `.obj-table`: left `<td>` (45%) holds a `.verdict` pill + `.obj-title` + bullets + Example paragraph, right `<td>` (55%, centered) holds the canvas. Section 9 is a plain `.summary-table` (not obj-table). Page ends with a `.philosophy` callout.
- **Verdict pills:** `.verdict` inline-block, padding 3px 10px, radius 12px, 0.85em bold. `.v-use` background `#d4efdf` / text `#1e8449`; `.v-partial` background `#fef9e7` / text `#b57d00`; `.v-reject` background `#fadbd8` / text `#c0392b`.
- **Summary table:** `.summary-table` full width, 0.9em; `th` background `#eaf2f8`, padding 10px 12px, left-aligned, `#1a5276`, bottom border `2px solid #bbb`; `td` padding 8px 12px, bottom border `1px solid #e0e0e0`; Decision cells inline-colored as noted above.
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.4em `#1a5276` with `border-bottom: 2px solid #2980b9`; subtitle `#666` 1.05em; `strong` in `#1a5276`; obj-table cell borders `1px solid #e0e0e0`, padding 20px 24px, even rows `#fafcfe`; `.obj-title` 1.05em, weight 600, `#1a5276`; `.philosophy` background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em. No nav bar, no back/home links.
- **Canvas:** all canvases 720×240; intrinsic `width`/`height` attributes, scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart text uses 17px -apple-system. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60` (dark `#1e8449`), red `#e74c3c` (dark `#c0392b`), orange `#e67e22`, yellow `#f4d03f`, light blue `#85c1e9`, text grays `#666`/`#333`.
- In regenerated HTML, any card/page links use `.html` extensions.
