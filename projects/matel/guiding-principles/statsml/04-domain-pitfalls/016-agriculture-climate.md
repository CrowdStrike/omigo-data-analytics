# Agriculture / Climate: Domain-Specific Pitfalls

**Page type:** detail page (h2 section per pitfall, each with a two-column obj-table row: text left 50%, canvas right 50%)
**HTML title tag:** Domain Pitfalls: Agriculture / Climate

**Subtitle:** Agriculture faces fundamental data scarcity — one harvest per year, non-stationary climate, unrepeatable experiments, and measurement that can't distinguish crop from weed.

## Callout (philosophy box)

**Core tension:** Agriculture demands prediction (when to plant, irrigate, spray) but provides almost no data per decision cycle. A 20-year veteran farmer has seen exactly 20 harvests — try fitting a neural network to that.

## One Harvest Per Year = Tiny Sample Sizes

**The Fundamental Data Scarcity**

Unlike web analytics (millions of events/day) or manufacturing (thousands of units/shift), agriculture yields **one outcome per year per field**. After 20 years of meticulous record-keeping, you have n=20.

- Can't learn complex interactions (soil x weather x variety x timing)
- Overfitting is almost guaranteed with more than 2-3 predictors
- Cross-validation is unreliable with so few samples
- A single drought year dominates the dataset

**Implication:** Simple models (linear, 1-2 variables) are often all the data can support.

### Visualization (canvas `canvas1`, 720×300)

Staircase bar chart: cumulative sample count, one bar per year.

- **Title (600 17px `#1a5276`, top center):** "Data Accumulation: 1 Sample Per Year".
- **Bars:** 20 bars, 22px wide with 10px gaps, starting at x=60; bar i has height ((i+1)/20)×100px (linear staircase from 1 to 20 samples), baseline at y=h-40. First 10 bars filled `#2980b9`, last 10 filled `#1a5276`.
- **X labels:** every 5th year labeled (2009, 2014, 2019, 2024 — computed as 2005+i) in 11px gray `#666`; thin `#ccc` axis line along the baseline.
- **Y-axis:** rotated gray 13px label "Samples (n)".
- **Annotation (right of last bar, red `#e74c3c`):** "n = 20" (600 15px) with "after 20 years!" (12px) beneath.
- **Caption (bottom center, gray 12px):** "Compare: a website gets n=20 in seconds".

## Climate Non-Stationarity

**"Normal" Rainfall Is Shifting**

Historical averages become misleading as climate changes. The rainfall distribution your model was trained on **no longer applies**.

- 30-year "normals" include data from a different climate regime
- Extreme events are becoming more frequent (tail fattening)
- Growing season dates are shifting earlier
- Past correlations (e.g., April rain predicts July yield) may break

**Implication:** Models must account for trend, not just variance. Past performance does not predict future results — literally.

### Visualization (canvas `canvas2`, 720×300)

Three overlaid bell curves showing a rainfall distribution shifting right and widening.

- **Title (600 17px `#1a5276`, top center):** "Rainfall Distribution Shift Over Time".
- **Curves** (Gaussian shape, peak height 100px above baseline y=h-45, drawn from x=80 to x=w-40; mean/sd expressed as fractions of the x-range; each labeled above its peak in its own color, 12px):
  - "Historical (1960-1990)": mean 0.35, sd 0.08, green `#27ae60`, solid, width 2.5.
  - "Current (2000-2020)": mean 0.45, sd 0.10, orange `#e67e22`, solid.
  - "Projected (2040-2060)": mean 0.58, sd 0.12, red `#e74c3c`, dashed 6/4.
- **X-axis:** thin `#999` baseline; gray 11px labels "Low" (left), "Rainfall Amount" (center), "High" (right).
- **Shift arrow:** red `#e74c3c` horizontal arrow below the axis from x-fraction 0.35 to 0.58 with filled arrowhead, labeled beneath in red 11px: "\"Normal\" is shifting".

## No Controlled Experiments at Scale

**Each Year Is a One-Off Experiment**

You cannot replicate a growing season. Every year brings a unique combination of weather, pest pressure, soil moisture history, and market conditions.

- Can't A/B test: "same field, same year, different treatment" is impossible
- Field trials control some variables but weather remains uncontrolled
- What worked in 2019 may fail in 2020 for unknowable reasons
- Regional variation means neighbor's results may not transfer

**Implication:** Causal claims require extreme caution. Correlation across years conflates treatment with year effects.

### Visualization (canvas `canvas3`, 720×300)

Timeline of eight years, each marked with a unique weather event.

- **Title (600 17px `#1a5276`, top center):** "Each Year: A Unique, Unrepeatable Experiment".
- **Timeline:** horizontal `#ccc` line width 2 at y=85 from x=60 to x=w-60, with 8 evenly spaced 8px-radius circle markers.
- **Years and events (marker color / event label below marker, year label above in 600 11px `#333`):**
  - 2017 — "Drought" — red `#e74c3c`
  - 2018 — "Flood" — blue `#2980b9`
  - 2019 — "Normal" — green `#27ae60`
  - 2020 — "Late frost" — purple `#9b59b6`
  - 2021 — "Heatwave" — orange `#e67e22`
  - 2022 — "Wet spring" — blue `#2980b9`
  - 2023 — "Early fall" — orange `#e67e22`
  - 2024 — "Ideal" — green `#27ae60`
- **"n=1" labels:** small 10px `#999` "n=1" below every marker.
- **Annotations:** dashed red line (dash 4/3) below the timeline spanning markers 2–4 with a red 600 16px "✗" beneath it; bottom caption in red 12px: "Cannot replicate any year to verify what caused the outcome".

## Satellite Imagery Limitations (NDVI)

**Weeds Are Green Too**

NDVI (Normalized Difference Vegetation Index) measures "greenness" — but it cannot distinguish crop from weed. A lush-looking field might be **50% weeds**.

- High NDVI ≠ healthy crop (could be vigorous weed growth)
- Cloud cover creates temporal gaps in critical growth stages
- Resolution limits: 10m pixels average over mixed vegetation
- Soil background affects readings in early season (sparse canopy)

**Implication:** Ground-truthing remains essential. Remote sensing complements but cannot replace field scouting.

### Visualization (canvas `canvas4`, 720×300)

Two green field boxes with near-identical NDVI readings.

- **Title (600 17px `#1a5276`, top center):** "NDVI Cannot Distinguish Crop from Weed".
- **Left box (200×100 at x=80, y=50):** solid green `#27ae60` with 5 horizontal darker-green `#1e8449` crop-row lines; white 600 13px centered label "Healthy Crop". Below: green 600 14px "NDVI = 0.82" and gray 11px "100% crop".
- **Right box (200×100 at x=420, y=50):** solid green `#27ae60` with 12 chaotic darker-green `#1e8449` quadratic-curve weed squiggles at seeded-random positions plus 5 red `#e74c3c` 4px dots (struggling crop); white 600 13px centered label "50% Weeds". Below: green 600 14px "NDVI = 0.79" and red 11px "50% weeds, 50% crop".
- **Between the boxes:** large orange `#e67e22` 600 28px "≈" symbol, with orange 600 12px caption below it: "Satellite sees same \"greenness\"!".

## Intervention Confounds (The Prevention Paradox)

**Successful Prevention Looks Like Waste**

When a farmer sprays pesticide and sees no pest damage, the model concludes: "pesticide was unnecessary." But the **absence of damage IS the evidence it worked**.

- Applied fungicide → no disease → model says "low risk, don't spray"
- Irrigated during dry spell → normal yield → model says "irrigation unnecessary"
- Historical data is biased: damage only appears when intervention was skipped
- Removing interventions to "test" them risks catastrophic loss

**Implication:** Observational data cannot evaluate preventive treatments without careful counterfactual reasoning.

### Visualization (canvas `canvas5`, 720×300)

Causal diagram: three rounded boxes with arrows and a dashed feedback loop.

- **Title (600 17px `#1a5276`, top center):** "The Intervention Paradox".
- **Boxes (rounded rect, radius 6, height 34, centered on y=95, 600 12px labels):**
  - "Spray Pesticide" — 130px wide at x=40, fill `#eaf4e8`, border/text green `#27ae60`.
  - "No Pest Damage" — 130px wide at x=240, same green style.
  - "\"Pesticide unnecessary\"" — 200px wide at x=440, fill `#fdecea`, border/text red `#e74c3c`.
- **Arrows:** green `#27ae60` arrow from box 1 to box 2 labeled "causes" (11px green above); red `#e74c3c` arrow from box 2 to box 3 labeled "model infers" (11px red above).
- **Feedback loop:** dashed orange `#e67e22` curve (dash 5/3, width 2) from the bottom of box 3 sweeping under the diagram back up to the bottom of box 1, ending in a filled orange arrowhead; labeled below in orange 600 12px: "Stop spraying → pest outbreak → \"see, pesticide was needed!\"".
- **Top annotation (gray `#999` 11px, centered):** "Reality: absence of evidence (damage) ≠ evidence of absence (risk)".

## Regeneration instructions

- **Layout:** h1 + `.subtitle` + `.philosophy` callout, then one `h2` per pitfall (1.4em `#1a5276`, bottom border `2px solid #2980b9`, padding-bottom 8px), each followed by a `.obj-table` with a single `<tr>`: left `<td>` (45%) holds `.obj-title` (1.05em, weight 600, `#1a5276` — rendered as a `<p class="obj-title">` on this page), an intro paragraph, a `<ul>` of bullets, and an **Implication** paragraph; right `<td>` (55%, centered) holds the canvas.
- **Table style:** full width, border-collapse; cell borders `1px solid #e0e0e0`, padding 20px 24px, vertical-align middle; even rows background `#fafcfe`.
- **Page CSS:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; `.subtitle` `#666` 1.05em; paragraphs `#333` 0.95em; `ul` 0.9em `#333`; `strong` `#1a5276`; `.philosophy` background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, dark green `#1e8449`, red `#e74c3c`, orange `#e67e22`, purple `#9b59b6`, gray text `#666`/`#999`/`#333`.
- **Canvas:** declare intrinsic `width`/`height` attributes as given per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
