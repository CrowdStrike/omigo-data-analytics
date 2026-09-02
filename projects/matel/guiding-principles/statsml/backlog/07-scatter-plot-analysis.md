# Scatter Plot Analysis via CNN or LLM

**Page type:** detail page (backlog kusto-style: intro callout, numbered h2 sections, each a 2-col table with text left 45% / canvas right 55%)
**HTML title tag:** Scatter Plot Analysis via CNN or LLM — Discussion Backlog

**Subtitle:** Catch interaction effects invisible in 1D histograms

**Intro callout:** Univariate profiling misses interaction effects. Render colored scatter plots and analyze visually to catch structure invisible in 1D histograms.

## 1. What It Catches

- Clusters invisible in 1D histograms
- Non-linear relationships (quadratic, exponential)
- Heteroscedasticity (funnel shapes)
- Outlier subgroups

### Visualization (canvas `c1`, 720×300)

Three small scatter plots side by side illustrating structure types.

- **Title (bold 17px, `#1a5276`, top center):** "Scatter Plot Structure Types".
- **Layout:** three plots, each (720−80)/3 ≈ 213px wide × (300−80) = 220px tall, starting at y=40, x = 20 + p×(plotW+20); each plot has a 1px `#bdc3c7` border rectangle.
- **Points:** 60 points per plot, radius 3, generated with a seeded LCG PRNG (seed 42, a=1103515245, c=12345, m=0x80000000) shared sequentially across the three plots; coordinates clamped 3px inside the border.
  - Plot 1 "Linear" (points `#1a5276`): y descends linearly with x plus uniform noise ±15px (py = plotH − x-proportional height + (rand−0.5)×30).
  - Plot 2 "Clustered" (points `#27ae60`): 3 clusters centered at fractional (x, y) positions (0.2, 0.3), (0.7, 0.3), (0.5, 0.75) of the plot, jitter ±20px horizontal / ±17.5px vertical.
  - Plot 3 "Funnel" (points `#e67e22`): x uniform; y centered at plotH/2 with spread growing linearly with x (spread = (px/plotW)×80), i.e. heteroscedastic funnel.
- **Labels:** bold 15px, centered below each plot in the plot's point color: "Linear", "Clustered", "Funnel".
- **Caption (italic, `.example`):** Structure types: linear, clustered, funnel

## 2. Two Approaches

- **Option A — CNN:** Classify structure (linear, clustered, funnel, etc.) with LLM-guided training data generation
- **Option B — LLM:** Pass rendered image to vision model, ask it to describe patterns

**Key Question:** Which pairs to plot? All O(n^2) is expensive. Heuristic: correlation pre-filter, or mutual information ranking?

### Visualization (canvas `c2`, 720×300)

Horizontal pipeline flow diagram with four filled rounded boxes connected by arrows.

- **Title (bold 17px, `#1a5276`, top center):** "Analysis Pipeline".
- **Boxes:** 120×60, rounded corners radius 8, solid fill with white bold 15px two-line centered labels, vertically centered at h/2+10; box centers at x = 60, 220, 380, 540:
  - "Feature / Pairs" — fill `#1a5276`
  - "Render / Scatter" — fill `#2980b9`
  - "CNN / LLM / Analysis" — fill `#27ae60`
  - "Structure / Label" — fill `#e67e22`
- **Arrows:** 2px `#2c3e50` horizontal lines with filled triangular heads between consecutive boxes.
- **Output list (14px `#2c3e50`, centered under the last box, one per line):** "linear", "clustered", "funnel", "random", "quadratic"; with a vertical orange (`#e67e22`) 1.5px bracket line to their left.
- **Sub-labels (13px `#555`, centered under the CNN/LLM box):** "Option A: Trained CNN classifier" / "Option B: Vision LLM (GPT-4V, Claude)".
- **Caption (italic, `.example`):** Analysis pipeline: feature pairs → render → CNN/LLM → structure label

## Regeneration instructions

- **Layout:** backlog detail page (kusto-style). Structure: `<h1>` with bottom border `2px solid #2980b9`, `.subtitle` paragraph, one `.intro` callout, then one `.lang-section` per numbered section. Each section: `<h2>` ("N. Title", bottom border `2px solid #2980b9`), then a `table.layout` (width 100%, border-collapse) with one `<tr>`: left `td.text-col` (45%) holding bullets and optional `.key-point` callout, right `td.viz-col` (55%) holding the canvas plus an italic `.example` caption. No index number in the h1/title.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276`; h2 1.3rem `#1a5276`; subtitle `#666` 0.95rem. `.intro`: background `#f0f4f8`, left border `3px solid #2980b9`, padding 8px 12px, 0.9rem. `.key-point`: background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem. `.example`: italic, `#555`, 0.9rem. `ul` 0.92rem. `code`: background `#e8f0f8`, color `#1a5276`, padding 2px 6px, radius 3px. Canvas: `width: 100%`, border `1px solid #e0e0e0`, radius 4px. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`.
- **Canvas:** intrinsic size 720×300 per chart; a shared `setupCanvas(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates.
- In regenerated HTML, any card/page links use `.html` extensions.
