# Pitfall: Multicollinearity Blindness

**Page type:** detail page (sectioned card layout: per section an h2, then a two-column table — text left ~45% with tag pills/bullets/example/key-point, canvas right ~55%)
**HTML title tag:** Multicollinearity Blindness

**Subtitle:** Correlated features inflate importance and destabilize coefficients

## The Problem

Tags: `the trap` (red), `collinearity` (blue)

- **Shared signal** — two features carrying the same information leave no unique credit to assign
- **Arbitrary tie-break** — scores become artifacts of tie-breaking, not measures of real signal
- **Wild coefficients** — with r = 1.0 a linear model can assign +500 and -498 to the pair
- **Net vs parts** — the summed effect stays sensible while each single coefficient is garbage
- **Split importance** — importance algorithms divide credit, so each group member looks weak
- **Instability signal** — dropping one twin makes the other suddenly look very important

*Example:* Five blood-lipid features with r > 0.8 each get about 4% Random Forest importance, but dropping four lifts the survivor to 18%.

**Impact:** Coefficients and importance rankings become unstable and misleading, so genuinely predictive signals get dismissed as weak.

### Visualization (canvas `c1`, 720×300)

Before/after horizontal bar chart of feature importance split across correlated features vs consolidated.

- **Title (bold 14px, `#1a5276`, centered):** "Feature Importance: Signal Cannibalization by Correlated Features".
- **Divider:** dashed `#bdc3c7` vertical line at mid-width.
- **Left panel — header bold red (`#e74c3c`) "Before Dedup":** five horizontal red bars (fill alpha 0.6, 1.5px stroke, height 22, scale max 0.20 over 200px) starting at y=65, spacing 36, labels right-aligned in blue 11px, bold red percentage after each bar. Data: income 4.2%, salary 3.9%, earnings 4.1%, compensation 3.8%, pay_grade 4.0%.
- **Left brace:** orange (`#e67e22`) curly-style brace spanning all five bars with bold orange 10px text "same" / "signal".
- **Right panel — header bold green (`#27ae60`) "After Dedup":** one green bar (fill alpha 0.7, 2px stroke, height 28, width 18/20 of 200px) labeled "income" in blue 12px, with bold green 14px "18%" after it. Blue 11px caption lines below: "4 redundant features removed" and "signal consolidated into 1 feature".
- **Bottom annotation (bold blue 11px, centered):** "5 x ~4% = ~20% total signal cannibalized across correlated features"

## Why It Happens

Tags: `root cause` (orange), `redundant features` (blue)

- **Silent arrival** — redundant columns come from different sources, unit conversions, derivations
- **No complaint** — training APIs accept any feature matrix, so the redundancy goes unnoticed
- **More-is-better mentality** — every available feature is added without a correlation check
- **Duplicate units** — sources encode one concept several ways: height_cm, height_in, height_ft
- **Derived features** — engineered sums like total = part1 + part2 are collinear by construction
- **Split credit** — the shared signal is spread across the group, so each member appears weak

*Example:* With height_cm and height_inches at r = 0.999, a logistic regression assigns β_cm = +342 and β_inches = -340.

**Root Cause:** If X1 ≈ X2, then β1·X1 + β2·X2 has infinitely many solutions along β1 + β2 = constant, and the optimizer picks one arbitrarily.

### Visualization (canvas `c2`, 720×300)

Three correlated feature clusters as node graphs, with a row of uniformly weak importance bars below.

- **Title (bold 14px, `#1a5276`, centered):** "Correlated Feature Clusters: Signal Cannibalization".
- **Clusters (nodes on a radius-55 circle, all pairs connected by thick red `#e74c3c` lines at width 3, alpha 0.4):**
  - Cluster 1 centered (130, 130): height_cm, height_in, height_ft, height_m.
  - Cluster 2 centered (360, 150): income, salary, earnings.
  - Cluster 3 centered (580, 130): temp_F, temp_C, temp_K, heat_idx.
- **Nodes:** circles radius 18, fill `rgba(26,82,118,0.1)`, blue 1.5px stroke, feature name in blue 8px centered.
- **Cluster labels:** bold orange (`#e67e22`) 10px "Cluster 1/2/3" below each, with red 9px "r > 0.8" underneath.
- **Importance strip (y=225):** bold blue label "Importance:" then 11 small red bars (22×30, fill alpha 0.5) each labeled "~4%" in blue 7px.
- **Bottom annotation (bold orange 12px, centered):** "Same signal, split N ways → each feature looks individually weak"

## The Correct Approach

Tags: `the fix` (green), `feature pruning` (blue)

- **Measure first** — quantify redundancy before trusting any coefficient or importance score
- **VIF screening** — flag features whose variance inflation factor exceeds roughly 5-10
- **Correlation clustering** — group features with pairwise |r| above ~0.8; keep one per cluster
- **Lasso regularization** — L1 drives redundant coefficients to zero, selecting automatically
- **PCA alternative** — orthogonal components remove collinearity at a cost to interpretability
- **Domain knowledge** — keep the cluster member that is most interpretable and best measured

*Example:* Keeping only "income" from five income features (r > 0.85) drops VIF from 45 to 3.2 and consolidates five 4% scores into one 18% score.

**Fix:** Cluster features above a chosen |r| level, keep one representative per cluster, and verify the survivors with a VIF check aiming below roughly 5.

### Visualization (canvas `c3`, 720×300)

Four-step pipeline, before/after cluster consolidation, and importance comparison bars.

- **Title (bold 14px, `#1a5276`, centered):** "Correct: VIF Check + Cluster Representative Selection".
- **Pipeline (four boxes 130×55 centered horizontally at y=42, blue arrows between):** "Correlation Matrix" (sub: "pairwise |r|"), "Identify Clusters" (sub: "|r| > 0.8"), "Select Representative" (sub: "best per cluster") — all fill `#ebf5fb` with blue border; "VIF < 5 Check" (sub: "verify clean") — fill `#eafaf1` with green border. Labels bold blue 11px on two lines, subs `#555` 9px.
- **Before (y=120):** bold red "Before:"; five red-tinted nodes (radius 16, fill `rgba(231,76,60,0.15)`) labeled income, salary, earnings, comp., pay_gr., pairwise-connected by faint red lines; bold red caption "VIF = 45".
- **Transition:** blue arrow labeled "keep best" (blue 10px).
- **After:** bold green "After:"; single green node (radius 24, fill `rgba(39,174,96,0.2)`, green 2px stroke) labeled "income"; bold green caption "VIF = 3.2"; bold green "✔" beside it.
- **Feature Importance Comparison (from y=200, bold blue centered title):** "Before:" (red 10px) row of five red bars (40×20, fill alpha 0.5) each labeled "4%"; "After:" (green 10px) one green bar (180×22, fill alpha 0.6) with white bold centered label "18% — income (representative)".
- **Bottom annotation (blue 11px, centered):** "Same predictive power, stable coefficients, interpretable importance scores"

## Regeneration instructions

- **Template/layout:** ml-pipeline-pitfalls detail page. h1 + `.subtitle`, then three `.card-section` blocks ("The Problem", "Why It Happens", "The Correct Approach"). Each section: `h2` with 2px `#2980b9` bottom border, then a `table.layout` (border-collapse, full width) with one row — `td.text-col` (45%) and `td.viz-col` (55%), both top-aligned, 12px padding.
- **Text column structure:** `.tags` div of pill spans, then `ul` of bullets with `<b>` lead-ins (bold `#1a5276`), then italic `.example` paragraph, then `.key-point` callout (background `#f8f9fa`, left border 3px solid `#e74c3c`, 8px 12px padding, 0.9rem) whose `<strong>` label is Impact/Root Cause/Fix. HTML entities used in source text: `&gt;`, `&beta;`, `&asymp;`, `&middot;`, `&mdash;`.
- **Tag pills:** inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px. Colors: blue `rgba(26,82,118,0.12)`/`#1a5276`; green `rgba(39,174,96,0.15)`/`#27ae60`; red `rgba(231,76,60,0.12)`/`#e74c3c`; orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; subtitle `#666` 0.95rem; canvas `width:100%`, 1px `#e0e0e0` border, 4px radius; ul 0.92rem. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, secondary blue `#2980b9`, bar fill `rgba(26,82,118,0.35)`.
- **Canvas:** each canvas declares intrinsic width=720 height=300 and is drawn via a shared `setup(id)` helper that sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- In regenerated HTML, any card links use `.html` extensions.
