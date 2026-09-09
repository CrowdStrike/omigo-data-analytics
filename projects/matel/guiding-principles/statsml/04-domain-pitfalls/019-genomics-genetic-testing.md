# Genomics / Genetic Testing: Domain-Specific Pitfalls

**Page type:** detail page (two-column obj-table layout: text left 50%, canvas right 50%, one h2 + one-row table per pitfall)
**HTML title tag:** Domain Pitfalls: Genomics / Genetic Testing

**Subtitle:** Genomics combines extreme multiple testing, population structure confounds, linkage effects, and tiny effect sizes requiring massive sample sizes — a perfect storm for false discoveries.

## Callout (philosophy box)

**Core challenge:** The genome contains ~20,000 protein-coding genes and millions of variants. Testing them all simultaneously while accounting for population structure, linkage, and minuscule effect sizes demands extraordinary statistical rigor. Most reported genetic associations fail to replicate.

## Massive Multiple Testing

**20,000 Genes Tested Simultaneously**

Genome-wide association studies (GWAS) test hundreds of thousands to millions of genetic variants. At a conventional p<0.05 threshold, you would expect ~50,000 false positives from 1 million tests.

- **Bonferroni threshold:** p < 0.05/20,000 = 2.5 × 10⁻⁶ (rendered in HTML as `2.5 &times; 10<sup>-6</sup>`)
- **GWAS standard:** p < 5 × 10⁻⁸ (genome-wide significance) (rendered as `5 &times; 10<sup>-8</sup>`)
- Most "significant" findings at p<0.05 are false positives
- Even Bonferroni is conservative due to correlated tests

### Visualization (canvas `canvas1`, 720×300)

Manhattan plot (scatter of SNP p-values by chromosome) with two dashed significance-threshold lines.

- **Title (bold 14px, `#1a5276`, top center):** "Manhattan Plot — Most \"Hits\" Are Below Significance".
- **Plot area:** margins left 60 / right 20 / top 25 / bottom 40; background `#f8f9fa`.
- **Y-axis:** -log10(p) from 0 to 10, tick labels every 2 (0,2,4,6,8,10) in `#333`, light horizontal gridlines `#e0e0e0` (0.5px); rotated y-axis label "-log10(p)" in `#1a5276`.
- **X-axis:** 22 chromosomes, each an equal-width slot; chromosome number labels every 3rd chromosome (1, 4, 7, …) in `#666`; axis label "Chromosome" centered below in `#666`.
- **Threshold lines (dashed 6/4, width 2):** genome-wide at -log10(p)=7.3 in red `#e74c3c`, labeled "Genome-wide: p<5e-8" (red, left-aligned near right edge, above line); Bonferroni at -log10(p)=5.6 in orange `#e67e22`, labeled "Bonferroni: p<2.5e-6" (orange).
- **Points:** per chromosome, 30–49 SNPs at seeded pseudo-random x positions (LCG seed 42: `seed = (seed*1664525 + 1013904223) & 0xffffffff`). Most points have -log10(p) drawn as `rand()*3 + rand()*2`; with probability 0.05 a point is boosted to `4 + rand()*3`; with probability 0.01 to `7 + rand()*3`. Point styling: above 7.3 → green `#27ae60`, radius 4; between 5.6 and 7.3 → orange `#e67e22`, radius 3.5; otherwise alternating chromosome colors `#1a5276` / `#2980b9` at 0.6 alpha, radius 2.5.

## Population Stratification

**Ancestry Confounds Genetic Associations**

Ancestry correlates with both genetic variants AND disease risk independently. Without correction, you find spurious associations driven by population structure rather than biology.

- **Example:** Chopstick gene — allele frequency differs by ancestry, as does diet-related disease
- Principal components (PCs) of ancestry used as covariates
- Genomic control inflation factor (λ) should be ~1.0
- Mixed ancestry cohorts especially vulnerable

### Visualization (canvas `canvas2`, 720×300)

Confounding-triangle diagram: three circular nodes connected by labeled arrows.

- **Nodes (radius 34, white bold 11px text, may be two lines):** "Ancestry" at top center (x = w/2, y=35), fill `#1a5276`; "Genetic\nVariant" at lower left (w/2 − 180, y=160), fill `#2980b9`; "Disease\nRisk" at lower right (w/2 + 180, y=160), fill `#e67e22`.
- **Arrows (2.5px, arrowheads, drawn behind nodes with 38px offset from centers, midpoint labels offset 14px perpendicular):** Ancestry → Genetic Variant, solid blue `#1a5276`, labeled "allele freq differs"; Ancestry → Disease Risk, solid blue `#1a5276`, labeled "disease rate differs"; Genetic Variant → Disease Risk, dashed (6/4) red `#e74c3c`, labeled "SPURIOUS association".
- **Bottom caption (red `#e74c3c`, 12px, centered):** "Confounder: ancestry drives BOTH variant frequency and disease rate".

## Linkage Disequilibrium (LD)

**Nearby Variants Inherited Together**

Genes close together on a chromosome are co-inherited as blocks. A significant association may tag a nearby causal variant rather than being causal itself — you cannot isolate the true driver.

- **LD blocks:** Regions of 10-500kb inherited as units
- Lead SNP may not be causal — just in LD with the causal variant
- Fine-mapping required to narrow candidates
- Different ancestries have different LD patterns (useful for fine-mapping)

### Visualization (canvas `canvas3`, 720×300)

Chromosome-backbone diagram with an LD block highlight, SNP tick marks, and two haplotype bars.

- **Title (bold 14px, `#1a5276`, centered):** "Linkage Disequilibrium — Variants Inherited as a Block".
- **Chromosome backbone:** rounded horizontal bar (height 16, radius 8) in `#d5dbdb` at y=65, margins 40 each side.
- **LD block highlight:** rectangle from 25% to 65% of plot width, spanning y 30–115: fill `rgba(231,76,60,0.12)`, dashed (4/3) red `#e74c3c` 2px border; bold red label above: "LD Block (~200kb)".
- **SNPs:** 9 tick marks (4px wide, extending 4px past backbone) at fractional positions `[0.1, 0.28, 0.35, 0.42, 0.5, 0.58, 0.63, 0.75, 0.9]`, labels below: `SNP1, SNP2, SNP3, SNP4, Causal?, SNP6, SNP7, SNP8, SNP9`, colors `[#2980b9, #e74c3c, #e74c3c, #e74c3c, #27ae60, #e74c3c, #e74c3c, #2980b9, #2980b9]`. Green star "★" (`#27ae60`, 16px) above the Causal? position (0.5).
- **Haplotypes:** bold blue label "Haplotypes:" at left (y=120); two rounded bars (320×18, radius 4, 0.85 alpha) with white bold monospace text and gray "(co-inherited)" note to the right: "Hap A:  A - G - T - C - A - G - T" in `#2980b9`; "Hap B:  G - A - C - T - G - A - C" in `#e67e22`.
- **Bottom caption (`#333`, 12px, centered):** "Problem: SNP2-7 are all correlated (r² > 0.8) — cannot pinpoint which is truly causal".

## Polygenic Score Portability Across Populations

**GWAS on Europeans Fails on Other Populations**

Polygenic risk scores (PRS) are derived primarily from European-ancestry GWAS. Their predictive accuracy drops dramatically when applied to non-European populations due to differences in allele frequencies, LD structure, and environmental interactions.

- **~80%** of GWAS participants are of European ancestry
- PRS accuracy drops 2-5x in African-ancestry populations
- Different causal variants may operate in different populations
- Risk of exacerbating health disparities

### Visualization (canvas `canvas4`, 720×300)

Vertical bar chart of relative PRS accuracy by population, with a dashed decline arrow.

- **Title (bold 14px, `#1a5276`, centered):** "Polygenic Score Accuracy by Population".
- **Data:** populations `["European\n(training)", "East Asian", "South Asian", "Hispanic/\nLatino", "African"]` with accuracy `[0.85, 0.52, 0.45, 0.38, 0.22]` and bar colors `[#27ae60, #2980b9, #2980b9, #e67e22, #e74c3c]`.
- **Axes:** margins left 80 / right 40 / top 35 / bottom 50. Y-axis 0–100% with ticks every 20% and light gridlines `#e8e8e8`; rotated y label "Relative PRS Accuracy (R²)" in `#1a5276`. Bars 60% of slot width, rounded top corners (radius 4), bold percentage value labels above each bar in the bar's color, population labels (possibly two lines) below in `#333`.
- **Decline arrow:** dashed (5/3) red `#e74c3c` 2px line from just above the first bar top to just above the last bar top, with red arrowhead at the end.
- **Bottom caption (red `#e74c3c`, 11px, centered):** "Accuracy declines with genetic distance from training population".

## Rare Variants and Tiny Effect Sizes

**Each Variant Explains Almost Nothing**

For complex traits, individual common variants each explain ~0.01% of phenotypic variance. Detecting such tiny effects requires enormous sample sizes (n > 500,000), and even then the clinical utility is limited.

- **Typical effect:** Each SNP explains 0.01-0.05% of variance
- Height: ~10,000 variants explain ~50% of heritability
- Need n=500,000+ to detect individual small effects
- "Missing heritability" — known variants explain only a fraction

### Visualization (canvas `canvas5`, 720×300)

Two-panel chart: left, a stacked variance-explained bar; right, horizontal log-scaled sample-size bars.

- **Title (bold 14px, `#1a5276`, centered):** "Effect Size per Variant vs. Sample Size Required".
- **Left panel — "Variance Explained" (bold 12px `#1a5276` heading):** a tall vertical bar (50×130) with background `#ecf0f1` and border `#bdc3c7` representing total variance = 100%. Bottom 15% is filled with 20 thin alternating slivers (`#2980b9` / `#3498db`) representing known variants. Side labels: blue "Known variants" / "(~15% of variance)"; gray `#999` "\"Missing" / "heritability\"" / "(~85%)" beside the unknown portion; red `#e74c3c` note below bar: "Each variant:" / "~0.01% of total".
- **Right panel — "Sample Size Required" (bold 12px `#1a5276` heading):** four horizontal rounded bars (height 18, radius 3), widths proportional to log10(n)/log10(500000):
  - "Large effect (OR>2)" — n=1,000, green `#27ae60`
  - "Medium effect (OR~1.3)" — n=10,000, orange `#e67e22`
  - "Small effect (OR~1.05)" — n=100,000, red `#e74c3c`
  - "Tiny effect (OR~1.01)" — n=500,000, dark red `#c0392b`
  - Each bar has its label above in `#333` and a bold "n=…" value inside the bar (white, right-aligned) when the bar is wide enough, otherwise beside it in the bar color.
- **Bottom caption (red `#e74c3c`, 11px, centered):** "Most GWAS variants have tiny effects (OR ~ 1.01-1.05) requiring n > 500,000".

## Regeneration instructions

- **Layout:** domains detail-page template (139-style): h1, `.subtitle`, one `.philosophy` callout, then per pitfall an unnumbered `<h2>` followed by a one-row `.obj-table` — left `<td>` (45%) with `.obj-title`, a paragraph, and a `<ul>` of bullets; right `<td>` (55%, centered) with a single `<canvas width="720" height="300">`. Even table rows have background `#fafcfe`. No thead, no nav, no badges, no cross-page links.
- **Page CSS:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6. h1 1.8em `#1a5276`; h2 1.4em `#1a5276` with 2px solid `#2980b9` bottom border, 8px padding-bottom, margin 40px 0 15px. `.subtitle` `#666` 1.05em. p 0.95em `#333`; ul 0.9em `#333`, li margin 4px 0; `strong` `#1a5276`. `.philosophy`: background `#f0f4f8`, left border 4px solid `#2980b9`, padding 12px 16px, 0.9em. `.obj-table` full width, collapsed borders, cells `1px solid #e0e0e0` with padding 20px 24px, vertical-align middle. `.obj-title` 1.05em, weight 600, `#1a5276`. Canvas `display: block; margin: 0 auto`.
- **Canvas scaling:** shared `setup(id)` helper using `window.devicePixelRatio` — multiply backing-store width/height by dpr, `ctx.scale` back to logical coordinates. Each chart in its own IIFE.
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`/`#3498db`, green `#27ae60`, red `#e74c3c`, dark red `#c0392b`, orange `#e67e22`, grays `#333`/`#666`/`#999`.
- In regenerated HTML, any card links use `.html` extensions (this page has no outgoing links).
