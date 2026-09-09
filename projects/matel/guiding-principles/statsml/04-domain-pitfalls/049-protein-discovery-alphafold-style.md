# Protein Discovery / AlphaFold-Style: Data Pitfalls

**Page type:** detail page (one h2 per pitfall, each followed by a two-column obj-table row: text left 50%, canvas right 50%)
**HTML title tag:** Protein Discovery / AlphaFold-Style - Data Pitfalls

**Subtitle:** Data traps in protein structure prediction — conformational ensembles, MSA depth bias, training-set skew, and distribution shift to designed proteins.

## Conformational Ensemble Ignored (Single Structure Bias)

- One amino acid sequence can fold into MULTIPLE structures depending on conditions, binding partners, pH
- AlphaFold predicts one static structure but reality is a probability distribution over conformations
- The "answer" is a probability cloud (Boltzmann ensemble) not a single point prediction
- Intrinsically disordered proteins (30% of human proteome) have NO single structure
- Drug binding sites often depend on rare conformational states not captured by the dominant fold

**Example:** Protein kinases switch between active/inactive conformations; AlphaFold picks one, missing the drug-targetable state that exists 5% of the time.

### Visualization (canvas `canvas1`, 720×300)

Branching diagram: one amino acid sequence on the left fanning out to four possible conformations with probabilities.

- **Title (bold 17px, `#1a5276`, at x=200, y=22):** "One Sequence → Multiple Conformations".
- **Backbone:** horizontal blue line (`#2980b9`, width 4) from x=30 to x=150 at mid-height, with 8 small blue filled circles (radius 4, spaced 15px starting x=40) representing amino acids; label below in 11px `#2c3e50`: "Amino acid sequence".
- **Four conformations** (quadratic-curved arrows from x=150/mid-height to x=350 at each target y, each with an arrowhead, a 30×14 ellipse blob at x=395 filled at 0.4 alpha and stroked in the conformation color, a bold 12px probability label and a 10px `#2c3e50` name label at x=430):
  - y=30, "60%", `#27ae60`, "Active (compact)"
  - y=75, "25%", `#2980b9`, "Inactive (extended)"
  - y=130, "10%", `#e67e22`, "Partially unfolded"
  - y=170, "5%", `#e74c3c`, "Rare drug-binding state"
- **Annotation (bold 11px red `#e74c3c` at x=530):** two lines "AlphaFold picks" / "this one →", with a dashed red line (dash 4/3, width 1.5) from x=528 to x=430 at y=37 pointing to the 60% blob.
- **Bottom note (11px purple `#8e44ad` at x=350, y=h-10):** "Reality: Boltzmann ensemble of states, not a single answer".

## MSA (Multiple Sequence Alignment) Depth Bias

- Model performance correlates strongly with number of homologous sequences available (MSA depth)
- Works great for well-studied protein families with 1000+ homologs in sequence databases
- Fails catastrophically for orphan proteins with few or no evolutionary relatives
- Performance gap: >90% accuracy for deep MSAs vs <50% for shallow MSAs
- Viral proteins, de novo designed proteins, and taxonomically isolated organisms have sparse MSAs
- Prediction confidence is really measuring "how studied is this family" not "how hard is this problem"

**Example:** AlphaFold confidence (pLDDT) drops from 90+ to below 50 when MSA depth goes from 1000 to under 10 sequences — the model literally doesn't know what to do without evolutionary context.

### Visualization (canvas `canvas2`, 720×300)

Scatter + curve chart: prediction accuracy rising with MSA depth on a log-scale x-axis.

- **Title (bold 17px, `#1a5276`):** "MSA Depth vs Prediction Accuracy".
- **Chart area:** left=70, right=w-40, top=40, bottom=h-35; dark axes `#2c3e50`, light gridlines `#ecf0f1`.
- **Y-axis:** 0–100% with labels at 0, 20, 40, 60, 80, 100 ("%" suffix); rotated axis label "Prediction Accuracy (%)" in gray `#7f8c8d`.
- **X-axis:** log scale labels 1, 10, 100, 1000, 10000 evenly spaced; axis label "MSA Depth (log scale)" in `#7f8c8d`.
- **Background regions:** left 35% of chart tinted `rgba(231,76,60,0.08)` (red), rightmost 40% tinted `rgba(39,174,96,0.08)` (green).
- **Threshold line:** dashed purple `#8e44ad` (dash 6/4, width 1.5) at 70%, labeled "Useful threshold" in 11px purple near the right end.
- **Curve:** sigmoid-like blue line (`#2980b9`, width 3) through fractional-x/accuracy points: [0, 38], [0.15, 42], [0.3, 48], [0.45, 58], [0.55, 68], [0.65, 78], [0.75, 85], [0.85, 89], [0.95, 91], [1.0, 92].
- **Scatter dots (radius 4):** points (fraction of x-range, accuracy%): [0.05, 35], [0.1, 42], [0.18, 40], [0.25, 45], [0.32, 50], [0.4, 55], [0.48, 62], [0.55, 70], [0.62, 75], [0.68, 80], [0.72, 82], [0.78, 87], [0.82, 86], [0.88, 90], [0.92, 91], [0.97, 93], [0.08, 39], [0.35, 52], [0.6, 72], [0.85, 88]; dots with x-fraction < 0.35 are red `#e74c3c`, others green `#27ae60`.
- **Region labels (10px):** "Orphan proteins" in red at top-left of chart; "Well-studied families" in green at top-right.

## PDB Training Bias (What Gets Solved Gets Learned)

- Trained on Protein Data Bank structures: biased toward easy-to-crystallize, stable, globular proteins
- Membrane proteins (30% of drug targets) are severely underrepresented (~3% of PDB)
- Intrinsically disordered regions have no crystal structures → absent from training data
- Large multi-protein complexes are underrepresented due to experimental difficulty
- Model is weakest precisely for the proteins that are most therapeutically important
- Crystallization conditions (pH, temperature, detergents) force proteins into non-physiological states

**Example:** Only ~1000 unique membrane protein structures in PDB vs ~150,000 soluble proteins. GPCRs (target of 35% of drugs) have fewer than 500 structures despite being the most important drug target class.

### Visualization (canvas `canvas3`, 720×300)

Grouped bar chart comparing PDB representation vs biological/therapeutic importance across four protein categories.

- **Title (bold 17px, `#1a5276`):** "PDB Representation vs Biological Reality".
- **Chart area:** left=120, right=w-30, top=45, bottom=h-30; y-axis 0–100% with labels every 20%; dark axes `#2c3e50`, light gridlines `#ecf0f1`.
- **Categories (x-axis, 9px labels, groups 140px wide, bars 40px wide, 5px apart):** "Soluble/Globular", "Membrane", "Disordered", "Multi-complex" (the "Soluble/Globular" label is split across two lines at the "/").
- **PDB bars (blue, fill `rgba(41,128,185,0.7)`, stroke `#2980b9`):** 85, 3, 1, 11 (%). Value labels bold 10px blue above each bar.
- **Bio bars (orange, fill `rgba(230,126,34,0.7)`, stroke `#e67e22`):** 40, 30, 30, 20 (%). Value labels bold 10px orange above each bar.
- **Legend (11px, top of chart):** blue swatch + "PDB representation"; orange swatch + "Biological/therapeutic importance".

## Fitness Landscape Epistasis (Combinatorial Interactions)

- Mutation A alone: neutral effect on protein fitness
- Mutation B alone: neutral effect on protein fitness
- Mutations A+B together: catastrophic loss of function (or unexpected gain)
- Single-mutation predictions cannot be composed to predict multi-mutation effects
- The fitness landscape is rugged with epistatic peaks and valleys invisible from single-mutant data
- Combinatorial explosion: for a 300-residue protein, pairs = 300×19×299×19 ≈ 32 million possible double mutants

**Example:** In TEM-1 beta-lactamase, mutations M182T and G238S are each mildly deleterious alone (-0.2 fitness) but together provide 1000x antibiotic resistance. No single-mutation model predicts this.

### Visualization (canvas `canvas4`, 720×300)

2D effect-space diagram: mutation A effect (x) vs mutation B effect (y), with a red danger zone at the origin and side annotations.

- **Title (bold 17px, `#1a5276`):** "Epistasis: Non-Additive Mutation Effects".
- **Grid area:** left=100, right=420, top=45, bottom=h-20; solid axes `#2c3e50` width 1.5; dashed center gridlines `#ecf0f1` (dash 3/3) through the midpoint.
- **Axis labels (11px `#2c3e50`):** "Mutation A effect" below x-axis; "Mutation B effect" rotated on y-axis. Tick labels (9px): "-1", "0", "+1" on each axis.
- **Red zone:** ellipse at grid center, radii 45×35, fill `rgba(231,76,60,0.4)`, stroke `#e74c3c` width 2; bold 11px red label inside: "A+B: lethal".
- **Green arrows (`#27ae60`, width 2, with triangular heads):** one pointing up from the x-axis at center labeled "A alone: neutral" (10px); one pointing right from the y-axis at center labeled on two lines "B alone:" / "neutral".
- **Right-side annotations (starting x=450):**
  - Bold 12px `#2c3e50`: "Additive model predicts:"
  - 11px green: "A = 0 (neutral)", "B = 0 (neutral)"; 11px blue `#2980b9`: "A+B = 0 (neutral)"
  - Bold 12px red: "Reality (epistasis):"
  - 11px red: "A+B = catastrophic!", "or 1000x gain"

## Experimental Resolution Limits (Training on Blurry Labels)

- X-ray crystallography: 2-3 Å resolution means individual atoms are blurred/uncertain
- Cryo-EM: even worse resolution for flexible regions (4-8 Å in peripheral areas)
- B-factors in crystal structures indicate per-atom positional uncertainty (often 20-80 Å²)
- Training labels have inherent measurement uncertainty that sets a ceiling on prediction accuracy
- Model cannot exceed the resolution of its training labels — it learns to reproduce blur
- Side-chain positions (critical for drug design) have much higher uncertainty than backbone

**Example:** At 3 Å resolution, individual water molecules and hydrogen bonds are invisible. A model trained on this data cannot predict hydrogen bonding networks needed for drug design, even if the model architecture could theoretically represent them.

### Visualization (canvas `canvas5`, 720×300)

Three horizontal bands showing atoms rendered with increasing blur at decreasing resolution.

- **Title (bold 17px, `#1a5276`):** "Resolution Limits in Training Data".
- **Bands** (label 12px `#2c3e50` at x=20; five fuzzy blue atoms per band starting x=220 spaced 55px, drawn as concentric `rgba(41,128,185,α)` circles with blur radius per band plus a solid `#2980b9` center dot; description 10px gray `#7f8c8d` at x=510; light separator line `#ecf0f1` above each band):
  - "1.5 Å (atomic)" at y=55, blur radius 3 — "Individual atoms visible"
  - "3.0 Å (typical)" at y=105, blur radius 10 — "Atoms blurred together"
  - "6.0 Å (low-res cryo-EM)" at y=155, blur radius 22 — "Only overall shape visible"
- **Annotation:** dashed red arrow (`#e74c3c`, dash 4/3) pointing down at the 3.0 Å band, with bold 11px red labels "Training data" / "resolution" (two lines, near x=505-510, y=63-75).
- **Bottom annotation (bold 12px purple `#8e44ad`, centered ~x=260, y=h-8):** "Model ceiling = label resolution".

## Distribution Shift: Natural → Designed Proteins

- Trained on NATURAL proteins that evolved over billions of years with specific evolutionary pressures
- Asked to predict DESIGNED proteins that evolution never explored (different sequence statistics)
- Natural proteins optimize for foldability, stability, evolvability — designed proteins optimize for function only
- Amino acid frequencies in designed sequences differ measurably from natural sequence statistics
- Designed sequences pack a more hydrophobic core and carry fewer historical evolutionary constraints
- Extrapolation outside training distribution: designed proteins occupy different regions of sequence space
- Co-evolutionary signals (key to AlphaFold) don't exist for de novo designed proteins with no homologs

**Example:** De novo designed mini-proteins from Baker lab have 15-30% lower AlphaFold confidence scores than natural proteins of similar size, even when the designed proteins fold perfectly in the lab. The model is uncertain because it hasn't seen these sequence patterns before.

### Visualization (canvas `canvas6`, 720×300)

Two-cluster sequence-space diagram: large natural-protein cluster (training distribution) vs smaller designed-protein cluster outside the model's learned space.

- **Title (bold 17px, `#1a5276`):** "Distribution Shift: Natural vs Designed Sequence Space".
- **Natural cluster:** ellipse center (230, 110), radii 140×60, fill `rgba(41,128,185,0.15)`, stroke `#2980b9` width 2; 25 scatter dots (radius 3, `rgba(41,128,185,0.5)`) at offsets from center: [-80,-20], [-50,10], [-30,-30], [0,15], [20,-10], [40,20], [-60,30], [-20,-15], [10,30], [50,-25], [-40,-5], [30,5], [-10,25], [60,15], [-70,-10], [80,-5], [20,-35], [-20,35], [0,-25], [45,30], [-55,-25], [70,10], [-30,15], [10,-5], [-80,10].
- **Designed cluster:** ellipse center (520, 105), radii 85×50, fill `rgba(230,126,34,0.15)`, stroke `#e67e22` width 2; 15 scatter dots (radius 3, `rgba(230,126,34,0.6)`) at offsets: [-30,-10], [-10,15], [10,-20], [30,5], [50,-10], [-20,25], [0,-5], [20,20], [40,-15], [-40,0], [15,10], [-15,-20], [35,15], [-5,30], [55,5].
- **Model boundary:** dashed dark-blue ellipse (`#1a5276`, dash 6/4, width 2) around the natural cluster with radii +15/+10, labeled above in 10px `#1a5276`: "Model's learned space".
- **Cluster labels (11px, two lines each, below clusters):** blue "Natural proteins" / "(training distribution)"; orange "Designed proteins" / "(target distribution)".
- **Extrapolation arrow:** dashed red line (`#e74c3c`, dash 3/3, width 1.5) from the designed cluster's left edge back to just past the natural cluster's right edge, ending in a red arrowhead; bold 12px red label at (380, 75): "Extrapolation!".

## Regeneration instructions

- **Layout:** per pitfall, an `<h2>` section heading followed by a single-row `.obj-table`: full-width table, left `<td>` (40%) with `.obj-title` (repeating the h2 text), a `<ul>` of bullets, and an `.example` callout div (`<strong>Example:</strong>` + text); right `<td>` (60%, centered) holds one canvas 720×300.
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.4em `#1a5276` with 2px solid `#2980b9` bottom border; subtitle `#666` 1.05em; table cell borders `1px solid #e0e0e0`, padding 20px 24px, even rows `#fafcfe`; `.obj-title` 1.05em weight 600 `#1a5276`; bullets 0.9em `#333`; `strong` `#1a5276`; `.example` background `#eaf2f8`, padding 10px 14px, radius 6px, 0.92em. A `.philosophy` style (background `#f0f4f8`, left border `4px solid #2980b9`) is defined but unused. No nav bar, no back/home links.
- **Canvas:** each canvas declares intrinsic `width="720" height="300"`; scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setupCanvas(id)` helper.
- **Palette:** primary blue `#1a5276`, mid blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, dark text `#2c3e50`, gray `#7f8c8d`, gridlines `#ecf0f1`.
