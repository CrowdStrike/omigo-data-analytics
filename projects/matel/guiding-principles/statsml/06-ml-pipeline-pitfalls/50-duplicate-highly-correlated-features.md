# Pitfall: Duplicate / Highly Correlated Features (Semantic Incoherence)

**Page type:** detail page (three card-sections, each a two-column layout table: text left 45%, canvas right 55%)
**HTML title tag:** Duplicate / Highly Correlated Features (Semantic Incoherence)

**Subtitle:** Near-identical features destroy interpretability and split importance across redundant variables.

## The Problem

**Tags:** `the trap` (red), `redundancy` (blue)

- **Near-identical features** — correlation above 0.95 from renamed columns or re-derivations
- **Renamed duplicates** — "revenue_usd" and "revenue_dollars" carry the same data twice
- **Importance splitting** — a 0.4 signal fractured into 0.15, 0.12, 0.13 falls below threshold
- **Semantic duplicates** — "age" vs "birth_year", or "price" vs "log_price" for tree models
- **Multicollinearity** — coefficients turn unstable, and regularization zeros one arbitrarily
- **Broken interpretation** — importance rankings stop reflecting the true signal structure

*Example:* Three sources contribute "total_clicks," "click_count," and "num_clicks" — a 0.42-importance signal splits into 0.16, 0.14, and 0.12, selection drops all three, and the retrained model loses 18% AUC.

**Impact:** Fractured importance makes the model uninterpretable, and feature selection can drop a genuinely important predictor whose diluted score falls below threshold.

### Visualization (canvas `c1`, 720×300)

Before/after diagram: fragmented tree splits and importance bars on the left, deduplicated versions on the right, split by a gray divider at x=345.

- **Title (bold 14px `#1a5276`, top center):** "Duplicate Features: Importance Fragmentation".
- **Left header (bold 11px `#1a1a1a`, centered at 170,38):** "BEFORE DEDUP".
- **Tree nodes (left, red):** three 140×24 outlined boxes stroke `#e74c3c` width 2, 10px monospace labels: "income > 50k" at (170,58), "annual_salary > 48k" at (100,108), "yearly_earnings > 52k" at (240,108); connected by `#333` edges from the root to both children.
- **Annotation (bold 10px `#e74c3c`, centered at 170,145):** "All 3 = same signal!".
- **Fragmented importance bars:** header bold 10px `#1a1a1a` "Feature Importance:" at (30,165). Three bars starting at x=140, y=175 stepping 18px: labels (9px monospace, right-aligned) `income`, `annual_salary`, `yearly_earnings`, each value 6% (bar width = 6×8 px), fill `#e74c3c` alpha 0.4 with red stroke, bold 9px red value labels "6%".
- **Threshold line:** dashed orange `#e67e22` width 1.5, dash 4/3, vertical at x=204 spanning the bars, labeled 9px "drop threshold (8%)".
- **Right header (bold 11px `#1a1a1a`, centered at 530,38):** "AFTER DEDUP".
- **Clean tree:** root box 140×28 stroke `#27ae60` labeled bold 11px monospace "income > 50k" at (530,~70); two child boxes 110×24 stroke `#1a5276` labeled 10px monospace "age > 35" (435,126) and "tenure > 5yr" (615,126); `#333` edges.
- **Annotation (bold 10px `#27ae60`, centered at 530,148):** "Readable path, distinct splits".
- **Consolidated importance bar:** header "Feature Importance:" at (380,170); single bar at (490,178), width 18×8, fill `#27ae60` alpha 0.4 with green stroke width 1.5; right-aligned 10px monospace label "income", bold 10px green value "18%".
- **Arrow:** green `#27ae60` width 2 arrow from left panel to right at y=195.
- **Bottom annotation (bold 11px `#e74c3c`, centered):** "3 duplicates x 6% each = below 8% threshold. After dedup: 18% = top feature."

## Why It Happens

**Tags:** `root cause` (orange), `feature sprawl` (blue)

- **Accretion growth** — every team and source adds its own view, and nothing forces convergence
- **Collect-everything habit** — teams pull in all available features because more feels safer
- **Cross-system renaming** — the same signal appears under different names per source system
- **Derived variants** — engineering yields amount, log_amount, and amount_zscore from one base
- **Siloed development** — teams build similar features with no visibility into what exists

*Example:* Three teams contribute "revenue_30d", "monthly_revenue", and "rev_last_month" — 0.98 correlated — so the tree model really uses one signal three times.

**Root Cause:** The "more features = better" assumption plus siloed development lets the same information enter the model through multiple correlated paths.

### Visualization (canvas `c2`, 720×300)

Three-panel diagram: converging feature names (left), a 6×6 correlation heatmap (center), redundant tree splits (right).

- **Title (bold 14px `#1a5276`, top center):** "Why Duplicates Accumulate".
- **Left panel header (bold 10px `#1a1a1a`, centered at 130,38):** "MULTIPLE NAMES, ONE SIGNAL".
- **Feature boxes:** three 120×20 boxes at x=45 starting y≈45 stepping 32px, stroke `#e74c3c` width 1.5, fill `rgba(231,76,60,0.08)`, 10px monospace red labels: `revenue_30d`, `monthly_revenue`, `rev_last_month`; each prefixed by an orange bold 9px team label right-aligned: "Team A", "Team B", "Team C"; gray `#555` arrows from each box converging to a signal node.
- **Signal node:** filled `#1a5276` circle radius 14 at (210,87) with white bold 9px "$$"; caption 9px `#1a5276` "(same underlying signal)" and bold 10px `#e74c3c` "r = 0.98" below.
- **Center panel header (bold 10px `#1a1a1a`, centered at 380,38):** "CORRELATION MATRIX".
- **Heatmap:** 6×6 grid, cell size 22px, origin (305,48); row labels 8px monospace right-aligned: `rev_30d`, `mon_rev`, `rev_lm`, `age`, `tenure`, `clicks`. Correlation matrix values:
  - `[1.0, 0.98, 0.97, 0.12, 0.05, 0.30]`
  - `[0.98, 1.0, 0.96, 0.10, 0.08, 0.28]`
  - `[0.97, 0.96, 1.0, 0.14, 0.06, 0.32]`
  - `[0.12, 0.10, 0.14, 1.0, 0.45, 0.20]`
  - `[0.05, 0.08, 0.06, 0.45, 1.0, 0.15]`
  - `[0.30, 0.28, 0.32, 0.20, 0.15, 1.0]`
  - Cell colors: diagonal `rgba(26,82,118,0.7)`; top-left 3×3 off-diagonal cells red `rgba(231,76,60, value×0.8)`; all other cells blue `rgba(26,82,118, value×0.4)`.
- **Cluster highlight:** dashed red `#e74c3c` width 2 rectangle (dash 3/2) around the top-left 3×3 block, labeled bold 9px red "duplicate cluster" below it.
- **Right panel header (bold 10px `#1a1a1a`, centered at 600,38):** "REDUNDANT TREE SPLITS".
- **Tree:** three 136×20 boxes stroke `#e74c3c` width 1.5, 9px monospace red labels: "revenue_30d > 5k" (600,60), "monthly_revenue > 4.8k" (555,100), "rev_last_month > 5.2k" (645,100); `#333` edges; annotation bold 9px red "Same signal at every split!" at (600,130).
- **Summary (11px `#1a1a1a`, centered, y = h−30):** "Siloed teams + no feature registry = same information enters model N times under different names".
- **Source labels:** 9px orange `#e67e22` "Source A", "Source B", "Source C" centered at x = 180/360/540, near the bottom.

## The Correct Approach

**Tags:** `the fix` (green), `deduplication` (blue)

- **One per concept** — deduplicate before training so each idea enters the model once
- **Correlation clustering** — group features whose pairwise correlation exceeds ~0.95
- **One representative** — keep the most interpretable or stable feature, drop the rest
- **VIF check** — variance inflation catches linear combinations that pairwise checks miss
- **Path audit** — after training, check whether multiple splits reuse one underlying signal
- **Feature registry** — lineage tracking lets teams discover features before creating new ones

*Example:* 200 features cluster into 85 groups, and keeping one representative per group changes AUC by about 0.1% while making importance stable and interpretable.

**Fix:** Cluster correlated features, keep one per cluster, and audit after training — if removing the "duplicates" would change predictions, the dedup threshold was too aggressive.

### Visualization (canvas `c3`, 720×300)

Flow diagram: correlation clusters with selected representatives (left) feeding a clean feature set and model (right).

- **Title (bold 14px `#1a5276`, top center):** "Correct Approach: Cluster, Select, Verify".
- **Left header (bold 10px `#1a1a1a`, centered at 150,38):** "CORRELATION CLUSTERS".
- **Cluster 1 box:** dashed `#ccc` rectangle 260×55 at (20,47), 8px label "Cluster 1 (r>0.95)". Three 78×20 feature chips at x=35/120/205: `revenue_30d` selected (fill `rgba(39,174,96,0.15)`, stroke `#27ae60` width 2, bold 9px monospace green, green "✓"); `monthly_revenue` and `rev_last_month` rejected (fill `rgba(0,0,0,0.05)`, stroke `#ccc`, 9px `#555`, red X mark).
- **Cluster 2 box:** same structure at (20,110), label "Cluster 2 (r>0.95)": `click_count` selected; `total_clicks` and `num_clicks` rejected.
- **Uniques box:** dashed `#ccc` rectangle 260×40 at (20,173), label "Unique features (no cluster)": `age`, `tenure`, `region` all green-selected chips.
- **Arrow:** green width 2 arrow from clusters (x≈290) to the right panel at y=140.
- **Right header (bold 10px `#1a1a1a`, centered at 530,38):** "CLEAN FEATURE SET → MODEL".
- **Representatives box:** 155×130 at (350,50), stroke `#27ae60` width 2, fill `rgba(39,174,96,0.06)`; bold 9px green header "5 Representatives"; 10px monospace green list: `revenue_30d`, `click_count`, `age`, `tenure`, `region`.
- **Arrow to model:** blue `#1a5276` width 2 arrow at y=115.
- **Model box:** 130×60 at (555,85), stroke `#1a5276` width 2, fill `rgba(26,82,118,0.08)`; bold 12px "Model", 10px "Stable & Interpretable".
- **Result block:** bold 10px `#1a1a1a` "RESULT" centered at (530,200); 10px red "Before: 200 features, unstable importance"; bold 10px green "After: 85 features, same AUC (≤0.1% diff)" and "Interpretability: dramatically improved".
- **Bottom summary (bold 11px `#1a5276`, centered):** "Cluster (r>0.95) → Select 1 per cluster → Verify AUC unchanged → Audit importance".

## Regeneration instructions

- **Layout:** three `.card-section` blocks (The Problem / Why It Happens / The Correct Approach), each an h2 with blue bottom border followed by a `table.layout` with one row: left `td.text-col` (45%) holding `.tags` pills, a `ul` of labeled bullets, an italic `.example` paragraph, and a `.key-point` callout; right `td.viz-col` (55%) holding one 720×300 canvas.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem. h2 1.3rem `#1a5276` with 2px `#2980b9` bottom border. `ul` 0.92rem; `li b` in `#1a5276`.
- **Tag pills:** `.tag` inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px. Colors: blue `rgba(26,82,118,0.12)`/`#1a5276`; green `rgba(39,174,96,0.15)`/`#27ae60`; red `rgba(231,76,60,0.12)`/`#e74c3c`; orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Key-point callout:** background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem; `strong` in `#1a5276`. `.example` italic `#555` 0.9rem.
- **Canvas:** intrinsic 720×300, CSS `width: 100%`, border `1px solid #e0e0e0`, radius 4px; scaled via `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) through a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
