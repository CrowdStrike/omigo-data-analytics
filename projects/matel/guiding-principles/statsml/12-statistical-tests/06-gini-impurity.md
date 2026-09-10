# Gini impurity — Statistical Tests Reference

**Page type:** detail page (two-column obj-table layout: text left 45%, two stacked canvases right 55%, plus a Quick Decision Reference table below)
**HTML title tag:** Gini impurity — Statistical Tests Reference

**Subtitle:** Probability that a randomly chosen sample would be misclassified

## Main section (obj-table, single row)

**What it measures**

Probability that a randomly chosen sample would be misclassified if randomly labeled according to the class distribution in that node.

**Key assumptions**

- Classes are equally important (no cost weighting built in)
- Split quality is measured by weighted impurity reduction
- Works on discrete splits of continuous features
- Range: 0 (pure) to 0.5 (binary, perfectly mixed)

**What breaks when violated**

- Gini favors larger, more balanced partitions (fewer isolated pure leaves)
- For multi-class: can miss splits that isolate a single rare class because overall impurity reduction is small
- Nearly identical to entropy for binary; diverges for multi-class (Gini is more conservative)

**Failure box (`.failure`, monospace, red-left-border):**

3-class problem [45,45,10] — class 3 is rare (10%)
Split A: [45,45,0] vs [0,0,10] — pure leaf for class 3, Gini reduction = 0.135
Split B: [45,0,5] vs [0,45,5] — class 3 scattered, Gini reduction = 0.405
Gini picks B (bigger reduction), so the rare class never gets its own leaf
even though Split A isolates it perfectly.

**Alternative box (`.alt-note`, green-left-border):**

**Use instead:** Weighted Gini (or entropy) with class costs when a rare class matters — plain entropy also favors splitting the big classes. Chi-squared split criterion (CHAID) for statistical validity of splits.

**Real-world callout 1 (`.real-world`, orange-left-border), domain line "💳 Fraud detection: Transaction classification tree":**

A decision tree for fraud detection sees 5000 legitimate transactions and 5 frauds (0.1%). Split A (amount > $5000) concentrates fraud: 200 legit / 3 fraud (1.5%) vs 4800 legit / 2 fraud (0.04%). It is the best split available, yet its Gini reduction is only 0.000016 — the parent is already 99.9% pure, so no split can reduce impurity by more than 0.002. Split B (merchant category, roughly equal halves) is even smaller: 0.0000002. Gains this small vanish under min-impurity-decrease thresholds or pruning, so the tree never isolates fraud. Weighted Gini (fraud weight = 100×) turns Split A's reduction into 0.052 — over 3000× larger — and clearly prefers it over Split B (0.0014).

**Real-world callout 2 (`.real-world`), domain line "🔬 Diagnostics: Rare disease subtyping":**

Building a tree to classify patients into 5 disease subtypes. Subtype E (autoimmune variant) is 2% of cases but requires completely different treatment. Gini consistently splits on features that separate the three common subtypes (A/B/C, each 25-30%) because those splits produce larger impurity reductions. Subtype E patients get scattered across leaves, never isolated. Switching to entropy with per-class cost weighting finally produces a branch that captures 85% of Subtype E in one leaf — actionable for clinicians.

### Visualization (canvas `c6`, 960×460)

Split-comparison diagram: two candidate splits shown as child-node boxes, result panels, and a proportional mini bar.

- **Title (bold 13px, `#1a5276`, top center):** "Gini Overlooks the Split That Isolates the Rare Class ([45, 45, 10])".
- **Split A (centered at w/4, header "Split A" in 17px `#1a5276`):** two 120×50 boxes side by side at y=55.
  - Left child: "[45, 45, 0]" in 11px SF Mono `#1a5276`, box fill `rgba(41,128,185,0.3)`, stroke `#2980b9`; below it "Gini=0.50" in gray `#666`.
  - Right child: "[0, 0, 10]" in green `#27ae60`, box fill `rgba(39,174,96,0.3)`, stroke `#27ae60`; below it "Gini=0.00".
- **Split B (centered at 3w/4, header "Split B"):** two 120×50 boxes.
  - Left child: "[45, 0, 5]" in orange `#e67e22`, box fill `rgba(230,126,34,0.3)`, stroke `#e67e22`; below it "Gini=0.18".
  - Right child: "[0, 45, 5]" in purple `#8e44ad`, box fill `rgba(142,68,173,0.3)`, stroke `#8e44ad`; below it "Gini=0.18".
- **Result panels (two `#f0f4f8` boxes at y=130, each half-width):**
  - Left (stroke `#e67e22`): bold orange "Split A: pure leaf for class 3" over gray "Reduction: 0.135 — passed over".
  - Right (stroke `#2980b9`): bold blue `#1a5276` "Gini picks Split B" over gray "Reduction: 0.405 — class 3 scattered".
- **Bottom note (17px `#555`, centered):** "Splitting the big classes wins on impurity. Isolating a 10% class needs class costs."
- **Mini bar (bottom, 300px total, 16px tall):** two segments proportional to the reductions — orange `rgba(230,126,34,0.6)` at 25% width labeled "0.135" in white, blue `rgba(41,128,185,0.6)` at 75% width labeled "Gini reduction: 0.405" in white.

### Visualization (canvas `c6r`, 960×300, 20px top margin)

Decision-tree diagram: fraud-detection root with two candidate splits and a verdict row.

- **Title (bold 16px, `#1a5276`, top center):** "💳 Fraud Detection: Gini Ignores Rare Class (0.1% Fraud)".
- **Root node (centered, 160×40 box, fill `#f0f4f8`, stroke `#2980b9`):** "All Transactions" (14px `#1a5276`) / "5000 legit / 5 fraud (0.1%)" (13px `#666`).
- **Split A (left side, header bold 14px orange `#e67e22`):** "Split A: amount > $5000".
  - Left child (110×50, fill `#eafaf1`, stroke `#27ae60`): "Low amount" / "4800 legit / 2 fraud" / green "0.04% fraud".
  - Right child (110×50, fill `#fdedec`, stroke `#e74c3c`): "High amount" / "200 legit / 3 fraud" / bold red "1.5% fraud ↑↑".
  - Caption below (13px red `#e74c3c`): "Gini Δ = 0.000016 — best split, still tiny".
- **Split B (right side, header bold 14px blue `#2980b9`):** "Split B: merchant category".
  - Left child (110×50, fill `rgba(41,128,185,0.1)`, stroke `#2980b9`): "Retail/Food" / "2800 legit / 2 fraud" / blue "0.07% fraud".
  - Right child (same style): "Online/Travel" / "2200 legit / 3 fraud" / blue "0.14% fraud".
  - Caption below (13px blue `#2980b9`): "Gini Δ = 0.0000002 (≈ zero)".
- **Verdict row (two boxes at bottom, 38px tall):**
  - Left (fill `#fdedec`, stroke `#e74c3c`, red text): bold "Plain Gini: best gain = 0.000016" / "Lost to noise / min-gain pruning".
  - Right (fill `#eafaf1`, stroke `#27ae60`, green text): bold "Weighted Gini (100×): Split A gain = 0.052" / "Concentrates fraud: 0.04% vs 1.5%".

## Quick Decision Reference

| Data Situation | If Assumptions Met | If Violated | Universal Fallback |
|---|---|---|---|
| Tree split criterion | Gini impurity (balanced classes) | Rare class ignored in reduction calc | Weighted Gini / Entropy / CHAID |

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table, one `<tr>`; left `<td>` (45%) holds `.obj-title` headings, paragraphs, bullet lists, `.failure`, `.alt-note`, and both `.real-world` callouts; right `<td>` (55%, centered) holds canvas `c6` stacked above canvas `c6r` (`margin-top:20px`). Below the table: `h2` "Quick Decision Reference" and the `.decision-table`. No nav bar, no back/home links.
- **Page CSS:** body -apple-system/'Segoe UI' sans-serif, white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6, font-size 0.95em; h1 1.8em `#1a5276`; subtitle `#666` 1.05em; `.obj-table td` border `1px solid #e0e0e0`, padding 20px 24px, vertical-align top; even rows `#fafcfe`; `.obj-title` 1.05em weight 600 `#1a5276`.
- **Callout styles:** `.failure` — background `#fdedec`, left border `3px solid #e74c3c`, monospace, color `#922`, 0.85em. `.alt-note` — background `#eafaf1`, left border `3px solid #27ae60`, color `#1a5276`, 0.85em. `.real-world` — background `#fef9e7`, left border `4px solid #e67e22`, 0.88em, `.domain` weight 600 `#7d6608`, `strong` `#e67e22`.
- **Decision table:** `.decision-table` — header background `#1a5276` white text; cell borders `1px solid #e0e0e0`; even rows `#fafcfe`; 3rd column `#e74c3c`, 4th column `#27ae60` weight 500.
- **Canvas:** intrinsic sizes `c6` 960×460 and `c6r` 960×300; `display:block; margin:0 auto`; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setupCanvas(id)` helper.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, accent blue `#2980b9`, gray text `#666`/`#555`/`#999`.
