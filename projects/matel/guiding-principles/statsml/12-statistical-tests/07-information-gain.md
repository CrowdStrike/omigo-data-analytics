# Information Gain — Statistical Tests Reference

**Page type:** detail page (two-column obj-table layout: text left 45%, two stacked canvases right 55%, plus a Quick Decision Table below)
**HTML title tag:** Information Gain — Statistical Tests Reference

**Subtitle:** Measures reduction in entropy after splitting on a feature

## Information Gain

**What it measures**

Reduction in entropy after splitting on a feature. How much "information" about the target a feature provides.

**Key assumptions**

- All features are compared on the same scale (they are not — this is the problem)
- More splits = more potential information (but also more overfitting)
- Does not account for the number of unique values a feature has
- Training data is representative of the true distribution

**What breaks when violated**

- High-cardinality features (IDs, timestamps) get maximum or near-maximum information gain because each leaf is pure by construction
- A feature with n unique values and n samples gets IG = H(target) — perfect score, zero predictive value
- Gain ratio (Quinlan's C4.5 fix) divides by split information, penalizing many-valued features

**Failure box (`.failure`, monospace, red-left-border):**

Feature: customer_id (unique per row), 10000 values.
Information Gain = 0.95 (near-maximum, H(target)=0.97)
Feature: age_group (5 categories, genuinely predictive).
Information Gain = 0.28
Naive IG picks customer_id. Gain Ratio: customer_id=0.07, age_group=0.17. Fixed.

**Alternative box (`.alt-note`, green-left-border):**

**Use instead:** Gain ratio (IG / split information) to correct cardinality bias. Permutation importance for model-agnostic feature evaluation. Mutual information with normalization for fair comparison across features.

**Real-world callout 1 (`.real-world`, orange-left-border), domain line "🛒 Recommendation engine: Feature selection for purchase prediction":**

An e-commerce team builds a decision tree to predict "will buy." Features include session_id (unique), product_page_url (8000 unique values), device_type (3 values), and time_of_day (4 buckets). Information gain ranks session_id first (IG=0.99), then product_page_url (IG=0.81). Both are useless for prediction on new users — they memorize the training set. The tree achieves 99% train accuracy and 51% test accuracy (random chance). Switching to gain ratio: device_type (GR=0.14) and time_of_day (GR=0.11) surface as top features. Test accuracy: 68%.

**Real-world callout 2 (`.real-world`), domain line "📞 Churn prediction: Timestamp leakage":**

A telecom builds a churn model. Feature "last_call_timestamp" (continuous, effectively unique per customer) gets the highest information gain because it perfectly partitions customers into pure leaves. It leaks the label: customers who churned have their last call in the past because they stopped calling. IG doesn't flag this — it just sees maximum impurity reduction. Gain ratio partially helps (GR drops from 0.98 to 0.09), but the real fix is domain knowledge: recognizing temporal leakage before computing any metric.

### Visualization (canvas `c7`, 960×460)

Side-by-side horizontal bar chart: Information Gain vs Gain Ratio for four features, showing cardinality bias and its fix.

- **Title (bold 13px, `#1a5276`, top center):** "Information Gain: Cardinality Bias".
- **Features (top to bottom):**
  - customer_id (10000 vals) — IG 0.95, GR 0.07, red `#e74c3c`, bad
  - zip_code (412 vals) — IG 0.72, GR 0.11, orange `#e67e22`, bad
  - age_group (5 vals) — IG 0.28, GR 0.17, green `#27ae60`, good
  - gender (2 vals) — IG 0.12, GR 0.12, blue `#2980b9`, good
- **Left group (header "Information Gain" in `#1a5276`):** bars start at x=180, max width 200px scaled to IG=1.0, height 30px, 12px gap. Feature name right-aligned in 11px SF Mono `#333` with "(N vals)" in gray `#999` below it. Bar fill `rgba(231,76,60,0.5)` for bad features, `rgba(39,174,96,0.5)` for good; stroke in the feature color; IG value in bold 11px feature color after the bar.
- **Right group (header "Gain Ratio (fixed)", starting at x=480):** bar width = GR/0.2 × 160px. Bad features drawn muted: fill `rgba(200,200,200,0.4)`, stroke `#bbb`, value in gray `#999`; good features green fill/stroke with green values.
- **Annotations (17px, bottom):** red under the left group: "IG ranks ID features first (useless!)"; green under the right group: "Gain Ratio penalizes high cardinality".
- **Arrow:** thick green `#27ae60` horizontal arrow (width 3, filled triangle head) from the left group to the right group at mid-height, labeled "fix" in green above it.

### Visualization (canvas `c7r`, 960×300, 20px top margin)

Two-panel figure: left, feature-ranking bars; right, train/test accuracy comparison.

- **Title (bold 16px, `#1a5276`, top center):** "🛒 Recommendation Engine: IG Picks Useless High-Cardinality Features".
- **Left panel (header "Feature Ranking" in 14px `#1a5276`):** horizontal bars starting at x=80, max width 300px, height 28px, 8px gap. Features: session_id IG=0.99 (card: ~unique), page_url IG=0.81 (card: 8000), device_type IG=0.18 (card: 3), time_of_day IG=0.12 (card: 4). Bars with IG>0.5 are "leaky": fill `rgba(231,76,60,0.4)`, stroke `#e74c3c`; others fill `rgba(39,174,96,0.4)`, stroke `#27ae60`. Feature name right-aligned in 12px SF Mono; after each bar: "IG=X.XX (card: N)" in 13px red or green.
- **Right panel (header "Model Accuracy", starting at x = w/2+60):**
  - "Using IG top features:" (14px `#333`), then two red bars (fill `rgba(231,76,60,0.3)`, stroke `#e74c3c`, 240px scale, 20px tall) labeled "Train: 99%" and "Test: 51% (random!)".
  - "Using Gain Ratio top features:" then two green bars (fill `rgba(39,174,96,0.3)`, stroke `#27ae60`) labeled "Train: 72%" and "Test: 68% (generalizes!)".
- **Bottom takeaway (14px `#555`, centered):** "IG memorizes unique identifiers → zero generalization. Gain Ratio selects actionable features."

## Quick Decision Table

| Data Situation | If Assumptions Met | If Violated | Universal Fallback |
|---|---|---|---|
| Feature selection for trees | Information gain (low cardinality) | High cardinality: ID features win | Gain ratio / Permutation importance |

## Regeneration instructions

- **Layout:** h1 + subtitle, then `h2 id="infogain"` "Information Gain" above a standard detail-page `.obj-table`: full-width table, one `<tr>`; left `<td>` (45%) holds `.obj-title` headings, paragraph, bullet lists, `.failure`, `.alt-note`, and both `.real-world` callouts; right `<td>` (55%, centered) holds canvas `c7` stacked above canvas `c7r` (`margin-top:20px`). Then `h2 id="decision"` "Quick Decision Table" and the `.decision-table`. No nav bar, no back/home links.
- **Page CSS:** body -apple-system/'Segoe UI' sans-serif, white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6, font-size 0.95em; h1 1.8em `#1a5276`; h2 1.4em `#1a5276` with `border-bottom: 2px solid #2980b9`; subtitle `#666` 1.05em; `.obj-table td` border `1px solid #e0e0e0`, padding 20px 24px, vertical-align top; even rows `#fafcfe`; `.obj-title` 1.05em weight 600 `#1a5276`; `code` on `#e8f0f8` in `#1a5276`; `strong` `#1a5276`.
- **Callout styles:** `.failure` — background `#fdedec`, left border `3px solid #e74c3c`, monospace, color `#922`, 0.85em. `.alt-note` — background `#eafaf1`, left border `3px solid #27ae60`, color `#1a5276`, 0.85em. `.real-world` — background `#fef9e7`, left border `4px solid #e67e22`, 0.88em, `.domain` weight 600 `#7d6608`, `strong` `#e67e22`.
- **Decision table:** `.decision-table` — header background `#1a5276` white text; cell borders `1px solid #e0e0e0`; even rows `#fafcfe`; 3rd column `#e74c3c`, 4th column `#27ae60` weight 500.
- **Canvas:** intrinsic sizes `c7` 960×460 and `c7r` 960×300; `display:block; margin:0 auto`; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setupCanvas(id)` helper.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, accent blue `#2980b9`, gray text `#666`/`#555`/`#999`.
