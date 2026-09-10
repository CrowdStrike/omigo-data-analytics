# Entropy

**Page type:** detail page (card-section layout: Overview two-column table with text left 45% / canvas right 55%, then a full-width Real-World Examples section with canvas + callouts, then a Quick Decision Table)
**HTML title tag:** Entropy — Statistical Tests Reference

**Back link (top of page):** "← Statistical Tests Reference" pointing to `../12-statistical-tests.md` (in regenerated HTML: `../12-statistical-tests.html`), styled `color:#2980b9`, no underline, 0.9em.

**Subtitle:** Quantifies the average uncertainty of a random variable

## Overview

**What it measures**

The average uncertainty (in bits) of a random variable. Quantifies how "spread out" or "uniform" a distribution is.

**Key assumptions**

- Probabilities sum to 1 (valid probability distribution)
- Log base determines units (base 2 = bits, base e = nats)
- Maximum entropy = log(k) for k classes (achieved at uniform)
- Zero probability contributes 0 (by convention, 0 log 0 = 0)

**What breaks when violated**

- As a feature quality metric: high entropy in the target given a feature = feature is useless; low entropy = feature is informative
- Entropy alone says nothing about WHICH class dominates — two distributions with same entropy can have very different structures
- For continuous data, differential entropy can be negative (not a true measure of uncertainty in the discrete sense)

**Failure box (`.failure`, monospace, red-left-border):**

Distribution A: [0.5, 0.5, 0.0, 0.0] — H = 1.0 bit
Distribution B: [0.25, 0.25, 0.25, 0.25] — H = 2.0 bits
Distribution C: [0.97, 0.01, 0.01, 0.01] — H = 0.24 bits
C is most useful for prediction (one class dominates). B is worst (maximum uncertainty).

**Alternative box (`.alt-note`, green-left-border):**

**Use instead:** Normalized entropy (H/log(k)) for comparing across different numbers of classes. Conditional entropy H(Y|X) for measuring feature utility. For continuous features, use KDE-estimated entropy or bin-based approximation.

### Visualization (canvas `c8`, 960×460)

Three grouped bar charts: peaked, skewed, and uniform 4-class distributions with entropy value badges and an increasing-entropy arrow.

- **Title (bold 13px, `#1a5276`, top center):** "Entropy: Shape Descriptor of Distributions".
- **Groups (left to right, 200px wide each, 30px gaps, centered as a block):**
  - "Peaked (H=0.24)" — probs [0.97, 0.01, 0.01, 0.01], green `#27ae60`, description "Very predictable", badge background `#eafaf1`.
  - "Skewed (H=1.0)" — probs [0.50, 0.50, 0.00, 0.00], orange `#e67e22`, description "Moderate uncertainty", badge background `#fef9e7`.
  - "Uniform (H=2.0)" — probs [0.25, 0.25, 0.25, 0.25], red `#e74c3c`, description "Maximum uncertainty", badge background `#fdedec`.
- **Bars:** 4 bars per group, category slots 38px wide, bar max height 110px, baseline y=195; fill in the group color with alpha 0.4 + p×0.6, 1px stroke in group color; probability value in 10px SF Mono above each nonzero bar; class labels "C1"–"C4" in gray `#999` below.
- **Badges:** 90×22 box under each group, stroked in group color, containing bold "H = X.XX bits" in the group color.
- **Arrow (bottom):** gray `#999` horizontal arrow with filled triangle head spanning the groups, labeled above (17px `#999`, centered): "Increasing entropy = less useful for prediction".

## Real-World Examples

### Visualization (canvas `c8r`, 960×300)

Three grouped bar charts of segment purchase behavior with entropy badges, showing equal entropy but opposite business value.

- **Title (bold 16px, `#1a5276`, top center):** "🎯 Ad Targeting: Same Entropy ≠ Same Value — Need Conditional Entropy".
- **Segments (left to right, 220px wide each, 20px gaps, centered as a block); categories per bar group: Buy, Browse, Cart, Bounce (12px `#666` under bars):**
  - "Luxury Shoppers" — probs [0.97, 0.01, 0.01, 0.01], H = 0.24 bits, green `#27ae60`, description "97% buy premium", verdict green "✓ Actionable".
  - "Weekend Visitors" — probs [0.02, 0.01, 0.00, 0.97], H = 0.24 bits, red `#e74c3c`, description "97% bounce", verdict red "✗ Useless target".
  - "General Browsers" — probs [0.25, 0.25, 0.25, 0.25], H = 2.0 bits, gray `#999`, description "Uniform (useless)", verdict gray "✗ No signal".
- **Bars:** category slots 40px wide, bar max height 80px, baseline y=155; fill in segment color with alpha 0.3 + p×0.7, 1px stroke.
- **Entropy badges:** 100×22 box under each group (background `#fef9e7` when H<1 else `#f8f9fa`), stroked in segment color, bold 14px "H = X.XX bits" in segment color.
- **Key insight:** dashed orange `#e67e22` line (dash 3/3, width 2) connecting the first two segments' badges, with bold orange "Same H!" above it and orange "opposite value" below.
- **Bottom line (14px `#555`, centered):** "Fix: Use H(conversion | segment), not H(segment) alone — measures actionability, not just predictability".

**Real-world callout 1 (`.real-world`, orange-left-border), domain line "🎯 Ad targeting: Audience segment quality":**

A marketing team evaluates audience segments by entropy of purchase behavior. Segment "luxury shoppers" has H=0.31 bits (97% buy premium) — highly predictable, great for targeting. Segment "general browsers" has H=1.95 bits (near-uniform across 4 product categories) — useless for personalization. But segment "weekend visitors" also has H=0.31 bits — 97% bounce without buying. Same entropy, completely different business value. Entropy tells you predictability, not utility. You need conditional entropy H(conversion|segment) to measure whether the segment is actionable.

**Real-world callout 2 (`.real-world`), domain line "🌐 NLP: Language model perplexity comparison":**

Model A (trained on 3 genres) reports entropy of 4.2 bits/token. Model B (trained on 12 genres) reports 5.8 bits/token. A naive comparison says A is "better." But A's vocabulary covers 30K tokens and B's covers 85K tokens. Normalized entropy: A = 4.2/log₂(30K) = 0.28, B = 5.8/log₂(85K) = 0.34 — still different, but the gap shrinks from 38% to 21%. And B handles 3× more genres. Raw entropy penalizes models with broader scope; normalized entropy reveals they're comparably uncertain relative to their vocabulary size.

## Quick Decision Table

| Data Situation | If Assumptions Met | If Violated | Universal Fallback |
|---|---|---|---|
| Distribution shape descriptor | Entropy (discrete, known k) | Different structures, same entropy value | Normalized entropy + class distribution |

## Regeneration instructions

- **Layout:** three `.card-section` blocks (Overview, Real-World Examples, Quick Decision Table), each with an `h2` underlined by `2px solid #2980b9`. Overview uses `table.layout` with `td.text-col` (45%) holding `.obj-title` headings + paragraphs/lists/callouts and `td.viz-col` (55%) holding canvas `c8`. Real-World Examples is full width: canvas `c8r` then two `.real-world` divs. Quick Decision Table holds the `.decision-table`.
- **Back link:** page opens with `<a href="../12-statistical-tests.html">← Statistical Tests Reference</a>` before the h1 (color `#2980b9`, no underline, 0.9em).
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with `border-bottom: 2px solid #2980b9`; subtitle `#666` 0.95rem; `strong` and `code` in `#1a5276`, code on `#e8f0f8`; `.obj-title` 1.05em weight 600 `#1a5276`.
- **Callout styles:** `.failure` — background `#fdedec`, left border `3px solid #e74c3c`, monospace ('SF Mono'/'Fira Code'), color `#922`, 0.85rem. `.alt-note` — background `#eafaf1`, left border `3px solid #27ae60`, color `#1a5276`, 0.85rem. `.real-world` — background `#fef9e7`, left border `4px solid #e67e22`, 0.88rem, `.domain` weight 600 `#7d6608`, `strong` `#e67e22`.
- **Decision table:** `.decision-table` — header background `#1a5276` white text; cell borders `1px solid #e0e0e0`; even rows `#fafcfe`; 3rd column `#e74c3c`, 4th column `#27ae60` weight 500.
- **Canvas:** intrinsic sizes `c8` 960×460 and `c8r` 960×300; CSS `width:100%` with `1px solid #e0e0e0` border and 4px radius; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setupCanvas(id)` helper.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, accent blue `#2980b9`, gray text `#666`/`#555`/`#999`.
