# Logistic Regression

**Page type:** detail page (tutorial card-sections: one `<h2>` per section, two-column `table.layout` — text 50% / viz 50%; section 2 uses the 3-column variant 38/31/31 with two canvases)
**HTML title tag:** Logistic Regression

**Subtitle:** A straight-line score squashed through an S-curve, so "will this order be returned?" comes out as a probability between 0 and 1

## A Score for Every Order, Then the Squash

**Tags:** `core idea` (blue), `running example` (green)

- **The question** — before shipping, how likely is this clothing order to come back?
- **The score** — start linear: score = −2 + 1×(returns in the shopper's last 5 orders)
- **The problem** — a line outputs −2 or +3, and "−2 chance of a return" is nonsense
- **The squash** — feed the score through the S-curve: probability = 1 / (1 + e^−score)
- **The output** — every score, however extreme, lands strictly between 0 and 1

*Example (italic):* A shopper with 2 past returns scores −2 + 2 = 0, and the S-curve turns 0 into exactly 50%.

**Logistic regression:** linear regression's score plus the S-curve (the sigmoid). The line does the thinking; the squash makes it a probability.

### Visualization (canvas `c1`, 720×300)

Sigmoid curve plot with six shopper points marked.

- **Title (bold 15px, `#1a5276`, top center):** "The S-Curve: Any Score In, a 0–1 Probability Out".
- **Axes:** L-shaped gray axes, padding top 48 / bottom 54 / left 64 / right 30. X: score z from −4 to +4 with 12px mute tick labels "−4"…"+4"; Y: 0%–100% with labels every 25%. Light gridline (`#e5e9ef`) at the 50% level. Axis captions (12px mute): "linear score = −2 + 1×(past returns)" bottom center; rotated "probability of a return" on the left.
- **Curve:** sigmoid `1/(1+e^−z)` sampled at 161 points across z ∈ [−4, 4], blue `#2a78d6`, width 3.
- **Shopper points:** orange `#d95926` radius-6 dots on the curve at scores `[-2, -1, 0, 1, 2, 3]` with probabilities `[0.12, 0.27, 0.50, 0.73, 0.88, 0.95]` (returns 0–5). Orange 12px labels: "0 returns" below the z=−2 dot, "2 returns" left of the z=0 dot, "4 returns" above the z=+2 dot.
- **Annotations:** bold blue 13px "probability = 1 / (1 + e⁻ˢᶜᵒʳᵉ)" upper left; bold green `#008300` 13px "score 0 → exactly 50%" beside the midpoint.

## Scoring Three Shoppers by Hand

**Tags:** `worked example` (green), `arithmetic` (blue)

- **Shopper A** — 0 past returns: score = −2 → probability = 1/(1+e²) ≈ 12%
- **Shopper B** — 2 past returns: score = 0 → probability = 1/(1+1) = 50%
- **Shopper C** — 4 past returns: score = +2 → probability ≈ 88%
- **Symmetry** — scores −2 and +2 give 12% and 88%: mirror images around 50%
- **Odds view** — each extra past return multiplies the odds of a return by e ≈ 2.7

*Example (italic):* Shopper B is a coin flip — the model literally says 50-50.

**Hand-checkable:** the score is plain arithmetic; the only "math" is one exponential — and a score of 0 always means exactly 50%.

### Visualization (canvas `c2a`, 420×300)

Bar chart of the raw linear scores (positive and negative bars around a zero line).

- **Title (bold 15px, `#1a5276`):** "Step 1: the Linear Score".
- **Data:** values `[-2, 0, 2]` for labels "A: 0 returns", "B: 2 returns", "C: 4 returns"; bar colors green `#008300`, blue `#2a78d6`, magenta `#d55181`; 66px bars at 0.75 alpha drawn up or down from the zero line; y scale −3 to +3 with signed tick labels; padding top 50 / bottom 56 / left 52 / right 16. B's zero-height bar is marked with a radius-5 blue dot on the zero line. Bold 13px signed value labels ("−2", "0", "+2") at bar ends.
- **Annotation (bold orange `#d95926` 12px, two lines, top center):** "a raw score — negative allowed," / "no upper limit: not yet a probability".
- **Caption (12px mute, bottom center):** "score = −2 + 1×(past returns)".

### Visualization (canvas `c2b`, 420×300)

Bar chart of the probabilities after the sigmoid.

- **Title (bold 15px, `#1a5276`):** "Step 2: After the S-Curve".
- **Data:** values `[12, 50, 88]` (%) for "A: 0 returns", "B: 2 returns", "C: 4 returns"; same colors (green, blue, magenta), 66px bars at 0.75 alpha; y scale 0–100% with labels every 25%; light gridline at 50%; bold 13px "%"-suffixed value labels above bars.
- **Annotation (bold orange 12px, two lines, top center):** "same three shoppers — now every" / "answer is a usable probability".
- **Caption (12px mute, bottom center):** "probability = 1 / (1 + e⁻ˢᶜᵒʳᵉ)".

## The 0.5 Line — or Wherever Costs Put It

**Tags:** `rule of thumb` (blue), `trade-off` (orange)

- **Default rule** — flag "likely return" when probability ≥ 0.5: shoppers B and C get flagged
- **Costs differ** — say a missed return costs $15 in shipping; every flag triggers a $2 size check
- **Do the math** — flagging pays whenever p × $15 > $2, i.e. above p ≈ 0.13
- **Lower bar** — at 0.13, everyone with at least 1 past return (27%) is flagged; A (12%) is not
- **Model unchanged** — same probabilities either way; only the cut line moved

*Example (italic):* At 0.5 only B and C trigger the size-check email; at 0.13 every shopper with a past return does.

**Key point:** the probability is the model's job; the threshold is a business decision. Move it with the costs (here illustrative), not by habit.

### Visualization (canvas `c3`, 720×300)

Sigmoid curve with two dashed threshold lines and shopper dots colored by the cost-based rule.

- **Title (bold 15px, `#1a5276`, top center):** "Same Model, Two Cut Lines: 0.5 by Default, 0.13 by Costs".
- **Axes:** same frame as c1 (padding 48/54/64/30); X ticks show past-return counts "0"–"5" positioned at scores −2…+3; Y 0%–100% every 25%. Bottom caption (12px mute): "past returns in the shopper's last 5 orders (costs illustrative)".
- **Curve:** sigmoid, blue `#2a78d6`, width 3.
- **Threshold lines (dashed 6/4, width 2):** violet `#4a3aa7` at p=0.5 labeled bold 12px "0.5 default: flags B and C only"; orange `#d95926` at p=0.13 labeled "0.13 cost-based: flags every shopper with 1+ past returns".
- **Shopper dots (radius 6) at probabilities `[0.12, 0.27, 0.50, 0.73, 0.88, 0.95]`:** magenta `#d55181` when p ≥ 0.13 (five dots), green `#008300` when below (the 12% shopper). Green 12px label below the first dot: "A: 12%, not flagged".
- **Cost math annotation (bold magenta 13px, upper left, two lines):** "missed return $15 vs wrong flag $2" / "→ flag when p × 15 > 2, i.e. p > 0.13".

## Why It's Still the Industry Workhorse

**Tags:** `where it's used` (blue), `common confusion` (red)

- **Everywhere** — churn, fraud, click prediction, credit risk, medical risk all start here
- **Fast** — trains in seconds on millions of rows; scoring is one multiply-add per feature
- **Readable** — each weight says how a feature moves the odds (×2.7 per past return here)
- **Calibrated** — when trained well, orders scored 70% really do return about 70% of the time
- **The name lies** — "regression" in the name, yet it is the default classification baseline

*Example (italic):* Many production fraud systems are a logistic regression plus good features — the fancy model came later, if ever.

**The confusion:** logistic regression outputs probabilities, not decisions — the yes/no only appears after you pick a threshold, and that pick belongs to the business.

### Visualization (canvas `c4`, 720×300)

Bar "odds ladder": odds of a return by past-return count, each step ×2.7.

- **Title (bold 15px, `#1a5276`, top center):** "Why the Weights Are Readable: Each Past Return Multiplies the Odds by ~2.7".
- **Data:** odds `[0.14, 0.37, 1.0, 2.7, 7.4]` (e^score for scores −2…+2) for "0 returns", "1 return", "2 returns", "3 returns", "4 returns"; 74px bars at 0.8 alpha — aqua `#199e70`, except the middle (odds 1.0) bar in blue `#2a78d6`; bold 13px two-decimal value labels above bars; y scale 0–8 with ticks every 2; padding top 52 / bottom 56 / left 64 / right 30.
- **Step arrows:** bold orange `#d95926` 13px "×2.7 →" between each adjacent pair of bars.
- **Annotations:** bold blue 12px above the middle bar: "odds 1.0 = the 50-50 shopper"; bottom caption (12px mute): "odds of a return = p / (1−p) — one weight, one clean multiplier".

## Regeneration instructions

- **Template:** tutorials topic-page layout (see `tutorials/CLAUDE.md`). h1 (no index number) + `.subtitle`, then four `.card-section` blocks each with an `<h2>` and a `table.layout`. Sections 1, 3, 4 use two columns (`td.text-col` 50% / `td.viz-col` 50%); section 2 uses the 3-column variant (`td.text-col3` 38%, two `td.viz-col3` 31% each holding a 420×300 canvas).
- **Left column per section:** `.tags` pill row, `<ul>` of one-line bullets opening with `<b>bold term</b>` (bold in `#1a5276`), one italic `.example` line, one `.key-point` callout. Superscripts (e², e⁻ˢᶜᵒʳᵉ) rendered with `<sup>` in text.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with 2px `#2980b9` bottom border; section h2 1.3rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem. Canvas `width:100%`, 1px `#e0e0e0` border, 4px radius. `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem. `.example` italic `#555` 0.9rem.
- **Tag pills:** `.tag` inline-block 0.72rem bold, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Chart palette object `P`:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette anchors: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- **Canvas:** intrinsic sizes 720×300 (c1, c3, c4) and 420×300 (c2a, c2b), scaled with `window.devicePixelRatio` via a shared `setup(id)` helper that reads the width/height attributes (backing store sized to rendered width × `window.devicePixelRatio` (display capped via `style.maxWidth`), `ctx.scale` back to logical coordinates). Shared helpers/data: `sigmoid(z) = 1/(1+e^−z)`; `RETURNS = [0,1,2,3,4,5]`, `SCORES = [-2,-1,0,1,2,3]`, `PROBS = [0.12, 0.27, 0.50, 0.73, 0.88, 0.95]`. Data hardcoded, no `Math.random()`; costs labeled "illustrative".
- No nav bar, no back/home links, no cross-page links. In regenerated HTML any card links use `.html` extensions.
