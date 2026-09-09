# Social Media / Content Platforms

**Page type:** detail page (h2 section per pitfall, each with a two-column obj-table row: text left 50%, canvas right 50%)
**HTML title tag:** Social Media / Content — Domain Pitfalls

**Subtitle:** Cross-platform challenges that affect ALL social media — engagement traps, bot contamination, virality unpredictability, and content moderation paradoxes.

## Engagement Optimization → Extremism

**Engagement Optimization → Extremism**

- Outrage generates clicks. Algorithm promotes outrage. Platform becomes toxic.
- Optimizing the metric (engagement) degrades the product (user wellbeing).
- Goodhart's Law: engagement becomes the target, ceases to measure value.

**Example:** video platform 2016-2019: "watch next" algorithm promoted increasingly extreme content because it maximized session time. Led to radicalization pipelines that were invisible in aggregate engagement metrics.

### Visualization (canvas `c1`, 720×300)

Escalating bar chart: five content categories with rising engagement and color shifting green-to-red.

- **Title (bold 17px `#1a5276`, left-aligned):** "Engagement Optimization Escalation".
- **Bars:** five bars 120px wide with 10px gaps starting at x=40; engagement values `[20, 35, 55, 80, 95]` scaled to 120px max height, tops relative to y=170 baseline.
- **Bar colors:** computed per index i as `rgba(100+i*40, 180-i*45, 60, 0.5)` — shifts from greenish to red across the five bars.
- **Category labels (17px `#333`, centered under each bar, split on two lines where noted):** "Mild interest" (two lines "Mild"/"interest"), "Moderate content" (two lines), "Provocative", "Outrage", "Extreme".
- **Caption (bottom center, bold 17px red `#e74c3c`):** "← Algorithm promotes this direction (more engagement) →".

## Bot Contamination (10-30% of Accounts)

**Bot Contamination (10-30% of Accounts)**

- Bots generate fake engagement, inflate metrics, distort trending topics.
- Your training data contains bot behavior labeled as "human."
- Model learns bot patterns → applies to humans (or can't distinguish them).

**Example:** microblogging platform acquisition audit (2022): estimated 5-20% bots depending on methodology. Any model trained on "user engagement" includes millions of non-human interactions as ground truth.

### Visualization (canvas `c2`, 720×300)

Pie chart of account composition with side labels.

- **Title (bold 17px `#1a5276`, left-aligned):** "Your \"User\" Data: 10-30% Bots".
- **Pie:** center (200,110), radius 70; blue slice `rgba(41,128,185,0.5)` from angle 0 to 1.6π (80% of circle); red slice `rgba(231,76,60,0.5)` from 1.6π to 2π (remaining 20%).
- **Legend labels (17px, left-aligned at x=300):** "Humans ~75%" in `#2980b9`; "Bots ~20%" in `#e74c3c`; "Unknown ~5%" in `#999`.
- **Caption (bottom center, bold 17px red `#e74c3c`):** "Model trained on this mix learns bot behavior as \"normal\"".

## Virality is Unpredictable (Power-Law Distribution)

**Virality is Unpredictable (Power-Law Distribution)**

- Same creator, same format, same topic: one post gets 10 views, next gets 10M.
- Distribution is power-law — no model predicts the tail.
- Features that "explain" viral posts are post-hoc narratives, not predictors.

**Example:** Research shows <5% of variance in virality is explainable by content features. 95%+ is network effects, timing luck, and algorithmic amplification decisions — all unobservable at creation time.

### Visualization (canvas `c3`, 720×300)

Power-law histogram of post reach with a lone viral outlier bin.

- **Title (bold 17px `#1a5276`, left-aligned):** "Post Reach: Power-Law (Unpredictable Tail)".
- **Data (20 bins):** `[500, 200, 80, 40, 20, 12, 8, 5, 3, 2, 2, 1, 1, 1, 0, 0, 0, 0, 0, 1]`, scale max 510.
- **Bars:** margins left 50 / right 30 / top 40 / bottom 30; bin width = plot width / 20 minus 1px gap; bins 0–14 filled blue `rgba(41,128,185,0.4)`, bins 15+ filled red `rgba(231,76,60,0.5)` (only the last bin, value 1, is visible in the red zone); zero-value bins not drawn.
- **Annotation (17px red `#e74c3c`, right-aligned near top right):** "viral (unpredictable)".
- **Caption (bottom center, 17px gray `#666`):** "95% of posts get <100 views. The viral 0.01% can't be predicted."

## Filter Bubbles / Feedback Loops

**Filter Bubbles / Feedback Loops**

- Show user X-type content → user engages → model shows more X → user never discovers Y.
- Recommendation creates the preference it claims to measure.
- Users appear to "prefer" what they were shown — not what they'd choose from a full menu.

**Example:** News feed shows political content → user clicks → more political content → user's feed becomes 80% political. Model: "user loves politics." Reality: user was shown nothing else.

### Visualization (canvas `c4`, 720×300)

Circular feedback-loop diagram: five nodes arranged on an ellipse.

- **Title (bold 17px `#1a5276`, left-aligned):** "Feedback Loop: Show → Engage → Show More → Trapped".
- **Nodes:** five circles radius 40, stroke `#2980b9` width 1.5, fill `#2980b9` at 15% alpha; placed on an ellipse centered at (w/2, 105) with x-radius 65×1.8=117 and y-radius 65, starting at the top (-π/2) and evenly spaced.
- **Node labels (17px `#333`, centered):** "Show X", "User clicks", "Model: likes X", "Show more X", "Only sees X".
- **Caption (bottom center, bold 17px red `#e74c3c`):** "Self-reinforcing: preference is CREATED, not measured".

## Content Moderation Paradox

**Content Moderation Paradox**

- Model must understand harmful content to block it → must process the harm.
- Cultural context: same gesture = friendly in one culture, offensive in another.
- Adversarial creation: bad actors study the model and engineer evasion.

**Example:** Coded language evolves weekly. "Boogaloo" meant civil war (2020), memes evolve to evade filters, context-dependent sarcasm is indistinguishable from genuine hate speech to NLP models.

### Visualization (canvas `c5`, 720×300)

Declining line chart: detection rate decaying over months.

- **Title (bold 17px `#1a5276`, left-aligned):** "Adversarial Evolution: Harmful Content Adapts".
- **Data:** detection rate by month — Jan 90, Mar 85, May 75, Jul 60, Sep 50, Nov 40 (percent, y scale 0–100).
- **Line:** red `#e74c3c`, width 2.5, connecting the six points; margins left 60 / right 40 / top 45 / bottom 30.
- **X labels (17px gray `#666`, centered at bottom):** "Jan", "Mar", "May", "Jul", "Sep", "Nov".
- **Annotation (17px red `#e74c3c`, right-aligned near top right):** "Detection rate decays as adversaries adapt".

## Metrics That Lie

**Metrics That Lie**

- **Likes ≠ value:** Users like posts they don't read (headline only).
- **Shares ≠ endorsement:** Sharing to criticize = same metric as sharing to recommend.
- **Time spent ≠ satisfaction:** Doomscrolling: 2 hours, feeling worse. High engagement, low value.
- **DAU ≠ product health:** Daily login rewards create compulsion, not enjoyment.

**Example:** photo-sharing platform internal research (2021): "32% of teen girls said that when they felt bad about their bodies, photo-sharing platform made them feel worse." But engagement metrics showed these users as highly active. Engagement ≠ wellbeing.

### Visualization (canvas `c6`, 720×300)

Paired horizontal bar chart: engagement vs actual value per metric.

- **Title (bold 17px `#1a5276`, left-aligned):** "Engagement Metrics vs Actual User Value".
- **Data (metric — engagement, value; bars scaled 3px per unit, starting at x=150; rows at y = 50 + i×35, bar height 14, value bar directly below engagement bar):**
  - Likes — 85, 30
  - Shares — 70, 45
  - Time Spent — 90, 25
  - Comments — 60, 55
- **Colors:** engagement bars blue `rgba(41,128,185,0.4)`; value bars green `rgba(39,174,96,0.4)`; metric names right-aligned in 17px `#333` at x=140.
- **Legend (17px, left-aligned at x=480):** "■ Engagement (what we measure)" in `#2980b9`; "■ Value (what matters)" in `#27ae60`.
- **Caption (bottom center, bold 17px red `#e74c3c`):** "Gap between measured and valuable = the lie".

## Regeneration instructions

- **Layout:** h1 + `.subtitle`, then one `h2` per pitfall (1.4em `#1a5276`, bottom border `2px solid #2980b9`, padding-bottom 8px), each followed by a `.obj-table` with a single `<tr>`: left `<td>` (40%) holds `.obj-title` (repeating the section title, 1.05em, weight 600, `#1a5276`), a `<ul>` of bullets, and an **Example** paragraph; right `<td>` (60%, centered) holds the canvas. This page has no philosophy callout (the `.philosophy` CSS class is defined but unused).
- **Table style:** full width, border-collapse; cell borders `1px solid #e0e0e0`, padding 20px 24px, vertical-align middle; even rows background `#fafcfe`.
- **Page CSS:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; `.subtitle` `#666` 1.05em; paragraphs `#333` 0.95em; `ul` 0.9em `#333`; `strong` `#1a5276`. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9` / `rgba(41,128,185,0.4-0.5)`, green `#27ae60` / `rgba(39,174,96,0.4)`, red `#e74c3c` / `rgba(231,76,60,0.5)`, gray text `#666`/`#999`/`#333`.
- **Canvas:** declare intrinsic `width`/`height` attributes as given per chart (all 720×300); scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart titles are left-aligned bold 17px; captions and labels use 17px throughout. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
