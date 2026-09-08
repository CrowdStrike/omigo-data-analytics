# E-Commerce Domain: Data Pitfalls

**Page type:** detail page (one h2 per pitfall, each followed by a two-column obj-table row: text left 50%, canvas right 50%)
**HTML title tag:** E-Commerce Domain - Data Pitfalls

**Subtitle:** Marketplace data mixes pricing mechanisms, position-biased clicks, missing return signals, and feedback loops that quietly break pricing and recommendation models.

## Auction vs Fixed-Price Coexistence

**A $450 "Average" Blending $550-600 Listings With $280-380 Auctions**

- Same product exists at wildly different prices depending on sale mechanism
- Auction prices are volatile and depend on timing, bidder count, and ending time
- Mixing auction and fixed-price data in pricing models creates bimodal distributions
- Average price becomes meaningless when two fundamentally different mechanisms coexist
- Auction items often sell below market value (sniping, low bidder turnout)

**Example:** A used iPhone 13 shows average price of $450 in the dataset. But this mixes Buy-It-Now listings at $550-600 with auction endings at $280-380. A pricing algorithm using the blended average underprices fixed listings and overbids on auctions.

### Visualization (canvas `canvas1`, 720×240)

Two-line price timeline: stable fixed-price vs volatile auction prices, with a misleading blended average.

- **Title (bold 17px, `#1a5276`):** "Price Timeline: Auction vs Fixed-Price".
- **Y axis:** $200–$700, gridlines `#ecf0f1` with 11px `#7f8c8d` labels at $200/$300/$400/$500/$600/$700; L-shaped dark `#2c3e50` axes; origin x=70.
- **Fixed-price line (green `#27ae60`, width 2.5), 15 points:** `[565, 570, 560, 575, 570, 565, 580, 570, 575, 565, 570, 575, 560, 570, 565]`.
- **Auction line (red `#e74c3c`, width 2, with 3px red scatter dots), 15 points:** `[320, 480, 280, 390, 510, 295, 350, 440, 270, 380, 520, 310, 290, 460, 340]`.
- **Blended average line:** dashed purple `#8e44ad` (dash 5/4, width 1.5) horizontal at $450, labeled in 11px purple: "Blended \"average\" = $450 (meaningless)".
- **Legend (12px, bottom):** green "Fixed-Price (~$570)"; red "Auction (volatile, $270-520)"; gray x-label "Time →".

## Long-Tail Price Distribution

**Mean Order Value $85, Median $23 — Rare Items Pull It Up 4x**

- E-commerce prices follow a power law: millions of items under $20, handful over $10,000
- Mean price is dominated by rare expensive items; median is far more representative
- Log-transform is essential before any statistical modeling of prices
- Outlier removal based on standard deviations fails because the distribution is not Gaussian
- Revenue concentration: top 1% of items may generate 40% of GMV

**Example:** A marketplace reports "average order value" of $85. The median is actually $23. A handful of luxury watches and electronics at $5,000-$50,000 pull the mean up 4x. Marketing campaigns optimized for the mean AOV misallocate budget entirely.

### Visualization (canvas `canvas2`, 720×240)

Power-law histogram of item prices with mean/median markers.

- **Title (bold 17px, `#1a5276`):** "Power-Law Price Distribution".
- **Bars:** 40 bins, fill `rgba(52,152,219,0.6)` with `#2980b9` stroke; heights follow power law `count = 50000 · (i+1)^-2.2`, normalized to chart height.
- **X axis labels (10px `#7f8c8d`, at fractional positions 0, 0.1, 0.2, 0.3, 0.5, 0.6, 0.8, 0.95):** $1, $10, $50, $100, $500, $1K, $5K, $10K+.
- **Annotations:** 12px `#2c3e50` "2.1M items" near the head of the distribution; red arrow to the tail with 12px red labels "47 items > $10K" / "(but 12% of revenue!)".
- **Mean vs median markers:** dashed vertical lines (dash 4/3, width 2) — red `#e74c3c` at 25% width labeled "Mean $85"; green `#27ae60` at 6% width labeled "Median $23".
- **Y label (rotated, 11px `#7f8c8d`):** "# of Items".

## Position Bias in Search Results

**Position 1 Takes 30-40% of Clicks, Position 10 Only 2-3%**

- Click-through rate drops exponentially with position regardless of item quality or relevance
- Position 1 gets 30-40% of clicks; position 10 gets 2-3% even for equally relevant items
- Training ML models on click data without position correction learns to rank what was already ranked high
- A/B tests of ranking algorithms are confounded by position effects
- Inverse propensity weighting or position-aware models are necessary corrections

**Example:** A search relevance model trained on click logs achieves 92% accuracy at predicting clicks. But it has actually learned position bias, not relevance. When items are randomly shuffled in an A/B test, the model performs no better than showing items in the original order.

### Visualization (canvas `canvas3`, 720×240)

Paired bar chart of CTR by search position for high- vs low-quality items.

- **Title (bold 17px, `#1a5276`):** "Click-Through Rate by Position".
- **X categories:** positions #1–#12 (10px `#2c3e50` labels).
- **High-quality CTR (blue `#3498db` bars):** `[35, 18, 12, 8.5, 6.2, 4.5, 3.4, 2.6, 2.0, 1.6, 1.2, 0.9]` %.
- **Low-quality CTR (red `#e74c3c` bars beside each blue bar):** `[32, 16, 10.5, 7.5, 5.5, 4.0, 3.0, 2.3, 1.8, 1.4, 1.0, 0.7]` %.
- **Y axis:** 0–35% labels every 5% (10px `#7f8c8d`) with `#ecf0f1` gridlines; scale max 38%.
- **Legend (right side, 12px with swatches):** blue "High quality"; red "Low quality".
- **Annotation (11px purple `#8e44ad`, bottom):** "Position dominates quality!" / "(nearly identical CTR regardless of item quality)".

## Returns Not Captured in Model

**Purchases +15%, Net Revenue −20% Once Returns Are Counted**

- Purchase event is logged immediately; return happens days/weeks later in a different system
- Models trained on "purchase" as positive signal learn to recommend high-return items
- Fashion/apparel: 25-40% return rates; "purchase and kept" is the true positive signal
- Recommendation models optimizing for purchase rate may actually increase costs
- Return data often lives in separate warehouse tables, never joined to training data

**Example:** A clothing recommender optimizes for purchase conversion. It learns to recommend "safe" sizes (multiple sizes of same item). Purchase rate is high (+15%) but return rate doubles. Net revenue per recommendation actually decreases by 20% when returns are accounted for.

### Visualization (canvas `canvas4`, 720×240)

Paired bar chart: purchases vs kept-after-returns by category.

- **Title (bold 17px, `#1a5276`):** "\"Purchase\" vs \"Purchase AND Kept\"".
- **Categories (10px `#2c3e50` labels):** Dresses, Shoes, Jeans, T-Shirts, Jackets, Accessories.
- **Purchased (blue `#3498db` bars):** `[1000, 850, 920, 1100, 600, 780]`.
- **Kept (green `#27ae60` bars beside each blue bar):** `[620, 560, 740, 950, 480, 700]`.
- **Return-rate annotations (10px red, above each purchase bar):** computed −% per category: −38%, −34%, −20%, −14%, −20%, −10%.
- **Y axis:** 0–1200 gridlines every 200 in `#ecf0f1` with `#7f8c8d` labels; dark `#2c3e50` L-axes.
- **Legend (right side, 12px with swatches):** blue "Purchased"; green "Kept"; plus 11px red note: "True signal" / "is KEPT," / "not bought".

## Popularity Feedback Loop

**50,000 Reviews at 4.3 Stars Outranks 200 Reviews at 4.8**

- Popular items get shown more → get more clicks → appear more popular → shown even more
- Rich-get-richer dynamics make it impossible to distinguish genuine quality from momentum
- New items can never break through without explicit exploration/exploitation balance
- Historical data reflects past algorithm decisions, not true user preferences
- Counterfactual evaluation methods needed to assess true item value

**Example:** A product ranked #1 for "wireless earbuds" has 50,000 reviews and 4.3 stars. A newer product with 200 reviews and 4.8 stars never reaches page 1 because the algorithm weighs historical sales. The feedback loop ensures the incumbent stays on top regardless of actual quality.

### Visualization (canvas `canvas5`, 720×240)

Circular feedback-loop diagram with four nodes ("Popularity Feedback Loop (Rich Get Richer)").

- **Title (bold 17px, `#1a5276`):** "Popularity Feedback Loop (Rich Get Richer)".
- **Circle:** gray `#bdc3c7` ring (width 3, radius 75) centered slightly below canvas middle.
- **Nodes (18px filled circles at compass points, with small directional triangle arrows in the node color, and 11px `#2c3e50` two-line labels outside the ring):**
  - Top (blue `#3498db`): "Item shown" / "more often"
  - Right (green `#27ae60`): "Gets more" / "clicks/sales"
  - Bottom (orange `#e67e22`): "Appears" / "more \"popular\""
  - Left (purple `#8e44ad`): "Algorithm" / "ranks higher"
- **Center label (bold 13px red `#e74c3c`, two lines):** "REINFORCING" / "CYCLE".
- **Side annotation (11px `#7f8c8d`, bottom right):** "New items cannot" / "break into the loop".

## Cold Start Problem

**10,000 New Listings a Day, Zero Behavioral Signal for Any**

- New items have zero interaction data - models cannot make predictions about them
- New users have no history - personalization is impossible at signup
- Content-based features (title, image, category) are poor proxies for behavioral signals
- Items that survive cold start are biased toward those with strong metadata, not quality
- Exploration budget allocation: how many impressions to "waste" on unproven items?

**Example:** A marketplace adds 10,000 new listings per day. The recommendation model, trained on click/purchase history, has zero signal for any of them. New sellers get no traffic for weeks, creating a survival bias where only sellers who buy ads survive the cold start period.

### Visualization (canvas `canvas6`, 720×240)

Side-by-side comparison panels: established item vs day-1 new item.

- **Title (bold 17px, `#1a5276`):** "Cold Start: New vs Established Items".
- **Left panel (260×155 outline, green `#27ae60` width 2, at x=60):** bold 13px green heading "Established Item"; 11px `#2c3e50` checkmark lines: "✓ Views: 45,230", "✓ Clicks: 8,412", "✓ Purchases: 1,247", "✓ Reviews: 342 (4.3★)", "✓ Return rate: 12%", "✓ Avg time on page: 45s", "✓ Cross-sell data: rich", "✓ Seasonal patterns: known". Below: solid green 230×8 bar and 10px caption "Signal strength: █████████████ HIGH".
- **Right panel (260×155 outline, red `#e74c3c` width 2, at x=390):** bold 13px red heading "New Item (Day 1)"; 11px gray `#95a5a6` cross lines: "✗ Views: 0", "✗ Clicks: 0", "✗ Purchases: 0", "✗ Reviews: 0", "✗ Return rate: unknown", "✗ Avg time on page: N/A", "✗ Cross-sell data: none", "✗ Seasonal patterns: unknown". Below: empty red-outlined 230×8 bar and 10px red caption "Signal strength: _____________ ZERO".
- **Between panels:** bold 20px `#2c3e50` "vs" at x=340, mid height.
- **Bottom annotation (11px purple `#8e44ad`):** "Model prediction confidence: high vs impossible — exploration budget needed".

## Regeneration instructions

- **Layout:** h1 + `.subtitle`, then per pitfall an `<h2>` heading (1.4em `#1a5276` with 2px `#2980b9` bottom border) followed by a single-row `.obj-table`: left `<td>` (40%) holds `.obj-title` (a refined restatement of the pitfall name, never a copy of it) + bullet list + `.example` callout, right `<td>` (60%, centered) holds the canvas. Even table rows get background `#fafcfe`.
- **Callouts:** `.example` — background `#f0f4f8`, left border `3px solid #2980b9`, padding 10px 14px, 0.9em, with bold "Example:" lead-in. (A `.philosophy` style — background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em — is defined but unused.)
- **Page style:** body system sans-serif, white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; subtitle `#666` 1.05em; ul 0.9em `#333`; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em weight 600 `#1a5276`; `strong` in `#1a5276`. No nav bar, no back/home links.
- **Canvas:** all canvases 720×240 intrinsic; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setupCanvas(id)` helper. Chart fonts use `-apple-system, sans-serif`. In canvas 6, position the "vs" label and bottom annotation with explicit y coordinates local to that canvas (the original code referenced an undefined `cy` variable for the "vs" label, which would prevent those last elements from drawing).
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`/`#3498db`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, dark slate `#2c3e50`, gray `#7f8c8d`/`#bdc3c7`/`#95a5a6`.
