# Web Ranking — Position Bias & CTR Feedback Loops

**Page type:** detail page (h2 section headings, each followed by a two-column obj-table row: text left 40%, canvas right 60%)
**HTML title tag:** Web Ranking — Position Bias & CTR Feedback Loops

**Subtitle:** How top-k positions on search results and product listings create self-reinforcing rank, starve new items, and make CTR a useless relevance signal.

## 1. Position Bias — Visibility Masquerades as Relevance

**Top-k ranks have static engagement regardless of content quality**

- **The problem:** Position 1 gets ~30% CTR. Position 5 gets ~4%. This is visibility, not relevance. Raw CTR conflates "user saw it" with "user wanted it."
- **Who hits this:** Search ranking teams, e-commerce product listing, ad placement optimization.
- **Why it persists:** Position 1 item always "validates" — it gets clicks no matter what. Teams see high CTR and conclude the ranking is working.
- **The invisible damage:** Items that would perform better if shown at position 1 are never tested there. You're optimizing within a biased observation window.

**Fix:** Position-debiased CTR (IPW weighting by position propensity). Randomized slot experiments. Normalize CTR by expected position baseline.

### Visualization (canvas `ca1`, 720×300)

Bar chart: CTR decay by ranking position.

- **Title (bold 14px `#1a5276`, top center):** "CTR by Position: Visibility, Not Relevance".
- **Data:** positions #1–#8 with CTR values `[32, 18, 11, 7, 4, 3, 2, 1.5]` (percent).
- **Layout:** margins left 80, right 40, top 45, bottom 50; bar width = plot width / 8 minus 10px gap; bar height scaled to max 35%.
- **Colors:** first two bars `rgba(39,174,96,0.6)` (green), remaining bars `rgba(26,82,118,0.35)` (blue).
- **Labels:** CTR value ("32%" etc.) bold 12px `#333` above each bar; "#1"…"#8" below each bar; x-axis label "Position" in gray `#666` centered below.
- **Caption (bold red `#e74c3c`, bottom center):** "Position 1 gets 8x the clicks of Position 5 — same content would get same ratio".

## 2. CTR → Rank → CTR Feedback Loop

**Rich-get-richer: once an item is shown, it wins forever**

- **The loop:** Item gets high position → gets clicks → CTR goes up → ranker boosts it → gets higher position → more clicks. Exponential divergence.
- **What breaks:** After a few retraining cycles, rank ordering becomes frozen. The top items are there because they were there, not because they're best.
- **How fast:** Unlike credit score loops (months), ranking loops compound hourly. A new item has hours to prove itself or gets buried permanently.

**Fix:** Exploration budget (epsilon-greedy, Thompson sampling). Periodic position randomization. Freshness decay that forces re-evaluation.

### Visualization (canvas `ca2`, 720×300)

Two-line divergence chart over 8 retraining cycles.

- **Title (bold 14px `#1a5276`, top center):** "Rank Divergence Over Retraining Cycles".
- **Layout:** margins left 70, right 40, top 45, bottom 40; x = 8 evenly spaced points; y scaled 0–100.
- **Winner line (green `#27ae60`, width 3):** `[30, 38, 50, 65, 78, 88, 94, 97]` (exponential up).
- **Loser line (red `#e74c3c`, width 3):** `[28, 22, 16, 11, 7, 4, 3, 2]` (decay).
- **Labels:** green bold 12px "Item A (got position 1 initially)" at ~50% width near top of plot; red "Item B (started at position 5)" at ~50% width near bottom; gray `#666` x-axis label "Retraining cycle" bottom center.
- **Axis:** thin light-gray `#ccc` baseline along the bottom of the plot.

## 3. Cold Start Starvation

**New items can't compete because they have no signal — because they're never shown**

- **The trap:** New item → no CTR data → low confidence → ranked low → few impressions → still no CTR data. The system never learns if the item is good.
- **Asymmetry:** Established items have thousands of impressions. Even mediocre items with stable CTR beat new items with zero signal. The system is risk-averse by default.
- **Business impact:** New sellers on marketplaces, new content creators, new products — all systematically disadvantaged regardless of quality.

**Fix:** Exploration-exploitation tradeoff. Upper confidence bound (UCB) scoring. Guaranteed impression quota for new items. Bayesian prior that assumes reasonable CTR until proven otherwise.

### Visualization (canvas `ca3`, 720×300)

Horizontal bar chart: impressions per item, established vs new.

- **Title (bold 14px `#1a5276`, top center):** "Impression Gap: Established vs New Items".
- **Data (label / impressions / CTR / bar color):**
  - Established A / 12,000 / 4.2% / `#27ae60`
  - Established B / 8,500 / 3.8% / `#27ae60`
  - Established C / 6,000 / 3.1% / `rgba(26,82,118,0.6)`
  - New Item X / 45 / null / `#e74c3c`
  - New Item Y / 12 / null / `#e74c3c`
- **Layout:** left margin 130 for right-aligned item labels; bars 36px tall with 8px gap, width scaled to max 12,000 impressions; bars drawn at 0.6 alpha.
- **Right-of-bar text (12px `#333`):** "<impressions> imp · CTR <x>%" for established items, "<impressions> imp · CTR: ???" for new items (e.g. "12,000 imp · CTR 4.2%", "45 imp · CTR: ???").
- **Caption (bold red `#e74c3c`, bottom center):** "New items get <1% of impressions — system literally cannot learn if they are good".

## Regeneration instructions

- **Layout:** each section is an `<h2>` heading ("1. …", "2. …", "3. …", 1.3em `#1a5276` with 2px `#2980b9` bottom border) followed by a single-row `.obj-table`: full-width, border-collapse, one `<tr>`; left `<td>` (40%) holds `.obj-title` div + `<ul>` bullets + a `<p>` Fix line, right `<td>` (60%, centered) holds the canvas. Even rows background `#fafcfe`.
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; `.subtitle` `#666` 1.05em; `ul` 0.9em `#333`, li margin 4px 0; `strong` `#1a5276`; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em weight 600 `#1a5276`. No nav bar, no back/home links.
- **Canvas:** intrinsic `width="720" height="300"` per chart; a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`, gray text `#666`/`#333`.
- In regenerated HTML, any card/page links use `.html` extensions (this page has none).
