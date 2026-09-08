# Sponsored vs Organic Search Results Intermixed

**Page type:** detail page (two-column obj-table layout: text left ~40%, canvas right ~60%, one row per section)
**HTML title tag:** 121. Sponsored vs Organic Search Results Intermixed

**Subtitle:** Mixing paid and organic results contaminates click signals, confounds experiments, and erodes user trust in the entire results page.

Note: all canvases on this page are declared `width="720" height="300"` in HTML, but each chart's JS resizes its own canvas to an effective 720×200 (backing store = rendered width × dpr, CSS 720px × 200px).

## Users Can't Distinguish Paid from Organic

- 60%+ of users don't notice tiny "Sponsored" label
- Visual similarity deliberately minimizes distinction
- Mobile layouts compress labels further

**Example:** Eye-tracking studies show users fixate on result text, not disclosure labels — leading to 62% misidentification rate.

### Visualization (canvas `c1`, 720×200)

Horizontal bar chart of label recognition rate by device.

- **Title (17px, `#1a5276`, at 200,25):** "User Label Recognition Rate (%)".
- **Rows (labels 17px `#555` at x=10, bars start x=120, height 28, width = value×5, spaced 38px from y=50):** Desktop 38% blue `#2980b9`; Mobile 22% red `#e74c3c`; Tablet 30% orange `#f39c12`; Voice UI 5% purple `#8e44ad`. Value labels ("38%" etc., `#333`) to the right of each bar.

## Click Data Contaminated

- Paid clicks attributed as relevance signals
- Position bias amplified by ad placement
- Feedback loop poisons organic ranking model

**Example:** A paid result in position 1 gets 35% CTR regardless of quality, feeding false relevance signal to the ranking algorithm.

### Visualization (canvas `c2`, 720×200)

Rising line chart of contamination percentage over 10 months.

- **Title (17px, `#1a5276`, at 220,25):** "Click Signal Contamination Over Months".
- **Line (blue `#2980b9`, width 3):** values `[10, 18, 28, 40, 55, 68, 75, 82, 88, 92]`, x = 80 + i×62, y = 180 − value×1.5.
- **Labels:** `#555` "Month 1" at (80,195), "Month 10" at (580,195); red `#e74c3c` "Contamination %" at (80,50).

## Advertisers Buy Position Regardless of Quality

- Bid amount determines visibility, not content value
- Deep-pocket competitors displace better resources
- Quality Score insufficient counterweight

**Example:** A mediocre product with $12 CPC outranks a comprehensive guide because bid trumps relevance weighting.

### Visualization (canvas `c3`, 720×200)

Grouped bar chart of bid amount vs content quality across five advertisers.

- **Title (17px, `#1a5276`, at 220,25):** "Bid Amount vs Content Quality Score".
- **Data (5 groups at x = 100 + i×130, bars 40px wide, height = value×12, baseline y=180):** bids `[12, 9, 7, 4, 2]` red `#e74c3c`; quality `[3, 5, 8, 9, 7]` green `#27ae60` (offset +45px).
- **Legend (x=500):** red 15×15 swatch "Bid ($)", green swatch "Quality" (`#333` text).

## Organic Algorithm Trained on Poisoned Click Data

- Historical clicks include ad-influenced behavior
- Model can't separate genuine preference from purchased attention
- Reinforcement loop entrenches low-quality results

**Example:** After 6 months of ad dominance, organic rankings shift to mirror ad patterns even after campaigns end.

### Visualization (canvas `c4`, 720×200)

Circular feedback-loop diagram with four stage labels.

- **Title (17px, `#1a5276`, at 210,25):** "Poisoned Feedback Loop — Organic Drift".
- **Loop:** blue `#2980b9` arc (width 2) centered at (360,115), radius 65, sweeping 0 to 1.7π, with a blue filled arrowhead at the arc end.
- **Stage labels (15px, red `#e74c3c`):** "Ad Clicks" at (80,100), "Train Model" at (520,100), "Rank Shift" at (340,190), "More Ad-like Results" at (290,55).

## A/B Tests Confounded by Ad Slot Presence

- Treatment group sees different ad density
- Organic CTR changes are artifact of ad displacement
- Interaction effects unmeasurable

**Example:** A ranking algorithm A/B test shows +5% CTR but the real driver is fewer ad slots in treatment, not better organic results.

### Visualization (canvas `c5`, 720×200)

Block decomposition of observed effect into true effect plus confound.

- **Title (17px, `#1a5276`, at 240,25):** "A/B Test: Observed vs True Effect".
- **Blocks (white labels inside):** red `#e74c3c` 160×50 at (150,60) "Observed +5%"; green `#27ae60` 50×50 at (150,130) "+1%"; yellow-orange `#f39c12` 90×50 at (220,130) "+4% (ads)".
- **Side labels (15px, `#555`, x=400):** "Combined (reported)" (y=90), "True algo improvement" (y=150), "Ad slot confound" (y=170).

## Revenue Incentive Degrades Organic Experience

- More ads = more revenue = organizational pressure
- Gradual creep: 1 ad slot becomes 4
- Organic "below the fold" on mobile

**Example:** Platform increases ad slots from 2 to 4, pushing first organic result to position 5 — revenue up 40%, user satisfaction down 25%.

### Visualization (canvas `c6`, 720×200)

Two crossing line series: revenue rising, satisfaction falling, as ad slots increase.

- **Title (17px, `#1a5276`, at 230,25):** "Ad Slots vs Revenue & Satisfaction".
- **Revenue line (green `#27ae60`, width 3):** values `[20, 45, 70, 90, 100, 105]`, x = 100 + i×110, y = 180 − value×1.4; labeled green "Revenue" at (600,60).
- **Satisfaction line (red `#e74c3c`):** values `[95, 80, 60, 45, 35, 28]`, same x/y mapping; labeled red "Satisfaction" at (600,150).
- **X labels (`#555`):** "1 ad" at (90,195), "6 ads" at (620,195).

## Quality Content Outranked by Paid Mediocre Content

- Expert content buried below paid placements
- Small publishers can't compete on ad spend
- Information quality inversely correlated with visibility

**Example:** A peer-reviewed medical resource appears at position 8 while supplement companies buy positions 1-4.

### Visualization (canvas `c7`, 720×200)

Dot plot of search position by content type along a horizontal position axis.

- **Title (17px, `#1a5276`, at 250,25):** "Search Position by Content Type".
- **Dots (8px radius at x = 60 + position×65, y = 60 + i×30, type name `#333` to the right):** Supplement Co. position 1.5 (red `#e74c3c`); Affiliate 2.5 (red); News Site 5 (yellow-orange `#f39c12`); Expert Blog 7 (green `#27ae60`); Peer-Reviewed 8.5 (green).
- **Axis:** blue `#2980b9` horizontal line from (60,175) to (660,175); `#555` labels "Position 1 (top)" at (60,190), "Position 10 (bottom)" at (480,190).

## Trust Erosion — Users Distrust ALL Results

- Once users realize mixing occurs, skepticism generalizes
- Organic results lose credibility by association
- Platform reputation suffers long-term

**Example:** Survey shows users who learn about ad mixing rate organic result trustworthiness 40% lower than naive users.

### Visualization (canvas `c8`, 720×200)

Two-bar comparison of trust scores for naive vs aware users.

- **Title (17px, `#1a5276`, at 230,25):** "Trust Score: Naive vs Aware Users".
- **Bars (white labels inside, x=150, height 40):** blue `#2980b9` 200 wide at y=60, "Naive: 78%"; red `#e74c3c` 120 wide at y=120, "Aware: 47%".
- **Side labels (15px, `#555`, x=400):** "Trust in organic results" (y=85), "Trust after learning about ad mixing" (y=145); red "-40% trust drop" at (400,175).

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: one `<h2>` per pitfall (1.4em `#1a5276`, bottom border `2px solid #2980b9`, padding-bottom 8px) followed by a single-row full-width table; left `<td>` (40%) holds `.obj-title` + bullet list + bold-labeled example paragraph, right `<td>` (60%, centered) holds the canvas. Even rows background `#fafcfe`.
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; `.subtitle` `#666` 1.05em; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em weight 600 `#1a5276`; `strong` `#1a5276`; bullets 0.9em `#333`. No nav bar, no back/home links.
- **Canvas:** 8 canvases with HTML attributes `width="720" height="300"`; each chart's IIFE individually sets backing store to 720×200 × `window.devicePixelRatio` to 720px × 200px, and calls `ctx.scale` so drawing stays in logical coordinates. Base chart font variable `fontSize = 17` px -apple-system.
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#f39c12`, purple `#8e44ad`, gray `#555`/`#333`.
- In regenerated HTML, any card/page links use `.html` extensions.
