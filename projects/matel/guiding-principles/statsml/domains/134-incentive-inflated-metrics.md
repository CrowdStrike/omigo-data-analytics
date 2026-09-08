# Incentive-Inflated Metrics Claimed as Organic Growth

**Page type:** detail page (two-column obj-table layout: text left 50%, canvas right 50%, one h2 + one-row table per pitfall)
**HTML title tag:** 134. Incentive-Inflated Metrics Claimed as Organic Growth

**Subtitle:** Incentive-driven spikes get reported as organic growth, poisoning forecasts, models, and pricing power.

## Signup Bonus Collectors

- $100 bank signup bonus → "accounts up 40%!"
- 40% are bonus collectors who close at month 7
- Data: "acquired customer." Reality: rented a number for $100

**Example:** Neobank reports record growth quarter; 18 months later, 40% of those accounts are closed with $0 balance after bonus clawed back.

### Visualization (canvas `c1`, 720×200)

Line chart: account survival curve after a signup bonus, vs an organic baseline.

- **Title (17px, `#1a5276`, top left):** "Account Survival After Signup Bonus".
- **Axes:** blue `#2980b9` lines, width 2 — x-axis from (50,180) to (700,180), y-axis from (50,40) to (50,180).
- **Series (red `#e74c3c`, width 2):** survival % over 8 months `[100, 95, 85, 72, 65, 60, 58, 55]`, points at x = 80 + i*82, y = 180 − value*1.35.
- **Baseline:** dashed green `#27ae60` (dash 4/4) horizontal line at the 60% level from x=80 to x=650.
- **Labels (12px):** gray `#7f8c8d` "Months →" at (350,195); green `#27ae60` "Organic baseline (60%)" at (500,85); red `#e74c3c` "Bonus accounts" at (500,105).

## Demand Pull-Forward

- 5% promo discount → "conversion improved 25%!"
- Pulled forward next month's demand. Day 31: -20%
- Net 60-day impact: 0%. But the +25% is in the quarterly report

**Example:** Flash sale shows 25% conversion lift; following month drops 20%. Net revenue unchanged but promo declared a success.

### Visualization (canvas `c2`, 720×200)

Line chart: demand over 12 time steps with promo spike and post-promo dip highlighted.

- **Title (17px, `#1a5276`):** "Demand Pull-Forward Effect".
- **Axes:** blue `#2980b9`, width 2 — x-axis (50,180)–(700,180), y-axis (50,40)–(50,180).
- **Series (light blue `#3498db`, width 2):** values `[100,100,100,125,125,125,80,80,80,100,100,100]`, points at x = 80 + i*53, y = 180 − value*1.3.
- **Baseline:** dashed gray `#7f8c8d` (dash 4/4) horizontal line at the 100 level from x=80 to x=680.
- **Highlight regions (alpha 0.2):** green `#27ae60` rect (210,40,160×90) over the promo spike; red `#e74c3c` rect (370,100,160×80) over the dip.
- **Labels (12px):** green "+25% (reported)" at (230,55); red "-20% (hidden)" at (390,115); gray `#7f8c8d` "Net = 0%" at (580,90).

## Forgetting Rate as Conversion

- Free trial → "1M users!"
- 900K forgot to cancel
- "Conversion rate: 90%!" Actually: forgetting rate 90%

**Example:** Streaming service reports 90% trial-to-paid conversion; support tickets show 60% of "converts" didn't know they were being charged.

### Visualization (canvas `c3`, 720×200)

Stacked vertical bar breaking down the "converted" trial users.

- **Title (17px, `#1a5276`):** "Trial \"Conversion\" Breakdown".
- **Stacked bar:** single column at x=150, width 200, total height 140 (from y=40 to y=180), split top-to-bottom: red `#e74c3c` 60%, orange `#f39c12` 30%, green `#27ae60` 10%.
- **Segment labels (13px, `#2c3e50`, right of bar at x=380):** "Forgot to cancel (60%)" (y=80), "Didn't notice charge (30%)" (y=130), "Actually wanted product (10%)" (y=170).
- **Annotation (14px, gray `#7f8c8d`, at 400,50):** "Reported: \"90% conversion rate!\"".

## Gaming Referral Systems

- Referral bonus → "viral coefficient 1.8!"
- Self-referral rings gaming $10-per-side
- Real organic viral: 0.3

**Example:** Fintech app shows viral growth; investigation reveals 70% of referrals are same-person multi-device schemes collecting bonus pairs.

### Visualization (canvas `c4`, 720×200)

Two-bar comparison of reported vs real viral coefficient with a threshold line.

- **Title (17px, `#1a5276`):** "Viral Coefficient: Reported vs Real".
- **Baseline:** blue `#2980b9` x-axis from (50,180) to (700,180), width 2.
- **Bars (120px wide, scale 80px per 1.0):** red `#e74c3c` bar at x=150, value 1.8; green `#27ae60` bar at x=420, value 0.3.
- **Value labels (14px, `#2c3e50`, above bars):** "Reported: 1.8"; "Real organic: 0.3".
- **Sub-captions (12px, gray `#7f8c8d`, at y=195):** "(includes self-referral rings)" under left bar; "(actual word-of-mouth)" under right bar.
- **Threshold:** dashed orange `#f39c12` (dash 4/4) horizontal line at value 1.0 across the plot, labeled "Viral threshold (1.0)" in orange at (560, just above the line).

## Model Trained on Incentive Period

- Model learns INCENTIVE-DRIVEN behavior as "normal" demand
- Forecasts post-incentive using inflated baseline
- Over-orders, over-hires based on artificial demand

**Example:** Demand model trained during BOGO promo predicts 2x normal volume as baseline; warehouse overstocks $2M in perishable inventory.

### Visualization (canvas `c5`, 720×200)

Line chart: training-period demand with diverging forecast vs actual lines.

- **Title (17px, `#1a5276`):** "Model Forecast vs Post-Incentive Reality".
- **Axes:** blue `#2980b9`, width 2 — x-axis (50,180)–(700,180), y-axis (50,40)–(50,180).
- **Training series (light blue `#3498db`):** demand values `[150,145,140,100,80,75,70]`, points at x = 80 + i*42, y = 180 − value.
- **Forecast line:** dashed red `#e74c3c` (dash 5/5) from (350,60) to (680,45) — rising, inflated.
- **Actual line:** solid green `#27ae60` from (350,60) to (680,130) — falling.
- **Labels (12px):** light blue "Incentive period (training data)" at (90,50); red "Model forecast (inflated)" at (500,40); green "Actual post-incentive" at (500,125).

## Attribution Without Counterfactual

- "Campaign acquired 50K users!"
- Counterfactual: 30K would have come anyway (organic)
- 15K collected bonus and left. Truly incremental: 5K
- Nobody measures the counterfactual

**Example:** Marketing claims 50K attributed users at $20 CPA ($1M). True incremental: 5K users. Actual CPA: $200/user. 10x reported efficiency.

### Visualization (canvas `c6`, 720×200)

Horizontal stacked bar decomposing the 50K "acquired" users.

- **Title (17px, `#1a5276`):** "Attribution Reality: 50K \"Acquired\" Users".
- **Stacked horizontal bar:** at (100,70), total width 500, height 45, split left-to-right: light gray `#bdc3c7` 60%, red `#e74c3c` 30%, green `#27ae60` 10%.
- **Legend lines (13px, `#2c3e50`, left-aligned at x=100):** "Would have come anyway: 30K (60%)" (y=140), "Collected bonus & left: 15K (30%)" (y=160), "Truly incremental: 5K (10%)" (y=180).
- **Right-side annotations (12px, gray `#7f8c8d`, x=450):** "Reported CPA: $20" (y=140), "Actual incremental CPA: $200" (y=160).

## Selective Chart Window

- Investor presentation: "Revenue growing 50%/month!"
- During promotion period. Post-promo: flat
- Chart shown to investors is the promo period only

**Example:** Series B deck shows 6-month hockey stick; those exact 6 months were a subsidized growth campaign. Month 7 onward: plateau.

### Visualization (canvas `c7`, 720×200)

Line chart: 12-month revenue curve with the shown-to-investors window shaded.

- **Title (17px, `#1a5276`):** "Investor Deck: Selective Window".
- **Axes:** blue `#2980b9`, width 2 — x-axis (50,180)–(700,180), y-axis (50,40)–(50,180).
- **Series (light blue `#3498db`, width 2):** revenue values `[160,145,130,110,90,70,55,50,50,50,50,50]` (note: y = 180 − value, so higher values plot lower — the curve rises then plateaus visually), points at x = 80 + i*53.
- **Shaded window:** green `#27ae60` at alpha 0.15, rect (80,40,318×140) covering the first ~6 months.
- **Labels (12px):** green "← Shown to investors →" at (150,55); red `#e74c3c` "← Hidden (post-promo plateau) →" at (430,135); gray `#7f8c8d` "Months →" at (350,195).

## Trained to Wait for Deals

- Incentive creates habitual expectation
- Train customers to WAIT for deals
- "Why pay full price when sale is next month?"
- Permanent margin destruction from temporary metric inflation

**Example:** After 3 quarterly sales, 45% of customers delay purchases until next promotion. Full-price revenue permanently down 30%.

### Visualization (canvas `c8`, 720×200)

Declining line chart of full-price willingness with vertical "Sale" event markers.

- **Title (17px, `#1a5276`):** "Full-Price Willingness Over Repeated Sales".
- **Axes:** blue `#2980b9`, width 2 — x-axis (50,180)–(700,180), y-axis (50,40)–(50,180).
- **Series (red `#e74c3c`, width 2):** willingness % over 8 quarters `[95, 88, 78, 68, 60, 55, 52, 50]`, points at x = 80 + i*80, y = 180 − value*1.5.
- **Sale markers:** dashed orange `#f39c12` (dash 3/3) vertical lines from y=50 to y=180 at quarters 1, 3, 5, 7 (x = 80 + i*80), each labeled "Sale" (11px orange) just above at y=45.
- **Labels (12px, gray `#7f8c8d`):** "Quarters →" at (350,195); "% willing to pay full price" at (500,70).

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: one `<h2>` (1.4em `#1a5276`, 2px solid `#2980b9` bottom border) per pitfall, followed by a full-width single-row table; left `<td>` (40%) holds `.obj-title` + `<ul>` bullets + an "**Example:**" paragraph, right `<td>` (60%, centered) holds the canvas. Even rows background `#fafcfe`.
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; `.subtitle` `#666` 1.05em; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em weight 600 `#1a5276`; `strong` `#1a5276`; `ul` 0.9em; `.philosophy` class defined (background `#f0f4f8`, left border 4px `#2980b9`) but unused on this page. No nav bar, no back/home links.
- **Canvas:** HTML attributes declare 720×300, but each chart's IIFE explicitly resets to 720×200 — backing store sized to rendered width × `window.devicePixelRatio` (display capped via `style.maxWidth`), CSS size fixed at 720×200px, `ctx.scale` back to logical coordinates. One self-invoking function per chart (no shared setup helper). Titles 17px, labels 11-14px, all in the -apple-system font stack.
- **Palette:** primary blue `#1a5276`, axis blue `#2980b9`, light blue `#3498db`, green `#27ae60`, red `#e74c3c`, orange `#f39c12`, grays `#7f8c8d`/`#2c3e50`/`#bdc3c7`.
- In regenerated HTML, any card/page links use `.html` extensions (this page has none).
