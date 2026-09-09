# Subscription Services / Auto-Renewal

**Page type:** detail page (two-column obj-table layout: text left ~40%, canvas right ~60%, one row per section)
**HTML title tag:** 120. Subscription Services / Auto-Renewal

**Subtitle:** Auto-renewal and cancellation friction inflate retention metrics, hiding the gap between paying subscribers and customers who actually want to stay.

## Auto-Renew Inflates "Retention"

- 30% of subscribers are zombies — paying but never using the service
- Reported retention rate conflates "wants to stay" with "forgot to cancel"
- True engaged retention is far lower than headline metrics suggest

**Example:** Streaming service reports 94% retention, but usage logs show 30% haven't logged in for 3+ months. Real "intent-to-stay" retention is closer to 66%.

### Visualization (canvas `c1`, 720×300)

Single stacked horizontal bar decomposing reported retention.

- **Title (17px, `#1a5276`, at 20,25):** "Retention: Reported vs Real Engagement".
- **Stacked bar (at y=50, height 50, total width 500 starting x=100):** green `#27ae60` segment 66% with white 14px label "Active users: 66%"; orange `#e67e22` segment 28% with white label "Zombies: 28%"; red `#e74c3c` segment 6% with red text "6% churned" beside it at (580,80).
- **Annotations (13px, `#333`):** "Reported retention: 94%" at (100,125), "Engaged retention: 66%" at (100,145); blue `#2980b9` underline stroke from (100,115) spanning 94% of the bar; caption '← Company reports this entire bar as "retained" →' at (150,165).

## Free Trial Conversion Includes Forgot-to-Cancel

- True intent-to-pay rate is much lower than reported conversion
- "Converted" users who cancel in month 2 were never real customers
- Opt-out trials inflate conversion by 40-60% vs opt-in

**Example:** Free trial "converts" at 62%. But 35% cancel within 48 hours of first charge. Actual intent-to-pay conversion: ~40%. The 22-point gap is pure friction revenue.

### Visualization (canvas `c2`, 720×300)

Three-step funnel bar chart decomposing trial conversion.

- **Title (17px, `#1a5276`):** 'Free Trial "Conversion" Decomposed'.
- **Funnel bars (14px white labels, x=80, height 30):** blue `#3498db` 550 wide at y=50, "100% start free trial"; green `#2ecc71` 341 wide at y=90, '62% "convert" (charged)'; red `#e74c3c` 220 wide at y=130, "40% true intent-to-pay".
- **Annotations (13px):** `#333` "22% gap = forgot to cancel" at (440,110) with a short dashed red connector line (dash 3/3) from (421,95) to (435,105); gray `#7f8c8d` footnote '35% of "converted" cancel within 48h of first charge' at (80,185).

## Churn Definition Ambiguity

- Cancelled vs stopped-using vs downgraded — all different events
- Company picks whichever definition makes churn look lowest
- "Voluntary churn" excludes payment failures, hiding true attrition

**Example:** Same company, same month — churn is 2.1% (cancelled only), 4.8% (cancelled + payment failure), 8.3% (cancelled + inactive 60 days), or 11.2% (including downgrades). Pick your narrative.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart of four churn definitions for the same month.

- **Title (17px, `#1a5276`):** "Same Company, Same Month — 4 Churn Definitions".
- **Rows (bars start x=180, height 28, width = value×40, spaced 38px starting y=50):** "Cancelled only" 2.1% green `#27ae60`; "+ Payment fail" 4.8% yellow-orange `#f39c12`; "+ Inactive 60d" 8.3% orange `#e67e22`; "+ Downgrades" 11.2% red `#e74c3c`. Definition labels (13px `#333`) left of bars; white 14px value labels inside bars.
- **Annotations:** gray `#7f8c8d` 12px "← Company picks whichever looks best for investors" at (200,195); vertical dashed `#1a5276` reference line (dash 4/3) at the 2.1% bar's end, from y=45 to y=200.

## Annual Lock-In Hides Monthly Dissatisfaction

- User unhappy at month 3 but trapped until month 12
- Annual plans show 0% churn for 11 months then cliff at renewal
- NPS surveys at month 3 predict renewal failure but nobody acts

**Example:** Annual subscribers: satisfaction drops to 4.2/10 by month 5, but churn is reported as 0% until the annual renewal cliff where 45% don't renew. Monthly equivalent would show gradual 4%/month churn.

### Visualization (canvas `c4`, 720×300)

Dual line chart over 12 months: satisfaction dropping vs reported churn flat then cliffing.

- **Title (17px, `#1a5276`):** "Annual Plan: Satisfaction Drops, Churn Hidden".
- **Axes:** black `#333` L-shape — x (60,170)→(660,170), y (60,40)→(60,170). Month labels "M1"…"M12" (11px) at x = 75 + m×50, y=185.
- **Satisfaction line (red `#e74c3c`, width 2):** monthly values `[8.5, 7.8, 6.5, 5.2, 4.2, 4.0, 3.8, 3.9, 4.0, 3.7, 3.5, 3.8]`, x = 80 + i×50, y = 170 − value×14.
- **Reported churn line (blue `#2980b9`, width 3):** flat from (80,168) to (580,168), then cliff up to (630,100).
- **Legend (12px, x=400):** red swatch "Satisfaction (drops early)", blue swatch "Reported churn (cliff at renewal)".

## Downgrade Friction as Manufactured Retention

- 7 clicks to cancel, hidden button, phone call required
- "Retained" users are hostages, not loyal customers
- Dark patterns inflate retention metrics but destroy LTV

**Example:** Company A: 1-click cancel, 88% retention. Company B: 7-step cancel flow, 93% retention. Company B reports higher retention but has 2x the negative reviews and 50% lower NPS.

### Visualization (canvas `c5`, 720×300)

Two labeled bars plus an NPS/reviews comparison row.

- **Title (17px, `#1a5276`):** "Cancel Friction vs Actual Loyalty".
- **Bars (13px white labels, x=80, height 35, width = retention×4):** green `#27ae60` at y=60, "Company A: 1-click cancel → 88% retention"; orange `#e67e22` at y=105, "Company B: 7-step cancel → 93% retention".
- **Comparison row (14px, y=165):** `#333` "NPS Score:", green "A: +42", red `#e74c3c` "B: -12", `#333` "Negative reviews:", green "A: 340", red "B: 680 (2x)".
- **Caption (12px, `#7f8c8d`, at 150,192):** "Higher retention ≠ higher loyalty when friction is the cause".

## Zombie Subscribers — Paying But Gone

- Paying $15/month, haven't opened the app in 6 months
- These subscribers are profitable but ethically questionable
- When zombies finally notice, they churn AND leave negative reviews

**Example:** Gym membership: 67% of members visit less than once per month. Average zombie pays for 7 months before noticing. Revenue model depends on $15 x 7 = $105 from people getting zero value.

### Visualization (canvas `c6`, 720×300)

Timeline diagram of declining usage with continuing payments.

- **Title (17px, `#1a5276`):** "Zombie Subscriber Lifecycle (Gym Example)".
- **Timeline:** blue `#2980b9` horizontal line (width 2) from (60,100) to (660,100); month labels "M1"…"M10" (10px `#333`) below at x = 80 + i×58.
- **Usage dots:** blue `#3498db` 3px-radius dots stacked above the line, counts per month `[8, 6, 4, 2, 1, 0, 0, 0, 0, 0]` (stacked 8px apart from y=85 upward).
- **Payments:** red `#e74c3c` 12px "$15" labels below months 1–7 at y=135.
- **Annotations:** red 13px "← Zero usage, still paying →" at (350,80) and "Finally notices → cancels + angry review" at (450,135); green `#27ae60` 14px "Zombie revenue: $15 × 7 months = $105 from zero value delivered" at (100,170); gray `#7f8c8d` 11px "67% of gym members visit < 1x/month" at (100,190).

## Win-Back Offers That Train Users to Churn

- Cancel → get 50% off for 3 months → creates perverse incentive
- Users learn: churn periodically to get discount
- "Win-back rate" looks great but you're training strategic churners

**Example:** 18% of "won back" subscribers churn again within 90 days to trigger another offer. Each cycle costs $22.50 in discounts. After 3 cycles, LTV is negative vs just keeping them at full price.

### Visualization (canvas `c7`, 720×300)

Cycle diagram of four circular nodes with arrows and a dashed loop-back curve.

- **Title (17px, `#1a5276`):** "Win-Back Offers Create Strategic Churners".
- **Nodes (30px-radius circles at y=80, white 10px two-line labels):** (100,80) "Subscribe / $15/mo" green `#27ae60`; (250,80) "Cancel / (trigger offer)" red `#e74c3c`; (400,80) "Win-back / $7.50/mo x3" green; (550,80) "Cancel / again..." red. Black `#333` arrows connect consecutive nodes.
- **Loop:** dashed red `#e74c3c` quadratic curve (dash 4/3) from (550,115) back to (100,115) via control point (350,170).
- **Annotations:** red 13px "18% repeat this cycle — each round costs $22.50 in discounts" at (120,165); gray `#7f8c8d` 12px "After 3 cycles: LTV goes negative vs full-price retention" at (120,185).

## Subscription Fatigue — Correlated Cancel Sprees

- Average household has 15+ subscriptions totaling $200+/month
- Periodic "cancel spree" creates correlated churn across services
- Your churn is not independent of competitors' churn

**Example:** January "subscription audit" trend: 34% of households cancel 3+ services in one week. Services see correlated churn spike — your retention model assuming independent cancellation is wrong.

### Visualization (canvas `c8`, 720×300)

Three-series monthly churn line chart with a highlighted January spike.

- **Title (17px, `#1a5276`):** "Subscription Fatigue: Correlated Cancel Sprees".
- **Axes:** black `#333` L-shape — x (60,165)→(660,165), y (60,40)→(60,165). Month labels Oct…Sep (10px) at x = 72 + m×49, y=178.
- **Series (width 2, x = 80 + i×49, y = 165 − value×13):** Streaming A blue `#2980b9` `[3, 3.2, 2.8, 8.5, 3.5, 3.1, 2.9, 3.0, 3.2, 2.8, 3.0, 3.1]`; Streaming B orange `#e67e22` `[2.5, 2.8, 2.4, 7.2, 3.0, 2.6, 2.5, 2.7, 2.4, 2.6, 2.5, 2.8]`; Fitness app purple `#8e44ad` `[4.0, 3.8, 3.5, 9.1, 4.2, 3.9, 3.7, 4.0, 3.8, 3.6, 3.9, 4.1]`.
- **January highlight:** translucent red `rgba(231,76,60,0.15)` rect at (215,35), 50×135, with red 11px label 'Jan "audit"' at (215,180).
- **Legend (11px, x=550):** "Streaming A" blue, "Streaming B" orange, "Fitness app" purple.
- **Caption (12px, `#7f8c8d`, at 100,195):** "34% cancel 3+ services in one week — churn is NOT independent".

## Tier Erosion — Old Plans Shrink, New Plans Re-Anchor

- Same plan name, shrinking contents: features migrate to a new top tier, ads appear, limits tighten
- The new pricier tier re-anchors the ladder — yesterday's standard becomes today's degraded mid-tier
- "Price unchanged" dashboards miss the value cut; effective price-per-feature went up
- Longitudinal metrics (ARPU, plan mix, churn by tier) silently compare different products under the same label

**Illustrative example:** A streaming "Standard" plan keeps its $12 price but loses 4K and gains ads, while a new $19 "Premium" tier holds the old feature set. Reported price increase: 0%. Effective increase for the same product: $7.

### Visualization (canvas `c9`, 720×300)

Three outlined feature boxes (then/now/new tiers) with a migration arrow.

- **Title (17px, `#1a5276`):** 'Same "Standard" Label, Different Product (illustrative)'.
- **Box 1 (green `#27ae60` outline, 200×130 at (40,45)):** bold 13px header "THEN — Standard $12"; 12px `#333` features: "4K quality", "No ads", "4 screens", "Downloads".
- **Box 2 (red `#e74c3c` outline, 190×130 at (300,45)):** header "NOW — Standard $12"; features: "HD only", "Ads included", "2 screens", plus gray `#999` "(features moved up)".
- **Box 3 (orange `#e67e22` outline, 180×130 at (520,45)):** header "NEW — Premium $19"; features: "4K quality", "No ads", "4 screens", "Downloads".
- **Arrow:** dashed orange `#e67e22` quadratic curve (dash 4/3) from (240,110) to (520,120) via control point (380,220), labeled orange 11px "old feature set migrates to the new top tier" at (280,210).
- **Caption (12px, `#7f8c8d`, at 40,250):** "Reported price change: 0%. Same product now costs $19 — a hidden $7 increase."

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: one `<h2>` per pitfall (1.4em `#1a5276`, bottom border `2px solid #2980b9`, padding-bottom 8px) followed by a single-row full-width table; left `<td>` (40%) holds `.obj-title` + bullet list + bold-labeled example paragraph, right `<td>` (60%, centered) holds the canvas. Even rows background `#fafcfe`.
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; `.subtitle` `#666` 1.05em; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em weight 600 `#1a5276`; `strong` `#1a5276`; bullets 0.9em `#333`. No nav bar, no back/home links.
- **Canvas:** all 9 canvases declared `width="720" height="300"`; a shared IIFE loops over all canvases, sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`), and calls `ctx.scale` so drawing stays in logical coordinates. Default chart font 17px -apple-system.
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`/`#3498db`, green `#27ae60`/`#2ecc71`, red `#e74c3c`, orange `#e67e22`/`#f39c12`, purple `#8e44ad`, gray `#7f8c8d`/`#555`/`#333`/`#666`.
- In regenerated HTML, any card/page links use `.html` extensions.
