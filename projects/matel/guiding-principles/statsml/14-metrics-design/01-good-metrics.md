# Good Metrics — 20 Real-World Examples by Domain

**Page type:** detail page, metric-testing template (white background; one two-column obj-table per metric: text left 40%, canvas right 60%; obj-title heading; canvases 720×200 with devicePixelRatio scaling and redraw on resize)
**HTML title tag:** Good Metrics — 20 Real-World Examples by Domain

**Subtitle:** Each row: one metric showing WHY it's good, with a visualization of the concept.

## 1. Weekly Users Performing Core Action (SaaS)

- Measures actual engagement, not vanity logins
- Chat app: sent message; Figma: edited design
- Gap between "logged in" and "core action" reveals at-risk users
- Leading indicator of retention and expansion revenue

### Visualization (canvas `c1`, 720×200)

Grouped bar chart: logged-in vs core-action weekly users for three products.

- **Title (bold 17px `#1a5276` at (80, 20)):** "Weekly Users: Logged In vs Core Action".
- **Data:** Chat app loggedIn 160k / core 95k; Figma 120k / 68k; Dropbox 180k / 72k. Scale max 180.
- **Chart area:** x 80–680, y 30–165; horizontal gridlines `#ddd` at 0/25/50/75/100% of max with gray `#555` 13px labels "0k, 45k, 90k, 135k, 180k".
- **Bars:** per product, pair of 35px bars centered in its third: light blue `#aed6f1` (Logged In) left, blue `#2980b9` (Core Action) right.
- **Gap labels:** red `#e74c3c` 12px above each logged-in bar: "gap:65k", "gap:52k", "gap:108k".
- **X labels:** `#333` 14px product names below baseline.
- **Legend (top right):** `#aed6f1` swatch "Logged In" at x=500, `#2980b9` swatch "Core Action" at x=590.

## 2. Revenue per Converting Session (E-commerce)

- Separates conversion rate from AOV — two clean distributions
- Avoids zero-inflation (97% sessions have $0 revenue)
- Each component is independently testable in A/B experiments
- Reveals whether growth comes from more buyers or bigger carts

### Visualization (canvas `c2`, 720×200)

Concept diagram: a zero-inflated revenue distribution split into two cleaner metrics.

- **Title (bold 17px `#1a5276`):** "Splitting Revenue/Session into Two Cleaner Metrics".
- **Left panel (label gray 13px):** "Revenue per ALL Sessions (zero-inflated)" — one tall red `#e74c3c` bar (20×95 at x=45) followed by 8 shrinking light-red `#f5b7b1` bars (14px wide, heights max(3, 30−4i)) on a gray baseline at y=150; small gray 11px caption "97% zeros".
- **Center:** big blue `#2980b9` bold 24px arrow "→" with 12px label "Split into".
- **Right panel:** bold 13px gray "1. Conversion Rate: 3.2%" above a green `#27ae60` filled bar (120×18) containing white 11px text "Converters 3.2%"; bold 13px gray "2. AOV (converters only): $87" above a blue `#2980b9` bell curve (Gaussian, amplitude 30, centered x=390, half-width 80, baseline y=155).
- **Callout:** green bold 16px at (530, 110): "✓ Both testable!".

## 3. MTTD + False Positive Rate Paired (Security)

- MTTD alone is gameable — alert on everything for instant detection
- FP rate constraint prevents gaming via volume
- Forces genuine detection quality improvement
- Both must improve simultaneously to claim progress

### Visualization (canvas `c3`, 720×200)

Two pairs of semicircular gauges: a good system vs a gamed system.

- **Title (bold 17px `#1a5276`):** "MTTD + False Positive Rate: Paired Constraint".
- **Gauge style:** half-circle arcs (radius 50, line width 12), gray `#eee` track with colored value arc; value text bold 16px in the gauge color, label 13px gray below.
- **Left pair (heading green `#27ae60` bold 14px "Good: Fast + Low FP"):** MTTD gauge at (140,140): 12min of 60, green; FP Rate gauge at (290,140): 5% of 100, green. Green 12px caption "✓ Passes both".
- **Right pair (heading red `#e74c3c` bold 14px "Gamed: Fast + HIGH FP"):** MTTD gauge at (500,140): 3min of 60, amber `#f39c12`; FP Rate gauge at (650,140): 72% of 100, red. Red caption "✖ Fails constraint!".

## 4. Net Revenue Retention NRR (Subscription)

- Single number: expansion + contraction + churn combined
- >100% means growing from existing customers alone
- Encodes full lifecycle economics without 3 dashboards
- Best SaaS companies: 120-140% NRR

### Visualization (canvas `c4`, 720×200)

Waterfall chart of ARR components.

- **Title (bold 17px `#1a5276`):** "Net Revenue Retention: Waterfall (NRR = 115%)".
- **Bars (70px wide, 30px gaps, baseline y=170, scale max 130):** Starting ARR $100k blue `#2980b9` (base 0); Expansion +$25k green `#27ae60` (base 100); Contraction −$5k orange `#e67e22` (base 125); Churn −$5k red `#e74c3c` (base 120); Ending ARR $115k dark blue `#1a5276` (base 0).
- **Value labels:** bold 14px `#333` above each bar ("$100k", "+$25k", "-$5k", "-$5k", "$115k"); 12px gray two-line category labels below ("Starting/ARR", "Expansion", "Contraction", "Churn", "Ending/ARR").
- **Callout (bold 15px `#1a5276` at (380, 80)):** "NRR = 115/100 = 115% (>100% = net growth)".

## 5. First Pass Yield + Cycle Time Paired (Manufacturing)

- FPY alone: slow down for perfection (gaming)
- Cycle time alone: rush and accept defects (gaming)
- Paired constraint forces genuine process improvement
- Target quadrant: fast AND high quality

### Visualization (canvas `c5`, 720×200)

Scatter plot with four quadrants.

- **Title (bold 17px `#1a5276`):** "First Pass Yield vs Cycle Time: Four Quadrants".
- **Axes:** chart area x 90–620, y 35–175; solid `#333` L axes; dashed gray `#bbb` (dash 5/5) mid lines splitting into quadrants.
- **Quadrant labels (bold 12px, centered):** green `#27ae60` "FAST + GOOD" (top right); orange `#e67e22` "SLOW + GOOD" (top left); red `#e74c3c` "SLOW + BAD" (bottom left); amber `#f39c12` "FAST + BAD" (bottom right).
- **Points (radius 6, white stroke):** normalized (x,y,color) = (0.75,0.85,green), (0.3,0.9,orange), (0.8,0.3,amber), (0.2,0.2,red), (0.65,0.7,green), (0.55,0.8,green).
- **Star:** green 20px "★" beside the (0.75,0.85) point marking the target quadrant.

## 6. Risk-Adjusted Mortality (Healthcare)

- Raw mortality penalizes hospitals taking sicker patients
- Compares actual vs EXPECTED given patient severity
- Ratio <1.0 = better than expected for patient mix
- Apples-to-apples comparison across very different facilities

### Visualization (canvas `c6`, 720×200)

Two triplets of bars comparing raw, expected, and adjusted mortality for two hospitals.

- **Title (bold 17px `#1a5276`):** "Raw vs Risk-Adjusted Mortality: Two Hospitals".
- **Headings (bold 13px gray):** "Hospital A (Trauma Center)" at x=110; "Hospital B (Suburban)" at x=430.
- **Bars (50px wide, baseline y=165, scale max 12%, value labels bold 12px above, 11px labels below):** Hospital A — Raw 8.5% red `#e74c3c`, Expected 9.0% gray `#bbb`, Adjusted 3.2% green `#27ae60` (at x=110/175/240). Hospital B — Raw 3.0% light blue `#aed6f1`, Expected 2.8% gray `#bbb`, Adjusted 5.1% orange `#e67e22` (at x=430/495/560).
- **Verdicts (bold 12px below baseline):** green "✓ Actually excellent" (left); orange "✖ Actually underperforming" (right).
- **Divider:** vertical dashed `#ddd` line (dash 4/4) at x=365.

## 7. Retention Curve Shape D1/D7/D30 (Content)

- Trajectory not snapshot — steep drop = doesn't stick
- Flat curve = habitual use, product-market fit
- Two products with same DAU can have opposite trajectories
- Shape predicts long-term LTV better than any single-day number

### Visualization (canvas `c7`, 720×200)

Two retention curves over 30 days.

- **Title (bold 17px `#1a5276`):** "Retention Curves: Shape Matters More Than Snapshot".
- **Chart area:** x 80–650, y 40–165; solid `#333` L axes.
- **Data (days [0,1,3,7,14,21,30], % retained, y scale 0–100):** Good curve green `#27ae60` width 3 with 4px dots: [100, 72, 58, 48, 43, 40, 38]. Bad curve red `#e74c3c`: [100, 40, 22, 12, 8, 6, 5].
- **Legend (bold 13px at x=420):** green "— Good (flat = habitual)"; red "— Bad (steep = churn)".
- **X labels:** gray 11px "D0, D1, D3, D7, D14, D21, D30" under each point.

## 8. Marketplace Liquidity: % Listings Transacting in 7 Days

- Measures supply-demand match quality directly
- GMV can grow while marketplace is dying (just add supply)
- Liquidity captures health of the matching function
- Core value prop of any marketplace is efficient matching

### Visualization (canvas `c8`, 720×200)

Three paired-bar groups: listings vs transactions, with liquidity percentage.

- **Title (bold 17px `#1a5276`):** "Marketplace Liquidity: Listings vs Transactions (7-day)".
- **Groups (bars 55px wide, baseline y=160, scale max 1000, group pitch 200px starting x=150):** Healthy — listings 500 (light blue `#d5e8f7` fill, `#2980b9` stroke), transactions 350 green `#27ae60`, label "70% liq."; Dying — 900 listings, 90 transactions red `#e74c3c`, "10% liq."; New — 150 listings, 80 transactions amber `#f39c12`, "53% liq.".
- **Labels:** liquidity % bold 13px in the group color above the taller bar; group name 12px gray below.

## 9. DORA: Change Failure Rate + MTTR (DevOps)

- Deploy fast BUT measure what breaks and how fast you fix it
- Four metrics that tension each other prevent gaming any one
- High frequency only good if failure rate is low
- Captures full delivery performance picture

### Visualization (canvas `c9`, 720×200)

Radar chart with 4 axes comparing elite vs current performance.

- **Title (bold 17px `#1a5276`):** "DORA Metrics: Radar Chart (4 Dimensions)".
- **Radar:** center (280,110), radius 70, 4 axes starting at top, 4 concentric gray `#ddd` ring polygons.
- **Axes/labels (12px `#333`):** "Deploy Freq", "Lead Time", "CFR (inv)", "MTTR (inv)".
- **Shapes:** Elite — values [0.95, 0.9, 0.85, 0.9], stroke `rgb(39,174,96)`, fill `rgba(39,174,96,0.2)`. Current — [0.7, 0.5, 0.4, 0.55], stroke `rgb(231,76,60)`, fill `rgba(231,76,60,0.15)`.
- **Legend (swatches at x=460):** green "Elite", red "Current".
- **Notes (gray 12px):** "All 4 must improve together." / "Can't game one axis alone."

## 10. Incrementality from Holdout (Ads)

- True causal lift, not correlated attribution
- Holdout experiment: identical people, only difference is ad exposure
- Attribution inflates ad value by crediting organic sales
- Only method that answers "did the spend actually move the needle?"

### Visualization (canvas `c10`, 720×200)

Two bar panels: holdout experiment vs attribution comparison.

- **Title (bold 17px `#1a5276`):** "Incrementality: True Lift vs Misleading Attribution".
- **Left panel (heading bold 13px gray "Holdout Experiment"):** bars 70px wide, baseline y=160, scale max 20%. Holdout 9.5% gray `#95a5a6`; Treatment 12% blue `#2980b9` with the top 2.5% slice overlaid green `#27ae60`. Value labels "9.5%" / "12%" and names "Holdout" / "Treatment" in 12px `#333`. Green bold 12px callout "True lift: 2.5%".
- **Right panel (heading "Attribution vs Reality"):** red `#e74c3c` bar at 60% alpha, height 12% labeled "12% attributed"; green bar height 2.5% labeled "2.5% true". Red bold 12px callout "4.8x overstatement!".

## 11. Time to First Value (SaaS)

- How fast a new user reaches their "aha" moment
- Leading indicator of activation and long-term retention
- Directly actionable: shorten onboarding, reduce friction
- Predicts 30-day retention better than signup volume

### Visualization (canvas `c11`, 720×200)

Bar chart of 30-day retention by time-to-first-value bucket, colored on a green-to-red gradient.

- **Title (bold 17px `#1a5276`):** "Time to First Value: Faster = Higher Retention".
- **Chart area:** x 80–680, y 45–165; gridlines `#ddd` at 0/25/50/75/100%, y labels "0%–100%" in 11px gray.
- **Data:** buckets `['<1h', '1-4h', '4-24h', '1-3d', '3-7d', '>7d']` with retention `[78, 62, 45, 28, 15, 5]`.
- **Bar colors:** computed `rgb(red, green, 60)` where green = 174·(retention/78) and red = 231·(1−retention/78) — leftmost bar green, rightmost red.
- **Labels:** retention % 11px above each bar, bucket below; axis caption `#1a5276` 13px "Time to \"aha\" moment →"; green bold 13px "← Faster = stickier" at top right.

## 12. Customer Health Score Composite (SaaS)

- Combines usage frequency + support tickets + billing status
- Early warning system: declining score precedes churn by 60-90 days
- Actionable: triggers CS outreach before customer decides to leave
- Weighted composite avoids single-signal false alarms

### Visualization (canvas `c12`, 720×200)

Three weighted signal bars combined into one gauge score.

- **Title (bold 17px `#1a5276`):** "Customer Health Score: Composite Early Warning".
- **Signal bars (50px wide, baseline y=155, heading bold 12px gray "Signal Weights:"):** Usage Freq 80% (weight 40%w) green `#27ae60`; Support Tickets 30% (30%w) red `#e74c3c` (colored green if >50%, red otherwise); Billing Status 90% (30%w) green. Value % and weight labels 11px `#333`, two-line names below.
- **Arrow:** blue bold 22px "→" at (265, 110).
- **Gauge:** half-circle at (400,130), radius 55, line width 14, gray `#eee` track, amber `#f39c12` arc for score 0.68; bold 20px amber "68" and 12px gray "Health Score".
- **Thresholds (11px):** green "80+ healthy"; amber "50-80 at risk"; red "<50 danger".
- **Callout (bold 12px `#1a5276`):** "68 = At Risk → trigger CS outreach".

## 13. Gross Margin per Order After Returns (E-commerce)

- TRUE profitability, not vanity revenue
- Includes COGS, shipping, return costs, processing fees
- Category with highest revenue often has lowest margin
- Prevents the "growing into bankruptcy" trap

### Visualization (canvas `c13`, 720×200)

Paired bars per category: revenue vs post-return margin.

- **Title (bold 17px `#1a5276`):** "Gross Margin per Order: Revenue vs TRUE Profit".
- **Data (scale max 130, bars 35px wide, group pitch 150px from x=80, baseline y=160):** Electronics $120 rev / $8 margin; Fashion $85 / $22; Grocery $45 / $18; Home $95 / $32.
- **Colors:** revenue light blue `#aed6f1`; margin green `#27ae60`. Value labels 11px `#333` above bars, category names below.
- **Legend:** swatches at x=530 — "Revenue", "Margin (post-return)".
- **Callout (red bold 12px at (480, 140)):** "Highest revenue ≠ highest margin!".

## 14. Error Budget Remaining (SRE)

- How much failure budget is left this month, SLO-based
- Balances reliability vs velocity: budget left = can deploy
- Teams self-regulate: freeze deploys when budget is low
- Converts abstract reliability targets into concrete decisions

### Visualization (canvas `c14`, 720×200)

Declining line/area chart of budget remaining over a month with a freeze threshold.

- **Title (bold 17px `#1a5276`):** "Error Budget Remaining: SLO-Based Decision Making".
- **Chart area:** x 60–680, y 50–145; solid `#333` L axes.
- **Data:** days `[1, 5, 8, 12, 15, 18, 22, 25, 28, 30]`, budget % `[100, 88, 82, 70, 55, 48, 40, 32, 28, 25]`; blue `#2980b9` line width 3 with fill `rgba(41,128,185,0.1)` down to the baseline.
- **Threshold:** dashed red `#e74c3c` horizontal line (dash 5/3) at 20%, labeled red 11px "FREEZE threshold (20%)".
- **X labels:** gray 11px "D1, D8, D15, D22, D28" (every other day value).
- **Verdict captions (bold 12px at y=175):** green "✓ Budget > 20%: deploy freely"; red "✖ Budget < 20%: freeze deploys, fix reliability".

## 15. Cart Abandonment by Step (E-commerce)

- WHICH step kills conversion — not just "abandoned"
- Pinpoints exact friction: shipping reveal? account creation?
- Directly actionable: fix the worst step first
- Funnel shape reveals UX problems invisible to overall rate

### Visualization (canvas `c15`, 720×200)

Funnel bar chart across checkout steps, colored by drop severity.

- **Title (bold 17px `#1a5276`):** "Cart Abandonment by Step: WHERE Users Drop Off".
- **Data:** steps `['Cart', 'Shipping Info', 'Shipping Cost', 'Payment', 'Confirm']` with remaining % `[100, 72, 48, 42, 38]`; chart area x 60–680, y 45–155.
- **Bar colors by step-drop:** drop >20 red `#e74c3c`; drop >5 amber `#f39c12`; else green `#27ae60`.
- **Labels:** % bold 12px above each bar; step name 10px below (two lines where needed); drops >10 annotated inside bar top in red bold 11px ("-28%", "-24%").
- **Callout (red bold 13px at (200, 42)):** "↓ Shipping cost reveal kills 24%!".

## 16. Repeat Purchase Within 90 Days (E-commerce)

- Loyalty signal, not just acquisition volume
- Separates "bought once from an ad" from "genuine customer"
- High repeat rate = sustainable unit economics
- Cohort-based: tracks improvement over time

### Visualization (canvas `c16`, 720×200)

Line chart of repeat-purchase rate by monthly cohort.

- **Title (bold 17px `#1a5276`):** "Repeat Purchase Within 90 Days: Cohort Trends".
- **Chart area:** x 80–650, y 45–155; solid `#333` L axes plus `#ddd` gridlines.
- **Data:** cohorts `['Jan','Feb','Mar','Apr','May','Jun']`, rates `[22, 24, 23, 28, 31, 34]`; y scale maps 15–40%.
- **Series:** blue `#2980b9` line width 3 with 5px dots; rate % 11px above each point, cohort name below baseline.
- **Callouts:** green bold 13px "↑ Loyalty improving: 22% → 34%"; gray 12px "Sustainable growth signal".

## 17. Offer Acceptance Rate (HR)

- Are candidates choosing YOU? Leading indicator of employer brand
- Low acceptance = comp, culture, or process problem
- Directly actionable: fix offers, speed up process, improve pitch
- Lagging alternative (attrition) arrives 12 months too late

### Visualization (canvas `c17`, 720×200)

Horizontal bar chart of acceptance rate by team against a target line.

- **Title (bold 17px `#1a5276`):** "Offer Acceptance Rate: Are Candidates Choosing You?".
- **Data:** Engineering 72%, Sales 88%, Design 65%, Marketing 82%, Support 91%; target 80%.
- **Bars:** horizontal, 18px tall, 8px gaps, starting x=130, width scaled to 100% over 550px; green `#27ae60` if ≥80%, red `#e74c3c` if below. Team names right-aligned 12px left of bars, rate % after each bar.
- **Target line:** vertical dashed `#1a5276` (dash 4/3, width 2) at 80% with bold 11px label "80% target".
- **Callout (red bold 12px):** "Below target = comp/process/brand problem".

## 18. p99 Latency Not Average (Infrastructure)

- Worst 1% experience catches what average hides
- Average can be 50ms while p99 is 3000ms — broken for some users
- Often the power users (most valuable) hit tail latency
- Forces engineering to fix outliers, not just optimize the common path

### Visualization (canvas `c18`, 720×200)

Right-skewed latency distribution with average and p99 marker lines.

- **Title (bold 17px `#1a5276`):** "p99 Latency vs Average: What Average Hides".
- **Chart area:** x 80–650, y 50–160.
- **Distribution:** blue `#2980b9` curve width 2 with fill `rgba(41,128,185,0.1)`: value = 100·exp(−x/20) + 5·exp(−(x−150)²/200) over x 0–200 (sharp exponential decay plus a small tail bump at x≈150), scaled to max 110.
- **Markers:** solid green `#27ae60` vertical line at x=20/200 labeled bold 12px "avg: 50ms"; solid red `#e74c3c` vertical line at x=140/200 labeled "p99: 3000ms" with red 12px note "Tail = broken for 1% of users".
- **X labels (11px gray):** "0" left, "Latency (ms) →" center, "5000" right.

## 19. OEE: Availability x Performance x Quality (Manufacturing)

- One number capturing three independent dimensions
- Multiplicative: can't hide weakness in one factor
- World-class OEE = 85%; most plants run 60%
- Decomposition reveals WHERE losses occur

### Visualization (canvas `c19`, 720×200)

Three factor bars multiplied into one OEE result bar.

- **Title (bold 17px `#1a5276`):** "OEE: Availability × Performance × Quality".
- **Factor bars (80px wide, 25px gaps from x=50, baseline y=160):** Availability 90% blue `#2980b9`; Performance 82% orange `#e67e22`; Quality 95% green `#27ae60`. Value % bold 13px above, name 11px below.
- **Operators:** gray bold 20px "×", "×", "=" between bars.
- **Result bar:** OEE = 0.90 × 0.82 × 0.95 ≈ 70%, colored amber `#f39c12` (green if ≥85%, amber if ≥60%, red below); label bold 15px "70% OEE".
- **Notes (right side):** gray 12px "World-class: 85%" / "Average plant: 60%"; amber bold 12px "This plant: 70% (room to improve)"; `#1a5276` 11px "Multiplicative: can't hide weakness" / "in one factor.".

## 20. DAU/MAU Ratio as Stickiness (Product)

- How often do monthly users come back daily?
- 50%+ = daily habit (social media); 10-20% = weekly tool
- Normalizes for growth: controls for base size changes
- Benchmarkable across products of vastly different scale

### Visualization (canvas `c20`, 720×200)

Bar chart of DAU/MAU stickiness across product categories with a daily-habit threshold line.

- **Title (bold 17px `#1a5276`):** "DAU/MAU Ratio: Stickiness Across Products".
- **Data:** Social Media 62% green `#27ae60`; Messaging 55% green; Streaming 28% amber `#f39c12`; E-commerce 15% orange `#e67e22`; B2B SaaS 12% orange; Travel 5% red `#e74c3c`. Chart area x 80–680, y 50–155, y scale 0–80% with gridlines `#ddd` and 11px labels "0%–80%" in 20% steps.
- **Threshold:** dashed green line (dash 4/3) at 50%, labeled green 11px "50%+ = daily habit".
- **Labels:** ratio % bold 11px above each bar, product name 10px below (two lines where needed).
- **Caption (`#1a5276` 12px):** "Higher = users come back more often = stickier product".

## Regeneration instructions

- **Layout:** detail-page `.obj-table`: full-width table, `border-collapse: collapse`; `<thead>` row with two `<th>` cells "Metric & Rationale" (width 40%) / "Visualization" (width 60%) — `#1a5276` background, white text, padding 10px 14px, border `1px solid #2980b9`; one `<tr>` per metric; left `<td>` holds `<h3>` (numbered "N. Title", 1.05em `#1a5276`) plus a `<ul>` of 4 bullets (0.93em, line-height 1.6, padding-left 18px); right `<td>` holds only the canvas. Cell borders `1px solid #2980b9`, padding 14px, `vertical-align: top`; even rows `td` background `#f0f6fb`.
- **Page style:** body system sans-serif (-apple-system stack), background `#fafafa`, padding 20px 10px; h1 `#1a5276`; h2 subtitle `#1a5276`, weight 400, 1.1em. No nav bar, no back/home links.
- **Canvas:** all canvases 720×200, `display: block`; a shared `setup(id)` helper sizes the backing store by `window.devicePixelRatio` to 720×200px, and calls `ctx.scale` so drawing stays in logical coordinates, and sets default font `17px -apple-system, sans-serif`. All charts drawn in per-chart IIFEs at the bottom of the page.
- **Chart title convention:** every canvas starts with a bold 17px `#1a5276` title near (40, 22).
- **Palette:** primary blue `#1a5276`, chart blue `#2980b9`, light blues `#aed6f1`/`#d5e8f7`, green `#27ae60`, red `#e74c3c`, light red `#f5b7b1`, orange `#e67e22`, amber `#f39c12`, grays `#555`/`#333`/`#bbb`/`#ddd`/`#95a5a6`.
