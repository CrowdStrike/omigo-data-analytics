# The App-Store Tax

**Page type:** detail page (h2 section per pitfall, each with a two-column obj-table row: text left 50%, canvas right 50%)
**HTML title tag:** 160. The App-Store Tax

**Subtitle:** The same subscription costs more inside a mobile app than on the web, and the difference is platform commission — not product value. Every cross-channel revenue, margin, and elasticity number is confounded by that rent.

## Callout (philosophy box)

**The fundamental problem:** A commission of roughly 30% on in-app purchases drives a wedge between what the customer pays and what the vendor receives. The wedge varies by channel, by developer size, and by subscriber age — so price, revenue, and margin all move for reasons that have nothing to do with the product or the customer. Treat channel as a confounder, not as a segment.

**Illustrative Example.** Every price, subscriber count, and rate below is constructed for arithmetic clarity. The ~30% standard commission and the existence of reduced tiers are matters of public record; the specific figures are not measurements of any real vendor.

## Gross Revenue and Received Revenue Are Two Different Lines

**A 30% Commission Splits One Metric Into Two That Differ by 42.9%**

- **The setup:** Vendor A sells a subscription at $10.00/month in-app and has 100,000 in-app subscribers.
- **Gross line:** 100,000 × $10.00 = $1,000,000 — the number the store's sales report shows.
- **Received line:** at a 30% commission the payout is $1,000,000 × 0.70 = $700,000, and $300,000 never arrives.
- **The two ARPUs:** gross ARPU = $1,000,000 / 100,000 = $10.00; net ARPU = $700,000 / 100,000 = $7.00.
- **The error factor:** $10.00 / $7.00 = 1.4286, so gross ARPU overstates received ARPU by 42.9%, not by 30%.
- **Why not 30%:** 30% is the share of gross removed; 42.9% is that same gap measured against the smaller base.
- **Where it leaks:** dashboards pull gross from the store feed and cost from the ledger, mixing the two bases.
- **Minimum hygiene:** label every revenue column gross or net, and never let one chart contain both unlabeled.

### Visualization (canvas `c1`, 720×340)

Stacked-bar diagram splitting one gross revenue bar into payout and commission, with both ARPUs printed. All figures computed in JS from `RATE = 0.30`, `PRICE = 10.00`, `SUBS = 100000`.

- **Title (bold 17px `#1a5276`, centered, y=22):** "One Product, Two Revenue Lines" — subtitle line (15px `#555`, y=44): "Illustrative Example — 100,000 in-app subs at $10.00/mo".
- **Gross bar:** label "Gross (store sales report)" in `#333` at x=50, y=76. Rect (50,84) 620×34 in `rgba(26,82,118,0.35)`, centered white-ish label in `#1a5276`: "$1,000,000" (computed `SUBS*PRICE`).
- **Split bar:** label "What Vendor A receives" at x=50, y=152. Payout rect (50,160) 434×34 in `#27ae60` (width = 620×0.70), centered label "$700,000 payout (70%)" in white. Commission rect (484,160) 186×34 in `#e74c3c` (width = 620×0.30), centered label "$300,000 (30%)" in white.
- **ARPU pair (y=228 and y=252, 17px, left aligned at x=50):** "Gross ARPU = $1,000,000 / 100,000 = $10.00" in `#1a5276`; "Net ARPU = $700,000 / 100,000 = $7.00" in `#27ae60`.
- **Bottom bold red (`#e74c3c`, centered, y=292):** "$10.00 / $7.00 = 1.429 → gross overstates by 42.9%, not 30%".
- **Bottom gray (`#555`, centered, y=316):** "A 30% cut of gross is a 42.9% inflation of net. Different base, different number."

## Adding 30% to the Web Price Does Not Recover the 30% Commission

**To Net $7.00 Through the Store You Must Charge $10.00, Not $9.10**

- **The target:** Vendor A nets $7.00 per subscriber on the web and wants the same $7.00 through the store.
- **The correct price:** solve P × (1 − 0.30) = $7.00 → P = $7.00 / 0.70 = $10.00 exactly.
- **The required markup:** $10.00 / $7.00 − 1 = 42.857%, because 30% is a margin, not a markup.
- **The common error:** "the store takes 30%, so add 30%" gives $7.00 × 1.30 = $9.10 as the store price.
- **What $9.10 nets:** $9.10 × 0.70 = $6.37, which is $0.63 short of the $7.00 target on every subscriber.
- **The shortfall identity:** the gap is exactly rate² × target = 0.30² × $7.00 = 0.09 × $7.00 = $0.63.
- **At scale:** 10,000 store subscribers × $0.63 = $6,300 of margin lost per month to one arithmetic slip.
- **The general rule:** gross-up divides by (1 − rate); it never multiplies by (1 + rate).

### Visualization (canvas `c2`, 720×360)

Two side-by-side price columns — correct gross-up vs naive markup — each showing the payout split against a dashed $7.00 target line. All values computed in JS from `RATE` and `TARGET = 7.00`.

- **Title (bold 17px `#1a5276`, centered, y=22):** "Markup vs Margin: $10.00 Nets $7.00, $9.10 Nets $6.37" — subtitle (15px `#555`, y=44): "Illustrative Example — target net $7.00, commission 30%".
- **Scale:** $1.00 = 20px of bar height; baseline y=280.
- **Left column (correct, x=140 width 110):** total height $10.00 → 200px, rect (140,80) 110×200. Payout sub-rect (140,140) 110×140 in `#27ae60` ($7.00); commission sub-rect (140,80) 110×60 in `#e74c3c` ($3.00). Labels: "$10.00 charged" bold `#1a5276` centered above at y=70; "= $7.00 / 0.70" 15px `#555` at y=302; "nets $7.00 ✓" bold `#27ae60` at y=324.
- **Right column (naive, x=460 width 110):** total $9.10 → 182px, rect (460,98) 110×182. Payout sub-rect (460,152.6) 110×127.4 in `rgba(39,174,96,0.45)` ($6.37); commission sub-rect (460,98) 110×54.6 in `#e74c3c` ($2.73). Labels: "$9.10 charged" bold `#1a5276` at y=88; "= $7.00 × 1.30" 15px `#555` at y=302; "nets $6.37 ✗" bold `#e74c3c` at y=324.
- **Target line:** dashed (6/4) `#1a5276` 2px horizontal line at y=140 (the $7.00 level above baseline 280) from x=90 to x=620, labeled "$7.00 net target" in `#1a5276` 15px, right aligned at x=628, y=134.
- **Shortfall bracket:** red 2px vertical line at x=590 from y=140 to y=152.6, with label "$0.63 short" bold `#e74c3c` 15px at x=598, y=150.
- **Bottom bold red (centered, y=348):** "Shortfall = rate² × target = 0.09 × $7.00 = $0.63 per subscriber, every month".

## A Cross-Channel Price Gap Is Rent, Not Willingness to Pay

**42.9% Higher In-App, Identical Product — the Fitted Elasticity of −1.13 Is Fiction**

- **The observation:** the same subscription lists at $7.00 on the web and $10.00 in-app, a 42.9% gap.
- **The temptation:** two prices and two quantities look like a demand curve waiting to be fitted.
- **The naive fit:** 6,000 web subs at $7.00 and 4,000 store subs at $10.00 gives an arc elasticity of −1.13.
- **The arithmetic:** %ΔQ = −2,000 / 5,000 = −40.0%; %ΔP = $3.00 / $8.50 = +35.3%; −40.0 / 35.3 = −1.13.
- **Why it is invalid:** the price gap is set by the commission, and the quantity split by where users happen to buy.
- **No exogenous variation:** nothing about customer taste generated either number, so the ratio estimates nothing.
- **The counterfactual test:** at a 15% commission, net parity needs $7.00 / 0.85 = $8.24, a 17.6% price drop.
- **The tell:** demand would move with the product unchanged — proof the "elasticity" was measuring policy.

### Visualization (canvas `c3`, 720×360)

Two-point scatter with a fitted line through it, annotated as an artifact, plus the commission-change counterfactual point. Elasticity computed in JS from the two plotted points, never hardcoded.

- **Title (bold 17px `#1a5276`, centered, y=22):** "Two Prices, One Product: A Line Through Rent" — subtitle (15px `#555`, y=44): "Illustrative Example — price set by commission, not by demand".
- **Axes:** x = price $6.00-$11.00 mapped to px 90-660; y = subscribers 3,000-7,000 mapped to py 290-80. Axis lines `#333` 1px. x ticks at $7, $8, $9, $10, $11; y ticks at 3K, 4K, 5K, 6K, 7K. Axis labels "Price charged" (centered, y=330) and "Subscribers" (rotated, x=30).
- **Points:** web point at ($7.00, 6,000) as a 7px `#27ae60` dot labeled "Web $7.00 — 6,000 subs" to its right in `#27ae60`; store point at ($10.00, 4,000) as a 7px `#e74c3c` dot labeled "In-app $10.00 — 4,000 subs" in `#e74c3c`.
- **Fitted line:** solid `#1a5276` 2px line through the two points, extended to the plot edges. Label near its midpoint in `#1a5276`: "arc elasticity = −1.13" (computed at render time as `((4000-6000)/5000)/((10-7)/8.5)`, formatted with a true minus sign).
- **Artifact banner:** rect behind the line label in `rgba(231,76,60,0.10)`, and bold red text "NOT a demand curve" at the midpoint, offset 20px below.
- **Counterfactual marker:** hollow `#e67e22` circle (radius 7, 2px stroke) at ($8.24, 4,000) with a dashed `#e67e22` arrow from the store point to it, labeled "Commission → 15%: $8.24, same product" in `#e67e22` 15px.
- **Bottom gray (`#555`, centered, y=350):** "$10.00 = $7.00 / 0.70 and $8.24 = $7.00 / 0.85 — both prices are policy, so the slope between them is too."

## Channel Mix Shifts Move Blended Margin With Nothing Else Changing

**Same $100,000 Gross, Same $10.00 Price, Margin Falls 88.0% → 79.0%**

- **The setup:** Vendor B charges one global price, $10.00/month, on the web and in-app alike.
- **Per-channel economics:** web nets $10.00; in-app nets $10.00 × 0.70 = $7.00. Neither changes across quarters.
- **Quarter A mix:** 6,000 web + 4,000 in-app → net = $60,000 + $28,000 = $88,000 on $100,000 gross.
- **Quarter B mix:** 3,000 web + 7,000 in-app → net = $30,000 + $49,000 = $79,000 on the same $100,000 gross.
- **Gross is flat:** 10,000 subs × $10.00 = $100,000 in both quarters, so the top line shows nothing at all.
- **Margin moves anyway:** $88,000/$100,000 = 88.0% falls to $79,000/$100,000 = 79.0%, a 9.0 pp drop.
- **The whole effect:** 3,000 subs migrating × $3.00 commission = $9,000, which is the entire net decline.
- **The false diagnosis:** "margin compression" invites a pricing or cost investigation; the cause is mix.
- **The fix:** report net margin per channel and mix weights separately, never a blended rate alone.

### Visualization (canvas `c4`, 720×360)

Two paired stacked bars (Quarter A, Quarter B) showing identical gross with different payout/commission splits, plus a blended-margin readout. All values computed in JS from `RATE`, `PRICE = 10.00`, and the two mix pairs.

- **Title (bold 17px `#1a5276`, centered, y=22):** "Identical Gross, Different Blended Margin" — subtitle (15px `#555`, y=44): "Illustrative Example — one price, mix shifts 6K/4K → 3K/7K".
- **Scale:** $100,000 = 460px of bar width; bars 46px tall.
- **Quarter A (y=90):** label "Quarter A — 6,000 web / 4,000 in-app" in `#333` at x=50, y=82. Web segment rect (50,90) 276×46 in `#27ae60` ($60,000) labeled "web $60,000"; in-app payout rect (326,90) 128.8×46 in `rgba(39,174,96,0.45)` ($28,000) labeled "$28,000"; commission rect (454.8,90) 55.2×46 in `#e74c3c` ($12,000) labeled "$12K lost". Right of bar at x=520, y=120: "net $88,000 = 88.0%" bold `#1a5276`.
- **Quarter B (y=190):** label "Quarter B — 3,000 web / 7,000 in-app" at x=50, y=182. Web rect (50,190) 138×46 in `#27ae60` ($30,000) labeled "web $30,000"; in-app payout rect (188,190) 225.4×46 in `rgba(39,174,96,0.45)` ($49,000) labeled "$49,000"; commission rect (413.4,190) 96.6×46 in `#e74c3c` ($21,000) labeled "$21K lost". Right of bar at x=520, y=220: "net $79,000 = 79.0%" bold `#e74c3c`.
- **Gross bracket:** thin `#1a5276` line spanning x=50 to x=510 above Quarter A at y=70 with end ticks, labeled centered "gross $100,000 — identical in both quarters" in `#1a5276` 15px at y=64.
- **Bottom bold red (centered, y=300):** "Blended margin 88.0% → 79.0% = 9.0 pp, with zero change in either channel".
- **Bottom gray (`#555`, centered, y=326):** "3,000 subs × $3.00 commission = $9,000 — the entire net decline is the mix shift."

## Commission Tiers Make the Effective Rate a Function of Account Age

**Same Price, Same Mix — Effective Rate Drops 24.75% → 19.50% on Cohort Aging Alone**

- **Two rates, not one:** the headline 30% applies to new subscriptions; a reduced 15% applies after year one.
- **Vendor B's store book:** 10,000 in-app subs at $10.00 = $100,000 gross in both snapshots below.
- **Year-1 snapshot:** 6,500 subs under 12 months, 3,500 past 12 months, so two rates apply at once.
- **Year-1 commission:** 6,500 × $3.00 + 3,500 × $1.50 = $19,500 + $5,250 = $24,750 → 24.75% effective.
- **Year-2 snapshot:** the book has aged to 3,000 under 12 months and 7,000 past 12 months.
- **Year-2 commission:** 3,000 × $3.00 + 7,000 × $1.50 = $9,000 + $10,500 = $19,500 → 19.50% effective.
- **The move:** 5.25 pp of rate improvement from cohort aging, with price, product, and mix all fixed.
- **The forecasting error:** a flat 30% predicts $70,000 net against an actual $80,500, low by $10,500.
- **Also size-dependent:** small-developer programs cut the rate further, so the rate depends on the account too.
- **The fix:** compute effective rate per cohort per period; never store a single commission constant.

### Visualization (canvas `c5`, 720×360)

Grouped bars comparing the two snapshots' commission composition, with the flat-30% forecast drawn as a dashed reference. All values computed in JS from `RATE`, `RATE2 = 0.15`, `PRICE = 10.00` and the cohort counts.

- **Title (bold 17px `#1a5276`, centered, y=22):** "Effective Rate Is a Cohort Statistic, Not a Constant" — subtitle (15px `#555`, y=44): "Illustrative Example — 10,000 in-app subs at $10.00, same $100,000 gross".
- **Left panel (Year-1 snapshot, x=90 width 200):** two stacked segments scaled at $1,000 = 0.008px… use $25,000 = 200px of height, baseline y=280. New-cohort segment rect (90,124) 200×156 in `#e74c3c` ($19,500) labeled "6,500 × $3.00 = $19,500" in white 15px; aged-cohort segment rect (90,82) 200×42 in `#e67e22` ($5,250) labeled "3,500 × $1.50 = $5,250" in white 15px. Below: "Year 1" bold `#1a5276` centered at y=302; "effective 24.75%" bold `#e74c3c` at y=324.
- **Right panel (Year-2 snapshot, x=430 width 200):** new-cohort rect (430,208) 200×72 in `#e74c3c` ($9,000) labeled "3,000 × $3.00 = $9,000"; aged-cohort rect (430,124) 200×84 in `#e67e22` ($10,500) labeled "7,000 × $1.50 = $10,500". Below: "Year 2" bold `#1a5276` at y=302; "effective 19.50%" bold `#27ae60` at y=324.
- **Flat-rate reference:** dashed (6/4) `#1a5276` 2px horizontal line at the $30,000 level (y = 280 − 30000/25000×200 = 40) spanning x=70 to x=650, labeled "flat 30% assumption = $30,000" in `#1a5276` 15px, left aligned at x=70, y=34.
- **Y-axis ticks (15px `#555`, right aligned at x=64):** $0 at y=280, $10,000 at y=200, $20,000 at y=120, $30,000 at y=40.
- **Bottom bold red (centered, y=348):** "Flat 30% forecasts $70,000 net; actual is $80,500 — a $10,500 miss from aging alone".

## Discovery and Purchase Happen on Different Channels

**In-App Purchases Credit 300 Signups; 520 of 1,000 Were App-Influenced**

- **The month:** Vendor A adds 1,000 new subscribers — 300 paid in-app, 700 checked out on the web.
- **The hidden overlap:** of those 700 web checkouts, 220 first encountered the product inside the app.
- **True app influence:** 300 + 220 = 520 of 1,000 signups, or 52.0%, touched the app before converting.
- **What the store shows:** 300 of 1,000 = 30.0%, because a store can only report purchases it processed.
- **The undercount:** 300 / 520 = 57.7% of app-influenced signups get credited; 42.3% are invisible.
- **The CAC distortion:** $6,000 of app-channel spend reads as $6,000/300 = $20.00 against a true $11.54.
- **Why it worsens:** the cheaper web price actively pushes discovery and purchase onto different channels.
- **The structural gap:** no identifier survives an app-to-browser hop, so the join is an estimate, not a fact.
- **The honest report:** state purchase-channel counts and influence estimates as separate, labeled quantities.

### Visualization (canvas `c6`, 720×340)

Sankey-style flow diagram from discovery channel to purchase channel, with the crediting boundary drawn. All figures computed in JS from the four flow counts.

- **Title (bold 17px `#1a5276`, centered, y=22):** "Discovery ≠ Purchase: 520 App-Influenced, 300 Credited" — subtitle (15px `#555`, y=44): "Illustrative Example — 1,000 new subscribers in one month".
- **Left nodes (discovery, x=60 width 90):** left node label "Discovered in app" above the rect; (60,80) 90×104 in `rgba(26,82,118,0.35)` (520 of 1,000 → 104px at 0.2px per sub), labeled "520" bold `#1a5276` centered; "Discovered on web" (single line below the rect) rect (60,204) 90×96 in `rgba(26,82,118,0.18)` (480), labeled "480".
- **Right nodes (purchase, x=560 width 90):** "Paid in-app" rect (560,80) 90×60 in `#e74c3c` (300), labeled "300" white; "Paid on web" rect (560,160) 90×140 in `#27ae60` (700), labeled "700" white.
- **Flows (bezier-edged bands, left node edge to right node edge):** app→in-app 300 in `rgba(231,76,60,0.30)`; app→web 220 in `rgba(230,126,34,0.35)`; web→web 480 in `rgba(39,174,96,0.22)`. Flow counts printed at each flow's midpoint in 15px matching colors: "300", "220 — app-influenced, web-paid", "480".
- **Crediting boundary:** dashed (5/5) `#e74c3c` 2px vertical line at x=470 from y=70 to y=310, labeled rotated or short-form "store credit stops here" in `#e74c3c` 15px above at y=64.
- **Bottom bold orange (`#e67e22`, centered, y=326):** "300 / 520 = 57.7% credited → 42.3% of app influence never appears in any store report".

## LTV on Gross Revenue Turns a Losing Cohort Into a Winning One

**LTV/CAC = 1.47 on Gross, 1.03 on Net, 0.85 on Contribution**

- **The cohort:** in-app subscribers at $10.00/month, average tenure 14 months, CAC $95.00, service cost $1.20/mo.
- **Gross LTV:** 14 × $10.00 = $140.00, giving $140.00 / $95.00 = 1.47 and an apparently healthy cohort.
- **Net LTV:** 14 × $7.00 = $98.00 after commission, so the ratio falls to $98.00 / $95.00 = 1.03.
- **Contribution LTV:** 14 × ($7.00 − $1.20) = 14 × $5.80 = $81.20, and $81.20 / $95.00 = 0.85.
- **Sign flip:** the same cohort reads 47% above break-even on gross and 15% below it on contribution.
- **Payback on gross:** $95.00 / $10.00 = 9.5 months, comfortably inside the 14-month tenure.
- **Payback on contribution:** $95.00 / $5.80 = 16.4 months, which is 2.4 months longer than the average sub lasts.
- **The consequence:** the cohort never repays acquisition, yet a gross-LTV dashboard signals "spend more".
- **The rule:** LTV must use the cash the vendor actually receives, minus the cost of serving it, and nothing else.

### Visualization (canvas `c7`, 720×360)

Three descending LTV bars against a CAC threshold line, with the ratio printed under each. All values computed in JS from `PRICE`, `RATE`, `TENURE = 14`, `COST = 1.20`, `CAC = 95.00`.

- **Title (bold 17px `#1a5276`, centered, y=22):** "Three LTVs for One Cohort: 1.47, 1.03, 0.85" — subtitle (15px `#555`, y=44): "Illustrative Example — $10.00/mo, 14-month tenure, CAC $95.00".
- **Scale:** $1.00 = 1.5px of bar height; baseline y=290.
- **Bars (width 120, gap 80, first at x=110):** Gross LTV $140.00 → 210px, rect (110,80) in `rgba(26,82,118,0.35)`; Net LTV $98.00 → 147px, rect (310,143) in `#e67e22`; Contribution LTV $81.20 → 121.8px, rect (510,168.2) in `#e74c3c`.
- **Bar value labels (bold 17px, centered above each bar):** "$140.00" `#1a5276` at y=72; "$98.00" `#e67e22` at y=135; "$81.20" `#e74c3c` at y=160.
- **Bar captions (two lines under baseline, centered):** "Gross LTV" / "14 × $10.00" at y=310/330; "Net LTV" / "14 × $7.00" ; "Contribution LTV" / "14 × $5.80".
- **Ratio badges (bold 15px, inside each bar near its base):** "LTV/CAC 1.47" white on the gross bar; "1.03" white; "0.85" white.
- **CAC line:** solid `#27ae60` 2.5px horizontal line at the $95.00 level (y = 290 − 95×1.5 = 147.5) spanning x=80 to x=670, labeled "CAC = $95.00" bold `#27ae60` 15px right aligned at x=668, y=141.
- **Below-threshold shading:** `rgba(231,76,60,0.12)` rect from (510,147.5) to (630,290) marking the contribution bar's shortfall, with "$13.80 short" `#e74c3c` 15px at x=636, y=200.
- **Bottom bold red (centered, y=350):** "Payback: 9.5 months on gross, 16.4 months on contribution — longer than the 14-month tenure".

## Channel Choice Is Selected, Not Assigned

**Web Cohort Retains 68.0% vs 60.0% In-App — and the Within-Tier Gap Is Zero**

- **The observation:** 12-month retention is 68.0% for web-purchased subs and 60.0% for in-app subs.
- **The tempting conclusion:** "web buyers are more loyal, so steer everyone to web checkout."
- **What buying on the web requires:** leaving the app, finding the site, and re-entering payment details.
- **The selection:** that friction is only worth it to users who already intended to keep the subscription.
- **Split by intent — web:** 4,000 high-intent at 72% + 1,000 low-intent at 52% = 3,400 of 5,000 = 68.0%.
- **Split by intent — in-app:** 2,000 high-intent at 72% + 3,000 low-intent at 52% = 3,000 of 5,000 = 60.0%.
- **Within-tier gap:** 72% vs 72% and 52% vs 52% — the channel effect inside each tier is exactly 0.0 pp.
- **The verdict:** the entire 8.0 pp headline gap is composition; channel caused none of it.
- **Why steering backfires:** moving low-intent users to web moves the friction, not the intent, and the gap closes.

### Visualization (canvas `c8`, 720×360)

Two-panel retention comparison: aggregate bars showing an 8.0 pp gap, then intent-stratified bars showing 0.0 pp, with mix weights labeled. All rates and weighted aggregates computed in JS from the four cell counts and two tier rates.

- **Title (bold 17px `#1a5276`, centered, y=22):** "An 8.0 pp Channel Gap That Is Entirely Composition" — subtitle (15px `#555`, y=44): "Illustrative Example — 5,000 subs per channel, 12-month retention".
- **Left panel — Aggregate (heading "Aggregate" bold `#1a5276` 15px at x=60, y=76):** scale 1% = 2px, baseline y=290. Web bar rect (80,154) 70×136 in `#27ae60` (68.0%) labeled "68.0%" bold `#27ae60` above at y=146; in-app bar rect (180,170) 70×120 in `#e74c3c` (60.0%) labeled "60.0%" bold `#e74c3c` at y=162. Channel names "Web" and "In-app" in `#333` 15px centered at y=310. Bracket between bar tops with label "8.0 pp" bold `#e74c3c` 15px at x=150, y=124.
- **Right panel — Stratified (heading "Split by purchase intent" bold `#1a5276` 15px at x=380, y=76):** four bars width 52, starting x=390, gap 22, same scale and baseline. High-intent web rect at 72.0% → 144px in `#27ae60`; high-intent in-app 72.0% → 144px in `#e74c3c`; low-intent web 52.0% → 104px in `rgba(39,174,96,0.5)`; low-intent in-app 52.0% → 104px in `rgba(231,76,60,0.5)`. Value "72.0%" / "72.0%" / "52.0%" / "52.0%" bold 15px above each bar in its own color.
- **Mix weights (15px `#555`, two lines under the stratified bars at y=310 and y=328):** "High intent: web 4,000 / app 2,000" and "Low intent: web 1,000 / app 3,000".
- **Equality markers:** thin dashed (4/4) `#1a5276` horizontal lines across each matched pair at 72.0% (y=146) and 52.0% (y=186), each labeled "identical" in `#1a5276` 14px right aligned at x=668.
- **Bottom bold red (centered, y=352):** "Within-tier channel effect = 0.0 pp. The 8.0 pp aggregate gap is the intent mix, nothing else."

## Regeneration instructions

- **Layout:** detail page. h1 (no index number) + `.subtitle` + one `.philosophy` callout, then one unnumbered `<h2>` per pitfall followed by a one-row `.obj-table`: left `<td>` (50%) holds an `.obj-title` div — a refined restatement of the pitfall name carrying a concrete number from that section's own bullets, never a copy of the heading — plus a `<ul>` of labeled bullets; right `<td>` (50%, centered) holds the canvas. Even table rows have background `#fafcfe`.
- **Page CSS:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.4em `#1a5276` with 2px `#2980b9` bottom border, padding-bottom 8px; subtitle `#666` 1.05em; ul 0.9em `#333`; `strong` `#1a5276`; `.obj-title` 1.05em weight 600 `#1a5276`; `.philosophy` background `#f0f4f8`, left border 4px solid `#2980b9`, padding 12px 16px, 0.9em. Table cell borders `1px solid #e0e0e0`, padding 20px 24px. `td:first-child` and `td:last-child` are both 50% — shrink a chart via the canvas `style.maxWidth`, never by narrowing the cell. No nav bar, no back/home links, no cross-page links, no `thead`, no status badges.
- **Canvas:** intrinsic `width`/`height` attributes as given per chart (all 720 wide, heights 340-360); a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Single source of truth for the commission arithmetic:** the script declares `RATE = 0.30`, `RATE2 = 0.15`, `PRICE = 10.00`, `WEB = 7.00`, `TENURE = 14`, `COST = 1.20`, `CAC = 95.00` once, and every printed dollar figure, percentage, ratio, and bar dimension is derived from those constants at render time. No commission figure is typed as a literal in a label. Helpers: `money(v)` formats to two decimals with a `$` and thousands separators; `pct(v, digits)` formats a fraction as a percentage.
- **No `Math.random()` anywhere on this page.** All data is either a hardcoded literal count (subscriber counts, cohort sizes, retention rates) or computed from the constants above. If a future edit needs generated data, add an inline seeded generator per chart: `function lcg(seed){var s=seed;return function(){s=(s*16807)%2147483647;return s/2147483647;};}`.
- **Palette:** primary blue `#1a5276` / `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`, gray text `#555`/`#333`.
- **Naming:** no real company or product names. Vendors are "Vendor A" / "Vendor B", platforms are "the store" / "the major mobile app stores", people are Alice/Bob. The ~30% standard rate and reduced tiers are stated generically. Every constructed figure is labeled "Illustrative Example".
- Chart ids are sequential in page order, `c1` through `c8`.
