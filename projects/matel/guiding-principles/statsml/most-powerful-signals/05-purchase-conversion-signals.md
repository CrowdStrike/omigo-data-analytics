# Purchase & Conversion Signals

**Page type:** detail page (most-powerful-signals compact style: per-section two-column layout table, text left 45% with tag pills / labeled bullets / example / key-point, canvas right 55%)
**HTML title tag:** Purchase & Conversion Signals

**Subtitle:** What people actually spend money on is the highest-intent behavioral signal — everything else is a proxy for it

## Revealed Preference: Wallets Beat Surveys

**Tags:** `signal` (blue), `bias` (orange), `best practice` (green)

- **Intent gap** — stated intent overshoots purchase 3-5x
- **Amazon ranking** — purchase-per-impression, not ratings
- **Netflix pivot** — stars replaced by completion and re-watch
- **Constraint caveat** — spending reflects budget too, not pure desire
- **Tiebreak rule** — survey vs transaction: trust the transaction

*Netflix users rated documentaries five stars, then watched sitcoms — so stars were retired.*

**Key point:** The say-spend gap is systematic, not noise — purchase data is the only self-correcting label.

### Visualization (canvas `c1`, 720×300)

Paired bar chart: stated preference vs actual purchase share per category.

- **Title (bold 14px `#1a5276`, top center):** "Stated Preference (survey) vs Purchase Share (wallet)".
- **Data:** categories `['Organic', 'Eco brand', 'Premium', 'Cheapest']`; stated `[68, 55, 40, 22]`; actual `[12, 8, 15, 61]`; y max 70. Padding top 50 / bottom 60 / left 55 / right 150; gray `#eee` gridlines with `#999` labels at 0/20/40/60%.
- **Bars (38px wide, paired per group):** stated fill `rgba(26,82,118,0.35)` stroke `#1a5276`; actual solid `#27ae60`. Bold 10px percent labels above each bar in the bar color; category labels 12px `#444` below.
- **Legend (right):** blue-tint swatch "Say (survey)", green swatch "Spend (wallet)".
- **Caption (bottom center, bold 11px `#e67e22`):** "Stated intent overshoots actual spend 3-5x — the wallet votes the other way".

## RFM Segmentation: Three Numbers, Still Competitive

**Tags:** `rule of thumb` (blue), `best practice` (green), `trade-off` (orange)

- **Pure signal** — all three axes derive from purchases directly
- **Quintile scoring** — score 1-5 per axis; "555" champions get retention
- **Win-back zone** — high-monetary, low-recency segments get offers
- **Blind spot** — ignores product margin and category
- **Baseline rule** — any fancy model must beat RFM first

*The 1960s catalog-mailing grid, one SQL query today, still lands within a few lift points of tuned XGBoost.*

**Key point:** RFM works because it never leaves the money — every added feature is a step away from the signal defining the outcome.

### Visualization (canvas `c2`, 720×300)

5×5 heatmap: repeat-purchase rate by Recency × Frequency.

- **Title:** "RFM Grid — Repeat-Purchase Rate by Recency x Frequency".
- **Grid values (rows = recency 5→1 top to bottom, cols = frequency 1→5):**
  - R=5: `[0.18, 0.29, 0.41, 0.55, 0.72]`
  - R=4: `[0.13, 0.22, 0.33, 0.45, 0.60]`
  - R=3: `[0.09, 0.15, 0.24, 0.34, 0.47]`
  - R=2: `[0.05, 0.09, 0.15, 0.22, 0.31]`
  - R=1: `[0.02, 0.04, 0.07, 0.11, 0.16]`
  Cell fill `rgba(26,82,118, 0.06 + value*0.9)`; percent text 11px, white when value > 0.4, else `#2c3e50`. Padding top 44 / bottom 52 / left 130 / right 130.
- **Axis labels (11px `#444`):** left "R=5 recent" (top) and "R=1 lapsed" (bottom); below "F=1 (one buy)" (left) and "F=5 (5+ buys)" (right).
- **Corner callouts (bold 12px, right of grid):** green `#27ae60` "\"555\" champions" (top), orange `#e67e22` "win-back zone" (middle), red `#e74c3c` "lost" (bottom).
- **Caption (bottom center, bold 11px `#1a5276`):** "Three purchase-derived scores — still the CRM baseline any model must beat".

## Basket Analysis & Co-purchase Graphs

**Tags:** `mechanism` (blue), `best practice` (green), `failure mode` (red)

- **Use lift** — P(A,B)/[P(A)P(B)] > 1, not raw counts
- **Minimum support** — ~0.1% of baskets, else huge meaningless lift
- **Not causal** — diapers and beer share a shopper, not mechanism
- **Repurchase cycle** — metadata separates complements from one-and-done items
- **Bundling risk** — correlation-based discounts subsidize sure buyers

*Amazon's "Frequently bought together" surfaces phone-case-charger because their mutual lift dwarfs everything else.*

**Failure mode:** Ranking by raw co-occurrence recommends milk next to everything — statistically true, commercially worthless.

### Visualization (canvas `c3`, 720×300)

Node-and-edge co-purchase graph with lift-weighted edges and one flagged spurious edge.

- **Title:** "Co-purchase Graph — Edge Width = Lift, Clusters = Shopping Missions".
- **Nodes (18px circles, fill `rgba(26,82,118,0.15)`, stroke `#1a5276`, bold 10px labels):** Phone (130,130), Case (260,80), Charger (260,185), Milk (470,85), Bread (600,140), Cereal (470,195).
- **Edges (blue `#1a5276`, line width = lift, lift value labeled 11px at midpoint):** phone–case 4.8x, phone–charger 3.2x, case–charger 2.1x, milk–bread 1.9x, milk–cereal 2.6x, bread–cereal 1.4x; charger–cereal 1.0x drawn as dashed red `#e74c3c` (spurious, lift ≤ 1.2).
- **Cluster labels (bold 11px green `#27ae60`):** "new-phone kit" under the left cluster, "weekly grocery run" under the right cluster.
- **Spurious annotation (bold 11px red):** "lift ≈ 1: co-occurs, no real affinity".
- **Caption (bottom center, bold 11px `#1a5276`):** "Rank pairs by lift with minimum support — raw co-occurrence just rediscovers popularity".

## Repeat Purchase: The Second Buy Predicts Everything

**Tags:** `signal` (blue), `best practice` (green), `failure mode` (red)

- **90-day rule** — second buy within 90 days: 2-3x retention
- **Reorder cycles** — timed reminders beat generic promotion; Amazon Subscribe & Save locks them in
- **Instacart reorders** — "buy it again" drives large share of carts
- **Cart-abandon emails** — recover roughly 5-10% of abandoned carts
- **Discount trap** — deep discounts inflate first buys, kill repeat rate

*A 40%-off flash-sale cohort repeats at a third of the full-price cohort's rate — transactions bought, not customers.*

**Failure mode:** Blended repeat rate hides new-cohort decay — track repeat rate by acquisition cohort.

### Visualization (canvas `c4`, 720×300)

Two retention curves over 12 months split by time to second purchase.

- **Title:** "12-Month Retention by Time to Second Purchase".
- **Data (months 0–12):** fast cohort green `#27ae60` `[100, 84, 74, 68, 63, 60, 57, 55, 53, 52, 51, 50, 49]`; slow cohort red `#e74c3c` `[100, 62, 44, 34, 28, 24, 21, 19, 18, 17, 16, 15, 15]`; y 0–100% with `#eee` gridlines every 25%. Padding top 50 / bottom 56 / left 60 / right 170. X label "months since first purchase".
- **Gap bracket:** orange `#e67e22` bracket at month 12 between the two endpoints, labeled bold 12px "~3x gap".
- **Legend (right):** green swatch "2nd buy < 90 days", red swatch "no 2nd buy in 90d".
- **Caption (bottom center, bold 11px `#1a5276`):** "The second purchase, not the first, separates customers from transactions".

## LTV Is Dominated by Whales

**Tags:** `signal` (blue), `bias` (orange), `defense` (green)

- **Concentration** — top ~2% of game payers drive 40%+ revenue
- **RMSE trap** — squared-error LTV models chase whales, ignore median
- **Robust reporting** — median and P90 alongside mean; winsorize experiments
- **Leave-one-out** — if one user flips the test, it's a whale artifact
- **Lookalikes** — Meta/Google seeds on top-LTV buyers inherit tail instability

*A +8% revenue lift inverts to -2% after removing one $12,000 player — the test measured whale placement.*

**Key point:** Model spend two-stage — P(buy) x spend given buy (BG/NBD + Gamma-Gamma) — or model quantiles directly.

### Visualization (canvas `c5`, 720×300)

Pareto/Lorenz-style curve: cumulative revenue vs customers ranked by spend.

- **Title:** "Cumulative Revenue vs Customers Ranked by Spend".
- **Curve:** blue `#1a5276` width 3, plotting revenue share = p^0.22 for customer fraction p in [0,1]; padding top 50 / bottom 56 / left 60 / right 40; y labels "100%" top, "0%" bottom; x label "% of customers (ranked by spend, highest first)".
- **Reference:** dashed gray `#ccc` diagonal labeled "uniform spend".
- **Markers (dot + dashed guide lines to both axes, bold 12px label):** orange `#e67e22` at p=0.02: "top 2% (whales) ≈ 42% of revenue"; red `#e74c3c` at p=0.10: "top 10% ≈ 60% of revenue".
- **Caption (bottom center, bold 11px `#1a5276`):** "Mean LTV rides on a handful of accounts — report median and P90, cap experiment metrics".

## The Shape of Spend: Zero-Inflated + Long Tail

**Tags:** `mechanism` (blue), `failure mode` (red), `defense` (green)

- **Invalid t-tests** — tail dominates variance at typical sample sizes
- **Zeros persist** — log-transform can't fix them; use hurdle/zero-inflated models
- **Two-stage** — classifier for P(spend>0) times heavy-tail regressor
- **Value bidding** — ad platforms train this two-part structure per auction
- **Dropping zeros** — silently switches revenue-per-user to revenue-per-payer

*Filtering non-payers before averaging produced an "ARPU" describing 4% of users and a 25x forecast overshoot.*

**Failure mode:** Bell-curve tools on spike-plus-tail — one histogram before choosing the test prevents the whole error class.

### Visualization (canvas `c6`, 720×300)

Zero-inflated spend histogram: red spike at $0 plus a heavy blue right tail.

- **Title:** "Per-User Spend Distribution — Spike at Zero + Long Tail".
- **Bins (20):** `[96, 0, 8, 12, 10, 7, 5, 3.5, 2.5, 1.8, 1.3, 1.0, 0.8, 0.6, 0.5, 0.4, 0.3, 0.25, 0.2, 0.15]`, scale max 100 (empty bin 1 skipped). First bin solid red `#e74c3c`; the rest `rgba(26,82,118,0.35)` stroked `#1a5276`. Padding top 50 / bottom 56 / left 60 / right 30.
- **Annotations:** bold 12px red "non-payers: ~96% of users" (with pointer line to the spike); bold 12px blue "payers: heavy right tail"; bold 12px orange "t-test on raw spend: invalid — tail owns the variance"; bold 11px green "model as: P(spend > 0)  x  E[spend | spend > 0]".
- **X labels (12px `#444`):** "$0" left, "spend per user →" right.
- **Caption (bottom center, bold 11px `#1a5276`):** "Dropping the zeros changes the question from revenue per user to revenue per payer".

## The Sparsity Problem: Proxies Fill the Funnel

**Tags:** `mechanism` (blue), `trade-off` (orange), `best practice` (green)

- **Rare labels** — conversion is 1-3% of sessions; proxies give 5-30x positives
- **Quality cost** — add-to-cart abandonment runs ~70%
- **Platform loops** — Meta pixel/CAPI feed purchases back; Google smart bidding wants 30-50 conversions/month
- **Delayed feedback** — late purchases mislabel recent negatives; correct or window
- **Validation rule** — evaluate against actual purchase, never the proxy

*A model lifted add-to-cart 12% by surfacing cheap impulse items; revenue stayed flat.*

**Key point:** Proxies are a volume loan against label quality — the only acceptable final metric is settled purchases.

### Visualization (canvas `c7`, 720×300)

Centered funnel diagram (widths on log scale) with a label-quality column on the right.

- **Title:** "The Funnel: Volume Shrinks ~100x, Label Quality Rises".
- **Stages (name, volume, corr.-with-purchase, side note):**
  - View 100%, 0.05
  - Click 32%, 0.12
  - Add-to-cart 8%, 0.45 — note "~70% of carts abandoned" (red)
  - Checkout 2.5%, 0.80 — note "payment + shipping friction" (orange)
  - Purchase 1.2%, 1.00 — note "the anchor label" (green)
  - Repeat buy 0.4%, 1.00 — note "strongest LTV predictor" (green)
  Bar widths use relative log-scale fractions `[1.0, 0.835, 0.63, 0.466, 0.36, 0.2]` of 380px, centered at x=245; rows 26px tall with 9px gaps starting y=44. Money stages (Purchase, Repeat buy) filled `rgba(39,174,96,0.35)` stroked `#27ae60`; others `rgba(26,82,118,0.35)` stroked `#1a5276`. Stage names bold 11px `#1a5276` left; volume centered inside; notes 10px right.
- **Quality column (x≈590, green `#27ae60`):** header "corr. with" / "purchase", values 0.05–1.00 per row, plus an upward green arrow along the column.
- **Caption (bottom center, bold 11px `#1a5276`):** "Train upstream for volume, calibrate downstream to settled purchases".

## Price Response & Willingness to Pay

**Tags:** `signal` (blue), `bias` (orange), `gaming` (orange)

- **Dynamic pricing** — airlines, ride-sharing read live conversion-at-price
- **Tier flows** — SaaS upgrade/downgrade shows where value perception breaks
- **Price as feature** — full-price buyers retain far better than couponed
- **Confounding** — prices rise with demand; only randomized variation identifies elasticity
- **Deal-seekers** — separating them from incremental buyers requires holdouts

*Higher prices coincided with more completed rides only because everyone wanted a car at 2am on New Year's Eve.*

**How it's exploited:** Deal-seekers wait for coupons they didn't need, the model reads "discounts work" and discounts harder — a margin leak only a no-discount holdout exposes.

### Visualization (canvas `c8`, 720×300)

Demand curve and revenue curve from randomized price tests, with a revenue-max marker.

- **Title:** "Demand Curve from Conversion-at-Price (randomized tests)".
- **Data:** prices `[5, 10, 15, 20, 25, 30, 35, 40, 45, 50]`; conversion % `[9.0, 7.8, 6.6, 5.5, 4.4, 3.4, 2.5, 1.8, 1.2, 0.8]` (blue `#1a5276` line, width 3, 3.5px dots, scale max 10). Revenue per visitor = price × conversion, green `#27ae60` line scaled to 90% of chart height. Padding top 50 / bottom 56 / left 60 / right 165; x label "price".
- **Revenue-max marker:** orange `#e67e22` 5px dot with dashed drop line at the argmax price, labeled bold 12px "revenue max ≈ $25".
- **Legend (right):** blue swatch "conversion %", green swatch "revenue / visitor".
- **Caption (bottom center, bold 11px `#e74c3c`):** "Only randomized price variation traces this — observational data shows demand raising prices".

## Attribution Reshapes Budgets, Not Reality

**Tags:** `mechanism` (blue), `bias` (orange), `defense` (green)

- **Rule sensitivity** — switching rules (last-click, linear, time-decay, Shapley) swings channel ROI several-fold
- **Retargeting inflation** — last-click credits users already converting
- **Holdout evidence** — large studies found near-zero lift for "profitable" spend
- **Causal tools** — only incrementality tests (holdouts, geo, PSA) measure lift
- **Cadence** — attribution for daily steering, incrementality quarterly

*eBay paused brand-keyword ads and traffic simply shifted to the free organic link below.*

**Key point:** Attribution allocates credit, not causation — 100% "responsible" under the rule can be 0% in reality.

### Visualization (canvas `c9`, 720×300)

Paired bars: credit share by channel under two attribution rules.

- **Title:** "Credit Share by Channel — Same Conversions, Two Rules".
- **Data:** channels `['Display', 'Social', 'Email', 'Search', 'Retargeting']`; last-click `[3, 7, 12, 46, 32]` (solid red `#e74c3c`); multi-touch `[18, 22, 17, 28, 15]` (solid blue `#1a5276`); y max 50 with `#eee` gridlines every 10%. Padding top 50 / bottom 62 / left 55 / right 150; bar width 32. Bold 10px percent labels above bars in the bar color; channel names 12px `#444` below.
- **Swing callouts (bold 10px orange `#e67e22`, top):** "6x swing" over Display, "halved" over Retargeting.
- **Legend (right):** red swatch "last-click", blue swatch "multi-touch".
- **Caption (bottom center, bold 11px `#1a5276`):** "The rule, not the data, decides where budget flows — only holdouts measure causation".

## Gaming the Purchase Signal

**Tags:** `abuse` (red), `gaming` (orange), `defense` (green)

- **Brushing** — fake orders (even empty packages) inflate rank and "verified" reviews
- **Affiliate fraud** — cookie stuffing steals credit for organic purchases
- **Install fraud** — networks claim app conversions they never caused
- **Refund abuse** — buy-review-refund loops; net post-return revenue is honest
- **Detection** — order bursts vs flat traffic, refund spikes, address clustering

*Sellers on Amazon and Taobao ship empty envelopes to strangers, each a "verified purchase" enabling a fake review.*

**How it's exploited:** A fake sale buys ranking, reviews, and ad credit at once — a purchase is ground truth only after it settles post-refund, so weight by account age and payment risk.

### Visualization (canvas `c10`, 720×300)

Three time series over 20 days: flat traffic, an order spike, and a following refund wave.

- **Title:** "Fraud Signature: Order Spike Decoupled from Traffic".
- **Data (20 days, y max 60):**
  - traffic (dashed gray `#999`, dash 4/4, width 2): `[50, 52, 49, 51, 50, 53, 51, 50, 52, 51, 50, 49, 51, 52, 50, 51, 49, 50, 52, 51]`
  - orders (blue `#1a5276`, width 3): `[10, 11, 10, 12, 11, 10, 11, 34, 42, 45, 38, 14, 11, 10, 11, 10, 11, 10, 11, 10]`
  - refunds (red `#e74c3c`, width 3): `[2, 2, 3, 2, 2, 3, 2, 2, 3, 3, 4, 16, 22, 25, 19, 9, 4, 3, 2, 2]`
  Padding top 50 / bottom 56 / left 60 / right 150; x label "days".
- **Annotations:** bold 12px orange "brushing spike" over the order peak (with pointer line); bold 12px red "refund wave follows"; 11px gray "traffic: flat — no marketing, no virality".
- **Legend (right):** gray swatch "traffic", blue swatch "orders", red swatch "refunds".
- **Caption (bottom center, bold 11px `#1a5276`):** "A purchase is only ground truth once it survives the refund window and fraud scoring".

## Regeneration instructions

- **Layout:** one `.card-section` per section, each containing an `<h2>` (1.3rem `#1a5276`, bottom border `2px solid #2980b9`) and a `table.layout` with a single `<tr>`: left `td.text-col` (45%) holding `.tags` pills, `<ul>` bullets, `p.example`, `.key-point`; right `td.viz-col` (55%) with one `<canvas width="720" height="300">` styled `width:100%`, border `1px solid #e0e0e0`, radius 4px.
- **Page style:** body system-ui sans, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with bottom border `2px solid #2980b9`; `.subtitle` `#666` 0.95rem; `ul` 0.92rem; `li b` in `#1a5276`.
- **Tag pills:** `.tag` inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Key-point callout:** background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem. `.example` italic `#555` 0.9rem.
- **Canvas:** shared `setup(id)` helper scaling by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates); one IIFE per chart. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`.
- No nav bar, no back/home links. In regenerated HTML any links use `.html` extensions.
