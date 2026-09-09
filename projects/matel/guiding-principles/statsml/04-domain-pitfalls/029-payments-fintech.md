# Payments / Fintech — Domain Pitfalls

**Page type:** detail page (h2 section per pitfall, each with a two-column obj-table row: text left 50%, canvas right 50%)
**HTML title tag:** 29. Payments / Fintech — Domain Pitfalls

**Subtitle:** Data pitfalls unique to payment processing, fraud detection, and financial technology systems

## Callout (philosophy box)

Payment systems operate in adversarial environments where labels arrive late, legitimate behavior mimics fraud, and every intervention has direct revenue consequences. The fundamental tension: catching more fraud means blocking more legitimate customers.

## Chargeback Ground Truth Delay

**Transaction today, label arrives months later**

- Transaction occurs at Day 0
- Cardholder dispute filed 30-60 days later
- Resolution (true label) at 90-120 days
- Model evaluated at Day 30 sees only partial labels
- Transactions labeled "not fraud" may simply be "not yet disputed"

**Impact:** Model performance measured on incomplete labels appears optimistic. True fraud rate is systematically underestimated because many fraud cases haven't been reported yet at evaluation time.

### Visualization (canvas `c1`, 720×300)

Horizontal timeline diagram of chargeback label delay.

- **Title (bold `#1a5276`, left-aligned at x=60, y=30):** "Chargeback Ground Truth Timeline".
- **Timeline axis:** horizontal line at y=140 from x=60 to x=(width−40), stroke `#333`, width 2, filled arrowhead at right end. Day-to-x mapping is linear over 0–130 days.
- **Day markers** (tick lines ±10px at each day position; bold label in `#1a5276` at y=timeline+30, sublabel in `#555` at y=timeline+50):
  - Day 0 — "Transaction"
  - Day 30 — "Model Eval"
  - Day 60 — "Dispute Filed"
  - Day 120 — "Resolution"
- **Day 30 callout:** dashed red vertical line (`#e74c3c`, dash 4/3, width 2) from the tick up to y=timeline−60; above it, bold 15px red text "Model evaluated here" and 14px red text "(only partial labels available)".
- **Shaded region:** rectangle from Day 0 to Day 120, y=timeline+60, height 40, fill `rgba(231,76,60,0.1)` with 1px `#e74c3c` border; centered 14px red text inside: "Labels incomplete — \"not fraud\" just means \"not yet reported\"".
- **Day 120 callout:** dashed green vertical line (`#27ae60`, dash 4/3, width 2) from the tick up to y=timeline−50; above it, bold 15px green text "True label available".

## Legitimate High-Value ≈ Fraud Pattern

**Good customers look identical to fraudsters**

- Customer buys $5000 laptop: high amount, new shipping address, first purchase
- Stolen credit card: high amount, new shipping address, first purchase
- Feature vectors are identical for both cases
- Cannot distinguish the two cases without context from outside the transaction record itself
- Separation needs customer history, device fingerprint, and behavioral signals gathered elsewhere

**Impact:** Transaction-level features alone cannot separate legitimate high-value purchases from fraud. Blocking all high-risk-looking transactions means rejecting your best customers.

### Visualization (canvas `c2`, 720×300)

Side-by-side comparison of two identical feature-profile boxes.

- **Title (bold 17px `#1a5276`, centered, y=28):** "Transaction Feature Comparison".
- **Two boxes** 260×200 each, gap 60, horizontally centered, top y=50:
  - Left box: fill `#eafaf1`, border 2px `#27ae60`; header bold 16px `#27ae60`: "Legitimate Buyer".
  - Right box: fill `#fdedec`, border 2px `#e74c3c`; header bold 16px `#e74c3c`: "Stolen Card Fraud".
- **Feature bullets** (15px `#333`, identical in both boxes, one per 30px row starting 55px below box top):
  - "• Amount: $5,000"
  - "• New shipping address"
  - "• First purchase"
  - "• Electronics category"
  - "• Expedited shipping"
- **Between boxes:** large bold 40px "=" sign in `#e67e22`, vertically centered.
- **Bottom note (bold 15px `#e67e22`, centered, 30px below boxes):** "Identical feature vectors — model cannot distinguish".

## Cross-Border Currency + Timezone

**Same transaction, different amounts and dates**

- Purchase in EUR, settled in USD, reported in GBP
- Exchange rate at authorization vs. settlement vs. reporting differs
- Timezone of "transaction date" varies by system (merchant local, UTC, issuer timezone)
- Amount looks different in every system
- Reconciliation across systems becomes a nightmare

**Impact:** Aggregations, velocity checks, and amount thresholds break when the same transaction appears as different amounts. Duplicate detection fails when dates don't match across systems.

### Visualization (canvas `c3`, 720×300)

Three system boxes showing the same transaction with different amounts/dates, joined by "≠" arrows, plus a warning box.

- **Title (bold 17px `#1a5276`, centered, y=28):** "Same Transaction in 3 Systems".
- **Three boxes** 190×140 each, 20px gaps, horizontally centered, top y=55; fill `#f8f9fa`, 2px border in the system color; contents: bold 14px system-color name, bold 22px `#1a5276` amount, 13px `#666` date:
  - "Merchant (EUR)" — €4,100 — "Jan 15, 23:45 CET" — border `#2980b9`
  - "Processor (USD)" — $4,500 — "Jan 15, 17:45 EST" — border `#27ae60`
  - "Reporting (GBP)" — £3,800 — "Jan 16, 04:45 GMT" — border `#8e44ad`
- **Between boxes:** short red horizontal connector lines (`#e74c3c`, width 2) at mid-height with a bold 16px red "≠" symbol above each.
- **Warning box** (full row width, 50px tall, 20px below the boxes): fill `#fef9e7`, border 2px `#f39c12`; line 1 bold 14px `#e67e22`: "⚠ Amount differs by ±15%, date differs by ±1 day"; line 2 13px `#666`: "Velocity checks, amount thresholds, and dedup all break".

## Card Testing Attacks

**Individual transactions look normal, pattern is anomalous**

- Attacker tries 1000 small charges ($0.50) to find valid cards
- Each individual transaction: small amount, common merchant category
- Pattern: same merchant, sequential card numbers, identical amounts, rapid succession
- Per-transaction model scores each as low risk
- Must detect VELOCITY, not individual transaction features

**Impact:** Transaction-level fraud models completely miss card testing attacks. Requires aggregate/velocity features computed across transactions — a fundamentally different detection paradigm.

### Visualization (canvas `c4`, 720×300)

Scatter plot of transaction amount vs. time with a dense low-amount attack cluster.

- **Title (bold 17px `#1a5276`, centered, y=25):** "Card Testing Attack Pattern".
- **Axes:** chart area left=80, right=width−40, top=50, bottom=height−60; L-shaped axes in `#333` width 1.5. Y-axis 0–$100 with gridlines and labels "$0, $25, $50, $75, $100" (12px `#666`, light `#eee` gridlines). Y-axis title (rotated, 14px `#555`): "Transaction Amount ($)". X-axis title (14px `#555`, bottom center): "Time (minutes)"; x scale 0–95 minutes.
- **Normal transactions:** 12 blue dots (`#2980b9`, radius 5) at (t, amount): (2,45), (5,78), (12,22), (18,55), (25,90), (35,33), (42,67), (50,12), (58,85), (72,41), (80,95), (88,28).
- **Attack cluster:** 30 red dots (`#e74c3c`, radius 4) from t=60 to t≈70 (0.33-minute spacing), amounts 0.5 plus random jitter up to 0.2 (near the x-axis).
- **Cluster highlight:** dashed red rectangle (`#e74c3c`, width 2, dash 5/3) around t=59–71 from just above the cluster down to the baseline.
- **Cluster label (bold 13px red, two lines, centered above the rectangle):** "Card testing: 30 x $0.50" / "in 10 minutes".
- **Legend (top-left inside plot, bold 13px):** "● Normal transactions" in `#2980b9`, "● Card testing attack" in `#e74c3c`.

## Decline ≠ Fraud

**Multiple causes collapse into one status code**

- Insufficient funds (customer is broke, not a fraudster)
- Expired card (needs to update payment method)
- Wrong CVV (typo, not theft)
- Velocity limit (issuer-side risk rule triggered)
- Actual fraud flag (issuer detected compromise)
- All show as "DECLINED" in merchant system

**Impact:** Training a model where "declined = fraud" contaminates labels with 80-90% non-fraud cases. Different decline reasons need different merchant responses (retry, re-authenticate, block) but look identical.

### Visualization (canvas `c5`, 720×300)

Pie chart of true decline reasons collapsing (via arrow) into a single "DECLINED" block.

- **Title (bold 17px `#1a5276`, centered, y=25):** "Decline Reasons (Reality vs. What Merchant Sees)".
- **Pie chart** centered at (180, 165), radius 100, white 2px slice borders, starting at 12 o'clock:
  - Insufficient funds (40%) — `#3498db`
  - Expired card (20%) — `#e67e22`
  - Velocity limit (15%) — `#9b59b6`
  - Wrong CVV (15%) — `#1abc9c`
  - Fraud flag (10%) — `#e74c3c`
- **Pie caption (bold 14px `#1a5276`, below pie):** "Issuer's View".
- **Arrow:** horizontal black arrow (`#333`, width 2) from x=300 to x=400 at y=165, with 13px `#666` label above: "collapses to".
- **DECLINED block:** rectangle at (420, 115) size 250×100, fill `#fdedec`, border 3px `#e74c3c`; bold 28px red text "DECLINED"; below it 14px `#666` "(no reason code)"; under the block "Merchant's View".
- **Legend** (below the block at x=420, y start 240, 16px rows, 10×10 color swatches, 12px `#333` text): the five slice labels with percentages as listed above.

## 3D Secure / SCA Friction

**Anti-fraud measure that may cost more than the fraud**

- Adding 3D Secure authentication reduces fraud by 60-80%
- But also reduces conversion rate by 10-30%
- Lost revenue from abandoned carts may exceed fraud savings
- Strong Customer Authentication (SCA) mandated in EU regardless
- Optimization: apply 3DS selectively to high-risk transactions only

**Impact:** Net revenue impact of fraud prevention is not always positive. Must model the full funnel: fraud prevented minus conversions lost. The "safest" policy (authenticate everything) is often the most expensive.

### Visualization (canvas `c6`, 720×300)

Grouped bar chart: gross revenue bars with fraud-loss overlays, without vs. with 3DS, plus a net-revenue callout box.

- **Title (bold 17px `#1a5276`, centered, y=25):** "3D Secure: Fraud Saved vs. Revenue Lost".
- **Axes:** chart area left=120, right=width−60, top=55, bottom=height−55; y scale $0–$1M with labels "$0K" to "$1000K" every $200K (12px `#666`, light `#eee` gridlines).
- **Bars** (100px wide, centered at 28% and 72% of chart width):
  - Without 3DS: green revenue bar (`#27ae60`, stroke `#1e8449`) to $900K, red overlay `rgba(231,76,60,0.7)` to $150K (fraud).
  - With 3DS: green revenue bar to $680K, red overlay to $30K.
- **Group labels (bold 15px `#1a5276`, below baseline):** "Without 3DS", "With 3DS".
- **In-bar value labels (bold 13px white):** "Revenue: $900K", "Revenue: $680K" near bar tops; "Fraud: $150K", "Fraud: $30K" near baseline.
- **Net revenue callout box** at (380, 60), size 250×70, fill `#f0f4f8`, border 2px `#2980b9`: heading bold 14px `#1a5276` "Net Revenue"; line in `#27ae60`: "Without 3DS: $900K - $150K = $750K"; line in `#e74c3c`: "With 3DS: $680K - $30K = $650K".
- **Legend (top-left, 13px, 12×12 swatches):** `#27ae60` "Gross Revenue", `rgba(231,76,60,0.7)` "Fraud Loss".

## Regeneration instructions

- **Layout:** domains detail-page template — h1, `.subtitle`, one `.philosophy` callout, then one `<h2 id="...">` per pitfall followed by a single-row `.obj-table` (`<table class="obj-table"><tr>`): left `<td>` (45%) with `.obj-title` div, `<ul>` bullets and an **Impact:** paragraph; right `<td>` (55%, centered) holding one canvas. No thead, no nav, no badges, no numbering on h2s.
- **Section ids:** `chargeback-delay`, `legitimate-high-value`, `cross-border-currency`, `card-testing`, `decline-reasons`, `3ds-friction`.
- **Page CSS:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.4em `#1a5276` with 2px `#2980b9` bottom border, padding-bottom 8px; `.subtitle` `#666` 1.05em; `ul` 0.9em `#333`; `strong` `#1a5276`; `.philosophy` background `#f0f4f8`, left border 4px `#2980b9`, padding 12px 16px, 0.9em; `.obj-table` full width, collapsed borders, cells `1px solid #e0e0e0` with 20px 24px padding, even rows `#fafcfe`; `.obj-title` 1.05em weight 600 `#1a5276`.
- **Canvases:** each declares intrinsic `width="720" height="300"`; a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Default chart font 17px system sans. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`/`#9b59b6`, gray text `#666`/`#555`/`#333`.
- In regenerated HTML, any card links use `.html` extensions (this page has no outbound links).
