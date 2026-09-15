# Chargebacks & Payment Fraud

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Chargebacks & Payment Fraud

**Subtitle:** When a cardholder disputes a charge, the money comes straight back out of the merchant's account — and blocking fraud too aggressively costs even more than the fraud itself

## A $100 Order That Comes Back as a $125 Loss

**Tags:** `core idea` (blue), `chargebacks` (orange), `merchant view` (green)

- **The store** — an online sneaker shop takes 100,000 card orders a month at about $100 each
- **The dispute** — a cardholder tells their bank "I didn't make this charge"; the bank reverses it
- **The clawback** — the $100 leaves the merchant's account immediately, plus a ~$25 dispute fee
- **The defense** — the merchant may submit evidence (delivery proof, order history): "representment"
- **Who eats fraud** — in-store chip transactions shifted liability to banks; online, the merchant pays

*Example (italic):* A stolen card buys $100 sneakers; 45 days later the chargeback lands — the $100 is clawed back, the $25 fee is charged, and the shoes already shipped: well over $125 lost on one order.

**Key point:** A chargeback reverses money that already settled; for card-not-present sales the merchant, not the bank, absorbs the fraud loss plus a fee — win or lose, the dispute itself has a price.

### Visualization (canvas `c1`, 720×300)

Two-row flow diagram: the two endings of one disputed $100 card-not-present order, shown as boxes flowing left to right from dispute to outcome.

- **Title (bold 15px, `#1a5276`, top center):** "One Disputed $100 Order: Evidence Wins It Back, Silence Eats the Loss".
- **Row 1 (y=95), label 12px `#444` at x=20:** "evidence wins"; blue `#2a78d6` rounded box at x=140 labeled "dispute filed — $125 debited" (12px), 3px arrow to a blue box at x=340 labeled "representment: delivery proof", 3px arrow to a green `#008300` box at x=540 labeled "$100 returned" with bold 12px green "✓".
- **Row 2 (y=205), label:** "no evidence"; blue box at x=140 labeled "dispute filed — $125 debited", 3px arrow to an orange `#d95926` box at x=340 labeled "no compelling evidence", 3px arrow to a red `#e74c3c` box at x=540 labeled "merchant eats $125 + goods" with bold 12px red "✗".
- **Box style:** 150–170px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(217,89,38,0.12)` / `rgba(0,131,0,0.12)` / `rgba(231,76,60,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px `#1a5276`, centered near y=270):** "card-not-present: the merchant is the default loser".
- **Caption (12px `#444`, bottom right):** "amounts illustrative; dispute fee varies by processor".

## Tightening the Fraud Rule: One Month, Two Thresholds

**Tags:** `worked example` (blue), `confusion matrix` (green)

- **The model** — every order gets a fraud score 0–1; orders scoring at or above a cutoff are declined
- **This month** — 100,000 orders: 500 fraudulent, 99,500 good (a 0.5% fraud rate, illustrative)
- **Loose rule (cutoff 0.90)** — blocks 250 of the 500 frauds, wrongly declines 200 good orders
- **Tight rule (cutoff 0.70)** — blocks 350 frauds, but wrongly declines 800 good orders
- **The delta (exact)** — tightening blocks $10,000 more fraud and turns away $60,000 of good orders
- **Hand-check** — extra fraud blocked: 100 × $100 = $10,000; extra declines: 600 × $100 = $60,000

*Example (italic):* Moving the cutoff from 0.90 to 0.70 catches 100 more $100 frauds but rejects 600 more real customers at checkout — six good dollars lost per fraud dollar saved.

**Key point:** Every notch tighter buys recall with precision; here each extra fraud dollar blocked costs six dollars of rejected genuine sales — arithmetic exact for these illustrative counts.

### Visualization (canvas `c2`, 720×300)

Grouped vertical bar chart: dollars of fraud blocked vs dollars of good orders declined, one group per threshold, from the counts in the text.

- **Title (bold 15px, `#1a5276`, top center):** "Cutoff 0.90 vs 0.70: +$10k Fraud Blocked, +$60k Good Orders Lost".
- **Axes:** origin x=70, baseline y=245, plot width 590, plot height 180; y = dollars 0 to $90,000, gridlines `#e5e9ef` at 20k/40k/60k/80k with 12px `#444` labels "$20k"–"$80k"; group labels 12px `#444` under baseline: "loose rule (0.90)" centered at x=235, "tight rule (0.70)" centered at x=505.
- **Bars (70px wide, values scaled to 180px = $90k):** loose group — green `#008300` fill `rgba(0,131,0,0.35)` bar at x=165 value $25,000 (fraud blocked), red `#e74c3c` fill `rgba(231,76,60,0.30)` bar at x=250 value $20,000 (good declined); tight group — green bar at x=435 value $35,000, red bar at x=520 value $80,000.
- **Value labels:** bold 12px `#2c3e50` above each bar: "$25,000", "$20,000", "$35,000", "$80,000"; 11px legend top left: green square "fraud blocked", red square "good orders declined".
- **Annotation (bold 13px red `#e74c3c`, near x=380, y=55):** "+$10k fraud blocked costs $60k in good orders".
- **Caption (12px `#444`, bottom right):** "counts illustrative, arithmetic exact".

## Friendly Fraud, Network Referees, and 3-D Secure

**Tags:** `where it's used` (blue), `dispute rate` (orange), `liability shift` (green)

- **Friendly fraud** — a genuine purchase disputed anyway ("don't recognize it") still counts against you
- **The referee** — card networks track every merchant's dispute rate and fine sustained offenders
- **The published line** — network monitoring programs begin near a 0.9% dispute rate
- **3-D Secure** — an extra online verification step; when it runs, fraud liability shifts to the issuer
- **Its price** — the added checkout friction makes some good customers abandon: the same trade again

*Example (italic):* A gym member forgets the billing descriptor and disputes three real $50 fees as fraud — the gym is out $150 plus three $25 fees unless it fights each one with signed-contract evidence.

**Key point:** The dispute rate is itself a governed metric — friendly fraud counts toward it, network monitoring programs cap it, and 3-D Secure can shift liability but charges friction at checkout.

### Visualization (canvas `c3`, 720×300)

Vertical bar chart: the shop's monthly dispute rate creeping upward over six months toward the published network monitoring level.

- **Title (bold 15px, `#1a5276`, top center):** "Dispute Rate Creep: Six Months from Comfortable to Monitored".
- **Axes:** origin x=70, baseline y=245, plot width 590, plot height 180; y = dispute rate 0% to 1.2%, gridlines `#e5e9ef` at 0.3/0.6/0.9 with 12px `#444` labels; x = months "Jan"–"Jun", 12px `#444` labels centered under bars.
- **Bars (60px wide, evenly spaced, centers at x = 115, 210, 305, 400, 495, 590):** rates `[0.34, 0.41, 0.52, 0.63, 0.78, 0.94]` percent; Jan–May blue fill `rgba(42,120,214,0.35)` with 2px `#2a78d6` border, Jun red fill `rgba(231,76,60,0.30)` with 2px `#e74c3c` border; bold 12px value labels above each bar ("0.34%" … "0.94%").
- **Threshold line:** horizontal dashed red `#e74c3c` (dash 6/4) line at y=110 (= 0.9%), 12px bold red label "network monitoring level ≈0.9%" above it at the left.
- **Annotation (bold 13px red `#e74c3c`, near x=430, y=75):** "cross the line: fines, monitoring, possible loss of processing".
- **Caption (12px `#444`, bottom right):** "monthly rates illustrative; 0.9% matches published network program levels".

## The Mistake: Maximizing Fraud Caught

**Tags:** `common mistake` (red), `asymmetric costs` (orange)

- **The reflex** — teams tune for fraud caught (recall) because fraud is the loss everyone can see
- **The blind spot** — a declined good customer is silent: no chargeback, no ticket, often no return
- **The studies** — industry studies repeatedly find false declines cost merchants several times the fraud
- **The right objective** — minimize total cost: missed fraud × $125 plus false declines × order value
- **The curve** — for this shop the total-cost minimum sits at cutoff 0.95, looser than either rule tried

*Example (italic):* At cutoff 0.70 the shop "wins" on fraud caught but pays $98,750 a month in total; at 0.95 it pays $49,000 — about half, while deliberately letting more fraud through.

**Common mistake:** Reporting fraud-caught as the win metric. Fraud detection is a precision/recall trade with asymmetric costs — the cost-minimizing rule lets some fraud through on purpose, bounded only by the network's dispute-rate limit.

### Visualization (canvas `c4`, 720×300)

Line chart: monthly fraud cost, false-decline cost, and their total across five cutoffs, showing the minimum away from the tightest setting.

- **Title (bold 15px, `#1a5276`, top center):** "Total Cost vs Cutoff: the Minimum Is Not Where Fraud Caught Peaks".
- **Axes:** origin x=70, baseline y=245, plot width 590, plot height 180; x = cutoff, five evenly spaced ticks at x = 70, 217.5, 365, 512.5, 660 labeled "0.99", "0.95", "0.90", "0.80", "0.70" (12px `#444`, left = loosest), axis title "fraud-score cutoff (axis not to scale)"; y = dollars/month 0 to $100,000, gridlines `#e5e9ef` at 25k/50k/75k with 12px `#444` labels.
- **Fraud-cost line (orange `#d95926`, 2px):** values `[55000, 40000, 31250, 23750, 18750]` at the five cutoffs (missed frauds × $125: 440, 320, 250, 190, 150 misses).
- **False-decline line (magenta `#d55181`, 2px):** values `[2000, 9000, 20000, 45000, 80000]` (false positives × $100: 20, 90, 200, 450, 800 declines).
- **Total line (ink `#1a5276`, 3px):** values `[57000, 49000, 51250, 68750, 98750]`; 6px green `#008300` dot at the 0.95 point ($49,000).
- **Labels:** 12px line labels at right ends — orange "missed fraud × $125", magenta "false declines × $100", bold ink "total"; bold 13px green annotation near x=217, y=95: "minimum at 0.95 — some fraud is cheaper to allow".
- **Caption (12px `#444`, bottom right):** "counts illustrative; dollar arithmetic exact (0.90 and 0.70 rows match the worked example)".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); order counts, fraud rate, fee, dispute-rate months, and confusion-matrix counts are invented and labeled illustrative; all dollar figures derived from them ($10,000 / $60,000 deltas, the $49,000–$98,750 cost curve) are exact arithmetic on those counts; the ≈0.9% monitoring threshold reflects published card-network program levels. Merchant/defender perspective only — no fraud how-to content.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
