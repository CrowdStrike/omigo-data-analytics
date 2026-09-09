# Domain Pitfalls: Startup Funding

**Page type:** detail page (h2 section headings, each followed by a one-row two-column obj-table: text left 50%, canvas right 50%)
**HTML title tag:** Domain Pitfalls: Startup Funding

**Subtitle:** Statistical and analytical pitfalls specific to startup funding, venture capital, and growth-stage metrics.

## Survivorship bias extreme

**Obj-title:** Survivorship bias extreme

- You study successful startups: a home-rental unicorn, a ride-hailing giant, a payment processor.
- For every one that survived, 100 with similar profile died.
- "What made them successful" = survivorship narrative.
- Dead startups had same features but failed.

### Visualization (canvas `chart1`, 720×200 — declared 720×300 in markup, resized to 720×200 by the setup helper)

Scatter diagram: a cloud of dead startups vs three spotlighted survivors.

- **Background:** full-canvas fill `#f0f4f8`.
- **Dead startups:** 100 gray `#bdc3c7` dots (radius 4) at random positions in the left region (x 50-400, y 30-180; positions generated with `Math.random`, not fixed data).
- **Survivors (green `#27ae60` dots, radius 10, with bold 13px `#1a5276` labels to the right):** "home-rental app" at (500,50); "ride-hailing app" at (560,100); "payment processor" at (620,150).
- **Spotlight:** red `#e74c3c` ellipse outline (width 2) centered (560,100), radii 90×70, around the survivors.
- **Labels:** 17px `#7f8c8d` "100 dead startups (ignored)" at (100,190); bold 15px `#1a5276` "Study focuses here →" at (450,30); 12px `#e74c3c` "Same features, different outcome" at (60,20).

## Vanity metrics for fundraising

**Obj-title:** Vanity metrics for fundraising

- GMV $10M sounds impressive; revenue = $100K; actual margin = -$50K per month.
- Metrics chosen to maximize impressive-ness not informativeness.
- Investor data = curated lie designed to get funding.
- Model trained on pitch decks = model of deception.

### Visualization (canvas `chart2`, 720×200 — declared 720×300, resized by setup helper)

Three-bar chart with wildly different scales, negative bar drawn below the baseline.

- **Background:** `#f0f4f8`.
- **Bars (100px wide, at x = 120, 320, 520, baseline y=170, positive height = value/10000 × 140, negative drawn 20px below baseline):**
  - GMV — value 10000, display "$10M", green `#27ae60` (full-height bar).
  - Revenue — value 100, display "$100K", orange `#f39c12` (tiny bar).
  - Margin — value −50, display "-$50K/mo", red `#e74c3c` (below baseline).
- **Labels:** bold 17px `#1a5276` metric names above/near each bar; 15px display value in the bar color.
- **Annotations:** italic 13px `#e74c3c` "Pitch deck highlights this →" at (15,25); 13px `#7f8c8d` "Reality: losing money every month" at (440,190).

## Valuation ≠ value

**Obj-title:** Valuation ≠ value

- Last round valued company at $5B; actual liquidation value: $200M.
- Valuation = price of LAST share sold under specific terms (liquidation preference, ratchets).
- Valuation reflects negotiation power not business value.

### Visualization (canvas `chart3`, 720×200 — declared 720×300, resized by setup helper)

Two-bar comparison: headline valuation vs liquidation value.

- **Background:** `#f0f4f8`.
- **Left bar (x=180, 120 wide, full height 140, baseline y=175):** blue `#3498db`, header bold 17px `#1a5276` "Valuation", value 15px `#3498db` "$5B".
- **Right bar (x=420, 120 wide, height proportional 200/5000 of 140 with a 20px minimum):** red `#e74c3c`, header "Liquidation Value", value "$200M".
- **Gap annotation:** dashed `#7f8c8d` diagonal line (dash 4/3) from (310,55) to (410,155); bold 14px `#e74c3c` centered "25x gap" at (360,95); 12px `#7f8c8d` "(liquidation pref, ratchets)" at (360,112).

## Cohort deterioration hidden by growth

**Obj-title:** Cohort deterioration hidden by growth

- January users retain 40%; February users retain 30%; March users retain 20% — total users growing!
- Headline: "500K users, growing 50% monthly!"
- Reality: product-market fit degrading while growth masks it.
- Overall metrics hide per-cohort decay.

### Visualization (canvas `chart4`, 720×200 — declared 720×300, resized by setup helper)

Split panel: declining cohort-retention bars (left) vs rising total-users line (right).

- **Background:** `#f0f4f8`; vertical dashed `#bdc3c7` divider (dash 3/3) at x=370.
- **Left panel title (bold 14px `#1a5276`, centered at 180,18):** "Cohort Retention".
- **Retention bars (35px wide, 48px pitch from x=40, baseline y=165, scaled to max 50% over 130px, red `#e74c3c` with alpha increasing 0.5→0.9 across months):** Jan 40%, Feb 30%, Mar 20%, Apr 15%, May 10%, Jun 7%; month and percent labels 12px `#555`.
- **Declining trend arrow:** red `#e74c3c` line (width 2) from (60,55) to (285,135) with arrowhead.
- **Right panel title:** "Total Users (headline)" (bold 14px `#1a5276`, centered at 560,18).
- **Total-users line (green `#27ae60`, width 3, 4px dots, 55px pitch from x=400, scaled max 550 over 130px):** Jan 50K, Feb 100K, Mar 170K, Apr 260K, May 380K, Jun 500K; value labels ("50K"…"500K") and month labels 11px `#555`.

## Burn rate vs runway mismatch

**Obj-title:** Burn rate vs runway mismatch

- Spending $2M/month with $24M in bank = "12 months runway".
- But: burn increasing 10%/month — actual runway = 9 months.
- And: next fundraise takes 6 months; real safety margin = 3 months, not 12.
- Linear extrapolation of non-linear burn = fatal.

### Visualization (canvas `chart5`, 720×200 — declared 720×300, resized by setup helper)

Two cash-remaining curves over 12 months: linear assumption vs compounding burn.

- **Background:** `#f0f4f8`; light `#ecf0f1` horizontal gridlines at quarter heights; solid `#2c3e50` zero line at baseline y=170.
- **Scale:** x from month 0 to 12 at 48px per month starting x=80; y from $0 (baseline) to $24M (chart height 130). Y labels 11px `#555`: "$24M" (top), "$0" (baseline). Month labels "M0", "M2", … "M12" (11px `#555`).
- **Linear series (blue `#3498db`, dashed 6/4, width 2):** cash = 24 − 2×month, i.e. [24, 22, 20, 18, 16, 14, 12, 10, 8, 6, 4, 2, 0].
- **Real series (red `#e74c3c`, solid, width 2.5):** cash after burn starting at $2M growing 10%/month (24, 22, 19.8, 17.38, 14.72, 11.79, 8.57, 5.03, 1.13, then negative — line clamped at $0 and stops once cash ≤ 0, around month 9).
- **Legend (top, 13px):** blue dashed swatch + "Linear assumption (12 mo)"; red solid swatch + "Reality: 10%/mo burn growth (~9 mo)".
- **Annotation:** bold 13px `#e74c3c` centered "☠ Out of cash" near month 9 just above the baseline.

## IPO lockup distortion

**Obj-title:** IPO lockup distortion

- "Stock price" post-IPO with 80% of shares locked up = artificial scarcity.
- Lockup expires → insiders sell → supply 5x → price crashes 30-50%.
- The IPO price is not a real market price, it's a constrained market price.

### Visualization (canvas `chart6`, 720×200 — declared 720×300, resized by setup helper)

Stock-price time series with a lockup-expiry crash and a float indicator bar.

- **Background:** `#f0f4f8`.
- **Price series (blue `#2980b9` line, width 2.5, 30 points from x=60 at even spacing, y scaled between $20 and $70 over 130px above baseline y=170):** generated, not fixed data — 18 pre-lockup points rising from $45 by 1.2/step with a small sine wobble (`45 + i*1.2 + sin(i*0.7)*2`), then 4 crash points dropping ~8-12.5 each, then 8 post-lockup points stabilizing near the floor (min $25) with a sine wobble.
- **Zone shading:** pre-lockup region (first 18 steps) `rgba(46,204,113,0.08)`; post-lockup region `rgba(231,76,60,0.06)`.
- **Expiry marker:** vertical dashed red `#e74c3c` line (dash 5/3, width 2) at the 18th step, headed by bold 14px `#e74c3c` "Lockup Expiry".
- **Zone labels (13px, centered):** green `#27ae60` "80% shares locked" / "(artificial scarcity)" over the pre-lockup zone; red `#e74c3c` "Insiders sell" / "Supply 5x → -40%" over the post-lockup zone.
- **Price annotations:** 12px `#1a5276` "IPO: $45" near the start; 12px `#e74c3c` "~$<final price>" (rounded last generated value) at the line's end.
- **Float indicator (below baseline at y+5, 8px tall):** short blue `#3498db` segment (20% of the pre-lockup width) labeled "Float: 20%"; full-width red `#e74c3c` segment from the expiry point labeled "Float: 100%" (10px `#555`).

## Regeneration instructions

- **Layout:** standard domains detail page (139-style): h1, `.subtitle` paragraph, then per pitfall an unnumbered `<h2>` followed by a one-row `.obj-table` — left `<td>` (40%) with `.obj-title` + `<ul>` bullets, right `<td>` (60%, centered) with one `<canvas>` (ids `chart1`-`chart6`). No thead, no nav, no badges, no cross-page links.
- **Page CSS:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.4em `#1a5276` with 2px solid `#2980b9` bottom border; `.subtitle` `#666` 1.05em; ul 0.9em `#333`; `strong` `#1a5276`; `.obj-table` cells border `1px solid #e0e0e0`, padding 20px 24px, even rows `#fafcfe`; `.obj-title` 1.05em weight 600 `#1a5276`; `.philosophy` callout style defined but unused. All six canvases use a light blue-gray `#f0f4f8` plot background.
- **Canvas:** markup declares `width="720" height="300"`; a shared `setupCanvas(id)` helper overrides to 720×200 CSS pixels and scales the backing store by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates), default font 17px system sans-serif.
- **Palette:** primary blue `#1a5276`, secondary blues `#2980b9`/`#3498db`, green `#27ae60`, red `#e74c3c`, orange `#e67e22` (project palette; this page uses `#f39c12` for orange bars), grays `#7f8c8d`/`#bdc3c7`/`#555`.
- In regenerated HTML, any card/page links use `.html` extensions (this page has none).
