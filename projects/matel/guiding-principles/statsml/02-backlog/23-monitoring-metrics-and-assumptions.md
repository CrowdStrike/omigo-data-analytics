# Monitoring Metrics & Assumption Validation

**Page type:** detail page (backlog kusto-style 2-col layout: text left 45%, canvas right 55%, one `.lang-section` per numbered section)
**HTML title tag:** Monitoring Metrics & Assumption Validation

**Subtitle:** Continuous validation of the assumption stack beneath every metric

**Intro callout (blue-accent `.intro` box):** Every system and business metric is built on a stack of assumptions, measurements, and facts. When any layer of that stack shifts — and nobody is watching — the metric becomes meaningless or misleading. This backlog item designs continuous validation of the entire assumption chain.

## 1. The Core Problem

A metric is the TIP of an iceberg. Below it:

- **Data distribution assumptions:** "Users are normally distributed by age" / "Fraud is 0.1% of transactions" / "80% of traffic is mobile"
- **Measurement assumptions:** "Our logging captures all events" / "Latency is measured end-to-end" / "Active means >1 action per day"
- **Causal assumptions:** "Feature X drives metric Y" / "More users → more revenue" / "Faster page load → better conversion"
- **Environmental assumptions:** "Customer mix is stable" / "No major competitor entered" / "Seasonality is consistent year-over-year"

**Key point (red-accent callout):** When any of these shift, the metric becomes uninterpretable. But nobody is monitoring the ASSUMPTIONS — only the final number.

### Visualization (canvas `c1`, 720×300)

Iceberg diagram: metric above the waterline, assumptions below.

- **Title (bold 18px, `#1a5276`, top center):** "The Metric Is the Tip — Assumptions Are the Iceberg".
- **Waterline:** horizontal line in `#2980b9` (width 2) from x=30 to x=w−30 at y=100, with centered label below in `#2980b9` 15px: "← visibility line →" at y=113.
- **Above waterline:** small upward triangle (apex at 340,50; base 290,100 to 390,100) filled `rgba(39,174,96,0.4)`. Bold 17px `#27ae60` text centered at x=340: "Revenue +8%" (y=75) and "(what leadership sees)" (y=92).
- **Below waterline:** large downward triangle (180,100 → 340,260 → 500,100) filled `rgba(231,76,60,0.2)`. Inside, 13px `#e74c3c` labels centered at x=340: "Distribution assumptions" (y=135), "Customer mix stability" (y=155), "Causal relationships" (y=175), "Measurement definitions" (y=195), "Environmental constants" (y=215).
- **Takeaway (bold 16px, `#555`, centered at 340,250):** "Nobody monitors these. When they break → metric becomes meaningless."

## 2. What Must Be Monitored

- **Distribution of inputs:** Feature distributions that fed model training. If production distribution diverges from training distribution (PSI, KL divergence, KS test) → model predictions are extrapolating into unknown territory.
- **Customer/segment composition:** What % of volume comes from each segment? One enterprise customer onboarding can shift your entire distribution. Monitor segment proportions and flag when any single entity exceeds X% of total.
- **Assumption registry:** Every assumption made during system design should be DOCUMENTED and have a MEASURABLE proxy. "Users browse 20+ items before purchase" → monitor median items-browsed. When it drops to 5 (mobile shift), the recommendation model built on that assumption is invalid.
- **Causal relationships:** The correlation between X and Y that justified building a feature. Monitor that correlation continuously. When it breaks (confound changed, market shifted), the feature is now noise not signal.
- **External facts:** Competitor pricing, market conditions, regulatory changes, platform algorithm updates. These aren't in your data but affect your metrics. Need external signal monitoring.

### Visualization (canvas `c2`, 720×300)

Stacked horizontal layer diagram: five monitoring layers with monitoring status.

- **Title (bold 18px, `#1a5276`, top center):** "Five Layers of Monitoring".
- **Layers:** five rectangles at x=40, width 480, height 35, y positions 40/80/120/160/200:
  1. fill `rgba(39,174,96,0.3)` — label "Business Metric (revenue, DAU, churn)" — status "✓ Monitored" (green `#27ae60`)
  2. fill `rgba(230,126,34,0.3)` — label "Model Performance (accuracy, precision, latency)" — status "~ Sometimes" (orange `#e67e22`)
  3. fill `rgba(231,76,60,0.3)` — label "Feature Distributions (drift, PSI, segment mix)" — status "✗ Rarely" (red `#e74c3c`)
  4. fill `rgba(231,76,60,0.4)` — label "Causal Assumptions (correlations that justified design)" — status "✗ Never" (red `#e74c3c`)
  5. fill `rgba(231,76,60,0.5)` — label "Environmental Facts (competitor, market, regulation)" — status "✗ Never" (red `#e74c3c`)
- Labels 13px `#333` left-aligned at x=50 inside each bar; statuses bold 16px right-aligned at x=610.
- **Takeaway (bold 16px, `#555`, centered, y=260):** "Most orgs only monitor the top layer. Breakage starts at the bottom."

## 3. Distribution Shift Detection

- **Population Stability Index (PSI):** Compare current distribution to reference (training time). PSI > 0.1 = notable shift. PSI > 0.25 = action required. Run per-feature, flag the ones drifting fastest.
- **KS test on sliding windows:** Compare last-7-days distribution to previous-30-days baseline. Detect shifts before they compound into metric degradation.
- **Segment dominance alerts:** Any single customer/entity exceeding 10% of total volume. Enterprise onboarding can flip your distribution overnight — detect it immediately.
- **Concept drift detection:** Monitor the relationship between features and target, not just feature distribution. Feature distribution can be stable while the feature→target relationship breaks (concept drift without data drift).

### Visualization (canvas `c3`, 720×300)

Two overlaid bell curves: training vs production distributions with an enterprise-customer spike.

- **Title (bold 18px, `#1a5276`, top center):** "Distribution Shift: Training vs Production (6 Months Later)".
- **Training curve:** blue `#2980b9` stroked Gaussian (width 2), 200 points at x = 50 + i·2.8, value `exp(−((i−80)/30)²/2)`, plotted as y = 230 − v·150 (centered near x≈274, full height). Label "Training distribution" in `#2980b9` 13px centered at (270,250).
- **Production curve:** red `#e74c3c` stroked Gaussian (width 2), same x mapping, value `exp(−((i−120)/45)²/2)·0.7` (shifted right, fatter, lower peak). Label "Production (6 months later)" in `#e74c3c` 13px centered at (430,250).
- **PSI annotation:** bold 17px `#e67e22` centered: "PSI = 0.38 (action required!)" at (w/2, 270).
- **Enterprise spike:** rectangle fill `rgba(231,76,60,0.3)` at (470,60) size 60×170, labeled above/inside in `#e74c3c` 14px: "Enterprise" (500,55) and "customer" (500,68).

## 4. The Assumption Registry

A structured document pairing every design assumption with its measurable validation:

- **Format:** Assumption → Metric → Threshold → Action
- "Fraud is <0.5% of transactions" → Monitor fraud_rate daily → If >1% for 3 consecutive days → Alert + model retrain trigger
- "User sessions average 12 minutes" → Monitor p50 session duration → If <5min for a week → Recommendation model assumptions violated
- "Feature X correlates with churn at r>0.3" → Monitor rolling correlation → If r<0.1 → Feature is now noise, remove or replace
- "Training data represents production" → PSI per feature → If any feature PSI>0.25 → Retrain with recent data

**Key point (red-accent callout):** This registry should be a FIRST-CLASS artifact — reviewed quarterly, updated when systems change, enforced by automated monitoring.

### Visualization (canvas `c4`, 720×300)

Canvas-drawn registry table with live status column.

- **Title (bold 18px, `#1a5276`, top center):** "Assumption Registry: Assumption → Metric → Threshold → Action".
- **Header row (bold 14px `#1a5276`, left-aligned, y=50):** "Assumption" (x=30), "Metric" (x=210), "Threshold" (x=340), "Action" (x=470), "Now" (x=590).
- **Rows (15px `#333`, one per 38px starting y=68, thin `#eee` separator line beneath each):**
  | Assumption | Metric | Threshold | Action | Now (status color) |
  |---|---|---|---|---|
  | Fraud < 0.5% | fraud_rate | >1% 3 days | Retrain | OK (green `#27ae60`) |
  | Sessions avg 12min | p50_duration | <5min 7 days | Revalidate recs | ⚠ 6.2min (orange `#e67e22`) |
  | Feature X → churn (r>0.3) | rolling_corr | r<0.1 | Remove feature | ✗ r=0.08 (red `#e74c3c`) |
  | Mobile < 50% | platform_split | >70% | Retune model | ⚠ 68% (orange `#e67e22`) |
- **Takeaway (bold 15px `#555`, centered, y=240):** "2 of 4 assumptions have broken. Metric still looks "fine" — for now."

## 5. Open Questions

(Full-width section, no canvas.)

- How to prioritize which assumptions to monitor first? (By impact × likelihood of shift?)
- What's the right granularity? Per-feature? Per-model? Per-business-metric?
- How to handle cascading alerts when one assumption breaks many downstream metrics?
- How to distinguish temporary fluctuation from genuine distribution shift? (Sequential testing?)
- How to monitor causal assumptions without running continuous experiments? (Observational causal inference?)
- What's the right organizational ownership? Data team? ML platform? Each team owns their own?

## Regeneration instructions

- **Layout:** backlog detail page. Body → h1 → `.subtitle` → `.intro` callout → one `.lang-section` per numbered section, each containing an `<h2>` and a `table.layout` with a single `<tr>`: left `td.text-col` (45%) for paragraphs/bullets/callouts, right `td.viz-col` (55%) for the canvas. Section 5 (Open Questions) is a `.lang-section` with only an `<h2>` and a full-width `<ul>` — no table, no canvas.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with `border-bottom: 2px solid #2980b9`, padding-bottom 8px. `.subtitle` `#666` 0.95rem. `.intro` background `#f0f4f8`, `border-left: 3px solid #2980b9`, padding 8px 12px, 0.9rem. h2 1.3rem `#1a5276` with 2px solid `#2980b9` bottom border. `.key-point` background `#f8f9fa`, `border-left: 3px solid #e74c3c`, padding 8px 12px, 0.9rem. `.example` italic `#555` 0.9rem. `ul` 0.92rem, margin 8px 0 8px 20px. `pre` background `#f4f4f4` (defined, unused). Canvas: `width: 100%`, border `1px solid #e0e0e0`, radius 4px. No nav bar, no back/home links, no index number in h1.
- **Palette:** primary blue `#1a5276`, accent blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`.
- **Canvas:** each declares intrinsic width/height attributes (all 720×300 here); a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- Regenerated HTML has no card links (detail page); any links elsewhere use `.html` extensions.
