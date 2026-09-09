# A/B Testing During Non-Standard Distribution Periods

**Page type:** detail page (h2 section per topic, each with a two-column obj-table: text left 50%, canvas right 50%)
**HTML title tag:** 148. A/B Testing During Non-Standard Distribution Periods

**Subtitle:** Experiments run during shopping seasons, fiscal year-end, elections, or crises measure a temporarily transformed population. The result gets applied year-round — and fails for the other 48 weeks.

## Callout (philosophy box)

**The core problem:** A/B tests assume the measured treatment effect persists after the test, but anomalous periods transform user intent, urgency, and demographics. A feature that wins during the exception may lose during the rule.

## Shopping Season — High-Intent Users Mask Real Effect

**Test During Black Friday → Everything "Wins"**

- **The distortion:** Holiday shoppers are primed to buy, so ANY change "lifts conversion" in the test.
- **The moving baseline:** The control also lifted versus prior months, so even "no change" looked good.
- **The false positive:** 10× holiday traffic reaches "statistical significance" on very small deltas.
- **Volume illusion:** Those same deltas would not survive testing at normal, non-holiday traffic.
- **Non-generalizable:** An urgency countdown wins on December gifting FOMO and annoys March browsers into bouncing.
- **The calendar trap:** Ship-before-holidays pressure validates the feature only on holiday data.
- **All-year cost:** It then runs all year, permanently optimized for a single 4-week window.

### Visualization (canvas `c1`, 720×300)

Two-line monthly conversion chart with a shaded holiday zone; background `#f9f9f9`; margins left 50, right 30, top 50, bottom 50; L-shaped axes `#333` width 1.5.

- **Title (bold 13px, center, `#1a5276`):** "Conversion rate: holiday season vs normal — both treatment and control elevated"
- **X axis:** 8 monthly slots labeled 10px `#666`: Sep, Oct, Nov, Dec, Jan, Feb, Mar, Apr (points centered per slot).
- **Holiday zone:** Nov–Dec slots shaded `rgba(231,76,60,0.08)` with bold red 10px label "Holiday Season" at the top.
- **Series** (y scale 0–8%, width 2.5):
  - Control, blue `#3498db`: `[3.0, 3.1, 5.5, 6.2, 3.0, 2.9, 3.0, 3.1]`
  - Treatment, red `#e74c3c`: `[3.2, 3.3, 6.0, 6.8, 3.1, 3.0, 3.1, 3.2]`
- **Legend (bottom-left, line swatches + 11px `#333` text):** "Control (~3% normal, ~6% holiday)" and "Treatment (~3.2% normal, ~6.8% holiday) — \"significant lift\" only during holiday".

## End of Fiscal Year — Artificial Urgency Changes Behavior

**B2B Sales at Quarter-End: Behavior Is Entirely Non-Representative**

- **The behavior shift:** Quarter-end discounting and compressed decision timelines distort buying behavior.
- **Budget pressure:** Use-it-or-lose-it budgets make the measured demand entirely non-representative.
- **The pricing experiment trap:** A Q4-close test shows low price elasticity because buyers must spend budget.
- **Q1 collapse:** The same pricing collapses demand once the budget deadline is gone.
- **Forecasting contamination:** "Created <7 days before quarter end → 90% close rate" learns fiscal pressure.
- **Mispredicts:** That rule reads calendar pressure as deal quality, and misfires every other quarter.
- **The churn paradox:** March renewals at 98% reflect budget-cycle switching costs, not stickiness.
- **Off-cycle truth:** July renewals run 82% — same product, and the real retention level.

### Visualization (canvas `c2`, 720×300)

Bar chart of deal close rate across 13 weeks of a quarter; background `#f9f9f9`; margins left 50, right 30, top 50, bottom 50; L-shaped axes `#333` width 1.5.

- **Title (bold 13px, center, `#1a5276`):** "Deal close rate by week-of-quarter (B2B) — fiscal pressure, not model quality"
- **Bars** (13 bars, W1–W13, y scale 0–100%): values `[8, 7, 9, 10, 8, 9, 11, 12, 15, 22, 38, 65, 90]`; colors: W1–W7 blue `#3498db`, W8–W10 orange `#e67e22`, W11–W13 red `#e74c3c` (index ≥ 9 red, ≥ 6 orange). Week labels 9px `#333` below each bar; bars above 20% get white bold 10px in-bar value labels ("22%", "38%", "65%", "90%").
- **Annotations:** bold red 11px right-aligned near the top: "← Fiscal pressure spike"; blue 11px near lower-left: "Normal buying behavior →".
- **Caption (bottom center, italic gray 12px):** "Testing a pricing model in weeks 11-13 → learns \"everyone buys regardless of price.\" Fails in week 1-9."

## Elections & Political Events — Attention Shifts Everywhere

**User Attention Is Elsewhere — Engagement Data Is Noise**

- **The attention shift:** Election weeks pull attention to news, so sessions shorten and ad CTRs drop.
- **Breadth of the shift:** Browsing patterns change across the board, not just on news and politics pages.
- **The content model problem:** A recommender tested during election week learns "political content wins."
- **Lingering damage:** It keeps pushing that content and annoying users long after the election ends.
- **Sentiment contamination:** Elevated baseline negativity in charged periods miscalibrates "neutral."
- **Wrong direction later:** The model then over-reports positive sentiment once the period passes.
- **Ad performance:** Political spend spikes CPMs, and the bid model learns those levels as normal.
- **Slow recovery:** After the post-election drop the bid model stays miscalibrated for months.

### Visualization (canvas `c3`, 720×300)

Paired horizontal bar chart (normal week vs election week per metric); background `#f9f9f9`; left margin 180, row height 35px with 12px gap; each row has a thin normal bar on top and an event bar below, bar widths normalized to the larger of the pair × 45% of track.

- **Title (bold 13px, center, `#1a5276`):** "User attention during election week vs normal — everything shifts"
- **Metrics** (label right-aligned 10px `#1a5276`; values in 9px `#333` at the end of each bar; normal bar always blue `#3498db`, event bar in the listed color):
  - "Avg session duration" — normal 12 min, election 5 min, event bar red `#e74c3c`
  - "Pages per session" — normal 8 pages, election 3 pages, red
  - "Ad CTR" — normal 2.1%, election 0.8%, red
  - "News content engagement" — normal 15%, election 65%, green `#27ae60`
  - "Product browse time" — normal 6.5 min, election 2.1 min, red
- **Legend (bottom, 12px swatches + 10px `#333` labels):** blue "Normal week", red "Election week".

## Major Outages, Pandemics, Crises — Behavior Under Stress ≠ Normal

**Data Collected During a Crisis Doesn't Represent Normal Operation**

- **Pandemic data:** Models trained on the 2020-2021 anomaly expected that behavior to continue.
- **The reversion:** They overshot every prediction during the 2022 reversion to the mean.
- **Outage-adjacent data:** A competitor outage brings desperate users who "convert at 25%".
- **The real number:** Those users leave when the competitor recovers, revealing the true 8% rate.
- **Weather events:** An ETA model tested during a heatwave sees uniformly high, predictable demand.
- **Varied weeks hurt:** Its accuracy drops in a normal week where demand is lower and more varied.
- **The general principle:** Behavior driven by an external force rather than intrinsic need is not the product.
- **What you optimize:** Such data teaches the model to optimize for the force, which then goes away.

### Visualization (canvas `c4`, 720×280)

Two overlaid Gaussian distribution curves; background `#f9f9f9`; margins left 50, right 30, top 50, bottom 50; L-shaped axes `#333` width 1.5.

- **Title (bold 13px, center, `#1a5276`):** "Model trained during crisis → fails when normal returns"
- **Curves** (unnormalized Gaussians over x 0–100, peak at 80% of plot height, width 2.5):
  - Normal behavior: solid blue `#3498db`, mean 50, SD 15
  - Crisis behavior: dashed red `#e74c3c` (dash 5/5), mean 80, SD 10
- **Labels (bold 11px centered over each peak):** blue "Normal behavior" at x=50; red "Crisis behavior" at x=80 with 10px subtext "(model trained here)".
- **Deployment arrow:** green `#27ae60` width-2 horizontal arrow just below the x-axis from the crisis peak (x=80) to the normal peak (x=50) with a filled triangular head, labeled in green 10px "← deployed to this reality".
- **Caption (bottom center, italic gray 12px):** "Training distribution ≠ deployment distribution. Crisis data doesn't represent normal operation."

## Regeneration instructions

- **Layout:** h1 + `.subtitle` + `.philosophy` callout, then one `h2` per section (1.4em `#1a5276`, bottom border `2px solid #2980b9`, padding-bottom 8px), each followed by a `.obj-table` with one `<tr>`: left `<td>` (40%) holds `.obj-title` + `<ul>` bullets, right `<td>` (60%, centered) holds the canvas.
- **Page style:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; subtitle `#666` 1.05em; `ul` 0.9em `#333`; `strong` in `#1a5276`; table cell borders `1px solid #e0e0e0`, padding 20px 24px, even rows background `#fafcfe`; `.obj-title` 1.05em, weight 600, `#1a5276`; `.philosophy` background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em. No nav bar, no back/home links.
- **Canvas:** declare intrinsic `width`/`height` attributes as given per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Each chart fills its background `#f9f9f9`. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, accent blue `#3498db`, gray text `#666`/`#555`/`#333`.
- In regenerated HTML, any card/page links use `.html` extensions.
