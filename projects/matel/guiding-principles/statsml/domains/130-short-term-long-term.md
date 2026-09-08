# Short-Term Gains with Long-Term Losses

**Page type:** detail page (h2 section heading per pitfall, each followed by a one-row two-column obj-table: text left ~40%, canvas right ~60%)
**HTML title tag:** 130. Short-Term Gains with Long-Term Losses

**Subtitle:** Decisions that boost this quarter's metrics while quietly destroying the baseline they will be measured against later.

## Aggressive Discounting

- Revenue spike now, brand devaluation later
- Customers trained to wait for sales
- "Why pay full price? Sale is next week."

**Example:** 40% off quarterly sale creates 60% revenue spike but baseline revenue drops 25% as customers learn to wait.

### Visualization (canvas `c1`, 720×200)

Sawtooth line chart: revenue spikes at each sale with a declining baseline.

- **Title (17px `#1a5276`, at 20,25):** "Revenue Over Time: Repeated Discounting Effect".
- **Data:** revenue points `[100, 160, 90, 155, 75, 150, 65, 140, 55, 130, 50]` plotted as blue `#2980b9` line (width 2) at x = 80 + i·58, y = 190 − value.
- **Baseline trend:** dashed red `#e74c3c` line (dash 4/4) from (80,90) to (660,140), i.e. declining.
- **Labels (14px):** red "Baseline declining" at (550,155); blue "Spikes = sale events" at (500,60).

## Technical Debt Accumulation

- Ship fast now, 10x cost to maintain/extend later
- Features added in weeks, bugs take months to find
- System becomes unmaintainable in 2 years

**Example:** Startup ships MVP in 3 months. By month 18, every new feature takes 4x longer due to spaghetti architecture.

### Visualization (canvas `c2`, 720×200)

Crossing lines: feature velocity falls while debt rises.

- **Title (17px `#1a5276`, at 20,25):** "Feature Delivery Speed vs Technical Debt Growth".
- **Velocity line:** green `#27ae60`, width 2 — values `[150, 140, 120, 90, 60, 35, 20]` at x = 80 + i·90, y = 190 − value.
- **Debt line:** red `#e74c3c` — values `[10, 30, 55, 85, 115, 140, 155]`, same x spacing/y mapping.
- **Legend (14px):** green "Velocity" at (620,175); red "Debt" at (620,45).

## Engagement Optimization Dark Side

- Addictive loops: notifications, variable rewards
- Engagement UP now
- User trust/mental health collapse later, regulatory action + exodus

**Example:** Infinite scroll + push notifications = 40% more DAU. 18 months later: regulatory investigation, user exodus, brand damage.

### Visualization (canvas `c3`, 720×200)

Two lines: engagement rises then collapses while trust steadily declines.

- **Title (17px `#1a5276`, at 20,25):** "Engagement (DAU) vs User Trust Over Time".
- **Engagement line:** blue `#2980b9`, width 2 — values `[60, 90, 120, 140, 145, 130, 90, 50]` at x = 80 + i·80, y = 190 − value.
- **Trust line:** red `#e74c3c` — values `[140, 135, 120, 100, 70, 45, 30, 20]`, same mapping.
- **Legend (14px):** blue "Engagement" at (580,100); red "Trust" at (600,175).

## Growth Hacking Inflation

- Inflated numbers: sign-up incentives, bot-counted MAU
- Impress investors now
- Truth emerges later, trust destroyed

**Example:** $5 sign-up bonus inflates MAU by 300%. 80% never return after collecting bonus. Series B due diligence exposes truth.

### Visualization (canvas `c4`, 720×200)

Two horizontal bars: reported vs actual MAU.

- **Title (17px `#1a5276`, at 20,25):** "Reported MAU vs Actual Active Users".
- **Bars:** blue `#2980b9` rect at (100,60) size 220×50 with white 15px label "Reported: 400K MAU"; red `#e74c3c` rect at (100,125) size 60×50 with white 13px label "Real: 110K".
- **Annotations (14px `#666`):** "72% are incentive-only sign-ups" at (380,100); "Due diligence reveals truth at Series B" at (380,125).

## Hiring Fast Without Culture Fit

- Bodies now, toxicity + turnover in 6 months
- Net negative productivity
- Onboarding wasted on people who leave

**Example:** Hire 20 in Q1 to meet deadline. By Q3: 12 have left, 3 toxic, remaining 5 demoralized. Net output worse than original team of 8.

### Visualization (canvas `c5`, 720×200)

Line chart of team output over 9 months vs a no-hiring counterfactual.

- **Title (17px `#1a5276`, at 20,25):** "Team Output: Hire-Fast Strategy Over 9 Months".
- **Output line:** blue `#2980b9`, width 2 — values `[80, 70, 85, 95, 75, 55, 40, 50, 60]` at x = 80 + i·70, y = 190 − value.
- **Counterfactual:** dashed green `#27ae60` line (dash 5/5) from (80,110) to (640,100), labeled "Original team (no hiring)" in green 14px at (450,88).
- **Annotation:** red `#e74c3c` "Turnover dip" at (350,160).

## Ignoring Data Quality

- Use dirty data now, launch fast
- Models degrade silently
- 6 months of wrong decisions before noticing

**Example:** Recommendation engine trained on duplicate/stale data. Looks fine at launch. Slowly recommends outdated items. Revenue dips attributed to "market conditions."

### Visualization (canvas `c6`, 720×200)

Declining accuracy line crossing an alert threshold.

- **Title (17px `#1a5276`, at 20,25):** "Model Accuracy: Silent Degradation (Dirty Data)".
- **Accuracy line:** red `#e74c3c`, width 2 — values `[92, 91, 90, 88, 85, 80, 74, 68, 60]` (%) at x = 80 + i·72, y = 190 − (value − 50)·3.2.
- **Threshold:** dashed gray `#999` horizontal line (dash 4/4) at the y of 75%, labeled "Alert threshold (75%)" in gray 13px just above at right (x≈520).
- **Annotations:** red "Noticed here (month 7)" ~30px below the threshold label; gray `#666` "Months →" at (340,195).

## Burning User Trust for Metrics

- Misleading notifications, dark patterns
- Engagement UP this quarter
- Uninstalls UP next quarter, trust hard to reverse once lost

**Example:** "Your friend joined!" notification (they didn't). CTR +25%. App store rating drops from 4.5 to 3.2 in 6 months. Uninstalls up 40%.

### Visualization (canvas `c7`, 720×200)

Paired bar chart per quarter: engagement vs uninstall rate.

- **Title (17px `#1a5276`, at 20,25):** "Dark Patterns: Short-Term Engagement vs Uninstall Rate".
- **Engagement bars:** blue `#2980b9`, values `[40, 55, 70, 80, 75, 60, 45]`, 35px wide at x = 60 + i·95, baseline y=180.
- **Uninstall bars:** red `#e74c3c`, values `[5, 8, 12, 20, 35, 55, 70]`, 35px wide at x = 97 + i·95 (adjacent to each engagement bar).
- **Legend (13px):** blue "Engagement" at (530,55); red "Uninstalls" at (530,75); gray `#666` "Quarters →" at (320,195).

## Deferred Maintenance

- Infrastructure, security patches, documentation skipped
- Saves time now
- Causes outages/breaches/confusion later at 10-100x the saved cost

**Example:** Skip security patch for 6 months (saves 2 engineer-days). Breach occurs: $2M in damages, 3 months recovery, customer trust gone.

### Visualization (canvas `c8`, 720×200)

Two-box cost comparison: fix now vs fix after incident.

- **Title (17px `#1a5276`, at 20,25):** "Cost: Fix Now vs Fix After Incident".
- **Boxes:** green `#27ae60` rect at (100,100) size 80×70; red `#e74c3c` rect at (350,40) size 250×130.
- **Labels (15px `#333`):** "Patch now" at (100,190), "2 eng-days" at (105,92); "Fix after breach" at (380,190), "$2M + 3 months + trust" at (370,35).
- **Caption (14px `#666`):** "10-100x multiplier on deferred costs" at (300,195).

## Regeneration instructions

- **Layout:** for each of the 8 pitfalls, an `<h2>` section heading (1.4em `#1a5276`, bottom border `2px solid #2980b9`, padding-bottom 8px) followed by a one-row `.obj-table`: full-width table, left `<td>` (40%) holds `.obj-title` + `<ul>` bullets + an `<p><strong>Example:</strong> ...</p>` paragraph, right `<td>` (60%, centered) holds the canvas. Even rows background `#fafcfe`; cell borders `1px solid #e0e0e0`, padding 20px 24px.
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; `.subtitle` `#666` 1.05em; `ul` 0.9em `#333`; `strong` `#1a5276`; `.obj-title` 1.05em weight 600 `#1a5276`. Unused `.philosophy` class: background `#f0f4f8`, left border `4px solid #2980b9`. No nav bar, no back/home links.
- **Canvas:** HTML attributes declare `width="720" height="300"` but a shared init loop overrides every canvas to a 720×200 logical size — backing store sized to rendered width × `window.devicePixelRatio` (display capped via `style.maxWidth`), CSS size fixed at 720×200 px, `ctx.scale` back to logical coordinates. All chart coordinates above are in the 720×200 space. Chart titles 17px, labels 13–15px, `-apple-system, sans-serif`.
- **Palette:** primary blue `#1a5276`, accent blue `#2980b9`, green `#27ae60`, red `#e74c3c`, gray text `#666`/`#333`/`#999`.
- Card links elsewhere referencing this page use the `.html` extension in regenerated HTML.
