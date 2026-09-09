# Metric Granularity & Denominator Selection

**Page type:** detail page (two-column obj-table layout: text left 50%, canvas right 50%, one h2 + one-row table per pitfall)
**HTML title tag:** 136. Metric Granularity & Denominator Selection

**Subtitle:** Statistical tests require independent observations: page-level events within a session are correlated, user-level aggregates are not. The wrong granularity doesn't just give a different number — it makes the math invalid.

## A/B Test at Session/Page Level Violates i.i.d. Assumption

**Page Events Are Correlated Within a Session — Not Independent Samples**

- **The mathematical requirement:** Every classic test assumes independent draws from the population.
- **Variance underestimated:** SE = σ/√n understates true variance whenever observations are correlated.
- **Events cluster within users:** Page views in a session share the same intent, query, and mood.
- **Sessions correlate too:** Repeat sessions from one user are auto-correlated, not independent draws.
- **User-level totals ARE independent:** One value per user per period, different people, no causal link.
- **Valid inference:** A t-test on user-level aggregates therefore satisfies the independence assumption.

### Visualization (canvas `c1`, 720×300)

Two-box comparison diagram: which aggregation level satisfies independence.

- **Title (bold 17px, `#1a5276`, centered):** "i.i.d. Requirement: Which Level Satisfies Independence?".
- **Left box (red `#e74c3c`, alpha-0.15 fill + 2px stroke, rect 30,40,320×75):** bold red heading "✗ Page events in session"; body text (17px `#333`): "SRP: click₁, click₂, click₃ ... (same user," / "same query, same intent → CORRELATED)".
- **Right box (green `#27ae60`, alpha-0.15 fill + 2px stroke, rect 380,40,310×75):** bold green heading "✓ User monthly totals"; body text: "User A: $200, User B: $50, User C: $80" / "Different people → INDEPENDENT)".
- **Bottom lines (centered):** bold `#1a5276` "SE = σ/√n is ONLY valid when n = independent observations" (y=140); red "Page-level n=10,000 (correlated) → SE too small → p-value fake" (y=165); green "User-level n=1,000 (independent) → SE correct → valid inference" (y=188).

## Independence Violation — Why Page-Level Data Can't Use a T-Test

**T-Test Requires Independent Observations. Page Events Are NOT Independent.**

- **The math requirement:** SE = σ/√n only holds for uncorrelated data, not clustered observations.
- **Design effect:** With intra-cluster correlation ρ the true SE inflates by the factor in the chart.
- **Page views are correlated:** Same user, same intent, same browsing sequence, one after another.
- **Dependent observations:** Each page view is therefore a dependent value, not an independent draw.
- **The inflation:** 1000 users × 10 pages = 10,000 rows, but the effective n is only about 1000.
- **False significance:** The naive SE runs ~3× too small, so "significant" results are often noise.

### Visualization (canvas `c2`, 720×300)

Formula panel plus confidence-interval width comparison.

- **Title (bold 17px, `#1a5276`, centered):** "Design Effect: True SE vs Naive SE".
- **Text lines (17px `#333`, left-aligned at x=60):** "Naive:  SE = σ / √n        (assumes independence)" (y=50); "True:   SE = σ / √n × √(1 + (m-1)ρ)    where m=cluster size, ρ=ICC" (y=75); "Example: m=10 pages/user, ρ=0.3 (intra-user correlation)" (y=105); "Design effect = 1 + (10-1)(0.3) = 3.7" (y=130).
- **Bold red lines (`#e74c3c`):** "True SE is √3.7 = 1.92× larger than naive SE" (y=155); "Naive p=0.001 → True p=0.08 (not significant!)" (y=178).
- **CI visual (right side):** red horizontal segment (450,55)–(550,55), width 3, with center dot — labeled "Fake narrow CI"; green segment (400,85)–(600,85) with center dot — labeled "Real wide CI".

## Denominator Manipulation (Changing What You Divide By)

**The Variant Changes the Denominator, Not the Behavior**

- **CTR inflation:** Showing fewer items per page halves impressions with zero behavior change.
- **Same click, "double" rate:** The very same single click now reads as twice the click-through rate.
- **Conversion inflation:** A pre-filter or longer flow drops low-intent users out of the denominator.
- **Revenue can fall:** The rate rises while total revenue declines, because fewer users ever enter.
- **Rule:** If the variant can mechanically move the denominator, use one it cannot affect.
- **Safe denominators:** Unique users randomized, or user-days — otherwise you measure arithmetic.

### Visualization (canvas `c3`, 720×300)

Two-box CTR comparison diagram.

- **Title (bold 17px, `#1a5276`, centered):** "Denominator Manipulation: Same Clicks, Different \"CTR\"".
- **Box A (blue `#2980b9`, alpha-0.2 fill + 2px stroke, rect 50,45,280×60):** bold blue centered text "A: 20 items shown, 1 click" / "CTR = 1/20 = 5%".
- **Box B (red `#e74c3c`, alpha-0.2 fill + 2px stroke, rect 390,45,280×60):** bold red centered text "B: 10 items shown, 1 click" / "CTR = 1/10 = 10%".
- **Bottom lines (centered):** bold red "\"B doubled CTR!\" — No. B halved impressions. User clicked same 1 item." (y=130); green `#27ae60` "Fix: clicks per USER per DAY — denominator the variant cannot change." (y=160); gray `#555` "Any variant that changes page layout, item count, or flow steps" (y=185) / "mechanically changes the denominator. Use user-level metrics only." (y=200).

## Simpson's Paradox from Mix Shift

**Aggregate Goes One Way, Every Segment Goes the Other**

- **Classic case:** Mobile and desktop conversion both improve, yet the aggregate rate declines.
- **The mechanism:** Mobile's traffic share grew, so the lower-converting channel carries more weight.
- **A/B test version:** "No effect" overall can hide +8% for new users and -3% for returning users.
- **Buried insight:** The aggregate reports a null result and hides two real, opposite effects.
- **Why it happens:** Sub-populations with different base rates shift their mix and flip the total.
- **Mix drivers:** Channel spend, seasonality, and growth demographics all reweight the population.
- **Fix:** Always segment by device, new/returning, geo, and channel before reading the aggregate.
- **Decision rule:** If segments diverge from the aggregate, act on the segments, not the total.

### Visualization (canvas `c4`, 720×300)

Stacked-bar mix-shift diagram (before vs after).

- **Title (bold 17px, `#1a5276`, centered):** "Simpson's Paradox in A/B Tests".
- **Before column (x=100, width 120):** light blue `#2980b9` "Mobile 40%" segment (y=50, h=50) atop dark blue `#1a5276` "Desktop 60%" segment (y=100, h=75, white label). Labeled "Before" below (y=160).
- **After column (x=300, width 180):** light blue "Mobile 60%" (y=50, h=50) atop dark blue "Desktop 40%" (y=100, h=45, white label). Labeled "After".
- **Right annotations (bold red `#e74c3c`, right-aligned):** "Both improved!" (y=75); "Aggregate: declined." (y=100).
- **Caption (17px `#555`, centered, y=185):** "Mix shifted toward lower-converting channel → aggregate falls while each improves.".

## Novelty Effect — Testing Too Short

**Day 1-3 Results ≠ Steady-State Behavior**

- **The pattern:** Any visible UI change spikes engagement for ~72 hours, then decays to steady state.
- **Novelty artifact:** Steady state is often +0%, so calling the test on day 3 ships pure novelty.
- **Reverse trap:** Compounding effects like retention are invisible at day 3, dominant by day 30.
- **Backwards priority:** Short tests amplify the least valuable effects and miss the most valuable.
- **Cycle bias:** Weekday-only or single-week tests catch different users and different paycheck phases.
- **Timing decides:** Same variant, different week of the month, different result on the same metric.
- **Fix:** Run at least 2 full business cycles and plot the measured effect day by day.
- **Reading the curve:** Still growing means the effect is real; big-then-declining means novelty.

### Visualization (canvas `c5`, 720×300)

Exponential-decay curve of measured effect over 30 days.

- **Title (bold 17px, `#1a5276`, centered):** "Novelty Effect: Day 3 ≠ Day 30".
- **Decay curve (red `#e74c3c`, width 2.5):** y = 150 − 110·exp(−d/3.5) for days d=0..29, x = 60 + d·20 — spikes high early, decays to baseline.
- **True-effect line:** dashed green `#27ae60` (dash 6/4) horizontal at y=150 from x=60 to x=660, labeled right-aligned "True effect: +0%".
- **Peak label (bold red, at 90,55):** "+15% Day 1".
- **Decision marker:** dashed orange `#e67e22` (dash 3/3, width 1.5) vertical line at x=120 from y=40 to y=170, labeled below (orange, centered) "Day 3: \"Ship it!\"".
- **Caption (bold 17px `#555`, centered, bottom):** "Min 2 full weeks. If effect declining → it's novelty, not improvement.".

## Wrong Level Hides the Real Problem

**Aggregate "Error Rate" Hides Whether It's 1 User or 1000**

- **Same rate, different severity:** A "1% request error rate" can be one user stuck in a retry loop.
- **Or the opposite:** The same 1% can be 100 different users each failing a single request once.
- **Metric can't tell:** Request-level error rate cannot distinguish a broken bot from a real outage.
- **Revenue version:** "$45 average order value" may sit inside a bimodal spend distribution.
- **Nobody at the mean:** Nobody spends near $45, so the average optimizes for a nonexistent customer.
- **Fix:** Report a distribution — a histogram, or p10/p50/p90 — instead of a single mean.
- **Dual denominators:** Always give error rate as both % of requests AND % of users affected.

### Visualization (canvas `c6`, 720×300)

Two-scenario comparison boxes for the same "1% error rate".

- **Title (bold 17px, `#1a5276`, centered):** "\"1% Error Rate\" — Is It 1 User or 100?".
- **Scenario A box (red `#e74c3c`, alpha-0.2 fill + stroke, rect 40,45,300×70):** centered text (17px `#333`) "Scenario A: 1 user × 100 errors" / "(retry loop — low severity)" / "0.1% of users affected".
- **Scenario B box (red, rect 380,45,300×70):** "Scenario B: 100 users × 1 error" / "(widespread — high severity)" / "10% of users affected".
- **Bottom lines (centered):** bold red "Both show \"1% request error rate.\" Completely different problems." (y=140); green `#27ae60` "Fix: always report BOTH % of requests AND % of users affected." (y=165) / "Distribution (histogram of errors-per-user) reveals the real shape." (y=185).

## Time Window Selection Bias

**Monthly Churn 5% Sounds Manageable. Annual = Lost Half Your Customers.**

- **The compounding trap:** 5% monthly churn compounds to losing 46% of customers over one year.
- **Opposite response:** Same metric, different timeframe, completely different organizational urgency.
- **The snapshot trap:** A stable DAU/MAU of 25% can hide 100% monthly churn underneath it.
- **Hidden dynamics:** Constant inflow replaces outflow, so the ratio holds while the business leaks.
- **Window cherry-pick:** Holiday-quarter revenue "grew 40%" on the quarter-over-quarter comparison.
- **Same quarter last year:** Against the year-ago quarter that same growth is only 3%.
- **Fix:** Report daily, weekly, monthly, and same-period-last-year views side by side.
- **The real finding:** If the story changes between timeframes, that change itself is the finding.

### Visualization (canvas `c7`, 720×300)

Compounding-churn decay curve over 12 months.

- **Title (bold 17px, `#1a5276`, centered):** "5% Monthly Churn → 46% Annual Loss".
- **Curve (red `#e74c3c`, width 2.5):** cumulative loss for months m=0..12: y = 55 + 90·(1 − 0.95^m), x = 80 + m·48 (retained base decaying from 100% to 54%).
- **X labels (17px `#333`, centered):** "Month 0" (x=80), "Month 6" (x=360), "Month 12" (x=650), all at y=170.
- **Endpoint labels (bold red):** "100%" at (50,60); "54%" at (660,145).
- **Caption (17px `#555`, centered, bottom):** "\"Only 5% churn\" per month. Annualized: lost nearly HALF the customer base.".

## Per-Impression vs Per-User Economics

**Ad Revenue Per Impression Grows While Revenue Per User Falls**

- **The ad trap:** More ads lift revenue per impression while simultaneously driving DAU downward.
- **Looks fine today:** Net revenue holds, but user loss compounds and RPM gains cannot keep pace.
- **Subscription version:** A price hike lifts ARPU this month by pushing marginal subscribers to churn.
- **Six months later:** Fewer users at higher ARPU adds up to lower total revenue than before.
- **Wrong timeframe:** Per-impression and per-user metrics give instant, flattering feedback.
- **Lagging damage:** The LTV harm they cause only surfaces months later, when it is too late to undo.
- **Fix:** Pair efficiency metrics (per-X) with volume metrics (total-X) and LTV leading indicators.
- **Diagnosis:** Efficiency up with volume down means you are harvesting the base, not growing it.

### Visualization (canvas `c8`, 720×300)

Text-panel arithmetic showing short-term gain vs compounding loss.

- **Title (bold 17px, `#1a5276`, centered):** "RPM Up + DAU Down = Short-Term Gain, Long-Term Loss".
- **Green lines (`#27ae60`, left-aligned at x=60):** "Revenue/impression: +10% ✓" (y=55); "Impressions/user:   +30% ✓ (more ads)" (y=80).
- **Red line:** "Daily active users: -20% ✗ (users leaving)" (y=105).
- **Body (`#333`):** "Net today: 1.10 × 1.30 × 0.80 = +14% revenue. Looks great!" (y=135).
- **Bold red:** "But DAU decline COMPOUNDS. Next quarter: 1.10 × 1.30 × 0.64 = -8% revenue." (y=160).
- **Caption (17px `#555`, centered, y=185):** "Efficiency metrics (per-X) optimize present. Volume metrics (total-X) predict future.".

## Geographic / Demographic Aggregation Hiding Inequity

**"Average Latency 200ms" When Half Your Users Get 2 Seconds**

- **CDN coverage bias:** Two populations with 10× different latency average to a middle number.
- **Nobody's number:** Neither group experiences that mean, so the "average user" does not exist.
- **Global A/B test:** A variant helps the majority region and hurts a smaller one, netting out positive.
- **Growth market harmed:** The hidden loser is often your fastest-growing market, degraded silently.
- **Demographic aggregation:** "92% accuracy" overall can conceal 78% accuracy for a minority group.
- **Review passes anyway:** The aggregate clears the bar while the model underserves those who need it.
- **Fix:** Split every metric by geography, device tier, and demographic group before shipping.
- **Constraint shape:** Set floor constraints ("no group below X") rather than average targets.

### Visualization (canvas `c9`, 720×300)

Two horizontal latency bars with a misleading average marker.

- **Title (bold 17px, `#1a5276`, centered):** "\"Average Latency 200ms\" — Nobody Experiences the Average".
- **Bar 1:** green `#27ae60` rect (100,50,200×30), centered label (17px `#333`) "US/EU: 50ms (60% users)".
- **Bar 2:** red `#e74c3c` rect (100,90,500×30), label "SEA/Africa: 500ms (40% users)".
- **Average marker:** dashed orange `#e67e22` (dash 5/5, width 2) vertical line at x=280 from y=45 to y=130, labeled bold orange "Avg: 230ms" (y=140).
- **Captions (17px `#555`, centered):** "Two populations with 10× different experience. Average represents neither." (y=165); "Set FLOOR constraints (no group below X) not average constraints (mean above Y)." (y=188).

## Survivor Bias in Denominator (Who Gets Counted)

**Measuring Only Users Who Didn't Leave**

- **NPS of active users only:** A high score among survivors means the unhappy 30% already churned.
- **Denominator filtered:** Those detractors left the denominator, so the score rose without anyone changing.
- **Engagement among remaining users:** When the least-engaged users leave, per-user engagement rises.
- **By construction:** Nobody became more engaged; the population being averaged simply got better.
- **A/B test version:** If variant B churns low-spenders early, day-30 revenue per user looks higher.
- **Unequal subsets:** That number compares a higher-quality surviving subset, not a better treatment.
- **Fix:** Use intent-to-treat — once randomized, a user stays in the denominator forever.
- **Correct reporting:** Report revenue per RANDOMIZED user, never revenue per active user.

### Visualization (canvas `c10`, 720×300)

Two-box cohort comparison: full cohort vs survivors-only NPS.

- **Title (bold 17px, `#1a5276`, centered):** "Survivor Bias: Measuring Only Who Stayed".
- **Left box (blue `#2980b9`, alpha-0.3 fill + stroke, rect 50,45,280×50):** blue centered label "Full cohort: NPS = +5"; below it (red, y=115): "30% churned (NPS = -60)".
- **Right box (green `#27ae60`, alpha-0.3 fill + stroke, rect 400,45,270×50):** green centered label "Survivors only: NPS = +45!".
- **Bottom lines (centered):** bold red `#e74c3c` "NPS +45 because the unhappy people ALREADY LEFT the denominator." (y=140); gray `#555` "Intent-to-treat: once randomized, they count FOREVER. Revenue per RANDOMIZED user." (y=165) / "Engagement \"rising\" after churn spike = mechanical artifact, not real improvement." (y=188).

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: one `<h2>` (1.4em `#1a5276`, 2px solid `#2980b9` bottom border) per pitfall, followed by a full-width single-row table; left `<td>` (40%) holds `.obj-title` (the bold sub-heading above) + `<ul>` of bold-labeled bullets, right `<td>` (60%, centered) holds the canvas. Even rows background `#fafcfe`. No Example paragraphs on this page.
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; `.subtitle` `#666` 1.05em; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em weight 600 `#1a5276`; `strong` `#1a5276`; `ul` 0.9em. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300 per chart; shared `setup(id)` helper reads the width/height attributes, sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`), fixes the CSS size, and calls `ctx.scale` so drawing stays in logical coordinates. All chart text is 17px -apple-system (titles bold); most charts are annotated text/box diagrams rather than plotted data.
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, grays `#333`/`#555`.
- In regenerated HTML, any card/page links use `.html` extensions (this page has none).
