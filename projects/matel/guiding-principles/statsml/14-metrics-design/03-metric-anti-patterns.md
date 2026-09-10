# Metric Anti-Patterns

**Page type:** detail page (two-column obj-table layout: text left 40%, canvas right 60%, one h2 + one-row table per anti-pattern, even rows shaded)
**HTML title tag:** Metric Anti-Patterns

**Subtitle:** Common patterns in metric design that guarantee gaming, misinterpretation, or invisible failure. Each one with a real-world example and visualization.

## 1. Single Metric Without Counter-Metric

**Obj-title:** Optimize one number without checking what it damages

- **Wells Fargo:** Measured "accounts opened per employee." No counter (complaints, account closure rate). Employees opened millions of FAKE accounts. $3B fines, CEO fired, brand destroyed.
- **Uber:** "Rides completed" without "driver safety incidents." More rides = more pressure to speed = more accidents.
- **Video platform:** "Watch time" without "user regret survey." Maximized time = promoted addictive/outrage content.

**Fix:** Every primary metric needs a counter: Handle time + Resolution. Speed + Safety. Conversion + Return rate. If primary improves and counter degrades = gaming.

### Visualization (canvas `ca1`, 720×300)

Two time series over eight quarters: the dashboarded primary metric rising, and the unmeasured counter compounding beneath it.

- **Title (bold 14px `#1a5276`, centered):** "The dashboard charted one line; the other one existed anyway".
- **Axes:** x = Q1…Q8 (11px `#6b7280` labels), y unlabeled index 0–140; axis lines `#e5e9ef`; plot area ~x 70–600, y 60–240.
- **Series 1 (solid `#1a5276`, 2.5px, dots r=3):** accounts opened (indexed) `[40, 48, 58, 70, 84, 100, 118, 138]`, right-end label bold 12px "accounts opened — on the dashboard".
- **Series 2 (dashed 6/4 `#e74c3c`, 2px):** unauthorized-account complaints (indexed) `[2, 3, 5, 9, 15, 24, 38, 60]`, right-end label bold 12px "complaints — never charted".
- **Bottom caption (italic 11px `#6b7280`, centered):** "Illustrative shapes — the counter-metric accumulates whether or not anyone charts it."

## 2. Measuring Activity, Not Outcome

**Obj-title:** Count what people DO, not what VALUE it creates

- **IBM (1990s):** "Lines of code" as productivity metric. Team rewrote 10K lines into 2K (cleaner, faster, fewer bugs). Manager: "You LOST 8,000 lines of productivity."
- **Police departments:** "Arrests made" as performance metric → incentivizes arresting for minor offenses, not reducing crime.
- **Content teams:** "Blog posts published per month" → incentivizes quantity over quality.

**Fix:** Outcome metrics: "user adopted feature" not "feature shipped." "Crime rate dropped" not "arrests made."

### Visualization (canvas `ca2`, 720×300)

Slopegraph: the same two teams ranked by an activity metric and by an outcome metric — the order inverts.

- **Title (bold 14px `#1a5276`, centered):** "Two rankings of the same two teams".
- **Two rank columns:** left axis label "ranked by lines of code" at x≈210, right axis label "ranked by defects per feature (lower is better)" at x≈510 (11px `#6b7280`, above the columns); rank slots at y=110 (1st) and y=210 (2nd), marked "1st" / "2nd" at the far left (11px `#6b7280`).
- **Team A (orange `#e67e22`, 2.5px line, dots r=4):** left slot 1st, right slot 2nd (line crosses down); labels at each end: "Team A — 10,000 LoC" (left), "26 defects" (right), 12px in series color.
- **Team B (blue `#1a5276`):** left slot 2nd, right slot 1st (line crosses up); labels "Team B — 2,000 LoC" / "6 defects".
- **Bottom caption (italic 11px `#6b7280`, centered):** "Illustrative counts — the activity metric and the outcome metric disagree about which team is better."

## 3. Redefining the Metric When It Looks Bad

**Obj-title:** Change what the number MEANS instead of fixing the business

- **Microblogging platform:** "mDAU" → "average mDAU" → stopped reporting entirely. Each redefinition prevented comparison.
- **Groupon (pre-IPO):** Invented "Adjusted CSOI" — a metric so customized it excluded all their actual costs. SEC made them restate.
- **Peloton:** Changed "connected fitness subscribers" definition to include users who hadn't used their bike in 90 days.

**Fix:** Lock definitions at creation. If you must change: new metric, new name, new baseline. Show discontinuity explicitly.

### Visualization (canvas `ca3`, 720×300)

One time axis, three line segments: the old definition falls, the new definition "recovers," and the old definition keeps falling unreported.

- **Title (bold 14px `#1a5276`, centered):** "The chart recovered; the measured thing did not".
- **Axes:** 12 x-positions, y index 30–110; plot ~x 70–620, y 55–245; axis lines `#e5e9ef`.
- **Old definition, reported (solid `#1a5276`, 2.5px):** indices 0–5 of `[80, 84, 86, 85, 82, 76, 70, 64, 58, 53, 49, 46]`.
- **Old definition, no longer reported (dashed 5/4 `#1a5276` at 45% alpha, 2px):** indices 5–11 of the same array; right-end label 11px muted blue "old definition, unreported".
- **New definition (solid `#e67e22`, 2.5px):** indices 5–11 `[76, 88, 91, 93, 96, 98, 99]` mapped so it starts at the switch point; right-end label bold 12px orange "new definition, reported".
- **Switch marker:** vertical dashed `#6b7280` line at index 5, label above (11px): "definition changed".
- **Bottom caption (italic 11px `#6b7280`, centered):** "Illustrative series — a rename produced the recovery; the original series never turned."

## 4. Average Hiding Bimodal Reality

**Obj-title:** Report one number that represents a state nobody actually experiences

- **Bank loan approval "average 3 days":** 70% auto-approved in 1 day. 30% manual review: 14+ days (disproportionately minority applicants). "3-day average" hid systematic discrimination.
- **Airline "on-time performance 82%":** 95% of flights ±10 minutes. 5% delayed 3+ hours. That 5% = thousands of missed connections.
- **"Average employee tenure: 4 years":** Senior staff stays 15+ years, new hires leave in 8 months. Nobody stays "4 years."

**Fix:** Report p50, p90, p99 minimum. Check for bimodality. If p99/p50 > 10×: you have two populations. Report them separately.

### Visualization (canvas `ca4`, 720×300)

Bimodal histogram of approval times with the reported average landing in an empty bin.

- **Title (bold 14px `#1a5276`, centered):** "The mean sits where no applicant is".
- **Histogram (20 day-bins, x axis "days to approval" labeled 1, 5, 10, 15, 20):** counts — day 1 = 62, day 2 = 8, day 13 = 10, day 14 = 12, day 15 = 6, day 16 = 2; all other bins zero. Bars filled `rgba(26,82,118,0.35)` with `#1a5276` 1px stroke; plot ~x 70–630, baseline y=235, top y=60.
- **Average marker:** vertical dashed `#e67e22` line (dash 5/4, 2px) at day 5, labeled above in bold 12px orange: "the reported average" — deliberately in a bin with zero count.
- **Cluster annotations (11px `#6b7280`):** "70% auto-approved" over the day-1 bar; "30% manual review" over the day-13–16 cluster.
- **Bottom caption (italic 11px `#6b7280`, centered):** "Illustrative distribution — the average describes neither population."

## 5. Success-Only Dashboard (Hiding Failures)

**Obj-title:** Only show metrics that can't look bad

- **Co-working startup pre-IPO:** Reported: memberships (growing), revenue (growing). Hidden: occupancy declining, unit economics negative, burning $2B/year. S-1 revealed failure metrics → IPO cancelled → 80% crash.
- **Medical testing startup:** Reported: "tests run" (growing). Hidden: accuracy rate (catastrophic), failed QC (90%+). Success metrics funded the company for a decade while the product didn't work.

**Fix:** Every success metric needs a paired failure metric. Revenue AND refunds. Users gained AND users churned.

### Visualization (canvas `ca5`, 720×300)

Diverging bar chart around a zero axis: year-over-year changes above the line made the deck; the ones below did not.

- **Title (bold 14px `#1a5276`, centered):** "Every bar is true — the deck stopped at the axis".
- **Zero axis:** horizontal `#2a2a2a` 1px line at y=150 spanning x 80–650; label at right end above the line (11px `#6b7280`): "in the deck", below the line: "not in the deck".
- **Bars (width 80, gap 28, y-scale 0.9 px per %):** Memberships +42 and Revenue +36 above the axis, fill `rgba(26,82,118,0.35)` stroke `#1a5276`; Occupancy −18, Unit margin −45, Free cash flow −80 below the axis, fill `rgba(231,76,60,0.3)` stroke `#e74c3c`. Value labels (bold 12px, series color) at each bar's far end; category labels (11px `#6b7280`) at the axis.
- **Bottom caption (italic 11px `#6b7280`, centered):** "Illustrative YoY changes — the selection, not the numbers, does the lying."

## 6. 2,400 Dashboards Nobody Looks At

**Obj-title:** Creating dashboards feels productive. Using them isn't anyone's job.

- **Fortune 500 Tableau audit:** 2,400 dashboards. 85% not viewed in 6 months. 40% had broken data connections (nobody noticed). Cost: $800K/year. Value: ~zero.
- **Monitoring tool sprawl:** 500 total metrics across 4 tools. Alert fatigue → all ignored. Real incident: buried in noise.

**Fix:** Metric registry requires OWNER + REVIEW FREQUENCY + THRESHOLD + ACTION PLAN. Missing any field → auto-flagged for deletion after 90 days.

### Visualization (canvas `ca6`, 720×300)

Waffle chart: a 10×10 grid where each square stands for 24 dashboards.

- **Title (bold 14px `#1a5276`, centered):** "An audit of 2,400 dashboards".
- **Waffle (10×10 squares, 19×19 px each, 5px gaps, grid origin ~x=110, y=52):** filled row-major — first 15 squares `rgba(26,82,118,0.75)` (viewed in the last 6 months), next 40 `rgba(230,126,34,0.55)` (broken data connection), remaining 45 `#dfe3e8` (live but abandoned).
- **Legend (right of the waffle, 12px `#2a2a2a`, swatch 13×13):** "Viewed in last 6 months — 360", "Broken connection — 960", "Live but abandoned — 1,080"; beneath in 11px `#6b7280`: "one square ≈ 24 dashboards".
- **Bottom caption (italic 11px `#6b7280`, centered):** "$800K per year in licenses and refresh compute keeps all 100 squares warm."

## 7. Metrics Interpreted Politically, Not Technically

**Obj-title:** The score becomes the product, not the insight

- **Investor pressure:** Metrics MUST go up every quarter. If they don't → redefine, change benchmark, adjust methodology.
- **Positive interpretation bias:** Score drops 5% → "within normal range." Rises 2% → "significant improvement!"
- **Cash flow manipulation:** Company delays salary increases, holds vendor payments to make quarterly cash flow look positive. Creates ARTIFICIAL spikes/gaps in financial data.

**The damage:** Models trained on politically-managed data learn the MANAGEMENT PATTERN, not the underlying business reality.

### Visualization (canvas `ca7`, 720×300)

Grouped bars around a zero line: quarterly cash flow as reported vs with payments left on their original dates.

- **Title (bold 14px `#1a5276`, centered):** "One quarter borrowed from the next".
- **Zero axis:** `#2a2a2a` 1px at y=160, x 90–630; quarter labels Q3, Q4, Q1 beneath (11px `#6b7280`).
- **Bars (per quarter, two bars 52px wide, 8px apart, scale ~7 px per $M):** "as reported" `rgba(26,82,118,0.35)` stroke `#1a5276`: +3, +5, −9; "payments on original dates" `rgba(230,126,34,0.5)` stroke `#e67e22`: +3, −6, +2. Value labels ("+$5M" etc.) bold 11px in stroke color at each bar end.
- **Legend (12px, top left):** swatches for the two series.
- **Bottom caption (italic 11px `#6b7280`, centered):** "Illustrative $M — the three quarters sum to the same total; only the boundary moved."

## 8. CTR — Measures the Promise, Not the Delivery

**Obj-title:** Curiosity Gap Exploitation

- **What CTR measures:** "Did the user take the bait?" Not: "Did they get value after clicking?"
- **Clickbait wins:** "You won't BELIEVE..." → 70% CTR, 90% bounce in 3s. Honest title → 35% CTR, 80% full engagement. Algorithm promotes the lie.
- **E-commerce:** Misleading product photo → high CTR → ranked higher → 30% return rate. CTR optimization SELECTS FOR deception.

**Fix:** "Satisfied CTR" = CTR × (1 - bounce rate). Or: "regret-adjusted CTR" = exclude clicks followed by immediate back-button.

### Visualization (canvas `ca9`, 720×300)

Scatter of headlines on two axes — click-through vs post-click reading — with the ranker's one-dimensional view marked on the x-axis.

- **Title (bold 14px `#1a5276`, centered):** "The promise and the delivery are different axes".
- **Axes:** x = CTR % (0–80, ticks 0/20/40/60/80), y = "read past 30 s" % (0–100, ticks 0/50/100); axis lines `#e5e9ef`, labels 11px `#6b7280`; plot ~x 90–620, y 55–225.
- **Clickbait cluster (orange `#e67e22` dots r=5):** (62,12), (68,8), (71,15), (75,6), (66,10); cluster label 12px orange "clickbait".
- **Honest cluster (blue `#1a5276` dots r=5):** (28,72), (33,80), (37,66), (41,74), (30,85); cluster label 12px blue "honest titles".
- **Ranker annotation:** a bold arrow along the x-axis (just below it, `#2a2a2a`) labeled 11px "the ranking algorithm sees only this axis".
- **Bottom caption (italic 11px `#6b7280`, centered):** "Illustrative points — ranked by CTR alone, every orange dot beats every blue one."

## 9. Metric Becomes Goal — Original Purpose Forgotten

**Obj-title:** Goodhart's Law at its most destructive

- **Calories:** Proxy for "balanced nutrition at lower volume." Reality: people eat 1500 cal of candy bars.
- **Steps:** "10,000 steps/day = healthy." Reality: shuffling to kitchen 50×. 30 min HIIT = 500 steps but vastly healthier.
- **GDP:** Proxy for prosperity. Hurricane → massive rebuilding → GDP UP. Divorce → two households → GDP UP.
- **Test scores:** Proxy for "learning." Teach to the test → scores up, actual understanding flat.

**The pattern:** Every metric was ORIGINALLY a proxy for something harder to measure. Over time, people forget the original purpose and optimize the proxy ITSELF.

### Visualization (canvas `ca8`, 720×300)

Proxy-vs-outcome scatter: the correlation that justified the proxy, and the horizontal path an optimizer takes along it.

- **Title (bold 14px `#1a5276`, centered):** "The correlation was real — until the proxy became the target".
- **Axes:** x = steps per day (thousands, ticks 0/5/10/15), y = "health benefit (index)" (0–100); axis lines `#e5e9ef`, labels 11px `#6b7280`; plot ~x 90–620, y 55–230.
- **Population cloud (blue `#1a5276` dots r=4.5):** (2,15), (3.5,26), (5,34), (6,45), (7.5,55), (9,63), (10.5,72), (12,80) — the diagonal that made "steps" a credible proxy; label 12px blue near the top of the cloud: "population, before targeting".
- **Optimizer path (orange `#e67e22`):** arrow from (6,45) horizontally to (11.5,46) with two orange dots at (8,46) and (10,46); label 12px orange under the arrow: "chasing the target: proxy up, outcome flat".
- **Target line:** vertical dashed `#6b7280` at x=10, label 11px "10,000-step target".
- **Bottom caption (italic 11px `#6b7280`, centered):** "Illustrative — optimizing moves you along the proxy axis, not the outcome axis (Goodhart's law)."

## 10. GMV — Vanity Dollar Sign That Hides Everything

**Obj-title:** The headline is 200× the reality

- **What it claims:** "We moved $10B in goods!" Sounds like revenue.
- **Reality:** Take rate = 5%. Revenue = $500M. After costs and subsidies: might be LOSING money.
- **Not independent:** One whale seller = 30% of GMV. They leave → "GMV crashed 30%!"

**Fix:** Revenue. Gross profit. Contribution margin per transaction. Anything that reflects what the PLATFORM earns.

### Visualization (canvas `ca10`, 720×300)

Magnified waterfall: the full GMV bar on a billions scale, its 5% take-rate slice blown up onto a millions scale where the waterfall to profit actually happens.

- **Title (bold 14px `#1a5276`, centered):** "A $10B headline, magnified 20×".
- **Left bar (x=60, width 90):** GMV $10B from baseline y=250 up to y=65, fill `rgba(26,82,118,0.35)` stroke `#1a5276`, value label bold 12px above; the bottom 5% of its height filled solid `#1a5276` and labeled at left (11px) "take ≈ 5%".
- **Magnifier:** two dashed `#6b7280` lines from the slice's top-right and bottom-right corners to the right panel's top-left and bottom-left (~x=250, y=70 and y=250).
- **Right panel — waterfall on a $M scale (bars 80px wide, gap 22, baseline y=250, scale 0–500 over 180px):** Revenue $500M (full bar, `rgba(26,82,118,0.35)` stroke `#1a5276`); − Costs $300M (floating step from 500 down to 200, fill `rgba(231,76,60,0.3)` stroke `#e74c3c`); − Subsidies $150M (floating from 200 down to 50, same red); Profit $50M (bar 0→50, fill `rgba(39,174,96,0.4)` stroke `#27ae60`). Thin `#6b7280` connector lines between step tops; value labels bold 11px; category labels 10px `#6b7280` beneath.
- **Scale notes (10px `#6b7280`):** "$B scale" under the left bar, "$M scale" under the right panel.
- **Bottom caption (italic 11px `#6b7280`, centered):** "The first bar and the last differ by 200× — they cannot share one axis, which is the point."

## 11. "Monthly Active Users" — Active Means Whatever You Want

**Obj-title:** Definition inflation

- **"Active" varies wildly:** Logged in once? Opened app 1 second? Received push notification? Bot pinged server? Browser auto-refreshed?
- **Redefinition trick:** MAU drops 10%. Redefine "active" (was: sent message; now: opened app OR received notification). Numbers recover. Reality unchanged.

**Fix:** "Users who performed [core action] at least [N] times in [period]." Specific, tied to value delivery, resistant to inflation.

### Visualization (canvas `ca11`, 720×300)

Horizontal bar ladder: five defensible definitions of "active," one product, one month, on a shared axis.

- **Title (bold 14px `#1a5276`, centered):** "Five defensible definitions of 'active' — an 11× spread".
- **Axis:** x 0–500M with gridline ticks every 100M (lines `#e5e9ef`, labels 10px `#6b7280` at the bottom); bars start at x=225.
- **Bars (height 26, rows 38px apart, fill `rgba(26,82,118,0.35)` stroke `#1a5276`):** Sent a message 45M; Used the core feature 120M; Opened the app 280M; Received a push / background refresh 420M; Any server ping 500M. Definition labels right-aligned at x=215 (12px `#2a2a2a`); values bold 12px `#1a5276` just past each bar end.
- **Bottom caption (italic 11px `#6b7280`, centered):** "Illustrative counts — same product, same month; only the definition moves."

## 12. NPS — One Number to Rule Them All (But Rules Nothing)

**Obj-title:** Bimodal hiding, timing bias, cultural bias, not actionable

- **Problem 1:** NPS 45 could be healthy (55% promoters, 10% detractors) OR troubled (60% promoters, 15% detractors). Same score, different reality.
- **Problem 2:** Ask after purchase = high. Ask after support ticket = low. Same customer.
- **Problem 3:** Americans give 9s/10s easily. Japanese rarely above 8. Cross-geo comparison = comparing culture, not satisfaction.
- **Problem 4:** NPS dropped 5 points. WHY? The number tells you nothing about what to fix.

**Fix:** NPS as STARTING POINT, never conclusion. Always segment. Always pair with "why" qualitative data.

### Visualization (canvas `ca12`, 720×300)

Two score distributions (0–10) side by side, both computing to NPS 45.

- **Title (bold 14px `#1a5276`, centered):** "Two companies, one score".
- **Panel A (left, header 12px `#2a2a2a` "Company A — NPS 45"):** counts per score 0–10: `[0, 1, 1, 1, 1, 2, 4, 15, 20, 20, 35]` (detractors 10, passives 35, promoters 55). Bars ~24px wide from x=60, baseline y=215, 2.2 px per count; score labels 0–10 beneath (9px `#6b7280`).
- **Panel B (right, from x=395, header "Company B — NPS 45"):** `[2, 2, 2, 2, 2, 2, 3, 13, 12, 15, 45]` (detractors 15, passives 25, promoters 60).
- **Segment colors (both panels):** scores 0–6 `rgba(230,126,34,0.7)` (detractors), 7–8 `#c8ced6` (passives), 9–10 `rgba(26,82,118,0.6)` (promoters); small legend under the title.
- **Bottom caption (italic 11px `#6b7280`, centered):** "Illustrative distributions — identical NPS; B carries half again as many detractors."

## 13. "Time on Page" — Confusion Looks Like Engagement

**Obj-title:** You CANNOT distinguish engagement from frustration without additional signals

- **Reality A (20%):** Deeply reading valuable content. Genuinely engaged.
- **Reality B (40%):** CONFUSED. Can't find what they want. Scrolling desperately.
- **Reality C (30%):** Went to make coffee. Tab open. Zero engagement.
- **Reality D (10%):** Filling out a form that's too long. Measuring frustration.

**Fix:** Time + scroll depth + click events + outcome. "Engaged time" (mouse moving) vs "idle time" (tab in background).

### Visualization (canvas `ca13`, 720×300)

One stacked horizontal bar: the reported 8:00 average decomposed into its four experiences.

- **Title (bold 14px `#1a5276`, centered):** "One average, four experiences".
- **Bracket (above the bar, `#2a2a2a` 1px with end ticks):** spanning the full bar, labeled bold 12px centered: "reported: 8:00 average time on page".
- **Stacked bar (x 80–660, y=130, height 46):** segments proportional to share — Reading 20% fill `rgba(26,82,118,0.6)`; Confused, scrolling 40% fill `rgba(230,126,34,0.6)`; Tab open, away 30% fill `#c8ced6`; Stuck in a form 10% fill `rgba(231,76,60,0.5)`; thin white separators.
- **Segment labels (below the bar, 11px, two lines each — name in `#2a2a2a`, time in `#6b7280`):** "reading — 1:36", "confused, scrolling — 3:12", "tab open, away — 2:24", "stuck in a form — 0:48"; leader ticks from bar to label.
- **Bottom caption (italic 11px `#6b7280`, centered):** "Illustrative split — only the first segment is what the metric is assumed to mean."

## 14. "Revenue Growth" Without Cohort Decomposition

**Obj-title:** Growth looks great until you run out of new people to burn through

- **Hidden:** January customers retained at 40%. February at 30%. March at 20%. Each cohort WORSE.
- **Why hidden:** Total revenue grows because NEW > churned. For now. Deteriorating cohorts eventually cliff.
- **Co-working startup:** Revenue growing impressively. Each new location less profitable. Unit economics deteriorating while topline grew.

**Fix:** ALWAYS show cohort retention alongside aggregate growth. Declining cohorts = growth is temporary (treadmill).

### Visualization (canvas `ca14`, 720×300)

Two aligned panels over the same months: the aggregate revenue line that gets shown, and the per-cohort retention bars that don't.

- **Title (bold 14px `#1a5276`, centered):** "The same business, charted twice".
- **Top panel (y ~48–125):** aggregate revenue line (solid `#1a5276`, 2.5px, dots r=3) `[20, 35, 52, 70, 85, 95, 100]` over Jan–Jul, x 90–620; panel label 11px `#6b7280` left: "aggregate revenue — the chart that gets shown".
- **Bottom panel (y ~160–255):** bars (width 44, fill `rgba(230,126,34,0.55)` stroke `#e67e22`): 3-month retention by signup cohort Jan–Jun: 40%, 34%, 28%, 22%, 17%, 13%; value labels 11px orange above bars; panel label 11px `#6b7280`: "3-month retention by cohort — the chart that doesn't".
- **Shared x labels:** month abbreviations 10px `#6b7280` under the bottom panel.
- **Bottom caption (italic 11px `#6b7280`, centered):** "Illustrative — the topline compounds while every successive cohort retains worse."

## 15. Trivially Improvable Metrics (The Degenerate Shortcut)

**Obj-title:** Can someone make this go UP by making the product WORSE?

- **CTR → Clickbait:** More sensational headlines. CTR up, trust erodes.
- **Engagement → Outrage:** Show anger-inducing content. Dwell time up, harm up.
- **CSAT → Cherry-pick surveys:** Only survey happy customers. Score up, product unchanged.
- **ESG Score → Report optimization:** Reframe existing practices. Score up, emissions unchanged.
- **University Rankings → Selectivity gaming:** Get more applicants, reject more. Education quality identical.
- **Response Time SLA → Autoresponders:** Auto-acknowledge within 4 hours. Problem unresolved 3 weeks.
- **Uptime SLO → Deployment freeze:** Stop all deploys last 3 weeks of quarter. Uptime hits 99.97%. Business-critical features (fraud rules, checkout fixes, security patches) sit unshipped. Cost: $2M+ in blocked value. The trivial shortcut to "site never goes down" is "site never changes."

**Design principle:** A well-designed metric has NO trivial shortcut. If the only way to improve is to actually improve the system — it's a good metric. The uptime freeze is the purest example: the easiest way to meet your SLO is to freeze the site — which makes the metric worthless as a proxy for "reliably serves users."

### Visualization (canvas `ca15`, 720×340)

One quantified shortcut: thirteen weeks of a quarter where uptime hits the SLO exactly when shipping stops.

- **Title (bold 14px `#1a5276`, centered):** "Hitting the SLO by stopping the work it protects".
- **Freeze band:** weeks 10–13 shaded `rgba(230,126,34,0.12)` full plot height, labeled at top (11px `#e67e22`): "deploy freeze".
- **Bars (left axis "deploys per week", 0–16, labels 10px `#6b7280`):** `[12, 14, 11, 13, 12, 15, 13, 12, 14, 3, 0, 0, 0]`, fill `rgba(26,82,118,0.35)` stroke `#1a5276`, plot x 90–600, baseline y=260, top y=70.
- **Line (right axis "rolling uptime %", 99.85–100.00, labels 10px `#6b7280`):** `[99.90, 99.88, 99.91, 99.89, 99.92, 99.90, 99.91, 99.89, 99.90, 99.95, 99.97, 99.97, 99.97]` in solid `#27ae60` 2.5px with dots r=3; dashed `#6b7280` horizontal at 99.95 labeled "SLO target".
- **X labels:** "wk 1" … "wk 13" every other week, 10px `#6b7280`.
- **Bottom caption (italic 11px `#6b7280`, centered):** "Illustrative quarter — uptime clears the target exactly when shipping stops; the metric stops measuring reliability work."

## 16. Percentage Without Sample Size (Unqualified Normalization)

**Obj-title:** Normalizing to percentage erases the evidence base from the published number

- **The format is lossy:** 99/100 and 99,000/100,000 both publish as "99%." Once normalized, the reader cannot recover whether there was sufficient data behind the claim.
- **The incentive:** collecting enough data for a proper evaluation requires effort and timeline. Publishing on whatever exists now is faster — and smaller datasets produce higher variance, making extreme (favorable) numbers more likely.
- **The exploitation:** the author can see the CI is wide, can see n is small, and publishes the bare percentage anyway — because the format looks self-contained to a non-technical audience and invites no follow-up questions.
- **ML papers:** "99% accuracy" on 100 curated examples. Startup pitch: "98% detection rate" on 50 positives. Product announcement: "95% satisfaction" from 47 opt-in respondents.

**Fix:** Require n, confidence interval, and dataset provenance alongside any published percentage. A number without these is a claim, not a finding — regardless of how many decimal places it carries.

### Visualization (canvas `ca16`, 720×300)

Forest plot: the same published "99%" at four sample sizes, with its confidence interval on a shared axis.

- **Title (bold 14px `#1a5276`, centered):** "Four results publish the same number".
- **Shared axis:** x = accuracy 88–100%, gridlines at 88/90/92/94/96/98/100 (`#e5e9ef` verticals, labels 10px `#6b7280` at the bottom); plot x 200–640.
- **Rows (y = 75, 120, 165, 210; left labels right-aligned 12px `#2a2a2a` at x=120; published value «"99%"» in bold 12px `#2a2a2a` right of each row at x=655):**
  - n=50 — whisker [89.5, 99.8]
  - n=100 — whisker [94.6, 99.9]
  - n=1,000 — whisker [98.1, 99.5]
  - n=100,000 — whisker [98.9, 99.1]
- **Whiskers:** horizontal `#1a5276` 2px lines with 8px end ticks; point estimate: orange `#e67e22` dot r=4 at 99 on every row.
- **Bottom caption (italic 11px `#6b7280`, centered):** "Approximate 95% intervals for 99% at each n — the interval is what the percentage format deletes."

## Regeneration instructions

- **Layout:** detail page. h1, `.subtitle`, then per anti-pattern: `<h2>N. Title</h2>` (h2 1.3em `#1a5276`, bottom border `2px solid #2980b9`, padding-bottom 6px) followed by a one-row `.obj-table` — left `<td>` (40%) holds `.obj-title`, bullets, and a fix/pattern `<p>`; right `<td>` (60%, centered) holds the canvas. `.obj-table tr:nth-child(even) td` background `#fafcfe`.
- **Canvas IDs:** note section 8 uses `ca9` and section 9 uses `ca8` (the ids are swapped relative to section order in the source); all others are `caN` matching their order (`ca1`–`ca7`, `ca10`–`ca16`). Sizes 720×300 except `ca15` at 720×340.
- **Page style:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; subtitle `#666` 1.05em; ul 0.9em `#333`; `strong` `#1a5276`; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em weight 600 `#1a5276`. No nav bar, no back/home links.
- **Canvas:** shared `setup(id)` reads intrinsic `width`/`height` attributes, sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates; charts redraw on window resize (debounced). All data arrays are hardcoded literals — never `Math.random()`.
- **Chart typography:** titles bold 14px `#1a5276` centered at y≈20; axis/segment labels 10–12px `#6b7280` or `#2a2a2a`; every chart ends with a bottom caption in italic 11px `#6b7280` marking the data as illustrative. No all-caps exclamations; series names in sentence case.
- **Palette:** primary blue `#1a5276`, bar fill `rgba(26,82,118,0.35)`, orange `#e67e22`, green `#27ae60` (reserved for genuinely good outcomes, e.g. profit, uptime), red `#e74c3c` (reserved for the damaged or hidden quantity), passives/abandoned gray `#c8ced6`/`#dfe3e8`, muted text `#6b7280`, gridlines `#e5e9ef`.
