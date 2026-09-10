# Bad Metrics — 27 Real-World Disasters by Domain

**Page type:** detail page, metric-testing template (white background; one two-column obj-table per metric: text left 40%, canvas right 60%; numbered obj-title with domain in parentheses; labeled bullets; canvases 720×200 with devicePixelRatio scaling and redraw on resize)
**HTML title tag:** Bad Metrics — 27 Real-World Disasters by Domain

**Subtitle:** Each row: one metric that looked good on a dashboard but caused real damage. Why it fails and what it hides.

## 1. Total Registered Users (Social)

- **Why bad:** Only goes up — mathematically impossible to decline, so it never signals trouble
- **What it hides:** Dead accounts (20%+), bots (10-30%), duplicates, one-time signups who never returned
- **Real damage:** Platform brags "3B users" while DAU stagnates — investors misled, ad pricing inflated
- **Fix:** DAU/MAU ratio (engagement intensity)

### Visualization (canvas `canvas1`, 720×200)

Two-line chart: cumulative registered users diverging from daily active users.

- **Title (bold 17px, `#1a5276`):** "Cumulative Users vs. Daily Active Users".
- **Data:** years 2018–2024; Registered (billions) `[1.2, 1.8, 2.3, 2.7, 2.9, 3.0, 3.1]` in red `#e74c3c`, width 3; Daily Active `[0.8, 1.1, 1.3, 1.4, 1.3, 1.1, 0.9]` in green `#27ae60`, width 3.
- **Axes:** y max 3.5, labels 0B–3B with light gridlines `#e0e0e0`; x years along bottom; gray axes `#999`. Padding: top 30, bottom 35, left 60, right 140.
- **Annotation:** vertical dashed dark-red (`#c0392b`, dash 4/3) gap marker at 85% width spanning y=3.05 to y=0.95, labeled "THE GAP" (bold 14px) and "= the lie" (12px).
- **Legend (top right):** red swatch "Registered", green swatch "Daily Active".

## 2. Average Handle Time (AHT) (Call Center)

- **Why bad:** Agents game by hanging up, transferring, refusing complex issues
- **What it hides:** Repeat calls — a 3-min "resolution" that causes 3 more calls = 12 min total
- **Real damage:** Best agents (who truly solve problems) get penalized for taking longer
- **Fix:** First Call Resolution (FCR) — did the problem actually get solved?

### Visualization (canvas `canvas2`, 720×200)

Two side-by-side bar panels comparing two agents on two metrics.

- **Title (bold 17px, `#1a5276`):** "Two Agents: AHT vs Repeat Calls".
- **Left panel (starts x=120), header bold 14px `#555`:** "Avg Handle Time (min)" — Agent A 3 min (red `#e74c3c`), Agent B 15 min (green `#27ae60`); bar scale max 18 over 100px height, bar width 80; value labels "3 min" / "15 min" above bars, "Agent A" / "Agent B" below.
- **Right panel (starts x=420), header:** "Repeat Calls (%)" — Agent A 40% (red), Agent B 2% (green); scale max 50; value labels "40%" / "2%".
- **Bottom captions (bold 13px):** red '"Star performer"' under Agent A columns, green 'Actually better' under Agent B columns.

## 3. GMV — Gross Merchandise Value (Startup)

- **Why bad:** Counts total transaction volume, not what the company earns or keeps
- **What it hides:** Negative unit economics — Uber reported $20B GMV, revenue $2B (10% take), profit: -$3B
- **Real damage:** Startups raise billions on GMV while burning cash on every transaction
- **Fix:** Contribution margin per transaction, path to profitability timeline

### Visualization (canvas `canvas3`, 720×200)

Waterfall-style bar chart from GMV down to negative profit.

- **Title (bold 17px, `#1a5276`):** "GMV Waterfall of Disappointment (Uber example)".
- **Bars (width 70, evenly spaced):** GMV $20B `#3498db`; Take Rate (10%) $2B `#f39c12`; Revenue $2B `#27ae60`; Costs -$5B `#e74c3c`; Profit -$3B `#c0392b`. Values `[20, 2, 2, -5, -3]`; negative bars drawn below the zero line.
- **Zero line:** thin gray `#999` horizontal line at 70% of chart height, labeled "$0" at left.
- **Value labels:** bold 14px `#333` above positive bars / below negative bars, formatted "$20B", "-$5B", etc.; category labels 12px `#555` beneath.
- **Bottom caption (bold 13px `#c0392b`):** '← "Impressive"                                    Reality →'.

## 4. Standardized Test Scores (Education)

- **Why bad:** Goodhart's Law — when a measure becomes a target, it ceases to be a good measure
- **What it hides:** Schools teach to the test; scores rise while critical thinking and real learning stay flat
- **Real damage:** Curriculum narrows, creativity disappears, students can pass tests but can't apply knowledge
- **Fix:** Longitudinal skill assessments, real-world problem-solving evaluations

### Visualization (canvas `canvas4`, 720×200)

Two-line divergence chart: test scores rising while real skills stay flat.

- **Title (bold 17px, `#1a5276`):** "Test Scores vs. Real-World Skills (Goodhart)".
- **Data:** years 2015–2022; Test Scores `[62, 66, 70, 74, 78, 81, 84, 87]` red `#e74c3c` width 3; Real Skills `[55, 56, 55, 54, 55, 53, 52, 51]` green `#27ae60` width 3.
- **Axes:** y 0–100%, labels every 25% with gridlines `#e8e8e8`; x years. Padding: top 30, bottom 35, left 60, right 140.
- **Right-edge annotations (bold 13px):** red '↑ "Improving"' next to the last test-score point; green '→ Flat/Declining' next to the last real-skills point.
- **Legend (top right):** red "Test Scores", green "Real Skills".

## 5. Raw Mortality Rate (Hospital)

- **Why bad:** Penalizes the best hospitals that accept the hardest cases
- **What it hides:** Case mix — top cancer center has highest mortality because it takes terminal patients others reject
- **Real damage:** Hospitals stop admitting sick patients to "improve" their numbers
- **Fix:** Risk-adjusted mortality — compare outcomes vs. expected given case difficulty

### Visualization (canvas `canvas5`, 720×200)

Grouped bar chart: case difficulty vs raw mortality for four hospitals.

- **Title (bold 17px, `#1a5276`):** "Case Difficulty vs. Raw Mortality".
- **Groups (x pitch 130 starting at x=90, bars 25px wide):** Community Hosp A, Regional Hosp B, Specialty Center C, Top Cancer Center (two-line labels).
- **Data:** Case Difficulty `[20, 40, 65, 90]`% in blue `#3498db` (scale max 100); Raw Mortality `[3, 5, 9, 18]`% in red `#e74c3c` (scale max 22). Value labels (11px) above each bar.
- **Legend (right):** blue "Case Difficulty", red "Raw Mortality".
- **Annotation (bold 12px `#c0392b`):** 'Best hospital = worst "score"!'.

## 6. Lines of Code (DevOps)

- **Why bad:** Rewards verbosity, punishes elegant solutions — IBM story of engineers penalized for reducing code
- **What it hides:** Value per line — 10 lines that solve a problem vs. 500 lines of boilerplate
- **Real damage:** Incentivizes duplicate code, unnecessary abstractions, bloated systems
- **Fix:** Deployed features per sprint, cycle time, bug escape rate

### Visualization (canvas `canvas6`, 720×200)

Three-line chart: code volume and bugs rising while value per line falls.

- **Title (bold 17px, `#1a5276`):** "Lines of Code vs. Bug Rate vs. Value/Line".
- **Data (quarters Q1–Q8):** Code Vol `[10, 25, 45, 70, 100, 140, 185, 240]` red `#e74c3c` (scale max 260); Bug Rate `[5, 10, 18, 30, 45, 65, 88, 120]` orange `#f39c12` (scale max 130); Value/Line `[80, 70, 58, 45, 35, 25, 18, 12]` green `#27ae60` (scale max 90). All lines width 3, each normalized to its own scale.
- **Axes:** gray L-axes `#999`; x labels Q1–Q8. Padding: top 30, bottom 35, left 60, right 140.
- **Legend (top right):** red "Code Vol ↑", orange "Bug Rate ↑", green "Value/Line ↓".

## 7. Same-Store Sales (without online cannibalization) (Retail)

- **Why bad:** Ignores channel shift — in-store "growth" is just survivors buying more as others leave for online
- **What it hides:** Total company revenue may be flat or declining while same-store shows +5%
- **Real damage:** Retailers celebrate while their total market share erodes to e-commerce
- **Fix:** Omni-channel revenue per customer, total addressable market share

### Visualization (canvas `canvas7`, 720×200)

Three-line chart: in-store vs online vs flat total revenue.

- **Title (bold 17px, `#1a5276`):** "In-Store vs. Online Cannibalization vs. Total".
- **Data (years 2019–2024, $M, y max 130):** In-Store `[100, 103, 105, 107, 108, 105]` blue `#3498db`; Online `[10, 20, 35, 50, 65, 80]` purple `#9b59b6`; Total `[110, 113, 115, 112, 113, 112]` red `#e74c3c` dashed (6/3). All width 3.
- **Axes:** "$M" label near top-left of y-axis; years along x. Padding: top 30, bottom 35, left 60, right 140.
- **Legend (top right):** blue "In-Store +5%", purple "Online ↑↑", red "Total: FLAT".

## 8. Vulnerabilities Patched Count (Security)

- **Why bad:** Rewards doing easy work — 490 informational patches are trivial, 10 critical ones are hard
- **What it hides:** Risk composition — "500 patched!" while 10 critical RCE vulns remain wide open
- **Real damage:** Teams cherry-pick easy vulns for metrics while actual attack surface stays exposed
- **Fix:** Mean time to remediate critical/high severity, risk-weighted coverage

### Visualization (canvas `canvas8`, 720×200)

Two stacked severity bars: patched vs unpatched composition.

- **Title (bold 17px, `#1a5276`):** "Patched vs. Unpatched by Severity".
- **Severity colors/labels:** Info `#bdc3c7`, Low `#95a5a6`, Medium `#f39c12`, High `#e67e22`, Critical `#c0392b`.
- **Left stacked bar (x=130, width 120):** header bold "PATCHED: 500" with 12px caption '"Great job team!"'; segments Info 300, Low 140, Medium 50, High 10, Critical 0 (proportional to total 500). Segment labels in white 11px ("Info: 300", etc.) when tall enough.
- **Right stacked bar (x=420, width 120):** header bold "UNPATCHED: 15" with dark-red caption '"Only 15, no big deal"'; segments Info 0, Low 1, Medium 2, High 5, Critical 7 (proportional to total 15).
- **Legend (far right):** the five severity swatches with labels.

## 9. Daily Steps (Fitness App)

- **Why bad:** Steps ≠ exercise — walking to the kitchen 20 times is not cardiovascular benefit
- **What it hides:** Intensity matters: 500 steps of HIIT > 10,000 steps of slow shuffling
- **Real damage:** Users optimize for step count instead of actual fitness; false sense of health
- **Fix:** Heart rate zones, VO2 max estimates, active minutes at elevated intensity

### Visualization (canvas `canvas9`, 720×200)

Scatter plot: daily steps vs cardiovascular benefit showing low correlation.

- **Title (bold 17px, `#1a5276`):** "Steps vs. Cardiovascular Benefit (Low Correlation)".
- **Points (steps, benefit 0–100), blue `#2980b9` dots radius 5:** (2000,15), (3000,70), (4500,25), (5000,55), (6000,30), (7000,60), (8000,40), (9000,35), (10000,50), (10500,20), (11000,65), (12000,45), (500,85), (1000,75), (13000,30), (14000,55), (15000,40).
- **Axes:** x 0–16000 with tick labels "0K", "5K", "10K", "15K"; x-axis caption "Daily Steps →" bottom center; y 0–100 (unlabeled). Padding: top 30, bottom 40, left 70, right 30.
- **Annotations (bold 11px `#c0392b`):** '← HIIT: 500 steps, great fitness' at the (500,85) point; '← Kitchen walker' at (2000,15); '10K slow stroll ↓' above (10500,20).

## 10. Click-Through Rate (CTR) (Ads)

- **Why bad:** Rewards clickbait — sensational headline gets 70% CTR, 90% immediate bounce
- **What it hides:** Post-click quality — quality content gets lower CTR but 5x engagement and conversions
- **Real damage:** Entire content ecosystems optimized for rage/curiosity bait over substance
- **Fix:** Post-click engagement (time on page, conversion rate, return visits)

### Visualization (canvas `canvas10`, 720×200)

Grouped bar chart: CTR and post-click engagement inversely related across content quality.

- **Title (bold 17px, `#1a5276`):** "CTR vs. Post-Click Engagement — Inverted!".
- **Categories (two-line labels, group pitch 120 starting x=80):** Pure Clickbait, Sensational Headline, Mixed Content, Quality Article, In-Depth Analysis.
- **Data (bars 30px wide, y 0–100%):** CTR `[70, 55, 45, 35, 25]` red `#e74c3c`; Engagement `[5, 20, 45, 70, 85]` green `#27ae60`. Percent value labels (11px) above each bar; y labels 0–100% every 25%.
- **Legend (top right):** red "CTR (rewards worst)", green "Post-Click Engagement".

## 11. Total App Downloads (Mobile)

- **Why bad:** Includes uninstalls, one-time opens, bot installs — pure vanity number
- **What it hides:** Day-30 retention may be 3% — 97% of "downloads" are ghosts
- **Real damage:** Companies spend millions on install campaigns that produce zero active users
- **Fix:** Day-7/Day-30 retention, weekly active users, session frequency

### Visualization (canvas `canvas11`, 720×200)

Horizontal funnel of app-user attrition.

- **Title (bold 17px, `#1a5276`):** "App Download Funnel: Where Users Vanish".
- **Stages (one horizontal bar each, max width 500px, height 22px):** Downloads 1,000,000 (100%), Opened Once 600,000 (60%), Day-7 Active 120,000 (12%), Day-30 Active 30,000 (3%), Paying 8,000 (0.8%). Bar widths proportional to the first stage.
- **Colors:** first bar blue `#3498db`, next two orange `#f39c12`, last two red `#e74c3c`. Stage names 13px `#333` at left; percent labels bold 11px in white inside the bar, or in dark red `#c0392b` beside it when the bar is too narrow.
- **Bottom caption (bold 13px `#c0392b`):** '"10M downloads!" = 80K paying users (0.8%)'.

## 12. Tickets Closed per Sprint (Agile)

- **Why bad:** Gamed by splitting 1 meaningful task into 5 tiny tickets — velocity "doubles" overnight
- **What it hides:** Actual value delivered — 20 micro-tickets may equal one real feature
- **Real damage:** Teams optimize for ticket throughput, product stagnates, stakeholders confused
- **Fix:** Outcomes shipped (customer-facing features), cycle time for meaningful work

### Visualization (canvas `canvas12`, 720×200)

Two-line chart: ticket count jumps after gaming begins while features shipped stay flat.

- **Title (bold 17px, `#1a5276`):** "Velocity Gaming: Ticket Split Before vs After".
- **Data (sprints S1–S8):** Tickets Closed `[8, 9, 10, 11, 22, 25, 28, 30]` red `#e74c3c` (scale max 35); Features Shipped `[4, 4, 5, 5, 5, 4, 4, 3]` green `#27ae60` (scale max 8). Lines width 3.
- **Split marker:** vertical dashed gray line (`#999`, dash 4/3) at 4/7 of chart width (between S4 and S5), annotated bold 12px `#c0392b`: '"Split tickets" →' to its left.
- **Legend (top right):** red "Tickets Closed", green "Features Shipped".

## 13. Test Coverage % (Testing)

- **Why bad:** Write tests that assert nothing — 100% coverage, 0% value; lines executed ≠ logic verified
- **What it hides:** Mutation survival rate — how many bugs would tests actually catch?
- **Real damage:** Teams hit 95% coverage mandate, ship with confidence, still have critical bugs in production
- **Fix:** Mutation testing score, bug escape rate, mean time to detect regressions

### Visualization (canvas `canvas13`, 720×200)

Triple grouped bars for four teams: coverage vs mutation kill vs bugs escaped.

- **Title (bold 17px, `#1a5276`):** "Coverage % vs. Mutation Kill Rate vs. Bugs Escaped".
- **Groups (pitch 155 starting x=80, bars 20px wide, two-line labels):** 'Team A "95% cov"', 'Team B "70% cov"', 'Team C "85% cov"', 'Team D "60% cov"'.
- **Data:** Coverage `[95, 70, 85, 60]`% blue `#3498db` (scale 100); Mutation Kill `[20, 75, 55, 80]`% green `#27ae60` (scale 100); Bugs Escaped `[12, 2, 5, 1]` red `#e74c3c` (scale 15). Value labels 10px above each bar.
- **Legend (top right):** blue "Coverage %", green "Mutation Kill", red "Bugs Escaped".

## 14. Year-over-Year Growth During Anomaly (Analytics)

- **Why bad:** 2021 vs 2020 (COVID) = meaningless comparison — any business looks like a rocket ship
- **What it hides:** True trend — are you actually growing or just reverting to pre-anomaly baseline?
- **Real damage:** Companies hire aggressively on "200% YoY growth" then mass-layoff when it normalizes
- **Fix:** Compare to 2019 baseline, use 2-year CAGR, flag anomaly periods explicitly

### Visualization (canvas `canvas14`, 720×200)

Combined chart: revenue line with YoY % bars floating near the top.

- **Title (bold 17px, `#1a5276`):** "Revenue & YoY % During COVID Anomaly".
- **Revenue line (blue `#3498db`, width 3, scale max 130):** years 2018–2023, values `[100, 110, 50, 105, 112, 115]`.
- **YoY bars (30px wide, drawn near top, height = |YoY|/130 × 40% of chart height):** `[null, 10, -55, 110, 7, 3]`% — the 2021 bar (+110%) is red `#e74c3c`; other positive bars green `#27ae60`; negative bars orange `#e67e22`. Value labels 10px: "+10%", "-55%", "+110%", "+7%", "+3%".
- **Annotation (`#c0392b`):** bold 12px '← "+110% YoY!!"' with 11px second line '(just recovering to baseline)' near the 2021 bar.
- **Legend (right):** blue "Revenue", red "YoY % (lie)".

## 15. Revenue per Employee (HR)

- **Why bad:** Fire low-earners or outsource → metric improves instantly, business capacity weakens
- **What it hides:** Total output, institutional knowledge loss, contractor costs booked differently
- **Real damage:** Layoffs of "low-productivity" support staff collapse the infrastructure that enables high-earners
- **Fix:** Revenue per total labor cost (including contractors), output quality metrics

### Visualization (canvas `canvas15`, 720×200)

Two-line chart: revenue per employee spikes after layoffs while total revenue declines.

- **Title (bold 17px, `#1a5276`):** "Revenue/Employee Improves as Business Weakens".
- **Data (quarters Q1–Q8; Q4 x-label is two lines "Q4" / "Layoffs"):** Rev/Employee `[200, 205, 210, 280, 310, 295, 270, 250]` red `#e74c3c` (scale max 350); Total Revenue `[100, 102, 105, 95, 88, 82, 75, 70]` green `#27ae60` (scale max 120). Lines width 3.
- **Layoffs marker:** vertical dashed gray line (`#999`, dash 4/3) at 3/7 of chart width (Q4), labeled bold 11px `#c0392b` "LAYOFFS" at top.
- **Legend (top right):** red "Rev/Employee ↑", green "Total Revenue ↓".

## 16. NPS Without Segmentation (Product)

- **Why bad:** Overall NPS 50 hides bimodality — power users at +80, casual users at -10
- **What it hides:** Two completely different user experiences averaged into one meaningless number
- **Real damage:** Product team thinks "things are fine" while 40% of users actively hate the product
- **Fix:** Segment NPS by cohort, usage tier, acquisition channel; show distribution not mean

### Visualization (canvas `canvas16`, 720×200)

Bimodal histogram of NPS scores with a misleading mean line.

- **Title (bold 17px, `#1a5276`):** "NPS Distribution: Mean Hides Bimodality".
- **Bins (x labels -100 to 100 step 20):** counts `[5, 15, 25, 20, 8, 5, 3, 8, 20, 35, 30]`, count scale max 40; bar width = chart width / 11 minus 2px gap.
- **Bar colors:** bins < 0 red `#e74c3c`; bins 0–40 orange `#f39c12`; bins ≥ 50 green `#27ae60`. Bin value labels 9px `#555` below.
- **Mean line:** vertical dashed blue `#1a5276` (dash 5/3, width 2) at bin position 7.5, labeled bold 12px "Mean NPS: +50" plus 11px '"Looks fine!"'.
- **Bottom labels (bold 11px):** dark red `#c0392b` "Detractors: 40%" at left, green "Promoters: 50%" at right.

## 17. Deployment Frequency Alone (Platform Eng)

- **Why bad:** High frequency may be hotfixes for previous bad deploys, not actual velocity
- **What it hides:** Rollback rate, change failure rate — 10 deploys/day where 6 are fixes = chaos
- **Real damage:** Teams chase "deploy daily" mandate, quality drops, alert fatigue sets in
- **Fix:** DORA metrics together (frequency + lead time + failure rate + recovery time)

### Visualization (canvas `canvas17`, 720×200)

Two-line chart: deploys per week rising alongside rollbacks.

- **Title (bold 17px, `#1a5276`):** "Deploy Frequency vs. Rollback Rate".
- **Data (weeks W1–W8):** Deploys/wk `[2, 3, 5, 8, 12, 15, 18, 20]` blue `#3498db` (scale max 25); Rollbacks/wk `[0, 0, 1, 2, 5, 7, 10, 12]` red `#e74c3c` (scale max 15). Lines width 3. Padding: top 30, bottom 35, left 60, right 140.
- **Annotation (bold 12px `#c0392b`, upper middle):** "W8: 12/20 = 60% are hotfixes!".
- **Legend (right):** blue "Deploys/wk", red "Rollbacks/wk".

## 18. A/B Test "Winner" from Peeking (Experimentation)

- **Why bad:** Checking results at day 3 gives p=0.03 (significant!); proper end at day 14 gives p=0.45 (nothing)
- **What it hides:** Multiple comparisons inflate false positives — peek 5 times, ~23% chance of false winner
- **Real damage:** Ship "winning" variant that has no real effect; compound over many tests = random product
- **Fix:** Pre-register sample size, use sequential testing with spending functions, or always run to completion

### Visualization (canvas `canvas18`, 720×200)

p-value trajectory over 14 days with an early false-positive dip.

- **Title (bold 17px, `#1a5276`):** "p-value Over Time: Peeking Creates False Winners".
- **Data (days 1–14, y scale 0–0.5):** p-values `[0.4, 0.15, 0.03, 0.08, 0.12, 0.25, 0.35, 0.28, 0.18, 0.22, 0.38, 0.42, 0.44, 0.45]`, line blue `#2980b9` width 3. X labels every other day: D1, D3, D5, D7, D9, D11, D13.
- **Threshold:** horizontal dashed red line (`#e74c3c`, dash 5/3) at p=0.05, labeled 11px "p=0.05 threshold" near the right.
- **Peek marker:** red `#e74c3c` filled dot radius 6 at day 3 (p=0.03), annotated bold 12px `#c0392b`: '"Ship it! p=0.03!"'.
- **Annotation (bold 12px green `#27ae60`, upper right):** "Actual: p=0.45 (nothing)".

## 19. Model Accuracy on Imbalanced Data (ML)

- **Why bad:** 95% accuracy = just predicting majority class always; 0% recall on the class you care about
- **What it hides:** The model learned nothing — a constant predictor achieves the same "accuracy"
- **Real damage:** Fraud model "95% accurate" catches zero fraud; cancer model "97% accurate" misses all tumors
- **Fix:** Precision-recall curve, F1 on minority class, AUPRC, confusion matrix

### Visualization (canvas `canvas19`, 720×200)

Text-panel comparison of two models plus accuracy bars (no axes).

- **Title (bold 17px, `#1a5276`):** "95% Accuracy = Predicting Majority Class Always".
- **Header (bold 13px `#333`):** "Data: 950 negative, 50 positive (5% prevalence)".
- **Left block (x=80):** heading bold 12px red `#e74c3c` 'Model A: "95% Accurate"'; lines 11px `#333`: "Pred Neg: 950 (correct!) + 50 (missed!)", "Pred Pos: 0"; verdict bold 11px `#c0392b`: "Recall on fraud/cancer: 0%  ← USELESS".
- **Right block (x=430):** heading bold 12px green `#27ae60` 'Model B: "80% Accurate"'; lines: "Pred Neg: 760 correct + 5 missed", "Pred Pos: 45 caught + 190 false alarm"; verdict bold 11px green: "Recall on fraud/cancer: 90%  ← USEFUL".
- **Bottom bars (16px tall):** red bar 300px wide with white bold 11px label "Accuracy: 95% (worthless)"; green bar 250px wide with white label "Accuracy: 80% (saves lives)".

## 20. Churn Rate — Monthly vs Annual Framing (SaaS)

- **Why bad:** "5% monthly churn" sounds manageable; compounded = 46% annual churn = business is dying
- **What it hides:** Framing trick — same number presented monthly vs annually gives opposite emotional reactions
- **Real damage:** Board sees "only 5%" monthly, doesn't panic; by year-end half the customers are gone
- **Fix:** Always show both frames; use cohort retention curves; compare to industry benchmarks at same timeframe

### Visualization (canvas `canvas20`, 720×200)

Compounding retention decay curve with the lost area shaded.

- **Title (bold 17px, `#1a5276`):** "5% Monthly Churn = 46% Annual Churn (Compounding)".
- **Data (procedural):** months M0–M12; remaining[0] = 100, remaining[i] = remaining[i-1] × 0.95 (ends at ~54.0). Red `#e74c3c` line width 3; the region between 100% and the curve is filled `rgba(231,76,60,0.15)`.
- **Axes:** y 0–100% labeled every 25% with gridlines `#e8e8e8`; x labels every other month (M0, M2, ... M12).
- **Annotations:** bold 13px `#c0392b` "LOST: 46%" with 11px second line '"Only 5%/month" framing' in the shaded area; bold 12px `#333` "54% remain" at the curve's end.

## 21. IQ Score as Intelligence Metric (Psychology)

- **Why bad:** Collapses a high-dimensional space (reasoning, memory, spatial, verbal, cultural knowledge) into one number, then reifies that number as the thing itself
- **Instability:** Flynn Effect drifts +3 pts/decade; stereotype threat shifts 0.5-1 SD; test-retest varies ±5-7 pts — a good metric is stable when the underlying thing hasn't changed
- **What it hides:** Score is normed to N(100,15) by construction — the Gaussian is imposed, not discovered. Content encodes its designers' cultural assumptions
- **Real damage:** Gifted cutoffs (≥130) systematically exclude minorities. When universal nonverbal screening replaced IQ+referral, minority identification tripled — the "gap" was measurement artifact
- **Fix:** Multiple domain-specific assessments, culturally fair instruments, report profiles not scalars, acknowledge r² ≈ 0.25 with job performance (moderate, not decisive)

### Visualization (canvas `canvas21`, 720×200)

Horizontal bar chart of what IQ actually correlates with (r²).

- **Title (bold 17px, `#1a5276`):** 'What "IQ" Actually Correlates With (r²)' (drawn starting at the horizontal center of the plot area).
- **Bars (horizontal, left padding 160 for right-aligned labels, r² scale max 0.5):** Parents' education 0.42 orange `#e67e22`; Household income 0.35 orange; Test-taking practice 0.30 orange; Job performance 0.25 blue `#2980b9`; School grades 0.22 blue; Creativity 0.10 green `#27ae60`; Life satisfaction 0.04 green. Each bar has its r² value (2 decimals, 11px) at its end.
- **Legend (bottom, 10px):** orange "Environment/SES", blue "Academic", green "What people think it measures".

## 22. Fahrenheit — Calibrated to a Drifting Reference (Measurement)

- **Why bad:** Fahrenheit (1724) anchored his scale to three points: brine cold (0°F), water freezing (32°F), and "human body" (96°F) — two of three are context-dependent, not physical constants
- **Instability:** The body-temp reference has drifted — average human temp dropped ~0.03°C/decade since 1860 (Protsiv 2020, Stanford). Modern average is 36.6°C (97.9°F), not Wunderlich's 37°C (98.6°F). Cause: reduced chronic infection, lower BMR, climate-controlled living — secular physiological drift, not evolution
- **What it hides:** Body temp varies ±1°F by time of day, age, fitness, menstrual cycle, and infection status. A "constant" that varies by era, population, and time of measurement is not a constant
- **Real damage:** 98.6°F is still taught as "normal" — patients with 99.1°F are told they're fine; elderly patients with actual infections present at 98.5°F and are missed because the threshold is wrong
- **Fix:** Kelvin — anchored to absolute zero and water's triple point (physical constants that don't drift). Celsius gets the same stability from phase transitions. Anchor metrics to things that can't move.

### Visualization (canvas `canvas22`, 720×200)

Line chart of average body temperature drifting away from the 37.0°C reference.

- **Title (bold 17px, `#1a5276`):** "Human Body Temperature: The Reference Point Drifted" (drawn starting at the horizontal center of the plot area).
- **Data:** eras 1860s, 1940s, 1970s, 2000s, 2020s; average temps `[37.0, 36.85, 36.75, 36.65, 36.55]` °C. Blue `#2980b9` line width 3 with 4px-radius dots at each point.
- **Axes:** y from 36.3 to 37.2°C, right-aligned labels at 5 evenly spaced values (one decimal + "°C") with gridlines `#e8e8e8`; x era labels.
- **Reference line:** horizontal dashed red (`#e74c3c`, dash 6/4, width 2) at 37.0°C, labeled bold 12px `#c0392b`: '98.6°F (37.0°C) — "the constant"'.
- **Annotations:** bold 11px blue `#2980b9` "97.9°F actual →" near the last point; 11px `#555` "↓ Less infection, lower BMR, climate control" near the bottom left of the plot.

## 23. IMDb Rating — Absolute Number Hiding Multiple Confounds (Entertainment)

- **Why bad:** A single number (e.g., 8.2/10) presented as absolute quality. But a film with 2 million ratings from a passionate fanbase and one with 15,000 casual votes occupy the same scale — the confidence intervals are incomparable
- **Reviewer count confound:** Niche cult films with 5,000 devoted fans score 8.5. Mainstream blockbusters with 500,000 diverse reviewers regress to 7.2. The niche film isn't "better" — it's measured by a self-selected audience who already love the genre
- **Era bias:** Classic films (pre-1980) are rated by people who specifically sought them out — survivorship + selection bias. A 1975 film with 8.4 was rated by cinephiles; a 2023 film with 7.8 was rated by everyone who bought a ticket
- **Fanbase effect:** Organized fandoms can coordinate 10/10 ratings on release day (or 1/10 for rival films). A rating that can be moved by coordinated action is not measuring quality
- **What it hides:** Genre expectations, demographic composition of raters, timing of votes (release hype vs. long-tail), and whether voters actually watched the film
- **Fix:** Bayesian weighted rating (IMDb does this partially), but display confidence interval. Segment by reviewer cohort. Compare within genre+era, not across all cinema. Show rating distribution shape, not just mean

### Visualization (canvas `canvas23`, 720×200)

Confidence-interval comparison of two film ratings on a 5–10 scale.

- **Title (bold 17px, `#1a5276`, centered):** "Same Rating, Different Meaning".
- **Scale:** x axis 5–10 with integer tick labels and vertical gridlines `#eee`; baseline `#ccc`.
- **Two rows (bar height 30px, 50px gap, first row at y=50):**
  - "Cult Classic (1978)" — rating 8.5, CI ±0.60, 4,800 voters, orange `#e67e22`. CI drawn as translucent rectangle (color + `33` alpha suffix) with 2px solid outline; point estimate as 6px-radius filled dot at 8.5; bold 14px label "8.5 ± 0.60" right of the CI; "4,800 voters" 11px `#666` beside the name.
  - "Blockbuster (2023)" — rating 7.8, CI ±0.05, 520,000 voters, blue `#2980b9`; label "7.8 ± 0.05"; "520,000 voters".
- **Verdict (bold 12px red `#e74c3c`, centered at bottom):** "8.5 from 4,800 self-selected fans ≠ 7.8 from 520,000 diverse viewers. The CI tells the real story."

## 24. Box Office Gross — Nominal Dollars Ignoring Inflation (Finance / Entertainment)

- **Why bad:** "Highest-grossing film of all time" lists use nominal dollars. Every new entry appears to break records simply because ticket prices increase ~3-5% annually. The metric rewards recency, not audience size
- **Scale of distortion:** Gone with the Wind (1939) sold ~200 million tickets in North America. Avengers: Endgame (2019) sold ~95 million. But Endgame "earned more" because $2.8B in 2019 dollars > $390M in 1939 dollars. In 2019-adjusted dollars, GWTW earned ~$3.7B
- **What it hides:** Ticket price inflation, population growth (more potential viewers), number of screens (wider releases), 3D/IMAX surcharges, and international market expansion. None of these reflect the film's quality or cultural penetration
- **Real damage:** Studios use "record-breaking gross" to justify sequel decisions. Journalists use it for "best ever" claims. Investors use it for performance analysis. All comparing 2024 dollars to 1997 dollars as if the unit is stable
- **Fix:** Report admissions (tickets sold), not revenue. If revenue is needed, inflation-adjust to a fixed base year. Better: report market share (% of total annual box office) — this normalizes for population, screens, and pricing simultaneously

### Visualization (canvas `canvas24`, 720×200)

Paired bar chart: nominal vs inflation-adjusted gross for four films.

- **Title (bold 17px, `#1a5276`, centered):** "Box Office: Nominal $ vs Inflation-Adjusted".
- **Data ($B, y axis $0B–$4B with gridlines `#eee`):** GWTW (1939) nominal 0.39 / adjusted 3.7, "200M tix"; Star Wars (1977) 0.78 / 3.3, "178M tix"; Titanic (1997) 2.2 / 3.6, "135M tix"; Endgame (2019) 2.8 / 2.9, "95M tix".
- **Bars (paired per film):** nominal fill `rgba(231,76,60,0.5)` stroked `#e74c3c` (1.5px); adjusted fill `rgba(39,174,96,0.5)` stroked `#27ae60`. Film name (10px `#333`) and ticket count (10px `#666`) centered beneath each pair.
- **Legend (top right, bold 11px):** red "Nominal $", green "2019-adjusted $".

## 25. File Download "Time Remaining" — Reactive Estimate on Volatile Signal (OS / UX)

- **Why bad:** Estimated time remaining computed from a ~5-second moving average of instantaneous download speed. Speed fluctuates wildly on congested 90s/00s connections — estimate swings from "30 sec" to "3h 22min" to "2 min" every few seconds
- **What it hides:** The underlying bandwidth is noisy but the long-run average is stable. A short-window reactive estimate amplifies noise instead of filtering it — presenting jitter as information
- **Real damage:** Users can't plan around it (should I wait or leave?). Erodes trust in the OS. Became a cultural joke (Windows copy dialog). Emotionally manipulative — hope, despair, hope in 10-second cycles
- **Statistical sin:** Point estimate with zero smoothing on a high-variance signal. No confidence band, no trend detection, no acknowledgment that "I don't know yet" is valid output
- **Fix:** Use a cumulative average like car trip MPG — averages the entire journey so each new sample has less influence as N grows; stabilizes over time instead of thrashing. Or: EWMA with long half-life (~60s), show a range ("2–8 minutes"), or display "Estimating…" until signal stabilizes. The car metric works because it's a running mean over the whole trip, not a sliding window over the last 5 seconds

### Visualization (canvas `canvas25`, 720×200)

Jittery displayed-ETA line vs the smooth true remaining time (procedurally derived).

- **Title (bold 17px, `#1a5276`):** '"Time Remaining" — Reactive Estimate on Noisy Signal'.
- **Inputs:** samples every 2s over 0–60s (31 samples); actual download speed (KB/s) array: `[480, 520, 450, 510, 490, 530, 100, 120, 480, 510, 490, 500, 520, 50, 80, 500, 510, 530, 490, 480, 510, 500, 490, 520, 500, 510, 480, 530, 490, 500, 510]`; file size 150,000 KB.
- **Displayed ETA formula (red `#e74c3c` line, width 2.5):** at each sample i, avgSpeed = mean of the last 2 samples (window 2); estimate = fileRemaining / avgSpeed / 60 minutes, capped at 200; then fileRemaining -= speed[i] × 2. Values above 30 are clipped to the top of the plot.
- **True remaining formula (green `#27ae60` dashed 6/3, width 2):** (150000 − i × 1000) / 500 / 60 minutes (smooth descent from 5 min).
- **Axes:** y 0–30 with labels "0min", "10min", "20min", "30min" and gridlines `#e8e8e8`; x labels every 5th sample ("0s", "10s", ... "60s"); note "Minutes remaining →" at top left. Padding: top 30, bottom 35, left 60, right 140.
- **Spike annotations (bold 11px `#c0392b`, near top):** '↑ "3h 22min"' at sample 6 and '↑ "2h 50min"' at sample 13 (the speed-drop spikes).
- **Legend (right):** red "Displayed ETA", green "True remaining"; gray 11px note beneath: "(5-sec window" / " on noisy speed)".

## 26. CPU Clock Speed (GHz) — Optimizing the Spec Sheet Number (Hardware)

- **Why bad:** Clock speed alone says nothing about compute throughput — instructions per cycle (IPC), pipeline depth, cache hierarchy, and memory bandwidth all determine actual performance. A 3.8 GHz chip can be slower than a 2.2 GHz chip
- **The con:** Intel's Pentium 4 "NetBurst" architecture used a 31-stage pipeline specifically designed to hit high GHz — marketing metric first, performance second. AMD's Athlon 64 at 2.2 GHz regularly beat the P4 at 3.8 GHz because it completed more work per cycle
- **What it hides:** IPC (work done per tick), pipeline stall penalties, branch misprediction cost, thermal throttling under load, and multi-core scaling. A deep pipeline inflates clock speed but makes each stall more expensive
- **Real damage:** Intel chased GHz so aggressively they hit thermal walls, cancelled the 4+ GHz "Tejas" chip, lost years of architectural progress, and consumers bought "faster" chips that were actually slower. Some vendors cherry-binned or overvolted CPUs to claim higher speeds on spec sheets
- **Fix:** Benchmark real workloads (SPECint, Geekbench), report IPC × clock × cores, perf/watt. Apple M1 at 3.2 GHz outperforms 5 GHz Intel chips — because architecture dominates clock

### Visualization (canvas `canvas26`, 720×200)

Paired bar chart: clock speed vs actual benchmark score for five CPUs.

- **Title (bold 17px, `#1a5276`):** "Clock Speed (GHz) vs. Actual Benchmark Performance".
- **CPUs (group pitch 120 starting x=70, bars 25px wide, two-line labels):** AMD Athlon 64 2.2 GHz → perf 78 (green `#27ae60`); Intel P4 3.0 GHz → 62 (red `#e74c3c`); Intel P4 3.8 GHz → 72 (red); Core 2 Duo 2.4 GHz → 95 (green); Apple M1 3.2 GHz → 100 (green).
- **Bars:** clock bar always orange `#f39c12` (scale max 4.5 GHz); benchmark bar in the per-CPU color (scale max 110). Labels 10px: "2.2 GHz" etc. above clock bars, perf number above benchmark bars.
- **Legend (right):** orange "Clock (GHz)", green "Benchmark (faster=better)"; annotation bold 11px `#c0392b`: "P4: highest GHz ≠ fastest chip".

## 27. Megapixels — Counting Resolution While Quality Degrades (Photography)

- **Why bad:** Megapixels only describe image resolution (how many dots), not image quality. Cramming more pixels onto the same tiny sensor means each pixel gets less light → more noise, worse dynamic range, worse low-light performance
- **The race:** The "megapixel war" (2003–2015) led manufacturers to ship 108MP phone sensors on tiny dies that produced noisier, softer images than 12MP alternatives with larger photosites
- **What it hides:** Sensor physical size, pixel pitch (μm), lens quality, image processing pipeline, dynamic range, and low-light capability. A 12MP full-frame sensor (8.4μm pixels) destroys a 48MP phone sensor (0.8μm pixels) in every quality dimension
- **Real damage:** Consumers chose cameras by MP count on the box. Manufacturers diverted R&D to pixel-packing instead of sensor quality. Marketing departments ran ads saying "48MP vs their 12MP" as if 4× pixels = 4× quality
- **Fix:** Sensor area × pixel pitch, actual SNR measurements, DxOMark perceptual scores, sample images in controlled low-light conditions. Apple held at 12MP for years while improving sensor size, processing, and optics — photos got dramatically better without the number changing

### Visualization (canvas `canvas27`, 720×200)

Paired bar chart: megapixel count vs perceptual image quality for five cameras.

- **Title (bold 17px, `#1a5276`):** "Megapixels vs. Actual Image Quality (DxOMark)".
- **Cameras (group pitch 120 starting x=70, bars 25px wide, two-line labels; pixel sizes noted in the data but not drawn):** Phone 12MP Large pixel → quality 85 (green `#27ae60`, 1.8μm); Phone 48MP Tiny pixel → 72 (red `#e74c3c`, 0.8μm); Phone 108MP Minuscule → 68 (red, 0.6μm); DSLR 12MP Full-frame → 100 (green, 8.4μm); DSLR 45MP Full-frame → 98 (green, 4.4μm).
- **Bars:** megapixel bar always purple `#9b59b6` (scale max 120 MP); quality bar in the per-camera color (scale max 110). Labels 10px: "12MP" etc. above MP bars, quality score above quality bars.
- **Legend (right):** purple "Megapixels", green "Image Quality Score"; annotation bold 11px `#c0392b`: "108MP phone < 12MP full-frame".

## Regeneration instructions

- **Layout:** single-page `.obj-table` catalog: h1, `.subtitle` paragraph, then one full-width table with a `<thead>` header row ("The Bad Metric & What Went Wrong" | "Visualization") and one `<tr>` per metric. Left `<td>` (40%): `.metric-domain` badge + `.metric-title` ("N. Title", unpadded index) + `.metric-desc` bullet list with bold lead-in labels. Right `<td>` (60%): the canvas.
- **Page style:** body `-apple-system` sans-serif, background `#fafafa`, text `#222`, padding 20px 10px; h1 `#1a5276` (4px bottom margin); `.subtitle` `#555`, 1.1em, 30px bottom margin.
- **Table style:** `border-collapse: collapse`; th background `#1a5276`, white text, padding 12px 16px, border `1px solid #2980b9`; td border `1px solid #2980b9`, padding 14px 16px, `vertical-align: top`; even rows tinted `#f0f8ff`.
- **Text styles:** `.metric-title` bold 1.05em `#1a5276`; `.metric-domain` inline-block badge, background `#2980b9`, white text, padding 2px 8px, radius 3px, 0.8em; `.metric-desc ul` 0.93em, line-height 1.7, margin `4px 0 0 16px`. No nav bar, no back/home links.
- **Canvas:** every canvas is 720×200 (CSS `display: block; width: 720px; height: 200px; margin-top: 8px`), created via a shared `setup(id)` helper that sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`), fixes the CSS size, and calls `ctx.scale` so drawing stays in logical coordinates, and sets the default font to `17px -apple-system, sans-serif`. Each chart is an IIFE drawing directly with canvas 2D primitives.
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9` / `#3498db`, green `#27ae60`, red `#e74c3c`, dark red (annotations) `#c0392b`, orange `#e67e22` / `#f39c12`, purple `#9b59b6`, grays `#555`/`#666`/`#333`/`#999`.
- **Links:** this page has no card links; if any are added in regenerated HTML, they use `.html` extensions.
