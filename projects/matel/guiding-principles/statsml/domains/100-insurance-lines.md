# Insurance Lines of Business — Each Line Has Unique Data Challenges

**Page type:** detail page (one h2 per line of business, each followed by a two-column obj-table row: text left 50%, canvas right 50%)
**HTML title tag:** 100. Insurance Lines of Business — Each Line Has Unique Data Challenges

**Subtitle:** Beyond the general actuarial pitfalls, each insurance line of business has data challenges all its own.

## Homeowners / Property

**Obj-title:** Catastrophe Correlation: 1000 "Independent" Claims Arrive at Once

- **Catastrophe correlation:** Claims look "independent" until one hurricane makes 1000 homes claim at once.
- **Demand surge:** Post-disaster, every contractor in the state is booked → reconstruction costs rise 40%.
- **Surge decays slowly:** The cost index peaks near +40% by week 4 and is still +25% at week 12.
- **Wrong baseline:** "Replacement cost" estimated from pre-disaster prices is wrong post-disaster.
- **Hidden renovation value:** Owner improved the kitchen but never notified the insurer → underinsured.

### Visualization (canvas `canvas1`, 720×300 declared; setup renders at 720×200)

Two side-by-side panels: monthly claims bar chart (left) and demand-surge line chart (right).

- **Left title (bold 14px `#1a5276`):** "Claims Count by Month".
- **Bars:** months J F M A M J J A S O N D, values `[12, 15, 10, 8, 11, 9, 14, 13, 180, 45, 20, 14]`; scale max 180 over 130px height, baseline y=170, bar width 22, spacing 28 from x=20. Colors: September (index 8) `#c0392b`, October `#e74c3c`, all others `#3498db`. Month labels gray `#555` 11px.
- **Annotation (bold 12px `#c0392b` near the spike):** "HURRICANE" / "1000 claims".
- **Right title:** "Reconstruction Cost (Demand Surge)".
- **Line:** dark red `#c0392b` width 2.5 through points Pre/Wk1/Wk2/Wk4/Wk8/Wk12 with cost index values `[100, 115, 130, 140, 135, 125]` (y scale: value 80–150 over 130px, baseline y=170, x from 420 step 50).
- **Baseline:** dashed gray `#7f8c8d` (dash 4/3) horizontal at value 100, labeled "Pre-disaster baseline" (11px `#7f8c8d`).
- **Annotation (bold 12px `#c0392b`):** "+40%".

## Auto / Motor

**Obj-title:** Telematics Privacy vs Pricing Accuracy; ADAS Repair Inflation

- **Telematics tradeoff:** Speed, braking, cornering, time-of-day, and location are a perfect risk signal.
- **Ownership question:** Who owns that driving data, and can the insurer hand it over to the police?
- **Privacy boundary:** Tracking a teenager driving to a therapy clinic may disclose health information.
- **Location worst of both:** It carries the strongest risk signal and the largest privacy cost at once.
- **Repair inflation:** A $200 bumper repair in 2015 is a $2000 sensor recalibration by 2024.
- **ADAS everywhere:** Cameras sit in every body panel, so even trivial damage forces a recalibration.

### Visualization (canvas `canvas2`, 720×300 declared; setup renders at 720×200)

Two panels: repair-cost growth line (left) and paired risk/privacy bars (right).

- **Left title (bold 14px `#1a5276`):** "Average Bumper Repair Cost ($)".
- **Line:** dark red `#c0392b` width 2.5 with 4px dots through years `['2015','2017','2019','2021','2023','2024']` and costs `[200, 450, 800, 1200, 1700, 2000]`; scale max 2200 over 130px, baseline y=170, x from 40 step 55; area under line filled `rgba(41,128,185,0.1)`. Value labels "$200"…"$2000" (10px `#555`) above each dot; year labels below.
- **Annotation (bold 12px `#c0392b`):** "ADAS sensors" / "in every panel".
- **Right title:** "Telematics: Risk Signal vs Privacy".
- **Paired horizontal bars** starting at x=500, width = value×1.5px, height 10, rows 28px apart from y=45: signals `['Speed', 'Braking', 'Cornering', 'Time', 'Location']`; risk-signal values (blue `#2980b9`) `[70, 80, 60, 85, 95]`; privacy-risk values (red `#e74c3c`) `[20, 15, 10, 60, 95]`; signal labels 12px `#333` at x=430.
- **Legend (10px `#555`):** blue swatch "Risk signal", red swatch "Privacy risk".

## Health Insurance

**Obj-title:** Claims Reflect What's BILLED, Not What's Wrong With the Patient

- **Definition drift:** What counts as "pre-existing" shifts with every advance in diagnostic technology.
- **Genomic endgame:** With whole genome sequencing, everyone carries a "pre-existing genetic risk."
- **Billed ≠ wrong:** Claims reflect upcoding for revenue, defensive medicine, and procedure-happy providers.
- **Not the condition:** So the record describes the billing incentive, not the patient's actual condition.
- **Network effects:** Cost depends on WHICH hospital the patient uses — 10× spread for one procedure.

### Visualization (canvas `canvas3`, 720×300 declared; setup renders at 720×200)

Two panels: price-by-hospital bars (left) and claims-distortion pie (right).

- **Left title (bold 14px `#1a5276`):** "Same Procedure: Price by Hospital".
- **Bars:** hospitals `['Hosp A','Hosp B','Hosp C','Hosp D','Hosp E']`, prices `[3200, 8500, 15000, 22000, 32000]`; scale max 35000 over 125px, baseline y=170, bar width 45, spacing 60 from x=30; Hosp E bar `#c0392b`, others `#2980b9`; value labels "$3.2k"…"$32.0k" above bars (10px `#555`).
- **Annotation (bold 11px `#c0392b`):** "10x difference!".
- **Right title:** "Claims Data Distortion".
- **Pie chart:** center (550,105), radius 60, starting at top, slices with white 2px separators: Upcoding 35% `#e74c3c`, Defensive Med 25% `#f39c12`, Actual Need 40% `#27ae60`. Legend swatches at x=640 with labels "Upcoding (35%)", "Defensive Med (25%)", "Actual Need (40%)" (11px `#333`).
- **Annotation (bold 11px `#c0392b`, bottom):** "60% of claims $ is NOT" / "the actual medical need".

## General Liability

**Obj-title:** Set a Reserve in 2024 That Must Predict a 2034 Jury

- **Long tail:** Claim filed today → lawsuit → discovery → trial → appeal → resolution 5-10 years later.
- **Impossible forecast:** Today's reserve must predict a verdict from judges and juries not yet seated.
- **Shifting ground:** Laws and social norms will have moved again by the time the case finally resolves.
- **Social inflation:** Jury awards rise 10-15% annually regardless of the actual damages suffered.
- **Understated history:** Awards growing far above CPI mean historical loss data undershoots future costs.

### Visualization (canvas `canvas4`, 720×300 declared; setup renders at 720×200)

Two panels: claim-lifecycle timeline (left) and social-inflation line chart (right).

- **Left title (bold 14px `#1a5276`):** "Claim Lifecycle: Filed to Resolved".
- **Timeline:** horizontal blue line (`#2980b9`, width 3) from (30,100) to (330,100); stage dots (radius 6) at years `[0, 1, 3, 5, 7, 10]` mapped over 300px: labels rotated 30° below — `['Filed', 'Lawsuit', 'Discovery', 'Trial', 'Appeal', 'Paid']`; "Filed" dot green `#27ae60`, "Paid" dot dark red `#c0392b`, others `#2980b9`; "Yr N" labels above each dot (10px `#888`).
- **Annotation (bold 12px `#c0392b`):** "Reserve set HERE" / "must predict outcome" / "10 years later".
- **Right title:** "Jury Awards: Social Inflation".
- **Lines** over years `['2014','2016','2018','2020','2022','2024']` (x from 420 step 50, scale max 4.0 over 130px, baseline y=175): actual awards solid `#c0392b` width 2.5 through `[1.0, 1.25, 1.6, 2.1, 2.8, 3.5]`; CPI-expected dashed gray `#7f8c8d` (dash 4/3) through `[1.0, 1.06, 1.12, 1.19, 1.26, 1.34]`.
- **Legend (11px `#333`):** solid red line sample "Actual awards (+12%/yr)"; dashed gray line sample "CPI-expected (+3%/yr)".

## Umbrella / Excess Liability

**Obj-title:** Gaps Fall in From Below; Pricing From n=5-10 Claims

- **Stacking gaps:** Umbrella sits ABOVE the primary policies — auto, home, and general liability.
- **Falls in from below:** Any exclusion in an underlying layer drops the loss straight into the umbrella.
- **No visibility:** The primary insurer changes an exclusion → creates a gap the umbrella didn't price for.
- **Tiny n, fat tail:** The umbrella triggers only for large claims, which are both rare and extreme.
- **Sparse history:** Rates therefore rest on 5-10 historical claims spread across 20 years.

### Visualization (canvas `canvas5`, 720×300 declared; setup renders at 720×200)

Two panels: coverage-layer stack (left) and sparse claim scatter (right).

- **Left title (bold 14px `#1a5276`):** "Coverage Layer Stacking".
- **Stacked bars** (x=30, width 280), top to bottom: "Umbrella ($5M+)" purple `#8e44ad` (y=35, h=35); "GAP (exclusion changed)" dark red `#c0392b` (y=72, h=15) with white diagonal hatching; "Auto Primary ($500K)" `#2980b9` (y=89, h=30); "Home Primary ($300K)" `#27ae60` (y=121, h=30); "GL Primary ($1M)" `#f39c12` (y=153, h=30). White bold 11px labels inside each layer.
- **Right title:** "Umbrella Claims: n=8 over 20 years".
- **Scatter:** light gray axes (`#bbb`) with origin (400,175); claims at years `[2004, 2007, 2009, 2012, 2014, 2017, 2020, 2023]` (mapped 2004–2024 over 280px from x=420) with amounts `[1.2, 2.5, 0.8, 5.1, 12.0, 1.5, 3.2, 45.0]` $M (scale max 50 over 130px); dots radius 8 dark red `#c0392b` when >$10M, else radius 5 blue `#2980b9`; value labels "$1.2M"… above dots (9px `#555`). X-axis labels "2004" and "2024" (10px `#888`).
- **Annotation (bold 11px `#c0392b`):** "How do you price" / "from n=8?".

## Business / Commercial Property

**Obj-title:** Business Interruption: Lost Revenue Is an Unobservable Counterfactual

- **Counterfactual claim:** "What would revenue have been?" is a quantity nobody can ever observe.
- **Growth makes it worse:** The business was GROWING when the event hit, so a flat baseline undercounts.
- **Adversarial estimates:** Insurer and insured both have incentives to bend the counterfactual estimate.
- **Supply chain cascade:** Factory burns in country X → 100 businesses in country Y can't get parts.
- **Correlated losses:** One upstream event therefore yields 100 business-interruption claims at once.

### Visualization (canvas `canvas6`, 720×300 declared; setup renders at 720×200)

Two panels: counterfactual revenue dispute (left) and supply-chain cascade diagram (right).

- **Left title (bold 14px `#1a5276`):** "Business Interruption: The Counterfactual".
- **Revenue lines** (x from 30 step 27, scale max 280 over 120px, baseline y=165): actual revenue solid blue `#2980b9` width 2.5 for months 0-5 through `[100, 110, 120, 130, 140, 150]`; after the event, insured's claim dashed green `#27ae60` (dash 5/3) continuing `[165, 180, 200, 220, 240, 260]` and insurer's estimate dashed red `#e74c3c` (dash 3/3) flat at 150.
- **Fire marker:** vertical dark red line (`#c0392b`, width 2) at month 6, labeled bold 11px "FIRE" at top.
- **Dispute zone:** area between the two projections (months 7–11) shaded `rgba(231,76,60,0.1)`, labeled in 10px `#c0392b` "DISPUTE" / "ZONE".
- **Legend (10px):** green "-- Insured claim", red "-- Insurer estimate", blue "-- Actual revenue".
- **Right title:** "Supply Chain: 1 Fire -> 100 Claims".
- **Cascade:** dark red circle (`#c0392b`, radius 18) at (530,55) with white bold 10px "Factory" / "FIRE"; red lines (`#e74c3c`, width 1.5) fanning to 5 orange circles (`#f39c12`, radius 10) at (430,120), (490,130), (540,135), (590,130), (650,120); 20 small blue dots (`#2980b9`, radius 3) in rows below at y≈162–178.
- **Caption (10px `#555`):** "100 downstream BI claims".

## Cyber Insurance

**Obj-title:** The "Uninsurable" Line: Fat-Tailed, Correlated, Evolving, Adversarial

- **Fat-tailed:** A single breach can cost $1B, so the average loss says almost nothing about exposure.
- **Correlated:** SolarWinds hit 18,000 organizations simultaneously — one event, thousands of claims.
- **Rapidly evolving:** Yesterday's risk model is tomorrow's joke; the risk changes every 6 months.
- **No actuarial history:** ~15 years of data for a risk that unstable — you cannot price from history.
- **Adversarial:** Attackers STUDY what's insured, so the "ransom demand = exactly the policy limit."

### Visualization (canvas `canvas7`, 720×300 declared; setup renders at 720×200)

Two panels: fat-tailed loss distribution (left) and ransom-vs-policy-limit bars (right).

- **Left title (bold 14px `#1a5276`):** "Cyber Loss Distribution (Log Scale)".
- **Curve:** blue `#2980b9` width 2 over 300px from x=30, baseline y=170: rises as sqrt for first half, decays exponentially to ~85%, then a wiggly persistent tail (sinusoidal bumps) at the far right.
- **Tail highlight:** rectangle x=270–330 shaded `rgba(192,57,43,0.15)` labeled bold 11px `#c0392b` "$1B+" / "tail".
- **X labels (10px `#555`):** "$10K", "$1M", "$100M", "$1B+".
- **Annotation (bold 10px `#c0392b`):** "SolarWinds: 18,000" / "orgs simultaneously".
- **Right title:** "Adversarial: Ransom = Policy Limit".
- **Paired bars** for companies `['Co A','Co B','Co C','Co D','Co E']` (x from 400 step 62, bar width 20, scale max 11 over 110px, baseline y=170): policy limits blue `#2980b9` `[2, 5, 10, 3, 8]`; ransom demands red `#e74c3c` `[1.9, 4.8, 9.5, 2.9, 7.7]`.
- **Legend (10px `#333`):** blue "Policy limit", red "Ransom demand".
- **Annotation (bold 11px `#c0392b`):** "Attackers KNOW" / "your coverage!".

## Life Insurance

**Obj-title:** Longevity Risk and Mortality Risk Point in Opposite Directions

- **Opposite bets:** Die too soon and the life policy pays out early, ahead of the premium collected.
- **Longevity side:** Live too long and the annuity business keeps paying, losing money every year.
- **COVID shock:** The mortality spike drove real life-insurance losses across the whole in-force book.
- **Fraud exposed:** The same episode revealed some "dead" policyholders had been alive all along.
- **Snapshot underwriting:** Healthy at 35 at application, obese at 45, still paying healthy rates.
- **Adverse selection after issue:** The premium stays locked while the underlying risk keeps drifting.

### Visualization (canvas `canvas8`, 720×300 declared; setup renders at 720×200)

Two panels: opposite-bets diagram (left) and adverse-selection line chart (right).

- **Left title (bold 14px `#1a5276`):** "Life vs Annuity: Opposite Bets".
- **Center dashed gray line** (`#7f8c8d`, dash 3/2) at y=110 from x=30 to 320, labeled "Expected mortality" (10px `#888`).
- **Upward arrow** (dark red `#c0392b`) at x=100 pointing up from the line, with bold 11px labels "Die too soon" / "Life policy LOSS"; **downward arrow** (purple `#8e44ad`) at x=250 pointing down, labeled "Live too long" / "Annuity LOSS".
- **COVID marker:** dark red filled box at (280,70) 35×30 with white bold 9px "COVID" / "spike".
- **Right title:** "Post-Issue Adverse Selection".
- **Actual health line:** dark red `#c0392b` width 2.5 through ages `['35','40','45','50','55','60']` (x from 410 step 50) with health scores `[95, 88, 72, 65, 55, 45]` (scale max 100 over 110px, baseline y=170).
- **Premium rate line:** dashed green `#27ae60` (dash 4/3) flat at 95.
- **Gap shading:** `rgba(192,57,43,0.1)` rectangle between the two lines from x=510 to 660.
- **Labels:** red 10px "Actual health"; green 10px "Premium rate (locked)"; x labels "Age 35"…"Age 60" (10px `#555`).

## Workers' Compensation

**Obj-title:** "Is the Injury Work-Related?" Is a Legal Question, Not a Medical One

- **Legal, not medical:** Coverage turns on where the injury happened, not on any medical finding.
- **Same pain, split verdict:** Back pain at work is fully covered; identical pain at home pays $0.
- **Intent prediction:** Malingering vs legitimate claim asks the model to predict INTENT, not medical facts.
- **Incentive effects:** Disability claim duration tracks benefit generosity, not just injury severity.
- **Dose response:** Duration runs 8 weeks at 50% wage replacement and 38 weeks at 80%.

### Visualization (canvas `canvas9`, 720×300 declared; setup renders at 720×200)

Two panels: duration-vs-benefit line (left) and legal-vs-medical comparison (right).

- **Left title (bold 14px `#1a5276`):** "Claim Duration vs Benefit Level".
- **Line:** dark red `#c0392b` width 2 with blue dots (`#2980b9`, radius 6) through wage-replacement levels `[50, 60, 66, 72, 80]`% (x from 50 step 65) and average durations `[8, 12, 18, 26, 38]` weeks (scale max 42 over 115px, baseline y=165); labels "50%"… below and "8wk"… above points (10px `#555`); axis caption "Wage replacement %" (10px `#888`).
- **Annotation (bold 11px `#c0392b`):** "Duration correlates with Benefits" / "NOT just injury severity".
- **Right title:** "Same Back Pain: Legal vs Medical".
- **Two person icons with arrows:** green (`#27ae60`) figure at y=55 labeled bold 12px "At WORK" with bold 13px green "COVERED" and 10px gray "Full benefits + medical"; dark red (`#c0392b`) figure at y=130 labeled "At HOME" with "NOT COVERED" and "Same injury, $0 WC benefit".
- **Between them:** purple (`#8e44ad`) bold 11px "Identical injury" / "Different legal answer", framed by dashed purple horizontal lines at y=85 and y=120.

## Reinsurance (Insurance of Insurance)

**Obj-title:** The Risk of the Risk: Layers Nobody Can See Through

- **Information asymmetry:** The insurer knows its own book far better than the reinsurer taking it on.
- **Selective cession:** It can CEDE the worst risks and keep the good ones, and nobody can tell.
- **Retrocessional layers:** Reinsurance of reinsurance lets one event cascade through 5 stacked layers.
- **Unknown total exposure:** Risk spreads across layers, so no single party sees the true total.
- **AIG 2008:** Nobody knew the concentration at the top node until every layer collapsed at once.

### Visualization (canvas `canvas10`, 720×300 declared; setup renders at 720×200)

Two panels: retrocession pyramid (left) and cession adverse-selection curves (right).

- **Left title (bold 14px `#1a5276`):** "Retrocessional Layers: Hidden Concentration".
- **Pyramid of boxes** (white 9px labels inside) connected by light gray lines (`#bbb`): bottom "Primary Insurer" `#3498db` (50,160); middle "Reinsurer A" `#2980b9` (30,120) and "Reinsurer B" `#2471a3` (110,120); upper "Retro Layer 1" `#1a5276` (70,80) and "Retro Layer 2" `#154360` (160,80); top "??? (AIG)" dark red `#c0392b` (120,40). All boxes ~70-80×25.
- **Annotation (bold 11px `#c0392b`):** "ALL risk concentrates" / "at unknown top node" / "(AIG 2008)".
- **Right title:** "Adverse Selection in Cession".
- **Two Gaussian curves** on a low-to-high risk axis (baseline y=170): retained book green `#27ae60` — taller (peak 90) centered left (peak near x=450, sd 25px); ceded book dark red `#c0392b` — shorter (peak 60) centered right (peak near x=550, sd 40px).
- **Labels:** bold 11px green "Retained (good)"; bold 11px dark red "Ceded to reinsurer (bad)"; bold 11px purple `#8e44ad` "Insurer KNOWS" / "Reinsurer DOESN'T"; axis labels 10px `#555` "Low risk" (left), "High risk" (right).

## Regeneration instructions

- **Layout:** detail page — h1 + `.subtitle`, then one `h2` per line of business (1.4em `#1a5276`, bottom border `2px solid #2980b9`), each followed by a single-row `.obj-table`: left `<td>` (40%) holds `.obj-title` (the bold pitfall headline given above) and a `<ul>` whose bullets each start with a bold label; right `<td>` (60%, centered) holds the canvas. Even table rows background `#fafcfe`.
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; subtitle `#666` 1.05em; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em, weight 600, `#1a5276`; `strong` in `#1a5276`; `.philosophy` callout style defined (background `#f0f4f8`, left border `4px solid #2980b9`) though unused on this page. No nav bar, no back/home links.
- **Canvas:** each `<canvas>` declares `width="720" height="300"`, but the shared `setupCanvas(id)` helper sets both backing store and CSS size to 720×200 (× `window.devicePixelRatio`, then `ctx.scale` back to logical coordinates). Regenerate with the same behavior or draw within a 720×200 area.
- **Palette:** primary blue `#1a5276`, secondary blues `#2980b9`/`#3498db`/`#2471a3`/`#154360`, green `#27ae60`, red `#e74c3c`, dark red `#c0392b`, orange `#f39c12`, purple `#8e44ad`, gray text `#555`/`#666`/`#7f8c8d`/`#888`.
