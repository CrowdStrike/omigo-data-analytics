# Death Over-Attribution During Outbreaks

**Page type:** detail page (h2 section per pitfall, each with a two-column obj-table row: text left 50%, canvas right 50%)
**HTML title tag:** 141. Death Over-Attribution During Outbreaks

**Subtitle:** During outbreaks, deaths of the already-terminal get attributed to the novel pathogen — a common cold can kill a stage-4 cancer patient, but that doesn't make it a deadly pandemic.

## Callout (philosophy box)

**The core problem:** Systematically attributing deaths to the salient new cause while ignoring competing risks inflates mortality statistics and distorts policy. The statistical sin: confusing "died WITH" for "died FROM."

## Competing Risks Erasure — The Person Was Already Dying

**"Died With X" ≠ "Died From X" — But Gets Counted the Same**

- **How it works:** A terminal cancer patient with a 3-month prognosis contracts the virus and dies 2 weeks later.
- **The certificate:** It lists the virus, and the stage-4 cancer vanishes from the mortality statistics.
- **Competing risks:** The virus didn't kill a healthy person — it accelerated a death already in progress.
- **The counterfactual:** Would this patient have been alive 6 months later without the virus? Likely not.
- **The "last straw" fallacy:** The proximate trigger, any minor infection, is not the underlying cause.
- **What systems record:** Attribution logs the trigger, not the condition that made death imminent.
- **Why it matters:** "5,000 dead" panics; "5,000 already-dying died sooner" does not — framing drives policy.

### Visualization (canvas `c1`, 720×300)

Two framed comparison boxes: reported story vs medical reality.

- **Title (bold 16px `#1a5276`, centered):** "\"Died With\" vs \"Died From\" — Same Death, Different Story".
- **Left box ("AS REPORTED"):** red-stroked rect (40,40) 300×90 with fill `#e74c3c` at 15% alpha; header "AS REPORTED" in bold red; body lines in `#333`: "Patient, 91, dies of Novel Virus" / "→ Outbreak death count +1" / "→ \"Pandemic kills elderly\"".
- **Right box ("REALITY"):** green-stroked rect (380,40) 300×90 with fill `#27ae60` at 15% alpha; header "REALITY" in bold green; body lines: "Patient, 91: stage 4 cancer, CHF," / "CKD stage 4, dementia. Prognosis: weeks." / "Also tested positive. Died on schedule."
- **Bottom text (centered):** bold red: "The virus was the last straw — not the load-bearing wall."; gray `#555`: "Substitute \"common cold\" or \"flu\" and the outcome is identical." and "If ANY respiratory infection would have killed them → fragility is the cause, not the specific pathogen."

## Age-Related Baseline Mortality Ignored

**Very Old People Die — With or Without the Outbreak**

- **Base rate of death:** A nursing home loses ~25% of residents per year from all causes, outbreak or not.
- **The right comparison:** Outbreak deaths must be measured against expected mortality, never against zero.
- **Naming anonymous deaths:** The outbreak names deaths that would otherwise read "natural causes."
- **Newsworthiness flip:** 22 deaths in a home averaging 20/year is nothing; "20 died after outbreak" is a story.
- **Pooled vs stratified:** A pooled "3% mortality rate" terrorizes the young, whose real risk is near zero.
- **Where the burden sits:** The deaths concentrate in age groups already at high baseline mortality.
- **The denominator problem:** A 95-year-old has ~2-3% chance of dying in any 28-day window regardless.
- **What the rule captures:** So "died within 28 days of positive test" relabels baseline deaths as outbreak deaths.

### Visualization (canvas `c2`, 720×300)

Bar chart of annual baseline mortality percentage by age group.

- **Title (bold 16px `#1a5276`):** "Annual Baseline Mortality by Age (No Outbreak)".
- **Bars (fill `rgba(26,82,118,0.35)`, stroke `#1a5276`, width 70, spacing 90 starting x=80, baseline y=180, scale max 35% over 130px):** 40-50: 0.3%, 50-60: 0.6%, 60-70: 1.5%, 70-80: 4%, 80-85: 10%, 85-90: 18%, 90+: 30%. Value labels bold `#1a5276` above each bar; age labels below.
- **Axis labels:** "Age group" (bottom center); rotated "Annual mortality %" on the left.
- **Bottom text (centered):** bold red: "A 90-year-old has ~3% chance of dying in ANY 28-day window — with or without outbreak."; gray `#555`: "\"Died within 28 days of positive test\" captures baseline mortality and relabels it."

## Comorbidity Stacking — 4+ Conditions, Each Individually Fatal

**When Someone Has 5 Fatal Conditions, Picking One as "The Cause" Is Arbitrary**

- **The typical case:** A multi-morbid elderly patient dies with the virus recorded as the cause of death.
- **Remove one block:** Take away any single comorbidity and they might well have survived the infection.
- **Single attribution is bureaucratic:** Death certificates demand exactly one "primary cause" be named.
- **What that forces:** One label gets stamped onto cascading multi-system failure across several organs.
- **The structural flaw:** That forced single choice is precisely what enables systematic over-counting.
- **Substitution test:** If a common cold or seasonal flu produces the same outcome, fragility is the cause.
- **Interchangeable pathogen:** The specific virus is then incidental — any respiratory insult would do.

### Visualization (canvas `c3`, 720×300)

Stack of comorbidity condition blocks with the virus block on top getting the blame.

- **Title (bold 16px `#1a5276`):** "Comorbidity Stack: Which One \"Caused\" the Death?"
- **Stacked blocks (400×30 rects at x=60, 4px gaps, each stroked in its color with 20% alpha fill, text `#333`):** Congestive Heart Failure `#e74c3c`; Stage 4 Cancer `#8e44ad`; COPD (Severe) `#e67e22`; CKD Stage 4 `#f39c12`; Diabetes (20yr) `#d35400`.
- **Top block:** blue `#2980b9` (30% alpha fill, 2px stroke), bold blue text: "+ Novel Virus (mild) ← THIS gets the blame"; bold red label to its right: "← \"Cause of death\"".
- **Bottom gray text (centered):** "Remove any ONE comorbidity → patient might survive the virus." and "Replace virus with common cold → same outcome. The stack kills. The virus is interchangeable."

## Availability Bias + Novelty = Panic Attribution

**Novel Threats Get All the Credit; Familiar Killers Stay Invisible**

- **Salience drives attribution:** The novel pathogen earns 24/7 coverage from its first reported case.
- **Invisible comparison:** Far larger familiar killers run in the background with no coverage at all.
- **The formula:** Novelty times media attention equals perceived threat, independent of actual body count.
- **Anchoring to the timeline:** A death already scheduled by terminal illness becomes an "outbreak death."
- **Why it counts:** The only qualifying fact is that the outbreak happened to be concurrent with the death.
- **Feedback loop:** Inflated statistics drive panic, and panic drives more testing of dying populations.
- **The ratchet:** More testing means more deaths get the outbreak label, so the statistics inflate further.

### Visualization (canvas `c4`, 720×300)

Horizontal bar chart of daily deaths by cause, with the smallest (novel outbreak) highlighted red.

- **Title (bold 16px `#1a5276`):** "Daily Deaths: Novel Outbreak vs Invisible Killers".
- **Bars (start x=180, scale 380px per 2200 deaths, height 28, 12px gaps; 35% alpha fill + 1px stroke; cause name right-aligned at x=170; value label after bar):** Heart disease 2000/day `#95a5a6`; Cancer 1600/day `#95a5a6`; Medical errors 700/day `#95a5a6`; Diabetes 300/day `#95a5a6`; Novel outbreak 200/day `#e74c3c`.
- **Annotations:** "(invisible, \"normal\")" in `#333` under the gray bars; bold red "← 24/7 NEWS COVERAGE, PANIC" beside the outbreak bar.
- **Bottom gray text (centered):** "Novelty × media = perceived threat. Familiar killers get no attention regardless of body count."

## "Died Within 28 Days of Positive Test" — A Definition That Guarantees Over-Counting

**Administrative Definitions Replace Clinical Judgment**

- **The definition:** Any death within 28 days of a positive test is counted as an outbreak death.
- **What that includes:** The bus accident and the pre-existing liver failure both land in the same total.
- **Why it was chosen:** Administrative definitions are fast, cheap, and consistent across every reporter.
- **What it displaced:** Clinical causation is slow and needs judgment, so counting speed beat causal truth.
- **Scale of over-count:** Against clinically-judged primary cause, the definition over-attributes 20-30%.
- **In the elderly:** That over-attribution reaches 40-50% in the oldest and most multi-morbid populations.
- **Perverse incentive:** Tie funding to outbreak patient counts and testing every dying patient pays.

### Visualization (canvas `c5`, 720×300)

Timeline diagram: positive test marker, 28-day window band, unrelated deaths inside it counted as outbreak deaths.

- **Title (bold 16px `#1a5276`):** "\"Died Within 28 Days of Positive Test\" — What It Captures".
- **Timeline:** horizontal `#333` line (width 2) from (60,100) to (660,100); blue `#2980b9` 6px dot at (150,100) labeled "Positive test" above.
- **Window:** dashed (4/3) red rect (150,90) 400×20 with 10% alpha red fill, labeled "28-day window" below.
- **Deaths inside window (red "†" dagger at x=250, 380, 500 with labels below):** "Bus accident", "Liver failure (pre-existing)", "Heart attack (chronic)" — each annotated in bold red: "→ \"Outbreak death\"".
- **Bottom text (centered):** bold red: "Administrative convenience ≠ clinical causation. But it's what gets reported."; gray `#555`: "If reimbursement depends on classification → economic incentive to test every dying patient."

## Excess Mortality — The Only Honest Metric (and Its Limitations)

**Compare Total Deaths to Expected Deaths — Everything Else Is Attribution Theater**

- **What it tells you:** "Did more people die than expected?" bypasses every attribution problem at once.
- **The diagnostic:** Near-zero excess with high attributed deaths means deaths were relabeled, not caused.
- **The gap is the measurement:** Attributed deaths minus excess all-cause mortality measures over-attribution.
- **Limitations:** Excess mortality also absorbs collateral deaths caused by the response, not the pathogen.
- **What that includes:** Delayed care and suicides land in the excess count as if the pathogen caused them.
- **Harvesting effect:** Below-average mortality after a wave means deaths were pulled forward by weeks.
- **Why that matters:** A deficit following a surplus is itself diagnostic of over-attribution to the pathogen.

### Visualization (canvas `c6`, 720×300)

Two horizontal comparison bars (attributed vs excess deaths) plus a harvesting-effect text block.

- **Title (bold 16px `#1a5276`):** "Excess Mortality: The Honest Metric".
- **Bar 1:** red rect (80,50) 250×40 (25% alpha fill, red stroke), label "Attributed outbreak deaths: 10,000" in bold `#333`.
- **Bar 2:** green rect (80,105) 90×40 (25% alpha fill, green stroke), label "Excess mortality: 3,000".
- **Gap annotation (bold orange `#e67e22`):** "Gap: 7,000 deaths relabeled, not caused."
- **Harvesting block:** bold blue heading "Harvesting Effect (mortality displacement):"; gray lines "Wave kills the most fragile → NEXT month shows BELOW-average mortality" and "Deaths were \"pulled forward\" by weeks, not created."; bold red: "If deficit follows surplus → the outbreak mostly accelerated already-imminent deaths."

## Policy Cascade — Inflated Numbers Drive Disproportionate Response

**Over-Attributed Deaths → Panic → Policy That Causes More Harm Than the Disease**

- **The cascade:** Inflated numbers drive panic-based policy, and the policy carries its own body count.
- **Net effect:** That collateral damage can kill more people than the pathogen the policy was aimed at.
- **Second-order mortality:** Delayed cancer diagnoses, skipped cardiac care, suicides, and overdoses.
- **Never attributed:** None of those deaths get charged to "the response to inflated statistics."
- **But that is the chain:** Inflated attribution to extreme policy to forgone care is their actual causal path.
- **One-sided accounting:** Outbreak deaths are visible and individually named in the daily reporting.
- **The invisible side:** Collateral deaths scatter across categories and never count against the intervention.

### Visualization (canvas `c7`, 720×300)

Five-step flow diagram with arrows.

- **Title (bold 16px `#1a5276`):** "Over-Attribution → Panic → Policy → Collateral Mortality".
- **Steps (120×50 boxes at y=45, 15% alpha fill + 1.5px stroke in each color, "→" between boxes):** "Over-attributed deaths" `#e74c3c` (x=30); "Inflated statistics" `#e67e22` (x=170); "Media panic" `#f39c12` (x=310); "Extreme policy" `#8e44ad` (x=450); "Collateral deaths" `#2c3e50` (x=590).
- **Bottom text (centered):** gray `#555`: "Collateral: delayed cancer Dx, skipped cardiac care, suicides, overdoses, domestic violence."; bold red: "These deaths NEVER get attributed to \"the response to inflated statistics.\""; `#333`: "One-sided accounting: outbreak deaths visible + named. Collateral deaths invisible + scattered." and "Net harm = (outbreak deaths - would-have-died-anyway) - collateral deaths. Sometimes NEGATIVE."

## The Correct Framework — Attributable Fraction, Not Binary Cause

**Disease X Contributed 15% to This Death, Not 100%**

- **Attributable fraction:** The honest question is "what fraction of this death is attributable to X?"
- **The binary alternative:** "Did X cause it?" counts a 15% contribution exactly as it counts a 100% one.
- **Years of life lost:** A terminal patient's final weeks and a healthy 40-year-old's four decades both score 1.
- **What that hides:** Orders-of-magnitude differences in real impact collapse into an identical death count.
- **How to do it right:** Measure mortality in patients carrying no other fatal condition at all.
- **What that isolates:** That figure is true pathogen lethality; the rest is interaction with comorbidity.
- **Why it's not done:** Nuance doesn't justify emergency powers, so incentives favor binary over-attribution.

### Visualization (canvas `c8`, 720×300)

Two comparison boxes (binary vs attributable fraction) plus a years-of-life-lost text block.

- **Title (bold 16px `#1a5276`):** "Binary Attribution vs Attributable Fraction".
- **Left box:** red rect (40,45) 300×70 (20% alpha fill, red stroke); heading "BINARY (current system)" bold red; lines "Did virus cause death? YES → count as 1." and "88yo w/ 4 fatal conditions = 1 death."
- **Right box:** green rect (380,45) 300×70 (20% alpha fill, green stroke); heading "ATTRIBUTABLE FRACTION (honest)" bold green; lines "Virus contributed 15% to this death." and "Pre-existing conditions: 85%. Count: 0.15."
- **YLL block:** bold blue heading "Years of Life Lost (YLL):"; `#333` lines "Terminal 91yo dies 3 weeks early = 0.06 YLL.    Healthy 40yo dies = 40 YLL." and "Current system: both = \"1 death.\" YLL reveals they're 700× different in impact."
- **Bottom text (centered):** bold red: "Why it's not done: \"15% attributable, 0.06 YLL\" doesn't justify emergency powers."; gray `#555`: "\"50,000 DEAD\" does. Incentives at every level favor binary over-attribution."

## No Baseline Established — Seasonal Spike Reported as Outbreak

**"10,000 Flu Cases This Month!" — But 7,000 Happen Every Month Regardless**

- **The missing baseline:** Respiratory infections happen year-round, in every month, at a steady rate.
- **Winter start date:** Begin counting in winter and every case looks like it belongs to the "outbreak."
- **Testing creates cases:** Mass testing "finds" the background hum of illness that was always there.
- **Never measured before:** That background was real but uncounted, so cases didn't surge — testing did.
- **The honest comparison:** Same test, same population, same month, previous year — then subtract.
- **The real signal:** The true outbreak signal is that year-over-year difference, not the gross count.

### Visualization (canvas `c9`, 720×300)

Monthly stacked bar chart splitting each bar into background baseline (blue) and true excess (red), with a dashed baseline line.

- **Title (bold 16px `#1a5276`):** "Seasonal Spike vs Year-Round Background Rate".
- **Bars:** months Jan–Dec with rates (thousands) `[11, 9.5, 8, 7, 6.5, 6, 6, 6.5, 7, 8, 9.5, 12]`; scale max 14 over 140px, bar width 48, spacing 55 from x=55, baseline y=200; below-6k portion filled `rgba(26,82,118,0.25)`, above-6k excess portion filled `rgba(231,76,60,0.3)`, thin `#1a5276` outline; month labels beneath.
- **Baseline line:** dashed (5/3) red horizontal line width 2 at the 6k level, right-aligned label "Background: ~6k/month" in bold red.
- **Bottom text (centered):** bold red: "Red = TRUE excess above year-round baseline. Blue = would happen anyway."; gray `#555`: "\"12,000 cases in December!\" — but 6,000 happen every month. Excess = 6,000, not 12,000."

## Incidence Inflation — Reporting Gross Instead of Net

**Total Cases Minus Background Rate = Actual Signal. Nobody Does the Subtraction.**

- **Gross vs net:** Headlines report gross totals, and nobody subtracts the normal-month baseline.
- **Size of the error:** With no subtraction, the reported number can be several times the true signal.
- **Why baselines aren't established:** Boring background rates are not funded, tracked, or newsworthy.
- **The systematic failure:** That absence of a baseline is precisely what makes the panic possible.
- **How to fix it:** Run continuous year-round surveillance and publish it before any outbreak begins.
- **What that buys:** Excess becomes immediately calculable, so gross totals can't pose as the whole signal.

### Visualization (canvas `c10`, 720×300)

Headline bar vs decomposed baseline+signal bars, plus a traffic-counter analogy.

- **Title (bold 16px `#1a5276`):** "Gross vs Net: What Headlines Report vs Actual Signal".
- **Gross bar:** red rect (80,50) 250×50 (30% alpha fill, 1.5px red stroke) labeled in bold red: "HEADLINE: \"50,000 hospitalized!\"".
- **Decomposed bar:** gray `#95a5a6` rect (80,120) 150×50 labeled "Baseline: 30,000" + red rect (230,120) 100×50 labeled "Signal: 20,000" in bold red; bold `#333` annotation: "← REALITY: 60% is noise, 40% is signal".
- **Analogy block:** bold blue heading "Without baseline:"; gray lines "City never measured traffic → installs counter → \"CRISIS: 50,000 cars today!\"" and "Normal daily traffic: 48,000. Actual excess: 2,000. Panic factor: 25×."
- **Bottom bold red (centered):** "No year-round surveillance = no denominator = every observation looks like a spike."

## Regeneration instructions

- **Layout:** detail page. h1 + `.subtitle` + one `.philosophy` callout, then one `<h2>` per pitfall followed by a one-row `.obj-table`: left `<td>` (40%) holds `.obj-title` div + `<ul>` of labeled bullets, right `<td>` (60%, centered) holds the canvas. Even table rows have background `#fafcfe`.
- **Page CSS:** body system sans-serif, white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.4em `#1a5276` with 2px `#2980b9` bottom border; subtitle `#666` 1.05em; ul 0.9em `#333`; `strong` `#1a5276`; `.obj-title` 1.05em weight 600 `#1a5276`; `.philosophy` background `#f0f4f8`, left border 4px solid `#2980b9`, padding 12px 16px, 0.9em; table cell borders `1px solid #e0e0e0`, padding 20px 24px. No nav bar, no back/home links.
- **Canvas:** all charts 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart text is 11-16px -apple-system. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276` / `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, yellow-orange `#f39c12`, dark orange `#d35400`, purple `#8e44ad`, gray `#95a5a6`, dark slate `#2c3e50`, bar fill `rgba(26,82,118,0.35)`, gray text `#555`/`#333`.
- In regenerated HTML, any card links use `.html` extensions (this page has none).
