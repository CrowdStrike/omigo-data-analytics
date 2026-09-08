# Curse of Sampling

**Page type:** detail page (h2 section per pitfall, each with a two-column obj-table row: text left 50%, canvas right 50%)
**HTML title tag:** 139. Curse of Sampling

**Subtitle:** When data is a mixture of sub-populations, random sampling loses the rare modes — and every fix (stratification, oversampling) distorts something else.

## Callout (philosophy box)

**The fundamental problem:** A random sample draws from each mode in proportion to its prevalence, so rare modes get 0-2 samples. Re-sampling to fix this distorts the natural base rates the model needs. Every sampling strategy helps one thing and hurts another.

## Rare Class Gets Zero Samples

**Positive Rate 0.1% → Random Sample Almost Certainly Misses It**

- **The math:** At 0.1% fraud rate, a 10K sample expects ~10 positives — and since fraud clusters, often 0-3.
- **Worse than random:** Rare events follow their own patterns — 3am timestamps, $0.01 test charges.
- **The tail you need:** Random sampling under-represents exactly the sub-patterns you have to model.
- **Size doesn't save you:** Segmenting fraud into 5 types at 100 each needs a 500K sample.
- **The alternative:** For rare classes, collect the positives exhaustively instead of sampling.
- **Real case:** A 2000-patient trial at 0.5% prevalence gets ~10 positives total — no subgroup analysis.

### Visualization (canvas `c1`, 720×300)

Diagram of stacked proportion bars showing a rare class vanishing in a sample.

- **Title (bold 17px `#1a5276`, top center):** "Population: 0.1% Positive. Sample 10K → Expected 10 Positives."
- **Population bar:** rect at (50,45) 580×30 in `rgba(41,128,185,0.3)` with centered label "99.9% Negative (millions)" in `#2980b9`; tiny positive slice rect at (632,45) 15×30 in `#e74c3c`, labeled "0.1%" in red to its right.
- **Sample bar:** rect at (50,95) 580×25 in `rgba(41,128,185,0.3)` labeled "Sample 10K: ~9,990 negative" in `#2980b9`; positive slice rect at (632,95) 3×25 in `#e74c3c`, labeled "0-10 pos" in red.
- **Bottom text lines (centered):** bold red (`#e74c3c`): "Can't train on 10 examples. Can't segment. Can't validate. Can't deploy."; gray (`#555`): "Need 100K sample for 100 positives. Need 500K to segment by fraud type." and "For rare classes: exhaustive collection, not sampling."

## Stratified Sampling Distorts Natural Base Rates

**50/50 Class Balance = Model Learns Wrong Priors**

- **The "fix":** Oversample fraud to a 50/50 balance and the model learns a prior of P(fraud) ≈ 50%.
- **Miscalibrated:** Deployed where the real rate is 0.1%, every posterior probability comes out far too high.
- **Threshold breaks:** "Flag if P > 0.5" was tuned on balanced data, not the production operating point.
- **No new information:** Oversampling duplicates the same few fraud examples, adding no fresh signal.
- **Memorization:** The model learns those duplicated rows by heart and misses novel fraud patterns.

### Visualization (canvas `c2`, 720×300)

Two horizontal class-proportion bars comparing natural vs oversampled distributions.

- **Title (bold 17px `#1a5276`, top center):** "Oversampling to 50/50: Model Learns Wrong P(fraud)".
- **Natural bar (labeled "Real: 99.9% neg / 0.1% pos"):** blue rect `#2980b9` at (50,50) 560×25; red rect `#e74c3c` at (610,50) 50×25.
- **Oversampled bar (labeled "Training: 50% neg / 50% pos"):** blue rect at (50,100) 330×25; red rect at (380,100) 280×25.
- **Bottom text (centered):** bold red: "Model learns P(fraud) ≈ 50%. Deploys where P(fraud) = 0.1%."; gray `#555`: "Result: flags 40% of transactions as \"suspicious.\" False positive rate: catastrophic." and "Calibration broken. Threshold meaningless. Precision/recall from training: fiction."

## Sub-Populations Disappear in Samples

**5 Modes in Population → Sample Captures 2-3. Others Vanish.**

- **The problem:** A 5% mode — executives in income data — yields ~50 points in a 1000-row sample.
- **Hint, not shape:** Those 50 points show the mode exists but cannot pin its location or spread.
- **Cascading loss:** Cross 4 dimensions — income × age × location × device — and most cells get 0-3 rows.
- **Worst where it matters:** The most distinctive intersectional sub-groups get the fewest observations.
- **Model sees unimodal:** With only 50 points, the rare mode reads as tail noise in the fitted density.
- **Outlier misdiagnosis:** The model then treats a real sub-population as outliers to be trimmed away.

### Visualization (canvas `c3`, 720×300)

Two overlaid density curves: 5-mode population vs 3-mode sample.

- **Title (bold 17px `#1a5276`):** "Population: 5 Modes. Sample: Captures 2-3, Loses Rest."
- **Population curve (solid `#2980b9`, width 2, baseline y=100, x from 60 to 660):** Gaussian mixture — gauss(x,60,20)×100 + gauss(x,180,25)×150 + gauss(x,300,30)×120 + gauss(x,420,20)×60 + gauss(x,540,15)×30 (gauss = normalized exp kernel /(σ·2.507)). Labeled below in blue: "Population (all 5 modes visible)".
- **Sample curve (dashed 5/5 `#e74c3c`, width 2, baseline y=165):** gauss(x,60,22)×90 + gauss(x,180,28)×140 + gauss(x,300,35)×100 — only 3 modes. Labeled in red: "Sample n=1000 (modes 4 & 5: gone or noise)".
- **Bottom bold red text:** "Model trained on sample: thinks feature has 3 modes. Deploys to population with 5."

## Geographic Bias — Some Locations Systematically Under-Sampled

**Online Sampling Over-Represents Urban, Young, English-Speaking**

- **The mechanism:** Online collection requires internet access, literacy, and willingness to respond.
- **Random within reach:** The "random sample" is random only inside the reachable subset of people.
- **App data:** 50M users represent app-users, not people — non-users are invisible to the pipeline.
- **Health data:** Clinic data represents that clinic's catchment; models fail on demographics never in it.
- **The interaction:** Geographic bias multiplies with rare-disease prevalence to erase a condition entirely.
- **Zero sensitivity:** A condition absent from the training geography goes undetected everywhere on deployment.

### Visualization (canvas `c4`, 720×300)

Table-style paired horizontal bar chart: population % vs in-sample % per group.

- **Title (bold 17px `#1a5276`):** "Online Sample: Who You CAN'T Reach".
- **Column headers (bold, `#333`):** "Group" (x=60), "Population %" (x=280), "In Sample %" (x=450).
- **Rows (bars scale 3px per percent, 18px tall, group name at left, value label after each bar):**
  - Urban tech-savvy — population 25%, sampled 70%, color `#27ae60`
  - Suburban middle — population 30%, sampled 22%, color `#f39c12`
  - Rural / elderly — population 25%, sampled 5%, color `#e74c3c`
  - Non-English / low-income — population 20%, sampled 3%, color `#e74c3c`
- **Bottom bold red text (centered):** "45% of population gets 8% representation. Model fails for MOST people."

## Age/Demographic Strata Have Different Distributions

**Same Feature, Different Shape Per Age Group — Sample Loses the Structure**

- **Income by age:** Each age band carries its own shape, some bimodal, so the aggregate has 5+ modes.
- **Per-band shortfall:** 200 observations per band is not enough to fit separate per-group models.
- **Hidden interactions:** A correlation of +0.4 in one age group and -0.2 in another averages to +0.15.
- **Simpson's paradox:** That pooled +0.15 hides both signs, and detecting it needs 1000+ per group.
- **The impossible requirement:** 30+ observations per sub-population is the bare minimum for estimation.
- **Intersectional blowup:** 1000+ strata cells need 30K+ samples, and most datasets are tiny by comparison.

### Visualization (canvas `c5`, 720×300)

Row of 5 mini Gaussian curves, one per age group, each with label and mean income.

- **Title (bold 17px `#1a5276`):** "Same Feature, Different Distribution Per Age Group".
- **Mini curves:** each a gauss(x,60,20)×60 curve of width 120px, origin x = 30 + i×140, baseline y=130, stroke width 1.5, colored per group with the age label and dollar mean beneath:
  - 18-25, $25K, `#3498db`
  - 25-35, $45K, `#27ae60`
  - 35-50, $80K, `#f39c12`
  - 50-65, $110K, `#e67e22`
  - 65+, $30K, `#8e44ad`
- **Bottom text (centered):** bold red: "Sample 200 per group: 200 per mode → barely enough for means, not for interactions."; gray `#555`: "100 sub-populations × 30 min per cell = need 3000+ sample. Most studies: 500."

## Time-Based Sampling Misses Regime Changes

**Sample From "Last Month" — Miss the Other 11 Months' Patterns**

- **Seasonal modes vanish:** E-commerce runs distinct monthly regimes — sample one, the model drifts 11 others.
- **Day-of-week:** Weekday and weekend sessions have genuinely different behavioural modes.
- **Either way loses:** Sample one day type and miss the other; sample both and get an unexplained mixture.
- **Regime changes:** No single time window captures a non-stationary process (e.g., pre/post COVID behavior).
- **Rare temporal events:** Black swans arrive every 5-10 years yet cause most of the cumulative losses.
- **Wrong conclusion:** Any window shorter than that reports "this never happens" with full confidence.

### Visualization (canvas `c6`, 720×300)

12-month bar chart with one sampled month highlighted.

- **Title (bold 17px `#1a5276`):** "Sample One Month → Miss 11 Other Regimes".
- **Bars:** months J F M A M J J A S O N D with values `[30, 50, 55, 60, 65, 45, 40, 42, 70, 80, 95, 150]` (bar height = value×0.8, baseline y=140, bar width = (w−120)/12 with 4px gap, starting x=60). All bars `rgba(41,128,185,0.4)` except March (index 2) filled `#e74c3c`; month initials beneath each bar.
- **Highlight:** red 2.5px stroke rect around the March column (y 38, height 120); bold red label below: "↑ Sampled: March".
- **Bottom gray text (centered):** "Missed: holiday spike (Dec), summer lull, back-to-school. Model: \"users always behave like March.\""

## SMOTE and Synthetic Oversampling — Inventing Data That Doesn't Exist

**Interpolating Between Rare Examples Creates Points in Empty Space**

- **The geometry:** SMOTE interpolates between minority neighbors along a straight line in feature space.
- **Territory crossing:** That line can cut through majority territory, minting "minority" points inside it.
- **No new information:** 5000 synthetic points are linear combinations of the original 50 real rows.
- **Zero added knowledge:** Interpolation adds nothing about the rare class that those 50 did not already carry.
- **Fake validation:** Synthetic-balanced test sets score well — they are interpolations of your training data.
- **Deployment gap:** Deployed recall collapses on real minority cases no interpolation resembled.

### Visualization (canvas `c7`, 720×300)

Scatter diagram: two real minority points, a dashed interpolation line crossing a majority cloud, synthetic points on the line.

- **Title (bold 17px `#1a5276`):** "SMOTE: Interpolating Through Majority Territory".
- **Majority cloud:** filled circle center (350,100) radius 80 in `rgba(41,128,185,0.15)`, centered label "Majority class" in `#2980b9`.
- **Real minority points:** two 6px-radius red (`#e74c3c`) dots at (150,70) and (550,130), labeled "Real minority A" and "Real minority B".
- **Interpolation line:** dashed 4/4 red line width 1.5 from (150,70) to (550,130).
- **Synthetic points:** three 5px orange (`#e67e22`) dots at (300,90), (380,105), (450,115).
- **Bottom text (centered):** bold orange: "Synthetic \"minority\" points — INSIDE majority territory!"; gray `#555`: "SMOTE assumes linear interpolation stays on class manifold. It doesn't."

## Sampling Bias in A/B Tests — Non-Representative Enrollment

**Who Enters the Test ≠ Who Will Use the Feature**

- **New-user bias:** A 2-week test enrolls 100% of daily users but almost none of the light users.
- **Narrow generalization:** The result therefore holds for heavy users, not for the wider user base.
- **Opt-in bias:** Cookie-accepters skew younger and more ad-tolerant than the population does.
- **Missing segment:** The privacy-conscious never enroll, and they react differently to the feature.
- **Geography:** "Passed in US" tells you little about Asia or Europe, where behavior differs.
- **Survivor enrollment:** Early adopters enroll first, log the most measurement time, and outweigh the mainstream.

### Visualization (canvas `c8`, 720×300)

Text-list diagram of enrollment rates by user visit frequency.

- **Title (bold 17px `#1a5276`):** "Who Enters the A/B Test ≠ Who Uses the Product".
- **Intro line (`#333`, left aligned):** "Test runs 2 weeks. Enrollment by visit frequency:".
- **Rows (name at x=80, enrollment at x=340, each in its own color):**
  - Daily users (power) — 100% enrolled — `#27ae60`
  - Weekly users — 90% enrolled — `#f39c12`
  - Monthly users — 50% enrolled — `#e67e22`
  - Quarterly users — ~0% enrolled — `#e74c3c`
- **Bottom text (centered):** bold red: "Test over-represents heavy users. Result: \"feature works!\" for daily users."; gray `#555`: "Deploy to all → light users (majority of user base) get a feature tested on power users."

## Feature Distribution in Sample ≠ Feature Distribution in Population

**Sample Sees Unimodal. Population Is Multimodal. Model Breaks on Deployment.**

- **The scenario:** Training data comes from one dominant mode and looks reassuringly normal-ish.
- **Production surprise:** Live traffic then arrives from the modes the sample never contained.
- **Invisible in validation:** The validation set shares the same sampling bias, so accuracy looks fine.
- **Wrong transforms:** A log-transform picked because the sample "looks right-skewed" cannot fix bimodality.
- **Weak tests:** Shapiro-Wilk "can't reject normality" on an under-sampled trimodal feature proves little.
- **Low power:** The test barely sees missing modes, and "can't reject" is not the same as "is normal."

### Visualization (canvas `c9`, 720×300)

Side-by-side density curves: unimodal-looking sample (left) vs trimodal population (right).

- **Title (bold 17px `#1a5276`):** "Sample Looks Normal. Population Is Trimodal."
- **Left curve (green `#27ae60`, width 2, x from 40 over 250px, baseline y=100):** gauss(x,125,40)×100. Labels beneath in green: "Sample (n=1000)" and "Shapiro-Wilk: p=0.3 \"Normal!\"".
- **Right curve (red `#e74c3c`, width 2, x from 420 over 250px, baseline y=100):** gauss(x,50,20)×80 + gauss(x,125,15)×60 + gauss(x,200,20)×50. Labels beneath in red: "Population (full)" and "Actually trimodal!".
- **Bottom text (centered):** bold red: "Log-transform applied (assuming unimodal) → wrong transformation for the real shape."; gray `#555`: "Validation set has same bias → looks fine. Production: hits modes 2 & 3 → breaks."

## The Bootstrap Doesn't Fix Representation — It Amplifies Bias

**Resampling From a Biased Sample Gives You Confident Wrong Answers**

- **The limit:** Bootstrap resamples only what you have — a mode the sample missed can never appear.
- **Precisely wrong:** A narrow CI around a biased estimate can sit entirely outside the true value.
- **False comfort:** "Stable across 10,000 resamples" only proves the sample is self-consistent.
- **Not representativeness:** Stability says nothing about whether the sample matches the population.
- **When it helps:** Estimating standard errors inside an already well-sampled population.
- **When it fails:** Whenever structure — modes, subgroups, strata — is under-represented in the sample.

### Visualization (canvas `c10`, 720×300)

Confidence-interval diagram: narrow bootstrap CI far from the dashed true-value line.

- **Title (bold 17px `#1a5276`):** "Bootstrap: Precise Estimate of the WRONG Number".
- **True value:** vertical dashed green (`#27ae60`, dash 6/4, width 2) line at x=450 from y=40 to y=130, labeled below "True μ = $78K".
- **Bootstrap CI:** red band `rgba(231,76,60,0.2)` rect (230,70) 80×20; red horizontal line width 2.5 from (230,80) to (310,80); red 5px dot at (270,80). Bold red labels: "Bootstrap CI: $55K ± $3K" (above) and "(precise, confident, WRONG)" (below).
- **Bottom text (centered):** gray `#555`: "Sample missed executives (5% of pop at $300K+). Bootstrap can't discover missing modes."; bold red: "Tight CI ≠ correct answer. Stability ≠ accuracy. More resamples ≠ less bias."

## Regeneration instructions

- **Layout:** detail page. h1 + `.subtitle` + one `.philosophy` callout, then one `<h2>` per pitfall followed by a one-row `.obj-table`: left `<td>` (40%) holds `.obj-title` div + `<ul>` of labeled bullets, right `<td>` (60%, centered) holds the canvas. Even table rows have background `#fafcfe`.
- **Page CSS:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.4em `#1a5276` with 2px `#2980b9` bottom border, padding-bottom 8px; subtitle `#666` 1.05em; ul 0.9em `#333`; `strong` `#1a5276`; `.obj-title` 1.05em weight 600 `#1a5276`; `.philosophy` background `#f0f4f8`, left border 4px solid `#2980b9`, padding 12px 16px, 0.9em; table cell borders `1px solid #e0e0e0`, padding 20px 24px. No nav bar, no back/home links.
- **Canvas:** intrinsic `width`/`height` attributes as given per chart (mostly 720×300); shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. A shared `gauss(x, mu, sigma)` helper (exp(−0.5·((x−mu)/σ)²)/(σ·2.507)) draws the density curves. Chart text uses 17px -apple-system font. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276` / `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, yellow-orange `#f39c12`, purple `#8e44ad`, bar fill `rgba(41,128,185,0.3-0.4)`, gray text `#555`/`#333`.
- In regenerated HTML, any card links use `.html` extensions (this page has none).
