# Churn Spikes at Stock-Grant Cliffs

**Page type:** detail page (h2 section per pitfall, each with a two-column obj-table row: text left 50%, canvas right 50%)
**HTML title tag:** 158. Churn Spikes at Stock-Grant Cliffs

**Subtitle:** Attrition jumps at the 4- and 8-year marks as initial equity grants finish vesting. A retention model blind to compensation structure misreads the spike as culture, management, or burnout — when it's a calendar.

## Callout (philosophy box)

**The fundamental problem:** Employee tenure is a survival process, and the vesting schedule is a covariate that shifts the hazard rate by an order of magnitude at contract-determined months. Leave it out of the model and the residual variation gets attributed to whatever else is measured — manager, team, engagement score. The spike was written into the offer letter years before it appeared in the dashboard.

## The Illustrative Model Used Throughout This Page

**One Hazard Function, One 1,000-Hire Cohort, Ten Years of Follow-Up**

- **Company A hazard by tenure month:** 0.25% for months 1-11, 1.50% at month 12, 0.60% for 13-47.
- **The two spikes:** 7.00% at month 48 and 5.00% at month 96, with 0.70% between and 0.80% after.
- **Company B hazard:** flat 0.7127% every month, no cliff structure, no suppressed early window.
- **Both cohorts:** 1,000 hires at month 0, followed for 120 months, no re-hiring, no censoring.
- **Departures (A):** 593.5 of 1,000 leave in ten years over 6,914.3 person-years of exposure.
- **Departures (B):** 576.1 of 1,000 leave over 6,712.3 person-years — fewer leavers, less exposure.
- **The identity:** 593.5 / 6,914.3 = 8.583%/yr and 576.1 / 6,712.3 = 8.583%/yr — equal to three digits.
- **Illustrative Example:** every figure on this page derives from these two hazard functions, not from measured data.

### Visualization (canvas `c1`, 720×360)

Step plot of the Company A hazard rate by tenure month, months 1 through 120, with the two cliff spikes annotated.

- **Title (bold 17px `#1a5276`, top center):** "Illustrative Example — Hazard Rate by Tenure Month (Company A)".
- **Hazard array (literal, in JS, index = month 1..120):** built from the piecewise rule above — `0.0025` ×11, `0.0150`, `0.0060` ×35, `0.0700`, `0.0070` ×47, `0.0500`, `0.0080` ×24.
- **Plot frame:** x from 60 to 690, y baseline 275, y axis 0% to 7.5% mapped over 215px.
- **Bars:** one bar per month, width derived from the 120-month span in `rgba(26,82,118,0.35)`; month 12 in `#e67e22`, months 48 and 96 in `#e74c3c`.
- **Axis:** x ticks and labels at months 12, 24, 36, 48, 60, 72, 84, 96, 108, 120 in `#555` 11px; y gridlines at 0/2/4/6% dashed `#e0e0e0` with left labels.
- **Annotations (computed at render time from the array):** red text "month 48 → 7.00% = 11.7× the 0.60% baseline" above the month-48 bar, and "month 96 → 5.00% = 7.1× the 0.70% baseline" above month 96, both ratios divided in JS from the plotted values.
- **Suppressed window:** a `rgba(39,174,96,0.12)` shaded rect over months 1-11 labeled in `#27ae60` "months 1-11: 0.25%, pre-cliff floor".
- **Bottom gray text (centered):** "Same contract, three regimes: suppressed, baseline, spike. Nothing about this shape is behavioural."

## Attrition Concentrates at Grant-Exhaustion Tenures, Not Smoothly

**Year 4 Carries 18.1% of a Decade's Departures on 10% of the Timeline**

- **The concentration:** Year 4 loses 107.5 of Company A's 593.5 ten-year departures — 18.1% of the total.
- **Company B for contrast:** its year 4 loses 63.6 of 576.1 departures, or 11.0%, close to the 10% a flat hazard implies.
- **A single month dominates:** month 48 alone accounts for 54.3 departures, 50.5% of the whole year.
- **Against its own neighbours:** the mean month in years 2-4 loses 5.20 people, so month 48 is 10.4× a normal month.
- **Month 96 repeats it:** 25.9 departures against a 4.11 mean for year 7 months — 6.3× the local rate.
- **Why the second spike is smaller:** the cohort is thinner by then, so a 5.00% hazard on 518.9 survivors moves fewer heads.
- **The misreading:** a year-granular report shows "year 4 is our worst year" and sends investigators after year-4 managers.
- **What is actually true:** the hazard is flat inside year 4 except for one contract-determined month.

### Visualization (canvas `c2`, 720×360)

Grouped bar chart of departures per tenure year, Company A versus Company B, with the year-4 excess called out.

- **Title (bold 17px `#1a5276`):** "Departures per Tenure Year — Same Overall Rate, Different Shape".
- **Data (literal arrays, computed from the hazard model):** A = `[41.8, 66.8, 62.1, 107.5, 58.4, 53.6, 49.3, 67.6, 45.3, 41.1]`; B = `[82.2, 75.5, 69.3, 63.6, 58.3, 53.5, 49.1, 45.1, 41.4, 38.0]`.
- **Bars:** paired per year, A in `rgba(26,82,118,0.35)` with `#1a5276` stroke, B in `rgba(230,126,34,0.30)` with `#e67e22` stroke; baseline y=280, scale 110 departures over 210px.
- **Highlights:** the A bars for years 4 and 8 stroked `#e74c3c` 2px.
- **Labels:** year numbers 1-10 beneath each pair in `#555` 11px; legend top-left "Company A (cliff hazard)" `#1a5276` and "Company B (flat hazard)" `#e67e22`.
- **Computed callouts (summed in JS from the plotted arrays, not hardcoded):** above year 4, red "18.1% of A's decade in one year"; above year 8, red "+50% vs B".
- **Bottom text (centered):** bold red "Column totals are equal to three digits: 8.583%/yr each."; gray `#555` "A: 593.5 leavers / 6,914.3 person-years. B: 576.1 / 6,712.3."

## The Aggregate Attrition Rate Carries No Information About the Shape

**8.58%/yr Is Consistent With a Cliff Hazard and a Flat One Alike**

- **Company A:** 593.5 departures across 6,914.3 person-years of exposure gives 8.583% per year.
- **Company B:** 576.1 departures across 6,712.3 person-years gives 8.583% per year — the same to three digits.
- **Different hazard everywhere:** A's monthly rate ranges 0.25% to 7.00%; B's is 0.7127% at every single month.
- **Different survivors:** at month 120, A retains 406.5 of 1,000 and B retains 423.9 — a 17.4-head gap.
- **Near-identical medians:** A's cohort crosses 500 survivors at month 96, B's at month 97.
- **The lesson:** a rate is a ratio of two totals, and totals are invariant to how the numerator is distributed in time.
- **What a headline number cannot tell you:** whether attrition is diffuse, whether it is structural, or where to intervene.
- **The right summary:** report the hazard by tenure bucket, not one company-wide percentage.

### Visualization (canvas `c3`, 720×360)

Two overlaid hazard curves with a reconciliation table underneath showing equal aggregate rates.

- **Title (bold 17px `#1a5276`):** "Two Hazard Shapes, One Aggregate Rate".
- **Curves:** Company A's step hazard in `#1a5276` 2px (same literal array as `c1`); Company B's flat 0.7127% line in `#e67e22` 2px dashed 6/4. Plot frame x 60-500, baseline y=200, 7.5% over 150px.
- **Curve labels:** "A: 0.25% → 7.00%" in `#1a5276` near the month-48 spike, "B: 0.7127% flat" in `#e67e22` at the dashed line.
- **Reconciliation table (drawn as text, x=60, rows y=250/272/294):** header "Company | Leavers | Person-yrs | Rate" in bold `#333`; row A "A | 593.5 | 6,914.3 | 8.583%"; row B "B | 576.1 | 6,712.3 | 8.583%". **Each rate cell is computed in JS as `leavers/personYears` and formatted to 3 decimals** — never written as a literal.
- **Bottom bold red text (centered):** "Neither company's number is wrong. Neither number is informative."

## The One-Year Cliff Suppresses Early Attrition Instead of Raising It

**Months 1-11 Lose 27.2 People Where a Flat Hazard Loses 75.7**

- **Nothing has vested yet:** a standard 4-year grant with a 1-year cliff pays zero equity before month 12.
- **So the hazard falls:** Company A's months 1-11 run at 0.25%, which is 0.42× its own 0.60% baseline.
- **The count:** 27.2 departures in months 1-11 versus 75.7 for flat-hazard Company B — 0.36× as many.
- **Then it releases:** month 12 jumps to 1.50%, giving 14.6 departures against B's 6.6 — 2.2× as many.
- **The subtler half:** a compensation structure can make a hazard artificially LOW, not only artificially high.
- **The false conclusion:** "our onboarding is excellent, first-year retention is 95.8%" credits a payment schedule.
- **The deferred cost:** those people did not stay, they waited — the departures reappear at month 12 and month 48.
- **Diagnostic:** if first-year attrition is lowest in the months just before a cliff, the cliff is doing the work.

### Visualization (canvas `c4`, 720×340)

Bar chart of monthly departures for months 1-24, Company A versus Company B, with the pre-cliff suppression shaded.

- **Title (bold 17px `#1a5276`):** "Months 1-24: The Cliff Suppresses, Then Releases".
- **Data (literal arrays from the model, one value per month 1-24):** A = `[2.5, 2.5, 2.5, 2.5, 2.5, 2.5, 2.5, 2.5, 2.5, 2.4, 2.4, 14.6, 5.7, 5.7, 5.7, 5.6, 5.6, 5.6, 5.5, 5.5, 5.5, 5.4, 5.4, 5.4]`; B = `[7.1, 7.1, 7.0, 7.0, 6.9, 6.9, 6.8, 6.8, 6.7, 6.7, 6.6, 6.6, 6.5, 6.5, 6.4, 6.4, 6.4, 6.3, 6.3, 6.2, 6.2, 6.1, 6.1, 6.0]`.
- **Bars:** paired per month, A `rgba(26,82,118,0.35)`, B `rgba(230,126,34,0.30)`; baseline y=250, scale 16 departures over 180px; month labels every third month in `#555` 11px.
- **Suppression shading:** `rgba(39,174,96,0.12)` rect over months 1-11 with `#27ae60` label "nothing vested — hazard 0.25%".
- **Month-12 marker:** the A bar for month 12 filled `#e67e22`, red label above "cliff vests — 1.50%".
- **Computed sums (summed in JS over the plotted months 1-11 and printed):** `#1a5276` "A months 1-11: 27.2" and `#e67e22` "B months 1-11: 75.7", plus a red ratio line computed as A/B → "0.36×".
- **Bottom gray text (centered):** "First-year retention 95.8% vs 91.8%. The difference is a vesting date, not an onboarding program."

## Exit Interviews Collect Narrative, Not Cause

**91.4% of Month-48 Departures Are Cliff-Driven; 0% of Exit Forms Say So**

- **Timing was fixed years earlier:** the decision variable is a vesting date set in the offer letter.
- **Attributable share:** at month 48 the hazard is 7.00% against a 0.60% baseline, so 1 − 0.60/7.00 = 91.4% is excess.
- **In headcount:** of 54.34 month-48 leavers, 49.68 are attributable to the cliff and 4.66 are baseline turnover.
- **Month 96 is similar:** 1 − 0.70/5.00 = 86.0% of that month's 25.9 departures are excess over baseline.
- **What the form captures:** a reason chosen at exit time — "growth", "culture", "manager" — for a pre-set date.
- **Post-hoc rationalisation:** every leaver has a genuine grievance available, and the timing selects who reports one.
- **The survey has no counterfactual:** it never asks the people who stayed why they stayed through the same month.
- **Consequence:** the "top exit reason" ranking is a ranking of available narratives, not of causes.

### Visualization (canvas `c5`, 720×340)

Stacked attribution bar for month-48 departures beside a bar chart of the exit-survey reasons those same leavers select.

- **Title (bold 17px `#1a5276`):** "Month 48: 54.34 Leavers — Cause vs Stated Reason".
- **Attribution bar (left, x=60, width 260, y=60, height 40):** `#e74c3c` segment sized `260 × (1 − 0.006/0.07)` labeled "cliff excess 91.4% (49.68)"; `rgba(26,82,118,0.35)` remainder labeled "baseline 8.6% (4.66)". **Both the fraction and the headcount are computed in JS** from the hazard pair `0.0700`/`0.0060` and the 54.34 total.
- **Stated-reason bars (right, x=400):** four rows, values `[38, 24, 22, 16]` percent of respondents — "career growth", "compensation", "manager / team", "work-life". Bars `rgba(230,126,34,0.30)` with `#e67e22` stroke, 3px per percent, percent label after each. **The 100% total is summed in JS** and printed beneath the bars.
- **Divider:** vertical `#e0e0e0` line at x=360.
- **Labels:** bold `#1a5276` "What caused the timing" over the left bar, bold `#e67e22` "What the exit form recorded" over the right.
- **Bottom text (centered):** bold red "Vesting appears in 0% of the stated reasons and 91.4% of the cause."; gray `#555` "Same 54.34 people. Two incompatible explanations, one of which is dated."

## Refresh-Grant Policy Is the Actual Treatment Variable

**Covering 70% of the Cliff Cuts the Month-48 Hazard From 7.00% to 2.10%**

- **The real lever:** whether a refresh grant lands before month 48, not whether engagement scores moved.
- **Modelled as coverage:** covering 70% of at-risk employees leaves a 2.10% month-48 hazard instead of 7.00%.
- **Month-48 effect:** departures that month fall from 54.3 to 16.3 — 38.0 people retained by a grant date.
- **Year-4 effect:** the year's departures fall from 107.5 to 69.4, still above the 62.1 of year 3.
- **Five-year headcount:** survivors at month 60 rise from 663.6 to 698.5, a gain of 35.0 of 1,000 hires.
- **Aggregate effect looks small:** the ten-year rate moves 8.58% → 8.07%, a 0.51-point shift that a dashboard buries.
- **Modelling error:** omit refresh coverage and its effect loads onto tenure, manager, or level — all correlated with it.
- **Test the policy, not the mood:** refresh coverage is observable, dated, and assignable; "culture" is none of those.

### Visualization (canvas `c6`, 720×340)

Paired hazard bars around month 48 for the no-refresh and 70%-coverage policies, plus a survival-at-month-60 comparison.

- **Title (bold 17px `#1a5276`):** "Refresh Coverage Is the Treatment — Months 44-52".
- **Hazard bars (left panel, x 60-380):** for months 44-52, no-refresh = `[0.60, 0.60, 0.60, 0.60, 7.00, 0.70, 0.70, 0.70, 0.70]` in `rgba(231,76,60,0.35)` stroked `#e74c3c`; with-refresh = `[0.60, 0.60, 0.60, 0.60, 2.10, 0.70, 0.70, 0.70, 0.70]` in `rgba(39,174,96,0.30)` stroked `#27ae60`. Baseline y=230, 7.5% over 160px, month labels 44/46/48/50/52.
- **Spike annotation (computed in JS as the ratio of the two month-48 values):** red "7.00% → 2.10% = 3.3× reduction".
- **Survival panel (right, x 430-690):** two horizontal bars for survivors at month 60, `663.6` in `rgba(231,76,60,0.35)` and `698.5` in `rgba(39,174,96,0.30)`, scaled 0-1000 over 240px, each labeled with its value and percentage; the difference `698.5 − 663.6` **computed in JS** and printed in `#27ae60` as "+35.0 heads".
- **Panel headings:** bold `#333` "Month-48 hazard" (left), "Survivors at month 60" (right).
- **Bottom text (centered):** bold `#1a5276` "Ten-year rate: 8.58% → 8.07%. Visible in the hazard, nearly invisible in the aggregate."; gray `#555` "The intervention with a date beats the intervention with a slogan."

## Cohort Comparison Breaks When Grant Terms Changed Between Cohorts

**Cohort Y Has the Smaller Spike and the Worse Four-Year Retention**

- **Cohort X:** larger initial grant — 0.25% early hazard, 0.60% baseline, 7.00% at month 48, an 11.7× spike.
- **Cohort Y:** smaller grant hired later — 0.40% early, 0.95% baseline, 3.00% at month 48, a 3.2× spike.
- **Spike comparison says Y is healthier:** Y's month-48 departures are 20.2 against X's 54.3.
- **Retention says the opposite:** survivors at month 48 are 654.6 for Y and 721.9 for X, or 65.5% vs 72.2%.
- **Why both are true:** a weak grant has less to hold back, so it neither suppresses early exit nor concentrates late exit.
- **Year-4 totals mislead too:** Y loses 95.0 in year 4 and X loses 107.5, again favouring the worse cohort.
- **The invalid comparison:** any tenure-matched cohort contrast assumes the grant schedule was held fixed.
- **Minimum fix:** stratify by grant vintage, and treat a terms change as a regime break, not as a covariate.

### Visualization (canvas `c7`, 720×340)

Two-panel comparison: month-48 spike height per cohort beside four-year survival per cohort, arranged so the two panels rank the cohorts oppositely.

- **Title (bold 17px `#1a5276`):** "Cohort X vs Cohort Y — Two Panels, Opposite Rankings".
- **Left panel (x 60-350) "Month-48 hazard vs own baseline":** for X, baseline bar 0.60% and spike bar 7.00%; for Y, baseline 0.95% and spike 3.00%. Baselines `rgba(26,82,118,0.35)`, spikes `#e74c3c`; 7.5% over 150px, baseline y=210. Ratio labels **computed in JS** → "11.7×" over X and "3.2×" over Y.
- **Right panel (x 400-690) "Survivors at month 48 (of 1,000)":** horizontal bars X = 721.9 and Y = 654.6, scaled 0-1000 over 270px, colored `#27ae60` for the higher and `#e74c3c` for the lower, each labeled with count and percent (**percent computed as value/10**).
- **Cross-panel arrows:** thin `#e67e22` text lines under each panel — "spike ranking: Y looks better" (left) and "retention ranking: X is better" (right).
- **Bottom text (centered):** bold red "Comparing spike heights across grant vintages inverts the answer."; gray `#555` "Y's month-48 departures: 20.2. Y's four-year retention: 65.5%. Both worse than they look, and better."

## A Retention Intervention Evaluated Across a Cliff Takes Credit for the Calendar

**Year 3 → Year 4 Reads +73.0%; Year 4 → Year 5 Reads −45.7%; Nothing Changed**

- **Same hazard function throughout:** no program was run, no policy shifted, no manager was replaced.
- **Launch before the cliff:** the year-3 baseline of 62.1 departures becomes 107.5 in year 4, reported as +73.0%.
- **Launch after the cliff:** the year-4 baseline of 107.5 becomes 58.4 in year 5, reported as −45.7%.
- **Same program, both signs:** the measured effect is set entirely by which side of month 48 the window opens.
- **Pre-post is not a design here:** the outcome has a deterministic time structure the comparison ignores.
- **Selection makes it worse:** a program launched *because* year 4 looked bad regresses toward the mean by construction.
- **What a valid evaluation needs:** tenure-matched control, or a discontinuity design anchored on the vesting date.
- **Practical rule:** never let an evaluation window straddle month 48 or month 96 for either arm.

### Visualization (canvas `c8`, 720×340)

Departures-per-year bars for Company A with two candidate evaluation windows bracketed, each annotated with the effect size it would report.

- **Title (bold 17px `#1a5276`):** "Same Data, Two Windows, Opposite Verdicts".
- **Bars:** the Company A yearly array `[41.8, 66.8, 62.1, 107.5, 58.4, 53.6, 49.3, 67.6, 45.3, 41.1]`, `rgba(26,82,118,0.35)` stroked `#1a5276`, baseline y=250, 110 over 175px, year labels 1-10.
- **Window 1 bracket:** `#e74c3c` 2px bracket spanning years 3-4 with label **computed in JS** as `(107.5/62.1 − 1)` → "+73.0% — 'the program backfired'".
- **Window 2 bracket:** `#27ae60` 2px bracket spanning years 4-5 with label **computed in JS** as `(58.4/107.5 − 1)` → "−45.7% — 'the program worked'".
- **Cliff marker:** dashed `#e67e22` vertical line at the year-4 bar with rotated label "month 48".
- **Bottom text (centered):** bold red "Both effect sizes come from one unchanged hazard function."; gray `#555` "A window that straddles a vesting date measures the vesting date."

## Kaplan-Meier Curves: Cliff Steps Versus Smooth Decay

**Both Curves End Near 41-42% Survival; Only One Is Shaped Like a Contract**

- **Company A survival by year:** 95.8, 89.1, 82.9, 72.2, 66.4, 61.0, 56.1, 49.3, 44.8, 40.7 percent.
- **Company B survival by year:** 91.8, 84.2, 77.3, 70.9, 65.1, 59.8, 54.8, 50.3, 46.2, 42.4 percent.
- **The crossings:** B is below A through year 7, then above it from year 8 onward as A's spikes bite.
- **Step versus slope:** A drops 10.7 points across year 4 and 6.8 across year 8; B never drops more than 8.2.
- **Medians nearly coincide:** A reaches 50% survival at month 96, B at month 97 — one month apart.
- **Ends nearly coincide:** 40.7% versus 42.4% at ten years, a 1.7-point gap after two very different journeys.
- **What a log-rank test sees:** two curves that cross, which violates the proportional-hazards premise outright.
- **Correct handling:** model the vesting months as time-varying covariates, not as a single hazard ratio.

### Visualization (canvas `c9`, 720×380)

Two Kaplan-Meier style survival curves over 120 months — Company A with cliff-shaped steps, Company B smooth — with the crossing point and both medians marked.

- **Title (bold 17px `#1a5276`):** "Survival Curves — Steps vs Smooth Decay (1,000 hires)".
- **Curves:** both drawn month-by-month from the hazard arrays already defined, so the plotted survival, the printed year values, and the prose all come from one computation. A in `#1a5276` 2.5px, B in `#e67e22` 2px dashed 6/4.
- **Plot frame:** x 70-655 for months 0-120, y 300 (0%) to 60 (100%); y gridlines every 20% dashed `#e0e0e0` with labels; x ticks every 12 months labeled by year.
- **Cliff steps:** `#e74c3c` short vertical tick marks at the month-48 and month-96 drops, each labeled with the drop in points **computed in JS** from the plotted series.
- **Median line:** horizontal dashed `#27ae60` line at 50% with `#27ae60` labels "A median: month 96" and "B median: month 97" placed at the respective crossings, **both located in JS** by scanning for the first month at or below 500 survivors.
- **Endpoint labels (computed from the series):** `#1a5276` "A: 40.7%" and `#e67e22` "B: 42.4%" at x=659.
- **Crossing annotation:** `#555` text near year 8 "curves cross — proportional hazards fails".
- **Bottom text (centered):** bold `#1a5276` "One aggregate rate. Two shapes. Only the shape tells you when to act."

## Regeneration instructions

- **Layout:** detail page. h1 (no index number) + `.subtitle` + one `.philosophy` callout, then one unnumbered `<h2>` per pitfall followed by a one-row `.obj-table`: left `<td>` (50%) holds an `.obj-title` div — a refined restatement of the pitfall name, never a copy of it — plus a `<ul>` of labeled bullets; right `<td>` (50%, centered) holds the canvas. Even table rows have background `#fafcfe`.
- **Page CSS:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.4em `#1a5276` with 2px `#2980b9` bottom border, padding-bottom 8px; subtitle `#666` 1.05em; ul 0.9em `#333`; `strong` `#1a5276`; `.obj-title` 1.05em weight 600 `#1a5276`; `.philosophy` background `#f0f4f8`, left border 4px solid `#2980b9`, padding 12px 16px, 0.9em; table cell borders `1px solid #e0e0e0`, padding 20px 24px. No nav bar, no back/home links, no cross-reference links, no thead, no status badges.
- **Canvas:** intrinsic `width`/`height` attributes as given per chart; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms). Shrink a chart via canvas max-width only — never by narrowing the viz `<td>` below 50%.
- **Data source of truth:** a single shared `hazardA` literal array (120 entries) and the constant `hazardB = 0.007127` drive every chart. Survival, per-year departures, person-years, ratios, and rates are **computed in JS from those arrays at render time** — no statistic printed beside a plotted series may be a hardcoded literal. The yearly and monthly literal arrays given in the chart specs above are the outputs of that same computation, included so prose and chart can be checked against each other.
- **No `Math.random()` anywhere.** This page has no stochastic data — the shape and the counts carry the lesson, so every series is a literal array or a deterministic function of one. If a future revision ever needs a draw, use an inline seeded LCG (`function lcg(seed){var s=seed;return function(){s=(s*16807)%2147483647;return s/2147483647;};}`) with its own fixed seed per chart.
- **Palette:** primary blue `#1a5276` / `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`, secondary fill `rgba(230,126,34,0.30)`, gray text `#555`/`#333`.
- **Naming:** "Company A" / "Company B", "Cohort X" / "Cohort Y", Alice/Bob for any individual. No invented brand names. Vesting described generically — a 4-year schedule with a 1-year cliff is standard practice and may be stated as such.
- Every constructed figure is labeled "Illustrative Example".
