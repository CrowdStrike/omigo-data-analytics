# Job-Seeker Visibility Bias

**Page type:** detail page (h2 section per pitfall, each with a two-column obj-table row: text left 50%, canvas right 50%)
**HTML title tag:** 159. Job-Seeker Visibility Bias

**Subtitle:** Professional-network data over-represents people actively looking for jobs — they polish profiles, connect, and engage more. Any model trained on visible activity learns "who is job hunting," not "who is qualified."

## Callout (philosophy box)

**The fundamental problem:** Profile completeness and platform activity are caused by job-seeking intent, not by qualification. A model trained on visible signals learns intent and reports it as merit. Worse, "visible" is a collider on the intent → visibility ← qualification path, so conditioning on it manufactures a correlation between activity and quality that does not exist in the population.

**Illustrative Example** — every figure on this page is derived from one constructed 10,000-person population in which qualification and job-seeking intent are exactly independent.

## Profile Completeness Is an Intent Signal, Not a Skill Signal

**Completeness Separates Seekers From Passives, Not Strong From Weak**

- **What completeness measures:** Time spent editing a profile, which is what people do when they want to be found.
- **The confound:** A polished summary and a full skills list are the output of a job search, not of competence.
- **The observed split:** Mean completeness sorts by intent, with a wide gap, and barely moves with qualification.
- **What the model learns:** Feature importance ranks completeness first because it predicts the label it was given.
- **Why it survives review:** The feature is genuinely predictive of recruiter response, so offline metrics look strong.
- **The failure on deployment:** It ranks whoever is between roles above whoever is doing the job well right now.
- **The tell:** Completeness correlates with recency of career disruption, not with any performance measure.

### Visualization (canvas `c1`, 720×340)

Grouped bar chart of mean profile completeness across the 2×2 of intent and qualification, with both marginal gaps computed at render time from the plotted bar values.

- **Title (bold 17px `#1a5276`, top center):** "Completeness Tracks Intent, Not Qualification".
- **Data (hardcoded literals, mean completeness %):** Seeking + High qual = 92, Seeking + Low qual = 90, Passive + High qual = 41, Passive + Low qual = 38.
- **Bars:** four vertical bars, baseline y=250, height = value×2.1, width 90, x positions 80, 190, 380, 490. Seeking pair filled `#e67e22`, passive pair filled `rgba(26,82,118,0.35)` with a 1.5px `#1a5276` stroke. Value printed in bold above each bar in its own color; two-line label beneath each bar in `#555` ("Seeking" / "High qual" etc.).
- **Group separator:** 1px dashed `#ccc` vertical line at x=340 from y=60 to y=255.
- **Computed labels (must be derived in JS from the four bar values, never hardcoded):** intent gap = mean(seeking) − mean(passive) = 51.5 points, printed in bold `#e67e22`; qualification gap = mean(high) − mean(low) = 2.5 points, printed in bold `#1a5276`. Both drawn as one line at y=285 in the form "Intent gap: 51.5 pts | Qualification gap: 2.5 pts".
- **Bottom line (gray `#555`, centered, y=310):** "Completeness answers \"is this person looking?\" — the model is asked \"is this person good?\"".

## Conditioning on Visibility Manufactures a Correlation From Nothing

**Population Correlation 0.000 → Observed Correlation −0.382**

- **The construction:** 10,000 people, 30% high-qualification, 20% job-seeking, drawn independently of each other.
- **Independence verified:** The population 2×2 gives an odds ratio of exactly 1.00 and phi of exactly 0.000.
- **The selection step:** Visibility depends on both — seekers polish profiles, and strong passives get found anyway.
- **Visibility rates used:** 90% for seekers of either quality, 50% for passive-strong, 10% for passive-weak.
- **The collider:** Visibility is a common effect of intent and quality, so filtering on it links the two.
- **The observed sample:** 3,560 visible profiles, phi = −0.382, odds ratio 0.200 — a strong association from zero.
- **The sign is arbitrary:** Change the rates to 90/20/30/15 and the same population yields phi = +0.197 instead.
- **The consequence:** Whatever sign the pipeline reports is a property of who gets seen, not of who is good.

### Visualization (canvas `c2`, 720×400)

Two 2×2 contingency tables drawn as grids — population on the left, visible-only on the right — with row totals, column totals, grand total, and a phi coefficient computed at render time from the cell counts.

- **Title (bold 17px `#1a5276`, top center):** "Same People, Two Tables: Selection Creates the Association".
- **Population cells (hardcoded literals):** high-qual & seeking 600, high-qual & passive 2400, low-qual & seeking 1400, low-qual & passive 5600. Row totals 3000 / 7000, column totals 2000 / 8000, grand total 10000.
- **Visible cells (hardcoded literals, = population cell × its visibility rate):** 540, 1200, 1260, 560. Row totals 1740 / 1820, column totals 1800 / 1760, grand total 3560.
- **Grid geometry:** row-header column 84px then three 76px value columns, table origins x=30 (left) and x=390 (right) so the right grid ends at x=702; header row at y=95, two data rows of 34px, totals row below; 1px `#ccc` cell borders; column headers "Seeking" / "Passive" / "Total" and row headers "High qual" / "Low qual" / "Total" in bold `#1a5276`; counts in `#2a2a2a`, totals in `#555`.
- **Table captions (bold 15px, above each grid at y=72):** left "Population (all 10,000)" in `#1a5276`; right "Visible profiles only" in `#e74c3c`.
- **Computed statistic per table (JS, phi = (ad − bc) / sqrt(r1·r2·c1·c2), printed to 3 decimals below its own grid at y=223):** left "phi = 0.000, OR = 1.00" in `#27ae60`; right "phi = −0.382, OR = 0.20" in `#e74c3c`. Both values must be computed from the literal cell arrays in the draw function.
- **Conditional check line (computed, centered, y=290, `#555`):** "P(high | seeking, visible) = 30.0% vs P(high | passive, visible) = 68.2%" — derived from the visible cells, no zero cells, no divide-by-zero.
- **Bottom bold red line (y=326):** "Zero in the population. Strong in the sample. The filter did all of it."
- **Footnote (gray `#555`, 14px, y=356):** computed from a third literal rate array (0.90 / 0.20 / 0.30 / 0.15): "Alternative visibility rates, same population: phi = +0.197 — selection sets the sign."

## The Passive-Candidate Blind Spot

**58% of Qualified People Are Visible; the Frame Loses 1,260 of 3,000**

- **Who is missing:** People doing well in a current role have no reason to maintain a public profile.
- **The frame problem:** The population recruiters most want is the population least represented in the data.
- **Quantified loss:** Of 3,000 high-qualification people, 1,740 are visible and 1,260 never enter the data at all.
- **Composition shift:** Seekers are 20% of the population but 50.6% of visible profiles — a 2.5× over-representation.
- **No reweighting fix:** Post-stratification needs the missing stratum's outcome distribution, which is unobserved.
- **Why the model cannot notice:** Held-out validation is drawn from the same visible pool, so accuracy looks fine.
- **The honest framing:** The dataset supports claims about visible candidates only, not about the labor market.

### Visualization (canvas `c3`, 720×340)

Two stacked composition bars derived from the same hardcoded cell counts as the previous chart, so all totals reconcile.

- **Title (bold 17px `#1a5276`, top center):** "The Frame Excludes Exactly Who You Are Hiring For".
- **Bar 1 — qualified people (label "High-qualification people: 3,000", at x=60, y=80, height 42, total width 600):** visible segment 1740/3000 of the width filled `#27ae60` with centered white label "Visible 1,740 (58.0%)"; invisible segment filled `#e74c3c` with centered white label "Invisible 1,260 (42.0%)". Both percentages computed in JS from the counts.
- **Bar 2 — visible pool composition (label "Visible profiles: 3,560", at x=60, y=180, height 42, total width 600):** seeking segment 1800/3560 filled `#e67e22` labeled "Seeking 1,800 (50.6%)"; passive segment 1760/3560 filled `rgba(26,82,118,0.35)` with `#1a5276` text labeled "Passive 1,760 (49.4%)". Percentages computed in JS.
- **Reference marker:** small dashed `#1a5276` vertical tick under bar 2 at 20% of its width, labeled below in `#1a5276` 15px: "Population share of seekers: 20%".
- **Computed line (bold `#e67e22`, centered, y=278):** "Seekers over-represented 2.5× in the visible pool" — ratio computed as (1800/3560)/0.20.
- **Bottom line (gray `#555`, centered, y=308):** "Reweighting cannot recover a stratum with zero observed outcomes."

## Contact Produces Activity, Which Produces More Contact

**Six Rounds of Feedback: 9.1× Gap Between Identical Candidates**

- **The mechanism:** Being contacted pulls a person back to the platform, and returning raises their activity score.
- **The ranking effect:** Higher activity lifts search rank, which produces more contact in the next round.
- **Identical starting point:** Two equally qualified people begin at the same activity level, one arbitrarily contacted.
- **Compounding gap:** With +40% per round for the contacted person and −10% for the other, six rounds give 9.1×.
- **Nothing changed but attention:** No skill, credential, or outcome differs — only the initial contact event.
- **Why it looks like signal:** Activity now predicts recruiter interest strongly, because it was caused by it.
- **The audit question:** Ask whether the feature could have been influenced by the system's own past decisions.

### Visualization (canvas `c4`, 720×340)

Two-line chart over six rounds, both lines generated from hardcoded start values and multipliers so the printed ratio is verifiable; the final ratio is computed at render time from the last plotted points.

- **Title (bold 17px `#1a5276`, top center):** "Same Qualification, One Contact Event, Six Rounds".
- **Axes:** baseline y=250 from x=70 to x=660; y-axis at x=70 from y=60 to y=250; "Activity score" rotated label on the left in `#555` 14px; round numbers 1-6 beneath the baseline in `#555`.
- **Series A (contacted, `#27ae60`, width 2.5, round dots r=4):** start 10, multiplied by 1.4 each round → 10.00, 14.00, 19.60, 27.44, 38.42, 53.78. Right-end label in `#27ae60`: "Contacted: 53.8".
- **Series B (not contacted, `#e74c3c`, width 2.5, dashed 5/5, dots r=4):** start 10, multiplied by 0.9 each round → 10.00, 9.00, 8.10, 7.29, 6.56, 5.90. Right-end label in `#e74c3c`: "Not contacted: 5.9".
- **Scale:** y value mapped as 250 − v×3.4 so the top point sits near y=67; both series share the scale.
- **Computed annotation (bold `#e67e22`, centered, y=285):** "Round 6 activity ratio: 9.1×" — computed in JS as lastA/lastB from the plotted arrays.
- **Bottom line (gray `#555`, centered, y=312):** "The feature was created by the decision it is now used to predict."

## Response Labels Exist Only for Candidates the Model Already Chose

**12.0% Label Coverage; the Other 8,800 People Have No Outcome**

- **How labels are made:** A response label exists only if a recruiter sent a message, which the ranker decided.
- **Double conditioning:** Labels are conditioned on visibility and then again on the model's own prior output.
- **The numbers:** 10,000 people → 3,560 visible → 1,200 contacted → 384 positive responses observed.
- **Coverage:** 12.0% of the population has any label; 32.0% of labeled candidates responded positively.
- **The unobserved counterfactual:** Whether the 8,800 uncontacted people would have responded is never learned.
- **Self-confirming retraining:** Next-round training data is the previous ranker's choices, so its errors persist.
- **The bandit framing:** Without deliberate exploration, this is a greedy policy evaluated on its own actions.
- **What to log:** Random-exploration contacts give the only labels not selected by the current model.

### Visualization (canvas `c5`, 720×340)

Four-stage horizontal funnel with hardcoded stage counts; every percentage printed is computed at render time from those counts.

- **Title (bold 17px `#1a5276`, top center, y=20 to clear the bracket annotation):** "Labels Are a 12% Slice Chosen by the Model Itself".
- **Stages (hardcoded, bar width proportional to count with 520px = 10,000):** Population 10,000 in `rgba(26,82,118,0.35)` with a 1.5px `#1a5276` stroke; Visible profiles 3,560 in `#1a5276`; Contacted by recruiter 1,200 in `#e67e22`; Positive response 384 in `#27ae60`.
- **Bar geometry:** left-aligned at x=60, bar width proportional to count with 520px = 10,000, height 38, vertical pitch 52, first bar top y=70. Stage name in `#333` above each bar's left edge; count and computed share of the population printed to the right of each bar in the bar's own color, in the form "10,000 — 100.0%".
- **Label-gap bracket:** `#e74c3c` 1.5px bracket spanning the unlabeled remainder of the population bar, annotated in `#e74c3c`: "8,800 people: no label ever" — count computed as 10000 − 1200.
- **Computed line (bold `#e74c3c`, centered, y=290):** "Positive rate among labeled: 32.0% — a fact about the ranker, not the market" (384/1200 computed in JS).
- **Bottom line (gray `#555`, centered, y=314):** "Every retrain fits the previous model's choices more closely."

## Search-Rank Exposure Concentrates Labels on a Handful of Profiles

**Top 25 of 100 Ranked Candidates Absorb 91.8% of Contacts**

- **How exposure works:** Recruiters open the first page, so contact probability decays sharply with rank.
- **The decay used:** Exposure weight proportional to exp(−rank/10) across 100 ranked candidates.
- **The concentration:** The top 25 receive 1,102 of 1,200 contacts; ranks 26-100 share the remaining 98.
- **Per-candidate view:** 44.1 contacts each in the top 25 against 1.3 each in the tail — a 33.7× gap.
- **Rank comes from activity:** Since rank is driven by activity, exposure is allocated by intent, not by fit.
- **Position bias in labels:** A no-response at rank 90 mostly means nobody scrolled, not that the person declined.
- **The correction:** Model exposure explicitly and treat unexposed candidates as missing, not as negatives.

### Visualization (canvas `c6`, 720×340)

Decaying exposure-weight bar chart across 100 ranks, with the cumulative share and contact split computed at render time from the plotted weights.

- **Title (bold 17px `#1a5276`, top center):** "Exposure Decays With Rank — So Do the Labels".
- **Weights (deterministic formula, no PRNG):** w[i] = exp(−(i+1)/10) for i = 0..99, normalized by their sum when computing shares.
- **Bars:** 100 bars from x=60 across 600px, height = (w[i]/w[0])×160 above baseline y=230, top-25 bars filled `#e67e22`, ranks 26-100 filled `rgba(26,82,118,0.35)`.
- **Divider:** 1px dashed `#e74c3c` vertical line at the rank-25 boundary from y=60 to y=235, labeled above in `#e74c3c` 14px: "rank 25".
- **X labels (`#555`, 14px, beneath baseline):** "rank 1" at the left edge, "rank 100" at the right edge.
- **Computed annotations (all derived in JS from the weight array and a hardcoded 1,200 contacts):** in `#e67e22` at y=262 — "Top 25: 91.8% of exposure → 1,102 contacts (44.1 each)"; in `#1a5276` at y=284 — "Ranks 26-100: 8.2% → 98 contacts (1.3 each)". Contact counts must be split so they sum to exactly 1,200.
- **Bottom line (gray `#555`, centered, y=312):** "A non-response from an unseen profile is missing data, not a negative label."

## Endorsements and Self-Reported Skills Carry No Verification

**Endorsements Track Network Size, Not Any Verified Skill Score**

- **Who endorses whom:** Endorsements are reciprocal courtesies exchanged inside a connection graph.
- **What they measure:** The count scales with how many connections a person has and how often they ask.
- **Self-reported fields:** Titles, tenure, and skill lists are unaudited free text with no confirming record.
- **The computed contrast:** Endorsements track network size strongly and verified skill essentially not at all.
- **Grade inflation:** Terms are added because they appear in job postings, so vocabulary tracks the market.
- **The seniority ratchet:** Self-assigned titles drift upward over time with no corresponding role change.
- **Usable alternative:** Only externally checkable facts — certifications, published work — survive verification.

### Visualization (canvas `c7`, 720×360)

Two side-by-side scatter panels over the same 60 seeded points; both correlation coefficients are computed at render time from the plotted coordinates.

- **Generator (seeded, one `lcg(777)` for this chart; draw order fixed):** for each of 60 points — `net = 50 + rnd()*450` (network size), `end = Math.round(net * (0.05 + rnd()*0.05))` (endorsements), `skill = 30 + rnd()*60` (verified skill score). Ranges produced: network 53-494, endorsements 3-45, skill 31-89.
- **Titles:** chart title (bold 17px `#1a5276`, centered, y=22) "Endorsements Measure Your Network, Not Your Skill"; panel captions (bold 15px, y=52) left "Endorsements vs verified skill" in `#e74c3c`, right "Endorsements vs network size" in `#27ae60`.
- **Left panel (plot box x 60-330, y 70-270):** x = endorsements, y = verified skill, dots r=3.5 in `#e74c3c`. Axis labels in `#555` 13px: "endorsements" (below), "verified skill" (rotated, left).
- **Right panel (plot box x 400-670, y 70-270):** x = endorsements, y = network size, dots r=3.5 in `#27ae60`. Axis labels "endorsements" and "network size".
- **Axis scaling:** each panel maps its own min/max of the plotted values with a 5% margin, so no point is drawn off-box.
- **Computed r labels (Pearson r from the plotted arrays, printed to 2 decimals under each panel at y=294):** left "r = 0.02" in bold `#e74c3c`; right "r = 0.91" in bold `#27ae60`.
- **Bottom line (gray `#555`, centered, y=326):** "A count of favors is not a measurement of ability. Illustrative Example, seeded data."

## A Stale Profile Is Indistinguishable From an Absent One

**31.2% of Visible Profiles Untouched for Over a Year**

- **The currency problem:** A profile records the last moment someone was job-hunting, not their present state.
- **Two identical rows:** Someone who left the field and someone thriving in it both show no recent edit.
- **The recency skew:** Edits cluster in the last three months because that is when people are searching.
- **Quantified staleness:** Of 3,560 visible profiles, 1,110 were last updated more than a year ago.
- **Feature drift by row:** Every field is as of its own timestamp, so rows are not measured at the same time.
- **Silent label rot:** A "current employer" field can be years out of date with no flag distinguishing it.
- **Minimum fix:** Carry a last-updated timestamp as a feature and never impute recency from absence.

### Visualization (canvas `c8`, 720×340)

Recency histogram over the 3,560 visible profiles, using hardcoded bucket counts that sum exactly to 3,560; shares are computed at render time.

- **Title (bold 17px `#1a5276`, top center):** "When Was This Profile Last True?".
- **Buckets (hardcoded literals, sum = 3,560):** under 1 month 980, 1-3 months 620, 3-6 months 480, 6-12 months 370, 1-2 years 560, over 2 years 550.
- **Bars:** six bars from x=70 across 590px, gap 12px, baseline y=240, height = count×0.19 (max bar 186px). First four buckets filled `rgba(26,82,118,0.35)` with a 1.5px `#1a5276` stroke; the last two filled `#e74c3c`. Count printed in bold above each bar; bucket label beneath in `#555` 14px.
- **Computed total check (JS, printed in `#555` 14px at the top right, y=44):** "Total 3,560" summed from the bucket array.
- **Computed annotations:** bold `#e74c3c` at y=272 — "Over a year old: 1,110 profiles (31.2%)"; `#1a5276` at y=294 — "Under 3 months: 1,600 (44.9%) — the search window, not the workforce".
- **Bottom line (gray `#555`, centered, y=318):** "Absent edit history and abandoned account produce the same feature vector."

## Regeneration instructions

- **Layout:** detail page. h1 (no index number) + `.subtitle` + one `.philosophy` callout, then one unnumbered `<h2>` per pitfall followed by a one-row `.obj-table`: left `<td>` (50%) holds an `.obj-title` div — a refined restatement of the pitfall name, never a copy of it — plus a `<ul>` of labeled bullets; right `<td>` (50%, centered) holds the canvas. Even table rows have background `#fafcfe`.
- **Page CSS:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.4em `#1a5276` with 2px `#2980b9` bottom border, padding-bottom 8px; subtitle `#666` 1.05em; ul 0.9em `#333`; `strong` `#1a5276`; `.obj-title` 1.05em weight 600 `#1a5276`; `.philosophy` background `#f0f4f8`, left border 4px solid `#2980b9`, padding 12px 16px, 0.9em; table cell borders `1px solid #e0e0e0`, padding 20px 24px. No nav bar, no back/home links, no cross-page links, no status badges, no `thead`.
- **Canvas:** intrinsic `width`/`height` attributes as given per chart; a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms). Chart text uses -apple-system at 17px for headings and 14-15px for annotations.
- **Determinism:** only chart `c7` uses generated data, from an inline seeded Park-Miller LCG `lcg(777)`. No `Math.random()` anywhere. All 2×2 counts, funnel stages, bucket counts, and series values are hardcoded literals so a reader can check the arithmetic by hand.
- **Computed-label rule:** every statistic printed on a canvas — phi, odds ratio, conditional probabilities, percentages, ratios, correlations, totals — is computed in the draw function from the same literal arrays or plotted points that the chart renders. No statistic is hardcoded as text.
- **Reconciliation:** the 2×2 cells (600 / 2400 / 1400 / 5600 and 540 / 1200 / 1260 / 560), the visible total 3,560, the funnel stages, and the recency buckets are one consistent construction; charts 2, 3, 5, 6, and 8 all draw on it and their totals must continue to agree.
- **Palette:** primary blue `#1a5276` / `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`, gray text `#555`/`#333`.
