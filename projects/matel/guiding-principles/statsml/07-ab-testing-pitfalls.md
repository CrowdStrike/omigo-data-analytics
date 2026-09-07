# A/B Testing: Tricks, Pitfalls & Malpractice

**Page type:** grid page (card navigation grid, 3 columns)
**HTML title tag:** A/B Testing: Tricks, Pitfalls & Malpractice

**Subtitle:** The most abused tool in data-driven decision making. Every org "does A/B testing." Almost none do it correctly. The complete catalog of what goes wrong — accidentally and deliberately.

## Callout (philosophy box)

**The single best defense: pre-registration.** Write down your hypothesis, primary metric, sample size, test duration, and decision rule BEFORE the test runs. Publish it where it can't be edited. Then follow it. Everything else is theater with a statistics department.

## Cards

Each card links to a detail page under `ab-testing/`. The card shows a colored uppercase category label, a numbered title, and a one-to-two sentence description.

| # | Category | Title | Link | Description |
|---|----------|-------|------|-------------|
| 1 | STATISTICAL | Peeking / Optional Stopping | [ab-testing/01-peeking.md](ab-testing/01-peeking.md) | Check p-value daily until p<0.05, then stop. False positive rate inflates from 5% to 20-30%. |
| 2 | STATISTICAL | Underpowered Tests (Too Small N) | [ab-testing/02-underpowered.md](ab-testing/02-underpowered.md) | Run with 500 users when you need 50,000. Declare "no effect." You could only detect a 20%+ lift. |
| 3 | STATISTICAL | Multiple Comparisons Without Correction | [ab-testing/03-multiple-testing.md](ab-testing/03-multiple-testing.md) | Test 20 metrics. Report the 1 significant. At α=0.05, you EXPECTED 1 false positive out of 20. |
| 4 | STATISTICAL | No A/A Test (Infrastructure Not Validated) | [ab-testing/04-no-aa-test.md](ab-testing/04-no-aa-test.md) | Never ran A/A to verify the test system works. If A/A shows significance, your entire framework is broken. |
| 5 | STATISTICAL | T-Test on Non-Normal / Non-Independent Data | [ab-testing/05-ttest-assumptions.md](ab-testing/05-ttest-assumptions.md) | Apply t-test blindly to revenue (heavy-tailed), sessions (correlated), ratios (non-normal). Wrong test = wrong answer. |
| 6 | STATISTICAL | Sample Size for Alpha Only (Ignoring Power) | [ab-testing/06-alpha-no-power.md](ab-testing/06-alpha-no-power.md) | Calculate sample for false positive control (α=0.05) but never compute power (1-β). Miss real effects 60% of the time. |
| 7 | STATISTICAL | No Pre-Declared Hypothesis | [ab-testing/07-no-hypothesis.md](ab-testing/07-no-hypothesis.md) | No hypothesis before the test. Afterwards: "We hypothesized X all along!" Post-hoc narrative disguised as prediction. |
| 8 | STATISTICAL | High FPR + Multiple Tests = Guaranteed "Win" | [ab-testing/08-inflated-fpr-multiple.md](ab-testing/08-inflated-fpr-multiple.md) | 20% false positive rate (bad sample) × 5 tests = 67% chance of at least one "significant" result. Guaranteed by math. |
| 9 | DESIGN | Wrong Randomization Unit | [ab-testing/09-wrong-unit.md](ab-testing/09-wrong-unit.md) | Randomize by pageview, analyze by user. 100 users × 50 pageviews = 5000 "observations." Variance wrong by 50×. |
| 10 | DESIGN | Non-Random Assignment as "A/B Test" | [ab-testing/10-selection-bias.md](ab-testing/10-selection-bias.md) | Beta opt-in users get treatment. They're already more engaged. You tested engaged vs average, not feature vs no-feature. |
| 11 | DESIGN | Treatment Contamination — When Test Users Affect Control Users | [ab-testing/11-contamination.md](ab-testing/11-contamination.md) | Treatment users influence control via network effects. Referral test: treatment users refer control users. Control is treated. |
| 12 | DESIGN | Survivorship Bias in Test Population | [ab-testing/12-survivorship.md](ab-testing/12-survivorship.md) | Treatment drives 10% of users away. Remaining 90% look great. You measured the survivors, not the treatment effect. |
| 13 | DESIGN | Novelty / Primacy Bias | [ab-testing/13-novelty-primacy.md](ab-testing/13-novelty-primacy.md) | Anything new gets explored → 2-week spike. You measured curiosity, not long-term preference. True effect is at week 4+. |
| 14 | DESIGN | Ratio Metric Traps | [ab-testing/14-ratio-metrics.md](ab-testing/14-ratio-metrics.md) | ARPU went up! Because you lost cheapest customers. Survivors spend more. Numerator flat, denominator shrank. |
| 15 | DESIGN | Proxy Metric Optimization (Goodhart's Law) | [ab-testing/15-wrong-metric.md](ab-testing/15-wrong-metric.md) | Clicks up 20%! Users are confused and clicking desperately. Downstream: purchase down, satisfaction down, retention down. |
| 16 | DELIBERATE | Asymmetric Infrastructure | [ab-testing/16-asymmetric-infra.md](ab-testing/16-asymmetric-infra.md) | Treatment on new fast servers. Control on degraded legacy. You tested infrastructure, not the feature. |
| 17 | DELIBERATE | Duration Manipulation (Stop When Winning) | [ab-testing/17-duration-manipulation.md](ab-testing/17-duration-manipulation.md) | Stop when treatment wins (day 7). Extend when losing ("need more data"). Asymmetric stopping rule = rigged test. |
| 18 | DELIBERATE | Post-Hoc Segmentation | [ab-testing/18-post-hoc-segments.md](ab-testing/18-post-hoc-segments.md) | Overall: no effect. Check 50 segments. One hits p=0.03. "Works for young male iOS users in California!" That's noise. |
| 19 | DELIBERATE | Test Reset Until You Win | [ab-testing/19-test-restart.md](ab-testing/19-test-restart.md) | Control winning? "Logging bug — restart." Still losing? "Contamination — restart." Treatment wins? "Ship it!" 3 tries = 14% FPR. |
| 20 | DELIBERATE | Hiding Downstream Costs | [ab-testing/20-hidden-costs.md](ab-testing/20-hidden-costs.md) | Conversion +10%! (Refunds +40%, support tickets +25%, NPS -15). A/B test only measured above the waterline. |
| 21 | ORG | HiPPO Override | [ab-testing/21-hippo.md](ab-testing/21-hippo.md) | VP already decided. A/B test is theater to confirm. If it disagrees: "test wasn't set up right." Feature ships regardless. |
| 22 | ORG | No-Loss Framing | [ab-testing/22-no-loss-framing.md](ab-testing/22-no-loss-framing.md) | Win → "Ship it!" Lose → "Valuable learning!" Both outcomes celebrated. The test has zero authority over decisions. |
| 23 | ORG | Twyman's Law Ignored | [ab-testing/23-twyman.md](ab-testing/23-twyman.md) | +40% conversion from a button color? That's a logging bug, not a discovery. Too-good results shipped without checking pipes. |
| 24 | ORG | Interaction Effects (Simultaneous Tests) | [ab-testing/24-interaction-effects.md](ab-testing/24-interaction-effects.md) | 5 tests running. Each claims independent credit. Sum: +15%. Actual combined: +4%. Interactions eat the gains. |
| 25 | ORG | Cargo Cult A/B Testing | [ab-testing/25-cargo-cult.md](ab-testing/25-cargo-cult.md) | "We A/B test everything" = culture signal. No test has ever killed a feature. Decision-reversal rate: <5%. The test is decoration. |
| 26 | CONFOUND | Placebo Effect | [ab-testing/26-placebo-effect.md](ab-testing/26-placebo-effect.md) | The act of receiving any intervention produces measurable change. Two-arm tests (treatment vs nothing) cannot separate belief from mechanism. Requires a three-arm design. |
| 27 | MINDSET | Testing to Justify, Not to Learn | [ab-testing/27-testing-to-justify.md](ab-testing/27-testing-to-justify.md) | The hypothesis is backwards — the test exists to produce evidence for a pre-decided launch. Post-hoc metric selection, window manipulation, and segment mining deployed together as institutional p-hacking. |
| 28 | POWER | Metric Too Far | [ab-testing/28-metric-too-far.md](ab-testing/28-metric-too-far.md) | Measuring a micro-feature against a macro-metric guarantees inconclusive results. A tooltip improvement can't move GMV in any feasible sample size — that's a power problem, not a feature problem. Measure the proximate metric instead. |
| 29 | DESIGN | Attribution Window Misalignment | [ab-testing/29-attribution-window.md](ab-testing/29-attribution-window.md) | Conversions measured too early or cut off too soon — pre-test intent leaks in (warm-up contamination), post-test conversions leak out (maturation truncation). |
| 30 | DESIGN | Wrong Split Level (Sub-Unit Randomization) | [ab-testing/30-wrong-split-level.md](ab-testing/30-wrong-split-level.md) | Randomize below the natural unit of independence and outcomes within a cluster are correlated — independence is violated and the variance is understated. |
| 31 | DESIGN | Consideration Window Exceeds Test Duration | [ab-testing/31-consideration-window.md](ab-testing/31-consideration-window.md) | High-price purchases take weeks of research. A 2-week test measures decisions that started before the test and misses ones that finish after — distortion scales with item price. |
| 32 | DESIGN | User Training Cost | [ab-testing/32-user-training-cost.md](ab-testing/32-user-training-cost.md) | Tenured users must relearn the layout, so the early dip is real efficiency loss, not opinion. Relearning advances per exposure — every segment is on its own clock, and big redesigns may never be testable to equilibrium. |

## Regeneration instructions

To rebuild the HTML from this spec:

- **Template:** nav-grid style (see `docs/statsml/ui-templates/02-nav-grid`). Single page: h1, `.subtitle` paragraph, one `.philosophy` callout, then one `.grid` of `.card` anchors.
- **Layout:** `.grid` is CSS grid, `repeat(3, 1fr)`, 16px gap; responsive: 2 columns below 900px, 1 column below 500px.
- **Links:** the table above links to the `.md` versions for navigation in markdown; in the regenerated HTML, each card's `href` is the same path with an `.html` extension instead.
- **Card structure:** `<a class="card" href="...">` containing `<div class="card-label" style="color:CATEGORY_COLOR">CATEGORY</div>`, `<h3>N. Title</h3>` (unpadded index number matching the file index), `<p>description</p>`.
- **Category label colors:** STATISTICAL and MINDSET `#e74c3c`; DESIGN and POWER `#e67e22`; DELIBERATE `#8e44ad`; ORG and CONFOUND `#555`.
- **Card style:** background `#f8fafb`, border `1px solid #e0e0e0`, radius 8px, padding 16px; hover: shadow `0 4px 12px rgba(0,0,0,0.1)`, border `#2980b9`. Label 0.72em bold uppercase, h3 `#1a5276` 1.0em, description 0.85em `#555`.
- **Callout style:** `.philosophy` — background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em.
- **Page style:** body system sans-serif, white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; subtitle `#666` 1.05em. No nav bar, no back/home links.
