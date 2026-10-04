# Reasoning Fallacies

Five systematic errors in probabilistic and causal reasoning that corrupt data analysis.

## 1. Gambler's Fallacy

The belief that past independent events change future probabilities. A fair coin has no memory, so nothing about a streak is "owed" back.

- **The setup:** five heads in a row — HHHHH
- **The wrong inference:** "tails is due, P(T) must be higher now"
- **The truth:** P(next H) = 0.50, exactly as before
- **The confusion:** sequence probability vs next-event probability
- **Sequence is rare:** P(HHHHH) = 1/32 before any flip
- **Conditional is not:** P(6th = H | first five H) = 1/2
- **What independence means:** the conditional equals the marginal

**Key point:** Under independence, conditioning on history changes nothing. A streak carries zero information about the next draw — the only thing a long streak should update is your belief that the coin is fair.

*Example: "The model was wrong 5 times in a row, so it's due for a correct prediction." A miscalibrated model stays miscalibrated regardless of streak length.*

## 2. Conjunction Fallacy

Adding conditions can never increase probability — P(A ∩ B) ≤ P(A), always. But a specific profile feels more likely than a general one, because detail buys plausibility rather than probability.

- **The rule:** P(A ∩ B) ≤ min(P(A), P(B)), with no exceptions
- **Why it misfires:** a vivid profile resembles the stereotype it describes
- **Representativeness:** resemblance is judged, probability is not
- **In segmentation:** every added filter multiplies the sample down
- **Filter cascade:** 30,000 → 15,000 → 1,500 → 330 → 231 → 5
- **Each step is plausible:** 50%, 10%, 22%, 70%, 2.2% conditional rates
- **The consequence:** an n=5 cell produces "significant" results from noise

**Key point:** Over-segmentation guarantees tiny, unstable cells, and a tiny cell will always contain some extreme rate. Pre-specify segments before looking, and require a minimum cell size for any claim.

*Illustrative Example: "Male, 45–50, income $80–100k, homeowner, bought in the last 30 days" sounds like a precise, targetable cohort — it is 5 people out of 30,000.*

## 3. McNamara Fallacy

Deciding with only the quantities that were easy to collect, then treating the rest as if it did not exist. Named for the Vietnam-era use of body counts as a proxy for winning.

- **Step 1:** measure whatever is easy to measure
- **Step 2:** disregard what cannot be measured yet
- **Step 3:** presume the unmeasured is unimportant
- **Step 4:** declare the unmeasurable to be nonexistent
- **The historical gap:** the real objective was territorial and political control
- **Easy proxies:** handle time, lines of code, accounts opened, test coverage
- **Hard objectives:** satisfaction, maintainability, trust, reliability

**Key point:** Cheap to collect is not the same as valid as a proxy. When a hard-to-measure objective is dropped from the scorecard, optimization pressure moves entirely onto the easy metric that replaced it.

*Example: Optimizing accuracy on a balanced test set while fairness, latency, and user trust go unmeasured — and therefore unmanaged.*

## 4. Goodhart's Law

When a measure becomes a target, it ceases to be a good measure. Optimization finds the cheapest route to the number, and the cheapest route usually bypasses the goal.

- **The law:** a measure under target pressure stops measuring
- **Mechanism:** the easiest way to move a number is rarely the intended way
- **Gaming is rational:** people are paid on the target, not the goal
- **Divergence:** the metric climbs while the underlying value decays
- **Retail bank case:** "accounts opened" as the KPI → unwanted accounts opened
- **Recommenders:** optimize clicks → clickbait; optimize engagement → outrage
- **The tell:** the metric improves and no downstream outcome improves with it

**Key point:** Any metric under optimization pressure needs a paired guardrail that gaming would visibly damage, plus an occasional holdout that measures the goal directly rather than the proxy.

*Illustrative Example: Click-through rate rises 18% after a headline change while completed reads per session fall 9% — the metric moved, the value did not.*

## 5. Lead Time Bias

Detecting a disease earlier increases measured "survival time from diagnosis" even when the patient dies on exactly the same date. The clock simply started earlier.

- **The illusion:** earlier detection lengthens measured survival, not life
- **Same endpoint:** both patients die at age 70
- **Screen-detected:** diagnosed at 50 → 20-year "survival"
- **Symptom-detected:** diagnosed at 65 → 5-year "survival"
- **The difference:** 15 years of lead time, not 15 years of extra life
- **What actually changed:** the duration of being a diagnosed patient
- **The honest metric:** mortality rate in the screened population

**Key point:** Survival-from-diagnosis is confounded by when the clock starts, so it cannot be compared across groups detected at different stages. Only all-cause mortality over a fixed window is immune.

*Example: Comparing "time from signup to churn" between early-flagged and late-flagged users shows the same illusion — earlier flagging inflates apparent tenure.*
