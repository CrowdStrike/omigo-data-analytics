# Cross-Domain Patterns

**Page type:** other (single-page long doc: TOC box with anchor links, then 12 pattern sections; each section = h2, a dark pattern-header banner, a pattern-body of domain rows, an insight callout, and a canvas below; closing philosophy callout)
**HTML title tag:** Cross-Domain Patterns — Same Pitfall, Different Disguise

**Subtitle:** The same fundamental pitfall wearing different disguises across domains. Recognizing the PATTERN — not just the domain-specific instance — is what makes you dangerous.

## Table of Contents

**Table of Contents** (ordered list of in-page anchor links)

1. The Measurement Creates the Reality (#measurement-creates-reality)
2. You Only See the Survivors (#survivor-invisible)
3. The Metric Gets Gamed the Moment It's Known (#metric-gamed)
4. Absence of Data IS the Signal (#absence-is-signal)
5. Marginal Observation Extrapolated to Whole (#marginal-extrapolated)
6. The Adversary Adapts Faster Than Your Model (#adversary-adapts)
7. The System's Output Becomes Its Input (#feedback-loop)
8. Past and Future Are Mixed (#temporal-contamination)
9. Rare Events That Define the Outcome (#rare-but-fatal)
10. Confidently Serving Wrong Answers (#confident-wrong)
11. Trained on One Population, Deployed on Another (#population-mismatch)
12. The Measurement Resolution Hides the Truth (#resolution-hides)

## The Measurement Creates the Reality

**Pattern header:** You can't measure something without changing it. The act of observing IS an intervention.

| Domain | Example |
|--------|---------|
| E-Commerce Search | Ranking algorithm uses view count as signal → ranked items get more views → view count rises → ranked higher. *The ranking creates the demand it measures.* |
| search engine | Position #1 gets 30% CTR regardless of quality. Click data = ranking bias. *You can't learn "true relevance" from clicks because clicks ARE the ranking.* |
| short-video platform | For You Page shows video to 300 people → 5-second watch rate determines distribution → that rate determines if anyone else EVER sees it. *One noisy measurement gates all future reality for that content.* |
| Employee Churn | Model predicts "flight risk" → manager treats them differently → employee leaves. *The prediction caused the outcome it claimed to predict.* |
| Predictive Policing | Model says "high crime area" → more police → more arrests → "confirms" high crime → more policing. *The prediction creates its own evidence.* |

**Insight:** **The pattern:** Whenever a model's output feeds back into the system that generates its training data, the model validates itself regardless of accuracy. The measurement and the phenomenon become inseparable.

### Visualization (canvas `cv1`, 720×200 as drawn; HTML attribute 720×300)

Circular feedback-loop diagram.

- **Loop:** circle centered at (360, 100), radius 65, stroked `#1a5276` 3px as an arc from angle 0.3 to 5.7 rad, with a filled `#1a5276` arrowhead at the arc end.
- **Stage labels (17px `#1a5276`, around the circle):** "Model Output" (top), "Changes Reality" (right), "New Training Data" (bottom), "Feeds Back" (left).
- **Center label (bold 15px `#e74c3c`, two lines):** "Self-Reinforcing" / "Loop".
- **Nodes:** four 5px orange (`#e67e22`) dots at the top, right, bottom, and left points of the circle.

## You Only See the Survivors

**Pattern header:** The dead/failed/departed are invisible. Your data only represents those who made it through a filter.

| Domain | Example |
|--------|---------|
| Stock Markets | S&P 500 historical return includes only current members. Failed companies removed retroactively. *Index returns are inflated 1-2% annually because losers are erased from history.* |
| Startups | Study "what makes startups succeed" by looking at a home-rental unicorn, a payment processor, a ride-hailing giant. For every one: 100 with identical profile that died. *Success traits = survivorship narrative.* |
| Court Cases | 95% of cases settle — never entering public record. Your "case outcome" database = the 5% weird enough to go to trial. *Litigated cases are systematically unrepresentative.* |
| Real Estate | You only see SOLD properties. Listings that expired (overpriced, defective) are invisible. *Sale prices are biased upward because unsold properties don't generate data.* |
| Hedge Funds | Mutual fund databases only include surviving funds. Closed funds (poor performance) are deleted. *"Average fund return" is the average of survivors, not all attempts.* |
| WWII Planes | Returning planes have bullet holes in fuselage → reinforce fuselage? No! *Planes hit in the ENGINE never came back. You're only seeing the ones that survived.* |

**Insight:** **The pattern:** Any dataset filtered by survival (active customers, listed properties, existing funds, returned aircraft) systematically excludes the failures that would tell you the most. Always ask: "what CAN'T I see because it didn't survive long enough to be recorded?"

### Visualization (canvas `cv2`, 720×200 as drawn; HTML attribute 720×300)

Iceberg diagram: small visible tip above a dashed water line, large hidden mass below.

- **Water:** light blue region `rgba(41,128,185,0.1)` filling below y=75; dashed `#2980b9` 2px water line (dash 6/4) at y=75, labeled in 14px `#2980b9`: "--- water line (selection filter) ---".
- **Tip (above water):** solid green (`#27ae60`) triangle with apex at (320, 20), base (280, 75)–(440, 75), containing bold white "5%".
- **Submerged mass:** solid red (`#e74c3c`) irregular polygon from (280, 75) through (440, 75), (480, 140), (400, 185), (300, 180), (240, 130), containing bold white "95%".
- **Side labels (17px, left-aligned at x=500):** green "Survivors (visible)"; red "Failed / Dead (invisible)".

## The Metric Gets Gamed the Moment It's Known

**Pattern header:** Goodhart's Law everywhere: "When a measure becomes a target, it ceases to be a good measure."

| Domain | Example |
|--------|---------|
| Education | Test scores as school quality metric → teachers teach to the test → scores rise, actual learning doesn't. *The metric improved while the thing it measured degraded.* |
| Call Centers | Metric = calls/hour → agents hang up fast, transfer aggressively, avoid complex issues. *Every metric is gamed the moment agents know about it.* |
| SEO / search engine | Ranking uses backlinks → entire industry manufactures fake backlinks → backlinks no longer signal quality. *Any learnable pattern will be exploited by the SEO industry.* |
| Research | Publications = career advancement → p-hacking, salami slicing, citation rings. *Publication count measures gamesmanship, not scientific contribution.* |
| Crypto | Volume = "exchange is popular" → exchanges wash-trade to inflate volume. *50-95% of reported volume is manufactured to game the metric.* |
| Hospitals | Mortality rate as quality metric → hospitals stop admitting the sickest patients → rate drops, actual quality unchanged. *Gaming by selection, not by improvement.* |

**Insight:** **The pattern:** As soon as you optimize for a metric, the underlying thing it was supposed to represent detaches from it. The metric becomes a target unto itself, gamed by rational actors. Multiple independent metrics that can't ALL be trivially gamed simultaneously are more robust than one.

### Visualization (canvas `cv3`, 720×200 as drawn; HTML attribute 720×300)

Diverging two-line chart: reported metric rises while actual quality declines.

- **Axes:** `#333` 1.5px L-shape from (80, 20) down to (80, 170) and right to (650, 170). X-axis caption (17px `#333`, centered): "Time (after metric is known)". Y labels right-aligned: "High" at top, "Low" at bottom.
- **Reported Metric line:** solid `#e74c3c` 3px, rising linearly from (80, 150) to (630, 30).
- **Actual Quality line:** dashed `#1a5276` 3px (dash 8/4), declining from (80, 100) to (630, 150) with a small sine wobble (amplitude 3).
- **Legend (left-aligned at x=475):** red solid swatch — "Reported Metric"; blue dashed swatch — "Actual Quality".
- **Annotation (bold 14px `#e67e22`, centered at (360, 85)):** "DIVERGENCE = Gaming".

## Absence of Data IS the Signal

**Pattern header:** What's NOT in the dataset carries more information than what IS.

| Domain | Example |
|--------|---------|
| Healthcare | Lab test not ordered = doctor thought patient was fine. *"Missing" IS the diagnosis: healthy.* Imputing with mean destroys this. |
| Cybersecurity | Sophisticated attacker leaves NO trace. The absence of logs from a critical server IS the signal — they deleted them. *Silence where there should be noise = compromise.* |
| Call Center | Customer navigates IVR → gives up → hangs up. Their intent is NEVER recorded. *The frustrated majority is invisible dark matter.* |
| Employee Survey | 40% response rate. The 60% who didn't respond are disproportionately unhappy. *Non-response IS the most important response.* |
| Earthquake | Pre-1970: only large quakes detected. Apparent "increase in earthquakes" = better detection, not more quakes. *Catalog completeness changes over time look like real trends.* |
| intelligence platform / Intel | Adversary uses cash, aliases, encrypted comms. *The absence of digital footprint where one should exist IS the indicator of sophistication.* |

**Insight:** **The pattern:** Every dataset has a "dark matter" problem — things that exist but are invisible. Missing data is NEVER "missing at random" — the reason it's missing IS information. Create "is_missing" features. Profile the absence.

### Visualization (canvas `cv4`, 720×200 as drawn; HTML attribute 720×300)

Vertical bar chart of feature importance with the IS_MISSING bar tallest.

- **Title (17px `#333`, top center):** "Feature Importance (predictive power)".
- **Bars:** labels `["age", "income", "tenure", "IS_MISSING", "region", "clicks"]`, values `[45, 55, 38, 140, 30, 42]`, max scale 150 over 140px height; bar width 70, gap 20, starting x=80, baseline y=170. IS_MISSING bar filled solid `#e74c3c` and stroked `#e74c3c` with a bold label; all other bars filled `rgba(26,82,118,0.35)` and stroked `#1a5276`.
- **Annotation (bold 14px `#e74c3c`, above the IS_MISSING bar):** "#1 Predictor!".

## Marginal Observation Extrapolated to Whole

**Pattern header:** A price/stat set by 1% of participants is applied as if it represents 100%.

| Domain | Example |
|--------|---------|
| Stock Market | 0.5% of shares trade daily → last price × ALL shares = "market cap." *If all shares tried to sell, price would collapse 60-80%. Market cap is a fiction.* |
| Crypto | 0.01% of tokens circulating → someone buys 100 tokens at $1 → "market cap = $1 billion." *$100 transaction creates a billion-dollar "valuation."* |
| Real Estate | Neighbor's house sold for $800k → home-buying platform says YOUR house = $800k. But yours has different lot, condition, layout. *One comparable ≠ your property's value.* |
| Insurance | 3 historical earthquakes on this fault → "average recurrence = 100 years." *n=3 extrapolated as if it represents the true distribution. CI: 50-200 years.* |
| Sports Betting | "This team is 8-2 against the spread at home." n=10 over 5 years. *Not statistically meaningful — narrative masquerading as signal.* |

**Insight:** **The pattern:** Humans love to take a single observation or tiny sample and declare it "the truth" for the whole population. Market cap, Zestimates, recurrence intervals — all marginal observations promoted to universal facts. Always ask: "what n is this based on, and what would happen if EVERYONE tried to realize this value simultaneously?"

### Visualization (canvas `cv5`, 720×200 as drawn; HTML attribute 720×300)

Small-vs-large bar comparison: tiny actual trade extrapolated into a huge implied valuation.

- **Small bar:** solid green `#27ae60` rect at (100, 130), 80×40, stroked `#1a5276`; labels (17px `#333`, centered): "$100" inside, "Actual Traded" below.
- **Large bar:** `rgba(231,76,60,0.3)` rect at (350, 20), 150×150, stroked `#e74c3c` 2px; labels: bold 17px red "$1 BILLION" centered inside, red "Extrapolated \"Value\"" below.
- **Connector:** dashed `#e67e22` 2px line (dash 4/3) from the small bar to the large bar, with orange label "0.01% volume x all shares".
- **Side note (13px `#666`, left-aligned at x=560, two lines):** "n = 100 tokens" / "traded at $1 each".

## The Adversary Adapts Faster Than Your Model

**Pattern header:** In adversarial domains, your defense decays because the attacker STUDIES it and evolves.

| Domain | Example |
|--------|---------|
| Cybersecurity | Model detects attack pattern A → attacker observes block → switches to pattern B within days. *Model halflife: weeks. Adversary adaptation speed: days.* |
| Email Spam | Filter blocks "V1agra" → spammer uses "V¡agra" → Unicode lookalikes → image-only emails → evolving weekly. *Every filter technique has a counter-technique within a month.* |
| Hedge Funds | Strategy earns alpha → others discover it → crowded trade → alpha goes to zero. *The signal dies the moment enough people trade on it.* |
| AI/LLM Security | Guardrail blocks jailbreak v1 → researchers find v2 in days → v3 the next week. *New jailbreak techniques every week. Defense halflife: days.* |
| SEO | search engine uses signal X → SEO industry optimizes X → X becomes meaningless → search engine finds signal Y → repeat forever. *Arms race with no finish line.* |
| Content Moderation | Filter blocks slur → users spell it differently, use emoji codes, context-dependent references. *Bad actors study your model and engineer evasion faster than you can patch.* |

**Insight:** **The pattern:** In adversarial domains (security, finance, content moderation, fraud), your model's published performance DECAYS over time because rational actors actively work to defeat it. Static models die. Continuous retraining + behavioral (not signature) detection is the only sustainable approach.

### Visualization (canvas `cv6`, 720×200 as drawn; HTML attribute 720×300)

Exponential accuracy-decay curve with attacker-version event lines.

- **Axes:** `#333` 1.5px from (70, 15) down to (70, 170) and right to (680, 170). X caption (17px `#333`): "Weeks after deployment". Y label (rotated): "Accuracy".
- **Decay curve:** `#1a5276` 3px, starting at (70, 30) and decaying as `y = 30 + 120·(1 − e^(−x/200))` for x 0–600.
- **Event lines:** dashed red `#e74c3c` 1.5px verticals (dash 3/3) at x-offsets 150, 300, 450, labeled above in 12px red: "Attacker v1", "Attacker v2", "Attacker v3".
- **Floor line:** dashed orange `#e67e22` 1px horizontal (dash 4/4) at y=90, labeled right-aligned in 13px orange: "50% (random)".

## The System's Output Becomes Its Input

**Pattern header:** Model predicts → action taken → action creates new data → model trains on that data → reinforces itself.

| Domain | Example |
|--------|---------|
| Recommendation | Show cat videos → user watches → model confirms "likes cats" → shows more cats → user never sees dogs. *The recommendation creates the preference it claims to measure.* |
| AI Content | LLM generates text → published to web → next LLM trained on that web → generates from generation → model collapse. *AI training on AI output: quality spirals down each generation.* |
| Popularity Bias | Popular item recommended → gets more clicks → becomes more popular → recommended more. *Rich get richer. Long-tail items starve in permanent invisibility.* |
| Credit Scoring | Low score → denied credit → can't build history → score stays low. *The prediction creates the condition that validates it.* |
| News | Article gets clicks → promoted by algorithm → more clicks → becomes "trending" → more promotion. *First-mover advantage: early clicks compound exponentially regardless of quality.* |

**Insight:** **The pattern:** Any system where the output influences future inputs will converge to an echo chamber, monopoly, or collapse. Breaking the loop requires: exploration injection (random exposure), counterfactual evaluation ("what would have happened without the intervention?"), and explicit diversity constraints.

### Visualization (canvas `cv7`, 720×200 as drawn; HTML attribute 720×300)

Inward-tightening spiral converging to a collapse point.

- **Spiral:** `#1a5276` 2.5px, centered at (360, 100), radius shrinking from 80 as `r = 80 − 6t` over t = 0 to 12 rad (stops when r < 5).
- **Collapse point:** solid red `#e74c3c` 8px dot at the center with bold white 10px "X".
- **Labels:** 14px `#1a5276` "Iteration 1 (broad)" at (140, 30) and "Iteration N (narrow)" below center; 15px red at x=540: "Echo Chamber" / "Collapse".
- **Diversity marker:** green `#27ae60` 1.5px vertical line segments at x=100 (from y=40 to 160), labeled below in 12px green: "Diversity".

## Past and Future Are Mixed

**Pattern header:** Information from the future leaks into the past — the most common cause of "too good to be true" results.

| Domain | Example |
|--------|---------|
| Finance | Earnings restated retroactively. The Q1 number in your database NOW is not what was known IN Q1. *Using restated data = training with future knowledge.* |
| Clinical Trials | "Treatment received" as a feature — but treatment was assigned AFTER the diagnosis you're trying to predict. *The feature is a consequence of the label, not a cause.* |
| E-Commerce | "Total support tickets" for churn prediction — but tickets are filed DURING the churn process. *Feature exists only because the outcome already happened.* |
| Agriculture | Economic data revised 3 times (preliminary → final). Model trained on "final" works great. In production, sees "preliminary" (30% different). *Training on polished future data, serving with raw present data.* |
| Court Cases | Outcome coded after appeal (3 years later) → changes from "plaintiff wins" to "defendant wins." Historical analysis depends on WHEN you query. *The past literally changes as appeals resolve.* |

**Insight:** **The pattern:** For every feature, draw a timeline: "when was this information ACTUALLY available?" If the answer is "after the prediction point" → it's leakage. Point-in-time databases, strict temporal splits, and "would I have this at prediction time?" gatekeeping prevent this.

### Visualization (canvas `cv8`, 720×200 as drawn; HTML attribute 720×300)

Timeline with past/future zones and a backward leakage arrow.

- **Timeline:** `#333` 2px horizontal line at y=100 from x=60 to x=660 with a filled arrowhead at the right end.
- **Prediction point:** vertical `#1a5276` 2px line at x=350 (from y=60 to 140), labeled above in 17px `#1a5276`: "Prediction Point".
- **Zones:** left band `rgba(39,174,96,0.1)` (x 60–350, y 70–130) labeled "PAST (valid features)" in 14px green; right band `rgba(231,76,60,0.1)` (x 350–660) labeled "FUTURE (forbidden)" in 14px red.
- **Tick labels (12px `#666`):** "t-3", "t-2", "t-1" on the past side; "t+1", "t+2", "t+3" on the future side.
- **Leakage arrow:** `#e74c3c` 3px quadratic curve from (530, 150) back to (250, 150) with a filled red arrowhead pointing left, labeled below in bold 14px red: "LEAKAGE".

## Rare Events That Define the Outcome

**Pattern header:** The thing that matters most happens 0.01% of the time — and you have almost no examples of it.

| Domain | Example |
|--------|---------|
| Self-Driving | 99.99% of driving is routine. The 0.01% kills people. Need 100M miles for one specific edge case. *The long tail is infinite, and every item in it is potentially fatal.* |
| Insurance | 1-in-100-year flood IS the business risk. But you have <100 years of data. *Can't estimate tail risk from empirical frequency when n < return period.* |
| Cybersecurity | 1 attack per 10M events. Even 99.999% accuracy = thousands of false positives daily. *Base rate makes precision impossible at any useful recall.* |
| Finance (Tail Risk) | "6-sigma events" happen every few years. Normal distribution says once per 500M years. *Fat tails mean your risk model underestimates catastrophe probability by 1000×.* |
| Earthquake | Prediction impossible (chaotic trigger). Recurrence estimated from n=3-5 events. *The most consequential prediction = the one with the least data.* |
| Nuclear Safety | Must estimate probability of events that have happened 0-2 times in history. *No statistical framework handles "estimate the frequency of something you've essentially never observed."* |

**Insight:** **The pattern:** The events that destroy companies, kill people, or collapse systems are the ones with the least training data. Standard ML (optimize average-case) is exactly wrong for these problems. Need: physics-based models, stress testing, scenario planning, and honest "we don't know" instead of confident point estimates from n=3.

### Visualization (canvas `cv9`, 720×200 as drawn; HTML attribute 720×300)

Flat routine band with a tall fatal spike in the tail.

- **Axes:** `#333` 1.5px from (60, 15) down to (60, 170) and right to (680, 170). X caption (17px `#333`): "Event Severity".
- **Routine band:** `rgba(39,174,96,0.2)` rect at (60, 140), 500×30, topped by a jittery `#27ae60` 2px line (random 0–4px vertical noise), labeled inside in 14px green: "99.99% Routine".
- **Fatal spike:** red triangle (fill `rgba(231,76,60,0.3)`, stroke `#e74c3c` 2.5px) with base (580, 170)–(640, 170) and apex (600, 20), labeled in red: "0.01%" / "FATAL".
- **Annotation (13px `#e67e22`, left-aligned near top right):** "Least data, most consequence".

## Confidently Serving Wrong Answers

**Pattern header:** The system gives high-confidence output that is systematically wrong — and nobody notices because it LOOKS right.

| Domain | Example |
|--------|---------|
| Caching | 99.5% hit rate → sounds great. But stale cache serves WRONG data instantly, confidently. *High hit rate + stale data = fast delivery of lies.* |
| GPU Utilization | "95% GPU utilization" → GPU is actually 95% idle waiting for memory. Metric reports busy when work throughput is 30%. *The dashboard says "healthy" while users experience terrible performance.* |
| AI Agents | Agent A hallucinates → B builds on it → C cites B → collective confidence HIGH. *Hallucination compounds through agent chain. Fiction with citations looks like fact.* |
| Data Center | 5-second SNMP poll shows 30% link utilization. Reality: 100% for 200ms bursts causing drops. *The monitoring says "healthy" because the problem exists BETWEEN measurements.* |
| Earnings | "Adjusted non-GAAP earnings: +$50M." GAAP reality: -$200M. *Confidently reported number that excludes all the bad stuff. Looks professional, is misleading.* |

**Insight:** **The pattern:** The most dangerous system failure is not a crash — it's a system that looks healthy while serving wrong answers. Staleness, aggregation, metric mismatch, and hallucination all produce confident outputs that are systematically wrong. The confidence itself prevents investigation. Always have a "ground truth check" independent of the system being evaluated.

### Visualization (canvas `cv10`, 720×200 as drawn; HTML attribute 720×300)

Split panel: a "HEALTHY" gauge on the left vs a failing reality line chart on the right.

- **Gauge:** centered at (180, 120), radius 55, 12px-wide arc — green `#27ae60` from π to 1.8π, light gray `#e0e0e0` for the remainder; `#333` 2.5px needle at angle π + 0.7π; bold 16px green label below: "HEALTHY"; caption above (13px `#666`): "Dashboard says:".
- **Right panel:** `#333` 1px axes from (380, 30) down to (380, 170) and right to (680, 170); caption above (13px `#666`): "Reality:".
- **Reality line:** `#e74c3c` 2.5px declining from (385, 50) as `y = 50 + (x/290)·100 + sin(x/20)·8` over 290px, labeled below in bold 14px red: "Actual: FAILING".
- **Separator:** bold 20px `#e67e22` "vs" between the panels at (310, 105).

## Trained on One Population, Deployed on Another

**Pattern header:** The population you learned from ≠ the population you serve. Everything breaks at the boundary.

| Domain | Example |
|--------|---------|
| Healthcare | Reference ranges derived from Caucasian males applied to everyone. Hemoglobin 11 = anemia for young male, normal for elderly female. *One threshold, systematically wrong for sub-populations.* |
| Self-Driving | Trained in California sun + grid roads. Deployed in Boston snow + narrow streets. Or US driving behavior deployed in India. *Completely different world, same model.* |
| Skin Cancer | Detection trained on 90% fair-skin images. Deployed globally: fails on darker skin. *Performance gap = representation gap in training data.* |
| Genomics | GWAS on European populations → polygenic risk scores. Applied to African populations: near-useless. *Genetic architecture differs by ancestry. Can't transfer.* |
| NLP / Translation | 80% of training = English/Chinese/European. "Universal" translation works for 20 languages, fails for 6980. *Low-resource languages have near-zero data.* |

**Insight:** **The pattern:** Every model has implicit population assumptions. "Works great" = works great on the training population. Deployment on a different population (demographics, geography, time period, culture) will ALWAYS degrade. The gap between populations = the gap in performance. Audit per-subgroup.

### Visualization (canvas `cv11`, 720×200 as drawn; HTML attribute 720×300)

Two Gaussian distributions with a small highlighted overlap region.

- **Baseline:** `#333` 1px horizontal line at y=165 from x=50 to x=670.
- **Training distribution:** Gaussian centered at x=220 (σ=60, peak height 130), fill `rgba(26,82,118,0.25)`, stroke `#1a5276` 2px; label (15px `#1a5276`, top): "Training Population".
- **Deployment distribution:** Gaussian centered at x=500 (σ=55, peak height 120), fill `rgba(231,76,60,0.25)`, stroke `#e74c3c` 2px; label (15px red, top): "Deployment Population".
- **Overlap:** orange `rgba(230,126,34,0.3)` rect at (330, 100), 70×65, labeled above in 12px `#e67e22`: "Overlap".
- **Caption (bold 13px `#e67e22`, bottom center):** "Minimal overlap = degraded performance".

## The Measurement Resolution Hides the Truth

**Pattern header:** Aggregation, averaging, and low-resolution sampling make problems invisible until they explode.

| Domain | Example |
|--------|---------|
| Electricity Grid | 15-min meter interval: average 1.8kW. Reality: 0.5kW for 14 min + 20kW spike for 1 min (blows transformer). *The spike that caused the failure is invisible in the interval data.* |
| Network | Microbursts: 100% for 200ms, idle for 800ms. 5-sec poll shows 20%. *Packet drops happen BETWEEN measurements.* |
| CPU | Average 50% CPU. Actually: bimodal (0% and 100% alternating). The average is where NO data exists. *Summary statistic represents a state the system never occupies.* |
| Auto-Scaling | Traffic spike lasts 30 seconds. Auto-scaler detects + provisions: 3-5 minutes. *By the time you scale, the event is over. Resolution of response > resolution of problem.* |
| Sports Tracking | GPS ±1-2m. At 30km/h: player could be in 3 different tactical zones. *Error radius exceeds the granularity needed for tactical analysis.* |

**Insight:** **The pattern:** Every measurement system has a resolution limit. Below that limit, reality exists but is invisible. Problems that operate at finer granularity than your measurement will NEVER appear in your data — until they cause a failure that IS visible (crash, outage, injury). Multi-resolution analysis and meta-distribution Gini (doc 28) detect when aggregation is hiding structure.

### Visualization (canvas `cv12`, 720×200 as drawn; HTML attribute 720×300)

Overlay of a spiky high-resolution signal and its smooth low-resolution average.

- **Axes:** `#333` 1px from (60, 10) down to (60, 170) and right to (680, 170).
- **True signal:** `#e74c3c` 1px line over 600 x-steps: baseline `80 + sin(i/80)·20`, a −60px spike for 10 steps every 120 steps, plus ±5px random noise.
- **Measured line:** `#1a5276` 3px line of 60-sample window averages of the same signal (points every 60 steps, plotted at window centers) — the spikes vanish in the averages.
- **Legend (13px, top right):** red swatch — "Reality (1ms)"; blue swatch — "Measured (5s poll)".
- **Annotation (bold 12px `#e67e22`, top center):** "Spikes invisible at low resolution".

## Closing callout (philosophy box)

**The patterns distill every domain into actionable principles:** If you understand these patterns, you'll recognize novel pitfalls in domains you've never worked in — because the same structural failure repeats everywhere. The domain-specific details change; the mathematical structure doesn't. Build pipelines that defend against these patterns and you're protected against most of what reality throws at you.

## Regeneration instructions

- **Layout:** single long page: h1, `.subtitle`, a `.toc` box (background `#f8fafb`, border `1px solid #e0e0e0`, padding 20px 30px, radius 4px, bold "Table of Contents" heading + `<ol>` of anchor links in `#2980b9`, 0.92em); then 12 sections. Each section: `<h2 id="anchor">` (1.4em `#1a5276`, 2px solid `#2980b9` bottom border), a `.pattern-header` banner (background `#1a5276`, white bold text, padding 12px 20px, radius 8px 8px 0 0, 1.05em), a `.pattern-body` (border `1px solid #e0e0e0`, no top border, radius 0 0 8px 8px) of stacked `.domain-row` divs (CSS grid `140px 1fr`, bottom border `#f0f0f0`; `.domain-name` cell background `#f8fafb`, weight 600, `#1a5276`, 0.88em; `.domain-example` 0.9em `#444` with `<em>` rendered as `#c0392b` weight 600 non-italic), ending with an `.insight` callout (background `#fef9e7`, left border `3px solid #e67e22`, padding 10px 14px, 0.88em `#555`); then a standalone `<canvas>` (`width="720" height="300"` attributes, inline style `display:block; margin:10px auto 30px;`). A closing `.philosophy` callout (background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em) ends the page. Anchor links are in-page only — no cross-page links, no nav.
- **Table structure in this spec:** each section's domain rows are rendered above as a two-column Domain / Example markdown table; the `<em>` phrases are the italicized sentences.
- **Page style:** body system sans-serif, white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; subtitle `#666` 1.05em; p 0.95em `#333`; `strong` `#1a5276`.
- **Canvas:** the drawing script resizes each canvas to 720×200 CSS pixels (overriding the 300px height attribute) and scales by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates); default font 17px system sans-serif. When regenerating, use a single script block per canvas (the source HTML contains two script blocks that both draw all 12 canvases; the second block's drawings, specced above, are what displays). Canvases cv9 and cv12 use unseeded `Math.random()` jitter/noise.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, secondary blue `#2980b9`, crimson em text `#c0392b`, bar fill `rgba(26,82,118,0.35)`, gray text `#666`/`#555`/`#333`.
