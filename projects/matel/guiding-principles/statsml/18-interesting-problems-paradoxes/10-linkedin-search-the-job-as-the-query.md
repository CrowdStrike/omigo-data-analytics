# LinkedIn Search — The Job as the Query

**Page type:** detail page (two-column obj-table layout: text left 50%, canvas right 50%, one h2 + table per aspect, plus philosophy callouts and a summary table)
**HTML title tag:** LinkedIn Search — The Job as the Query

**Subtitle:** On a professional network, the job posting is a query against people and the profile is a query against jobs. The same engine runs in both directions — and that symmetry breaks almost every assumption borrowed from web search.

## Callout (philosophy box, top)

**The question:** In web search, a document has no opinion about being retrieved. What changes when the document can decline?

**The answer:** Everything downstream of the score. A hiring marketplace runs two retrieval systems over each other's corpus: the job ranks people, the person ranks jobs. A result is only useful if it clears *both* ranking functions, so the object being optimized stops being a sorted list and becomes an assignment. And the cheapest signal for predicting whether the other side will say yes — recent activity — quietly smuggles *availability* into a score that claims to measure *relevance*.

## 1. Two Corpora, One Engine — Run the Query Backwards

**Obj-title:** The Mirror Retrieval Problem

Recruiter search takes a job description and retrieves people. Job search takes a profile and retrieves postings. These are not two products that happen to share infrastructure — they are the same inverted index, the same tokenizer, the same learned scorer, with the roles of query and document swapped.

Math-box:

**The same score, read two ways:** a single relevance model `s(job, person)` supports both surfaces.

Recruiter side: fix `job`, rank over the people corpus → `top-k people`
Candidate side: fix `person`, rank over the postings corpus → `top-k jobs`

In classic ad-hoc retrieval the query is ephemeral and the document is durable. Here **both sides are durable, indexed, and updated** — a profile is a standing query that keeps firing after you close the tab.

- **Query and document swap roles:** a posting is a long structured query; a profile is a long structured document — and vice versa
- **Both sides are indexed:** neither side is ephemeral, so the system can precompute matches in both directions offline
- **Both sides drift:** a posting's bar moves as applicants arrive, a profile's skills move as the person works
- **Vocabulary mismatch cuts twice:** the posting's jargon and the profile's jargon must be bridged in the same shared space
- **Length asymmetry is real:** a posting is one role, a profile is a career — so a profile matches many postings by construction
- **Recall targets differ:** a recruiter wants 20 good people out of millions; a candidate wants every job they could plausibly get

### Visualization (canvas `canvas1`, 720×360)

Schematic diagram: two corpora boxes with bidirectional arrows through a shared scorer in the middle.

- **Layout:** left box at (40, 90) size 190×180; right box at (490, 90) size 190×180; center scorer box at (275, 140) size 170×80.
- **Left box:** stroke `#1a5276` width 2, fill `rgba(26,82,118,0.06)`, radius-6 rounded rect. Header bold 13px `#1a5276` centered: "CORPUS: PEOPLE". Body 11px `#666`, four lines starting y=150, 22px spacing: "Alice — 8 yrs, distributed systems", "Bob — 3 yrs, data pipelines", "Carol — 6 yrs, ranking models", "… millions of profiles".
- **Right box:** stroke `#27ae60` width 2, fill `rgba(39,174,96,0.06)`. Header bold 13px `#27ae60` centered: "CORPUS: POSTINGS". Body 11px `#666`, four lines: "Vendor A — Backend Engineer", "Vendor A — Data Platform Lead", "Vendor B — Search Engineer", "… millions of postings".
- **Center box:** stroke `#e67e22` width 2, fill `rgba(230,126,34,0.08)`. Two lines centered: bold 13px `#e67e22` "SHARED SCORER" at y=168, and 12px `#666` monospace-ish "s(job, person)" at y=190.
- **Arrow 1 (posting → people), above the center box:** quadratic arc from (490, 110) through control (360, 50) to (238, 110), stroke `#27ae60` width 2, filled-triangle arrowhead (9px) at the left end. Label bold 12px `#27ae60` centered at (360, 74): "the job is the query".
- **Arrow 2 (profile → postings), below the center box:** quadratic arc from (230, 250) through control (360, 310) to (482, 250), stroke `#1a5276` width 2, arrowhead (9px) at the right end. Label bold 12px `#1a5276` centered at (360, 302): "the profile is the query".
- **Footnote (11px `#999`, centered at (360, 338)):** "Illustrative Example — same index, same scorer, two directions".
- **Title (bold 14px `#1a5276`, top center at y=22):** "One Engine, Two Directions — Each Side Is the Other's Query".

## 2. Both Sides Have Preferences, So One Ranking Function Isn't Enough

**Obj-title:** Top-k Retrieval vs Stable Matching

A web page cannot refuse to be clicked. A candidate can ignore the message, and an employer can pass on the application. So the useful quantity is not "how relevant is this person" but "will a match actually form" — the product of two acceptances. A candidate perfectly relevant to a job that would never hire them, or who would never take it, is a *wrong answer* that scores perfectly.

Math-box:

**Five candidates for one opening (illustrative).** `r` = fit to the job, `q` = probability the candidate engages. Expected match value = `r × q`.

| Person | r | q | r × q |
|---|---|---|---|
| Alice | 0.95 | 0.10 | 0.0950 |
| Bob | 0.80 | 0.55 | 0.4400 |
| Carol | 0.70 | 0.75 | 0.5250 |
| Dan | 0.55 | 0.90 | 0.4950 |
| Erin | 0.35 | 0.95 | 0.3325 |

Rank by `r`: **Alice, Bob, Carol, Dan, Erin.** Rank by `r × q`: **Carol, Dan, Bob, Erin, Alice.**
The best-fitting candidate ranks **last** on expected outcome. Three messages sent by the `r` ranking return `0.095 + 0.440 + 0.525 = 1.060` expected engagements; by `r × q`, `0.525 + 0.495 + 0.440 = 1.460` — a `37.7%` lift with a *less* relevant slate.

**Why the objective changes shape, not just its weights.** Top-k retrieval solves each query independently: the correct output is the list sorted by score, and one query's answer does not consume another's. A hiring market has capacity — the job hires one person, the person takes one job — so filling a slot **removes that person from every other job's feasible set**. The object being optimized is an assignment over both sides, with preference orders on both, and the correctness criterion is *stability*: no job–candidate pair who both prefer each other to their assignment. Deferred acceptance produces such an assignment; independent per-query top-k cannot, because it has no term for the externality.

**Congestion, made concrete (illustrative).** 100 openings, 100 qualified people, each posting ranks independently and messages its top 1. If all 100 agree on the same person and that person answers 5 messages, at most 1 hire forms — a fill rate of `1/100 = 1%`, and 99 qualified people receive zero contacts. Spreading one contact per person can fill up to `100/100 = 100%`. Same relevance model, same candidates, two orders of magnitude apart.

- **Relevance is necessary, not sufficient:** a perfect match that never replies produces exactly zero hires
- **The product breaks transitivity of "better":** the top-`r` person is the worst expected outcome here
- **Capacity creates an externality:** every filled slot shrinks the candidate pool the other queries draw from
- **Stability is the right correctness test:** no pair should mutually prefer each other over what they got
- **Independent ranking causes congestion:** all queries pile onto the same head of the distribution
- **Estimating `q` is a second model:** predicting engagement is a different problem from predicting fit

### Visualization (canvas `canvas2`, 720×360)

Scatter of the five candidates on fit (`r`) vs engagement probability (`q`), with iso-value hyperbolas for `r × q`.

- **Layout:** origin at (75, 285), plot width 560, plot height 235. Axes `#1a5276` width 2.
- **Data (hardcoded literal array — the shape carries the lesson):** `[["Alice",0.95,0.10],["Bob",0.80,0.55],["Carol",0.70,0.75],["Dan",0.55,0.90],["Erin",0.35,0.95]]`. Both axes span 0 to 1.0.
- **Gridlines / ticks:** at 0, 0.25, 0.5, 0.75, 1.0 on both axes, labels 11px `#666`, gridlines `#eee`.
- **Axis labels (13px `#1a5276`):** x: "r — fit to the job"; y (rotated): "q — probability the candidate engages".
- **Iso-value curves:** `q = v / r` for `v` in `[0.2, 0.4, 0.525]`, drawn only where `q ≤ 1`, stroke `#e67e22` width 1.25, dashed 4/4. Label each curve at its right end in 10px `#e67e22`: "r·q = 0.20", "r·q = 0.40", "r·q = 0.53". The 0.525 curve must pass exactly through Carol.
- **Points:** filled circles radius 6. Carol (the `r × q` winner) in green `#27ae60`; Alice (the `r` winner) in red `#e74c3c`; Bob, Dan, Erin in `rgba(26,82,118,0.75)`.
- **Point labels (bold 11px, matching each point's color, offset +10 x, −10 y):** name plus computed product to 3 decimals, e.g. "Carol 0.525" — every printed product computed in JS as `r*q`, never typed.
- **Annotation (bold 12px `#e74c3c`, right-aligned two lines left of Alice, at q ≈ 0.16 and 0.08):** "best fit, worst" / "expected outcome".
- **Annotation (bold 12px `#27ae60`, right-aligned left of Carol at q ≈ 0.80):** "best expected match".
- **Footnote (11px `#999`, at (75, 348), left-aligned):** "Illustrative Example — products computed from the plotted points at render time".
- **Title (bold 14px `#1a5276`, top center at y=20):** "Two Acceptances, Not One — the Best Fit Can Be the Worst Result".

## 3. Activity Leaks Intent — and Contaminates Relevance

**Obj-title:** Availability Masquerading as Relevance

Recency of profile edits, application rate, and reply rate are the strongest cheap predictors of `q`. Add them to the score and reply rate jumps immediately, which is why every such system does it. The cost is that one number now mixes two different things, and the entangled score is dominated by whichever component varies more — which is availability, by an order of magnitude.

Math-box:

**1,000 retrieved profiles bucketed by days since last edit (illustrative).**

| Days since edit | Profiles | Reply rate | Meets the job's bar |
|---|---|---|---|
| 0–7 | 120 | 42% | 62% |
| 8–30 | 210 | 31% | 64% |
| 31–90 | 260 | 19% | 61% |
| 91–180 | 190 | 11% | 63% |
| 181–365 | 140 | 6% | 60% |
| 365+ | 80 | 3% | 62% |
| **All** | **1,000** | **19.7%** | **62.1%** |

Reply rate spans `42 / 3 = 14.0x` across buckets. Qualification spans `0.64 / 0.60 = 1.07x` — essentially flat. So a score that adds activity is largely re-sorting on availability.

**What the boost buys and what it costs.** Message only the two freshest buckets (`120 + 210 = 330` profiles): expected replies `50.4 + 65.1 = 115.5`, a reply rate of `115.5 / 330 = 35.0%` versus `196.6 / 1000 = 19.7%` overall — a `1.78x` lift. Qualified share barely moves: `208.8 / 330 = 63.3%` versus `62.1%`. And the discarded 670 profiles contained `620.7 − 208.8 = 411.9` qualified people.

- **Activity predicts response, not competence:** the qualification curve across recency buckets is flat within noise
- **One score, two meanings:** callers read the number as fit while it mostly encodes availability
- **The passive-candidate inversion:** the strongest engineer who is content in their job sinks below a mediocre active one
- **The lift is real:** reply rate nearly doubles, which is why the shortcut survives every review
- **The cost is invisible:** 411.9 qualified people are dropped, and no dashboard counts candidates never shown
- **Fix by factoring, not weighting:** predict fit and engagement separately, then combine explicitly per use case

### Visualization (canvas `canvas3`, 720×360)

Grouped bar chart: reply rate vs qualified share across six recency buckets.

- **Layout:** origin at (75, 290), plot width 590, plot height 230. Axes `#1a5276` width 2.
- **Data (hardcoded literal array):** `[["0–7",120,42,62],["8–30",210,31,64],["31–90",260,19,61],["91–180",190,11,63],["181–365",140,6,60],["365+",80,3,62]]` as `[label, profiles, replyPct, qualifiedPct]`. Value scale 0 to 70 (%).
- **Gridlines / y labels:** at 0, 10, 20, 30, 40, 50, 60, 70 with "%" suffix, 11px `#666`, gridlines `#eee`.
- **Bars:** two per bucket, each 26px wide, 6px apart, group centered in its slot. Reply-rate bar filled `rgba(231,76,60,0.75)`; qualified-share bar filled `rgba(26,82,118,0.35)` with stroke `#1a5276` width 1.
- **Bar value labels:** bold 10px above each bar, colored `#e74c3c` and `#1a5276`, printed from the array.
- **X labels:** bucket label 11px `#666` at `oy + 15`; profile count 10px `#999` at `oy + 29` as "n=120" etc.
- **Axis labels (13px `#1a5276`):** x: "Days since last profile edit"; y (rotated): "Percent of bucket".
- **Flat reference line:** dashed 5/5 `#1a5276` width 1.5 horizontal at the count-weighted mean qualified share, computed at render as `Σ(nᵢ·qualᵢ)/Σnᵢ` — must evaluate to 62.07. Label bold 11px `#1a5276` at the right end: "weighted mean qualified = 62.1%" with the value computed, not typed.
- **Spread annotation (bold 12px, two lines left-aligned at slot 3.1, at values 66 and 59):** `#e74c3c` "reply rate spread: 14.0x" and `#1a5276` "qualified spread: 1.07x" — both ratios computed at render from the array as `max/min`.
- **Title (bold 14px `#1a5276`, top center at y=20):** "Availability Varies 14x, Qualification Doesn't — Guess Which One the Score Learns".

## 4. Findability Feeds Itself — Where the Ranks Freeze

**Obj-title:** Preferential Attachment with a Threshold

Being findable produces contacts. Contacts produce activity — replies, edits, profile views. Activity raises the activity score, which raises findability. That is a closed positive loop, and because search only shows a top-`k`, the loop has a hard cutoff: profiles below the fold receive no impressions at all, so their score can only decay.

Math-box:

**A deterministic six-profile loop (illustrative).** Activity score `aᵢ` decays 15% per round; each round the top 3 by score receive contact boosts of `+6, +3, +1`; everyone else gets `0`.

`aᵢ(t+1) = 0.85 · aᵢ(t) + boost(rank)`

Start: `[10, 9, 8, 7, 6, 5]` — a 1.11x gap between first and second.

Because the boost depends only on rank, and the initial order never inverts, the recurrence has fixed points `6/0.15 = 40`, `3/0.15 = 20`, `1/0.15 = 6.67`, and `0` for everyone below the fold. The initial `1.11x` gap between the top two becomes a permanent `40/20 = 2.00x`; the gap to the fourth profile becomes unbounded.

**Say the mechanism precisely.** This is a rank-thresholded Matthew effect, and it converges to fixed points — it is *not* a power law. A power law needs unbounded superlinear growth; here the decay term caps every profile. The defect is not a heavy tail, it is **rank freezing**: the initial ordering, whatever produced it, becomes self-certifying and the system stops learning about profiles 4 through 6.

- **The loop is closed, not causal one-way:** findability → contacts → activity → findability, all four arrows live
- **Top-k is the amplifier:** below-the-fold profiles get zero impressions, so their score can only decay
- **Tiny initial gaps become permanent:** 1.11x at start locks in as 2.00x at the fixed point
- **The exploited set is self-certifying:** the system only collects evidence about profiles it already shows
- **No power law claimed:** the decay term caps growth, so this converges rather than fattening a tail
- **Break it with exploration, not reweighting:** randomized impressions below the fold restore the missing data

### Visualization (canvas `canvas4`, 720×360)

Line chart: six profiles' activity scores over 40 rounds of the deterministic recurrence, with fixed-point asymptotes.

- **Layout:** origin at (70, 300), plot width 560, plot height 250. Axes `#1a5276` width 2.
- **Data (computed in JS, no PRNG):** start `[10, 9, 8, 7, 6, 5]`; for each of 40 rounds, sort indices by current score descending, apply `boosts = [6, 3, 1, 0, 0, 0]` by rank, then `a = 0.85 * a + boost`. Store the full trajectory per profile. Value scale 0 to 45.
- **Gridlines / y labels:** at 0, 10, 20, 30, 40, 11px `#666`, gridlines `#eee`. X ticks at rounds 0, 10, 20, 30, 40, 11px `#666`.
- **Axis labels (13px `#1a5276`):** x: "Search-and-contact rounds"; y (rotated): "Activity score (drives findability)".
- **Lines:** profile 1 `#e74c3c` width 2.5; profile 2 `#e67e22` width 2.5; profile 3 `#1a5276` width 2.5; profiles 4–6 `#999` width 1.5.
- **Asymptotes:** dashed 3/3 horizontal lines at the computed final values of profiles 1, 2, 3 in their own colors at 40% alpha.
- **End labels (bold 11px, at the right end of each line, matching color):** the profile name plus its final score to 1 decimal, computed from the trajectory — "P1 → 40.0", "P2 → 20.0", "P3 → 6.7", and a single gray "P4–P6 → 0.0" for the collapsed group.
- **Start annotation (11px `#666`, left of the curves near round 1):** "start: 10, 9, 8, 7, 6, 5".
- **Amplification callout (bold 12px `#e74c3c`, two lines near round 13, high on the plot at values 43 and 39.5):** "P1/P2 gap: 1.11x → 2.00x" and "below-the-fold profiles decay to 0" — the ratios computed at render from round 0 and the final round.
- **Footnote (11px `#999`, at (70, 348), left-aligned):** "Illustrative Example — deterministic recurrence, no random draws".
- **Title (bold 14px `#1a5276`, top center at y=20):** "Fixed Points, Not a Power Law — the Ranking Freezes Itself".

## 5. The Evaluation Trap — Offline Relevance Rewards the Worse Ranker

**Obj-title:** When Two Metrics Point Opposite Ways

Offline evaluation asks a rater: was this candidate qualified for this job? It is cheap, reproducible, and available for every impression. Online evaluation asks: did a hire happen? It is the thing you actually want, it depends on the other side's decision, and it arrives weeks later on a fraction of the traffic. On a two-sided problem these two metrics can rank two candidate systems in *opposite* orders — and the offline one, being cheaper, usually wins the argument.

Math-box:

**Same five candidates, two rankers, two metrics.** Define "qualified" as `r ≥ 0.70`, so Alice, Bob, and Carol qualify.

| Metric | Rank by `r` | Rank by `r × q` |
|---|---|---|
| Top-3 slate | Alice, Bob, Carol | Carol, Dan, Bob |
| Precision@3 (qualified) | `3/3 = 100.0%` | `2/3 = 66.7%` |
| Expected engagements | `1.060` | `1.460` |

The `r` ranker wins the offline metric outright and loses the outcome metric by `37.7%`. Nothing here is a measurement error — both numbers are correct about different things. Precision@3 scores the slate against a one-sided notion of correctness that the market does not use.

**Why the disagreement is structural.** Offline relevance is a property of the pair `(job, person)` and can be labelled in isolation. A hire is a property of the *assignment*, and depends on choices the labeller cannot see: the other applicants, the other offers, the candidate's counterfactual job. No amount of rater agreement fixes this, because the label is measuring a different object.

- **Offline labels a pair, online labels an assignment:** the two objects genuinely differ, so the metrics can disagree
- **Cheap metric, wrong target:** precision@3 is available on every impression and silently defines "good"
- **Outcome data is thin and late:** hires are rare, delayed, and confounded by the other side's alternatives
- **Counterfactuals are unobservable:** you never see whether the passed-over candidate would have accepted
- **Interleaving does not save you:** it measures one side's clicks, and clicks are not the other side's consent
- **Use both, ranked explicitly:** offline as a guardrail against irrelevance, online as the decision metric

### Visualization (canvas `canvas5`, 720×360)

Grouped bar chart: two rankers compared on precision@3 and expected engagements, both computed from the section-2 candidate table.

- **Layout:** origin at (95, 285), plot width 520, plot height 225. Axes `#1a5276` width 2 (left, bottom, and a right spine for the second scale). Two metric groups on the x axis, centers at `ox+150` and `ox+380`.
- **Data (computed in JS from the same literal candidate array as canvas2):** build `rankR` = sorted by `r` desc, `rankRQ` = sorted by `r*q` desc; take the top 3 of each; compute `precision = count(r >= 0.70) / 3` and `expected = Σ(r*q)`.
- **Dual scale:** left axis 0–100 for precision (%), right axis 0–1.6 for expected engagements. Draw both axis tick sets: left at 0, 25, 50, 75, 100 in `#666`; right at 0, 0.4, 0.8, 1.2, 1.6 in `#666`. Left axis title (rotated, 12px `#1a5276`): "Precision@3 (%)"; right axis title (rotated at x≈700, 12px `#1a5276`): "Expected engagements".
- **Bars:** in each metric group, two bars 46px wide, 10px apart. `rank by r` filled `rgba(231,76,60,0.75)`; `rank by r × q` filled `rgba(39,174,96,0.75)`.
- **Group x labels (12px `#1a5276`, bold):** "Offline: Precision@3" and "Online: Expected engagements", centered under each group at `oy + 20`.
- **Bar value labels (bold 11px above each bar, matching color):** precision printed as one decimal percent, expected as three decimals — all four values computed, none typed.
- **Legend (top left inside the plot, 11px):** two 12×12 swatches with labels "rank by r" (`#e74c3c`) and "rank by r × q" (`#27ae60`).
- **Crossover annotation (bold 12px `#e67e22`, one line under the plot title, centered at y=42):** "the offline winner is the online loser".
- **Delta annotation (11px `#666`, right of the online group's green bar):** "+37.7%" computed at render as `(expectedRQ/expectedR - 1) * 100` to one decimal.
- **Footnote (11px `#999`, at (95, 340), left-aligned):** "Illustrative Example — both metrics computed from the same five-candidate table".
- **Title (bold 14px `#1a5276`, top center at y=20):** "Both Metrics Are Correct — They Are Measuring Different Objects".

## 6. The Complete Picture

Summary table (`.summary-table`, header row + 7 rows):

| Property | Ordinary web search | Two-sided hiring search |
|---|---|---|
| **Query** | Ephemeral text typed by a user | A durable, indexed posting — or a durable, indexed profile |
| **Document** | Indifferent to being retrieved | Has preferences, capacity, and the right to decline |
| **Objective** | Top-`k` list, one query at a time | A stable assignment over both sides, with capacity |
| **Correctness test** | Ordered by score, judged per pair | No blocking pair mutually preferring each other |
| **Cheap winning signal** | Clicks, dwell time | Recent activity — which encodes availability, not fit |
| **Positive feedback** | Popular pages get more links | Findability → contacts → activity → findability |
| **Evaluation** | Offline relevance tracks online clicks | Offline relevance and hires can rank systems oppositely |

## Callout (philosophy box, bottom)

**One sentence:** The moment the document can say no, ranking stops being retrieval and becomes matching — and the signal that best predicts "yes" is availability, which is why the most findable person on a professional network is usually the one most actively looking rather than the one most worth finding.

## Regeneration instructions

- **Layout:** detail page. h1 (no index number), `.subtitle`, opening `.philosophy` callout, then per aspect: `<h2>N. Title</h2>` (h2 1.4em `#1a5276`, bottom border `2px solid #2980b9`, padding-bottom 8px) followed by a one-row `.obj-table` — left `<td>` (50%) holds `.obj-title`, paragraph, `.math-box`(es), bullets; right `<td>` (50%, centered) holds the canvas. Section 6 is a `.summary-table`; page closes with a `.philosophy` callout.
- **Page style:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; subtitle `#666` 1.05em; table cell borders `1px solid #e0e0e0`, padding 20px 24px, vertical-align middle; `.obj-title` 1.05em weight 600 `#1a5276`; `strong` `#1a5276`; ul 0.9em `#333`. No nav bar, no back/home links, no cross-page links.
- **Callout style:** `.philosophy` — background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em.
- **Math box:** `.math-box` — background `#f8fafb`, border `1px solid #e0e0e0`, radius 6px, padding 16px 20px, 0.9em; `code` background `#eef2f7`, padding 2px 6px, radius 3px. Inner tables use `.mini-table` (0.88em, th background `#f0f4f8`, 1px `#e0e0e0` borders, 6px 10px padding).
- **Summary table:** `.summary-table` — 0.9em, th background `#f0f4f8` `#1a5276` padding 10px 14px left-aligned, td padding 10px 14px, borders `1px solid #e0e0e0`.
- **Canvas:** intrinsic 720×360 each; a shared `setupCanvas(id, w, h)` sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Data discipline:** no `Math.random()` anywhere. Candidate table, recency buckets, and loop starting scores are hardcoded literal arrays because the counts and shape carry the lesson; every ratio, precision, expected value, weighted mean, and fixed point printed beside a chart is computed in JS from those arrays at render time.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`, gray text `#666`/`#999`, accent `#2980b9`.
