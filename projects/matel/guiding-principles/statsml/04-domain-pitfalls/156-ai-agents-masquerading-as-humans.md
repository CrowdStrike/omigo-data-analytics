# AI Agents Masquerading as Humans

**Page type:** detail page (h2 section per pitfall, each with a two-column obj-table row: text left 50%, canvas right 50%)
**HTML title tag:** 156. AI Agents Masquerading as Humans

**Subtitle:** Without robust identification, AI agents pass human heuristics and pollute user-generated content and tracking data simultaneously — every metric becomes a weighted average of two populations in unknown proportion.

## Callout (philosophy box)

**The fundamental problem:** Your dataset is now a mixture of humans and agents with no label column. Every statistic you report is `p × (agent value) + (1 − p) × (human value)`, and `p` is unknown. You cannot subtract a population you cannot identify, and estimating `p` requires assuming the agent distribution you were trying to detect.

All figures on this page are constructed to be arithmetically self-consistent and are marked **Illustrative Example**; they are not measurements of any real platform.

## Behavioural Heuristics No Longer Separate the Two Populations

**Fixed 5% False-Positive Budget: Detection Falls 89% → 25%**

- **What used to work:** Keystroke cadence, pointer jitter, and dwell rhythm sat in disjoint ranges per population.
- **Older scripted traffic:** Inter-key intervals clustered at 0-120 ms, well below the human mode at 240-280 ms.
- **Current agent traffic:** The same interval histogram now peaks at 200-240 ms, inside the human body.
- **Overlap grew:** Distribution overlap rises from 16.0% to 67.0% of probability mass between the two eras.
- **At a fixed 5% FPR:** The "flag if interval < 160 ms" rule caught 89.0% of old agents and catches 25.0% now.
- **Ceiling, not tuning:** Overlap 67% caps any single-feature classifier at 66.5% balanced accuracy.
- **Why retuning fails:** The best threshold moves to 240 ms, but that flags 37.0% of real humans to reach 70.0% recall.

### Visualization (canvas `c1`, 720×350)

Two stacked panels of overlaid bin histograms — human vs agent keystroke-interval distributions, past era on top, current era below. Hardcoded literal arrays (the bin shape carries the lesson); overlap statistic computed at render time.

- **Title (bold 17px `#1a5276`, top center):** "Keystroke Interval Histograms — Overlap Then vs Now (Illustrative Example)".
- **Bins:** 10 bins of 40 ms spanning 0-400 ms. Each series has n = 1,000 sessions.
  - `human = [0, 0, 10, 40, 120, 200, 260, 220, 110, 40]` (sums to 1000)
  - `agentPast = [180, 300, 260, 150, 70, 30, 10, 0, 0, 0]` (sums to 1000)
  - `agentNow = [10, 30, 70, 140, 210, 240, 180, 90, 25, 5]` (sums to 1000)
- **Panel A (baseline y = 145, x from 50, bar slot width `(w−320)/10` to leave the right side free for the stat block):** human bars in `rgba(26,82,118,0.35)`, `agentPast` bars outlined in `#e74c3c` (2px stroke, no fill) drawn over them. Bar height = count × 0.28, axis rule in `#999` with "0 ms" / "400 ms" end labels in `#555`.
- **Panel B (baseline y = 270, same geometry):** human bars in `rgba(26,82,118,0.35)`, `agentNow` outlined in `#e74c3c`.
- **Threshold marker:** dashed 5/4 `#e67e22` vertical line at the bin-3 boundary (160 ms) in both panels, labeled once in orange: "flag if < 160 ms".
- **Computed stat block (right-aligned at x = 700, first line y = 52 for panel A and y = 177 for panel B; overlap computed in JS as `Σ min(human_i, agent_i) / 1000`):** era name in bold `#1a5276` ("Past era" / "Current era"), then in `#e74c3c` "overlap 16.0%" / "overlap 67.0%", then "recall 89.0% @ 5.0% FPR" / "recall 25.0% @ 5.0% FPR". Recall and FPR are also computed from the arrays as `Σ agent[0..3]/1000` and `Σ human[0..3]/1000`.
- **Bottom bold red text (centered, y = 330):** "Same rule, same false-positive budget — the signal it relied on is gone."

## Your Dataset Is a Two-Population Mixture With No Label Column

**Observed Mean 4.00 Belongs to Neither Population**

- **The identity:** For any statistic, observed mean = `p·μ_agent + (1 − p)·μ_human`, with `p` the agent share.
- **Worked case:** Humans rate 3.80, agents rate 4.60, agent share 25% → observed 0.25(4.60) + 0.75(3.80) = 4.00.
- **Nobody's number:** 4.00 is not the human mean and not the agent mean; it is an artifact of the mix.
- **Spread lies too:** Mixture variance is `p·σ²_a + (1−p)·σ²_h + p(1−p)(μ_a − μ_h)²`, not a simple average.
- **Numerically:** 0.25(0.36) + 0.75(1.44) + 0.1875(0.64) = 1.290 → observed SD 1.136 vs human SD 1.200.
- **Counter-intuitive:** The observed spread is *narrower* than the human spread because agents are more uniform.
- **Consequence:** Confidence intervals are centered on the mixture, so more data narrows them around the wrong value.

### Visualization (canvas `c2`, 720×340)

Two component density curves (human, agent) plus their weighted mixture, with three computed vertical mean markers.

- **Title (bold 17px `#1a5276`):** "Mixture of Two Populations (Illustrative Example)".
- **Axis:** star rating 1.0-5.0 mapped to x = 70 → 660, ticks at 1, 2, 3, 4, 5, baseline y = 250, axis stroke `#999`.
- **Human component:** Gaussian μ = 3.80, σ = 1.20, stroke `#1a5276` 2px, scaled ×(1 − p) = 0.75.
- **Agent component:** Gaussian μ = 4.60, σ = 0.60, stroke `#e74c3c` 2px, scaled ×p = 0.25.
- **Mixture:** pointwise sum of the two scaled curves, stroke `#e67e22` 3px.
- **Mean markers (x positions and printed values computed in JS from `mh`, `ma`, `p`):** dashed `#1a5276` line at 3.80 labeled "μ human = 3.80"; dashed `#e74c3c` at 4.60 labeled "μ agent = 4.60"; solid `#e67e22` 2.5px at the computed `p·ma + (1−p)·mh` labeled "observed = 4.00".
- **Legend (top left, 17px):** blue "Human 75%", red "Agent 25%", orange "What you measure".
- **Bottom text (centered):** bold red: "You report 4.00. No population in your data has that mean."; gray `#555`: "Observed SD 1.136 is narrower than the human SD 1.200." Both values printed from the computed `obs` and `√varMix`.

## The Mixing Proportion Is Not Identifiable

**Three Different (p, μ_agent) Pairs Give the Identical Observed Mean**

- **The trap:** One observed number, two unknowns — the mixture equation has infinitely many solutions.
- **Same answer, three worlds:** With humans at 3.80, all three of these produce exactly 4.00.
- **20% agents at 4.80:** 0.20(4.80) + 0.80(3.80) = 0.960 + 3.040 = 4.000.
- **25% agents at 4.60:** 0.25(4.60) + 0.75(3.80) = 1.150 + 2.850 = 4.000.
- **40% agents at 4.30:** 0.40(4.30) + 0.60(3.80) = 1.720 + 2.280 = 4.000.
- **No sample size helps:** More rows shrink the error bar on 4.00; they do not split it into its two parts.
- **Backwards too:** Fix agents at 4.60 and the implied human mean swings 3.933 (p = 10%) to 3.400 (p = 50%).
- **The unverifiable assumption:** `p` is estimable only if you already know the agent distribution — the thing you lack.

### Visualization (canvas `c3`, 720×360)

Three stacked composition bars, one per candidate world, each split into a human and an agent segment sized by `p`, with a shared computed observed-mean line proving all three coincide.

- **Title (bold 17px `#1a5276`):** "Non-Identifiability: Three Worlds, One Observation (Illustrative Example)".
- **Data array (JS literal, `mh = 3.80` shared):** `[{p:0.20, ma:4.80}, {p:0.25, ma:4.60}, {p:0.40, ma:4.30}]`.
- **Bars (x from 70, total width 520, height 34, tops at y = 70, 130, 190):** human segment `rgba(26,82,118,0.35)` of width `520·(1−p)` labeled inside in `#1a5276` "human `(1−p)`% @ 3.80"; agent segment `#e74c3c` of width `520·p` labeled above the segment in `#e74c3c` "agent `p`% @ `ma`". All percentages and means printed from the array, never hardcoded in the label string.
- **Row result (right of each bar, x = 612, left-aligned, bold `#e67e22`):** the value of `p·ma + (1−p)·mh` computed in JS and printed to 2 decimals as "→ 4.00" — identical on all three rows.
- **Shared observed line:** dashed 6/4 `#e67e22` 2.5px vertical line at x = 600 spanning y = 58 → 235, labeled above right-aligned in bold orange: "observed mean 4.00" (value computed, not written).
- **Bottom text (centered):** bold red: "The data cannot tell these three worlds apart. Ever."; gray `#555`: "Fixing agents at 4.60, the implied human mean runs 3.933 (p=10%) down to 3.400 (p=50%)." (both endpoints computed as `(4.00 − p·4.60)/(1 − p)`) and "Choosing p is choosing an assumption, not estimating a parameter."

## Review and Rating Corpora Carry Synthetic Text With No Ground-Truth Flag

**Aggregate Shift Obvious at 19.0% TVD; Per-Review Accuracy Caps at 59.5%**

- **Two facts coexist:** The corpus distribution has visibly moved, yet no single review can be adjudicated.
- **Stylometric drift:** Sentence-length-variance scores shift toward the middle of the range across eras.
- **Measured shift:** Total variation distance between the old and current corpora is 19.0% of the mass.
- **Where it went:** 180 of 1,000 reviews pile into score bins 3-4 that the older corpus barely used.
- **The per-item bound:** With equal priors, best possible accuracy from this feature is `0.5 + TVD/2 = 59.5%`.
- **Barely better than a coin:** A 59.5% ceiling means labeling individual reviews is not a viable operation.
- **No backfill:** Historical reviews were never flagged, so there is no clean period to calibrate against.
- **Downstream:** Every rating average, aspect model, and helpfulness ranking inherits the unlabeled mixture.

### Visualization (canvas `c4`, 720×340)

Paired vertical bar chart of the stylometric score distribution, old corpus vs current corpus, with TVD and the derived accuracy ceiling computed at render time.

- **Title (bold 17px `#1a5276`):** "Review Corpus Stylometric Drift (Illustrative Example)".
- **Bins:** 10 score bins 0-9, n = 1,000 reviews per corpus.
  - `oldCorpus = [140, 210, 190, 140, 100, 80, 60, 40, 25, 15]` (sums to 1000)
  - `curCorpus = [90, 150, 180, 230, 190, 90, 40, 20, 7, 3]` (sums to 1000)
- **Bars (baseline y = 240, slot width `(w−140)/10`, height = count × 0.72):** old corpus in `rgba(26,82,118,0.35)` on the left half of each slot, current corpus in `#e67e22` on the right half. Bin index printed beneath each slot in `#555`.
- **Excess highlight:** 2px `#e74c3c` stroke rectangle enclosing the bins 3-4 slot pair, labeled above in red with the computed excess `(cur[3]+cur[4]) − (old[3]+old[4])` = "+180 reviews".
- **Legend (top left, 17px):** `#1a5276` "old corpus", `#e67e22` "current corpus".
- **Computed stat box (top right, right-aligned, bold):** `#e67e22` "TVD = 19.0%" and `#e74c3c` "best per-review accuracy = 59.5%", both computed in JS as `0.5·Σ|old_i − cur_i|/1000` and `0.5 + TVD/2`.
- **Bottom text (centered, gray `#555`):** "The corpus has clearly moved. No individual review can be assigned to a population." and "A 59.5% ceiling is not a labeling pipeline — it is a coin flip with a lean." (the ceiling value is printed from the computation).

## Engagement Metrics Count Agent Sessions as Users

**Reported +50.0% Growth; Human Series Grows 5.0% and Peaks in Quarter 5**

- **The mechanism:** Session, MAU, and DAU counters increment on activity, not on personhood.
- **Reported series:** Quarterly actives rise 100 → 150 (thousands), a headline gain of +50.0%.
- **Agent share rises with it:** 0% → 30% of sessions across the same eight quarters.
- **Human series:** Multiply through and human actives run 100.0 → 105.0, a gain of just 5.0%.
- **Hidden inflection:** The human series peaks at 111.8 in quarter 5 and then declines.
- **Peak-to-latest:** From that 111.8 peak to 105.0 is a 6.1% contraction, invisible in the reported line.
- **Compounding error:** Retention, funnels, and revenue-per-active all use the inflated denominator.
- **The catch:** Reconstructing the human line requires the agent share — which is not identifiable.

### Visualization (canvas `c5`, 720×360)

Dual line chart: reported actives vs the reconstructed human series, with a shaded agent-attributed wedge between them and a marker at the computed human peak.

- **Title (bold 17px `#1a5276`):** "Reported Growth vs Human Growth (Illustrative Example)".
- **Data (JS literals, 8 quarters):** `reported = [100, 108, 115, 122, 130, 138, 144, 150]` (thousands); `agentShare = [0, 2, 5, 9, 14, 19, 25, 30]` (percent).
- **Derived series (computed in JS, never hardcoded):** `human_i = reported_i × (1 − agentShare_i/100)` → 100.0, 105.84, 109.25, 111.02, 111.80, 111.78, 108.00, 105.00.
- **Axes:** x = quarters Q1-Q8 mapped 80 → 650; y = 90-160 thousand mapped to 260 → 60, gridlines every 10 in `#eee` with labels in `#555`.
- **Reported line:** `#e74c3c` 2.5px with 4px dots, labeled at its right end in red "reported 150".
- **Human line:** `#1a5276` 2.5px with 4px dots, labeled at its right end in blue "human 105.0".
- **Agent wedge:** the band between the two lines filled `rgba(231,76,60,0.15)`, labeled in the middle in `#e74c3c`: "counted, not human".
- **Peak marker (index and value computed in JS via max of the derived series):** 6px `#e67e22` ring at Q5 with a dashed drop line, labeled "human peak 111.8 (Q5)".
- **Computed footer labels (left, y = 300 and 324):** bold `#e74c3c` "reported +50.0%" and bold `#1a5276` "human +5.0%, −6.1% from peak", both computed from the series endpoints and the located peak.
- **Bottom gray text (`#555`, right-aligned at the plot's right edge, y = 324):** "The blue line is what you would see if the label existed. It does not."

## Models Trained on the Polluted Corpus Learn Agent Style as Normal

**Contamination Compounds to 48.4% of the Corpus in Six Retrain Cycles**

- **The loop:** Each retrain ingests the newest user-generated content, which carries a rising agent share.
- **Corpus growth assumption:** The corpus grows 25% per cycle, and each increment is 20% → 90% agent-authored.
- **Recurrence:** `s_t = (s_{t−1} + 0.25·a_t) / 1.25`, where `s` is the cumulative agent-authored fraction.
- **Trajectory:** 0% → 4.0% → 10.2% → 18.2% → 27.5% → 38.0% → 48.4% across six cycles.
- **Half the corpus:** By cycle 6 the model's notion of "how a user writes" is nearly half agent output.
- **Style becomes the prior:** Agent phrasing stops being an outlier and starts being the modal target.
- **Detector inversion:** A style detector trained on this corpus flags distinctive human writing as anomalous.
- **Evaluation is affected too:** Held-out sets come from the same corpus, so contamination is invisible in scores.

### Visualization (canvas `c6`, 720×340)

Stacked bar chart across seven retrain cycles: human-authored vs agent-authored fraction of the training corpus, with per-cycle percentages computed from the recurrence at render time.

- **Title (bold 17px `#1a5276`):** "Corpus Contamination Across Retrain Cycles (Illustrative Example)".
- **Inputs (JS literals):** `growth = 0.25`; `newShare = [0.20, 0.35, 0.50, 0.65, 0.80, 0.90]` for cycles 1-6.
- **Derived (computed in JS, cycle 0 = 0):** cumulative agent fraction 0.0%, 4.0%, 10.2%, 18.2%, 27.5%, 38.0%, 48.4%.
- **Bars (7 slots from x = 70 to 660, bar width = slot × 0.62, baseline y = 250, full height 170):** human portion `rgba(26,82,118,0.35)` on the bottom, agent portion `#e74c3c` on top; the computed agent percentage printed above each bar in `#e74c3c` to 1 decimal; cycle index beneath in `#555`.
- **Reference line:** dashed 6/4 `#e67e22` horizontal line at the 50% height, labeled right in orange "50% of corpus".
- **Bottom text (centered):** bold red: "By cycle 6, 48.4% of the training corpus is agent-authored." (cycle count and percentage printed from the arrays); gray `#555`: "Held-out sets share the contamination, so validation scores never flag it."

## Agent Share Varies by Segment, So No Global Correction Works

**Uniform 22.0% Deflator: −17.9% on One Segment, +95.0% on Another, 0 Net**

- **Suppose you had `p`:** Even a correct global agent share is the wrong correction per segment.
- **The setup:** 100,000 sessions across four segments, 22,000 agent sessions → global share 22.0%.
- **Segment shares differ wildly:** Mobile app 5%, organic search 10%, review submission 50%, API integration 60%.
- **Applying 0.780 uniformly:** Mobile human sessions come out 23,400 against a true 28,500 — off by −17.9%.
- **Opposite sign next door:** API human sessions come out 11,700 against a true 6,000 — off by +95.0%.
- **Errors cancel exactly:** −4,800 + 5,700 − 5,100 + 4,200 = 0, and both totals equal 78,000.
- **Why that is dangerous:** The aggregate validates perfectly while every segment decision is wrong.
- **Segment-level `p`:** Estimating per-segment shares multiplies an already non-identifiable problem by four.

### Visualization (canvas `c7`, 720×340)

Grouped horizontal bar chart per segment: true human sessions vs the uniform-deflator estimate, with per-segment percentage error and a zero-sum total row, all computed at render time.

- **Title (bold 17px `#1a5276`):** "Uniform Correction, Segment-Level Damage (Illustrative Example)".
- **Data (JS literal):** `[{name:'Organic search', n:40000, p:0.10}, {name:'API integration', n:15000, p:0.60}, {name:'Mobile app', n:30000, p:0.05}, {name:'Review submission', n:15000, p:0.50}]`.
- **Derived in JS:** `agent_i = n_i·p_i` → 4,000 / 9,000 / 1,500 / 7,500, total 22,000; global `p = 22000/100000 = 0.220`; `trueHuman_i = n_i(1−p_i)` → 36,000 / 6,000 / 28,500 / 7,500; `estHuman_i = n_i(1−0.220)` → 31,200 / 11,700 / 23,400 / 11,700; error and percent error from those pairs.
- **Rows (4 rows, 56px pitch from y = 60, bar scale = 240px per 40,000 sessions, x origin 200):** segment name at x = 55 in `#333`; upper bar = true human in `rgba(26,82,118,0.35)` labeled with its computed count; lower bar = uniform estimate in `#e67e22` labeled with its computed count.
- **Legend (y = 44):** `#1a5276` "true human", `#e67e22` "uniform 22.0% deflator" (percentage computed from the global share).
- **Error column (x = 690, right-aligned, bold):** computed percent error per row, colored `#e74c3c` when the estimate exceeds truth and `#1a5276` when it falls short: −13.3%, +95.0%, −17.9%, +56.0%.
- **Total row (y = 290, bold `#27ae60`):** computed sums printing "Total: true 78,000 · estimated 78,000 · net error 0".
- **Bottom text (centered):** bold `#e74c3c`: "The total reconciles exactly. Every segment is wrong."; gray `#555`: "Aggregate agreement is not evidence that a correction is valid."

## Verification Countermeasures Exclude Real Users Along With Agents

**Escalating Tiers: Humans Blocked Per Agent Blocked Rises 0.18 → 0.67**

- **The arms race:** Each verification tier that raises agent friction raises human friction too.
- **Population:** The same 100,000 sessions — 22,000 agent, 78,000 human — pass through each tier.
- **Tier 1, checkbox:** 2,200 agents blocked against 390 humans → 0.18 humans per agent blocked.
- **Tier 2, image grid:** 8,800 agents blocked against 3,120 humans → 0.35 humans per agent blocked.
- **Tier 3, audio plus behavioural:** 14,300 agents against 7,020 humans → 0.49 humans per agent.
- **Tier 4, device attestation:** 18,700 agents against 12,480 humans → 0.67 humans per agent.
- **Not uniform harm:** Screen-reader-dependent users fail tier 3 at roughly 3× the 9% baseline, near 27%.
- **Survivorship added:** Blocked humans leave the dataset, so the surviving sample is now biased twice over.

### Visualization (canvas `c8`, 720×340)

Four-tier paired bar chart of agents blocked vs humans blocked, overlaid with a computed collateral-ratio line.

- **Title (bold 17px `#1a5276`):** "Verification Escalation and Collateral Exclusion (Illustrative Example)".
- **Inputs (JS literal, `agentPop = 22000`, `humanPop = 78000`):** `[{tier:'Checkbox', pass:0.90, fail:0.005}, {tier:'Image grid', pass:0.60, fail:0.040}, {tier:'Audio + behavioural', pass:0.35, fail:0.090}, {tier:'Device attestation', pass:0.15, fail:0.160}]`.
- **Derived in JS:** `agentsBlocked = agentPop·(1−pass)` → 2,200 / 8,800 / 14,300 / 18,700; `humansBlocked = humanPop·fail` → 390 / 3,120 / 7,020 / 12,480; `ratio = humansBlocked/agentsBlocked` → 0.18 / 0.35 / 0.49 / 0.67.
- **Bars (4 tier slots from x = 80 to 660, baseline y = 240, height scale 170px per 20,000):** agents blocked in `rgba(26,82,118,0.35)` on the left of each slot with its computed count above; humans blocked in `#e74c3c` on the right with its computed count above. Tier name beneath in `#555`, wrapped to two lines where needed.
- **Ratio line:** `#e67e22` 2.5px polyline across the four slot centres on a right-hand scale 0 → 0.80 (mapped 240 → 60), 5px dots, each labeled with the computed ratio to 2 decimals. Legend entry in orange: "humans blocked per agent blocked".
- **Bottom text (centered):** bold red: "Every tier improves agent blocking and worsens the collateral ratio."; gray `#555`: "Excluded humans are not random — assistive tech and unusual setups fail first."

## Regeneration instructions

- **Layout:** detail page. h1 (no index number) + `.subtitle` + one `.philosophy` callout, then one unnumbered `<h2>` per pitfall followed by a one-row `.obj-table`: left `<td>` (50%) holds `.obj-title` div + `<ul>` of labeled bullets, right `<td>` (50%, centered) holds the canvas. Even table rows have background `#fafcfe`.
- **Obj-title:** a refined restatement of the pitfall name, never a copy of it — one line, Title Case, carrying a concrete number already present in that section's own bullets.
- **Page CSS:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.4em `#1a5276` with 2px `#2980b9` bottom border, padding-bottom 8px; subtitle `#666` 1.05em; ul 0.9em `#333`; `strong` `#1a5276`; `.obj-title` 1.05em weight 600 `#1a5276`; `.philosophy` background `#f0f4f8`, left border 4px solid `#2980b9`, padding 12px 16px, 0.9em; table cell borders `1px solid #e0e0e0`, padding 20px 24px. No nav bar, no back/home links, no cross-references, no `thead`, no status badges.
- **Column split is fixed at 50/50.** Shrink a chart via the canvas `style.maxWidth`, never by narrowing the viz `<td>`.
- **Canvas:** intrinsic `width`/`height` attributes as given per chart; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. A shared `gauss(x, mu, sigma)` helper (exp(−0.5·((x−mu)/σ)²)/(σ·2.5066)) draws the density curves. Chart text uses 17px -apple-system font. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **No `Math.random()` anywhere.** All chart data on this page is hardcoded literal arrays because the bin shapes, counts, and totals carry the lesson. Where a seeded draw is ever added, use the canonical inline `lcg(seed)` Park-Miller generator with a fixed per-chart seed.
- **Every statistic printed beside data is computed at render time** from the plotted arrays: overlap coefficients, TVD, accuracy ceilings, mixture means, derived human series, peak location, contamination recurrence, percent errors, and collateral ratios. No statistic is written as a string literal.
- **Palette:** primary blue `#1a5276` / `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`, gray text `#555`/`#333`.
- **Tone:** defender/analyst perspective only — what breaks in the data and how to measure it. Nothing framed as guidance for evading detection. Fictional actors are Alice/Bob; organizations are "Platform A"/"Vendor A". All constructed figures labeled "Illustrative Example".
