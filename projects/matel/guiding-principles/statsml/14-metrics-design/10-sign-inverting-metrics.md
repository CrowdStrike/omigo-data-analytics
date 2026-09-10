# Sign-Inverting Metrics

**Page type:** detail page (two-column obj-table layout: text left 40%, canvas right 60%, one h2 + one-row table per pattern, even rows shaded)
**HTML title tag:** Sign-Inverting Metrics

**Subtitle:** A lossy metric blurs the truth; a sign-inverting metric reverses it. These metrics move in the good direction precisely because something bad happened outside their field of view — the dashboard doesn't just miss the problem, it reports the problem as an improvement.

## 1. Sleep Score High, Person Exhausted

**Obj-title:** The event that ends the window well is itself the harm

- **Setup:** A wearable computes a sleep score from in-window signals only — duration (11pm–6am, a solid 7 hours) and depth of the recorded sleep stages. Both look good, so the score comes out high.
- **What actually happened:** The alarm fired mid sleep-cycle and cut deep sleep short, so the person spends the morning groggy from sleep inertia — the exact outcome the score claims to predict.
- **The inversion:** An abrupt cutoff can even look "efficient" because there are no trailing light-sleep or wake fragments, so waking at a worse moment can score better than a natural wake at a cycle boundary.
- **Root cause:** The score is validated against its own inputs (what happened between 11pm and 6am), never against the downstream outcome (next-day alertness), and the boundary condition — where in the cycle the window ended — dominates that outcome without being a feature.

**Fix:** Validate composite scores against the outcome they claim to represent (alertness ratings, reaction-time tests), and include boundary features like wake timing relative to cycle phase — not just window aggregates.

### Visualization (canvas `sv1`, 720×300)

Sleep-stage step chart with an alarm cutting the final cycle.

- **Title (bold 16px `#1a5276`, top center):** "7 Hours, Good Depth, Score: 88 — Woken Mid-Cycle, Groggy All Morning".
- **Plot area:** margins left 70, right 30, top 45, bottom 55. Three y levels labeled on the left in 12px `#555`: "Awake" (top), "Light", "Deep" (bottom). Light horizontal gridlines `#e0e0e0` at each level.
- **Step line:** blue `#1a5276`, width 2.5, stepping through ~90-minute cycles from 11pm to 6am: Awake→Light→Deep→Light→Deep→Light→Deep→Light→Deep, with the final Deep segment truncated at the right edge (the line ends at the Deep level — it never returns to Light/Awake).
- **Alarm marker:** red `#e74c3c` dashed (4/3) vertical line at x = right edge of plot, labeled "alarm — mid deep cycle" in bold 13px red, rotated horizontal above the plot bottom.
- **X labels (12px `#555`):** "11pm", "1am", "3am", "5am", "6am" spaced along the bottom.
- **Score badge:** green `#27ae60` filled rounded rect ~110×34 at top-left of plot with white bold 15px text "Score: 88".
- **Bottom line (bold 14px `#e74c3c`, centered):** "Actual outcome: sleep inertia → groggy. The score never sees anything after 6am."

## 2. Average Latency Improves Because the Heaviest Users Left

**Obj-title:** The worst observations exited the frame — the average followed them out

- **Setup (Illustrative Example):** Average request latency trends down for three straight quarters and the team reports a performance win.
- **What actually happened:** The largest accounts — biggest datasets, slowest queries — churned *because of* the slowness, and their departure removed the slowest requests from the mix.
- **The inversion:** The worse the product gets for heavy users, the faster they leave, and the better the latency chart looks.

**Fix:** Pair every population average with a mix/coverage metric — latency per cohort, weighted by account size — and segment before celebrating a trend shift.

### Visualization (canvas `sv2`, 720×300)

Two-line chart: average latency falling while heavy-user count falls with it.

- **Title (bold 16px `#1a5276`, top center):** "Latency ↓ Looks Like a Win — Until You Plot Who Left".
- **Data:** 8 monthly points. Avg latency (ms) `[420, 410, 395, 370, 340, 320, 300, 290]` in green `#27ae60`, width 3. Heavy-user accounts `[100, 98, 92, 84, 73, 62, 55, 50]` in red `#e74c3c`, width 3, scaled to the same plot height.
- **Axes:** margins left 60, right 40, top 45, bottom 40; gray `#999` axes, light gridlines `#e0e0e0`; x labels "M1"…"M8" in 12px `#555`.
- **Legend (top right, 13px):** green swatch "avg latency (ms)", red swatch "heavy-user accounts".
- **Annotation (bold 14px `#c0392b`, centered at bottom):** "Same cause drives both lines: the slowest workloads churned."

## 3. Crash Rate Falls Because Crashes Kill the Reporter

**Obj-title:** The measurement channel shares a failure mode with the thing it measures

- **Setup:** Crash telemetry is uploaded from inside the app — by a crash handler or on next launch.
- **What actually happened:** A severe regression hard-kills the process before the handler can run, so the worst crashes are exactly the ones least likely to be reported, and the dashboard shows crash rate improving.
- **General form:** Any metric whose reporting path can die with the failure inverts under severe failures — a logging pipeline that falls over during incidents makes error rate "improve" during outages.

**Fix:** Measure from outside the failure domain (server-side session heartbeats, external watchdogs), treat missing data as signal, and alert on drops in reporting volume itself.

### Visualization (canvas `sv3`, 720×300)

Grouped bar chart: reported vs actual crashes by severity.

- **Title (bold 16px `#1a5276`, top center):** "Reported vs Actual Crashes — Fatal Ones Can't Phone Home".
- **Data:** three severity groups "Minor", "Major", "Fatal". Reported bars (solid blue `rgba(26,82,118,0.65)`): `[92, 60, 8]`. Actual bars (red outline `#e74c3c`, fill `rgba(231,76,60,0.2)`): `[95, 78, 70]`.
- **Layout:** margins left 55, right 30, top 50, bottom 45; y max 100 with gridlines every 25; each group has two bars ~60px wide with 8px gap; group labels in 13px `#555` under the bars.
- **Legend (top right, 13px):** blue swatch "reported", red swatch "actual".
- **Annotation (bold 14px `#e74c3c`, centered under Fatal group):** gap arrow or bracket between the Fatal pair labeled "invisible".

## 4. Support Tickets Drop Because Users Gave Up

**Obj-title:** Silence read as satisfaction

- **Setup (Illustrative Example):** Ticket volume falls 30% year over year and is presented as a product-quality improvement.
- **What actually happened:** Users learned that reporting doesn't help, so they stopped filing tickets and started quietly churning instead.
- **The inversion:** The more hopeless support feels, the fewer tickets arrive — the metric improves as trust collapses.

**Fix:** Pair complaint volume with retention of past complainers and the first-time-filer rate; a complaint is engagement, and losing it is not automatically good news.

### Visualization (canvas `sv4`, 720×300)

Two-line chart: tickets and retention falling together.

- **Title (bold 16px `#1a5276`, top center):** "Tickets ↓ 30% — So Did Retention".
- **Data:** 8 quarterly points. Tickets/month `[500, 480, 450, 420, 390, 370, 355, 350]` in green `#27ae60`, width 3. 12-month retention (%) `[86, 85, 82, 78, 73, 69, 66, 64]` in red `#e74c3c`, width 3, scaled to the same plot height.
- **Axes:** margins left 60, right 40, top 45, bottom 40; gray axes `#999`, light gridlines; x labels "Q1"…"Q8" in 12px `#555`.
- **Legend (top right, 13px):** green swatch "tickets/month", red swatch "12-mo retention".
- **Annotation (bold 14px `#c0392b`, centered at bottom):** "Both falling together = disengagement, not quality."

## 5. Success Rate Rises Because Hard Cases Are Turned Away

**Obj-title:** The denominator got easier, not the team better

- **Setup (Illustrative Example):** A team's success rate climbs quarter over quarter and is read as improving skill or process.
- **What actually happened:** Intake started declining the riskiest cases, so the denominator got easier while total outcomes across all cases — accepted or rejected — got worse.
- **Where it shows up:** Surgical report cards that discourage operating on the sickest patients, school averages that push weak students out of the tested pool, approval-quality metrics that reward rejecting marginal applicants.

**Fix:** Track the full funnel including rejections, and measure outcomes over the original population (intention-to-treat), not the filtered one.

### Visualization (canvas `sv5`, 720×300)

Paired bars per quarter: success rate rising while declined cases rise.

- **Title (bold 16px `#1a5276`, top center):** "Success Rate ↑ Every Quarter — So Are Declined Cases".
- **Data:** 4 quarters "Q1"…"Q4". Success rate (%) bars in green `rgba(39,174,96,0.6)`: `[78, 82, 86, 91]`. Declined cases bars in red `rgba(231,76,60,0.55)`: `[12, 21, 33, 48]`, plotted on the same 0–100 scale.
- **Layout:** margins left 55, right 30, top 50, bottom 45; y gridlines every 25; paired bars ~65px wide with 10px gap per group; quarter labels 13px `#555`.
- **Legend (top right, 13px):** green swatch "success rate (%)", red swatch "cases declined".
- **Annotation (bold 14px `#c0392b`, centered at bottom):** "The metric improved by shrinking who gets counted."

## 6. Beating Your Own Sleep Score at a Net Loss

**Obj-title:** Two hours of optimization to avoid two hours of sleep

- **Setup (Illustrative Example):** A person spends 1–2 hours every night on relaxation routines — meditation, stretching, wind-down protocols — so they can "get by" on 4–6 hours of sleep with a high quality score.
- **What they're optimizing:** The sleep-efficiency score, on a device or in their head — time asleep ÷ time in bed, depth per hour — and compressed sleep scores beautifully on exactly those terms.
- **The irony:** The optimization overhead equals or exceeds the sleep it "saves," and an hour of relaxing is not a substitute for an hour of sleeping — memory consolidation and physical recovery happen only in actual sleep. Simply sleeping the extra 1–2 hours dominates: same time budget, better outcome, zero extra effort.
- **General form:** Goodhart's law applied to yourself — once the score becomes the target, effort flows into making the score go up instead of the outcome, and the effort spent beating the metric can exceed the entire value the metric was built to capture.

**Fix:** When optimizing a proxy costs real resources, compare against the naive baseline of spending those same resources on the outcome directly; if the naive baseline wins, the metric is consuming value, not measuring it.

### Visualization (canvas `sv6`, 720×300)

Two horizontal stacked time-budget bars covering the same 7 hours.

- **Title (bold 16px `#1a5276`, top center):** "Same 7 Hours — One Optimizes the Score, One the Outcome".
- **Bar A (y ≈ 80, height 44, x from 150 to 670):** left segment ~29% in orange `#e67e22` labeled inside in white bold 13px "2h wind-down routine"; right segment ~71% in blue `#1a5276` labeled "5h compressed sleep". Row label left of the bar in 13px `#555`: "Optimizer". Right of the bar, green bold 14px: "score: 92".
- **Bar B (y ≈ 160, height 44, same x range):** single segment 100% in blue `#1a5276` labeled inside in white bold 13px "7h sleep". Row label "Just sleep". Right of the bar, gray bold 14px: "score: 85".
- **Bottom annotation (bold 14px `#c0392b`, centered):** "B wakes more rested with less effort. A paid 2 hours to make a number bigger."

## 7. More Training, Less Muscle

**Obj-title:** The effort metric counts the stimulus — muscle is built during recovery

- **Setup:** Someone training for muscle tracks weekly volume — sessions, sets, hours — and the natural read is monotone: more volume, more muscle.
- **What actually happens:** Adaptation happens during recovery, not during the workout, so past the point where recovery capacity is exhausted, added volume stalls gains — and combined with a caloric or sleep deficit, it tips into a catabolic state where muscle is actually lost.
- **The inversion:** The dose-response is an inverted U, but the effort metric is a straight line — so in the overtrained regime, the harder the metric says you're working, the worse the outcome you're working for.

**Fix:** Track the outcome (strength, measurements) against the effort metric and look for the plateau; when the outcome flattens while effort climbs, the marginal unit of effort has changed sign.

### Visualization (canvas `sv7`, 720×300)

Inverted-U outcome curve against a monotone effort metric.

- **Title (bold 16px `#1a5276`, top center):** "Effort Metric Is a Straight Line — the Outcome Is an Inverted U".
- **Axes:** margins left 60, right 30, top 45, bottom 45; x label "weekly training volume →" (12px `#555`, centered below), y label "muscle gained" rotated on the left; gray `#999` axes, no gridlines.
- **Outcome curve:** green `#27ae60`, width 3 — rises from the origin, peaks around 55% of x range, then declines below its starting slope by the right edge.
- **Effort line:** blue `#1a5276`, width 2, dashed (6/4) — straight diagonal from origin to top-right, labeled "what the tracker shows" in 13px blue near its upper end.
- **Overtraining zone:** shade x > 65% with `rgba(231,76,60,0.10)`, labeled "recovery exhausted" in bold 13px `#e74c3c` at the top of the zone.
- **Bottom annotation (bold 14px `#c0392b`, centered):** "Right of the peak, every added unit of effort buys a worse outcome."

## 8. Following the Sleep Rule Destroys What the Rule Was For

**Obj-title:** The letter of a heuristic beats its spirit

- **The rule (partly sound, partly pseudoscience):** "It doesn't matter when you sleep as long as you wake at the same time." Consistent wake time is a real circadian-anchoring heuristic — the false half is that bedtime therefore doesn't matter.
- **How it gets gamed:** Bedtime drifts later, the alarm enforces the fixed wake time anyway, and the alarm lands mid-cycle on shortened sleep — the rule is followed perfectly while duration and cycle completion, the things the rule was meant to protect, are destroyed.
- **General form:** A heuristic is a compressed proxy for a richer condition, and enforcing the compressed form against the richer condition inverts it — the more faithfully the rule is followed, the worse the protected outcome gets.

**Fix:** State the condition a heuristic proxies for alongside the rule ("consistent wake time *and* enough hours before it"), and audit whichever half is cheapest to violate.

### Visualization (canvas `sv8`, 720×300)

Two horizontal night-timeline bars with a fixed wake line.

- **Title (bold 16px `#1a5276`, top center):** "Fixed Wake Time, Drifting Bedtime — the Rule Holds, the Sleep Doesn't".
- **Time axis:** x maps 10pm→8am across the plot (margins left 110, right 40, top 50, bottom 45); tick labels "10pm", "12am", "2am", "4am", "6am", "8am" in 12px `#555`.
- **Bar A (y ≈ 85, height 40):** blue `#1a5276` rect from 11pm to 6:30am, row label "Week 1" left of the bar; inside white bold 13px text "7.5h — ends near cycle boundary".
- **Bar B (y ≈ 160, height 40):** blue rect from 2am to 6:30am with the final portion (last ~45 min) overlaid in red `rgba(231,76,60,0.55)`; row label "Week 4"; inside white bold 13px text "4.5h — alarm cuts mid-cycle".
- **Fixed wake line:** vertical dashed (4/3) green `#27ae60` line at 6:30am spanning both bars, labeled "same wake time ✓" in bold 13px green above.
- **Bottom annotation (bold 14px `#c0392b`, centered):** "The rule is satisfied in both rows. Only one of them slept."

## 9. Hitting the Protein Number Through the Cheapest Channel

**Obj-title:** A rich requirement collapsed to one scalar gets satisfied by the channel that carries nothing else

- **Setup:** "Eat for muscle" gets compressed into a single tracked number — grams of protein per day — and the number gets satisfied the cheapest way available: protein powder at every gap.
- **What the scalar dropped:** A protein-rich diet carries micronutrients, fiber, satiety, and food variety along with the protein; the powder ships the tracked dimension and none of the untracked ones.
- **The honest caveat:** For the narrow target the metric names — total daily protein for muscle synthesis — powder genuinely works; the failure is that the scalar silently absorbed the whole goal ("eat well"), so beating the number feels like completing the goal, and pseudoscience supplies the story for why the shortcut is equivalent.
- **General form:** Whenever a rich requirement is compressed into one number, the number will eventually be satisfied through a channel that carries none of the requirement's other dimensions.

**Fix:** When a metric is a compression of a richer goal, either track the dropped dimensions too or constrain the satisfying channel — "grams from meals," not just "grams."

### Visualization (canvas `sv9`, 720×300)

Two stacked bars delivering the same tracked layer with very different untracked layers.

- **Title (bold 16px `#1a5276`, top center):** "Two Ways to Hit 140g — the Tracker Can't Tell Them Apart".
- **Layout:** two bar groups centered at ~30% and ~70% of width, bars 140px wide, baseline at h−55; group labels below in 13px `#555`: "Protein-rich diet" and "Powder-first".
- **Bar 1 (diet):** bottom layer blue `#1a5276` height 90 labeled inside in white bold 13px "140g protein"; top layer green `rgba(39,174,96,0.55)` height 80 labeled in 12px `#1a5276`-on-light "micronutrients · fiber · satiety".
- **Bar 2 (powder):** bottom layer blue `#1a5276` height 90 labeled "140g protein"; top layer green height 10, unlabeled.
- **Tracker box:** dashed (5/4) orange `#e67e22` rectangle enclosing just the two blue layers, labeled to the right in bold 13px `#e67e22`: "what the tracker sees: identical".
- **Bottom annotation (bold 14px `#c0392b`, centered):** "The metric was a compression of 'eat well.' The shortcut ships only the compressed part."

## 10. The Inversion Test

**Obj-title:** Ask what would make this metric improve if things got worse

- **The shared mechanism:** An aggregate over a window or population improves whenever the worst observations exit the frame — and churn, crashes, giving up, gatekeeping, and alarm cutoffs are exactly the events that remove them, so the harm and the exit are the same event.
- **The gamed variant:** Once the score becomes the target — sleep efficiency, weekly training volume, protein grams, a fixed wake time — effort flows to whatever satisfies the number most cheaply, and the cheapest channel is usually the one that drops what the number was a proxy for.
- **Blur vs inversion:** An incomplete metric usually just correlates weakly with the truth, but in these regimes the metric anti-correlates — within the affected range, up means worse.
- **Checklist:** (1) Name a concrete scenario where the metric improves while the real outcome worsens, and if you can name one, instrument it. (2) Validate composite scores against downstream outcomes, never only against their own inputs. (3) Pair every rate and average with its denominator or coverage. (4) Confirm the measurement channel cannot die with the thing it measures. (5) Price the effort spent beating the metric against the outcome it buys — a metric that costs more to satisfy than it returns is consuming value, not measuring it.

### Visualization (canvas `sv10`, 720×300)

Causal diagram: one bad event, two arrows with opposite signs.

- **Title (bold 16px `#1a5276`, top center):** "Same Cause, Opposite Signs".
- **Top box:** orange `#e67e22` outlined rounded rect ~360×46 centered near top with 14px `#333` text "Bad event: churn / crash / give-up / cutoff / rejection".
- **Two arrows:** from the box's bottom corners diverging down-left and down-right, drawn in `#555`, width 2, with arrowheads.
- **Left box:** green `#27ae60` filled rounded rect ~280×56 with white bold 14px two-line text "Worst observations leave the window" / "Metric ↑ (looks better)".
- **Right box:** red `#e74c3c` filled rounded rect ~280×56 with white bold 14px two-line text "Reality degrades" / "Outcome ↓ (actually worse)".
- **Bottom line (bold 15px `#c0392b`, centered):** "If a metric can move for reasons like these, its trend is unsigned until you check coverage."

## Regeneration instructions

To rebuild the HTML from this spec:

- **Template:** same detail-page style as `metrics/03-anti-patterns.html`. Single page: h1, `.subtitle` paragraph, then one h2 + one-row `.obj-table` per section above.
- **Table layout:** `.obj-table` full width, collapsed borders `#e0e0e0`, cell padding 20px 24px; first cell 40% width (text), second cell 60% centered (canvas). Even rows shaded `#fafcfe`.
- **Text cell structure:** `.obj-title` (1.05em, 600 weight, `#1a5276`), then a `ul` of the labeled bullets, then a `<p><strong>Fix:</strong> …</p>` where the section has one.
- **Canvas:** each canvas 720×300 with `width: 100%`, drawn via a shared `setup(id)` helper that sizes the backing store to displayed width × `devicePixelRatio` and rescales the context; all draw functions pushed to a `__charts` array, executed on load and re-executed on debounced window resize (150ms).
- **Page style:** body system sans-serif, white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.3em `#1a5276` with 2px `#2980b9` bottom border; subtitle `#666` 1.05em. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`-family.
