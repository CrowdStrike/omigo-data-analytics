# Call Center

**Page type:** detail page (one h2 per pitfall, each followed by a one-row two-column obj-table: text left 50%, canvas right 50%)
**HTML title tag:** Call Center - Domain-Specific Pitfalls

**Subtitle:** Statistical pitfalls in call center analytics — metric gaming, survivor bias in IVR data, and the deception of averages.

## Handle Time vs Resolution Quality

**AHT 5 min Costs 15 Minutes per Resolution; AHT 12 min Costs 12**

- **The false dichotomy:** A short call may mean cut off; a long call may mean fully resolved.
- **Priya's path:** 4 min unresolved, then callbacks of 5 and 6 min — three queue waits.
- **Marcus's path:** The same billing issue in one 12-minute call, zero callbacks.
- **Dashboard illusion:** ~5 min/call vs 12 — she looks more than twice as efficient.
- **True cost:** 15 minutes per resolution vs 12, plus a far worse customer experience.
- **What to change:** Score time per resolution; AHT alone rewards the wrong behavior.

### Visualization (canvas `canvas1`, 720×200; HTML attributes declare 720×300 but the setup helper renders 720×200)

Two-row block diagram comparing total time per resolution; call blocks scaled 12px per minute.

- **Title (top center, bold 17px, `#1a5276`):** "Same Issue: Total Time per Resolution".
- **Priya row (y=70):** header 'Priya — AHT 5 min ("efficient")' bold 15px `#c0392b`. First call: red `#e74c3c` 48×25 block labeled "4 min" (white 12px); then two orange `#f39c12` callback blocks 60×25 "5 min" and 72×25 "6 min" (x offsets 0/63/138); note "2 callbacks" (13px `#666`) to the right. Summary bar: dark red `#c0392b` 170×25 block with white bold 13px "Per resolution: 15 min".
- **Marcus row (y=135):** header 'Marcus — AHT 12 min ("slow")' bold 15px `#27ae60`. One green `#27ae60` 144×25 block labeled "12 min (resolved)" (white 12px); gray `#999` 13px note "0 callbacks"; summary bar green 170×25 with white bold 13px "Per resolution: 12 min".
- **Winner indicator (bottom right, bold 17px, `#27ae60`, right-aligned):** "Better outcome".

## Agent Gaming Metrics

**Every Metric Is Gamed the Moment Agents Know About It**

- **If metric = calls/hour:** Agents hang up fast, transfer aggressively, dodge complex issues.
- **If metric = resolution:** Agents hoard "easy" calls and escalate the hard ones away.
- **The shift is immediate:** Behavior re-optimizes as soon as the new metric is announced.
- **Cost of the shift:** Gains come at the expense of the outcome the metric was meant to track.
- **Goodhart's Law:** When a measure becomes a target, it ceases to be a good measure.

### Visualization (canvas `canvas2`, 720×200; HTML attributes declare 720×300 but the setup helper renders 720×200)

Two-line time chart: calls/hour rises while resolution rate falls after the metric is introduced.

- **Title (top center, bold 17px, `#1a5276`):** 'Effect of Introducing "Calls/Hour" Metric'.
- **Axes:** black `#333` x and y axes; padding left 80, right 40, top 40, bottom 40. Bottom-center label "Time →" and rotated left label "Performance" (12px `#555`).
- **Metric-introduction marker:** vertical dashed gray `#999` line (dash 5/3) at 35% of chart width, with 12px `#666` caption "Metric introduced" below the axis.
- **Calls/Hour line:** blue `#2980b9` width 3 through data `[6, 6.2, 6.1, 6.3, 8, 9.5, 11, 12, 13, 14]` (scaled with range 4–16 over chart height) — flat before the marker, rising after.
- **Resolution Rate line:** dark red `#c0392b` width 3 through data `[82, 83, 81, 82, 75, 68, 60, 55, 50, 45]` (scaled with range 30–100) — flat before, falling after.
- **Legend (top right, 13px):** blue swatch + "Calls/Hour"; dark red swatch + "Resolution Rate %".

## Repeat Caller Identification

**Three Calls From Three Phones Become Three "Resolved" Tickets**

- **The mechanism:** One person calls 3 times about one issue from home, mobile, and work phones.
- **What the system sees:** 3 separate interactions, with no link between them.
- **Why it fails:** Linking them needs sophisticated identity matching that most stacks lack.
- **Metric damage:** "First call resolution" is inflated because repeats are miscounted as new.
- **The fiction:** If caller #47, #112, and #203 are one frustrated person, FCR is not real.

### Visualization (canvas `canvas3`, 720×200; HTML attributes declare 720×300 but the setup helper renders 720×200)

Reality-vs-system split diagram: one person with three phones vs three "resolved" tickets.

- **Title (top center, bold 17px, `#1a5276`):** 'Same Person, Three Phones, Three "Resolved" Tickets'.
- **Left (reality):** three blue `#2980b9` 30×20 phone rectangles at x=80 (y=60/100/140) with white 9px "TEL" text and 11px `#555` labels "Home", "Mobile", "Work"; blue connector lines (width 1.5) converge on a person icon at (240, 100) drawn in `#1a5276` (circle head radius 12 + semicircle body radius 18); captions "1 Person" and "1 Unresolved Issue" (12px `#333`) below; small gray `#888` 11px section label "REALITY" at bottom.
- **Divider:** vertical dashed `#ccc` line (dash 4/4) at x=350.
- **Right (system view):** bold 14px `#1a5276` header "System View:"; three 200×30 ticket boxes at x=430 (fill `#f0f0f0`, stroke `#27ae60` width 1.5) labeled "Ticket #1 - Resolved", "Ticket #2 - Resolved", "Ticket #3 - Resolved" in green 13px.
- **Reality label (bottom center-right, bold 13px, `#c0392b`):** "FCR = 100%?  Reality: 0%".

## IVR Dropout = Lost Data

**Only Successful IVR Completions Generate Data**

- **The scenario:** A caller navigates the IVR for 5 minutes, gives up, and hangs up.
- **What is lost:** Their intent is never recorded anywhere — they become invisible demand.
- **Dark matter:** The frustrated majority exists and has needs your analytics cannot see.
- **Survivor bias:** You model the patients who survived the waiting room, not those who left.

### Visualization (canvas `canvas4`, 720×200; HTML attributes declare 720×300 but the setup helper renders 720×200)

Three-tier funnel with dropout exit arrow.

- **Title (top center, bold 17px, `#1a5276`):** "IVR Funnel: The Invisible 60%".
- **Top tier (blue `#2980b9`, trapezoid from x=150–570 narrowing to x=200–520, y 40–85):** white bold 15px label "1,000 Callers Enter IVR".
- **Middle tier (red `#e74c3c` at 70% alpha, trapezoid narrowing further, y 88–135):** white bold 14px label "600 Drop Out (DARK MATTER)"; a dark red `#c0392b` arrow (width 2, filled head) exits right with 12px labels "Invisible" / "No data".
- **Bottom tier (green `#27ae60`, trapezoid, y 138–180):** white bold 14px label "400 Reach Agent" with 12px sub-caption "(Only these generate data)".
- **Left annotation (right-aligned, 12px `#666`):** "Your analytics" / "only sees this →" beside the bottom tier.

## Sentiment from Voice Tone

**Voice Sentiment Tops Out at 60-70% Accuracy**

- **Tone is ambiguous:** An angry voice may be excitement; a happy voice may carry a complaint.
- **Accuracy ceiling:** 60-70% at best, and worse for non-native speakers.
- **Regional idiom:** A Southern US "Well bless your heart" reads completely wrong.
- **Understatement:** British phrasing registers as mild when the meaning is severe.
- **Sarcasm:** It inverts meaning entirely, so the label lands opposite the truth.

### Visualization (canvas `canvas5`, 720×200; HTML attributes declare 720×300 but the setup helper renders 720×200)

4×4 confusion matrix for voice sentiment.

- **Title (top center, bold 17px, `#1a5276`):** "Voice Sentiment Confusion Matrix".
- **Matrix (cells 110×35, from x=180, y=45):** column header "PREDICTED SENTIMENT" (bold 12px `#555`); rotated row header "ACTUAL"; row/column labels "Positive", "Negative", "Sarcasm", "Neutral" (11px `#1a5276`).
- **Values (rows = actual, columns = predicted):**
  - Positive: 65, 5, 20, 10
  - Negative: 8, 62, 10, 20
  - Sarcasm: 35, 15, 30, 20
  - Neutral: 15, 10, 15, 60
- **Cell coloring:** diagonal cells green `rgba(39,174,96, 0.2 + value/100·0.6)`; off-diagonal red `rgba(231,76,60, 0.1 + value/100·0.6)`. Value text "N%" bold 13px, white when value > 40 else `#333`.
- **Annotation (right of matrix, 12px, `#c0392b`):** "Sarcasm detected only 30%" / "of the time — worse than" / "random for 4 categories".

## Seasonal Surge Modeling

**A Model Trained on the "Average Month" Fails in Every Surge Month**

- **January:** Returns from Christmas dominate the queue.
- **April:** Tax questions arrive with their own issue mix.
- **September:** School enrollment drives a third, unrelated surge.
- **Not just volume:** Call mix, caller demographics, complexity, and resolution paths all shift.
- **Why averaging breaks:** Each surge has a different character, so one flat model misses all.
- **What to change:** Build surge-specific models rather than one average-month model.

### Visualization (canvas `canvas6`, 720×200; HTML attributes declare 720×300 but the setup helper renders 720×200)

Monthly bar chart with a flat "average model" line missing the surges.

- **Title (top center, bold 17px, `#1a5276`):** 'Monthly Call Volume vs "Average" Model'.
- **Axes:** black `#333` axes; padding left 70, right 30, top 35, bottom 40. Y labels (11px `#555`): "10K", "5K", "0". X label "Month" centered at the bottom.
- **Bars (12 months Jan–Dec, 10px `#555` month labels):** volumes `[9500, 4200, 4000, 8800, 4100, 3800, 3900, 4300, 8200, 4000, 3900, 5500]`, max scale 10,000. Surge months Jan, Apr, Sep filled red `#e74c3c`; all others blue `#2980b9`.
- **Surge captions (9px `#c0392b`, under the month labels):** "Returns" (Jan), "Tax" (Apr), "School" (Sep).
- **Average model line:** dashed orange `#f39c12` horizontal line (dash 8/4, width 2.5) at 5,400.
- **Legend (top left, 12px `#333`):** red swatch "Surge months"; blue swatch "Normal months"; orange line swatch "Average model prediction (misses all spikes)".

## Regeneration instructions

- **Template/layout:** domains detail page. h1 + `.subtitle`, then per pitfall an `<h2>` (blue `#1a5276`, 1.4em, bottom border `2px solid #2980b9`) followed by a one-row `.obj-table`: left `<td>` (40%) with `.obj-title` (1.05em, weight 600, `#1a5276`) holding the one-line punchline, then a `<ul>` of labeled bullets (`ul` margin `8px 0 8px 20px`, 0.9em, `#333`; `li` margin `4px 0`), each `<li>` a `<strong>` label plus a short one-line phrase; right `<td>` (60%, centered) with the canvas. Even table rows have background `#fafcfe`; cell borders `1px solid #e0e0e0`, padding 20px 24px.
- **Page CSS:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; `.subtitle` `#666` 1.05em; `strong` in `#1a5276`; `.philosophy` callout style defined (background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em) but unused on this page. No nav bar, no back/home links.
- **Canvases:** six canvases (`canvas1`–`canvas6`) declare `width="720" height="300"` attributes in the HTML, but the shared `setupCanvas(id)` helper overrides each to 720×200 CSS pixels and scales the backing store by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). Default chart font 17px `-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif`.
- **Palette:** `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange; supporting colors `#2980b9`, `#f39c12`, `#c0392b`, grays `#555`/`#666`/`#888`/`#999`/`#ccc`.
- In regenerated HTML, any card/page links use `.html` extensions (this page has none).
