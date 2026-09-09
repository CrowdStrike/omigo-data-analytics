# Cancer Screening

**Page type:** detail page (one h2 per pitfall, each followed by a two-column obj-table row: text left ~40%, canvas right ~60%)
**HTML title tag:** 72. Cancer Screening

**Subtitle:** Screening detects cancer earlier and more often — but earlier detection, more detection, and more testing are not the same thing as saving lives.

## Lead Time Bias

**Earlier Diagnosis Inflates "Survival" Without Extending Life**

- **The illusion:** Screening finds cancer 2 years earlier and "5-year survival" jumps from 40% to 70%.
- **The headline:** That 30-point survival gain reads as a major win for the screening program.
- **The reality:** The death date is unchanged — the survival clock simply started two years sooner.
- **Wrong metric:** Survival-from-diagnosis is mechanically inflated by any earlier detection.
- **Zero benefit case:** The inflation shows up even when the treatment that follows helps nobody.
- **The fix:** Judge screening by age-adjusted mortality in screened vs unscreened populations only.

### Visualization (canvas `c1`, 720×300)

Two parallel patient timelines (years 0-5) sharing one death date.

- **Title (bold 17px `#1a5276`, top center):** "Same Death Date — \"Survival\" Doubles Anyway".
- **Timelines:** horizontal lines `#2980b9` width 2 from x=80 to x=640 (year 0 to 5); top line at y=75 labeled "No screening" (17px `#333`), bottom line at y=150 labeled "Screening".
- **No-screening survival segment:** thick orange `#e67e22` segment (width 6) from year 3 to year 5 with a 6px orange dot at year 3; labels "Diagnosed yr 3" below the dot and "\"2-yr survival\"" above the segment (both 17px orange).
- **Screening survival segment:** thick green `#27ae60` segment from year 1 to year 5 with a 6px green dot at year 1; labels "Diagnosed yr 1" below and "\"4-yr survival\"" above (green).
- **Shared death line:** vertical dashed (6/4) red `#e74c3c` line width 2 at year 5 crossing both timelines, labeled bold red centered: "Death: yr 5" / "(both)".
- **Bottom annotations (centered):** bold 17px `#e74c3c` at y=250: "Survival statistic doubled. Life extended: zero years."; 17px `#555` at y=275: "The clock started earlier — the ending never moved. Judge screening by mortality."

## Overdiagnosis

**Finding Cancers That Would Never Have Caused Symptoms or Death**

- **The phenomenon:** Sensitive screening detects indolent tumors that never progress to symptoms.
- **The lifetime window:** Those tumors would have stayed silent for the rest of the patient's life.
- **The evidence:** Autopsies find prostate cancer in ~50% of 70-year-old men who died of other causes.
- **The harm:** Detection triggered biopsies, surgery, and radiation for tumors that needed nothing.
- **The trap:** Every overdiagnosed case still counts as a "success" in detection statistics.
- **Pure cost:** So the metric goes up while the patient receives harm with no offsetting benefit.

### Visualization (canvas `c2`, 720×300)

Three horizontal bars comparing detected cancers vs those that would ever matter.

- **Title (bold 17px `#1a5276`, top center):** "Cancers Detected vs Cancers That Would Ever Matter".
- **Bars (starting x=60, max width 560, height 26, rows 58px apart starting y=48; label above bar in 17px `#333`, value after bar):**
  - "Detected by intensive screening" — 100 — fill `rgba(41,128,185,0.3)`, edge `#2980b9`
  - "Would ever cause symptoms" — 50 — fill/edge `#f39c12`
  - "Would ever cause death" — 20 — fill/edge `#e74c3c`
- **Bottom annotations (centered):** bold 17px `#e74c3c` at y=245: "The gap = overdiagnosis: real cancers that needed no treatment."; 17px `#555` at y=268: "Autopsy: ~50% of 70-yr-old men have prostate cancer that never harmed them."; 17px `#555` at y=288: "Each one detected gets biopsy, surgery, radiation — harm with no benefit."

## Sensitivity vs Specificity at Population Scale

**95% Sensitive, 90% Specific, 0.5% Prevalence → 95.5% of Positives Are False**

- **The math:** At 0.5% prevalence, a 95%-sensitive / 90%-specific test gives a PPV of just 4.5%.
- **What that means:** A positive result carries only a 4.5% chance of actually having cancer.
- **Why:** The 10% false-positive rate applies to the 99.5% of the cohort who are healthy.
- **Swamped signal:** Those false alarms vastly outnumber true positives from the tiny sick fraction.
- **Scale effect:** At population scale specificity dominates the positive predictive value.
- **The trade:** One point of specificity removes more false alarms than one point of sensitivity adds.
- **Design rule:** Evaluate any mass screening test at the actual population prevalence.
- **Validation trap:** Never quote numbers from the balanced case-control set the test was built on.

### Visualization (canvas `c3`, 720×300)

PPV-vs-prevalence curves for three specificity levels at fixed 95% sensitivity.

- **Title (bold 17px `#1a5276`, top center):** "PPV vs Prevalence: Most Positives Are False".
- **Plot area:** padding left 70, right 190 (legend space), top 40, bottom 50. Axes L-shape `#2980b9` width 1.5. Y axis "PPV (%)" (rotated label), ticks 0%/25%/50%/75%/100% with light `#e0e0e0` gridlines; x axis "Disease Prevalence (%)", ticks 0% to 10% every 2%.
- **Curves (computed PPV = sens·prev / (sens·prev + (1−spec)·(1−prev)), sensitivity fixed 0.95, prevalence swept 0-10% in 200 steps, line width 2.5):**
  - Spec=90% — `#e74c3c`
  - Spec=95% — `#f39c12`
  - Spec=99% — `#27ae60`
- **Marker:** red dot radius 5 on the 90%-spec curve at prevalence 0.5%, labeled (17px red): "PPV = 4.5% at 0.5% prevalence".
- **Legend (right side):** header "Sensitivity = 95%" (17px `#1a5276`), then a colored line sample + label per curve ("Spec=90%", "Spec=95%", "Spec=99%").

## Incidentalomas

**Full-Body Scans Find "Something" in 40% of Healthy People**

- **The finding rate:** A full-body CT turns up an incidental abnormality in roughly 40% of healthy people.
- **The cascade:** Most findings are benign, but each one still demands a follow-up workup.
- **What follow-up costs:** More imaging, more radiation, sometimes a biopsy with real complication risk.
- **The hidden cost:** Every follow-up carries anxiety, expense, and iatrogenic risk for the never-sick.
- **The lesson:** Finding things is not intrinsically good, however sensitive the scanner is.
- **The test of value:** A detection is worth having only if acting on it improves the outcome.

### Visualization (canvas `c4`, 720×300)

Cascade funnel of horizontal bars: 100 healthy people scanned down to ~1 dangerous finding.

- **Title (bold 17px `#1a5276`, top center):** "Full-Body CT of 100 Healthy People".
- **Bars (labels right-aligned at x=238, bars start x=250, max width 380, height 26, rows 46px apart starting y=42, value after bar; minimum bar width 6px):**
  - "Scanned (all healthy)" — 100 — fill `rgba(41,128,185,0.3)`, edge `#2980b9`
  - "\"Something\" found" — 40 — `#f39c12`
  - "Follow-up imaging / biopsy" — 40 — `#e67e22`
  - "Actually dangerous" — ~1 — `#e74c3c`
- **Bottom annotations (centered):** bold 17px `#e74c3c` at y=248-270: "~39 healthy people pay anxiety, cost, radiation, and biopsy risk" / "to find the 1 that matters."; 17px `#555` at y=292: "Finding things is only good if acting on them improves outcomes."

## Screening Interval Optimization

**Annual vs Biennial: Marginal Benefit vs Doubled Harm**

- **The benefit:** Annual mammograms catch cancer about 6 months earlier than biennial screening.
- **The cost:** Screening twice as often doubles radiation exposure and doubles false positives.
- **Anxiety too:** It also doubles the anxiety episodes triggered by every positive result.
- **The asymmetry:** The benefit grows marginally with frequency while the harms scale linearly.
- **Not monotonic:** So "more screening" stops being better once the added harm outweighs the gain.
- **The method:** The optimal interval is a math problem, not an intuition that vigilance is safer.
- **What to compute:** Expected life-years gained weighed against cumulative harm across the cohort.

### Visualization (canvas `c5`, 720×300)

Paired horizontal bars per metric: biennial baseline vs annual, with multiplier notes.

- **Title (bold 17px `#1a5276`, top center):** "Annual vs Biennial Mammogram: What Doubles, What Doesn't".
- **Rows (labels right-aligned at x=213, bars start x=225, unit width 140px, two thin 10px bars stacked per row, rows 48px apart starting y=44; note after annual bar in 17px `#333`):**
  - Detection lead time — biennial 1×, annual 1.25× in `#27ae60` — note "+6 months earlier"
  - Radiation exposure — biennial 1×, annual 2× in `#e74c3c` — note "×2"
  - False positives — biennial 1×, annual 2× in `#e74c3c` — note "×2"
  - Anxiety episodes — biennial 1×, annual 2× in `#e74c3c` — note "×2"
- **Biennial baseline bar style:** fill `rgba(41,128,185,0.3)`, stroke `#2980b9`.
- **Legend (y≈238-248):** blue swatch "Biennial (baseline)"; red swatch "Annual".
- **Caption (bold 17px `#e74c3c`, centered at y=282):** "Marginal benefit, doubled harm. The optimal interval is math, not intuition."

## Psychological Cost of False Positives

**Three Weeks of Believing You Have Cancer Is Real Harm**

- **The experience:** "You might have cancer" → 3 weeks waiting for the biopsy → "actually, it's benign."
- **The accounting gap:** Those 3 weeks of fear are real health harm, not a bookkeeping footnote.
- **By design:** The system produced them while operating exactly at its designed specificity.
- **Not an error:** The false positive was later corrected, so no metric records any failure at all.
- **Already spent:** The harm happened during the wait, and the correction cannot refund it.
- **The implication:** Count corrected false positives as costs with health consequences, not as zero.

### Visualization (canvas `c6`, 720×300)

Patient timeline from positive screen (day 0) to benign biopsy (day 21), with an anxiety band over the interval.

- **Title (bold 17px `#1a5276`, top center):** "A \"Correct\" False Positive, As Lived by the Patient".
- **Anxiety band:** rect fill `rgba(231,76,60,0.15)` spanning x=90 to x=630, 76px tall centered at y=110; inside it centered bold 17px red text "3 weeks believing you may have cancer" and 17px red text "sleepless nights, calls to family, planning for the worst".
- **Timeline:** horizontal `#2980b9` line width 2 at y=148 extending slightly beyond the band; red `#e74c3c` dot radius 7 at the left end with bold red label "Day 0: screen POSITIVE"; green `#27ae60` dot radius 7 at the right end with bold green label "Day 21: biopsy benign".
- **Bottom annotations (centered):** 17px `#555` at y=230: "At 90% specificity this happens to ~10% of every healthy screening cohort — by design."; bold 17px `#e74c3c` at y=258: "No metric records a failure. The harm still happened. Count it as a cost."

## Regeneration instructions

- **Layout:** detail page in the domains-page style: h1 + `.subtitle`, then one `<h2>` per pitfall (1.4em `#1a5276`, bottom border `2px solid #2980b9`, padding-bottom 8px, margin 40px 0 15px), each followed by a full-width `.obj-table` with a single `<tr>`: left `<td>` (40%) holds `.obj-title` div + `<ul>` bullets, right `<td>` (60%, centered) holds the canvas. No thead, no nav, no badges, no cross-page links. HTML `<title>` is "72. Cancer Screening"; the on-page h1 is unnumbered.
- **Page CSS:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; `.subtitle` `#666` 1.05em; `ul` 0.9em `#333`, margin 8px 0 8px 20px; `strong` in `#1a5276`; `.obj-table td` border `1px solid #e0e0e0`, padding 20px 24px, vertical-align middle; even rows background `#fafcfe`; `.obj-title` 1.05em, weight 600, `#1a5276`; `.philosophy` callout style defined (background `#f0f4f8`, left border `4px solid #2980b9`) but unused on this page.
- **Palette:** primary blue `#1a5276`, accent blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, amber `#f39c12`, gray text `#666`/`#555`/`#333`, bar fill `rgba(41,128,185,0.3)`, gridlines `#e0e0e0`.
- **Canvas:** each canvas declares intrinsic `width`/`height` attributes (720×300); a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart fonts are 17px -apple-system (bold for titles/emphasis). Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
