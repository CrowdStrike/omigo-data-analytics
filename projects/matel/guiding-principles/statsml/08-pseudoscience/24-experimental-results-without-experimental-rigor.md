# Experimental Results Without Experimental Rigor

**Page type:** detail page (two-column obj-table layout: text left 50%, canvas right 50%, one row per section)
**HTML title tag:** Experimental Results Without Experimental Rigor — Pseudoscience in Data Analysis

**Subtitle:** Claiming "we ran experiments" while skipping everything that makes an experiment meaningful

## Section 1: The Pattern

- **The claim:** "We ran experiments. Our model achieves 99% accuracy." Presented as empirical science.
- **What was done:** trained a model on a small or curated dataset, evaluated on a convenient test set, published the percentage as a finding.
- **What was skipped:** no power analysis, no confidence interval, no proper held-out design, no baseline, no description of what the test set represents or excludes.
- **Why it's pseudoscience:** the word "experiment" and the precision of "99%" borrow the authority of the scientific method. Without controls, uncertainty quantification, and reproducibility, it is measurement theater.

### Visualization (canvas `c1`, 720×340)

Two side-by-side comparison boxes: what the audience infers vs what was actually done.

- **Title (bold 14px, `#1a5276`, centered):** "What \"We Ran Experiments\" Implies vs What Was Actually Done".
- **Boxes:** 290×240 each, top y=45; left at x=40, right at x = w−330.
- **Left box:** fill `rgba(39,174,96,0.06)`, border green `#27ae60` 1.5px; centered green bold 13px header "WHAT THE AUDIENCE INFERS"; 12px `#333` lines (24px spacing):
  - "✓ Hypothesis stated beforehand"
  - "✓ Sample size calculated (power analysis)"
  - "✓ Proper held-out test set"
  - "✓ Confidence interval computed"
  - "✓ Baseline compared"
  - "✓ Reproducible by others"
  - "✓ Falsifiable (can be disproven)"
  - "✓ Dataset described & representative"
- **Right box:** fill `rgba(231,76,60,0.06)`, border red `#e74c3c` 1.5px; centered red bold 13px header "WHAT WAS ACTUALLY DONE"; lines:
  - "✗ No hypothesis (post-hoc measurement)"
  - "✗ No power analysis (used whatever exists)"
  - "✗ Test set = convenient subset"
  - "✗ No CI reported (bare percentage)"
  - "✗ No baseline stated"
  - "✗ Not reproducible (dataset undescribed)"
  - "✗ Not falsifiable (can't replicate)"
  - "✗ Dataset: \"our benchmark\""
- **Caption (bottom center, italic 12px `#666`):** "The word \"experiment\" bridges this gap. The audience fills in the rigor that was never present."

## Section 2: What Makes an Experiment Scientific

- **Hypothesis:** a falsifiable prediction stated before the run. "Model X beats baseline Y by at least Z% on population P."
- **Sample size:** calculated up front to detect the expected effect with adequate power and significance.
- **Confidence interval:** "99%" at n=100 has CI [94.6%, 99.9%]. Reporting the point estimate alone presents a guess as a fact.
- **Reproducibility:** dataset, method, and conditions described precisely enough that others can replicate and potentially falsify the result.
- **Baseline:** 99% relative to what — random, previous best, a simple heuristic? Without one, the number has no context.

**Missing any of these? You ran a computation and called it science.**

### Visualization (canvas `c2`, 720×340)

Horizontal bar chart: scientific requirement vs share of typical ML papers complying.

- **Title (bold 14px, `#1a5276`, centered):** "Scientific Experiment Requirements — Typical ML Paper Compliance".
- **Rows (label right-aligned at x=268, 12px `#333`; bars start at x=280, row height 38, bar height 22, top y=50):**
  - "Falsifiable hypothesis" — 15%
  - "Power analysis / sample size justification" — 5%
  - "Confidence interval reported" — 20%
  - "Reproducible (code + data available)" — 30%
  - "Proper held-out test set" — 55%
  - "Baseline comparison" — 65%
  - "Metric computed" — 100%
- **Bar color:** interpolated red→green by compliance: `rgba(231−192·c, 76+98·c, 60, 0.5)` fill with matching 0.8-alpha stroke (red at 0%, green at 100%).
- **Value labels:** bold 11px `#333` right of each bar: "N% of papers".
- **Threshold line:** red `#e74c3c` dashed horizontal line (width 2, dash 4/3) between the 5th and 6th rows, with right-aligned red bold 11px label above it: "← Below this line: not science".
- **Caption (bottom center, italic 12px `#666`):** "Most published ML \"experiments\" meet only the bottom 2–3 requirements. The rest is assumed."

## Section 3: How Make-Believe Numbers Are Produced

- **Step 1:** curate a small, convenient dataset — often from the same distribution as training.
- **Step 2:** run the model and pick the most impressive-sounding metric.
- **Step 3:** normalize to a percentage: "99% accuracy."
- **Step 4:** publish without CI, n, or dataset description, labeled "experimental results."
- **Step 5:** the audience assigns it the credibility of a controlled study. The missing rigor is invisible to anyone who doesn't ask.

**The number is technically computed and not technically wrong — it is scientifically meaningless.**

### Visualization (canvas `c3`, 720×340)

Vertical flowchart of four steps with dashed annotations of what's missing at each step.

- **Title (bold 14px, `#1a5276`, centered):** "How a Computation Becomes \"Scientific Evidence\"".
- **Step boxes** (160×44 at x=140; y=50, 110, 170, 230; light tinted fill from the step color + '11' alpha suffix, 1.5px border, centered bold 12px two-line labels; gray `#ccc` connector lines between boxes):
  - "Curate / small dataset" — orange `#e67e22`
  - "Run model / compute metric" — orange `#e67e22`
  - "Normalize / to percentage" — orange `#e67e22`
  - "Publish as / \"experimental result\"" — red `#e74c3c`
- **Missing annotations** (at x=380, connected by dashed `#ccc` lines from each box's right edge; 12px, orange `#e67e22` for first three, red `#e74c3c` for last):
  - "✗ No power analysis. n chosen by convenience."
  - "✗ No hypothesis. Metric chosen post-hoc."
  - "✗ CI not computed. Denominator erased."
  - "✗ Reader infers full scientific rigor."
- **Caption (bottom center, italic 12px `#666`):** "Each step is technically valid. The sequence is not science. Calling it science is pseudoscience."

## Section 4: The Pseudoscience Markers

- **Borrowed authority:** "we ran experiments" implies hypothesis, controls, and reproducibility. None were present — the word does the legitimizing work.
- **Unfalsifiable:** without methodology or dataset description, no one can replicate or disprove the claim. It is insulated from scrutiny by omission.
- **Precision without accuracy:** "99.2%" carries 3 significant figures while the CI spans 8 percentage points. The precision exceeds the knowledge.
- **Resistant to challenge:** questioning the number requires asking "what was your sample size?" — a question that socially implies distrust and is rarely asked.

### Visualization (canvas `c4`, 720×340)

Paired horizontal bars comparing apparent precision vs actual knowledge for two claims.

- **Title (bold 14px, `#1a5276`, centered):** "Precision of the Number vs Actual Knowledge".
- **Two rows** (row height 120, top y=55; claim label bold 14px `#1a5276` left-aligned):
  - Row 1: label "\"99.2% accuracy\"" — knowledge 0.15, n=100.
  - Row 2: label "\"94.2% ± 1.3%\"" — knowledge 0.85, n=12,400.
- **Per row:** "Apparent precision:" bar — 95% of available width, fill `rgba(26,82,118,0.3)`, stroke `#1a5276`, right-aligned inside label "3 sig figs" (bold 10px `#1a5276`). "Actual knowledge:" bar — width = knowledge fraction; fill/stroke red `rgba(231,76,60,0.5)`/`#e74c3c` if knowledge ≤ 0.5, else green `rgba(39,174,96,0.5)`/`#27ae60`; label right of bar: "n=100 — CI spans 5+ points" (row 1) / "n=12,400 — CI ±1.3%" (row 2).
- **Gap annotation (row 1 only, red bold 11px):** "← Gap between what the number claims and what is known".
- **Caption (bottom center, italic 12px `#666`):** "Precision without adequate sample size is performance — not measurement."

## Section 5: Repeated Evaluation on the Same Test Set

- **The practice:** evaluate → tune → re-evaluate on the same test set, dozens of times. Report the best score and call each iteration an "experiment."
- **The violation:** a test set is valid exactly once. After it informs tuning, it silently becomes a second training set, and every later score is biased upward.
- **What happens:** you correct the specific mistakes this sample exposed. Failure modes a different sample would expose remain unaddressed and invisible.
- **The result:** the published number reflects coverage of one sample's error surface, not generalization to the problem.
- **The fix:** after a few rounds of tuning, retire the test set and source a fresh one. Budget for test set rotation the way you budget for retraining.

**You optimized one sample of the problem — not the problem itself.**

### Visualization (canvas `c6`, 720×340)

Two-line chart over 10 tuning iterations: test-set score climbs while unseen-data score stalls.

- **Title (bold 14px, `#1a5276`, centered):** "Repeated Evaluation: Fixing One Sample's Errors ≠ Solving the Problem".
- **Plot area:** left 80, right w−50, top 55, bottom 200; y-scale maps value range 75–100 to plot height; gridlines `#f0f0f0` every 5 from 80 to 100; light `#ddd` axes.
- **Data (iterations 1–10):**
  - Score on this test set: `[85, 87, 89, 90, 92, 93, 94, 95, 96, 97]` — green `#27ae60` line, width 2.5, 3px dots.
  - Score on unseen data: `[84, 86, 87, 87, 86, 85, 84, 83, 83, 82]` — red `#e74c3c` line, width 2.5, 3px dots.
- **X labels (11px `#666`):** "Iter 1", "Iter 5", "Iter 10"; **Y labels:** 80%, 90%, 100%.
- **Legend (top-left, 12px `#333`):** green swatch — "Score on this test set (climbing — errors fixed)"; red swatch — "Score on unseen data (stalls — other errors remain)".
- **Gap annotation:** orange `#e67e22` dashed vertical line (width 1.5, dash 3/3) at x = right−30 between the final points, with two-line bold 11px orange label: "Gap = errors only" / "this dataset exposed".
- **Bottom notes (centered):** 12px `#333`: "Each iteration corrects mistakes this test set surfaced. Mistakes a different sample would expose remain invisible."; red bold 12px: "Published: \"97% accuracy.\" Actual generalization: 82%."; italic 12px `#666`: "You optimized for one sample of the problem — not the problem itself."

## Section 6: Real vs Pseudoscientific Claims

- **Real:** "94.2% ± 1.3% (95% CI) on 12,400 production samples vs 89.1% baseline (p < 0.001). Dataset: Jan–Mar 2026 production traffic."
- **Pseudo:** "Our model achieves 99% accuracy on our benchmark."
- Both are published as "experimental results." One can be verified, replicated, and falsified. The other borrows the appearance of science.

### Visualization (canvas `c5`, 720×300)

Two stacked full-width claim boxes contrasting epistemic status.

- **Title (bold 14px, `#1a5276`, centered):** "Same Sentence Structure — Different Epistemic Status".
- **Boxes:** width w−80 at x=40, height 95, gap 20, first at y=50.
- **Top box (Pseudoscientific):** fill `rgba(231,76,60,0.05)`, border red `#e74c3c` 2px; red bold 12px header "PSEUDOSCIENTIFIC"; 13px `#333` claim: "\"Our model achieves 99% accuracy on our benchmark.\""; 11px `#666` lines: "Cannot verify. Cannot replicate. Cannot falsify. CI unknown. n unknown. Benchmark undescribed." and "Status: a claim borrowing the appearance of empirical evidence."
- **Bottom box (Scientific):** fill `rgba(39,174,96,0.05)`, border green `#27ae60` 2px; green bold 12px header "SCIENTIFIC"; claim: "\"94.2% ± 1.3% (95% CI) on 12,400 production samples vs 89.1% baseline (p < 0.001).\""; lines: "Verifiable. Replicable. Falsifiable. Uncertainty bounded. Baseline contextualized. Dataset specified." and "Status: an empirical finding that can be challenged, reproduced, or refuted."
- **Caption (bottom center, italic 12px `#666`):** "Both use numbers. Both say \"accuracy.\" Only one is science. The difference is everything around the number."

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table, one `<tr>` per section (6 rows); left `<td>` (50%) holds `.obj-title` + bullets (+ bold closing paragraph where noted), right `<td>` (50%, centered) holds the canvas. Canvas element order/ids: c1, c2, c3, c4, c6, c5 (section 5 uses `c6`, section 6 uses `c5`).
- **Page style:** body system sans-serif, white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; `.subtitle` `#666` 1.0em; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em, weight 600, `#1a5276`; `strong` in `#1a5276`; `ul` 0.9em `#333`; canvas `max-width: 100%`. No nav bar, no back/home links.
- **Canvas:** intrinsic `width`/`height` attributes as given per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper returning `{ctx, w, h}`. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray text `#666`/`#333`.
- In regenerated HTML, any card/page links use `.html` extensions (this page has none).
