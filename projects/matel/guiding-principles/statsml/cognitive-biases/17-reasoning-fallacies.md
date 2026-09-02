# Reasoning Fallacies

**Page type:** detail page (one `.card-section` per fallacy, each a two-column layout table: text left 50%, canvas right 50%)
**HTML title tag:** Reasoning Fallacies

**Subtitle:** Five systematic errors in probabilistic and causal reasoning that corrupt data analysis.

## 1. Gambler's Fallacy

The belief that past independent events change future probabilities. A fair coin has no memory, so nothing about a streak is "owed" back.

- **The setup:** five heads in a row — HHHHH
- **The wrong inference:** "tails is due, P(T) must be higher now"
- **The truth:** P(next H) = 0.50, exactly as before
- **The confusion:** sequence probability vs next-event probability
- **Sequence is rare:** P(HHHHH) = 1/32 before any flip
- **Conditional is not:** P(6th = H | first five H) = 1/2
- **What independence means:** the conditional equals the marginal

**Key point:** Under independence, conditioning on history changes nothing. A streak carries zero information about the next draw — the only thing a long streak should update is your belief that the coin is fair. *(key-point box)*

*Example: "The model was wrong 5 times in a row, so it's due for a correct prediction." A miscalibrated model stays miscalibrated regardless of streak length.*

### Visualization (canvas `c1`, 720×300)

Coin-flip sequence diagram on `#f9fafb` background.

- **Header (bold 14px `#555`, centered at x=200, y=30):** "Past flips (independent events):".
- **Coins:** five circles radius 28, 80px apart starting at x=68, y=80; gold fill `#f4d03f`, stroke `#d4ac0d` width 2, each with bold 20px `#1a5276` letter "H".
- **Arrow:** solid `#1a5276` line width 3 with arrowhead pointing right after the fifth coin, leading to a sixth dashed-outline coin (fill `#eaf2f8`, dashed (5/4) stroke `#2980b9` width 2.5) containing bold 22px `#2980b9` "?".
- **Correct answer (green `#27ae60`):** bold 16px "P(next H) = 50%" then 14px "Each flip is independent. The coin has no memory."
- **Wrong belief (red `#e74c3c`):** bold 16px "\"Tails is due! P(T) must be higher now!\"" with a red strikethrough line through it; below, 14px red "WRONG — confuses sequence probability with next-event probability."
- **Bottom note (12px `#555`):** "P(HHHHH) = 1/32 ← rare sequence. But P(6th = H | first 5 = H) = 1/2 ← unchanged."

## 2. Conjunction Fallacy

Adding conditions can never increase probability — P(A ∩ B) ≤ P(A), always. But a specific profile feels more likely than a general one, because detail buys plausibility rather than probability.

- **The rule:** P(A ∩ B) ≤ min(P(A), P(B)), with no exceptions
- **Why it misfires:** a vivid profile resembles the stereotype it describes
- **Representativeness:** resemblance is judged, probability is not
- **In segmentation:** every added filter multiplies the sample down
- **Filter cascade:** 30,000 → 15,000 → 1,500 → 330 → 231 → 5
- **Each step is plausible:** 50%, 10%, 22%, 70%, 2.2% conditional rates
- **The consequence:** an n=5 cell produces "significant" results from noise

**Key point:** Over-segmentation guarantees tiny, unstable cells, and a tiny cell will always contain some extreme rate. Pre-specify segments before looking, and require a minimum cell size for any claim. *(key-point box)*

*Illustrative Example: "Male, 45–50, income $80–100k, homeowner, bought in the last 30 days" sounds like a precise, targetable cohort — it is 5 people out of 30,000.*

### Visualization (canvas `c2`, 720×300)

Area-proportional nested rectangles (left) plus a filter list with n values (right), on `#f9fafb` background.

- **Counts:** `[30000, 15000, 1500, 330, 231, 5]` for filters `Population`, `+ Male`, `+ Age 45–50`, `+ Income $80–100k`, `+ Homeowner`, `+ Bought last 30 days`. Each count is derived in JS from the previous one via the conditional rates `[0.50, 0.10, 0.22, 0.70, 0.022]` and rounded, so the chart, its labels, and the prose cannot drift.
- **Nested rectangles:** each rectangle's **area is proportional to n** — linear scale = `sqrt(n_i / n_0)` applied to a base 420×250 box, computed at render time, giving widths 420, 297, 94, 44, 37 and heights 250, 177, 56, 26, 22. All boxes share the center of the base box at (230, 150). Fills progressively darker blue: `rgba(26,82,118,0.08)`, `0.12`, `0.18`, `0.25`, `0.35`; borders `#1a5276` width 1.5. The final level (n = 5) would be ~5×3px, so it is drawn instead as a red dot radius 4 (`#e74c3c`) at the shared center with a bold 11px red "n = 5" label beside it.
- **Right list (x=470):** heading bold 13px `#1a5276` "Each filter shrinks n:", then rows 30px apart starting at y=52 (13px, ↓ arrows between): "Population n = 30,000" / "+ Male n = 15,000" / "+ Age 45–50 n = 1,500" / "+ Income $80–100k n = 330" / "+ Homeowner n = 231" / "+ Bought last 30 days n = 5" (last row bold red `#e74c3c`); n values right-aligned at x=700, bold 13px `#1a5276` (last red).
- **Bottom notes (centered):** bold 12px red "P(A∩B∩C∩D∩E) ≤ P(A) — specificity guarantees rarity" at y=272; 10px `#999` "box area ∝ n (to scale)" at y=289.

## 3. McNamara Fallacy

Deciding with only the quantities that were easy to collect, then treating the rest as if it did not exist. Named for the Vietnam-era use of body counts as a proxy for winning.

- **Step 1:** measure whatever is easy to measure
- **Step 2:** disregard what cannot be measured yet
- **Step 3:** presume the unmeasured is unimportant
- **Step 4:** declare the unmeasurable to be nonexistent
- **The historical gap:** the real objective was territorial and political control
- **Easy proxies:** handle time, lines of code, accounts opened, test coverage
- **Hard objectives:** satisfaction, maintainability, trust, reliability

**Key point:** Cheap to collect is not the same as valid as a proxy. When a hard-to-measure objective is dropped from the scorecard, optimization pressure moves entirely onto the easy metric that replaced it. *(key-point box)*

*Example: Optimizing accuracy on a balanced test set while fairness, latency, and user trust go unmeasured — and therefore unmanaged.*

### Visualization (canvas `c3`, 720×300)

Two-column comparison of boxed items on `#f9fafb` background, with a "vs" divider between columns.

- **Left column (orange, header "Easy to Measure"** bold 15px `#e67e22` with 3px orange underline bar): five solid-bordered boxes 240×32 (fill `rgba(230,126,34,0.12)`, stroke `#e67e22` width 1.5), 14px `#2c3e50` centered text: "Body count", "Handle time (sec)", "Lines of code", "Accounts opened", "Test coverage %".
- **Right column (green, header "Hard but Matters"** bold 15px `#27ae60` with green underline bar): five dashed-bordered (4/3) boxes (fill `rgba(39,174,96,0.08)`, stroke `#27ae60`): "Territory control", "Customer satisfaction", "Code quality / maintainability", "Genuine customer trust", "System reliability".
- **Divider:** bold 18px `#999` "vs" centered between columns.
- **Bottom captions (11px):** red under left column "← optimized (but irrelevant)"; green under right column "ignored (but critical) →".

## 4. Goodhart's Law

When a measure becomes a target, it ceases to be a good measure. Optimization finds the cheapest route to the number, and the cheapest route usually bypasses the goal.

- **The law:** a measure under target pressure stops measuring
- **Mechanism:** the easiest way to move a number is rarely the intended way
- **Gaming is rational:** people are paid on the target, not the goal
- **Divergence:** the metric climbs while the underlying value decays
- **Retail bank case:** "accounts opened" as the KPI → unwanted accounts opened
- **Recommenders:** optimize clicks → clickbait; optimize engagement → outrage
- **The tell:** the metric improves and no downstream outcome improves with it

**Key point:** Any metric under optimization pressure needs a paired guardrail that gaming would visibly damage, plus an occasional holdout that measures the goal directly rather than the proxy. *(key-point box)*

*Illustrative Example: Click-through rate rises 18% after a headline change while completed reads per session fall 9% — the metric moved, the value did not.*

### Visualization (canvas `c4`, 720×300)

Two-line divergence chart over time on `#f9fafb` background.

- **Title (bold 14px `#1a5276`, top center):** "Metric vs Underlying Value Over Time".
- **Axes:** L-shaped `#ccc` axes; plot area padded 80 left, 40 right, 50 top, 60 bottom; rotated y label "Performance" and x label "Time →" (12px `#555`).
- **Target marker:** dashed (4/4) gray `#999` vertical line at 25% of x-range, labeled "Metric becomes target" (11px `#999`) below the axis.
- **Metric line (solid orange `#e67e22`, width 3):** rises gently until t=0.25, then rises steeply afterwards (metric goes UP).
- **Underlying value line (dashed 6/4 green `#27ae60`, width 3):** rises gently until t=0.25, then falls steadily afterwards (value goes DOWN).
- **Legend (at 60% x, near top):** orange solid line + "Reported metric (accounts opened)"; green dashed line + "Underlying value (customer trust)" (12px).
- **Annotation:** bold 12px red "DIVERGENCE" at ~72% x, mid-chart, with short red vertical arrows above and below indicating the widening gap.

## 5. Lead Time Bias

Detecting a disease earlier increases measured "survival time from diagnosis" even when the patient dies on exactly the same date. The clock simply started earlier.

- **The illusion:** earlier detection lengthens measured survival, not life
- **Same endpoint:** both patients die at age 70
- **Screen-detected:** diagnosed at 50 → 20-year "survival"
- **Symptom-detected:** diagnosed at 65 → 5-year "survival"
- **The difference:** 15 years of lead time, not 15 years of extra life
- **What actually changed:** the duration of being a diagnosed patient
- **The honest metric:** mortality rate in the screened population

**Key point:** Survival-from-diagnosis is confounded by when the clock starts, so it cannot be compared across groups detected at different stages. Only all-cause mortality over a fixed window is immune. *(key-point box)*

*Example: Comparing "time from signup to churn" between early-flagged and late-flagged users shows the same illusion — earlier flagging inflates apparent tenure.*

### Visualization (canvas `c5`, 720×300)

Dual-timeline comparison over an age axis on `#f9fafb` background.

- **Title (bold 14px `#1a5276`, top center):** "Same Death Date, Different \"Survival\" Due to Earlier Detection".
- **Age axis:** horizontal `#999` line at y=260 spanning padded width (100 left, 40 right); ticks and 12px `#555` labels at ages 40, 50, 55, 60, 65, 70, 75; "Age →" label at right end. Linear scale age 40–75.
- **Death line:** dashed (4/3) red `#e74c3c` vertical line at age 70, labeled bold 12px red "Death: age 70" at top.
- **Bar A (Screen-detected):** rectangle from age 50 to age 70 at y=80, height 30; fill `rgba(26,82,118,0.25)`, stroke `#1a5276` width 2; blue `#2980b9` detection dot radius 5 at left edge; left label bold 13px `#1a5276` "Screen-detected" / 12px "(age 50)"; centered bold 13px annotation "20 yr \"survival\"".
- **Bar B (Symptom-detected):** rectangle from age 65 to age 70 at y=155, height 30; fill `rgba(231,76,60,0.15)`, stroke `#e74c3c` width 2; red detection dot; left label bold 13px `#e74c3c` "Symptom-detected" / 12px "(age 65)"; centered bold 13px red annotation "5 yr \"survival\"".
- **Lead-time bracket:** orange `#e67e22` bracket between age 50 and age 65 at y=125, labeled bold 12px orange "Lead time (15 yr) — NOT extra life".
- **Bottom note (12px `#555`, centered):** "Both patients die at age 70. Earlier detection only moves the diagnosis clock, not the outcome."

## Regeneration instructions

- **Layout:** single detail page; h1 with 2px `#2980b9` bottom border, `.subtitle`, then five `.card-section` blocks (40px bottom margin). Each section: `<h2>` numbered title with 2px `#2980b9` bottom border, then a `table.layout` (border-collapse, one `<tr>`) with `.text-col` td (50%) holding a short lead paragraph + labeled-bullet `<ul>` + `.key-point` box + italic `.example` paragraph, and `.viz-col` td (50%) holding the canvas.
- **Bullets:** each `<li>` is `<strong>` label + a short phrase that fits on one line at normal page width. No wrapping bullets; split into more bullets instead of lengthening one.
- **Boxes:** `.key-point` — background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem, `strong` label inside. `.example` — italic, `#555`, 0.9rem, no box.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px 24px, line-height 1.6; h1 2rem `#1a5276`; h2 1.3rem `#1a5276`; subtitle `#666` 0.95rem; lists 0.92rem. Canvases `width: 100%`, `1px solid #e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300 each; scale by `window.devicePixelRatio` (backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper returning `{ctx, w, h}`. Draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, accent blue `#2980b9`, coin gold `#f4d03f`/`#d4ac0d`, grays `#555`/`#999`, canvas background `#f9fafb`.
- In regenerated HTML, any card links use `.html` extensions (this page has no outbound links).
