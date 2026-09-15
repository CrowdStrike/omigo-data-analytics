# Classification vs Regression

**Page type:** detail page (tutorial: 4 card-sections; each two-column table.layout 50/50 — text left, canvas right)
**HTML title tag:** Classification vs Regression

**Subtitle:** Predicting WHETHER a loan defaults gives a category; predicting HOW MUCH is lost gives a number — different outputs, different losses, different metrics

## One Loan Book, Two Predictions

Tags: `core idea` (blue), `running example` (green)

- **The book** — 10 loans of $10,000 each; three of them went bad
- **Question 1** — WHICH loans default? The answer is a category: yes or no
- **Question 2** — HOW MUCH is lost? The answer is a number: $2,000, $5,000, $8,000
- **Classification** — predicting a category (yes/no, spam/normal, cat/dog)
- **Regression** — predicting a number (dollars, days, degrees)

*Example (italic):* Same loans, two models: one flags risky borrowers, the other budgets the losses.

**Key point:** Look at the output. A category means classification; a number means regression — everything else follows from that.

### Visualization (canvas `c1`, 720×300)

Split panel: category tiles on the left, dollar bars on the right.

- **Title (bold 15px, `#1a5276`, top center):** "Same 10 Loans — a Category Question and a Number Question"
- **Divider:** vertical dashed gray line (`#bdc3c7`, dash 4/3) at x=320.
- **Left panel** (header bold 13px violet `#4a3aa7`: "WHICH loans default? (yes / no)"): 10 tiles 42×42 in 2 rows of 5, labeled #1–#10; defaulted loans #2, #5, #9 filled `rgba(217,89,38,0.75)` with orange `#d95926` border and white text; the rest filled `rgba(42,120,214,0.30)` with blue `#2a78d6` border and ink text. Captions: bold 12px orange "3 defaults: #2, #5, #9"; 12px `#444` "output = a category per loan".
- **Right panel** (header bold 13px aqua `#199e70`: "HOW MUCH is lost? (dollars)"): 10 bars (24px wide, baseline y=226, chart height 140, scale max $9,000) for losses `[0, 2000, 0, 0, 5000, 0, 0, 0, 8000, 0]` per loan #1–#10; non-zero bars in aqua with bold value labels "$2k"/"$5k"/"$8k"; zero-loss loans drawn as thin empty outline stubs; loan numbers below. Caption (12px `#444`): "loan number — output = a dollar amount per loan".
- **Bottom caption (bold 13px magenta `#d55181`, centered, y=282):** "same data, different output type — that one difference changes the whole toolbox"

## Scoring Both Models by Hand

Tags: `worked example` (green)

- **Actual defaults** — loans #2, #5, #9 defaulted; the other seven repaid
- **Classifier says** — flags #2, #5, #7: two hits, one false alarm (#7), one miss (#9)
- **Classifier score** — 8 of 10 loans called correctly: 80% accuracy
- **Regressor says** — $3,000, $4,000, $6,000 vs actual $2,000, $5,000, $8,000
- **Regressor score** — misses of $1,000, $1,000, $2,000: average miss $1,333

*Example (italic):* Every number here checks out with pencil arithmetic — that is the whole point.

**Key point:** Classification is graded on right vs wrong calls; regression is graded on how FAR OFF the numbers are.

This section's viz cell holds both canvases side by side in a `.viz-pair` flex row.

### Visualization (canvas `c2a`, 310×340)

2×2 confusion-count grid for the classifier.

- **Title (bold 15px, `#1a5276`, top center):** "Classifier: right vs wrong calls"
- **Grid:** 2×2 cells 104px square at (51, 70); column headers "predicted: yes" / "predicted: no" (bold 12px `#444`); rotated row headers "actual: yes" / "actual: no" on the left.
- **Cells:** top-left "2 hits" / "#2, #5" green `#008300` on `rgba(0,131,0,0.14)`; top-right "1 miss" / "#9" orange `#d95926` on `rgba(217,89,38,0.16)`; bottom-left "1 false alarm" / "#7" orange; bottom-right "6 correct" / "the rest" green. Cell main text bold 13px in cell color, sub text 12px `#555`.
- **Summary (bold 14px green, centered):** "accuracy: 8 of 10 = 80%"
- **Caption (12px `#444`, bottom center):** "every loan is simply right or wrong — no sizes"

### Visualization (canvas `c2b`, 310×340)

Grouped bar chart: predicted vs actual loss for the three defaulted loans.

- **Title (bold 15px, `#1a5276`, top center):** "Regressor: how far off in $"
- **Data:** loans #2, #5, #9; actual `[2000, 5000, 8000]` in aqua `#199e70`; predicted `[3000, 4000, 6000]` in translucent violet `rgba(74,58,167,0.55)`. Scale max $9,000; padding top 50 / bottom 70 / left 46 / right 12; bars 30px wide, actual left of predicted in each group.
- **Labels:** bold 12px value labels ("$2k" style) above each bar in the bar's color; bold 12px "loan #N" below the baseline; under each group a bold 12px magenta `#d55181` miss label: "off $1k", "off $1k", "off $2k".
- **Legend (top left):** aqua swatch "actual loss"; violet swatch "predicted".
- **Bottom caption (bold 13px magenta, centered):** "average miss (MAE) = $1,333"

## Different Outputs, Different Losses, Different Metrics

Tags: `why it matters` (blue), `rule of thumb` (blue)

- **Cost of a class error** — a missed default and a false alarm are wrong in different ways
- **Cost of a number error** — $500 off is bad, $3,000 off is worse: errors have sizes
- **Class metrics** — accuracy, precision, recall: counts of right and wrong calls
- **Number metrics** — average miss (MAE) or squared miss (RMSE): distances, not counts
- **Mismatch danger** — grade a regressor with accuracy and every prediction is just "wrong"

*Example (italic):* A classifier cannot say the $8,000 miss hurts more than the $2,000 one — regression can.

**Key point:** The output type picks the loss, and the loss picks the metric — never mix the two families.

### Visualization (canvas `c3`, 720×300)

Two horizontal chip-and-arrow flow rows mapping output type → loss → metric.

- **Title (bold 15px, `#1a5276`, top center):** "The Output Type Picks the Loss, the Loss Picks the Metric"
- **Classification row** (violet `#4a3aa7`, three 190×56 chips at y=62 linked by violet arrows): "category output" / "default: yes / no" → "loss: right or wrong" / "a call has no size" → "accuracy, precision," / "recall — counts of calls".
- **Regression row** (aqua `#199e70`, chips at y=172): "number output" / "loss in dollars" → "loss: distance off" / "$500 off beats $3,000 off" → "MAE, RMSE —" / "average distance off".
- Chips have faint fill `rgba(0,0,0,0.02)`, 2px colored border, bold 13px colored first line, 12px `#333` second line.
- **Bottom caption (bold 13px orange `#d95926`, centered, y=272):** "pick the row before picking the metric — accuracy on a dollar prediction is meaningless"

## Two Naming Traps

Tags: `common mistake` (red)

- **Trap 1** — "logistic regression" outputs a default probability plus a cutoff: it is a classifier
- **Trap 2** — binning losses into "small vs large" turns a number into a category — and loses detail
- **What binning costs** — $5,000 and $8,000 land in one "large" bucket though $3,000 apart
- **Probability is not a class** — a 0.7 default risk becomes "yes" only after you pick a cutoff

*Example (italic):* Cut at $4,000: the $2,000 loss is "small"; the $5,000 and $8,000 losses are both just "large".

**Common mistake (key-point callout):** Trusting names and buckets — always ask what the raw output is: a category or a number.

### Visualization (canvas `c4`, 720×300)

Number line with shaded bins showing information lost by binning.

- **Title (bold 15px, `#1a5276`, top center):** "Binning Losses at $4,000: What the \"Large\" Bucket Hides"
- **Bins:** shaded regions over y=70–200 — $0–$4,000 in `rgba(42,120,214,0.10)` labeled bold blue `#2a78d6` "bin: \"small\""; $4,000–$9,000 in `rgba(217,89,38,0.10)` labeled bold orange `#d95926` "bin: \"large\"".
- **Number line:** horizontal gray line at y=150 spanning $0–$9,000 (left pad 70, right pad 60), tick labels every $3k ("$0k", "$3k", "$6k", "$9k") in 12px `#666`.
- **Cutoff:** vertical dashed magenta line (`#d55181`, dash 6/4, width 2) at $4,000, labeled bold 12px "cutoff: $4,000".
- **Points:** 8px dots at $2,000 (blue), $5,000 and $8,000 (orange) with bold value labels above ("$2k", "$5k", "$8k").
- **Brace:** orange bracket under $5k–$8k with bold 13px orange text: "both just \"large\" — $3,000 of information gone".
- **Bottom caption (12px `#444`, centered, y=278):** "keep the number when the number is what you need; bin only when the decision is truly yes/no"

## Regeneration instructions

- **Template:** tutorials topic-page skeleton (most-powerful-signals compact style). Each `.card-section` has an `<h2>` (1.3rem `#1a5276`, 2px bottom border `#2980b9`) and a `table.layout`; every row uses `.text-col` (50%) / `.viz-col` (50%). One section places canvases `c2a`/`c2b` (310×340 each) side by side inside its single viz cell, wrapped in a `.viz-pair` flex row (`display:flex; gap:10px`, each canvas `flex:1 1 0; min-width:0`).
- **Left column per section:** `.tags` pill row first (0.72rem bold, 10px radius pills — blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`), then a `<ul>` of one-line bullets each opening with `<b>` term in `#1a5276`, then an italic `.example` line (`#555`, 0.9rem), then a `.key-point` callout (background `#f8f9fa`, left border 3px `#e74c3c`, 0.9rem).
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px bottom border `#2980b9`; `.subtitle` `#666` 0.95rem. No nav bar, no back/home links.
- **Canvas:** intrinsic sizes as given per chart (720×300, 310×340), CSS `width:100%`, 1px border `#e0e0e0` radius 4px; scaled via `window.devicePixelRatio` in a shared `setup(id)` helper reading width/height attributes (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange.
- **Data:** shared literal arrays — DEFAULTED `[2, 5, 9]` and LOSSES `[0, 2000, 0, 0, 5000, 0, 0, 0, 8000, 0]` for loans #1–#10; predicted `[3000, 4000, 6000]`; no `Math.random()`.
- In regenerated HTML, any card links use `.html` extensions.
