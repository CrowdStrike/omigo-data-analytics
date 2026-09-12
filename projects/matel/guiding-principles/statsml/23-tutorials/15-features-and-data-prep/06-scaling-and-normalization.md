# Scaling & Normalization

**Page type:** detail page (tutorial layout: h2 card-sections, two-column table with text left 50% / canvas right 50%)
**HTML title tag:** Scaling & Normalization

**Subtitle:** Putting columns with wildly different ranges onto a common scale so no single one dominates

**Shared chart data (used across canvases):** customers A–E; ages `[25, 32, 40, 48, 60]`; incomes `[22000, 35000, 48000, 61000, 84000]`; age z-scores `[-1.31, -0.74, -0.08, 0.57, 1.55]`; income z-scores `[-1.31, -0.70, -0.09, 0.51, 1.59]`; age min-max `[0.00, 0.20, 0.43, 0.66, 1.00]`; income min-max `[0.00, 0.21, 0.42, 0.63, 1.00]`.

## Age and Income on One Shared Ruler

**Tags:** `core idea` (blue), `standardize` (orange)

- **The table** — five customers A–E, each with an age (25–60) and an income ($22,000–$84,000)
- **Two rulers** — age spans 35 units; income spans 62,000 units — a 1,700x wider column
- **Shared axis** — plot both on one 0–90,000 ruler and every age squishes into a single dot
- **Scaling** — rewrite each column in its own "typical spread" units so both get a voice
- **Same info** — scaling only relabels the ruler; customer order within a column never changes

*Example:* Customer E earns $84,000 and is 60 — on the raw ruler, the 60 is invisible next to the 84,000.

**Key point:** A model comparing raw numbers hears income shout and age whisper — scaling turns the volume knobs to equal.

### Visualization (canvas `c1`, 720×300)

Two parallel number-line rulers on the same 0–90,000 scale: incomes spread out, ages collapsed to one dot.

- **Title (bold 16px, `#1a5276`, top center):** "Five Customers, One Shared Ruler (0 to 90,000)".
- **Rulers:** horizontal lines from x=70 to w−40 with ticks and muted 12px labels at 0, 30,000, 60,000, 90,000 (locale-formatted).
- **Income ruler (y=110, label "income ($)" bold 13px above left):** five blue (`#2a78d6`) dots (radius 8) at the income values, each with a white bold 11px customer letter A–E inside.
- **Age ruler (y=210, label "age (years), same ruler"):** five orange (`#d95926`) dots at ages 25–60 on the 0–90,000 scale — all cluster at the far left. A dashed orange ellipse (22×18, dash 5/4) circles the cluster.
- **Annotations:** orange bold 13px "all five ages (25–60) collapse into one dot here" next to the ellipse; blue bold 13px "income spans the whole ruler — it decides everything" above the income ruler.
- **Caption (muted 12px, bottom center):** "illustrative customer table".

## Z-Scores and Min-Max for the Five Customers, by Hand

**Tags:** `worked example` (green), `z-score` (blue), `min-max` (blue)

- **Ages** — 25, 32, 40, 48, 60: mean 41, spread (standard deviation) 12.2
- **Incomes** — $22k, $35k, $48k, $61k, $84k: mean $50,000, spread $21,400
- **Z-score** — (value − mean) / spread; customer E's age: (60 − 41) / 12.2 = 1.55
- **Same for income** — E's income: (84,000 − 50,000) / 21,400 = 1.59 — now comparable to 1.55
- **Min-max** — (value − min) / (max − min) squeezes each column into 0 to 1 instead

*Example:* E sits 1.55 spreads above average age and 1.59 above average income — "high on both, by the same amount".

**Key point:** After scaling, "how unusual is this value in its own column?" becomes one shared language for every column.

### Visualization (canvas `c2`, 720×300)

Two scaled rulers: a z-score ruler (−2 to +2) and a min-max ruler (0 to 1), with age dots above and income dots below each line.

- **Title (bold 16px, `#1a5276`, top center):** "After Scaling: Both Columns Speak the Same Language".
- **Z-score ruler (y=105, label "z-scores: (value − mean) / spread" bold 13px above left):** ticks at −2, −1, 0, +1, +2 (signed labels). Age z-values `[-1.31, -0.74, -0.08, 0.57, 1.55]` as orange (`#d95926`) dots (radius 7) 13px above the line; income z-values `[-1.31, -0.70, -0.09, 0.51, 1.59]` as blue (`#2a78d6`) dots 13px below.
- **Annotation (green `#008300` bold 13px, near z≈0.9):** "E: age z = 1.55, income z = 1.59 — finally comparable".
- **Min-max ruler (y=225, label "min-max: (value − min) / (max − min)"):** ticks at 0.00, 0.25, 0.50, 0.75, 1.00. Age min-max `[0.00, 0.20, 0.43, 0.66, 1.00]` orange dots above; income min-max `[0.00, 0.21, 0.42, 0.63, 1.00]` blue dots below.
- **Legend (bottom left):** orange swatch "age"; blue swatch "income".

## Why the Income Column Silently Wins Every Distance

**Tags:** `what goes wrong` (red), `where it's used` (blue)

- **Distance models** — k-nearest-neighbors, k-means, and similarity search all add up squared gaps
- **A vs B** — age gap 7 gives 7² = 49; income gap $13,000 gives 13,000² = 169,000,000
- **The split** — raw distance is 99.99997% income; age contributes effectively nothing
- **After z-scoring** — gaps become 0.57 and 0.61; the split turns into 47% age, 53% income
- **Also affected** — gradient descent converges slower and regularization punishes unfairly unscaled

*Example:* An unscaled customer-segmentation run produced clusters that were just income bands — age never mattered.

**Key point:** Skipping scaling doesn't crash anything — the model quietly becomes a one-column model, and nobody gets an error message.

### Visualization (canvas `c3`, 720×300)

Two stacked horizontal share bars comparing who owns the squared distance, raw vs z-scored.

- **Title (bold 16px, `#1a5276`, top center):** "Who Decides the Distance Between Customers A and B?".
- **Raw bar (y=75, 46px tall, row label "raw columns" bold 13px at left):** income share 99.99997% in blue `#2a78d6` with white bold 13px label inside "income: 13,000² = 169,000,000  →  99.99997% of the distance"; age sliver (min 2px) in orange `#d95926`, annotated above in bold orange 12px "age: 7² = 49 (invisible sliver)".
- **Scaled bar (y=175, row label "z-scored columns"):** income 53% blue with white label "income 0.61² → 53%"; age 47% orange with white label "age 0.57² → 47%". Bars span from x=190 to w−60.
- **Annotations (centered):** green (`#008300`) bold 14px "scaling turns a one-column distance into a fair two-column vote" (y=258); muted 12px "A = (age 25, $22,000)   B = (age 32, $35,000)   —   squared-gap shares" (y=282).

## The One Thing People Get Wrong: Scaling the Test Data on Itself

**Tags:** `common mistake` (red), `rule of thumb` (green)

- **Fit on train only** — compute mean and spread from training rows, reuse them everywhere
- **The leak** — recomputing stats on test rows lets test data shape the transform it's judged on
- **Serving too** — production rows must be scaled with the saved training numbers, not fresh ones
- **Trees don't care** — random forests and boosted trees split on order, so scaling changes nothing
- **Not a skew fix** — z-scoring a lopsided column keeps it lopsided; that's the log's job

*Example:* A pipeline z-scored each daily batch on itself — the same customer got a different score every day.

**Key point:** The scaler is part of the model. Learn it once from training data, save its numbers, and apply them unchanged to everything that follows.

### Visualization (canvas `c4`, 720×300)

Flow diagram: correct fit-on-train path in green feeding three scale steps, plus a wrong recompute-on-test path crossed out in red.

- **Title (bold 16px, `#1a5276`, top center):** "Learn the Scaler Once, Apply It Everywhere".
- **Correct path boxes (outlined 2px, light fills, bold 13px titles with optional 12px second line):** "TRAINING ROWS" / "age, income" (ink `#1a5276` border, fill `#eef4f9`, 170×52 at 40,60) → arrow → "SAVED NUMBERS" / "mean 41, spread 12.2" (green `#008300` border, fill `#eef9f0`, 190×52 at 290,60) → three green arrows fanning to "scale TRAIN" (140×44 at 550,45), "scale TEST" (550,110), "scale SERVING" (550,175), all green-bordered.
- **Path caption (green bold 13px, centered at 385,145):** "one set of numbers, reused unchanged".
- **Wrong path:** box "TEST ROWS" / "recompute mean/spread" (orange `#d95926` border, fill `#fdf3ee`, 170×52 at 40,200) with an orange arrow labeled bold 13px "fresh stats per batch"; a thick red (`#e74c3c`, 4px) X crosses the arrow; red bold 14px label to the right: "LEAK: test data shapes its own transform".
- **Caption (muted 12px, bottom center):** "the scaler is part of the model — fit on train only".

## Regeneration instructions

- **Template:** tutorial topic page (tutorials/CLAUDE.md conventions). `<h1>` concept name, `.subtitle`, four `.card-section` blocks each `<h2>` + `table.layout` with one `<tr>`: `td.text-col` (50%) text, `td.viz-col` (50%) one 720×300 canvas.
- **Left column structure per section:** `.tags` pill row, `<ul>` of one-line bullets with `<b>` lead terms (colored `#1a5276`), italic `.example` line, `.key-point` callout (background `#f8f9fa`, left border `3px solid #1a5276` on this page, padding 8px 12px, 0.9rem) with bold "Key point:" lead-in.
- **Tag pill CSS:** inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; h2 1.3rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; canvas `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300 (this page's `setup(id)` helper hardcodes 720×300), scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object `P`:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- Shared data arrays (CUST, AGE, INC, AGE_Z, INC_Z, AGE_MM, INC_MM) are declared once and used by multiple charts.
- Card links in regenerated HTML (if referenced from grids) use `.html` extensions.
