# XGBoost & LightGBM

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** XGBoost & LightGBM

**Subtitle:** Gradient boosting builds a team of small trees where each new tree trains on the errors of the trees before it — for a decade the default winner on tabular data

## A Team of Trees That Fix Each Other's Mistakes

**Tags:** `core idea` (blue), `boosting` (green), `residuals` (orange)

- **The task** — predict sale prices for houses from size, age, and neighborhood
- **The start** — the first guess is just the mean price: $300k for every house
- **The residual** — house C actually sold for $190k, so the guess is off by −$110k
- **The next tree** — tree 1 trains on those errors themselves, not on the original prices
- **The step** — add half of tree 1's correction: house C's guess moves from $300k to $245k
- **The loop** — repeat; every new tree fits whatever error the whole team still makes

*Example (italic):* After five trees, house C's prediction has walked from $300k down to $193.4k against a true price of $190k (illustrative).

**Key point:** Gradient boosting builds trees one after another; each tree's training target is the current ensemble's leftover error, so the team improves exactly where it is still worst.

### Visualization (canvas `c1`, 720×300)

Line chart of one house's predicted price walking toward its true price, one boosting round at a time — each step closes half the remaining gap.

- **Title (bold 15px, `#1a5276`, top center):** "House C: Each New Tree Closes Half the Remaining Gap".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = boosting round 0 to 5, 12px `#444` tick labels "round 0"–"round 5"; y = predicted price $180k to $310k, gridlines `#e5e9ef` at 200/240/280, 12px `#444` labels "$200k"/"$240k"/"$280k".
- **Prediction line:** blue `#2a78d6` 3px line with 5px dots through rounds `[0, 1, 2, 3, 4, 5]`, prices `[300, 245, 217.5, 203.8, 196.9, 193.4]` ($k); 12px `#2a78d6` value labels "$300k" above the first dot and "$193.4k" above the last.
- **Truth line:** green `#008300` dashed (dash 6/4) 2px horizontal line at price 190, 12px green label "true price $190k" at its right end.
- **Annotation (bold 13px violet `#4a3aa7`, near round 2.5, y=90):** "tree 2 trains on what tree 1 left behind".
- **Caption (12px `#444`, bottom right):** "prices illustrative; halving exact for learning rate 0.5".

## Five Houses, Three Rounds: Watching the Error Halve

**Tags:** `worked example` (blue), `learning rate` (green)

- **The data** — five sold houses: $250k, $310k, $190k, $420k, $330k; the mean is $300k
- **Round 0** — predict $300k for all; residuals −50, +10, −110, +120, +30 ($k); MAE $64k
- **Tree 1** — fits those five residuals; add half its output, so every residual halves
- **Round 1** — residuals become −25, +5, −55, +60, +15 ($k); MAE drops to $32k
- **Round 2** — halve again: −12.5, +2.5, −27.5, +30, +7.5; MAE $16k, then $8k after round 3
- **Hand-check** — house D ($420k): guesses go 300 → 360 → 390 → 405, always half the gap

*Example (italic):* Three rounds cut the mean absolute error from $64k to $8k — with a 0.5 learning rate, each round halves whatever error is left.

**Key point:** The learning rate scales each tree's correction; a smaller rate takes smaller, safer steps and needs more trees — XGBoost and LightGBM both ship it as the key dial.

### Visualization (canvas `c2`, 720×300)

Bar chart of mean absolute error by boosting round for the five-house example: 64, 32, 16, 8, 4 — a clean geometric collapse.

- **Title (bold 15px, `#1a5276`, top center):** "Five Houses: Mean Absolute Error Halves Every Round".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = MAE $0k to $70k, gridlines `#e5e9ef` at 20/40/60 with 12px `#444` labels "$20k"/"$40k"/"$60k".
- **Bars:** five bars at rounds `[0, 1, 2, 3, 4]`, MAE values `[64, 32, 16, 8, 4]` ($k); width 70px, centers evenly spaced from x=130 to x=590; fill `rgba(42,120,214,0.35)`, 2px `#2a78d6` border; bold 13px `#1a5276` value labels "$64k"…"$4k" above each bar; 12px `#444` tick labels "round 0"–"round 4" below the baseline.
- **Annotation (bold 13px green `#008300`, near x=420, y=95):** "each tree fixes half of what remains".
- **Caption (12px `#444`, bottom right):** "house prices illustrative; the halving is exact arithmetic for this setup".

## Why Boosted Trees Owned Tabular Data for a Decade

**Tags:** `where it's used` (blue), `XGBoost 2014` (green), `LightGBM` (orange)

- **XGBoost (2014)** — added regularization and second-order gradients; became the Kaggle-winning workhorse
- **LightGBM (Microsoft)** — bins features into histograms and grows trees leaf-wise, much faster on large data
- **Mixed types** — trees split on raw columns directly; no feature scaling or normalization needed
- **Missing values** — XGBoost learns a default direction for missing values at every split
- **Modest data** — strong with thousands of rows, where deep nets typically want far more

*Example (italic):* On a typical 20-column customer table with numbers, categories, and holes, a boosted tree trains on the raw frame while a neural net first needs encoding, imputation, and scaling.

**Key point:** Boosted trees stay the tabular default because they eat messy real-world tables as-is — mixed types, missing cells, unscaled columns, modest row counts — and still fit accurately.

### Visualization (canvas `c3`, 720×300)

Two-column readiness diagram: four things tabular data throws at a model, and how boosted trees vs neural nets cope with each.

- **Title (bold 15px, `#1a5276`, top center):** "What a Real Table Throws at You — Trees vs Nets".
- **Column headers (bold 13px `#2c3e50`, y=55):** "boosted trees" centered at x=330, "neural net" centered at x=575.
- **Rows (y = 90, 140, 190, 240), each with a left-aligned bold 12px `#444` hurdle label at x=20:** "mixed types", "missing values", "unscaled columns", "10k rows".
- **Tree column boxes (centered x=330, 190px wide, 34px tall, 8px radius, fill `rgba(0,131,0,0.12)`, 2px `#008300` border, 12px `#2c3e50` text):** "✓ splits raw columns", "✓ default split direction", "✓ splits ignore scale", "✓ fits well".
- **Net column boxes (centered x=575, 190px wide, 34px tall, 8px radius, fill `rgba(217,89,38,0.12)`, 2px `#d95926` border, 12px `#2c3e50` text):** "needs encoding", "needs imputation", "needs normalization", "wants far more data".
- **Annotation (bold 13px magenta `#d55181`, centered near y=278):** "the preprocessing the net needs is where tabular projects stall".

## More Trees Is Not Always Better

**Tags:** `common mistake` (red), `early stopping` (orange)

- **The dial** — the number of boosting rounds; every extra tree keeps cutting training error
- **The trap** — training error falls forever, but validation error bottoms out and turns back up
- **The memorizer** — late trees fit noise: they learn which houses, not which features
- **Early stopping** — hold out data; stop when validation error hasn't improved for ~50 rounds
- **The pair** — a lower learning rate needs more rounds; tune the two together, never separately

*Example (italic):* Train MAE glides from $64k to $3k over 500 rounds, but validation MAE bottoms at $25k near round 200 and climbs back to $34k (illustrative).

**Common mistake:** Reading falling training error as progress. Past the validation minimum, every new tree is memorizing the training set — both libraries ship early stopping precisely because "more rounds" quietly becomes "more overfitting".

### Visualization (canvas `c4`, 720×300)

Two-line chart of train vs validation error over 500 boosting rounds: train falls monotonically, validation dips then rises; a dashed marker at the early-stopping point.

- **Title (bold 15px, `#1a5276`, top center):** "Train Error Falls Forever — Validation Error Turns Back Up".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = boosting rounds 0 to 500, 12px `#444` tick labels every 100 rounds; y = MAE $0k to $70k, gridlines `#e5e9ef` at 20/40/60.
- **Train line:** blue `#2a78d6` 3px line through rounds `[0, 25, 50, 100, 150, 200, 250, 300, 400, 500]`, MAE `[64, 42, 30, 22, 17, 13, 10, 8, 5, 3]` ($k), 12px blue label "train" at its right end.
- **Validation line:** red `#e74c3c` 3px line through the same round grid, MAE `[64, 46, 36, 29, 26, 25, 25.5, 27, 30, 34]` ($k), 12px red label "validation" at its right end.
- **Stop marker:** vertical dashed `#6b7280` (dash 4/3) line at round 200, bold 12px green `#008300` label "early stop here — valid MAE $25k" near its top.
- **Annotation (bold 13px red `#e74c3c`, near round 380, y=120):** "300 extra trees make it worse".
- **Caption (12px `#444`, bottom right):** "error curves illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); house prices, residuals, and error curves are invented and labeled illustrative; the residual-halving sequence (64 / 32 / 16 / 8 / 4) is exact arithmetic given the 0.5 learning rate and a tree that fits residuals perfectly; library facts (XGBoost 2014 regularization and second-order gradients, LightGBM histogram binning and leaf-wise growth, XGBoost's learned default direction for missing values) are publicly documented.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
