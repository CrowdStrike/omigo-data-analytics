# scikit-learn

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** scikit-learn

**Subtitle:** scikit-learn gives every classical ML model the same three-method interface — fit, predict, transform — so swapping a logistic regression for a random forest is a one-line change

## One Interface for Every Model

**Tags:** `core idea` (blue), `fit/predict` (green), `classical ML` (orange)

- **The table** — 1,000 customers of a phone plan: monthly spend, support tickets, tenure; 200 churned
- **The question** — given a new customer's row, will they cancel next month?
- **The contract** — `.fit(X, y)` learns from labeled rows; `.predict(X_new)` answers for new rows
- **The promise** — linear models, trees, SVMs, clustering: every estimator speaks this same interface
- **The third verb** — preprocessors add `.transform(X)`: scalers, encoders, imputers reshape columns

*Example (italic):* `model.fit(X_train, y_train)` then `model.predict(X_new)` — the same two calls work whether `model` is a logistic regression or a 500-tree forest.

**Key point:** scikit-learn's design bet is uniformity: because every estimator honors fit/predict/transform, the code around the model never has to change when the model does.

### Visualization (canvas `c1`, 720×300)

Flow diagram: three different estimator boxes all funneling into one shared fit/predict contract box, which emits churn predictions.

- **Title (bold 15px, `#1a5276`, top center):** "Three Very Different Models, One Identical Contract".
- **Left column (blue `#2a78d6` rounded boxes, 200px wide, 38px tall, 8px radius, fill `rgba(42,120,214,0.15)`, 12px `#2c3e50` text, x=30):** "LogisticRegression()" at y=70, "RandomForestClassifier()" at y=140, "GradientBoostingClassifier()" at y=210.
- **Arrows:** 3px `#6b7280` lines from each left box's right edge converging to the contract box's left edge.
- **Contract box (green `#008300` rounded box at x=300, y=125, 210px wide, 66px tall, fill `rgba(0,131,0,0.12)`):** two 13px lines ".fit(X, y)" and ".predict(X_new)".
- **Output box (blue rounded box at x=570, y=140, 130px wide, 38px tall):** "churn: 0 / 1"; 3px arrow from contract box to it.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=280):** "swap the model, keep every other line".

## Swapping Models on the Churn Table

**Tags:** `worked example` (blue), `one-line swap` (green)

- **The split** — `train_test_split` holds out 200 of the 1,000 customers as a test set
- **Line 1** — `model = LogisticRegression()`; lines 2 and 3 are `.fit(...)` and `.predict(...)`
- **Swap one** — change line 1 to `RandomForestClassifier()`; lines 2 and 3 stay byte-for-byte identical
- **Swap two** — change line 1 to `GradientBoostingClassifier()`; still nothing else moves
- **The scores** — test accuracy 0.79, 0.84, 0.86 for the three models (illustrative)

*Example (italic):* Trying all three models on the churn table is three edits to a single line — the fit and predict calls never change.

**Key point:** Model comparison in scikit-learn is editing one constructor call; the shared interface turns "rewrite the experiment" into "change one line and rerun".

### Visualization (canvas `c2`, 720×300)

Vertical bar chart: test accuracy of the three models on the same churn split, emphasizing that only line 1 differed.

- **Title (bold 15px, `#1a5276`, top center):** "Same Three Lines, Three Models: Churn Test Accuracy".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = accuracy 0.0 to 1.0, gridlines `#e5e9ef` at 0.25/0.50/0.75 with 12px `#444` tick labels.
- **Bars (110px wide, centered at x=180/360/540):** heights for accuracies `[0.79, 0.84, 0.86]`; fills blue `rgba(42,120,214,0.30)` with 2px `#2a78d6` border, aqua `rgba(25,158,112,0.30)` with 2px `#199e70` border, green `rgba(0,131,0,0.30)` with 2px `#008300` border.
- **Value labels (bold 13px, matching bar border color, atop each bar):** "0.79", "0.84", "0.86".
- **Bar labels (12px `#444` under baseline):** "LogisticRegression", "RandomForest", "GradientBoosting".
- **Annotation (bold 13px violet `#4a3aa7`, near x=180, y=70):** "only the constructor line changed".
- **Caption (12px `#444`, bottom right):** "accuracies illustrative".

## Pipelines and Honest Cross-Validation

**Tags:** `where it's used` (blue), `Pipeline` (green), `GridSearchCV` (orange)

- **The chain** — `Pipeline([('scale', StandardScaler()), ('model', LogisticRegression())])` is one estimator
- **The guarantee** — inside a Pipeline, transforms are fit on training folds only, never on test rows
- **One line, five scores** — `cross_val_score(pipe, X, y, cv=5)` refits the whole chain per fold
- **The search** — `GridSearchCV` tries parameter grids with the same fit/predict face as any model
- **The legacy** — this API standardized how classical ML code looks; other libraries now copy it

*Example (italic):* Five-fold `cross_val_score` on the gradient boosting model returns `[0.84, 0.87, 0.85, 0.88, 0.86]` — mean 0.86, matching its single-split score (illustrative).

**Key point:** Because a Pipeline is itself an estimator, cross-validation and grid search treat "preprocessing plus model" as one unit — every fold gets its own honestly-fit scaler.

### Visualization (canvas `c3`, 720×300)

Vertical bar chart: the five fold scores from cross_val_score with a dashed mean line, showing the spread one train/test split would hide.

- **Title (bold 15px, `#1a5276`, top center):** "cross_val_score: Five Folds, Five Honest Scores".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = accuracy 0.0 to 1.0, gridlines `#e5e9ef` at 0.25/0.50/0.75 with 12px `#444` tick labels.
- **Bars (80px wide, centered at x=150/265/380/495/610):** fold scores `[0.84, 0.87, 0.85, 0.88, 0.86]`; fill `rgba(42,120,214,0.30)`, 2px `#2a78d6` border; bold 12px `#2a78d6` value labels atop each bar; 12px `#444` labels "fold 1"–"fold 5" under the baseline.
- **Mean line:** dashed magenta `#d55181` (dash 5/4) horizontal line at accuracy 0.86, bold 12px magenta label "mean 0.86" at its right end.
- **Annotation (bold 13px green `#008300`, near x=150, y=75):** "one line refits the whole Pipeline per fold".
- **Caption (12px `#444`, bottom right):** "fold scores illustrative".

## Fitting the Scaler Before the Split

**Tags:** `common mistake` (red), `data leakage` (orange)

- **The shortcut** — `scaler.fit_transform(X)` on all 1,000 rows, then split into train and test
- **The leak** — the test rows' means and spreads shaped the scaling the model trained on
- **The symptom** — the leaky run reports 0.80 test accuracy; the honest one reports 0.79 (illustrative)
- **The sting** — the inflated score evaporates in production, where future rows shaped nothing
- **The fix** — put the scaler inside a Pipeline, so it is fit on the 800 training rows only

*Example (italic):* The leaky logistic regression looks 1 point better (0.80 vs 0.79) — a small gap, but it is the test set quietly grading its own exam.

**Common mistake:** Preprocessing on the full dataset before splitting. any statistic computed from test rows — a mean, a min-max range, an imputed value — leaks their information into training. Scaling leaks mildly; target-aware steps like imputation or feature selection can inflate scores far more.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: scaling before the split (leak, inflated score) vs scaling inside a Pipeline (honest score), shown as data boxes flowing into an evaluation box.

- **Title (bold 15px, `#1a5276`, top center):** "Scale Then Split vs Split Then Scale".
- **Row 1 (y=95), label 12px `#444` at x=20:** "scale, then split"; blue `#2a78d6` rounded box at x=160 labeled "scaler fit on all 1,000 rows" (12px), 3px arrow to a red `#e74c3c` box at x=430 labeled "test rows shaped the scaling" with bold 12px red "reports 0.80 — inflated".
- **Row 2 (y=205), label:** "Pipeline: split, then scale"; blue box at x=160 "scaler fit on 800 train rows", 3px arrow to a green `#008300` box at x=430 labeled "test rows never touched" with bold 12px green "reports 0.79 — honest".
- **Box style:** 190–210px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=270):** "even this 1-point gap is leakage, not skill — accuracies illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the churn table, all accuracies (0.79 / 0.84 / 0.86), the fold scores (0.84 / 0.87 / 0.85 / 0.88 / 0.86, mean 0.86), and the leaky-vs-honest pair (0.80 vs 0.79) are invented and labeled illustrative; the API facts (fit/predict/transform contract, Pipeline fitting transforms on training folds only, cross_val_score and GridSearchCV behavior) are publicly documented scikit-learn behavior.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
