# Pitfall: Scaling Before Splitting

**Page type:** detail page (sectioned card layout: per section an h2, then a two-column table — text left ~45% with tag pills/bullets/example/key-point, canvas right ~55%)
**HTML title tag:** Scaling Before Splitting

**Subtitle:** Normalizing with global mean/std → test statistics leak into train

## The Problem

Tags: `the trap` (red), `leakage` (blue)

- **Statistic leakage** — a scaler fit on the full dataset computes mean and std over test rows too
- **Shifted training** — train features get standardized toward the test distribution before fitting
- **Broken evaluation** — the test score no longer simulates unseen data, though no row was shared
- **Subtle inflation** — the boost is often just a point or two, enough to flip model comparisons
- **Broader pattern** — PCA, feature selection, and binning fitted on full data leak the same way

*Example:* A feature spans 0-100 in train but 0-150 in test, and a globally fitted scaler quietly adapts to the 150.

**Impact:** Reported test performance is quietly inflated, so model comparisons and go/no-go decisions rest on scores the model will not reproduce on genuinely unseen data.

### Visualization (canvas `c1`, 720×300)

Two-path flow diagram: wrong (scale before split) vs right (split before scale).

- **Title (bold 14px, `#1a5276`, centered):** "Scaling Before vs After Split".
- **WRONG path (top, y=65):** bold red (`#e74c3c`) left label "WRONG". Rounded boxes (100×30, white bold 11px labels) at x = 80, 210, 340, 470: "Full Data" (blue `#1a5276`), then "Scale", "Split", "Train" (all red), connected by red arrows. Bold red "✖" mark after the last box.
- **Leak annotations:** dashed orange (`#e67e22`) tick below the Scale box with italic orange 10px text "test mean/std leak into scaler"; dashed orange line from the Split box down to an orange note box (320×28, fill `rgba(230,126,34,0.1)`, orange border) reading "Test statistics (mean, std) leak into scaler fitted on full data".
- **RIGHT path (bottom, y=165):** bold green (`#27ae60`) left label "RIGHT". Rounded boxes: "Full Data" (blue, 100 wide), "Split" (green, 100), "Scale(train)" (green, 110), "Apply(test)" (green, 110) at x = 80, 210, 360, 530, connected by green arrows. Bold green "✔" after the last box.
- **Divider:** dashed `#ccc` horizontal line between the two paths.
- **Bottom note (blue 11px, centered):** "Fit scaler on train only. Transform test using train-fitted parameters."

## Why It Happens

Tags: `root cause` (orange), `convenience` (blue)

- **Least resistance** — one scaler call on the whole dataframe is shorter code that runs cleanly
- **Bad examples** — many tutorials demonstrate scale-then-split without flagging it as leakage
- **Invisible symptom** — a one- or two-point inflation looks normal, so nobody investigates
- **Nothing breaks** — the code runs without error, so the bias never raises suspicion
- **Blind spot** — teams guard the model from test data but forget fitted transforms learn too

*Example:* The feature "income" averages $50k in train and $65k in test, so a global scaler uses $55k and buys roughly 1.5% unearned accuracy.

**Root Cause:** StandardScaler.fit on the full data computes mean and std over test rows too, so the scaling parameters the model trains on already encode the test distribution.

### Visualization (canvas `c2`, 720×300)

Flow of test statistics into contaminated scaler parameters.

- **Title (bold 14px, `#1a5276`, centered):** "How Test Info Leaks Into Scaler Parameters".
- **Top flow (rounded boxes, height 32, white bold labels, red arrows):** "Full Data (N=1000)" (blue, x=30 w=120) → "fit(mean, std)" (red, x=185 w=150) → "Includes test rows!" (red, x=370 w=180, wrapped in a dashed red highlight rectangle).
- **Section label (bold blue 12px, left):** "Scaler Parameters (Contaminated):".
- **Three stat boxes (180×50 each):** "Train Mean" — green border, fill `rgba(39,174,96,0.1)`, value "$50k" bold 20px green, at x=30. "Test Mean" — red border, fill `rgba(231,76,60,0.1)`, value "$65k" bold red, at x=230. "Global Mean (LEAKED)" — orange border 2.5px, fill `rgba(230,126,34,0.15)`, 220 wide, value "$55k" bold orange, at x=430. Gray arrow train→test, red arrow test→global.
- **Contamination label (bold red 11px, centered):** "Test info contaminates global mean".
- **Explanation lines (`#555` 11px, centered):** "Training data scaled with mean=$55k instead of correct mean=$50k" and "Model implicitly \"knows\" test distribution shifted upward".
- **Bottom warning (bold red 12px, centered):** "Result: 1-2% artificial accuracy inflation — subtle but real leakage"

## The Correct Approach

Tags: `the fix` (green), `pipeline` (blue)

- **Split first** — separate train and test before any fitted preprocessing touches the data
- **Train-only fit** — fit the scaler on training data, then transform test with train mean and std
- **Pipeline guarantee** — sklearn Pipeline makes fitting on test data impossible by construction
- **CV safety** — cross-validating the Pipeline re-fits the scaler on each training fold alone
- **Production match** — save the fitted scaler with the model so serving uses the same statistics

*Example:* Split first, then scaler.fit(X_train), then scaler.transform on both train and test — no test information leaks.

**Fix:** Wrap the scaler and model in an sklearn Pipeline — it fits on train only, transforms test with train parameters, and cross-validates without leakage by construction.

### Visualization (canvas `c3`, 720×300)

Correct pipeline flow with branch diagram, code snippet box, and per-split flows.

- **Title (bold 14px, `#1a5276`, centered):** "Correct: sklearn Pipeline (Fit on Train Only)".
- **Branching flow (rounded boxes, height 30, white bold 10px labels, green arrows):** "Full Data" (blue, x=20 w=80) → "Split" (green, x=125 w=70), which branches to a train path (upper: "fit_transform(train)" green 140 wide → "Model" green 80 wide) and a test path (lower: "transform(test)" green 130 wide → "Evaluate" green 80 wide). Dashed green connector between branches labeled "train params" (green 9px).
- **Blocked path:** dashed red line dropping from the test branch to a bold red "✖" with red 10px caption "Test cannot influence scaler".
- **Code box (full width minus margins, at y=175, fill `rgba(39,174,96,0.08)`, green border):** bold green heading "sklearn Pipeline (enforces correct order):" then two monospace 11px blue lines:
  - `pipeline = Pipeline([('scaler', StandardScaler()), ('model', LogisticRegression())])`
  - `cross_val_score(pipeline, X, y, cv=StratifiedKFold(5))  # re-fits each fold`
- **Correct Flow row (y=245, green):** label "Correct Flow:" then boxes "Train Data" → "fit + transform" → "Train Model" (all green, arrows green).
- **Test Flow row (y=275, blue):** label "Test Flow:" then boxes "Test Data" → "transform only" → "Predict" (all blue, arrows blue).

## Regeneration instructions

- **Template/layout:** ml-pipeline-pitfalls detail page. h1 + `.subtitle`, then three `.card-section` blocks ("The Problem", "Why It Happens", "The Correct Approach"). Each section: `h2` with 2px `#2980b9` bottom border, then a `table.layout` (border-collapse, full width) with one row — `td.text-col` (45%) and `td.viz-col` (55%), both top-aligned, 12px padding.
- **Text column structure:** `.tags` div of pill spans, then `ul` of bullets with `<b>` lead-ins (bold `#1a5276`), then italic `.example` paragraph, then `.key-point` callout (background `#f8f9fa`, left border 3px solid `#e74c3c`, 8px 12px padding, 0.9rem) whose `<strong>` label is Impact/Root Cause/Fix.
- **Tag pills:** inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px. Colors: blue `rgba(26,82,118,0.12)`/`#1a5276`; green `rgba(39,174,96,0.15)`/`#27ae60`; red `rgba(231,76,60,0.12)`/`#e74c3c`; orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; subtitle `#666` 0.95rem; canvas `width:100%`, 1px `#e0e0e0` border, 4px radius; ul 0.92rem. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, secondary blue `#2980b9`, bar fill `rgba(26,82,118,0.35)`.
- **Canvas:** each canvas declares intrinsic width=720 height=300 and is drawn via a shared `setup(id)` helper that sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Rounded boxes and arrowheads are drawn with local `drawBox`/`drawArrow` helpers. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- In regenerated HTML, any card links use `.html` extensions.
