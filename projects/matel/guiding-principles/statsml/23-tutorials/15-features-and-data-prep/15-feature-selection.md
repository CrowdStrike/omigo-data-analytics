# Feature Selection

**Page type:** detail page (tutorial layout: one `.card-section` per concept, each with h2 + two-column `table.layout` — text left 50%, canvas right 50%)
**HTML title tag:** Feature Selection

**Subtitle:** Most of your columns aren't helping — find the small set that does the work and drop the rest

## 60 Features In, 12 Do the Work

**Tags:** core idea (blue), running example (green), why it matters (blue)

- **The setup** — a churn model with 60 columns: usage, billing, support, marketing tags
- **The test** — retrain using only the best 12 columns and compare accuracy
- **The result** — 12 features score 0.84; all 60 score 0.83; nothing was lost
- **Simpler** — 12 inputs are easy to explain, check, and monitor
- **Faster** — less data to collect, clean, and compute at prediction time
- **More stable** — fewer columns means fewer ways for noise and drift to sneak in

*Example:* 48 of the 60 columns were dead weight — the model just needed help admitting it.

**Key point:** Feature selection means keeping the few inputs that earn their place and deleting the rest — usually at no cost in accuracy.

### Visualization (canvas `c1`, 720×300)

Line chart: test accuracy vs number of features kept, plateauing at 12.

- **Title (bold 16px `#1a5276`, top center):** "Test Accuracy vs Number of Features Kept".
- **Data:** number of features `[4, 8, 12, 20, 30, 45, 60]`, accuracy `[0.76, 0.81, 0.84, 0.84, 0.84, 0.84, 0.83]`.
- **Axes:** L-shaped gray `#999` axis; padding top 55, bottom 55, left 65, right 35; x linear 0–60; y from 0.70 to 0.90 with tick labels every 0.05 (12px muted `#6b7280`).
- **Plateau shading:** `rgba(0,131,0,0.07)` region from x=12 to x=60 across the full plot height.
- **Series:** blue `#2a78d6` line, 3px; 4px dots at each point, except the 12-feature point which is a 6px green `#008300` dot; bold 12px accuracy labels above each point, 12px feature counts below the axis.
- **Annotation (bold 13px green, centered near x≈34):** "flat after 12 — the extra 48 features add nothing".
- **X-axis caption (12px muted, bottom center):** "number of features kept (illustrative)".

## Scoring Columns by Hand

**Tags:** worked example (green), by hand (blue)

- **Score each column** — how strongly does it move with churn, on its own?
- **The leaders** — support_calls 0.41, tenure 0.38, late_payments 0.33
- **The tail** — signup_month 0.06, browser 0.04, app_version 0.03 barely move with churn
- **Draw a line** — keep columns scoring above 0.15, drop the rest
- **Repeat at scale** — the same one-column scoring ranks all 60 in one pass

*Example:* browser scored 0.04 — knowing someone uses Chrome tells you almost nothing about churn.

**Key point:** The simplest selection is a scoreboard — score each column alone against the target, keep the top of the list. One caveat: a column weak alone can still matter in combination with others.

### Visualization (canvas `c2`, 720×300)

Horizontal sorted bar chart: per-column correlation scores with a dashed cutoff line.

- **Title (bold 16px `#1a5276`, top center):** "Score Each Column Alone: |correlation with churn|".
- **Data (feature, score):** support_calls 0.41, tenure 0.38, late_payments 0.33, monthly_bill 0.24, plan_type 0.18, signup_month 0.06, browser 0.04, app_version 0.03. Cutoff = 0.15.
- **Bars:** start at x=150, right margin 140, first row y=46, row height 28, bar height 18, scale max 0.45; kept bars (score > 0.15) green `#008300`, dropped bars gray `#c8cdd4`.
- **Labels:** feature name 12px right-aligned left of bar (`#1a5276` if kept, muted if dropped); score bold 12px right of bar (green if kept, muted if dropped).
- **Cutoff line:** dashed red `#e74c3c` vertical line (dash 6/4, 2px) at score 0.15, spanning all rows; label bold 13px red to its right: "cutoff 0.15: keep 5, drop 3".
- **Caption (italic 11px muted, bottom center):** "8 of the 60 columns shown — illustrative scores".

## Three Ways to Pick: Filter, Wrapper, Embedded

**Tags:** the toolbox (blue), rule of thumb (blue)

- **Filter** — score each column alone before any model; cheap and fast (the scoreboard above)
- **Wrapper** — try feature subsets, train a model on each, keep the best subset; thorough but slow
- **Embedded** — the model prunes while it trains; lasso shrinks useless weights to zero
- **Cost order** — filter cheapest, embedded in between, wrapper most expensive
- **In practice** — filter 60 columns down to ~25, then embedded or wrapper down to 12

*Example:* Lasso trained on all 60 churn columns set 41 of the weights to exactly zero on its own.

**Key point:** All three answer the same question — they differ in whether a model is consulted, and how often.

### Visualization (canvas `c3`, 720×300)

Three-panel flow diagram showing where the model sits in each selection strategy, panels separated by dashed `#bdc3c7` vertical dividers at x=240 and x=480.

- **Title (bold 16px `#1a5276`, top center):** "Where the Model Sits in Each Strategy".
- **FILTER panel (left, header bold 14px aqua `#199e70` at x=120):** vertical flow of outlined boxes (150px wide, aqua border, fill `rgba(25,158,112,0.08)`) connected by aqua arrows: "score columns / alone" → "keep the top" → "train model ONCE" (this last box in ink `#1a5276` border, fill `rgba(26,82,118,0.08)`). Footer 12px muted: "cheapest".
- **WRAPPER panel (center, header bold 14px orange `#d95926` at x=360):** flow "pick a subset" (orange box, fill `rgba(217,89,38,0.08)`) → "train model" (ink box) → "score & compare" (orange box), with an orange loop arrow returning from the bottom box to the top box on the right side; rotated bold 11px orange label along the loop: "repeat many times". Footer 12px muted: "most expensive".
- **EMBEDDED panel (right, header bold 14px violet `#4a3aa7` at x=600):** one large ink-bordered container box (150×110, fill `rgba(26,82,118,0.06)`) labeled bold 12px ink "train model", holding an inner violet box (110×56, fill `rgba(74,58,167,0.08)`) with three lines "pruning built in: / useless weights / shrink to zero"; violet arrow down to bold 12px violet text "model + selection in one pass". Footer 12px muted: "in between (e.g. lasso)".

## Select on Training Data Only

**Tags:** common mistake (red), leakage (orange)

- **The mistake** — scoring features on ALL rows, then splitting into train and test
- **Why it leaks** — the test rows already voted on which features got kept
- **What you see** — test accuracy looks like 0.88 in the notebook
- **What's real** — on genuinely new customers the model gets 0.82
- **The fix** — split first; score and select using training rows only

*Example:* The 12 "best" features were partly best because they fit the very test set you graded on.

**Key point:** Feature selection is part of training — it must never see the test rows.

### Visualization (canvas `c4`, 720×300)

Two-panel bar comparison: reported vs real accuracy under leaky vs clean selection, dashed `#bdc3c7` vertical divider at x=360.

- **Title (bold 16px `#1a5276`, top center):** "Where You Ran Selection Changes What the Test Score Means".
- **Left panel (title bold 13px red `#e74c3c`):** "SELECTED ON ALL DATA (leak)" — bars: "notebook / test score" 0.88 (blue `#2a78d6`), "new / customers" 0.82 (yellow `#c98500`); note bold 12px red: "gap of 0.06 was leak, not skill".
- **Right panel (title bold 13px green `#008300`):** "SELECTED ON TRAIN ONLY" — bars: "notebook / test score" 0.84 (blue), "new / customers" 0.83 (yellow); note bold 12px green: "the notebook number holds up".
- **Panel geometry:** each panel 280px wide (left at x=50, right at x=395), baseline y=225, chart height 130, y-scale 0.70–0.92, bars 92px wide; values bold 13px above bars; two-line 12px labels below baseline.

## Regeneration instructions

- **Layout:** tutorial detail page. h1 (2rem, `#1a5276`, 2px `#2980b9` bottom border) + `.subtitle` (`#666`, 0.95rem), then four `.card-section` blocks (40px bottom margin). Each section: `<h2>` (1.3rem, `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row — left `td.text-col` (50%) holds tags, bullets, `.example`, `.key-point`; right `td.viz-col` (50%) holds the canvas.
- **Text column structure:** `.tags` row of pill spans (0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`); `<ul>` bullets (0.92rem) each opening with `<b>` term in `#1a5276`; italic `.example` paragraph (`#555`, 0.9rem); `.key-point` callout (background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem) with `<strong>` lead.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; universal `box-sizing: border-box` reset; canvases have `width:100%`, 1px `#e0e0e0` border, 4px radius.
- **Canvas JS:** shared palette object `P = { blue:#2a78d6, green:#008300, magenta:#d55181, yellow:#c98500, aqua:#199e70, orange:#d95926, violet:#4a3aa7, ink:#1a5276, text:#2c3e50, mute:#6b7280, grid:#e5e9ef }`; shared `setup(id)` helper scales each 720×300 canvas by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Site palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
