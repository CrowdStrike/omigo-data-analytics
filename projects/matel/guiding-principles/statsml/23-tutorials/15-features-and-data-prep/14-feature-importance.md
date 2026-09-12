# Feature Importance

**Page type:** detail page (tutorial layout: one `.card-section` per concept, each with h2 + two-column `table.layout` — text left 50%, canvas right 50%)
**HTML title tag:** Feature Importance

**Subtitle:** A trained model can rank its own inputs — the score says how much the model leaned on each column, and nothing more

## A Churn Model Ranks Its Own Inputs

**Tags:** core idea (blue), running example (green)

- **The setup** — a model predicts which customers cancel, using six facts about each one
- **The question** — which of the six facts did the model actually lean on?
- **Importance score** — one number per feature saying how much the model used it
- **The ranking** — tenure scores 0.34, support calls 0.22, age only 0.07
- **Read as shares** — the scores are normalized to add to 1.00, like slices of the model's attention

*Example:* Tenure — how long someone has been a customer — carries a third of this model's decisions.

**Key point:** Feature importance ranks how useful each input was to THIS trained model — it is a fact about the model, not about the world.

### Visualization (canvas `c1`, 720×300)

Horizontal bar chart: importance scores for the six churn features.

- **Title (bold 16px `#1a5276`, top center):** "Churn Model — Feature Importance (adds to 1.00)".
- **Data (feature, value, bar color):**
  - tenure 0.34, blue `#2a78d6`
  - support calls 0.22, aqua `#199e70`
  - monthly bill 0.16, violet `#4a3aa7`
  - contract type 0.12, orange `#d95926`
  - payment method 0.09, magenta `#d55181`
  - age 0.07, yellow `#c98500`
- **Geometry:** bars start at x=150, right margin 90, first row at y=48, row height 38, bar height 24; scale max 0.40 across the available width.
- **Labels:** feature name 13px `#2c3e50` right-aligned left of each bar; value (two decimals) bold 13px in the bar's color right of each bar.
- **Annotation (bold 13px green `#008300`, in empty area at x=400 beside the shorter bars, two lines):** "tenure + support calls = 0.56" / "— over half the model's attention".
- **Caption (italic 11px muted, bottom center):** "illustrative importances".

## Shuffle One Column, Watch Accuracy Drop

**Tags:** worked example (green), by hand (blue)

- **Start** — the trained model gets 0.86 accuracy: 86 of 100 customers called right
- **Shuffle tenure** — scramble just that column and re-score: accuracy falls to 0.74
- **Shuffle support calls** — same trick on that column: accuracy falls to 0.79
- **Shuffle age** — accuracy barely moves: 0.85
- **The drop is the score** — bigger fall means the model needed that column more

*Example:* 0.86 − 0.74 = a 0.12 drop, so tenure is the column this model can least afford to lose.

**Key point:** You can rank features with nothing but a shuffle and a subtraction — this is called permutation importance.

### Visualization (canvas `c2`, 720×300)

Vertical bar chart: model accuracy after shuffling each column, against a dashed baseline.

- **Title (bold 16px `#1a5276`, top center):** "Accuracy After Shuffling One Column".
- **Bars (two-line x labels, accuracy, drop annotation, color):**
  - "none / (baseline)" — 0.86, no drop label, muted gray `#6b7280`
  - "shuffle / tenure" — 0.74, "-0.12" in bold red `#e74c3c`, blue `#2a78d6`
  - "shuffle / support calls" — 0.79, "-0.07" in muted gray, aqua `#199e70`
  - "shuffle / age" — 0.85, "-0.01" in muted gray, yellow `#c98500`
- **Axes:** y from 0.60 to 0.90, tick labels at 0.60/0.70/0.80/0.90 (12px muted); L-shaped gray `#999` axis; left pad 70, baseline y=240, chart height 175; bars 88px wide.
- **Baseline line:** dashed green `#008300` (dash 6/4, 1.5px) at 0.86, labeled "baseline 0.86" bold 12px green near the right end.
- **Value labels:** accuracy bold 13px `#2c3e50` above each bar; drop annotation above that.
- **Callout (bold 13px red `#e74c3c`, centered under title):** "biggest drop = most important: tenure (-0.12)".
- **Caption (italic 11px muted, bottom center):** "illustrative accuracies".

## What the Ranking Does and Does Not Tell You

**Tags:** where it's used (blue), not causation (red)

- **Debugging** — a leaked feature (like "cancellation date") jumps to the top and exposes the bug
- **Trust** — stakeholders can sanity-check the top features against common sense
- **Focus** — collect the high scorers carefully; stop paying for useless columns
- **Not causation** — support calls score 0.22, but stopping the calls won't stop churn
- **Model-specific** — retrain with different features or data and the ranking changes

*Example:* Unhappy customers call support and then cancel — the calls signal churn, they don't cause it.

**Key point:** Importance says "the model used this to predict" — never "change this and the outcome changes".

### Visualization (canvas `c3`, 720×300)

Two-panel bar comparison: prediction works vs intervention fails, split by a dashed vertical divider at x=360.

- **Title (bold 16px `#1a5276`, top center):** "Support Calls: Great Predictor, Useless Lever".
- **Left panel (title bold 13px blue `#2a78d6`):** "PREDICTION: churn rate by calls made" — two bars: "0 calls" 8% (aqua `#199e70`), "3+ calls" 41% (blue `#2a78d6`); note below in bold blue: "calls flag who will churn — 5x rate".
- **Right panel (title bold 13px red `#e74c3c`):** "INTERVENTION: remove the call button" — two bars: "button shown" 26%, "button hidden" 26%, both orange `#d95926`; note below in bold red: "churn unchanged — calls were a symptom".
- **Panel geometry:** each panel 280px wide (left at x=50, right at x=395), baseline y=230, chart height 140, value scale max 50%, bars 92px wide; percent values bold 13px above bars, labels 12px below baseline.
- **Divider:** dashed `#bdc3c7` vertical line (dash 4/3) at x=360 from y=40 to bottom.
- **Caption (italic 11px muted, bottom center):** "illustrative experiment".

## Twins Split the Credit

**Tags:** common confusion (orange), trap (red)

- **Add a twin** — months_since_signup is almost the same fact as tenure
- **Before** — tenure alone scores 0.34, top of the list
- **After** — tenure 0.18 and the twin 0.16; each now looks mediocre
- **Nothing was lost** — together they still carry 0.34; the credit just split
- **The trap** — dropping "low-importance" features can drop both halves of a strong signal

*Example:* Two colleagues splitting one job each look half as busy — the job is unchanged.

**Key point:** Correlated features share credit — read importance in groups, not one row at a time.

### Visualization (canvas `c4`, 720×300)

Two-panel before/after bar comparison of importance credit splitting, dashed vertical divider at x=360.

- **Title (bold 16px `#1a5276`, top center):** "Add a Near-Duplicate of Tenure: the 0.34 Splits".
- **Left panel (title bold 13px `#1a5276`):** "BEFORE: tenure alone" — bars: tenure 0.34 (blue `#2a78d6`), support calls 0.22 (aqua `#199e70`).
- **Right panel (title bold 13px `#1a5276`):** "AFTER: twin added" — bars: tenure 0.18 (blue `#2a78d6`), months since signup 0.16 (violet `#4a3aa7`), support calls 0.22 (aqua `#199e70`).
- **Panel geometry:** each panel 280px wide (left at x=45, right at x=395), baseline y=225, chart height 145, scale max 0.40, bars 66px wide; values bold 13px above bars; two-line 11px labels below baseline.
- **Bracket:** green `#008300` 2px bracket spanning the tenure and twin bars in the right panel, labeled bold 12px green: "0.18 + 0.16 = 0.34 — same total, split label".
- **Divider:** dashed `#bdc3c7` vertical line at x=360.
- **Caption (italic 11px muted, bottom center):** "illustrative importances".

## Regeneration instructions

- **Layout:** tutorial detail page. h1 (2rem, `#1a5276`, 2px `#2980b9` bottom border) + `.subtitle` (`#666`, 0.95rem), then four `.card-section` blocks (40px bottom margin). Each section: `<h2>` (1.3rem, `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row — left `td.text-col` (50%) holds tags, bullets, `.example`, `.key-point`; right `td.viz-col` (50%) holds the canvas.
- **Text column structure:** `.tags` row of pill spans (0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`); `<ul>` bullets (0.92rem) each opening with `<b>` term in `#1a5276`; italic `.example` paragraph (`#555`, 0.9rem); `.key-point` callout (background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem) with `<strong>` lead.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; universal `box-sizing: border-box` reset; canvases have `width:100%`, 1px `#e0e0e0` border, 4px radius.
- **Canvas JS:** shared palette object `P = { blue:#2a78d6, green:#008300, magenta:#d55181, yellow:#c98500, aqua:#199e70, orange:#d95926, violet:#4a3aa7, ink:#1a5276, text:#2c3e50, mute:#6b7280, grid:#e5e9ef }`; shared `setup(id)` helper scales each 720×300 canvas by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Site palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
