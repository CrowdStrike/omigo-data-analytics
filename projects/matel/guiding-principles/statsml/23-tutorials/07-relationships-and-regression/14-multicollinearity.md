# Multicollinearity

**Page type:** detail page (tutorial: card-sections, each a two-column layout table — text left 50%, canvas right 50%)
**HTML title tag:** Multicollinearity

**Subtitle:** When two inputs carry the same signal, the model can't split credit between them — coefficients go haywire while predictions stay fine

## Two Copies of the Same Dial

**Tags:** `core idea` (blue), `running example` (green)

- **The model** — predict weekly sales from ad spend in dollars AND the same spend in euros
- **Near-duplicates** — every week, euros ≈ 0.9 × dollars; two columns, one signal
- **Nothing warns you** — the fit runs cleanly and the predictions come out fine
- **Multicollinearity** — inputs so correlated the model can't tell them apart
- **Not just currencies** — height in cm and inches, total and subtotal, age and birth year

*Example:* Across 10 weeks the two spend columns correlate at 0.999 — one dial wearing two labels.

**Key point:** A model needs each input to move on its own sometimes; these two always move together.

### Visualization (canvas `c1`, 720×300)

Scatter plot — dollars vs euros, 10 weeks on a straight line.

- **Title (bold 15px, ink `#1a5276`, top center):** "Ten weeks of ad spend: the two columns move as one"
- **Data (blue `#2a78d6` dots, radius 6):** dollars `[2, 4, 5, 6, 7, 8, 9, 10, 11, 12]` k$ vs euros `[1.8, 3.7, 4.4, 5.5, 6.2, 7.3, 8.0, 9.1, 9.8, 10.9]` k€
- **Fit line:** dashed light-gray (`#e5e9ef`, dash 6/4, width 2) line euros = 0.9 × dollars, drawn from 0 to 13
- **Axes:** both x and y from 0 to 13 with muted tick labels every 3; x-axis title "ad spend in dollars (k$)", rotated y-axis title "ad spend in euros (k€)"; L-shaped gray axes `#999`; padding: top 50, bottom 52, left 70, right 40
- **Annotations:** bold 13px magenta `#d55181` (upper left area): "correlation 0.999 — one signal, two columns"; muted 12px near the line: "euros ≈ 0.9 × dollars"

## Three Models, One Prediction

**Tags:** `worked example` (green), `do it by hand` (blue)

- **Fit 1** — sales = 50 + 2.0×dollars + 0×euros
- **Fit 2** — sales = 50 + 0×dollars + 2.22×euros
- **Fit 3** — sales = 50 + 12×dollars − 11.1×euros
- **Check at $10k** (= 9k€) — 50+20, 50+20, 50+120−100: all say 70
- **Why** — since euros = 0.9×dollars, endless coefficient pairs draw the same line

*Example:* Fit 3 claims dollars help hugely and euros hurt — an absurd story that predicts perfectly.

**Key point:** With duplicated inputs the data cannot pick one coefficient pair — many stories fit equally well.

This section's viz cell holds both canvases side by side in a `.viz-pair` flex row.

### Visualization (canvas `c2a`, 310×340)

Grouped bar chart of coefficient pairs for the three fits (bars extend above/below a zero line).

- **Title (bold 15px, ink, top center):** "Three coefficient stories"
- **Groups (bar width 28px, in-group gap 6px), dollar coef = blue `#2a78d6`, euro coef = violet `#4a3aa7`:**
  - "Fit 1": dollars 2.0, euros 0
  - "Fit 2": dollars 0, euros 2.22
  - "Fit 3": dollars 12, euros −11.1
- **Value labels:** bold 12px numbers above positive bars / below negative bars
- **Axes:** y from −14 to +14, gridlines and muted labels at −12, −6, 0, 6, 12; solid gray zero line (`#999`, width 1.5); padding: left 46, right 12, top 45, bottom 65
- **Legend (top left, 11px squares):** blue "dollar coef", violet "euro coef"
- **Caption (bold 13px magenta `#d55181`, bottom center):** "wildly different stories"

### Visualization (canvas `c2b`, 310×340)

Three identical bars — same prediction from all fits.

- **Title (bold 15px, ink, top center):** "Predicted sales at $10k spend"
- **Bars (width 56px, all aqua `#199e70`, all value 70):** labels "Fit 1" / "Fit 2" / "Fit 3" with muted 12px sublabels "50 + 20", "50 + 20", "50 + 120" over a second line "− 100"
- **Value labels:** bold 13px "70" above each bar
- **Axes:** y 0 to 90, gridlines and muted labels every 30; padding: left 46, right 12, top 45, bottom 78
- **Caption (bold 13px green `#008300`, bottom center):** "identical predictions, every time"

## Coefficients Go Haywire, Predictions Stay Calm

**Tags:** `where it's used` (blue), `silent failure` (red)

- **Refit five times** — the dollar dial reads 2.0, 13.1, −6.5, 9.4, −2.8 (illustrative)
- **Signs flip** — the same channel looks helpful one run and harmful the next
- **Errors stay flat** — prediction error barely moves: ~3 units every run
- **Silent failure** — accuracy checks pass, so nobody notices the dials are noise
- **Stakeholder risk** — "which channel drives sales?" gets a confident, random answer

*Example:* Marketing nearly cut the euro-billed campaigns because their coefficient came out negative.

**Key point:** Multicollinearity barely hurts prediction — it destroys the story you tell about the features.

### Visualization (canvas `c3`, 720×300)

Two-panel figure split by vertical dashed divider (`#bdc3c7`, dash 4/3, at x=400).

- **Overall title (bold 15px, ink, top center):** "Five refits on slightly different weeks (illustrative)"
- **Left panel — line chart, dollar coefficient per refit (plot at x=65, width 300, top 60, height 175):**
  - Panel title (bold 13px ink): "dollar coefficient per refit"
  - Data: coefficients `[2.0, 13.1, -6.5, 9.4, -2.8]` at runs 1–5, connected blue (`#2a78d6`, width 2.5) line; dots radius 5, colored red `#e74c3c` when negative, blue otherwise; bold 11px value labels above points, muted "run 1"…"run 5" x labels
  - y from −10 to 15, muted labels every 5; dashed red zero line (`#e74c3c`, dash 5/4, width 1.5)
  - Annotation (bold 12px red, bottom center): "sign flips run to run"
- **Right panel — bar chart, prediction error per refit (plot at x=440, width 250, top 60, height 175):**
  - Panel title (bold 13px ink): "prediction error per refit"
  - Data: RMSE `[3.1, 3.0, 3.2, 3.1, 3.0]` at runs 1–5, aqua `#199e70` bars (width 34px), 11px value labels above, muted "run N" labels below
  - y 0 to 6, muted labels every 2
  - Annotation (bold 12px green `#008300`, bottom center): "accuracy never notices"

## The Fix, and When Not to Worry

**Tags:** `common mistake` (orange), `rule of thumb` (green)

- **Not a math bug** — the regression answers honestly; the question is unanswerable
- **Simplest fix** — drop one duplicate: keep dollars, delete euros; nothing is lost
- **Or combine** — merge near-twins into one feature: a total, an average, a ratio
- **Detect it first** — check pairwise correlations or VIF before trusting any dial
- **When to relax** — if you only need predictions, correlated inputs are mostly harmless

*Example:* After dropping the euro column, the dollar dial settled near 2.0 in every single refit.

**Key point:** Ask "which feature matters?" only after checking no two features are the same signal twice.

### Visualization (canvas `c4`, 720×300)

Two-panel before/after line chart split by vertical dashed divider (`#bdc3c7`, dash 4/3, at x=w/2).

- **Overall title (bold 15px, ink, top center):** "Dollar coefficient across refits: before vs after dropping euros"
- **Both panels (280px wide, plot top 62, height 170):** y from −10 to 15 with muted labels every 5; light-gray solid zero line (`#e5e9ef`, width 1.5); dashed green (`#008300`, dash 5/4, width 1) reference line at the true value 2.0; blue (`#2a78d6`, width 2.5) connected line with radius-5 blue dots and 11px value labels over 5 runs
- **Left panel (x0=65):** title "both spend columns in"; data `[2.0, 13.1, -6.5, 9.4, -2.8]`; note (bold 12px red `#e74c3c`, bottom center): "haywire"
- **Right panel (x0=w/2+45):** title "euro column dropped"; data `[2.0, 2.1, 1.9, 2.0, 2.1]`; note (bold 12px green, bottom center): "settles at the true 2.0"

## Regeneration instructions

- **Template/layout:** tutorials topic-page skeleton. `<h1>` (no index number) with 2px bottom border `#2980b9`, `.subtitle` paragraph, then four `.card-section` blocks each with an `<h2>` (1.3rem, `#1a5276`, 2px bottom border `#2980b9`) and a `table.layout` (one `<tr>`: `.text-col` 50% / `.viz-col` 50%). One section places canvases `c2a`/`c2b` (310×340 each) side by side inside its single viz cell, wrapped in a `.viz-pair` flex row (`display:flex; gap:10px`, each canvas `flex:1 1 0; min-width:0`).
- **Left column structure:** `.tags` row of pill spans first (`.tag.blue` rgba(26,82,118,0.12)/#1a5276, `.tag.green` rgba(39,174,96,0.15)/#27ae60, `.tag.red` rgba(231,76,60,0.12)/#e74c3c, `.tag.orange` rgba(230,126,34,0.15)/#e67e22; 0.72rem, 600 weight, 2px 10px padding, radius 10px), then a `<ul>` of one-line bullets each opening with `<b>` (bold terms colored `#1a5276`), an italic `.example` paragraph (`#555`, 0.9rem), and a `.key-point` callout (background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem) beginning with `<strong>Key point:</strong>`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; canvases `width:100%`, 1px solid `#e0e0e0` border, 4px radius; ul 0.92rem.
- **Chart palette (JS object P):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- **Canvas:** declare intrinsic `width`/`height` attributes per chart; a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`), and calls `ctx.scale` so drawing stays in logical coordinates, and fills a white background.
- No nav bar, no back/home links, no cross-page links. In regenerated HTML, any card links use `.html` extensions.
