# Matrix Rank & Linear Independence

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Matrix Rank & Linear Independence

**Subtitle:** Rank counts how many columns carry genuinely new information — a column built from other columns adds width to the table but nothing the data didn't already say

## One Spreadsheet Column Too Many

**Tags:** `core idea` (blue), `redundant column` (orange), `rank` (green)

- **The shop** — a coffee shop logs five days: coffees sold, pastries sold, and a "total items" column
- **Coffees** — Mon–Fri counts are 12, 8, 15, 10, 9; pastries are 5, 7, 3, 6, 8
- **Totals** — the third column reads 17, 15, 18, 16, 17 — exactly coffees + pastries every day
- **No news** — knowing coffees and pastries, you can rebuild totals; the column is dependent
- **Rank** — the table has 3 columns but rank 2: only two columns carry independent information

*Example (italic):* Monday's total of 17 is just 12 + 5 — the totals column never tells you anything the first two columns didn't.

**Key point:** The rank of a matrix is the size of the largest set of columns in which none can be rebuilt from the rest. A dependent column is a copy in disguise: 3 columns wide, 2 columns of substance.

### Visualization (canvas `c1`, 720×300)

Grouped bar chart: for each of five days, a stacked bar (coffees + pastries) next to a solid "total items" bar of identical height, showing the third column is rebuilt from the first two.

- **Title (bold 15px, `#1a5276`, top center):** "Totals Column = Coffees + Pastries, Every Single Day".
- **Data:** days Mon–Fri; coffees `[12, 8, 15, 10, 9]`, pastries `[5, 7, 3, 6, 8]`, totals `[17, 15, 18, 16, 17]`.
- **Layout:** axis origin x=60, plot width 560, baseline y=245, chart height 180, y scale 0–20; y gridlines at 5, 10, 15, 20 in `#e5e9ef` with 11px `#6b7280` labels.
- **Bars:** each day gets a pair — left bar stacked: coffees in `rgba(42,120,214,0.55)` bottom, pastries in `rgba(0,131,0,0.45)` on top; right bar solid violet `rgba(74,58,167,0.45)` at the total height; bars 34px wide, 6px gap within a pair, day pairs evenly spaced; day labels 12px `#444` below baseline.
- **Legend (12px, top left under title):** blue swatch "coffees", green swatch "pastries", violet swatch "total items column".
- **Annotation (bold 13px violet `#4a3aa7`, above the Wed pair):** "the stack and the column match all 5 days".
- **Caption (12px `#444`, bottom right):** "3 columns on paper, 2 columns of information — rank = 2".

## Catching the Copycat by Hand

**Tags:** `worked example` (blue), `rule of thumb` (green)

- **The test** — a column is dependent if some weighted mix of the other columns rebuilds it exactly
- **The recipe** — try weights 1 and 1: predicted total = 1×coffees + 1×pastries for each day
- **Check Mon** — 1×12 + 1×5 = 17, matching the totals column; Tue: 8 + 7 = 15, also a match
- **All five** — predictions 17, 15, 18, 16, 17 equal the recorded totals with zero error every day
- **Verdict** — an exact rebuild means dependence; rank = 3 columns − 1 dependent column = 2

*Example (italic):* Plot predicted total against recorded total for the five days — all five points sit exactly on the diagonal, so the rebuild is perfect.

**Key point:** Independence is testable by hand: hunt for weights that rebuild one column from the rest. Zero error on every row means the column is dependent and rank drops by one.

### Visualization (canvas `c2`, 720×300)

Dual-panel: scatter of predicted vs recorded totals sitting exactly on the diagonal (left), and a columns-vs-rank slot diagram (right), split by a vertical dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "The Rebuild Test: Predict One Column From the Others".
- **Left panel (scatter):** axis origin x=60, width 260, baseline y=245, chart height 180; both axes span 14–19; dashed `#bdc3c7` diagonal y=x; five green `#008300` 6px dots at (17,17), (15,15), (18,18), (16,16), (17,17) — the coincident pair (17,17) drawn once with a "×2" label 11px `#6b7280` beside it; x label 12px `#444` "coffees + pastries", y label "recorded total"; green bold 13px annotation "zero error on all 5 days".
- **Right panel (rank slots):** three rounded rectangles 70×130 starting x=410, y=90, 25px apart, labeled 12px below "coffees", "pastries", "total items"; first two filled `rgba(42,120,214,0.45)` and `rgba(0,131,0,0.4)` with bold 13px white-on-color "NEW INFO"; third filled `#f0f0f0` with 2px dashed `#d55181` border and magenta bold 13px "REBUILT"; below the slots, bold 14px `#1a5276` centered "columns = 3, rank = 2".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.

## Why Regression Chokes on It

**Tags:** `where it's used` (blue), `regression` (orange), `failure mode` (red)

- **The task** — predict daily revenue 46, 38, 51, 42, 43 from coffees, pastries, and total items
- **Recipe A** — weights (3, 2, 0): coffee $3, pastry $2, ignore totals — fits all five days exactly
- **Recipe B** — weights (1, 0, 2) fits identically: 1×12 + 0×5 + 2×17 = 46 on Monday, and so on
- **Recipe C** — weights (0, −1, 3) also fits exactly; infinitely many recipes tie perfectly
- **The break** — the solver's matrix is singular: it cannot pick one answer, and coefficients lose meaning

*Example (italic):* Recipe B says pastries are worth $0 and Recipe C says −$1 — same predictions, so the data cannot arbitrate between them.

**Key point:** A rank-deficient feature matrix means many coefficient sets give identical predictions. The fit may look fine while every individual coefficient is arbitrary — this is perfect multicollinearity.

### Visualization (canvas `c3`, 720×300)

Dual-panel: three coefficient recipes as grouped bars (left) and their single shared prediction line over the five days (right), split by a vertical dashed divider at x=380.

- **Title (bold 15px, `#1a5276`, top center):** "Three Different Recipes, One Identical Prediction".
- **Left panel (coefficients):** axis origin x=55, width 290; zero line at y=170, 1 unit = 28px, range −1 to +3; three groups labeled 12px `#444` below: "coffees", "pastries", "total"; within each group three 18px bars, 4px apart — Recipe A blue `rgba(42,120,214,0.55)` values `[3, 2, 0]`, Recipe B orange `rgba(217,89,38,0.5)` values `[1, 0, 2]`, Recipe C magenta `rgba(213,81,129,0.5)` values `[0, -1, 3]`; negative bar drawn below the zero line; value labels bold 11px in each bar's color above (or below, if negative) each bar; legend 12px top left "A / B / C".
- **Right panel (predictions):** axis origin x=430, width 250, baseline y=245, chart height 180, y scale 35–55; days Mon–Fri 12px `#444` below baseline; one green `#008300` 3px line through revenue `[46, 38, 51, 42, 43]` with 5px dots; green bold 13px annotation, two lines: "A, B and C all land" / "on this exact line".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=380 from y=38 to h-12.
- **Caption (12px `#444`, bottom center of left panel):** "the solver cannot choose — the matrix is singular".

## Almost-Dependent Is Almost as Bad

**Tags:** `common mistake` (red), `near-dependence` (orange), `rule of thumb` (green)

- **The trap** — "no column is an exact copy, so we're safe" — near-copies cause most real damage
- **Two columns** — cups sold 12, 8, 15, 10, 9 vs lids used 12, 9, 15, 10, 10 — off by 1 on two days
- **Technically** — rank is a full 2, since no exact rebuild exists; the matrix is invertible on paper
- **Practically** — the columns are nearly parallel, so tiny data noise swings coefficients wildly
- **The fix** — drop or merge near-duplicate columns; exact rank won't warn you, closeness will

*Example (italic):* Changing Tuesday's lid count by 1 can flip the fitted cup coefficient from strongly positive to negative — the sign was never real.

**Common mistake:** Trusting the rank number alone. Rank is all-or-nothing — it only drops for exact dependence — but a 99% copy makes the solution unstable long before rank says anything.

### Visualization (canvas `c4`, 720×300)

Scatter of lids used against cups sold hugging the y=x line, with the near-dependence gap highlighted and a warning strip contrasting "rank says" vs "reality says".

- **Title (bold 15px, `#1a5276`, top center):** "Not an Exact Copy — But Close Enough to Break the Math".
- **Scatter:** axis origin x=70, width 380, baseline y=235, chart height 170; both axes span 6–16 with ticks at 6, 8, 10, 12, 14, 16 (11px `#6b7280`); dashed `#bdc3c7` reference line y=x labeled 11px `#6b7280` "lids = cups"; five blue `#2a78d6` 7px dots at (12,12), (8,9), (15,15), (10,10), (9,10); the two off-line points (8,9) and (9,10) ringed with 2px magenta `#d55181` circles of radius 11; x label 12px `#444` "cups sold", y label "lids used".
- **Annotation (bold 13px magenta `#d55181`, pointing at the ringed points):** "off by 1 on just 2 days".
- **Verdict panel (right side, from x=480):** two stacked rounded boxes 200×70 at y=70 and y=160; top box border 2px `#008300`, bold 13px green heading "rank says:" and 12px `#444` text "2 of 2 — technically fine"; bottom box border 2px `#d55181`, bold 13px magenta heading "reality says:" and 12px `#444` text "nearly parallel — coefficients unstable".
- **Caption (12px `#444`, bottom center):** "cups and lids, five days (illustrative)".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
