# ROC & AUC

**Page type:** detail page (tutorial card-sections: h2 per section, two-column table.layout with text left 50%, canvas right 50%)
**HTML title tag:** ROC & AUC

**Subtitle:** Slide the alarm threshold from strict to loose and trace what happens — AUC is the chance a random fraud outscores a random legit transaction

## One Fraud Model, Every Threshold at Once

**Tags:** `core idea` (blue), `threshold sweep` (orange)

- **The setup** — 1,000 card transactions, 50 fraud; the model scores each from 0 (safe) to 1 (risky)
- **One knob** — pick a threshold; every transaction scoring above it gets flagged as fraud
- **Strict (0.8)** — flags 39: catches 20 of the 50 frauds, disturbs 19 legit customers
- **Loose (0.4)** — flags 235: catches 45 of 50 frauds but disturbs 190 legit customers
- **The curve** — plot catch rate vs false-alarm rate at every threshold: that line is the ROC curve

*Example:* At threshold 0.6 the model catches 35 of 50 frauds (70%) while flagging 57 of 950 legit (6%).

**Key point:** The ROC curve is not one result — it is every possible threshold of the same model drawn as one line.

### Visualization (canvas `c1`, 720×300)

ROC curve of the running fraud model with one labeled point per threshold, shaded AUC area, and the coin-flip diagonal.

- **Title (bold 15px, `#1a5276`, top center):** "The ROC Curve: One Point per Threshold".
- **Data (threshold sweep, 50 fraud / 950 legit):** thresholds `["1.0","0.9","0.8","0.6","0.4","0.2","0.0"]`; FPR % `[0, 0.53, 2, 6, 20, 50, 100]`; TPR % `[0, 20, 40, 70, 90, 98, 100]`. (Underlying counts: frauds caught 0/10/20/35/45/49/50; legit flagged 0/5/19/57/190/475/950.)
- **Axes:** padding top 50, bottom 52, left 62, right 30; both axes 0–100%; x ticks at 0/20/40/60/80/100 (12px `#222`); x-axis caption "false-alarm rate: legit flagged / 950, %" and rotated y-axis caption "catch rate: frauds caught / 50, %" (12px `#444`); L-shaped `#999` axes.
- **Diagonal:** dashed `#bbb` line (dash 5/4, width 1.5) from (0,0) to (100,100), with rotated gray `#888` 12px label along it: "coin flip (AUC 0.5)".
- **Curve:** blue `#2a78d6` line, width 3, through all sweep points; area under the curve shaded `rgba(42,120,214,0.10)`. Points are 4px blue dots, except threshold 0.6 which is a 6px orange `#d95926` dot; each point (except the first) labeled "t=0.9" … "t=0.0" in 12px `#555`.
- **Annotations (left-aligned near mid-plot):** orange bold 13px "AUC ≈ 0.92 = shaded area"; gray 12px "loosening the threshold walks up the curve".

## AUC by Hand: Nine Pairs, Eight Wins

**Tags:** `worked example` (green), `core idea` (blue)

- **The question AUC answers** — pick one random fraud and one random legit: who scores higher?
- **Tiny version** — 3 frauds score 0.9, 0.7, 0.4; 3 legit transactions score 0.6, 0.3, 0.2
- **All pairs** — 3 × 3 = 9 fraud-vs-legit pairs to compare
- **Count wins** — the fraud outscores the legit in 8 of 9 pairs; only 0.4 vs 0.6 goes wrong
- **AUC = 8/9 ≈ 0.89** — the same number the area under the curve gives, no curve needed

*Example:* The full 1,000-transaction model wins about 92 of every 100 random fraud-vs-legit pairs — AUC ≈ 0.92.

**Key point:** AUC = the probability a random fraud scores above a random legit; 0.5 is a coin flip, 1.0 is a perfect sort.

### Visualization (canvas `c2`, 720×300)

A 3×3 pair-comparison grid (fraud scores as rows, legit scores as columns) with win/loss cells and a tally.

- **Title (bold 15px, `#1a5276`, top center):** "Every Fraud-vs-Legit Pair: Who Scores Higher?".
- **Data:** fraud scores `[0.9, 0.7, 0.4]` (row headers in magenta `#d55181`, bold 12px, "fraud 0.9" etc.); legit scores `[0.6, 0.3, 0.2]` (column headers in violet `#4a3aa7`, "legit 0.6" etc.).
- **Grid (at x=170, y=78; cells 108×58):** each cell shows "0.9 > 0.6"-style comparison (bold 13px) plus "win"/"loss" (bold 12px). Win cells: fill `rgba(0,131,0,0.13)`, border and text green `#008300`. Loss cell (fraud 0.4 vs legit 0.6): fill `rgba(231,76,60,0.15)`, border and text red `#e74c3c`. Cell borders 1.5px, inset 3px.
- **Right-side tally (centered at x=590):** "8 wins" (bold 15px green), "1 loss" (bold 15px red), "AUC = 8/9" and "≈ 0.89" (bold 14px `#1a5276`).
- **Caption (gray 12px, bottom center):** "the one loss: the 0.4 fraud slipped below the 0.6 legit transaction".

## What 0.5, 0.75 and 0.92 Actually Look Like

**Tags:** `where it's used` (blue), `rule of thumb` (green)

- **Coin flip (0.5)** — the diagonal: scores carry no information, every catch costs an equal false alarm
- **Weak (0.75)** — bows modestly above the diagonal; useful, but catches come at a real price
- **Strong (0.92)** — hugs the top-left corner: high catch rates while false-alarm rates stay low
- **Threshold-free** — AUC compares models before anyone commits to a production threshold
- **Rank-only** — AUC cares only about ordering; doubling every score changes nothing

*Example:* Two fraud vendors quote AUC 0.75 and 0.92 — the second sorts fraud above legit far more reliably, at any threshold.

**Key point:** Use AUC to compare rankers; it says nothing about which threshold you should run in production.

### Visualization (canvas `c3`, 720×300)

Three ROC curves on one plot: coin flip, weak model, strong model.

- **Title (bold 15px, `#1a5276`, top center):** "Three Rankers: Coin Flip, Weak, Strong".
- **Axes:** padding top 50, bottom 52, left 62, right 170; both axes 0–100%; x ticks 0/25/50/75/100; captions "false-alarm rate, %" (x) and rotated "catch rate, %" (y) in 12px `#444`; L-shaped `#999` axes.
- **Curves:**
  - Coin flip: dashed gray `#6b7280` diagonal (dash 5/4, width 2).
  - Weak (AUC ~0.75): yellow `#c98500` line, width 3, points FPR `[0, 5, 15, 30, 50, 70, 100]` / TPR `[0, 25, 48, 68, 83, 92, 100]`.
  - Strong (our fraud model, AUC ~0.92): blue `#2a78d6` line, width 3, points FPR `[0, 0.53, 2, 6, 20, 50, 100]` / TPR `[0, 20, 40, 70, 90, 98, 100]`.
- **Legend (right side, x=w-155, line swatches):** blue "our model, 0.92"; yellow "weak model, 0.75"; gray "coin flip, 0.50" (12px `#222`).
- **Annotation (blue bold 13px, two lines under the legend):** "closer to the top-left" / "corner = better sort".

## Where ROC Flatters: The Rare-Class Trap

**Tags:** `common mistake` (red), `rule of thumb` (green)

- **Tiny FPR, big pile** — a 2% false-alarm rate sounds tiny, but 2% of 950 legit = 19 false alarms
- **Compare the piles** — at threshold 0.8 the flag pile is 20 frauds + 19 legit: barely half right
- **Worse when looser** — at 0.4 the pile is 45 frauds + 190 legit: only 1 in 5 flags is real
- **ROC can't see it** — its x-axis divides by 950 legit, hiding how false alarms swamp rare frauds
- **The fix** — when positives are rare, read the precision-recall view alongside ROC

*Example:* AUC 0.92 sounds superb, yet at a 90% catch rate 4 of every 5 flagged transactions are innocent.

**Key point:** ROC rates are computed within each class — with 19 legit per fraud, even small false-alarm rates bury the analysts.

### Visualization (canvas `c4`, 720×300)

Two stacked bars showing the composition of the flag pile at two thresholds.

- **Title (bold 15px, `#1a5276`, top center):** "Inside the Flag Pile: Real Frauds vs Innocent Customers".
- **Axes:** padding top 56, bottom 66, left 62, right 200; y scale 0–250 flagged transactions; rotated y caption "flagged transactions" (12px `#444`); L-shaped `#999` axes.
- **Bars (130px wide, centered at 27% and 73% of plot width):** bottom segment green `#008300` = frauds caught, top segment orange `#d95926` = false alarms.
  - Bar 1: TP 20 + FP 19; total label "39 flags" (bold 13px `#222` above); x label "threshold 0.8  (FPR 2%)" (bold 12px); note "barely half the flags are real" (gray 12px). Inline counts: "20" (white bold 12px in green segment), "19" (dark brown `#7a3a10` in orange segment).
  - Bar 2: TP 45 + FP 190; total label "235 flags"; x label "threshold 0.4  (FPR 20%)"; note "only 1 in 5 flags is real". Inline counts: "45" (white) and "190" (white).
- **Legend (right side, x=w-185):** green swatch "real fraud caught", orange swatch "innocent, flagged" (12px `#222`).
- **Annotation (red `#e74c3c` bold 13px, three lines under the legend):** "\"2% FPR\" still means" / "19 angry customers —" / "950 legit dwarf 50 frauds".

## Regeneration instructions

- **Template:** tutorial detail page (tutorials/ style). h1 (no index number) with 2px `#2980b9` bottom border, `.subtitle` paragraph, then four `.card-section` blocks. Each section: `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one `<tr>`: left `<td class="text-col">` (50%) holding `.tags` pills, a `<ul>` of one-line bullets with `<b>` lead terms, an italic `.example` paragraph, and a `.key-point` callout; right `<td class="viz-col">` (50%) holding one canvas.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; ul 0.92rem; `li b` in `#1a5276`; `.example` italic `#555` 0.9rem; `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem; canvases `width:100%`, `1px solid #e0e0e0` border, 4px radius.
- **Tag pills:** `.tag` inline-block, 0.72rem bold, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Chart palette (JS object P):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- **Canvas:** intrinsic 720×300 attributes; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates; all data hardcoded literal arrays (no Math.random). The threshold sweep arrays (thresholds / frauds caught / legit flagged) are shared between c1 and c3. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- In regenerated HTML, any card links use `.html` extensions. No nav bar, no back/home links.
