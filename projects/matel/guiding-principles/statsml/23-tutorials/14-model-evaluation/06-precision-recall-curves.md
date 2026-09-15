# Precision-Recall Curves

**Page type:** detail page (tutorial card-sections: h2 per section; two-column table.layout 50/50; one section holds two canvases side by side in a `.viz-pair` flex row inside its viz cell)
**HTML title tag:** Precision-Recall Curves

**Subtitle:** The same threshold sweep, replotted as "how many flags are right" vs "how many frauds get caught" — the view that stays honest when fraud is rare

## Same Sweep, Two Honest Questions

**Tags:** `core idea` (blue), `threshold sweep` (orange)

- **Same model** — 1,000 transactions, 50 fraud, one score each, one sliding threshold
- **Precision** — of the transactions we flagged, what fraction were really fraud?
- **Recall** — of the 50 real frauds, what fraction did we manage to flag?
- **The trade** — loosen the threshold and recall climbs while precision crumbles
- **The curve** — one precision-recall point per threshold; joined up, they form the PR curve

*Example:* At threshold 0.8: 39 flags, 20 real → precision 51%; 20 of 50 frauds caught → recall 40%.

**Key point:** Precision judges the flag pile, recall judges the fraud pile — the PR curve prices each in terms of the other.

### Visualization (canvas `c1`, 720×300)

The PR curve of the running fraud model with one labeled point per threshold and a random-flagging baseline.

- **Title (bold 15px, `#1a5276`, top center):** "The Precision-Recall Curve: One Point per Threshold".
- **Data (shared threshold sweep, 1,000 transactions, 50 fraud / 950 legit):** thresholds `["0.9","0.8","0.6","0.4","0.2"]`; recall % `[20, 40, 70, 90, 98]`; precision % `[66.7, 51.3, 38.0, 19.1, 9.4]`. (Underlying counts: flags 15/39/92/235/524; real fraud in flags 10/20/35/45/49.)
- **Axes:** padding top 50, bottom 52, left 62, right 30; both axes 0–100%; x ticks at 0/20/40/60/80/100 (12px `#222`); x caption "recall: frauds caught / 50, %"; rotated y caption "precision: real frauds / flags, %" (12px `#444`); L-shaped `#999` axes.
- **Baseline:** dashed `#bbb` horizontal line (dash 5/4, width 1.5) at precision 5%, labeled in gray `#888` 12px: "flagging at random: 5% precision".
- **Curve:** aqua `#199e70` line, width 3, with 4px aqua dots; the threshold-0.6 point is a 6px orange `#d95926` dot. Each point labeled "t=0.9" … "t=0.2" in 12px `#555`.
- **Annotation (orange bold 13px, mid-plot):** "chasing recall past 70% makes precision crumble".

## One Point by Hand: Threshold 0.6

**Tags:** `worked example` (green)

- **Flag pile** — at threshold 0.6 the model flags 92 transactions in total
- **Inside the pile** — 35 are real frauds, 57 are innocent customers
- **Precision** — 35 / 92 = 38%: barely 2 in 5 flags are real
- **Recall** — 35 / 50 = 70%: we caught 7 of every 10 frauds
- **Missed** — the other 15 frauds scored below 0.6 and sailed through

*Example:* Redo it for threshold 0.4: 235 flags, 45 real → precision 45/235 = 19%, recall 45/50 = 90%.

**Key point:** Every PR point is just two divisions on the same counts — the curve is nothing more mysterious than that.

### Visualization (canvas `c2`, 720×300)

Two stacked bars showing the two denominators at threshold 0.6: the flag pile and the fraud pile.

- **Title (bold 15px, `#1a5276`, top center):** "Threshold 0.6: Two Piles, Two Divisions".
- **Axes:** padding top 56, bottom 66, left 62, right 195; y scale 0–100 transactions; rotated y caption "transactions" (12px `#444`); L-shaped `#999` axes.
- **Bar 1 "the flag pile" (130px wide, centered at 27% of plot):** bottom segment green `#008300` = 35 fraud (white bold 12px inline label "35 fraud"), top segment orange `#d95926` = 57 innocent ("57 innocent"); total label "92 flagged" (bold 13px `#222` above); below the axis: "the flag pile" (bold 12px) and in green bold 12px "precision = 35/92 = 38%".
- **Bar 2 "the fraud pile" (centered at 73%):** bottom segment green = 35 caught ("35 caught"), top segment red `#e74c3c` = 15 missed ("15 missed"); total label "50 real frauds"; below: "the fraud pile" and in blue `#2a78d6` bold 12px "recall = 35/50 = 70%".
- **Legend (right side, x=w-180):** green swatch "fraud, flagged"; orange swatch "innocent, flagged"; red swatch "fraud, missed" (12px `#222`).
- **Annotation (`#1a5276` bold 13px, two lines under the legend):** "same 35 catches," / "two denominators".

## Why ROC Looks Rosy at 5% Fraud — and PR Doesn't

**Tags:** `common mistake` (red), `where it's used` (blue)

- **Same model twice** — both charts plot the identical threshold sweep
- **ROC verdict** — AUC 0.92, hugging the top-left: looks nearly solved
- **PR verdict** — precision 38% at recall 70%, and 19% at recall 90%
- **The reason** — 950 legit vs 50 fraud: rare false alarms still outnumber catches
- **Rule of thumb** — the rarer the positive class, the more PR is the honest view

*Example:* The demo shows the ROC curve; the analyst lives the PR curve — 4 junk alerts per real fraud at 90% recall.

**Key point:** ROC divides false alarms by 950 legit; PR divides them by the flag pile — only PR feels the imbalance.

This row's viz cell holds both canvases side by side in a `.viz-pair` flex row.

### Visualization (canvas `c3a`, 310×340)

ROC view of the sweep (the flattering one).

- **Title (bold 14px, `#1a5276`, top center):** "ROC View: Looks Nearly Solved".
- **Data:** FPR % `[0, 0.53, 2, 6, 20, 50, 100]`; TPR % `[0, 20, 40, 70, 90, 98, 100]`.
- **Axes:** padding top 50, bottom 56, left 46, right 18; both axes 0–100%; x ticks 0/50/100; captions "false-alarm rate, %" (x) and rotated "catch rate, %" (y) in 12px `#444`; L-shaped `#999` axes.
- **Diagonal:** dashed `#bbb` line (dash 5/4) from (0,0) to (100,100).
- **Curve:** blue `#2a78d6` line, width 3, with 3.5px blue dots at each point.
- **Annotation (blue bold 13px near mid-plot):** "AUC ≈ 0.92".

### Visualization (canvas `c3b`, 310×340)

PR view of the same sweep (the honest one).

- **Title (bold 14px, `#1a5276`, top center):** "PR View: The Honest Story".
- **Data:** recall % `[20, 40, 70, 90, 98]`; precision % `[66.7, 51.3, 38.0, 19.1, 9.4]`.
- **Axes:** padding top 50, bottom 56, left 46, right 18; both axes 0–100%; x ticks 0/50/100; captions "recall, %" (x) and rotated "precision, %" (y); L-shaped `#999` axes.
- **Baseline:** dashed `#bbb` line at precision 5%, labeled "random: 5%" (gray `#888` 12px).
- **Curve:** aqua `#199e70` line, width 3, with 3.5px aqua dots.
- **Annotation (red `#e74c3c` bold 13px, two lines, centered near recall 62 / precision 45):** "at 90% recall," / "precision is 19%".

## Picking the Operating Point

**Tags:** `where it's used` (blue), `rule of thumb` (green)

- **One point ships** — production runs a single threshold, not a curve
- **Capacity** — a team that can review ~90 alerts a day fits threshold 0.6 (92 flags)
- **Costs** — costly missed fraud pushes the threshold down; annoyed customers push it up
- **Read it off** — at 0.6 you accept 38% precision to secure 70% recall
- **Revisit** — fraud drifts; this quarter's right point can be wrong next quarter

*Example:* Cutting the threshold from 0.6 to 0.4 catches 10 more frauds but adds 133 more false alarms.

**Key point:** The curve shows the menu; costs and review capacity choose the dish — no metric picks the threshold for you.

### Visualization (canvas `c4`, 720×300)

Precision and recall plotted against the threshold value, with the chosen operating point marked.

- **Title (bold 15px, `#1a5276`, top center):** "Precision and Recall as the Threshold Moves".
- **Data:** thresholds `[0.2, 0.4, 0.6, 0.8, 0.9]`; precision % `[9.4, 19.1, 38.0, 51.3, 66.7]`; recall % `[98, 90, 70, 40, 20]`.
- **Axes:** padding top 50, bottom 52, left 62, right 170; x from 0.1 to 1.0 (threshold), tick labels at each data threshold (0.2, 0.4, 0.6, 0.8, 0.9); y 0–100%; x caption "flag everything scoring above this threshold"; rotated y caption "percent" (12px `#444`); L-shaped `#999` axes.
- **Operating point:** vertical dashed orange `#d95926` line (dash 5/4, width 2) at threshold 0.6, labeled to its right in orange: bold 13px "chosen: 0.6" and 12px "92 alerts fits the team".
- **Series:** recall line blue `#2a78d6`, width 3, 4px dots; precision line aqua `#199e70`, width 3, 4px dots.
- **Legend (right side, x=w-155, line swatches):** blue "recall", aqua "precision" (12px `#222`).
- **Annotation (`#1a5276` bold 12px, three lines under the legend):** "lower threshold:" / "recall up," / "precision down".

## Regeneration instructions

- **Template:** tutorial detail page (tutorials/ style). h1 (no index number) with 2px `#2980b9` bottom border, `.subtitle` paragraph, then four `.card-section` blocks. Each section: `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one `<tr>`: left text `<td>` holding `.tags` pills, a `<ul>` of one-line bullets with `<b>` lead terms, an italic `.example` paragraph, and a `.key-point` callout; right cell(s) holding canvases. Every section uses `.text-col` 50% / `.viz-col` 50%; one section places canvases `c3a`/`c3b` (310×340 each) side by side inside its single viz cell, wrapped in a `.viz-pair` flex row (`display:flex; gap:10px`, each canvas `flex:1 1 0; min-width:0`).
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; ul 0.92rem; `li b` in `#1a5276`; `.example` italic `#555` 0.9rem; `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem; canvases `width:100%`, `1px solid #e0e0e0` border, 4px radius.
- **Tag pills:** `.tag` inline-block, 0.72rem bold, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Chart palette (JS object P):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- **Canvas:** intrinsic width/height attributes as given per chart; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates; all data hardcoded literal arrays (no Math.random). The shared threshold sweep (thresholds 0.9/0.8/0.6/0.4/0.2 → flags 15/39/92/235/524, real fraud 10/20/35/45/49, precision 66.7/51.3/38.0/19.1/9.4%, recall 20/40/70/90/98%) drives c1, c3b, and c4; the ROC arrays drive c3a. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- In regenerated HTML, any card links use `.html` extensions. No nav bar, no back/home links.
