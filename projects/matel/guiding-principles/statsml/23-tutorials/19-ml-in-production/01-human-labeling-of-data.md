# Human Labeling of Data

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Human Labeling of Data

**Subtitle:** Training labels come from people, and people disagree more than you expect — inter-annotator agreement measures how much of your "ground truth" is actually agreed on

## Three People, Same Reviews, Different Labels

**Tags:** `core idea` (blue), `labels are opinions` (green), `why they disagree` (orange)

- **The store** — a pet-supply shop hires Anna, Ben, and Cara to label reviews "complaint" or "not"
- **Easy ones** — "the leash snapped in a week" is a complaint; "fast shipping, happy dog" is not
- **Hard ones** — "great, my 'chew-proof' toy lasted one afternoon" reads as praise or as sarcasm
- **The split** — on 12 shared reviews the three agree unanimously on 8; the other 4 get mixed labels
- **Why they disagree** — sarcasm, mixed feelings, vague instructions, and honest borderline cases
- **The lesson** — "ground truth" is a committee's opinion; measure how often the committee agrees

*Example (italic):* A review saying "third order this month, and the third late delivery" got two complaint votes and one not — the sentence genuinely carries both.

**Key point:** Labels are human judgments, not facts of nature — before trusting them as truth, measure how often independent labelers reach the same one.

### Visualization (canvas `c1`, 720×300)

Label grid: 3 annotator rows × 12 review columns, each cell colored by the label given, with the four split columns outlined so the disagreement is visible at a glance.

- **Title (bold 15px, `#1a5276`, top center):** "Three Labelers, Twelve Reviews: 8 Unanimous, 4 Split".
- **Grid:** 12 columns × 3 rows of 44×44 cells with 4px gaps; grid origin x=120, row tops at y=80, 128, 176; 11px `#6b7280` column headers "r1"–"r12" centered above each column at y=72; 12px `#444` row labels "Anna", "Ben", "Cara" right-aligned at x=110, vertically centered per row.
- **Label data (C = complaint, N = not):** Anna `["N","N","C","N","C","C","N","N","C","N","N","C"]`; Ben `["N","N","C","N","C","N","N","N","C","C","N","C"]`; Cara `["N","N","C","N","N","C","N","N","C","N","N","N"]`.
- **Cell style:** complaint fill orange `rgba(217,89,38,0.85)`, not-complaint fill blue `rgba(42,120,214,0.85)`; bold 12px white "C" or "N" centered in each cell.
- **Split columns (r5, r6, r10, r12):** 2px magenta `#d55181` rounded outline around the full 3-cell column; bold 11px magenta "split" label centered below each at y=240.
- **Legend (bottom left, y=272):** two 12×12 swatches (orange, blue) with 12px `#444` labels "complaint" and "not a complaint".
- **Annotation (bold 12px magenta `#d55181`, right-aligned at x = width−15, y=262):** "4 of 12 reviews split the room — all sarcastic or mixed".
- **Caption (12px `#444`, bottom right):** "illustrative — 12 of the shop's reviews".

## Scoring Agreement Beyond Luck: Cohen's Kappa

**Tags:** `worked example` (blue), `cohen's kappa` (green)

- **Two labelers** — Anna and Ben each label the same 100 reviews as complaint or not complaint
- **The table** — both say complaint on 20, both say not on 60; they clash on the remaining 20
- **Raw agreement** — they match on 20 + 60 = 80 of 100 reviews, so observed agreement is 0.80
- **Agreement by luck** — each calls 30% complaint; random guessers match 0.3×0.3 + 0.7×0.7 = 0.58
- **Cohen's kappa** — (0.80 − 0.58) / (1 − 0.58) = 0.22 / 0.42 ≈ 0.52: agreement earned beyond luck
- **Reading it** — 1 is perfect, 0 is coin-flip luck; 0.52 is moderate — usable but noticeably noisy

*Example (italic):* Of the 42 points of agreement not owed to luck, Anna and Ben earned 22 — kappa 0.52 says they captured about half the possible skill.

**Key point:** kappa = (observed − chance) / (1 − chance) = (0.80 − 0.58) / (1 − 0.58) ≈ 0.52 — agreement with the lucky matches subtracted out.

### Visualization (canvas `c2`, 720×300)

Two-part panel: the 2×2 agreement table on the left with agree/clash cells tinted, and on the right a 0–100% scale bar splitting the 80% raw agreement into luck's share and earned skill.

- **Title (bold 15px, `#1a5276`, top center):** "Anna vs Ben on 100 Reviews: 80% Agreement, kappa 0.52".
- **Table (left):** 2×2 cells of 80×62 at origin x=130, y=95; 11px `#444` column headers "Ben: complaint", "Ben: not" above; 11px `#444` row labels "Anna: complaint", "Anna: not" right-aligned at x=122; counts bold 16px `#2c3e50` centered: row 1 `[20, 10]`, row 2 `[10, 60]`; diagonal (agree) cells filled `rgba(0,131,0,0.18)`, off-diagonal (clash) cells filled `rgba(217,89,38,0.18)`.
- **Table footers (12px, y=250, under the table):** green `#008300` "agree on 80" and orange `#d95926` "clash on 20", side by side.
- **Scale bar (right):** horizontal bar x=380 to x=680 (width 300, so 3px per point), y=140, 26px tall, mapping 0–100%; segment 0–58 fill `rgba(107,114,128,0.35)` with 12px `#6b7280` label "luck: 58%" inside; segment 58–80 fill green `#008300` with bold 12px white label "earned: 22"; segment 80–100 fill `#e5e9ef` with 11px `#6b7280` label "missed: 20" above; 11px `#444` tick labels "0%", "58%", "80%", "100%" below their boundaries.
- **Annotation (bold 13px green `#008300`, near x=358, y=215):** "kappa = 22 / 42 ≈ 0.52 — about half the possible skill".
- **Caption (12px `#444`, bottom right):** "illustrative — 100 double-labeled reviews".

## Noisy Labels Set the Model's Ceiling

**Tags:** `where it's used` (blue), `label quality` (green), `model ceiling` (orange)

- **Training data** — the model learns from these labels; whatever noise is in them, it learns too
- **The ceiling** — 80%-agreement labels cap honest accuracy; higher scores mostly fit labeler quirks
- **False plateau** — accuracy flatlines and the team blames the model, when the labels are the cap
- **Wasted months** — teams tune architectures and features when the real fix is the labeling guide
- **The fix** — measure kappa before training; if it is low, rewrite the guidelines and re-label
- **Adjudication** — send the split cases to a third labeler or a discussion round to settle them

*Example (italic):* The pet-store complaint model plateaued at 81% accuracy through every redesign — its labelers only agreed with each other 80% of the time.

**Key point:** Model accuracy is capped by label quality: past the annotators' own agreement level, the model is fitting their disagreements, not the truth.

### Visualization (canvas `c3`, 720×300)

Line chart of model accuracy against training-set size, flattening under a dashed ceiling drawn at the annotators' agreement level.

- **Title (bold 15px, `#1a5276`, top center):** "More Data Stops Helping: the Labels Set the Ceiling".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 190; x = six evenly spaced category ticks labeled "1k", "2k", "5k", "10k", "20k", "50k" (12px `#444`) titled "labeled reviews" (12px `#6b7280`, centered below); y = accuracy 60% to 90%, light `#e5e9ef` gridlines at 65, 70, 75, 80, 85 with 12px `#444` labels.
- **Accuracy line:** blue `#2a78d6` 3px line through hardcoded values `[65, 70, 75, 78, 80, 81]` at the six ticks, 5px blue dots at each point; 12px blue label "model accuracy" near the third point.
- **Ceiling:** horizontal dashed magenta `#d55181` (dash 6/4) line at 80% across the plot; bold 12px magenta label above its left end (left-aligned at x=68): "annotator agreement ceiling ≈ 80%".
- **Annotation (bold 12px orange `#d95926`, near x=420, y=150):** two lines: "past 10k examples the labels," / "not the data, are the bottleneck".
- **Caption (12px `#444`, bottom right):** "illustrative — accuracy vs training-set size".

## High Agreement Can Still Be Mostly Luck

**Tags:** `common mistake` (red), `rare classes` (orange)

- **A new task** — flag reviews that mention a safety hazard; only about 4 in 100 reviews do
- **Impressive number** — two labelers agree on 94 of 100 reviews, and 94% sounds near-perfect
- **Luck's share** — both say "no hazard" 96% of the time, so luck alone matches 0.9232 of pairs
- **Kappa's verdict** — (0.94 − 0.9232) / (1 − 0.9232) ≈ 0.22: on the rare class they barely agree
- **The mistake** — quoting raw percent agreement on rare-class tasks, where luck does the lifting

*Example (italic):* The hazard labelers agreed on 94 of 100 reviews yet split on 3 of the 4 true hazards — the 94% was almost entirely shared "no" votes.

**Common mistake:** Trusting raw percent agreement. When one class is rare, nearly all pairs agree by luck alone — kappa is the number to report.

### Visualization (canvas `c4`, 720×300)

Grouped bar chart: raw agreement vs kappa for the balanced complaint task and the rare hazard task, showing raw agreement rising while kappa collapses.

- **Title (bold 15px, `#1a5276`, top center):** "94% Agreement Can Be Worse Than 80%".
- **Axes:** origin x=90, baseline y=245, plot width 560, plot height 185; y = 0 to 100 with light `#e5e9ef` gridlines at 20, 40, 60, 80 and 12px `#444` labels.
- **Groups:** two groups centered at x=240 and x=530, each with a 12px `#444` label below the baseline: "complaints — 30% of reviews" and "safety hazards — 4% of reviews".
- **Bars (70px wide, 20px gap within a group):** raw agreement in blue `rgba(42,120,214,0.8)` at heights `[80, 94]`; kappa (×100) in green `rgba(0,131,0,0.8)` at heights `[52, 22]`; bold 13px value labels above each bar: "80%", "0.52", "94%", "0.22" (blue over blue bars, green over green bars).
- **Legend (top right, 12px `#444`):** blue swatch "raw agreement", green swatch "kappa".
- **Annotation (bold 13px magenta `#d55181`, centered near x=385, y=80):** "raw agreement up, real agreement down — luck did the lifting".
- **Caption (12px `#444`, bottom right):** "illustrative — kappa drawn on a 0–100 scale".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all label arrays, counts, and bar heights are the hardcoded literals above (no randomness); kappa values follow from the stated 2×2 tables — balanced task `[[20,10],[10,60]]` gives 0.80/0.58/0.52, rare task `[[1,3],[3,93]]` gives 0.94/0.9232/0.22 — and text numbers must keep matching chart numbers.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
