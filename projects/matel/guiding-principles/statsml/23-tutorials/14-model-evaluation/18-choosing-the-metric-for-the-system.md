# Choosing the Metric for the System

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Choosing the Metric for the System

**Subtitle:** There is no universally "best" model — accuracy, recall, AUC, and calibration each crown a different winner, so a short decision map about how predictions are USED must pick the metric first

## One Hospital, Three "Best" Models

**Tags:** `core idea` (blue), `metric choice` (green), `no single best` (orange)

- **The task** — a hospital scores 1,000 discharged patients; 80 (8%) will be readmitted in 30 days
- **The action** — nurses can make exactly 200 follow-up calls a month to the highest-scored patients
- **Three models** — A, B, C each score every patient, and the team asks "which model is best?"
- **Three winners** — A tops accuracy (93%), B tops recall@200 (70%), C tops AUC (0.83)
- **No tie-breaker** — the metric decides the winner, so choosing the metric IS the real decision

*Example (italic):* With 200 calls budgeted, model B finds 56 of the 80 readmissions; "most accurate" model A finds only 24.

**Key point:** "Best model" is meaningless until you pick the metric — and picking the metric means deciding what question the system must answer.

### Visualization (canvas `c1`, 720×300)

Grouped bar chart: three metric groups (accuracy, recall@200, AUC×100) with one bar per model, showing a different winner in each group.

- **Title (bold 15px, `#1a5276`, top center):** "Same Three Models, Three Different Winners".
- **Data:** model A `[93, 30, 74]` blue `#2a78d6`; model B `[88, 70, 79]` orange `#d95926`; model C `[91, 55, 83]` aqua `#199e70` — order per group: accuracy %, recall@200 %, AUC×100.
- **Axes:** origin x=60, baseline y=240, chart height 180, y scale 0–100 with gridlines `#e5e9ef` at 25/50/75/100 and 11px `#6b7280` tick labels.
- **Groups:** centered at x=170 ("accuracy"), x=390 ("recall@200"), x=600 ("AUC ×100"); group labels bold 12px `#444` below baseline; bars 40px wide, 8px gap, fills at 0.5 alpha of each model color with a 2px solid top edge.
- **Winners:** bold 13px value label above the winning bar in its solid color with a star: "★ 93" (A, accuracy), "★ 70" (B, recall@200), "★ 83" (C, AUC).
- **Legend (top right, 12px):** solid squares + "A", "B", "C" in the three model colors.
- **Caption (12px `#444`, bottom center):** "recall@200 = share of the 80 readmissions inside the 200 calls (illustrative)".

## Four Questions That Pick the Metric

**Tags:** `worked example` (blue), `decision map` (green)

- **Question 1** — the hospital predicts a class (readmit / not), so number metrics like MAE drop out
- **Question 2** — nurses have a fixed budget of 200 calls, so top-k metrics apply: recall@200 it is
- **The pick** — recall@200 asks: of the 80 readmissions, how many land inside the 200 calls made
- **If no budget** — asymmetric costs would instead push toward recall with a tuned threshold
- **If probabilities matter** — scores shown to doctors as risks would demand calibration + AUC

*Example (italic):* Following the map, the hospital lands on recall@200 in two questions — and model B (70%) wins.

**Key point:** The map turns "which metric?" into three or four yes/no questions about how the predictions are actually used, not about the models.

### Visualization (canvas `c2`, 720×300)

Decision-tree diagram: four question boxes stacked down the left-center, each with a "yes" arrow right to a metric leaf and a "no" arrow down to the next question; the hospital's two-question path is highlighted in green.

- **Title (bold 15px, `#1a5276`, top center):** "The Metric Decision Map (hospital path in green)".
- **Question boxes:** white fill, rounded rect 300×32, centered at x=230, bold 12px centered text:
  - Q1 center (230, 62): "Does the system output a number?" — 2px `#6b7280` border, `#2c3e50` text.
  - Q2 center (230, 126): "Is there a fixed action budget (top-k)?" — 3px `#008300` border, `#008300` text (hospital stop).
  - Q3 center (230, 190): "Is a miss far costlier than a false alarm?" — 2px `#6b7280` border.
  - Q4 center (230, 252): "Are the probabilities used directly?" — 2px `#6b7280` border.
- **Leaf boxes:** rounded rect 240×32, centered at x=580, bold 12px centered text:
  - L1 (580, 62): "MAE / RMSE / MAPE" — 2px `#4a3aa7` border, violet text.
  - L2 (580, 126): "precision@k · recall@k" — fill `rgba(0,131,0,0.12)`, 3px `#008300` border, green text (the pick).
  - L3 (580, 190): "recall + tuned threshold" — 2px `#d95926` border, orange text.
  - L4 (580, 252): "yes: calibration + AUC · no: F1" — 2px `#2a78d6` border, blue text.
- **Yes arrows:** horizontal from each Q right edge (x=380) to leaf left edge (x=460), 2px with small arrowhead, labeled "yes" bold 11px above midpoint; Q2→L2 arrow is 3px `#008300`, the rest `#6b7280`.
- **No arrows:** vertical from each Q bottom to the next Q top (x=230), 2px, labeled "no" bold 11px to the left; Q1→Q2 arrow is 3px `#008300` (hospital path), the rest `#6b7280`.
- **Annotation (bold 12px `#008300`):** "hospital lands here in two questions", placed at (580, 96) just above L2.

## Metrics in Dollars

**Tags:** `where it's used` (blue), `cost of a miss` (orange)

- **The stakes** — a missed readmission costs about $12,000; a follow-up call costs $15 (illustrative)
- **Fixed spend** — 200 calls cost $3,000 a month no matter which model chooses the list
- **Model A** — 93% accurate but misses 56 readmissions: $672,000 a month in miss cost
- **Model B** — 88% accurate yet misses only 24: $288,000 — the accuracy "loser" saves $384,000
- **General rule** — translate each candidate metric into the action's cost; the dollars pick it

*Example (italic):* Ranking models by accuracy instead of recall@200 would quietly cost the hospital $384,000 a month.

**Key point:** A good system metric is the one that moves the real outcome — before choosing, connect every candidate metric to dollars, patients, or minutes.

### Visualization (canvas `c3`, 720×300)

Bar chart of monthly missed-readmission cost per model, showing the accuracy ranking inverted by the cost ranking.

- **Title (bold 15px, `#1a5276`, top center):** "Monthly Cost of Missed Readmissions by Model (illustrative)".
- **Data:** A misses 56 → $672,000; B misses 24 → $288,000; C misses 36 → $432,000 (misses × $12,000).
- **Axes:** origin x=70, baseline y=235, chart height 170, y scale 0–$700k with gridlines `#e5e9ef` at $200k/$400k/$600k and 11px `#6b7280` labels "$200k", "$400k", "$600k".
- **Bars:** 90px wide, centered at x=190 (A), x=390 (B), x=590 (C); fills A `rgba(42,120,214,0.5)`, B `rgba(0,131,0,0.45)`, C `rgba(25,158,112,0.45)`, each with 2px solid top edge in the matching solid color.
- **Value labels:** bold 13px above each bar in the bar's solid color: "$672k", "$288k", "$432k".
- **Below baseline (12px `#444`):** "A — acc 93%", "B — acc 88%", "C — acc 91%" centered under each bar.
- **Annotation (bold 12px `#d55181`, two lines near A's bar top):** "the accuracy winner costs" / "$384k more than B".
- **Caption (12px `#444`, bottom center):** "misses × $12,000; every model spends the same $3,000 on its 200 calls".

## The One-Number Trap

**Tags:** `common mistake` (red), `rule of thumb` (green)

- **One-number trap** — no single metric covers correctness, ranking, calibration, and cost at once
- **Accuracy default** — predicting "no readmission" for all 1,000 scores 92% accuracy, catches 0 of 80
- **Nearly tied** — that empty model trails B by only 4 accuracy points (92% vs 88%) yet helps no one
- **Guardrails** — pick ONE decision metric, plus two or three guardrail metrics that must not degrade
- **Revisit** — when the call budget or miss cost changes, rerun the map; the metric is not forever

*Example (italic):* The always-say-no model scores 92% accuracy yet helps zero patients — the default metric was the trap.

**Common mistake:** Grabbing accuracy (or any single default) without asking how predictions are used — the metric looks fine while the system quietly fails.

### Visualization (canvas `c4`, 720×300)

Grouped bar chart comparing the always-"no" model against model B on accuracy and recall@200: accuracy nearly tied, recall wildly apart.

- **Title (bold 15px, `#1a5276`, top center):** "The Always-'No' Model vs Model B".
- **Data:** accuracy `[92, 88]`, recall@200 `[0, 70]` — first bar always-"no" magenta `#d55181`, second bar model B green `#008300`.
- **Axes:** origin x=60, baseline y=240, chart height 180, y scale 0–100 with gridlines `#e5e9ef` at 25/50/75/100 and 11px `#6b7280` tick labels.
- **Groups:** centered at x=230 ("accuracy") and x=520 ("recall@200"); group labels bold 12px `#444` below baseline; bars 70px wide, 14px gap; fills at 0.45 alpha with 2px solid top edge.
- **Zero bar:** the always-"no" recall bar has height 0 — draw a 3px magenta tick on the baseline at its slot with bold 13px "0" label just above it.
- **Value labels:** bold 13px above each nonzero bar in its solid color: "92", "88", "70".
- **Legend (top right, 12px):** magenta square "always 'no'", green square "model B".
- **Annotation (bold 13px `#d55181`, two lines, centered between the groups):** "4 points of accuracy hide" / "all 56 extra catches".
- **Caption (12px `#444`, bottom center):** "920 of 1,000 patients are not readmitted, so saying 'no' to everyone scores 92%".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Diagram helpers:** the decision map in `c2` uses a small rounded-rect helper (fill, border color/width, centered bold text) and an arrow helper (line + filled triangular head, optional bold 11px label); all node centers, sizes, and colors are as specified above — no layout computation.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
