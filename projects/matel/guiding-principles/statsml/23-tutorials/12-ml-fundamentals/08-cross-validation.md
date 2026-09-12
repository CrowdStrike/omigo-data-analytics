# Cross-Validation

**Page type:** detail page (tutorial: 4 card-sections, each h2 + two-column layout table, text left 50% / canvas right 50%)
**HTML title tag:** Cross-Validation

**Subtitle:** With only 500 rows, don't spend a big pile on one exam — run five rounds where every row gets a turn as exam material, then average the five scores

## 500 Rows, Five Rounds, Every Row Sits the Exam

**Tags:** `core idea` (blue), `running example` (green)

- **The problem** — only 500 rows; holding out 20% for a test leaves little to learn from
- **Cut into 5 folds** — five slices of 100 rows each
- **Round 1** — train on folds 2-5 (400 rows), grade on fold 1 (100 rows)
- **Rounds 2-5** — rotate: each fold takes one turn as the exam
- **Nothing wasted** — every row studies four times and is examined exactly once

*Example:* Five practice exams that together cover the entire textbook, one chapter held out at a time.

**Key point:** **Cross-validation:** rotate which slice is the exam so all 500 rows contribute to both learning and grading — the definition after you've seen the rotation.

### Visualization (canvas `c1`, 720×300)

5×5 grid of cells (rounds × folds) where the exam fold rotates down the diagonal.

- **Title (bold 16px, `#1a5276`, top center):** "5-Fold Rotation: 500 Rows, 100 per Fold".
- **Grid:** origin x=150, y=62; cells 96×30 with gaps 6 (x) and 8 (y). Column headers "fold 1"…"fold 5" (gray `#6b7280` 12px, above). Row labels "round 1"…"round 5" (text `#2c3e50` 12px, right-aligned left of grid).
- **Cells:** exam cell when fold index equals round index — orange `#d95926`, alpha 0.85, white bold 12px text "EXAM"; other cells blue `#2a78d6`, alpha 0.28, ink `#1a5276` 12px text "study".
- **Legend (below grid):** blue swatch (alpha 0.28) "400 rows to learn from"; orange swatch (alpha 0.85) "100 rows graded".
- **Annotation (magenta `#d55181` bold 13px, bottom center):** "every row is exam material exactly once".

## Five Scores Instead of One

**Tags:** `worked example` (green), `arithmetic` (blue)

- **The five grades** — 86, 82, 90, 84, 88 percent, one per round
- **The mean** — (86 + 82 + 90 + 84 + 88) / 5 = 430 / 5 = 86%
- **The spread** — folds range from 82 to 90; typical wobble about ±3 points
- **Report both** — "86% ± 3" says how good and how sure in one line
- **A wide spread is a warning** — the score depends heavily on which rows you got

*Example:* Five quiz grades of 86, 82, 90, 84, 88 tell you far more about a student than any single one.

**Key point:** **Hand-checkable:** the whole method is an average of five ordinary accuracy scores — nothing more exotic than 430 / 5 = 86.

### Visualization (canvas `c2`, 720×300)

Five-bar chart of the round scores with a dashed mean line.

- **Title (bold 16px, `#1a5276`, top center):** "The Five Round Scores, and Their Mean".
- **Bars:** scores `[86, 82, 90, 84, 88]`, one per round, labels "round 1"…"round 5"; colors blue `#2a78d6`, orange `#d95926`, green `#008300`, violet `#4a3aa7`, aqua `#199e70`; fill alpha 0.45, 2px solid stroke; bar width 88, gap 32, first bar x=115.
- **Axes:** y from 75% to 95%, labels every 5% (gray `#6b7280` 12px right-aligned); baseline y=235, chart height 160, left pad 70; L-shaped `#999` axis.
- **Value labels:** bold 13px in each bar's color above bar; round labels 12px `#2c3e50` below.
- **Mean line:** magenta `#d55181` dashed (7/5), width 2, horizontal at y=86% across the plot; bold 13px magenta label "mean 86%" at right end above the line.
- **Annotation (magenta bold 13px, centered 42px below baseline):** "report the pair: 86% ± 3 — folds ranged 82 to 90".

## Why One Lucky Split Can Lie

**Tags:** `where it's used` (blue), `small data` (orange)

- **One split gambles** — with 500 rows, a single 100-row exam is a small, lucky-or-not sample
- **Re-deal and repeat** — ten different single splits of the same data score 79 to 91
- **Same model, same data** — a 12-point swing caused only by which rows landed in the exam
- **CV averages the luck out** — five rotated exams land on 86% ± 3
- **Standard on small data** — under a few thousand rows, CV is the default honest grade

*Example:* Judging a restaurant by one dish, you might hit the best or worst thing on the menu; five dishes tell the truth.

**Key point:** **Why it matters:** a single split can make a mediocre model look great or a good one look broken — cross-validation replaces one lottery ticket with an average.

### Visualization (canvas `c3`, 720×300)

Scatter of ten single-split scores against a shaded CV band.

- **Title (bold 16px, `#1a5276`, top center):** "Ten Single Splits vs One Cross-Validation (illustrative)".
- **Axes:** y from 76% to 94%, labels every 3% (gray 12px); padding top 52, bottom 55, left 70, right 40; L-shaped `#999` axis.
- **CV band (drawn first):** horizontal band from 83% to 89% filled `rgba(0,131,0,0.10)`; dashed green `#008300` center line (7/5, width 2) at 86%.
- **Points:** ten single-split scores `[79, 83, 91, 84, 88, 81, 90, 85, 80, 87]` as orange `#d95926` dots radius 6, spaced 56px apart starting at left pad + 55; each dot has its value (11px `#2c3e50`) above it and "#1"…"#10" (gray 11px) below the axis.
- **Annotations:** orange bold 13px near top-left: "one split says anything from 79 to 91"; green bold 13px at right of the mean line: "CV: 86% ± 3".
- **Caption (gray 12px, bottom center):** "ten different random 400 / 100 splits of the same 500 rows, same model".

## The Confusion: CV Grades the Recipe, Not One Model

**Tags:** `common mistake` (red), `caution` (orange)

- **Five models were trained** — one per round; none of them is "the" model
- **86% grades the recipe** — the training procedure, not any single trained model
- **Final step** — retrain the recipe on all 500 rows; that model ships
- **Don't ship a fold model** — it saw only 400 rows; the full-data model is stronger
- **Prepare inside each round** — scaling or feature picking on all 500 rows leaks exam answers

*Example:* A recipe tested five times in five ovens gets one rating; then you cook the real dinner with all the ingredients.

**Key point:** **The confusion:** people ask "which of the five models do I keep?" — none. Keep the score, retrain on everything, and expect roughly 86% from the result.

### Visualization (canvas `c4`, 720×300)

Flow diagram with two paths: a grading path producing the recipe's score and a shipping path retraining on all data.

- **Title (bold 16px, `#1a5276`, top center):** "The Score and the Shipped Model Come from Different Paths".
- **Boxes** (rounded-rect style: fill at alpha 0.13, 2px stroke, colored 12px text, first line bold):
  - Source at (40, 115, 120×62), ink `#1a5276`: "all 500 rows" / "+ the recipe".
  - Top path: (240, 55, 170×62) orange `#d95926`: "5 rotated rounds" / "5 scores:" / "86 82 90 84 88"; then (490, 55, 190×62) green `#008300`: "the recipe's grade" / "86% ± 3".
  - Bottom path: (240, 180, 170×62) blue `#2a78d6`: "retrain once" / "on all 500 rows"; then (490, 180, 190×62) violet `#4a3aa7`: "the model you ship" / "expect about 86%".
- **Arrows:** filled-head arrows (width 2) from the source box to each path's first box and between boxes: orange to rounds, green rounds→grade, blue to retrain, violet retrain→ship.
- **Path labels (gray 12px):** "grading path" near top-left arrow, "shipping path" near bottom-left arrow.
- **Annotation (magenta `#d55181` bold 13px, bottom center):** "none of the five fold models ships — the score travels, the models are thrown away".

## Regeneration instructions

- **Template:** tutorials topic page (per `tutorials/CLAUDE.md`, social-graph reference skeleton). `<h1>` (no index number), `.subtitle` line, then 4 `.card-section` blocks, each an `<h2>` with 2px `#2980b9` bottom border plus a `table.layout` row: left `td.text-col` (50%) with `.tags` pills, 5 one-line bullets (each opening with `<b>` in `#1a5276`), an italic `.example` line, and a `.key-point` callout; right `td.viz-col` (50%) with one canvas 720×300.
- **Page CSS:** body system-ui sans, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with 2px `#2980b9` bottom border; subtitle `#666` 0.95rem; h2 1.3rem `#1a5276`. `.key-point`: background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem. `.example`: italic `#555` 0.9rem. ul 0.92rem. Canvas: `width:100%`, 1px `#e0e0e0` border, 4px radius.
- **Tag pills:** 0.72rem bold, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Chart palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette anchors: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- **Canvas scaling:** shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates; intrinsic width/height attributes as given per chart. All data arrays hardcoded (no `Math.random()`). Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- No nav bar, no back/home links, no cross-page links. In regenerated HTML, any card links would use `.html` extensions.
