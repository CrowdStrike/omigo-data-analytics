# Learning to Rank

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Learning to Rank

**Subtitle:** When the answer is an ordering — which result goes first, second, third — you don't grade the model on its scores, you grade it on the order those scores produce

## Which Recipe Goes First?

**Tags:** `core idea` (blue), `ordering` (green), `search results` (orange)

- **The search** — a recipe app gets the query "chocolate cake" and must decide which of four recipes to show first
- **The candidates** — A classic chocolate cake, B chocolate mug cake, C vanilla cake, D chocolate cookies
- **No single column works** — sort by star rating and vanilla cake (4.9 stars) beats every chocolate recipe
- **What users say** — out of 100 searches, clicks go 46 to A, 27 to B, 15 to C, and 4 to D
- **The task** — learn a scoring rule from those clicks so that sorting by score reproduces the order users want

*Example (italic):* Sorting by rating puts vanilla cake (4.9) on top; users overwhelmingly pick the classic chocolate cake — the rating column alone cannot see the query.

**Key point:** Learning to rank means the model is judged by the order it produces for a query, not by how pretty any single score looks.

### Visualization (canvas `c1`, 720×300)

Bump chart: two columns of four ranked slots — left "sorted by star rating alone", right "order users actually click" — with a colored line per recipe connecting its slot on each side, making the rearrangement visible.

- **Title (bold 15px, `#1a5276`, top center):** "One Query, Two Orders: Star Rating vs What Users Click".
- **Columns:** left slot column centered at x=200, right at x=520; bold 13px `#2c3e50` column headers at y=65: "sorted by star rating" and "order users click"; four slot rows at y = 105, 150, 195, 240; 12px `#6b7280` slot numbers "1."–"4." at x=120 and x=650.
- **Left column (top to bottom):** "C vanilla cake — 4.9★", "D chocolate cookies — 4.8★", "A classic chocolate cake — 4.6★", "B chocolate mug cake — 4.2★"; each a 12px label beside a 7px dot in the recipe's color.
- **Right column (top to bottom):** "A classic chocolate cake — 46 clicks", "B chocolate mug cake — 27", "C vanilla cake — 15", "D chocolate cookies — 4"; same dot style.
- **Recipe colors:** A blue `#2a78d6`, B green `#008300`, C orange `#d95926`, D mute `#6b7280`.
- **Connecting lines:** 2.5px line per recipe from its left dot to its right dot in the recipe's color; A and B cross upward over C and D.
- **Annotation (bold 12px orange `#d95926`, centered near x=360, y=282):** "the best-rated recipe is the wrong first answer — the order must be learned".
- **Caption (11px `#444`, bottom right):** "ratings and clicks illustrative".

## Counting the Swapped Pairs

**Tags:** `worked example` (blue), `pairwise` (green)

- **The labels** — clicks give each recipe a usefulness grade for this query: A gets 3, B gets 2, C gets 1, D gets 0
- **The model's try** — its scores are B 2.9, A 2.7, D 1.2, C 1.0, so sorting gives the order B, A, D, C
- **Check by pairs** — four recipes make 6 pairs; for each pair ask one question: did the better recipe score higher?
- **The tally** — B over D, B over C, A over D, A over C are right; B over A and D over C are swapped: 4 of 6
- **The training trick** — pairwise methods nudge the scores to fix each swapped pair, one comparison at a time

*Example (italic):* A (grade 3) sits below B (grade 2), so training says "raise A's score, lower B's" — the exact score values never enter the complaint, only who beat whom.

**Key point:** 4 of 6 pairs in the right order is a pairwise accuracy of 67% — and swapped pairs are exactly what a pairwise ranker trains on.

### Visualization (canvas `c2`, 720×300)

Pair checklist: the model's ranked list across the top, then six rows — one per recipe pair — each marked correct (green check) or swapped (red cross), so the reader can re-count 4 of 6 by eye.

- **Title (bold 15px, `#1a5276`, top center):** "The Model's Order B, A, D, C — Checked One Pair at a Time".
- **Ranked list strip (y=70):** four rounded 90×26 boxes left to right at x = 150, 270, 390, 510: "B (grade 2)", "A (grade 3)", "D (grade 0)", "C (grade 1)"; fill `rgba(42,120,214,0.12)`, 1.5px `#2a78d6` border, bold 12px `#1a5276` text; 11px `#6b7280` slot labels "1st"–"4th" above each box.
- **Pair rows (y = 115, 141, 167, 193, 219, 245), each a 12px `#2c3e50` label starting at x=90:**
  - "A vs B — grade 3 vs 2 — model put B first" → red cross, bold 12px `#e74c3c` "swapped"
  - "A vs C — grade 3 vs 1 — A placed higher" → green check, 12px `#008300` "correct"
  - "A vs D — grade 3 vs 0 — A placed higher" → green check, "correct"
  - "B vs C — grade 2 vs 1 — B placed higher" → green check, "correct"
  - "B vs D — grade 2 vs 0 — B placed higher" → green check, "correct"
  - "C vs D — grade 1 vs 0 — model put D first" → red cross, bold 12px `#e74c3c` "swapped"
- **Marks:** 13px bold check "✓" in `#008300` or cross "✕" in `#e74c3c` at x=560; verdict word at x=590.
- **Annotation (bold 13px green `#008300`, near x=90, y=278):** "4 of 6 pairs right — pairwise accuracy 67%".

## Why the Top Slot Is Worth the Most

**Tags:** `where it's used` (blue), `position bias` (orange), `NDCG` (green)

- **Position bias** — searchers read from the top and often stop after one or two results; slot 1 does most of the work
- **The discount** — DCG-style scoring pays position p a credit of 1/log2(p+1): weights 1.00, 0.63, 0.50, 0.43
- **Ideal score** — the perfect order A, B, C, D earns 3×1.00 + 2×0.63 + 1×0.50 + 0×0.43 = 4.76
- **A swap at the top** — swapping A and B (slots 1 and 2) drops the score to 4.39, a loss of 0.37
- **The same swap low down** — swapping C and D (slots 3 and 4) drops it only to 4.69, a loss of 0.07
- **Where it lives** — web search, product search, feeds, and ads: anywhere the output the user sees is a list

*Example (italic):* The same neighbouring swap costs 0.37 points at the top of the list but only 0.07 at the bottom — roughly five times cheaper.

**Key point:** Ranking metrics weight mistakes by position, so a good ranker spends its effort getting the top of the list right and forgives noise at the bottom.

### Visualization (canvas `c3`, 720×300)

Vertical bar chart: DCG of three orderings of the same four recipes — perfect, top pair swapped, bottom pair swapped — with the position-credit weights listed beside the bars, showing the top swap costing five times more.

- **Title (bold 15px, `#1a5276`, top center):** "Same Swap, Different Cost: DCG of Three Orderings".
- **Axes:** origin x=70, baseline y=245, plot width 430, plot height 180; y = DCG 0 to 5 with 12px `#444` tick labels at 0–5 and light `#e5e9ef` gridlines; no x-axis line beyond the baseline.
- **Bars (90px wide, centered at x = 150, 290, 430):** "perfect A, B, C, D" height for 4.76 in green `#008300` fill `rgba(0,131,0,0.35)`; "A/B swapped (top)" 4.39 in orange `#d95926` fill `rgba(217,89,38,0.30)`; "C/D swapped (bottom)" 4.69 in blue `#2a78d6` fill `rgba(42,120,214,0.30)`; each bar 2px border in its solid color, bold 13px value label ("4.76", "4.39", "4.69") above the bar top, 12px `#444` two-line name labels below the baseline.
- **Loss brackets:** thin dashed `#6b7280` (dash 4/3) horizontal reference line at 4.76 across the plot; bold 12px orange "−0.37" beside the middle bar's gap to the line, bold 12px blue "−0.07" beside the right bar's gap.
- **Side panel (x=540–700):** 12px `#2c3e50` header "credit by position", then four 12px lines "slot 1 — 1.00", "slot 2 — 0.63", "slot 3 — 0.50", "slot 4 — 0.43".
- **Annotation (bold 12px orange `#d95926`, near x=540, y=220):** two lines: "the top swap costs" / "about 5× the bottom one".
- **Caption (11px `#444`, bottom right):** "grades 3, 2, 1, 0; credits 1/log2(p+1) — illustrative".

## Good Scores, Bad Order

**Tags:** `common mistake` (red), `pointwise vs pairwise` (orange)

- **The trap** — treating ranking as regression: predict each grade accurately and hope the order follows
- **Close but wrong** — one model predicts A 2.7, B 2.8, C 1.0, D 0.1; average miss just 0.3, yet B tops A
- **Far but right** — another predicts A 90, B 70, C 40, D 10 on a wildly wrong scale, yet the order is perfect
- **Sorting forgives scale** — sorting only compares scores to each other, so absolute values wash out entirely
- **The three families** — pointwise fits the grades, pairwise fixes the swaps, listwise optimizes the list metric

*Example (italic):* The model that missed every grade by dozens of points beats the one that missed by 0.3, because only the order ever reaches the user.

**Common mistake:** Judging a ranker by score error (RMSE). A tiny score error can still swap the top two results — evaluate with ranking metrics like pairwise accuracy or NDCG instead.

### Visualization (canvas `c4`, 720×300)

Two horizontal number lines, one per model: each recipe drawn as a labeled dot at its predicted score, so the reader sees the close-scores model ordering B above A while the far-scores model gets the order exactly right.

- **Title (bold 15px, `#1a5276`, top center):** "Score Accuracy Is Not Order Accuracy".
- **Row 1 (axis at y=125, from x=150 to x=670, score scale 0 to 3):** left 12px `#444` two-line label at x=20: "close scores," / "wrong order"; tick labels "0", "1", "2", "3" (12px `#444`); 7px dots with bold 12px labels above: D at 0.1 (`#6b7280`), C at 1.0 (orange `#d95926`), A at 2.7 (blue `#2a78d6`), B at 2.8 (green `#008300`); labels show "D 0.1", "C 1.0", "A 2.7", "B 2.8" with A and B staggered to avoid overlap; bold 12px `#e74c3c` verdict at x=150, y=80: "order B, A, C, D — top pair swapped (avg miss 0.3)".
- **Row 2 (axis at y=235, same x range, score scale 0 to 100):** left label: "far scores," / "right order"; tick labels "0", "25", "50", "75", "100"; dots D at 10, C at 40, B at 70, A at 90, same recipe colors, labels "D 10", "C 40", "B 70", "A 90"; bold 12px `#008300` verdict at x=150, y=190: "order A, B, C, D — perfect".
- **True grades reminder (11px `#6b7280`, top right under the title):** "true grades: A 3, B 2, C 1, D 0".
- **Annotation (bold 13px violet `#4a3aa7`, centered near x=360, y=285):** "sorting keeps only the order — the scale washes out".
- **Caption (11px `#444`, bottom right):** "predicted scores illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all positions, scores, grades, click counts, and DCG values are the hardcoded literals above (no randomness); DCG figures are true sums of grade × 1/log2(p+1) rounded to 2 decimals; text numbers and chart numbers must stay identical.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
