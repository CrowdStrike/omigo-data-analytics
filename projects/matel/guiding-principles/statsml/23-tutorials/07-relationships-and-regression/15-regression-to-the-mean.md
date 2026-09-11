# Regression to the Mean

**Page type:** detail page (tutorial: card-sections, each a two-column layout table — text left 50%, canvas right 50%)
**HTML title tag:** Regression to the Mean

**Subtitle:** Pick something because it just scored extremely high or low, and its next score will usually be closer to average — with no help at all

## The Worst Store Bounces Back

**Tags:** `core idea` (blue), `running example` (green)

- **The chain** — 20 stores get a 0-100 sales score every quarter; the chain averages about 75
- **The worst** — store #17 scores 58 in Q1, dead last, and is sent a coaching program
- **The rebound** — next quarter it scores 70, and the coach claims all 12 points of the gain
- **The check** — the next five lowest stores got no coaching and still rose from 62 to 70 on average
- **The name** — extreme scores drifting back toward the average is regression to the mean

*Example:* The uncoached low stores gained 8 points on their own — most of the "coaching effect" was coming anyway.

**Key point:** When you select something because its score was extreme, expect the next score to be less extreme with no intervention at all.

### Visualization (canvas `c1`, 720×300)

Paired Q1/Q2 bar chart: coached store vs uncoached low stores.

- **Title (bold 15px, ink `#1a5276`, top center):** "Q1 vs Q2 Score: Coached Store vs Uncoached Low Stores"
- **Scale:** y from 50 to 90 (labels "50" and "90" only), baseline y=252, chart height 180
- **Reference line:** dashed gray (`#6b7280`, dash 5/4, width 1.5) horizontal line at 75, labeled bold 12px "chain average 75"
- **Group 1 (centered x=210):** "store #17 (coached)" — Q1 bar 58 in rgba(42,120,214,0.35), Q2 bar 70 in solid blue `#2a78d6`; bold gain label "+12" above; bar width 62px, "Q1"/"Q2" muted labels below
- **Group 2 (centered x=460):** "5 low stores, no coaching (avg)" — Q1 bar 62 in rgba(25,158,112,0.35), Q2 bar 70 in solid aqua `#199e70`; gain label "+8"
- **Value labels:** bold 13px scores above each bar
- **Annotation (bold 13px orange `#d95926`, below title, centered):** "+8 of the +12 happens without any coaching"

## Skill Plus Luck, by Hand

**Tags:** `worked example` (green), `core idea` (blue)

- **The model** — each quarter's score is true skill plus luck, and luck swings about −10 to +10
- **Why last place** — store #17's skill is about 68; a −10 luck quarter made it print a 58
- **Luck resets** — bad luck does not repeat, so the next score lands back near the skill of 68
- **Both ends** — the top five stores fell from an average of 88 to 82: same drift, other direction
- **The rule** — the more luck inside a number, the harder its extremes snap back

*Example:* Skill 68 with luck −10 prints 58; next quarter luck +2 prints 70 — the store "improved" without changing.

**Try it:** Roll ten dice totals, coach the lowest one by yelling at it, roll again — the lowest usually rises anyway. *(this section's callout is labeled "Try it:" instead of "Key point:")*

### Visualization (canvas `c2`, 720×300)

Slope chart Q1 → Q2: bottom 5 stores rise, top 5 fall, both toward 75.

- **Title (bold 15px, ink, top center):** "Both Extremes Drift Toward the Average of 75"
- **Layout:** two vertical columns at xL=190 ("Q1") and xR=530 ("Q2"), bold 13px column labels below; y scale 55–95, baseline y=250, height 185
- **Reference line:** dashed gray at 75 spanning past both columns, labeled bold 12px "75" at right
- **Bottom set (aqua `#199e70`, lines width 2 at 75% alpha, dots radius 4):** Q1→Q2 pairs `[60→68], [61→69], [62→70], [63→71], [64→72]`
- **Top set (orange `#d95926`):** pairs `[86→80], [87→81], [88→82], [89→83], [90→84]`
- **Side labels (bold 13px):** left: orange "top 5: avg 88", aqua "bottom 5: avg 62"; right: orange "avg 82  (−6)", aqua "avg 70  (+8)"
- **Annotation (bold 13px violet `#4a3aa7`, below title, centered):** "no coaching anywhere — luck re-rolls, extremes shrink"

## Why Coaching Programs Always "Work"

**Tags:** `where it's used` (blue), `common mistake` (red)

- **Selection on extremes** — any fix aimed at the worst performers gets the drift as a free gift
- **Everywhere** — worst schools, sickest patients, slowest servers improve after any intervention
- **Naive read** — the coached store gained 12 points, so coaching gets credit for 12
- **Honest read** — similar uncoached stores gained 8, so coaching earned at most about 4
- **The fix** — compare against untreated cases that were picked by the same extreme rule

*Example:* Speed cameras placed at last year's worst-accident corners look effective even when they do nothing.

**Key point:** Without a control group selected the same way, drift back to average masquerades as impact.

### Visualization (canvas `c3`, 720×300)

Three-bar decomposition of the coached store's gain.

- **Title (bold 15px, ink, top center):** "The Coached Store Gained 12 Points — How Many Were Coaching?"
- **Scale:** y from 0 to 14 (labels "0" and "12" only), baseline y=245, height 170; L-shaped gray axes `#999`
- **Bars (width 90px):**
  - x=130: +12, blue `#2a78d6`, labels "coached store gain" / muted "(58 to 70)"
  - x=330: +8, yellow `#c98500`, labels "drift alone" / "(uncoached low stores)"
  - x=530: +4, green `#008300`, labels "left for coaching" / "(12 − 8)"
- **Value labels:** bold 14px "+12" / "+8" / "+4" above bars
- **Annotation (bold 13px magenta `#d55181`, upper right area):** "the honest coaching estimate is +4, not +12"

## It Is Not a Force Pulling Everyone to Average

**Tags:** `common mistake` (red), `rule of thumb` (blue)

- **Not erasure** — good stores stay mostly good; scores shrink toward 75 but ranks barely shuffle
- **Not gravity** — nothing acts on the store; the luck part of the score simply re-rolls
- **Needs selection** — it only bites when you picked the case because of an extreme noisy score
- **Prediction rule** — forecast next quarter closer to the average than last quarter was

*Example:* A store that scored 90 predicts near 82 next quarter — partway back to 75, not all the way (this one landed at 84).

**Key point:** Regression to the mean shrinks extremes toward the average; it never flips the ordering wholesale.

### Visualization (canvas `c4`, 720×300)

Scatter of Q1 vs Q2 scores for all 20 stores with identity and shrink lines.

- **Title (bold 15px, ink, top center):** "All 20 Stores: Q2 Sits Closer to 75 Than Q1 Did"
- **Axes:** x "Q1 score" and y "Q2 score" (rotated label), both 55–95 with tick labels at 55/75/95; L-shaped gray axes `#999`; padding: left 70, right 40, top 45, bottom 45
- **Identity line:** dashed light gray (`#bbb`, dash 5/4, width 1.5) Q2 = Q1 diagonal, labeled 12px gray "no drift (Q2 = Q1)"
- **Shrink line:** solid violet (`#4a3aa7`, width 2.5) Q2 = 75 + 0.5×(Q1 − 75), from (55,65) to (95,85), labeled bold 12px violet "what happened: halfway back to 75"
- **Points (Q1, Q2), radius 4, blue `#2a78d6`; first point radius 6 magenta `#d55181`:** `[58,70], [60,68], [61,69], [62,70], [63,71], [64,72], [66,71], [68,72], [70,73], [72,74], [74,75], [76,76], [78,77], [80,78], [82,79], [86,80], [87,81], [88,82], [89,83], [90,84]`
- **Point label (bold 12px magenta):** "coached store (58, 70)" next to the magenta point
- **Annotation (bold 13px green `#008300`, top of plot):** "extremes shrink toward 75 — the ordering mostly holds"

## Regeneration instructions

- **Template/layout:** tutorials topic-page skeleton. `<h1>` (no index number) with 2px bottom border `#2980b9`, `.subtitle` paragraph, then four `.card-section` blocks each with an `<h2>` (1.3rem, `#1a5276`, 2px bottom border `#2980b9`) and a `table.layout` (one `<tr>`: `.text-col` 50% / `.viz-col` 50%). All four sections use the 2-column layout; no 3-column rows on this page.
- **Left column structure:** `.tags` row of pill spans first (`.tag.blue` rgba(26,82,118,0.12)/#1a5276, `.tag.green` rgba(39,174,96,0.15)/#27ae60, `.tag.red` rgba(231,76,60,0.12)/#e74c3c, `.tag.orange` rgba(230,126,34,0.15)/#e67e22; 0.72rem, 600 weight, 2px 10px padding, radius 10px), then a `<ul>` of one-line bullets each opening with `<b>` (bold terms colored `#1a5276`), an italic `.example` paragraph (`#555`, 0.9rem), and a `.key-point` callout (background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem) beginning with `<strong>Key point:</strong>` (section 2 uses `<strong>Try it:</strong>`).
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; canvases `width:100%`, 1px solid `#e0e0e0` border, 4px radius; ul 0.92rem.
- **Chart palette (JS object P):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- **Canvas:** all canvases 720×300; this page's `setup(id)` helper hardcodes W=720, H=300, sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`), and calls `ctx.scale` so drawing stays in logical coordinates, and clears the canvas (no white fill).
- No nav bar, no back/home links, no cross-page links. In regenerated HTML, any card links use `.html` extensions.
