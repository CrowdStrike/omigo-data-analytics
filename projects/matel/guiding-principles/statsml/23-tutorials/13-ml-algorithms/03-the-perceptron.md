# The Perceptron

**Page type:** detail page (tutorial card-sections: h2 + two-column table.layout, text left 50% / canvas right 50%; one section holds both canvases side by side in a `.viz-pair` flex row)
**HTML title tag:** The Perceptron

**Subtitle:** 1958's spam filter: each word votes with a weight, the email is flagged if the votes cross a threshold — and every mistake nudges the weights

## Four Words, Four Weighted Votes

**Tags:** `core idea` (blue), `running example` (green)

- **The rule** — score an email by adding the weights of the words it contains
- **The votes** — a trained filter here: free +1, winner +1, meeting −1, invoice +1
- **Fire or not** — call it spam if the score > 0, otherwise let it through
- **1958** — Frank Rosenblatt's perceptron, built as room-sized hardware, worked this way
- **One neuron** — inputs × weights → sum → threshold: the original artificial neuron

*Example (italic):* "free winner" scores 1 + 1 = 2 → spam; "meeting invoice" scores −1 + 1 = 0 → let through.

**Key point callout:** **A perceptron:** a weighted vote plus a threshold — nothing more. Every neural network is built by wiring up stacks of exactly this unit.

### Visualization (canvas `c1`, 720×300)

Node-and-edge perceptron diagram scoring the email "free winner".

- **Title (bold 15px, `#1a5276`, top center):** "One Perceptron Scoring the Email \"free winner\""
- **Word nodes:** 4 circles (radius 26) at x=120, y = 70/130/190/250, labeled `free`, `winner`, `meeting`, `invoice`. Present words (free, winner): fill `rgba(0,131,0,0.12)`, stroke green `#008300`, bold green label. Absent words (meeting, invoice): fill `#f4f4f4`, stroke `#bbb`, gray `#6b7280` label.
- **Edges word → sum:** from each word node to the sum node at (400,160); present edges green `#008300` width 3, absent edges `#ccc` width 1.5. Weight labels on edge midpoints (bold 13px): `+1`, `+1`, `−1`, `+1` — green for present, gray `#6b7280` for absent.
- **Caption under word column (gray 12px, two lines):** "words in the email vote;" / "absent words stay silent"
- **Sum node:** circle radius 32 at (400,160), fill `rgba(42,120,214,0.12)`, stroke blue `#2a78d6`, bold 20px "Σ" in blue; below it bold 13px blue "1 + 1 = 2".
- **Edge sum → threshold:** blue `#2a78d6` width 3 horizontal line.
- **Threshold box:** rectangle 110×52 stroked violet `#4a3aa7` near x=600, bold 14px violet text "score > 0 ?" centered.
- **Output:** above the box, bold 16px magenta `#d55181`: "2 > 0 → SPAM"; below the box, gray 12px: "(0 or less → let through)".

## Learning: Nudge the Weights Only on Mistakes

**Tags:** `worked example` (green), `arithmetic` (blue)

- **Start at zero** — all four weights 0; every email scores 0 and is let through
- **Mistake rule** — missed spam: add 1 to each word present; false alarm: subtract 1
- **Mistake 1** — "free winner" spam scored 0 → missed → free and winner go to +1
- **Mistake 2** — "free meeting" (a real email) scores 1 → false alarm → free 0, meeting −1
- **Mistake 3** — "free invoice" scam scores 0 → missed → free +1, invoice +1
- **Done** — weights (1, 1, −1, 1) now get all five training emails right

*Example (italic):* Three mistakes, three nudges — emails it already gets right change nothing.

**Key point callout:** **The perceptron rule:** leave correct answers alone; move the weights one step toward fixing each error. Repeat passes until one is mistake-free.

This section's viz cell holds both canvases side by side in a `.viz-pair` flex row.

### Visualization (canvas `c2a`, 310×300)

Step-line chart of the four weights after each mistake.

- **Title (bold 15px, `#1a5276`, top center):** "Weights, Nudge by Nudge"
- **X axis:** 4 stages labeled `start`, `1`, `2`, `3` (gray 12px), with the axis caption "mistake number" 16px below them. Padding: top 50, bottom 62, left 46, right 100.
- **Y axis:** −2 to +2, tick labels at every integer (`−2`…`+2`, positive values prefixed `+`), gray `#6b7280` 12px; vertical axis line `#999`; light horizontal gridline `#e5e9ef` at y=0.
- **Series (connected lines width 2.5 with 4px dots, each with a small vertical pixel offset so overlapping paths stay visible):**
  - free, green `#008300`, values `[0, 1, 0, 1]`, offset −3
  - winner, blue `#2a78d6`, values `[0, 1, 1, 1]`, offset +3
  - meeting, orange `#d95926`, values `[0, 0, -1, -1]`, offset 0
  - invoice, violet `#4a3aa7`, values `[0, 0, 0, 1]`, offset +7
  - (offsets multiplied by 0.8 in pixels)
- **End labels:** at the right of each line, bold 12px in the series color: "free +1", "winner +1", "meeting −1", "invoice +1".
- **Caption (bottom center, gray 12px):** "only mistakes move the weights"

### Visualization (canvas `c2b`, 310×300)

Bar chart of mistakes per training pass.

- **Title (bold 15px, `#1a5276`, top center):** "Mistakes per Pass"
- **Data:** mistakes `[2, 1, 0]` for passes 1–3; y axis 0–3 with integer tick labels; axis lines `#999`. Padding: top 50, bottom 56, left 46, right 12.
- **Bars:** 56px wide, alpha 0.8; passes 1–2 magenta `#d55181`, pass 3 green `#008300`. Value label bold 14px `#2c3e50` above each bar; "pass 1"/"pass 2"/"pass 3" gray 12px below.
- **Annotation (bold 13px green, centered in the plot's upper area, two lines):** "pass 3: zero mistakes" / "→ training stops"
- **Caption (bottom center, gray 12px):** "one pass = all 5 emails, in order"

## The Ancestor of Every Neural Network

**Tags:** `where it's used` (blue), `big picture` (green)

- **A neuron today** — the same weighted sum, with a soft curve in place of the hard step
- **Logistic cousin** — swap the step for the S-curve and you get logistic regression
- **Stack them** — layers of these units are exactly what "deep learning" makes deep
- **Same learning idea** — nudge weights to cut error: the rule grew into backpropagation
- **Words to pixels** — swap word votes for pixel votes and the same unit reads digits

*Example (italic):* A modern image network is millions of these four-word voters, wired in layers, trained by the same nudge-on-error idea.

**Key point callout:** **Key point:** learn the perceptron and you have the atom of deep learning — everything since 1958 is more units, softer thresholds, and smarter nudging.

### Visualization (canvas `c3`, 720×300)

Side-by-side diagram: one perceptron (left) vs a small multilayer network (right), separated by a dashed vertical divider at x=300 (`#bdc3c7`, dash 4/3).

- **Title (bold 15px, `#1a5276`, top center):** "1958 to Now: the Same Unit, Stacked"
- **Left (single perceptron):** 4 input circles (radius 11, fill `rgba(42,120,214,0.55)`) at x=70, y = 80/130/180/230; thin `#aaa` edges converging to one solid blue `#2a78d6` circle (radius 19) at (200,155); short output edge to the right labeled bold 13px blue "spam?".
  Captions centered at x=165: bold 13px `#1a5276` "one perceptron:", then gray 12px "word votes → one decision".
- **Right (multilayer network):** 4 layers at x = 380/480/580/665 with node y-positions `[70,125,180,235]`, `[95,155,215]`, `[120,190]`, `[155]`; all pairs of adjacent layers fully connected with `rgba(120,120,120,0.4)` width-1 edges; nodes radius 11 colored per layer: `rgba(42,120,214,0.55)`, aqua `#199e70`, violet `#4a3aa7`, magenta `#d55181`.
  Captions centered at x=522: bold 13px `#1a5276` "a neural network: layers of the same voter", then gray 12px "each circle = one perceptron with a softer threshold".

## What One Voter Cannot Learn

**Tags:** `common confusion` (red), `limits` (orange)

- **One straight line** — a single perceptron can only draw one straight boundary
- **XOR** — "free alone or winner alone = spam, both or neither = fine" breaks that
- **Why** — it needs free > 0, winner > 0, yet free + winner ≤ 0: impossible
- **1969** — Minsky and Papert published these limits; perceptron research stalled for years
- **The fix** — hidden layers: two straight lines combined carve out XOR easily
- **Convergence catch** — the nudge rule is guaranteed to finish only if a line exists

*Example (italic):* No two weights can fire for each word alone yet stay silent when both appear together.

**Key point callout:** **The confusion:** the perceptron didn't fail because voting is weak — one straight line is weak. Layers of the same voter remove the limit.

### Visualization (canvas `c4`, 720×300)

2×2 XOR scatter plot with two failed dashed separating lines and side annotations. Plot area is centered (padding: top 52, bottom 58, left 220, right 220); points sit at 15%/85% of each axis.

- **Title (bold 15px, `#1a5276`, top center):** "The XOR Wall: Spam if \"free\" OR \"winner\" — but Not Both"
- **Axes:** L-shaped `#999` axes; x tick labels "no"/"yes" with axis label `"free" present?`; y tick labels "no"/"yes" with rotated axis label `"winner" present?` (all gray 12px).
- **Failed lines (dashed `#bbb`, width 2, dash 7/5):** one vertical at x=0.5, one diagonal from (0,0.95) to (1,0.05) in data coordinates.
- **Points (radius 12):**
  - (0,0) green `#008300`, label "neither: fine"
  - (1,0) magenta `#d55181`, label "\"free\" alone: spam"
  - (0,1) magenta `#d55181`, label "\"winner\" alone: spam"
  - (1,1) green `#008300`, label "both: fine"
  - Labels bold 12px in point color, placed left of left-column points and right of right-column points (±20px).
- **Right-margin annotations:** bold 13px magenta: "no single straight line puts the two" / "pink points alone on one side —"; then bold 13px `#1a5276`: "every cut strands a green point"; lower, bold 12px green: "a hidden layer (two lines," / "combined) solves it".

## Regeneration instructions

- **Template:** tutorials topic-page skeleton (per `tutorials/CLAUDE.md`, modeled on `most-powerful-signals/07-social-graph-connections.html`): h1 + `.subtitle`, then four `.card-section` blocks, each `<h2>` + `table.layout` with `.text-col` (50%) and `.viz-col` (50%); the learning section places canvases `c2a`/`c2b` (310×300 each) side by side inside its single viz cell, wrapped in a `.viz-pair` flex row (`display:flex; gap:10px`, each canvas `flex:1 1 0; min-width:0`).
- **Left column structure per section:** `.tags` pill row, then `<ul>` of one-line bullets each opening with `<b>bold term</b> —`, one italic `.example` paragraph, one `.key-point` callout with a `<strong>` lead.
- **Tag pill classes:** `.tag.blue` bg `rgba(26,82,118,0.12)` text `#1a5276`; `.tag.green` bg `rgba(39,174,96,0.15)` text `#27ae60`; `.tag.red` bg `rgba(231,76,60,0.12)` text `#e74c3c`; `.tag.orange` bg `rgba(230,126,34,0.15)` text `#e67e22`. Pills 0.72rem, weight 600, padding 2px 10px, radius 10px.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px bottom border `#2980b9`; section h2 1.3rem `#1a5276` with 2px bottom border `#2980b9`; `.subtitle` `#666` 0.95rem; bullets 0.92rem with `li b` in `#1a5276`; `.example` italic `#555` 0.9rem; `.key-point` bg `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas JS:** shared palette object `P` (blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`); shared `setup(id)` helper that sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates; shared `circle(ctx,x,y,r,fill,stroke)` helper. All data hardcoded (no `Math.random()`). Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Site palette reference:** #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange.
- In regenerated HTML, any card/grid links use `.html` extensions (this page has no links).
