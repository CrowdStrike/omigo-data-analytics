# What a Neuron Computes

**Page type:** detail page (tutorial layout: h1 + subtitle, 4 `.card-section` blocks, each h2 + two-column `table.layout` with text left 50% / canvas right 50%)
**HTML title tag:** What a Neuron Computes

**Subtitle:** A neuron is a weighted vote with a mood filter — multiply each clue by how much it matters, add them up, then run the total through a squashing curve — here, into a 0-to-1 score

## One Neuron, Three Clues, One Spam Score

**Tags:** `core idea` (blue), `running example` (green)

- **The job** — read three yes/no clues about an email and output one spam score, 0 to 1
- **Each clue has a weight** — has "free": +0.4, unknown sender: +0.3, has your name: −0.2
- **Multiply and add** — weight × clue for each wire, then sum everything up
- **Plus a bias** — a fixed −0.1 head start: the neuron's default mood before any evidence
- **Then squash** — the raw sum runs through a curve that maps any number into 0-1

*Example (italic):* An email with "free" from a stranger: 0.4 + 0.3 + 0.0 − 0.1 = 0.6, squashed to a score of 0.65.

**Key point:** A neuron is just multiply, add, squash — a weighted vote of its inputs, filtered through its own mood.

### Visualization (canvas `c1`, 720×300)

Network diagram of a single neuron with email A's numbers flowing through it.

- **Title (bold 15px, ink `#1a5276`, top center):** "Email A Flowing Through the Spam Neuron"
- **Three input nodes** (white circles r=16, colored strokes, bold value inside, gray 12px right-aligned label to the left) at x=165: 'has "free"' value 1 in blue `#2a78d6` at y=80; "unknown sender" value 1 in aqua `#199e70` at y=150; "has your name" value 0 in violet `#4a3aa7` at y=220.
- **Wires** — colored arrows from each input to the sum node at (400,150), each with a bold 13px weight-and-product label at its midpoint: "× 0.4 = 0.4" (blue), "× 0.3 = 0.3" (aqua), "× −0.2 = 0.0" (violet).
- **Bias arrow** — orange `#d95926` arrow from below (y=262 up to the node); bold 13px orange label "bias −0.1" at y=280.
- **Sum node** — circle r=28 at (400,150), fill `rgba(26,82,118,0.10)`, stroke ink; labels "add" and "0.6" inside.
- **Squash box** — arrow to a box at (500, 126) 92×48, fill `#fdf0e6`, stroke orange; bold 13px orange label "squash" plus a small sigmoid glyph drawn inside.
- **Output node** — arrow to circle r=24 at (668,150), fill `rgba(0,131,0,0.12)`, stroke green `#008300`; bold 15px green "0.65" inside; 12px gray "spam score" beneath.
- **Annotation (bold 13px orange, centered at (300,52)):** "sum = 0.4 + 0.3 + 0.0 − 0.1 = 0.6  →  squashed to 0.65: leans spam"

## Three Emails Through the Same Neuron

**Tags:** `worked example` (green), `core idea` (blue)

- **Email A** — "free", stranger, no name: 0.4 + 0.3 + 0.0 − 0.1 = 0.6 → score 0.65
- **Email B** — "free", known sender, your name: 0.4 + 0.0 − 0.2 − 0.1 = 0.1 → score 0.52
- **Email C** — no "free", known sender, your name: 0.0 + 0.0 − 0.2 − 0.1 = −0.3 → score 0.43
- **Evidence balances** — B's "free" is nearly cancelled by the sender knowing your name
- **Negative sums are fine** — the squash turns C's −0.3 into 0.43, leaning "not spam"

*Example (italic):* Redo email B on paper: 0.4×1 + 0.3×0 + (−0.2)×1 + (−0.1) = 0.1 — one line of arithmetic.

**Key point:** The same fixed weights score every email — only the clues change from one email to the next.

### Visualization (canvas `c2`, 720×300)

Two-panel bar chart: raw sums (left, diverging around a zero line) vs squashed scores (right, 0–1 with a 0.5 dashed reference line), split by a dashed divider at x=365.

- **Title (bold 15px, ink, top center):** "Same Neuron, Three Emails: Raw Sum and Final Score"
- **Data:** emails A/B/C; raw sums `[0.6, 0.1, -0.3]`; scores `[0.65, 0.52, 0.43]`; descriptions '"free", stranger' / '"free", knows you' / 'plain, knows you'; bar colors magenta `#d55181`, orange `#d95926`, green `#008300` (fill alpha 0.7).
- **Left panel:** zero line (`#999`) mid-panel (panel x=65 width 260, top y=58 height 170); scale ±0.8 max magnitude; bars 54px wide grow up for positive, down for negative; bold 13px value labels ("0.6", "0.1", "-0.3"), bold email letters and 11px gray descriptions below; panel header 12px gray "raw sum (weights × clues + bias)".
- **Right panel:** baseline at bottom (panel x=410 width 265, same height); bars scaled 0–1; dashed `#bbb` line at 0.5 labeled "0.5"; value labels "0.65", "0.52", "0.43"; header "score after squash (0 to 1)".
- **Annotation (bold 13px orange, bottom of right panel):** 'only A clears 0.5 — B\'s "free" is nearly cancelled by your name'

## Where a Data Scientist Meets This Unit

**Tags:** `where it's used` (blue), `best practice` (green)

- **Logistic regression** — one neuron with a sigmoid squash is exactly that classic model
- **Networks are stacks** — a large network is millions of this identical unit, wired together
- **Weights are learned** — nobody hand-picks 0.4; training nudges weights until scores fit examples
- **Weights are readable** — sign says direction of evidence, size says how loud that clue votes
- **Scores near 0.5** — the evidence roughly cancelled; the neuron is genuinely unsure

*Example (italic):* Seeing "has your name" carry −0.2 tells you the model treats personalization as a mark of legitimacy.

**Key point:** Understand this one unit and you can read simple models directly — and know what deep networks are built from.

### Visualization (canvas `c3`, 720×300)

Sigmoid curve plot with the three emails plotted as points on the curve.

- **Title (bold 15px, ink, top center):** "The Squash: Any Sum In, a 0-to-1 Score Out"
- **Axes:** padding top 52 / bottom 52 / left 62 / right 30; x from −4 to 4 with 12px ticks at −4, −2, 0, 2, 4; y from 0 to 1; gray `#999` L-shaped axis lines. Axis titles 12px gray: "raw sum going in" (bottom center), "score coming out" (rotated left).
- **Reference line:** dashed `#bbb` horizontal line at y=0.5 labeled "score 0.5" (12px `#888`).
- **Curve:** sigmoid `1/(1+e^{-x})` sampled over 100 steps, blue `#2a78d6`, width 3.
- **Points** (r=6 with bold 13px labels): A at (0.6, 0.65) magenta `#d55181` labeled "A: 0.6 → 0.65"; B at (0.1, 0.52) orange `#d95926` labeled "B: 0.1 → 0.52"; C at (−0.3, 0.43) green `#008300` labeled "C: −0.3 → 0.43" (label to the left).
- **Annotation (bold 13px blue, upper left near y=0.93):** "the curve never leaves 0–1 — huge sums just saturate"

## What the 0.65 Is Not

**Tags:** `common mistake` (red), `rule of thumb` (blue)

- **A score, not a guarantee** — 0.65 means "leans spam", not "65% certified spam"
- **Negative weight ≠ bug** — −0.2 is evidence against spam, pulling the score down on purpose
- **Bias ≠ error** — it is the default verdict when all clues are 0, not a mistake term
- **Bias sets the mood** — same clues, same weights, different bias → different verdict
- **The neuron reads numbers** — it never sees the word "free", only the 1 or 0 you fed it

*Example (italic):* Give email A to a trusting neuron (bias −1.5): sum = 0.7 − 1.5 = −0.8, score drops from 0.65 to 0.31.

**Key point:** Weights weigh the evidence, bias sets where "neutral" sits — change either and the same email gets a different score.

### Visualization (canvas `c4`, 720×300)

Two-bar comparison: the same email through two neurons with different biases.

- **Title (bold 15px, ink, top center):** "Email A, Same Weights, Two Different Moods (Biases)"
- **Layout:** padding top 56 / bottom 70 / left 62 / right 30; L-shaped gray axes; y scaled so 0.8 fills the plot height; dashed `#bbb` decision line at 0.5 labeled "0.5 = coin flip" (12px `#888`); rotated 12px gray y-axis label "spam score".
- **Bars** (150px wide, alpha 0.72, centered at 28% and 72% of plot width): left bar value 0.65 in magenta `#d55181`, bold 16px value label "0.65", bold 12px caption "suspicious neuron, bias −0.1" with 12px gray "sum = 0.7 − 0.1 = 0.6" beneath; right bar value 0.31 in aqua `#199e70`, label "0.31", caption "trusting neuron, bias −1.5" with "sum = 0.7 − 1.5 = −0.8".
- **Annotation (bold 13px orange, top center inside plot):** 'identical evidence (0.7), opposite verdicts — the bias moved "neutral"'

## Regeneration instructions

- **Template:** tutorials topic-page layout (social-graph reference style). h1 + `.subtitle`, then 4 `.card-section` blocks; each has an `<h2>` (1.3rem, `#1a5276`, 2px solid `#2980b9` bottom border) and a `table.layout` (`width:100%`, border-collapse collapse) with one row: `td.text-col` (50%) and `td.viz-col` (50%), both `vertical-align: top`, padding 12px.
- **Text column structure:** `.tags` row of pill spans first (`.tag` — inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px; `.tag.blue` bg `rgba(26,82,118,0.12)` color `#1a5276`; `.tag.green` bg `rgba(39,174,96,0.15)` color `#27ae60`; `.tag.red` bg `rgba(231,76,60,0.12)` color `#e74c3c`; `.tag.orange` bg `rgba(230,126,34,0.15)` color `#e67e22`), then a `<ul>` (0.92rem) of one-line bullets each opening with `<b>` in `#1a5276`, one italic `.example` paragraph (`#555`, 0.9rem), and one `.key-point` callout (bg `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem) with a `<strong>Key point:</strong>` lead-in.
- **Page CSS:** global reset; body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem. Canvases styled `width:100%`, border `1px solid #e0e0e0`, radius 4px. No nav bar, no back/home links.
- **Canvas:** each canvas declares `width="720" height="300"`; a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates; a shared `arrow()` helper draws lines with filled triangular heads. All data is hardcoded literal arrays (no `Math.random()`). Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- In regenerated HTML, any card links use `.html` extensions (this page has none — it is a leaf tutorial page).
