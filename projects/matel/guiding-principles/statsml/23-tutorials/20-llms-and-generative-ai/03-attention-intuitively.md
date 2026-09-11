# Attention, Intuitively

**Page type:** detail page (tutorial layout: `.card-section` blocks, each a two-column `table.layout` — text left 50%, canvas right 50%)
**HTML title tag:** Attention, Intuitively

**Subtitle:** When a model produces a word, it looks back at all the words it has and decides how much each one matters — a set of learned "where should I look?" weights

## Translating "the red car" — Each Output Word Looks Back

**Tags:** `core idea` (blue), `running example` (green)

- **The task** — turn "the red car" into French: "la voiture rouge"
- **The flip** — French puts the color after the noun, so output word 3 comes from input word 2
- **Attention** — each output word puts weights on the input words: "where should I look?"
- **rouge looks at red** — weight 0.88 on "red", almost nothing on "the" and "car"
- **voiture looks at car** — weight 0.85 on "car"; position in the sentence didn't matter

*Example:* While writing "rouge", the model barely glances at "car" — its eyes are on "red".

**Key point:** Attention is a set of learned weights saying how much each existing word matters for the word being produced right now.

### Visualization (canvas `c1`, 720×300)

Alignment diagram: two rows of word boxes (English input above, French output below) connected by lines whose thickness encodes attention weight.

- **Title (bold 15px `#1a5276`, top center):** "Every Output Word Draws From the Input — Thicker Line = More Attention".
- **Input words (y=80):** "the" x=210, "red" x=360, "car" x=510 — boxes outlined in ink `#1a5276`, white fill, bold 15px text.
- **Output words (y=220):** "la" x=210 blue `#2a78d6`, "voiture" x=360 green `#008300`, "rouge" x=510 magenta `#d55181` — boxes outlined/labeled in their color.
- **Weights matrix (output × input):** la `[0.80, 0.08, 0.12]`; voiture `[0.05, 0.10, 0.85]`; rouge `[0.05, 0.88, 0.07]`.
- **Lines:** one per output-input pair, colored by the output word's color, line width `1 + weight × 9`, alpha 0.9 when weight ≥ 0.5, else 0.3; drawn behind the boxes.
- **Side labels (12px mute `#6b7280`, left, x=40):** "English input" beside the top row, "French output" beside the bottom row.
- **Weight labels on the two strong crossing lines:** bold 12px "0.88" in magenta at (452, 150); "0.85" in green at (420, 178).
- **Caption (bold 14px orange `#d95926`, bottom center):** "the crossing lines are the word-order flip — attention handles it without moving anything".

## The Heat Grid, Row by Row

**Tags:** `worked example` (green), `core idea` (blue)

- **Rows = output words**, columns = input words; each cell = how much attention
- **Row "la"** — the 0.80, red 0.08, car 0.12
- **Row "voiture"** — the 0.05, red 0.10, car 0.85
- **Row "rouge"** — the 0.05, red 0.88, car 0.07
- **Each row sums to 1.00** — one unit of focus, divided across the inputs
- **Darker = more weight** — the bright path through the grid is the translation alignment

*Example:* Check the "voiture" row by hand: 0.05 + 0.10 + 0.85 = 1.00 — a full unit of focus, split three ways.

**Key point:** An attention pattern is just a table of weights, one row per word produced. The weights here are illustrative.

### Visualization (canvas `c2`, 720×300)

3×3 attention heatmap grid with numeric cell values, row sums, and a shade legend.

- **Title (bold 15px `#1a5276`, top center):** "The Attention Grid: Rows Are Output Words, Columns Are Input Words".
- **Columns (input, English):** the, red, car — bold 13px ink headers above. **Rows (output, French):** la, voiture, rouge — bold 13px ink labels right-aligned left of the grid.
- **Weights:** la `[0.80, 0.08, 0.12]`; voiture `[0.05, 0.10, 0.85]`; rouge `[0.05, 0.88, 0.07]`.
- **Cells:** 62px square, grid origin (240, 70); fill `rgba(42,120,214, 0.06 + weight × 0.9)` with 2px white borders; centered weight text (2 decimals), white bold when weight ≥ 0.5, else `#2c3e50`.
- **Row sums:** bold green `#008300` "= 1.00" to the right of each row.
- **Axis captions (12px mute):** "input words (English)" above the column headers; "output words (French)" rotated −90° left of the row labels.
- **Legend:** 5 vertical swatches right of the grid stepping alpha from 0.06 to 0.96 of `rgba(42,120,214,…)`, labeled "0 — ignore" (top) and "1 — full focus" (bottom).
- **Caption (bold 14px orange, bottom center):** "every row spends exactly 1.00 of focus — the dark cells trace the alignment".

## Where a Data Scientist Meets It: "it" Means What?

**Tags:** `where it's used` (blue), `meaning, not distance` (orange)

- **Pronouns** — "The car wouldn't fit in the garage because it was too big" — what is "it"?
- **Attention answers** — from "it": car 0.72, garage 0.15, rest 0.13 → "it" is the car
- **Swap one word** — "…because it was too small" → garage 0.68, car 0.18: weights flip
- **Long documents** — attention lets token 5,000 pull directly from token 12, no relay race
- **Transformers** — GPT-style models are, at heart, stacks of these attention layers

*Example:* Changing "big" to "small" moved the model's gaze from the car to the garage.

**Key point:** Attention is the mechanism that lets meaning, not distance, decide which words connect.

### Visualization (canvas `c3`, 720×300)

Two side-by-side horizontal-bar panels showing attention weights from "it" under the two sentence endings, split by a dashed divider.

- **Title (bold 15px `#1a5276`, top center):** 'Attention From "it": One Swapped Word Flips the Weights'.
- **Subtitle (12px mute):** '"The car wouldn\'t fit in the garage because it was too ___"  (weights illustrative)'.
- **Divider:** vertical dashed `#bdc3c7` line (dash 4/3) at x=360 from y=56 to h−40.
- **Left panel (x0=20):** heading '“… too big”' in bold blue `#2a78d6`; bars for car / garage / other words = `[0.72, 0.15, 0.13]`; winner "car" bar in blue, others light gray `#c9d2dc`; verdict below in bold blue: '"it" = the car'.
- **Right panel (x0=390):** heading '“… too small”' in bold aqua `#199e70`; values `[0.18, 0.68, 0.14]`; winner "garage" bar in aqua; verdict: '"it" = the garage'.
- **Bar geometry:** labels right-aligned (winner label bold), bars 20px tall, max width 180, row height 38, top y=96, bold value labels after bars.
- **Caption (bold 14px orange, bottom center):** "same sentence shape, same distances — only the meaning changed, and the weights followed".

## The Confusion: It's Not Reading One Word at a Time

**Tags:** `common mistake` (red), `mechanism` (blue)

- **Myth** — the model reads word by word, keeping a fading summary in memory
- **Reality** — each new word attends to all earlier words at once, in parallel
- **No fading memory** — word 1 is as reachable as word 99; weights decide, not recency
- **Many heads** — dozens of attention patterns run side by side: nouns, syntax, names
- **Learned, not programmed** — nobody wrote "look at red"; the weights emerge from training

*Example:* The grid above is one head's view — a real model computes dozens of such grids in every layer.

**Key point (labeled "Common mistake:"):** Picturing attention as memory. It is recomputed from scratch for every new token — a fresh set of glances, not a stored summary.

### Visualization (canvas `c4`, 720×300)

Myth-vs-reality split panel: a fading word chain on the left (crossed out), all-at-once attention arcs on the right.

- **Title (bold 15px `#1a5276`, top center):** "Myth: a Fading Chain.  Reality: Fresh Glances at Everything.".
- **Divider:** vertical dashed `#bdc3c7` line (dash 4/3) at x=360 from y=38 to h−15.
- **Words (both sides):** the, red, car, is, fast — small white boxes with 2px colored outlines.
- **Left (myth):** red `#e74c3c` bold heading "MYTH: read in order, memory fades" at (190, 62). Word boxes at x = 60, 125, 190, 255, 320, y=140, drawn in mute gray with alphas `[0.25, 0.4, 0.6, 0.8, 1.0]` (earlier words fade); gray arrows between consecutive words at matching alpha. Red 12px note "by word 5, word 1 is a faint memory" at (190, 190); a thick red strike-through line from (70,215) to (310,215); bold red "not how transformers work" at (190, 240).
- **Right (reality):** green `#008300` bold heading 'REALITY: "fast" attends to all words at once' at (540, 62). Word boxes at x = 410, 475, 540, 605, 670, y=200; "fast" outlined green, the rest ink. Quadratic-curve arcs from "fast" back to each earlier word with weights `[0.06, 0.14, 0.62, 0.18]` (the / red / car / is), line width `1 + weight × 9`, alpha 0.95 if weight ≥ 0.5 else 0.4, all green. Bold green "0.62" label at (605, 118). Mute 12px note "weights: the 0.06, red 0.14, car 0.62, is 0.18 — sum 1.00 (illustrative)" at (540, 235). Bold orange 12px "word 1 costs no more to reach than word 4" at (540, 262).

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`, social-graph reference skeleton). `<h1>` (no index number) with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks; each has an `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) and a `table.layout` with `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` row of colored pill spans first, then a `<ul>` of one-line bullets each opening with `<b>` (bold terms colored `#1a5276`), one italic `.example` paragraph, one `.key-point` callout (background `#f8f9fa`, left border `3px solid #e74c3c`, `<strong>` label).
- **Tag pill colors:** blue `rgba(26,82,118,0.12)`/`#1a5276`; green `rgba(39,174,96,0.15)`/`#27ae60`; red `rgba(231,76,60,0.12)`/`#e74c3c`; orange `rgba(230,126,34,0.15)`/`#e67e22`. Pills: 0.72rem, weight 600, padding 2px 10px, radius 10px.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; ul 0.92rem; canvases `width:100%` with `1px solid #e0e0e0` border, radius 4px. No nav bar, no back/home links, no cross-page links.
- **Canvas:** all charts 720×300 logical, scaled by `window.devicePixelRatio` via a shared `setup(id)` helper (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). Hardcoded data arrays only — no `Math.random()`.
- **Chart palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- In regenerated HTML, any card links use `.html` extensions (this page has no outbound links).
