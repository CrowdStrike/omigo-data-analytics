# Ground Truth Is a Myth

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Ground Truth Is a Myth

**Subtitle:** The labels a model trains on sound like facts, but for the hard cases they are somebody's judgment call written down — even careful experts reading the same item often disagree

## Three Baristas, Twenty Reviews, Six Arguments

**Tags:** `core idea` (blue), `label disagreement` (green), `judgment calls` (orange)

- **The task** — a coffee shop asks three baristas to label the same 20 reviews positive or negative
- **Easy ones** — on 14 of the reviews all three mark the same label without a second thought
- **The arguments** — on 6 reviews they split 2-vs-1: sarcasm, mixed feelings, faint praise
- **No answer sheet** — nothing hides behind the review to check against; the label IS the judgment
- **The myth** — "ground truth" sounds like fact, but for those 6 it is only a vote that got recorded

*Example (italic):* "Not bad at all" — Ana and Ben read it as praise and mark positive; Cara reads it as lukewarm and marks negative. Nobody is wrong.

**Key point:** For ambiguous items there is no true label waiting to be found — the "ground truth" file stores opinions, and honest opinions differ.

### Visualization (canvas `c1`, 720×300)

Label matrix: 3 rows (one per barista) × 20 columns (one per review), each cell colored by the label given, with the six split columns boxed in red.

- **Title (bold 15px, `#1a5276`, top center):** "Three Baristas × 20 Reviews — 14 Unanimous, 6 Split".
- **Legend (12px `#444`, at y=52, left of grid):** blue swatch "positive", orange swatch "negative".
- **Grid:** 20 columns starting x=115, column pitch 28 (24px cells, 4px gaps); rows Ana / Ben / Cara at y=80, 130, 180, cell height 40; row labels 13px `#444` right-aligned at x=105; column numbers "1", "5", "10", "15", "20" in 11px `#6b7280` at y=238 under the matching columns.
- **Labels (1 = positive blue `rgba(42,120,214,0.75)`, 0 = negative orange `rgba(217,89,38,0.8)`):** Ana `[1,1,1,1,1,1,1,1,1,1,0,0,0,0,1,1,1,1,1,0]`, Ben `[1,1,1,1,1,1,1,1,1,1,0,0,0,0,1,1,1,0,0,1]`, Cara `[1,1,1,1,1,1,1,1,1,1,0,0,0,0,0,0,0,1,1,1]`.
- **Split highlight:** one 2px `#e74c3c` rounded rectangle around columns 15–20 spanning all three rows; bold 12px `#e74c3c` label above it: "2-vs-1 splits".
- **Annotation (bold 12px violet `#4a3aa7`, bottom left near x=115, y=275):** "for 6 of 20 reviews the 'truth' is just a vote".
- **Caption (12px `#444`, bottom right):** "illustrative — one coffee shop's review batch".

## Scoring the Agreement by Hand

**Tags:** `worked example` (blue), `pairwise agreement` (green)

- **Pair by pair** — count matches: Ana–Ben agree on 17 of 20, Ana–Cara on 16, Ben–Cara on 15
- **The percentages** — that is 85%, 80%, and 75% agreement; the average across the pairs is 80%
- **Redo it** — 14 unanimous, plus the splits where that pair sided together (3, 2, 1) = 17, 16, 15
- **Even the best pair** — Ana and Ben, the closest two, still dispute 3 reviews out of 20
- **A number for fuzz** — pairwise agreement is the simplest honest measure of how fuzzy labels are

*Example (italic):* Ana–Ben = the 14 unanimous reviews plus the 3 splits where they sided together: (14 + 3) / 20 = 85%.

**Key point:** Human agreement here averages 80% — that number, not 100%, is what "correct label" means for this dataset.

### Visualization (canvas `c2`, 720×300)

Three vertical bars, one per barista pair, showing percent agreement, with a dashed average line.

- **Title (bold 15px, `#1a5276`, top center):** "How Often Do Two Baristas Agree?".
- **Axes:** origin x=90, baseline y=245, plot width 560, plot height 175; y = agreement 0–100% with light `#e5e9ef` gridlines at 25, 50, 75, 100 and 12px `#444` tick labels; no x axis ticks.
- **Bars (width 110, centered at x=200, 380, 560):** Ana–Ben 85% blue `#2a78d6`, Ana–Cara 80% aqua `#199e70`, Ben–Cara 75% violet `#4a3aa7`; bold 13px value labels "85% (17/20)", "80% (16/20)", "75% (15/20)" above each bar in the bar's color; 12px `#444` pair names below the baseline.
- **Average line:** horizontal dashed orange `#d95926` (dash 4/3) line at 80% across the plot; 12px orange label "average 80%" at its left end (x=95), above the line to avoid the 80% bar top.
- **Annotation (bold 12px `#d95926`, near x=430, y=75):** "even the closest pair disputes 3 reviews in 20".
- **Caption (12px `#444`, bottom right):** "illustrative — agreement on the 20-review batch".

## The Ceiling Your Model Cannot Honestly Beat

**Tags:** `where it's used` (blue), `accuracy ceiling` (orange), `evaluation` (green)

- **Training data** — the shop trains a model on Ana's labels and scores it against Ana's labels too
- **The human ceiling** — Ben, a careful human, scores only 85% against Ana; Cara scores 80%
- **A red flag** — model v2 reports 91% against Ana's labels, higher than any human can score
- **What it learned** — on the 6 argued reviews it learned Ana's personal quirks, not the truth
- **Honest reporting** — always quote model accuracy next to human agreement, on the same items

*Example (italic):* Model v2's 91% "beats humans" only because the exam was written and graded by the same person — Ana.

**Key point:** When humans agree at most 85% of the time, model accuracy above 85% is measuring the labeler, not the world.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: two humans and two models all scored against Ana's labels, with a dashed red vertical line marking the human ceiling.

- **Title (bold 15px, `#1a5276`, top center):** "Scored Against Ana's Labels — Humans vs Models".
- **Axis:** horizontal 2px `#999` line at y=245 from x=170 to x=650 (width 480 = 0–100%); 12px `#444` tick labels "0%", "25%", "50%", "75%", "100%" below; light `#e5e9ef` vertical gridlines at each tick.
- **Bars (height 26, left edge x=170, at y=85, 125, 165, 205), 12px `#444` row labels right-aligned at x=160:** "Ben (human)" 85% aqua `#199e70`; "Cara (human)" 80% aqua `#199e70`; "model v1" 83% blue `#2a78d6`; "model v2" 91% magenta `#d55181`; bold 12px value label at each bar's right end in the bar's color.
- **Ceiling:** vertical dashed `#e74c3c` (dash 4/3) line at 85% from y=60 to y=245; bold 12px `#e74c3c` label at its top: "human ceiling 85%".
- **Annotation (bold 12px magenta `#d55181`, near x=480, y=280):** two lines: "91% is past the ceiling —" / "it learned Ana's quirks, not the truth".
- **Caption (12px `#444`, top right):** "illustrative — same 20-review test set".

## More Voters Don't Make Sarcasm Positive

**Tags:** `common mistake` (red), `majority vote` (orange)

- **The hope** — hire 15 baristas instead of 3 and let a majority vote manufacture the truth
- **Clear cases** — easy reviews collect 13, 14, or 15 of the 15 votes; the crowd just confirms them
- **Sarcastic cases** — the argued reviews sit near 7–9 of 15 (47–60%): a coin flip with more coins
- **Vote is not fact** — a 53% majority stored as one tidy "positive" label hides the disagreement
- **Better** — keep the vote share as a soft label, or flag near-50% items as genuinely ambiguous

*Example (italic):* "Great, my latte was cold again" got 7 of 15 positive votes (47%) — yet the database shows one clean label, same as a 15-of-15 review.

**Common mistake:** Treating a 53% majority as the same kind of fact as a 100% one. Record the vote share, not just the winner — the disagreement is information.

### Visualization (canvas `c4`, 720×300)

Bar chart of 12 reviews sorted by the share of 15 voters who marked them positive, showing a cliff between clear reviews and argued ones hugging the 50% line.

- **Title (bold 15px, `#1a5276`, top center):** "15 Voters Instead of 3 — Sarcasm Is Still a Coin Flip".
- **Axes:** origin x=70, baseline y=245, plot width 580, plot height 175; y = share voting positive 0–100% with light `#e5e9ef` gridlines at 25, 50, 75 and 12px `#444` tick labels.
- **Bars (12 bars, width 36, gap 12, starting x=85):** values `[100, 100, 100, 100, 93, 93, 87, 87, 60, 53, 53, 47]`; first 8 (clear) blue `#2a78d6`, last 4 (argued) orange `#d95926`; bold 12px value labels above each bar in the bar's color.
- **Group labels (12px `#6b7280`, below the baseline):** "clear reviews" centered under the blue bars, "argued reviews" centered under the orange bars.
- **Coin-flip line:** horizontal dashed `#6b7280` (dash 4/3) line at 50%; 12px `#6b7280` label "coin flip" at its left end.
- **Annotation (bold 12px `#d95926`, near x=490, y=100):** two lines: "more voters," / "same argument".
- **Caption (12px `#444`, bottom right):** "illustrative — share of 15 baristas voting positive, reviews sorted".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all label arrays, agreement percentages, and vote shares are the hardcoded literals above (no randomness); the text's numbers (14 unanimous, 6 splits, 17/16/15 pair matches, 85/80/75%, 80% average, 91% model v2, 53% and 47% vote shares) must match the chart values exactly.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
