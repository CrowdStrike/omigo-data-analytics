# Cross-Entropy

**Page type:** detail page (tutorial layout: h1 + subtitle, then card-sections each with an h2 and a text/viz table row, 50% text / 50% canvas)
**HTML title tag:** Cross-Entropy

**Subtitle:** Score a classifier by how surprised it is at the truth — a small penalty for confident right answers, a huge one for confident wrong ones

## One Spam Email, Three Model Opinions

Tags: `core idea` (blue), `running example` (green)

- **The email** — it really IS spam; the model outputs a probability that it is
- **Model says 0.9** — confident and right: loss = log₂(1/0.9) = 0.15 bits, tiny
- **Model says 0.5** — fence-sitting: loss = log₂(1/0.5) = 1 bit
- **Model says 0.1** — confident and wrong: loss = log₂(1/0.1) = 3.32 bits, 22× the 0.9 case
- **The name** — this "surprise at the truth" penalty is cross-entropy, a.k.a. log loss

*Example:* The model that said 0.1 was "sure" the spam was clean — the loss makes that sureness expensive.

**Key point:** Cross-entropy charges the model the surprise of the true answer under its own predicted probabilities.

### Visualization (canvas `c1`, 720×300)

Three-bar chart: loss for each model opinion on one spam email.

- **Title (bold 15px, `#1a5276`, top center):** "The Email IS Spam — Loss for Each Model Opinion".
- **Data:** labels `['says 0.9 spam', 'says 0.5 spam', 'says 0.1 spam']`, sub-captions `['confident + right', 'fence-sitting', 'confident + wrong']`, values `[0.15, 1.00, 3.32]` bits, colors `[#008300 green, #c98500 yellow, #e74c3c red]`.
- **Axes:** L-shaped gray axes (`#999`); y max 3.8; padding top 56, bottom 66, left 62, right 30. Bars 140px wide at 18%/50%/82% of chart width, 0.75 alpha; bold value labels "0.15 bits" / "1.00 bits" / "3.32 bits" above bars; bold labels + gray sub-captions below.
- **Y-axis label (rotated):** "loss = log₂(1/p of truth), bits".
- **Annotation:** bold red (`#e74c3c`) "confident wrongness costs 22× confident rightness" near top at 45% width.

## Scoring Four Emails by Hand

Tags: `worked example` (green)

- **The rule** — per email, take the model's probability of what actually happened, then log₂(1/p)
- **Email 1** — spam, model said 0.9 spam: loss 0.15 bits
- **Email 2** — clean, model said 0.2 spam, so 0.8 clean: loss 0.32 bits
- **Email 3** — spam, model said 0.6 spam: loss 0.74 bits
- **Email 4** — clean, model said 0.9 spam, so 0.1 clean: loss 3.32 bits
- **Average** — (0.15 + 0.32 + 0.74 + 3.32) / 4 = 1.13 bits per email

*Example:* Three decent calls cost 1.21 bits combined; the single confident mistake costs 3.32 on its own.

**Key point:** Cross-entropy is the average surprise across examples — and one confidently wrong prediction can dominate it.

### Visualization (canvas `c2`, 720×300)

Four-bar chart of per-email loss with a dashed average line.

- **Title:** "Per-Email Loss: One Confident Miss Dominates the Average".
- **Data:** labels `['email 1: spam', 'email 2: clean', 'email 3: spam', 'email 4: clean']`, sub-captions `['said 0.9 spam', 'said 0.2 spam', 'said 0.6 spam', 'said 0.9 spam']`, values `[0.15, 0.32, 0.74, 3.32]` bits; first three bars blue `#2a78d6`, fourth red `#e74c3c`; 0.75 alpha; bars 110px wide, evenly spaced.
- **Average line:** horizontal dashed orange (`#d95926`, dash 6/4, width 2) at y = 1.13, labeled bold orange "average = 1.13 bits" at the left.
- **Axes:** y max 3.8; padding top 56, bottom 66, left 62, right 30; bold value labels above bars; rotated y-axis label "loss, bits".
- **Annotation:** bold red right-aligned "one bad call = 73% of the total loss" near the top right.

## Why It's THE Classification Loss

Tags: `where it's used` (blue), `rule of thumb` (orange)

- **Accuracy is blunt** — it only asks right or wrong, so 0.51 and 0.99 look identical
- **Cross-entropy is smooth** — every nudge of the probability changes the loss a little
- **Gradients** — that smoothness is what lets neural networks learn step by step
- **Honesty wins** — the loss is minimized by reporting your true belief, not bluffing to 1.0
- **The cliff** — loss grows without bound as p(truth) → 0: overconfidence is ruinous

*Example:* Pushing a prediction from 0.9 to 0.99 saves 0.14 bits; being wrong at 0.99 costs 6.64 bits.

**Key point:** Cross-entropy rewards calibrated confidence — which is exactly what you want a probability to be.

### Visualization (canvas `c3`, 720×300)

Curve chart: loss vs the probability given to the true answer, with four markers.

- **Title:** "Loss vs the Probability Given to the True Answer".
- **Curve:** log₂(1/p) plotted for p from 1/400 to 1 (400 steps), clipped at y max 7; violet `#4a3aa7`, line width 3. Padding top 52, bottom 52, left 62, right 30.
- **X ticks:** 0, 0.25, 0.5, 0.75, 1; x-axis label "p the model gave to what actually happened"; rotated y-axis label "loss = log₂(1/p), bits".
- **Markers (6px filled dots with bold labels):** p=0.9 at 0.15, green `#008300`, "0.9 → 0.15 bits"; p=0.5 at 1.0, yellow `#c98500`, "0.5 → 1 bit"; p=0.1 at 3.32, red `#e74c3c`, "0.1 → 3.32 bits"; p=0.01 at 6.64, red `#e74c3c`, "0.01 → 6.64 bits".
- **Annotations:** bold red "the cliff: confident wrongness" near the steep left side; bold green right-aligned "flat, safe zone: confident rightness" near the flat right side.

## The Confusion: Same Accuracy, Very Different Loss

Tags: `common mistake` (red)

- **Two models** — both get 3 of the same 4 emails right: identical 75% accuracy
- **Modest model A** — predicts 0.7 when right, 0.3 on its miss: average loss 0.82 bits
- **Cocky model B** — predicts 0.99 when right, 0.02 on its miss: average loss 1.42 bits
- **The miss decides** — B's single 0.02 mistake costs 5.64 bits, dwarfing its cheap wins
- **The lesson** — accuracy ties; cross-entropy breaks the tie toward trustworthy probabilities

*Example:* If you act on the probabilities — pricing risk, ranking leads — model B's swagger will hurt you.

**Key point:** Cross-entropy measures probability quality, not hit rate — don't read low loss as high accuracy or vice versa.

### Visualization (canvas `c4`, 720×300)

Grouped bar chart: model A vs model B per-email loss and averages.

- **Title:** "Both Models: 3 of 4 Right (75% Accuracy) — Loss Disagrees".
- **Groups (x labels):** `['email 1', 'email 2', 'email 3', 'email 4 (the miss)', 'AVERAGE']` (last two bold).
- **Data:** model A (blue `#2a78d6`): `[0.51, 0.51, 0.51, 1.74, 0.82]`; model B (violet `#4a3aa7`): `[0.01, 0.01, 0.01, 5.64, 1.42]`. Paired 34px bars per group, 0.75 alpha, minimum 2px height; bold value labels above each bar in the bar's color.
- **Axes:** y max 6.2; padding top 56, bottom 78, left 62, right 30; rotated y-axis label "loss, bits".
- **Sub-caption (gray, below group labels):** "A: 0.7 right / 0.3 on miss      B: 0.99 right / 0.02 on miss".
- **Legend (top left):** blue square "model A (modest)"; violet square "model B (cocky)".
- **Annotation:** bold violet "B's one cocky miss makes its average loss 1.7× A's" near top center.

## Regeneration instructions

- **Template/layout:** tutorial concept page (tutorials category). h1 (no index number) with 2px `#2980b9` bottom border, `.subtitle` gray one-liner, then 4 `.card-section` blocks. Each section: `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one `<tr>`: left `td.text-col` (50%) and right `td.viz-col` (50%, one 720×300 canvas).
- **Text column structure:** `.tags` pill row first, then a `<ul>` of 5–6 one-line bullets each opening with `<b>bold term</b>` (bold terms colored `#1a5276`), one italic `.example` line, one `.key-point` callout (`#f8f9fa` background, 3px `#e74c3c` left border).
- **Tag pills:** 0.72rem, weight 600, padding 2px 10px, radius 10px. Colors: blue `rgba(26,82,118,0.12)`/`#1a5276`; green `rgba(39,174,96,0.15)`/`#27ae60`; red `rgba(231,76,60,0.12)`/`#e74c3c`; orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; ul 0.92rem; canvases `width:100%` with 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Chart palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`; error bars use literal `#e74c3c`. Site palette anchors: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- **Canvas:** intrinsic `width`/`height` attributes per chart; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart titles bold 15px system-ui; labels 12–13px; all data hardcoded (no `Math.random()`). Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- In regenerated HTML, any card links use `.html` extensions (this page has no links).
