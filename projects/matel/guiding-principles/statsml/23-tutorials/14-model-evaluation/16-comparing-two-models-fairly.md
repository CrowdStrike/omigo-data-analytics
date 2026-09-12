# Comparing Two Models Fairly

**Page type:** detail page (tutorial layout: h1 + subtitle, 4 `.card-section` blocks each with a two-column `table.layout` — text left 50%, canvas right 50%)
**HTML title tag:** Comparing Two Models Fairly

**Subtitle:** Model B beat model A by 0.8 points on one test split — before believing it, rerun the race a few times and look at the spread of results

## A 0.8-Point Win on One Split

Tags: `core idea` (blue), `running example` (green)

- **The claim** — on the test split, model A scores 84.1%, model B scores 84.9%
- **The excitement** — "+0.8 points, ship B!" says the team after one measurement
- **The doubt** — a test score is one draw of a noisy number, not a fixed truth
- **Sources of noise** — which rows landed in the split, random seeds, tie-breaks in training
- **The question** — would B still win if we cut the data differently and reran?

*Example:* Two runners race once and finish 0.3 seconds apart — you would not declare one faster forever.

**Key point — Core idea:** a single test score has luck baked in. Comparing two of them is comparing two lottery tickets — you need repeated draws.

### Visualization (canvas `c1`, 720×300)

Two bars with noise whiskers showing the single-split comparison.

- **Title (bold 16px `#1a5276`, top center):** "One Test Split: B Wins by 0.8 — But Look at the Noise Band".
- **Data:** model A 84.1% (blue `#2a78d6`), model B 84.9% (green `#008300`); bar width 130px, gap 150px, centered.
- **Axes:** y zoomed 82–87%, ticks every 1% with gridlines `#e5e9ef`; padding top 55, bottom 55, left 65, right 40; gray `#999` L axes.
- **Whiskers:** each bar gets a ±0.7 error whisker (typical rerun-to-rerun swing) in `#2c3e50`, width 2, with 20px caps; bold 14px value label ("84.1%" / "84.9%") above the top cap; model name below baseline.
- **Annotation (bold 13px red `#e74c3c`, two lines near the top):** "whiskers: ±0.7 rerun-to-rerun swing — the bands overlap," / "so the 0.8 gap could be luck".
- **Footnote (12px `#444`, bottom-right):** "y-axis zoomed to 82–87%, not from 0".

## Rerunning the Race: Same Five Folds for Both

Tags: `worked example` (green), `paired comparison` (blue)

- **The setup** — cut the data into 5 folds; test both models on the *same* folds
- **Model A** — fold scores 84.6, 82.8, 84.5, 83.9, 84.7 — mean 84.1
- **Model B** — same folds: 85.3, 83.4, 85.5, 84.4, 85.9 — mean 84.9
- **Per-fold gaps** — +0.7, +0.6, +1.0, +0.5, +1.2 — B wins all 5
- **Why same folds** — fold 2 is hard for both; pairing subtracts fold luck from the comparison

*Example:* Both models stumble on fold 2 — but B still beats A on it, which is the fair signal.

**Key point — Same folds:** if each model gets different splits, score gaps mix "better model" with "easier data". Pair them and the data luck cancels.

### Visualization (canvas `c2`, 720×300)

Paired line chart: both models' scores across the same five folds, with per-fold gap labels.

- **Title (bold 16px `#1a5276`):** "Same Five Folds: B Wins Every One".
- **Data:** model A `[84.6, 82.8, 84.5, 83.9, 84.7]` (blue `#2a78d6`); model B `[85.3, 83.4, 85.5, 84.4, 85.9]` (green `#008300`); gap labels `+0.7, +0.6, +1.0, +0.5, +1.2` in bold 12px orange `#d95926` above each B point.
- **Axes:** y 82–87%, ticks every 1% with gridlines `#e5e9ef`; x labels "fold 1" … "fold 5" in muted 12px; padding top 60, bottom 55, left 65, right 175.
- **Marks:** light gray `#ccc` 1.5px vertical connector between the A and B points of each fold; series lines width 2.5 with radius-5 dots.
- **Hard-fold callout (bold 12px red `#e74c3c`, at fold 2, two lines):** "fold 2 is hard for both —" / "pairing subtracts that".
- **Legend (right column, x = width−160):** blue swatch "model A (mean 84.1)"; green swatch "model B (mean 84.9)"; bold orange "B wins 5 of 5 folds"; muted "(on this one seed)".

## Ten Seeds Later: the Win Shrinks

Tags: `worked example` (green), `why it matters` (red)

- **The rerun** — repeat the whole comparison with 10 random seeds (new splits, new training runs)
- **The gaps (B−A)** — +0.8, −0.3, +1.2, +0.1, −0.5, +0.9, +0.4, −0.2, +0.7, +0.3
- **The average** — +0.34, well under the +0.8 that started the party
- **The flips** — in 3 of 10 reruns, A actually wins; the first run was a lucky draw for B
- **The report** — "B is ahead by about +0.3 ± 0.5, winning 7 of 10 reruns" — the honest sentence

*Example:* The very first seed gave +0.8 — the most flattering of the ten results, and the only one anyone saw.

**Key point — Why it matters:** shipping B costs retraining, review, and risk. The spread tells you whether you are paying that for a real gain or for one lucky split.

### Visualization (canvas `c3`, 720×300)

Dot strip of the 10 seed gaps with a zero line and a mean line.

- **Title (bold 16px `#1a5276`):** "Gap (B − A) Across 10 Reruns with Different Seeds".
- **Data:** gaps `[0.8, -0.3, 1.2, 0.1, -0.5, 0.9, 0.4, -0.2, 0.7, 0.3]`, one radius-7 dot per seed at x positions s1–s10; positive gaps green `#008300`, negative gaps red `#e74c3c`; 11px signed value label above each dot; muted "s1"…"s10" labels below the baseline and axis title "seed used for the rerun".
- **Axes:** y from −1.0 to +1.5, ticks at −1.0, −0.5, 0, +0.5, +1.0, +1.5 with gridlines `#e5e9ef`; padding top 65, bottom 60, left 65, right 40.
- **Zero line:** horizontal dashed (dash 6/4, width 2) in `#2c3e50` at 0, labeled bold "tie" near the right end.
- **Mean line:** solid violet `#4a3aa7` width 2 at +0.34, labeled bold 12px "mean +0.34".
- **Annotation (bold 13px red `#e74c3c`, centered near top):** "3 of 10 reruns flip the winner — the famous +0.8 (seed 1) was a lucky draw".

## A Single-Split Win Is an Anecdote

Tags: `common mistake` (orange), `rule of thumb` (green)

- **The confusion** — treating one measured gap as the true gap; single numbers feel exact
- **What a real win looks like** — a model C that beats A on every rerun: gaps +0.8 to +1.6
- **The tell** — B's gaps straddle zero; C's never touch it — spread, not the mean, separates them
- **Rule of thumb** — if reruns flip the winner, you don't have a winner yet
- **Report format** — mean gap, spread, and wins-out-of-N — never a bare "+0.8"

*Example:* "B beat A by 0.8" and "B beats A 7 times out of 10 by 0.3 on average" describe the same experiment.

**Key point — Rule:** one split is an anecdote; the distribution of gaps across reruns is the evidence.

### Visualization (canvas `c4`, 720×300)

Two horizontal dot-strip rows comparing gap distributions against a vertical zero line.

- **Title (bold 16px `#1a5276`):** "What a Real Win Looks Like: Gaps Across 10 Reruns".
- **Axis:** horizontal, gap from −1.0 to +2.0, ticks at −1.0, −0.5, 0, +0.5, +1.0, +1.5, +2.0 with vertical gridlines `#e5e9ef`; axis title "accuracy gap vs model A (points)"; padding top 60, bottom 55, left 150, right 45.
- **Zero line:** vertical dashed (dash 6/4, width 2) in `#2c3e50` at 0.
- **Row 1 "model B − A"** (bold 13px right-aligned row label): dots `[0.8, -0.3, 1.2, 0.1, -0.5, 0.9, 0.4, -0.2, 0.7, 0.3]`, positive dots orange `#d95926`, negative dots red `#e74c3c`, radius 7 on a `#e5e9ef` guide line; bold 12px orange note above: "straddles zero: not settled".
- **Row 2 "model C − A":** dots `[1.2, 0.9, 1.6, 1.1, 0.8, 1.4, 1.0, 1.3, 0.9, 1.5]` in green `#008300`; bold 12px green note above: "never touches zero: real win".
- **Annotation (bold 13px red `#e74c3c`, centered near top):** "if reruns can flip the winner, you don't have a winner yet".

## Regeneration instructions

- **Template:** tutorial detail page (tutorials style, per `tutorials/CLAUDE.md`). Structure: `<h1>` (no index number), `.subtitle` paragraph, then 4 `.card-section` divs each containing `<h2>` and a `table.layout` with one `<tr>`: left `<td class="text-col">` (50%) and right `<td class="viz-col">` (50%) holding one canvas.
- **Text column structure per section:** `.tags` row of colored pills (`.tag.blue` rgba(26,82,118,0.12)/#1a5276, `.tag.green` rgba(39,174,96,0.15)/#27ae60, `.tag.red` rgba(231,76,60,0.12)/#e74c3c, `.tag.orange` rgba(230,126,34,0.15)/#e67e22), then a `<ul>` of one-line bullets each opening with `<b>bold term</b>` (bold in `#1a5276`), one italic `.example` paragraph (`#555`, 0.9rem), one `.key-point` callout (background `#f8f9fa`, left border 3px solid `#e74c3c`, 0.9rem, with `<strong>` lead).
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px solid `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; `.card-section h2` 1.3rem `#1a5276` with 2px `#2980b9` bottom border; table cells padding 12px, vertical-align top; canvases `width:100%`, 1px solid `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** each 720×300 logical; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Hardcoded literal data arrays, no `Math.random()`.
- **Chart palette (JS object P):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange; red reserved for error/alarm annotations.
- In regenerated HTML, any card links use `.html` extensions (this page has no outgoing links).
