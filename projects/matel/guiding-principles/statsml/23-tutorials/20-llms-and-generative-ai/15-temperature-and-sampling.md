# Temperature & Sampling

**Page type:** detail page (tutorial layout: h1 + subtitle, then one `.card-section` per concept, each with h2 and a two-column `table.layout` — text left 50%, canvas right 50%)
**HTML title tag:** Temperature &amp; Sampling

**Subtitle:** The model picks each word with a weighted dice roll — temperature is the dial that makes the roll strict (reliable) or loose (creative)

## The Capital of France, at Three Dial Settings

**Tags:** `core idea` (blue), `counter-intuitive` (orange)

- **The prompt** — "The capital of France is ___", asked 20 times at each setting
- **Temperature 0** — the model always takes its top word: "Paris", 20 out of 20
- **Temperature 0.7** — mostly "Paris" (16 of 20), an occasional "Lyon" or "Rome"
- **Temperature 1.5** — only 11 of 20 say "Paris"; the model wanders down its list
- **The dial** — temperature sets how strictly the model sticks to its favorite

*Example (italic):* Same prompt, same model, same day — the only thing changed across the 60 runs is one number.

**Key point:** The model never stops rating "Paris" as most likely — temperature only changes how often it actually says so.

### Visualization (canvas `c1`, 720×300)

Grouped bar chart: 20 runs at each temperature — which word came out.

- **Title (bold 15px, `#1a5276`, top center):** `"The capital of France is ___" — 20 Runs per Setting`
- **Groups (x-axis):** `T = 0`, `T = 0.7`, `T = 1.5` (bold 13px labels, `#222`, below baseline)
- **Data (counts out of 20):** T=0: `[20, 0, 0]`; T=0.7: `[16, 3, 1]`; T=1.5: `[11, 6, 3]` for words `Paris`, `Lyon`, `Rome`
- **Bar colors:** Paris blue `#2a78d6`, Lyon aqua `#199e70`, Rome violet `#4a3aa7`; bars at 0.8 alpha, 34px wide with 8px gap, zero-count bars omitted from labels
- **Value labels:** bold 12px in the bar's own color above each nonzero bar
- **Axes:** L-shaped axis in `#999` (left + bottom); y scale 0–20; padding top 56 / bottom 64 / left 62 / right 160
- **Y-axis label (rotated, 12px, `#444`):** "runs (of 20) giving each word"
- **Legend (right side, x = w−145):** 12×12 color swatches with word names, 12px `#222`
- **Annotation (orange `#d95926`, bold 13px, under legend):** "only the dial moved:" / "Paris 20/20 → 11/20"; below it in muted gray `#6b7280` 12px: "illustrative runs"
- **Caption (bottom center, 12px `#444`):** "same prompt, same model — temperature is the only difference"

## The Recipe: Divide Every Score by T, Then Roll

**Tags:** `worked example` (green), `core idea` (blue)

- **Raw scores** — the model ends with a score per word: Paris 2.0, Lyon 1.0, Rome 0.0
- **Untouched (T = 1)** — e^score shares out as Paris 67%, Lyon 24%, Rome 9%
- **T = 0.7** — divide scores by 0.7 first: gaps stretch — Paris 77%, Lyon 19%, Rome 4%
- **T = 1.5** — divide by 1.5: gaps shrink — Paris 56%, Lyon 29%, Rome 15%
- **T = 0** — no dice roll at all: just take the top score, Paris 100%

*Example (italic):* Check T = 0.7 on a calculator: e^2.86 = 17.4, e^1.43 = 4.2, e^0 = 1.0 — Paris gets 17.4 of 22.6 ≈ 77%.

**Key point:** Temperature rescales the scores before the dice roll — it never adds or removes any knowledge.

### Visualization (canvas `c2`, 720×300)

Grouped bar chart: probabilities after dividing scores 2.0 / 1.0 / 0.0 by T.

- **Title (bold 15px, `#1a5276`, top center):** "Scores 2.0 / 1.0 / 0.0 — What Each Temperature Makes of Them"
- **Groups (x-axis):** `T = 0`, `T = 0.7`, `T = 1`, `T = 1.5` (bold 13px, `#222`)
- **Data (probability %, per word Paris/Lyon/Rome):** T=0: `[100, 0, 0]`; T=0.7: `[77, 19, 4]`; T=1: `[67, 24, 9]`; T=1.5: `[56, 29, 15]`
- **Bar colors:** Paris blue `#2a78d6`, Lyon aqua `#199e70`, Rome violet `#4a3aa7`; 0.8 alpha, 26px wide with 6px gap; bold 12px value labels above nonzero bars in bar color
- **Axes:** L-shaped `#999` axis; y scale 0–100; padding top 56 / bottom 64 / left 62 / right 160
- **Y-axis label (rotated, 12px, `#444`):** "chance of being picked, %"
- **Legend (right side, x = w−145):** swatches + word names as in c1
- **Annotation (orange `#d95926`, bold 13px):** "higher T flattens" / "the odds — same" / "scores underneath"
- **Caption (bottom center, 12px `#444`):** "divide each score by T, take e^score, share out of the total"

## Picking a Setting: Reliable vs Creative

**Tags:** `where it's used` (blue), `rule of thumb` (blue)

- **Extraction & code** — pin near 0: same invoice in, same JSON out, every run
- **Q&A & summaries** — 0.3–0.7 keeps wording natural without drifting off the facts
- **Brainstorming** — 0.9–1.3 surfaces the underdog words that make ideas feel fresh
- **Evaluations** — comparing prompts at high T mixes prompt effect with dice luck
- **No best value** — the right setting is a property of the task, not of the model

*Example (italic):* A team chased a "flaky" invoice parser for a week — the fix was turning temperature from 0.9 down to 0.

**Key point:** Choose by task: measurement and repeatability want 0; idea generation wants the dial turned up.

### Visualization (canvas `c3`, 720×300)

Horizontal zone-band diagram: the temperature dial as a task spectrum.

- **Title (bold 15px, `#1a5276`, top center):** "One Dial, Three Working Zones"
- **Band:** horizontal bar at y=120, height 44px, spanning left pad 62 to right pad 40; x scale is temperature 0 to 1.5
- **Zones (fill at 0.28 alpha plus 2px solid outline in the same color):**
  - 0.0–0.25, green `#008300`, label "reliable" (bold 14px above band), tasks line "extraction · code · evals" (12px `#444`)
  - 0.25–0.75, blue `#2a78d6`, label "balanced", tasks "Q&A · summaries · chat"
  - 0.75–1.5, magenta `#d55181`, label "creative", tasks "brainstorm · names · fiction"
- **Tick marks (6px, `#999`) with 12px `#222` labels** at temperatures: 0, 0.25, 0.5, 0.7, 1.0, 1.25, 1.5; axis caption "temperature" (12px `#444`) centered below
- **Endpoint captions (bold 12px):** left-aligned green "same answer every run"; right-aligned magenta "different answer every run"
- **Takeaway (orange `#d95926`, bold 13px, centered below):** "no best value — match the dial to the task"

## Two Things Temperature Is Not

**Tags:** `common mistake` (red), `trade-off` (orange)

- **Not an accuracy dial** — lowering T sharpens habit; a wrong top guess stays wrong
- **Not learning** — the model's weights are frozen; the dice roll is only at output
- **Creativity ≠ quality** — high T delivers fresh words and more nonsense in one batch
- **0 ≠ truth mode** — T = 0 means "repeat the favorite", which can be a confident error

*Example (italic):* If the model's top guess for a fact is wrong, it is wrong at every temperature — just more consistently at 0.

**Key point:** Temperature moves the reliability↔variety trade-off; correctness lives in the model, not in the dial.

### Visualization (canvas `c4`, 720×300)

Two-panel bar comparison: what the dial barely moves vs what it strongly moves.

- **Title (bold 15px, `#1a5276`, top center):** "The Dial Moves Variety a Lot — Knowledge Barely"
- **Divider:** vertical dashed line (`#bdc3c7`, dash 4/3, 1px) at x=360 from y=40 to h−15
- **Left panel (x=60, width 270):** subtitle bold 13px `#1a5276` "fact-quiz accuracy, %"; bars for `T = 0`, `T = 0.7`, `T = 1.5` with values `[72, 70, 66]` (scale max 100, chart height 140, baseline y=226); bars blue `#2a78d6` at 0.75 alpha, 56px wide; bold 12px blue value labels "72%", "70%", "66%" above bars; temp labels 12px `#222` below; blue bold 12px caption "barely moves" centered under panel
- **Right panel (x=400, width 270):** subtitle "distinct wordings in 10 runs"; bars for the same temps with values `[1, 4, 9]` (scale max 10); bars magenta `#d55181` at 0.75 alpha; magenta bold value labels; magenta bold caption "moves a lot"
- **Baselines:** thin `#999` line under each panel at y=226
- **Takeaway (orange `#d95926`, bold 13px, bottom center):** "temperature trades consistency for variety — it is not an accuracy knob"
- **Note (muted `#6b7280`, 12px, top right):** "illustrative numbers"

## Regeneration instructions

- **Template:** tutorials topic-page skeleton (social-graph reference style). Structure: `<h1>` (no index number), `.subtitle` paragraph, then four `.card-section` blocks; each has an `<h2>` and a `table.layout` with one `<tr>`: left `<td class="text-col">` (50%) holding `.tags` pills, a `<ul>` of one-line bullets with `<b>` lead terms, an italic `.example` paragraph, and a `.key-point` callout; right `<td class="viz-col">` (50%) holding one canvas.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with 2px solid `#2980b9` bottom border. Section h2 1.3rem `#1a5276` with 2px `#2980b9` bottom border. `.subtitle` `#666` 0.95rem. Canvas `width:100%`, 1px `#e0e0e0` border, 4px radius. `.key-point` background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem. `.example` italic `#555` 0.9rem. `ul` 0.92rem; `li b` colored `#1a5276`.
- **Tag pills:** `.tag` inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px. Colors — blue: bg `rgba(26,82,118,0.12)` text `#1a5276`; green: bg `rgba(39,174,96,0.15)` text `#27ae60`; red: bg `rgba(231,76,60,0.12)` text `#e74c3c`; orange: bg `rgba(230,126,34,0.15)` text `#e67e22`.
- **Chart palette object `P`:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- **Canvas:** all four canvases 720×300 logical; a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`), and calls `ctx.scale` so drawing stays in logical coordinates, and clears. All data arrays hardcoded (no randomness).
- No nav bar, no back/home links. In regenerated HTML any card links would use `.html` extensions (this page has none).
