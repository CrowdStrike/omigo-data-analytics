# Class Imbalance

**Page type:** detail page (tutorial layout: h1 + subtitle, 4 `.card-section` blocks each with a two-column `table.layout` — text left 50%, canvas right 50%)
**HTML title tag:** Class Imbalance

**Subtitle:** When one outcome is rare — 1 fraud in 100 transactions — a model can score 99% by ignoring the very thing you built it to find

## 99% Accurate and Completely Useless

Tags: `core idea` (blue), `running example` (green)

- **The data** — 10,000 card transactions; only 100 are fraud, 9,900 are legit
- **The cheat** — a "model" that says "legit" every single time, no matter what
- **Its score** — right on all 9,900 legit ones: 9,900/10,000 = 99% accuracy
- **Its value** — zero: it catches none of the 100 frauds, the only thing that matters
- **The name** — this lopsided 99-to-1 mix is called class imbalance

*Example:* A doctor who tells every patient "you're healthy" is right 99% of the time for a disease 1 in 100 people have.

**Key point — Class imbalance:** when one class is rare, accuracy mostly measures the mix of the data, not the skill of the model.

### Visualization (canvas `c1`, 720×300)

Area-proportional strip showing the 99-to-1 mix, plus the do-nothing score.

- **Title (bold 16px `#1a5276`, top center):** "10,000 Transactions: the Fraud Is Almost Invisible".
- **Strip:** one rectangle at x=60, y=60, 600×110px. Left 99% filled `rgba(42,120,214,0.30)` with blue `#2a78d6` 1.5px outline, labeled bold 14px blue centered "9,900 legit (99%)". Right 1% sliver filled magenta `#d55181`.
- **Callout to the sliver:** magenta 1.5px leader line from the sliver down-left, bold 13px magenta right-aligned label "100 fraud (1%) — the sliver".
- **Text lines below the strip (left-aligned at x=60):** 13px `#2c3e50` "Strategy: say "legit" every time"; bold 15px red `#e74c3c` "accuracy 99.0% — frauds caught: 0 of 100"; 12px muted `#6b7280` "the score comes from the mix of the data, not from any skill".

## Accuracy Picks the Wrong Winner

Tags: `worked example` (green), `arithmetic` (blue)

- **Do-nothing** — "all legit": 9,900 right, 0 frauds caught, accuracy 99.0%
- **A real model** — catches 8 frauds but flags 40 legit by mistake
- **Its accuracy** — (9,860 + 8) / 10,000 = 98.7% — *lower* than doing nothing
- **The verdict flips** — accuracy prefers the useless model; fraud-caught prefers the real one
- **Why** — 100 frauds can move accuracy by at most 1 point; the 9,900 legit drown them out

*Example:* Check it yourself: 9,900 − 40 = 9,860 legit right, plus 8 frauds = 9,868 correct, so 98.68%.

**Key point — The lie:** on a 99-to-1 mix, accuracy rewards ignoring the minority. Any model that even tries to catch fraud looks worse on it.

### Visualization (canvas `c2`, 720×300)

Two side-by-side bar panels split by a vertical dashed divider at x=360 (`#bdc3c7`, dash 4/3).

- **Title (bold 16px `#1a5276`):** "Two Scoreboards, Opposite Verdicts".
- **Models:** "all legit" (accuracy 99.0, caught 0, yellow `#c98500`); "real model" (accuracy 98.7, caught 8, blue `#2a78d6`).
- **Left panel** (x=70, width 240, baseline y=230, plot height 155): bold 13px header "accuracy (zoomed 98–100%)"; y ticks 98%, 99%, 100%; two 80px-wide bars with bold value labels above ("99%" and "98.7%") and model names below. Bold 12px red annotation below: "accuracy says: do nothing".
- **Right panel** (x=420, width 240, same baseline): bold 13px header "frauds caught (of 100)"; y ticks 0, 5, 10; bars for 0 (drawn as a 2px outline stub) and 8, value labels "0" / "8". Bold 12px green `#008300` annotation below: "the job says: the real model".
- **Bottom annotation (bold 13px red `#e74c3c`, centered):** "the metric that ranks them backwards is the one on the slide".

## The Standard Toolkit: Weights, Resampling, Better Metrics

Tags: `best practice` (green), `where it's used` (blue)

- **Class weights** — tell training "one missed fraud costs as much as 99 missed legits"
- **Resampling** — duplicate the rare class (oversample) or drop majority rows (undersample) in training
- **Better metrics** — judge on recall and precision for the rare class, not overall accuracy
- **The effect here** — with class weights the model catches 62 of 100 frauds instead of 8
- **The price** — false alarms rise from 40 to 250; accuracy drops to 97.1%, and that's fine

*Example:* Same data, same algorithm — only the penalty for missing a fraud changed, and recall went 8% to 62%.

**Key point — The trade:** every fix buys fraud-catching with more false alarms. You are choosing the trade on purpose instead of letting the 99% majority choose it for you.

### Visualization (canvas `c3`, 720×300)

Two side-by-side bar panels split by a vertical dashed divider at x=360 (`#bdc3c7`, dash 4/3).

- **Title (bold 16px `#1a5276`):** "Same Algorithm, Class Weights On: What Changes".
- **Left panel** (x=70, width 240, baseline y=230, plot height 150): header "frauds caught (of 100)"; y ticks 0, 50, 100; bars naive 8 (blue `#2a78d6`) and weighted 62 (green `#008300`), value labels above, names below. Bold 12px green annotation: "recall: 8% → 62%".
- **Right panel** (x=420, width 240): header "false alarms (of 9,900 legit)"; y ticks 0, 125, 250; bars naive 40 (blue) and weighted 250 (orange `#d95926`). Bold 12px orange annotation: "the price: 40 → 250".
- **Bottom annotation (bold 13px red `#e74c3c`, centered):** "accuracy fell 98.7% → 97.1% — and the model got better at its job".

## Test on the Real Mix, Not the Fixed One

Tags: `common mistake` (orange), `gotcha` (red)

- **The mistake** — resampling the test set too, so the model is graded on a 50/50 world
- **Balanced test** — our weighted model looks superb: about 96% of its flags are real fraud
- **Real 1% traffic** — same model: 62 real frauds among 312 flags — precision drops to 20%
- **Nothing broke** — with 99x more legit around, even a small error rate makes many false flags
- **The rule** — rebalance training if you like; the test set keeps the real-world 1% mix

*Example:* The demo dazzled on the balanced sample; in production four of five alerts were false.

**Key point — Common confusion:** precision depends on the mix it's measured on. Report it at the rate the model will actually face.

### Visualization (canvas `c4`, 720×300)

Two-bar comparison of precision on two test mixes, with a right-side arithmetic panel.

- **Title (bold 16px `#1a5276`):** "Same Model, Two Test Mixes: Precision of Its Flags".
- **Bars (width 120px, gap 90px, centered in plot area):** "balanced 50/50 test" 96% in aqua `#199e70`; "real 1% fraud traffic" 20% in violet `#4a3aa7`. Bold 14px value labels above, 12px labels below.
- **Axes:** y 0–100%, ticks every 25% with gridlines `#e5e9ef`; padding top 55, bottom 55, left 65, right 260.
- **Right-side explanation (left-aligned at x = width−245):** 12px `#2c3e50` "On real traffic the model flags 312:"; green `#008300` "• 62 real frauds"; orange `#d95926` "• 250 false alarms"; `#2c3e50` "62 / 312 = 20% precision"; then bold 12px red `#e74c3c` three lines: "4 of 5 alerts are false — the" / "balanced test hid this, because" / "it removed the 99x legit crowd".

## Regeneration instructions

- **Template:** tutorial detail page (tutorials style, per `tutorials/CLAUDE.md`). Structure: `<h1>` (no index number), `.subtitle` paragraph, then 4 `.card-section` divs each containing `<h2>` and a `table.layout` with one `<tr>`: left `<td class="text-col">` (50%) and right `<td class="viz-col">` (50%) holding one canvas.
- **Text column structure per section:** `.tags` row of colored pills (`.tag.blue` rgba(26,82,118,0.12)/#1a5276, `.tag.green` rgba(39,174,96,0.15)/#27ae60, `.tag.red` rgba(231,76,60,0.12)/#e74c3c, `.tag.orange` rgba(230,126,34,0.15)/#e67e22), then a `<ul>` of one-line bullets each opening with `<b>bold term</b>` (bold in `#1a5276`), one italic `.example` paragraph (`#555`, 0.9rem), one `.key-point` callout (background `#f8f9fa`, left border 3px solid `#e74c3c`, 0.9rem, with `<strong>` lead).
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px solid `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; `.card-section h2` 1.3rem `#1a5276` with 2px `#2980b9` bottom border; table cells padding 12px, vertical-align top; canvases `width:100%`, 1px solid `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** each 720×300 logical; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Hardcoded literal data arrays, no `Math.random()`.
- **Chart palette (JS object P):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange; red reserved for error/alarm annotations.
- In regenerated HTML, any card links use `.html` extensions (this page has no outgoing links).
