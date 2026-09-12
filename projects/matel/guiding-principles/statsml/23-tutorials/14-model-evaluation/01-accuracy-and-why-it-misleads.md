# Accuracy & Why It Misleads

**Page type:** detail page (tutorial layout: h1 + subtitle, then card-sections each with an h2 and a text/viz table row, 50% text / 50% canvas)
**HTML title tag:** Accuracy & Why It Misleads

**Subtitle:** A model that never says "fraud" still gets 95% of transactions right — accuracy can look great while the model does nothing

## The Model That Never Says Fraud

Tags: `core idea` (blue), `running example` (green)

- **The data** — 1,000 card transactions; 950 are legit, 50 are fraud (5%)
- **The lazy model** — answers "legit" for every single transaction, no exceptions
- **Its score** — right on all 950 legit, wrong on all 50 fraud: 950/1,000 = 95%
- **The catch** — it caught 0 of the 50 frauds; it does literally nothing
- **Why so high** — accuracy rewards agreeing with whichever class is most common

*Example:* A smoke alarm that never beeps is "right" every day without a fire — and useless on the one day that matters.

**Key point:** When one class dominates, accuracy mostly measures the class mix — not whether the model learned anything.

### Visualization (canvas `c1`, 720×300)

Two stacked horizontal proportion bars: the truth vs the model's report card.

- **Title (bold 15px, `#1a5276`, top center):** 'Guess "Legit" 1,000 Times: What Actually Happens'.
- **Bars:** both 560px wide × 42px tall starting at x=70, each outlined in mute gray `#6b7280`; the fraud segment is 50/1000 of the width (28px) at the right end.
  - Row 1 (y=70), left label "the truth": legit segment `rgba(42,120,214,0.30)` with bold blue (`#2a78d6`) centered text "950 legit"; fraud segment solid orange `#d95926` with bold orange text "50 fraud" to the right of the bar.
  - Row 2 (y=160), left label '"always legit"': correct segment `rgba(0,131,0,0.30)` with bold green (`#008300`) centered text '950 correct ("legit" was right)'; wrong segment solid red `#e74c3c` with bold red text "50 wrong" to the right of the bar.
- **Arrow:** vertical mute-gray arrow between the two rows with the label "model says \"legit\" every time" beside it.
- **Bottom annotation (bold 14px orange `#d95926`, center):** "950 / 1,000 correct = 95% accuracy — with 0 of 50 frauds caught".

## Scoring the Do-Nothing Model by Hand

Tags: `worked example` (green), `core idea` (blue)

- **Legit rows** — 950 transactions, model says "legit" on each: 950 correct
- **Fraud rows** — 50 transactions, model says "legit" on each: 50 wrong
- **Accuracy** — correct / total = 950 / 1,000 = 95%
- **Fraud caught** — 0 / 50 = 0%
- **Same model** — "95% accurate" and "catches nothing" describe one model

*Example:* Push the imbalance to 1% fraud and the same do-nothing model scores 99%.

**Key point:** The do-nothing model's accuracy always equals the majority share — 95% here, by construction, not by skill.

### Visualization (canvas `c2`, 720×300)

Two-bar chart: accuracy 95% vs fraud caught 0%.

- **Title:** "One Model, Two Very Different Stories".
- **Data:** labels `['accuracy', 'fraud caught']`, values `[95, 0]` %, sub-captions `['950 of 1,000', '0 of 50']`, colors `[#2a78d6 blue, #e74c3c red]`. The 0% bar is drawn as a 3px red line at the baseline instead of a filled bar.
- **Axes:** L-shaped gray axes; y ticks 0%, 50%, 100% (scale max 110) with light grid lines `#e5e9ef`; padding top 52, bottom 52, left 70, right 30. Bars 130px wide, evenly spaced; bold value labels "95%" / "0%" above.
- **Annotation:** bold red "the 95% and the 0% come from the same do-nothing model" near the top center.

## A Real Model Can Lose on Accuracy and Win on Value

Tags: `where it's used` (blue), `rule of thumb` (green)

- **A working model** — flags 100 transactions and catches 40 of the 50 frauds
- **Its mistakes** — 60 false alarms on legit customers, 10 frauds missed
- **Its accuracy** — (40 + 890) / 1,000 = 93% — LOWER than the do-nothing 95%
- **Its value** — 40 frauds stopped versus 0; accuracy ranked the useless model higher
- **Rare-event problems** — fraud, disease, churn, defects: the class you care about is rare

*Example:* Judged on accuracy alone, you would ship the never-fraud model and reject the one catching 40 frauds.

**Key point:** Judge a model on the rare class it exists to find — precision, recall, the confusion matrix — not one blended percentage.

### Visualization (canvas `c3`, 720×300)

Two side-by-side two-bar panels separated by a dashed vertical divider.

- **Title:** "Accuracy Prefers the Useless Model".
- **Divider:** dashed vertical line (`#bdc3c7`, dash 4/3) at x=360.
- **Left panel (x=30, width 300), title "accuracy":** bars "never fraud" 95% (mute `#6b7280`) and "real model" 93% (blue `#2a78d6`); scale max 110; bars 90px wide; baseline y=240, 150px chart height; bold value labels "95%" / "93%".
- **Right panel (x=390, width 300), title "frauds caught (of 50)":** bars "never fraud" 0 (mute, drawn as a 3px line at baseline) and "real model" 40 (green `#008300`); scale max 55; bold value labels "0" / "40".
- **Bottom annotation (bold 13px orange `#d95926`, center):** "93% < 95%, yet only one of these models stops any fraud".

## The Confusion: "High Accuracy" Means "Good Model"

Tags: `common mistake` (red), `imbalance` (orange)

- **The trap** — "95% accurate" sounds like an A grade; here it means "did nothing"
- **The baseline** — always ask what a model that just guesses the majority would score
- **Rarer = worse** — the rarer the fraud, the better doing nothing looks
- **Balanced data** — at a 50/50 mix accuracy is fine; it degrades as imbalance grows
- **First question** — "what's the class mix?" comes before admiring any accuracy number

*Example:* A "99.9% accurate" defect scanner on 1-in-1,000 defects may be the do-nothing score exactly.

**Common mistake:** Quoting accuracy without the class mix — the number is meaningless until you know the do-nothing baseline.

### Visualization (canvas `c4`, 720×300)

Line chart: do-nothing accuracy vs fraud rarity.

- **Title:** "The Rarer the Fraud, the Better Doing Nothing Looks".
- **Data:** x = fraud share `[50%, 20%, 10%, 5%, 1%]` (equally spaced points), y = do-nothing accuracy `[50, 80, 90, 95, 99]` %.
- **Axes:** L-shaped gray axes; y range 40–105 with ticks 50%, 75%, 100% and light grid lines `#e5e9ef`; padding top 56, bottom 54, left 70, right 40; x-axis label "share of transactions that are fraud (majority guess = \"legit\" every time)".
- **Series:** blue `#2a78d6` line, width 3, with 4px blue dots; the 5% point highlighted as a 7px orange (`#d95926`) dot with bold value label. Each point labeled with its accuracy above and its fraud share below the axis.
- **Annotation:** bold orange "our example: 5% fraud → do-nothing scores 95%" near the highlighted point.

## Regeneration instructions

- **Template/layout:** tutorial concept page (tutorials category). h1 (no index number) with 2px `#2980b9` bottom border, `.subtitle` gray one-liner, then 4 `.card-section` blocks. Each section: `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one `<tr>`: left `td.text-col` (50%) and right `td.viz-col` (50%, one 720×300 canvas).
- **Text column structure:** `.tags` pill row first, then a `<ul>` of 5 one-line bullets each opening with `<b>bold term</b>` (bold terms colored `#1a5276`), one italic `.example` line, one `.key-point` callout (`#f8f9fa` background, 3px `#e74c3c` left border). The last section's callout opens with "**Common mistake:**" instead of "**Key point:**".
- **Tag pills:** 0.72rem, weight 600, padding 2px 10px, radius 10px. Colors: blue `rgba(26,82,118,0.12)`/`#1a5276`; green `rgba(39,174,96,0.15)`/`#27ae60`; red `rgba(231,76,60,0.12)`/`#e74c3c`; orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; ul 0.92rem; canvases `width:100%` with 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Chart palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`; error/wrong segments use literal `#e74c3c`. Site palette anchors: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- **Canvas:** intrinsic `width`/`height` attributes per chart; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart titles bold 15px system-ui; labels 12–13px; all data hardcoded (no `Math.random()`). Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- In regenerated HTML, any card links use `.html` extensions (this page has no links).
