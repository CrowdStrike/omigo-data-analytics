# Dropout

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column `table.layout` — text left 50%, canvas right 50%)
**HTML title tag:** Dropout

**Subtitle:** During training, randomly silence 30% of the neurons on every step so no neuron can freeload on another — at prediction time everyone is back on

## Three Random Team Members Stay Home Each Morning

**Tags:** `core idea` (blue), `running example` (green)

- **The team** — picture a layer of 10 neurons as a team of 10 workers
- **The rule** — each step ~30% are randomly silenced; this toy keeps it at exactly 3 workers
- **Fresh draw** — a different random 3 each step; nobody knows who is off tomorrow
- **The rest cover** — the 7 active neurons must handle the example on their own
- **No freeloading** — a neuron can't lean on a teammate that might be silent next step

*Example:* A shop where 3 random staff are out daily: everyone ends up learning the till, the stock, and the returns desk.

**Key point:** Dropout randomly silences a fraction of neurons each training step, forcing the rest to carry the work alone.

### Visualization (canvas `c1`, 720×300)

Dot grid: 10 neurons × 5 training steps, 3 silenced per step.

- **Title (bold 15px `#1a5276`, top center):** "Five Training Steps: a Different Random 3 of 10 Silenced Each Time".
- **Grid:** starts at (150,62); 10 columns 52px apart, 5 rows 38px apart; circles radius 12. Column headers gray 12px "n1"…"n10"; row labels bold `#444` 12px right-aligned "step 1"…"step 5".
- **Silenced sets (hardcoded, 0-indexed neurons off per step):** step 1 `[1, 4, 8]`, step 2 `[0, 5, 9]`, step 3 `[2, 3, 7]`, step 4 `[4, 6, 9]`, step 5 `[0, 2, 8]`.
- **Active circles:** filled `rgba(42,120,214,0.75)`. **Silenced circles:** fill `#f1f3f5`, dashed gray (`#6b7280`, dash 3/3) outline, orange (`#d95926`) 2px X drawn across.
- **Legend (bold 12px):** blue "blue = active (7 per step)" at x=150, orange "crossed = silenced (3 per step, 30%)" at x=340, both y=262.
- **Takeaway (bold 13px magenta `#d55181`, bottom center):** "no neuron can rely on a teammate — anyone might be off next step".

## Counting the Silences: About 30 Out of 100 Steps Each

**Tags:** `worked example` (green), `rule of thumb` (blue)

- **The setting** — layer of 10 neurons, dropout rate 0.3, so 3 silenced per step here
- **Over 100 steps** — 100 × 3 = 300 silences shared among 10 neurons
- **Fair share** — each neuron expects 300 ÷ 10 = 30 silences, i.e. off ~30% of steps
- **Our tally** — the ten counts came out 31, 28, 30, 33, 27, 29, 32, 30, 26, 34
- **Check the sum** — those ten counts add to exactly 300 in this fixed-3 toy

*Example:* Neuron 10 sat out 34 steps and neuron 9 only 26 — random draws wobble around 30, and that is fine.

**Key point:** Rate 0.3 means each neuron trains on only ~70% of steps — and must be useful in whichever 70% it gets.

### Visualization (canvas `c2`, 720×300)

Column chart: silences per neuron over 100 steps with an expected line.

- **Title (bold 15px `#1a5276`, top center):** "Times Silenced in 100 Steps — Everyone Hovers Around 30".
- **Data:** counts per neuron n1–n10: `[31, 28, 30, 33, 27, 29, 32, 30, 26, 34]`; y scale 0–40; padding top 60, bottom 62, left 62, right 40; bar width 62% of slot; gray L-shaped axes.
- **Expected line:** horizontal dashed green (`#008300`, dash 6/4, width 2) at y=30, labeled bold green 12px "expected: 30 (rate 0.3 × 100 steps)".
- **Bars:** 0.65 alpha, colors cycling `#2a78d6`, `#199e70`, `#4a3aa7`, `#d55181`, `#c98500` (repeating for n6–n10); bold `#222` count labels above bars; gray "n1"…"n10" below.
- **Axis captions (`#444` 12px):** x "neuron"; rotated y "steps silenced (of 100)".
- **Takeaway (bold 13px magenta `#d55181`, bottom center):** "the ten counts sum to exactly 300 = 100 steps × 3 silenced".

## Why Bother: Redundant Skills Beat One Star Player

**Tags:** `where it's used` (blue), `best practice` (green)

- **Without dropout** — one neuron can memorize a quirk and the others just echo it
- **That's overfitting** — our net scores 99% on training photos but only 84% on new ones
- **With dropout 0.3** — training dips to 95%, but new photos jump to 91%
- **The trade** — give up 4 points on memorized data, gain 7 on data that matters
- **Cheap insurance** — one line of code in any framework; rates 0.2–0.5 are typical

*Example:* A team that rehearsed with random absences barely notices when one member has an off day on match day.

**Key point:** Dropout hurts the training score on purpose — the payoff is a better score on data the model has never seen.

### Visualization (canvas `c3`, 720×300)

Grouped bar chart: training vs new-photo accuracy, with and without dropout.

- **Title (bold 15px `#1a5276`, top center):** "Accuracy With and Without Dropout (illustrative)".
- **Data:** group "no dropout": train 99%, test 84%; group "dropout 0.3": train 95%, test 91%. Y range 75–102; padding top 60, bottom 70, left 62, right 180; bars 74px wide at 0.7 alpha, train bar blue `#2a78d6`, test bar orange `#d95926`; bold colored percentage labels above bars; bold `#222` group labels below.
- **Gap annotations (bold 12px, centered under groups):** magenta `#d55181` "15-point gap: memorizing" (left group); green `#008300` "4-point gap: learning" (right group).
- **Legend (right side, 12px swatches):** blue "training photos", orange "new photos".
- **Side annotation (bold 13px orange, two lines):** "84% → 91% on the" / "data that matters".

## The Confusion: Dropout Is Off at Prediction Time

**Tags:** `common mistake` (red), `trade-off` (orange)

- **Training only** — the random silencing happens while learning, never while predicting
- **Prediction time** — all 10 neurons are on; the full trained team answers together
- **Not pruning** — no neuron is deleted; each is only benched for random steps
- **Not a fixed 3** — real dropout coin-flips each neuron: ~3 of 10 on average, not exactly 3
- **Frameworks handle it** — model.eval() turns dropout off and auto-rescales so sums still match

*Example:* Forgetting eval() means predictions still silence random neurons — the same photo gets different answers each call.

**Key point:** Silence some neurons to train, use all of them to predict — mixing these two modes up is the classic dropout bug.

### Visualization (canvas `c4`, 720×300)

Two-panel diagram split by a dashed divider at x=360: training mode vs prediction mode.

- **Title (bold 15px `#1a5276`, top center):** "Two Modes: Silence to Train, Full Team to Predict".
- **Divider:** vertical dashed `#bdc3c7` line (dash 4/3) at x=360.
- **Each panel:** 10 neuron circles (radius 17) in a 5×2 grid (55px column pitch, 60px row pitch), active circles filled `rgba(42,120,214,0.75)` with white bold "n1"…"n10" labels; silenced circles fill `#f1f3f5`, dashed gray outline, orange 2.5px X.
- **Left panel (center x=185):** header bold 14px orange "TRAINING: 3 of 10 silenced this step"; neurons 2, 5, 9 (indices `[1, 4, 8]`) silenced; gray 12px caption "fresh random trio next step".
- **Right panel (center x=540):** header bold 14px green "PREDICTION: all 10 on, every time"; no neurons silenced; green 12px caption "same photo in → same answer out".
- **Takeaway (bold 13px magenta `#d55181`, bottom center):** "leaving dropout on at prediction time makes answers random — the classic bug".

## Regeneration instructions

- **Layout:** tutorial detail page. `<h1>` (no index number), `.subtitle` paragraph, then four `.card-section` blocks; each has an `<h2>` (1.3rem `#1a5276`, 2px solid `#2980b9` bottom border) and a `table.layout` (full width, border-collapse) with one `<tr>`: left `td.text-col` (50%) and right `td.viz-col` (50%), both 12px padding, top-aligned.
- **Left column structure:** `.tags` row of colored pill spans (0.72rem, 600 weight, 2px 10px padding, 10px radius — blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`); then a `<ul>` (0.92rem) of bullets each opening with `<b>` term in `#1a5276`; one italic `.example` paragraph (`#555`, 0.9rem); one `.key-point` callout (background `#f8f9fa`, left border 3px solid `#e74c3c`, 8px 12px padding, 0.9rem) starting with `<strong>Key point:</strong>`.
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; subtitle `#666` 0.95rem. No nav bar, no back/home links.
- **Canvas:** each canvas declared `width="720" height="300"`, CSS `width:100%`, 1px `#e0e0e0` border, 4px radius; a shared `setup(id)` helper scales by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). The per-step silenced-neuron sets are a shared hardcoded array `offSteps` used by c1. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** JS object `P = { blue:#2a78d6, green:#008300, magenta:#d55181, yellow:#c98500, aqua:#199e70, orange:#d95926, violet:#4a3aa7, ink:#1a5276, text:#2c3e50, mute:#6b7280, grid:#e5e9ef }`. Doc palette anchors: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`. All chart data hardcoded (no `Math.random()`).
- In regenerated HTML, any card links use `.html` extensions (this page has no outgoing links).
