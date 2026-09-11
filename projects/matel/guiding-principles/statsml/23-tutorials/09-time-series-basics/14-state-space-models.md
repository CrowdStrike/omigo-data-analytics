# State-Space Models

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** State-Space Models

**Subtitle:** Every series is two things at once — a hidden state that evolves smoothly and the noisy readings you actually see; a state-space model recovers the line from the dots

## The Bathroom Scale You Can't Quite Trust

**Tags:** `core idea` (blue), `hidden state` (green), `observed noise` (orange)

- **The ritual** — you weigh in every morning for 14 days; the scale says 183.0, 180.3, 181.8, ...
- **Hidden state** — your true weight is real but never seen; it drifts 181.6 → 179.0 lb (−0.2/day)
- **Observed noise** — each reading = true weight + scale error, bouncing as much as ±1.8 lb a day
- **Two rules** — a state rule (weight drifts slowly) and an observation rule (the scale adds error)
- **The job** — recover the hidden line from the noisy dots; that pair of rules is a state-space model

*Example (italic):* Day 7 reads 182.2 and day 8 reads 178.9 — you did not lose 3.3 lb overnight; the hidden weight moved only 0.2.

**Key point:** A state-space model splits a time series into a hidden state that evolves by a simple rule and noisy observations of it. You see the dots; you want the line.

### Visualization (canvas `c1`, 720×300)

Single-panel line chart: 14 noisy scale readings as dots over the smooth hidden true-weight line.

- **Title (bold 15px, `#1a5276`, top center):** "14 Mornings on the Scale: Noisy Readings vs Hidden True Weight (illustrative)".
- **Data:** days 1–14; true weight `[181.6, 181.4, 181.2, 181.0, 180.8, 180.6, 180.4, 180.2, 180.0, 179.8, 179.6, 179.4, 179.2, 179.0]`; readings `[183.0, 180.3, 181.8, 179.4, 182.0, 180.2, 182.2, 178.9, 180.2, 178.9, 181.1, 178.8, 180.1, 177.3]`.
- **Axes:** origin x=60, plot width 620, baseline y=250, chart height 195; y range 176.5–183.5 with ticks 177, 179, 181, 183 (12px `#444`, light grid lines `#e5e9ef`); day numbers 1–14 12px `#444` below baseline.
- **Readings:** blue `#2a78d6` 5px dots joined by a thin 1.5px `rgba(42,120,214,0.35)` zig-zag line.
- **True weight:** ink `#1a5276` dashed (dash 6/4) 3px line, no dots.
- **Annotations:** magenta `#d55181` bold 12px near days 7–8: "reading drops 3.3 lb overnight"; green `#008300` bold 13px along the dashed line's right end: "hidden weight drifts just −0.2 lb/day".
- **Legend (top right, 12px):** blue dot "scale reading (observed)", ink dash "true weight (hidden)".

## Predict, Peek, Nudge — One Update by Hand

**Tags:** `worked example` (blue), `predict & correct` (green)

- **Start** — yesterday's best estimate of your true weight is 180.0 lb (numbers rounded for hand math)
- **Predict** — the state rule says weight drifts −0.2 lb/day, so today's prediction is 179.8
- **Peek** — the scale reads 181.0; the gap between reading and prediction is +1.2 lb
- **Nudge** — trust the scale 25%: move a quarter of the gap, +0.3, giving today's estimate 180.1
- **Repeat** — next day: predict 179.9, read 178.7, gap −1.2, nudge −0.3, estimate 179.6
- **The gain** — the 25% trust knob is the gain: a noisier scale earns a smaller gain and smaller nudges

*Example (italic):* Day three is the same arithmetic: predict 179.4, read 180.2, gap +0.8, nudge +0.2, estimate 179.6.

**Key point:** Filtering is predict-then-correct: run the state rule forward, then move a fixed fraction of the way toward the new reading. This recipe is a Kalman filter with a fixed gain.

### Visualization (canvas `c2`, 720×300)

Three-day "predict, peek, nudge" ladder: for each day a prediction marker, a reading marker, and the resulting estimate one quarter of the way between them.

- **Title (bold 15px, `#1a5276`, top center):** "Three Mornings of Predict → Peek → Nudge (gain = 25%)".
- **Data:** day B: prediction 179.8, reading 181.0, estimate 180.1; day C: prediction 179.9, reading 178.7, estimate 179.6; day D: prediction 179.4, reading 180.2, estimate 179.6.
- **Axes:** y axis at x=60 with range 178–182, ticks 178, 179, 180, 181, 182 (12px `#444`, light grid `#e5e9ef`); baseline y=250, chart height 195; day groups centered at x=200 ("day B"), x=400 ("day C"), x=600 ("day D"), labels 13px bold `#444` below baseline.
- **Per day:** open blue `#2a78d6` circle (7px, 2.5px stroke, white fill) at the prediction; magenta `#d55181` filled 6px dot at the reading; grey dashed 1.5px vertical line connecting them; green `#008300` filled 7px square at the estimate with a short green 3px arrow from prediction toward reading stopping at the estimate (25% of the gap).
- **Value labels (12px):** each marker labeled with its number ("179.8", "181.0", "180.1", etc.) offset left/right to avoid overlap; prediction labels blue, readings magenta, estimates bold green.
- **Legend (top left, 12px):** open blue circle "prediction", magenta dot "scale reading", green square "estimate (25% nudge)".
- **Annotation:** green bold 12px under day B: "estimate = prediction + 0.25 × gap".

## One Diagram, Many Famous Names

**Tags:** `where it's used` (blue), `unifying framework` (green), `kalman & HMM` (orange)

- **The pattern** — hidden states x1 → x2 → x3 → x4 evolve by a rule; each xt emits one noisy reading yt
- **Kalman filter** — continuous state like the weight example; runs GPS, rockets, and sensor fusion
- **HMM** — hidden Markov model: the same picture with a categorical state, e.g. asleep vs awake
- **Exponential smoothing** — the classic forecaster is a state-space model whose one state is the level
- **Filtering vs smoothing** — filtering estimates today from the past; smoothing revisits day 5 with hindsight
- **Forecasting** — run the state rule forward with no new readings left to nudge you

*Example (italic):* A GPS chip, a speech recognizer, and a demand forecaster all draw this exact two-row diagram — only the meaning of x changes.

**Key point:** Kalman filters, HMMs, exponential smoothing, and structural time-series models are one framework: pick what the hidden state is, how it moves, and how noisily you observe it.

### Visualization (canvas `c3`, 720×300)

Two-row graphical-model diagram: a chain of four hidden-state boxes on top, four observation circles below, with transition and emission arrows.

- **Title (bold 15px, `#1a5276`, top center):** "The State-Space Diagram: Hidden Chain on Top, Noisy Readings Below".
- **State boxes (top row):** rounded rects 96×44 centered at (140, 105), (300, 105), (460, 105), (620, 105); fill `rgba(42,120,214,0.12)`, 2px blue `#2a78d6` border; labels bold 13px `#1a5276`: "x1 181.6", "x2 181.4", "x3 181.2", "x4 181.0" (the first four true weights).
- **Transition arrows:** violet `#4a3aa7` 2.5px horizontal arrows between consecutive boxes; one violet bold 12px label above the middle arrow: "state rule: drift −0.2 + noise".
- **Observation circles (bottom row):** radius 24 centered at (140, 215), (300, 215), (460, 215), (620, 215); fill `rgba(0,131,0,0.10)`, 2px green `#008300` border; labels bold 13px `#008300`: "y1 183.0", "y2 180.3", "y3 181.8", "y4 179.4" (the first four readings).
- **Emission arrows:** green `#008300` 2.5px vertical arrows from each box down to its circle; one green bold 12px label right of the last arrow: "observation rule: + scale error".
- **Annotation:** magenta `#d55181` bold 12px left of the bottom row: "you only ever see this row".
- **Caption strip (bold 12px `#1a5276`, centered at y=280):** "Kalman filter · HMM · exponential smoothing · structural models — same diagram, different state".

## It's Not Just a Fancy Moving Average

**Tags:** `common mistake` (red), `moving average` (orange)

- **The shortcut** — a 5-day moving average also smooths the dots, so why bother with a model?
- **It lags** — the average of days 1–5 describes day 3; every MA point trails the drifting truth
- **It can't start** — no MA exists until day 5; the filter produces an estimate from day 1
- **It can't forecast** — an MA has no state rule; the filter predicts tomorrow: 178.75 − 0.2 ≈ 178.6
- **No uncertainty** — a full state-space model also tracks how sure it is; an average cannot
- **Similar lines** — the two curves can look alike, but only one explains the data and extends it

*Example (italic):* On day 14 the MA still reads 179.24 — roughly where the true weight was two days earlier — while the filter sits at 178.75 and already has tomorrow's forecast.

**Common mistake:** Treating any smooth line through noisy data as the same thing. A moving average describes the past; a state-space model explains the series and can predict beyond it.

### Visualization (canvas `c4`, 720×300)

Comparison chart on the same 14 mornings: faint reading dots, hidden true line, trailing 5-day moving average, filter estimate, and a one-day-ahead forecast dot.

- **Title (bold 15px, `#1a5276`, top center):** "Moving Average vs State-Space Filter on the Same 14 Mornings".
- **Axes:** origin x=60, plot width 620, baseline y=250, chart height 195; x spans days 1–15; y range 176.5–183.5, ticks 177, 179, 181, 183 (12px `#444`, light grid `#e5e9ef`); day numbers 1–15 12px `#444` below baseline.
- **Readings (context):** faint blue `rgba(42,120,214,0.35)` 4px dots at `[183.0, 180.3, 181.8, 179.4, 182.0, 180.2, 182.2, 178.9, 180.2, 178.9, 181.1, 178.8, 180.1, 177.3]`, no connecting line.
- **True weight:** ink `#1a5276` dashed (dash 6/4) 2px line through `[181.6, 181.4, 181.2, 181.0, 180.8, 180.6, 180.4, 180.2, 180.0, 179.8, 179.6, 179.4, 179.2, 179.0]`.
- **Trailing 5-day MA:** orange `#d95926` 3px line for days 5–14 only: `[181.30, 180.74, 181.12, 180.54, 180.70, 180.08, 180.26, 179.58, 179.82, 179.24]`.
- **Filter estimate:** green `#008300` 3px line for days 1–14 (gain 0.25, drift −0.2, start 181.5): `[181.50, 181.05, 181.09, 180.52, 180.74, 180.45, 180.74, 180.13, 180.00, 179.57, 179.80, 179.40, 179.43, 178.75]`.
- **Forecast:** violet `#4a3aa7` filled 7px dot at day 15, value 178.55, joined to the day-14 filter point by a violet dashed 2px segment; violet bold 12px label "tomorrow's forecast ≈ 178.6".
- **Annotations:** orange bold 12px above the MA's left end: "MA starts day 5 and trails the drift"; green bold 12px below the filter's left end: "filter runs from day 1".
- **Legend (top right, 12px):** faint blue dot "readings", ink dash "true weight", orange line "5-day MA", green line "filter estimate", violet dot "forecast".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- All series are hardcoded literal arrays (no `Math.random()`); readings = true weight + fixed noise offsets `[+1.4, −1.1, +0.6, −1.6, +1.2, −0.4, +1.8, −1.3, +0.2, −0.9, +1.5, −0.6, +0.9, −1.7]`; the filter series in c4 follows the section-2 recipe (predict with drift −0.2, correct 25% of the gap) starting from 181.5.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
