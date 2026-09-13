# Train/Serve Skew

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Train/Serve Skew

**Subtitle:** A model is trained on features computed one way and served features computed another way — same feature name, different number, so the model starts wrong on day one

## One Model, Two Kitchens

**Tags:** `core idea` (blue), `two pipelines` (green), `same name, different number` (orange)

- **The shop** — a pizza shop builds a model that predicts delivery time from features like "open orders now"
- **Training kitchen** — features for training come from clean historical logs, rebuilt overnight by a batch job
- **Serving kitchen** — features at order time come from live code reading a counter that lags a few minutes
- **Same name** — both pipelines call the feature "open orders", but they count it differently
- **The skew** — the model learned on the batch 12 but is fed the live 9 — a world it never saw

*Example (italic):* For the exact same moment, the overnight batch job counts 12 open orders while the live counter shows 9 — the model was trained on the 12 and is served the 9.

**Key point:** Train/serve skew is when the feature values a model was trained on are computed differently from the ones it receives in production — the model is answering a question nobody is asking.

### Visualization (canvas `c1`, 720×300)

Two-lane flow diagram: a TRAINING lane and a SERVING lane, each building the "open orders" feature from a different source, converging on the same model box but producing different predictions.

- **Title (bold 15px, `#1a5276`, top center):** "Same Feature Name, Two Different Kitchens".
- **Lane labels (bold 13px, left at x=20):** "TRAINING (offline)" in blue `#2a78d6` at y=85; "SERVING (live)" in orange `#d95926` at y=205.
- **Training lane (y=60–115):** three rounded boxes 140×48 at x = 110, 290, 470 with 12px `#2c3e50` two-line text: "historical logs / (cleaned overnight)" → "batch SQL job" → "open orders = **12**" (the 12 bold 14px blue); 2px blue arrows between boxes.
- **Serving lane (y=180–235):** three rounded boxes 140×48 at the same x: "live order counter / (syncs every 5 min)" → "app feature code" → "open orders = **9**" (the 9 bold 14px orange); 2px orange arrows.
- **Model box:** rounded box 110×64 centered at x=640, y=148, fill `rgba(26,82,118,0.12)`, 2px `#1a5276` border, bold 13px ink text "SAME / MODEL"; blue arrow from the training lane's last box, orange arrow from the serving lane's last box.
- **Outputs (12px, right of model box is off-canvas, so place below at y=245–265):** "trained to expect 12 → predicts 35 min" (blue) and "fed 9 → predicts 29 min" (orange), both 12px at x=430.
- **Annotation (bold 12px magenta `#d55181`, near x=250, y=150, between the lanes):** "same moment, same feature name — 12 vs 9".
- **Caption (12px `#444`, bottom left):** "illustrative — one feature shown; real systems have dozens".

## Twelve Orders on Paper, Nine on the Screen

**Tags:** `worked example` (blue), `feature mismatch` (green)

- **The rule** — the model is simple enough to run by hand: predicted minutes = 11 + 2 × open orders
- **The truth** — in the last hour 12 orders came in; the last 3 arrived within the past 5 minutes
- **The lag** — the live counter syncs every 5 minutes, so those 3 newest orders are invisible to it
- **Two answers** — training data says 11 + 2×12 = 35 min; the live app computes 11 + 2×9 = 29 min
- **The bill** — the pizza actually takes 36 min; the app promised 29 and is off by 7 minutes

*Example (italic):* The customer sees "29 minutes", waits 36, and one-stars the shop — the model was fine, its input was stale.

**Key point:** Predicted = 11 + 2 × orders, so 12 vs 9 orders is 35 vs 29 minutes — a 6-minute error created entirely by the feature pipeline, not by the model.

### Visualization (canvas `c2`, 720×300)

Single-panel order timeline for the last hour: 12 order dots placed by minutes-ago, the 3 newest drawn hollow orange because the live counter has not synced them yet, with the two resulting counts and predictions labeled.

- **Title (bold 15px, `#1a5276`, top center):** "The Last Hour: 12 Orders Happened, the Live Counter Sees 9".
- **Axis:** horizontal 2px `#999` line at y=190 from x=60 to x=660 (width 600); x = minutes ago, 60 on the left to 0 (now) on the right; 12px `#444` tick labels "60 min ago", "45", "30", "15", "now" at x = 60, 210, 360, 510, 660.
- **Order dots (8px radius, centered at y=160):** minutes-ago positions `[58, 52, 47, 41, 36, 30, 24, 17, 11, 4, 2, 1]`; the first 9 filled blue `#2a78d6`; the last 3 (at 4, 2, 1) hollow with 2.5px orange `#d95926` stroke.
- **Sync-lag zone:** light `rgba(217,89,38,0.10)` band from 5 minutes ago to now (x=610 to 660), full plot height 60–190; 11px orange label above it: "not yet synced".
- **Count labels (bold 13px, y=70–95, left side near x=70):** blue "training pipeline counts 12 → predicts 35 min"; below it orange "live counter counts 9 → predicts 29 min".
- **Annotation (bold 13px `#e74c3c`, near x=430, y=125):** "actual delivery: 36 min — the app promised 29".
- **Caption (12px `#444`, bottom right):** "illustrative — one hour at one pizza shop".

## Why the Demo Was Great and Launch Day Wasn't

**Tags:** `where it bites` (blue), `silent failure` (orange)

- **Offline glory** — the evaluation used batch features for both training and testing: error 4.1 minutes
- **Day one** — in production the live features arrive skewed, and the error jumps to about 7.2 minutes
- **No alarm** — nothing crashes and no log fills with errors; predictions are just quietly worse
- **Not decay** — the model did not rot slowly; it shipped into a world it was never trained on
- **The fix** — compute features once and reuse them, or log served features and compare to the batch rebuild

*Example (italic):* The launch review asks "did the world change?" — it didn't; the 4.1-minute model was never actually deployed, a 7.2-minute one was.

**Key point:** Offline metrics certify the model plus the batch feature pipeline; production runs the model plus a different pipeline — the day-one gap is the skew, measured in your own metric.

### Visualization (canvas `c3`, 720×300)

Single-panel line chart of live daily error over the first 10 days after launch, sitting far above the dashed offline-test error line, showing the gap is present from day one and never closes on its own.

- **Title (bold 15px, `#1a5276`, top center):** "Launch Week: Live Error vs the Offline Test Score".
- **Axes:** origin x=70, baseline y=245, plot width 580, plot height 185; x = days since launch 1 to 10, 12px `#444` tick labels "day 1" through "day 10"; y = mean error in minutes 0 to 9, 12px `#444` labels at 0, 3, 6, 9 with light `#e5e9ef` gridlines.
- **Offline line:** horizontal dashed blue `#2a78d6` (dash 6/4) 2px line at 4.1 minutes; 12px blue label at its left end: "offline test error 4.1 min".
- **Live line:** orange `#d95926` 3px line with 5px dots through days 1–10 at values `[7.2, 7.5, 6.9, 7.4, 7.1, 7.6, 7.0, 7.3, 7.5, 7.2]`.
- **Gap marker:** vertical dashed `#6b7280` (dash 4/3) line at day 1 from 4.1 up to 7.2 with small end ticks; 11px `#6b7280` label beside it: "gap ≈ 3 min".
- **Annotation (bold 13px `#e74c3c`, near day 6, y=75):** "wrong from day one — skew, not decay".
- **Caption (12px `#444`, bottom right):** "illustrative — mean absolute error per day".

## Skew Is Not Drift

**Tags:** `common mistake` (red), `skew vs drift` (orange)

- **Drift** — the world changes over months: new menu, new neighborhoods, and error creeps up slowly
- **Skew** — the world is the same but the two pipelines disagree, so error is high from the first hour
- **The tell** — drift shows a rising error curve; skew shows a flat curve that starts too high
- **Wrong cure** — retraining "fixes" drift; retraining on batch features does nothing for skew
- **Right test** — for the same request, log the served feature values and diff them against the batch rebuild

*Example (italic):* A team retrained the delivery model three times to fight "drift" and the error never moved — one feature diff would have shown 12 vs 9 in an afternoon.

**Common mistake:** Blaming a day-one performance gap on data drift and scheduling retrains. Drift grows with time; skew is born at launch — check the shape of the error curve before choosing the cure.

### Visualization (canvas `c4`, 720×300)

Single-panel comparison of the two failure shapes over 12 months: a skewed model's error is flat but high from month 1, while a drifting model's error starts at the offline score and climbs.

- **Title (bold 15px, `#1a5276`, top center):** "Two Ways to Be Wrong: Flat-and-High vs Slowly Climbing".
- **Axes:** origin x=70, baseline y=245, plot width 580, plot height 185; x = months in production 1 to 12, 12px `#444` tick labels "m1"–"m12"; y = error in minutes 0 to 10, labels at 0, 2, 4, 6, 8, 10 with light `#e5e9ef` gridlines.
- **Offline reference:** horizontal dashed `#6b7280` (dash 4/3) 1.5px line at 4.1; 11px `#6b7280` label "offline score 4.1" at its left end.
- **Skew line:** orange `#d95926` 3px line, months 1–12 values `[7.2, 7.3, 7.1, 7.2, 7.4, 7.2, 7.3, 7.1, 7.2, 7.3, 7.2, 7.3]`; bold 12px orange label "skew: high from month 1" above its left half.
- **Drift line:** violet `#4a3aa7` 3px line, months 1–12 values `[4.1, 4.3, 4.6, 5.0, 5.4, 5.9, 6.4, 7.0, 7.5, 8.1, 8.7, 9.3]`; bold 12px violet label "drift: starts fine, climbs" below its middle.
- **Crossing marker:** 6px `#2c3e50` dot where the drift line passes the skew line (month 8, value 7.0); 11px `#444` label "same error, different disease".
- **Annotation (bold 13px magenta `#d55181`, near x=200, y=80):** "the shape of the curve names the problem".
- **Caption (12px `#444`, bottom right):** "illustrative — both curves are mean error in minutes".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all dot positions, line values, and box numbers are the hardcoded arrays and literals above (no randomness); the worked-example arithmetic (11 + 2 × orders → 35 vs 29 minutes) must match both the text and the charts exactly; red `#e74c3c` appears only in genuine failure annotations (the missed promise, the day-one gap).
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
