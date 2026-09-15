# The Life of an ML Prediction

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** The Life of an ML Prediction

**Subtitle:** One card swipe traced end to end — request, features, score, decision, log, and the label that comes back days later to retrain the model

## One Swipe, Eight Milliseconds

**Tags:** `core idea` (blue), `serving path` (green), `latency budget` (orange)

- **The request** — a $480 card swipe at 2:14am arrives with raw fields: card id, merchant, amount
- **The lookup** — the serving layer fetches a precomputed feature from the online store: 30-day txn count = 42
- **The fresh math** — one feature is computed on the spot: $480 vs the user's $62 average → ratio 7.7
- **The score** — the fraud model takes the feature vector and returns 0.87 in 4ms
- **The budget** — 3ms features + 4ms model + 1ms rules = 8ms of the 50ms answer-the-terminal budget

*Example (italic):* The card terminal needs an answer in 50ms; the whole trace — fetch, score, decide — uses 8ms and the shopper never feels it.

**Key point:** A prediction is not one call — it is a pipeline of hops (fetch features, score, apply rules), each spending part of a hard latency budget.

### Visualization (canvas `c1`, 720×300)

Left-to-right flow diagram of the serving path with per-hop latencies, plus a stacked latency bar underneath showing 8ms used of the 50ms budget.

- **Title (bold 15px, `#1a5276`, top center):** "One Prediction's Path: 8ms Spent of a 50ms Budget".
- **Flow row (boxes centered at y=105, each 120px wide, 46px tall, 8px radius, 12px `#2c3e50` text, 3px `#6b7280` arrows between):** "request $480" at x=20 (fill `rgba(42,120,214,0.15)`, border `#2a78d6`) → "features 3ms" at x=165 (fill `rgba(25,158,112,0.15)`, border `#199e70`) → "model 4ms" at x=310 (fill `rgba(74,58,167,0.12)`, border `#4a3aa7`) → "rules 1ms" at x=455 (fill `rgba(217,89,38,0.15)`, border `#d95926`) → "decision + log" at x=600 (fill `rgba(26,82,118,0.12)`, border `#1a5276`).
- **Sub-labels (11px `#6b7280`, under each box):** "raw fields", "online store + real-time", "score 0.87", "0.87 > 0.8", "step-up".
- **Budget bar (y=210, x from 60 to 660, 22px tall, background `rgba(229,233,239,0.9)` with 1px `#999` border = 50ms):** stacked segments scaled 12px/ms — green `#199e70` width 36 ("3ms"), violet `#4a3aa7` width 48 ("4ms"), orange `#d95926` width 12 ("1ms"); 12px `#444` segment labels above, 12px `#6b7280` label "unused 42ms" inside the empty region.
- **Annotation (bold 13px green `#008300`, right of the bar at y=255):** "8ms of 50ms — latency budget respected".
- **Caption (12px `#444`, bottom right):** "hop latencies illustrative; 3+4+1=8 exact".

## From 0.87 to "Ask for a Code"

**Tags:** `worked example` (blue), `decision layer` (green)

- **The threshold** — the risk policy splits scores into bands: below 0.80 approve, 0.80–0.95 step-up, above 0.95 block
- **Hand-check** — 0.87 ≥ 0.80 is true and 0.87 < 0.95 is true, so the band is step-up verification
- **Why not block** — silently declining a real $480 purchase loses a customer; a text code costs 20 seconds
- **The rules** — business rules run after the score: trusted-merchant allowlists can downgrade a step-up to approve
- **The log** — one row records request, both feature values, score 0.87, and decision "step-up" with a timestamp

*Example (italic):* The shopper gets a one-time code by text, types it in 20 seconds, and the $480 purchase completes — no analyst ever touches it.

**Key point:** The model outputs a score, not a decision — a separate decision layer maps 0.87 into an action, and every input to that mapping is logged.

### Visualization (canvas `c2`, 720×300)

Horizontal score scale from 0 to 1 with three colored decision bands, five illustrative transaction scores plotted as dots, and the traced transaction's 0.87 highlighted.

- **Title (bold 15px, `#1a5276`, top center):** "Score Bands: Where 0.87 Lands".
- **Scale:** horizontal 2px `#999` axis at y=170, x from 60 to 660 mapping score 0→1; 12px `#444` tick labels at 0, 0.2, 0.4, 0.6, 0.8, 0.95, 1.0.
- **Bands (36px tall rectangles sitting on the axis, y=134 to 170):** approve 0–0.80 fill `rgba(0,131,0,0.18)`, step-up 0.80–0.95 fill `rgba(217,89,38,0.25)`, block 0.95–1.0 fill `rgba(231,76,60,0.25)`; bold 12px band labels centered above each: green `#008300` "approve", orange `#d95926` "step-up", red `#e74c3c` "block".
- **Dashed cutoffs:** vertical dashed `#6b7280` (dash 4/3) lines at 0.80 and 0.95, 11px `#6b7280` labels "0.80" and "0.95" at their tops.
- **Other transactions:** 8px `#2a78d6` dots on the axis at scores `[0.12, 0.34, 0.55, 0.79, 0.97]`, 11px `#6b7280` value labels below.
- **This transaction:** 12px `#d95926` dot at 0.87 with a 3px orange stem rising to y=90 and bold 13px orange `#d95926` label "this swipe: 0.87 → step-up".
- **Annotation (bold 12px violet `#4a3aa7`, below the axis near x=440, y=225):** "0.79 approves, 0.87 steps up — the threshold is policy, not math".
- **Caption (12px `#444`, bottom right):** "scores illustrative; band checks exact".

## The Label Arrives Six Days Late

**Tags:** `closing the loop` (blue), `delayed labels` (green), `feedback loop` (orange)

- **The gap** — fraud truth arrives as a chargeback or a customer report; here it lands on day 6, not at serving time
- **The join** — the day-6 label "fraud = yes" joins the day-0 log row by transaction id
- **Censoring** — a day-30 retrain only sees labels old enough to have arrived; last week's txns are still unlabeled
- **Feedback** — blocked txns never complete, so they never get a chargeback label; the model's own decisions shape its next training set
- **The retrain** — the monthly retrain consumes joined log rows, and the new model starts scoring day-31 swipes

*Example (italic):* The day-0 log row plus the day-6 fraud label becomes one training example in the day-30 retrain — the loop closes in 30 days.

**Key point:** Retraining eats the serving logs, so the loop only closes if labels can find their log rows — and blocked traffic quietly drops out of the loop entirely.

### Visualization (canvas `c3`, 720×300)

Circular loop diagram with five stages around a ring, day stamps on each, and a red side-branch showing blocked transactions exiting the loop unlabeled.

- **Title (bold 15px, `#1a5276`, top center):** "The Loop: Serve → Log → Label → Retrain → Serve".
- **Ring:** five rounded boxes (110px wide, 42px tall, 8px radius, 12px `#2c3e50` text) placed clockwise — "score + decide" top center (360, 70, border `#2a78d6`), "log row written" right (570, 130, border `#1a5276`), "label joins day 6" bottom right (500, 235, border `#008300`), "retrain day 30" bottom left (200, 235, border `#4a3aa7`), "new model day 31" left (120, 130, border `#199e70`); 3px `#6b7280` curved or elbow arrows connecting them clockwise back to the top.
- **Day stamps (bold 11px `#6b7280`, beside each box):** "day 0", "day 0", "day 6", "day 30", "day 31".
- **Blocked branch:** 3px red `#e74c3c` dashed arrow leaving "score + decide" toward x=640, y=55 into a red-bordered box "blocked — no label ever" (fill `rgba(231,76,60,0.12)`), bold 12px red label "exits the loop".
- **Annotation (bold 13px violet `#4a3aa7`, center of the ring near (360, 160)):** "retraining sees only what came back around".
- **Caption (12px `#444`, bottom right):** "day counts illustrative".

## Accuracy Looks Fine While the Features Rot

**Tags:** `common mistake` (red), `feature drift` (orange)

- **The break** — a nightly online-store job starts writing nulls for 30-day txn count; serving imputes them as 0
- **The skew** — training computed the feature from clean warehouse data, serving now feeds zeros: silent train/serve skew
- **The mask** — the accuracy dashboard joins on delayed labels, so today's number describes predictions from weeks ago
- **The symptom** — the step-up rate creeps from 2% to 9% of swipes before anyone opens the feature dashboard
- **The fix** — monitor serving-time feature null rates and distributions directly, not just downstream accuracy

*Example (italic):* For five weeks the accuracy chart reads ~96% while the feature's null rate climbs past 30% — the drop only shows once the late labels catch up.

**Common mistake:** Watching only model accuracy. Label delay makes accuracy a weeks-old rearview mirror, so a silently drifting feature pipeline can degrade every live decision long before the metric moves.

### Visualization (canvas `c4`, 720×300)

Dual-line timeline over 8 weeks: the feature's serving-time null rate climbing (red, left axis) while reported accuracy stays flat and only dips at the end (blue, right axis).

- **Title (bold 15px, `#1a5276`, top center):** "The Feature Breaks in Week 2 — Accuracy Notices in Week 7".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = weeks 0 to 8, 12px `#444` tick labels every week; left y = null rate 0–50% (12px `#e74c3c` labels at 0/25/50), right y at x=660 = accuracy 90–100% (12px `#2a78d6` labels at 90/95/100); gridlines `#e5e9ef`.
- **Null-rate line:** red `#e74c3c` 3px line through weeks `[0, 1, 2, 3, 4, 5, 6, 7, 8]`, null % `[0, 0, 1, 8, 16, 24, 31, 37, 40]`.
- **Accuracy line:** blue `#2a78d6` 3px line through the same weeks, accuracy % `[96.1, 96.0, 96.2, 96.1, 95.9, 96.0, 95.4, 93.8, 91.9]`.
- **Break marker:** vertical dashed `#6b7280` (dash 4/3) line at week 2, 12px `#6b7280` label "store job breaks" at its top.
- **Annotation (bold 13px red `#e74c3c`, near week 5, upper area y=75):** "features rot for 5 weeks before accuracy moves".
- **Annotation (bold 12px blue `#2a78d6`, near week 7.5 above the blue line):** "late labels finally catch up".
- **Caption (12px `#444`, bottom right):** "null rates and accuracy illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); latencies, scores, day counts, null rates, and accuracy values are invented and labeled illustrative; the latency sum 3+4+1=8ms and the band checks 0.87 ≥ 0.80 / 0.87 < 0.95 are exact arithmetic. The score 0.87, threshold 0.80, feature values (30-day txn count 42, $480 vs $62 average, ratio 7.7) must match between text and charts.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
