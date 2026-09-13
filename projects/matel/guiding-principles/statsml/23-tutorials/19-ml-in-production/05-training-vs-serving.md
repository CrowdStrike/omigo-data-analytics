# Training vs Serving

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Training vs Serving

**Subtitle:** A model lives twice — once at night, learning a rule from a year of history, and once every morning, answering one live question in milliseconds — and most production failures happen where the two lives don't match

## The Night Job and the Morning Job

**Tags:** `core idea` (blue), `two lives` (green), `batch vs live` (orange)

- **The bakery** — a bakery wants a model that answers one question each dawn: how many croissants to bake?
- **The night job** — training: the model reads 365 days of sales logs and learns a rule; it runs once, takes hours
- **The morning job** — serving: at 6am the saved model gets today's numbers and must answer in milliseconds
- **Different worlds** — training sees a clean table of history; serving sees one messy live row at a time
- **One artifact** — the night job's only output is a model file; the morning job replays it thousands of mornings

*Example (italic):* The rule is learned once from last year's logs, but "bake 116" gets asked and answered fresh every single morning.

**Key point:** Training and serving are two different programs sharing one model file — one learns slowly from history, the other answers instantly from live data.

### Visualization (canvas `c1`, 720×300)

Two-lane flow diagram: a training lane on top and a serving lane below, each a left-to-right chain of rounded boxes joined by arrows, meeting at a shared "model file" box.

- **Title (bold 15px, `#1a5276`, top center):** "One Model, Two Lives: the Night Job and the Morning Job".
- **Lane labels (bold 13px, left at x=20):** "TRAINING" in blue `#2a78d6` at y=90; "SERVING" in orange `#d95926` at y=210.
- **Training lane (boxes centered at y=95, height 44, rounded 6px, 1.5px blue border, fill `rgba(42,120,214,0.10)`):** box 1 at x=110–250 "1 year of sales logs / (365 rows)"; box 2 at x=290–420 "learn the rule"; 2px blue arrows between boxes; 12px `#6b7280` label under the lane at y=135: "runs once, offline, hours are fine".
- **Serving lane (boxes centered at y=215, height 44, rounded 6px, 1.5px orange border, fill `rgba(217,89,38,0.10)`):** box 1 at x=110–250 "this morning's numbers / (1 row)"; box 2 at x=290–420 "apply the rule"; box 3 at x=460–610 "answer: bake 116"; 2px orange arrows between; 12px `#6b7280` label under the lane at y=255: "runs every morning, live, milliseconds".
- **Model file:** shared rounded box at x=460–610 centered at y=95, 2px `#1a5276` border, fill `#f8f9fa`, bold 12px `#1a5276` text "model file"; 2px `#1a5276` dashed (dash 5/4) arrow from its bottom edge down to serving box 2 ("apply the rule").
- **Annotation (bold 13px violet `#4a3aa7`, near x=640, y=160):** two lines: "built once —" / "answers every day".
- **Box text:** 12px `#2c3e50`, two lines where shown with "/"; all coordinates are in the 720×300 logical space.

## Monday 6am: Same Model, Different Answer

**Tags:** `worked example` (blue), `training–serving skew` (red)

- **The rule** — training learned: croissants = 20 + 0.8 × (7-day average sales); simple enough to check by hand
- **Training's view** — the clean logs show last week's sales averaging 120, so the rule says 20 + 0.8×120 = 116
- **Serving's view** — at 6am Sunday's row is still half-written: only 36 sales logged of the real 120
- **The skewed feature** — the live average comes out 108 instead of 120, so the model says 20 + 0.8×108 ≈ 106
- **Same model, wrong input** — nothing about the world changed; the two pipelines computed one feature differently
- **The name** — this gap between offline and live feature values is called training–serving skew

*Example (italic):* The bakery bakes 106 instead of 116 and sells out by 9am — the model was fine, the 6am feature was not.

**Key point:** 20 + 0.8×120 = 116 offline but 20 + 0.8×108 ≈ 106 live — when the same feature is computed two ways, the same model gives two answers.

### Visualization (canvas `c2`, 720×300)

Grouped bar chart with two groups on the x axis — the 7-day-average feature and the predicted croissants — each holding a training-pipeline bar and a serving-pipeline bar, exposing the skew.

- **Title (bold 15px, `#1a5276`, top center):** "Same Model, Same Monday — Two Pipelines, Two Answers".
- **Axes:** origin x=70, baseline y=245, plot width 560, plot height 185; y = 0 to 140 with light `#e5e9ef` gridlines and 12px `#444` tick labels at 0, 40, 80, 120; x = two group labels (13px `#2c3e50`, centered under groups): "7-day average sales (feature)" and "predicted croissants".
- **Bars (width 70, gap 16 within a group; groups centered at x=210 and x=490):** group 1 — training blue `#2a78d6` bar height for value `120`, serving orange `#d95926` bar for value `108`; group 2 — training blue bar for value `116`, serving orange bar for value `106`; bold 13px value labels above each bar in the bar's color: "120", "108", "116", "106".
- **Legend (12px, top right near x=560, y=60):** blue swatch "training pipeline", orange swatch "serving pipeline (6am)".
- **Gap markers:** thin dashed `#6b7280` (dash 4/3) bracket above each group spanning the two bar tops, with 12px `#6b7280` labels "−12" (group 1) and "−10" (group 2).
- **Annotation (bold 12px red `#e74c3c`, centered near x=365, y=48):** two lines: "the feature changed, not the world —" / "Sunday's row was still half-written".
- **Caption (12px `#444`, bottom right):** "illustrative bakery numbers".

## The World Keeps Moving After Training

**Tags:** `why it matters` (blue), `drift` (orange), `retraining` (green)

- **Frozen rule, moving world** — training ends but customers, seasons, and menus keep changing under the model
- **Drift** — month by month the frozen rule fits the street a little worse; the daily miss creeps upward
- **Serving is the scoreboard** — offline scores are rehearsal; wasted trays and sell-outs happen at serving time
- **Retraining** — rerunning the night job on fresh logs snaps the rule back to the current street
- **Watch the live miss** — teams track serving error week by week and retrain when it drifts past a threshold

*Example (italic):* A new office opens next door in week 4; the never-retrained model's average daily miss climbs from 4 croissants to 23 by week 10.

**Key point:** Training happens once but serving never stops — without monitoring and retraining, a model that was right in January is quietly wrong by June.

### Visualization (canvas `c3`, 720×300)

Two-line time-series chart: average daily prediction miss over ten weeks for a never-retrained model versus one retrained every three weeks, with retrain moments marked.

- **Title (bold 15px, `#1a5276`, top center):** "Average Daily Miss After Launch: Retrained vs Never Retrained".
- **Axes:** origin x=70, baseline y=245, plot width 570, plot height 180; x = weeks 1–10, 12px `#444` tick labels "wk 1" ... "wk 10"; y = croissants missed per day, 0 to 25, light `#e5e9ef` gridlines at 5, 10, 15, 20 with 12px `#444` labels.
- **Never retrained:** red `#e74c3c` 3px line with 4px dots through weeks 1–10 at values `[4, 5, 6, 8, 10, 12, 15, 17, 20, 23]`; bold 12px red label "never retrained" near week 9 above the line.
- **Retrained every 3 weeks:** green `#008300` 3px line with 4px dots at values `[4, 5, 6, 4, 5, 6, 4, 5, 6, 4]`; bold 12px green label "retrained every 3 weeks" near week 6 below its line.
- **Retrain markers:** vertical dashed `#6b7280` (dash 4/3) lines at weeks 4, 7, 10 from baseline to y=90, 11px `#6b7280` label "retrain" above each.
- **Annotation (bold 12px red `#e74c3c`, near week 4, y=105):** "world drifts — new office opens wk 4".
- **Caption (12px `#444`, bottom right):** "illustrative — miss = |baked − actually sold|, daily average".

## "But It Scored Great Offline"

**Tags:** `common mistake` (red), `offline vs live` (orange)

- **The mistake** — treating the offline test score as the finish line, when it is only the dress rehearsal
- **Offline** — on held-out history the bakery model missed by just 4 croissants a day on average
- **Live** — in its first real week the same model missed by 12 a day, three times worse
- **Why the gap** — skewed 6am features, late-arriving rows, and a world already drifted past the training logs
- **The fix** — check the live feature values against training's, and judge the model on its serving miss

*Example (italic):* The team celebrated "average miss: 4" from the notebook, then spent launch week explaining daily misses of 12.

**Common mistake:** Believing the offline test number is the model's real performance. The offline score assumes serving sees the same features training saw — the one assumption production loves to break.

### Visualization (canvas `c4`, 720×300)

Two-bar comparison chart: average daily miss measured offline on held-out history versus measured live in the first serving week, with the gap called out.

- **Title (bold 15px, `#1a5276`, top center):** "The Rehearsal and the Opening Night".
- **Axes:** origin x=90, baseline y=245, plot width 520, plot height 175; y = average daily miss (croissants), 0 to 15, light `#e5e9ef` gridlines at 5 and 10 with 12px `#444` labels; no x axis line beyond the baseline.
- **Bar 1 (width 130, centered at x=250):** blue `#2a78d6`, fill `rgba(42,120,214,0.35)` with 2px blue border, height for value `4`; bold 14px blue value label "4" above; 13px `#2c3e50` label below baseline: "offline test (held-out history)".
- **Bar 2 (width 130, centered at x=470):** orange `#d95926`, fill `rgba(217,89,38,0.35)` with 2px orange border, height for value `12`; bold 14px orange value label "12" above; 13px `#2c3e50` label below baseline: "first live week (serving)".
- **Gap arrow:** vertical 2px `#6b7280` double-headed arrow between the two bar-top heights at x=590, 12px `#6b7280` two-line label to its right: "3× worse" / "on real mornings".
- **Annotation (bold 13px red `#e74c3c`, centered near x=360, y=75):** "a good offline score is a promise, not a result".
- **Caption (12px `#444`, bottom right):** "illustrative — same model, two measurements".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all bar values, line points, and box positions are the hardcoded literals above (no randomness); the worked-example numbers (120/108 feature, 116/106 prediction, offline 4 vs live 12) must match between text and charts.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
