# Feature Stores

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Feature Stores

**Subtitle:** A feature store computes each model input once, from one shared recipe, and hands the exact same number to both the training table and the live app

## One Average, Two Kitchens

**Tags:** `core idea` (blue), `one recipe` (green), `two worlds` (orange)

- **The app** — a food-delivery app wants to predict, at checkout, whether an order will arrive late
- **The feature** — one key input: this driver's average delivery time over the last 7 days
- **Two kitchens** — the training team computes it from logs; the app team re-writes it in app code
- **The drift** — two hand-written copies of "average delivery time" slowly stop agreeing
- **The store** — a feature store keeps ONE recipe, runs it once, and serves the result to both sides

*Example (italic):* Training reads the driver's 7-day average from history, the app needs it in milliseconds at checkout — a feature store gives both the same 32.0 minutes.

**Key point:** A feature store is one shared recipe per feature, computed once, so training and the live app never disagree about the same number.

### Visualization (canvas `c1`, 720×300)

Left-to-right flow diagram: raw delivery logs feed one feature-store box holding the single recipe, which fans out to a training table (batch history) and the live app (this order) — both receiving the identical value 32.0.

- **Title (bold 15px, `#1a5276`, top center):** "One Recipe, Computed Once, Served to Both Worlds".
- **Left box (x=30–190, y=115–170):** fill `rgba(107,114,128,0.10)`, 2px `#6b7280` border, 4px radius; bold 13px `#2c3e50` centered text "raw delivery logs" with 12px `#6b7280` line "every trip, every day" below it.
- **Center box (x=255–465, y=95–190):** fill `rgba(42,120,214,0.10)`, 3px `#1a5276` border, 6px radius; bold 14px `#1a5276` centered heading "FEATURE STORE" with 12px `#2c3e50` lines "one recipe:" / "mean of last 7 days" below.
- **Top-right box (x=535–700, y=50–105):** fill `rgba(42,120,214,0.12)`, 2px `#2a78d6` border; bold 13px `#2a78d6` text "training table" with 12px `#444` line "months of history" and bold 13px `#2a78d6` value "32.0 min".
- **Bottom-right box (x=535–700, y=180–235):** fill `rgba(0,131,0,0.10)`, 2px `#008300` border; bold 13px `#008300` text "live app" with 12px `#444` line "this order, right now" and bold 13px `#008300` value "32.0 min".
- **Arrows:** 3px `#6b7280` arrow from x=190 to x=255 at y=142; 3px `#2a78d6` arrow from (465, 120) to (535, 78); 3px `#008300` arrow from (465, 165) to (535, 207); each with a small filled arrowhead.
- **Annotation (bold 13px orange `#d95926`, centered near x=360, y=265):** "computed once — both worlds read 32.0".
- **Caption (11px `#444`, bottom right):** "illustrative — one driver's feature flowing to training and serving".

## Averaging the Driver's Week by Hand

**Tags:** `worked example` (blue), `training-serving skew` (red)

- **The week** — the driver's last 7 daily averages, in minutes: 28, 34, 31, 40, 26, 33, 32
- **The recipe** — the feature store's definition: mean of all 7 days = 224 / 7 = 32.0 minutes
- **The app's copy** — the app team's re-write quietly used only the last 5 days: 31, 40, 26, 33, 32
- **The skew** — the app's version gives 162 / 5 = 32.4 minutes, while training saw 32.0
- **The bite** — the model was trained on 32.0-style inputs but is fed 32.4-style ones at checkout

*Example (italic):* Same driver, same week, two recipes: 224/7 = 32.0 for training, 162/5 = 32.4 in the app — the model meets numbers it was never trained on.

**Key point:** Training-serving skew is two hand-written copies of one recipe drifting apart — 32.0 vs 32.4 here — and the feature store removes it by having only one copy.

### Visualization (canvas `c2`, 720×300)

Bar chart of the driver's 7 daily delivery times with two horizontal mean lines: the feature store's 7-day mean (solid blue) and the app's accidental 5-day mean (dashed orange), the last 5 bars lightly highlighted.

- **Title (bold 15px, `#1a5276`, top center):** "Same Week of Data, Two Recipes, Two Answers".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 185; y = minutes 0 to 50 with light `#e5e9ef` gridlines at 10, 20, 30, 40 and 12px `#444` labels; x = 7 bars labeled "Mon"–"Sun" (12px `#444`).
- **Bars:** values `[28, 34, 31, 40, 26, 33, 32]`, width 52px, gap 32px; first two bars fill `rgba(42,120,214,0.20)`, last five bars fill `rgba(42,120,214,0.40)` with a 2px `#d95926` top edge marking "the days the app's code used"; 12px `#444` value label above each bar.
- **Store mean line:** solid blue `#2a78d6` 3px horizontal line at 32.0 across the plot; bold 13px blue label at its left end: "feature store: 224/7 = 32.0".
- **App mean line:** dashed orange `#d95926` (dash 6/4) 3px horizontal line at 32.4; bold 13px orange label right-aligned at the plot's right edge, above the bar labels (baseline y=100): "app's copy: 162/5 = 32.4".
- **Zoom inset (top left, box x=88, y=34, 180×64, white fill, 1px `#6b7280` stroke):** 11px `#6b7280` caption "zoom: 0.4 min apart"; inside, a dashed orange 2px line labeled "32.4" above a solid blue 2px line labeled "32.0" (11px labels at the line ends), showing the two means separated.
- **Annotation (bold 13px `#d95926`, near x=450, y=70):** "two hand-written recipes drifted: 32.0 vs 32.4 min".
- **Caption (11px `#444`, bottom right):** "illustrative — one driver's daily average delivery times".

## Why the Live Model Quietly Got Worse

**Tags:** `where it's used` (blue), `silent failure` (red), `reuse` (green)

- **Offline it shone** — on held-out history the late-order model scored 84% accuracy
- **Live it sagged** — in the app it managed only 71%, with no error, no crash, no alert
- **The culprit** — every input was a slightly different re-implementation of the training recipe
- **The fix** — routing both worlds through the feature store brought the live score back to 83%
- **The bonus** — the ETA team reuses "driver 7-day average" from the store instead of re-writing it

*Example (italic):* The model never broke — it scored 84% offline, 71% live, and 83% once training and serving read the same stored features.

**Key point:** Skew fails silently: nothing crashes, the live model just underperforms its offline promise until both worlds read the same computed features.

### Visualization (canvas `c3`, 720×300)

Three-bar chart of the late-order model's accuracy: offline test, live app before the feature store, and live app after — showing the drop was skew, not the model.

- **Title (bold 15px, `#1a5276`, top center):** "Same Model, Three Scores — the Gap Was the Features".
- **Axes:** origin x=70, baseline y=245, plot width 580, plot height 180; y = accuracy 0% to 100% with light `#e5e9ef` gridlines at 25, 50, 75 and 12px `#444` labels "0%", "25%", "50%", "75%", "100%".
- **Bars (width 120px, evenly spaced):** "offline test" 84% fill `rgba(42,120,214,0.40)` with 2px `#2a78d6` border; "live, own recipes" 71% fill `rgba(217,89,38,0.35)` with 2px `#d95926` border; "live, feature store" 83% fill `rgba(0,131,0,0.30)` with 2px `#008300` border; bold 14px matching-color value labels "84%", "71%", "83%" above the bars; 12px `#444` category labels below the baseline.
- **Guide line:** dashed `#6b7280` (dash 4/3) horizontal line at 84% across the plot, 11px `#6b7280` label "offline promise" at its right end.
- **Annotation (bold 13px `#d95926`, near x=330, y=85):** "the 13-point drop was skew, not the model".
- **Caption (11px `#444`, bottom right):** "illustrative accuracy numbers".

## It Is Not Just a Database

**Tags:** `common mistake` (red), `offline vs online` (orange)

- **The mistake** — "why not just query the warehouse at checkout?" — because checkout has ~50 ms
- **Two doors** — one store, two access paths: a slow offline door and a millisecond online door
- **Offline door** — months of history for building training tables, where a 900 ms query is fine
- **Online door** — the latest value per driver, pre-computed, answering a lookup in about 5 ms
- **Same recipe** — both doors serve values produced by the one shared recipe; only the speed differs

*Example (italic):* The warehouse answers the 7-day average in about 900 ms — 18 times the checkout budget of 50 ms — while the online store returns the pre-computed 32.0 in about 5 ms.

**Common mistake:** Treating a feature store as just another database. The point is one recipe with two doors — slow-and-deep for training, instant for serving — not a new place to dump tables.

### Visualization (canvas `c4`, 720×300)

Horizontal bar chart comparing lookup speed for the same feature — warehouse query vs online store lookup — against the app's latency budget line, showing why serving needs its own fast door.

- **Title (bold 15px, `#1a5276`, top center):** "Same Feature, Two Doors: 900 ms vs 5 ms".
- **Axis:** horizontal 2px `#999` baseline at y=235 from x=250 to x=680 (width 430), scale 0 to 1000 ms; 12px `#444` tick labels "0", "250", "500", "750", "1000 ms" every 250 ms.
- **Row 1 (bar center y=105, height 40px), 12px `#444` label at x=20:** "warehouse query (offline door)"; bar from 0 to 900 ms, fill `rgba(217,89,38,0.35)`, 2px `#d95926` border; bold 13px `#d95926` value label "900 ms" just past the bar end.
- **Row 2 (bar center y=180, height 40px), label:** "online store lookup (online door)"; bar from 0 to 5 ms (draw a minimum 4px sliver), fill `rgba(0,131,0,0.35)`, 2px `#008300` border; bold 13px `#008300` value label "5 ms" to its right.
- **Budget line:** vertical dashed `#6b7280` (dash 4/3) line at 50 ms from y=60 to the baseline; bold 12px `#6b7280` label "checkout budget: 50 ms" at its top.
- **Annotation (bold 13px violet `#4a3aa7`, near x=460, y=170):** two lines: "same recipe, same 32.0 —" / "only the door speed differs".
- **Caption (11px `#444`, bottom right):** "illustrative latencies".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all bar values, means, accuracies, and latencies are the hardcoded literals above (no randomness); 224/7 = 32.0 and 162/5 = 32.4 must match between text and charts; invented accuracy and latency figures keep their "illustrative" captions.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
