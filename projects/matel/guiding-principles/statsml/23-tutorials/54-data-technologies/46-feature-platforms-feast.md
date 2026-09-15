# Feature Platforms (Feast)

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Feature Platforms (Feast)

**Subtitle:** A feature store defines a feature once and serves the same value to model training and to the live app — ending the silent drift between the two code paths

## The Order Count That Meant Two Things

**Tags:** `core idea` (blue), `one definition` (green), `Feast` (orange)

- **The feature** — "user's 30-day order count" feeds a churn model at a food-delivery app
- **The training side** — a nightly SQL job counts orders over 30 calendar days, cancelled orders kept
- **The serving side** — the app's own code counts a rolling 720 hours and drops cancelled orders
- **The drift** — for user 4217 training sees 12, serving sees 9 — same name, two definitions
- **The fix** — a feature platform (Feast) holds ONE definition and feeds both sides from it

*Example (italic):* User 4217 counts 12 orders under the SQL job's rules but 9 under the app's — the churn model was trained on numbers it never sees in production.

**Key point:** Training/serving skew is a feature computed one way in the offline training pipeline and a subtly different way in the online serving path; a feature platform removes it by making the definition single-sourced.

### Visualization (canvas `c1`, 720×300)

Two-row flow diagram: the same user flowing through the training pipeline and the serving pipeline, ending at two different feature values.

- **Title (bold 15px, `#1a5276`, top center):** "One Feature Name, Two Pipelines, Two Answers".
- **Row 1 (y=95), label 12px `#444` at x=20:** "training (nightly SQL)"; blue `#2a78d6` rounded box at x=175 labeled "orders table" (12px), 3px arrow to a blue box at x=345 labeled "30 calendar days, cancelled kept", 3px arrow to a red `#e74c3c` box at x=575 labeled "count = 12" (bold 13px).
- **Row 2 (y=205), label:** "serving (app code)"; blue box at x=175 labeled "orders API", 3px arrow to a blue box at x=345 labeled "720 hours, cancelled dropped", 3px arrow to a red box at x=575 labeled "count = 9" (bold 13px).
- **Box style:** 130–190px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` for blue and `rgba(231,76,60,0.12)` for red, 12px `#2c3e50` text.
- **Annotation (bold 13px red `#e74c3c`, centered near y=270):** "user 4217: trained on 12, served 9 — the model never notices".
- **Caption (12px `#444`, bottom right):** "counts illustrative".

## Counting User 4217 Both Ways

**Tags:** `worked example` (blue), `hand-check` (green)

- **The window** — SQL uses 30 calendar days in UTC; the app uses a rolling 720 hours in local time
- **The filter** — SQL keeps cancelled orders; the app silently drops them
- **Hand-check** — offline 12, minus 2 cancelled, minus 1 order outside the 720-hour window = 9 (exact)
- **Across users** — of five sampled users, three get different answers offline vs online
- **The symptom** — backtests look clean because both sides of the backtest use the SQL definition

*Example (italic):* 12 − 2 cancelled − 1 boundary-day order = 9; the 3-order gap is pure definition drift, not bad data — and nothing in either pipeline flags it.

**Key point:** Skew hides because each pipeline is internally consistent; only comparing the two paths for the same user at the same moment exposes the gap.

### Visualization (canvas `c2`, 720×300)

Grouped bar chart: five users' 30-day order counts, offline (training) value vs online (serving) value side by side.

- **Title (bold 15px, `#1a5276`, top center):** "Same Feature, Five Users: Offline vs Online Values".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = orders 0 to 24, gridlines `#e5e9ef` at 6/12/18 with 12px `#444` tick labels; x = five user groups centered at x = 120, 240, 360, 480, 600 with 12px `#444` labels "4217", "5810", "6642", "7003", "8125".
- **Bars:** per group, offline bar (blue `#2a78d6`, fill `rgba(42,120,214,0.30)`, 2px solid edge) left and online bar (orange `#d95926`, fill `rgba(217,89,38,0.30)`, 2px solid edge) right; each 28px wide, 6px gap between the pair.
- **Data (hardcoded):** offline `[12, 5, 8, 20, 3]`, online `[9, 5, 6, 17, 3]` — users 4217, 6642, 7003 mismatch; 5810 and 8125 match.
- **Value labels:** bold 12px in bar color, centered above each bar top.
- **Mismatch markers:** bold 13px red `#e74c3c` "≠" centered above the mismatched pairs (groups 1, 3, 4).
- **Legend (12px, top right inside plot):** blue swatch "offline (SQL job)", orange swatch "online (app code)".
- **Annotation (bold 13px red `#e74c3c`, near x=300, y=70):** "3 of 5 users disagree".
- **Caption (12px `#444`, bottom right):** "user counts illustrative; 12 − 2 − 1 = 9 exact".

## One Definition, Two Stores

**Tags:** `where it's used` (blue), `offline + online` (green), `point-in-time` (orange)

- **The registry** — Feast declares the feature once in code: source table, 30-day window, drop cancelled
- **The offline store** — materializes point-in-time-correct feature rows in the warehouse for training
- **The online store** — keeps a key-value copy (Redis-style) for millisecond lookups at serving time
- **The lineage** — Uber's Michelangelo popularized the pattern; Feast open-sourced it; Tecton sells a managed one
- **The payoff** — training and serving now read the same 9 for user 4217, by construction

*Example (italic):* After moving the order-count feature into Feast, the churn model's training rows and its live inputs match value for value — the skew class of bug is gone.

**Key point:** A feature platform is one definition plus two stores — an offline store for historical training data and an online store for low-latency serving — both fed by the same computation.

### Visualization (canvas `c3`, 720×300)

Fan-out flow diagram: a single feature definition feeding an offline store (to training) and an online store (to the live model), both ending at the same value.

- **Title (bold 15px, `#1a5276`, top center):** "Feast: Define Once, Serve Twice".
- **Source box:** violet `#4a3aa7` rounded box at x=40, y=130, 170px wide, 56px tall, fill `rgba(74,58,167,0.12)`, two 12px lines "feature definition" / "30d orders, no cancelled".
- **Fan-out arrows:** 3px `#6b7280` arrows from the source box to two store boxes.
- **Offline branch (y=80):** green `#008300` box at x=300 labeled "offline store (warehouse)", 3px arrow to a blue `#2a78d6` box at x=545 labeled "training job"; 12px green label above the branch: "point-in-time rows".
- **Online branch (y=190):** green box at x=300 labeled "online store (key-value)", 3px arrow to a blue box at x=545 labeled "live model"; 12px green label below the branch: "~5 ms lookup (illustrative)".
- **Box style:** 150–170px wide, 40px tall (source 56px), 8px radius, fills `rgba(0,131,0,0.12)` green and `rgba(42,120,214,0.15)` blue, 12px `#2c3e50` text.
- **Value tags:** bold 12px green `#008300` "user 4217 → 9" at the right edge of both branch endpoints (y≈70 and y≈240).
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=275):** "both paths compute the number the same way — skew is impossible by design".

## The Join That Leaks the Future

**Tags:** `common mistake` (red), `label leakage` (orange)

- **The shortcut** — joining today's feature values onto last quarter's training labels
- **The leak** — a label observed on day 40 paired with today's feature value knows the future
- **The number** — user 4217's count was 4 on day 40 but is 12 today; the naive join trains on 12
- **The result** — offline accuracy looks brilliant, then collapses the day the model goes live
- **The guard** — Feast's point-in-time join picks the feature value as of each label's own timestamp

*Example (italic):* Trained on today's 12 against day 40's churn label, the model "predicts" the past using orders that had not happened yet — 8 of the 12 came after the label.

**Common mistake:** Joining the latest feature values onto historical training rows. Point-in-time joins exist precisely to pair each label with the value that was true at that moment — skipping them is label leakage, not a shortcut.

### Visualization (canvas `c4`, 720×300)

Step chart of user 4217's 30-day order count over 180 days, with the correct point-in-time join value marked at the label date and the leaking latest-value join marked at today.

- **Title (bold 15px, `#1a5276`, top center):** "Point-in-Time Join: Use the Value That Was True Then".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = days 0 to 180 with 12px `#444` tick labels every 45 days ("day 0" … "day 180"); y = order count 0 to 14, gridlines `#e5e9ef` at 4/8/12.
- **Step line:** blue `#2a78d6` 3px stepped line through days `[0, 20, 40, 70, 100, 130, 160, 180]`, values `[3, 4, 4, 6, 7, 9, 11, 12]`.
- **Label marker:** vertical dashed `#6b7280` (dash 4/3) line at day 40, 12px `#6b7280` label "label observed" at its top; green `#008300` filled 6px dot at (day 40, value 4) with bold 13px green label "point-in-time join → 4".
- **Leak marker:** red `#e74c3c` filled 6px dot at (day 180, value 12) with bold 13px red label "naive latest join → 12"; red dashed 2px horizontal line from (day 40, 12) to (day 180, 12) showing the future value dragged back to the label date.
- **Annotation (bold 12px orange `#d95926`, near x=day 95, y=75):** "8 of the 12 orders happened after the label".
- **Caption (12px `#444`, bottom right):** "order counts illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); user IDs, order counts, the five-user comparison, the step-line history, and the 5 ms lookup latency are invented and labeled illustrative; the hand-check arithmetic 12 − 2 − 1 = 9 is exact. Feature-store facts (offline/online stores, point-in-time joins, Feast/Michelangelo/Tecton lineage) are publicly documented.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
