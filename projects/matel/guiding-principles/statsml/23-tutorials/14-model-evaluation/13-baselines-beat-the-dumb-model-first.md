# Baselines: Beat the Dumb Model First

**Page type:** detail page (tutorial layout: `.card-section` blocks, each with a two-column `table.layout` — text left 50%, canvas right 50%)
**HTML title tag:** Baselines: Beat the Dumb Model First

**Subtitle:** Before celebrating any model's score, check what a model with zero intelligence would score — that free score is your starting line, not zero

## A Churn Model That Barely Beats Doing Nothing

**Tags:** `core idea` (blue), `running example` (green)

- **The model** — a churn predictor scores 82% accuracy; the team is ready to celebrate
- **The dumb rival** — a "model" that predicts *nobody ever churns*, no data, no training
- **Its score** — 80% of customers do stay, so saying "stays" every time is 80% accurate
- **The real gap** — months of work bought 2 points over a one-line if-statement
- **The lesson** — a score means nothing until you know what "no model at all" scores

*Example:* A weather app that always says "no rain tomorrow" is right about 80% of days in a dry city.

**Key point (labeled "Baseline:"):** the score of the simplest possible strategy. Your model's real achievement is the distance above it — here 2 points, not 82.

### Visualization (canvas `c1`, 720×300)

Two-bar accuracy comparison with a gap bracket highlighting the 2-point lift.

- **Title (bold 16px, `#1a5276`, top center):** "82% Sounded Great — Until the Dumb Model Answered"
- **Padding:** top 55, bottom 50, left 60, right 30. Gray `#999` L-frame axes; y 0–100% with gridlines (`#e5e9ef`) and 12px `#6b7280` tick labels every 25%.
- **Bars (150px wide, 130px gap):** "churn model (months of work)" = 82% in blue `#2a78d6`; "\"nobody churns\" (one line)" = 80% in yellow `#c98500`. Bold 14px `#2c3e50` value labels ("82%", "80%") above bars; 12px labels below.
- **Gap bracket:** red `#e74c3c` vertical bracket (width 2, with 6px end ticks) between the 82% and 80% heights, centered between the bars; bold 13px red annotation above: "the real achievement: +2 points" / "not 82".

## Counting the 1,000 Customers by Hand

**Tags:** `worked example` (green), `arithmetic` (blue)

- **The data** — 1,000 customers: 800 stay, 200 churn
- **Baseline** — "nobody churns" gets all 800 stayers right, all 200 churners wrong: 800/1000 = 80%
- **The model** — right on 790 stayers and 30 churners: 820/1000 = 82%
- **Look closer** — the model finds only 30 of the 200 churners it was built to find
- **Same headline** — 82% sounds strong; "catches 15% of churn" sounds like what it is

*Example:* Redo it yourself: 790 + 30 = 820 correct out of 1,000 — the whole check is one addition.

**Key point (labeled "Hand-checkable:"):** most of the model's 82% is inherited from the easy majority — the baseline earns those 800 for free.

### Visualization (canvas `c2`, 720×300)

Stacked bar chart of correct predictions out of 1,000, with a callout on the churners-caught segment.

- **Title (bold 16px, `#1a5276`):** "Correct Predictions out of 1,000 Customers"
- **Padding:** top 55, bottom 50, left 60, right 210. Gray `#999` L-frame axes; y 0–1000 with gridlines (`#e5e9ef`) and ticks every 250.
- **Bars (130px wide, 110px gap), stacked stayers (blue `#2a78d6`) + churners (green `#008300`):**
  - "\"nobody churns\"": 800 stayers, 0 churners; total label "800  (80%)".
  - "churn model": 790 stayers, 30 churners; total label "820  (82%)".
  - White bold 13px in-bar labels "800 stayers" / "790 stayers"; bold 14px `#2c3e50` totals above; 12px labels below.
- **Callout:** green connector line from the model bar's churn segment to bold 13px green text "30 churners caught", with the second line in red `#e74c3c`: "out of 200 — just 15%".
- **Legend (bottom right, x = w−190, y=210):** blue swatch "stayers correct"; green swatch "churners correct".

## The Baseline Menu: Majority, Last Value, Mean

**Tags:** `where it's used` (blue), `rule of thumb` (green)

- **Majority class** — for yes/no problems, always predict the common answer (our 80%)
- **Last value** — for forecasts, predict tomorrow = today; shockingly hard to beat
- **Mean** — for predicting numbers, always guess the average of what you've seen
- **Pick by task** — every prediction task has a matching dumb strategy; find yours first
- **Free lunch** — each takes one line of code and zero training time

*Example:* Forecasting store sales? "Same as yesterday" often beats a model that took a month to build.

**Key point (labeled "Rule of thumb:"):** compute the dumb baseline before training anything — it takes a minute and defines what "good" means for this problem.

### Visualization (canvas `c3`, 720×300)

Three mini-panels (one per baseline strategy) separated by vertical dashed dividers (`#bdc3c7`, dash 4/3) at x=245 and x=485, from y=40 to h−12.

- **Title (bold 16px, `#1a5276`, top center):** "Three Dumb Strategies, One per Task Type"
- **Panel 1 — Majority class (x=35, width 180, baseline y=205):** header bold 13px blue `#2a78d6` "Majority class", sub 12px gray `#6b7280` "yes/no problems". Two bars 62px wide: "stay" = 800 in blue `#2a78d6`, "churn" = 200 in magenta `#d55181` (scaled to 800 max, height 130). 12px value and category labels. Footer bold 12px blue "always say \"stay\"", sub 12px gray "scores 80% free".
- **Panel 2 — Last value (x=265, width 200, baseline y=205, height 120):** header bold 13px aqua `#199e70` "Last value", sub "forecasting problems". Actual sales line (aqua, width 2.5, 3.5px dots): `[42, 55, 48, 60, 52, 66, 58]` over 7 days (y range 30–70); prediction = same series shifted one day, drawn dashed orange `#d95926` (dash 5/4, width 2) for days 2–7. Footer bold 12px orange "tomorrow = today", sub 12px gray "actual (solid) vs copy (dashed)".
- **Panel 3 — Mean (x=505, width 190, baseline y=205, height 120):** header bold 13px violet `#4a3aa7` "Mean", sub "predict-a-number problems". Violet scatter dots (radius 4) at fractional-x/value pairs: `[0.08,44], [0.2,58], [0.32,40], [0.45,65], [0.55,50], [0.68,61], [0.8,46], [0.92,56]` (y range 30–70); dashed orange horizontal line at the mean 52.5. Footer bold 12px orange "always guess 52.5", sub 12px gray "(the average)".
- **Takeaway (bold 13px red `#e74c3c`, bottom center):** "each is one line of code — your model must beat its matching one"

## Lift Over Baseline Is the Real Score

**Tags:** `common mistake` (orange), `why it matters` (red)

- **The trap** — comparing raw accuracies across problems: 82% is not better than 75%
- **Project A** — 82% accuracy, but the baseline is 80%: lift = 2 points
- **Project B** — 75% accuracy on a 50/50 problem, baseline 50%: lift = 25 points
- **The verdict** — B learned far more from the data, despite the lower headline number
- **Report both** — always state the baseline next to the model score, never the score alone

*Example:* "82% accurate" impressed the room until someone asked what guessing "stays" scores.

**Key point (labeled "Common confusion:"):** high accuracy = good model. Accuracy is rented from the problem; lift over the dumb baseline is what the model actually earned.

### Visualization (canvas `c4`, 720×300)

Grouped baseline-vs-model bars for two projects, with lift brackets.

- **Title (bold 16px, `#1a5276`, top center):** "Lower Accuracy, Better Model: Lift Tells the Truth"
- **Padding:** top 55, bottom 60, left 60, right 30. Gray `#999` L-frame axes; y 0–100% with gridlines (`#e5e9ef`) and ticks every 25%.
- **Groups (bars 90px wide, baseline bar then model bar 20px apart; group width 220, 150px between groups):**
  - "Project A (churn)": baseline 80% in yellow `#c98500`, model 82% in blue `#2a78d6`, lift "+2".
  - "Project B (50/50 problem)": baseline 50% in yellow `#c98500`, model 75% in green `#008300`, lift "+25".
  - Bold 13px value labels above bars; dashed red `#e74c3c` horizontal line (dash 4/3, width 2) at each baseline height across the group; bold 14px red "lift +2" / "lift +25" above each model bar; 12px gray `#6b7280` "baseline" / "model" labels under bars; bold 12px `#2c3e50` group names below.
- **Annotation (bold 13px red, centered at y=46):** "B earned 25 points above dumb; A earned 2 — B is the better model"

## Regeneration instructions

- **Layout:** tutorial detail page — h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks. Each section: `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one `<tr>`: left `td.text-col` (50%) holding `.tags` pill row, a `<ul>` of bold-term bullets (`li b` in `#1a5276`), an italic `.example` line, and a `.key-point` callout; right `td.viz-col` (50%) holding one 720×300 canvas.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; ul 0.92rem; `.example` italic `#555` 0.9rem; `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, 0.9rem. No nav bar, no back/home links.
- **Tag pills:** `.tag` inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px. Colors: blue `rgba(26,82,118,0.12)`/`#1a5276`; green `rgba(39,174,96,0.15)`/`#27ae60`; red `rgba(231,76,60,0.12)`/`#e74c3c`; orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Canvas:** intrinsic 720×300, CSS `width:100%`, border `1px solid #e0e0e0` radius 4px; scaled via `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) in a shared `setup(id)` helper (this page's helper uses a fixed 720×300 rect rather than reading canvas attributes). Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Doc palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- **Shared data:** 1,000 customers — 800 stay, 200 churn; baseline ("nobody churns") = 80%; churn model = 790 stayers + 30 churners correct = 82%; Project B: 75% accuracy vs 50% baseline.
- This page has no card links; in regenerated HTML any links would use `.html` extensions.
