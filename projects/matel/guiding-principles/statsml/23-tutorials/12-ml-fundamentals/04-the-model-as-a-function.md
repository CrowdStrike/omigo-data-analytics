# The Model as a Function

**Page type:** detail page (tutorial: 4 card-sections, each a two-column table.layout — text left 50%, canvas right 50%)
**HTML title tag:** The Model as a Function

**Subtitle:** A churn model is just f(age, tenure, usage) → risk score — training tunes the dials inside f, and making a prediction is nothing more than calling the function

## A Churn Model Is Just f(age, tenure, usage)

Tags: `core idea` (blue), `running example` (green)

- **The inputs** — three numbers per customer: age, years of tenure, hours of use per week
- **The output** — one number back: a churn risk score, typically landing between 0 and 100
- **The inside** — a formula with dials: a starting value plus one weight per input
- **Our dials** — risk = 40 + 0.6·age − 4·tenure − 3·usage
- **Nothing else** — no memory, no mood: same three inputs in, same score out, every time

*Example (italic):* Age 30, tenure 2 years, usage 5 hrs/wk: 40 + 18 − 8 − 15 = risk 35.

**Key point:** A model is a function — inputs go in, a score comes out, and all the "knowledge" lives in the dial settings.

### Visualization (canvas `c1`, 720×300)

Function-box diagram: three input chips feeding a dial box that emits one output chip.

- **Title (bold 15px, `#1a5276`, top center):** "Inputs In, Score Out — the Knowledge Is the Dials"
- **Input chips** (left, 150×34, blue `#2a78d6` border on `rgba(42,120,214,0.12)` fill, bold 12px blue text, at y=70/135/200): "age = 30", "tenure = 2 yrs", "usage = 5 hrs/wk"; blue arrow lines converge into the function box.
- **Function box** (250×180 at x=252, y=62; violet `#4a3aa7` border 2.5px on `rgba(74,58,167,0.06)`): bold 16px violet header "f( age, tenure, usage )"; four dial circles (19px radius, white fill, violet outline and needle) labeled with values "40" / "+0.6" / "−4" / "−3" (bold 12px violet) and names "base" / "age" / "tenure" / "usage" (11px `#555`). Below the box (12px `#555`): "risk = 40 + 0.6·age − 4·tenure − 3·usage".
- **Output:** green arrow (`#008300`, width 2.5) to a 120×60 green-bordered chip on `rgba(0,131,0,0.12)`: bold 14px "risk = 35" over 12px `#333` "40+18−8−15".
- **Bottom caption (bold 13px magenta `#d55181`, centered, y=282):** "change a dial and every score changes — the dials ARE the model"

## Three Customers Through the Function

Tags: `worked example` (green)

- **Customer A** — age 30, tenure 2, usage 5: 40 + 18 − 8 − 15 = 35
- **Customer B** — age 50, tenure 1, usage 1: 40 + 30 − 4 − 3 = 63
- **Customer C** — age 25, tenure 6, usage 8: 40 + 15 − 24 − 24 = 7
- **Read the dials** — each tenure year removes 4 points; each weekly hour removes 3
- **Sanity check** — the long-tenure heavy user (C) scores lowest, as it should

*Example (italic):* B is new and barely uses the product — the function flags exactly that customer.

**Key point:** Every score is reproducible by hand — the model is arithmetic, not judgment.

### Visualization (canvas `c2`, 720×300)

Three-bar chart of risk scores with the full arithmetic under each bar.

- **Title (bold 15px, `#1a5276`, top center):** "Same Formula, Three Customers"
- **Data:** customers A/B/C, scores `[35, 63, 7]`, bar colors blue `#2a78d6` / orange `#d95926` / green `#008300` (alpha 0.75), bars 110px wide.
- **Axes:** y 0–70 with gray tick labels at 0/35/70; L-shaped gray axis; padding top 50 / bottom 76 / left 65 / right 30.
- **Per-bar labels:** bold 14px colored "risk 35"/"risk 63"/"risk 7" above bars; below baseline three stacked lines — bold 12px "customer A/B/C"; 12px `#555` inputs "age 30 · tenure 2 · usage 5" / "age 50 · tenure 1 · usage 1" / "age 25 · tenure 6 · usage 8"; bold 12px colored arithmetic "40 + 18 − 8 − 15" / "40 + 30 − 4 − 3" / "40 + 15 − 24 − 24".
- **Annotation (bold 13px orange, centered, y=46):** "highest risk: B — new and barely using the product"

## Training Means Turning the Dials

Tags: `core idea` (blue), `worked example` (green)

- **Start wrong** — set every dial to 0 and the scores miss the labeled outcomes badly
- **Measure the miss** — compare predicted risk to what labeled customers actually did
- **Nudge the dials** — shift each weight the way that shrinks the miss, then re-measure
- **Repeat** — here the average miss falls 32 → 5.4 points over 8 rounds (illustrative)
- **Converge** — the tenure dial settles near −4 and stops moving: training is done

*Example (italic):* Watch one dial: the tenure weight walks 0 → −1.5 → −2.6 → −3.3 → −3.7 → −4 and parks there.

**Key point:** Training never changes what the function IS — it only searches for the dial settings that best fit the labeled examples.

### Visualization (canvas `c3`, 720×300)

Two-panel line chart: falling error and converging weight over 8 training rounds.

- **Title (bold 15px, `#1a5276`, top center):** "Eight Rounds of Dial-Turning (illustrative)"
- **Divider:** vertical dashed gray line (`#bdc3c7`, dash 4/3) at x=360.
- **Left panel** (x=60, width 270, y range 60–245, scale max 35): average miss `[32, 21, 14, 9, 7, 6, 5.5, 5.4]` over rounds 1–8, orange `#d95926` line width 3 with 4px dots; panel header bold 13px orange "average miss: 32 → 5.4"; x-axis caption 12px `#444` "round of training".
- **Right panel** (x=410, width 260): tenure weight `[0, -1.5, -2.6, -3.3, -3.7, -3.9, -4.0, -4.0]` in violet `#4a3aa7`, line width 3 with dots; dashed muted-gray horizontal target line at −4 labeled "−4 (best fit)" (12px `#6b7280`); header bold 13px violet "tenure dial: 0 → −4, then parks"; same x-axis caption.
- **Bottom caption (bold 13px green `#008300`, centered):** "dials stop moving → training is done"

## Inference Is Just Calling the Function

Tags: `where it's used` (blue), `rule of thumb` (blue)

- **Train once** — hours of dial-turning on labeled history, done offline
- **Freeze the dials** — deploying a model means shipping the settings, not the training
- **Call it cheaply** — scoring a customer is one line of arithmetic, milliseconds each
- **No learning live** — the function does not update itself as it scores new customers
- **Retrain = new dials** — fresh labels produce new settings, released as a new version

*Example (italic):* The same frozen formula scored two million customers overnight without changing once.

**Key point:** Learning happens at training time; at prediction time the model is a plain, frozen function call.

### Visualization (canvas `c4`, 720×300)

Two-box pipeline: training box → frozen-dials arrow → inference box.

- **Title (bold 15px, `#1a5276`, top center):** "Train Once, Call Forever"
- **TRAINING box** (260×150 at x=45, y=60; violet `#4a3aa7` border 2.5px on `rgba(74,58,167,0.06)`): bold 14px header "TRAINING — once, offline"; 12px `#333` lines "1,000 labeled customers in", "dial search over many rounds", "takes hours"; bold 12px violet footer "output: the dial settings".
- **Arrow** (ink `#1a5276`, width 2.5) between boxes labeled bold 12px "freeze the dials:" (above) and "40, +0.6, −4, −3" (below).
- **INFERENCE box** (260×150 at x=415; green `#008300` border on `rgba(0,131,0,0.07)`): bold 14px header "INFERENCE — millions of calls"; lines "new customer in → score out", "one line of arithmetic", "milliseconds per call"; bold 12px green footer "no learning happens here".
- **Captions:** bold 13px orange `#d95926` at y=246: "deploying a model = shipping the dial settings, not the training"; 12px `#444` at y=272: "retraining on fresh labels produces new dials — released as a new model version".

## Regeneration instructions

- **Template:** tutorials topic-page skeleton (most-powerful-signals compact style). Each `.card-section` has an `<h2>` (1.3rem `#1a5276`, 2px bottom border `#2980b9`) and a `table.layout` with `.text-col` (50%) and `.viz-col` (50%).
- **Left column per section:** `.tags` pill row first (0.72rem bold, 10px radius pills — blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`), then a `<ul>` of one-line bullets each opening with `<b>` term in `#1a5276`, then an italic `.example` line (`#555`, 0.9rem), then a `.key-point` callout (background `#f8f9fa`, left border 3px `#e74c3c`, 0.9rem).
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px bottom border `#2980b9`; `.subtitle` `#666` 0.95rem. No nav bar, no back/home links.
- **Canvas:** all 720×300 intrinsic, CSS `width:100%`, 1px border `#e0e0e0` radius 4px; scaled via `window.devicePixelRatio` in a shared `setup(id)` helper reading width/height attributes (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange.
- **Data:** all values hardcoded literal arrays (scores 35/63/7, error and weight sequences); no `Math.random()`; invented sequences labeled "illustrative".
- In regenerated HTML, any card links use `.html` extensions.
