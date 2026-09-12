# Data Leakage

**Page type:** detail page (tutorial: 4 card-sections; section 2 uses a 3-column layout — text 38% + two canvases 31% each — the rest use the two-column 45/55 text/canvas layout)
**HTML title tag:** Data Leakage

**Subtitle:** A feature secretly contains the answer — the model looks brilliant offline because a piece of the future leaked into its inputs

## The Feature That Already Knew the Answer

**Tags:** `core idea` (blue), `running example` (green)

- **The task** — predict at the start of the month which customers will churn during it
- **The feature** — "number of support calls last month", computed over the whole month
- **The catch** — churning customers call support to cancel; that call lands in the window
- **The leak** — the feature counts a call that is caused by the churn it should predict
- **At prediction time** — those cancel calls have not happened yet, so the signal vanishes

*Example:* Predicting who will call an ambulance using "hospital visits this month" — the visit IS the outcome.

**Key point:** **Data leakage:** any information in the training inputs that would not exist at the moment of a real prediction — the future hiding inside a feature.

### Visualization (canvas `c1`, 720×300)

Timeline diagram of one customer: the feature window reaches past the prediction moment into the churn.

- **Title (bold 16px, `#1a5276`, top center):** "One Customer's Timeline: the Window Contains the Answer".
- **Timeline:** horizontal `#999` axis at y=150 from x=70 to x=660, mapping day −30 to day +30; gray `#6b7280` 12px tick labels "day −30", "day −15", "day +15", "day +30" below the axis; region captions "known past" (left) and "the month being predicted" (right) at y=70.
- **Prediction line:** vertical ink `#1a5276` line, width 2.5, at day 0 from y=55 to y=245, with bold 13px ink label "moment of prediction" above it.
- **Leaky feature window:** rectangle from day −2 to day +30 (y=92, height 40), fill `rgba(213,81,129,0.14)`, magenta `#d55181` 2px stroke, bold 12px magenta label above: "feature window: "support calls last month"".
- **Events on the timeline:** blue `#2a78d6` dot radius 6 at day −20 labeled "ordinary billing call" (12px); magenta dot radius 6 at day +18 labeled bold "call to CANCEL"; orange `#d95926` X mark (width 3, 7px arms) at day +24 labeled bold "churn" below.
- **Annotations:** magenta bold 13px, centered 30px from bottom: "the window crosses the line — it counts a call caused by the churn itself"; gray 12px, centered 10px from bottom: "a real prediction on day 0 cannot see anything to the right of the line".

## Worked Numbers: 95% Offline Melts to 63% Live

**Tags:** `worked example` (green), `arithmetic` (blue)

- **The data** — 200 customers, 50 churned, 150 stayed
- **Leaky window** — churners average 4.6 calls, stayers 1.2: a giveaway gap
- **Honest window** — calls before the cutoff: churners 1.7, stayers 1.3: barely different
- **Leaky model** — offline 190 / 200 = 95%; live next month 126 / 200 = 63%
- **Honest model** — offline 164 / 200 = 82%; live 162 / 200 = 81%, as promised

*Example:* The 4.6-vs-1.2 gap was mostly cancellation calls — remove them and the "signal" collapses to 1.7 vs 1.3.

**Key point:** **Hand-checkable:** the leaky model's 95% was real on paper and worthless in production — the honest 82% was the true ceiling all along.

### Visualization (canvas `c2a`, 420×300)

Grouped bar chart: average support calls per customer, leaky window vs honest window.

- **Title (bold 15px, `#1a5276`, top center):** "Average Support Calls per Customer".
- **Groups:** "leaky window" — churned 4.6, stayed 1.2; "honest window" — churned 1.7, stayed 1.3. Churned bars magenta `#d55181`, stayed bars aqua `#199e70`; fill alpha 0.5, 2px stroke; bar width 52, 8px between the pair; group x positions 95 and 255.
- **Axes:** y from 0 to 5 calls, integer labels (gray `#6b7280` 12px); baseline y=226, chart height 150, left pad 52; L-shaped `#999` axis.
- **Value labels (bold 12px in bar color above bars):** "4.6", "1.2", "1.7", "1.3"; group labels 12px `#2c3e50` below baseline.
- **Legend (x=250, upper right):** magenta swatch "churned (50)"; aqua swatch "stayed (150)" (12px).
- **Annotation (magenta bold 12px, centered below):** "the giveaway gap is mostly cancel calls"; below it gray 11px "illustrative numbers".

### Visualization (canvas `c2b`, 420×300)

Grouped bar chart: offline vs live accuracy for the leaky and honest models.

- **Title (bold 15px, `#1a5276`, top center):** "Offline Score vs Live Score".
- **Groups:** "leaky model" — offline 95%, live 63%; "honest model" — offline 82%, live 81%. Offline bars blue `#2a78d6`, live bars violet `#4a3aa7`; fill alpha 0.5, 2px stroke; bar width 52, 8px between the pair; group x positions 95 and 255.
- **Axes:** y from 0 to 100%, labels every 25% (gray 12px); baseline y=226, chart height 150, left pad 52; L-shaped `#999` axis.
- **Value labels (bold 12px in bar color above bars):** "95%", "63%", "82%", "81%"; group labels 12px `#2c3e50` below baseline.
- **Legend (x=260, upper right):** blue swatch "offline"; violet swatch "live, next month" (12px).
- **Annotation (magenta `#d55181` bold 12px, centered below):** "95% melts to 63%; the honest 82% holds"; below it gray 11px "illustrative numbers".

## Why "Too Good to Be True" Is a Diagnosis

**Tags:** `where it's used` (blue), `smell test` (orange)

- **Know the baseline** — guessing "nobody churns" is already 150 / 200 = 75% right
- **Honest gains are modest** — a good churn model here reaches about 82%
- **A sudden 95% is a smell** — a jump far past everything tried before means check for leaks first
- **Leaks are everywhere** — future-window features, the target in disguise, test rows seen in training
- **The bill arrives late** — the failure appears only after deployment, in front of users

*Example:* The first reaction to 95% should be an investigation, not a celebration.

**Key point:** **Why it matters:** leakage passes every offline check — the score is genuinely high on the data you have. Only the deployed model, or a leak hunt, reveals it.

### Visualization (canvas `c3`, 720×300)

Four-bar smell-test chart placing the 95% among the baseline, honest model, and live result.

- **Title (bold 16px, `#1a5276`, top center):** "Where 95% Sits Among Everything Else".
- **Bars:** labels `guess "nobody churns"`, "honest model", "leaky model, offline", "leaky model, live"; values `[75, 82, 95, 63]` percent; colors mute `#6b7280`, green `#008300`, magenta `#d55181`, orange `#d95926`; fill alpha 0.5, 2px stroke; bar width 118, gap 36, first bar x=100.
- **Axes:** y from 0 to 100%, labels every 25% (gray 12px); baseline y=230, chart height 160, left pad 70; L-shaped `#999` axis.
- **Value labels:** bold 14px in bar color above each bar; category labels 12px `#2c3e50` below baseline.
- **Annotations:** magenta bold 13px two lines above the 95% bar: "13 points past the honest model:" / "a smell, not a win"; orange bold 13px above the live bar: "worse than guessing".
- **Caption (gray 12px, centered 44px below baseline):** "accuracy on 200 customers, 50 of whom churn (illustrative)".

## The One Question That Catches It

**Tags:** `common mistake` (red), `rule of thumb` (green)

- **The question** — "could I have computed this feature at the moment of prediction?"
- **Draw the cutoff** — a vertical line at prediction time; every feature must stop before it
- **Safe** — plan price, account age, calls in the 30 days before the cutoff
- **Leaking** — calls during the target month, anything dated after the cutoff
- **Check the winners** — when one feature dominates the model, audit its time window first

*Example:* "Days until cancellation" once topped a churn model's feature list — it is the answer wearing a costume.

**Key point:** **The confusion:** leakage is not cheating on purpose — it is usually one innocent-looking date filter, applied to the wrong side of the cutoff line.

### Visualization (canvas `c4`, 720×300)

Feature-window audit: horizontal time bars for five features against a vertical cutoff line, each marked safe or leaking.

- **Title (bold 16px, `#1a5276`, top center):** "Audit Every Feature Against the Cutoff Line".
- **Cutoff line:** vertical ink `#1a5276` line, width 2.5, at x=500 from y=52 to y=248, labeled bold 12px "moment of prediction" above; gray 12px labels "past" (left of line) and "future" (right of line) at the bottom.
- **Feature rows** (bars 17px tall, rows starting y=78, spaced 32px; name right-aligned at x=238 in 12px `#2c3e50`; bar fill alpha 0.35 with 2px stroke; verdict bold 13px at x=648):
  - "plan price" — bar x 260–470, green `#008300`, "✓ safe".
  - "account age" — bar x 270–470, green, "✓ safe".
  - "calls, 30 days before cutoff" — bar x 340–490, green, "✓ safe".
  - "calls during the target month" — bar x 440–630 (crosses the line), magenta `#d55181`, "✗ leaks".
  - ""days until cancellation"" — bar x 520–630 (entirely in the future), magenta, "✗ leaks".
- **Annotation (magenta bold 13px, bottom center):** "if a feature's window reaches past the line, it is leaking the answer".

## Regeneration instructions

- **Template:** tutorials topic page (per `tutorials/CLAUDE.md`). `<h1>` (no index number), `.subtitle` line, then 4 `.card-section` blocks, each an `<h2>` with 2px `#2980b9` bottom border plus a `table.layout` row. Section 2 uses the 3-column variant: `td.text-col3` (38%) + two `td.viz-col3` (31% each) holding canvases `c2a` and `c2b` at 420×300. Sections 1, 3, 4 use `td.text-col` (50%) + `td.viz-col` (50%) with one 720×300 canvas. Text cells hold `.tags` pills, 5 one-line bullets (each opening with `<b>` in `#1a5276`), an italic `.example` line, and a `.key-point` callout.
- **Page CSS:** body system-ui sans, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with 2px `#2980b9` bottom border; subtitle `#666` 0.95rem; h2 1.3rem `#1a5276`. `.key-point`: background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem. `.example`: italic `#555` 0.9rem. ul 0.92rem. Canvas: `width:100%`, 1px `#e0e0e0` border, 4px radius.
- **Tag pills:** 0.72rem bold, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Chart palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette anchors: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- **Canvas scaling:** shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates; intrinsic width/height attributes as given per chart. All data arrays hardcoded (no `Math.random()`). Curly typographic quotes are used inside chart label strings (e.g. "support calls last month", "days until cancellation"). Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- No nav bar, no back/home links, no cross-page links. In regenerated HTML, any card links would use `.html` extensions.
