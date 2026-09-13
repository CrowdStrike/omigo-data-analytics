# Propensity Models

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Propensity Models

**Subtitle:** A propensity model scores each customer's chance of doing something — cancelling, clicking, buying — and the score is only safe to act on after it is calibrated against what really happened

## Which Members Will Cancel Next Month?

**Tags:** `core idea` (blue), `propensity score` (green), `churn / click / buy` (orange)

- **The gym** — a neighborhood gym with 1,000 members wants to know who will cancel next month
- **One number each** — a propensity model gives every member a score from 0% to 100%: chance of cancelling
- **Same trick everywhere** — swap "cancel" for "click the ad" or "buy the upgrade" and nothing else changes
- **Most are safe** — 400 members score under 20%; only 170 score above 60% and get a retention call
- **Score, then act** — the score itself does nothing; it decides who gets the call, email, or discount

*Example (italic):* The front desk cannot call 1,000 members, but it can call the 170 whose cancel score is above 60% — the model turns a crowd into a short list.

**Key point:** A propensity model attaches a chance-of-doing-it score to each customer so limited actions (calls, discounts, ad spend) go to the right people first.

### Visualization (canvas `c1`, 720×300)

Single-panel bar chart: how the 1,000 members' cancel scores are spread across five score buckets, with the actionable tail highlighted.

- **Title (bold 15px, `#1a5276`, top center):** "1,000 Members, One Cancel Score Each — illustrative".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 190; x = five score buckets with 12px `#444` labels "0–20%", "20–40%", "40–60%", "60–80%", "80–100%" centered under the bars; y = member count 0 to 400, light `#e5e9ef` gridlines at 100, 200, 300, 400 with 12px `#444` labels.
- **Bars:** counts `[400, 250, 180, 100, 70]`; first three bars fill `rgba(42,120,214,0.35)` with 2px `#2a78d6` border; last two bars fill `rgba(217,89,38,0.35)` with 2px `#d95926` border; bold 13px count label in the bar's border color above each bar ("400", "250", "180", "100", "70").
- **Annotation (bold 13px orange `#d95926`, near x=470, y=95):** two lines: "170 members score above 60% —" / "they get the retention call".
- **Caption (12px `#444`, bottom right):** "illustrative — scores from last month's model run".

## Scoring Three Members by Hand

**Tags:** `worked example` (blue), `score recipe` (green)

- **A tiny recipe** — start every member at 5%, then add points for warning signs; that is the whole model
- **The signs** — +20 if under 4 gym visits last month, +15 if month-to-month contract, +10 if joined under 3 months ago
- **Ben** — 12 visits, annual contract, member 24 months: no signs fire, score stays at 5%
- **Maya** — 2 visits, month-to-month, member 8 months: 5 + 20 + 15 = 40%
- **Ana** — 3 visits, month-to-month, joined 1 month ago: 5 + 20 + 15 + 10 = 50%
- **Real models** — logistic regression or boosted trees learn the points from history instead of guessing

*Example (italic):* Ana's 50% is just four additions — 5 base, 20 for low visits, 15 for month-to-month, 10 for being new — anyone can redo it on paper.

**Key point:** A propensity score is warning signs added up; real models only differ in learning how many points each sign deserves.

### Visualization (canvas `c2`, 720×300)

Horizontal stacked-bar buildup: three member rows on a shared 0–60% score axis, each bar assembled from the colored point segments that fired for that member.

- **Title (bold 15px, `#1a5276`, top center):** "Three Members, Same Recipe, Three Scores".
- **Axis:** horizontal 2px `#999` line at y=250 from x=170 to x=680 (width 510), score 0% to 60%; tick labels "0%", "10%", ..., "60%" every 10 (12px `#444`) below.
- **Rows (16px-tall bars at y = 95, 150, 205), each with a left-aligned 12px `#444` name label at x=20:**
  - "Ben — 12 visits, annual": one segment, base 5 → total 5
  - "Maya — 2 visits, month-to-month": segments base 5 + low visits 20 + month-to-month 15 → total 40
  - "Ana — 3 visits, new member": segments base 5 + low visits 20 + month-to-month 15 + new member 10 → total 50
- **Segment fills:** base `rgba(107,114,128,0.45)`, low visits `rgba(42,120,214,0.45)`, month-to-month `rgba(217,89,38,0.45)`, new member `rgba(213,81,129,0.45)`; each with a 1px border in its full-strength color (`#6b7280`, `#2a78d6`, `#d95926`, `#d55181`); thin white 1px gaps between segments.
- **Totals:** bold 13px `#1a5276` label just right of each bar end: "5%", "40%", "50%".
- **Legend (12px, top right near y=60):** four color swatches labeled "base 5", "low visits +20", "month-to-month +15", "new member +10".
- **Annotation (bold 12px blue `#2a78d6`, near x=420, y=285):** "every score is just the fired signs added up".

## Check the Scores Against Reality Before Spending

**Tags:** `where it's used` (blue), `calibration` (green), `reliability curve` (orange)

- **The check** — bucket last month's 1,000 scored members, then count who actually cancelled in each bucket
- **The gap** — members scored near 90% cancelled only 40% of the time; near 70%, only 35% did
- **Overconfident** — this model ranks fine but exaggerates: raw scores run far above real cancel rates
- **The fix** — stretch scores to match last month's outcomes (calibration), so 40% printed means 40% real
- **Why it matters** — a $30 save-offer only pays off above a real 25% risk; raw scores would roughly double the list
- **Redo it often** — calibration drifts as seasons and pricing change; re-check against each fresh month

*Example (italic):* Before calibration the gym budgeted save-offers for everyone above a raw 25%; after calibration only the true above-25% members qualified — a much shorter, cheaper list.

**Key point:** Calibrate before acting: compare score buckets to actual outcomes, and do any cost-benefit math with the corrected rates, never the raw scores.

### Visualization (canvas `c3`, 720×300)

Single-panel reliability curve: raw predicted score on x versus actual cancel rate on y for five buckets, with the perfect-calibration diagonal shown dashed for contrast.

- **Title (bold 15px, `#1a5276`, top center):** "Raw Score vs What Really Happened".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 190; x = raw predicted score 0% to 100% with 12px `#444` tick labels "0%", "20%", ..., "100%" every 20; y = actual cancel rate 0% to 100%, light `#e5e9ef` gridlines at 20/40/60/80 with 12px `#444` labels.
- **Perfect line:** dashed `#6b7280` (dash 6/4) 2px diagonal from (0%, 0%) to (100%, 100%); 12px `#6b7280` label "perfectly calibrated" along it near x=78%.
- **Model curve:** blue `#2a78d6` 3px line through five points at raw score `[10, 30, 50, 70, 90]` with actual rate `[8, 18, 25, 35, 40]`; 7px blue dots at each point; 12px blue value label above each dot ("8%", "18%", "25%", "35%", "40%").
- **Gap marker:** vertical dashed orange `#d95926` (dash 4/3) line at x=90% from the model point (40%) up to the diagonal (90%).
- **Annotation (bold 13px orange `#d95926`, near x=52%, y=75):** two lines: "scored 90% → only 40% cancel:" / "the model exaggerates the risk".
- **Caption (12px `#444`, bottom right):** "illustrative — five buckets of last month's 1,000 members".

## A Score Is a Rank, Not a Promise

**Tags:** `common mistake` (red), `ranking vs probability` (orange)

- **Two jobs** — a score can order members (call Ana before Ben) and claim a rate (40% will cancel)
- **Ranking survives** — even uncalibrated, higher raw score still means higher risk, so the call order holds
- **The rate lies** — raw buckets `[10, 30, 50, 70, 90]`% actually cancelled `[8, 18, 25, 35, 40]`%
- **Budget blow-up** — raw scores predict 338 cancels across the 1,000 members; only 185 really happen
- **Lost causes** — a high cancel score also never promises the call will work; some leavers are already gone

*Example (italic):* The gym pre-ordered 338 win-back gift cards because the raw scores summed to 338 expected cancels — 185 members cancelled, and 153 cards sat in a drawer.

**Common mistake:** Treating a raw score as a probability. Using it to rank who to contact first is fine; plugging it into expected-cost or budget math before calibrating is not.

### Visualization (canvas `c4`, 720×300)

Grouped bar chart: for each of the five score buckets, expected cancels from raw scores (blue) next to the cancels that actually happened (green), with the two totals compared.

- **Title (bold 15px, `#1a5276`, top center):** "Cancels the Raw Scores Promised vs Cancels That Happened".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 190; x = five buckets with 12px `#444` labels "0–20%", "20–40%", "40–60%", "60–80%", "80–100%" centered under each pair; y = cancels 0 to 100, light `#e5e9ef` gridlines at 25, 50, 75, 100 with 12px `#444` labels.
- **Bar pairs (bars 34px wide, 6px gap within a pair):** raw expected cancels `[40, 75, 90, 70, 63]` fill `rgba(42,120,214,0.35)` with 2px `#2a78d6` border; actual cancels `[32, 45, 45, 35, 28]` fill `rgba(0,131,0,0.35)` with 2px `#008300` border; bold 12px value label in the border color above each bar.
- **Legend (12px, top left near x=80, y=65):** blue swatch "raw score promise", green swatch "actually cancelled".
- **Annotation (bold 13px red `#e74c3c`, near x=430, y=85):** two lines: "raw plan: 338 cancels — real: 185" / "budget built on raw scores runs double".
- **Caption (12px `#444`, bottom right):** "illustrative — same 1,000 members and buckets as the calibration check".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all bar heights, segment sizes, curve points, and totals are the hardcoded arrays above (no randomness); bucket counts `[400, 250, 180, 100, 70]` sum to 1,000 and are reused by c1 and c4; raw-vs-actual rates `[10, 30, 50, 70, 90]` / `[8, 18, 25, 35, 40]` are shared by c3 and c4 (c4 cancels = count × rate: raw 40+75+90+70+63 = 338, actual 32+45+45+35+28 = 185). All numbers are invented and labeled "illustrative".
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
