# Differential Privacy

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Differential Privacy

**Subtitle:** Publish the true aggregate while exposing no individual — the released number would look (almost) the same whether or not your row was in the dataset

## One Row In, One Row Out

**Tags:** `core idea` (blue), `plausible deniability` (green), `privacy` (orange)

- **The survey** — a company of 1,000 employees is asked "have you ever padded an expense report?"
- **The fear** — 700 have, but nobody answers honestly if their single row can be traced back
- **The promise** — the published count is noised so it looks the same with or without your row
- **The test** — compare the world where you answered and the world where you didn't: near-identical
- **The payoff** — the aggregate (about 700) is still recoverable; your individual answer is not

*Example (italic):* The company publishes "703 yes"; whether the true count was 700 with you or 699 without you, "703" was about equally likely either way — your row is deniable.

**Key point:** Differential privacy is a property of the release, not the data: the output's probability distribution barely changes when any one person's row is added or removed, so nothing specific to you can be learned from it.

### Visualization (canvas `c1`, 720×300)

Two overlapping Laplace-shaped curves: the distribution of possible published counts with your row in the dataset vs without it — nearly indistinguishable.

- **Title (bold 15px, `#1a5276`, top center):** "The Published Count Looks the Same With or Without Your Row".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = published count 660 to 740 (7.5px per unit), 12px `#444` tick labels at 660/680/700/720/740; no y ticks (relative likelihood).
- **Curve A (with you, center 700):** blue `#2a78d6` 3px line through x-values `[660, 665, 670, 675, 680, 685, 690, 695, 700, 705, 710, 715, 720, 725, 730, 735, 740]` with heights (px above baseline) `[3, 5, 8, 12, 20, 33, 55, 91, 150, 91, 55, 33, 20, 12, 8, 5, 3]` — a Laplace(700, 10) shape.
- **Curve B (without you, center 699):** green `#008300` 3px line, the identical height array drawn shifted 7.5px left (centered at 699).
- **Fills:** none; curves only, so the overlap is visible.
- **Legend (12px, top right):** blue swatch "with your row (center 700)", green swatch "without it (center 699)".
- **Annotation (bold 13px violet `#4a3aa7`, near x=720, y=110):** "an attacker can't tell which world produced the output".
- **Caption (12px `#444`, bottom right):** "Laplace noise, scale 10 — heights exact for that scale, scenario illustrative".

## The Coin-Flip Survey

**Tags:** `worked example` (blue), `randomized response` (green)

- **The scheme** — each employee flips a coin in private: heads, answer truthfully; tails, flip again
- **Second flip** — on tails, answer "yes" for heads and "no" for tails, ignoring the truth entirely
- **Deniability** — any single "yes" can be blamed on the coin, so no answer incriminates anyone
- **The counts** — 500 flip heads and tell the truth (350 yes); 500 answer at random (250 yes)
- **The reversal** — 600 of 1,000 say yes; true rate = (600 − 250) / 500 = 70% — recovered exactly
- **The formula** — observed = 0.5 × true + 0.25, so true = (observed − 0.25) / 0.5

*Example (italic):* 60% say yes under the coin scheme, so the true rate is (0.60 − 0.25) / 0.50 = 70% — matching the 700 employees who actually padded a report.

**Key point:** Randomized response is the gateway to differential privacy: noise is injected at each individual answer, every answer stays deniable, yet simple arithmetic recovers the true aggregate.

### Visualization (canvas `c2`, 720×300)

Three horizontal segmented bars: the truthful half, the random half, and the observed total — with the recovery arithmetic written next to the result.

- **Title (bold 15px, `#1a5276`, top center):** "1,000 Coin Flips: Every Answer Deniable, the True Rate Recoverable".
- **Layout:** bars start at x=210, scale 0.5px per person (1,000 people = 500px); bar height 26px; left-aligned 12px `#444` row labels at x=20.
- **Row 1 (y=75), label "500 truthful (heads)":** green `#008300` fill `rgba(0,131,0,0.30)` segment width 175 ("350 yes"), then blue `#2a78d6` fill `rgba(42,120,214,0.25)` segment width 75 ("150 no"); 11px segment labels inside.
- **Row 2 (y=130), label "500 random (tails)":** green segment width 125 ("250 yes"), blue segment width 125 ("250 no").
- **Row 3 (y=185), label "observed answers":** green segment width 300 ("600 yes"), blue segment width 200 ("400 no").
- **Divider:** dashed `#6b7280` (dash 4/3) horizontal line at y=165 separating inputs from the observed total.
- **Annotation (bold 13px magenta `#d55181`, centered near y=250):** "true rate = (600 − 250) / 500 = 70% — exact, no estimation error in the arithmetic".
- **Caption (12px `#444`, bottom right):** "counts shown at their expected values; the 60%→70% reversal is exact".

## The Noise Dial and the Privacy Budget

**Tags:** `where it's used` (blue), `epsilon` (orange), `privacy budget` (green)

- **The mechanism** — add Laplace noise to the query answer, scaled to the query's sensitivity
- **Sensitivity** — how much one person can change the answer: for a count, exactly 1
- **Epsilon** — the privacy dial: noise scale = sensitivity / ε, so smaller ε = more private = noisier
- **The budget** — every query spends ε; four queries at ε = 0.5 spend a total of ε = 2
- **You can't ask forever** — repeated queries compound exposure, so the budget caps total questions
- **Deployed** — the US Census 2020 release and big-tech telemetry collection use documented DP

*Example (italic):* At ε = 0.1 the published count of 700 comes back as 713 or 679; at ε = 2 it comes back as 700 or 701 — the dial trades privacy for accuracy.

**Key point:** Epsilon makes the privacy–accuracy trade-off explicit and finite: each answer costs budget, and when the budget is spent, the dataset must stop answering.

### Visualization (canvas `c3`, 720×300)

Strip plot of released counts at four epsilon settings: five hardcoded noisy releases per ε scatter around the true count 700, with spread shrinking as ε grows.

- **Title (bold 15px, `#1a5276`, top center):** "Same True Count (700), Four Epsilon Settings: the Accuracy–Privacy Dial".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = released count 660 to 740 (2.25px per unit), gridlines `#e5e9ef` and 12px `#444` labels at 660/680/700/720/740; four x positions at 150/290/430/570 with 12px `#444` labels "ε=0.1 (scale 10)", "ε=0.5 (scale 2)", "ε=1 (scale 1)", "ε=2 (scale 0.5)".
- **True-value line:** dashed `#6b7280` (dash 4/3) horizontal line at count 700, 12px `#6b7280` label "true count 700" at its left end.
- **Dots (6px radius, jittered ±12px in x):** ε=0.1 orange `#d95926` at counts `[713, 679, 704, 691, 717]`; ε=0.5 yellow `#c98500` at `[703, 696, 701, 698, 703]`; ε=1 aqua `#199e70` at `[699, 702, 700, 698, 701]`; ε=2 blue `#2a78d6` at `[700, 701, 699, 700, 700]`.
- **Annotation (bold 13px orange `#d95926`, near x=150, y=75):** "smaller ε: more private, noisier".
- **Annotation (bold 12px violet `#4a3aa7`, bottom center near y=272):** "budget: these four queries together spend ε = 0.1 + 0.5 + 1 + 2 = 3.6".
- **Caption (12px `#444`, bottom right):** "noise draws illustrative; scales 10 / 2 / 1 / 0.5 exact for sensitivity 1".

## What the Promise Does Not Cover

**Tags:** `common mistake` (red), `group inference` (orange)

- **The confusion** — DP protects your row's contribution, not conclusions about groups you belong to
- **Group findings** — "70% of employees padded reports" applies to you whether or not you answered
- **No opt-out shield** — refusing to participate does not stop the finding from being used against you
- **What it blocks** — learning your specific answer, or that your record was in the dataset at all
- **What it allows** — accurate population-level statistics, which is the entire point of publishing

*Example (italic):* After the 70% finding, the CFO tightens expense audits for everyone — including the employee who never took the survey; DP never promised to prevent that.

**Common mistake:** Reading "differentially private" as "nothing about me can be inferred." DP bounds what the output reveals about your individual row; group-level findings still apply to every member of the group, participant or not.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: individual-level inference blocked by the noise, group-level inference flowing straight through.

- **Title (bold 15px, `#1a5276`, top center):** "Protected vs Not: Your Row vs Your Group".
- **Row 1 (y=95), label 12px `#444` at x=20:** "your row"; blue `#2a78d6` rounded box at x=160 labeled "your answer: yes" (12px), 3px arrow toward a box at x=430 labeled "published: 703" — arrow interrupted mid-way by a bold 16px red `#e74c3c` "✗" and 12px red label "noise blocks the link"; bold 12px green `#008300` "✓ protected" right of the second box.
- **Row 2 (y=205), label:** "your group"; blue box at x=160 labeled "finding: 70% padded", 3px solid arrow to an orange `#d95926` box at x=430 labeled "audits tighten for all", bold 12px red `#e74c3c` "✗ not protected — applies to non-participants too" right of it.
- **Box style:** 160–180px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(217,89,38,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px magenta `#d55181`, centered near y=270):** "DP hides individuals inside the statistic — it does not hide the statistic".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness, no `Math.random`); the randomized-response arithmetic (500/350/250/600, (600 − 250)/500 = 70%, (0.60 − 0.25)/0.50 = 0.70) is exact; Laplace scales 10/2/1/0.5 for ε = 0.1/0.5/1/2 at sensitivity 1 are exact; the c1 curve heights are exact for Laplace scale 10; the noisy release dots in c3 and the 700-of-1,000 scenario are invented and labeled illustrative.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
