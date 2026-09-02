# Correlation vs Causation: Five Other Things It Could Be

**Page type:** detail page — card-section template (see `statistical-paradoxes/03-berksons-paradox.html`)
**HTML title tag:** Correlation vs Causation — Cognitive Biases

**Subtitle:** Everyone knows two things moving together proves nothing. Almost nobody can name what else it would be.

---

## Section 1 — Twenty-Eight Branches, and the Ones That Train More Score Better

**Tags:** `core idea` (violet), `one cloud` (blue), `a hidden third thing` (magenta)

**Bullets:**
- **The data** — twenty-eight branches, each with its training hours and its customer score
- **What the cloud shows** — the more a branch trains, the higher it scores, with little scatter
- **The line through it** — ten extra hours of training sits alongside three tenths of a star
- **The obvious reading** — buy hours, the score follows, so fund training in the branches that lag
- **What is also true** — the branches that train hardest are the ones a manager runs closely
- **What close management does** — it clears the rota for training and it sets the service standard
- **What an hour is really worth** — a twentieth of a star per ten, in the world that made this
- **So the line overstates it** — by nearly six times, and nothing on the chart hints at that

**Key point:** The upward cloud is a fact about the data. "Training raises the score" is a claim about a world nobody observed — and at least five other worlds draw this exact cloud.

**Source note (`.src`):** Illustrative Example — twenty-eight constructed branches where a hidden factor lifts both figures; the fitted line and every gap are computed in the draw function.

### Visualization — canvas `c1`, 720×330

The branch cloud with its fitted line, the points shaded by the hidden factor, and the promised gain set against the real one.

- **Construction:** seeded Park–Miller LCG, seed 3796; 28 branches. For each, a hidden `q` in 0–1 (how closely the branch is run) drawn first, then `hours = 16 + 32q + (rng()·2−1)·3.0`. With `hbar = mean(hours)`, `score = 3.45 + 1.00q + 0.005·(hours − hbar) + (rng()·2−1)·0.15`. The **true** worth of an hour of training is therefore 0.005 stars, i.e. **+0.05 per ten hours**, hard-coded as `TRUE_H` and printed from that constant.
- **Computed in the draw function:** hours span 15.9–49.2, scores 3.49–4.40; the fitted line gives **+0.29 stars per ten hours**; observed ÷ true = **5.8×**. The seven lightest-training branches average 20 hours and 3.60 stars, the seven heaviest 44 hours and 4.28 stars, a gap of 0.68 stars.
- **Title (bold 15px `P.ink`, centered, y=22):** "Twenty-Eight Branches: Training Hours Against Customer Score"
- **Plot box:** `PX=52`, `PY=44`, width `= w − 236 − PX`, bottom at `h − 74`. X axis 12–52 hours, Y axis 3.2–4.6 stars. Axis lines `#ccc` 1px; tick labels 12px `P.mute`; axis captions 12px `P.mute` "training hours per person, per year" and rotated "customer score".
- **Points:** radius 5. Fill interpolates violet by the hidden factor — `rgba(74,58,167,α)` with `α = 0.20 + 0.55q` — stroked `P.violet` 1px. A 12px `P.mute` note under the plot reads "each dot is one branch — darker means the branch is run more closely", so the hidden factor is visible rather than asserted.
- **Fitted line:** 2.5px `P.blue`, drawn from the fit across the plotted x range. Its printed slope, bold 12px `P.blue` beside the line's right end, is `(slope×10).toFixed(2)` — never a literal.
- **Right panel** at `w − 216`: bold 13px `P.ink` "WHAT THE LINE PROMISES", then bold 19px `P.blue` "+0.29" with 12px `P.mute` "stars per 10 hours"; bold 13px `P.ink` "WHAT AN HOUR IS ACTUALLY WORTH", then bold 19px `P.green` "+0.05" with 12px `P.mute` "stars per 10 hours"; below, bold 12px `P.magenta` "the line overstates it 5.8×", ratio computed.
- **Caption (bold 13px `P.violet`, centered, `h−10`):** "The slope is real. What it is a slope of is the open question."

---

## Section 2 — Six Worlds That Draw the Same Cloud

**Tags:** `the actual list` (orange), `six panels` (yellow), `one picture` (red)

**Bullets:**
- **Same line six times** — six panels, each fitted at about three tenths of a star per ten hours
- **Panel one** — training genuinely lifts the score, and paying for hours pays what the line says
- **Panel two** — the arrow runs backwards: a branch that scores well is handed a bigger budget
- **Panel three** — a closely run branch gets both, and neither one is doing anything to the other
- **Panel four** — forty-four branches with no link, until the sixteen that disagreed were dropped
- **Panel five** — eight branches, nothing behind them; chance draws this about once in sixty tries
- **Panel six** — hours and scores both climb year on year, sharing nothing but the calendar
- **What a reader gets** — one cloud, and no way from the cloud alone to say which panel drew it

**Key point:** "Correlation is not causation" is the easy half. The useful half is this list — and choosing between the six needs facts about how the numbers were produced, which no chart contains.

**Source note (`.src`):** Illustrative Example — six constructed worlds, each on its own seed; every panel's slope is fitted from that panel's own plotted points.

### Visualization — canvas `c2`, 720×340

Six small scatter panels in a three-by-two grid, each in its own hue, each carrying the slope fitted from its own points.

- **Common axes:** every panel uses x 10–52 hours and y 3.2–4.7 stars, so the six clouds are directly comparable and the near-identical slopes are visible as shape, not just as numbers.
- **Panel 1 — "training lifts the score"** (`P.green`): seed 15, 28 points. `hours = 16 + 32·rng()`, `score = 3.10 + 0.0300·hours + (rng()·2−1)·0.20`. Fitted **+0.29 per 10 h**.
- **Panel 2 — "the score wins the budget"** (`P.blue`): seed 30, 28 points. `score = 3.45 + 1.05·rng()` drawn first, then `hours = (score − 3.10)/0.0300 + (rng()·2−1)·3.0`. Fitted **+0.29**. Drawn with the arrow reversed in the panel label.
- **Panel 3 — "one thing drives both"** (`P.violet`): seed 2, 28 points. `q = rng()`, `hours = 16 + 32q + (rng()·2−1)·3.0`, `score = 3.45 + 1.05q + (rng()·2−1)·0.17`. Fitted **+0.29**.
- **Panel 4 — "odd rows were queried away"** (`P.magenta`): seed 2317. Draw independent pairs `hours = 16 + 32·rng()`, `score = 3.45 + 1.05·rng()` until 44 exist. With `zx = (hours−32)/16` and `zy = (score−3.975)/0.525`, a row is **kept** when `|zx − zy| < 0.72`; that keeps 28 and drops 16. The 28 kept fit **+0.29**; all 44 together fit **+0.02**. Dropped points drawn as small hollow `P.mute` circles so the reader sees what left.
- **Panel 5 — "eight branches, pure chance"** (`P.yellow`): seed 411, 8 independent points from the same two draws as panel 4. Fitted **+0.29**. The "about once in sixty tries" in the bullets is the share of eight-branch draws reaching this slope, 1.6% over 60,000 seeded trials (1 in 60), stable to ±0.06pp across seeds 7 / 42 / 99 / 2024 / 3796.
- **Panel 6 — "both drifting upward"** (`P.aqua`): seed 2481, 28 monthly points. `m = i/27`, `hours = 16 + 32m + (rng()·2−1)·2.6`, `score = 3.45 + 1.05m + (rng()·2−1)·0.16`. Fitted **+0.29**.
- **Title (bold 15px `P.ink`, centered, y=20):** "Six Worlds, One Cloud"
- **Panel geometry:** grid origin `GX=16`, `GY=34`; cell `CW = (w − 2·GX)/3`, `CH = 142`. Inside each cell a plot box inset 34px left / 8px right / 26px top / 22px bottom. Panel frame 1px `P.grid`.
- **Per panel:** bold 12px panel label in the panel's hue at the top of the cell; points radius 3 filled at 0.45 alpha of the hue, stroked in the hue; a 2px fitted line in the hue; the slope printed bold 12px in the hue, bottom-right of the plot box, as `'+' + (slope*10).toFixed(2)`.
- **Panel 4 extra:** bold 12px `P.mute` "16 of 44 rows dropped" under the label, count taken from the split, plus the all-44 slope printed in `P.mute`.
- **Panel 5 extra:** bold 12px `P.mute` "8 branches" under the label, from the array length.
- **Caption (bold 13px `P.orange`, centered, `h−9`):** "Only the first panel rewards spending on training. All six draw the same line."

---

## Section 3 — Shelf Space Follows Sales, Not the Other Way Round

**Tags:** `the arrow reversed` (aqua), `shelf facings` (orange), `acting on it` (blue)

**Bullets:**
- **The chain's data** — thirty product lines, the facings each gets, and its weekly units sold
- **What it looks like** — one more facing sits alongside four more units a week, and it fits tight
- **How the facings were set** — last year's sales decided them, so the fast lines started out wide
- **What a facing is really worth** — four tenths of a unit a week, ten times less than it looks
- **The plan it invites** — give the ten slowest lines three facings each, worth twelve units each
- **What thirty facings actually buy** — a bit over one unit a week per line, not twelve
- **Where those facings came from** — the ten fastest lines, which each shed the same bit over one
- **Net change for the chain** — nothing, after a month of shelf resets to get there

**Key point:** When the outcome set the input, the input looks powerful. Acting on that reading shifts space from the lines that earned it to the lines that did not, and the chain total does not move.

**Source note (`.src`):** Illustrative Example — thirty constructed lines where last year's sales set this year's facings; the fitted slope and both payoffs are computed in the draw function.

### Visualization — canvas `c3`, 720×330

The facings-against-units cloud with its steep fitted line on the left, and the promised-versus-delivered payoff of moving thirty facings on the right.

- **Construction:** seeded LCG, seed 4; 30 lines. `last = 4 + 34·rng()` (last year's weekly units), then `facings = clamp(round(last/3.6 + (rng()·2−1)·1.0), 1, 12)`. With `fbar = mean(facings)`, this year's `units = max(1, last + 0.4·(facings − fbar) + (rng()·2−1)·2.2)`. The **true** worth of a facing is `TRUE_F = 0.4` units a week.
- **Computed in the draw function:** facings run 1–11, units 1.2–40.1; the fitted slope is **+4.0 units per facing**, which is **10×** the truth. The ten slowest lines average 2.4 facings and 6.9 units a week; the ten fastest average 8.7 facings and 34.2 units.
- **The move:** three extra facings to each of the ten slowest = **30 facings**, taken off the ten fastest. Line promises `4.0 × 3 = +12` units a week per slow line, **+120 chain-wide**. Truth delivers `0.4 × 3 = +1.2` per slow line, **+12 chain-wide** — and the fast lines each shed the same 1.2, **−12**. Net **0**.
- **Title (bold 15px `P.ink`, centered, y=22):** "Thirty Product Lines: Shelf Facings Against Weekly Units"
- **Left plot:** `PX=48`, `PY=46`, width `0.46w`, bottom `h − 76`. X axis 0–12 facings, Y axis 0–44 units. Points radius 4.5, `rgba(25,158,112,0.50)` stroked `P.aqua`. Fitted line 2.5px `P.aqua` with its slope printed bold 12px `P.aqua` — "each facing looks worth +4.0/wk", value computed.
- **Left annotation (12px `P.mute`, under the plot):** "each dot is one product line"; then bold 12px `P.orange` "last year's sales chose the facings — the arrow runs right to left".
- **Right panel** at `0.52w`: bold 13px `P.ink` "GIVE THE TEN SLOWEST LINES THREE FACINGS EACH", with bold 12px `P.mute` "30 facings, taken off the ten fastest" beneath. Then three rows on a 52px pitch, each a horizontal bar on a shared scale of 0–130 units a week:
  - "what the line promises" — `rgba(217,89,38,0.50)` / `P.orange`, bold 19px `+120`
  - "what the slow lines gain" — `rgba(25,158,112,0.50)` / `P.aqua`, bold 19px `+12`
  - "what the fast lines lose" — `rgba(107,114,128,0.35)` / `P.mute`, bold 19px `−12`, bar drawn leftward from the same origin
  Every figure comes from `slope × 3 × 10` or `TRUE_F × 3 × 10`, none typed in.
- **Net line:** bold 13px `P.green` "net for the chain: 0 units a week", computed as gain + loss so it cannot drift.
- **Caption (bold 13px `P.aqua`, centered, `h−9`):** "A facing did not make a line fast. Being fast is what won it the facing."

---

## Section 4 — Everything a Growing Chain Tracks Moves Together

**Tags:** `shared climb` (yellow), `dashboard pairs` (magenta), `nothing in common` (violet)

**Bullets:**
- **The dashboard** — twelve measures a growing chain records every month for eight years
- **Nothing connects them** — each was built to climb at its own pace with its own separate wobble
- **Pairs you can form** — sixty-six, and sixty-five of them hug a rising line closely
- **Why they do** — over eight years anything that grows tracks anything else that grows
- **The typical pair** — reading one measure lands two thirds closer than the other's average
- **The fix** — compare month-to-month changes, not levels, which strips the shared climb
- **What survives it** — not one pair of the sixty-six stays close once the climb is gone
- **The real hazard** — a rising dashboard mints convincing pairs faster than anyone can check them

**Key point:** Two things climbing over the same eight years will look linked whatever they are. The shared climb does all the work, and subtracting it leaves nothing behind.

**Source note (`.src`):** Illustrative Example — twelve constructed monthly series, each with its own growth rate and independent wobble; every count is tallied in the draw function.

### Visualization — canvas `c4`, 720×320

Twelve unrelated rising series on the left, and on the right how many of their sixty-six pairs stay close on levels versus on month-to-month changes.

- **Construction:** seeded LCG, seed 42; 12 series of 96 months. Per series a growth rate `drift = 0.6 + 1.0·rng()` and a wobble scale `wob = 0.5 + 0.5·rng()`, then `value[i] = drift · (i/95) · 100 + (rng()·2−1) · wob · 18`. No series references any other, so every apparent link is the shared climb.
- **Closeness measure (stated in plain words on the chart):** for a pair, how much smaller the typical miss becomes when you read one series off the other instead of just quoting the second one's own average. A pair counts as close when that miss is at least halved.
- **Computed in the draw function:** of **66** pairs, **65** are close on the levels, median miss cut **66%**, best **81%**, worst **49%**. On month-to-month changes, **0** of 66 are close, median cut **1%**, best **8%**.
- **Title (bold 15px `P.ink`, centered, y=22):** "Twelve Unrelated Measures Over Eight Years"
- **Left plot:** `PX=44`, `PY=44`, width `0.48w`, bottom `h − 66`. Y axis auto-scaled from the series minimum and maximum (−14 to 166). Twelve 1.5px polylines, all in `P.yellow` at 0.55 alpha except the two tightest-hugging pair members, drawn 2px solid `P.yellow` and `P.magenta` so one example pair is followable. X axis ticks at years 0, 2, 4, 6, 8, labels 12px `P.mute`; axis caption 12px `P.mute` "months, over eight years".
- **Left note (12px `P.mute`):** "each line is one measure — none was built from any other".
- **Right panel** at `0.54w`: bold 13px `P.ink` "PAIRS THAT TRACK EACH OTHER CLOSELY", then two bar rows on a 56px pitch over a shared 0–66 scale:
  - "comparing the levels" — `rgba(213,81,129,0.50)` / `P.magenta`, bold 19px "65" plus 12px `P.mute` "of 66 pairs"
  - "comparing month-to-month change" — `rgba(107,114,128,0.30)` / `P.mute`, bold 19px "0" plus 12px `P.mute` "of 66 pairs"
  Both counts tallied from the pair loop, not typed in.
- **Right footnote (12px `P.mute`, two lines):** "close = reading one measure at least halves the typical miss"; then bold 12px `P.violet` "on levels the typical pair cuts the miss 66%; on changes, 1%", both medians computed.
- **Caption (bold 13px `P.yellow`, centered, `h−9`):** "The calendar is the only thing these twelve measures share."

---

## Section 5 — Stocking Umbrellas Without a Theory of Weather

**Tags:** `where it is fine` (green), `read the signal` (blue), `pull the lever` (red)

**Bullets:**
- **A shop and the clouds** — move umbrellas to the front when the sky darkens, and they sell
- **No theory needed** — clouds do not make anyone want an umbrella, they simply arrive first
- **What the shop is doing** — reading a signal, not pulling a lever, and the signal keeps working
- **Back to the branches** — read a branch's score off its hours and the typical miss shrinks
- **The two misses** — a quarter of a star quoting the average, under a tenth reading the hours
- **Why that is safe** — nothing was changed, so no claim about what causes what was ever needed
- **Now pull the lever** — lift the eleven lightest branches from twenty-two hours to forty-four
- **What the line promised** — two thirds of a star each; what the hours deliver is about a tenth
- **The line that matters** — an unexplained signal is usable; an unexplained lever is not

**Key point:** Using a link to guess what you have not measured is fine, and needs no story about cause. Using it to decide what to change is a different act — it swaps the world that drew the picture for one that never did.

**Source note (`.src`):** Illustrative Example — the same twenty-eight constructed branches as the opening section; both misses and both payoffs are computed in the draw function.

### Visualization — canvas `c5`, 720×330

Left: predicting a branch's score from its hours, with the two typical misses drawn to scale. Right: what happens when the eleven lightest branches are pushed to forty-four hours.

- **Construction:** identical to `c1` — seed 3796, 28 branches, `TRUE_H = 0.005`. Both charts rebuild the world from the same seed in the same draw order, so their figures describe one dataset.
- **Predicting, computed in the draw function:** quote the overall average of 3.93 stars for every branch and the typical miss is **0.26** stars; read the score off the fitted line instead and it is **0.08** — the miss cut by **70%**.
- **Intervening, computed in the draw function:** the **11** branches below 26 hours average **21.6** hours and **3.63** stars. Push each to **44** hours (**247** extra hours in all, about 22 each). The line promises `slope × (44 − 21.6) = +0.66` stars, landing at **4.29**. The true effect gives `0.005 × (44 − 21.6) = +0.11`, landing at **3.74** — short by **5.8×**, the same factor as the opening section, because it is the same ratio.
- **Title (bold 15px `P.ink`, centered, y=22):** "Reading the Signal, Then Pulling the Lever"
- **Left panel — PREDICT:** header bold 13px `P.green` "PREDICT: GUESS A BRANCH'S SCORE". A compact scatter (`PX=44`, plot width `0.40w`, height 118) of the 28 branches, points radius 3.5 `rgba(0,131,0,0.40)` stroked `P.green`, with the fitted line 2px `P.green` and a dashed 1.5px `P.mute` horizontal line at the overall average. Below, two horizontal miss bars on a shared 0–0.30 scale: "quote the average" `rgba(107,114,128,0.35)` / `P.mute` with bold 19px "0.26", and "read the hours" `rgba(0,131,0,0.45)` / `P.green` with bold 19px "0.08". Bold 12px `P.green` "typical miss cut 70%" beneath, computed.
- **Right panel — INTERVENE:** header bold 13px `P.magenta` "INTERVENE: BUY THE LIGHT BRANCHES MORE HOURS". A single hours-against-score frame at `0.54w`, x 15–48 hours, y 3.4–4.5 stars. The 11 light branches plotted as a cluster of radius-4 `rgba(213,81,129,0.45)` dots stroked `P.magenta` at their mean point, the other 17 as small hollow `P.mute` circles. Two arrows leave the cluster mean at (21.6, 3.63):
  - a 2.5px dashed `P.magenta` arrow rising to (44, **4.29**) labelled bold 12px `P.magenta` "where the line said they would land"
  - a 2.5px solid `P.green` arrow rising to (44, **3.74**) labelled bold 12px `P.green` "where they actually land"
  Both endpoints computed from the fit and from `TRUE_H`. Bold 12px `P.mute` "247 extra training hours bought" under the frame.
- **Caption (bold 13px `P.green`, centered, `h−9`):** "Predicting needs no cause. Changing something does, and this change buys almost nothing."

---

## Regeneration instructions

- **Template:** the card-section layout from `statistical-paradoxes/03-berksons-paradox.html`. Five `.card-section` blocks, each an `<h2>` (1.3rem `#1a5276`, `border-bottom: 2px solid #2980b9`, 4px bottom padding) plus a `table.layout` with one `<tr>`: `td.text-col` 50% / `td.viz-col` 50%.
- **Text column order:** `.tags` pill row → `<ul>` of one-line bullets each opening `<b>label</b>` then an em dash → one `.key-point` callout → `.src` note. No paragraph blocks, no data tables, no `.example` line restating a bullet.
- **Bullet form:** ONE line each, ≤95 characters including the bold label, so nothing wraps at 50% column width.
- **Page CSS:** body system-ui, white, `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with `border-bottom: 2px solid #2980b9`, 8px bottom padding. `.subtitle` `#666` 0.95rem, 32px bottom margin. `.card-section` 40px bottom margin. `table.layout` full width, border-collapse; td vertical-align top, padding 12px; `.viz-col` `text-align: center`. `ul` 0.92rem, margin `8px 0 8px 20px`; `li` 4px bottom margin; `li b` `#1a5276`. `.key-point` `#f8f9fa`, `border-left: 3px solid #e74c3c`, padding 8px 12px, 0.9rem. `.src` 0.78rem `#888`. No nav, no `.nav` CSS, no back/home links, no cross-page links.
- **Tag pills:** `display:inline-block`, 0.72rem, weight 600, padding 2px 10px, radius 10px. Classes used: `.blue` `.green` `.red` `.orange` plus `.violet` `rgba(74,58,167,0.12)`/`#4a3aa7`, `.magenta` `rgba(213,81,129,0.14)`/`#c2426f`, `.aqua` `rgba(25,158,112,0.14)`/`#17805d`, `.yellow` `rgba(201,133,0,0.15)`/`#a06c00`.
- **Hue family per section:** 1 violet with a blue line and a magenta verdict; 2 one hue per panel (green, blue, violet, magenta, yellow, aqua) with an orange caption, since which panel a cloud came from *is* the data; 3 aqua cloud with orange/mute payoff bars and a green net line; 4 yellow series with magenta/mute count bars and a violet footnote; 5 green predict panel against a magenta intervene panel.
- **Canvas:** CSS `width: 100%`, `border: 1px solid #e0e0e0`, radius 4px. Intrinsic `width="720"`; heights `c1` 330, `c2` 340, `c3` 330, `c4` 320, `c5` 330. `setup(id)` caches the logical size in `dataset` on first call (because `canvas.width` overwrites the attribute), sets `style.maxWidth = 720px`, computes `scale = (cssW/720) × devicePixelRatio`, sizes the backing store to `logical × scale`, and `ctx.scale(scale, scale)` back to logical coordinates. Draws registered in `__charts`, re-run on a 150ms debounced resize.
- **Canvas font sizes:** chart title bold 15px; in-chart header bold 12–13px; labels 12px floor; the big callout figure bold 19px; caption bold 13px.
- **Palette** (shared `P` object): `blue #2a78d6`, `green #008300`, `magenta #d55181`, `yellow #c98500`, `aqua #199e70`, `orange #d95926`, `violet #4a3aa7`, `ink #1a5276`, `text #2c3e50`, `mute #6b7280`, `grid #e5e9ef`.
- **Determinism:** no `Math.random()`. Seeded Park–Miller LCG (`s = (s × 16807) % 2147483647`). Seeds: `c1` and `c5` 3796; `c2` panels 15, 30, 2, 2317, 411, 2481; `c3` 4; `c4` 42.
- **Every slope is fitted at render time** from the points that panel plots, and printed with `.toFixed(2)`. The two "truth" constants — `TRUE_H = 0.005` stars per training hour and `TRUE_F = 0.4` units per facing — are the only planted quantities, and every promised-versus-delivered figure and every overstatement ratio is derived from them rather than typed in.
- **Deliberate scope limits.** Selection manufacturing a link (panel 4) and a lurking third variable (panel 3) are shown as members of the list and not developed — the mechanisms belong to `statistical-paradoxes/03-berksons-paradox` and `01-simpsons-paradox`. Coincidence (panel 5) is likewise named and left to `05-clustering-illusion` in this folder. This page's own contribution is the *complete list of alternatives* plus the predict/intervene boundary.
- **Replaces the old three-topic page.** `13-causal-reasoning` previously carried "Correlation ≠ Causation", "Base-Rate Neglect in Co-occurrence" and "Crediting the Wrong Active Ingredient" as three list sections; the latter two are now separate pages. Corrections applied while rewriting the first topic:
  - The old page taught only ONE alternative to "A causes B" — the shared third cause — via five arrow diagrams with the same layout, and called it "the most common trap". Four further alternatives are now shown, each with its own computed cloud.
  - The old diagrams contained **no data at all**: three labelled boxes and an arrow per example, with the conclusion written on the canvas. Nothing was computed, so nothing could be checked. Every figure on this page is now fitted from plotted points.
  - The old takeaway asserted "most 'A causes B' headlines are driven by an unmeasured confound C" — an unsourced claim about a population of headlines nobody counted. It is gone.
  - The old page implied any non-causal link is useless. Section 5 corrects that: prediction needs no cause, and only intervention does.
