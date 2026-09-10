# Regression to the Mean: Nobody Got Worse — The Luck Just Ran Out

**Page type:** detail page — card-section template (see `tutorials/llms-generative-ai/08-positional-encoding.html`)
**HTML title tag:** Regression to the Mean — Statistical Paradoxes

**Subtitle:** Why the campaigns you promote after a great week always slump, and the ones you were ready to kill always pick up — with nobody's actual ability changing at all

---

## Section 1 — Your Best Ten Campaigns Slump and Nobody Touched Them

**Tags:** `core idea` (blue), `selection` (green), `luck vs skill` (orange)

**Bullets:**
- **The setup** — 100 ad campaigns run for a week, and the ten with the best click rate get picked
- **Fixed by construction** — each campaign's true rate is set once and never changes across the two weeks
- **Week one** — the chosen ten averaged 10.6% clicks against 7.9% for the whole hundred
- **Week two** — the same ten averaged 8.8%, and nine of the ten came out lower than before
- **Their real rate** — 9.2%, so much of that first-week 10.6% was a good week, not a good campaign
- **The other tail** — the ten worst rose from 5.0% to 7.0%, with eight of the ten improving
- **Nothing was done** — no budget change, no new copy, no intervention of any kind in this data
- **What survives** — only about a third of the top ten's lead over the average carried into week two

**Example line (italic):** The chosen ten averaged 10.6% in week one and 8.8% in week two, while their fixed true rate is 9.2%.

**Key point:** Regression to the mean is what selecting on one noisy measurement does all by itself — the group you picked was picked partly for its luck, and luck does not repeat.

**Source note (`.src`):** Illustrative Example — a seeded simulation in which every campaign's true rate is held fixed across both weeks, so any movement is luck alone.

### Visualization — canvas `c1`, 720×340

Paired-slope chart: all 100 campaigns drawn week 1 → week 2 in gray, with the selected top and bottom tens highlighted. Every printed figure is computed in the draw function from the same arrays that are plotted.

- **Data:** seeded Park–Miller LCG (`s = (s × 16807) % 2147483647`), seed 42. Normal-ish noise by summing three draws: `z() = ((r()+r()+r()) − 1.5) × 2`. For each of 100 campaigns, in this draw order: `t = 8.0 + 1.1·z()`, `w1 = t + 1.4·z()`, `w2 = t + 1.4·z()`. Sorted descending on `w1`; top = first 10, bottom = last 10.
- **Derived figures:** all 100 average 7.87% then 7.90%; top ten 10.63% → 8.78% with a true rate of 9.20%, 9 of 10 falling; bottom ten 5.04% → 7.02%, 8 of 10 rising; the top ten's lead over the average shrinks from 2.76 to 0.88 points, so 32% survives.
- **Title (bold 15px `P.ink`, centered, y=22):** "Same Campaigns, Same Skill — Only the Luck Changed"
- **Plot box:** `PX0=58`, `PX1=452`, `PY0=56`, `PY1=272`; y domain 3.0% to 12.5% (covers the full generated range); `W1X = PX0 + 0.28·PW`, `W2X = PX0 + 0.78·PW` where `PW = PX1 − PX0`.
- **Grid:** `P.grid` horizontal lines at 4, 6, 8, 10, 12 with 12px `P.mute` right-aligned labels; `#ccc` left axis; rotated 12px `P.mute` "click rate, %" at `PX0−42`.
- **Column labels:** bold 12px `P.ink` at `PY1+18` — "WEEK 1" / "WEEK 2"; 12px `P.mute` at `PY1+34` — "(you pick from this)" / "(what happened next)".
- **All 100 slopes:** width 1, `rgba(107,114,128,0.13)`.
- **Top ten:** slopes `rgba(213,81,129,0.55)` width 1.8, endpoint dots radius 3.5 `P.magenta`. **Bottom ten:** slopes `rgba(25,158,112,0.50)` width 1.8, dots radius 3.5 `P.aqua`.
- **Group mean markers:** 26px-wide bars 3px thick at each group's week-1 and week-2 mean, in the group colour, with the mean printed bold 12px beside it to one decimal.
- **Average line:** dashed `P.green` (dash 6/4, width 2) across the plot at the mean of all 200 measurements, labelled 12px `P.green` "average of all 100 — 7.9%", computed.
- **Right annotation column at x=470:** bold 12px `P.magenta` "TOP TEN — picked on week 1" then 12px `P.mute` "10.6% → 8.8% next week" and "9 of the 10 fell"; bold 12px `P.aqua` "BOTTOM TEN" then "5.0% → 7.0% next week" and "8 of the 10 rose"; bold 12px `P.orange` "true skill never moved" then 12px `P.mute` "top ten really run at 9.2%"; finally bold 19px `P.magenta` "32%" with 12px `P.mute` "of the lead survived". Rows spaced in pixels, not data units.
- **Caption (bold 13px `P.magenta`, centered, `h−10`):** "Pick on one week and you pick that week's luck too."

---

## Section 2 — The Coaching Program That Beat Doing Nothing by Nothing

**Tags:** `where it bites` (blue), `false credit` (green), `control group` (orange)

**Bullets:**
- **The program** — 200 stores measured for a quarter, and the 60 weakest are sent a coaching program
- **The cutoff** — every store converting below about 3.5% of its visitors made the list
- **The saving grace** — half of the chosen 60 got the coaching, half were quietly left alone
- **The coached half** — went from 2.90% to 3.58% conversion, a gain of nearly a quarter
- **The untouched half** — went from 2.94% to 3.63%, the same size of gain with no coaching at all
- **The verdict** — the program's effect in this data is zero, yet the write-up would claim a 23% lift
- **The mirror image** — the stores above the cutoff drifted the other way, 4.40% down to 4.09%
- **Who earned the gain** — the selection rule did, by picking the quarter's unluckiest stores

**Example line (italic):** Coached stores rose 2.90% → 3.58% and the stores left alone rose 2.94% → 3.63% — the same gain, no program.

**Key point:** A before-and-after on a group chosen for being extreme measures the choosing, not the doing. Hold back half of the selected group, or the improvement cannot be attributed to anything.

**Source note (`.src`):** Illustrative Example — a seeded simulation with no coaching effect built in, so the entire measured "lift" is regression.

### Visualization — canvas `c2`, 720×340

A selection strip over three paired before/after bar groups. Cutoff, group means and gains are all computed in the draw function.

- **Data:** fresh `lcg(42)` and the same three-draw `z()`. For each of 200 stores, in order: `t = 4.0 + 0.55·z()`, `q1 = t + 0.75·z()`, `q2 = t + 0.75·z()`. Sorted ascending on `q1`; the worst 60 are the program list, split by alternating index into coached (even) and left-alone (odd), 30 each; the remaining 140 are "everyone else".
- **Derived figures:** cutoff 3.48%; coached 2.90% → 3.58% (+0.68 points, +23%); left alone 2.94% → 3.63% (+0.68, +23%); everyone else 4.40% → 4.09% (−0.31); difference in gains −0.01 points.
- **Title (bold 15px `P.ink`, centered, y=22):** "Coach the Worst Stores and Both Halves Improve"
- **Selection strip:** a `#ccc` axis line at y=76 spanning x=60…660, domain 1.0% to 6.5%, with 12px `P.mute` ticks at 2, 3, 4, 5, 6 below it. Each of the 200 stores is a 1px vertical tick 9px tall, `rgba(107,114,128,0.45)`; the region left of the cutoff is filled `rgba(217,89,38,0.12)` with a dashed `P.orange` vertical at the cutoff. Bold 12px `P.orange` left-aligned above the strip: "THE 60 WORST — picked for the program (below 3.5%)", computed; and right-aligned on the same line the computed "program effect: −0.01 points".
- **Bar groups:** centers at x=180, 360, 540; `baseY=272`; value scale 20px per conversion point (nothing generated exceeds 6.2%). Bars 46px wide, 14px gap; "before" bars `rgba(107,114,128,0.35)` stroked `P.mute`, "after" bars in the group colour at 0.45 alpha stroked in that colour.
  - **COACHED — 30 stores** (`P.magenta`), subtitle "got the program"
  - **LEFT ALONE — 30 stores** (`P.aqua`), subtitle "same list, nothing done"
  - **EVERYONE ELSE — 140** (`P.mute`), subtitle "above the cutoff"
- **Labels per group:** group title bold 12px in the group colour at y=118, subtitle 12px `P.mute` at y=132; bold 13px value above each bar to two decimals; 12px `P.mute` "before" / "after" at `baseY+16`; bold 12px change line in the group colour at `baseY+34`, printed from the computed delta and percentage — "+0.68 points (+23%)", "+0.68 points (+23%)", "−0.31 points (−7%)".
- **Baseline:** `#ccc` line under each group.
- **Caption (bold 13px `P.magenta`, centered, `h−10`):** "The coached and the untouched gained the same amount."

---

## Section 3 — A Real Case: Galton Watched Tall Parents Have Shorter Children

**Tags:** `real case` (blue), `named the effect` (green), `mistaken for a law` (red)

**Bullets:**
- **The study** — Galton compared the heights of 928 grown children with the average of their two parents
- **His measure** — mothers' heights were scaled up first, so one number stands for each couple
- **What he found** — a child sits only about two thirds as far from the average as the parents do
- **Tallest tenth** — those couples averaged 71.4 in and their children 70.1 in, over an inch shorter
- **Shortest tenth** — those couples averaged 65.1 in and their children 66.2 in, an inch taller
- **Not a force** — nothing pulls a family back; very tall couples simply carry favourable luck too
- **The business echo** — Secrist tracked leading firms drifting to average and blamed competition
- **The tell he missed** — the worst firms drifted up too, which decaying excellence cannot explain

**Example line (italic):** Galton's tallest tenth of couples averaged 71.4 in and their children 70.1 in — two thirds of the way out from average, not all of it.

**Key point:** Galton named the effect in 1886 in "Regression Towards Mediocrity in Hereditary Stature": children of extreme parents land nearer the average because extreme parents are extreme partly by luck. Secrist's 1933 "The Triumph of Mediocrity in Business" mistook the same arithmetic for an economic law, as Hotelling's review pointed out.

**Source note (`.src`):** Illustrative Example — the scatter is simulated at Galton's reported two-thirds slope; every group mean printed on the chart is computed from those plotted points.

### Visualization — canvas `c3`, 720×340

Scatter of 928 parent–child pairs with the fitted line, the "children match their parents" line for comparison, and both selected tails marked with arrows.

- **Data:** fresh `lcg(42)`, same three-draw `z()`. For each of 928 pairs, in order: `mid = 68.2 + 1.8·z()`, `child = 68.2 + (2/3)·(mid − 68.2) + 2.3·z()`. Sorted descending on `mid`; the tallest tenth is the first 93, the shortest tenth the last 93.
- **Derived figures:** fitted slope 0.67; tallest tenth 71.4 in → 70.1 in, a fall of 1.3 in, keeping 65% of its distance above average; shortest tenth 65.1 in → 66.2 in, a rise of 1.1 in, keeping 61% of its distance below.
- **Title (bold 15px `P.ink`, centered, y=22):** "Two Thirds of the Way Out, Not All of It"
- **Plot box:** `PX0=70`, `PX1=440`, `PY0=56`, `PY1=272`; both axes share the domain 60 in to 77 in so the two lines are directly comparable.
- **Grid:** `P.grid` lines at 62, 65, 68, 71, 74, 77 on both axes; 12px `P.mute` tick labels; `#ccc` L-shaped axis; 12px `P.mute` "parents' average height, in" under the plot and rotated "child's height, in" at `PX0−44`.
- **Points:** radius 1.8, `rgba(107,114,128,0.32)`, clipped to the plot box.
- **Tail bands:** drawn first, behind the grid — `rgba(213,81,129,0.10)` over the tallest tenth's parent range and `rgba(25,158,112,0.10)` over the shortest tenth's.
- **Match line:** dashed `P.orange` (dash 6/4, width 2) along `child = mid`.
- **Fitted line:** `P.green` width 2.5 through the computed least-squares fit.
- **Tail arrows:** at each band's mean parent height, a vertical arrow from the match line to the group's mean child height, `P.magenta` / `P.aqua` width 2.5 with a filled arrowhead, a radius-5 dot at the group mean, and the computed drop/rise in inches printed bold 12px beside it.
- **In-plot key** on an `rgba(255,255,255,0.88)` panel at (224, 220): the dashed orange swatch with 12px `P.orange` "children as tall as parents", the green swatch with bold 12px `P.green` "what actually happens", and 12px `P.mute` "(fitted slope 0.67)" printed from the fit.
- **Right annotation column at x=456:** bold 12px `P.magenta` "TALLEST TENTH of couples" then 12px `P.mute` "71.4 in → children 70.1 in" and "keeps 65% of its head start"; bold 12px `P.aqua` "SHORTEST TENTH" then "65.1 in → children 66.2 in" and "keeps 61% of its shortfall"; bold 12px `P.orange` "no gene reverted" then 12px `P.mute` "the extremes hold more luck". Rows spaced in pixels.
- **Caption (bold 13px `P.ink`, centered, `h−10`):** "Galton called it reversion — it is the arithmetic of extremes."

---

## Section 4 — When Does the Slump Actually Happen?

**Tags:** `rule of thumb` (blue), `the boundary` (green), `the fix` (orange)

**Bullets:**
- **What sets the size** — how much of the measure is week-to-week luck rather than lasting difference
- **A steady measure** — when almost none of the spread is luck, this quarter's leaders lead again
- **A jumpy measure** — when almost all of it is luck, a chosen tail lands right back at the average
- **Roughly proportional** — the share of the lead that survives tracks the share that was real skill
- **Longer windows help** — a full quarter carries less luck per measurement than a single week does
- **Bigger units help** — a whole region jumps around less than one store, more events per number
- **The real fix** — hold back half of the selected group and compare the halves, not before with after
- **Or pre-declare it** — name the comparison and the window before seeing which units look extreme

**Example line (italic):** With a tenth of the spread down to luck, 89% of a chosen tail's lead holds; with four fifths down to luck, only 19% does.

**Key point:** Ask what share of the measure is noise before you select on it. The one honest test of an action taken on a selected tail is a comparison group chosen by the very same rule.

**Source note (`.src`):** Illustrative Example — each point on the curve is a fresh seeded population of 4,000 units whose total spread is held constant while the luck share varies.

### Visualization — canvas `c4`, 720×300

How much of a selected tail's lead survives, plotted against how much of the spread is luck. Every point is swept live from a seeded population.

- **Sweep:** for each luck share `f` in 0, 0.1, … 1.0, hold the total spread at 1.6 and set `nsd = 1.6·√f`, `tsd = 1.6·√(1−f)`. Fresh `lcg(42)` per point; 4,000 units, `t = 8 + tsd·z()`, `r1 = t + nsd·z()`, `r2 = t + nsd·z()`. Select the top tenth on `r1` and plot `(f, lead in round 2 ÷ lead in round 1)`.
- **Computed curve:** 100%, 89%, 80%, 71%, 62%, 51%, 40%, 30%, 19%, 9%, 0% surviving as the luck share climbs from none to all.
- **Title (bold 15px `P.ink`, centered, y=22):** "The Noisier the Measure, the Harder the Fall"
- **Plot box:** `PX0=70`, `PX1=w−30`, `PY0=52`, `PY1=238`; x is the luck share 0 to 100%, y is the surviving share of the lead 0 to 100%.
- **Grid:** `P.grid` horizontal lines every 20% with 12px `P.mute` right-aligned labels; `#ccc` baseline and left axis; 12px `P.mute` x-ticks at 0/20/40/60/80/100%; x title "share of the spread that is luck (noisier →)"; rotated y title "share of the chosen tail's lead that survives".
- **Curve:** `P.magenta` width 2.5 with radius-3 dots at each swept point.
- **Marked points:** a `P.green` dot at the 10%-luck point with bold 12px `P.green` "a steady measure barely regresses" and a 12px `P.mute` computed "(89% of the lead holds)"; a `P.magenta` dot at the 80%-luck point with bold 12px "a jumpy measure regresses almost all of it" and computed "(19% holds)".
- **The page's own example:** the ad campaigns of section 1 re-derived from the same construction — luck share `1.4² ÷ (1.1² + 1.4²)` = 62%, surviving lead 32% — drawn as a radius-5 `P.orange` ring with bold 12px `P.orange` "the ad campaigns above" and 12px `P.mute` "(only 100 of them, so it scatters)".
- **Caption (bold 13px `P.ink`, centered, `h−8`):** "Reliable measures barely regress. Noisy ones snap back."

---

## Regeneration instructions

- **Template:** the card-section layout from `tutorials/llms-generative-ai/08-positional-encoding.html`. One `.card-section` per section, each holding an `<h2>` (1.3rem `#1a5276`, `border-bottom: 2px solid #2980b9`, 4px bottom padding) and a `table.layout` with `td.text-col` 50% / `td.viz-col` 50%.
- **Canvas placement:** `td.viz-col` gets `text-align: center` and the canvas `display: block; width: 100%; margin: 0 auto`. The canvas is capped at 720px, so a wide cell leaves slack — centering puts the chart in the middle of the right half rather than flush against the text.
- **Text column order:** `.tags` pill row → `<ul>` of one-line bullets each opening `<b>term</b>` → one italic `.example` line → one `.key-point` callout → optional `.src` note. No paragraph blocks, no `.philosophy` box, no data tables, no `.labels` strip, no `.subhead` line.
- **Bullets:** 6–8 per section, each ONE line that does not wrap at a 50% column (~≤100 characters), opening with a `<b>bold term</b>` followed by an em dash and the fact.
- **Page CSS:** body system-ui, white, `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with `border-bottom: 2px solid #2980b9`, 8px bottom padding. `.subtitle` `#666` 0.95rem, 32px bottom margin. `.card-section` 40px bottom margin. `table.layout` full width, border-collapse, cells vertical-align top padding 12px. `ul` 0.92rem, margin `8px 0 8px 20px`, `li` 4px bottom margin, `li b` in `#1a5276`. `.example` italic `#555` 0.9rem. `.key-point` background `#f8f9fa`, `border-left: 3px solid #e74c3c`, padding 8px 12px, 0.9rem. `.src` 0.78rem `#888`. No nav, no back/home links, no cross-page links.
- **Tag pills:** `display:inline-block`, 0.72rem, weight 600, padding 2px 10px, radius 10px; `.blue` `rgba(26,82,118,0.12)`/`#1a5276`, `.green` `rgba(39,174,96,0.15)`/`#27ae60`, `.red` `rgba(231,76,60,0.12)`/`#e74c3c`, `.orange` `rgba(230,126,34,0.15)`/`#e67e22`.
- **Canvas:** CSS `width: 100%`, `border: 1px solid #e0e0e0`, radius 4px. Intrinsic `width="720"` plus the per-chart `height` (340, 340, 340, 300). `setup(id)` caches the logical size in `dataset` on the first call (because `canvas.width` overwrites the HTML attribute), sets `style.maxWidth = 720px`, computes `scale = (cssW/720) × devicePixelRatio`, sizes the backing store to `logical × scale`, and `ctx.scale(scale, scale)` back to logical coordinates. Draws registered in `__charts`, re-run on debounced (150ms) resize.
- **Canvas font sizes:** chart title bold 15px; in-chart section header bold 12–13px; body/axis labels 12px (floor); the single big callout figure bold 19px; caption bold 13px.
- **Palette** (shared `P` object): `blue #2a78d6`, `green #008300`, `magenta #d55181`, `yellow #c98500`, `aqua #199e70`, `orange #d95926`, `violet #4a3aa7`, `ink #1a5276`, `text #2c3e50`, `mute #6b7280`, `grid #e5e9ef`. Magenta carries "the tail that was picked and then fell", aqua "the tail that was picked and then rose", orange "the selection rule", green "the honest comparison".
- **Determinism:** no `Math.random()` anywhere. Seeded Park–Miller LCG (`s = (s × 16807) % 2147483647`), seed 42, re-seeded per chart. Normal-ish noise is the sum of three draws, recentred and doubled — stated on the page as a seeded simulation. Every mean, gain, slope, count and surviving share is computed inside the draw function and printed from those variables.
- **Shared helpers:** `setup(id)`, `lcg(seed)`, `noise(rng)` returning the three-draw `z`, `fit(pts)` for the least-squares slope and means, and `campaigns()` returning the section-1 population so `c1` and `c4` cannot disagree about it.
