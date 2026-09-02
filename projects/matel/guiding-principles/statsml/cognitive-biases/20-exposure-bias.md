# Exposure Bias: The Fault You Watched Fail Goes to the Front

**Page type:** detail page — card-section template (see `statistical-paradoxes/03-berksons-paradox.html`)
**HTML title tag:** Exposure Bias — Cognitive Biases

**Subtitle:** You cannot unsee a problem, so the ones that break in front of you get fixed first — and how many people they hit never enters the decision.

---

## Section 1 — Six Faults, Four Days of Fixing

**Tags:** `core idea` (violet), `effort follows sight` (blue), `same budget` (magenta)

**Bullets:**
- **The list** — six faults live in one product, each hitting a known number of people a month
- **The three you saw** — a checkout warning, a broken link, a receipt printing yesterday's date
- **The three nobody saw** — a half-saved form, a search finding nothing, a mislabelled export
- **What the week holds** — four days of fixing, exactly what the three witnessed ones cost
- **Spend it on what you saw** — 600 people a month stop being hit, 15 percent of the harm
- **Spend it on the biggest three** — 3,400 stop being hit, 85 percent, same four days
- **The gap that opens** — one ordering spares nearly six times as many people as the other
- **The largest fault of the six** — the half-saved form, 45 percent of the harm, unwitnessed

**Key point:** Nobody chose to help 600 people instead of 3,400. The three faults that happened to break in front of somebody used up the whole week, and the four days were identical either way.

**Source note (`.src`):** Illustrative Example — six constructed faults with made-up monthly reach and fix costs; the totals, both shares and the best affordable set are all computed in the draw function.

### Visualization — canvas `c1`, 720×340

Six size bars with the days-spent column beside them, then the harm each ordering actually removes.

- **Data (literal array, in the draw order shown):**

  | fault label | people hit each month | days to fix | witnessed |
  |---|---|---|---|
  | form comes back half-saved | 1800 | 2 | no |
  | search returns nothing | 1200 | 1 | no |
  | export column mislabelled | 400 | 1 | no |
  | receipt shows wrong date | 250 | 1 | yes |
  | broken link on a menu | 200 | 1 | yes |
  | checkout warning shown | 150 | 2 | yes |

- **Computed in the draw function:** `TOT = 4000` (sum of all six); `BUD = 4` days (sum of the witnessed rows' costs); `seenHarm = 600` = 15% of `TOT`; `bestHarm` found by exhaustive search over all 64 subsets costing ≤ `BUD` days, which returns the three unwitnessed faults (1200 + 1800 + 400 = 3400) at exactly 4 days = 85% of `TOT`; ratio `bestHarm / seenHarm = 5.7`. Largest single fault share = 45%.
- **Title (bold 15px `P.ink`, centered, y=22):** "Six Faults, Four Days of Fixing"
- **Column headers (bold 12px `P.ink`, y=44):** "PEOPLE HIT EACH MONTH" at `BX`; "DAYS SPENT" at the effort column.
- **Rows:** six rows, `y0 = 56`, pitch 25, bar height 16. Labels right-aligned 12px `P.mute` at `LX = 186`. Bars from `BX = 192`, width `210 × size / 1800`. Witnessed rows fill `rgba(74,58,167,0.50)` stroked 1.5px `P.violet`; unwitnessed fill `rgba(107,114,128,0.26)` stroked 1px `P.mute`. Count printed bold 12px in the row's hue just past the bar end.
- **Effort column** at `x = 470`: one 9px square per day (3px gap), filled `rgba(74,58,167,0.55)` stroked `P.violet`, drawn only for witnessed rows; unwitnessed rows get a 12px `P.mute` en dash. Then bold 12px `P.violet` "watched fail" or 12px `P.mute` "nobody saw it" at `x = 520`.
- **Divider:** 1px `P.grid` line at y=214.
- **Lower panel:** header bold 13px `P.ink` at y=232 "PEOPLE SPARED BY THE SAME FOUR DAYS". Two bars from `BX = 192`, track width 260 scaled to `TOT`, height 18, at y=244 and y=270. Row 1 "fix what you saw" fills `rgba(213,81,129,0.50)` stroked `P.magenta`; row 2 "fix the biggest three" fills `rgba(42,120,214,0.45)` stroked `P.blue`. Each labelled right-aligned 12px `P.mute` at `LX`, with bold 12px in-hue "600 — 15%" / "3,400 — 85%" past the bar, both computed.
- **Callout:** bold 19px `P.blue` "5.7×" at `x = 560, y = 262`, with 12px `P.mute` "as many people," / "same four days" beneath. A 12px `P.mute` note "all six faults were live the whole month" at `x = 40, y = 312`.
- **Caption (bold 13px `P.violet`, centered, `h−10`):** "Same four days of work. One ordering reaches 15% of the harm, the other 85%." — both percentages interpolated from the computed shares.

---

## Section 2 — Being Watched Says Nothing About How Many It Hits

**Tags:** `what sight tells you` (aqua), `size is unrelated` (blue), `the sample you got` (yellow)

**Bullets:**
- **Thirty-six faults** — all live in one product at once, hitting from 5 to 499 people a month
- **How many anyone watched fail** — nine of the thirty-six, a quarter of them
- **Which nine** — whichever ones happened while somebody was looking, and nothing else
- **Of the ten biggest, three were witnessed** — and of the ten smallest, also three
- **The two averages** — witnessed faults hit 167 people, unwitnessed 154, near enough the same
- **The biggest of all thirty-six** — 499 people, and nobody was watching when it failed
- **What being seen tells you** — that the fault is real, and nothing about how many it hits
- **What it does to you anyway** — it moves that fault to the front of a queue sorted by nothing

**Key point:** Witnessing is a coin toss with respect to size. It arrives carrying no information about how many people a fault reaches, yet it reorders the work queue as if it did.

**Source note (`.src`):** Illustrative Example — thirty-six seeded fault sizes with witnessing assigned independently of size; both averages and every count on the chart are tallied in the draw function.

### Visualization — canvas `c2`, 720×340

The thirty-six faults as dots ordered smallest to largest, witnessed ones filled, with the two averages compared underneath.

- **Data:** seeded Park–Miller LCG, seed 42. First loop: `size[i] = 5 + round(rng()² × 500)` for i = 0…35. Second loop on the same stream: `bucket[i] = floor(rng() × 6)`, and `witnessed[i] = bucket[i] >= 4` — assigned with no reference to size. This yields the sizes `[5,143,275,40,76,24,481,136,146,38,11,338,410,107,35,36,23,57,10,331,148,362,277,8,150,14,377,113,116,106,32,5,5,499,424,314]` and 9 witnessed faults.
- **Computed in the draw function:** 9 witnessed, 27 not; average witnessed size 167, average unwitnessed 154; largest fault 499, unwitnessed; largest witnessed fault 377; among the ten largest, 3 witnessed; among the ten smallest, 3 witnessed.
- **Title (bold 15px `P.ink`, centered, y=22):** "Thirty-Six Faults, Nine of Them Watched Fail" — the count is printed through a small-number word table so it stays computed while reading as prose.
- **Scatter:** `PX = 54`, `PW = w − 36 − PX`, `PTOP = 44`, `PBOT = 180`. Faults sorted ascending by size along x, evenly spaced. Y scale 0 to 520 with `P.grid` gridlines and 12px `P.mute` ticks at 0, 250, 500.
- **Dots:** radius 5. Witnessed fill `rgba(25,158,112,0.65)` stroked `P.aqua` 1.5px; unwitnessed fill `rgba(107,114,128,0.10)` stroked `P.mute` 1px. The largest fault gets an extra 2px `P.blue` ring, a short 1.5px leader line to its left, and a bold 12px `P.blue` "499 — nobody saw it" label right-aligned at the end of the leader.
- **Axis note (12px `P.mute`, y = PBOT+20):** "each dot is one fault, ordered smallest to largest"
- **Legend (y=218):** aqua filled dot + 12px `P.mute` "someone watched it fail — 9"; hollow dot + "nobody did — 27". Counts from the tally.
- **Divider:** 1px `P.grid` at y=234.
- **Average bars:** header bold 13px `P.ink` at y=252 "AVERAGE PEOPLE HIT EACH MONTH". Two bars from `BX = 196`, track 200px scaled to 220, height 18, at y=262 and y=286. Witnessed `rgba(25,158,112,0.55)` stroked `P.aqua`; unwitnessed `rgba(107,114,128,0.30)` stroked `P.mute`. Labels right-aligned 12px `P.mute` at 186; the figures printed bold 12px in-hue past each bar. The two bars come out within 7% of each other, which is the point.
- **Right figures** at `x = 468` (label text at 538): bold 19px `P.aqua` "3 of 10", 12px `P.mute` "of the biggest, watched" (y=274); bold 19px `P.yellow` "3 of 10", 12px `P.mute` "of the smallest, watched" (y=302). Both counted.
- **Caption (bold 13px `P.aqua`, centered, `h−10`):** "Witnessed and unwitnessed faults are the same size on average."

---

## Section 3 — The Big One Never Reaches the Front of the Queue

**Tags:** `the cost` (orange), `ten weeks` (magenta), `queue-jumping` (red)

**Bullets:**
- **The rule in force** — one fix a week, given to whichever fault someone just watched fail
- **Ten quiet weeks** — a different small fault surfaces each week, hitting 10 to 59 people
- **The one at the back** — a fault hitting 420 people a week, counted, never witnessed by anyone
- **Every week it loses** — the freshly seen fault jumps ahead, because you cannot unsee a fault
- **Count every encounter** — visibility order lets 6,044 of them happen across the ten weeks
- **Size order instead** — the 420 goes first, and the same ten weeks cost 2,117 encounters
- **The price of the queue-jumping** — 3,927 extra encounters, from ten choices that felt right
- **Why it was irreversible** — nothing got unfixed; the big one simply never reached the front

**Key point:** No single week's decision was wrong on its own. The large fault lost every week to a fresher one, and ten defensible choices in a row cost three times the harm the same ten fixes could have removed.

**Source note (`.src`):** Illustrative Example — ten seeded weekly fault sizes plus one constructed large fault; both cumulative curves and the difference between them are computed in the draw function.

### Visualization — canvas `c3`, 720×320

Two cumulative harm curves over ten weeks, the shaded gap between them being the cost of ordering by sight.

- **Data:** seeded LCG, seed 42. `small[k] = 10 + round(rng() × 50)` for k = 0…9, giving `[10,36,47,23,29,20,59,36,37,23]`, sum 320. One constructed large fault, `BIG = 420` people a week. All eleven faults are live from week 1.
- **Harm model, computed in the draw function:** each fault contributes `size × (number of weeks it stayed live)`. A fault fixed at the end of week k was live for weeks 1…k.
  - Visibility order: week k fixes `small[k−1]`, the one just witnessed; `BIG` is never picked. Total = 6,044 encounters.
  - Size order: week 1 fixes `BIG`, weeks 2–10 fix the nine largest smalls in descending order, leaving the smallest (10) unfixed. Total = 2,117 encounters.
  - Difference = 3,927. Week 1 is identical under both orders (740 encounters), so the curves separate only from week 2.
- **Title (bold 15px `P.ink`, centered, y=22):** "Ten Weeks, One Fix a Week"
- **Plot:** `PX = 62`, right panel width `PR = 180`, `PW = w − PR − PX`, `PTOP = 48`, `PBOT = 228`. Y scale 0 to 6500, `P.grid` gridlines and 12px `P.mute` labels at 0 / 2,000 / 4,000 / 6,000. X = weeks 1…10, 12px `P.mute` tick labels at `PBOT+16`, axis label "week" at `PBOT+34`.
- **Curves:** visibility order 2.5px `P.orange` with 3.5px dots; size order 2.5px `P.green` with 3.5px dots. The band between them filled `rgba(217,89,38,0.10)`.
- **Annotations:** bold 12px `P.green` "the 420-a-week fault fixed in week 1" placed inside the band near week 4 so it does not sit on either curve; bold 12px `P.orange` "still unfixed in week 10" above the last orange point. Endpoint totals printed bold 12px in-hue beside each final dot.
- **Right panel** at `x = w − PR + 14`: bold 13px `P.ink` "EXTRA ENCOUNTERS", then bold 19px `P.orange` "3,927"; below, 12px `P.mute` lines "visibility order — 6,044" and "size order — 2,117"; then bold 12px `P.magenta` "the biggest fault waited" / "all ten weeks"; then 12px `P.mute` "one person hit once" / "is one encounter" as the unit note. Every figure from the model above.
- **Caption (bold 13px `P.orange`, centered, `h−10`):** "Ten reasonable weekly choices, 3,927 encounters that did not have to happen."

---

## Section 4 — When Fixing the Fault You Watched Fail Is the Right Call

**Tags:** `the boundary` (green), `confirmed beats suspected` (aqua), `where it turns` (orange)

**Bullets:**
- **The honest case for acting** — a fault you watched fail is confirmed, and needs no hunch
- **The one you inferred** — bigger if it exists, but nobody counted it and it may not exist
- **The confirmed fault** — 240 people a month, certain, and the half-day you have covers it
- **The suspected fault** — 1,200 people a month if the hunch holds, nobody if it does not
- **Where the two break even** — a one-in-five chance of being right makes them worth the same
- **Below that line** — chasing the hunch helps fewer people than fixing the sure thing
- **Above it** — the hunch earns the half-day even though it may turn out to be nothing
- **What settles it cheaply** — an hour of counting turns the hunch into a number
- **The bias stated precisely** — not that you fixed what you saw, but that you priced nothing else

**Key point:** Acting on what you witnessed is not the mistake — a confirmed fault carries no risk of being imaginary, and below one in five that certainty is worth more than a bigger maybe. The mistake is leaving the rest of the list unmeasured, because then you never learn which side of the line you are on.

**Source note (`.src`):** Illustrative Example — two constructed faults, one counted and one only suspected; the crossing point is computed in the draw function as the confirmed reach divided by the suspected reach.

### Visualization — canvas `c4`, 720×330

Expected people helped by the same half-day, plotted against how likely the suspected fault is to be real, with the crossing marked.

- **Data (constructed):** `CONFIRMED = 240` people a month, certain. `SUSPECTED = 1200` people a month if the hunch is right, 0 if not.
- **Computed in the draw function:** the confirmed line is flat at `CONFIRMED`; the suspected line is `SUSPECTED × p` for p from 0 to 1; they cross at `p* = CONFIRMED / SUSPECTED = 0.20`, printed as "1 in 5".
- **Title (bold 15px `P.ink`, centered, y=22):** "Expected People Helped by the Same Half-Day"
- **Plot:** `PX = 66`, right panel width `PR = 178`, `PW = w − PR − PX`, `PTOP = 52`, `PBOT = 236`. Y scale 0 to 1300 with `P.grid` gridlines and 12px `P.mute` labels at 0 / 400 / 800 / 1,200. X scale 0 to 100% with 12px `P.mute` labels at 0, 20, 40, 60, 80, 100 and axis label "chance the suspected fault is real" at `PBOT+34`.
- **Shading:** the region left of `p*` filled `rgba(0,131,0,0.07)` with a 12px `P.green` label "what you saw" / "wins here" low in the plot; the region right of it `rgba(25,158,112,0.07)` with a 12px `P.aqua` label "the hunch wins here" on the same low baseline. Both boundaries drawn at the computed `p*`.
- **Lines:** confirmed 2.5px `P.green`, flat, labelled bold 12px `P.green` "the fault you watched fail — 240, certain" just below the line to the right of the crossing. Suspected 2.5px `P.aqua`, rising from the origin, labelled bold 12px `P.aqua` "the fault you suspect — 1,200 if real" above its right end.
- **Crossing:** 1.5px dashed `P.mute` vertical at `p*`, a 6px `P.ink` dot at the intersection, and bold 19px `P.ink` "1 in 5" near the top of the plot to the right of the dashed line with 12px `P.mute` "the two are worth the same here" beneath.
- **Right panel** at `x = w − PR + 14`: bold 13px `P.ink` "WHAT AN HOUR OF" / "COUNTING BUYS", then 12px `P.mute` lines "it replaces the chance", "with a count, and the", "line stops mattering"; then bold 12px `P.orange` "unmeasured is not" / "the same as small".
- **Caption (bold 13px `P.green`, centered, `h−10`):** "Fixing what you saw is right until the hunch clears 1 in 5." — the "1 in 5" interpolated from the computed `p*`.

---

## Regeneration instructions

- **Template:** the card-section layout from `statistical-paradoxes/03-berksons-paradox.html`, as converted in `05-clustering-illusion.html`. One `.card-section` per section, each holding an `<h2>` (1.3rem `#1a5276`, `border-bottom: 2px solid #2980b9`, 4px bottom padding) and a `table.layout` with a single row: `td.text-col` 50% / `td.viz-col` 50%.
- **No index number** on the page. `<h1>` is "Exposure Bias: The Fault You Watched Fail Goes to the Front".
- **Text column order:** `.tags` pill row → `<ul>` of one-line bullets each opening `<b>label</b>` then an em dash → one `.key-point` callout → `.src` note. No paragraph blocks, no data tables, no `.example` lines that restate a bullet.
- **Bullet form:** each is ONE line that does not wrap at 50% column width (≤95 characters including the bold label). Bullet counts follow the content: 8 / 8 / 8 / 9.
- **Page CSS:** body system-ui, white, `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with `border-bottom: 2px solid #2980b9`, 8px bottom padding. `.subtitle` `#666` 0.95rem, 32px bottom margin. `.card-section` 40px bottom margin. `table.layout` full width, border-collapse, cells vertical-align top padding 12px; `.viz-col` centered. canvas `display:block; width:100%; margin:0 auto; border:1px solid #e0e0e0; border-radius:4px`. `ul` 0.92rem margin `8px 0 8px 20px`, `li` 4px bottom margin, `li b` `#1a5276`. `.key-point` `#f8f9fa` background, `border-left:3px solid #e74c3c`, padding 8px 12px, 0.9rem. `.src` 0.78rem `#888`.
- **Tag pills:** inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px. Classes used: `.blue`, `.green`, `.red`, `.orange`, `.violet` `rgba(74,58,167,0.12)`/`#4a3aa7`, `.magenta` `rgba(213,81,129,0.14)`/`#c2426f`, `.aqua` `rgba(25,158,112,0.14)`/`#17805d`, `.yellow` `rgba(201,133,0,0.15)`/`#a06c00`.
- **No nav, no `.nav` CSS, no back/home links, no cross-page links of any kind.**
- **Hue family per section:** 1 violet/blue with a magenta losing bar; 2 aqua/yellow with a blue outlier ring; 3 orange/green with a magenta note; 4 green/aqua with an orange warning. Do not let one palette repeat across all four charts.
- **Palette** (shared `P` object): `blue #2a78d6`, `green #008300`, `magenta #d55181`, `yellow #c98500`, `aqua #199e70`, `orange #d95926`, `violet #4a3aa7`, `ink #1a5276`, `text #2c3e50`, `mute #6b7280`, `grid #e5e9ef`.
- **Canvas:** intrinsic `width="720"` plus the per-chart height. `setup(id)` caches the logical size in `dataset` on the first call, sets `style.maxWidth = 720px`, computes `scale = (cssW/720) × devicePixelRatio`, sizes the backing store to `logical × scale`, and `ctx.scale(scale, scale)` back to logical coordinates. Draws registered in `__charts`, re-run on debounced (150ms) resize.
- **Canvas fonts:** chart title bold 15px; in-chart header bold 12–13px; labels 12px floor; big callout figures bold 19px; caption bold 13px.
- **Determinism:** no `Math.random()`. Seeded Park–Miller LCG (`s = (s × 16807) % 2147483647`), seed 42, or literal arrays. Every printed figure — totals, shares, averages, counts, cumulative harms, the ratio and the crossing point — is derived in the draw function from the plotted data and printed from that variable.
- **The lead chart must show the mismatch itself**, not describe it: the size bars and the days-spent column sit side by side so the biggest bar visibly has no days against it, and the two spared-people bars put the cost of the ordering on screen as a number.
- **Corrections applied to the earlier version of this page:** the old lead chart labelled the effort column with hardcoded strings ("80% of your effort") that were never derived from any budget, and its caption asserted "80% of effort on 1.5% of the problem" — 1.5% was the sum of two listed percentages while the effort figures came from nowhere and summed to 145%. The effort model is now an explicit four-day budget with the alternative allocation solved by exhaustive search. The old third chart plotted invented accuracy curves and captioned them "4× more effective" while the legend on the same chart implied 8 ÷ 2.3 ≈ 3.5× — the two figures contradicted each other; it is replaced by cumulative harm curves whose gap is computed. The old page also claimed a 6× improvement in the prose of the same section, a third inconsistent number. The workflow-box and domain-table charts have been dropped: one drew a table on canvas, the other contained no data at all. The ML-pipeline framing is gone; the page is now analyst psychology in plain language, and the closing section states the case where acting on the witnessed fault is correct rather than treating it as always a mistake.
