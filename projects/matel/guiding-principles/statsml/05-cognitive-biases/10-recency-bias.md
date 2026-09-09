# Recency Bias: The Last Thing That Happened Becomes the Whole Story

**Page type:** detail page — card-section template (see `statistical-paradoxes/03-berksons-paradox.html`)
**HTML title tag:** Recency Bias — Cognitive Biases

**Subtitle:** A restaurant you loved for three years serves you one cold meal, and three years quietly stop counting.

---

## Section 1 — One Cold Meal Against Three Good Years

**Tags:** `core idea` (blue), `the newest entry wins` (violet), `history ignored` (magenta)

**Bullets:**
- **The place** — a neighbourhood restaurant visited forty times over three years, rated each time
- **The record** — thirty-four of those visits earned four stars or five, and not one was truly bad
- **Last Friday** — cold food, slow service, one star, and the place "has gone downhill"
- **What the visit is worth** — one entry in forty, a fortieth of everything the diner knows
- **What it actually moves** — the honest average slips eight hundredths, still above four stars
- **What it moved instead** — the whole verdict, because the newest entry reads as current truth
- **The tell** — the diner can describe Friday in detail and cannot name a single earlier visit

**Key point:** The most recent observation does not carry more information than the ones before it — it carries the same single share. What it has is position, and position is not evidence.

**Source note (`.src`):** Illustrative Example — forty seeded visit ratings; the averages and counts are computed in the draw function.

### Visualization — canvas `c1`, 720×330

Forty rating bars in visit order, the final one-star bar in magenta, with the honest average drawn straight through it and a side panel weighing the last visit against the record.

- **Data:** seeded Park–Miller LCG, seed 42. For visits 1–39, draw `u = rng()` and rate 5 if `u < 0.45`, 4 if `u < 0.85`, else 3. Visit 40 is set to 1. The sequence is
  `5,4,4,5,5,5,3,4,4,5,5,4,3,4,5,5,5,5,5,4,4,4,4,5,4,5,3,4,4,4,5,5,5,3,3,4,4,4,4,1`.
- **Computed in the draw function:** average of all forty = **4.20**; average of the first thirty-nine = **4.28**; the drop caused by the last visit = **0.08**; four-or-five count = **34**; the last visit's share of the record = **1/40 = 2.5%**.
- **Title (bold 15px `P.ink`, centered, y=22):** "Forty Visits to One Restaurant"
- **Plot box:** `PX=46`, `PY=52`, plot width `= 0.62w − PX`, bar area height `= h − PY − 74`. Bars scaled 0 → 5 stars.
- **Bars:** forty columns, gap 2px. Visits 1–39 filled `rgba(74,58,167,0.45)` stroked `P.violet` 1px. Visit 40 filled `rgba(213,81,129,0.65)` stroked `P.magenta` 2px, so the one bad bar is the only magenta thing on the chart.
- **Star gridlines:** horizontal `P.grid` 1px at 1–5 stars, labels 12px `P.mute` right-aligned at `PX − 8` reading "1★" … "5★".
- **Honest average line:** solid `P.aqua` 2px across the full plot at the all-forty average, labelled bold 12px `P.aqua` "average of all forty: 4.20" just above its right end.
- **Last-visit callout:** bold 12px `P.magenta` "one star" above the final bar, and 12px `P.mute` "last Friday" below it on the axis.
- **Side panel** at `0.66w`: bold 13px `P.ink` "WHAT THE LAST VISIT IS WORTH", then bold 19px `P.magenta` "2.5%" with 12px `P.mute` "of the record" and "one visit in 40"; bold 13px `P.ink` "HOW FAR IT MOVES THE AVERAGE", then bold 19px `P.aqua` "0.08" with 12px `P.mute` "of a star" and "4.28 down to 4.20"; then bold 12px `P.violet` "34 of 40 visits were four stars or five", printed from the tally.
- **Axis notes (12px `P.mute`, under the bars):** "oldest visit at the left" on the left, "last Friday" on the right.
- **Caption (bold 13px `P.magenta`, centered, `h−10`):** "The newest visit gets one fortieth of the evidence and all of the verdict."

---

## Section 2 — Same Forty Ratings, Three Different Verdicts

**Tags:** `weight by age` (violet), `nothing new learned` (yellow), `position not evidence` (magenta)

**Bullets:**
- **Same record, three readings** — the forty ratings never change, only how much each visit counts
- **Every visit counts once** — the newest is one fortieth of the answer, and the record reads 4.20
- **Fade with age** — each visit worth one over its age in visits, and the same record reads 3.39
- **Halve every step back** — the newest counts as much as everything else, and it reads 2.48
- **What halving does** — the newest three visits carry seven eighths of the verdict
- **The other side of it** — the thirty-seven visits before them carry one eighth between them
- **Nothing new was learned** — no rating changed; the whole swing came from position in time alone
- **How people actually weigh** — much closer to halving than counting once, and never on purpose

**Key point:** Three verdicts from one unchanged record — 4.20, 3.39 and 2.48. The gap between them is not a fact about the restaurant. It is a choice about how fast the past is allowed to fade.

**Source note (`.src`):** Illustrative Example — the same forty seeded ratings, re-weighted three ways in the draw function.

### Visualization — canvas `c2`, 720×340

Three weight profiles drawn as stacked strips over visit position, each with the verdict it produces printed beside it, so the reader sees weight collapse onto the right-hand end as the verdict falls.

- **Data:** the identical forty-rating array from section 1. Three weight vectors over visit index `i` (0 oldest, 39 newest), with `age = 40 − i`:
  - **equal:** `w = 1`
  - **fade with age:** `w = 1 / age`
  - **halve each step back:** `w = 0.5^(39 − i)`
- **Computed in the draw function:** each verdict is the weighted average of the ratings, and each share is that weight divided by the vector's total:

  | reading | verdict | newest visit's share | newest three | remaining thirty-seven |
  |---|---|---|---|---|
  | every visit counts once | 4.20 | 3% | 8% | 93% |
  | fade with age | 3.39 | 23% | 43% | 57% |
  | halve each step back | 2.48 | 50% | 88% | 12% |

  Shares are printed rounded to whole percent (the equal row's 3% share and 8% newest-three are 2.5% and 7.5% before rounding), and the text never states them more precisely than the chart prints.
- **Title (bold 15px `P.ink`, centered, y=22):** "Same Forty Ratings, Three Ways to Weigh Them"
- **Three rows** on an 88px pitch starting at `y=62`. Each row: a row label bold 12px `P.ink` left at `LX=30` ("COUNTS ONCE EACH", "FADES WITH AGE", "HALVES EACH STEP BACK"), then a strip of forty cells across `x = 208 … w − 132`, height 28.
- **Strip cells:** each cell's height is proportional to its share of that row's total weight, scaled so the row's largest cell fills the 28px strip, drawn bottom-aligned over a faint `rgba(107,114,128,0.10)` track so the near-zero cells still read as forty visits. Row hues: equal `rgba(25,158,112,0.50)` stroked `P.aqua`; fade `rgba(201,133,0,0.50)` stroked `P.yellow`; halve `rgba(213,81,129,0.55)` stroked `P.magenta`.
- **Verdict figures:** at the right of each row, bold 19px in that row's hue printing the verdict (4.20 / 3.39 / 2.48), with 12px `P.mute` "stars" beneath.
- **Newest-share note:** under each strip, 12px `P.mute` "newest three visits: 8% of the verdict" (then 43%, then 88%), each summed from the weight vector.
- **Foot notes (12px `P.mute`):** "bar height is how much that visit counts" under the labels, "oldest visit at the left, last Friday at the right" under the strips.
- **Caption (bold 13px `P.magenta`, centered, `h−10`):** "The record is identical in all three rows. Only the fading rate changed the answer."

---

## Section 3 — The Last Sale Becomes the Price

**Tags:** `where it bites` (orange), `one quote sets the market` (yellow), `log ignored` (blue)

**Bullets:**
- **The log** — thirty-one private sales of the same used bicycle model over one year
- **The spread** — the first thirty ran from $265 to $355, averaging $305
- **The newest sale** — $205, a rushed weekend clearance by someone moving house
- **What it becomes** — "the bike is worth about two hundred", quoted by buyer and seller alike
- **How far that sits** — sixty dollars below the cheapest of the thirty sales before it
- **What it does to the average** — pulls it down three dollars, from $305 to $302
- **The gap it opens** — the quoted price lands a third below the average of the log it came from
- **Why it sticks** — a price you just saw feels like a fact, thirty older prices feel like history

**Key point:** One clearance sale cannot move a year of prices by more than a few dollars, and it does not. It moves the number people say out loud, which is a different thing entirely.

**Source note (`.src`):** Illustrative Example — thirty-one seeded sale prices; the range, both averages and the gap are computed in the draw function.

### Visualization — canvas `c3`, 720×320

Thirty-one sale prices as dots along the year, the last one dropped far below the pack, with the average line barely moving and the "market price" arrow snapping down to the last dot.

- **Data:** seeded LCG, seed 42, with a near-bell draw `g()` built as `(u1+u2+u3+u4 − 2) / sqrt(1/3)`. For sales 1–30, `price = round((310 + 28·g()) / 5) · 5`, giving
  `285,315,295,305,280,320,310,270,345,325,290,265,265,340,305,325,320,265,300,310,340,325,355,340,280,300,290,305,275,315`. Sale 31 is set to `205`.
- **Computed in the draw function:** lowest of the first thirty = **265**; highest = **355**; average of the first thirty = **305** (305.33, printed to the dollar); average of all thirty-one = **302** (302.10); the average's move = **$3**; the gap between the all-thirty-one average and the last sale = **$97**, which is **32%** below that average; sales at or below $205 before the last one = **0**.
- **Title (bold 15px `P.ink`, centered, y=22):** "Thirty-One Sales of the Same Used Bicycle"
- **Axes:** `PX=64`, `PY=48`, right margin 150, bottom margin 52. y from $180 to $380 with `P.grid` lines every $50, labels 12px `P.mute` "$200" … "$350". x is sale index 1–31.
- **Band:** a filled rectangle `rgba(201,133,0,0.10)` spanning the first thirty sales' low-to-high range ($265–$355), stroked dashed 1px `P.yellow` (dash 4/3), with 12px `P.yellow` "$265 – $355, thirty sales" inside its upper edge.
- **Dots:** sales 1–30 radius 4, `rgba(217,89,38,0.55)` stroked `P.orange` 1px. Sale 31 radius 7, `rgba(213,81,129,0.75)` stroked `P.magenta` 2px.
- **Average line:** solid 2px `P.aqua` across the plot at the all-thirty-one average, labelled bold 12px `P.aqua` "average of the log: $302" above its left end.
- **The snap:** a dashed 2px `P.magenta` vertical arrow (dash 5/4) from the average line down to the last dot, with bold 12px `P.magenta` "\"the price is $205\"" beside the dot and 12px `P.mute` "$97 below the average — 32%" under it.
- **Side panel** at `w − 144`: bold 13px `P.ink` "WHAT IT MOVED", then bold 19px `P.aqua` "$3" with 12px `P.mute` "off the average" and "$305 → $302"; bold 13px `P.ink` "WHAT PEOPLE QUOTE", then bold 19px `P.magenta` "$205" with 12px `P.mute` "$60 under the cheapest of the thirty".
- **Caption (bold 13px `P.orange`, centered, `h−10`):** "The average moved three dollars. The asking price moved a hundred."

---

## Section 4 — Did the Thing Change, or Was It Just a Bad Stretch

**Tags:** `the boundary` (green), `sometimes recency is right` (aqua), `the real skill` (red)

**Bullets:**
- **Recency is often right** — when the process truly changed, old figures describe a dead world
- **The setting** — a year of weekly figures around forty, then eight recent weeks that look low
- **The bad stretch** — those eight averaged 38.0, and five earlier stretches were as low
- **What followed it** — the next eight averaged 39.9, back where the year had been sitting
- **Who forecast better** — the whole year was off by 0.5, the recent eight by 1.9, so history wins
- **The real change** — a second version averages 29.5, below every eight-week stretch in the year
- **How clearly** — seven of its eight weeks fall under the worst single week of the whole year
- **What followed there** — the next eight stayed near thirty, and the year average was off by 9.2
- **The separating question** — has the recent stretch left the range the history already covered
- **The actual skill** — not distrusting recent data, but checking whether it fits the history

**Key point:** In the first case the history forecast better and reacting was the error; in the second the history was misleading and reacting was correct. Recent data is not automatically noise and not automatically news — the test is whether it has left the range the past already produced.

**Source note (`.src`):** Illustrative Example — one seeded year of weekly figures with two constructed continuations; every average, count and forecast error is computed in the draw function.

### Visualization — canvas `c4`, 720×340

One year of weekly figures on the left, then the same history continued two ways side by side — an ordinary dip and a real drop — each showing whether it fell outside the band of eight-week stretches the year already produced, and which forecast came closer.

- **History:** seeded LCG, seed 42, near-bell `g()` as in section 3. Fifty-two weeks, `round(40 + 4·g())`:
  `37,40,38,39,36,41,40,34,45,42,37,34,33,44,39,42,41,34,38,40,44,42,47,45,36,38,37,40,35,41,39,36,37,36,43,36,41,40,37,37,35,47,38,47,42,45,34,38,38,44,40,41`.
- **Computed from the history:** year average = **39.4**; worst single week = **33**; best = 47; there are **45** eight-week stretches inside the year, and their averages run from **37.6** to **41.4** — this is the band the chart draws.
- **Case A — ordinary dip.** Seed 4, same generator, level 40. Recent eight weeks `36,39,40,32,37,43,36,41`, average **38.0**. Prior stretches this low or lower: **5 of 45**. Weeks below the year's worst week: **1 of 8**. The next eight from the same stream, `37,40,46,40,39,41,43,33`, average **39.9**. Forecasting that continuation from the whole year is off by **0.5**; from the recent eight, off by **1.9**. History wins.
- **Case B — the level really moved.** Seed 14, level 30. Recent eight weeks `29,28,25,32,29,28,28,37`, average **29.5**. Prior stretches this low or lower: **0 of 45**. Weeks below the year's worst week: **7 of 8**. The next eight, `32,33,29,26,32,27,37,26`, average **30.3**. Forecasting from the whole year is off by **9.2**; from the recent eight, off by **0.8**. Recency wins.
- **Title (bold 15px `P.ink`, centered, y=22):** "Same History, Two Recent Stretches"
- **Layout:** one shared y scale from 20 to 50 with `P.grid` lines every 10, labels 12px `P.mute`. The history occupies `x = 56 … 0.52w`; the two continuations sit in two panels to its right, each `0.19w` wide with a 16px gutter, drawn on the same y scale and separated from the history by a 1px `P.grid` vertical rule.
- **Band:** across the full width, a `rgba(0,131,0,0.08)` rectangle spanning 37.6 to 41.4, its top and bottom edges dashed 1px `P.green` (dash 4/3), labelled 12px `P.green` "every eight-week stretch of the year fell in here (37.6–41.4)" above its top edge on the history side.
- **History series:** header bold 12px `P.ink` "ONE YEAR OF WEEKLY FIGURES"; a 1.5px line `rgba(107,114,128,0.75)` with radius-2 `P.mute` dots, plus the year average as a solid 1.5px `P.mute` line. Beneath: 12px `P.mute` "average 39.4, worst single week 33" and "the eight-week band above is what this year already did".
- **Panel A:** eight points, line 2px `P.aqua`, dots radius 3.5 `rgba(25,158,112,0.60)`; its eight-week average as a short solid 2px `P.aqua` bar. Header bold 12px `P.aqua` "AN ORDINARY DIP"; beneath it 12px `P.mute` "eight weeks, average 38.0 — inside the band", "5 of 45 year stretches were this low", "1 of 8 weeks under the year's worst week", then bold 12px `P.aqua` "the year forecast better" and 12px `P.mute` "next eight off by 0.5 from the year, 1.9 from these eight".
- **Panel B:** eight points, line 2px `P.magenta`, dots radius 3.5 `rgba(213,81,129,0.60)`; its average as a short solid 2px `P.magenta` bar. Header bold 12px `P.magenta` "THE LEVEL MOVED"; beneath it 12px `P.mute` "eight weeks, average 29.5 — below the band", "0 of 45 year stretches were this low", "7 of 8 weeks under the year's worst week", then bold 12px `P.magenta` "these eight forecast better" and 12px `P.mute` "next eight off by 9.2 from the year, 0.8 from these eight".
- **Every printed figure** in both panels — the averages, the stretch counts, the under-worst-week counts, and the two forecast errors — is computed from the plotted arrays in the draw function, never written as a literal.
- **Caption (bold 13px `P.green`, centered, `h−10`):** "Recency is wrong inside the band and right outside it. That is the whole test."

---

## Regeneration instructions

- **Template:** the card-section layout from `statistical-paradoxes/03-berksons-paradox.html`, matching the approved conversion in `05-clustering-illusion.html`. One `.card-section` per section, each holding an `<h2>` (1.3rem `#1a5276`, `border-bottom: 2px solid #2980b9`, 4px bottom padding) and a `table.layout` with `td.text-col` 50% / `td.viz-col` 50%.
- **Canvas placement:** `td.viz-col` gets `text-align: center`, the canvas `display: block; width: 100%; margin: 0 auto`, capped at 720px by `style.maxWidth` so a wide cell leaves slack and the chart sits centred in its half.
- **Text column order:** `.tags` pill row → `<ul>` of one-line bullets each opening `<b>term</b>` → one `.key-point` callout → `.src` note. No paragraph blocks, no data tables, no `.example` line restating a bullet.
- **Bullet form:** one line that does not wrap at 50% column width (≤95 characters including the bold label), a `<b>bold label</b>` then an em dash then the fact. Counts follow the content: seven, eight, eight, ten.
- **Section titles name the content**, never a role. No index number appears anywhere on the page.
- **The last section must not teach that recency is always a bias.** Both directions are demonstrated with computed numbers: a dip inside the band where the history forecasts better, and a real level change outside the band where the recent data forecasts better.
- **Page CSS:** body system-ui, white, `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with `border-bottom: 2px solid #2980b9`, 8px bottom padding. `.subtitle` `#666` 0.95rem, 32px bottom margin. `.card-section` 40px bottom margin. `table.layout` full width, border-collapse, cells vertical-align top padding 12px. `ul` 0.92rem, margin `8px 0 8px 20px`, `li` 4px bottom margin, `li b` in `#1a5276`. `.key-point` background `#f8f9fa`, `border-left: 3px solid #e74c3c`, padding 8px 12px, 0.9rem. `.src` 0.78rem `#888`. No nav, no back/home links, no cross-page links, no `.nav` CSS.
- **Tag pills:** `display:inline-block`, 0.72rem, weight 600, padding 2px 10px, radius 10px. Classes used: `.blue`, `.green`, `.red`, `.orange`, `.violet` `rgba(74,58,167,0.12)`/`#4a3aa7`, `.magenta` `rgba(213,81,129,0.14)`/`#c2426f`, `.aqua` `rgba(25,158,112,0.14)`/`#17805d`, `.yellow` `rgba(201,133,0,0.15)`/`#a06a00`.
- **Hue family per section:** 1 violet bars with a magenta last entry and an aqua average; 2 aqua/yellow/magenta strips; 3 orange dots in a yellow band with a magenta snap; 4 grey history in a green band with aqua and magenta panels. No section repeats another's dominant fill.
- **Canvas:** intrinsic `width="720"`, heights 330 / 340 / 320 / 340. `setup(id)` caches the logical size in `dataset` on first call, sets `style.maxWidth`, computes `scale = (cssW/720) × devicePixelRatio`, sizes the backing store to `logical × scale`, and `ctx.scale(scale, scale)` back to logical coordinates. Draws registered in `__charts`, re-run on debounced (150ms) resize.
- **Canvas font sizes:** chart title bold 15px, in-chart header bold 12–13px, labels 12px floor, big callout figures bold 19px, caption bold 13px.
- **Palette** (shared `P` object): `blue #2a78d6`, `green #008300`, `magenta #d55181`, `yellow #c98500`, `aqua #199e70`, `orange #d95926`, `violet #4a3aa7`, `ink #1a5276`, `text #2c3e50`, `mute #6b7280`, `grid #e5e9ef`.
- **Determinism:** no `Math.random()`. Seeded Park–Miller LCG (`s = (s × 16807) % 2147483647`), seed 42 for the ratings, the price log and the year of weekly figures; seeds 4 and 14 for the two continuations in section 4. Near-bell draws come from four uniforms, `(u1+u2+u3+u4 − 2) / sqrt(1/3)`.
- **Every printed figure is computed in the draw function** from the plotted arrays — the averages, the weighted verdicts, the weight shares, the price gap, the eight-week band, the stretch counts and the forecast errors. Prose, bullets and chart labels reconcile to the digit at the rounding the chart prints.
- **Corrections applied to the earlier version of this page:** it asserted "ACME drops 3% on 200 shares" as "$80B lost" with no arithmetic connecting the two, and captioned a hardcoded 30-point price series with figures never derived from it. Its "perceived importance ≈ 1/(days since event)" chart printed "10–90× more weight than it deserves" beside a hand-drawn curve — a label with no computation behind it; the weight comparison is now three real weight vectors applied to one real record, with every share summed from the vector. Its control-chart section asserted "2σ = 6, 3σ = 9" for a deterministic sine series whose spread was never measured; it has been replaced by the band of eight-week stretches the history actually produced. Its "S&P positive 73% of years" claim was an unsourced real-world figure and is gone. The old page also framed recency as always an error; the closing section now shows a computed case where reacting to the recent data is correct.
