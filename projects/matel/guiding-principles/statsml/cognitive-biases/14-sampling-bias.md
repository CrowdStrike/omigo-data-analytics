# Sampling Bias: The Way You Gathered Them Decided Who They Are

**Page type:** detail page — card-section template (see `statistical-paradoxes/03-berksons-paradox.html`)
**HTML title tag:** Sampling Bias — Cognitive Biases

**Subtitle:** You meant to ask the whole town. You asked whoever was standing where you handed out the forms, and they answered honestly.

---

## Section 1 — Sixteen Times the Forms, the Same Wrong Answer

**Tags:** `lead result` (magenta), `more data hurts` (violet), `false confidence` (mute→green)

Tag classes in HTML: `magenta`, `violet`, `green`.

**Bullets:**
- **The town** — 10,000 working adults, whose journeys to work truly average 28.3 minutes
- **The method** — forms left at the railway station, filled in by whoever walks past them
- **250 forms** — the answer comes out 40.3 minutes, run to run wobbling by about a minute
- **4,000 forms** — the answer comes out 40.0 minutes, and the wobble drops to a third of that
- **Sixteen times the data** — bought a run-to-run wobble four times smaller and nothing else
- **The error stayed** — about twelve minutes too high at every size, never once shrinking
- **What the wobble measures** — how much this method's answer moves, not how close it lands
- **Knocking on doors instead** — 250 forms already lands on 27.3, and every size covers the truth

**Key point:** Collecting more of the same thing makes an answer steadier, not truer. The station forms converge — they just converge on the wrong number, and every extra form makes the report sound more certain about it.

**Source note (`.src`):** Illustrative Example — a constructed town of 10,000 where the true average is known; every band and error on the chart is computed from the drawn samples.

### Visualization — canvas `c1`, 720×340

Six horizontal wobble bands — station forms and door knocks at three sample sizes — against a vertical line marking the town's true average. The station bands visibly narrow down the page while sitting entirely to the right of the truth line; the door bands narrow around it.

- **Town construction** (shared `buildTown()`, used by every chart on the page): seeded Park–Miller LCG, seed 42, 10,000 people. For each person: `train = rng() < 0.30`, `early = rng() < 0.20`, `wobble = round((rng()·2−1)·6)`, `mins = (train ? 42 : 20) + (early ? 9 : 0) + wobble`, floored at 5; `left = rng() < 0.11`. Two reach weights: `station = (train ? 0.50 : 0.06) · (early ? 1.8 : 1.0)` and `park = (train ? 0.05 : 0.45)`.
- **Computed from the town array:** true average **28.3** min; 2,950 of 10,000 travel by train (**29.5%**); train riders average **43.8**, road users **21.8**, a gap of **22.0** min; 2,012 work an early shift (**20%**); 1,133 are left-handed (**11%**).
- **Channels:** `channel(key)` builds a cumulative weight table over the town and draws by binary search, so a person's chance of appearing is proportional to that channel's reach. `channel(null)` gives the even door knock.
- **Per row:** draw `n` people on a fresh seeded stream, take the mean, and set the band half-width to `1.645 · s / √n` where `s` is the standard deviation **of that drawn sample**. Both are printed from the drawn numbers.
- **The six rows** (station seeds 101/102/103, door seeds 201/202/203):
  | method | n | answer | half-width | band | covers 28.3 |
  |---|---|---|---|---|---|
  | station forms | 250 | 40.3 | ±1.1 | 39.2 – 41.4 | no |
  | station forms | 1,000 | 39.9 | ±0.6 | 39.4 – 40.5 | no |
  | station forms | 4,000 | 40.0 | ±0.3 | 39.7 – 40.3 | no |
  | door knocks | 250 | 27.3 | ±1.1 | 26.2 – 28.4 | yes |
  | door knocks | 1,000 | 27.9 | ±0.6 | 27.4 – 28.5 | yes |
  | door knocks | 4,000 | 28.4 | ±0.3 | 28.1 – 28.7 | yes |
- **Title (bold 15px `P.ink`, centered, y=22):** "The Same Question, Asked of More and More People"
- **Axis:** minutes 25 → 43, `AX = 196` to `w − 40`, ticks and 12px `P.mute` labels every 3 minutes, axis line at `y = h − 52`, axis title 12px `P.mute` centered below: "reported average journey (minutes)".
- **Truth line:** 2px `P.green` vertical at 28.3 spanning the rows, with bold 12px `P.green` "true average 28.3" above it at `y = 42`, drawn from the town array.
- **Rows:** two groups of three on a 34px pitch, first group starting `y = 66`, a 22px gap before the second. Each band a 12px-tall rounded rectangle from `mean − hw` to `mean + hw`; station rows `rgba(213,81,129,0.45)` stroked `P.magenta` 1.5px, door rows `rgba(0,131,0,0.30)` stroked `P.green` 1.5px. A 2.5px tick in the solid hue marks the mean.
- **Row labels** (right-aligned at `AX − 12`): 12px `P.mute` "250 forms" / "1,000 forms" / "4,000 forms", and a bold 12px group heading above each trio — "FORMS LEFT AT THE STATION" in `P.magenta`, "KNOCKING ON DOORS" in `P.green`.
- **Per-row annotation** to the right of each band, bold 12px: the wobble as "±1.1" in the row's hue, and for station rows the error as "+12.0 min off" in `P.magenta`. Both from the computed numbers.
- **Shrink bracket:** a dashed 1.5px `P.violet` vertical connector on the left edge of the three station bands with bold 12px `P.violet` "band 3.9× narrower" and 12px `P.mute` "error unchanged" — the multiple computed as the first half-width divided by the last.
- **Caption (bold 13px `P.magenta`, centered, `h − 10`):** "Sixteen times the forms bought a narrower band around the same wrong number."

---

## Section 2 — Two Honest Surveys, Seventeen Minutes Apart

**Tags:** `same question` (violet), `two channels` (blue), `both wrong` (magenta)

Tag classes in HTML: `violet`, `blue`, `magenta`.

**Bullets:**
- **One question** — how long does the average person in this town take to get to work
- **Survey A** — 1,200 forms left at the railway station, answering 39.7 minutes
- **Survey B** — 1,200 forms left under windscreen wipers in car parks, answering 23.0 minutes
- **The truth** — 28.3 minutes, which neither survey came anywhere near
- **The spread** — the two answers sit nearly seventeen minutes apart on the same question
- **Nobody lied** — every form was filled in truthfully and both piles were counted correctly
- **Who filled them in** — 78 percent train riders in one pile, 5 percent in the other
- **Splitting the difference** — averaging the two lands on 31.3, still three minutes adrift

**Key point:** Two surveys of the same size, asking the same words, reached answers seventeen minutes apart. The only thing that differed was where the forms were placed — so the placement, not the question, produced the answer.

**Source note (`.src`):** Illustrative Example — the same constructed town; both survey answers and the shape of each pile are computed from the drawn samples.

### Visualization — canvas `c2`, 720×340

Three overlaid journey-time histograms on one shared axis — the whole town in flat grey, the station pile in violet, the car-park pile in blue — with each pile's average marked as a vertical line and the truth marked in green. The two coloured masses sit on opposite sides of the grey one.

- **Data:** the shared town; station pile = 1,200 draws on the station channel (seed 11), car-park pile = 1,200 draws on the park channel (seed 13).
- **Computed and printed:** station answer **39.7** (+11.4 off), car-park answer **23.0** (−5.3 off), truth **28.3**; the two are **16.7** minutes apart; midpoint **31.3**, still **+3.1** off. Train share **78%** in the station pile, **5%** in the car-park pile, **29%** in the town.
- **Bins:** width 5 minutes from 10 to 60, ten bins, each drawn as a share of its own pile so the three are comparable. Peak shares: town 0.253 in the 20–24 bin, station 0.240 in 40–44, car park 0.330 in 20–24; y scale runs 0 to 0.34.
- **Title (bold 15px `P.ink`, centered, y=22):** "Where the Forms Were Left, and Who Filled Them In"
- **Plot box:** `PX = 52` to `PR = w − 200`, baseline `y = 236`, bar tops floor at `TOP = 74`. Minutes axis 10–60, ticks and 12px `P.mute` labels every 10 minutes, axis title 12px `P.mute` centered below at `BASE + 34`: "journey to work (minutes)".
- **Bars:** town bars fill the full bin width in `rgba(107,114,128,0.22)` stroked `P.mute` 1px. The two piles draw inset — station bars occupy the left 42% of the bin in `rgba(74,58,167,0.55)` stroked `P.violet` 1.5px, car-park bars the right 42% in `rgba(42,120,214,0.50)` stroked `P.blue` 1.5px — so all three are readable at once.
- **Average lines:** truth a dashed 2px `P.green` vertical (dash 5/4) from `y = 58` to the baseline; station a solid 2px `P.violet` vertical; car park a solid 2px `P.blue` vertical. Bold 12px labels at `y = 50`, each centered over its own line and nudged apart so they never touch: "23.0" `P.blue`, "true 28.3" `P.green`, "39.7" `P.violet`.
- **Gap bracket:** a 2px `P.magenta` horizontal segment at `y = 66` between the car-park and station lines with 5px end caps, labelled bold 12px `P.magenta` "16.7 min apart" centered above — the difference of the two computed averages.
- **Right strip (left-aligned at `w − 186`):** bold 13px `P.ink` "SHARE WHO TRAVEL BY TRAIN", then three rows on a 30px pitch, each a short 60px bar plus a bold 12px percentage: station **78%** `P.violet`, town **29%** `P.mute`, car park **5%** `P.blue`. Below, bold 13px `P.ink` "AVERAGING THE TWO", bold 19px `P.magenta` "31.3" with 12px `P.mute` "still 3.1 min high" beside it.
- **Legend (12px, at `y = 292`):** three 12×12 swatches with labels "all 10,000 in town" `P.mute`, "the station pile" `P.violet`, "the car-park pile" `P.blue`.
- **Caption (bold 13px `P.violet`, centered, `h − 10`):** "One question, two placements, two answers — and the truth sits between them, unvisited."

---

## Section 3 — A Form Left at the Station Counts Train Riders

**Tags:** `the mechanism` (orange), `channel composition` (yellow), `arithmetic of the error` (red)

Tag classes in HTML: `orange`, `yellow`, `red`.

**Bullets:**
- **What the station reaches** — 58 of every 100 train riders, and only 7 of every 100 road users
- **So the pile fills up** — 77 percent train riders, against 30 percent out in the town
- **The channel's own shape** — a station is where train riders pass, so its forms are theirs
- **Nobody in the pile is odd** — train riders say 44.6 minutes on the forms, 43.8 in the town
- **Road users on the forms too** — they report 22.8 against the town's 21.8, also about right
- **Every group answers honestly** — the whole error lives in how many of each group turned up
- **The arithmetic** — 47.7 more points of train riders times a 22.0-minute longer trip is 10.5 min
- **That covers it** — 10.5 of the 11.4 minutes the pile is wrong, so 92 percent is pure mix

**Key point:** Ask what kind of person your gathering method physically passes through, and you have already predicted your answer. The forms did not distort anybody's reply — the station simply decided how many of each kind of person got one.

**Source note (`.src`):** Illustrative Example — reach rates, group shares, group averages and the mix arithmetic are all computed from the constructed town and the drawn pile.

### Visualization — canvas `c3`, 720×340

Two hundred-square blocks showing what share of each group the station reaches, above a bar pair comparing town and pile composition, and a single line of arithmetic that reconstructs the error.

- **Data:** the shared town; the pile is 1,000 draws on the station channel (seed 11).
- **Computed and printed:** average station reach among train riders **58 in 100**, among road users **7 in 100** (mean of the `station` weight within each group, times 100 and rounded); town train share **29.5%**, pile train share **77.2%**; excess **47.7** points; train trip longer by **22.0** min; product **10.5** min; the pile's total error **11.4** min; **92%**. Pile group averages **44.6** (train) and **22.8** (road) against town **43.8** and **21.8**.
- **Title (bold 15px `P.ink`, centered, y=22):** "How Many of Each Group a Station Form Reaches"
- **Reach blocks:** two 10×10 grids of 12px squares with 2px gutters, at `y = 46`, the first at `x = 52` and the second at `x = 236`. In each grid the first `round(100·reach)` squares are filled — 58 for train riders in `rgba(217,89,38,0.60)` stroked `P.orange`, 7 for road users in `rgba(201,133,0,0.55)` stroked `P.yellow`; the rest are `rgba(107,114,128,0.08)` stroked `P.grid`. Bold 12px headings above each grid in its hue: "TRAIN RIDERS" and "ROAD USERS"; bold 19px figure below each grid in its hue, "58" and "7", with 12px `P.mute` "reached per 100" beneath.
- **Composition bars** (right of the grids, left-aligned at `x = 424`): bold 13px `P.ink` "SHARE WHO TRAVEL BY TRAIN", then two rows on a 42px pitch — "out in the town" with a bar to 29% in `rgba(107,114,128,0.30)` stroked `P.mute`, "in the station pile" with a bar to 77% in `rgba(217,89,38,0.55)` stroked `P.orange`. Bar track 200px wide representing 100%, each labelled bold 12px with its own percentage. A 2px `P.orange` bracket spans the difference with bold 12px `P.orange` "47.7 points more".
- **Group-average row** at `y = 214`: bold 13px `P.ink` "WHAT EACH GROUP REPORTED", then two 12px `P.mute` lines showing pile against town — "train riders 44.6 on the forms, 43.8 in town" and "road users 22.8 on the forms, 21.8 in town" — with bold 12px `P.yellow` "each group is near enough right" beneath.
- **Arithmetic strip** at `y = 286`: bold 13px `P.ink` "WHERE THE ERROR COMES FROM", then a single 12px `P.mute` line assembled from the computed variables — "47.7 more points of train riders × 22.0 min longer per trip = 10.5 min" — the excess printed to one decimal so the product closes; rounding it to 48 would imply 10.6 — followed by bold 12px `P.orange` "10.5 of the pile's 11.4 min error — 92%".
- **Caption (bold 13px `P.orange`, centered, `h − 10`):** "The station did not change anybody's answer. It changed how many of each kind of person answered."

---

## Section 4 — Scaling the Train Riders Back Down

**Tags:** `the partial repair` (aqua), `what you measured` (yellow), `what you did not` (mute)

Tag classes in HTML: `aqua`, `yellow`, `green`.

**Bullets:**
- **The repair** — the pile has too many train riders, so count each of them for less
- **The sums** — every train rider counts 0.38 of a person, every road user 3.09
- **Why those figures** — they pull each group's share in the pile back to its share in the town
- **The result** — the answer moves from 39.7 minutes to 29.2, closing 92 percent of the error
- **What you need to do it** — the town's true travel mix, from a census or a survey you trust
- **The thing nobody recorded** — the forms never asked what shift the person works
- **Why it mattered** — early shifts add nine minutes, and the pile holds 31 percent against 20
- **Correct that too** — the answer lands on 28.3, exactly right, but only because it was recorded
- **The limit** — a repair reaches every imbalance you measured and not one you did not

**Key point:** Weighting works, and it is worth doing. It only ever fixes the imbalances you thought to record — every unrecorded difference between your pile and the town survives the repair untouched, and nothing in the numbers tells you it is still there.

**Source note (`.src`):** Illustrative Example — the same 1,000-form pile; both scaled answers are computed by re-weighting that pile to the town's known group shares.

### Visualization — canvas `c4`, 720×330

A four-step ladder of answers, each step a dot on a shared minutes axis with the truth marked, showing the answer walking most of the way in one step and the rest only when the unrecorded factor is added.

- **Data:** the shared town and the 1,000-form station pile (seed 11). Group shares from `shares()`, weighted means from `wmean()` — each person's weight is `town share of their cell ÷ pile share of their cell`, and the reported figure is the weighted mean of that pile.
- **Computed and printed:**
  | step | answer | off by |
  |---|---|---|
  | the pile as it came | 39.7 | +11.4 |
  | travel mode scaled back | 29.2 | +1.0 |
  | mode and shift both scaled | 28.3 | 0.0 |
  | the town itself | 28.3 | — |
  Scale factors **×0.38** (train riders) and **×3.09** (road users); mode alone closes **92%** of the error; early-shift share **31%** in the pile against **20%** in the town.
- **Title (bold 15px `P.ink`, centered, y=22):** "What Each Repair Recovers"
- **Axis:** minutes 27 → 41, `AX = 268` to `w − 96`, ticks and 12px `P.mute` labels every 2 minutes, axis line at `y = h − 74`, axis title 12px `P.mute` centered below: "reported average journey (minutes)".
- **Truth marker:** 2px `P.green` vertical at 28.3 spanning the rows, bold 12px `P.green` "true average 28.3" centered above at `y = 48`.
- **Steps:** four rows on a 46px pitch from `y = 76`. Each row draws a 2px lead line in the row's hue from the previous step's position to this one (the first from the axis start), a filled 7px dot at the answer, and a bold 13px figure to the right of the dot. Row hues: the raw pile `P.magenta`, mode-scaled `P.yellow`, mode-and-shift-scaled `P.aqua`, the town `P.green`.
- **Row labels** (right-aligned at `AX − 14`): 12px `P.mute` names — "the pile as it came" / "travel mode scaled back" / "mode and shift both scaled" / "the town itself" — with a second 12px `P.mute` line under the middle two giving the scale factors "train riders ×0.38, road users ×3.09" and the shift shares "31% early on the forms, 20% in town".
- **Residual marks:** for the first two rows a bold 12px label in the row's hue to the right of the axis, "+11.4 min off" and "+1.0 min off", computed as the answer minus the truth. The third row gets bold 12px `P.aqua` "on the truth".
- **Unrecorded-factor callout** at `y = h − 42`, left-aligned at `x = 52`: bold 13px `P.ink` "THE FIELD THE FORM NEVER ASKED", then 12px `P.mute` "shift pattern — early starts add 9 minutes to a journey" and bold 12px `P.yellow` "unmeasured means uncorrectable: the last 1.0 min sat here".
- **Caption (bold 13px `P.aqua`, centered, `h − 10`):** "Weighting repaired what the form recorded, and left what it never asked about."

---

## Section 5 — Left-Handers Survive the Station Forms, Commutes Do Not

**Tags:** `the boundary` (green), `when handy is fine` (aqua), `the test to apply` (orange)

Tag classes in HTML: `green`, `aqua`, `orange`.

**Bullets:**
- **The setting** — 250 forms from the station, the cheapest sample anybody could gather
- **Safe question** — what share of the town is left-handed, where the forms say 9.2 percent
- **The truth** — 11.3 percent, and an honest 250-person sample wobbles by about 3 points anyway
- **So it passed** — the error sits inside the wobble any small sample gives you for free
- **Why it passed** — left-handedness runs 11.1 percent among train riders, 11.4 among road users
- **Unsafe question** — the average journey, where the same 250 forms say 39.6 against 28.3
- **How far out** — 11.3 minutes, about ten times the wobble an honest sample that size gives
- **Why it failed** — train riders average 43.8 minutes and road users 21.8, a 22-minute split
- **The one test** — does what you are measuring differ between who you reach and who you miss
- **Cost of getting it wrong** — demanding a perfect sample for the handedness question is waste

**Key point:** A convenient sample is not a flaw in itself. It goes wrong only when the channel that gathered it sorts people by the very quantity you are measuring — so ask whether your groups would answer differently, and if they would not, take the cheap sample and move on.

**Source note (`.src`):** Illustrative Example — one seeded 250-form pile answering two questions; both errors and both honest-sample wobbles are computed from the town.

### Visualization — canvas `c5`, 720×330

Two stacked panels, one per question. Each shows the wobble an honest 250-person sample gives as a shaded band around the truth, with the station forms' answer marked — inside the band for the left-handed question, far outside it for the journey question.

- **Data:** the shared town; one 250-form station pile (seed 23). The honest wobble comes from 400 seeded 250-person door knocks (seed 29), taking the 5th and 95th values of the sorted run means for each quantity.
- **Computed and printed:**
  | question | forms say | town | off by | honest wobble | verdict |
  |---|---|---|---|---|---|
  | share left-handed | 9.2% | 11.3% | 2.1 points | ±3.2 points | inside |
  | average journey | 39.6 min | 28.3 min | 11.3 min | ±1.2 min | 10× outside |
  Group split for each quantity: left-handedness **11.1%** among train riders against **11.4%** among road users (**0.3** points); journey **43.8** min against **21.8** min (**22.0** min).
- **Title (bold 15px `P.ink`, centered, y=22):** "Two Questions, One Pile of Station Forms"
- **Panels:** two blocks, the safe one at `y = 44` and the unsafe at `y = 176`, each 104px tall with its own axis so both bands are legible. Panel heading bold 13px in the panel's hue: "WHAT SHARE OF THE TOWN IS LEFT-HANDED" in `P.green`, "HOW LONG IS THE AVERAGE JOURNEY" in `P.orange`.
- **Per panel:** axis from `AX = 92` to `w − 156` — the safe panel spans 5–18 percent, the unsafe 25–42 minutes — with ticks and 12px `P.mute` labels, and a 12px `P.mute` axis title ("share left-handed" / "average journey, minutes").
- **Wobble band:** a 26px-tall rectangle from the 5th to the 95th value of the honest runs, `rgba(0,131,0,0.16)` stroked dashed 1.5px `P.green` in the safe panel and `rgba(0,131,0,0.16)` stroked dashed 1.5px `P.green` in the unsafe one — the same colour in both, because it is the same honest reference. Bold 12px `P.green` "what an honest 250 wobbles by" above the band's left edge, and a 2px `P.green` tick at the truth with bold 12px `P.green` label ("11.3%" / "28.3 min").
- **Forms marker:** a filled 8px dot at the pile's answer in the panel's hue — `P.aqua` in the safe panel, `P.orange` in the unsafe — with a bold 13px figure beside it ("9.2%" / "39.6 min") and a 2px arrow from the truth tick to the dot labelled bold 12px in the panel hue ("2.1 points off" / "11.3 min off").
- **Right strip (left-aligned at `w − 148`) per panel:** bold 12px verdict in the panel hue — "inside the wobble" `P.aqua` / "ten times outside it" `P.orange` — then two 12px `P.mute` lines giving the group split that explains it: "train 11.1%, road 11.4%" / "train 43.8, road 21.8", and a bold 12px line naming the consequence in the panel hue: "the channel does not sort on this" / "the channel sorts exactly on this".
- **Caption (bold 13px `P.green`, centered, `h − 10`):** "A handy sample is fine until the thing you are measuring is what your channel sorted on."

---

## Regeneration instructions

- **Template:** the card-section layout from `statistical-paradoxes/03-berksons-paradox.html`, matching `cognitive-biases/05-clustering-illusion.html`. One `.card-section` per section, each holding an `<h2>` (1.3rem `#1a5276`, `border-bottom: 2px solid #2980b9`, 4px bottom padding) and a `table.layout` with `td.text-col` 50% / `td.viz-col` 50%.
- **Canvas placement:** `td.viz-col` gets `text-align: center` and the canvas `display: block; width: 100%; margin: 0 auto`. The canvas is capped at 720px, so a wide cell leaves slack.
- **Text column order:** `.tags` pill row → `<ul>` of one-line bullets each opening `<b>term</b>` → one `.key-point` callout → `.src` note. No paragraph blocks, no data tables, no `.example` boxes.
- **Bullet form:** each is ONE line that does not wrap at 50% column width (≤95 characters), opening with a `<b>bold term</b>` then an em dash and the fact. Count follows the content: 8, 8, 8, 9, 10 across the five sections.
- **Page CSS:** body system-ui, white, `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with `border-bottom: 2px solid #2980b9`, 8px bottom padding. `.subtitle` `#666` 0.95rem, 32px bottom margin. `.card-section` 40px bottom margin. `table.layout` full width, border-collapse, cells vertical-align top padding 12px. `ul` 0.92rem, margin `8px 0 8px 20px`, `li` 4px bottom margin, `li b` in `#1a5276`. `.key-point` background `#f8f9fa`, `border-left: 3px solid #e74c3c`, padding 8px 12px, 0.9rem. `.src` 0.78rem `#888`. No nav, no back/home links, no cross-page links, no `.nav` CSS.
- **Tag pills:** `display:inline-block`, 0.72rem, weight 600, padding 2px 10px, radius 10px. Classes used: `.blue`, `.green`, `.red`, `.orange`, `.violet` `rgba(74,58,167,0.12)`/`#4a3aa7`, `.magenta` `rgba(213,81,129,0.14)`/`#c2426f`, `.aqua` `rgba(25,158,112,0.14)`/`#17805d`, `.yellow` `rgba(201,133,0,0.15)`/`#a06c00`.
- **Colour rotation, one hue family per section:** section 1 magenta bands against green truth with a violet shrink bracket; section 2 violet and blue histograms against a green truth line; section 3 orange and yellow reach grids; section 4 a magenta→yellow→aqua→green ladder; section 5 green reference bands with aqua and orange markers.
- **Canvas:** CSS `width: 100%`, `border: 1px solid #e0e0e0`, radius 4px. Intrinsic `width="720"` plus the per-chart height (340, 340, 340, 330, 330). `setup(id)` caches the logical size in `dataset` on the first call, sets `style.maxWidth = 720px`, computes `scale = (cssW/720) × devicePixelRatio`, sizes the backing store to `logical × scale`, and `ctx.scale(scale, scale)` back to logical coordinates. Draws registered in `__charts`, re-run on debounced (150ms) resize.
- **Canvas font sizes:** chart title bold 15px; in-chart header bold 12–13px; body/axis labels 12px floor; the big callout figure bold 19px; caption bold 13px.
- **Palette** (shared `P` object): `blue #2a78d6`, `green #008300`, `magenta #d55181`, `yellow #c98500`, `aqua #199e70`, `orange #d95926`, `violet #4a3aa7`, `ink #1a5276`, `text #2c3e50`, `mute #6b7280`, `grid #e5e9ef`.
- **One shared town, five charts.** `buildTown()` is called once and cached; every chart draws from that same 10,000-person array via `channel(key)`. This is what makes the figures reconcile across sections — the 28.3-minute truth, the 29% train share and the 22.0-minute group gap are the same numbers on every chart because they come from one array.
- **Determinism:** no `Math.random()`. Seeded Park–Miller LCG (`s = (s × 16807) % 2147483647`), seed 42 for the town and distinct per-chart seeds for the draws. Every printed figure — sample means, band half-widths, group shares, reach counts, scale factors, the mix product and its percentage — is computed inside the draw function from the drawn data and printed from that variable.
- **The lead chart is the counterintuitive result, deliberately.** Intuition says more data helps, so the page opens on six bands where the station rows narrow by 3.9× across a 16× increase in data while staying about twelve minutes wrong. A chart that merely showed a biased sample being wrong would not have made the point that the wrongness is immune to sample size.
- **Bands are honest sampling wobble, not asserted error bars.** Each half-width is `1.645 · s / √n` computed from the drawn sample, checked against a 400-run simulation of the same channel: at n=250 the simulated 5th–95th range was 38.80–40.88 against the analytic 38.75–40.95, and at n=4000 39.60–40.13 against 39.57–40.13. The band describes run-to-run movement only, which is exactly why it cannot see the bias.
- **Section 5's boundary case is verified in both directions, not asserted.** The left-handed answer is 2.1 points off against an honest wobble of ±3.2 points, so it genuinely passes; the journey answer is 11.3 minutes off against ±1.1, so it genuinely fails. The construction makes handedness independent of travel mode (11.1% against 11.4%) and journey time strongly dependent on it (43.8 against 21.8), which is why one question survives the channel and the other does not.
- **Corrections applied to the earlier version of this page:** the old page was a four-topic list; this file now covers sampling bias alone. Its one sampling-bias figure — "10M biased samples are still wrong" — was asserted with no construction behind it, and is now the computed lead chart. Its Venn-diagram visualization showed a sample circle overlapping a population circle with no quantity attached to it, illustrating nothing measurable; it is replaced by charts whose every mark is a computed number. A named 1936 election survey has been dropped in favour of a constructed town where the true answer is known, so both methods' errors are measurable rather than described.
