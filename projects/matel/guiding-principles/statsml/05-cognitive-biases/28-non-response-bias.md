# Non-Response Bias: Your Feedback Is Filtered by Who Can Afford to Give It

**Page type:** detail page — card-section template (see `05-cognitive-biases/25-mere-exposure-effect.html`)
**HTML title tag:** Non-Response Bias — Cognitive Biases

**Subtitle:** Ask for feedback and you get answers from whoever can afford to give them. The people the problem lands on hardest are usually the ones least able to say so out loud.

**Which sibling holds which half:** the construction below is split across the siblings and must not be re-merged. The reader-facing half — the unnumbered preamble, the two private numbers and the push arithmetic in words, the table of who is surveyed on which form, and the derivation table — lives in `.txt.md`. The code-level half — the constant *names* `ROUGH` / `FINE` / `SPREAD` / `COST` / `RESID` / `THRESH`, the `bell()` / `panel()` / `form()` / `feltScores()` / `cnt()` / `pct()` helpers, the `lcg()` seed and all drawing detail — lives in `.viz.md`. This combined spec keeps both.

---

## Preamble (unnumbered, before Section 1 — appears in `.txt.md` and in the html as a canvas-less `.card-section`)

**Heading (`<h2>`):** The one construction behind every figure on this page

**The one construction behind every figure on this page.** Every respondent carries two private numbers. A **private opinion** out of ten — what they actually think — spread **1.6** around **4.6** for a poorly served population and around **6.9** for a well served one. And a **personal price of speaking up** between **0.7** and **1.3**, how heavily that cost falls on this particular person. What they **write on the form** is their private opinion pushed upward by **2.8 × dependence × their personal price**, where **dependence** runs from 0 (free to leave tomorrow) to 1 (no alternative at all). Take the name off the form and only **18%** of that upward push survives. A written score **under 5** counts as a complaint, so the raw value has to land under **4.5** before rounding.

| Section | Who is being surveyed | Which form they fill in |
|---|---|---|
| 1 | two groups of 150, both privately around 4.6 — one at dependence 0.95, one at 0.10 | a named form, once |
| 2 | 400 around 4.6 at dependence 0.95, and 400 around 6.9 at dependence 0.12 | named, then unnamed, same people both times |
| 3 | one group of 400 around 4.6, surveyed six times at dependence 0.95, 0.85, 0.68, 0.48, 0.30, 0.14 | a named form every time |

What the upward push does before a single answer is counted — this is the whole mechanism:

| Situation | Upward push at an average price of 1.0 | Highest private opinion that still files |
|---|---|---|
| no alternative (0.95), named form | 2.66 = 2.8 × 0.95 × 1.0 | 1.0 to 2.6 |
| no alternative (0.95), unnamed form | 0.48 = 2.66 × 0.18 | 3.9 to 4.2 |
| three alternatives (0.10), named form | 0.28 = 2.8 × 0.10 × 1.0 | 4.1 to 4.3 |

A captive respondent has to privately rate the service a 1 or a 2 before a complaint survives the push onto the form. A free one only has to think it is below about 4.2. Both ranges are **4.5 − 2.8 × dependence × price** at the two ends of the price range, 0.7 and 1.3.

Every quantity the bullets and charts quote, and how it follows:

| Quantity | Value | How it checks out |
|---|---|---|
| Section 1 pooled size | 300 | two panels of 150 |
| Quietly unhappy, section 1 | 74 and 72, so 146 of 300 | 74 + 72 = 146; 146 ÷ 300 = 48.7% |
| Complaints filed, section 1 | 4 and 64, so 68 of 300 | 4 + 64 = 68; 68 ÷ 300 = 22.7% |
| Felt against filed, captive group | 49.3% against 2.7% | 74 ÷ 150 and 4 ÷ 150 |
| What never reached the form | 46.7 points | 70 ÷ 150 = 46.67, printed to one decimal |
| The 70 | 70 of the 74 | 74 quietly unhappy − 4 who filed |
| Named forms, section 2 | 5.3% and 5.5% | 21 ÷ 400 = 5.25 and 22 ÷ 400 |
| Unnamed forms, section 2 | 39.0% and 7.3% | 156 ÷ 400 and 29 ÷ 400 = 7.25 |
| The two gaps | 33.8 and 1.8 points | 135 ÷ 400 = 33.75 and 7 ÷ 400 = 1.75 |
| Who newly spoke up | 135 and 7 | 21 + 135 = 156 and 22 + 7 = 29 |
| What the named form tells apart | 0.3 points | 1 ÷ 400 = 0.25, the gap between 22 and 21 |
| What the unnamed form tells apart | 31.8 points | 127 ÷ 400 = 31.75, the gap between 156 and 29 |
| Private rate, section 3 | 191 of 400 — 47.8% | 191 ÷ 400 = 47.75; one panel, so one count |
| Reported across the six steps | 21, 26, 48, 79, 117, 162 | 5.3, 6.5, 12.0, 19.8, 29.3, 40.5% of 400 |
| The rise end to end | 35.3 points | 141 ÷ 400 = 35.25 |
| The multiple | 7.7× | 162 ÷ 21 |
| Still hidden at step six | 7.3 points | 29 ÷ 400 = 7.25, the gap between 191 and 162 |

Section 2's captive world and section 3's panel are the same 400 people, which is why 21 complaints and 191 privately below passing appear in both.

One class of figure cannot be redone with a pen: every **count** above. The private opinions and personal prices come from a fixed seeded stream, so 74, 72, 4, 64, 21, 156, 22, 29, 191 and the six-step ladder are read off that draw rather than derived. The push arithmetic, the two filing ranges, and every sum, difference, percentage and ratio in the table close by hand once those counts are given.

---

## Section 1 — The Survey Everyone Answered Honestly

**Tags:** `core idea` (violet), `nobody lied` (blue), `two rooms` (magenta)

**Bullets:**
- **The setup** — one rough service, one satisfaction form, and two groups of customers on it
- **The locked-in group** — this service is the only one they have, and leaving is not an option
- **The free group** — three other providers will take them next week if they decide to ask
- **What they went through** — the same service, and privately about as many were unhappy in each
- **What reached the form** — a few complaints from the locked-in group, a flood from the free
- **Nobody lied about anything** — a complaint is something you do, and doing it had a price attached
- **The people who stayed quiet** — thought it was bad and wrote a passing score, passing being cheap
- **What the aggregate said** — most customers were fine, from a room where half quietly were not

**Key point:** Not one person in either group misjudged the service. The people with somewhere else to go filed their complaints; the people without one filed a passing score. The defect belongs to whoever averaged the two groups together and read the result as the population.

**Source note (`.src`):** Illustrative Example — 300 seeded respondents in two groups of 150; every rate, count and pooled figure is counted inside the draw function.

### Visualization — canvas `c1`, 720×350

Three pairs of bars — the two groups and then everyone pooled — each pair showing how many quietly held a low opinion against how many put a complaint on the form.

- **Shared construction — implementation of the reader-facing one in the preamble above:** the
  preamble carries the premise, the push arithmetic in words and the derivation table for every figure
  on the page; this block is only the code that produces them. In code the module-level constants are
  `ROUGH`, `FINE`, `SPREAD`, `COST`, `RESID` and `THRESH`; `bell(rng, sd)` returns the noise generator
  `(rng()+rng()+rng()−1.5) × 2 × sd`; `panel(rng, nz, n, base)` builds `n` records of
  `{ felt: base + nz(), pv: 0.7 + 0.6·rng() }`; `form(p, dep, isAnon)` returns
  `round(clamp10(felt + COST × dep × pv × (isAnon ? RESID : 1)))`; `feltScores(p)` returns
  `round(clamp10(felt))`; `cnt()` and `pct()` count and rate everything below `THRESH`. Every chart
  calls `panel()` and `form()`, differing only in `dep` and `isAnon`. Seeded Park–Miller LCG, seed 42,
  one fresh stream per chart. Changing any of those six constants or the seed invalidates the
  preamble tables as well as the bullets.
- **Data:** two panels of 150 at `ROUGH`. Group one at `dep = 0.95`, group two at `dep = 0.10`,
  both on a named form. Quietly unhappy (private opinion under five): **74 of 150** and **72 of 150**,
  so **146 of 300 — 48.7%**. Complaints reaching the form: **4 of 150 (2.7%)** and **64 of 150
  (42.7%)**, so **68 of 300 — 22.7%**. `4 + 64 = 68` and `74 + 72 = 146`, both checked in the draw.
- **The 70:** group one's quietly unhappy who nonetheless wrote five or more — `74 − 4 = 70`,
  counted respondent by respondent rather than subtracted.
- **Title (bold 15px `P.ink`, centered, y=21):** "Who Felt It Against Who Said It"
- **Legend (bold 12px, y=42):** `P.mute` "quietly held a low opinion" at x=58, `P.violet`
  "put a complaint on the form" at x=282.
- **Bars:** plot box `PX=62`, width `w − 102`, `TOPY=70`, `BASEY=252`, percent axis 0–60 with
  `P.grid` gridlines at 0/15/30/45/60 and 12px `P.mute` labels. Three groups; within each, a pale
  hollow bar (`rgba(107,114,128,0.14)` stroked `P.mute`, dashed) for the felt rate on the left and a
  solid bar on the right for the reported rate — `rgba(74,58,167,0.50)` stroked `P.violet` for group
  one, `rgba(42,120,214,0.45)` stroked `P.blue` for group two, and `rgba(213,81,129,0.50)` stroked
  `P.magenta` for the pooled pair. Each bar carries its rate in bold 15px above and its raw
  "n of N" in 12px `P.mute` below the axis.
- **Group labels (bold 12px `P.ink`, two lines each):** "no alternative / 150 people",
  "three alternatives / 150 people", "the reported average / all 300".
- **The gap callout:** inside group one, a 2px `P.violet` vertical arrow from the top of the felt bar
  down to the top of the reported bar, labelled bold 12px `P.violet` with the computed difference
  ("46.7 points of it never reached the form") and 12px `P.mute` "70 of the 74 wrote a passing score".
- **Caption (bold 13px `P.violet`, centered, `h−10`):** "They saw it. They were not going to say it to your face."

---

## Section 2 — Take the Name Off the Form and Watch What Arrives

**Tags:** `the test` (aqua), `two worlds` (magenta), `falsifiable` (green)

**Bullets:**
- **Two explanations, one number** — either nothing is wrong, or plenty is and nobody will say it
- **The named form hides both** — a badly served captive room and a well served free one score alike
- **The test** — hand the same people the same questions again with no name attached to the answer
- **If nothing was wrong** — anonymity has nothing to release, and the second form barely moves
- **If it was held back** — complaints arrive in bulk from people who signed off an hour earlier
- **The gap between the forms** — is the measurement: 33.8 points in one world, 1.8 in the other
- **Cheap to run** — the same questions, the same people, one column removed from the form
- **What the gap does not tell you** — which complaint is right, only that they were being withheld

**Key point:** This is the falsifiable half of the page. If people genuinely had nothing to report, taking the name off changes almost nothing. The complaints that appear the moment nobody can be identified were there all along, and how many appear is a direct reading of how much your named survey suppresses.

**Source note (`.src`):** Illustrative Example — two seeded populations of 400 surveyed twice each; both gaps, both counts and the number who newly spoke up are computed in the draw function.

### Visualization — canvas `c2`, 720×380

Four bars in two labelled worlds — the named form and the unnamed form for each — with the gap between the two forms drawn and measured inside each world.

- **Data:** two panels of 400 sharing one seeded stream. The silenced world is at `ROUGH` with
  `dep = 0.95`; the nothing-wrong world is at `FINE` with `dep = 0.12`. Each panel is scored twice,
  once with `anonymous = false` and once `true`, with the same private opinions and the same personal
  prices both times.
- **Figures, all counted in the draw:** silenced world **21 of 400 (5.3%)** named against **156 of 400
  (39.0%)** unnamed, a gap of **33.8 points**; nothing-wrong world **22 of 400 (5.5%)** named against
  **29 of 400 (7.3%)** unnamed, a gap of **1.8 points**. The two named forms sit **0.3 points** apart,
  the two unnamed forms **31.8 points** apart.
- **Who moved:** respondents who scored five or more on the named form and under five on the unnamed
  one — **135** in the silenced world, **7** in the nothing-wrong world. Nobody moves the other way,
  so `21 + 135 = 156` and `22 + 7 = 29`; both identities are asserted in the draw and the printed
  arrow label comes from the counted value.
- **Title (bold 15px `P.ink`, centered, y=21):** "The Same People, Named Form and Unnamed Form"
- **Legend (bold 12px, y=42):** `P.mute` "name attached to the answer" at x=58, `P.aqua`
  "no name attached" at x=282.
- **Bars:** plot box `PX=62`, width `w − 210`, `TOPY=74`, `BASEY=278`, percent axis 0–45 with
  `P.grid` gridlines at 0/15/30/45. Two world groups; within each, the named bar in
  `rgba(107,114,128,0.16)` stroked `P.mute` on the left and the unnamed bar on the right —
  `rgba(25,158,112,0.45)` stroked `P.aqua` in the silenced world, `rgba(213,81,129,0.40)` stroked
  `P.magenta` in the nothing-wrong world. Each bar carries its rate in bold 17px above and
  "n of 400" in 12px `P.mute` beneath the axis.
- **World labels (bold 12px `P.ink`, two lines):** "no alternative, rough service / dep 0.95",
  "free to leave, decent service / dep 0.12".
- **Gap arrows:** within each world, a 2px vertical arrow from the top of the named bar to the top of
  the unnamed bar in the world's colour, labelled bold 13px with the computed point gap, and 12px
  `P.mute` beneath with the number who newly spoke up ("135 people who said nothing before").
- **Side panel** at `PX + PW + 26`: bold 13px `P.ink` "WHAT EACH FORM / CAN TELL APART", then bold
  17px `P.mute` "0.3 points" over 12px `P.mute` "between the two named forms", then bold 17px `P.aqua`
  "31.8 points" over "between the two unnamed forms", then bold 12px `P.aqua` "the unnamed form /
  separates the two worlds; / the named form does not."
- **Caption (bold 13px `P.aqua`, centered, `h−10`):** "The complaints did not appear. The reason to withhold them disappeared."

---

## Section 3 — The Number Moves When the Exit Door Opens

**Tags:** `not personality` (orange), `the exit door` (yellow), `read it backwards` (blue)

**Bullets:**
- **The same panel** — surveyed again and again across a year, on the same named form every time
- **What never changed** — their private opinion, the same from the first survey to the last
- **What did change** — how easily each of them could walk away from it if they decided to
- **While there was nowhere to go** — the form came back almost spotless and the service looked fine
- **As alternatives appeared** — a shorter notice, a cheaper switch, and complaints climb steeply
- **Nearly eight times the complaints** — from the same people, so silence was never a trait
- **Read the rise backwards** — a jump in complaints can mean your people gained somewhere else to go
- **Even wide open** — the form still lags private opinion, so the door never fully opens

**Key point:** Nothing about these people changed across the year except how expensive it was to leave. Treating a low complaint rate as a quality reading gets it exactly backwards: the rate is lowest precisely where the respondents have the least power, and it rises as they gain options rather than as the service gets worse.

**Source note (`.src`):** Illustrative Example — one seeded panel of 400 surveyed at six dependence levels; the flat private rate, every reported count and the ratio between the ends are computed in the draw function.

### Visualization — canvas `c3`, 720×350

A rising line of reported complaint rate across six steps of easier exit, drawn against a flat dashed line for the private opinion that never moved.

- **Data:** one panel of 400 at `ROUGH`, held fixed, scored on a named form at
  `dep = 0.95, 0.85, 0.68, 0.48, 0.30, 0.14`. Reported complaints **21, 26, 48, 79, 117, 162** —
  **5.3%, 6.5%, 12.0%, 19.8%, 29.3%, 40.5%**. The private rate is **191 of 400 — 47.8%** at every step,
  because the same private opinions are reused.
- **Read off the plotted points:** the rise end to end is **35.3 points**, the ratio **7.7×**, and the
  distance still left at step six is **47.8 − 40.5 = 7.3 points**. All three printed from variables.
- **Title (bold 15px `P.ink`, centered, y=21):** "Reported Complaints as Leaving Gets Easier"
- **Axes:** plot box `PX=62`, width `w − 102`, `TOPY=66`, `BASEY=252`, percent axis 0–55 with `P.grid`
  gridlines at 0/15/30/45 and 12px `P.mute` labels. Six evenly spaced steps; two-line 12px `P.mute`
  step labels beneath the axis: "no / alternative", "one other / provider", "two other / providers",
  "notice cut / to a month", "switching / made cheap", "free to leave / any time".
- **The flat line:** dashed 2px `P.mute` horizontal at the private rate, labelled bold 12px `P.mute`
  "191 of 400 privately below passing — unchanged all year" at the right.
- **The rising line:** 3px `P.orange` through the six reported rates, dots 5.5px — pale
  `rgba(217,89,38,0.45)` where the reported rate is under half the private rate, solid `P.orange`
  above it. Each rate printed bold 12px `P.orange` above its dot with the raw count in 12px `P.mute`
  below it.
- **Shading:** the band between the rising line and the flat private line filled
  `rgba(201,133,0,0.12)` — the complaints that exist and are not being filed, narrowing as the exit
  door opens.
- **End callout (bold 12px `P.yellow`, right-aligned above the last point):** the computed ratio,
  "7.7× the complaints from the same 400 people", with 12px `P.mute` "and 7.3 points of it still
  hidden" beneath.
- **Caption (bold 13px `P.orange`, centered, `h−10`):** "A quiet room can mean a locked door rather than a good service."

---

## Regeneration instructions

- **Template:** copied verbatim from `05-cognitive-biases/25-mere-exposure-effect.html` —
  the entire `<style>` block, the `setup(id)` canvas helper, the `lcg(seed)` PRNG, the `P` palette
  object, the `__charts` array with its debounced resize tail, the `table.layout` /
  `td.text-col` / `td.viz-col` 50/50 structure, the `.tags` pills, `.key-point` and `.src`
  conventions. Only the content differs.
- **Structure:** a canvas-less `.card-section` for the unnumbered preamble — an `<h2>`, prose
  paragraphs and three `table.layout` derivation tables, no `table.layout` text/viz row — then three
  numbered `.card-section` blocks, each an `<h2>` plus a `table.layout` row of
  `td.text-col` (50%) then `td.viz-col` (50%). The 50/50 split is fixed; a chart is shrunk through
  canvas `max-width` / `height`, never by narrowing the viz column.
- **Text column order (numbered sections only):** `.tags` pill row of three → `<ul>` of 8–9 one-line
  bullets each opening `<b>label</b>` then an em dash → one `.key-point` → the `.src` note. No
  paragraph blocks, no data tables, no nav, no back/home links, no cross-page links. The preamble is
  the one exception: it is prose and tables with no tags, key point or source note.
- **Bullet form:** roughly 90–100 characters including the bold label. A slight wrap is acceptable;
  a fact is never dropped to hit a length — it becomes another bullet instead.
- **Register: the prose states mechanisms, the charts carry the arithmetic.** No bullet opens with a
  count or a group size, no decimal percentages appear in prose, and no bullet reconciles subgroup
  counts to a total — that is the chart's job, done at render time. A figure earns a place in a
  bullet only where the figure *is* the argument: section 2 keeps the named-versus-unnamed gap
  (33.8 points against 1.8) because the contrast between those two numbers is the whole test, and
  section 3 keeps "nearly eight times the complaints" because the multiple is the finding. Elsewhere
  the fact is stated in words — "most customers were fine, out of a room where about half quietly
  were not" rather than a pair of rates. Every exact value still appears on the canvas.
- **Colour families, one per section:** section 1 violet against blue with a magenta pooled pair,
  section 2 aqua against magenta over muted named bars, section 3 orange with a yellow band.
- **Canvas:** intrinsic `width="720"` with heights 350, 380, 350. CSS `width: 100%`,
  `border: 1px solid #e0e0e0`, radius 4px. `setup(id)` caches the logical size in `dataset` on the
  first call, sets `style.maxWidth = 720px`, computes `scale = (cssW / 720) × devicePixelRatio`,
  sizes the backing store to `logical × scale` and `ctx.scale(scale, scale)` back to logical
  coordinates. Draws registered in `__charts`, re-run on a 150ms debounced resize.
- **Canvas font sizes:** chart title bold 15px; in-chart headers bold 12–13px; axis and body labels
  12px floor; the big callout figure bold 17px; caption bold 13px.
- **Palette** (shared `P`): `blue #2a78d6`, `green #008300`, `magenta #d55181`, `yellow #c98500`,
  `aqua #199e70`, `orange #d95926`, `violet #4a3aa7`, `ink #1a5276`, `text #2c3e50`, `mute #6b7280`,
  `grid #e5e9ef`.
- **One construction runs the whole page.** `ROUGH = 4.6`, `FINE = 6.9`, noise spread `1.6`,
  `COST = 2.8`, `RESID = 0.18`, complaint threshold `< 5`. Every chart calls the same `panel()` and
  `form()` helpers, so section 1's split, section 2's named/unnamed gap and section 3's rising line
  are the same model under different `dep` and `anonymous` arguments. Changing one constant moves
  every figure on the page at once, which is the point.
- **Determinism:** no `Math.random()` anywhere. Seeded Park–Miller LCG (`s = (s × 16807) %
  2147483647`), seed 42, one fresh stream per chart. Every rate, count, gap, ratio and difference
  printed on a canvas is computed inside that draw function from the plotted values and printed
  from that variable.
- **What the page claims, precisely.** The claim is *not* that respondents fail to notice harm — it
  is that they notice it and do not report it, because reporting it costs them something. Three
  behaviours are kept distinct: genuinely not noticing (a real bias, not this page), noticing and
  accepting anyway (a correct choice when the alternatives are worse, not an error), and noticing
  and staying quiet under a power gap (what this page measures). Silence is never framed as a
  reasoning mistake by the person staying silent; the defect belongs to whoever collects the
  feedback and reads the average as the population.
- **Scope boundaries.** How bad news is weighted against good belongs to
  `26-negativity-dominance`; a steady good level going unremarked belongs to
  `27-absence-blindness`. Neither is discussed here, and neither is linked.
- **Language.** Plain and physical throughout. No survey-methodology or social-science terminology
  in the prose — "they saw it, they just were not going to say it to your face" rather than a named
  effect. Organisations are `Vendor A` / `Team A` style if named at all; no real companies, no
  politics. Every constructed figure sits under an "Illustrative Example" `.src` note.
