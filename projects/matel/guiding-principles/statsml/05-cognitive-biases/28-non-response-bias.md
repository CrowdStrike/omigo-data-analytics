# Non-Response Bias: Your Feedback Is Filtered by Who Can Afford to Give It

**Page type:** detail page — card-section template (see `05-cognitive-biases/25-mere-exposure-effect.html`)
**HTML title tag:** Non-Response Bias — Cognitive Biases

**Subtitle:** Ask for feedback and you get answers from whoever can afford to give them. The people the problem lands on hardest are usually the ones least able to say so out loud.

---

## Section 1 — The Survey Everyone Answered Honestly

**Tags:** `core idea` (violet), `nobody lied` (blue), `two rooms` (magenta)

**Bullets:**
- **The setup** — one rough service, one satisfaction form, and two groups of customers on it
- **The locked-in group** — this service is the only one they have, and leaving is not an option
- **The free group** — three other providers will take them next week if they decide to ask
- **What they went through** — the same service, and privately about as many were unhappy in each
- **What reached the form** — a handful of complaints from the locked-in group, a flood from the free one
- **Nobody lied about anything** — a complaint is something you do, and doing it had a price attached
- **The people who stayed quiet** — thought it was bad and wrote a passing score, because passing is cheap
- **What the aggregate said** — most customers were fine, out of a room where about half quietly were not

**Key point:** Not one person in either group misjudged the service. The people with somewhere else to go filed their complaints; the people without one filed a passing score. The defect belongs to whoever averaged the two groups together and read the result as the population.

**Source note (`.src`):** Illustrative Example — 300 seeded respondents in two groups of 150; every rate, count and pooled figure is counted inside the draw function.

### Visualization — canvas `c1`, 720×350

Three pairs of bars — the two groups and then everyone pooled — each pair showing how many quietly held a low opinion against how many put a complaint on the form.

- **Shared construction (used by every chart on the page):** each respondent carries a private
  opinion `felt = base + noise` and a personal price of speaking up `pv = 0.7 + 0.6·rng()`. What they
  write on a form is `round(clamp(felt + COST × dep × pv × (anonymous ? RESID : 1), 0, 10))`, where
  `COST = 2.8` is how far a complaint gets softened when it is fully attributable, `dep` is how much
  the respondent depends on the thing being rated (0 = free to leave, 1 = no alternative at all),
  and `RESID = 0.18` is the little that survives once the name comes off the form. Noise is
  `(rng()+rng()+rng()−1.5) × 2 × 1.6`. `ROUGH = 4.6` is the private opinion of a poorly served
  population, `FINE = 6.9` that of a well served one. A score under **5** counts as a complaint.
  Seeded Park–Miller LCG, seed 42, one fresh stream per chart.
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
- **The named form cannot separate them** — a badly served captive room and a well served free one score alike
- **The test** — hand the same people the same questions again with no name attached to the answer
- **If nothing was wrong** — anonymity has nothing to release, and the second form barely moves
- **If it was being held back** — the complaints arrive in bulk from people who signed off an hour earlier
- **The gap between the two forms** — is the measurement: 33.8 points in one world against 1.8 in the other
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
- **What never changed** — their private opinion of the service, the same from the first survey to the last
- **What did change** — how easily each of them could walk away from it if they decided to
- **While there was nowhere to go** — the form came back almost spotless and the service looked fine
- **As alternatives appeared** — a shorter notice period, a cheaper switch, and the complaints climb steeply
- **Nearly eight times the complaints** — from the very same people, so silence was never a personality trait
- **Read the rise backwards** — a jump in complaints can mean your people gained somewhere else to go
- **Even wide open** — the form still lags what they privately think, so the door never opens all the way

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
- **Structure:** three `.card-section` blocks, each an `<h2>` plus a `table.layout` row of
  `td.text-col` (50%) then `td.viz-col` (50%). The 50/50 split is fixed; a chart is shrunk through
  canvas `max-width` / `height`, never by narrowing the viz column.
- **Text column order:** `.tags` pill row of three → `<ul>` of 8–9 one-line bullets each opening
  `<b>label</b>` then an em dash → one `.key-point` → the `.src` note. No paragraph blocks, no data
  tables, no nav, no back/home links, no cross-page links.
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
