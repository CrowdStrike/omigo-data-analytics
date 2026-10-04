# Absence Blindness: The Baseline Goes Invisible

**Page type:** detail page — card-section template (see `05-cognitive-biases/25-mere-exposure-effect.html`)
**HTML title tag:** Absence Blindness — Cognitive Biases

**Subtitle:** The thing that always works stops being noticed within a few weeks, and the work that keeps it working never shows up in any number anyone reports.

**Which sibling holds which half:** the construction below is split across the siblings and must not be re-merged. The reader-facing half — the unnumbered preamble, the rule in words, the section-by-section table of what the weeks are, and the derivation table — lives in `.txt.md`. The code-level half — `ADAPT` / `NOTICE` / `PRIOR`, the `walk()` helper, the `lcg()` seeds and all drawing detail — lives in `.viz.md`. This combined spec keeps both.

---

## Preamble (unnumbered, before Section 1 — appears in `.txt.md` and in the html as a canvas-less `.card-section`)

**Heading (`<h2>`):** The one rule behind every figure on this page

**The one rule behind every figure on this page.** A **level** is what was actually delivered in a week. The **level people silently expect** starts at **70** — what the service was before any of this — and each week creeps **35% of the way** toward whatever it has just received: **new expected = old expected + 0.35 × (delivered − old expected)**. A week **registers** at all only when delivered sits **4 or more** away from that expected level: above it as a noticed-good week, below it as a complaint. Sections 2 and 3 both run that two-line rule. Section 1 is the special case where the bar is a written threshold instead of a felt one.

| Section | What the weeks are | Which bar is applied |
|---|---|---|
| 1 | 52 weeks each: Team A one 260-minute stop in week 19, Team B 130 stops of 2 minutes | written — a single stop of 5 minutes or more gets an incident report |
| 2 | 24 weeks each: steady hands in 82 every week, wobbling cycles 88, 88, 70, 76, 84, 86 | felt — register only at 4 or more from the expected level |
| 3 | 48 weeks, four quarters aimed at 72 / 78 / 84 / 90 | both — a written bar at 72 and the same felt 4 |

Every quantity the bullets quote, and how it follows from those weeks:

| Quantity | Value | How it checks out |
|---|---|---|
| Minutes in a year | 525,600 | 365 × 24 × 60 |
| Team A minutes offline | 260 | one stop of 260 minutes |
| Team B minutes offline | 260 | 130 stops × 2 minutes each |
| Uptime, either team | 99.95% | (525,600 − 260) ÷ 525,600 |
| Incident reports | 1 and 0 | A's 260 clears the 5-minute bar; none of B's 2s do |
| Both 24-week averages | 82.0 | 24 × 82 = 1968; (88+88+70+76+84+86) × 4 = 1968; 1968 ÷ 24 |
| Steady team's noticed weeks | 3 | gaps 12.0, 7.8, 5.1 clear 4; week 4's is 3.3 and falling |
| Steady weeks registering nothing | 21 | 24 − 3 noticed − 0 complaints |
| Wobbling team's tally | 14 good, 6 bad | each climb reopens the gap past 4, each dip past −4 |
| The perverse ratio | 4.7× | 14 ÷ 3 |
| Quarter averages over 48 weeks | 71.4 up to 90.2 | mean of each quarter's 12 delivered levels |
| The rise | 18.8 points | 90.2 − 71.4 |
| Weeks under the written 72 | 7, 2, 1, 0 | the bar sits still while delivery climbs past it |
| Weeks 4 or more below expectation | 3, 3, 3, 3 | expectation climbs 70 to 86.7 behind delivery |
| Complaints logged all year | 12 | 3 + 3 + 3 + 3 |

Two of those cannot be redone with a pen. Team B's placement of its 130 stops and the 48 weekly levels come from a fixed seeded stream, so the **47 of 52 weeks touched**, the **12-minute worst week** and the four quarter averages are read off that draw. Team B's 260-minute total holds whatever the placement, because 130 stops of 2 minutes is 260 by construction. Everything else above closes by arithmetic.

---

## Section 1 — The Service That Never Went Down

**Tags:** `two teams` (violet), `same uptime` (blue), `nothing to point at` (magenta)

**Bullets:**
- **The setup** — Team A and Team B, a year each, one agreement, 260 minutes offline apiece
- **Team A, which broke once** — one 260-minute outage in week 19, then a clean run to December
- **Team B, which never broke** — 130 two-minute stops in 47 of 52 weeks, worst week 12 minutes
- **What the totals say** — 99.95% uptime each on a 525,600-minute year, equal to the decimal
- **The write-up rule** — one report per stop of 5 minutes or more, and B's longest ran 2 minutes
- **What that rule produced** — 1 report for Team A, 0 for the team that never stopped at all
- **What Team A can point at** — a report, a fix, a recovery, a named project on somebody's slide
- **What Team B can point at** — nothing, because a service that never stopped leaves no paper

**Key point:** Both teams lost the same 260 minutes and reported the same 99.95% uptime. Only one of them lost them in a shape the write-up rule could see — 1 report against 0 — and that shape, not the downtime, decided which team ended the year with something to show.

**Source note (`.src`):** Illustrative Example — 52 seeded weeks per team; both totals, both uptime figures and both report counts are counted off the plotted bars in the draw function.

### Visualization — canvas `c1`, 720×360

Two 52-week downtime strips stacked over one week axis — Team A's single spike above, Team B's low scatter below — with a right-hand panel reporting what each year produced.

- **Shared construction — implementation of the reader-facing one in the preamble above:** the
  preamble carries the premise, the rule in words and the derivation table for every figure on the
  page; this block is only the code that produces them. In code the module-level constants are
  `ADAPT = 0.35`, `NOTICE = 4` and `PRIOR = 70`, and `walk(levels, start)` returns one record per week
  holding the delivered level `L`, the expected level `E` in force that week and their difference
  `gap`, updating `E ← E + ADAPT × (L − E)` after each week. Charts 2 and 3 both call `walk()` and
  count off `gap`; chart 1 applies a written `REPORT_MIN` instead. Changing `ADAPT`, `NOTICE` or
  `PRIOR` invalidates the preamble table as well as the bullets.
- **Data:** `MINUTES_YEAR = 525600`, 52 weeks. Team A: a single 260-minute stop in week 19, zero every
  other week. Team B: 130 stops of 2 minutes each, placed by a seeded stream (`lcg(2731)`), so the
  total is 260 by construction whatever the placement. Team B lands blips in **47** of the 52 weeks and
  its worst week totals **12 minutes**.
- **Computed at render time:** each team's total (**260** and **260**), each team's uptime
  (`100 × (525600 − total) / 525600` = **99.95%** both, printed to two decimals), Team B's count of
  weeks touched, Team B's worst week, and the report count per team (single stops of 5 minutes or
  more: **1** and **0**).
- **Title (bold 15px `P.ink`, centered, y=21):** "Two Teams, 260 Minutes Down Each"
- **Upper strip** (`AT=54`, `AB=150`, minutes axis 0–280): bold 12px `P.violet` header
  "TEAM A — one stop of 260 minutes"; one violet bar (`rgba(74,58,167,0.50)` stroked `P.violet`) at
  week 19 reaching the full 260, labelled bold 19px `P.violet` "260 min" beside its top; gridlines at
  0 / 140 / 280 in `P.grid` with 12px `P.mute` labels.
- **Lower strip** (`BT=196`, `BB=268`, minutes axis 0–280, same scale as above so the two are
  comparable by eye): bold 12px `P.blue` header "TEAM B — 130 stops of 2 minutes"; 52 blue bars
  (`rgba(42,120,214,0.45)` stroked `P.blue`), all of them nearly flat against the axis; week numbers
  1 / 13 / 26 / 39 / 52 in 12px `P.mute` beneath, plus 12px `P.mute` "worst week: 12 minutes".
- **Right-hand panel** at `PX + PW + 24`: bold 13px `P.ink` "WHAT THE YEAR / PRODUCED", then per team
  a bold 19px count of incident reports over a 12px `P.mute` team label — `P.violet` "1 report" over
  "Team A", `P.blue` "0 reports" over "Team B" — then bold 12px `P.mute` "uptime 99.95% / uptime
  99.95%" so the equality is stated next to the inequality.
- **Caption (bold 13px `P.violet`, centered, `h−10`):** "Same minutes lost. Only one shape leaves a receipt."

---

## Section 2 — The Level Stops Being Felt

**Tags:** `same average` (orange), `felt vs delivered` (yellow), `recovery pays` (blue)

**Bullets:**
- **Two teams, half a year** — 24 weeks each, both landing on an average quality of exactly 82
- **The steady team** — hands in 82 every single week, with no better week and no worse one
- **The wobbling team** — cycles 88, 88, 70, 76, 84, 86 four times over, averaging 82 as well
- **What people carry** — a level they silently expect, closing 35% of the gap to delivery each week
- **What registers at all** — only a week sitting 4 or more clear of the level they now expect
- **What happens to the steady team** — expectation reaches 82, and 21 of its 24 weeks go silent
- **What happens to the wobbling team** — every climb reopens the gap: 14 good weeks, 6 complaints
- **The perverse result** — 4.7× as many noticed-good weeks as the steady team, for equal work

**Key point:** A steady level is felt only while it is still new (the effect psychologists call hedonic adaptation). Once what people expect has climbed to 82 and met it, holding 82 forever registers as nothing at all — 21 of 24 weeks silent — while a team that dips and climbs back collects 14 noticed-good weeks on exactly the same average.

**Source note (`.src`):** Illustrative Example — two hardcoded 24-week series, chosen so both averages are exactly 82; every count, both averages and the ratio are computed in the draw function.

### Visualization — canvas `c2`, 720×380

Two panels over one 24-week axis. Above, both delivered levels with the level each team's audience silently expects trailing behind. Below, a tally of which weeks registered as good or bad for each.

- **Data (hardcoded, because the shape is the lesson):** steady = 82 repeated 24 times; wobbling =
  `[88, 88, 70, 76, 84, 86]` repeated four times. Both sum to **1968** over 24 weeks, so both average
  exactly **82.0** — computed and printed, not asserted.
- **Adaptation:** both audiences start from an expected level of **70** (what the service was before
  either team took over) and update `E ← E + 0.35 × (L − E)` each week. By week 24 the steady team's
  audience expects **82.0** — dead level with what it is getting, so the gap is zero. The wobbling
  team's audience is still at **80.6** and never settles.
- **Counting rule:** a week registers as good when `L − E ≥ +4`, as bad when `L − E ≤ −4`, and not at
  all in between. Steady: **3 good** (weeks 1, 2, 3), **0 bad**. Wobbling: **14 good** (weeks 1, 2, 5,
  6, 7, 8, 11, 12, 13, 17, 18, 19, 23, 24), **6 bad** (weeks 3, 9, 15, 16, 21, 22). The ratio
  **4.7×** is computed as 14 ÷ 3 and printed from that variable.
- **Title (bold 15px `P.ink`, centered, y=21):** "Same Average Quality, Delivered Two Ways"
- **Upper panel** (`AT=58`, `AB=214`, level axis 66–92): gridlines at 70 / 75 / 80 / 85 / 90 in
  `P.grid`; a 3px `P.orange` line for the steady team, a 3px `P.blue` line for the wobbling team, and
  each team's silently expected level as a matching 1.5px dashed trail. Bold 12px labels
  `P.orange` "steady — average 82.0" and `P.blue` "wobbling — average 82.0", both figures printed
  from the computed means.
- **Notice band** (`NT=236`, `NB=300`): one row of 24 slots per team. A slot is filled
  `rgba(0,131,0,0.55)` when the week registered as good, `rgba(213,81,129,0.55)` when it registered as
  bad, and left as a hollow `P.grid` outline when nothing registered. Row labels bold 12px:
  `P.orange` "steady" and `P.blue` "wobbling". Week numbers 1–24 in 11px `P.mute` beneath.
- **Tally text (bold 12px, below the band):** `P.orange` "steady: 3 weeks noticed, 0 complaints" and
  `P.blue` "wobbling: 14 weeks noticed, 6 complaints", both read off the same arrays the band draws.
- **Highlight (bold 19px `P.blue`, right side):** the computed "4.7×" over 12px `P.mute` "more
  noticed-good weeks, on the same average".
- **Caption (bold 13px `P.orange`, centered, `h−10`):** "The flat line is the same quality. It just stopped being felt."

---

## Section 3 — What It Does to the Numbers

**Tags:** `the measurement` (aqua), `fixed bar` (green), `sliding bar` (red)

**Bullets:**
- **The system** — genuinely improves over 48 weeks, a quarterly average of 71.4 rising to 90.2
- **The size of it** — a rise of 18.8 points, far too large to pass off as noise or a lucky quarter
- **Counted against a written bar** — weeks under 72 fall 7, 2, 1, 0 as the bar stays where it is
- **Counted against the felt bar** — a week counts only when it lands 4 or more below expectation
- **What that second count gives** — 3 complaints in every quarter, flat across an improving year
- **Why it stays flat** — expectation climbs from 70 to 86.7 behind delivery, so the gap persists
- **What the log therefore holds** — 12 complaints and no trace of the 18.8-point climb at all
- **What a well-run year looks like** — exactly like a year in which nothing whatsoever happened

**Key point:** If the only thing you log is a departure from what people currently expect, then the bar you are measuring against moves up whenever you improve. An 18.8-point rise cancels itself out of the count, leaving 3 complaints a quarter all year — a record indistinguishable from a system standing still.

**Source note (`.src`):** Illustrative Example — 48 seeded weeks (`lcg(26640)`, spread 4.5 around quarterly targets of 72 / 78 / 84 / 90); all eight quarterly counts, both quarter averages and the rise are computed in the draw function.

### Visualization — canvas `c3`, 720×400

A weekly level rising across four quarters with the silently expected level chasing it, above a paired bar chart of the two ways the same 48 weeks were counted.

- **Data:** 48 weeks, quarterly targets **72 / 78 / 84 / 90**, each week drawn as
  `target + (rng()+rng()+rng()−1.5) × 2 × 4.5` from a single seeded stream `lcg(26640)`. Realised
  quarter averages **71.4 / 78.2 / 83.0 / 90.2**; the rise, computed as the fourth quarter average
  minus the first, is **18.8 points**.
- **The two counts, both computed from the same 48 weeks:**
  - *Fixed bar* — weeks delivering under a written **72**: **7 / 2 / 1 / 0**, total **10**. This count
    falls, because the bar sits still while the system improves.
  - *Sliding bar* — weeks landing 4 or more below the level people had come to expect, with that
    expected level updating `E ← E + 0.35 × (L − E)` from a start of 70: **3 / 3 / 3 / 3**, total
    **12**. This count is flat.
- **Arithmetic check:** 7 + 2 + 1 + 0 = 10 and 3 + 3 + 3 + 3 = 12, both totals printed from the summed
  arrays rather than typed.
- **Title (bold 15px `P.ink`, centered, y=21):** "One Improving System, Two Ways of Counting It"
- **Upper panel** (`AT=56`, `AB=214`, level axis 62–100): faint `P.grid` gridlines at 70 / 80 / 90;
  the 48 weekly levels as a 2px `P.aqua` line with 3px dots; the level people expect as a 2px dashed
  `P.mute` trail climbing behind it; a solid 1.5px `P.green` horizontal at 72 labelled bold 12px
  `P.green` "the written bar — stays at 72"; four faint quarter separators with bold 12px `P.ink`
  quarter labels and each quarter's realised average in 12px `P.mute`.
- **Lower panel** (`BT=258`, `BB=344`, count axis 0–8): four quarter groups, each with a green bar
  (`rgba(0,131,0,0.45)` stroked `P.green`) for the fixed-bar count and a red bar
  (`rgba(231,76,60,0.45)` stroked `P.red`) for the sliding-bar count, each carrying its value in bold
  13px above. Legend bold 12px: `P.green` "weeks under the written 72 — falls 7 → 0" and `P.red`
  "weeks below what people expected — 3, 3, 3, 3", both strings built from the computed arrays.
- **Footnote (12px `P.mute`, under the axis):** "the service improved 18.8 points across these four
  quarters; the sliding count did not move", with the rise printed from the computed difference.
- **Caption (bold 13px `P.red`, centered, `h−10`):** "Log only the departures and the improvement erases itself."

---

## Regeneration instructions

- **Template:** copied verbatim from `05-cognitive-biases/25-mere-exposure-effect.html` — the
  whole `<style>` block, the `setup(id)` canvas helper, the `lcg(seed)` Park–Miller generator, the `P`
  palette object, the `__charts` array and its debounced resize tail, and the `table.layout` /
  `td.text-col` 50% / `td.viz-col` 50% structure. Only the content differs.
- **Text column order:** `.tags` pill row of three → `<ul>` of 8 one-line bullets each opening with a
  `<b>bold label</b>` and an em dash → one `.key-point` callout → one `.src` note. No paragraph
  blocks, no data tables, no status badges.
- **Bullet form:** roughly 90–100 characters including the label, aimed at one line at 50% column
  width. A fact is never dropped to hit the length; it goes in another bullet instead.
- **Text stands alone; the chart adds clarity** — the text carries the argument and names every quantity it turns on; the canvas adds precision, intermediate values and per-point labels. No bullet points at a position on the canvas. See `ui-templates/README.md`.
- **Canvas:** intrinsic `width="720"`, heights 360 / 380 / 400, CSS `width: 100%` capped at 720px by
  `setup()`. Charts registered in `__charts`, re-run on a 150ms debounced resize.
- **Colour split across sections:** section 1 violet against blue, section 2 orange against blue with
  a green/magenta notice band, section 3 aqua with green against red.
- **Determinism:** no `Math.random()` anywhere. Chart 1 uses `lcg(2731)`, chart 3 uses `lcg(26640)`,
  one fresh stream each. Chart 2's two series are hardcoded literal arrays because the shape and the
  equal averages are the lesson — a seeded draw would blur both. Every total, average, count, uptime
  figure and ratio printed on any chart is computed inside its draw function from the plotted values.
- **One rule runs the page.** The level everyone silently expects updates `E ← E + 0.35 × (L − E)`, and
  a week registers only when the delivered level is at least 4 away from it. Section 2 uses it to show
  a steady team going unfelt; section 3 uses it to show the same rule wiping an 18.8-point improvement
  out of the count. Section 1 is the written-threshold version of the same idea.
- **Scope:** this page covers a steady good level going unnoticed and unmeasured. It deliberately does
  not compare how heavily good and bad weeks are weighted against each other, and it does not touch who
  is willing to give feedback in the first place.
- **No navigation:** no back or home links, no cross-page links, no `.nav` CSS.
