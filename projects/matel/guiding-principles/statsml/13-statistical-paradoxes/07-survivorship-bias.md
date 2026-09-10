# Survivorship Bias: The Failures Left No Forwarding Address

**Page type:** detail page — standard `statistical-paradoxes` skeleton (50/50 `table.layout`, `.card-section` per
section, tag pills, `setup()` + `P` palette + `lcg()` + `fit()` helpers). CSS and boilerplate: see the shared
skeleton, not repeated here.

**HTML title tag:** Survivorship Bias — Statistical Paradoxes
**Subtitle:** Every trait the winners share is also a trait the losers had — you just never met the losers

---

## Section 1 — The coffee shops that are still open

**Tags:** blue `core idea` · green `hidden filter` · orange `invisible failures`

**Bullets**
- **The cohort** — two hundred coffee shops opened in one city in the same year.
- **What is left** — five years on, fifty-three still trade and one hundred forty-seven have shut.
- **The shared trait** — most of the surviving shops open their doors before seven in the morning.
- **The tempting recipe** — open early and you will last, because the survivors nearly all do it.
- **The missing group** — the shops that shut opened early at the same rate.
- **Why they vanish** — a closed shop has no website, no sign, and nobody left to answer.
- **The truth here** — opening early was set independently of survival when this cohort was built.
- **The lesson** — a habit common among survivors is only news if the failures lacked it.

**Example:** Of two hundred shops opened in one year, fifty-three still trade and thirty-seven of those open early —
and one hundred three of the one hundred forty-seven that shut did as well.

**Key point:** A trait counts as evidence only when you can also check its rate in the group that disappeared.

**Source note:** Illustrative Example — cohort generated from a fixed seed.

**Chart c1 (720×340) — "Two hundred coffee shops, five years on"**
- Sub-note under title: each dot is one shop from the founding cohort.
- 20×10 dot grid, index order: closed shops small hollow `P.mute`, survivors filled `P.aqua` with darker ring.
- Legend row: mute swatch "shut (147)", aqua swatch "still open (53)".
- Two horizontal bars, share opening before seven: survivors `P.magenta` (misleading view), shut `P.mute`
  (data you never saw). Percentages computed from the generated grid, printed at each bar end.
- Caption: the survivors' habit was just as common among the shops that shut.

---

## Section 2 — Fund tables that only list the funds still open

**Tags:** blue `core idea` · magenta-role `visible average` · orange `who exits`

**Bullets**
- **The performance table** — one page listing every fund a firm runs beside its yearly return.
- **The quiet edit** — funds that were closed or folded into others drop off the page entirely.
- **Who exits** — the funds that get closed are overwhelmingly the ones that were doing badly.
- **The visible average** — the twenty-two funds still listed average close to nine a year.
- **The honest average** — counting all forty that were launched, the average is under eight.
- **The size of the flattery** — the page reads about a point a year better than the cohort did.
- **The same shape elsewhere** — a satisfaction survey mailed only to customers who stayed.
- **The tell** — the number of rows is smaller than the number of things that entered.

**Example:** Vendor A's brochure lists twenty-two funds; eighteen more were launched alongside them and were
closed before the brochure went to print.

**Key point:** When a list holds only what is still here, its average describes the exit rule, not the performance.

**Source note:** Illustrative Example — forty funds generated from a fixed seed, closure chance falling with return.

**Chart c2 (720×330) — "Forty funds launched, twenty-two still listed"**
- Horizontal value axis: yearly return, ticks 0/3/6/9/12/15.
- Forty jittered dots: closed funds `P.mute`, still-open funds `P.blue`.
- Vertical `P.green` line at the mean of all forty; vertical `P.magenta` line at the mean of the still-open ones.
- Legend below the axis, two rows, each average printed from the computed variable.
- One big figure (bold 19px): the gap in points between the two lines.
- Caption: removing the funds that closed lifts the average without changing a single fund.

---

## Section 3 — The vanished funds put nearly a point a year on the record

**Tags:** blue `documented case` · green `honest rate` · red `overstatement`

**Bullets**
- **The database** — commercial fund databases carry the funds that still exist today.
- **The measured effect** — dropping dead funds lifts the surviving average by about nine tenths of a point a year.
- **The study** — Elton, Gruber and Blake examined this directly in nineteen ninety-six.
- **Independent agreement** — Malkiel reported the same direction and size a year earlier.
- **Why it is that large** — a fund is usually closed precisely because it had been lagging.
- **What compounding does** — over twenty years the flattered rate pulls far ahead of the honest one.
- **Practical effect** — a savings plan built on the survivor rate quietly overshoots its target.
- **The fix** — use a database that keeps dead funds and the returns they had when they died.

**Example:** Ten thousand compounding for twenty years at the survivor rate ends about ten thousand ahead of the
same stake at the rate the whole cohort actually earned.

**Key point:** Elton, Gruber & Blake (1996) and Malkiel (1995) both place the survivor-only overstatement near
nine tenths of a percentage point a year.

**Source note:** Elton, Gruber & Blake (1996), *Journal of Business*; Malkiel (1995), *Journal of Finance*.
Growth figures are plain arithmetic on those two rates.

**Chart c3 (720×330) — "Ten thousand compounding for twenty years"**
- Two curves over years 0–20: `P.magenta` at the survivor-inflated rate, `P.green` at the rate minus the
  documented gap. Both rates held in variables; the gap is subtracted, not typed twice.
- Value axis on the left with rounded ticks; year axis along the bottom.
- End labels at year twenty print both balances, computed at draw time.
- One big figure (bold 19px): the difference between the two endings.
- Caption: nine tenths of a point a year is invisible in one year and large in twenty.

---

## Section 4 — When the disappearance is blind to the outcome

**Tags:** blue `boundary` · green `harmless attrition` · orange `the fix`

**Bullets**
- **The condition** — the bias needs the leaving to be linked to the thing you are measuring.
- **Harmless case** — sixty entrants, half of them dropped by a coin flip that ignores their score.
- **What happens** — the survivors' average lands a fraction of a point from the full cohort's.
- **Harmful case** — the same sixty entrants, but everyone below the middle score is dropped.
- **What happens** — the survivors' average jumps by more than fourteen points on the same scale.
- **Fix one** — fix the denominator at the moment of entry rather than at the moment of reporting.
- **Fix two** — keep a permanent record of everything that entered, including what later died.
- **Fix three** — print the cohort's starting count beside every survivor-only average.

**Example:** Both panels start from the same sixty entrants averaging 47.2; blind dropout leaves 47.4, while
score-linked dropout leaves 61.5.

**Key point:** Attrition only distorts an average when it correlates with the outcome — so always ask what
decided who left.

**Source note:** Illustrative Example — sixty entry scores generated from a fixed seed.

**Chart c4 (720×340) — "Who leaves decides whether the average moves"**
- Two stacked strips sharing one bottom axis (entry score, ticks 20…80).
- Upper strip: coin-flip dropout; dropped dots `P.mute`, kept dots `P.blue`.
- Lower strip: below-the-middle dropout; dropped dots `P.mute`, kept dots `P.violet`.
- In both strips a solid `P.green` line at the full-cohort average and a dashed `P.magenta` line at the
  survivors' average; in the upper strip the two land on top of each other, and the chart says so.
- Each strip header prints its own computed shift.
- Caption: random loss leaves the average alone; outcome-linked loss moves it.
