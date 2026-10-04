# Negativity Dominance — Viz

**Page type:** detail page, card-section (template `06-sectioned-cards-callout`)
**HTML title tag:** Negativity Dominance — Cognitive Biases
**Source note wording:** the sibling `.txt.md` says section 2's figures are "solved for at render time"; the html `.src` note says "solved for inside the draw function".

House style (bullet form, tag-pill markup, text-column order, canvas DPR scaling, shared palette `P`, canvas font floors, section-header borders) lives in `ui-templates/` and is not restated here.

**Figures printed in the sibling `.txt.md` are computed here.** The split is: the sibling `.txt.md` carries the reader-facing construction — what the log is, the verdict rule in words, and a derivation table for both totals — while this file carries its implementation (the literal array, the helper, the seeds, the drawing). Changing `LOG` or `R_FELT` moves every figure on the page at once — both totals, the two one-step moves, the 17-fold gap, the repair bill and the break-even price must be re-read from the new construction and the sibling's preamble and bullets updated to match.

**Determinism:** no `Math.random()` anywhere. The log is a fixed literal, the verdicts are closed-form, and the seeded `lcg()` is used only for cosmetic dot jitter — no printed statistic depends on a draw. Everything printed beside a chart is computed inside its draw function.

---

## 1. One Log, Weighed Two Ways

**Tag colors:** `the log` violet, `one dial` blue, `two totals` magenta
**Hue family:** green tiles against magenta with a violet caption

### canvas `c1` — 720×400

A row of entry tiles, then the same log drawn twice as a 100%-weight bar — once counted evenly, once weighted as it feels — and a verdict strip underneath carrying both totals.

- **Shared construction — implementation of the reader-facing one in the sibling `.txt.md`:** that
  sibling's preamble carries the premise and the derivation table in human-readable form; this block
  is only the code that produces them. The log is the fixed literal array
  `LOG = [1,1,1,1,0, 1,1,1,1,0, 1,1,1,1,1,1,0, 1,1,1,1,0, 1,1]` where `1` went well and `0` went
  badly — 20 good, 4 bad, 24 entries. The verdict helper is
  `verdict(g, b, R) = 10 · g / (g + R·b)`, with `R = 1` the fair tally and `R_FELT = 5` the page's
  dial. Counts are tallied from the array, never typed in, so every figure on the page follows from
  the tiles that are drawn. Changing `LOG` or `R_FELT` invalidates the sibling's table — re-derive it.
- **Why there is no random draw:** the log is a fixed literal because the counts themselves carry
  the lesson, and every verdict is closed-form. The seeded `lcg()` helper is present and is used
  only to jitter the vertical position of the forty dots on the verdict strip so they do not
  overlap; no printed number depends on it. `Math.random()` appears nowhere on the page.
- **Title (bold 15px `P.ink`, centered, y=22):** "One Log, Weighed Two Ways"
- **Sub-line (12px `P.mute`, x=40, y=44):** "one supplier's log — 20 entries went well, 4 went badly",
  both counts printed from the tally of `LOG`.
- **Tiles:** 24 slots across `x = 40 … 680` at `y = 54`, height 26. Good entries filled
  `rgba(0,131,0,0.30)` stroked `P.green`; bad entries filled `rgba(213,81,129,0.45)` stroked
  `P.magenta`. Drawn straight from `LOG`, so the four bad tiles sit at entries 5, 10, 17 and 22.
- **Legend (12px, y=96):** a green swatch with "went well" and a magenta swatch with "went badly".
- **Bar A — counted once each** (`y=118`, height 32, bar spans `x = 176 … 680`): good segment
  `20/24` of the width labelled "83.3% of the log", bad segment `4/24` labelled "16.7%". Row label
  bold 12px `P.ink` at x=40: "counted once each".
- **Bar B — weighted as it feels** (`y=170`, same span): good segment `20/(20 + 5·4) = 50.0%`, bad
  segment `50.0%`, both printed from the computed shares. Row label bold 12px `P.ink` "weighted as
  it feels" with 11px `P.mute` second line "one bad = 5 good".
- **Verdict strip:** header bold 12px `P.ink` "VERDICT OUT OF TEN" at x=40, y=228. Axis line at
  `y=268` spanning the same `x = 176 … 680` for scores 0–10, end labels "0" and "10" in 11px
  `P.mute`. Forty dots in the band `y = 238 … 260`, one per person, personal dials evenly spaced
  from 2 to 8 (`R_i = 2 + 6·i/39`) and vertical jitter from the seeded generator only.
- **The two totals:** triangles below the axis pointing up at the fair total (blue, 8.3) and the felt
  total (red/magenta, 5.0), each with its figure in bold 19px at `y=302` and an 11px `P.mute` caption
  at `y=318` — "fair tally" and "as it feels". Both positions come from `verdict()`.
- **Footnote (12px `P.mute`, x=40, y=352):** "forty people, dials from 2 to 8 — the mildest lands at
  7.1, the harshest at 3.8, and every one of them sits under the fair 8.3", all three figures read
  off the plotted dots.
- **Caption (bold 13px `P.violet`, centered, `h−10`):** "A sixth of the log carries half the verdict."

---

## 2. One Bad Entry Sets the Level, the Good Ones Only Dilute It

**Tag colors:** `the asymmetry` orange, `one step each way` yellow, `the repair bill` red
**Hue family:** a four-colour curve family captioned orange

### canvas `c2` — 720×360

Four curves of verdict against the number of good entries, one curve per number of bad entries, with both one-step moves drawn off a single marked starting point.

- **Data:** `verdict(g, b, 5)` for `b = 0, 1, 2, 4` over `g = 1 … 60`. The `b = 0` curve is flat at ten,
  the model's statement that a log with nothing bad on it has nothing pulling it down. At `g = 60`
  the curves read 10.00, 9.23, 8.57 and 7.50.
- **The tie back to section 1:** the `b = 4` curve at `g = 20` is 5.0 — the felt total of the first
  chart, the same formula with the same dial.
- **Starting point:** `(g=20, b=1)` marked with a large dot at 8.00.
- **One step each way, both computed:** `(21, 1) = 8.08`, a gain of `+0.08`; `(20, 2) = 6.67`, a drop
  of `−1.33`. The printed ratio `1.33 / 0.08 = 17×` is computed from those two differences, not typed.
- **The repair bill:** solved from the formula — `g = target·R·b / (10 − target)` with target 8.0,
  `R = 5`, `b = 2` gives `g = 40`, so 20 more good entries. Printed from that variable.
- **Title (bold 15px `P.ink`, centered, y=22):** "One Step Each Way from the Same Log"
- **Axes:** plot box `PX=58`, width `w − 58 − 116`, `TOPY=54`, `BASEY=272`. Vertical axis 0–10 with
  `P.grid` gridlines at 0, 2, 4, 6, 8, 10 and 12px `P.mute` labels; horizontal axis good entries
  0–60 with ticks every 10. Axis caption 12px `P.mute` "good entries on the log".
- **Curves:** 2.5px lines — `b=0` `P.green`, `b=1` `P.blue`, `b=2` `P.orange`, `b=4` `P.magenta`, each
  labelled at its right end in bold 11px of its own colour: "no bad entry", "1 bad entry",
  "2 bad entries", "4 bad entries".
- **The two steps:** from the marked start, a short `P.green` up-tick to `(21,1)` labelled bold 12px
  `P.green` "+1 good entry: +0.08", and a 2px `P.magenta` arrow straight down to `(20,2)` labelled
  bold 12px `P.magenta` "+1 bad entry: −1.33".
- **The repair bill:** dashed 1.5px `P.orange` horizontal at 8.0 running from `g=20` to `g=40` with an
  arrowhead, labelled bold 12px `P.orange` "20 more good entries to get back to 8.0".
- **Footnote (12px `P.mute`, below the axis caption):** "one step each way off the same log — the bad
  step moves the verdict 17× as far as the good one", the multiple computed from the two steps.
- **Caption (bold 13px `P.orange`, centered, `h−10`):** "The small side sets the level; the big side only dilutes it."

---

## 3. Right for a Hazard, Wrong for a Colleague

**Tag colors:** `the defence` green, `cost of being wrong` aqua, `the crossover` violet
**Hue family:** an orange wedge between a magenta and a green zone with a violet break-even line

### canvas `c3` — 720×360

A wedge showing how much good the alarm rule throws away, plotted against what one bad entry actually costs, falling to nothing at the break-even price and staying there.

- **The quantity plotted:** with 20 good and 4 bad entries, keeping the source is worth
  `20 − 4·x` good entries, where `x` is what one bad entry costs in units of one good one. The alarm
  rule fires on this log regardless of `x`, so the good it throws away is `max(0, 20 − 4·x)` — a wedge
  from 19.6 at `x = 0.1` down to zero at the break-even `x = 20/4 = 5.0`, and flat zero above it.
  Every value comes from the counts in `LOG`.
- **Break-even:** `g / b = 5.0`, computed, and the same number as the felt dial `R_FELT` — which is the
  point: a fixed dial is only ever right where it happens to match the real costs.
- **Title (bold 15px `P.ink`, centered, y=22):** "What One Bad Entry Actually Costs You"
- **Axes:** plot box `PX=64`, width `w − 104`, `TOPY=58`, `BASEY=262`. Horizontal axis is
  `log10(x)` from 0.1 to 100 with ticks at 0.1, 0.3, 1, 3, 10, 30, 100 in 12px `P.mute`, captioned
  "what one bad entry costs, in units of one good entry". Vertical axis 0–22 with `P.grid` gridlines
  every 5, captioned "good entries the alarm rule throws away".
- **Zones:** left of break-even filled `rgba(213,81,129,0.08)`, right of it `rgba(0,131,0,0.08)`.
  Zone captions in bold 11px: `P.magenta` "the good acts were the point — the rule spends them" on the
  left, `P.green` "a miss costs far more than a false alarm — alarm is correct" on the right.
- **The wedge:** 2.5px `P.orange` line over a `rgba(217,89,38,0.22)` fill down to the axis.
- **Break-even marker:** dashed 2px `P.violet` vertical at 5.0, labelled bold 12px `P.violet`
  "break-even 5.0" and 11px `P.mute` "one bad = five good", both printed from `g / b`.
- **Three cases**, each a 5px dot on the wedge with a label in bold 11px and its computed cost beneath
  in 11px `P.mute`: "a snappish reply — 0.3" at 18.8 thrown away, "a late shipment — 8" at 0.0, and
  "a contaminated delivery — 40" at 0.0. The two right-hand labels are staggered vertically so they
  clear the axis and each other.
- **Footnote (12px `P.mute`):** "the rule fires the same way at every price — only the left half of this
  chart is a mistake".
- **Caption (bold 13px `P.green`, centered, `h−10`):** "Lean toward alarm where a miss is expensive, not where the good acts are the point."

---

## Page-specific constraints

- **Bespoke helpers carried over verbatim from the source page** (`05-cognitive-biases/25-mere-exposure-effect.html`):
  the `setup()` canvas helper, the `lcg()` seeded generator, the `P` palette object, and the
  `__charts` array with its debounced resize tail. Only the content differs.
- **Canvas heights:** 400 / 360 / 360 — the first is taller than the page norm because it stacks
  tiles, two weight bars and a verdict strip in one figure.
- **One construction runs the whole page.** The literal `LOG`, the single dial `R_FELT = 5`, and
  `verdict(g, b, R) = 10·g/(g + R·b)`. Section 1 reads it at `R = 1` and `R = 5`, section 2 moves `g`
  and `b` by one step each, section 3 replaces the dial with the real cost of a bad entry and asks
  where the two agree. Changing `LOG` or `R_FELT` moves every figure on the page at once.
- **Everything printed beside a chart is computed inside its draw function** — both shares and both
  totals from the plotted tiles, each verdict and the 17-fold gap from the two differences, the
  repair bill solved from the formula, the break-even price and every discarded amount from the
  counts in `LOG`.
- **The log stays a fixed literal, not a seeded draw.** The counts themselves carry the lesson and
  every verdict is closed-form; `lcg()` is cosmetic dot jitter only.
- **Scope:** this page is only about how good and bad are weighted. It does not claim anything about
  how long either kind of memory lasts.
- **Citation discipline:** the direction of the asymmetry is attributed to Baumeister et al. (2001)
  and Rozin & Royzman (2001) and nothing more. The five in the title is a figure of speech for this
  construction's dial. No "magic ratio" is cited, and no divorce-prediction claim appears.
- **Label placement note:** section 3's two right-hand case labels ("a late shipment", "a contaminated
  delivery") both sit at 0.0 on the flat part of the wedge and must be staggered vertically so they
  clear the axis and each other.
