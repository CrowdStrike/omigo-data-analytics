# Negativity Dominance: The Ledger Nobody Keeps Fairly

**Page type:** detail page — card-section template (see `05-cognitive-biases/25-mere-exposure-effect.html`)
**HTML title tag:** Negativity Dominance — Cognitive Biases

**Subtitle:** One complaint colours a whole review, while the twenty quiet successes that surround it average into nothing.

---

## Section 1 — One Log, Weighed Two Ways

**Tags:** `the log` (violet), `one dial` (blue), `two totals` (magenta)

**Bullets:**
- **The log** — twenty-four entries from one supplier: twenty went well and four went badly
- **Every entry the same size** — no entry is a catastrophe, none is a triumph, each counts as one
- **The one dial** — the impression prices a bad entry as heavily as five good ones
- **Counted once each** — the bad entries are four of twenty-four, a sixth of the log
- **Weighted as it feels** — those four carry twenty units against the good side's twenty
- **So a sixth of the log** — holds exactly half of the weight sitting behind the verdict
- **The fair total** — 8.3 out of ten, which is just the share of entries that went well
- **The felt total** — 5.0 out of ten, a coin flip on a record that is five-sixths clean

**Key point:** Nothing in the log changed between the two totals — only the price put on a bad entry. One dial, set at five, turns a five-sixths-clean record into a coin flip (this asymmetry is what the literature calls negativity dominance).

**Source note (`.src`):** Illustrative Example — one fixed 24-entry log; both shares, both weights and both totals are computed from the plotted tiles. The direction of the asymmetry is well replicated (Baumeister et al. 2001; Rozin & Royzman 2001); the price of five is a dial of this construction, not a measured quantity.

### Visualization — canvas `c1`, 720×400

A row of entry tiles, then the same log drawn twice as a 100%-weight bar — once counted evenly, once weighted as it feels — and a verdict strip underneath carrying both totals.

- **Shared construction (used by every chart on the page):** the log is the fixed literal array
  `LOG = [1,1,1,1,0, 1,1,1,1,0, 1,1,1,1,1,1,0, 1,1,1,1,0, 1,1]` where `1` went well and `0` went
  badly — 20 good, 4 bad, 24 entries. A verdict out of ten is the good side's share of the weight,
  `verdict(g, b, R) = 10 · g / (g + R·b)`, where `R` is how many good entries one bad entry is
  priced at. `R = 1` is a fair tally, `R_FELT = 5` is the dial this page is about. Counts are taken
  from the array, never typed in, so every figure on the page follows from the tiles that are drawn.
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

## Section 2 — One Bad Entry Sets the Level, the Good Ones Only Dilute It

**Tags:** `the asymmetry` (orange), `one step each way` (yellow), `the repair bill` (red)

**Bullets:**
- **Start here** — twenty good entries and one bad one, which the same dial scores 8.0 out of ten
- **Add one more good entry** — the verdict moves to 8.08, a gain of eight hundredths of a point
- **Add one more bad entry instead** — it falls to 6.67, a drop of 1.33 points off the same start
- **One step each way** — same size of entry, and the bad step moves the verdict 17 times as far
- **Why the gap beats the dial** — the good side already holds twenty, so entry twenty-one adds little
- **The bad side holds one** — so a second bad entry doubles the entire weight pulling downward
- **The repair bill** — twenty more good entries just to climb back to the 8.0 you already had
- **Never all the way back** — with a bad entry on the log the verdict approaches ten, never reaches it

**Key point:** Good entries pile up with shrinking returns and bad ones do not, so the two sides never average. The small side sets the level and the big side can only dilute it, which is why a verdict full of good news still reads like the one complaint.

**Source note (`.src`):** Illustrative Example — the same weighting as the first chart applied to logs of every size; each printed verdict, the 17-fold gap and the twenty-entry repair bill are solved for inside the draw function.

### Visualization — canvas `c2`, 720×360

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

## Section 3 — Right for a Hazard, Wrong for a Colleague

**Tags:** `the defence` (green), `cost of being wrong` (aqua), `the crossover` (violet)

**Bullets:**
- **The rule under test** — call the log bad once the weighted bad side matches the weighted good side
- **On this log** — that rule fires exactly when one bad entry is priced at five good ones
- **What settles whether it is right** — what it costs you to be wrong in each direction, nothing else
- **A contaminated delivery** — worth about forty clean ones, so cutting the supplier off is correct
- **A late shipment** — worth about eight on-time ones, past the line too, so alarm is right again
- **A snappish reply from a colleague** — worth about a third of a helpful one, and the rule still bins them
- **What that throws away** — 18.8 helpful acts' worth, and those acts were the thing you wanted to keep
- **The crossover** — break-even sits at 5.0, and one fixed dial cannot be right on both sides of it

**Key point:** Leaning toward alarm is the correct call wherever missing a real problem costs far more than a false alarm, which covers most hazards — there, a dial of five is if anything too mild. It stops being correct where the good acts are the thing you were trying to sustain, because then the rule spends them to buy an alarm you did not need.

**Source note (`.src`):** Illustrative Example — the same fixed 24-entry log at three different costs of a bad entry; the break-even price and every discarded amount are computed from the same counts the first chart plots.

### Visualization — canvas `c3`, 720×360

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

## Regeneration instructions

- **Template:** copied from `05-cognitive-biases/25-mere-exposure-effect.html` — the entire
  `<style>` block, the `setup()` canvas helper, the `lcg()` seeded generator, the `P` palette object,
  the `__charts` array with its debounced resize tail, and the `table.layout` / `.text-col` /
  `.viz-col` 50/50 structure, all verbatim. Only the content differs.
- **Structure:** three `.card-section` blocks, each an `<h2>` plus a `table.layout` row of
  `td.text-col` (50%) then `td.viz-col` (50%). Text column order: `.tags` pill row of three →
  `<ul>` of eight labelled bullets → one `.key-point` → one `.src`. No paragraphs, no data tables.
- **Bullet form:** `<li><b>label</b> — phrase</li>`, around 90–100 characters, opening bold label in
  `#1a5276`. A slight wrap is acceptable; a fact is never dropped to shorten a line.
- **The 50/50 split is fixed.** A chart is shrunk through the canvas cap, never by narrowing the
  viz column.
- **Canvas:** intrinsic `width="720"`, heights 400 / 360 / 360, CSS `width: 100%`. `setup(id)` caches
  the logical size in `dataset`, sets `style.maxWidth = 720px`, and scales the backing store by
  `(cssW / 720) × devicePixelRatio` before scaling the context back to logical coordinates.
- **Canvas font sizes:** chart title bold 15px, in-chart headers bold 12px, body and axis labels 11–12px,
  the big callout figure bold 19px, caption bold 13px.
- **Palette** (shared `P` object): `blue #2a78d6`, `green #008300`, `magenta #d55181`, `yellow #c98500`,
  `aqua #199e70`, `orange #d95926`, `violet #4a3aa7`, `ink #1a5276`, `text #2c3e50`, `mute #6b7280`,
  `grid #e5e9ef`.
- **Colour variety across sections:** section 1 green tiles against magenta with a violet caption,
  section 2 a four-colour curve family captioned orange, section 3 an orange wedge between a magenta
  and a green zone with a violet break-even line.
- **One construction runs the whole page.** The literal `LOG`, the single dial `R_FELT = 5`, and
  `verdict(g, b, R) = 10·g/(g + R·b)`. Section 1 reads it at `R = 1` and `R = 5`, section 2 moves `g`
  and `b` by one step each, section 3 replaces the dial with the real cost of a bad entry and asks
  where the two agree. Changing `LOG` or `R_FELT` moves every figure on the page at once.
- **Determinism:** no `Math.random()` anywhere. The log is a fixed literal, the verdicts are
  closed-form, and the seeded `lcg()` is used only for cosmetic dot jitter — no printed statistic
  depends on a draw. Everything printed beside a chart is computed inside its draw function.
- **Scope:** this page is only about how good and bad are weighted. It does not claim anything about
  how long either kind of memory lasts.
- **Citation discipline:** the direction of the asymmetry is attributed to Baumeister et al. (2001)
  and Rozin & Royzman (2001) and nothing more. The five in the title is a figure of speech for this
  construction's dial. No "magic ratio" is cited, and no divorce-prediction claim appears.
- **No navigation:** no back or home links, no cross-references to other pages, no status badges.
