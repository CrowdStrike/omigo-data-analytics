# Social Desirability Bias — Viz

**Page type:** detail page — card-section template
**HTML title tag:** Social Desirability Bias — Cognitive Biases
**Template:** the card-section layout from `statistical-paradoxes/03-berksons-paradox.html`, matching the converted `05-clustering-illusion.html` and `01-confirmation-bias.html`
**Source note wording:** the sibling `.txt.md` says figures are "computed at render time"; the html `.src` notes say "computed in the draw function".

House style (bullet form, tag-pill markup, text-column order, canvas DPR scaling, shared palette `P`, canvas font floors, section-header borders) lives in `ui-templates/` and is not restated here.

**Figures printed in the sibling `.txt.md` are computed here.** The prose quotes the six person/box rates and their gaps, the average gap 13.5, the three log values with every shortfall, the average distances 28.3 and 9.3 and their 3.0× ratio, the flat rates 33 and 12, the five mix shares, the full blended series 14.1 → 30.9 with its 16.8-point rise, 119 percent relative rise and 4.2-point quarterly step, and all six manufactured amounts against the 2.0 bar with the four-of-six tally. Changing a literal array or the mix invalidates that prose — re-read the computed values and update the text to match.

**Determinism:** no `Math.random()` anywhere, and no seeded generator either — the `lcg` helper from the reference is deliberately omitted because every series on this page is a literal constructed array. Every printed figure (each gap, the average gap, each shortfall, both average distances and their ratio, every blended point, the rise and the relative rise, every manufactured amount, the tally clearing the bar) is computed inside the draw function from the plotted data.

---

## 1. Same Six Questions, a Person or a Chat Box

**Tag colors:** `core idea` violet, `two ways of asking` blue, `same people` magenta
**Hue family:** violet/blue

### canvas `c1` — 720×340

A dumbbell chart: one row per question, a dot for each channel, and the connecting bar *is* the gap, so the awkward questions read as long bars and the harmless ones as stubs.

- **Data (literal array, people in every hundred):**

  | Question | A person | A chat box | Gap (computed) |
  |---|---|---|---|
  | Never read the instructions | 21 | 44 | 23 |
  | Hid a small mistake | 12 | 33 | 21 |
  | Kept retrying, never asked | 18 | 37 | 19 |
  | Typed a placeholder | 26 | 41 | 15 |
  | Prefers email to a call | 61 | 63 | 2 |
  | Commutes over half an hour | 48 | 49 | 1 |

- **Title (bold 15px `P.ink`, centered, y=22):** "Same Six Questions, Asked Two Ways"
- **Geometry:** label column right-aligned at `LX=190`; axis `AX=200`, `AW=300`. Scale 0–70, ticks at 0, 20, 40, 60 as plain numbers. Rows on a 34px pitch starting `y=76`, axis rule at `y=76 + 6×34 − 14`.
- **Connector:** 6px `rgba(74,58,167,0.35)` bar from the person dot to the box dot; the two benign rows draw as near-nothing, which is the point.
- **Dots:** radius 5.5. Person = `rgba(42,120,214,0.75)` stroked `P.blue`; box = `rgba(74,58,167,0.85)` stroked `P.violet`.
- **Row labels:** 12px `P.mute`, right-aligned at `LX − 10`, vertically centered on the row.
- **Gap labels:** bold 12px `P.violet` at `AX + AW + 10`, text `'+' + (box − person)` — computed, never typed. Rows whose gap is under 3 print in `P.mute` with the word "level" instead.
- **Legend (12px `P.mute`, y=48, at `AX + 6` and `AX + 152`):** blue dot "asked by a person", violet dot "typed into a box".
- **Right panel** at `AX + AW + 70`: bold 13px `P.ink` "AVERAGE GAP" / "ACROSS THE SIX", then bold 19px `P.violet` with the computed mean (13.5) and 12px `P.mute` "points"; then 12px `P.mute` "widest question" with bold 12px `P.violet` "+23" beneath, and 12px `P.mute` "two questions barely move at all". Both figures read off the tally.
- **Axis caption (12px `P.mute`, centered under the ticks):** "people in every hundred who said yes"
- **Caption (bold 13px `P.violet`, centered, `h−10`):** "Change what is listening and the same question gets a different answer."

---

## 2. The Usage Log Says the Box Was Closer

**Tag colors:** `checked against a log` aqua, `who lands closer` yellow, `the freedom pays off` green
**Hue family:** aqua/yellow

### canvas `c2` — 720×320

Three questions, each a pair of horizontal bars with the log value drawn as a dashed rule the bars fall short of — the shortfall is visible as empty track.

- **Data (literal array):**

  | Question | Log | A person | A chat box | Person short by | Box short by |
  |---|---|---|---|---|---|
  | Never read the instructions | 56 | 21 | 44 | 35 | 12 |
  | Kept retrying, never asked | 49 | 18 | 37 | 31 | 12 |
  | Typed a placeholder | 45 | 26 | 41 | 19 | 4 |

- **Computed:** average distance from the log — person 28.3, box 9.3, ratio 3.0×. All three from the plotted arrays.
- **Title (bold 15px `P.ink`, centered, y=22):** "How Far Each Channel Fell Short of the Log"
- **Geometry:** label column right-aligned at `LX=180`; axis `AX=190`, `AW=300`. Scale 0–60, ticks 0, 20, 40, 60. Row tops at `58 + i×62`, bars 17px tall on a 21px inner pitch.
- **Bars:** person bar at `rowTop`, height 17, `rgba(201,133,0,0.40)` stroked `P.yellow`; box bar at `rowTop+21`, same height, `rgba(25,158,112,0.45)` stroked `P.aqua`.
- **Log rule:** 2px `P.ink` dashed (5/4) vertical line at the log value spanning the pair, with bold 12px `P.ink` "log 56" right-aligned above it. Behind each bar sits a `rgba(107,114,128,0.10)` track running from the axis to the rule, so the shortfall reads as empty space.
- **Shortfall labels:** 12px, printed just right of each bar's end as `'−' + (log − rate)`, in the bar's own colour. Computed by subtraction, never typed.
- **Right panel** at `AX + AW + 30`: bold 13px `P.ink` "AVERAGE DISTANCE" / "FROM THE LOG", then bold 19px `P.yellow` "28.3" + 12px `P.mute` "asked by a person", bold 19px `P.aqua` "9.3" + 12px `P.mute` "typed into a box", then bold 12px `P.aqua` "3.0× closer".
- **Under-plot notes (12px `P.mute`, left-aligned at `AX`):** "bar length is people in every hundred who said yes"; then "both channels undercount — neither one is the log".
- **Caption (bold 13px `P.aqua`, centered, `h−10`):** "Closer to the log, and still short of it. Better instrument, not a true one."

---

## 3. A Number That Doubled While Nobody Changed

**Tag colors:** `the punchline` orange, `nothing changed` yellow, `a fake trend` magenta
**Hue family:** orange/yellow with a magenta blended line

### canvas `c3` — 720×330

Two dead-flat channel lines, a rising blended line drawn between them, and the moving mix shown as a band along the axis so the cause sits under the effect.

- **Data (literals):** `boxRate = 33`, `personRate = 12`, `mix = [0.10, 0.30, 0.50, 0.70, 0.90]`.
- **Blended series:** computed in the draw function as `mix[i]*boxRate + (1 − mix[i])*personRate` → 14.1, 18.3, 22.5, 26.7, 30.9. No blended value is typed anywhere. At 1,000 answers a quarter, quarter three is 500 box answers with 165 yeses plus 500 person answers with 60 — 225 of 1,000.
- **Padding:** left 54, right 150, top 52, bottom 88. Scale 0–36, ticks 0, 12, 24, 36. `X(i) = PL + pw*(i+0.5)/5`.
- **Mix band (behind the lines):** per quarter a 34px-wide `rgba(217,89,38,0.16)` bar rising from the axis through a fixed 44px band, with bold 12px `P.orange` `Math.round(mix[i]*100) + '%'` above each bar top, and a 12px `P.mute` note "share arriving through the box".
- **Box line (`P.orange`, width 2.5, dashed 6/4):** flat at 33; right-side 12px label "chat box — 33, flat" printed from the variable.
- **Person line (`P.yellow`, width 2.5, dashed 6/4):** flat at 12; right-side 12px label "a person — 12, flat".
- **Blended line (`P.magenta`, width 3, solid, radius-4 dots):** the computed series; right-side bold 12px label "blended — " + last computed value.
- **Mid-plot annotation (bold 12px `P.magenta`, above the third dot):** `'+' + (last − first).toFixed(1) + ' points, none of it real'` — computed.
- **Big callout (right column, y=128):** bold 19px `P.magenta` with the computed relative rise (`+119%`), then 12px `P.mute` "against quarter one," and "which read 14.1" — the first value printed from the series.
- **X labels (12px `P.mute`):** "Q1" … "Q5". Under them a 12px `P.mute` line printing the formula check: every blended point equals mix × 33 + (1 − mix) × 12, residual 0.0000.
- **Caption (bold 13px `P.magenta`, centered, `h−10`):** "Two flat lines and a moving mix make a rising number out of nothing."

---

## 4. Which Channel to Trust, and When the Choice Stops Mattering

**Tag colors:** `the boundary` green, `which one to trust` orange, `common mistake` red
**Hue family:** green with orange over-the-bar rows

### canvas `c4` — 720×340

The six questions ranked by how much a mix drift alone can move their blended number, against the smallest change this team would act on — so the page ends with a line rather than an opinion.

- **Data:** the same six gaps as section 1 (23, 21, 19, 15, 2, 1), sorted descending.
- **Computed per question:** `manufactured = 0.20 × gap` → 4.6, 4.2, 3.8, 3.0, 0.4, 0.2. The count clearing the bar (4 of 6) is tallied in the draw function.
- **Title (bold 15px `P.ink`, centered, y=22):** "What a Mix Drift Alone Can Move"
- **Geometry:** label column right-aligned at `LX=200`; axis `AX=210`, `AW=250`. Scale 0–5, ticks 0–5 by 1. Rows on a 30px pitch from `y=68`, bars 18px tall.
- **Bars:** over the bar `rgba(217,89,38,0.45)` stroked `P.orange`; under it `rgba(0,131,0,0.40)` stroked `P.green`. Value printed bold 12px in the bar's colour just past its end.
- **Threshold rule:** 2.5px `P.green` dashed (5/4) vertical line at 2.0 spanning the rows, labelled bold 12px `P.green` "worth acting on: 2.0" centred above it.
- **Verdict column** at `AX + AW + 40`: 12px `P.orange` "split by channel" for rows over the bar, 12px `P.green` "safe to blend" for rows under it, then 12px `P.mute` "gap n" at `AX + AW + 160`.
- **Axis caption (12px `P.mute`, centered):** "points the blended number moves when the mix drifts 20 points"
- **Tally line (bold 12px `P.orange`, centered):** the computed count — "4 of 6 questions clear the bar on channel mix alone".
- **Caption (bold 13px `P.green`, centered, `h−10`):** "Split the questions people are shy about. Blend the rest and lose nothing."

---

## Page-specific constraints

- **No index number** anywhere on the page — not in the h1, not in the section headings.
- **Every section carries a `.src`** because every section's figures are constructed. No paragraph blocks, no data tables, no `.math-box`, no `.example` line.
- **Text stands alone; the chart adds clarity** — the text carries the argument and names every quantity it turns on; the canvas adds precision, intermediate values and per-point labels. No bullet points at a position on the canvas. See `ui-templates/README.md`.
- **Tone:** neutral and observational. Lower disclosure cost is treated as genuinely useful, not as a pathology — section 2 exists to say so. No moralising, no sensitive subject matter: the illustrations are admitting you skipped the instructions, admitting a small mistake, and asking a question you think is stupid.
- **No real product or company names**, no invented brand names. The two channels are "a person" and "a chat box".
- **Hue family per section:** 1 violet/blue, 2 aqua/yellow, 3 orange/yellow with a magenta blended line, 4 green with orange over-the-bar rows. Pills, chart fills and caption all sit in the section's family.
- **Canvas heights:** 340 / 320 / 330 / 340 at intrinsic `width="720"`.
- **Hard red `#e74c3c` is reserved** for the `.key-point` border and is not used in any chart — even though section 4 carries a red tag pill.
- **Reconciliation:** the prose names every quantity its argument turns on and each one matches the chart to the digit — gaps 23/21/19/15/2/1, average gap 13.5, rates 21/44 and 18/37 and 61/63, log values 56/49/45 with shortfalls 35/31/19 and 12/12/4, average distance from the log 28.3 and 9.3 at a ratio of 3.0×, flat rates 33 and 12, mix 10/30/50/70/90 percent, blended series 14.1 → 30.9 for a rise of 16.8 points and 119 percent at a step of 4.2 a quarter, manufactured amounts 4.6/4.2/3.8/3.0/0.4/0.2 against a bar of 2.0 with four of six clearing it. The charts add the per-row labels and the residual check.
- **Corrections applied to the earlier version of this page:** the old section 2 claimed the machine channel was "the more accurate one" and told the reader not to correct it downward, while its own chart showed the machine short of the benchmark on all three items — the page argued for treating a known undercount as the truth. Section 2 now states the same finding as "closer, and still short", and the trust question is settled by the last section instead. The old page also gave no boundary at all: it closed on a defenses workflow diagram with no statement of how large a channel gap has to be before pooling matters, so a reader had no way to tell a real hazard from a harmless one. That is now the final section, with the manufactured amount computed per question against a stated bar. The old subtitle asserted the two channels "measure two different populations", which overstates it — the people are the same, their willingness to answer is not; the wording is now about the answers, not the populations. Numbers were rebuilt from scratch: the old five-item set mixed a safety-step item into a page whose examples are meant to stay mundane, and its mean gap label (+13.6) came from a five-item set that the later sections then quietly re-used with different rates.
