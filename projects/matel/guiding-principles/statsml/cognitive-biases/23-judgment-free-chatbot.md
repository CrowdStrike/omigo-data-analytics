# The Judgment-Free Chatbot: People Own Up to a Box, Not to a Person

**Page type:** detail page — card-section template (see `statistical-paradoxes/03-berksons-paradox.html`)
**HTML title tag:** The Judgment-Free Chatbot — Cognitive Biases

**Subtitle:** Ask the same question two ways and you get two answers. The people did not change — the thing on the other end did.

---

## Section 1 — Same Six Questions, a Person or a Chat Box

**Tags:** `core idea` (violet), `two ways of asking` (blue), `same people` (magenta)

**Bullets:**
- **The setting** — one team answered six questions about how they use an internal tool
- **Two ways of asking** — half spoke to a person, half typed the same answers into a chat box
- **Held constant** — same six questions, same wording, same order, same week
- **Never read the instructions** — 21 in a hundred admitted it to the person, 44 to the box
- **Kept retrying rather than ask** — 18 to the person, 37 to the box, nineteen points apart
- **The harmless question** — preferring email to a call sits at 61 and 63, near enough level
- **So the gap belongs to the question** — the more awkward it is, the further the two land apart
- **Nobody misled the interviewer** — they left out the part that would have cost them something

**Key point:** Nothing about the people differs between the two halves. The gap measures what the question costs to answer out loud — which means the two channels are not two samples of one measurement.

**Source note (`.src`):** Illustrative Example — six constructed questions with two constructed channel rates; every gap and the average are computed in the draw function.

### Visualization — canvas `c1`, 720×340

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

## Section 2 — The Usage Log Says the Box Was Closer

**Tags:** `checked against a log` (aqua), `who lands closer` (yellow), `the freedom pays off` (green)

**Bullets:**
- **An outside answer** — three of the questions leave a trace in the tool's own usage log
- **Never read the instructions** — the log shows 56 in a hundred never opened the help page
- **What the person heard** — 21, well under half the people the log says actually did it
- **What the box heard** — 44, much closer, and still short of what the log recorded
- **Across the three questions** — the box lands 9.3 points from the log, the person 28.3
- **Three times closer** — where admitting costs something, the freer channel is the better one
- **Still not the truth** — the box undercounts all three, so it is no benchmark either
- **Why the freedom is worth having** — a channel nobody watches gets nearer to what happened

**Key point:** Where owning up costs something, the channel with nobody listening gets closer to what actually happened — that is the real value of it. It still falls short of the log, so closer is not the same as right.

**Source note (`.src`):** Illustrative Example — the log figures are stipulated, not measured; both average distances are computed from the plotted bars.

### Visualization — canvas `c2`, 720×320

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

## Section 3 — A Number That Doubled While Nobody Changed

**Tags:** `the punchline` (orange), `nothing changed` (yellow), `a fake trend` (magenta)

**Bullets:**
- **One question tracked** — hiding a small mistake at work, asked once a quarter for five quarters
- **Neither channel moved** — the box reports 33 in a hundred every quarter, the person 12
- **What did move** — the share of answers arriving through the box, 10 in a hundred up to 90
- **The blended number** — 14.1 in a hundred in the first quarter, 30.9 in the fifth
- **How a slide reads it** — the rate more than doubled, up 119 percent, a team in trouble
- **What produced it** — the 80-point swing in the mix times the 21-point gap, exactly 16.8
- **A suspiciously straight line** — 4.2 points a quarter, because the mix moved 20 a quarter
- **Nobody changed** — every person in every quarter answered exactly as they had before

**Key point:** Both channels held still and the published number more than doubled. Any series that spans a change in how the answers were collected is partly measuring that change and not the people.

**Source note (`.src`):** Illustrative Example — two constructed flat rates and a constructed rollout; every blended point is computed from the mix in the draw function.

### Visualization — canvas `c3`, 720×330

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

## Section 4 — Which Channel to Trust, and When the Choice Stops Mattering

**Tags:** `the boundary` (green), `which one to trust` (orange), `common mistake` (red)

**Bullets:**
- **Set the bar first** — name the smallest change you would act on, two points for this team
- **The arithmetic** — a 20-point drift in the mix moves the blend by a fifth of the channel gap
- **The harmless questions** — gaps of two and one manufacture 0.4 and 0.2, under the bar
- **The awkward ones** — gaps of 15 to 23 manufacture 3.0 to 4.6 points, over the bar alone
- **Where a log exists** — measure both channels against it and keep whichever lands closer
- **Where the answer costs nothing** — the channels agree, so use whichever is cheaper to run
- **Where being watched is the point** — behaviour in front of people is the thing being measured
- **A cost to admit and no log** — neither is truth, so print the channel beside the number

**Key point:** Neither channel is better in general. The box wins where the answer costs something to say out loud, the person wins where being observed is the very thing you meant to measure, and under the bar the choice does not matter at all.

**Source note (`.src`):** Illustrative Example — the two-point bar is a stated decision rule, not a measurement; every manufactured amount is computed from the section-one gaps.

### Visualization — canvas `c4`, 720×340

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

## Regeneration instructions

- **Template:** the card-section layout from `statistical-paradoxes/03-berksons-paradox.html`, matching the converted `05-clustering-illusion.html` and `01-confirmation-bias.html`. One `.card-section` per section, each an `<h2>` (1.3rem `#1a5276`, `border-bottom: 2px solid #2980b9`, 4px bottom padding) plus a `table.layout` with `td.text-col` 50% / `td.viz-col` 50%.
- **No index number** anywhere on the page — not in the h1, not in the section headings.
- **Text column order:** `.tags` pill row → `<ul>` of one-line bullets each opening `<b>label</b>` → one `.key-point` callout → `.src` note. Every section's figures are constructed, so every section carries a `.src`. No paragraph blocks, no data tables, no `.math-box`, no `.example` line.
- **Bullet form:** each is ONE line under 95 characters that does not wrap at 50% column width, `<b>bold label</b>` then an em dash then the fact.
- **Tone:** neutral and observational. Lower disclosure cost is treated as genuinely useful, not as a pathology — section 2 exists to say so with numbers. No moralising, no sensitive subject matter: the illustrations are admitting you skipped the instructions, admitting a small mistake, and asking a question you think is stupid.
- **No real product or company names**, no invented brand names. The two channels are "a person" and "a chat box".
- **Page CSS:** body system-ui, white, `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with `border-bottom: 2px solid #2980b9`, 8px bottom padding. `.subtitle` `#666` 0.95rem, 32px bottom margin. `.card-section` 40px bottom margin. `table.layout` full width, border-collapse, cells vertical-align top padding 12px, `.viz-col` centered. `ul` 0.92rem margin `8px 0 8px 20px`, `li` 4px bottom margin, `li b` `#1a5276`. `.key-point` `#f8f9fa`, `border-left: 3px solid #e74c3c`, padding 8px 12px, 0.9rem. `.src` 0.78rem `#888`. No nav, no `.nav` CSS, no back/home links, no cross-page links.
- **Tag pills:** `display:inline-block`, 0.72rem, weight 600, padding 2px 10px, radius 10px. Classes used: `.blue`, `.green`, `.red`, `.orange`, `.violet` `rgba(74,58,167,0.12)`/`#4a3aa7`, `.magenta` `rgba(213,81,129,0.14)`/`#c2426f`, `.aqua` `rgba(25,158,112,0.14)`/`#17805d`, `.yellow` `rgba(201,133,0,0.15)`/`#a06c00`.
- **Hue family per section:** 1 violet/blue, 2 aqua/yellow, 3 orange/yellow with a magenta blended line, 4 green with orange over-the-bar rows. Pills, chart fills and caption all sit in the section's family.
- **Canvas:** intrinsic `width="720"` with heights 340 / 320 / 330 / 340. CSS `width: 100%`, `border: 1px solid #e0e0e0`, radius 4px. `setup(id)` caches the logical size in `dataset` on first call, sets `style.maxWidth = 720px`, computes `scale = (cssW/720) × devicePixelRatio`, sizes the backing store to `logical × scale`, and `ctx.scale(scale, scale)` back to logical coordinates. Draws registered in `__charts`, re-run on a debounced 150ms resize.
- **Canvas font sizes:** chart title bold 15px; in-chart header bold 12–13px; body and axis labels 12px floor; the one big callout figure bold 19px; caption bold 13px.
- **Palette** (shared `P`): `blue #2a78d6`, `green #008300`, `magenta #d55181`, `yellow #c98500`, `aqua #199e70`, `orange #d95926`, `violet #4a3aa7`, `ink #1a5276`, `text #2c3e50`, `mute #6b7280`, `grid #e5e9ef`. Hard red `#e74c3c` is reserved for the `.key-point` border and is not used in any chart.
- **Determinism:** no `Math.random()` anywhere, and no seeded generator either — the `lcg` helper from the reference is deliberately omitted because every series on this page is a literal constructed array. Every printed figure (each gap, the average gap, each shortfall, both average distances and their ratio, every blended point, the rise and the relative rise, every manufactured amount, the tally clearing the bar) is computed inside the draw function from the plotted data.
- **Reconciliation:** the bullets, the key points and the chart labels state the same quantities to the digit — gaps 23/21/19/15/2/1, average gap 13.5, average distance from the log 28.3 and 9.3 at a ratio of 3.0×, blended series 14.1 → 30.9 for a rise of 16.8 points and 119 percent, manufactured amounts 4.6 down to 0.2 against a bar of 2.0.
- **Corrections applied to the earlier version of this page:** the old section 2 claimed the machine channel was "the more accurate one" and told the reader not to correct it downward, while its own chart showed the machine short of the benchmark on all three items — the page argued for treating a known undercount as the truth. Section 2 now states the same finding as "closer, and still short", and the trust question is settled by the last section instead. The old page also gave no boundary at all: it closed on a defenses workflow diagram with no statement of how large a channel gap has to be before pooling matters, so a reader had no way to tell a real hazard from a harmless one. That is now the final section, with the manufactured amount computed per question against a stated bar. The old subtitle asserted the two channels "measure two different populations", which overstates it — the people are the same, their willingness to answer is not; the wording is now about the answers, not the populations. Numbers were rebuilt from scratch: the old five-item set mixed a safety-step item into a page whose examples are meant to stay mundane, and its mean gap label (+13.6) came from a five-item set that the later sections then quietly re-used with different rates.
