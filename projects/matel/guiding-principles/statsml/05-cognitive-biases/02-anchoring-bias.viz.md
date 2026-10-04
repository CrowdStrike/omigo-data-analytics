# Anchoring Bias — Viz

**Page type:** detail page, card-section
**HTML title tag:** Anchoring Bias — Cognitive Biases
**Template:** the card-section layout from `statistical-paradoxes/03-berksons-paradox.html`, matching the approved conversion in `05-clustering-illusion.html`

House style (bullet form, tag-pill markup, text-column order, canvas DPR scaling, shared palette `P`, canvas font floors, section-header borders) lives in `ui-templates/` and is not restated here.

**Figures printed in the sibling `.txt.md` are computed here.** Changing a seed or the construction invalidates that prose — re-read the computed values and update the text to match. The prose quotes: averages 516 and 1021 with the 505-bean gap and the 37-of-40 / 39-of-40 counts (c1); typical worth $107 / $133 / $154 and deal shares 35% / 67% / 87% (c2); settlements $114k and $89k, ranges $111k–$116k and $84k–$93k, gap $25k (c3); evidence mean 19.6, path ends 16.1 and 22.6, gaps 14 and 6.5, surviving share 46% (c4); typical misses 188 / 140 / 281 and the spinner's average guess 1025 (c5).

**Determinism:** no `Math.random()`. Seeded Park–Miller LCG (`s = (s × 16807) % 2147483647`), seed 42, with a sum-of-four-uniforms bell-curve helper. Every average, count, share and gap is computed inside the draw function from the plotted arrays and printed from that variable, so no label can drift from the data beside it.

---

## 1. One Jar, Two Spins of a Wheel

**Tag colors:** `core idea` violet, `arbitrary number` blue, `it moves anyway` magenta
**Hue family:** violet/blue with a green truth line

### canvas `c1` — 720×340

Two swarms of guesses on one shared bean-count axis, one swarm per wheel spin, with the gap between their averages bracketed. The clouds overlap only at their edges — 5 of Group A's guesses reach into Group B's range and 4 of B's fall back into A's — so the separation is visible before a single number is read.

- **Data:** seeded Park–Miller LCG, seed 42. True count `TRUE = 750`. For each of 40 people per group, an unanchored belief `b = 750 + 260 · g` where `g` is a sum-of-four-uniforms approximation to a bell curve, floored at 60; the reported guess is `round(0.35 · anchor + 0.65 · b)`. Group A anchor 200, Group B anchor 1500. Group A drawn first from the shared stream, then Group B.
- **Computed and printed from the arrays:** Group A average 516, Group B average 1021, gap 505; 37 of Group A's 40 below 750, 39 of Group B's 40 above it. Group A spans 277–838, Group B 712–1352.
- **Title (bold 15px `P.ink`, centered, y=22):** "One Jar of 750 Beans, Two Spins of a Wheel"
- **Axis:** value range 0–1600 mapped across `PX = 58` to `w − 34`. Baseline at `y = 268`, 1px `#ccc`. Ticks and 12px `P.mute` labels every 200 from 0 to 1600. Axis title 12px `P.mute` centered below at `BASE + 36`: "guessed number of beans".
- **Swarm bands:** each 50px tall — Group A at `y = 50 … 100`, Group B at `y = 164 … 214`. Each dot is placed at its guessed value, with vertical position spread inside the band by a second seeded stream (seed 7) so overlapping guesses stay visible. Radius 4.5. Group A fill `rgba(74,58,167,0.50)` stroked `P.violet`; Group B fill `rgba(42,120,214,0.50)` stroked `P.blue`.
- **True count:** dashed 1.5px `P.green` vertical line (dash 5/4) at 750 running from `y = 44` to the baseline, with bold 12px `P.green` centered label "true count 750" at `y = 40`.
- **Anchor markers:** within each band a solid 2px vertical tick in the band's hue at its anchor value, with a bold 12px label in that hue just above the band: "wheel said 200" at `y = 46` left-aligned, "wheel said 1500" at `y = 160` right-aligned so it stays on canvas.
- **Average markers:** a filled diamond (7px half-width) in the band's solid hue at the group average, with bold 13px label below the band reading "average 516" at `y = 114` and "average 1021" at `y = 228`, printed from `mean()`.
- **Gap bracket:** a 2px `P.magenta` horizontal segment between the two averages at `y = 130`, with 6px end caps; the callout sits to the right of the higher average — bold 19px `P.magenta` "505 beans" above 12px `P.mute` "apart, from a wheel spin".
- **Band annotations (12px `P.mute`, left-aligned at `PX`, `y = 242` and `256`):** "37 of 40 guessed below the truth" and "39 of 40 guessed above it", both counts scanned from the plotted arrays.
- **Caption (bold 13px `P.violet`, centered, `h − 10`):** "Same jar, same eyes. The number heard first chose the neighbourhood."

---

## 2. The Price With a Line Through It

**Tag colors:** `where you meet it` orange, `discount tags` yellow, `manufactured deal` magenta
**Hue family:** orange/yellow against a mute majority

### canvas `c2` — 720×330

Three swarm rows on one shared dollar axis, one row per version of the tag, with the $120 asking price as a fixed vertical line. Dots to the right of the line are shoppers who think the jacket is worth more than it costs, so the growing orange majority is the anchoring effect on screen.

- **Data:** seeded LCG, seed 42. Asking price `ASK = 120`. For each of 60 shoppers per row, an own valuation `o = 112 + 42 · g`, floored at 25; with a crossed-out reference the stated worth is `0.28 · ref + 0.72 · o`, without one it is `o`. Rows drawn in order from the shared stream: no reference, `ref = 180`, `ref = 260`.
- **Computed and printed from the arrays:** typical worth $107 / $133 / $154; good-deal counts 21, 40 and 52 out of 60, printed as 35% / 67% / 87%. No row is degenerate — even the strongest tag leaves 8 shoppers unconvinced.
- **Title (bold 15px `P.ink`, centered, y=22):** "One $120 Jacket, Three Versions of the Tag"
- **Axis:** value range 0–300 across `PX = 128` to `w − 92`. Ticks and 12px `P.mute` labels every 50 with a leading `$`. Baseline 1px `#ccc` at `y = 254`, axis title 12px `P.mute` centered below: "what the shopper says the jacket is worth".
- **Row labels (bold 12px, right-aligned at `PX − 12`):** "no “was” price" in `P.mute`, "was $180" in `P.yellow`, "was $260" in `P.orange`, at each row's centre line.
- **Rows:** centres at `y = 82, 148, 214`, each a 46px-tall band with seeded vertical spread. Dot radius 4. A dot at or above $120 is filled `rgba(217,89,38,0.55)` stroked `P.orange`; below $120 it is filled `rgba(107,114,128,0.28)` stroked `P.mute`.
- **Asking-price line:** solid 2px `P.ink` vertical line at $120 spanning `y = 44` to the baseline, labelled bold 12px `P.ink` "asked: $120" centered at `y = 38`.
- **Typical-worth markers:** a filled diamond (6px half-width) in the row's hue at the row mean, with bold 12px label above the band "typical: $107" / "$133" / "$154", printed from `mean()`.
- **Share column (left-aligned at `RX + 12`):** per row a bold 19px figure in the row's hue — 35%, 67%, 87% — with 12px `P.mute` "call it" / "a deal" on two lines beneath the first row only, so the column does not become a table.
- **Caption (bold 13px `P.orange`, centered, `h − 10`):** "The jacket is unchanged. The reference price is the product."

---

## 3. Whoever Names a Figure First

**Tag colors:** `negotiation` magenta, `who opens first` blue, `the range is set` green
**Hue family:** magenta/blue with a green fair line

### canvas `c3` — 720×320

Two rows on one shared salary axis: the opening figure as a hollow marker, the eight settlements as filled dots, and an arrow from opener to settlement cluster showing which way each side had to travel. The fair-value line sits between the two clusters, and neither cluster contains it.

- **Data:** seeded LCG, seed 42. Private fair value `MKT = 100` (thousands). For each of 8 conversations per row, a private sense of fair `m = 100 + 6 · g`; the settlement is `round(0.5 · open + 0.5 · m)`. Row 1 opens at 130, row 2 at 78, drawn in that order from the shared stream.
- **Computed and printed from the arrays:** candidate-opens settlements 113, 115, 113, 114, 112, 116, 115, 111 — range 111–116, average $114k. Employer-opens settlements 93, 91, 87, 84, 84, 92, 88, 90 — range 84–93, average $89k. Gap between averages $25k.
- **Title (bold 15px `P.ink`, centered, y=22):** "Both Sides Privately Call $100k Fair"
- **Axis:** value range 70–140 across `PX = 132` to `w − 40`. Ticks and 12px `P.mute` labels every 10 formatted "$70k" … "$140k". Baseline 1px `#ccc` at `y = 244`, axis title 12px `P.mute` centered below: "salary the conversation settled on".
- **Row labels (bold 12px, right-aligned at `PX − 12`, two lines each):** "candidate" / "opens first" in `P.magenta`; "employer" / "opens first" in `P.blue`.
- **Fair line:** dashed 1.5px `P.green` (dash 5/4) vertical line at 100 from `y = 54` to the baseline, bold 12px `P.green` label "both call $100k fair" centred at the top.
- **Rows:** centres at `y = 96` and `y = 178`, each a 40px band with seeded vertical spread. Settlement dots radius 5, row 1 `rgba(213,81,129,0.55)` stroked `P.magenta`, row 2 `rgba(42,120,214,0.55)` stroked `P.blue`.
- **Opening markers:** a hollow 7px circle in the row's hue with 2px stroke at the opening figure, plus a bold 12px label in that hue above it: "opens $130k" / "opens $78k".
- **Travel arrows:** a 2px arrow in the row's hue from the opening marker to the row average, drawn along the row centre, showing the distance the conversation actually moved.
- **Average markers:** a filled diamond (6px half-width) in the row's hue at the row average with bold 13px label below the band: "settles $114k" / "settles $89k".
- **Gap callout (left-aligned in the strip between the rows at `y = 132`):** bold 19px `P.magenta` "$25k" at `PX + 6` followed by 12px `P.mute` "decided by who spoke first" at `PX + 62` — computed as the difference of the two averages.
- **Caption (bold 13px `P.magenta`, centered, `h − 10`):** "Neither cluster reaches the figure both sides privately called fair."

---

## 4. Adjustment Stops Where It Looks Defensible

**Tag colors:** `why it lingers` aqua, `revising too little` yellow, `two whiteboards` orange
**Hue family:** aqua evidence with yellow/orange paths

### canvas `c4` — 720×330

Two estimate paths converging toward the same measured evidence but stopping short of it and of each other. The shrinking-yet-open gap is the visible claim: adjustment is real and incomplete.

- **Data:** seeded LCG, seed 42. Six trial-run readings `ev[k] = 20 + 1.2 · g`, giving 19.0, 20.1, 19.4, 19.7, 18.8, 20.4 with mean 19.6. Team A starts at 12, Team B at 26; after each reading, `est ← est + 0.12 · (ev[k] − est)`.
- **Computed and printed from the paths:** Team A ends at 16.1, Team B at 22.6, final gap 6.5 days against a starting gap of 14.0 — 46% of the original gap remaining. Team A finishes 3.5 days short of the evidence mean, Team B 3.0 days beyond it.
- **Title (bold 15px `P.ink`, centered, y=22):** "Two Whiteboards, Six Identical Trial Runs"
- **Plot box:** `PX = 62`, `PY = 54`, right edge `w − 128` (the right strip carries the end labels), baseline `y = 252`. Y range 10–28 days with 12px `P.mute` gridline labels every 4 days on `P.grid` 1px lines. X range rounds 0–6, ticks labelled 12px `P.mute` "start", "1" … "6", axis title 12px `P.mute` centered below: "trial runs seen".
- **Evidence:** horizontal dashed 1.5px `P.aqua` line (dash 6/4) at 19.6 with bold 12px `P.aqua` right-aligned label "measured average 19.6 days"; each reading a 4.5px `rgba(25,158,112,0.55)` dot stroked `P.aqua` at its round.
- **Paths:** Team A 2.5px `P.yellow` polyline with 5px dots at each round; Team B 2.5px `P.orange` polyline with 5px dots. Start points drawn as hollow 7px circles in the same hues.
- **Gap shading:** a `rgba(107,114,128,0.10)` band filled between the two paths across the full plot, so the wedge closing but never meeting is visible without reading numbers.
- **Start bracket:** at round 0, a 2px `P.mute` vertical segment between the two starts with bold 12px `P.mute` "14 days apart" to its right.
- **End bracket:** at round 6, a 2px `P.magenta` vertical segment between the two ends, with bold 19px `P.magenta` "6.5" and 12px `P.mute` "days apart," / "still" on two lines in the right strip, placed relative to the bracket midpoint.
- **End labels (bold 12px in each path's hue, right strip at `RX + 10`):** "Team A 16.1" in `P.yellow`, "Team B 22.6" in `P.orange`, each vertically at its own path end.
- **Surviving-share line:** bold 12px `P.magenta` at `PX + 96`, `y = 70` — "46% of the opening gap survived all six readings", computed as `endGap / startGap` and placed in the empty strip above the paths.
- **Caption (bold 13px `P.aqua`, centered, `h − 10`):** "They moved the right way and stopped too soon — that residue is the anchor."

---

## 5. A Counted Jar Versus a Spinner

**Tag colors:** `the real distinction` green, `informative reference` aqua, `phantom` magenta
**Hue family:** green versus magenta over a mute baseline

### canvas `c5` — 720×320

Three bars of typical miss against a dashed baseline set by guessing with no reference at all. One reference pushes the bar below the baseline, the other pushes it above — same mechanism, opposite verdicts, and the verdict is readable without any number.

- **Data:** seeded LCG, seed 42. True count 750, 40 guesses per condition, unanchored belief `b = 750 + 260 · g` floored at 60. With a reference the guess is `round(0.35 · ref + 0.65 · b)`, without one it is `round(b)`. Conditions drawn in order: no reference, `ref = 700`, `ref = 1500`.
- **Computed and printed from the arrays:** typical miss (mean absolute distance from 750) is 188 with no reference, 140 with the counted jar, 281 with the spinner. Average guess 687 / 741 / 1025 — the spinner's average sits 275 beans past the truth. Guesses landing within 150 of the truth: 18, 23 and 9 out of 40.
- **Title (bold 15px `P.ink`, centered, y=22):** "How Far Off, With and Without a Reference Number"
- **Plot box:** `PX = 92`, baseline `y = 236`, top `y = 62`. Y axis is typical miss in beans, 0–320, with 12px `P.mute` gridline labels every 80 on `P.grid` lines.
- **Bars:** three columns evenly spaced across `PX … w − 40`, width capped at 96. No reference `rgba(107,114,128,0.30)` stroked `P.mute`; counted jar `rgba(0,131,0,0.45)` stroked `P.green`; spinner `rgba(213,81,129,0.45)` stroked `P.magenta`. Each bar carries its miss as a bold 19px figure in its own hue just above the bar top.
- **Y-axis note:** 12px `P.mute` "beans off" right-aligned at `PX − 8`, `TOP − 16`.
- **Baseline rule:** dashed 1.5px `P.mute` horizontal line (dash 5/4) at the no-reference miss, extended across all three bars, labelled bold 12px `P.mute` "guessing with nothing to go on" right-aligned at `RX`.
- **Column captions (below the baseline, centered under each bar):** bold 12px in the bar's hue on the first line — "no reference" / "counted jar: 700" / "spinner: 1500" — then 12px `P.mute` second line "the honest baseline" / "measured on a like jar" / "measured on nothing".
- **Verdict row (bold 13px, under the column captions):** `P.mute` "—" for the baseline column, `P.green` "reference earns its pull" for the counted jar, `P.magenta` "phantom: pull without content" for the spinner. Assigned by comparing each bar to the baseline in the draw function, not hardcoded.
- **Caption (bold 13px `P.green`, centered, `h − 10`):** "Ask where the number was measured, not how hard it pulled."

---

## Page-specific constraints

- **Five `.card-section` blocks, one canvas per section, no more.** No index number anywhere on the page.
- **Canvas heights per chart:** 340, 330, 320, 330, 320.
- **Text stands alone; the chart adds clarity** — the text carries the argument and names every quantity it turns on; the canvas adds precision, intermediate values and per-point labels. No bullet points at a position on the canvas. See `ui-templates/README.md`.
- **Language:** layman-first. No jargon from the banned list appears — no p-value, prior, posterior, correlation coefficient, confidence interval, variance, holdout, or pipeline framing. The old page's ML vocabulary (learning rate, grid search, AUC, epochs, batch size, `α = 0.05`, `n = 30`, 80/20 split, BERT defaults) is gone entirely; the scope is analyst and everyday psychology.
- **Scope boundary:** this page covers the single salient number — one figure, consciously seen, at one moment. Volume-based reference-setting from repeated curated exposure belongs to `19-reference-class-substitution` and is deliberately absent here. No cross-links of any kind.
- **Section titles name content**, never a role. "The Trap", "Where It Strikes", "In Data Science" and "Pipeline Defense" from the old page were all replaced.
- **Last section is the boundary case** and must stay precise: it does not claim every reference point is bias. A measured reference from a comparable case genuinely improves the answer (its bar sits below the no-reference baseline); the bias is a reference with no bearing on the question that still moves the answer (its bar sits above it). The discriminator is provenance, not strength of pull. The bullets name the three typical misses — 188 cold, 140 with the counted jar, 281 with the spinner — so the verdict is readable without the bars.
- **`.red` tag pill is not used** — no section on this page is a genuine alarm. Hard red `#e74c3c` appears only as the `.key-point` left border.
- **Colour rotation across sections is a requirement:** section 1 violet/blue with a green truth line, section 2 orange/yellow against a mute majority, section 3 magenta/blue with a green fair line, section 4 aqua evidence with yellow/orange paths, section 5 green versus magenta over a mute baseline.
- **Lead chart shows the effect, not a description of it.** Two swarms of guesses on one axis with a bracketed gap between their averages; the separation is visible before any number is read. No second-order construction is used as the opening figure.
- **Non-degenerate constructions checked.** The jacket rows deliberately avoid a 0% or 100% good-deal share — the strongest tag still leaves 8 of 60 shoppers unconvinced. The salary rows both exclude the fair value rather than straddling it. The estimate paths converge but never meet, so the final gap is neither 0 nor unchanged.
- **Corrections applied to the old version of this page:** its second chart drew two bell curves with a hardcoded "~30pp gap from anchor alone" label next to curves that were themselves hardcoded means (35 and 65), so nothing on it was computed — every figure here is derived from plotted data. Its first chart asserted "3x enrichment" between a 10% base rate and a 30% observation with no data behind either number. Its grid-search heatmap used sparse *random* cells, making the figure non-reproducible. The `α = 0.05` / `n = 30` / `80/20` default-value table conflated "a convention chosen for another problem" with anchoring bias and has been dropped; the legitimate-reference-versus-phantom distinction now carries that ground properly in section 5.
