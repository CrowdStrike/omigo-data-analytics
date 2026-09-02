# Anchoring Bias: The First Number Sticks

**Page type:** detail page — card-section template (see `statistical-paradoxes/03-berksons-paradox.html`)
**HTML title tag:** Anchoring Bias — Cognitive Biases

**Subtitle:** Say a number out loud before someone estimates, and their estimate moves toward it — even when everyone in the room knows the number came from nowhere.

---

## Section 1 — One Jar, Two Spins of a Wheel

**Tags:** `core idea` (violet), `arbitrary number` (blue), `it moves anyway` (magenta)

**Bullets:**
- **The setup** — one jar of beans on a table, and forty people asked how many are inside
- **Before guessing** — each person spins a wheel and reads out whatever number it lands on
- **Group A's spin** — the wheel gave 200, a number with no connection to the jar at all
- **Group B's spin** — the same wheel gave 1500, equally unconnected and equally arbitrary
- **Group A's guesses** — cluster low, averaging 516, and 37 of the 40 sit under the true count
- **Group B's guesses** — cluster high, averaging 1021, and 39 of the 40 sit above it
- **The gap** — 505 beans between the two averages, put there entirely by a wheel spin

**Key point:** Both groups looked at the identical jar and knew the wheel was a wheel. The number they read out first still decided which half of the answer space they searched.

**Source note (`.src`):** Illustrative Example — forty seeded guesses per group; every average and count printed on the chart is scanned from the plotted dots.

### Visualization — canvas `c1`, 720×340

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

## Section 2 — The Price With a Line Through It

**Tags:** `where you meet it` (orange), `discount tags` (yellow), `manufactured deal` (magenta)

**Bullets:**
- **The tag** — a jacket priced at $120, and sixty shoppers asked what it is actually worth
- **No crossed-out price** — the typical answer is $107, and 35% call $120 a good deal
- **Tag reads "was $180"** — the typical answer climbs to $133, and 67% call it a good deal
- **Tag reads "was $260"** — the typical answer reaches $154, and 87% call it a good deal
- **The jacket never changed** — same cloth, same stitching, same $120 asked for it
- **What changed** — a struck-through number the shopper knows the shop chose itself
- **Why the shop prints it** — that crossed-out number is the cheapest part of the jacket

**Key point:** The struck-through price is not information about the jacket; it is the shop choosing where your judgement starts. It works even on shoppers who say out loud that it is a marketing trick.

**Source note (`.src`):** Illustrative Example — sixty seeded shoppers per tag; the typical worth and the good-deal share are tallied on the chart from the plotted dots.

### Visualization — canvas `c2`, 720×330

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

## Section 3 — Whoever Names a Figure First

**Tags:** `negotiation` (magenta), `who opens first` (blue), `the range is set` (green)

**Bullets:**
- **The room** — a hiring conversation where both sides privately think $100k is fair
- **Candidate opens at $130k** — eight such conversations settle between $111k and $116k
- **Employer opens at $78k** — eight otherwise identical ones settle between $84k and $93k
- **The averages** — $114k when the candidate spoke first, $89k when the employer did
- **Nothing about the job differed** — same role, same skills, same private sense of fair
- **Every settlement leans** — toward whichever side put a figure on the table first
- **Why both sides stall** — each waits for the other to hand over the number that sets the range

**Key point:** The opening figure does not persuade anyone that it is correct — it does not have to. It only has to become the thing both sides then argue away from, and the settlement lands nearer to it than to fair.

**Source note (`.src`):** Illustrative Example — eight seeded conversations per opener; the settlement range and average are scanned from the plotted dots.

### Visualization — canvas `c3`, 720×320

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

## Section 4 — Adjustment Stops Where It Looks Defensible

**Tags:** `why it lingers` (aqua), `revising too little` (yellow), `two whiteboards` (orange)

**Bullets:**
- **The question** — how many days a job takes, with two teams estimating it separately
- **Team A's whiteboard** — opened at 12 days, a figure nobody could source afterwards
- **Team B's whiteboard** — opened at 26 days, sourced no better, and 14 days away from A
- **Then real data** — six trial runs, the same six for both teams, averaging 19.6 days
- **Both teams revised** — every reading pulled each estimate toward what was measured
- **Where they stopped** — A at 16.1 days, B at 22.6, still 6.5 days apart after all six
- **Adjustment ran out** — each team stopped once its number looked defensible, not once it fit
- **The residue** — 46% of a gap built from two unsourced figures survived six measurements

**Key point:** People do adjust away from an anchor, and they adjust in the right direction. They stop early, so the anchor keeps a share of the final answer no matter how much evidence arrives afterwards.

**Source note (`.src`):** Illustrative Example — six seeded trial runs and a fixed revision step; every day figure on the chart is computed from the plotted path.

### Visualization — canvas `c4`, 720×330

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

## Section 5 — A Counted Jar Versus a Spinner

**Tags:** `the real distinction` (green), `informative reference` (aqua), `phantom` (magenta)

**Bullets:**
- **The same jar** — 750 beans inside, guessed forty times under three conditions
- **No reference number** — guesses miss by 188 beans on average, the honest baseline
- **A counted jar of 700** — a genuinely similar jar, counted; the typical miss falls to 140
- **A spinner landing on 1500** — the typical miss rises to 281, worse than guessing cold
- **Both numbers moved the answers** — that alone does not tell you which one was bias
- **What separates them** — whether the number was measured on something comparable
- **The counted jar qualifies** — it says something real about how big a jar of beans gets
- **The spinner does not** — it says nothing, and still pulls the average 275 past the truth

**Key point:** Being pulled by a first number is not the bias — a measured reference from a comparable case earns the pull, and ignoring it makes you worse. The bias is being pulled by a number that carries no information about the question, and the test is where the number came from, not how strongly it moved you.

**Source note (`.src`):** Illustrative Example — forty seeded guesses per condition against the same 750-bean jar; each typical miss is averaged on the chart from the plotted guesses.

### Visualization — canvas `c5`, 720×320

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

## Regeneration instructions

- **Template:** the card-section layout from `statistical-paradoxes/03-berksons-paradox.html`, matching the approved conversion in `05-clustering-illusion.html`. Five `.card-section` blocks, each an `<h2>` (1.3rem `#1a5276`, `border-bottom: 2px solid #2980b9`, 4px bottom padding) plus a `table.layout` with one row: `td.text-col` 50% / `td.viz-col` 50%. One canvas per section, no more. No index number anywhere on the page.
- **Text column order:** `.tags` pill row → `<ul>` of one-line bullets each opening `<b>label</b>` then an em dash → one `.key-point` → `.src` note (present in all five sections here, because every figure on the page is constructed). No paragraph blocks, no `.example` lines, no data tables, no philosophy box.
- **Bullet form:** each bullet is ONE line that does not wrap at 50% column width — verified at ≤95 characters including the bold label. Counts follow content: 7, 7, 7, 8, 8. Nothing padded, nothing restated between a bullet and the key point.
- **Language:** layman-first. No jargon from the banned list appears — no p-value, prior, posterior, correlation coefficient, confidence interval, variance, holdout, or pipeline framing. The old page's ML vocabulary (learning rate, grid search, AUC, epochs, batch size, `α = 0.05`, `n = 30`, 80/20 split, BERT defaults) is gone entirely; the scope is analyst and everyday psychology.
- **Scope boundary:** this page covers the single salient number — one figure, consciously seen, at one moment. Volume-based reference-setting from repeated curated exposure belongs to `19-manufactured-reference-frame` and is deliberately absent here. No cross-links of any kind.
- **Section titles name content**, never a role. "The Trap", "Where It Strikes", "In Data Science" and "Pipeline Defense" from the old page were all replaced.
- **Last section is the boundary case** and must stay precise: it does not claim every reference point is bias. A measured reference from a comparable case genuinely improves the answer (miss falls from 188 to 140); the bias is a reference with no bearing on the question that still moves the answer (miss rises to 281). The discriminator is provenance, not strength of pull.
- **Page CSS:** body system-ui, white, `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with `border-bottom: 2px solid #2980b9`, 8px bottom padding. `.subtitle` `#666` 0.95rem, 32px bottom margin. `.card-section` 40px bottom margin. `table.layout` full width, border-collapse, td vertical-align top padding 12px, `.text-col`/`.viz-col` 50% each, `.viz-col` `text-align: center`. `ul` 0.92rem margin `8px 0 8px 20px`, `li` 4px bottom margin, `li b` `#1a5276`. `.key-point` `#f8f9fa` background, `border-left: 3px solid #e74c3c`, padding 8px 12px, 0.9rem. `.src` 0.78rem `#888`. No nav, no `.nav` CSS, no back/home links.
- **Tag pills:** `display:inline-block`, 0.72rem, weight 600, padding 2px 10px, radius 10px. Classes used: `.blue` `rgba(26,82,118,0.12)`/`#1a5276`, `.green` `rgba(39,174,96,0.15)`/`#27ae60`, `.orange` `rgba(230,126,34,0.15)`/`#e67e22`, `.violet` `rgba(74,58,167,0.12)`/`#4a3aa7`, `.magenta` `rgba(213,81,129,0.14)`/`#c2426f`, `.aqua` `rgba(25,158,112,0.14)`/`#17805d`, `.yellow` `rgba(201,133,0,0.15)`/`#a06b00`. `.red` is not used — no section on this page is a genuine alarm.
- **Colour rotation across sections is a requirement:** section 1 violet/blue with a green truth line, section 2 orange/yellow against a mute majority, section 3 magenta/blue with a green fair line, section 4 aqua evidence with yellow/orange paths, section 5 green versus magenta over a mute baseline. Hard red `#e74c3c` appears only as the `.key-point` left border.
- **Canvas:** intrinsic `width="720"` plus per-chart height (340, 330, 320, 330, 320). CSS `width: 100%`, `border: 1px solid #e0e0e0`, radius 4px. `setup(id)` caches the logical size in `dataset` on first call (because `canvas.width` overwrites the attribute), sets `style.maxWidth = 720px`, computes `scale = (cssW / 720) × devicePixelRatio`, sizes the backing store to `logical × scale`, and `ctx.scale(scale, scale)` back to logical coordinates. Draws are registered in `__charts` and re-run on a debounced (150ms) resize.
- **Canvas fonts:** chart title bold 15px; in-chart header and inline labels bold 12–13px; plain labels 12px floor; one big callout figure per chart at bold 19px; caption bold 13px ending every chart.
- **Palette** (shared `P` object): `blue #2a78d6`, `green #008300`, `magenta #d55181`, `yellow #c98500`, `aqua #199e70`, `orange #d95926`, `violet #4a3aa7`, `ink #1a5276`, `text #2c3e50`, `mute #6b7280`, `grid #e5e9ef`.
- **Determinism:** no `Math.random()`. Seeded Park–Miller LCG (`s = (s × 16807) % 2147483647`), seed 42, with a sum-of-four-uniforms bell-curve helper. Every average, count, share and gap is computed inside the draw function from the plotted arrays and printed from that variable, so no label can drift from the data beside it.
- **Lead chart shows the effect, not a description of it.** Two swarms of guesses on one axis with a bracketed gap between their averages; the separation is visible before any number is read. No second-order construction is used as the opening figure.
- **Non-degenerate constructions checked.** The jacket rows deliberately avoid a 0% or 100% good-deal share — the strongest tag still leaves 8 of 60 shoppers unconvinced. The salary rows both exclude the fair value rather than straddling it. The estimate paths converge but never meet, so the final gap is neither 0 nor unchanged.
- **Corrections applied to the old version of this page:** its second chart drew two bell curves with a hardcoded "~30pp gap from anchor alone" label next to curves that were themselves hardcoded means (35 and 65), so nothing on it was computed — every figure here is derived from plotted data. Its first chart asserted "3x enrichment" between a 10% base rate and a 30% observation with no data behind either number. Its grid-search heatmap used sparse *random* cells, making the figure non-reproducible. The `α = 0.05` / `n = 30` / `80/20` default-value table conflated "a convention chosen for another problem" with anchoring bias and has been dropped; the legitimate-reference-versus-phantom distinction now carries that ground properly in section 5.
