# Automation Bias: A Machine Said It, So Nobody Checked

**Page type:** detail page — card-section template (see `05-cognitive-biases/25-mere-exposure-effect.html`)
**HTML title tag:** Automation Bias — Cognitive Biases

**Subtitle:** An answer gets easier to accept once a machine produces it, and the checking that would have caught it quietly stops happening.

---

## Section 1 — Same Suggestions, One Label Changed

**Tags:** `core idea` (violet), `identical answers` (blue), `only the badge moved` (magenta)

**Bullets:**
- **The setup** — 240 suggestions land in a review queue, and a reviewer either accepts or rejects each one
- **The suggestions** — 190 of them are right and 50 are wrong, and that split is the same for both reviewers
- **The only difference** — one reviewer is told a colleague wrote them, the other that a tool produced them
- **Told a colleague wrote it** — 31 of the 50 wrong ones get caught, so 19 slip through to the customer
- **Told a tool produced it** — only 8 get caught, and 42 wrong suggestions go out unchallenged
- **The badge alone** — waves through 23 extra wrong answers, more than twice as many as the other reviewer
- **It also lifts the good ones** — 98% of correct suggestions accepted against 87%, which looks like a win
- **What that costs** — 18% of everything accepted is wrong, against 10% under the sceptical reviewer

**Key point:** The suggestions were identical, so nothing about the accepting reviewer's higher throughput reflects better work arriving. The machine label bought agreement, and it bought it on the wrong answers just as readily as the right ones.

**Source note (`.src`):** Illustrative Example — 240 seeded suggestions reviewed twice; every count and share is tallied in the draw function.

### Visualization — canvas `c1`, 720×340

Two review outcomes for one identical pool of suggestions, with the 50 wrong ones split into caught and waved through.

- **Data:** seeded Park–Miller LCG, seed 42. Each suggestion is correct with probability `ACC = 0.75`. A wrong suggestion is caught with probability `CATCH_H = 0.62` under the colleague label and `CATCH_M = 0.22` under the tool label; a correct one is accepted with probability `OK_H = 0.86` and `OK_M = 0.97`. One `rng()` draw per reviewer per suggestion, so both reviewers see the same pool.
- **Computed:** pool 240 → **190 correct, 50 wrong**. Wrong caught: **31 (62%)** colleague-labelled, **8 (16%)** tool-labelled → waved through **19** and **42**, a difference of **23** and a ratio of **2.2×**. Correct accepted: **166 (87%)** and **186 (98%)**. Share of all accepted work that is wrong: **10.3%** and **18.4%**. All read off the tallies in the draw function.
- **Title (bold 15px `P.ink`, centered, y=21):** "One Pool of Suggestions, Reviewed Twice"
- **Upper block:** bold 12px `P.ink` header "THE 50 WRONG SUGGESTIONS — WHAT EACH REVIEWER DID WITH THEM" at y=48. Two horizontal stacked bars, 30px tall on a 52px pitch from y=64, spanning `AX=196 … w−58` and scaled so the full 50 fills the track. Caught segment `rgba(25,158,112,0.45)` stroked `P.aqua`; waved-through segment `rgba(213,81,129,0.50)` stroked `P.magenta`.
- **Row labels** (12px `P.mute`, right-aligned at `AX−10`, two lines): "told a colleague / wrote them" and "told a tool / produced them". In-bar counts bold 13px white where the segment is wide enough, else bold 12px in the segment hue just outside it; each row ends with bold 19px `P.magenta` giving the waved-through count.
- **Difference bracket:** 2px `P.magenta` vertical bracket at the right of the two waved-through ends with bold 12px `P.magenta` "23 more wrong answers, 2.2×" beside it.
- **Lower block:** bold 12px `P.ink` header "SHARE OF EVERYTHING ACCEPTED THAT IS WRONG" at y=196. Two bars 26px tall on a 40px pitch from y=212 over a 0–25% axis across the same span, `rgba(74,58,167,0.40)`/`P.violet` for the colleague row and `rgba(213,81,129,0.50)`/`P.magenta` for the tool row, each labelled bold 19px in its hue with the computed percentage and 12px `P.mute` with the accepted total.
- **Note line** (12px `P.mute`, left-aligned at `AX`, y=298): "identical suggestions, identical reviewer skill — only the badge differs".
- **Caption (bold 13px `P.violet`, centered, `h−10`):** "Agreement went up. Accuracy went down. Nothing arrived that was any better."

---

## Section 2 — Handing It Over Helps on the Routine Work Only

**Tags:** `the useful half` (aqua), `two kinds of case` (green), `pick per case` (blue)

**Bullets:**
- **The work** — a hundred cases a day, of which 85 are routine and 15 are unusual in some way
- **What the tool does well** — 95 of every 100 routine cases right, because routine is what it was trained on
- **What the tool does badly** — 40 of every 100 unusual cases right, worse than a coin on the hard ones
- **What a person does** — 80% on routine and 75% on unusual, steady but never brilliant at either
- **Person handles everything** — 79.3 cases right a day, which is the mark the tool has to beat
- **Tool handles everything** — 86.8 right, a real gain, and it is this gain that earns the trust
- **Tool on routine, person on unusual** — 92.0 right, and no better arrangement of these two exists
- **What blanket trust costs** — 9 wrong cases a day on unusual work against 3.8, all in the hard cases

**Key point:** Handing everything to the tool genuinely beats doing everything by hand, which is why it feels right. The 5.2 cases a day lost to it are all in the small hard slice, so the arrangement that looks best in aggregate is the one that fails hardest exactly where failure matters.

**Source note (`.src`):** Illustrative Example — constructed accuracy rates on a 100-case day; every daily total and error count is computed in the draw function from those rates.

### Visualization — canvas `c2`, 720×330

Three arrangements of the same hundred cases, each bar split into the routine and unusual work, with the errors on unusual cases called out.

- **Data (constructed rates, no PRNG):** `ROUTINE = 0.85`, `UNUSUAL = 0.15` of the day's 100 cases. Tool accuracy `0.95` routine / `0.40` unusual; person accuracy `0.80` routine / `0.75` unusual.
- **Computed:** person only `85×0.80 + 15×0.75 = 68.0 + 11.25 =` **79.3**; tool only `85×0.95 + 15×0.40 = 80.75 + 6.0 =` **86.8**; tool on routine and person on unusual `80.75 + 11.25 =` **92.0**. Selective minus blanket = **5.2** cases a day. Wrong unusual cases: blanket `15×0.60 =` **9.0**, selective `15×0.25 =` **3.8**. Because 0.95 > 0.80 on routine and 0.75 > 0.40 on unusual, the selective row is the best of the four possible pairings — verified by scanning all four in the draw function rather than asserted.
- **Title (bold 15px `P.ink`, centered, y=21):** "One Hundred Cases a Day, Three Ways to Handle Them"
- **Rows:** three horizontal bars, 34px tall on a 62px pitch from y=64, spanning `AX=188 … w−150`, scaled 0–100 cases. Each bar is two stacked segments — cases got right on routine work in `rgba(25,158,112,0.45)`/`P.aqua`, cases got right on unusual work in `rgba(0,131,0,0.45)`/`P.green` — so the bar's length *is* the daily total. The remainder of the track to 100 is filled `rgba(107,114,128,0.08)`.
- **Row labels** (12px `P.mute`, right-aligned at `AX−10`, two lines each): "person handles / every case", "tool handles / every case", "tool on routine, / person on unusual".
- **Totals:** bold 19px at each bar's end — `P.mute` for the first row, `P.aqua` for the second, `P.green` for the best row, which is also ringed with a 2px `P.green` outline and labelled bold 12px `P.green` "best of the four possible pairings".
- **Gain bracket:** 2px `P.green` bracket spanning the second and third bar ends with bold 12px `P.green` "+5.2 cases a day" beside it.
- **Side panel** at `w−138`: bold 12px `P.ink` "WRONG ON THE 15 / UNUSUAL CASES", then bold 19px `P.orange` "9.0" over 12px `P.mute` "tool handles / every case", and bold 19px `P.green` "3.8" over 12px `P.mute` "person takes / the unusual ones".
- **Note line** (12px `P.mute`, centered, `h−32`): "the tool is better on 85 cases and much worse on 15 — one number a day hides both facts".
- **Caption (bold 13px `P.aqua`, centered, `h−10`):** "The gain is real, and it is entirely in the easy cases."

---

## Section 3 — The Checking Fades Before the Tool Breaks

**Tags:** `how it goes wrong` (orange), `nobody decided this` (yellow), `nothing watching` (red)

**Bullets:**
- **The arrangement** — 200 outputs a week get a spot check, and anything wrong that is spotted gets fixed
- **The first twelve weeks** — the tool is right 95 times in 100, so almost every check finds nothing
- **What that does to the checking** — it drifts from 60% of outputs in week one down to 4% by week 12
- **Nobody decided to stop** — each week on its own felt like time spent looking at things that were fine
- **Week 13** — an upstream change drops the tool to 60% right, and weekly errors jump from 12 to 81
- **What the faded checking catches** — 10 of the 651 errors made over the eight broken weeks
- **So 641 bad outputs** — 98% of them, reach customers, and no week looks unusual from the inside
- **Had checking stayed at 60%** — around 391 of those 651 would have been caught instead of 10

**Key point:** The tool breaking is the ordinary part. The costly part is that the instrument which would have shown it had already been switched off, and it was switched off by twelve weeks of correctly observing that there was nothing to find.

**Source note (`.src`):** Illustrative Example — 20 seeded weeks of 200 outputs; every weekly count and both totals are computed in the draw function.

### Visualization — canvas `c3`, 720×340

Weekly errors as bars with the catch line over them, the checking rate falling away behind, and the break marked where the two diverge.

- **Data:** seeded LCG, seed 2024. Accuracy `0.95` for weeks 1–12 and `0.60` from week 13; 200 outputs a week. Checking rate `max(0.02, 0.60 × e^(−(t−1)/4.0))`, and each error is caught with that probability.
- **Computed:** checking **60%** in week 1, **4%** in week 12. Errors average **12** a week before the break and **81** after, and the eight broken weeks make **651** errors of which **10** are caught, so **641 (98%)** get out. At a held 60% rate roughly **391** of the 651 would have been caught. All counted in the draw function.
- **Title (bold 15px `P.ink`, centered, y=21):** "Twenty Weeks of Outputs, One Fading Habit"
- **Axes:** x = week 1…20 across `PX=54 … w−150`; y = counts 0–90, baseline `h−54`, top `y=58`. Faint `P.grid` horizontals every 20 with 12px `P.mute` labels, week numbers every 4 below the baseline and "week" centred beneath.
- **Error bars:** one per week, `rgba(107,114,128,0.22)` stroked `rgba(107,114,128,0.45)`, labelled 12px `P.mute` "grey bars — errors the tool made" under the axis.
- **Checking rate:** drawn as a filled area on its own 0–60% right-hand scale in `rgba(201,133,0,0.18)` with a 2px `P.yellow` top edge, labelled bold 12px `P.yellow` "share of outputs checked" with "60%" and "4%" printed at its two ends from the computed rates.
- **Catch line:** 3px `P.orange` with 4px dots, one point per week, labelled bold 12px `P.orange` "errors actually caught".
- **Break marker:** 2px `#e74c3c` dashed vertical at week 13, bold 12px `#e74c3c` "week 13 — the tool drops to 60% right" above it and 12px `P.mute` "errors a week: 11 → 80" beneath, both computed.
- **Side panel** at `w−138`: bold 12px `P.ink` "OVER THE EIGHT / BROKEN WEEKS", bold 19px `#e74c3c` "631" over 12px `P.mute` "bad outputs out, / 98% of them", then bold 19px `P.orange` "11" over 12px `P.mute` "caught by the / faded checking".
- **Caption (bold 13px `P.orange`, centered, `h−10`):** "The checking was abandoned during the calm, so the break arrived unwatched."

---

## Regeneration instructions

- **Template:** the card-section layout from `05-cognitive-biases/25-mere-exposure-effect.html`. One `.card-section` per section, each holding an `<h2>` (1.3rem `#1a5276`, `border-bottom: 2px solid #2980b9`, 4px bottom padding) and a `table.layout` with `td.text-col` 50% / `td.viz-col` 50%.
- **Canvas placement:** `td.viz-col` gets `text-align: center`, the canvas `display: block; width: 100%; margin: 0 auto`, capped at 720px so a wide cell leaves slack either side.
- **Text column order:** `.tags` pill row → `<ul>` of one-line bullets each opening `<b>label</b>` then an em dash → one `.key-point` callout → `.src` note. No paragraph blocks, no data tables.
- **Bullet form:** one line at 50% column width, 90–100 characters including the bold label.
- **Page CSS:** identical to `25-mere-exposure-effect.html` — body system-ui, white, `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with `border-bottom: 2px solid #2980b9`; `.subtitle` `#666` 0.95rem; `.card-section` 40px bottom margin; `.key-point` `#f8f9fa` with `border-left: 3px solid #e74c3c`; `.src` 0.78rem `#888`. No nav, no `.nav` CSS, no back/home links, no cross-page links.
- **Hue family per section:** 1 violet with a magenta spread, 2 aqua/green, 3 orange with a red alarm.
- **Canvas:** intrinsic `width="720"` plus per-chart height (340, 330, 340). `setup(id)` caches the logical size in `dataset` on first call, sets `style.maxWidth = 720px`, computes `scale = (cssW/720) × devicePixelRatio`, sizes the backing store to `logical × scale`, and `ctx.scale(scale, scale)` back to logical coordinates. Draws registered in `__charts`, re-run on debounced (150ms) resize.
- **Canvas fonts:** chart title bold 15px; in-chart header bold 12px; body and axis labels 12px floor; big callout figure bold 19px; caption bold 13px.
- **Palette** (shared `P`): `blue #2a78d6`, `green #008300`, `magenta #d55181`, `yellow #c98500`, `aqua #199e70`, `orange #d95926`, `violet #4a3aa7`, `ink #1a5276`, `text #2c3e50`, `mute #6b7280`, `grid #e5e9ef`. Hard red `#e74c3c` reserved for the genuine alarms — the break week and the 956 that got out.
- **Determinism:** no `Math.random()`. Seeded Park–Miller LCG (`s = (s × 16807) % 2147483647`), seed 42 in section 1, seed 2024 in section 3. Section 2 uses no PRNG at all — it is arithmetic on four stated rates.
- **Every printed figure is computed in its draw function** — section 1's counts and shares from the review tallies, section 2's daily totals from the rate arithmetic with the best pairing found by scanning all four, section 3's weekly counts and both escape totals from the simulated stream.
- **Section 3 was cut down from a two-team version.** It ran 26 weeks with a second team, a second PRNG stream and an alarm rule defined as the first post-break week beating that team's own worst quiet week. The numbers were sound but it was a simulation study, not a tutorial panel — one team and one fading line carry the same lesson. Do not reinstate the second arm.
- **Do not add an alarm rule.** An earlier construction fired in week 15 for both teams and so demonstrated nothing; the honest statement is simply that 98% of the errors got out.
- **Replaces the previous page at this index,** `12-deciding-in-advance`, which taught pre-commitment — a remedy rather than a bias — and so did not name a bias in its title. Its shopping-list, scorecard-weighting and re-marking constructions are not carried over.
