# Automation Bias — Viz

**Page type:** detail page, card-section (template `06-sectioned-cards-callout`)
**HTML title tag:** Automation Bias — Cognitive Biases
**Template:** the card-section layout from `05-cognitive-biases/25-mere-exposure-effect.html`
**Source note wording:** the sibling `.txt.md` says figures are "tallied / computed at render time"; the html `.src` notes say "tallied in the draw function" and "computed in the draw function".

House style (bullet form, tag-pill markup, text-column order, canvas DPR scaling, shared palette `P`, canvas font floors, section-header borders) lives in `ui-templates/` and is not restated here.

**Figures printed in the sibling `.txt.md` are computed here.** Changing a seed, a catch/accept probability, an accuracy rate or the checking-decay constant invalidates that prose — counts, shares and daily totals must be re-read from the new draw and the text updated to match.

**Determinism:** no `Math.random()`. Seeded Park–Miller LCG (`s = (s × 16807) % 2147483647`), seed 42 in section 1, seed 2024 in section 3. Section 2 uses no PRNG at all — it is arithmetic on four stated rates.

---

## 1. Same Suggestions, One Label Changed

**Tag colors:** `core idea` violet, `identical answers` blue, `only the badge moved` magenta
**Hue family:** violet with a magenta spread

### canvas `c1` — 720×340

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

## 2. Handing It Over Helps on the Routine Work Only

**Tag colors:** `the useful half` aqua, `two kinds of case` green, `pick per case` blue
**Hue family:** aqua/green

### canvas `c2` — 720×330

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

## 3. The Checking Fades Before the Tool Breaks

**Tag colors:** `how it goes wrong` orange, `nobody decided this` yellow, `nothing watching` red
**Hue family:** orange with a red alarm

### canvas `c3` — 720×340

Weekly errors as bars with the catch line over them, the checking rate falling away behind, and the break marked where the two diverge.

- **Data:** seeded LCG, seed 2024. Accuracy `0.95` for weeks 1–12 and `0.60` from week 13; 200 outputs a week. Checking rate `max(0.02, 0.60 × e^(−(t−1)/4.0))`, and each error is caught with that probability.
- **Computed:** checking **60%** in week 1, **4%** in week 12. Errors average **12** a week before the break and **81** after, and the eight broken weeks make **651** errors of which **10** are caught, so **641 (98%)** get out. At a held 60% rate roughly **391** of the 651 would have been caught. All counted in the draw function.
- **Title (bold 15px `P.ink`, centered, y=21):** "Twenty Weeks of Outputs, One Fading Habit"
- **Axes:** x = week 1…20 across `PX=54 … w−150`; y = counts 0–90, baseline `h−54`, top `y=58`. Faint `P.grid` horizontals every 20 with 12px `P.mute` labels, week numbers every 4 below the baseline and "week" centred beneath.
- **Error bars:** one per week, `rgba(107,114,128,0.22)` stroked `rgba(107,114,128,0.45)`, labelled 12px `P.mute` "grey bars — errors the tool made" under the axis.
- **Checking rate:** drawn as a filled area on its own 0–60% right-hand scale in `rgba(201,133,0,0.18)` with a 2px `P.yellow` top edge, labelled bold 12px `P.yellow` "share of outputs checked" with "60%" and "4%" printed at its two ends from the computed rates.
- **Catch line:** 3px `P.orange` with 4px dots, one point per week, labelled bold 12px `P.orange` "errors actually caught".
- **Break marker:** 2px `#e74c3c` dashed vertical at week 13, bold 12px `#e74c3c` "week 13 — the tool drops to 60% right" above it and 12px `P.mute` "errors a week: 12 → 81" beneath, both computed.
- **Side panel** at `w−138`: bold 12px `P.ink` "OVER THE EIGHT / BROKEN WEEKS", bold 19px `#e74c3c` "641" over 12px `P.mute` "bad outputs out, / 98% of them", then bold 19px `P.orange` "10" over 12px `P.mute` "caught by the / faded checking".
- **Caption (bold 13px `P.orange`, centered, `h−10`):** "The checking was abandoned during the calm, so the break arrived unwatched."

---

## Page-specific constraints

- **Hard red `#e74c3c` is reserved for the genuine alarms** — the break week and the count that got out. It sits outside the shared palette `P` and is not used decoratively.
- **Every printed figure is computed in its draw function** — section 1's counts and shares from the review tallies, section 2's daily totals from the rate arithmetic with the best pairing found by scanning all four, section 3's weekly counts and both escape totals from the simulated stream.
- **Section 3 was cut down from a two-team version.** It ran 26 weeks with a second team, a second PRNG stream and an alarm rule defined as the first post-break week beating that team's own worst quiet week. The numbers were sound but it was a simulation study, not a tutorial panel — one team and one fading line carry the same lesson. Do not reinstate the second arm.
- **Do not add an alarm rule.** An earlier construction fired in week 15 for both teams and so demonstrated nothing; the honest statement is simply that 98% of the errors got out.
- **Section 2 deliberately uses no PRNG.** It is arithmetic on four stated rates, so its numbers are exact rather than sampled; do not convert it to a simulation.
- **Replaces the previous page at this index,** `12-deciding-in-advance`, which taught pre-commitment — a remedy rather than a bias — and so did not name a bias in its title. Its shopping-list, scorecard-weighting and re-marking constructions are not carried over.
