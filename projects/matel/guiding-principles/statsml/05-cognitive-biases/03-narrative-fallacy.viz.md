# Narrative Fallacy — Viz

**Page type:** detail page, card-section (template `06-sectioned-cards-callout`)
**HTML title tag:** Narrative Fallacy — Cognitive Biases
**Template:** the card-section layout from `statistical-paradoxes/03-berksons-paradox.html`, matching `05-clustering-illusion.html` in this folder
**Source note wording:** the sibling `.txt.md` says figures are "computed at render time"; the html `.src` notes say "computed in the draw function".

House style (bullet form, tag-pill markup, text-column order, canvas DPR scaling, shared palette `P`, canvas font floors, section-header borders) lives in `ui-templates/` and is not restated here.

**Figures printed in the sibling `.txt.md` are computed here.** Changing a seed, the direction test, or the rule set invalidates that prose — every direction count, hit rate, percentage and band edge must be re-read from the new draw and the text updated to match. The prose quotes, by section: **(1)** trough 94.7 on day 9, peak 108.6 on day 32, window close 97.9, 28 up against 32 down, the 7.4-point fall from day 60 to day 240, and the three leg sizes 5.3 / 13.9 / 10.7 (differences of the same scanned levels, drawn as legs but not labelled with numbers); **(2)** 16/20 = 80% and 20/42 = 48% for "a slide feeds on itself", the pooled 137/292 = 47%, and 33/55 = 60% for the bottom three rows (the sum of their three printed later scores, 10/16 + 11/21 + 12/18); **(3)** 30 games, 18 wins, cut after game 20, 10–10 = 50% before, 8–2 = 80% after, the 30-point jump, 24% searched and just under 9% fixed; **(4)** the 43%–57% band, 90/180 = 50%, 112/180 = 62%, and about 1 in 1,600.

**Determinism:** no `Math.random()`. Park–Miller LCG `s = (s × 16807) % 2147483647`. Seeds are **4472** for the price series (chosen because its first sixty fair days happen to trace a rise-and-fall a reader will read as a story), **101** for the season, **9** for the tilted series, **42** for the reference seasons. Every direction count, hit rate, percentage and band edge is computed in the draw function; the prose quotes those same computed values.

---

## 1. A Coin Flip Drew This Price Series

**Tag colors:** `core idea` violet, `written afterwards` blue, `explains everything` magenta
**Hue family:** violet line with a magenta cut marker

### canvas `c1` — 720×330

The full 240-day seeded series as a line, with the first sixty days highlighted and annotated as a three-act story, and the rest drawn in grey.

- **Data:** seeded Park–Miller LCG, seed **4472**. For each of 240 days: `d = rng() < pDown ? -1 : +1` with `pDown = 0.5`, then `m = 0.4 + rng()*1.2`, then `px[i+1] = px[i] + d*m`, starting at `px[0] = 100`. The two `rng()` calls per day must happen in that order, and the direction test must be written against `pDown`, or the series mirrors and every figure below changes.
- **Computed from the series in the draw function:** window start 100.0, trough 94.7 on day 9, peak 108.6 on day 32, window close 97.9; 28 up days and 32 down days in the first sixty; series close 90.4; overall range 85.4–108.6.
- **Title (bold 15px `P.ink`, centered, y=22):** "One Coin-Flip Price Series, 240 Days"
- **Plot box:** `PX=48`, `PY=44`, right margin 24, bottom for labels — plot height ends at `h−72`. Y scale spans 84 to 110 with faint `P.grid` horizontals and 12px `P.mute` labels at 85, 90, 95, 100, 105, 110. X spans day 0 to day 240 with 12px `P.mute` ticks at 0, 60, 120, 180, 240.
- **Window shading:** `rgba(74,58,167,0.06)` rectangle over days 0–60.
- **Line:** days 0–60 stroked `P.violet` 2.2px; days 60–240 stroked `P.mute` 1.4px.
- **Story markers:** 4px `P.violet` dots at days 0, 9, 32 and 60 (positions found by scanning the window for its minimum and maximum, not hardcoded).
- **Act labels (12px `P.violet`):** "the slide" under the leg to the trough, "the recovery" above the leg to the peak, "the correction" below the leg down to day 60. Each label is placed at the midpoint of its leg; "the slide" is nudged 30px right so it clears the y-axis numbers, which sit at the same height.
- **Story-written marker:** dashed 2px `P.magenta` vertical at day 60 (dash 5/4), bold 12px `P.magenta` label "story written here" placed above it.
- **Forward note (bold 12px `P.magenta`, over the grey stretch):** "the level kept sliding — the story felt confirmed", with 12px `P.mute` "no story was written for these days" beneath.
- **Up/down callout, upper right inside the plot:** bold 13px `P.ink` header "SIXTY COIN FLIPS", then bold 19px `P.violet` "28" + 12px `P.mute` "up days" and bold 19px `P.mute` "32" + 12px `P.mute` "down days". Both counts scanned from the direction array.
- **Caption (bold 13px `P.violet`, centered, `h−10`):** "Nothing caused the shape. One coin flip decided every single day."

---

## 2. Eight Stories Fitted to the Same Sixty Days

**Tag colors:** `the proof` magenta, `scored twice` aqua, `no predictive value` blue
**Hue family:** magenta-versus-aqua dumbbells

### canvas `c2` — 720×340

A dumbbell chart: eight story rows, each with a magenta dot for its score inside the window and an aqua dot for its score on the following stretch, joined by a grey line.

- **Data:** the same seed-4472 series. Each story is a function of day `i` returning `−1` (predicts down), `+1` (predicts up) or `0` (stays silent). Scored over days 0–59 (the window) and days 60–239 (afterwards); silent days do not count.
- **The eight rules, exactly as implemented:**
  1. `a slide feeds on itself` — `dir[i−1]<0 && dir[i−2]<0 → −1`
  2. `a big drop brings a bigger one` — `px[i]−px[i−1] < −1.2 → −1`
  3. `a rally feeds on itself` — `dir[i−1]>0 && dir[i−2]>0 → +1`
  4. `under its ten-day average it sinks` — `px[i] < mean(px[i−9..i]) → −1`
  5. `over its ten-day average it climbs` — `px[i] > mean(px[i−9..i]) → +1`
  6. `a jump invites profit-taking` — `px[i]−px[i−1] > 1.2 → −1`
  7. `three down days end in a bounce` — three consecutive down days `→ +1`
  8. `a new twenty-day high gets sold` — `px[i] ≥ max(px[i−20..i−1]) → −1`
- **Computed scores (window → afterwards):** 16/20 = 80% → 20/42 = 48%; 7/10 = 70% → 11/29 = 38%; 12/18 = 67% → 17/41 = 41%; 13/20 = 65% → 42/85 = 49%; 18/30 = 60% → 47/95 = 49%; 4/11 = 36% → 10/16 = 63%; 3/15 = 20% → 11/21 = 52%; 2/11 = 18% → 12/18 = 67%. Rows sorted by window score, descending.
- **Pooled figures:** the five rules that cleared 60% in the window went **137/292 = 47%** afterwards. Across all eight, window 75/135 = 56% against 170/347 = 49% afterwards. The window score and the later score run in opposite directions across the eight rows (−0.78 as a straight-line fit).
- **Title (bold 15px `P.ink`, centered, y=22):** "Eight Stories, Scored Where They Were Born and Where They Were Not"
- **Axis:** percentage scale 10–90 from `x=250` to `x=696`, baseline under the rows; dashed 1.5px `P.mute` vertical at 50% (dash 4/4) with a 12px `P.mute` "coin flip" label beneath it.
- **Rows:** eight rows at a 24px pitch starting `y=74`. Story text 12px `P.text`, right-aligned at `x=242`. Connector 1.5px `#c9ced6`. Window dot radius 5 `P.magenta`; later dot radius 5 `P.aqua`. Percentages 12px, printed on the far side of each dot so the pair never collides — window figure in `P.magenta`, later figure in `P.aqua`.
- **Legend (bold 12px, above the rows at y=52):** `P.magenta` "● scored on the 60 days it was fitted to" and `P.aqua` "● scored on the next 180 days".
- **Footer callout:** bold 13px `P.ink` "THE FIVE THAT SOUNDED TRUE, ON DAYS THEY NEVER SAW", then bold 19px `P.aqua` "47%" with 12px `P.mute` "137 of 292 calls — a coin flip gets 50%", all computed from the pooled tally.
- **Caption (bold 13px `P.magenta`, centered, `h−9`):** "The best-fitting story predicted nothing. The worst-fitting ones did slightly better."

---

## 3. The Turning Point of a Coin-Flip Season

**Tag colors:** `where it bites` orange, `searched afterwards` red, `stated first` green
**Hue family:** yellow strip, orange cut and bar against a green bar

### canvas `c3` — 720×320

The thirty games as a strip of win/loss squares with the searched cut marked, above two bars comparing how often chance delivers a jump that big when the cut is searched for versus named in advance.

- **Data:** seeded LCG, **seed 101**; 30 games, win if `rng() < 0.5`. Yields `111101000101011100001110111110` — 18 wins. `bestSplit` scans every cut leaving at least 8 games on each side and keeps the largest `(after rate − before rate)`: cut after game 20, before 10–10 (50%), after 8–2 (80%), jump **30 points**. All read from the scan.
- **Reference chances:** 4,000 seeded seasons on `lcg(42)`. Share whose *best* cut reaches a 30-point jump: **24%**. Share whose *fixed* cut at game 20 reaches it: **9%**. Stable across 4,000 / 8,000 / 16,000 trials and across seeds 42 / 7 / 99 (23.3–24.1% and 7.8–8.6%).
- **Title (bold 15px `P.ink`, centered, y=22):** "One Coin-Flip Season, Thirty Games"
- **Game strip:** 30 cells from `x=44` to `w−28` at `y=48`, height 26. Wins `rgba(201,133,0,0.45)` stroked `P.yellow`; losses `rgba(107,114,128,0.14)` stroked `#dcdfe4`.
- **Cut marker:** 2.5px `P.orange` vertical through the strip at the scanned cut, extended 8px above and below, with bold 12px `P.orange` "the turning point" above it.
- **Half labels (12px, under the strip):** `P.mute` "10 wins, 10 losses — 50%" centred under the first half and bold 12px `P.orange` "8 wins, 2 losses — 80%" under the second, both printed from the scan; then bold 12px `P.orange` "+30 points" beside the cut.
- **Bar panel:** header bold 13px `P.ink` "HOW OFTEN A FAIR SEASON HANDS YOU A JUMP THAT BIG" at `y=200`. Two horizontal bars, track `rgba(107,114,128,0.12)`, full width = 40%, from `x=252` to `x=668`, height 20, pitch 42 starting `y=210`. Bar 1 `rgba(217,89,38,0.50)` stroked `P.orange`, label right-aligned 12px `P.mute` "cut chosen after looking", value bold 12px `P.orange`. Bar 2 `rgba(0,131,0,0.40)` stroked `P.green`, label "cut named before the season", value bold 12px `P.green`. Both percentages printed from the tally.
- **Ratio note (bold 12px `P.orange`, under the bars):** "the freedom to choose the cut afterwards — nearly ×3" — the multiple computed as searched ÷ fixed (2.8 from the raw shares) and printed only as the word, so it cannot disagree with the two rounded bar percentages.
- **Caption (bold 13px `P.orange`, centered, `h−9`):** "The turning point is the biggest gap the search was allowed to keep."

---

## 4. What Separates an Account From a Decoration

**Tag colors:** `the boundary` green, `stated in advance` aqua, `forbids something` blue
**Hue family:** grey band with a green marker and green verdicts

### canvas `c4` — 720×330

One horizontal scale of "share of later days that went down", with the band chance alone produces shaded, two series marked on it, and a three-row test panel underneath.

- **Data — series A:** the same seed-4472 fair series. Days 60–239 contain **90 down days out of 180 = 50%**, scanned from the array.
- **Data — series B:** the same `makeSeries` helper, **seed 9**, 240 days, down with probability 0.58 by construction. Days 60–239 contain **112 down days out of 180 = 62%**, scanned from the array.
- **Chance band:** exact fair-coin arithmetic over 180 days. The central 95% of outcomes runs from 77 to 103 down days, i.e. **43% to 57%**. Computed in the draw function by summing the exact term-by-term probabilities inward from each tail until 2.5% is passed — no simulation.
- **Rarity figure:** the exact chance of 112 or more down days in 180 fair days is 0.064%, i.e. **about 1 in 1,600**, summed term by term and printed as `Math.round(1/p)` rounded to the nearest hundred.
- **Title (bold 15px `P.ink`, centered, y=22):** "Two Down Stretches, One Hundred Eighty Days Each"
- **Scale:** 38%–68% from `x=90` to `x=666` at `y=110`, 1px `#ccc` axis, 12px `P.mute` ticks every 5 points ("40%", "45%" … "65%").
- **Chance band:** `rgba(107,114,128,0.14)` rectangle spanning the computed 43%–57%, height 46 centred on the axis, with 12px `P.mute` "what chance alone delivers — 19 stretches in 20" centred inside it and the two edge percentages printed from the computation.
- **Marker A:** 6px `P.mute` dot on the axis with a 2px `P.mute` stem, bold 13px `P.mute` "the story series — 50% down" above it, 12px `P.mute` "inside the band; the story ruled nothing out" beneath that.
- **Marker B:** 6px `P.green` dot with a 2px `P.green` stem, bold 13px `P.green` "a real tilt — 62% down", 12px `P.green` "outside the band; about 1 in 1,600 by chance".
- **Marker label placement:** the two captions are wide enough to overlap at these positions, so they sit on two tiers — marker B's stem stops 28px above the axis, marker A's 62px above — and each caption is clamped with `measureText` so it cannot run off either edge of the canvas.
- **Test panel:** header bold 13px `P.ink` "TWO ACCOUNTS, THREE QUESTIONS" at `y=222`, then three rows on a 26px pitch. Row labels 12px `P.text`: "named before these days began", "says what would have broken it", "holds up on days it never saw". Two verdict columns headed bold 12px `P.mute` "the story" and bold 12px `P.green` "the tilt". Verdicts drawn as bold 14px glyphs: `P.mute` "✕" for the story in all three rows, `P.green` "✓" for the tilt in all three.
- **Caption (bold 13px `P.green`, centered, `h−9`):** "An account states its neck before the data. A decoration fits whatever arrives."

---

## Page-specific constraints

- **Canvas heights are per-chart:** 330, 340, 320, 330.
- **Canvas placement:** `td.viz-col` gets `text-align: center`; the canvas is `display: block; width: 100%; margin: 0 auto` and capped at 720px by `style.maxWidth`, so it sits centred in the right half.
- **No paragraph blocks, no data tables, no `.example` line restating a bullet** in the text column.
- **Text stands alone; the chart adds clarity** — the text carries the argument and names every quantity it turns on; the canvas adds precision, intermediate values and per-point labels. No bullet points at a position on the canvas. See `ui-templates/README.md`.
- **Bullet counts follow the content** — 7, 8, 8, 9 here — never a quota.
- **Section titles name the content.** No role labels ("The Trap", "Where It Strikes", "Pipeline Defense") and no phrasing that would fit another page.
- **Tag pill classes beyond the standard set:** `.violet` `rgba(74,58,167,0.12)`/`#4a3aa7`, `.magenta` `rgba(213,81,129,0.14)`/`#c2426f`, `.aqua` `rgba(25,158,112,0.14)`/`#17805d`.
- **Blue fill plus orange highlight must not be every chart** — the four hue families above are required.
- **`makeSeries` takes a down probability, not an up probability.** Flipping that test mirrors the series and silently invalidates every figure on charts 1, 2 and 4 — the story shape disappears and the eight story scores all change. Verified: with `pDown` the window holds 28 up days against 32 down, trough 94.7 on day 9, peak 108.6 on day 32.
- **The page must prove the story predicts nothing, not assert it.** Section 2 exists for that reason: the same eight rules are scored on the days that produced them and on days they never saw, and the ranking inverts. A page that only asserts unpredictiveness has not made its case.
- **The last section must not claim all explanation is fallacy.** The distinction it draws is order and refutability — stated before the data and forbidding some outcome, versus assembled afterwards to fit any outcome.
- **Corrections applied to the earlier version of this page:**
  - The old lead chart was a confidence-interval diagram with a hardcoded estimate of 1.4× and a hardcoded interval of [0.9, 2.1] drawn next to no data at all. Nothing on it was computed, and the interval was not derived from any sample. It has been replaced by the seeded price series, where every printed figure is scanned from the plotted points.
  - The old scatter section printed `r = 0.42` as a title while generating its points from a different formula (`y = 0.5x + rand·0.4 + 0.1` under a non-Park–Miller LCG), so the label could not have matched the data. That section is gone; the correlation claim it rested on is replaced by the fitted-versus-forward test, which is computed.
  - The old page asserted that a post-hoc story "explains nothing" without ever measuring predictive value. Section 2 now measures it: 80% inside the fitted window against 48% afterwards for the winning rule, and 47% pooled across the five rules that looked convincing.
  - The old "Pipeline Defense" section was a flow diagram of gates with no data on it, and it used the banned vocabulary of pre-registration, effect size, confidence intervals and multiple-testing correction. Its one durable idea — a claim must be on the record before the data — survives as the first question in section 4, stated in plain words and demonstrated numerically against a real tilt.
  - The "three burglaries on Elm Street" example has been dropped; it belongs to `05-clustering-illusion`, which already builds a street map for it.
