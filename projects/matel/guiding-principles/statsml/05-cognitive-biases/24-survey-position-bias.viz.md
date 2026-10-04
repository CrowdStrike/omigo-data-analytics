# Survey Position Bias — Viz

**Page type:** detail page — card-section template (see `statistical-paradoxes/03-berksons-paradox.html`, as converted in `05-clustering-illusion.html`)
**HTML title tag:** Survey Position Bias — Cognitive Biases
**Template:** card-section layout from `statistical-paradoxes/03-berksons-paradox.html`
**Source note wording:** the sibling `.txt.md` says figures are "computed at render time" (sections 1 and 2); the html `.src` notes say "computed in the draw function".

House style (bullet form, tag-pill markup, text-column order, canvas DPR scaling, shared palette `P`, canvas font floors, section-header borders) lives in `ui-templates/` and is not restated here.

**Figures printed in the sibling `.txt.md` are computed here.** Changing `TRUE`, `DROP`, a seed or the construction invalidates that prose — re-read the computed values and update the text to match. The prose quotes, by section: the song names and the `26, 23, 27, 24` likes (preamble); `284`, `278`, `236`, and the earned `260, 230, 270, 240` with their 40-vote spread (1); wins `Cold 11, Amber 10, Desert 2, Broken 1`, `11 of 24` at 46 percent, seat counts `15 / 8 / 1 / 0` (2); seat 1 `8.5%`, seat 20 `2.6%` on a `5.0%` fair share, read rates `48%` at seat 13 and `31%` at seat 20, `24%` first three against `8%` last three (3); ends `5.5%`, trough `4.7%`, `12 of 20` below fair share (4); `28.4%` reported against `26` earned for a `2.44`-point worst gap, under `0.01` shuffled, `4 of 40` and `35 of 40` with 5 dropped (5); win rates `100 / 46 / 39 / 8` percent, the `60%` and `4%` leads, and leads needed `20%` on four names and `224%` on twenty (6).

**Determinism:** no `Math.random()`. Seeded Park–Miller LCG (`s = (s × 16807) % 2147483647`), seed 42, used only where sampling is genuinely needed — section 5's repeat polls and section 6's twenty-name orderings. Sections 1, 2, 3 and 4 are exact; section 5's shuffled limit is the exact average over all 24 orderings, not a simulation.

---

## The one rule behind every number on this page

No visualization. Prose preamble only, placed above section 1 and carried verbatim in the `.txt.md`. No tags.

---

## 1. Two Ballots, Two Champions, One Set of Listeners

**Tag colors:** `core idea` violet, `the list decides` blue, `nobody lied` magenta
**Hue family:** violet/blue with a magenta winner tag

### canvas `c1` — 720×330

Two side-by-side vote-count panels, one per ballot order, each bar carrying a gray tick at the votes that
song actually earned.

- **Data:** the shared model only — `TRUE = [26, 23, 27, 24]`, `DROP = 0.94`, names `Amber / Broken / Cold / Desert`. `poll(order, TRUE)` applies `DROP^seat` to each song in its listed seat and rescales to 100; `votes(shares, 1000)` converts with largest-remainder rounding so each panel totals exactly 1,000.
- **Computed results:**
  | Panel | Ballot order, top → bottom | Votes `A, B, C, D` | Winner |
  |---|---|---|---|
  | Left | Amber, Broken, Cold, Desert | `284, 237, 261, 218` | Amber Skies |
  | Right | Desert, Cold, Broken, Amber | `236, 223, 278, 263` | Cold Coffee |
  | tick | what each song earned | `260, 230, 270, 240` | Cold Coffee |
- **Title (bold 15px `P.ink`, centered, y=22):** "Same Thousand Listeners, Two Ballot Orders"
- **Panels:** left plot `x = 44 … 344`, right `x = 384 … 684`; a 1px `P.grid` vertical divider at `x = 364` running `y = 40 … 244`.
- **Panel headers (bold 13px, centered over each panel, y=46):** left `P.violet` "BALLOT A — ALPHABETICAL", right `P.blue` "BALLOT B — REVERSED".
- **Bars:** four per panel in the ballot's own top-to-bottom order, bar width 44, baseline `y = 244`, scale 0–320 votes over 150px. The song sitting in the top seat of that panel is filled `rgba(74,58,167,0.55)` stroked `P.violet` (left) / `rgba(42,120,214,0.55)` stroked `P.blue` (right); the rest `rgba(107,114,128,0.28)` stroked `P.mute`.
- **Bar labels:** vote count bold 12px above each bar in the bar's stroke colour; song short name 12px `P.mute` below the baseline.
- **Earned ticks:** a 2px `#888` dashed (3/3) horizontal segment across each bar at that song's earned votes, drawn from `votes(TRUE, 1000)`. One legend line centered beneath the panels, 12px `P.mute`: "dashed tick = votes the song earned".
- **Winner tags:** the tallest bar in each panel gets bold 12px `P.magenta` "WINNER" above its count, located by an `argmax` scan of the plotted array — never typed.
- **Finishing-order strips:** under each panel, 12px `P.mute` "result: " followed by the ranking built by sorting that panel's plotted votes — left "Amber > Cold > Broken > Desert", right "Cold > Desert > Amber > Broken".
- **Swing callout (below the legend line, bold 12px `P.magenta`, centered):** "Amber Skies took 284 votes from the top seat and 236 from the last — 48 votes of seating", the difference computed as `284 − 236`.
- **Caption (bold 13px `P.violet`, centered, `h−10`):** "The votes did not change. The order of the names did."

---

## 2. All Twenty-Four Orderings of the Same Four Songs

**Tag colors:** `every arrangement` blue, `four possible champions` aqua, `the seat wins` yellow
**Hue family:** the four fixed song colours with aqua and yellow tallies

### canvas `c2` — 720×340

A 24-row strip of the orderings colour-coded by who won, above two small tallies: wins per song, and which
seat the winner sat in.

- **Data:** all 24 permutations of `[0,1,2,3]` generated in the draw function, each scored with `poll(perm, TRUE)` and its winner found by `argmax`. Nothing enumerated by hand.
- **Computed results:** wins — `Amber 10, Broken 1, Cold 11, Desert 2`; distinct winners `4 of 4`; the truly best song wins `11 / 24` (46 percent); winner's seat — `seat 1: 15, seat 2: 8, seat 3: 1, seat 4: 0`.
- **Title (bold 15px `P.ink`, centered, y=22):** "Twenty-Four Ways to Order Four Songs"
- **Song colours (fixed, used by both halves of the chart):** Amber `P.violet`, Broken `P.yellow`, Cold `P.aqua`, Desert `P.orange`, each at 0.55 alpha for fill and solid for stroke.
- **Ordering strip:** 24 columns across `x = 44 … 684` at `y = 44`, height 26. Each column is filled in its winner's colour; the winner's initial is printed bold 12px white centered inside. 12px `P.mute` label beneath: "each column is one ballot order — colour is who won it".
- **Wins tally (left half, `x = 44 …`, header y=126):** bold 13px `P.ink` "ORDERINGS EACH SONG WINS", then four horizontal bars on a 28px pitch, 118px track in `rgba(107,114,128,0.12)`, length scaled so 12 wins fills the track. Each bar in its song's colour; song name 12px `P.mute` right-aligned before the track; count bold 12px in the bar colour after it. The truly best song's row gets bold 12px `P.aqua` "liked most" trailing it.
- **Seat tally (right half, `x = 402 …`, header y=126):** bold 13px `P.ink` "SEAT THE WINNER SAT IN", then four rows on the same pitch and track width, bars `rgba(201,133,0,0.45)` stroked `P.yellow`, labelled "seat 1" … "seat 4" 12px `P.mute` with counts bold 12px `P.yellow` — seat 4 reads 0, drawn as an empty track.
- **Big figure (bold 19px `P.aqua`, left, below the tallies):** "11 of 24" with 12px `P.mute` "orderings won by the song people like most" beside it, and a second 12px `P.mute` line "the other 13 crown somebody else, Broken among them — liked least of the four". Both counts and the least-liked name come from the scan.
- **Caption (bold 13px `P.aqua`, centered, `h−10`):** "Four songs, four possible champions — and the best one wins under half the time."

---

## 3. Twenty Nominees, and the Scroll Runs Out

**Tag colors:** `long lists` orange, `attention runs down` yellow, `the dead tail` red
**Hue family:** orange/yellow fading to gray with a green fair-share line

### canvas `c3` — 720×330

A twenty-bar cascade of vote share by seat with the fair share drawn across it, plus a small three-pair
comparison of first-versus-last for lists of five, ten and twenty.

- **Data:** for a list of `n`, seat `p` (0-based) has read rate `DROP^p`; shares are those rates rescaled to 100. Same `DROP` as every other chart.
- **Computed results:** seat 1 `8.5%`, seat 20 `2.6%`, fair share `100/20 = 5.0%`, ratio `3.2×`; first three `23.9%`, last three `8.3%`; seat 13 is the first where the read rate falls under half (`48%`); seat 20's read rate is `31%`. First-vs-last ratios: `n=5 → 1.28×`, `n=10 → 1.75×`, `n=20 → 3.24×`.
- **Title (bold 15px `P.ink`, centered, y=22):** "Twenty Equally Loved Films — Unequal Votes"
- **Cascade:** 20 bars across `x = 46 … 684`, baseline `y = 208`, scale 0–10 percent over 150px, bar width `slot − 6`. Fill interpolated by seat from `rgba(217,89,38,0.60)` at seat 1 to `rgba(107,114,128,0.20)` at seat 20; stroke interpolated the same way from `P.orange` to `P.mute`, so the tail visibly fades out.
- **Fair-share line:** dashed (5/4) 1.5px `P.green` horizontal across the cascade at 5.0 percent, labelled 12px `P.green` at the right end "fair share 5.0%" — the value computed as `100 / n`.
- **Bar labels:** share printed bold 12px above seats 1 and 20 only, in each bar's stroke colour; x-axis ticks 12px `P.mute` at seats 1, 5, 10, 15, 20 with "seat on the ballot" centered beneath.
- **End brackets:** bold 12px `P.orange` "first three take 24%" over seats 1–3, bold 12px `P.mute` "last three take 8%" over seats 18–20, both from the plotted sums.
- **Length strip (y = 250 … 300):** three groups for `n = 5, 10, 20`, each a pair of bars — first seat `rgba(217,89,38,0.55)`/`P.orange` and last seat `rgba(107,114,128,0.28)`/`P.mute`, width 26, scaled against the group's own first-seat value. Group label 12px `P.mute` ("5 names", "10 names", "20 names"); the computed ratio printed bold 13px `P.orange` beside each pair ("1.3×", "1.7×", "3.2×"), each rounded to one decimal from the plotted values.
- **Caption (bold 13px `P.orange`, centered, `h−9`):** "The film in the last seat is not less loved. It is less read."

---

## 4. The Bottom of the List Gains Too

**Tag colors:** `both ends` magenta, `not simply top-wins` violet, `a shape, not a direction` blue
**Hue family:** magenta ends against a violet reference curve

### canvas `c4` — 720×330

The vote share by seat for twenty equally liked options, drawn twice: reading downward only, and reading
both directions — so the second curve visibly sags in the middle.

- **Data:** reading down gives seat `p` weight `DROP^p`; looking back up gives it `DROP^(n−1−p)`; the both-ends curve is the sum. Each set of weights rescaled to 100 percent independently.
- **Computed results (both-ends curve, 20 seats):** `5.53, 5.36, 5.21, 5.08, 4.97, 4.88, 4.81, 4.75, 4.72, 4.70, 4.70, 4.72, 4.75, 4.81, 4.88, 4.97, 5.08, 5.21, 5.36, 5.53`. Ends `5.5%` each, lowest seats 10 and 11 at `4.7%`, fair share `5.0%`, `12 of 20` seats below fair share, end-to-middle ratio `1.18×`. The downward-only curve for reference runs `8.45%` → `2.61%`.
- **Title (bold 15px `P.ink`, centered, y=22):** "Where Votes Go When a List Is Read From Both Ends"
- **Plot box:** `x = 56 … 686`, `y = 52 … 258`. Y axis 0–10 percent with 12px `P.mute` ticks every 2.5; faint `P.grid` gridlines; `#ccc` L-shaped axes; x labelled at seats 1, 5, 10, 15, 20 with 12px `P.mute` "seat on the ballot" centered beneath.
- **Fair-share line:** dashed (5/4) `P.mute` at 5.0 percent, labelled 12px `P.mute` "fair share 5.0%".
- **Downward-only curve:** 2px `rgba(74,58,167,0.55)` polyline with 3px `P.violet` dots, the "read top-down only" reference.
- **Both-ends bars:** 20 bars under the both-ends curve, width `slot − 8`, baseline at the axis. Seats at or above fair share `rgba(213,81,129,0.50)` stroked `P.magenta`; seats below `rgba(107,114,128,0.22)` stroked `P.mute` — so the sagging middle reads as a gray trough between two magenta ends.
- **Labels:** bold 12px `P.magenta` "5.5%" above seats 1 and 20; bold 12px `P.mute` "4.7%" above the trough with a 12px `P.mute` "the middle is the worst place to be" note above that. All three figures printed from the computed array.
- **Legend (top right of the plot, 10×10 swatches, 12px labels):** `P.violet` "read top-down only", `P.magenta` "read from both ends".
- **Verdict lines (upper left inside the plot):** bold 12px `P.magenta` "12 of 20 seats fall below their fair share", then 12px `P.mute` "and every one of them is in the middle". The count comes from a scan against the fair share.
- **Caption (bold 13px `P.magenta`, centered, `h−9`):** "Position is a shape, not a direction: both ends win, the middle pays."

---

## 5. Shuffle the Ballot for Every Voter

**Tag colors:** `the fix` green, `and its price` aqua, `what it cannot reach` red
**Hue family:** magenta error against green

### canvas `c5` — 720×340

The reported-share error of each approach, above two strips of forty repeat polls showing which song each
poll crowned.

- **Data:** `fixed = poll([0,1,2,3], TRUE)`. The shuffled limit is the average of `poll(perm, TRUE)` over all 24 permutations — the exact value shuffling converges to, so no randomness enters this half. The repeat polls draw each voter's choice from the seeded generator, seed 42, forty polls of two thousand voters, fixed and shuffled run from separate streams.
- **Computed results:** fixed shares `28.44, 23.65, 26.10, 21.81` against earned `26, 23, 27, 24` — worst gap `2.44` points. Shuffled limit `25.999, 23.003, 26.997, 24.002` — worst gap `0.0033`, printed as "under 0.01 points". Repeat polls: fixed ballot crowns the best song in `4 of 40`; shuffled in `35 of 40`.
- **Title (bold 15px `P.ink`, centered, y=22):** "Fixed Ballot Against a Shuffled One"
- **Error rows (y = 78 … 132):** two rows on a 54px pitch. Each row: 12px `P.mute` name right-aligned at `x = 164` ("one fixed order", "shuffled per voter"), then four small horizontal deviation bars from a zero line at `x = 296`, one per song, height 8 on a 2px gap, length scaled so 3.0 points spans 128px. Fixed row bars `rgba(213,81,129,0.50)`/`P.magenta`; shuffled row bars `rgba(0,131,0,0.45)`/`P.green` and effectively invisible at this scale, which is the point. Zero line 1.5px `P.mute` with 12px `P.mute` "what the songs earned" above it.
- **Worst-gap figures:** bold 19px past the deviation bars — `P.magenta` "2.44 pts" on the fixed row, `P.green` "0.00 pts" on the shuffled row — each with 12px `P.mute` "worst gap" beneath, both from a max scan over the row's deviations. The shuffled row adds bold 12px `P.green` "under 0.01", printed only when the gap is under a hundredth of a point.
- **Repeat-poll strips (header y = 200):** bold 13px `P.ink` "WHO WON, ACROSS FORTY REPEAT POLLS". Two strips of 40 cells each across `x = 152 … 686`, heights 24, at `y = 212` and `y = 254`, labelled 12px `P.mute` right-aligned "fixed ballot" / "shuffled". A cell is `rgba(0,131,0,0.50)` stroked `P.green` when that poll crowned the genuinely best song, `rgba(107,114,128,0.25)` stroked `P.mute` otherwise.
- **Strip tallies:** bold 12px after each strip's label row — `P.magenta` "4 of 40 right" and `P.green` "35 of 40 right", counted from the arrays.
- **Strip legend and limit note (12px `P.mute`, under the strips):** "green = this poll crowned the song people like most", then "shuffling still drops 5 polls to ordinary luck — more voters shrink that, more names do not" with the 5 computed as forty minus the shuffled tally.
- **Caption (bold 13px `P.green`, centered, `h−9`):** "Shuffling does not stop people reading from the top. It stops one song owning the top."

---

## 6. How Short and How Clear a List Has to Be

**Tag colors:** `the boundary` green, `length versus lead` yellow, `where it decides` red
**Hue family:** green through yellow and orange to one hard red

### canvas `c6` — 720×340

Four cases on one panel — short or long list, clear or close favourite — each showing how often the truly
best option wins, beside the lead a favourite needs to be safe at each list length.

- **Data:** four constructed preference sets. `short clear = [40,25,20,15]`; `short close = [26,23,27,24]`; `long clear` = twenty entries of 25 with the first raised to 40; `long close` = twenty entries of `50 − (seat mod 5)` with one raised to 52. Lists of four are enumerated exactly (24 orderings); lists of twenty use 3,000 seeded shuffles, seed 42 — stable to the printed digit across seeds 7, 99 and 2024.
- **Computed results:**
  | Case | Names | Favourite's lead | Best option wins | Distinct winners |
  |---|---|---|---|---|
  | short + clear | 4 | 60% | `100%` of 24 orderings | 1 |
  | short + close | 4 | 4% | `46%` of 24 orderings | 4 |
  | long + clear | 20 | 60% | `39%` of 3,000 orderings | 20 |
  | long + close | 20 | 4% | `8%` of 3,000 orderings | 20 |
- **Title (bold 15px `P.ink`, centered, y=22):** "When the Order Decides the Winner"
- **Case bars (y = 66 … 208):** header bold 13px `P.ink` "ORDERINGS THAT STILL FIND THE BEST OPTION" at `x = 236`, then four horizontal bars on a 40px pitch, track `x = 236 … 596` in `rgba(107,114,128,0.12)`, length = win rate on a 0–100 scale. Fill by outcome: 100 percent gets `rgba(0,131,0,0.55)`/`P.green`; above 50 percent `rgba(201,133,0,0.50)`/`P.yellow`; above 20 percent `rgba(217,89,38,0.50)`/`P.orange`; at or below 20 percent `rgba(231,76,60,0.50)`/`#e74c3c` — the one place hard red is used, because a poll that finds the right answer 8 times in 100 is a genuine alarm.
- **Case labels:** two 12px `P.mute` lines right-aligned at `x = 226` per row — the case name ("four songs, clear favourite") and its make-up ("favourite liked 60% more than the next"). The win rate is printed bold 13px in the bar colour past the end of the track, in a fixed column so the four figures line up.
- **Threshold line:** dashed (4/3) 1.5px `P.green` vertical at the 100 percent end of the track with 12px `P.green` "order-proof" beside it, so only the first bar reaches it.
- **Lead-needed panel (y = 264 … 300):** bold 13px `P.ink` "LEAD A FAVOURITE NEEDS TO WIN FROM THE LAST SEAT" at `x = 46`, then four inline figures on one row at 4, 5, 10 and 20 names — bold 19px in `P.green` when under 50 percent and `P.orange` above, with the name count 12px `P.mute` beside each: `4 → 20%`, `5 → 28%`, `10 → 75%`, `20 → 224%`. Each computed as `DROP^−(n−1) − 1`.
- **Caption (bold 13px `P.green`, centered, `h−9`):** "Few options and one clear favourite: order barely matters. Many near-equals: order is the answer."

---

## Page-specific constraints

- **Single shared model.** One top-level `TRUE = [26, 23, 27, 24]`, `DROP = 0.94`, `SHORT` name array, `readRate(seat) = DROP^seat`, `poll(order, like)`, `votes(shares, total)` with largest-remainder rounding, and `argmax`. No chart re-derives the rule locally and no chart hardcodes a share, count, ratio, winner or ranking — all six draw functions compute their labels from the model and print from those variables.
- **Vote counts total exactly 1,000** in section 1 via largest-remainder rounding, so no panel shows 999 or 1,001.
- **Text stands alone; the chart adds clarity** — the text carries the argument and names every quantity it turns on; the canvas adds precision, intermediate values and per-point labels. No bullet points at a position on the canvas. See `ui-templates/README.md`.
- **Every section here is constructed, so every section carries a `.src`.** No paragraph blocks, no data tables, no `.math-box`, no `.example` line — apart from the unnumbered rule preamble above section 1, which is prose and has no canvas.
- **Bullet count follows the content** — seven where seven covers it, nine where the fix and its limits need nine.
- **Section titles name the content.** No role labels ("The Trap", "The Fix", "In the Pipeline") and no phrasing that would fit another page.
- **No link to `10-recency-bias`** in particular — that page covers position in *time*; this page is position in a list presented all at once and shares no chart form with it.
- **Hue family rotates across the six sections** (listed per section above). Blue-fill-plus-orange-highlight must not become every chart.
- **Hard red `#e74c3c` appears exactly once** on the page — the 8-percent bar in section 6. It is not part of the shared `P` palette.
- **Plain language.** The page says "each seat is read by a few voters fewer than the seat above", never "decay constant" or a Greek letter. Charts say "what the song earned", "fair share" and "worst gap", never "relative error" or "bias magnitude".
- **The lead chart shows the winner changing.** Two panels of the same preferences under two orderings, with the earned votes ticked on both — the reader sees the trophy move before reading a word. A distribution or an abstract curve would not open the page.
- **Rounding discipline:** the shuffled worst gap is `0.0033` points, printed as "0.00 pts" with "under 0.01" beside it rather than a false-precision figure. The 3.24 ratio in section 3 prints as "3.2×" on the chart; the prose says "several times" rather than repeating the figure. Section 6's twenty-name win rates print as whole percentages because the sampled figures are only stable to that digit.
- **Corrections from the earlier version of this page:**
  - The old section 3 claimed a spoken list "behaves exactly like the same list printed backwards" and inferred from it that "on the radio, late wins". Under the page's own rule that is a *pure reversal*, not a both-ends effect — it moves the advantage to the bottom rather than giving both ends an advantage. That section has been replaced by section 4, which adds the two directions and shows the resulting end-heavy, middle-poor shape (ends 5.5 percent, middle 4.7, twelve of twenty seats below fair share). The old framing also implied radio voters do not experience primacy at all, which the page never justified.
  - The old fix section printed the shuffled worst gap as "0.002 pp". The correct value under the stated model is `0.0033` points, and it is now printed as "0.00 pts / under 0.01" rather than as a spuriously precise decimal.
  - The old page asserted a "3.2x advantage" and a "1.3x" short-list ratio without showing that a clear favourite on a *long* list also loses — the practically useful boundary. Section 6 now quantifies both axes and reports the lead a favourite needs (20 percent on four names, 224 percent on twenty).
  - The old page had no section showing that the same preferences produce four different champions across orderings. That enumeration is now section 2 and is the strongest evidence on the page: all 24 orderings, four distinct winners, the best song winning 11 of them.
