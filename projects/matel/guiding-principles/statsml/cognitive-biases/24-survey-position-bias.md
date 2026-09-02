# Survey Position Bias: The Ballot Picks the Winner

**Page type:** detail page — card-section template (see `statistical-paradoxes/03-berksons-paradox.html`)
**HTML title tag:** Survey Position Bias — Cognitive Biases

**Subtitle:** Ask a thousand people to vote for the best song and the winner changes when you reorder the list. Nobody lied; the ballot did the choosing.

---

## The one rule behind every number on this page

Four songs are genuinely liked by **26, 23, 27 and 24** listeners in a hundred — close enough to be a
photo finish. Voters read the ballot from the top down, and each seat further down is read by about
**6 in 100 fewer** of them. A song nobody read cannot be voted for, so the votes that remain get shared
among the songs people actually looked at. That is the whole model: **true liking × how many voters got
that far**, rescaled to 100 percent. Every share, vote count, ranking and ratio on this page comes out of
it. Nothing is asserted.

Six percent per seat is deliberately small. A real poll's ordering effect is a few points, not a
landslide, and the unsettling part is that a *small* nudge still changes who wins.

---

## Section 1 — Two Ballots, Two Champions, One Set of Listeners

**Tags:** `core idea` (violet), `the list decides` (blue), `nobody lied` (magenta)

**Bullets:**
- **The poll** — a thousand listeners, four nominated songs, one vote each, no ties allowed
- **How people vote** — they read from the top, tire on the way down, and pick something they like
- **Alphabetical ballot** — Amber Skies takes 284 votes and is announced song of the year
- **Same voters, reversed ballot** — Cold Coffee takes 278 and Amber Skies drops to third
- **What Amber Skies earned** — 260 votes; it got 284 from the top seat and 236 from the last
- **The race was tight** — the four songs are liked by 26, 23, 27 and 24 listeners in a hundred
- **Nobody misbehaved** — every voter picked a song they liked, and the layout picked which one

**Key point:** A poll with one fixed running order cannot tell "most loved" apart from "listed first" —
both produce exactly the same vote counts. Alphabetical order feels fair precisely because it looks
like nobody chose it.

**Source note (`.src`):** Illustrative Example — one constructed set of listener preferences; every vote count and finishing order is computed in the draw function.

### Visualization — canvas `c1`, 720×330

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

## Section 2 — All Twenty-Four Orderings of the Same Four Songs

**Tags:** `every arrangement` (blue), `four possible champions` (aqua), `the seat wins` (yellow)

**Bullets:**
- **The experiment** — same voters, same likes, and all 24 ways of ordering the four names
- **Winners produced** — all four songs win at least one ordering, including the least liked
- **The genuinely best song** — Cold Coffee wins 11 of the 24, so it loses more often than it wins
- **Where the winner sat** — 15 of the 24 winners were printed first, 23 of 24 in the top two seats
- **The rank outsider** — Broken Compass, liked least of the four, still wins one ordering outright
- **What the poll measures** — the running order, plus enough real liking to stay believable
- **One ordering tells you little** — you cannot tell your result apart from the other 23

**Key point:** Run every arrangement and the trophy moves around the whole shortlist. A single ordering
hands you one of 24 answers and no way to know which one you got.

**Source note (`.src`):** Illustrative Example — all 24 orderings enumerated in the draw function; the win tallies and seat counts are scanned from that enumeration.

### Visualization — canvas `c2`, 720×340

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

## Section 3 — Twenty Nominees, and the Scroll Runs Out

**Tags:** `long lists` (orange), `attention runs down` (yellow), `the dead tail` (red)

**Bullets:**
- **The poll** — twenty films nominated for best romance, and voters like all twenty equally
- **Down the list** — each seat is read by about 6 percent fewer voters than the seat above it
- **Seat one** — read by every voter, and it collects 8.5 percent of the vote
- **Seat twenty** — read by 31 voters in 100, and it collects 2.6 percent
- **Past seat thirteen** — fewer than half the voters are still reading, so the tail is barely seen
- **The advantage** — the top seat takes 3.2 times the votes of the bottom, on equal merit
- **The two ends** — the first three films take 24 percent of the vote, the last three take 8
- **Adding a nominee** — pushes every film below it one seat further out of sight

**Key point:** On a long list the bottom half is measuring who kept scrolling, not who was loved. If a
shortlist runs to twenty names, the shortlist is the result.

**Source note (`.src`):** Illustrative Example — twenty films constructed to be liked exactly equally, so every printed difference is seating alone.

### Visualization — canvas `c3`, 720×330

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

## Section 4 — The Bottom of the List Gains Too

**Tags:** `both ends` (magenta), `not simply top-wins` (violet), `a shape, not a direction` (blue)

**Bullets:**
- **Not only the top** — the last name on a list gets a second look, so it gains as well
- **Why the top gains** — everybody starts there, and some voters stop before the end
- **Why the bottom gains** — it is the name still in mind when the reader looks back up
- **Read the list aloud** — the order flips, and the song heard last is now the freshest
- **Both ends together** — the two end seats take 5.5 percent each against a fair share of 5.0
- **The worst seats** — the two in the middle, on 4.7 percent each, below every other seat
- **Twelve seats of twenty** — sit below their fair share, and all twelve are in the middle
- **So it is a shape** — position pushes votes toward the ends, not simply toward the top

**Key point:** "First wins" is the wrong summary. Both ends of a list are advantaged and the middle is
where options go to die — so moving an option up can hurt it if it lands in the middle.

**Source note (`.src`):** Illustrative Example — twenty equally liked options; the end-and-middle shape is the reading-down rule added to the same rule applied from the bottom up.

### Visualization — canvas `c4`, 720×330

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

## Section 5 — Shuffle the Ballot for Every Voter

**Tags:** `the fix` (green), `and its price` (aqua), `what it cannot reach` (red)

**Bullets:**
- **The fix** — shuffle the running order for every voter, and record the list each one saw
- **Why it works** — across many voters, every song spends an equal share of time in every seat
- **One fixed order** — a song's reported share lands as much as 2.4 points from what it earned
- **Shuffled instead** — that worst gap falls below a hundredth of a point, so the tilt is gone
- **Forty repeat polls, fixed ballot** — the genuinely best song is declared winner in only 4
- **Forty repeat polls, shuffled** — it wins 35, and the 5 it drops are lost to ordinary luck
- **What shuffling costs** — no two voters see the same list, so a single result wobbles more
- **What shuffling cannot fix** — a film nobody scrolled to; every long list has a dead middle
- **So keep the list short** — shuffling shares the bad seats out, a short list has fewer of them

**Key point:** Shuffling removes the systematic tilt and replaces it with ordinary luck, which more voters
will settle. It cannot conjure attention that was never spent, so a name nobody reached stays unreachable.

**Source note (`.src`):** Illustrative Example — forty seeded polls of two thousand voters each, run once on a fixed ballot and once shuffled per voter; the true shuffled shares come from averaging all 24 orderings.

### Visualization — canvas `c5`, 720×340

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

## Section 6 — How Short and How Clear a List Has to Be

**Tags:** `the boundary` (green), `length versus lead` (yellow), `where it decides` (red)

**Bullets:**
- **Two things decide it** — how many names are on the list, and how far ahead the favourite is
- **Four songs, clear favourite** — liked 60 percent more than the next: it wins from every seat
- **Four songs, photo finish** — all within 4 percent: the best song wins 46 percent of orderings
- **Twenty films, clear favourite** — liked 60 percent more: it wins 39 percent of orderings
- **Twenty films, photo finish** — all within 4 percent: 8 percent, so the seating decides it
- **Length beats merit** — a runaway favourite on a long list fares worse than a close short list
- **The lead you need** — on four names the favourite must be liked 20 percent more than the next
- **On twenty names** — that required lead becomes 224 percent, which real tastes rarely reach
- **When to stop worrying** — a handful of options with one obvious favourite; otherwise shuffle

**Key point:** Order effects are negligible on a short list with one clear favourite and decisive on a long
list of near-equals. The test is whether the favourite's lead beats what the last seat costs — 20 percent
on four names, 224 percent on twenty.

**Source note (`.src`):** Illustrative Example — four constructed preference sets; the four-name cases enumerate all 24 orderings exactly, the twenty-name cases use 3,000 seeded orderings each.

### Visualization — canvas `c6`, 720×340

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

## Regeneration instructions

- **Template:** the card-section layout from `statistical-paradoxes/03-berksons-paradox.html`, as converted in `05-clustering-illusion.html`. One `.card-section` per section, each holding an `<h2>` (1.3rem `#1a5276`, `border-bottom: 2px solid #2980b9`, 4px bottom padding) and a `table.layout` with `td.text-col` 50% / `td.viz-col` 50%.
- **Text column order:** `.tags` pill row → `<ul>` of one-line bullets each opening `<b>label</b>` then an em dash → one `.key-point` callout → `.src` note. Every section here is constructed, so every section carries a `.src`. No paragraph blocks, no data tables, no `.math-box`, no `.example` line.
- **Bullet form:** each bullet is ONE line under 95 characters that does not wrap at 50 percent column width. Count follows the content — seven where seven covers it, nine where the fix and its limits need nine.
- **Section titles name the content.** No role labels ("The Trap", "The Fix", "In the Pipeline") and no phrasing that would fit another page.
- **Page CSS:** body system-ui, white, `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with `border-bottom: 2px solid #2980b9`, 8px bottom padding. `.subtitle` `#666` 0.95rem, 32px bottom margin. `.card-section` 40px bottom margin. `table.layout` full width, border-collapse, cells vertical-align top padding 12px. `ul` 0.92rem, margin `8px 0 8px 20px`, `li` 4px bottom margin, `li b` `#1a5276`. `.key-point` `#f8f9fa`, `border-left: 3px solid #e74c3c`, padding 8px 12px, 0.9rem. `.src` 0.78rem `#888`. Canvas `display: block; width: 100%; margin: 0 auto; border: 1px solid #e0e0e0; border-radius: 4px`.
- **No nav, no `.nav` CSS, no back/home links, no cross-page links of any kind.** In particular no link to `10-recency-bias`, which covers position in *time*; this page is position in a list presented all at once and shares no chart form with it.
- **Tag pills:** `display:inline-block`, 0.72rem, weight 600, padding 2px 10px, radius 10px. Classes used: `.blue`, `.green`, `.red`, `.orange`, `.violet` `rgba(74,58,167,0.12)`/`#4a3aa7`, `.magenta` `rgba(213,81,129,0.14)`/`#c2426f`, `.aqua` `rgba(25,158,112,0.14)`/`#17805d`, `.yellow` `rgba(201,133,0,0.15)`/`#a06c00`.
- **Hue family per section, rotated:** 1 violet/blue with a magenta winner tag, 2 the four fixed song colours with aqua and yellow tallies, 3 orange/yellow fading to gray with a green fair-share line, 4 magenta ends against a violet reference curve, 5 magenta error against green, 6 green through yellow and orange to one hard red. Blue-fill-plus-orange-highlight must not become every chart.
- **Canvas:** intrinsic `width="720"` plus the per-chart height (330, 340, 330, 330, 340, 340). `setup(id)` caches the logical size in `dataset` on first call, sets `style.maxWidth = 720px`, computes `scale = (cssW / 720) × devicePixelRatio`, sizes the backing store to `logical × scale`, and `ctx.scale(scale, scale)` back to logical coordinates. Draws registered in `__charts`, re-run on debounced (150ms) resize. Copy `setup`, `lcg`, the `P` palette, `__charts` and the resize handler verbatim from `05-clustering-illusion.html`.
- **Canvas fonts:** chart title bold 15px, in-chart header bold 13px, labels 12px floor, one big callout figure bold 19px, caption bold 13px. Every chart ends with a bold 13px caption stating its takeaway. No tables drawn on canvas.
- **Palette** (shared `P` object): `blue #2a78d6`, `green #008300`, `magenta #d55181`, `yellow #c98500`, `aqua #199e70`, `orange #d95926`, `violet #4a3aa7`, `ink #1a5276`, `text #2c3e50`, `mute #6b7280`, `grid #e5e9ef`. Hard red `#e74c3c` appears once, on the 8-percent bar in section 6.
- **Single shared model.** One top-level `TRUE = [26, 23, 27, 24]`, `DROP = 0.94`, `SHORT` name array, `readRate(seat) = DROP^seat`, `poll(order, like)`, `votes(shares, total)` with largest-remainder rounding, and `argmax`. No chart re-derives the rule locally and no chart hardcodes a share, count, ratio, winner or ranking — all six draw functions compute their labels from the model and print from those variables.
- **Determinism:** no `Math.random()`. Seeded Park–Miller LCG (`s = (s × 16807) % 2147483647`), seed 42, used only where sampling is genuinely needed — section 5's repeat polls and section 6's twenty-name orderings. Sections 1, 2, 3 and 4 are exact; section 5's shuffled limit is the exact average over all 24 orderings, not a simulation.
- **Vote counts total exactly 1,000** in section 1 via largest-remainder rounding, so no panel shows 999 or 1,001.
- **Plain language.** The page says "each seat is read by about 6 percent fewer voters", never "decay constant" or a Greek letter. Charts say "what the song earned", "fair share" and "worst gap", never "relative error" or "bias magnitude".
- **The lead chart shows the winner changing.** Two panels of the same preferences under two orderings, with the earned votes ticked on both — the reader sees the trophy move before reading a word. A distribution or an abstract curve would not open the page.
- **Rounding discipline:** the shuffled worst gap is `0.0033` points, printed as "0.00 pts" with "under 0.01" beside it rather than a false-precision figure. The 3.24 ratio in section 3 prints as "3.2×" everywhere including the prose. Section 6's twenty-name win rates print as whole percentages because the sampled figures are only stable to that digit.
- **Corrections from the earlier version of this page:**
  - The old section 3 claimed a spoken list "behaves exactly like the same list printed backwards" and inferred from it that "on the radio, late wins". Under the page's own rule that is a *pure reversal*, not a both-ends effect — it moves the advantage to the bottom rather than giving both ends an advantage. That section has been replaced by section 4, which adds the two directions and shows the resulting end-heavy, middle-poor shape (ends 5.5 percent, middle 4.7, twelve of twenty seats below fair share). The old framing also implied radio voters do not experience primacy at all, which the page never justified.
  - The old fix section printed the shuffled worst gap as "0.002 pp". The correct value under the stated model is `0.0033` points, and it is now printed as "0.00 pts / under 0.01" rather than as a spuriously precise decimal.
  - The old page asserted a "3.2x advantage" and a "1.3x" short-list ratio without showing that a clear favourite on a *long* list also loses — the practically useful boundary. Section 6 now quantifies both axes and reports the lead a favourite needs (20 percent on four names, 224 percent on twenty).
  - The old page had no section showing that the same preferences produce four different champions across orderings. That enumeration is now section 2 and is the strongest evidence on the page: all 24 orderings, four distinct winners, the best song winning 11 of them.
