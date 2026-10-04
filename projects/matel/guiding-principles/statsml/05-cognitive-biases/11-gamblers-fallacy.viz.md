# Gambler's Fallacy — Viz

**Page type:** detail page — card-section template (see `statistical-paradoxes/03-berksons-paradox.html`), matching the converted sibling `05-clustering-illusion.html`
**HTML title tag:** Gambler's Fallacy — Cognitive Biases
**Template:** card-section layout from `statistical-paradoxes/03-berksons-paradox.html`
**Source note wording:** the sibling `.txt.md` says figures are "counted / computed / summed at render time"; the html `.src` notes say "in the draw function".

House style (bullet form, tag-pill markup, text-column order, canvas DPR scaling, shared palette `P`, canvas font floors, section-header borders) lives in `ui-templates/` and is not restated here.

**Figures printed in the sibling `.txt.md` are computed here.** Changing a seed or the construction invalidates that prose — re-read the computed values and update the text to match.

**Determinism:** no `Math.random()`. Seeded Park–Miller LCG (`s = (s × 16807) % 2147483647`), seed 42, used only by c3. c1, c2, c4 and c5 are exact — pocket tallies, remaining-card counts, a log-space binomial sum, and a one-line multiplier. Every printed figure is computed in the draw function from the plotted data.

---

## 1. Six Reds, and the Wheel Owes You Nothing

**Tag colors:** `core idea` violet, `the next spin` blue, `"black is due"` magenta
**Hue family:** violet/magenta

### canvas `c1` — 720×340

The six settled spins as a strip, then the next spin's chances printed twice — once as they stood before any of it, once as they stand after — so the reader sees two identical bar groups.

- **Data:** a pocket array built in the draw function — one `G`, eighteen `R`, eighteen `B`, 37 entries. Tallied to give red 48.6%, black 48.6%, green 2.7%. Nothing hardcoded.
- **Run chance:** `Math.pow(18/37, 6)` = 1.33%, printed as "1 in 75" via `Math.round(1/p)`.
- **Title (bold 15px `P.ink`, centered, y=22):** "One Wheel, Thirty-Seven Pockets"
- **Settled strip:** header bold 13px `P.ink` "THE SIX SPINS ALREADY SETTLED" at `x=44, y=46`. Six squares 30×22 at `y=54`, pitch 34, fill `rgba(213,81,129,0.45)` stroked `P.magenta`, each with a white bold 12px "R" centred.
- **Strip note (12px `P.mute`, `y=95`):** "six reds from a standing start: 1 in 75 — and that bill is already settled", the figure from the run-chance variable.
- **Bar groups:** track `BX=150`, width `w−250−BX` = 320, scale runs 0 to 60%. Rows red / black / green on a 26px pitch, bar height 17.
  - Group A: header bold 13px `P.ink` "SPIN ONE — BEFORE ANY OF THIS" at `y=118`, bars at `y=128, 154, 180`.
  - Group B: header bold 13px `P.ink` "SPIN SEVEN — AFTER SIX REDS" at `y=220`, bars at `y=230, 256, 282`.
  - Both groups draw from the same tally in the same loop, so they are pixel-identical by construction — that identity is the whole claim of the chart.
- **Bar colours:** red row `rgba(213,81,129,0.45)`/`P.magenta`, black row `rgba(74,58,167,0.50)`/`P.violet`, green row `rgba(25,158,112,0.45)`/`P.aqua`. Track `rgba(107,114,128,0.10)`.
- **Row labels:** 12px `P.mute` right-aligned at `BX−8` — "red", "black", "green". Percentages bold 12px from the tally: red and green print just right of the bar end in the row hue, black prints white inside its own bar so the empty part of the track stays clear for the belief marker.
- **Belief marker:** on Group B's black row only, a dashed 1.5px `P.magenta` arrow (dash 5/4) leaving the bar end and running to the end of the track with a solid arrowhead but no destination mark, labelled italic 12px `P.magenta` "where the table feels / black has moved to" on two lines beyond the track. No number — it is a feeling, not a measurement.
- **Caption (bold 13px `P.violet`, centered, `h−10`):** "Same wheel, same pockets, same chances. The run changed the room, not the odds."

---

## 2. A Deck Remembers, a Wheel Cannot

**Tag colors:** `two kinds of draw` aqua, `the pool shrinks` yellow, `sometimes really due` blue
**Hue family:** aqua/blue with a yellow gap marker

### canvas `c2` — 720×320

Two lines over the same x-axis — red cards or red spins removed, zero to six — showing the chance the next one is black. The deck line climbs; the wheel line is flat.

- **Data (computed in the draw function):** deck `26 / (52 − k)` for k = 0…6, giving 50.0, 51.0, 52.0, 53.1, 54.2, 55.3, 56.5 percent. Wheel `18 / 37` = 48.6% at every k.
- **Gap:** `26/46 − 18/37` = 7.9 percentage points, computed and printed, not typed.
- **Title (bold 15px `P.ink`, centered, y=22):** "Chance the Next One Is Black"
- **Plot box:** `PX=64`, `PY=52`, right margin 158 (room for end labels), baseline `h−70` so the x label clears the caption.
- **Axes:** y spans 44% to 60% with `P.grid` 1px lines and 12px `P.mute` tick labels every 4 points; x is 0 to 6 with 12px `P.mute` tick numbers and the label "reds taken out of the pool" centered beneath.
- **Deck line:** 2.5px `P.aqua` with filled 4px `P.aqua` dots at each k. End label bold 12px `P.aqua` "one deck — 56.5%" plus 12px `P.mute` "the pool actually shrank" beneath.
- **Wheel line:** 2.5px `P.blue`, dash 6/4, with hollow 4px `P.blue` dots. End label bold 12px `P.blue` "one wheel — 48.6%" plus 12px `P.mute` "nothing was removed" beneath.
- **Gap marker:** at k=6 a 1.5px `P.yellow` double-headed vertical arrow between the two lines with bold 12px `P.yellow` "7.9 points apart" beside it, the figure from the computed gap.
- **Caption (bold 13px `P.aqua`, centered, `h−10`):** "Removing a card changes the deck. Removing a spin changes nothing."

---

## 3. The Ratio Comes Back Because the Run Gets Diluted

**Tag colors:** `the long run` orange, `dilution` yellow, `no repayment` red
**Hue family:** orange/yellow

### canvas `c3` — 720×340

One run plotted two ways on shared x: the share of heads on the left axis sinking toward half, and the raw surplus of heads over tails on the right axis refusing to come down.

- **Data:** seeded Park–Miller LCG, seed 42. Tosses 1–10 are forced heads; tosses 11–400 are heads when `rng() < 0.5`. Running `share = H/n` and `surplus = H − T` recorded at every n and read off the arrays.
- **Computed marks (seed 42):** share 100.0% at n=10, 62.0% at 50, 60.0% at 100, 58.5% at 200, **54.5% at 400**. Surplus 10 at n=10, 12 at 50, 20 at 100, 34 at 200, **36 at 400**, peaking at **45 around n=337** — found by a scan, not asserted.
- **Across-run block:** 1,000 further runs continue on the same seeded stream, each with the ten forced heads then 390 fair tosses. Average surplus **10.3**, average share **51.3%** — against the exact expectations of 10 and 51.25%.
- **Title (bold 15px `P.ink`, centered, y=22):** "Ten Heads to Open, Then Three Hundred Ninety Fair Tosses"
- **Plot box:** `PX=60`, `PY=100`, right margin 66, baseline `h−58` — the tall top margin leaves the across-run block a clear band rather than sitting it on top of the surplus line. x runs n=10 to 400 with 12px `P.mute` ticks at 10, 100, 200, 300, 400 and the label "tosses so far" centered beneath.
- **Left axis (share):** 40% to 100%, `P.grid` lines every 10 points, 12px `P.orange` tick labels, axis title 12px `P.orange` "share of heads" just above the box on the left.
- **Right axis (surplus):** 0 to 50, 12px `P.yellow` tick labels every 10 on the right edge, axis title 12px `P.yellow` "heads minus tails" just above the box on the right.
- **Share line:** 2.5px `P.orange`. Endpoint dot 5px `P.orange` with bold 12px `P.orange` "54.5%" printed left of it so it stays inside the box.
- **Surplus line:** 2.5px `P.yellow`. A hollow 5px `P.yellow` circle at the scanned peak with bold 12px `P.yellow` "peak 45" above it, and a filled 5px dot at n=400 labelled bold 12px `P.yellow` "36".
- **Half line:** 1.5px `P.mute` horizontal at 50% on the left scale, labelled 12px `P.mute` "half" centred at 45% across so it sits clear of both lines and of the end markers.
- **Across-run block** in the clear band above the plot at `y=44`: bold 13px `P.ink` "ACROSS 1,000 SUCH RUNS", then on the line below bold 19px `P.yellow` "10.3" with 12px `P.mute` "average surplus at the finish", and to its right bold 19px `P.orange` "51.3%" with 12px `P.mute` "average share at the finish". Both from the tally.
- **Caption (bold 13px `P.orange`, centered, `h−10`):** "The share sinks toward half. The ten extra heads are still sitting there."

---

## 4. When the Record Says Bet Red, Not Black

**Tag colors:** `reading the machine` magenta, `a real signal` violet, `opposite direction` red
**Hue family:** magenta with a green read

### canvas `c4` — 720×330

The full spread of red counts a true wheel produces over four hundred spins, with the logged count marked far out in the right tail.

- **Data (exact, computed in the draw function):** for n=400 and p=18/37, the chance of exactly k reds is summed in log space (`logC` for the count of combinations, then exponentiated). Columns are drawn for k=160…240; centre 194.6, tallest column at k=195.
- **Tail:** summed separately over the whole range k=225…400 rather than only the drawn columns — that distinction matters, since stopping at k=240 gives 1 in 725 instead of the correct 1 in 724. Result 0.138%, printed as "1 in 724" via `Math.round(1/tail)`. Typical spread either side of centre is 10.0 spins, computed as `sqrt(n·p·(1−p))` and printed as a whole number.
- **Title (bold 15px `P.ink`, centered, y=22):** "Reds in Four Hundred Spins of a True Wheel"
- **Plot box:** `PX=54`, `PY=74`, right margin 30, baseline `h−72`. x is k=160 to 240 with 12px `P.mute` ticks every 20 and the label "reds in the four hundred" centered beneath.
- **Spread:** one thin column per k, fill `rgba(107,114,128,0.28)` stroked `#dcdfe4`, scaled so the tallest column reaches the top of the box. Columns from k=225 up are refilled `rgba(213,81,129,0.55)` stroked `P.magenta` — the tail being talked about, visible on screen.
- **Centre marker:** 1.5px `P.mute` dashed vertical at k=195 with 12px `P.mute` "a true wheel centres here, give or take 10 spins" on one line above the box, the spread figure from `sqrt(n·p·(1−p))`.
- **Logged marker:** 2.5px `P.magenta` vertical at k=225 running the box height, with bold 12px `P.magenta` "this log: 225 reds" placed to its right low in the box, where the columns have flattened to nothing.
- **Tail callout** at `PX + 0.64·PW`, clear of the tall columns: bold 13px `P.ink` "A TRUE WHEEL GETS THERE", then bold 19px `P.magenta` "1 in 724" and 12px `P.mute` "logs of four hundred spins".
- **Read strip** under the callout: bold 12px `P.green` "the read: this wheel leans red", then 12px `P.mute` "the fallacy would bet black".
- **Caption (bold 13px `P.magenta`, centered, `h−10`):** "The record accuses the wheel. It says nothing about the next spin being owed."

---

## 5. How Far the Odds Move Depends on How Much Was Taken Out

**Tag colors:** `the boundary` green, `how much was removed` blue, `when history pays` aqua
**Hue family:** green/yellow over mute

### canvas `c5` — 720×330

Five pools as rows. The bar length is the share of the pool taken out and kept out — the quantity the section's rule is about — and each row's note carries the odds it produces.

- **Rows (name, removed, of, base chance):** fair coin (0, —, 50%), roulette wheel (0, —, 48.6%), six-deck shoe (6 of 312, 50%), single deck (6 of 52, 50%), raffle drum (80 of 100, 1%).
- **Computed per row:** `frac = removed / pool`, `mult = 1 / (1 − frac)`, `after = base × mult`. Gives 0.0% gone / ×1.00 / 50.0%, 0.0% / ×1.00 / 48.6%, 1.9% / ×1.02 / 51.0%, 11.5% / ×1.13 / 56.5%, 80.0% / ×5.00 / 5.0%. Each cross-checked against the direct count — 156/306, 26/46, 1/20 — and they agree to the digit.
- **Why the bar is the share removed, not the multiplier:** on a linear ×1-to-×5 scale the shoe and deck rows collapse into the left edge and the chart says nothing about the interesting middle. The share removed spreads them out and is the quantity the rule names.
- **Title (bold 15px `P.ink`, centered, y=22):** "How Far the Next Draw's Odds Move"
- **Header (bold 13px `P.ink`, `x=130`, `y=44`):** "SHARE OF THE POOL TAKEN OUT AND KEPT OUT"
- **Rows:** five rows on a 50px pitch from `y=68`. Track `rgba(107,114,128,0.10)` from `BX=130`, width `w−200−BX` = 390, spanning 0 to 100% of the pool. A row with nothing removed draws a 2px tick at the track's left edge instead of a zero-width bar.
- **Row colours:** the two no-removal rows `rgba(107,114,128,0.30)`/`P.mute`; the shoe and deck `rgba(201,133,0,0.45)`/`P.yellow`; the drum `rgba(0,131,0,0.40)`/`P.green`.
- **Row names:** bold 12px `P.ink` at `BX`, on the line above each bar — "fair coin", "roulette wheel", "six-deck shoe", "single deck", "raffle drum".
- **Row figures:** bold 12px in the row hue just right of the bar end — "0.0% gone" through "80.0% gone" — then 12px `P.mute` on the line below the bar at `BX`, e.g. "six cards out of fifty-two · odds 50.0% → 56.5% (×1.13)" or "nothing ever leaves the pool · odds 50.0% stays 50.0% (×1.00)". Every number from the computed row.
- **Caption (bold 13px `P.green`, centered, `h−10`):** "No removal, no shift. History only pays where the pool actually shrank."

---

## Page-specific constraints

- **Canvas heights:** c1 340, c2 320, c3 340, c4 330, c5 330.
- **Canvas placement:** `td.viz-col` gets `text-align: center` and the canvas `display: block; width: 100%; margin: 0 auto`. The canvas is capped at 720px via `style.maxWidth`, so a wide cell leaves slack and the chart centres in the right half.
- **No paragraph blocks, no data tables, no `.example` line restating a bullet.**
- **Section titles name the content.** No role labels ("The Trap", "Where It Strikes", "Pipeline Defense") and no phrasing that would fit another page.
- **One hue family per section.** Section 1 violet/magenta, section 2 aqua/blue with a yellow gap marker, section 3 orange/yellow, section 4 magenta with a green read, section 5 green/yellow over mute. Do not let blue-fill-plus-orange-highlight become every chart.
- **Every printed figure is computed in the draw function** from the plotted data — pocket tallies, remaining-card counts, the log-space binomial sum, the scanned surplus peak, and the row multipliers.
- **The page's whole point is the pair of cases.** Section 2 must show a rising line beside a flat one on the same axes; if the reader cannot see the deck's odds moving while the wheel's do not, the page has not earned its subject. Most treatments of this fallacy cover only the memoryless case and leave the reader unable to tell when tracking history is smart.
- **Corrections applied to the earlier version of this page:**
  - The old lead chart plotted a "feeling of getting closer" as a rising dashed curve against a flat reality line — a drawn shape with no data behind it, and the rising curve had no computed meaning at all. Replaced by two identical computed bar groups, which is the actual claim.
  - The old page carried a lottery table asserting 9.6% versus 10.0% for spreading ten tickets over ten days. The arithmetic (`1 − 0.99¹⁰` = 9.56% against `10/100` = 10.0%) was right, but the setup silently assumed the same hundred-ticket pool refilled daily while the ten-on-one-day case drew from a single pool without replacement — the two rows were not the same experiment. Dropped.
  - The old "Independent vs Dependent" chart drew the dependent case as a curve following `0.7 − 0.5t²`, an invented shape presented as probability. Replaced by the exact `26/(52−k)` deck curve.
  - The old page claimed a five-heads run in twenty tosses happens "in 1 out of every 4 sequences" (the exact figure is 25.0%, so the claim was sound) but the whole streak-in-a-series angle belongs to `05-clustering-illusion` and is removed here to avoid duplicating it. This page is forward-looking only: what the next draw's odds actually are.
  - The old page's model-training and autocorrelation sections are gone with the pipeline framing. The dilution mechanism — the ratio returning to half without tails becoming likelier — replaces them, since that is the part of this fallacy usually taught wrongly.
