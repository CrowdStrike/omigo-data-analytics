# Gambler's Fallacy: Nothing Owes You a Correction

**Page type:** detail page — card-section template (see `statistical-paradoxes/03-berksons-paradox.html`)
**HTML title tag:** Gambler's Fallacy — Cognitive Biases

**Subtitle:** Six reds in a row and the table agrees black is overdue. The wheel has not heard about any of it — but a deck of cards genuinely has.

---

## Section 1 — Six Reds, and the Wheel Owes You Nothing

**Tags:** `core idea` (violet), `the next spin` (blue), `"black is due"` (magenta)

**Bullets:**
- **What just happened** — six spins running landed on red, and the table has gone quiet
- **What the room concludes** — black is owed one now, so the next spin is the safe side
- **What the wheel is** — thirty-seven pockets, eighteen red, eighteen black, one green
- **What it remembers** — nothing at all; the ball cannot know where it landed a minute ago
- **The next spin** — black comes up a shade under half the time, exactly as it did on spin one
- **The figure people quote** — six reds from a standing start turns up about once in seventy-five
- **Why that misleads** — it prices a run not yet begun, and yours is already finished and paid
- **The only live question** — where one ball lands next, on a wheel that has not changed

**Key point:** The wheel keeps no ledger. Six reds is a fact about spins already settled, and the next spin is a fresh draw from the same thirty-seven pockets — so "due" names a feeling, not a change in the odds.

**Source note (`.src`):** Illustrative Example — a single-zero wheel; every printed chance is counted from its pockets in the draw function.

### Visualization — canvas `c1`, 720×340

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

## Section 2 — A Deck Remembers, a Wheel Cannot

**Tags:** `two kinds of draw` (aqua), `the pool shrinks` (yellow), `sometimes really due` (blue)

**Bullets:**
- **Put it back** — a spun wheel, a tossed coin, a rolled die; the pool is whole again every time
- **Take it away** — cards dealt from one deck never go back, so what is left over has changed
- **Six red cards dealt** — twenty reds left against twenty-six blacks, so black really is likelier
- **How much likelier** — black climbs from an even half to better than five chances in nine
- **Those same six on a wheel** — black sits where it always sat, a shade under half, unmoved
- **So "due" is not always wrong** — it is simply right whenever what came out stayed out
- **The test to run** — ask whether the last outcome was put back before the next draw was made
- **Card counters live here** — they track a shrinking deck, which is bookkeeping, not superstition

**Key point:** The question is never whether the game feels fair — it is whether what came out went back in. Return it and the history tells you nothing about the next draw. Keep it out and the history is the only thing that tells you anything.

**Source note (`.src`):** Illustrative Example — one standard deck against a single-zero wheel; both curves are computed from counts in the draw function.

### Visualization — canvas `c2`, 720×320

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

## Section 3 — The Ratio Comes Back Because the Run Gets Diluted

**Tags:** `the long run` (orange), `dilution` (yellow), `no repayment` (red)

**Bullets:**
- **The promise people hear** — a coin evens out over enough tries, so tails must arrive to fix it
- **The setup** — ten heads to open, then three hundred and ninety tosses of an ordinary coin
- **The share of heads** — it slides from every toss to a little over half, looking like repair
- **The count of heads** — the ten extra are never handed back; by the finish the surplus is larger
- **What actually happened** — the ten got buried under later tosses rather than cancelled by them
- **Across many runs** — the surplus averages ten wherever you stop, and only the share moves
- **Why the fraction sinks** — ten of ten is everything, ten of four hundred is barely visible
- **The wrong picture** — a coin repaying a debt out of future tosses that somehow know about it
- **The right picture** — a fixed debt shrinking against a bill that keeps getting bigger

**Key point:** The long-run average drifts back toward half by dilution, not by correction. Later tosses never repay the surplus — they simply outnumber it, and the raw gap between heads and tails typically grows rather than closes.

**Source note (`.src`):** Illustrative Example — ten forced heads then 390 seeded fair tosses; the across-run averages come from 1,000 further runs on the same stream.

### Visualization — canvas `c3`, 720×340

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

## Section 4 — When the Record Says Bet Red, Not Black

**Tags:** `reading the machine` (magenta), `a real signal` (violet), `opposite direction` (red)

**Bullets:**
- **A different suspicion** — not that black is owed, but that this particular wheel leans to red
- **The record** — four hundred spins logged, of which two hundred and twenty-five came up red
- **What a true wheel gives** — about half of four hundred to red, give or take ten spins
- **How far off this is** — a true wheel gets a count that high about once in seven hundred logs
- **The honest read** — the wheel is probably tilted, and the tilt is pointing at red
- **Where "due" lands** — it bets black, the one colour the whole record argues against
- **What made the record useful** — it was used to size up the wheel, not to predict a correction
- **The price of that** — hundreds of spins before a lean this size separates from ordinary wobble
- **The everyday version** — a failing machine is more suspect, not more likely to behave

**Key point:** A long record can be genuinely informative — about the machine, never about a debt. The same four hundred spins that say nothing whatever about "due" can say the wheel is crooked, and that reading points the opposite way to the fallacy.

**Source note (`.src`):** Illustrative Example — a constructed log of 400 spins; the reference spread is the exact spread for a true single-zero wheel, summed in the draw function.

### Visualization — canvas `c4`, 720×330

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

## Section 5 — How Far the Odds Move Depends on How Much Was Taken Out

**Tags:** `the boundary` (green), `how much was removed` (blue), `when history pays` (aqua)

**Bullets:**
- **The rule** — the odds move by the share of the pool that has been taken out and kept out
- **Nothing removed** — a coin, a die, a wheel: the move is zero, and no amount of history helps
- **Six cards from one deck** — a bit over a tenth of the pool gone, and the next card's odds shift
- **Six cards from a six-deck shoe** — the same six against a far bigger pool, so almost no shift
- **Eighty tickets gone** — four fifths of the drum removed, and a win is five times likelier
- **What kills the effect** — a pool so large that everything you have seen rounds away against it
- **Why shuffled games feel rigged** — most reshuffle every hand, so nothing is ever depleted
- **The condition on the rule** — it holds when the ones removed were all of the other kind
- **The one question worth asking** — how much of the pool did that last draw permanently remove

**Key point:** Tracking history pays off in proportion to how much of the pool you have removed and kept out. Against a pool that never shrinks — a coin, a wheel, a deck reshuffled every hand — that share is zero, and the entire record is worth nothing to the next draw.

**Source note (`.src`):** Illustrative Example — five constructed pools; each multiplier is computed as one divided by the share of the pool still left.

### Visualization — canvas `c5`, 720×330

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

## Regeneration instructions

- **Template:** the card-section layout from `statistical-paradoxes/03-berksons-paradox.html`, matching the converted sibling `05-clustering-illusion.html`. One `.card-section` per section, each holding an `<h2>` (1.3rem `#1a5276`, `border-bottom: 2px solid #2980b9`, 4px bottom padding) and a `table.layout` with `td.text-col` 50% / `td.viz-col` 50%.
- **Canvas placement:** `td.viz-col` gets `text-align: center` and the canvas `display: block; width: 100%; margin: 0 auto`. The canvas is capped at 720px via `style.maxWidth`, so a wide cell leaves slack and the chart centres in the right half.
- **Text column order:** `.tags` pill row → `<ul>` of one-line bullets each opening `<b>label</b>` then an em dash → one `.key-point` callout → `.src` note. No paragraph blocks, no data tables, no `.example` line restating a bullet.
- **Bullet form:** each is ONE line that does not wrap at 50% column width (≤95 characters including the bold label) and is a complete thought. Bullet count follows the content — no quota.
- **Section titles name the content.** No role labels ("The Trap", "Where It Strikes", "Pipeline Defense") and no phrasing that would fit another page.
- **Page CSS:** body system-ui, white, `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with `border-bottom: 2px solid #2980b9`, 8px bottom padding. `.subtitle` `#666` 0.95rem, 32px bottom margin. `.card-section` 40px bottom margin. `table.layout` full width, border-collapse, cells vertical-align top padding 12px. `ul` 0.92rem, margin `8px 0 8px 20px`, `li` 4px bottom margin, `li b` in `#1a5276`. `.key-point` background `#f8f9fa`, `border-left: 3px solid #e74c3c`, padding 8px 12px, 0.9rem. `.src` 0.78rem `#888`. No nav, no `.nav` CSS, no back/home links, no cross-page links.
- **Tag pills:** `display:inline-block`, 0.72rem, weight 600, padding 2px 10px, radius 10px. Classes used: `.blue`, `.red`, `.orange`, `.violet` `rgba(74,58,167,0.12)`/`#4a3aa7`, `.magenta` `rgba(213,81,129,0.14)`/`#c2426f`, `.aqua` `rgba(25,158,112,0.14)`/`#17805d`, `.yellow` `rgba(201,133,0,0.15)`/`#a06a00`.
- **Colour: one hue family per section.** Section 1 violet/magenta, section 2 aqua/blue with a yellow gap marker, section 3 orange/yellow, section 4 magenta with a green read, section 5 green/yellow over mute. Do not let blue-fill-plus-orange-highlight become every chart.
- **Canvas:** CSS `width: 100%`, `border: 1px solid #e0e0e0`, radius 4px. Intrinsic `width="720"`; heights c1 340, c2 320, c3 340, c4 330, c5 330. `setup(id)` caches the logical size in `dataset` on first call (because `canvas.width` overwrites the attribute), sets `style.maxWidth = 720px`, computes `scale = (cssW/720) × devicePixelRatio`, sizes the backing store to `logical × scale`, and `ctx.scale(scale, scale)` back to logical coordinates. Draws registered in `__charts`, re-run on debounced (150ms) resize.
- **Canvas fonts:** chart title bold 15px; in-chart header bold 12–13px; body/axis labels 12px floor; the big callout figure bold 19px; caption bold 13px.
- **Palette** (shared `P` object): `blue #2a78d6`, `green #008300`, `magenta #d55181`, `yellow #c98500`, `aqua #199e70`, `orange #d95926`, `violet #4a3aa7`, `ink #1a5276`, `text #2c3e50`, `mute #6b7280`, `grid #e5e9ef`.
- **Determinism:** no `Math.random()`. Seeded Park–Miller LCG (`s = (s × 16807) % 2147483647`), seed 42, used only by c3. c1, c2, c4 and c5 are exact — pocket tallies, remaining-card counts, a log-space binomial sum, and a one-line multiplier. Every printed figure is computed in the draw function from the plotted data.
- **The page's whole point is the pair of cases.** Section 2 must show a rising line beside a flat one on the same axes; if the reader cannot see the deck's odds moving while the wheel's do not, the page has not earned its subject. Most treatments of this fallacy cover only the memoryless case and leave the reader unable to tell when tracking history is smart.
- **Corrections applied to the earlier version of this page:**
  - The old lead chart plotted a "feeling of getting closer" as a rising dashed curve against a flat reality line — a drawn shape with no data behind it, and the rising curve had no computed meaning at all. Replaced by two identical computed bar groups, which is the actual claim.
  - The old page carried a lottery table asserting 9.6% versus 10.0% for spreading ten tickets over ten days. The arithmetic (`1 − 0.99¹⁰` = 9.56% against `10/100` = 10.0%) was right, but the setup silently assumed the same hundred-ticket pool refilled daily while the ten-on-one-day case drew from a single pool without replacement — the two rows were not the same experiment. Dropped.
  - The old "Independent vs Dependent" chart drew the dependent case as a curve following `0.7 − 0.5t²`, an invented shape presented as probability. Replaced by the exact `26/(52−k)` deck curve.
  - The old page claimed a five-heads run in twenty tosses happens "in 1 out of every 4 sequences" (the exact figure is 25.0%, so the claim was sound) but the whole streak-in-a-series angle belongs to `05-clustering-illusion` and is removed here to avoid duplicating it. This page is forward-looking only: what the next draw's odds actually are.
  - The old page's model-training and autocorrelation sections are gone with the pipeline framing. The dilution mechanism — the ratio returning to half without tails becoming likelier — replaces them, since that is the part of this fallacy usually taught wrongly.
