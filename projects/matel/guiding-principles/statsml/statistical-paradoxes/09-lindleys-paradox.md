# Lindley's Paradox: When "Not Chance" Still Means "Nothing Happened"

**Subtitle:** A big enough sample can flag a difference as real while the difference itself is the best evidence that nothing is going on

Four `.card-section` blocks, 50/50 text/canvas, canvas ids `c1`–`c4`.

---

## 1. A winning test nobody would act on

Tags: `core idea` (blue), `A/B test` (green), `10 million visitors` (orange)

- **The setup** — 10 million visitors split evenly, and the old page converts 10.000% of them.
- **The result** — the new page converts 10.041%, which is 2,050 extra sign-ups out of 5 million.
- **Typical wobble** — chance alone shifts this comparison about 0.019 percentage points either way.
- **The verdict** — the gap is 2.2 wobbles wide, so it clears the usual 5% bar for "not chance".
- **What 5% means** — if nothing were happening, one test in twenty would still look like a winner.
- **The catch** — anything above 0.037 points clears that bar at this size, and 0.041 barely does.
- **Worth shipping** — the team agreed up front that only a 0.50-point lift pays for the rewrite.
- **Better explanation** — if a real lift would sit near a point, "nothing" fits this gap 5× better.

Example: *Illustrative: 502,050 conversions against 500,000 — clears the bar, changes no decision.*

Key point: A sample this large can make a difference far too small to act on look like a discovery.

Src: Illustrative Example — figures computed from the stated conversion rates.

**Chart c1** (720×340) — two stacked number lines of lift in percentage points:
- Zoomed strip, axis 0 to 0.10 points: orange dashed line at 0.037 (bar for "not chance"), magenta dot at 0.041 with its uncertainty range 0.004 to 0.078 drawn as a horizontal bar.
- Full-scale strip, axis 0 to 0.60 points: green band from 0.50 rightward labelled "worth acting on", same magenta dot near zero.
- Mute double-headed arrow spanning the two, big bold figure "12× too small to matter" computed as 0.50 ÷ 0.041.
- Caption: clears the bar, misses the floor by twelvefold.

---

## 2. At web scale the bar stops filtering

Tags: `second domain` (blue), `web experiments` (aqua), `sample size` (orange)

- **Smallest catchable gap** — at 1,000 visitors per side only a 2.6-point lift clears the bar.
- **It shrinks fast** — 5,000 per side needs 1.18 points, and 100,000 per side needs 0.26 points.
- **At millions** — 5 million per side catches 0.037 points, 10 million per side catches 0.026.
- **Square-root rule** — 1,000 times more visitors makes the catchable gap only 32 times smaller.
- **The crossing** — past about 27,700 per side, the bar sits below the 0.50-point action line.
- **What breaks** — the bar then separates nothing from almost-nothing, not real from worthwhile.
- **Everything wins** — with enough traffic, button colour and font size both come back "not chance".
- **More data is not the fix** — extra visitors sharpen the estimate and lower the bar in lockstep.

Example: *Illustrative: a 0.03-point lift is invisible at 20,000 per side and a clear winner at 10 million.*

Key point: Growing the sample does not just sharpen the test, it drags the finish line below the point of caring.

**Chart c2** (720×320) — smallest lift that clears the bar, against visitors per side:
- Log x from 1,000 to 10,000,000; log y from 0.02 to 4 points, gridlines at 0.02/0.05/0.1/0.2/0.5/1/2/4.
- Blue curve of the catchable gap; green horizontal line at 0.50 labelled "smallest lift worth acting on".
- Magenta shading in the wedge between curve and green line to the right of the crossing.
- Orange dashed vertical at the crossing, solved in code, labelled with the rounded sample size.
- Dots with computed labels at 5,000, 100,000 and 5,000,000 per side.
- Caption: past the crossing, clearing the bar stops meaning anything worth shipping.

---

## 3. The birth records behind the original puzzle

Tags: `documented case` (blue), `birth records` (violet), `1957` (orange)

- **The record** — the original paper weighed 98,451 births, of which 49,581 were boys.
- **The question** — whether boys and girls arrive in an exact 50-50 split, nothing more.
- **The observation** — boys came to 50.361% of the total, a hair over half by 355 births.
- **Typical wobble** — an exact 50-50 process moves that share about 0.159 points either way.
- **The bar** — anything above 50.312% clears the 5% mark here, and 50.361% just does.
- **How rare** — an exact 50-50 process produces a gap this wide about one time in 43.
- **The other reading** — spread the guess evenly across every possible share and exact half wins 19 to 1.
- **Older credit** — Jeffreys made the same point earlier, so the puzzle carries both names.

Example: *The same 98,451 births read as a rejection of the exact split and as roughly 95% confidence in it.*

Key point: One dataset, two honest readings, and it is the size of the record rather than the size of the effect that pulls them apart.

Src: Source: D. V. Lindley, "A Statistical Paradox", Biometrika 44 (1957); the same argument appears earlier in Jeffreys, Theory of Probability. Figures recomputed from the quoted counts.

**Chart c3** (720×340) — what an exact 50-50 split would produce, on a zoomed share axis:
- x from 49.50% to 50.50%; bell curve centred at 50% with wobble 0.159 points, mute fill.
- Both tails beyond the 5% marks filled magenta; orange dashed vertical at the upper mark 50.312%.
- Green tick and label at exact 50.000%; magenta stem and dot at the observed 50.361%.
- Big bold figure "1 in 43" computed from the curve, with a two-line gray gloss beneath it.
- Line noting that an even-handed weighing favours exact half about 19 to 1 (computed in code).
- Caption: the same hair-thin gap reads as a winner and as evidence for exactly half.

---

## 4. When the bar still behaves

Tags: `boundary` (blue), `what to do` (green), `pre-declared floor` (orange)

- **Two ingredients** — the trap needs a very large sample and a very tiny effect at the same time.
- **Modest samples are fine** — at 5,000 per side a barely-clearing result favours a real effect 2.1 to 1.
- **The flip point** — near 75,000 per side, barely-clearing starts favouring "nothing is happening".
- **Big effects are fine** — a 3-point lift at any sample size is a real effect, not a threshold artifact.
- **Decide the floor first** — write down the smallest effect worth acting on before collecting data.
- **Report the range** — say "0.004 to 0.078 points" rather than handing over a yes-or-no verdict.
- **Read the label honestly** — "clears the bar" means "probably not chance", and nothing beyond that.
- **Two questions, not one** — is it real, and is it big enough; the bar only ever answers the first.

Example: *Illustrative: 0.041 points, range 0.004 to 0.078 — the entire range sits under the 0.50-point floor.*

Key point: Pair every "not chance" with an effect size and a floor set in advance, and the paradox never costs a decision.

**Chart c4** (720×320) — for a result sitting exactly on the bar, which story explains it better:
- Log x from 1,000 to 5,000,000 visitors per side; log y odds from 1:10 to 3:1, ticks labelled "1:10", "1:3", "even", "3:1".
- Light green band above even odds labelled "favours a real effect"; light magenta band below labelled "favours nothing happening".
- Violet curve of the odds, humped near 5,000 and falling through even odds; mute line at even.
- Orange dashed vertical at the flip point, solved by bisection in code, labelled with the rounded sample size.
- Dots with computed labels at 5,000 per side and 5,000,000 per side.
- Caption: the bar is trustworthy at everyday sample sizes and misleading at web scale.
