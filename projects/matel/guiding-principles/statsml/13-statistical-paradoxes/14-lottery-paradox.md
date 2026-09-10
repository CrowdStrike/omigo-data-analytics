# Lottery Paradox: Every Ticket Will Lose, Yet One of Them Wins

**Subtitle:** A belief can be safe on its own and still belong to a set that is certainly wrong

Four `.card-section` blocks, canvases `c1`–`c4`, 720×320. Layout, CSS, `setup()`, `P` palette
copied verbatim from the shared skeleton.

---

## 1. A Million Tickets, A Million Safe Bets, One Certain Winner

**Tags:** `core idea` (blue) · `one draw` (green) · `all at once` (orange)

- **The setup** — a fair draw sells a million tickets and exactly one of them will be the winner.
- **Any single ticket** — saying "this one loses" is right 999,999 times out of a million.
- **Name your bar** — whatever level of certainty you demand, that claim clears it easily.
- **So accept them all** — the same rule licenses the same claim about every ticket in the pile.
- **Now the trouble** — believing all million at once says nobody wins, which you know is false.
- **Nothing was sloppy** — no single step was careless; the set is wrong even though each part is fine.
- **The lesson about rules** — "accept anything past a high bar" cannot be applied claim by claim.
- **What survives** — the odds on each ticket; what does not survive is the pile of accepted claims.

**Example:** Believe the first 500,000 "this ticket loses" claims and you are right about all of
them barely half the time; take all 1,000,000 and you are right zero percent of the time.

**Key point:** High confidence in each piece says nothing about confidence in the whole set.

**Chart c1** — confidence per claim vs confidence in the whole set
- x-axis: number of "this ticket loses" claims believed, 0 → 1,000,000 (ticks 0, 250k, 500k, 750k, 1M).
- y-axis: chance every claim believed so far is true, 0 → 100%.
- Magenta flat line at the per-claim value `1 − 1/N`, labelled with that computed number.
- Green descending line: exact joint value `(N − k)/N`, since exactly one ticket wins.
- Orange marker at k = 500,000 with its computed value; big 19px figure at the k = N end.
- Caption: each bet is nearly certain, believing all of them together is certainly wrong.
- `.src`: Illustrative Example.

---

## 2. Two Hundred Reliable Steps Make One Unreliable Pipeline

**Tags:** `data work` (blue) · `pipelines` (aqua) · `dashboards` (orange)

- **Same arithmetic** — every step in a pipeline is a claim that it worked; you need all of them.
- **A long pipeline** — 200 steps that each work 999 times in 1,000 finish clean about 82% of runs.
- **Read that again** — roughly one run in five breaks somewhere, with no unreliable step to blame.
- **A dashboard** — 50 tiles that are each right 99 times in 100 are all right only about 61% of days.
- **What gets reported** — the per-step number, because it is the flattering one and easy to measure.
- **What is never reported** — the chance the whole board is clean, which is the number decisions use.
- **Length is the driver** — adding steps lowers the chance of a clean run even if nothing degrades.
- **The fix is arithmetic** — to promise 95% for a 200-step run each step must clear about 99.97%.

**Example:** A 200-step pipeline at 99.9% per step succeeds end-to-end 81.9% of the time; a
50-tile dashboard at 99.0% per tile is fully correct 60.5% of the time.

**Key point:** Per-item reliability is not system reliability, and only the second one ships.

**Chart c2** — grouped bars, per-item vs whole-set
- Group one: 200-step pipeline — magenta bar for per-step 99.9%, green bar for `0.999^200`.
- Group two: 50-tile dashboard — magenta bar for per-tile 99.0%, green bar for `0.99^50`.
- All four heights and printed percentages computed with `Math.pow` in the draw function.
- Big 19px figure over each green bar; small mute note giving the failure share of each.
- Caption: the per-item number looks fine, the number that matters is the one for the whole set.
- `.src`: Illustrative Example.

---

## 3. One Hundred Published Findings, Thirty-Six That Held Up

**Tags:** `documented case` (red) · `repeat the study` (blue) · `honest caveat` (mute)

- **The project** — a large team repeated 100 published psychology studies with bigger samples.
- **Each original** — had cleared the usual bar for being publishable on its own terms.
- **The repeat run** — roughly 36 of the 100 came back with a result in the same direction and size range.
- **Read as a set** — a shelf of individually accepted findings did not hold together as a body.
- **Why it fits here** — the accept-each-one rule was followed, and the collected shelf still failed.
- **Honest caveat** — several causes drive this: smaller true effects, method drift, what gets published.
- **Not the whole story** — the all-at-once problem is one contributor, not the full explanation.
- **The usable part** — never quote a per-study confidence as confidence in a literature.

**Example:** Of 100 repeated studies, about 36 reproduced — a shelf where every single item had
individually passed the bar for publication.

**Key point:** A body of work is only as sound as the joint claim, never as the best single paper.

**Chart c3** — 10×10 waffle of the 100 repeated studies
- 36 cells green (result reproduced), 64 cells mute grey with thin outline; counts computed from
  the fill loop and printed, not hardcoded.
- Big 19px figure "36 of 100" plus a small grey parenthetical naming the 2015 study.
- Right-side mute lines: each had passed on its own; several causes contribute.
- Caption: every item passed on its own, the shelf did not hold together.
- `.src`: Open Science Collaboration (2015), *Estimating the reproducibility of psychological
  science*, Science 349(6251).

---

## 4. When One Claim At A Time Keeps A High Bar Honest

**Tags:** `boundary` (green) · `set size` (blue) · `practice` (orange)

- **The trap needs two things** — many claims, and a decision that depends on all of them at once.
- **Acting on one claim** — if each decision stands alone, a high bar is exactly the right rule.
- **Independent bets** — running 50 unrelated experiments is fine if no single call needs all 50 right.
- **Where it returns** — the moment one report, model, or launch rests on the whole set being true.
- **Two dials, not one** — how sure you are per claim, and how many claims you are stacking up.
- **State the joint number** — quote the confidence for the exact set you are about to act on.
- **Raise the bar as the set grows** — ten claims and two hundred claims cannot share one threshold.
- **Keep the odds** — carry probabilities forward instead of flipping claims to flatly true.

**Example:** At 95% per claim the set is more likely wrong than right by the 14th claim; at 99.9%
per claim that point does not arrive until roughly the 693rd.

**Key point:** A high bar is safe for one claim at a time and needs raising for every claim you add.

**Chart c4** — family of joint-confidence curves
- x-axis: number of claims in the set, 1 → 200. y-axis: chance the whole set is right, 0 → 100%.
- Four curves `q^k` for q = 0.90 (violet), 0.95 (blue), 0.99 (aqua), 0.999 (green).
- Orange dashed line at even chance; a dot on each curve where it crosses.
- Right-side legend per curve: value at 200 claims, and the claim count where it hits even odds —
  both computed (`Math.pow`, `log 0.5 / log q`).
- Header note: at one claim each every line starts safe; the drop comes from stacking.
- Caption: how many claims you act on decides the bar each claim must clear.
- `.src`: Illustrative Example.
