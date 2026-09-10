# AI Over-Trust: A Few Good Answers Are Not a Reference Check

**Page type:** detail page — card-section template (see `05-cognitive-biases/25-mere-exposure-effect.html`)
**HTML title tag:** AI Over-Trust — Cognitive Biases

**Subtitle:** Spot-checking a new colleague is sound reasoning, because a clean sample says something about the person. The same spot check on a generator says nothing about the answers you did not read.

---

## Section 1 — The Same Clean Spot Check, Two Different Meanings

**Tags:** `core idea` (violet), `five checks` (blue), `nothing learned` (magenta)

**Bullets:**
- **The batch** — 100 pieces of work arrive, you read 5 closely, and all 5 come back clean
- **Checking a person** — a person has one skill level, and every item in the batch came out of it
- **What the sample does** — 5 clean items are unlikely from a careless worker, so they rule that out
- **Before the check** — a new colleague could be anywhere from flawless to 30% wrong, so 14.3 bad
- **After 5 clean** — the same arithmetic now expects 10.8% wrong, so 10.3 bad in the 95 unread
- **The check earned** — 28% of the expected trouble removed, without opening the other 95
- **Checking a generator** — there is no skill level, only an error rate that each answer redraws
- **Before and after** — 11.4 bad items among the 95 unread, the identical number both times
- **Why it does not move** — a clean run happens 53% of the time at that rate, so it is unremarkable

**Key point:** With a person, "the first five were fine" is evidence about the sixth, because the sample and the rest share one cause. With a generator there is no shared cause to learn about, so the same clean five leave the estimate exactly where it started.

**Source note (`.src`):** Illustrative Example — the person's figures are an exact update on a flat prior over 0–30% error; the generator's are its fixed rate. Both are computed in the draw function.

### Visualization — canvas `c1`, 720×340

Two panels, one per assumption, each showing expected bad items among the 95 unread before and after the five clean checks — so the person's bar drops and the generator's does not.

- **Data:** no PRNG on this chart, arithmetic only. `N=100`, `CHECK=5`, `UNREAD=95`. Person: flat prior over error rate 0.000–0.300 in 0.001 steps, posterior weight `(1−e)^k`, expected rate `Σ e·w / Σ w`. Generator: `ERR = 0.12` fixed, items independent.
- **Computed:** person expected rate **15.0% → 10.8%**, bad items in the unread **14.3 → 10.3**, a **28%** reduction. Generator **11.4 → 11.4**, reduction **0%**. `P(5 clean | generator) = (1−0.12)^5 = 53%`. Every one of these is derived in the draw function, including the percentage drop.
- **Title (bold 15px `P.ink`, centered, y=21):** "Expected Bad Items Among the 95 You Did Not Read"
- **Layout:** two panel groups side by side across `PX=62 … w−40`, baseline y=246, top y=64, one shared y-scale to 16 items with gridlines at 0/4/8/12/16 labelled 12px `P.mute`. Two bars per group, 64px wide, at "before any check" and "after 5 clean".
- **Bars:** person `rgba(74,58,167,0.45)` stroked `P.violet`; generator `rgba(213,81,129,0.50)` stroked `P.magenta`. Each bar labelled bold 19px in its hue with its value to one decimal.
- **Panel headers** (bold 12px at each group's left): `P.violet` "IF A PERSON WROTE IT — one skill level behind all 100" and `P.magenta` "IF A GENERATOR WROTE IT — each item drawn on its own".
- **Drop arrows:** inside each group, a 1.5px arrow from the first bar's top to the second's, labelled bold 12px in the group hue with the computed reduction — "−28%" and "no change".
- **Footnote line** (12px `P.mute`, left at `PX`, `BASEY+56`): "a clean run of five is the ordinary outcome for the generator — it happens 53% of the time", with the 53% computed.
- **Caption (bold 13px `P.violet`, centered, `h−8`):** "Same five clean items. One of these panels learned something."

---

## Section 2 — Twenty Quiet Rounds and the Checking Is Gone

**Tags:** `the ratchet` (orange), `trust grows` (yellow), `defects do not` (red)

**Bullets:**
- **Each answer** — has 8 things that could be wrong in it, each wrong 10% of the time on its own
- **So most answers** — carry at least one wrong thing 57% of the time, and 0.8 on average
- **Round one** — you are new to this and unsure, so you check 5 of the 8 things and find nothing
- **The rule everyone follows** — a clean round earns a shorter check next time, a bad one resets it to 5
- **Rounds two to five** — clean, clean, clean, clean, and the check has fallen from 5 to 1
- **Rounds five onward** — 11 of the 20 rounds are checked one thing deep, which is 12% of the surface
- **What was caught** — 2 of the 13 wrong things across the 20 rounds, so 11 went out unnoticed
- **Had the check held at 5** — 8 of the 13, four times as many, for a little over twice the reading
- **Why the streak felt earned** — checking 5 of 8 leaves a clean round 59% of the time by itself

**Key point:** Nothing about the generator changed across the 20 rounds — the error rate was fixed the whole way. What changed was the checking, and it was cut by the very streak of clean rounds that a fixed error rate produces on its own.

**Source note (`.src`):** Illustrative Example — 20 seeded rounds of 8 aspects at a fixed 10% error rate, run twice under the two checking rules; every count and depth is read off the plotted rounds.

### Visualization — canvas `c2`, 720×340

One row per round showing how deep the check went and where the wrong things sat, so the shrinking check and the wrong things drifting past its edge appear on the same picture.

- **Data:** seeded Park–Miller LCG with 50 warm-up draws, seed 7. `ROUNDS=20`, `ASPECTS=8`, `PA=0.10`, start depth 5, floor 1. Aspect `a` of a round is wrong when `r() < PA`. Adaptive rule: found a defect → depth back to 5, otherwise depth `max(1, depth−1)`. Held rule: depth 5 every round.
- **Computed:** depth path **5,4,3,2,1,1,1,1,1,1,5,5,4,3,2,1,1,1,1,1** — average **2.20** of 8, i.e. **28%** of the surface. **13** wrong things present. Adaptive caught **2**, escaped **11**. Held at 5 caught **8**, escaped **5** on the same draws — a **4.0×** difference. **11** of the 20 rounds sit at depth 1. `P(5 of 8 clean) = 0.9^5 = 59%`.
- **Warm-up is load-bearing.** Park–Miller's first output from a small seed is near zero, which would force aspect 0 wrong in round one and reset the ratchet before it ever starts. The 50 discarded draws remove that.
- **Title (bold 15px `P.ink`, centered, y=21):** "Twenty Rounds, Eight Things Each, One Fixed Error Rate"
- **Layout:** 20 rows from y=62 on a 12px pitch, 8 aspect cells per row across `PX=104 … PX+8×26`. Round numbers 12px `P.mute` at the left of each row; aspect numbers 12px `P.mute` above the first row.
- **Cells:** an aspect inside the round's check depth is filled `rgba(217,89,38,0.16)`, outside it `#fff`, both stroked `P.grid`. A wrong aspect gets a 2px cross — `P.orange` when it fell inside the depth and was caught, hard `#e74c3c` when it fell outside and escaped.
- **Depth edge:** a 2px `P.orange` line stepping down the rows at the boundary of each round's checked region, so the ratchet reads as one staircase, with 12px `P.orange` "the check shrinks" beside its first descent and the two resets marked with a small `P.yellow` tick.
- **Right-hand tally** (from `PX+8×26+26`): bold 13px `P.ink` "ACROSS 20 ROUNDS", then bold 19px `P.orange` "2 of 13" over 12px `P.mute` "wrong things caught", then bold 19px `#e74c3c` "8 of 13" over 12px `P.mute` "caught if the check had held at 5". All four figures computed.
- **Note line** (12px `P.mute`, centered, `h−28`): "average check 2.2 of 8 things — 28% of what was there to read", computed from the depth path.
- **Caption (bold 13px `P.orange`, centered, `h−8`):** "The streak of clean rounds is what removed the checking."

---

## Regeneration instructions

- **Template:** the card-section layout from `05-cognitive-biases/25-mere-exposure-effect.html`. One `.card-section` per section, each an `<h2>` plus a `table.layout` with `td.text-col` 50% / `td.viz-col` 50%.
- **Text column order:** `.tags` pill row → `<ul>` of one-line bullets each opening `<b>label</b>` then an em dash → one `.key-point` callout → `.src` note.
- **Bullet form:** one line at 50% column width, 90–100 characters including the bold label.
- **Page CSS:** identical to `25-mere-exposure-effect.html`. No nav, no `.nav` CSS, no back/home links, no cross-page links.
- **Hue family per section:** 1 violet against magenta, 2 orange with a hard red for the escapes.
- **Canvas:** intrinsic `width="720"`, both heights 340. `setup(id)` caches the logical size in `dataset`, sets `style.maxWidth = 720px`, computes `scale = (cssW/720) × devicePixelRatio`, sizes the backing store to `logical × scale`, and `ctx.scale(scale, scale)`. Draws registered in `__charts`, re-run on debounced (150ms) resize.
- **Canvas fonts:** chart title bold 15px; in-chart header bold 12px; body and axis labels 12px floor; big callout figure bold 19px; caption bold 13px.
- **Palette** (shared `P`): `blue #2a78d6`, `green #008300`, `magenta #d55181`, `yellow #c98500`, `aqua #199e70`, `orange #d95926`, `violet #4a3aa7`, `ink #1a5276`, `text #2c3e50`, `mute #6b7280`, `grid #e5e9ef`. Hard red `#e74c3c` is used only for the escaped defects.
- **Determinism:** no `Math.random()`. Section 1 uses no generator at all — it is an exact update, so a draw would only add noise to a number the reader can check. Section 2 uses the seeded LCG with **50 discarded warm-up draws**.
- **Every printed figure is computed in its draw function** — the two posteriors, both bad-item counts, the 28% and the 53% in section 1; the depth path, both catch counts, the 4.0× and the average depth in section 2.
- **What separates this page from card 12.** Card 12 is about a machine's name on a suggestion lowering scrutiny on that suggestion. This page is about a sample being read as evidence about the items nobody opened — sound inference for a person, empty for a generator. Keep the two arguments apart; do not add a badge or a label effect here.
- **Keep this page at two sections.** The trust arc the page describes — hesitant at first, then barely checking — is section 2's staircase, not a third section.
- **Do not add a fluency or hesitancy section.** An earlier draft contrasted how a person's wrong work arrives hedged while a generator's arrives uniformly fluent. It computes cleanly but needs a second construction and a hit-rate statistic, and the ratchet already carries the practical lesson.
- **The 0–30% prior is a stated assumption, not a measurement.** It stands for "a new colleague could be anywhere from flawless to fairly careless"; the point survives any reasonable range, since only the person's number moves at all.
