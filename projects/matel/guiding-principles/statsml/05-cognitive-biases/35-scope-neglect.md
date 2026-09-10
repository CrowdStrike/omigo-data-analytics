# Scope Neglect: Judged on My Use Cases, Not Its Feature Set

**Page type:** detail page — card-section template (see `05-cognitive-biases/25-mere-exposure-effect.html`)
**HTML title tag:** Scope Neglect — Cognitive Biases

**Subtitle:** A system is built to do a stated set of things well. Each person tries the handful they happened to need, and the verdict they walk away with describes their handful rather than the system. Nobody checks what it was built for before deciding what it is.

---

## Section 1 — Everyone Grades It on Their Own Three Features

**Tags:** `core idea` (violet), `ten features` (blue), `fifteen verdicts` (magenta)

**Bullets:**
- **The system** — 10 features in the version that shipped, 8 of them solid and 2 of them weak
- **Its honest score** — 8 of 10, and that is what it is regardless of who is asked
- **What each person does** — tries the 3 features their own work needs, and judges from those
- **Nobody reads the built-for list** — so there is nothing to compare their 3 against
- **7 of the 15 people** — happen to miss both weak features and call the system perfect
- **7 of the 15 people** — hit one weak feature and score it 2 of 3, a third of it broken
- **1 of the 15 people** — hits both and comes away with 1 of 3, which reads as unusable
- **Not one verdict is 8 of 10** — no slice of 3 can land on the number the system deserves
- **Average all 15 verdicts** — and they come to exactly 8 of 10, which nobody individually said

**Key point:** The bias is not that people are harsh. Half of them are too generous by the same mechanism. It is that a verdict formed on a self-chosen slice gets carried and repeated as a verdict on the system, and the slice is invisible in the retelling — so the loudest few slices become the system's reputation.

**Source note (`.src`):** Illustrative Example — one 10-feature system with 2 weak features, graded by 15 people on a fixed 3-feature slice each; every verdict and the average are counted in the draw function.

### Visualization — canvas `c1`, 720×340

Fifteen verdicts as a column of short bars against the system's true score, so the spread of opinions sits beside the one line none of them touch.

- **Data:** no PRNG. 10 features, weak set `{4, 9}`, and 15 fixed 3-feature slices listed in the code so each feature is tried by 4 or 5 people. Each person's verdict is the count of solid features in their own slice.
- **Computed:** verdict tallies **7 at 3 of 3**, **7 at 2 of 3**, **1 at 1 of 3**; solid features seen **36 of 45**, which is **80%**, the same as the system's 8 of 10. Counted from the slices rather than typed.
- **Title (bold 15px `P.ink`, centered, y=21):** "Fifteen People, Fifteen Verdicts, One System"
- **Layout:** a feature strip across the top and a verdict column beneath it. Strip at y=52, 10 slots across `PX=62 … w−150`, height 26, labelled 12px `P.mute` "1" … "10"; solid features `rgba(25,158,112,0.30)` stroked `P.aqua`, the 2 weak ones `rgba(213,81,129,0.55)` stroked `P.magenta` and tagged bold 12px `P.magenta` "weak".
- **Verdict rows:** 15 rows on a 13px pitch from y=118, each a 3-cell run of 11px cells showing that person's slice — a solid feature aqua, a weak one magenta with a 1.5px `#e74c3c` cross.
- **Row labels** (12px `P.mute`, right of each run): the verdict as "3 of 3" / "2 of 3" / "1 of 3", in `P.aqua` when 3 and `P.magenta` otherwise.
- **The truth line:** a 2px dashed `P.violet` vertical line at the 80% position of the verdict scale, labelled bold 12px `P.violet` "the system is 8 of 10", with a 12px `P.mute` second line "no single verdict can land here".
- **Tally block** (right side, from y=118): bold 19px `P.aqua` "7 say perfect" over 12px `P.mute` "missed both weak features", then bold 19px `P.magenta` "8 say broken" over 12px `P.mute` "hit at least one", all computed.
- **Note line** (12px `P.mute`, centered, `h−28`): "the 15 verdicts average to exactly 8 of 10 — the number none of them reported", computed.
- **Caption (bold 13px `P.violet`, centered, `h−8`):** "The slice is invisible by the time the verdict is repeated."

---

## Section 2 — The Slice Disappears When the Verdict Is Repeated

**Tags:** `how it spreads` (orange), `slice dropped` (yellow), `one voice left` (red)

**Bullets:**
- **What the person actually knows** — "3 of the 3 features I needed worked", which is true and narrow
- **What gets said out loud** — "it works well", with the 3 features left out of the sentence
- **What the harsh version becomes** — "it is broken", from someone whose 3 happened to include a weak one
- **Both sentences lost the same thing** — the list of what was tried, which was the only qualifier
- **Passed on once** — the hearer has a verdict on the system and no idea it came from 3 features
- **Nobody is lying** — the slice is dropped because it feels irrelevant, not because it is hidden
- **Which verdict travels** — the strong one, since "it is broken" is more worth repeating than "it is fine"
- **So the average never forms** — the 15 verdicts would balance out, but only the loud ones move
- **What restores it** — saying which features you tried, which costs one clause and rarely happens

**Key point:** The verdict and its scope are formed together and travel separately. Every person's opinion is honest and correctly qualified in their own head; the qualifier is dropped at the first retelling, and from then on a 3-feature observation circulates as a statement about a 10-feature system.

**Source note (`.src`):** Illustrative Example — the same 15 verdicts, shown with and without the slice attached; the counts are tallied in the draw function.

### Visualization — canvas `c2`, 720×340

**A rumour, not a chart.** The same verdict passed along a chain of people, with the qualifier visibly falling off between the first and second speaker.

- **Data:** no PRNG. The 15 slices from section 1 supply the two verdicts that get repeated — one from a person who saw 3 solid features, one from the person who saw 2 weak ones. The chain is 4 speakers long, fixed.
- **Computed:** the two originating verdicts and the underlying "3 of 3" / "1 of 3" are taken from the section-1 slice list, and the count of people holding an unqualified verdict at the end of the chain is tallied.
- **Title (bold 15px `P.ink`, centered, y=21):** "The Same Verdict, One Retelling Later"
- **Layout:** two chains stacked, one per verdict, each a row of 4 speaker bubbles across `PX=52 … w−52` with an arrow between consecutive bubbles — chain one at y=104, chain two at y=232.
- **Speaker bubbles:** 128×58 rounded rectangles, the first in each chain stroked in the chain hue with a solid fill, the rest white stroked `P.grid`.
- **Bubble text:** the first bubble carries two lines — bold 12px in the chain hue with the verdict, and 12px `P.mute` with the slice, e.g. "features 1, 2, 3". Every later bubble carries only the verdict line, with a 12px `P.grid` strikethrough placeholder where the slice used to be.
- **The dropped qualifier:** between bubble 1 and 2 in each chain, a small 12px `P.mute` label "the slice falls off here" with a 1px `P.grid` line curving downward away from the arrow.
- **Chain hues:** chain one `P.aqua` for the kind verdict ("it works well"), chain two `P.magenta` for the harsh one ("it is broken").
- **Chain labels** (bold 12px at each chain's left, above bubble 1): `P.aqua` "SAW 3 SOLID FEATURES" and `P.magenta` "SAW 2 WEAK ONES".
- **The travelling note:** bold 12px `#e74c3c` under chain two, "the harsh verdict is the one worth repeating, so it travels further".
- **Note line** (12px `P.mute`, centered, `h−28`): "by the fourth speaker, nobody can say which features were ever tried".
- **Caption (bold 13px `P.orange`, centered, `h−8`):** "The qualifier costs one clause and is dropped every time."

---

## Regeneration instructions

- **Template:** the card-section layout from `05-cognitive-biases/25-mere-exposure-effect.html`. One `.card-section` per section, each an `<h2>` plus a `table.layout` with `td.text-col` 50% / `td.viz-col` 50%.
- **Text column order:** `.tags` pill row → `<ul>` of one-line bullets each opening `<b>label</b>` then an em dash → one `.key-point` callout → `.src` note.
- **Bullet form:** one line at 50% column width, 90–100 characters including the bold label.
- **Page CSS:** identical to `25-mere-exposure-effect.html`. No nav, no `.nav` CSS, no back/home links, no cross-page links.
- **Hue family per section:** 1 aqua against magenta with a violet caption, 2 aqua against magenta with a hard red travelling note.
- **Section 2 is not about search order.** An earlier version of this material covered the reviewer's tendency to hunt for corner cases before checking what works, with a running-verdict chart per search order. That is a separate bias with a separate mechanism — the reviewer needs a visible artifact — and it now lives on its own page, `05-cognitive-biases/37-flaw-finding-reflex`. Do not reintroduce a search-order section, a running score, or a two-orders chart here.
- **Neither visualization is a data-science chart.** Section 1 is a strip of features with one verdict row per person; section 2 is a rumour travelling along a chain of speakers. Keep them concrete.
- **Canvas:** intrinsic `width="720"`, both heights 340. `setup(id)` caches the logical size in `dataset`, sets `style.maxWidth = 720px`, computes `scale = (cssW/720) × devicePixelRatio`, sizes the backing store to `logical × scale`, and `ctx.scale(scale, scale)`. Draws registered in `__charts`, re-run on debounced (150ms) resize.
- **Canvas fonts:** chart title bold 15px; in-chart header bold 12px; body and axis labels 12px floor; big callout figure bold 19px; caption bold 13px.
- **Palette** (shared `P`): `blue #2a78d6`, `green #008300`, `magenta #d55181`, `yellow #c98500`, `aqua #199e70`, `orange #d95926`, `violet #4a3aa7`, `ink #1a5276`, `text #2c3e50`, `mute #6b7280`, `grid #e5e9ef`. Hard red `#e74c3c` for weak-feature crosses and the travelling note only.
- **Determinism:** no `Math.random()` and no generator on either chart. The 15 slices are a fixed list and the chains are fixed, because the lesson is the spread of verdicts and a draw would only make the spread irreproducible. Keep the slice list balanced so each feature is tried by 4 or 5 people; an unbalanced list would make the average miss 80% for an uninteresting reason.
- **Every printed figure is computed in its draw function** — all 15 verdicts, both tallies and the average in section 1; both originating verdicts in section 2, read off the same slice list.
- **Both directions of error are required.** 7 of the 15 call the system perfect and 8 find a flaw. Showing only the harsh verdicts would turn the page into a complaint about users; the point is that a self-chosen slice is wrong in whichever direction it happens to fall.
- **Do not frame this as "the system is fine and users are wrong."** The 2 weak features are genuinely weak and every verdict on the page is honest about what that person saw. The error is in the generalisation step, not in the observation.
- **Keep the numbers round.** 10 features, 2 weak, 3 checked each, 15 people. No probability arithmetic — the 47% chance that a random 3-slice misses both weak features is a real figure and it does not belong on the page, because the fixed slice list already shows the outcome by counting.
- **The slice is the subject, in both sections.** Section 1 shows the slice being formed, section 2 shows it being dropped. Anything that is not about the slice belongs on another page.
- **What separates this from card 33.** Halo Effect is generalising from strengths you did observe to skills you did not. This page is the opposite direction — a verdict formed on a slice, where the slice was never compared against what the system claimed to cover.
- **What separates this from card 34.** Over-Stretch Blame is about the operating point: a system run past its rated load. This page is about the feature set: a system used inside its load but graded on features it never claimed.
- **What separates this from card 20.** Salience Bias is about a seen failure getting fixed regardless of size. Here the seen failure is not being over-fixed, it is standing in for the whole system in a judgement.
- **What separates this from card 37.** Flaw-Finding Reflex is about a reviewer's opening move and the credit it earns. This page is about a verdict formed on whichever features someone happened to need, and about the qualifier being dropped when that verdict is passed on.
- **Keep this page at two sections.**
