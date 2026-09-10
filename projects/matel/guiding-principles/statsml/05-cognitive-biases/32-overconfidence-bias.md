# Overconfidence Bias: Good Enough to Skip the Tests, and Shipping More Bugs

**Page type:** detail page — card-section template (see `05-cognitive-biases/25-mere-exposure-effect.html`)
**HTML title tag:** Overconfidence Bias — Cognitive Biases

**Subtitle:** An experienced developer really does write cleaner code, decides it no longer needs testing, and ships more bugs than a beginner who tests. The skill is real; the conclusion drawn from it is not.

---

## Section 1 — Three Times the Skill, More Bugs Out the Door

**Tags:** `core idea` (violet), `one release` (blue), `skill is real` (magenta)

**Bullets:**
- **The release** — one batch of work, and the only question is how many bugs reach the customer
- **The beginner** — writes 300 bugs into it, which is three times as many as the experienced one
- **The experienced developer** — writes 100, genuinely three times cleaner, and that part is true
- **A test suite** — catches roughly 4 out of every 5 bugs that are actually in the code
- **Beginner who tests** — 300 written, 240 caught, and 60 get out to the customer
- **Experienced developer who tests** — 100 written, 80 caught, and 20 get out
- **Experienced developer who skips** — 100 written, none caught, and all 100 get out
- **So the best coder here** — ships more bugs than the weakest one, by skipping one step
- **What went wrong** — being better at writing was read as being good enough not to check

**Key point:** Writing well and checking your work are two separate steps, and skill only helps with the first. The experienced developer wins the part they are proud of and loses the part that decides what the customer sees.

**Source note (`.src`):** Illustrative Example — three cases on one release; the caught and shipped counts follow from the bug counts and the catch rate, computed in the draw function.

### Visualization — canvas `c1`, 720×340

Three bars, one per case, each split into bugs caught and bugs that got out, so the longest "got out" bar belongs to the most skilled developer.

- **Data:** no PRNG — plain arithmetic. Beginner writes 300, experienced writes 100, tests catch 80%. Caught `= written × 0.8` when tested, else 0; got out `= written − caught`.
- **Computed:** beginner tested **300 written / 240 caught / 60 out**; experienced tested **100 / 80 / 20**; experienced untested **100 / 0 / 100**. Read out of the same three numbers the bars are drawn from.
- **Title (bold 15px `P.ink`, centered, y=21):** "Bugs That Reach the Customer From One Release"
- **Layout:** three horizontal bars from `PX=196`, bar height 44 on a 78px pitch starting y=68, scaled so 300 spans to `w−60`. Row labels 12px right-aligned at `PX−10`, two lines each — the person in `P.ink`, then "writes tests" or "skips tests" in `P.mute`.
- **Bars:** caught portion `rgba(0,131,0,0.30)` stroked `P.green` labelled 12px `P.green` inside with "caught N"; the got-out portion `rgba(213,81,129,0.55)` stroked `P.magenta` drawn to its right, labelled bold 19px `P.magenta` just past the bar end.
- **Emphasis:** the third row's got-out segment ringed 2px `P.magenta`, tagged bold 12px `P.magenta` above with "the best coder on this chart".
- **Scale line:** 1px `#ccc` axis under the last bar, ticks at 0, 100, 200, 300 labelled 12px `P.mute`, caption "bugs written into the release".
- **Note line** (12px `P.mute`, left at `PX−186`, `h−28`): "the skill gap is real — it just is not the step that decides what gets out".
- **Caption (bold 13px `P.violet`, centered, `h−8`):** "The skill was real. The conclusion drawn from it was not."

---

## Section 2 — Practice Runs Out. Checking Does Not.

**Tags:** `why it feels right` (orange), `practice has a limit` (yellow), `checking does not` (red)

**Bullets:**
- **Why the belief is not silly** — take any single piece of the work and it probably is fine
- **Roughly four pieces in five** — are clean when the experienced developer writes them
- **But a release has hundreds** — so plenty of pieces carry a bug even at that hit rate
- **The mistake** — something true about one piece gets applied to the whole release
- **Practice genuinely helps** — the bug count drops steeply over the first few years of work
- **Then it levels off** — the drop from year ten to year twenty is small, and after that tiny
- **So there is a limit** — untested work never gets much below about 50 bugs a release
- **A first-year developer who tests** — is already at about that level, in their first year
- **The difference** — practice runs out, while checking removes most of whatever is left

**Key point:** Practice and checking are not two routes to the same place. Practice lowers how many bugs you write and then flattens out; checking removes most of however many you wrote, whatever your level. That is why a lifetime of practice lands where a beginner with tests already is.

**Source note (`.src`):** Illustrative Example — an assumed practice curve that flattens rather than reaching zero; both lines and the level-off are computed in the draw function.

### Visualization — canvas `c2`, 720×340

Bugs that got out plotted against years of experience, drawn twice — skipping tests and writing them — so the skipping line flattens onto a level the testing line passed in year one.

- **Data:** no PRNG. Bugs written per release `= 50 + 250 × e^(−y/4)`, so year 0 is 300 and it flattens toward 50. Got out when skipping `= written`; when testing `= written × 0.2`. Years plotted 0, 1, 2, 3, 5, 10, 20, 40.
- **Computed:** skipping **300, 245, 202, 168, 122, 71, 52, 50**; testing **60, 49, 40, 34, 24, 14, 10, 10**. The flat level **50** and the year-1 testing value **49** both come from the same curve.
- **Title (bold 15px `P.ink`, centered, y=21):** "Bugs That Got Out, Against Years of Experience"
- **Layout:** `PX=62 … w−150`, baseline y=232, top y=58, y-axis 0 to 300 with gridlines every 50 labelled 12px `P.mute`. X-axis on `log10(1+y)` so the early years are readable, ticks at the eight plotted years.
- **Lines:** skipping 3px `#e74c3c` with dots radius 5; testing 3px `P.green` with dots radius 5. Each labelled bold 12px in its hue at its right end — "skips tests" and "writes tests".
- **The level-off:** a 2px dashed `P.yellow` line across the plot at the flat value, labelled bold 12px `P.yellow` "however long you practise, skipping tests stays around here".
- **The crossing:** a `P.green` ring on the testing line at year 1 with a 1.5px `P.green` leader to bold 12px "a first-year developer who tests is already here".
- **Right-hand block** (from `w−140`): bold 13px `P.ink` "TWO THINGS", then bold 12px `#e74c3c` "practice — helps a lot, then runs out" and bold 12px `P.green` "checking — removes most of what is left, at any level".
- **Note line** (12px `P.mute`, centered, `h−28`): "the steep part is real; it is the flat part that the belief ignores".
- **Caption (bold 13px `P.orange`, centered, `h−8`):** "Something true about one piece of work is not true about a release."

---

## Regeneration instructions

- **Template:** the card-section layout from `05-cognitive-biases/25-mere-exposure-effect.html`. One `.card-section` per section, each an `<h2>` plus a `table.layout` with `td.text-col` 50% / `td.viz-col` 50%.
- **Text column order:** `.tags` pill row → `<ul>` of one-line bullets each opening `<b>label</b>` then an em dash → one `.key-point` callout → `.src` note.
- **Bullet form:** one line at 50% column width, 90–100 characters including the bold label.
- **Page CSS:** identical to `25-mere-exposure-effect.html`. No nav, no `.nav` CSS, no back/home links, no cross-page links.
- **Hue family per section:** 1 green caught against magenta shipped, 2 green testing against hard red skipping with a yellow level-off.
- **Canvas:** intrinsic `width="720"`, both heights 340. `setup(id)` caches the logical size in `dataset`, sets `style.maxWidth = 720px`, computes `scale = (cssW/720) × devicePixelRatio`, sizes the backing store to `logical × scale`, and `ctx.scale(scale, scale)`. Draws registered in `__charts`, re-run on debounced (150ms) resize.
- **Canvas fonts:** chart title bold 15px; in-chart header bold 12px; body and axis labels 12px floor; big callout figure bold 19px; caption bold 13px.
- **Palette** (shared `P`): `blue #2a78d6`, `green #008300`, `magenta #d55181`, `yellow #c98500`, `aqua #199e70`, `orange #d95926`, `violet #4a3aa7`, `ink #1a5276`, `text #2c3e50`, `mute #6b7280`, `grid #e5e9ef`. Hard red `#e74c3c` for the skipping case only.
- **Determinism:** no `Math.random()` and no generator on either chart — both sections are plain arithmetic on round numbers, so a seeded draw would only add noise.
- **Keep the numbers round and the arithmetic visible.** 300 and 100 bugs, 4 caught in 5. A reader should be able to check every figure in their head; that is the point of the page, not a nicety.
- **No derived multipliers, break-even searches or per-function probabilities.** An earlier draft carried a 1.7× ratio, a 5×-to-break-even calculation and an `e^(−0.25)` clean-piece share. All were correct and all read as a physics problem. Say "more bugs than the weakest developer" and "roughly four pieces in five", not the coefficients.
- **This page also replaced a calibrated-interval version, which must not come back.** That draft opened with stated "90% sure" ranges being three times too narrow — correct, textbook, and unrecognisable. Overconfidence is experienced as skipping the check because past work was good.
- **Keep the skill genuinely superior.** The lesson dies if the experienced developer is secretly no better; the point is that the advantage is real and still insufficient.
- **The flattening curve is a stated assumption.** It stands for "practice does not reach zero mistakes". The conclusion needs only that it flattens somewhere above what checking achieves.
- **What separates this from card 9.** Card 9 (Overconfidence in Small Samples) is a judgement about evidence — calling a result before the data settles. This page is a settled belief about your own reliability that removes a safeguard.
- **Keep this page at two sections.**
