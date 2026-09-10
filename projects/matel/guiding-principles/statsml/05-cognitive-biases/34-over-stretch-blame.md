# Over-Stretch Blame: A Good System Earns a Bad Name

**Page type:** detail page — card-section template (see `05-cognitive-biases/25-mere-exposure-effect.html`)
**HTML title tag:** Over-Stretch Blame — Cognitive Biases

**Subtitle:** Run a sound system past what it was built for and it works almost every time. The rare failure is the one people remember, and the name it earns belongs to the load it was given rather than to the system.

---

## Section 1 — The Same System, Two Loads, One Reputation

**Tags:** `core idea` (violet), `two loads` (blue), `same system` (magenta)

**Bullets:**
- **The system** — well built, and inside the load it was designed for it almost never fails
- **Run at its rated load** — about one bad week in every hundred weeks of running
- **Run past that load** — about one bad week in every ten, because the margin has been spent
- **Over a hundred weeks** — that is 1 bad week at the rated load and 10 past it
- **What changed** — only the load, since it is the same system with the same design
- **So 9 of the 10 failures** — exist only because of the decision to push it, not the build
- **What people remember** — the bad weeks, and nothing at all about the load at the time
- **The name it gets** — unreliable, which is a fair description of the second column only
- **Who chose the column** — whoever set the load, and that decision is nowhere in the story

**Key point:** A reputation is formed from remembered failures, and failures come from the operating point rather than the design. Push a good system and it will earn the name of a bad one, because the load that caused the failures leaves no trace in anybody's memory of them.

**Source note (`.src`):** Illustrative Example — one system at two loads over a hundred weeks; the failure counts and the share caused by the stretch are computed in the draw function.

### Visualization — canvas `c1`, 720×340

A hundred week-squares per load, bad weeks marked, so the two grids sit side by side as the same system twice.

- **Data:** no PRNG. 100 weeks per load, failure in 1 week per 100 at the rated load and 10 per 100 past it, laid out at fixed positions so the marked weeks are spread rather than clustered.
- **Computed:** rated load **1 bad week of 100**; past the load **10 of 100**. Difference **9**, which is the share of failures the stretch is responsible for, computed as the gap between the two counts rather than typed.
- **Title (bold 15px `P.ink`, centered, y=21):** "One Hundred Weeks of Running, the Same System Twice"
- **Layout:** two 10×10 grids of 16px squares on an 18px pitch, one per load, side by side across `PX=62 … w−40` with a 40px gutter, starting y=76.
- **Squares:** a good week is `#fff` stroked `P.grid`; a bad week is filled `rgba(213,81,129,0.55)` stroked `P.magenta` with a 2px `#e74c3c` cross over it.
- **Grid headers** (bold 12px above each grid): `P.aqua` "RUN AT ITS RATED LOAD" and `P.magenta` "RUN PAST ITS RATED LOAD", each with 12px `P.mute` beneath — "the load it was built for" and "the load someone chose to add".
- **Grid footers** (under each grid): bold 19px in the grid hue with the bad-week count "1 bad week" / "10 bad weeks", then 12px `P.mute` "out of 100".
- **The attribution line** (bold 12px `#e74c3c`, centered under both grids): "9 of the 10 bad weeks exist only because of the load", with the 9 computed from the two counts.
- **Note line** (12px `P.mute`, centered, `h−28`): "nothing about the system differs between these two grids".
- **Caption (bold 13px `P.violet`, centered, `h−8`):** "The name it earns describes the load, not the build."

---

## Section 2 — The Stretch Looks Free for a Long Time

**Tags:** `why it spreads` (orange), `quiet at first` (yellow), `the drift` (red)

**Bullets:**
- **The first push past the limit** — nothing goes wrong, because 9 weeks in 10 are fine anyway
- **After a month past the limit** — still nothing wrong, and that is the likely outcome, not luck
- **What that quiet is taken for** — proof the old limit was too cautious and there is room to spare
- **What the quiet actually is** — the ordinary result of a 1-in-10 chance not landing yet
- **What it would take to prove it** — many months of clean running, far more than anyone waits
- **So the limit moves** — each quiet stretch makes the next push feel like the normal way to work
- **Nobody ever decides this** — there is no meeting, no sign-off, just a limit that drifted outward
- **When the bad week arrives** — it is read as the system failing, since the load now looks normal
- **The gain in between** — real, which is why the ledger looks positive right up to the failure

**Key point:** The push is invisible for exactly as long as it takes to become the new normal. By the time a failure arrives, the stretched load is what everyone considers ordinary, so the failure has nothing left to be attributed to except the system itself.

**Source note (`.src`):** Illustrative Example — the chance of getting through with nothing going wrong, at a 1-in-10 weekly rate; every value is computed in the draw function.

### Visualization — canvas `c2`, 720×340

The chance of still having seen no failure, week by week past the limit, so the early weeks sit high and the confidence they buy is visibly unearned.

- **Data:** no PRNG. Weekly failure chance 1 in 10 past the limit; the chance of no failure yet after `n` weeks is `0.9^n`. Weeks 1 to 26 plotted.
- **Computed:** week 1 **90%**, week 4 **66%**, week 8 **43%**, week 13 **25%**, week 26 **6%**. Read off the same curve that is drawn.
- **Title (bold 15px `P.ink`, centered, y=21):** "Chance Nothing Has Gone Wrong Yet, Week by Week"
- **Layout:** `PX=62 … w−40`, baseline y=238, top y=62, y-axis 0% to 100% with gridlines every 25 labelled 12px `P.mute`. X-axis weeks 1 to 26 with ticks at 1, 4, 8, 13, 20, 26.
- **The curve:** 3px `P.orange` with a `rgba(217,89,38,0.14)` fill beneath it, and a dot radius 5 at each labelled week with its value in bold 12px `P.orange`.
- **The early band:** the first four weeks shaded `rgba(201,133,0,0.14)` and labelled bold 12px `P.yellow` "a month of quiet is the likely outcome, not evidence".
- **The late marker:** a 2px dashed `#e74c3c` vertical line at week 13 labelled bold 12px `#e74c3c` "even here, a quarter of runs have still seen nothing".
- **Note line** (12px `P.mute`, centered, `h−28`): "by the time this curve is low enough to prove anything, the stretched load is the normal one".
- **Caption (bold 13px `P.orange`, centered, `h−8`):** "Quiet is what a 1-in-10 chance looks like most of the time."

---

## Regeneration instructions

- **Template:** the card-section layout from `05-cognitive-biases/25-mere-exposure-effect.html`. One `.card-section` per section, each an `<h2>` plus a `table.layout` with `td.text-col` 50% / `td.viz-col` 50%.
- **Text column order:** `.tags` pill row → `<ul>` of one-line bullets each opening `<b>label</b>` then an em dash → one `.key-point` callout → `.src` note.
- **Bullet form:** one line at 50% column width, 90–100 characters including the bold label.
- **Page CSS:** identical to `25-mere-exposure-effect.html`. No nav, no `.nav` CSS, no back/home links, no cross-page links.
- **Hue family per section:** 1 aqua against magenta with a violet caption, 2 orange with a yellow early band and a hard red marker.
- **Canvas:** intrinsic `width="720"`, both heights 340. `setup(id)` caches the logical size in `dataset`, sets `style.maxWidth = 720px`, computes `scale = (cssW/720) × devicePixelRatio`, sizes the backing store to `logical × scale`, and `ctx.scale(scale, scale)`. Draws registered in `__charts`, re-run on debounced (150ms) resize.
- **Canvas fonts:** chart title bold 15px; in-chart header bold 12px; body and axis labels 12px floor; big callout figure bold 19px; caption bold 13px.
- **Palette** (shared `P`): `blue #2a78d6`, `green #008300`, `magenta #d55181`, `yellow #c98500`, `aqua #199e70`, `orange #d95926`, `violet #4a3aa7`, `ink #1a5276`, `text #2c3e50`, `mute #6b7280`, `grid #e5e9ef`. Hard red `#e74c3c` for bad weeks and the week-13 marker only.
- **Determinism:** no `Math.random()` and no generator on either chart. Both sections are plain arithmetic on round numbers — 1 in 100 against 10 in 100, and `0.9^n` — so a seeded draw would only add noise. The bad weeks in section 1 are placed at fixed positions, since a draw could clump them and the lesson is the count, not the pattern.
- **Every printed figure is computed in its draw function** — both bad-week counts and the 9-of-10 attribution in section 1; every point on the curve in section 2.
- **Keep the numbers round.** 1 in 100 and 1 in 10. An earlier version of this material carried expected-weeks-to-failure, a rule-of-three sample size and a break-even ratio between throughput and reputation. All were correct and all read as an engineering calculation rather than an illustration.
- **The system must stay genuinely good.** The lesson dies if the design is secretly weak; the whole point is that a sound system acquires the name of an unsound one.
- **Do not put a number on reputation.** How many good weeks one failure cancels is not knowable, and inventing a figure would make the strongest claim on the page the least supported one. Say that failures are what gets remembered and leave it there.
- **What separates this from card 26.** Negativity Dominance is about one bad event outweighing many good ones in a judgement. This page is about where the bad events came from — the operating point — and about the load being absent from the story that gets told.
- **What separates this from card 27.** Absence Blindness is about quiet success earning no credit. Here the quiet is actively misread as proof that the limit was too cautious, which is a different error with a different fix.
- **Keep this page at two sections.**
