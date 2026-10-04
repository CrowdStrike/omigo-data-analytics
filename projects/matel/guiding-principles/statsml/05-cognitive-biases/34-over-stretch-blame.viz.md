# Over-Stretch Blame — Viz

**Page type:** detail page, card-section (template `06-sectioned-cards-callout`)
**HTML title tag:** Over-Stretch Blame — Cognitive Biases
**Source note wording:** the sibling `.txt.md` says figures are "computed at render time"; the html `.src` notes say "computed in the draw function".

House style (bullet form, tag-pill markup, text-column order, canvas DPR scaling, shared palette `P`, canvas font floors, section-header borders) lives in `ui-templates/` and is not restated here.

**Figures printed in the sibling `.txt.md` are computed here.** Changing a rate or the construction invalidates that prose — the bad-week counts, the 9-of-10 attribution and every curve value must be re-read from the new draw and the text updated to match. The prose quotes the week-4 **66%**, week-13 **25%** and week-26 **6%** points of the `0.9^n` curve by name, so those three in particular must stay in step with the draw.

**Determinism:** no `Math.random()` and no generator on either chart. Both sections are plain arithmetic on round numbers — 1 in 100 against 10 in 100, and `0.9^n` — so a seeded draw would only add noise. The bad weeks in section 1 are placed at fixed positions, since a draw could clump them and the lesson is the count, not the pattern.

**Palette note:** the shared `P` plus hard red `#e74c3c`, used for bad weeks and the week-13 marker only.

---

## 1. The Same System, Two Loads, One Reputation

**Tag colors:** `core idea` violet, `two loads` blue, `same system` magenta
**Hue family:** aqua against magenta with a violet caption

### canvas `c1` — 720×340

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

## 2. The Stretch Looks Free for a Long Time

**Tag colors:** `why it spreads` orange, `quiet at first` yellow, `the drift` red
**Hue family:** orange with a yellow early band and a hard red marker

### canvas `c2` — 720×340

The chance of still having seen no failure, week by week past the limit, so the early weeks sit high and the confidence they buy is visibly unearned.

- **Data:** no PRNG. Weekly failure chance 1 in 10 past the limit; the chance of no failure yet after `n` weeks is `0.9^n`. Weeks 1 to 26 plotted.
- **Computed:** week 1 **90%**, week 4 **66%**, week 8 **43%**, week 13 **25%**, week 26 **6%**. Read off the same curve that is drawn.
- **Title (bold 15px `P.ink`, centered, y=21):** "Chance Nothing Has Gone Wrong Yet, Week by Week"
- **Layout:** `PX=62 … w−40`, baseline y=238, top y=62, y-axis 0% to 100% with gridlines every 25 labelled 12px `P.mute`. X-axis weeks 1 to 26 with ticks at 1, 4, 8, 13, 20, 26.
- **The curve:** 3px `P.orange` with a `rgba(217,89,38,0.14)` fill beneath it, and a dot radius 5 at each labelled week with its value in bold 12px `P.orange`.
- **The early band:** the first four weeks shaded `rgba(201,133,0,0.14)` and labelled bold 12px `P.yellow` "a month of quiet is the likely outcome, not evidence".
- **The late marker:** a 2px dashed `#e74c3c` vertical line at week 13 labelled bold 12px `#e74c3c` "even here, N% of runs have still seen nothing", where N is `Math.round(quiet(13) × 100)` = **25** — computed, not written as a word, so the label cannot drift from the curve it sits on.
- **Note line** (12px `P.mute`, centered, `h−28`): "by the time this curve is low enough to prove anything, the stretched load is the normal one".
- **Caption (bold 13px `P.orange`, centered, `h−8`):** "Quiet is what a 1-in-10 chance looks like most of the time."

---

## Page-specific constraints

- **Every printed figure is computed in its draw function** — both bad-week counts and the 9-of-10 attribution in section 1; every point on the curve in section 2.
- **Keep the numbers round.** 1 in 100 and 1 in 10. An earlier version of this material carried expected-weeks-to-failure, a rule-of-three sample size and a break-even ratio between throughput and reputation. All were correct and all read as an engineering calculation rather than an illustration.
- **The system must stay genuinely good.** The lesson dies if the design is secretly weak; the whole point is that a sound system acquires the name of an unsound one.
- **Do not put a number on reputation.** How many good weeks one failure cancels is not knowable, and inventing a figure would make the strongest claim on the page the least supported one. Say that failures are what gets remembered and leave it there.
- **What separates this from card 26.** Negativity Dominance is about one bad event outweighing many good ones in a judgement. This page is about where the bad events came from — the operating point — and about the load being absent from the story that gets told.
- **What separates this from card 27.** Absence Blindness is about quiet success earning no credit. Here the quiet is actively misread as proof that the limit was too cautious, which is a different error with a different fix.
- **Keep this page at two sections.**
