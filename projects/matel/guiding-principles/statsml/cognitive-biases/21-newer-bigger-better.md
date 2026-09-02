# Newer, Bigger, Pricier: An Empty Fault List Is Not a Clean Record

**Page type:** detail page — card-section template (see `statistical-paradoxes/03-berksons-paradox.html`)
**HTML title tag:** Newer, Bigger, Pricier — Cognitive Biases

**Subtitle:** Version 10 must beat version 9, and the dearer one must be the better one. Both claims are free to make.

---

## Section 1 — The Fault List Nobody Has Started Yet

**Tags:** `core idea` (violet), `two fault lists` (blue), `nobody has looked` (magenta)

**Bullets:**
- **The pitch** — Vendor A's new release has no reported faults; the tool you run has a long list
- **Why the long list** — that tool has been in daily use for years and every fault written down
- **Why the short list** — nobody has run the new one long enough to trip over anything yet
- **What is really being compared** — a measured record against an absence of measurement
- **Underneath** — the new release carries thirty real faults, the tool you run carries eighteen
- **Month zero** — the new one shows none of its thirty, the old one shows sixteen of its eighteen
- **Month fifteen** — the new one's known list overtakes the old one's, and keeps climbing
- **Two years in** — twenty-two faults on the books and eight of the thirty still unfound

**Key point:** An empty fault list is a statement about how long anyone has been looking, not about the thing being looked at. The old tool loses the comparison precisely because somebody bothered to measure it.

**Source note (`.src`):** Illustrative Example — both fault counts are constructed, and every printed figure is read off the seeded discovery curves in the draw function.

### Visualization — canvas `c1`, 720×340

Known faults against real faults over twenty-four months, for a new release and a mature tool, with the crossover marked and the unfound faults shaded.

- **Construction:** the new release has `NEW_TRUE = 30` real faults, the mature tool `MAT_TRUE = 18`. Each month every still-unfound fault is discovered with chance `HAZ = 0.06`, drawn from a seeded Park–Miller LCG, seed 42. The new release's curve is its own first 25 months (`t = 0…24`). The mature tool has been in service `HEAD = 30` months longer, so its curve is months 30–54 of the *same* process — a separate `lcg(42)` stream run for 55 steps and sliced from index 30.
- **Computed values, all read off the arrays in the draw function:**
  - new release known: `[0,1,4,5,6,7,7,8,9,11,14,16,16,17,17,18,20,21,21,21,21,22,22,22,22]`
  - mature tool known: `[16,16,16,16,16,16,16,16,16,16,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17]`
  - crossover: **month 15**, where the new release's 18 passes the mature tool's 17 — found by scanning for the first month with `newKnown > matKnown`, not asserted.
  - month 0: new shows **0 of 30**, mature shows **16 of 18**.
  - month 24: new **22** known and **8** still unfound; mature **17** known and **1** still unfound.
- **Title (bold 15px `P.ink`, centered, y=22):** "Known Faults Against Real Faults, Month by Month"
- **Plot box:** `PX = 52`, `PW = w − 34 − PX`, `TOP = 48`, `BOT = h − 76`. X maps months 0–24, Y maps 0–33 faults. Y axis ticks every 5 in 12px `P.mute`; baseline and axis in `#ccc`.
- **Real-fault lines:** dashed (5/4) 1.5px horizontal lines at 30 in `P.violet` and at 18 in `P.blue`, each labelled at its right end in 12px of its own colour: "30 real faults" and "18 real faults", both printed from the constants.
- **Unfound shading:** the band between each known-faults curve and its own real-fault line filled `rgba(213,81,129,0.13)` — the new release's band is large and shrinks; the mature tool's is a sliver. One bold 12px `P.magenta` label "faults nobody has found yet" sits inside the wide part of the new release's band, around month 6.
- **Known-faults curves:** new release 2.5px `P.violet`, mature tool 2px `P.blue`, both drawn as straight segments through the monthly points with a 3px dot at each point.
- **Crossover marker:** dashed (4/3) 1.5px `P.magenta` vertical line at the crossover month, running from the 14-fault level up to just under the 30-fault ceiling so it clears the day-one panel, a 5.5px `P.magenta` ring on each curve at that month, and a bold 12px `P.magenta` label "known lists cross at month 15" above, with the month printed from the scan.
- **Day-one panel** in the free lower-right of the plot, left-aligned at the month-13.2 position: bold 12px `P.ink` "ON DAY ONE, OF ITS OWN REAL FAULTS —"; then 12px `P.violet` "new release shows 0 of 30"; 12px `P.blue` "mature tool shows 16 of 18"; then bold 12px `P.magenta` "the empty list is the unmeasured one". All counts from the arrays.
- **Curve labels (12px, left side where the two are far apart):** `P.blue` "mature tool, known" and `P.violet` "new release, known".
- **X-axis label (12px `P.mute`, centered under the axis):** "months in service", with month ticks every 4.
- **Caption (bold 13px `P.violet`, centered, `h−10`):** "An empty fault list says nobody has looked yet, not that there is nothing to find."

---

## Section 2 — Judging Both Releases at the Same Age

**Tags:** `the fair test` (aqua), `same age` (yellow), `the flip` (blue)

**Bullets:**
- **The fair question** — not whose list is longer today, but whose was longer at the same age
- **Line them up** — replay each from its own first month in service and compare month by month
- **At six months** — the new release had seven faults on the books, the old one had five
- **At one year** — sixteen against eight, twice as many surfacing over the same span of service
- **Across two years** — the new one leads the old one in twenty-three of the twenty-four months
- **So the ranking flips** — the shorter list today belongs to the worse product on equal terms
- **Why nobody checks** — the old one's first months are ancient history and the notes are gone
- **What gets used instead** — today's snapshot, which rewards whatever arrived most recently

**Key point:** Lining the two up by age turns the comparison around completely. The only reason the snapshot favours the new release is that its early months are still happening while the old one's have been forgotten.

**Source note (`.src`):** Illustrative Example — the same two constructed products as the previous section, replayed from each one's own first month.

### Visualization — canvas `c2`, 720×320

The two discovery curves re-plotted against age in service rather than the calendar, with the gap between them filled.

- **Construction:** identical seeded discovery process to `c1`. The new release curve is unchanged; the mature tool's curve is now its **first** 25 months (`matFull.slice(0, 25)`) instead of months 30–54.
- **Computed values:**
  - new release: `[0,1,4,5,6,7,7,8,9,11,14,16,16,17,17,18,20,21,21,21,21,22,22,22,22]`
  - mature tool at the same age: `[0,1,3,4,4,5,5,5,6,7,7,7,8,8,8,9,9,9,10,12,12,13,14,14,16]`
  - age 6: **7** against **5**; age 12: **16** against **8** (exactly twice); age 24: **22** against **16**.
  - months where the new release is at or above the old one: **24 of 24**; strictly above: **23 of 24**. Counted by a loop over months 1–24.
- **Title (bold 15px `P.ink`, centered, y=22):** "Both Judged From Their Own First Month in Service"
- **Plot box:** `PX = 50`, `PW = w − 208 − PX` (the right strip carries the age panel), `TOP = 46`, `BOT = h − 70`. X maps age 0–24 months, Y maps 0–24 faults, ticks every 4 in 12px `P.mute`.
- **Gap fill:** the region between the two curves filled `rgba(25,158,112,0.14)` — the new release is above the old one at every age, so the fill is one unbroken wedge and needs no explanation.
- **Curves:** new release 2.5px `P.aqua` with 3px dots; mature tool 2px `P.yellow` with 3px dots. End labels in 12px of each colour: "new release" and "mature tool, same age".
- **Age markers:** dashed (3/3) 1px `P.grid` verticals at ages 6, 12 and 24, each with a 5px ring on both curves.
- **Age panel** at `PX + PW + 20`: bold 12px `P.ink` header "FAULTS ON THE BOOKS AT THE SAME AGE", then three rows on a 40px pitch. Each row: 12px `P.mute` age label ("6 months", "1 year", "2 years"); bold 15px `P.aqua` new-release count; 12px `P.mute` "vs"; bold 15px `P.yellow` mature count — all pulled from the arrays. Beneath the rows, bold 12px `P.aqua` "new release leads in 23 of the 24 months", counted in the draw function.
- **X-axis label (12px `P.mute`, centered):** "months since that release first shipped"
- **Caption (bold 13px `P.aqua`, centered, `h−10`):** "On equal service time the new release is the faultier one, every month of the way."

---

## Section 3 — A Number Anyone Can Print on the Box

**Tags:** `cheap to fake` (orange), `free claims` (yellow), `signal dries up` (red)

**Bullets:**
- **What a version number costs** — nothing; anyone can print 10 on a box that used to say 9
- **What a price tag costs** — nothing; moving upmarket is a decision, not an improvement
- **So a claim anyone can make** — carries no weight, however loudly the box makes it
- **The setup** — two thousand head-to-head pairs, one of the two genuinely the better product
- **Buy on the bigger claimed jump** — with nobody padding, that picks the better one 94 in 100
- **One seller in five padding** — down to 83, and the padding sellers changed nothing at all
- **Half of them padding** — down to 72, from a claim that costs the seller nothing to print
- **Four in five padding** — 66, so the number on the box has nearly stopped sorting the two

**Key point:** A signal is only worth what it costs to produce. Because a bigger number and a higher price are free, the sellers who improved nothing look exactly like the sellers who did — and the honest ones lose the ability to prove it.

**Source note (`.src`):** Illustrative Example — two thousand seeded head-to-head pairs; every share is counted in the draw function.

### Visualization — canvas `c3`, 720×320

How often the bigger advertised jump belongs to the genuinely better product, as more sellers pad the number for free.

- **Construction:** 2,000 pairs from a seeded LCG, seed 42, with a Box–Muller transform for the bell-shaped draws. For each pair, the two products have true qualities `qa, qb ~ N(0, 10²)`. Each product's advertised jump is `q + honest + boost`, where `honest ~ N(0, 2²)` is the harmless slop in an honest claim and `boost = |N(0,1)| × 30` is the free padding — added only to the sellers who pad. A per-product uniform draw decides who pads at each share.
- **Computed curve:** for each padding share from 0 to 100% in ten steps, the share of pairs where the bigger advertised jump belongs to the genuinely better product: `94, 88, 83, 80, 76, 72, 70, 68, 66, 66, 66` percent. Stable to a point or two across seeds 7, 123, 999 and across 2,000 / 4,000 / 8,000 pairs.
- **Title (bold 15px `P.ink`, centered, y=22):** "How Often the Bigger Claim Belongs to the Better Product"
- **Plot box:** `PX = 62`, `PW = w − 48 − PX`, `TOP = 52`, `BOT = h − 74`. X maps padding share 0–100%, Y maps 50–100% with ticks every 10 in 12px `P.mute`.
- **Coin-flip floor:** solid 1.5px `P.mute` line along the bottom of the Y range, labelled 12px `P.mute` "no better than a coin flip" just above it at the right.
- **Information band:** the area between the curve and the coin-flip floor filled `rgba(217,89,38,0.14)` — the band narrows left to right, so the shrinking wedge *is* the loss of information.
- **Curve:** 2.5px `P.orange` through the eleven computed points, 4px `P.orange` dots at each.
- **Called-out points:** at padding shares 0%, 20%, 50% and 80% a 6.5px ring plus a bold 13px `P.orange` percentage above it, each printed from the computed array. Two plain-language markers sit clear of the curve: 12px `P.yellow` "nobody padding" at the left edge and 12px `P.mute` "four sellers in five padding" beneath the 80% point.
- **Free-claim note** in the free upper-right of the plot: bold 12px `P.yellow` "padding the number changes nothing about the product", then 12px `P.mute` "and costs the seller nothing to do".
- **X-axis label (12px `P.mute`, centered):** "share of sellers padding the number instead of improving anything"
- **Caption (bold 13px `P.orange`, centered, `h−10`):** "A claim that costs nothing to make ends up telling you almost nothing."

---

## Section 4 — Thirty Tasters, One Drink, Two Price Tags

**Tags:** `two pours` (magenta), `identical drink` (violet), `the control` (blue)

**Bullets:**
- **The setup** — thirty tasters, one drink, poured twice: once plainly, once with a premium tag
- **Same liquid both times** — the only thing that changed between the two pours was the tag
- **Poured plainly** — the room averaged 5.3 out of ten
- **Poured with the tag** — 6.8, a jump of 1.5 points on a drink that did not change
- **How widespread** — twenty-seven of the thirty scored the tagged pour above the plain one
- **The control** — pour twice with no tag either time and the two averages land 0.04 apart
- **Which way the control leans** — twelve tasters up, fifteen down, three level, so no drift
- **What the tag did** — it did not just bend the verdict, it changed what the taster tasted

**Key point:** The control is what makes this worth showing. Two unlabelled pours of the same drink land on top of each other, so the 1.5-point jump has nowhere to come from except the tag — and the tasters were not lying, they genuinely liked it more.

**Source note (`.src`):** Illustrative Example — thirty constructed tasters, each with a seeded baseline and a fresh pour-to-pour wobble; both averages and all three tallies are computed in the draw function.

### Visualization — canvas `c4`, 720×350

Two paired-dot panels side by side: the same drink poured plainly against poured with a premium tag, and the tag-free control beside it.

- **Construction:** seeded LCG, seed 42. Each of 30 tasters gets `base = 5.5 + U(−1.5, 1.5)` and two independent pour wobbles `n1, n2 = U(−1.4, 1.4)`. Three scores per taster, each clamped to 1–10 and rounded to one decimal: `plain = base + n1`, `badge = base + n2 + 1.5`, `blind = base + n2`. The tag panel plots `plain → badge`; the control panel plots `plain → blind`, so the control differs from the tag panel only by the missing 1.5.
- **Computed values:** plain average **5.33**, tagged **6.79**, control **5.29**. Printed to one decimal as 5.3, 6.8 and 5.3; the tag gap printed as `(6.79 − 5.33).toFixed(1)` = **+1.5 points**, the control gap as `|5.29 − 5.33|` to two decimals = **0.04**. Tallies: tagged higher for **27**, lower for **2**, level for **1**; control higher for **12**, lower for **15**, level for **3**.
- **Title (bold 15px `P.ink`, centered, y=22):** "The Same Drink, Poured Twice"
- **Two panels:** left panel columns at `x = 118` and `x = 258`, right panel at `x = 462` and `x = 602`. Shared vertical scale, ratings 2–10, `TOP = 74`, `BOT = h − 96`, ticks at 2/4/6/8/10 in 12px `P.mute` on the far left only. A faint 1px `P.grid` vertical divider at `x = 360`.
- **Panel headers (bold 13px, centered above each panel):** `P.magenta` "WITH A PREMIUM TAG ON THE SECOND POUR" and `P.mute` "CONTROL — NO TAG EITHER TIME".
- **Paired lines:** one 1.5px segment per taster between its two columns. In the tag panel, upward pairs `rgba(213,81,129,0.45)`, downward `rgba(107,114,128,0.35)`. In the control panel, upward `rgba(42,120,214,0.30)`, downward `rgba(107,114,128,0.30)` — deliberately near-equal weights, because the control's point is that neither direction wins.
- **Dots:** 3.5px, first column `P.mute`, second column `P.magenta` in the tag panel and `P.blue` in the control panel.
- **Mean bars:** 3px horizontal bar 34px wide at each column's average, in that column's colour, with the average printed bold 15px just outside it — 5.3, 6.8, 5.3, 5.3, all from the arrays.
- **Tagged gap bracket:** a 2.5px `P.magenta` bracket at `x = 300` spanning the two mean bars, with bold 15px `P.magenta` "+1.5" and bold 12px "points" beside it.
- **The control gap is stated in words, not bracketed.** At 0.04 points the two mean bars overlap, so a bracket would be a single pixel tall — which is the finding. It appears as 12px `P.mute` "the two averages land 0.04 apart" under the control panel, printed from the two averages.
- **Column labels (12px `P.mute`, under each column):** "plain", "premium tag", "plain", "no tag".
- **Tallies (bold 12px, under each panel):** `P.magenta` "27 of 30 scored the tagged pour higher"; `P.mute` "12 up, 15 down, 3 level — no drift", both from the tallies.
- **Caption (bold 13px `P.magenta`, centered, `h−10`):** "The tag did not change the drink. It changed what thirty people tasted."

---

## Section 5 — How Much the Price Tag Actually Tells You

**Tags:** `the boundary` (green), `real but weak` (aqua), `common mistake` (red)

**Bullets:**
- **The honest position** — these clues do carry real information; they just do not settle anything
- **The test** — two thousand pairs where one is genuinely better, chosen from one clue alone
- **Higher version number** — picks the genuinely better product in 61 pairs out of 100
- **The bigger of the two** — 65, barely ahead of the version number and still not much
- **The higher price** — 71, the strongest of the three things printed on the box
- **A short hands-on trial** — 87, ahead of every label, and still not a certainty
- **In everyday terms** — over twenty choices the price tag misjudges six and the trial three
- **Where it turns** — not in reading the clue, but in treating it as the end of the enquiry

**Key point:** Seventy-one out of a hundred is genuinely better than a coin flip and genuinely worse than looking. The price tag earns a nudge, not a verdict — and the error is never noticing which of the two you gave it.

**Source note (`.src`):** Illustrative Example — two thousand seeded pairs; every rate and every "out of twenty" figure is computed in the draw function.

### Visualization — canvas `c5`, 720×350

How often each clue on its own picks the genuinely better of two products, against a coin flip and against a short trial.

- **Construction:** 2,000 pairs from a seeded LCG, seed 42, Box–Muller for the bell-shaped draws. Both products have a true quality `~ N(0, 10²)`. Each clue is that quality seen through its own blur: version number `N(0, 35²)`, size `N(0, 22²)`, price `N(0, 13²)`, a short hands-on trial `N(0, 4²)`. A clue "gets it right" when the product it ranks higher is the one with the higher true quality — checked pair by pair.
- **Computed rates:** version number **61%**, size **65%**, price **71%**, short trial **87%**. Stable within two or three points across seeds 7, 123, 999 and across 2,000 / 4,000 / 8,000 pairs.
- **Everyday translation, computed as `Math.round(20 × (1 − rate))`:** wrong in **8** of 20 choices for the version number, **7** for size, **6** for price, **3** for the trial.
- **Title (bold 15px `P.ink`, centered, y=22):** "Picking the Better of Two Products From One Clue"
- **Panel header (bold 13px `P.ink`, left-aligned at `PX = 30`, y=50):** "SHARE OF PAIRS WHERE THE CLUE PICKS THE BETTER PRODUCT"
- **Bars:** label column right-aligned at `BX − 12`; track from `BX = 208` of width `BW = 336`, four rows on a 46px pitch starting `y = 84`, bar height 22. Track `rgba(107,114,128,0.12)`, bar length `BW × rate`. Colours: version number `rgba(107,114,128,0.40)`/`P.mute`, size `rgba(201,133,0,0.45)`/`P.yellow`, price `rgba(217,89,38,0.45)`/`P.orange`, short trial `rgba(0,131,0,0.40)`/`P.green` — the honest measurement in green, the labels in warm neutrals.
- **Percentages:** bold 15px in each row's own colour, printed just past the bar end from the computed rate.
- **Everyday column:** 12px `P.mute` "wrong in N of 20" right-aligned at `BX + BW + 148`, from the rounded translation.
- **Coin-flip line:** dashed (5/4) 2px `P.mute` vertical at 50% of the track, spanning all four rows, labelled bold 12px `P.mute` "coin flip" above the top row.
- **Axis label (12px `P.mute`, centered under the track):** "share of pairs called correctly"
- **Boundary note** below that: bold 12px `P.green` "the trial is the only one that beats every label", then 12px `P.mute` "and even that is wrong in 3 choices out of 20" — both from the computed figures, so the section does not overclaim the honest option either.
- **Caption (bold 13px `P.green`, centered, `h−10`):** "Real information, weak information — enough to lean on, never enough to decide."

---

## Regeneration instructions

- **Template:** the card-section layout from `statistical-paradoxes/03-berksons-paradox.html`, matching the converted `05-clustering-illusion.html` and `01-confirmation-bias.html`. One `.card-section` per section, each holding an `<h2>` (1.3rem `#1a5276`, `border-bottom: 2px solid #2980b9`, 4px bottom padding) and a `table.layout` with a single row: `td.text-col` 50% / `td.viz-col` 50%.
- **Canvas placement:** `td.viz-col` gets `text-align: center` and the canvas `display: block; width: 100%; margin: 0 auto`, capped at 720px, so a wide cell leaves slack and the chart sits centred in the right half.
- **Text column order:** `.tags` pill row → `<ul>` of one-line bullets each opening `<b>label</b>` → one `.key-point` callout → `.src` note. Every section on this page is built on constructed data, so every section carries a `.src`. No paragraph blocks, no data tables, no `.example` lines restating a bullet.
- **Bullet form:** one line that does not wrap at 50% column width (≤95 characters including the bold label), opening `<b>bold label</b>` then an em dash then the fact. Bullet counts follow the content — eight per section here, because each mechanism needs the full construction stated.
- **Section titles name the content.** No role labels ("The Trap", "The Defense") and no phrasing that would fit another page.
- **Page CSS:** body system-ui, white, `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with `border-bottom: 2px solid #2980b9`, 8px bottom padding. `.subtitle` `#666` 0.95rem, 32px bottom margin. `.card-section` 40px bottom margin. `table.layout` full width, border-collapse, cells vertical-align top padding 12px. `ul` 0.92rem margin `8px 0 8px 20px`, `li` 4px bottom margin, `li b` `#1a5276`. `.key-point` `#f8f9fa` background, `border-left: 3px solid #e74c3c`, padding 8px 12px, 0.9rem. `.src` 0.78rem `#888`. No nav, no `.nav` CSS, no back/home links, no cross-page links of any kind.
- **Tag pills:** `display:inline-block`, 0.72rem, weight 600, padding 2px 10px, radius 10px. Classes used: `.blue`, `.green`, `.red`, `.orange`, `.violet` `rgba(74,58,167,0.12)`/`#4a3aa7`, `.magenta` `rgba(213,81,129,0.14)`/`#c2426f`, `.aqua` `rgba(25,158,112,0.14)`/`#17805d`, `.yellow` `rgba(201,133,0,0.15)`/`#a06c00`.
- **Colour variety across sections is a requirement.** Section 1 violet/blue with a magenta shaded gap; section 2 aqua/yellow; section 3 orange/yellow; section 4 magenta with a mute control; section 5 green with warm neutrals for the labels. No chart repeats blue-fill-plus-orange-highlight.
- **Canvas:** CSS `width: 100%`, `border: 1px solid #e0e0e0`, radius 4px. Intrinsic `width="720"` plus the per-chart height (340, 320, 320, 350, 350). `setup(id)` caches the logical size in `dataset` on the first call, sets `style.maxWidth = 720px`, computes `scale = (cssW/720) × devicePixelRatio`, sizes the backing store to `logical × scale`, and `ctx.scale(scale, scale)` back to logical coordinates. Draws registered in `__charts`, re-run on debounced (150ms) resize.
- **Canvas font sizes:** chart title bold 15px; in-chart header bold 12–13px; labels 12px floor; callout figures bold 15px, with the biggest single figure per chart no smaller than that; caption bold 13px.
- **Palette** (shared `P` object): `blue #2a78d6`, `green #008300`, `magenta #d55181`, `yellow #c98500`, `aqua #199e70`, `orange #d95926`, `violet #4a3aa7`, `ink #1a5276`, `text #2c3e50`, `mute #6b7280`, `grid #e5e9ef`.
- **Determinism:** no `Math.random()`. Seeded Park–Miller LCG (`s = (s × 16807) % 2147483647`), seed 42, with a Box–Muller helper for the bell-shaped draws in `c3` and `c5`. Every printed count, average, gap and percentage is computed inside the draw function from the plotted arrays.
- **Shared construction:** `c1` and `c2` must build their discovery curves from the same helper with the same constants (`NEW_TRUE = 30`, `MAT_TRUE = 18`, `HAZ = 0.06`, `HEAD = 30`). `c2` differs from `c1` only in slicing the mature tool's curve from index 0 instead of index 30. Changing the constants in one place silently breaks the other section's prose.
- **Scope:** this page covers size, novelty, version number and price as stand-ins for quality. The separate effect where repeated exposure makes a familiar thing feel better is not covered here and is not referenced.
- **Corrections applied to the earlier version of this page:**
  - The old lead chart drew a "perceived quality" line and an "actual quality" line from two hardcoded ten-point arrays with a hand-placed "divergence zone" band — nothing was computed, and neither line came from a stated construction. It is replaced by the known-versus-real fault curves, where the crossover month, both fault counts and the shaded gap are all read off seeded arrays.
  - The old section 2 drew a four-row table on canvas, which the template forbids. That material is now carried by the section 3 bullets.
  - The old page cited a named chip maker's pipeline depth and clock speeds against a named competitor as fact, and quoted specific figures from a wine-tasting study and brain-imaging work without a source. All real company names are gone, and the price-tag effect is now a constructed thirty-taster example with its own control, labelled illustrative.
  - The old page asserted that "blind tests routinely erase price-driven quality ratings" with no figure. The claim is now quantified: the constructed control puts two unlabelled pours 0.04 points apart against a 1.5-point tagged gap.
  - The old closing section was a workflow diagram of four defence boxes with a dashed shortcut arrow — a prescription, not a boundary. It is replaced by the honest position the subject requires: each clue's actual hit rate, computed, showing price at 71 out of 100 rather than either 50 or 100.
