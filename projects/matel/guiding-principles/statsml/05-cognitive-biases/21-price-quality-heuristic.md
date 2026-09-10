# Price–Quality Heuristic: The Price Tag Is Read as a Quality Rating

**Page type:** detail page — card-section template (see `cognitive-biases/05-clustering-illusion.html`)
**HTML title tag:** Price–Quality Heuristic — Cognitive Biases

**Subtitle:** Nobody sets the price except the seller, and nothing stops them setting it high. We read it as a verdict on the product anyway.

---

## Section 1 — What the Bias Is

**Tags:** `core idea` (blue), `plain terms` (violet), `who sets the price` (red)

**Bullets:**
- **The shortcut** — the price on the tag gets read as a rating of the product
- **Why we use it** — dearer things often are better, so the shortcut is right often enough
- **Where it breaks** — the seller writes the price, and can raise it without touching the product
- **The newer twist** — a fresh model number does the same job as a high price
- **Two cameras** — same sensor, same lens mount, same ISO range, same burst rate, same screen
- **What actually differs** — the model year printed on the box and $270 on the tag
- **What the $270 buys** — a higher number, 52% more paid, no feature anyone can name
- **The tell** — if you cannot say what the extra money bought, it probably bought the tag

**Key point:** Price is a claim the seller makes, not a measurement anyone took. The moment you cannot name the feature the extra money buys, the price has stopped being evidence and started being decoration.

**Source note (`.src`):** Illustrative Example — two constructed camera models; the price difference and percentage are computed in the draw function from the two listed prices.

### Visualization — canvas `c1`, 720×340

Two camera models compared spec by spec: five identical pairs of bars, then the one row where they differ.

- **Construction:** two products defined by a small literal list. Model X200, two years old, $520; Model X300, new, $790. Five shared specs, each with the *same* value for both models: sensor `24 MP`, lens mount `identical`, ISO range `100–25600`, burst `6 fps`, screen `3.0 in`. The price percentage is computed as `Math.round((790/520 − 1) × 100)` = **52%**, and the gap as `790 − 520` = **$270**. Nothing is hardcoded that could disagree with the two prices.
- **Title (bold 15px `P.ink`, centered, y=22):** "Two Cameras, One Spec Sheet"
- **Column headers (bold 12px, centered over each bar column):** `P.mute` "MODEL X200 — TWO YEARS OLD" and `P.violet` "MODEL X300 — NEW".
- **Layout:** spec labels right-aligned in 12px `P.mute` at `LX = 150`; two bar columns starting at `BX1 = 166` and `BX2 = 396`, each `BW = 200` wide; five rows on a 34px pitch from `y = 76`, bar height 18.
- **Spec bars:** for each of the five specs, both bars drawn at the **same** full width in `rgba(42,120,214,0.30)` with a 12px `P.blue` value string centred inside each — so the eye sees five matched pairs and reads the same number twice. This equality is the whole point of the chart.
- **Identical bracket:** a 2px `P.blue` bracket down the right of the five spec rows with bold 12px `P.blue` "every spec the same, twice" rotated or stacked beside it, drawn from the row geometry.
- **Divider:** a 1px `P.grid` horizontal rule below the spec rows, so the price row reads as a different kind of row.
- **Price row:** two bars in `rgba(231,76,60,0.35)` with a `P.red` stroke, lengths proportional to the two prices (`BW × price / 790`), each labelled bold 15px `P.red` with its dollar figure printed from the price constants.
- **Price callout:** bold 13px `P.red` "+$270, or 52% more" beside the longer bar, both figures computed; then 12px `P.mute` "for the same five specs" beneath it.
- **Caption (bold 13px `P.red`, centered, `h−10`):** "The only line on the sheet that changed is the one the seller wrote."

---

## Section 2 — The Same Thing at Twelve Times the Price

**Tags:** `everyday examples` (orange), `identical contents` (yellow), `per-unit price` (green)

**Bullets:**
- **The pattern** — the clearest cases are the ones where the contents are provably identical
- **Table salt** — 80 cents a pound against $9.60 for the artisan jar, twelve times the price
- **What is in both jars** — sodium chloride, the same compound, no difference to find
- **Headache tablets** — a store box works out at 4 cents a tablet, the branded one at 28
- **Same active ingredient** — printed on both boxes in the same milligram dose
- **The camera again** — $520 against $790, the mildest of the three at 1.5 times
- **Why the mild one fools more people** — a small premium sounds like it must buy something
- **The common thread** — you can name what changed on the label, never inside the package

**Key point:** These are not close calls where quality is hard to judge. The contents are identical by their own labels, so the entire price difference is the tag. The twelve-times case is easy to spot; the 1.5-times case is the one that empties wallets.

**Source note (`.src`):** Illustrative Example — constructed prices in familiar units; every per-unit price and multiple is computed in the draw function from the listed prices and pack sizes.

### Visualization — canvas `c2`, 720×300

Three pairs of everyday products with identical contents, shown as the multiple between the cheap and dear version of each.

- **Construction:** three pairs defined by their raw prices and pack sizes, with every displayed figure derived:
  - **salt** — `$0.80` per pound against `$9.60` per pound; multiple `9.60/0.80` = **12×**
  - **headache tablets** — `$4.00` for 100 tablets against `$11.20` for 40; per-tablet `4 cents` and `28 cents`, multiple = **7×**
  - **camera** — `$520` against `$790`; multiple `790/520` = **1.5×**
  - Multiples are computed with `(dear/cheap)`, printed to one decimal and trimmed to a whole number when it is one.
- **Title (bold 15px `P.ink`, centered, y=22):** "What the Extra Money Buys: Nothing You Can Point At"
- **Layout:** three rows on a 68px pitch from `y = 72`. Product name in bold 13px `P.ink` right-aligned at `LX = 172`, with the "what is identical" line beneath it in 12px `P.mute` ("same compound", "same active ingredient, same dose", "same five specs").
- **Bars:** for each row, a short `rgba(107,114,128,0.35)` bar for the cheap version at a fixed 40px, and an `rgba(217,89,38,0.45)` bar with a `P.orange` stroke for the dear version at `40 × multiple`, capped by the plot width — the salt bar is twelve times the length of its partner, which carries the finding without any explanation.
- **Per-unit labels:** 12px on each bar, in `P.mute` and `P.orange` respectively: "80c / lb" against "$9.60 / lb", "4c / tablet" against "28c / tablet", "$520" against "$790", all built from the price and pack-size constants.
- **Multiple labels:** bold 15px `P.orange` at the end of each dear bar, printed from the computed multiple: "12× the price", "7× the price", "1.5× the price".
- **Bottom note:** bold 12px `P.green` centred "the mildest one is the one that catches people", then 12px `P.mute` "a small premium sounds like it must buy something".
- **Caption (bold 13px `P.orange`, centered, `h−10`):** "Identical contents by their own labels — the whole difference is the tag."

---

## Section 3 — The Tag Changes the Taste

**Tags:** `not just talk` (magenta), `same drink` (violet), `the control` (blue)

**Bullets:**
- **The obvious objection** — people say dearer things are better to look clever, not because they are
- **The test** — thirty tasters, one drink, poured twice: plainly, then with a premium tag
- **Same liquid both times** — the only thing that changed between the pours was the tag
- **Poured plainly** — the room averaged 5.3 out of ten
- **Poured with the tag** — 6.8, a jump of 1.5 points on a drink that did not change
- **How widespread** — twenty-seven of the thirty scored the tagged pour above the plain one
- **The control** — pour twice with no tag either time and the averages land 0.04 apart
- **What that rules out** — the jump is not tasting twice, and it is not posturing either

**Key point:** The control is what makes this worth showing. Two unlabelled pours land on top of each other, so the 1.5-point jump has nowhere to come from except the tag. The tasters were not pretending — the price genuinely changed what they tasted.

**Source note (`.src`):** Illustrative Example — thirty constructed tasters, each with a seeded baseline and a fresh pour-to-pour wobble; both averages and all three tallies are computed in the draw function.

### Visualization — canvas `c3`, 720×350

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

## Regeneration instructions

- **Scope — read this first.** This page is about **price and newness standing in for quality**: the tag is read as a rating, and the seller writes the tag. Keep it concrete. Do **not** reintroduce version-number fault counts, release-age discovery curves, signalling-theory padding curves, or Monte Carlo hit-rate tables — earlier versions of this page carried all four, and they buried a simple idea under invented statistics.
- **Explain before measuring.** Section 1 says what the bias is in plain words with one worked example. Section 2 gives everyday examples with arithmetic anyone can check in their head. Only section 3 uses generated data, and only because the claim there — that the tag changes perception rather than just talk — genuinely needs a control group to support it.
- **Three sections. Do not add a fourth.** The earlier five-section version was unreadable and the extra sections were where the topic drift happened.
- **Prefer arithmetic over simulation.** A ratio of two prices is checkable by the reader; a correlation coefficient from a seeded draw is not. If a new figure is needed, look for one that divides two numbers already on the page. An earlier draft of section 2 asserted the best product was "the eleventh most expensive of sixty" when its own code made it the thirty-fourth — the kind of error that is invisible when the number comes out of a simulation.
- **Template:** the card-section layout from `cognitive-biases/05-clustering-illusion.html`. One `.card-section` per section, each holding an `<h2>` (1.3rem `#1a5276`, `border-bottom: 2px solid #2980b9`, 4px bottom padding) and a `table.layout` with a single row: `td.text-col` 50% / `td.viz-col` 50%.
- **Canvas placement:** `td.viz-col` gets `text-align: center` and the canvas `display: block; width: 100%; margin: 0 auto`, capped at 720px, so a wide cell leaves slack and the chart sits centred in the right half.
- **Text column order:** `.tags` pill row → `<ul>` of one-line bullets each opening `<b>label</b>` → one `.key-point` callout → `.src` note. Every section carries a `.src` because every example is constructed. No paragraph blocks, no data tables, no `.example` lines restating a bullet.
- **Bullet form:** one line that does not wrap at 50% column width (≤95 characters including the bold label), opening `<b>bold label</b>` then an em dash then the fact. Eight per section.
- **Section titles name the content.** No role labels ("The Trap", "The Defense") and no phrasing that would fit another page.
- **Page CSS:** body system-ui, white, `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with `border-bottom: 2px solid #2980b9`, 8px bottom padding. `.subtitle` `#666` 0.95rem, 32px bottom margin. `.card-section` 40px bottom margin. `table.layout` full width, border-collapse, cells vertical-align top padding 12px. `ul` 0.92rem margin `8px 0 8px 20px`, `li` 4px bottom margin, `li b` `#1a5276`. `.key-point` `#f8f9fa` background, `border-left: 3px solid #e74c3c`, padding 8px 12px, 0.9rem. `.src` 0.78rem `#888`. No nav, no `.nav` CSS, no back/home links, no cross-page links of any kind.
- **Tag pills:** `display:inline-block`, 0.72rem, weight 600, padding 2px 10px, radius 10px. Classes used: `.blue`, `.green`, `.red`, `.orange`, `.violet` `rgba(74,58,167,0.12)`/`#4a3aa7`, `.magenta` `rgba(213,81,129,0.14)`/`#c2426f`, `.aqua` `rgba(25,158,112,0.14)`/`#17805d`, `.yellow` `rgba(201,133,0,0.15)`/`#a06c00`.
- **Colour variety across sections is a requirement.** Section 1 blue spec bars with a red price row; section 2 mute-against-orange bars with a green note; section 3 magenta with a mute control. No chart repeats blue-fill-plus-orange-highlight.
- **Canvas:** CSS `width: 100%`, `border: 1px solid #e0e0e0`, radius 4px. Intrinsic `width="720"` plus the per-chart height (340, 300, 350). `setup(id)` caches the logical size in `dataset` on the first call, sets `style.maxWidth = 720px`, computes `scale = (cssW/720) × devicePixelRatio`, sizes the backing store to `logical × scale`, and `ctx.scale(scale, scale)` back to logical coordinates. Draws registered in `__charts`, re-run on debounced (150ms) resize.
- **Canvas font sizes:** chart title bold 15px; in-chart header bold 12–13px; labels 12px floor; callout figures bold 15px; caption bold 13px.
- **Palette** (shared `P` object): `blue #2a78d6`, `green #008300`, `magenta #d55181`, `yellow #c98500`, `aqua #199e70`, `orange #d95926`, `violet #4a3aa7`, `red #e74c3c`, `ink #1a5276`, `text #2c3e50`, `mute #6b7280`, `grid #e5e9ef`.
- **Determinism:** no `Math.random()`. Charts `c1` and `c2` are pure arithmetic on literal prices. `c3` uses a seeded Park–Miller LCG (`s = (s × 16807) % 2147483647`), seed 42. Every printed price, per-unit figure, multiple, percentage, average and tally is computed inside the draw function.
- **The camera prices appear in two charts.** `c1` and `c2` both use $520 and $790, and both derive their percentage and multiple from those two numbers rather than restating a result. Changing one price must change both charts consistently.
