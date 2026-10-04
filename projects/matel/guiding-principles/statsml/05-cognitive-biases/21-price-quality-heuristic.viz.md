# Price–Quality Heuristic — Viz

**Page type:** detail page, card-section (template `06-sectioned-cards-callout`)
**HTML title tag:** Price–Quality Heuristic — Cognitive Biases
**Template:** the card-section layout from `cognitive-biases/05-clustering-illusion.html`
**Source note wording:** the sibling `.txt.md` says figures are "computed at render time"; the html `.src` notes say "computed in the draw function".

House style (bullet form, tag-pill markup, text-column order, canvas DPR scaling, shared palette `P`, canvas font floors, section-header borders) lives in `ui-templates/` and is not restated here.

**Figures printed in the sibling `.txt.md` are computed here.** Changing a price, a pack size, or the seed invalidates that prose — the percentage, the multiples, the averages and the tallies must be re-read from the new draw and the text updated to match. The prose quotes, specifically: the `52%` and `+$270` camera gap and the model names `X200`/`X300` from `c1`; the `12×`, `7×` and `1.5×` multiples and the `80c`/`$9.60`, `4c`/`28c`, `$520`/`$790` per-unit figures from `c2`; and from `c3` the averages `5.3`, `6.8`, `5.3`, the `+1.5` tag gap, the `0.04` control gap, the tag tally `27` up / `2` down / `1` level, the control tally `12` up / `15` down / `3` level, and the plain-pour range — `min(plain)` = **3.0**, `max(plain)` = **8.1** — which the chart plots as dots but does not print as text.

**Determinism:** no `Math.random()`. Charts `c1` and `c2` are pure arithmetic on literal prices. `c3` uses a seeded Park–Miller LCG (`s = (s × 16807) % 2147483647`), seed 42. Every printed price, per-unit figure, multiple, percentage, average and tally is computed inside the draw function.

---

## 1. What the Bias Is

**Tag colors:** `core idea` blue, `plain terms` violet, `who sets the price` red
**Hue family:** blue spec bars with a red price row

### canvas `c1` — 720×340

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

## 2. The Same Thing at Twelve Times the Price

**Tag colors:** `everyday examples` orange, `identical contents` yellow, `per-unit price` green
**Hue family:** mute-against-orange bars with a green note

### canvas `c2` — 720×300

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

## 3. The Tag Changes the Taste

**Tag colors:** `not just talk` magenta, `same drink` violet, `the control` blue
**Hue family:** magenta with a mute control

### canvas `c3` — 720×350

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

## Page-specific constraints

- **Scope — read this first.** This page is about **price and newness standing in for quality**: the tag is read as a rating, and the seller writes the tag. Keep it concrete. Do **not** reintroduce version-number fault counts, release-age discovery curves, signalling-theory padding curves, or Monte Carlo hit-rate tables — earlier versions of this page carried all four, and they buried a simple idea under invented statistics.
- **Explain before measuring.** Section 1 says what the bias is in everyday vocabulary — no statistical terms, no significance talk — with one worked example that names its own figures. Section 2 gives everyday examples with arithmetic anyone can check in their head. Only section 3 uses generated data, and only because the claim there — that the tag changes perception rather than just talk — genuinely needs a control group to support it.
- **Text stands alone; the chart adds clarity** — the text carries the argument and names every quantity it turns on; the canvas adds precision, intermediate values and per-point labels. No bullet points at a position on the canvas. See `ui-templates/README.md`.
- **Three sections. Do not add a fourth.** The earlier five-section version was unreadable and the extra sections were where the topic drift happened.
- **Prefer arithmetic over simulation.** A ratio of two prices is checkable by the reader; a correlation coefficient from a seeded draw is not. If a new figure is needed, look for one that divides two numbers already on the page. An earlier draft of section 2 asserted the best product was "the eleventh most expensive of sixty" when its own code made it the thirty-fourth — the kind of error that is invisible when the number comes out of a simulation.
- **Canvas placement:** `td.viz-col` gets `text-align: center` and the canvas `display: block; width: 100%; margin: 0 auto`, capped at 720px, so a wide cell leaves slack and the chart sits centred in the right half.
- **Every section carries a `.src`** because every example is constructed. No paragraph blocks, no data tables, no `.example` lines restating a bullet.
- **Section titles name the content.** No role labels ("The Trap", "The Defense") and no phrasing that would fit another page.
- **Colour variety across sections is a requirement.** Section 1 blue spec bars with a red price row; section 2 mute-against-orange bars with a green note; section 3 magenta with a mute control. No chart repeats blue-fill-plus-orange-highlight.
- **Canvas heights are per-chart:** 340, 300, 350.
- **Every printed figure is computed inside its draw function** — price, per-unit figure, multiple, percentage, average and tally.
- **The camera prices appear in two charts.** `c1` and `c2` both use $520 and $790, and both derive their percentage and multiple from those two numbers rather than restating a result. Changing one price must change both charts consistently.
