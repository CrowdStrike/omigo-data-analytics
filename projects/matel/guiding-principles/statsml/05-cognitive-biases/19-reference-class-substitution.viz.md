# Reference-Class Substitution — Viz

**Page type:** detail page, card-section (template `06-sectioned-cards-callout`)
**HTML title tag:** Reference-Class Substitution — Cognitive Biases
**Template:** the card-section layout from `statistical-paradoxes/03-berksons-paradox.html`, matching the approved conversions in `05-clustering-illusion.html` and `01-confirmation-bias.html`. Canvas heights 270, 270, 260. No index number anywhere on the page.
**Source note wording:** the sibling `.txt.md` says figures are "computed at render time"; the html `.src` notes for sections 2 and 3 say "tallied from the arrays in the draw function" and "computed in the draw function".

House style (bullet form, tag-pill markup, text-column order, canvas DPR scaling, shared palette `P`, canvas font floors, section-header borders) lives in `ui-templates/` and is not restated here.

**Figures printed in the sibling `.txt.md` are computed here.** The arrays below are the page's single source of data: the sibling's price table and its derivation table are both read off them, and the prose quotes town middle $190, window middle $360, restored middle $340, the $170 gap, the $20 restored miss, the 8.5× ratio of the two misses, the $70–$450 town range, the $180 cheapest bike on show, the 11 bikes priced under it, and the 13-of-25 versus 1-of-9 reach counts. Changing a price or Alice's budget invalidates every one of those — re-read them from the new arrays and update the sibling text to match.

**Determinism:** **No PRNG on this page at all.** No `Math.random()` and no seeded LCG either — nothing is generated, so nothing needs seeding. Distinct prices mean dots never collide, so there is no jitter and therefore no jitter seeds. A future edit that adds a generated series must add the canonical `lcg()` helper rather than reaching for `Math.random()`.

**The one dataset behind all three charts.** Written out as literal arrays so a reader can count them on screen and check every figure by hand. The human-readable price table and derivation table in the sibling `.txt.md` are derived from these arrays and must be regenerated whenever they change.

```js
// 14 ordinary bikes for sale around town
var PLAIN    = [70, 80, 90, 100, 110, 120, 130, 140, 150, 160, 170, 190, 200, 220];
// 11 shop-restored bikes for sale
var RESTORED = [180, 240, 280, 300, 320, 340, 360, 380, 400, 430, 450];
// the 9 restored ones the shop has room to display
var WINDOW   = [180, 280, 320, 340, 360, 380, 400, 430, 450];
```

All twenty-five prices are distinct, so dots never collide and no vertical jitter is needed.

**Shared helpers:** `median()`, `money()` (thousands comma, half-dollars printed honestly), `dot()`, `diamond()`, `bracket()` (a span with 5px end caps, used for the gap on chart 1 and both misses on chart 3), and `priceAxis()` which draws the $100 ticks and the "asking price" title on all three charts. `TOWN` is built once as `PLAIN.concat(RESTORED)` sorted, so all three charts describe one town by construction. No `arrow()` helper — nothing on the page draws an arrow any more.

---

## 1. Twenty-Five Bikes for Sale, Nine in the Window

**Tag colors:** `core idea` violet, `a slice, not the whole` blue, `every price is real` magenta
**Hue family:** violet slice over grey with a green truth line

### canvas `c1` — 720×270

**One number line, twenty-five dots, two middles.** The nine on display are the filled violet dots; the rest are hollow grey. The violet ones are all bunched at the right-hand end, so the slice is visible before any number is read.

- **Data:** the `PLAIN`, `RESTORED` and `WINDOW` literals. No generation, no jitter — all prices distinct.
- **Computed at render time:** town middle $190, window middle $360, gap $170.
- **Title (bold 15px `P.ink`, centered, y=22):** "Twenty-Five Bikes for Sale, Nine of Them on Display"
- **Axis:** price $0–$500 across `PX = 60` to `PR = 660`, baseline `y = 170`. This chart has no right-hand labels, so it keeps the full width. Ticks and 12px `P.mute` labels every $100 via the shared `priceAxis()` helper; axis title 12px `P.mute` centered at `BASE + 37`: "asking price".
- **The single dot row (`cy = 126`, radius 6):** all twenty-five prices on one line. $1 spans 1.2px, so the $10 price steps put adjacent dots edge-to-edge and every dot stays countable. On display → filled `rgba(74,58,167,0.60)` stroked `P.violet` 1.5px. Not on display → filled `rgba(107,114,128,0.15)` stroked `P.mute` 1px.
- **Two middle lines:** town middle a dashed 2px `P.green` vertical (dash 5/4) from `y = 82` to the baseline; window middle a solid 2px `P.violet` vertical over the same span. Labels bold 12px at `y = 74`, each centred on its own line — at this scale they sit 204px apart and need no nudging.
- **Gap bracket:** a 2px `P.magenta` horizontal segment at `y = 52` between the two lines with 5px end caps, labelled bold 12px `P.magenta` "$170 apart" centred above at `y = 40` — computed as the difference of the two middles.
- **Legend (12px, swatches at `y = 226`, text baseline `y = 236`):** a 12×12 `rgba(107,114,128,0.15)` swatch with "not in the window" in `P.mute` at `PX`, and a 12×12 `rgba(74,58,167,0.60)` swatch with "the 9 on display" in `P.violet` at `PX + 200`.
- **Caption (bold 13px `P.violet`, centered, `y = 260`):** "Real prices, honestly shown, describing a town they were never drawn from."
- **Deliberately absent:** no second row, no right-hand callout strip, no counting instructions, no histogram. The lesson is "the filled dots are all on one side", and one row of dots says it.

---

## 2. A Middling Buyer Who Reads as Nearly Broke

**Tag colors:** `judging yourself` magenta, `the median feels poor` blue, `it feeds itself` red
**Hue family:** blue affordable against magenta window

### canvas `c2` — 720×270

**One budget line through two crowds.** Blue dots are the bikes she can afford. The top row is half blue, the bottom row has one blue dot. Nothing about Alice changes between the rows — only who she is standing next to.

- **Data:** the same literals. Her budget is set in code to the town's own middle, so the framing cannot drift from the data.
- **Computed at render time:** 13 of 25 town bikes at or under $190; 1 of 9 window bikes at or under $190.
- **Title (bold 15px `P.ink`, centered, y=22):** "One Budget of $190, Two Crowds to Stand In"
- **Axis:** price $0–$500 across `PX = 140` to `PR = 560`, baseline `y = 200`, shared `priceAxis()` helper, axis title "asking price". The box stops at 560 so the per-row count labels fit inside the 720 box.
- **Rows:** town at `cy = 90`, window at `cy = 150`, dots radius 4, no jitter — $10 price steps land 8.4px apart here, so radius 4 keeps neighbours separable.
- **Dot colour by side of the line:** at or under $190 filled `rgba(42,120,214,0.55)` stroked `P.blue`; above it filled `rgba(107,114,128,0.15)` stroked `P.mute`. The top row reads blue-then-grey; the bottom row is one blue dot then all grey.
- **Row labels (bold 12px, right-aligned at `PX − 12`, two lines each):** "all 25 bikes" / "for sale in town" in `P.blue`; "the 9 bikes" / "in the window" in `P.magenta`.
- **Budget line:** solid 2.5px `P.ink` vertical at $190 from `y = 46` to the baseline, labelled bold 12px `P.ink` centred at `y = 38`: "Alice can spend $190".
- **Per-row counts (left-aligned at `PR + 14` = 574, past the axis end):** bold 13px in the row's hue — "13 of 25" then "1 of 9" — each with 12px `P.mute` "in reach" on the line beneath. Both tallies computed in the draw function.
- **Caption (bold 13px `P.magenta`, centered, `y = 260`):** "Her budget did not shrink. The crowd she was shown did the shrinking."
- **Deliberately absent:** no percentage drawn beside the counts — the canvas prints "13 of 25" and "1 of 9" and the prose carries the "just over half" share — and no "she stretches $170" arrow, which belongs in a bullet rather than as a third annotation layer on a chart whose whole point is one vertical line.

---

## 3. One Window, Two Questions, One Right Answer

**Tag colors:** `the boundary` green, `sometimes it is the right reference` aqua, `silent substitution` magenta
**Hue family:** green truth with aqua-versus-magenta verdicts

### canvas `c3` — 720×260

**One fixed line, two questions, two distances.** The window's answer is a single vertical line that never moves. Each question gets a green diamond at its true middle and a bracket showing how far off the line is — a stub for one question, a long span for the other.

- **Data:** the same literals. Row 1's truth is the median of the 11 `RESTORED` prices, row 2's is the median of all 25.
- **Computed at render time:** window says $360. Restored middle $340 — miss $20. Town middle $190 — miss $170. Verdicts assigned by comparing the two misses in code, never hardcoded.
- **Title (bold 15px `P.ink`, centered, y=22):** "The Same Window Answering Two Different Questions"
- **Axis:** price $0–$500 across `PX = 190` to `PR = 530`, baseline `y = 190`, shared `priceAxis()` helper, axis title "asking price". The box stops at 530 so the three-line row labels clear the left edge and the verdict labels fit on the right.
- **Window line:** solid 2.5px `P.magenta` vertical at $360 from `y = 48` to the baseline, labelled bold 12px `P.magenta` centred at `y = 40`: "the window says $360".
- **Rows:** `cy = 90` (restored question, `P.aqua`) and `cy = 150` (town question, `P.magenta`). Each carries a filled `P.green` diamond (8px half-width) at its true middle, labelled bold 12px `P.green` centred at `cy + 26`: "truth $340", "truth $190".
- **Miss brackets:** a 2px horizontal segment in the row's hue from the diamond to the window line, drawn at `cy` with 5px end caps by the shared `bracket()` helper. Row 1's spans 14px on screen, row 2's spans 116px — the honest ratio, and the reason the two rows read differently at a glance.
- **Verdicts (left-aligned at `PR + 14` = 544):** bold 13px in the row's hue giving the miss — "$20 off" then "$170 off" — with the verdict beneath in 12px of the same hue: "the right group" and "the wrong group". No percentage restatement of the miss; the bracket lengths already carry the comparison.
- **Caption (bold 13px `P.green`, centered, `y = 250`):** "Name the group your question is about before trusting the examples in front of you."
- **Deliberately absent:** the two population dot clouds. Plotting 11 and 25 faint dots behind the markers restated section 1's figure and buried the only thing this chart is for — the two distances from one line.

---

## Page-specific constraints

- **One idea per chart, and this is the tightest constraint on the page.** Chart 1 is a single row of dots with two middles. Chart 2 is one vertical line through two rows. Chart 3 is one fixed line and two distances from it. Each earlier revision of this page failed by *adding* to these figures — right-hand callout strips with three stacked statistics, a duplicate "window only" row above the town row, a stretch arrow, faint population clouds behind markers, share-normalised twin histograms. Every one of those was defensible on its own and collectively they buried a simple point. **If a figure needs a second annotation layer to make its case, the case belongs in a bullet.**
- **The example is deliberately countable.** Twenty-five bikes as literal price arrays, all distinct. A reader can count the dots, find the middle by eye, and verify the gap without trusting the page. An earlier version generated 600 prices from a seeded lognormal and plotted share-normalised histograms — reproducible, but no reader could check a single figure on it. Do not reintroduce generated data here: if a chart on this page needs a number, the number must be countable on the chart.
- **Dot radius is set by the price spacing, not by taste.** The prices step by $10 in the dense stretch, so each chart's radius is chosen against its own `$1 → px` scale to keep neighbours touching-but-distinct: radius 6 at 1.20px/$ on chart 1 (12px gap, 12px diameter), radius 4 at 0.84px/$ on chart 2 (8.4px gap, 8px diameter). Narrowing a plot box to make room for labels means rechecking the radius — that is exactly why chart 2 is radius 4 and not 5.
- **Three sections only, and two were cut on purpose.** A five-section version added a "same excess two ways" chart (forty prices plus either one $1,510 bike or fifteen $320 bikes, three medians compared through an equality check) and a "which repair closes the gap" chart (four repairs as share-of-gap bars). Both asked the reader to hold several medians in their head to reach a point the three remaining sections already make. Do not reinstate them here; the volume-beats-intensity argument deserves its own page if it is wanted.
- **Text stands alone; the chart adds clarity** — the text carries the argument and names every quantity it turns on; the canvas adds precision, intermediate values and per-point labels. No bullet points at a position on the canvas. See `ui-templates/README.md`.
- **No data tables on the page.** No paragraph blocks, no `.example` lines, no philosophy box.
- **Language:** layman-first. No recommender, algorithm, feed-ranking, engagement, impression, ad-revenue or platform-incentive vocabulary — the page runs on a shop window, a town with twenty-five bikes for sale, and one buyer named Alice. "Reference class" appears in the title and nowhere in the body.
- **Scope boundary against `02-anchoring-bias`:** that page covers a single salient number, consciously seen at one moment, pulling one estimate. This page is the accumulated-exposure version — no figure is ever quoted to Alice, which is why "nobody quoted her a price" is a load-bearing bullet in section 2 rather than an aside. The distinction is carried by that bullet and by the chart shapes, not by a comparison section. No cross-links of any kind.
- **Chart shapes deliberately unlike `02-anchoring-bias`:** that page opens on two swarms split by an arbitrary number and uses a gap bracket between two group averages as its signature. This page opens on a single countable dot row with its displayed members filled in; its other charts are one budget line through two rows and one fixed line with two distances.
- **Section titles name content**, never a role. "The Mechanism", "The Exposure → Belief → Action Pipeline", "Domains of Application" and "Why It Persists" are banned as headings here.
- **Last section is the boundary case** and must stay precise. It does not claim curated exposure is always misleading: asked what a shop-restored bike costs, the window lands $20 from the truth and is the correct reference. The bias is the silent substitution of one group for another, and the discriminator is whether the population your question names is the population the examples were drawn from — not how strongly the examples moved you.
- **Colour rotation across sections is a requirement:** section 1 violet slice over grey with a green truth line, section 2 blue affordable against magenta window, section 3 green truth with aqua-versus-magenta verdicts. Hard red `#e74c3c` appears only as the `.key-point` left border.
- **No 19px callout figures** — the numbers on this page are small integers and dollar amounts that read fine at 12–13px, and a giant figure was part of what made the old charts busy. No tables drawn on canvas.
- **Non-degenerate constructions checked.** The window is a *strict* subset of the restored bikes — $240 and $300 are for sale but not displayed — so "curated" does not collapse into "all restored bikes". Alice's window count is 1 of 9, not 0, so no share is 0% or 100%. Section 3's restored miss is $20 rather than $0, so the "right reference" row is close without being a suspicious exact hit, and its bracket is 14px wide rather than invisible.
- **Label geometry verified.** Every `fillText` on all three canvases was checked against the 720-wide logical box: nothing overflows either edge, nothing falls below the canvas height, no label sits under 12px, and no two labels on the same canvas overlap. Chart 1's two middle labels sit 204px apart and need no horizontal offset — the nudges the old version used are gone. Charts 2 and 3 end their plot boxes at 560 and 530 precisely to leave a label column inside the canvas; widening either one pushes those labels off the right edge.
