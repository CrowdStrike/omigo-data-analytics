# Manufactured Reference Frame: Someone Else Chose What Counts as Normal

**Page type:** detail page — card-section template (see `statistical-paradoxes/03-berksons-paradox.html`)
**HTML title tag:** Manufactured Reference Frame — Cognitive Biases

**Subtitle:** Show a person sixty real examples out of six hundred and they will tell you what is typical. They will be describing the sixty, and they will believe they are describing the six hundred.

---

## Section 1 — Six Hundred Bikes for Sale, Sixty in the Window

**Tags:** `core idea` (violet), `a slice, not the whole` (blue), `every price is real` (magenta)

**Bullets:**
- **The town** — 600 second-hand bikes are for sale, priced from $65 up to $655
- **The shop window** — 60 of them on display, every one a shop-restored bike
- **Nothing is hidden** — every price in the window is real and not one of them is a lie
- **What the window leaves out** — 177 bikes in town cost less than the cheapest one shown
- **Middle of the window** — $325, and after a fortnight that is what "a bike costs"
- **Middle of the town** — $225, which is the number Alice actually needed
- **The gap** — $100, put there by the choosing rather than by the market

**Key point:** No false price was ever shown. The window's whole effect comes from which bikes it had room for, and the sense of "normal" it installs is a fact about the window that gets stored as a fact about the town.

**Source note (`.src`):** Illustrative Example — 600 seeded bike prices and a seeded draw of 60 restored ones; every middle price and count on the chart is computed from the plotted data.

### Visualization — canvas `c1`, 720×340

Two overlaid price histograms on one shared axis — the whole town in flat grey, the sixty on show in violet — with each group's middle price marked as a vertical line. The violet mass sits visibly to the right of the grey mass, and the two middle lines are $100 apart on screen.

- **Data:** seeded Park–Miller LCG, seed 42. 600 bikes; each is shop-restored with chance 0.25, and its price is `round(m · exp(sg · g) / 5) · 5` where `m = 340, sg = 0.26` for restored and `m = 190, sg = 0.36` otherwise, and `g` is a sum-of-four-uniforms bell approximation. This gives 170 restored bikes. The window is 60 of those 170, drawn without replacement by a second seeded stream (seed 31).
- **Computed and printed from the arrays:** town price range $65–$655; town middle $225; window middle $325; gap $100; 177 town bikes priced under the cheapest one on show; 475 of 600 town bikes (79%) priced under the window's middle.
- **Title (bold 15px `P.ink`, centered, y=22):** "Every Bike for Sale, and the Sixty on Display"
- **Plot box:** `PX = 50` to `PR = 500` (the right strip from x=528 carries the callouts), baseline `y = 246`, bar tops floor at `TOP = 92`. Price 0–700, bin width $50, 14 bins. Ticks and 12px `P.mute` labels every $100 formatted "$0" … "$700" via a shared `priceAxis()` helper. Axis title 12px `P.mute` centered below at `BASE + 37`: "asking price".
- **Bars:** each bin drawn as a share of its own group so the two are comparable, with the y scale running 0 to the larger of the two peak shares — town peaks at 124/600 in the $150 bin, window at 14/60 in the $250 bin. Town bars fill the full bin width, `rgba(107,114,128,0.28)` stroked `P.mute` 1px. Window bars are drawn inset (centred, 54% of bin width) in `rgba(74,58,167,0.60)` stroked `P.violet` 1.5px, so both are readable at once.
- **Middle-price lines:** town middle a dashed 2px `P.green` vertical (dash 5/4) from `y = 70` to the baseline; window middle a solid 2px `P.violet` vertical over the same span. Bold 12px labels at `y = 44`: "town middle $225" in `P.green` centred 34px left of its line, "window middle $325" in `P.violet` centred 46px right of its line, so the two never touch.
- **Gap bracket:** a 2px `P.magenta` horizontal segment at `y = 60` between the two lines with 5px end caps, labelled bold 12px `P.magenta` "$100 apart" 10px to the right of the window line — the figure computed as the difference of the two middles.
- **Right strip (left-aligned at x=528):** bold 13px `P.ink` "NO ROOM IN THE WINDOW"; bold 19px `P.mute` "177" with 12px `P.mute` "bikes in town cost" / "less than anything" / "on display" on three lines; then bold 13px `P.ink` "TOWN UNDER THE" / "WINDOW'S MIDDLE", bold 19px `P.violet` "79%" with 12px `P.mute` "475 of 600" beside it; then 12px `P.mute` "town prices run" / "$65 to $655".
- **Legend (12px, at `y = 298`):** a 12×12 `rgba(107,114,128,0.28)` swatch with "all 600 bikes for sale" in `P.mute`, and a 12×12 `rgba(74,58,167,0.60)` swatch with "the 60 in the window" in `P.violet`.
- **Caption (bold 13px `P.violet`, centered, `h − 10`):** "Real prices, honestly shown, describing a town they were never drawn from."

---

## Section 2 — A Middling Buyer Who Reads as Nearly Broke

**Tags:** `judging yourself` (magenta), `the median feels poor` (blue), `it feeds itself` (red)

**Bullets:**
- **Alice's budget** — $225, exactly the middle price of every bike for sale in town
- **Against the town** — it clears 304 of the 600 bikes, so she is a middling buyer
- **Against the window** — it clears 2 of the 60 on show, so she reads as nearly broke
- **What she concludes** — "I cannot afford a decent bike", which is false about her town
- **Why it lands** — she is measuring herself against a reference that was assembled
- **The self-feeding part** — feeling short, she stretches to $325 and joins the window
- **Nobody quoted her a price** — there is no figure she could have argued herself down from

**Key point:** She has not misjudged her own budget — she has misjudged the crowd she is standing in. Swap the crowd and the identical budget goes from ordinary to inadequate, which is why the conclusion feels like self-knowledge rather than an error about the town.

**Source note (`.src`):** Illustrative Example — the same seeded town and window as the previous section; both counts are scanned from the plotted dots.

### Visualization — canvas `c2`, 720×330

The same budget line drawn through two different crowds. In the top row half the dots are on the affordable side of the line; in the bottom row two dots are. Nothing about Alice changes between the rows.

- **Data:** the town and window arrays rebuilt from the same seeds as section 1. Her budget is set in code to the town's own middle, so the framing cannot drift from the data.
- **Computed and printed from the arrays:** 304 of 600 town bikes at or under $225 (51%); 2 of 60 window bikes at or under $225 (3%); the window's middle sits $100 above her budget.
- **Title (bold 15px `P.ink`, centered, y=22):** "One Budget of $225, Two Crowds to Stand In"
- **Axis:** price 0–700 across `PX = 146` to `w − 40`, baseline `y = 250`, shared `priceAxis()` helper — ticks and 12px `P.mute` labels every $100, axis title 12px `P.mute` centered below: "asking price".
- **Rows:** centres at `y = 92` (all 600 bikes for sale) and `y = 184` (the 60 in the window), each a 54px band with vertical position spread by one seeded stream (seed 13) so overlapping prices stay visible. Town dots radius 2.6, window dots radius 4.
- **Dot colour by side of the line:** at or under $225 filled `rgba(42,120,214,0.55)` stroked `P.blue`; above it filled `rgba(107,114,128,0.20)` stroked `P.mute` — so the affordable half of the top row is a solid blue block and the bottom row has two blue dots on the far left.
- **Row labels (bold 12px, right-aligned at `PX − 12`, two lines each):** "all 600 bikes" / "for sale in town" in `P.blue`; "the 60 bikes" / "in the window" in `P.magenta`.
- **Budget line:** solid 2.5px `P.ink` vertical at $225 from `y = 44` to the baseline, labelled bold 12px `P.ink` centred at `y = 38`: "Alice can spend $225".
- **Per-row counts (left-aligned at `PX + 4`, one line below each band):** bold 19px in the row's hue — "51%" then "3%" — followed at `PX + 56` by 12px `P.mute` "of the town is within reach — 304 of 600" and "of the window is — 2 of 60". Both percentages tallied in the draw function.
- **Stretch arrow:** a 2px `P.magenta` arrow along `y = 234` from $225 to the window middle $325, labelled bold 12px `P.magenta` 14px past its head: "she stretches $100 to feel ordinary again".
- **Caption (bold 13px `P.magenta`, centered, `h − 10`):** "Her budget did not shrink. The crowd she was shown did the shrinking."

---

## Section 3 — Fifteen Dull Prices Outrun One Shocking One

**Tags:** `volume beats intensity` (orange), `the forgettable ones` (yellow), `opposite of one big number` (magenta)

**Bullets:**
- **Her own year of looking** — 40 bikes seen around town, middle price $235
- **One shocking bike** — a $1,510 racer appears, sitting $1,275 above that middle
- **What it does to normal** — the middle moves to $245, ten dollars, and becomes an anecdote
- **Fifteen dull bikes** — each at $320, $85 over her middle, $1,275 over it in total
- **What they do to normal** — the middle moves to $275, forty dollars, four times as far
- **Same total excess** — $1,275 either way, and the mild many win by four to one
- **Why** — one freak price is filed as a freak; fifteen ordinary ones become the pile
- **The reversal** — a single memorable figure is the weaker mover here, not the stronger

**Key point:** A price so far out that you remember it gets stored as an exception and barely moves your sense of normal. Fifteen prices dull enough to forget cannot be quarantined as exceptions, so they quietly become the pile the next price is compared against.

**Source note (`.src`):** Illustrative Example — 40 seeded bikes drawn from the same town, with two additions carrying an identical total excess; every middle price is computed in the draw function.

### Visualization — canvas `c3`, 720×330

Three rows of the same forty prices with the same total excess added in two different shapes, and each row's middle price marked. The single shocking bike is drawn off the right edge as an arrow, so the eye can see it is one dot while the mild fifteen are a visible cluster — and the middle marker moves further for the cluster.

- **Data:** seeded LCG, seed 99, draws 40 bikes without replacement from the same seeded 600-bike town. Row 1 is those 40 alone. Row 2 adds one bike at $1,510. Row 3 adds fifteen bikes at $320.
- **Computed and printed from the arrays:** row 1 middle $235 (range $70–$420); row 2 middle $245, a shift of $10; row 3 middle $275, a shift of $40. Excess above the row-1 middle: `1510 − 235 = 1275` for the single bike and `15 × (320 − 235) = 1275` for the fifteen — identical, asserted by an equality check in the draw function that drives the "SAME EXCESS BOTH WAYS" header. Shift ratio 4×.
- **Title (bold 15px `P.ink`, centered, y=22):** "Forty Prices, Then $1,275 of Excess Added Two Ways"
- **Axis:** price 0–600 across `PX = 152` to `PR = 600` (the strip past x=620 carries the row notes), baseline `y = 250`, shared `priceAxis()` helper. Axis title 12px `P.mute` centered below: "asking price".
- **Rows:** centres at `y = 78, 146, 214`, each a 40px band with seeded vertical spread (seed 5, restarted per row so the forty base dots land identically in all three). Base dots radius 4, filled `rgba(107,114,128,0.30)` stroked `P.mute`.
- **Added marks:** row 2's single bike is beyond the axis, so it is drawn as a 2.5px `P.yellow` arrow running from `PR − 26` out to `PR + 14`, with bold 12px `P.yellow` "$1,510" and 12px `P.mute` "one bike," / "off the scale" beyond it. Row 3's fifteen bikes are drawn at $320 in the band with seeded spread (seed 23), radius 4.5, filled `rgba(217,89,38,0.60)` stroked `P.orange` — a visible vertical cluster — annotated bold 12px `P.orange` "15 dull bikes," over 12px `P.mute` "all in view".
- **Row labels (bold 12px, right-aligned at `PX − 12`, two lines each):** "the forty she" / "already knew" in `P.mute`; "plus one bike" / "at $1,510" in `P.yellow`; "plus 15" / "bikes at $320" in `P.orange`.
- **Reference line:** dashed 1.5px `P.mute` vertical (dash 5/4) at the row-1 middle $235 spanning all three bands, labelled bold 12px `P.mute` centred at `y = 42`: "her old middle $235".
- **Middle markers:** a filled diamond (7px half-width) in the row's hue at each row's middle, on the band's lower edge. Rows 2 and 3 also get a 2px arrow in the row's hue from $235 to that middle, and a bold 13px label to the right of the diamond combining the new middle and the shift: "$245   +$10" and "$275   +$40". Row 1 shows "$235" alone.
- **Bottom strip:** bold 12px `P.mute` "SAME EXCESS BOTH WAYS" at x=20, `y = 288` — the wording chosen by the equality check, not fixed — with 12px `P.mute` "$1,275 above her old middle" beneath; then bold 19px `P.orange` "4×" at x=300, `y = 300` with 12px `P.mute` "further, and it was the dull ones that did it" beside it. The ratio is computed from the two shifts.
- **Caption (bold 13px `P.orange`, centered, `h − 10`):** "The price you would repeat at dinner moved you least."

---

## Section 4 — The Prices She Can Name Are Not the Ones That Moved Her

**Tags:** `why it resists` (aqua), `nothing to discount` (yellow), `the quiet majority` (blue)

**Bullets:**
- **The gap to close** — $100 between the window's middle and the town's
- **The prices she can name** — $620, $610, $585 and $580, the four that stood out
- **Throwing those four out** — the middle falls to $322.50, three dollars of the hundred
- **Throwing out twenty** — a third of all she saw, and it closes twenty of the hundred
- **Why so little** — 27 of the 60 prices sat within $60 of the middle and left no trace
- **What actually closes it** — the ordinary bikes she never saw, put back into the pile
- **The remedy that fails** — discounting the loud prices, since the quiet ones did the work
- **The remedy that works** — going out and finding the cases the window had no reason to show

**Key point:** Correcting for a number you remember works because you can name it. Here the shift was done by prices too unremarkable to recall, so introspection has nothing to grab: the only repair is to go and collect the examples that were never put in front of you.

**Source note (`.src`):** Illustrative Example — the same seeded window of 60 prices; each repair is applied to the array in the draw function and its middle price recomputed.

### Visualization — canvas `c4`, 720×330

The sixty window prices as a tick strip on top, with the four loudest flagged and the quiet middle band shaded, and underneath a bar for how much of the $100 gap each repair actually closes. Three yellow stubs and one full aqua bar.

- **Data:** the same seeded 60-price window as sections 1 and 2. "Loudest" is defined in code as furthest from the window's own middle, so the flagged prices are read off a sort rather than picked by hand.
- **Computed and printed from the arrays:** window middle $325, town middle $225, gap $100. Dropping the 4 loudest leaves 56 prices with middle $322.50 — $2.50 closed, 3%. Dropping the 8 loudest leaves 52 with middle $320 — $5, 5%. Dropping the 20 loudest leaves 40 with middle $305 — $20, 20%. Restoring the town's missing ordinary bikes gives $225 — $100, 100%. The four loudest prices are $620, $610, $585, $580; 27 of the 60 lie within $60 of the window's middle.
- **Title (bold 15px `P.ink`, centered, y=22):** "Sixty Prices She Saw, Four She Could Repeat"
- **Tick strip:** price axis $150–$650 across `SX = 56` to `w − 36`, strip at `y = 62` height 30. The band within $60 of the window middle is shaded `rgba(42,120,214,0.12)` first, labelled 12px `P.blue` centred below at `y = 110`: "27 of the 60 sat in this band and left no trace". Each price is a 2px vertical tick spanning the strip in `rgba(107,114,128,0.55)`; the four loudest are 2.5px `P.yellow`. The window middle is a 2px `P.magenta` tick overhanging the strip by 5px, labelled bold 12px `P.magenta` "window middle $325" centred at `y = 51`.
- **Loud-price callout:** a 1.5px `P.yellow` horizontal bracket at `y = 42` spanning the four flagged ticks, with bold 12px `P.yellow` above it at `y = 35` reading "the 4 she can name: $620, $610, $585, $580" — the prices taken from the sort, so the label cannot drift from the flagged ticks.
- **Bars:** header bold 13px `P.ink` at `SX`, `y = 138`: "SHARE OF THE $100 GAP EACH REPAIR CLOSES". Four horizontal bars, `BX = 262`, full width `BW = w − 118 − BX` representing the whole $100 gap, pitch 38, first bar top `y = 148`, height 22. Track `rgba(107,114,128,0.12)` full width; fill proportional to dollars closed, floored at 2px so a near-zero repair is still visible. The three discounting repairs fill `rgba(201,133,0,0.45)` stroked `P.yellow`; the last fills `rgba(25,158,112,0.50)` stroked `P.aqua`.
- **Bar labels (right-aligned at `BX − 12`, two lines each):** 12px `P.mute` name over 12px `P.mute` resulting middle — "drop the 4 loudest prices" / "middle becomes $322.50"; "drop the 8 loudest" / "middle becomes $320"; "drop the 20 loudest" / "middle becomes $305"; "add back the bikes she never saw" / "middle becomes $225".
- **Bar values (left-aligned 8px past each fill):** bold 12px in the bar's hue giving dollars closed and share — "$2.50 · 3%", "$5 · 5%", "$20 · 20%", "$100 · 100%" — every figure derived from the recomputed middle.
- **Callout:** bold 19px `P.aqua` "$100" at `SX`, `y = 304` with 12px `P.mute` "closed only by the examples nobody put in the window" at `SX + 58`.
- **Caption (bold 13px `P.aqua`, centered, `h − 6`):** "You cannot subtract prices you never noticed — only add the ones you missed."

---

## Section 5 — One Window, Two Questions, One Right Answer

**Tags:** `the boundary` (green), `sometimes it is the right reference` (aqua), `silent substitution` (magenta)

**Bullets:**
- **One window, two questions** — the same 60 restored bikes, asked to answer both
- **What does a restored bike cost** — the window says $325, the truth is $330
- **What does a bike in town cost** — the window says $325, the truth is $225
- **The window never changed** — the same prices answer one question well and one badly
- **When it is the right reference** — when the group you asked about is the group on show
- **When it quietly substitutes** — when you wanted the town and got the restored corner
- **The test** — name the group your question is about, then ask who was left out
- **Restored bikes are real** — 170 of the 600 are restored, so the window is no fiction

**Key point:** A curated stream is not a distortion by nature — asked what a restored bike costs, sixty restored bikes are exactly the reference you want and land within $5. It becomes the bias only when it stands in for a group it was never drawn from, and the tell is that the substitution is silent: the window looks identical in both cases.

**Source note (`.src`):** Illustrative Example — the same seeded town and window; both true middles and both misses are computed in the draw function.

### Visualization — canvas `c5`, 720×320

One vertical line for what the window says, held fixed, with two rows for the two questions. In the top row the truth marker sits almost on the line; in the bottom row it sits $100 away. The line never moves — only the question does.

- **Data:** the same seeded 600-bike town and 60-bike window. Row 1's population is the 170 restored bikes, row 2's is all 600.
- **Computed and printed from the arrays:** window middle $325. Restored middle $330 — miss $5, 2%. Town middle $225 — miss $100, 44%. Restored count 170 of 600 (28%).
- **Title (bold 15px `P.ink`, centered, y=22):** "The Same Window Answering Two Different Questions"
- **Axis:** price 0–700 across `PX = 176` to `w − 118`, baseline `y = 236`, shared `priceAxis()` helper — ticks and 12px `P.mute` labels every $100, axis title 12px `P.mute` centered below: "asking price".
- **Window line:** solid 2.5px `P.magenta` vertical at $325 from `y = 44` to the baseline, labelled bold 12px `P.magenta` centred at `y = 38`: "the window says $325".
- **Rows:** centres at `y = 94` and `y = 176`, each a 48px band. Row 1 plots the 170 restored prices, row 2 the 600 town prices, both as dots of radius 2.8 with seeded vertical spread (seed 17, restarted per row), filled `rgba(107,114,128,0.24)` stroked `P.mute` — the population the question is about, drawn faintly so the markers read on top.
- **Row labels (bold 12px, right-aligned at `PX − 14`, three lines each so nothing runs off the left edge):** "if the question is" / "what a restored" / "bike costs" in `P.aqua`; "if the question is" / "what a bike in" / "town costs" in `P.magenta`.
- **Truth markers:** a filled `P.green` diamond (8px half-width) at each row's true middle — $330 and $225 — each with a bold 13px `P.green` label below the band: "truth $330", "truth $225".
- **Miss brackets:** a 2px horizontal segment from the truth marker to the window line at `cy + 30` with 5px end caps — row 1 in `P.aqua`, row 2 in `P.magenta`. Row 1's is a stub labelled bold 12px `P.aqua` "$5 off" just past it; row 2's spans the gap and is labelled bold 12px `P.magenta` "$100 off".
- **Right strip (left-aligned at `w − 104`):** per row a bold 19px figure in the row's hue — "2% off" then "44% off" — each vertically aligned with its band, with a bold 12px verdict beneath: `P.aqua` "the right" / "reference" for row 1 and `P.magenta` "the wrong" / "group" for row 2. The verdicts are assigned in the draw function by comparing the two misses, never hardcoded.
- **Bottom note (12px `P.mute`, centered, `y = 288`):** "170 of the 600 bikes in town really are shop-restored — the window is a true slice of something".
- **Caption (bold 13px `P.green`, centered, `h − 10`):** "Name the group your question is about before trusting the examples in front of you."

---

## Regeneration instructions

- **Template:** the card-section layout from `statistical-paradoxes/03-berksons-paradox.html`, matching the approved conversions in `05-clustering-illusion.html` and `01-confirmation-bias.html`. Five `.card-section` blocks, each an `<h2>` (1.3rem `#1a5276`, `border-bottom: 2px solid #2980b9`, 4px bottom padding) plus a `table.layout` with one row: `td.text-col` 50% / `td.viz-col` 50%. One canvas per section. No index number anywhere on the page.
- **Text column order:** `.tags` pill row → `<ul>` of one-line bullets each opening `<b>label</b>` then an em dash → one `.key-point` → `.src` note. Every section carries a `.src` note because every figure on the page is constructed. No paragraph blocks, no `.example` lines, no data tables, no philosophy box.
- **Bullet form:** each bullet is ONE line that does not wrap at 50% column width — verified at ≤95 characters including the bold label. Counts follow content: 7, 7, 8, 8, 8. Nothing padded, nothing restated between a bullet and the key point.
- **Language:** layman-first. No jargon from the banned list. No recommender, algorithm, feed-ranking, engagement, impression, ad-revenue or platform-incentive vocabulary — the whole page runs on a shop window, a town full of bikes, and one buyer named Alice.
- **Scope boundary against `02-anchoring-bias`:** that page covers a single salient number, consciously seen at one moment, pulling one estimate. This page is the accumulated volume-based version, and the distinction is made load-bearing rather than mentioned. Section 3 is the argument: with the total excess held identical at $1,275, fifteen forgettable prices move the sense of normal four times as far as one unforgettable price does, which reverses the direction anchoring would predict. Section 4 completes it — an anchor can be named and discounted, and here the discounting repair recovers 3% of the gap because the movers were the prices too dull to recall. No cross-links of any kind.
- **Chart shapes deliberately unlike `02-anchoring-bias`:** that page opens on two swarms split by an arbitrary number and uses a gap bracket between two group averages as its signature. This page opens on two overlaid histograms of a population against a slice of it, and its other charts are a shared budget line through two crowds, three rows sharing one reference line with off-scale marking, a repair-effectiveness bar set with a tick strip above it, and one fixed line answering two questions. No swarm-pair-with-gap-bracket figure appears.
- **Section titles name content**, never a role. The old page's "The Mechanism", "Differs From Classical Anchoring", "The Exposure → Belief → Action Pipeline", "Domains of Application" and "Why It Persists" are all replaced.
- **Last section is the boundary case** and must stay precise. It does not claim curated exposure is always misleading: asked what a shop-restored bike costs, the window lands within $5 of the truth and is the correct reference. The bias is the silent substitution of one group for another, and the discriminator is whether the population your question names is the population the examples were drawn from — not how strongly the examples moved you.
- **Page CSS:** body system-ui, white, `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with `border-bottom: 2px solid #2980b9`, 8px bottom padding. `.subtitle` `#666` 0.95rem, 32px bottom margin. `.card-section` 40px bottom margin. `table.layout` full width, border-collapse, td vertical-align top padding 12px, `.text-col`/`.viz-col` 50% each, `.viz-col` `text-align: center`. `canvas` `display:block; width:100%; margin:0 auto; border:1px solid #e0e0e0; border-radius:4px`. `ul` 0.92rem margin `8px 0 8px 20px`, `li` 4px bottom margin, `li b` `#1a5276`. `.key-point` `#f8f9fa` background, `border-left: 3px solid #e74c3c`, padding 8px 12px, 0.9rem. `.src` 0.78rem `#888`. No nav, no `.nav` CSS, no back/home links.
- **Tag pills:** `display:inline-block`, 0.72rem, weight 600, padding 2px 10px, radius 10px. Classes used: `.blue` `rgba(26,82,118,0.12)`/`#1a5276`, `.green` `rgba(39,174,96,0.15)`/`#27ae60`, `.red` `rgba(231,76,60,0.12)`/`#e74c3c`, `.orange` `rgba(230,126,34,0.15)`/`#e67e22`, `.violet` `rgba(74,58,167,0.12)`/`#4a3aa7`, `.magenta` `rgba(213,81,129,0.14)`/`#c2426f`, `.aqua` `rgba(25,158,112,0.14)`/`#17805d`, `.yellow` `rgba(201,133,0,0.15)`/`#a06b00`.
- **Colour rotation across sections is a requirement:** section 1 violet slice over a mute population with a green truth line, section 2 blue crowd against a magenta window, section 3 yellow single against an orange cluster over mute, section 4 yellow failing repairs against one aqua repair with a blue quiet band, section 5 green truth with aqua-versus-magenta verdicts. Hard red `#e74c3c` appears only as the `.key-point` left border.
- **Canvas:** intrinsic `width="720"` plus per-chart height (340, 330, 330, 330, 320). `setup(id)` caches the logical size in `dataset` on first call (because `canvas.width` overwrites the attribute), sets `style.maxWidth = 720px`, computes `scale = (cssW / 720) × devicePixelRatio`, sizes the backing store to `logical × scale`, and `ctx.scale(scale, scale)` back to logical coordinates. Draws are registered in `__charts` and re-run on a debounced (150ms) resize.- **Canvas fonts:** chart title bold 15px; in-chart headers and inline labels bold 12–13px; plain labels 12px floor; one big callout figure per chart at bold 19px; caption bold 13px ending every chart. No tables drawn on canvas.
- **Palette** (shared `P` object): `blue #2a78d6`, `green #008300`, `magenta #d55181`, `yellow #c98500`, `aqua #199e70`, `orange #d95926`, `violet #4a3aa7`, `ink #1a5276`, `text #2c3e50`, `mute #6b7280`, `grid #e5e9ef`.
- **Determinism:** no `Math.random()`. Seeded Park–Miller LCG (`s = (s × 16807) % 2147483647`) with a sum-of-four-uniforms bell helper. Seed 42 builds the town, seed 31 draws the window, seed 99 draws Alice's forty, seeds 5/13/17/23 provide vertical jitter only. Three shared builders — `buildTown()`, `buildWindow()` and `buildSeen()` — plus `restoredPrices()`, `median()`, `money()` and a shared `priceAxis()` are called by every chart, so all five describe one town. Changing the draw order in one place silently breaks the numbers in the others.
- **Shared helpers:** `money(v)` prints half-dollars honestly (a median of an even-sized set lands on $322.50) and inserts a thousands comma, so $1,510 and $1,275 read correctly. `priceAxis()` draws the $100 ticks and the "asking price" title on all five charts.
- **Lead chart shows the effect, not a description of it.** Two overlaid price histograms with their middles $100 apart; the violet slice sitting to the right of the grey population is readable before any number. No second-order construction is used as the opening figure.
- **Non-degenerate constructions checked.** The window is not disjoint from the town's cheap end — its cheapest bike at $175 still has 177 town bikes below it, so the overlap is partial rather than total. Alice's window count is 2 of 60, not 0, so no share is exactly 0% or 100%. The two excess totals in section 3 are equal to the dollar (1,275 both ways) and both shifts are non-zero, so the 4× ratio is neither undefined nor trivial. In section 4 the weakest repair closes $2.50 rather than $0, so its bar is drawn rather than absent.
- **Label geometry verified.** Every `fillText` on all five canvases was checked against the 720-wide logical box: nothing overflows either edge, nothing falls below the canvas height, no label sits under 12px, and no two labels on the same canvas overlap. Section 5's row labels are split across three lines for this reason, and section 3's off-scale note is kept short enough to sit in the right margin.
- **Corrections applied to the old version of this page:** every number on the old page was asserted, not computed. Its first chart hardcoded two proportion arrays as "actual market" and "what the user was shown", so the "$100 is typical" claim had no data behind it and the two series did not come from one population. Its second, third, fourth and fifth charts drew no data at all — they were text boxes, a flow diagram, a table rendered on canvas (banned) and a decorative circle, none of which showed the effect. The `.example` paragraph and duplicated `.key-point` blocks in sections 2 and 4 of the old HTML were literal copy-paste defects. The recommender/engagement/ad-revenue framing has been dropped in favour of the shop window, and the anchoring comparison is now a computed result rather than a side-by-side list of adjectives.
