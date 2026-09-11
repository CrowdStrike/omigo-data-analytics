# Kidney Exchange

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Kidney Exchange

**Subtitle:** Paying for organs is illegal, so kidneys trade through swap cycles and donor chains instead of prices

## A Willing Donor Who Can't Give

**Tags:** `core idea` (blue), `no prices` (red)

- **The stuck pair** — Alice needs a kidney; husband Bob volunteers, but his blood type doesn't fit
- **The other stuck pair** — Carol needs a kidney too; her brother Dan is willing and incompatible as well
- **The swap** — Bob's kidney fits Carol, Dan's fits Alice — two transplants instead of zero
- **No prices allowed** — organ sale is illegal in the US (since 1984); economists call it "repugnant"
- **Matching's job** — with money banned, an algorithm must do the work prices normally do in a market

*Example (italic):* Two incompatible pairs point in opposite directions — neither can transplant alone, both can by trading donors.

**Key point:** Kidney exchange is a matching market by law: prices are forbidden, so who-gives-to-whom is decided by matching, not money.

### Visualization (canvas `c1`, 720×300)

Two donor-patient pairs side by side; red crossed arrows inside each pair (own donor incompatible), green diagonal arrows between pairs (the swap).

- **Title (bold 15px, `#1a5276`, top center):** "Two Stuck Pairs, One Swap (illustrative)".
- **Pair boxes:** rounded rects 250×170, 2px border `#e5e9ef`, fill `#fbfcfd` — Pair 1 at (60, 60), Pair 2 at (410, 60); bold 13px ink `#1a5276` labels "PAIR 1" at (185, 78) and "PAIR 2" at (535, 78) centered.
- **People:** blue `#2a78d6` filled circles r=14 for donors — Bob at (120, 130), Dan at (600, 130); magenta `#d55181` filled circles r=14 for patients — Alice at (120, 195), Carol at (600, 195). Bold 12px labels next to each circle (offset 22px toward panel center): "Bob — donor", "Alice — patient", "Dan — donor", "Carol — patient"; 11px mute `#6b7280` under the donor labels: "type doesn't fit".
- **Incompatible arrows:** 2.5px red `#e74c3c` vertical arrow Bob(120,146)→Alice(120,179) with filled arrowhead; bold 15px red "✗" at (138, 166). Mirrored: Dan(600,146)→Carol(600,179), "✗" at (582, 166).
- **Swap arrows:** 3px green `#008300` arrow from Bob (134, 130) to Carol (586, 195) with arrowhead at the Carol end, bold 12px green "Bob → Carol ✓" above the midpoint (360, 148); 3px green arrow from Dan (586, 130) to Alice (134, 195) with arrowhead at the Alice end, bold 12px green "Dan → Alice ✓" below the midpoint (360, 196).
- **Callout (bold 12px violet `#4a3aa7`, centered at y=278):** "no money changes hands — the trade is kidney for kidney".

## Three Pairs, One Circle

**Tags:** `worked example` (blue), `swap cycles` (green)

- **The near-miss** — Pair 1 (donor A, patient B) and Pair 2 (donor B, patient A) look like a swap
- **The block** — patient 2's antibodies react to donor 1 (a positive crossmatch); the 2-way swap is dead
- **The third pair** — Pair 3 (donor A, patient A) reopens the trade as a circle
- **The cycle** — donor 1 → patient 3, donor 3 → patient 2, donor 2 → patient 1: three transplants
- **No smaller deal** — check every 2-way among the three: each fails on blood type or crossmatch
- **All at once** — cycles run simultaneously; else a donor whose patient already received backs out

*Example (italic):* No two of the three pairs can trade alone — the circle is the only deal, verified donor-by-patient.

**Key point:** Bigger cycles rescue trades that 2-ways miss, but every extra pair adds two simultaneous operating rooms — real programs cap cycles at 2-3.

### Visualization (canvas `c2`, 720×300)

Three pair-boxes arranged in a triangle with green donation arrows running around the circle and one red dashed blocked link.

- **Title (bold 15px, `#1a5276`, top center):** "A 3-Way Cycle When No 2-Way Works (illustrative)".
- **Pair boxes:** rounded rects 170×64, 2px `#e5e9ef` border, fill `#fbfcfd` — Pair 1 centered at (360, 88), Pair 2 at (170, 218), Pair 3 at (550, 218). Inside each: bold 12px ink title "PAIR 1" / "PAIR 2" / "PAIR 3" (top line) and 11px text `#2c3e50` second line "donor A · patient B" / "donor B · patient A" / "donor A · patient A".
- **Cycle arrows (3px green `#008300`, filled arrowheads, gently curved):** Pair 1 → Pair 3 with bold 12px green label "donor 1 → patient 3 (A→A) ✓" beside the midpoint; Pair 3 → Pair 2 with label "donor 3 → patient 2 (A→A) ✓" below the midpoint; Pair 2 → Pair 1 with label "donor 2 → patient 1 (B→B) ✓" beside the midpoint.
- **Blocked link:** 2px red `#e74c3c` dashed line between Pair 1 and Pair 2 midpoints with bold 15px red "✗" at its center and 11px red "positive crossmatch — direct swap dead" alongside.
- **Annotation (bold 12px orange `#d95926`, centered at y=282):** "one circle = 3 transplants = 6 simultaneous surgeries".

## Chains: One Stranger Starts a Cascade

**Tags:** `donor chains` (green), `where it's used` (orange)

- **The altruistic donor** — someone with no attached patient walks in and offers a kidney to the pool
- **The cascade** — they give to pair 1's patient; pair 1's donor gives forward to pair 2, and so on
- **Why no room limit** — each pair receives BEFORE its donor gives; a broken promise strands no one
- **Months, not hours** — segments schedule weeks apart; documented chains exceed 30 transplants
- **The pool problem** — choosing the best mix of cycles and chains is a graph optimization

*Example (italic):* One altruistic donor gives in March; the chain is still adding transplants in November, hospital by hospital.

**Key point:** A cycle needs simultaneity because everyone gives and receives at once; a chain doesn't, because receiving always comes first — that asymmetry is why chains grow so long.

### Visualization (canvas `c3`, 720×300)

A left-to-right cascade: altruistic donor, then five pairs, green arrows carrying the kidney forward, dates showing non-simultaneity.

- **Title (bold 15px, `#1a5276`, top center):** "An Altruistic Donor Starts a Chain (illustrative dates)".
- **Altruistic donor:** aqua `#199e70` filled circle r=16 at (75, 150); bold 12px aqua "altruistic" / "donor" stacked under it (y=182, y=197).
- **Pairs:** five rounded rects 88×56 (2px `#e5e9ef`, fill `#fbfcfd`) centered at x = 190, 300, 410, 520, 630, y=122; inside each, bold 12px ink "Pair 1".."Pair 5" and 11px mute "patient ← / donor →" as two 10-11px lines.
- **Chain arrows:** 3px green `#008300` arrows along y=150: donor(91,150)→(146,150), then from each pair's right edge to the next pair's left edge; filled arrowheads.
- **Dates (bold 12px `#c98500`, centered under each pair at y=210):** "Mar", "Apr", "Jun", "Sep", "Nov".
- **Renege note:** 11px mute `#6b7280` centered at (410, 240): "if a donor backs out, the chain pauses — but every earlier pair already received first".
- **Callout (bold 12px violet `#4a3aa7`, centered at y=280):** "no simultaneity constraint — documented chains exceed 30 transplants".

## Matching Theory With a Body Count

**Tags:** `why it matters` (orange), `market design` (blue)

- **Real scale** — thousands of transplants a year happen through exchange programs; none could be priced
- **Design = matching** — which cycles to pick, how big a cap, how long to let the pool thicken
- **Batching trade-off** — match too often and you waste future 3-cycles; too rarely and patients wait
- **The Nobel** — the 2012 economics prize (Roth & Shapley) honored matching and market design

*Example (italic):* The same graph machinery that pairs candidates with jobs decides which operating rooms open next month.

**Key point:** When society forbids prices, matching IS the allocation mechanism — and its design choices are measured in transplants.

### Visualization (canvas `c4`, 720×300)

A pipeline: the pair pool flows into a matching engine with two design dials, and cycles + chains flow out.

- **Title (bold 15px, `#1a5276`, top center):** "The Exchange Pool as a Matching Pipeline".
- **Pool box:** rounded rect 170×120 at (50, 90); 2px blue `#2a78d6` border, fill `rgba(42,120,214,0.06)`; bold 13px blue "PAIR POOL" centered near the top (y=112); inside, six small magenta `#d55181` circles r=6 in two rows (x = 95/135/175, y = 140/172) and 11px mute "incompatible pairs" centered at y=196.
- **Engine box:** rounded rect 190×120 at (270, 90); 2px ink `#1a5276` border, fill `#fbfcfd`; bold 13px ink "MATCHING ENGINE" centered at y=112; 11px text two lines centered: "max transplants on the" (y=140), "compatibility graph" (y=156); two dial labels in bold 11px orange `#d95926`: "dial: cycle cap 2-3" (y=178), "dial: batch timing" (y=194).
- **Output boxes:** two rounded rects 160×52 at (520, 92) and (520, 156); 2px green `#008300` borders, fill `rgba(0,131,0,0.06)`; bold 12px green centered labels "CYCLES — simultaneous" (y=112) and "CHAINS — sequential" (y=176) with 11px mute second lines "2-3 pairs each" (y=128) and "can run for months" (y=192).
- **Flow arrows:** 3px mute `#6b7280` arrow (220,150)→(270,150); two 3px green arrows from the engine's right edge (460,138)→(520,118) and (460,162)→(520,182), filled arrowheads.
- **Callout (bold 12px red `#e74c3c`, centered at y=272):** "design choices here are measured in transplants, not clicks".

## Regeneration instructions

To rebuild the HTML from this spec:

- **Template:** tutorials detail page (same skeleton as sibling pages, e.g. `09-online-matching.html`): `<h1>Kidney Exchange</h1>`, `.subtitle`, then four `.card-section` blocks, each `h2` + `table.layout` with `td.text-col` (50%) and `td.viz-col` (50%) holding one `<canvas>` (`c1`..`c4`, 720×300).
- **Left column per section:** `.tags` pill row, 4-6 one-line `<li>` bullets each opening with a `<b>bold term</b>`, one italic `.example` line, one `.key-point` callout — verbatim text above.
- **Tag pill colors:** blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Canvas code:** one `setup(id)` helper sizing the backing store to 720×300 × `devicePixelRatio` (rescaled from the displayed width), `width:100%` CSS; palette object `P` with blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`; all data hardcoded (no `Math.random`); charts registered in a `__charts` array and redrawn on window resize.
- **Fonts:** chart titles bold 15px, data/axis labels 12-13px, secondary notes 11px — nothing below 11px.
- **No** cross-page links, nav/back/home elements, or item counts anywhere on the page.
