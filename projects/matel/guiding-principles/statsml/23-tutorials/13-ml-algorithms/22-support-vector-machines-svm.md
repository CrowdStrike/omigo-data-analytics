# Support Vector Machines (SVM)

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Support Vector Machines (SVM)

**Subtitle:** An SVM separates two groups by drawing the widest possible street between them — and only the few points touching the street's edges decide where it goes

## The Widest Road Between Two Orchards

**Tags:** `core idea` (blue), `max margin` (green), `decision boundary` (orange)

- **The farm** — a field has 6 apple trees near the southwest corner and 6 pear trees near the northeast
- **The job** — the farmer wants one straight road through the field with apples on one side, pears on the other
- **Many roads work** — dozens of straight lines separate the two orchards perfectly on this field
- **SVM's pick** — of all separating lines, choose the one with the widest empty street around it
- **This street** — the widest road here runs corner to corner and is 2.8 m wide, edge to edge
- **Why widest** — a wide buffer means a new tree planted slightly off-pattern still lands on the right side

*Example (italic):* A near-vertical road also splits the orchards, but a tree 30 cm from it flips sides on a breeze — the 2.8 m street shrugs that off.

**Key point:** An SVM does not just separate the two groups — it separates them with the biggest possible safety gap. The middle of the widest street is the decision boundary.

### Visualization (canvas `c1`, 720×300)

Single 2D scatter of the field with two thin "barely legal" candidate roads (gray dashed) and the SVM's wide shaded street.

- **Title (bold 15px, `#1a5276`, top center):** "Many Roads Separate the Orchards — SVM Picks the Widest".
- **Field mapping:** field coords 0–10 on both axes; x_px = 210 + fx×30 (plot 210→510), y_px = 260 − fy×21 (plot 260→50); light `#e5e9ef` 1px border box around the plot area.
- **Apple trees (green `#008300`, 6px dots):** `[[1,2],[2,3],[3,2],[2,5],[4,4],[5,3]]`, labeled "apples" bold 12px green near (1.5,1).
- **Pear trees (violet `#4a3aa7`, 6px dots):** `[[7,5],[6,6],[8,5],[7,7],[9,6],[8,8]]`, labeled "pears" bold 12px violet near (8.7,8.5).
- **Candidate road 1:** gray `#9aa3ad` dashed (dash 5/4) 2px line from field (5.5,0) to (5.5,10).
- **Candidate road 2:** gray `#9aa3ad` dashed 2px line from field (0,9.5) to (10,0.5).
- **SVM street:** band between lines x+y=8 (from (0,8) to (8,0)) and x+y=12 (from (2,10) to (10,2)), fill `rgba(42,120,214,0.15)`; center line x+y=10 (from (0,10) to (10,0)) solid blue `#2a78d6` 3px; street edges thin blue 1px.
- **Annotations (left of plot, x≈15):** orange `#d95926` bold 12px "thin roads: legal but risky" with a short arrow to candidate road 2; blue `#2a78d6` bold 13px "widest street: 2.8 m of buffer" with arrow to the shaded band.
- **Caption (12px `#444`, bottom center):** "field positions in meters (illustrative)".

## Add the Coordinates: A Score You Can Check by Hand

**Tags:** `worked example` (blue), `support vectors` (green)

- **The score** — for any tree at (east, north), compute score = east + north; that one number decides its side
- **Apples score low** — the six apples score 3, 5, 5, 7, 8, 8 — every apple is at or below 8
- **Pears score high** — the six pears score 12, 12, 13, 14, 15, 16 — every pear is at or above 12
- **The street in scores** — the empty gap from 8 to 12 is the street; the boundary sits at score 10
- **Support trees** — the two apples scoring 8 and the two pears scoring 12 touch the edges and hold the street
- **Everyone else** — a tree scoring 3 or 16 could move closer and nothing about the road would change

*Example (italic):* The apple at (4,4) scores 8 and the pear at (6,6) scores 12 — nudge either one and the street's edge moves with it.

**Key point:** The trees sitting exactly on the street's edges are the support vectors. They alone define the boundary; the SVM could forget every other tree and draw the same road.

### Visualization (canvas `c2`, 720×300)

Horizontal number line of the twelve scores with the street shaded between 8 and 12 and the four support trees circled.

- **Title (bold 15px, `#1a5276`, top center):** "Every Tree Reduced to One Score (east + north)".
- **Axis:** horizontal 2px `#999` line at y=170 from x=80 to x=640; score 0–16 maps to x_px = 80 + score×35; ticks with labels 0, 4, 8, 10, 12, 16 (12px `#444`); tick at 10 drawn as ink `#1a5276` dashed vertical line from y=70 to y=220 labeled bold 12px "boundary = 10".
- **Street band:** fill `rgba(42,120,214,0.15)` from score 8 (x=360) to score 12 (x=500), y=90 to y=215; bold 13px blue `#2a78d6` label "the street (scores 8 → 12)" centered above at y=82.
- **Apple dots (green `#008300`, 7px):** scores `[3, 5, 5, 7, 8, 8]`; duplicates stacked vertically 16px apart above the axis line; word "apples" bold 12px green near x=115.
- **Pear dots (violet `#4a3aa7`, 7px):** scores `[12, 12, 13, 14, 15, 16]`; duplicates stacked the same way; word "pears" bold 12px violet near x=615.
- **Support vectors:** the two dots at 8 and the two dots at 12 get orange `#d95926` 11px ring outlines (2.5px stroke); orange bold 13px annotation "these 4 trees hold the edges" centered at y=250 with short arrows up to the rings.
- **Caption (12px `#444`, bottom center):** "no apple above 8, no pear below 12 — the gap between them is the margin".

## A Stray Tree in the Road: The Soft Margin

**Tags:** `soft margin` (blue), `outliers` (orange), `C parameter` (green)

- **The stray** — one odd apple grows at (5.5, 5.5), scoring 11 — inside the street, on the pear half
- **Hard margin** — insisting on a perfectly clean street shrinks it to the 11 → 12 gap, only 0.7 m wide
- **Soft margin** — allow the one stray to sit in the road, pay a penalty, and keep the full 2.8 m street
- **The dial** — the C parameter sets the penalty price: huge C forces the cramped clean road, small C tolerates strays
- **The trade** — one flagged tree buys back four times the buffer for every future tree

*Example (italic):* With a hard margin, the single stray apple at score 11 dictates the whole road; with a soft margin it is one paid exception.

**Key point:** Real data has strays. A soft-margin SVM trades a few in-street violations for a much wider street, and C is the knob that prices each violation.

### Visualization (canvas `c3`, 720×300)

Two stacked score number lines: hard margin (top) shrinks the street to fit the stray, soft margin (bottom) keeps the wide street and flags it.

- **Title (bold 15px, `#1a5276`, top center):** "One Stray Apple at Score 11: Hard Margin vs Soft Margin".
- **Shared mapping:** score 0–16 maps to x_px = 80 + score×35; both axes are 2px `#999` lines from x=80 to x=640 with ticks at 0, 8, 11, 12, 16 (12px `#444`).
- **Dots on both lines:** apples green `#008300` 6px at scores `[3, 5, 5, 7, 8, 8]` (duplicates stacked 12px apart), pears violet `#4a3aa7` 6px at `[12, 12, 13, 14, 15, 16]`, stray apple green 6px at 11 with an orange `#d95926` 10px ring.
- **Top line (hard), axis at y=105:** heading bold 12px `#444` "hard margin: no tree allowed in the street" at y=52; street fill `rgba(217,89,38,0.18)` from score 11 (x=465) to 12 (x=500), y=70 to y=125; orange bold 12px annotation "street squeezed to 0.7 m" with arrow to the narrow band.
- **Bottom line (soft), axis at y=225:** heading bold 12px `#444` "soft margin: one violation allowed, penalty paid" at y=172; street fill `rgba(42,120,214,0.15)` from score 8 (x=360) to 12 (x=500), y=190 to y=245; orange bold 12px label "penalty" with arrow to the ringed stray at 11; blue `#2a78d6` bold 13px annotation "full 2.8 m street kept".
- **Takeaway (bold 13px magenta `#d55181`, bottom center at y=290):** "one flagged stray vs a street four times wider — that trade is the C parameter".

## The Trees Far From the Road Don't Vote

**Tags:** `common mistake` (red), `rule of thumb` (green)

- **The instinct** — people expect the boundary to shift toward wherever most of the data piles up
- **The reality** — an SVM only listens to the edge trees; planting 9 more far from the road changes nothing
- **The test** — add apples deep in the southwest and pears deep in the northeast: the street is identical
- **Contrast** — averages and centroids move with every added point; the max-margin street does not
- **The flip side** — one new tree near or inside the street can move the boundary a lot, so edges deserve scrutiny

*Example (italic):* Doubling the pear orchard deep in the northeast corner leaves the road exactly where the two edge pears at score 12 pinned it.

**Common mistake:** Assuming more data always moves the boundary. Far-away points have zero influence on an SVM — only points on or inside the street matter, which makes edge outliers and mislabels disproportionately powerful.

### Visualization (canvas `c4`, 720×300)

Two side-by-side 2D field panels: the original 12 trees (left) and the same field with 9 far trees added (right), showing an identical street.

- **Title (bold 15px, `#1a5276`, top center):** "Adding 9 Far Trees: The Street Does Not Move".
- **Left panel mapping:** field 0–10 both axes; x_px = 60 + fx×27 (60→330), y_px = 250 − fy×19 (250→60); light `#e5e9ef` 1px border box; heading bold 12px `#444` "original field" centered above at y=48.
- **Right panel mapping:** same, with x_px = 400 + fx×27 (400→670); heading "+9 far trees added".
- **Both panels:** apples green `#008300` 5px dots `[[1,2],[2,3],[3,2],[2,5],[4,4],[5,3]]`; pears violet `#4a3aa7` 5px dots `[[7,5],[6,6],[8,5],[7,7],[9,6],[8,8]]`; street band between x+y=8 and x+y=12 filled `rgba(42,120,214,0.15)` with solid blue `#2a78d6` 2.5px center line x+y=10.
- **Right panel extras:** new apples green hollow 5px circles (2px stroke) `[[0,1],[1,1],[0,3],[1,0],[2,1]]`; new pears violet hollow circles `[[9,9],[10,8],[9,10],[10,10]]`.
- **Right panel annotation:** green bold 13px `#008300` "same street — far trees don't vote" placed above the band near field (2.2,9.3), with a short arrow to the center line.
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=365 from y=38 to h-12.
- **Caption (12px `#444`, bottom center):** "the four edge trees still pin the road (illustrative)".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- All scatter/dot data is hardcoded literal arrays as specified above — no `Math.random()`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
