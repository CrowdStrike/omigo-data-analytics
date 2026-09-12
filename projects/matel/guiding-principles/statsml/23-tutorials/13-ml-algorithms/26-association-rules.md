# Association Rules

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Association Rules: Support, Confidence & Lift

**Subtitle:** Count how often items land in the same basket — support says how common a pair is, confidence says how reliable, and lift says whether it beats chance

## One Week of Receipts, One Odd Pair

**Tags:** `core idea` (blue), `market baskets` (green), `diapers & beer` (orange)

- **The store** — a corner grocery logs 200 receipts in one week, each listing what a basket held
- **The counts** — diapers appear in 40 baskets, beer in 50, and 30 baskets contain both items
- **The surprise** — 30 of the 40 diaper baskets also hold beer, far more overlap than chance
- **The rule** — we write it "diapers → beer": baskets with diapers tend to also contain beer
- **The name** — mining receipts for these if-this-then-that pairs is association rule mining

*Example (italic):* In our week of receipts, 3 out of every 4 diaper baskets also ring up beer — the classic diapers-and-beer pattern.

**Key point:** An association rule is just a counted pattern in baskets — "when A is in the cart, B tends to be too." No model, no fitting, only counting.

### Visualization (canvas `c1`, 720×300)

Dot grid of all 200 baskets (20 columns × 10 rows), each dot one basket, colored by whether it holds diapers, beer, both, or neither, with a legend on the right.

- **Title (bold 15px, `#1a5276`, top center):** "One Week of Receipts: 200 Baskets".
- **Data (200 dots in reading order):** first 30 violet `#4a3aa7` (diapers + beer), next 10 blue `#2a78d6` (diapers only), next 20 orange `#d95926` (beer only), last 140 grid-grey `#e5e9ef` with 1px `#d0d5dc` stroke (neither).
- **Grid:** origin x=55, y=60; 20 columns × 10 rows; horizontal pitch 24px, vertical pitch 21px; circles radius 7px.
- **Legend (x=560, starting y=75, one swatch per 30px):** 7px dots + 12px `#444` labels: "both: 30" (violet), "diapers only: 10" (blue), "beer only: 20" (orange), "neither: 140" (grey).
- **Annotation (bold 13px violet `#4a3aa7`, below grid at y=288, left-aligned x=55):** "30 of the 40 diaper baskets also hold beer".
- **Caption (12px `#444`, legend column bottom y=230, wrapped two lines):** "each dot =" / "one basket".

## Three Numbers from Four Counts

**Tags:** `worked example` (blue), `support` (green), `confidence` (green), `lift` (orange)

- **Support** — the pair {diapers, beer} sits in 30 of 200 baskets, so support = 30/200 = 15%
- **Confidence** — of the 40 diaper baskets, 30 also hold beer, so confidence = 30/40 = 75%
- **Baseline** — beer on its own is in 50 of 200 baskets, so a random basket has beer 25% of the time
- **Lift** — confidence over baseline: 75% / 25% = 3.0, so diapers triple the usual beer rate
- **Reading lift** — lift above 1 means the pair travels together; lift of exactly 1 means unrelated

*Example (italic):* Support 15%, confidence 75%, lift 3.0 — all three fall out of four counts: 200 baskets, 40 diapers, 50 beer, 30 both.

**Key point:** Support asks "how common is the pair?", confidence asks "given A, how often B?", and lift asks "is that more than chance?" — always compute all three.

### Visualization (canvas `c2`, 720×300)

Four horizontal count bars (all baskets, diapers, beer, both) drawn to a shared scale, with the three formulas spelled out beneath.

- **Title (bold 15px, `#1a5276`, top center):** "From Basket Counts to Support, Confidence, Lift".
- **Data:** counts `[200, 40, 50, 30]` with labels "all baskets", "diapers", "beer", "diapers + beer".
- **Bars:** left edge x=175, scale 360px = 200 baskets (1.8 px per basket), so widths `[360, 72, 90, 54]`; rows at y=58, 102, 146, 190; height 24px; fills: all-baskets `#e5e9ef` with 1px `#6b7280` border, diapers `rgba(42,120,214,0.55)`, beer `rgba(217,89,38,0.55)`, both `rgba(74,58,167,0.55)`.
- **Labels:** row names 12px `#444` right-aligned at x=165; count values bold 12px in matching bar color, 8px right of each bar end ("200", "40", "50", "30").
- **Formulas (bold 13px, one per third at y=262, centered at x=130 / x=360 / x=590):** blue `#2a78d6` "support = 30/200 = 15%"; green `#008300` "confidence = 30/40 = 75%"; magenta `#d55181` "lift = 75% / 25% = 3.0".
- **Caption (11px `#444`, centered at y=285):** "25% = beer's base rate (50 of 200 baskets)".

## The Milk Trap: High Confidence, Zero Signal

**Tags:** `common mistake` (red), `lift` (orange)

- **The trap rule** — "diapers → milk" has confidence 80%: 32 of the 40 diaper baskets hold milk
- **Looks stronger** — 80% beats the beer rule's 75%, so ranking by confidence puts milk on top
- **The catch** — milk is in 160 of all 200 baskets, so ANY basket holds milk 80% of the time
- **Lift exposes it** — 80% / 80% = 1.0: knowing a basket has diapers tells you nothing about milk
- **The rescue** — the beer rule keeps its lift of 75% / 25% = 3.0; lift, not confidence, finds real pairs

*Example (italic):* A naive report crowns "diapers → milk (80%)" the top rule, yet moving milk next to diapers would change nothing.

**Common mistake:** Ranking rules by confidence alone — popular items like milk score high confidence in every rule while carrying zero information. Always divide by the item's base rate.

### Visualization (canvas `c3`, 720×300)

Dual-panel bar comparison: each panel shows a rule's confidence bar next to the target item's baseline bar, split by a vertical dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "Confidence Alone vs Confidence Against the Baseline".
- **Left panel (diapers → beer):** axis origin x=70, width 250, baseline y=235, chart height 160, y scale 0–100%; two vertical bars 60px wide at x=105 and x=205: confidence 75% fill `rgba(0,131,0,0.5)`, baseline 25% fill `rgba(107,114,128,0.35)`; value labels bold 12px above bars ("75%", "25%"); labels 12px `#444` below ("confidence", "beer base"); green `#008300` bold 13px annotation above at y=52: "lift = 75/25 = 3.0"; caption 12px `#444` at y=280: "real signal".
- **Right panel (diapers → milk):** axis origin x=430, width 250, same baseline/height/scale; bars at x=465 and x=565: confidence 80% fill `rgba(213,81,129,0.5)`, baseline 80% fill `rgba(107,114,128,0.35)`; value labels "80%", "80%"; labels "confidence", "milk base"; magenta `#d55181` bold 13px annotation at y=52: "lift = 80/80 = 1.0"; caption "popularity, not signal".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.

## Screening and Ranking a Rule List

**Tags:** `where it's used` (blue), `rule of thumb` (green)

- **Recommenders** — "customers who bought X also bought Y" widgets are ranked association rules
- **Shelf & bundles** — high-lift pairs guide store layout, cross-sell emails, and bundle pricing
- **Support filter** — demand a minimum support first; a pair seen in only 2 baskets is noise
- **Lift below 1** — "beer → milk" scores lift 0.75, so beer baskets hold milk LESS than average
- **The algorithm** — Apriori scans baskets for frequent pairs, then keeps rules passing both bars

*Example (italic):* From the same 200 receipts, chips → salsa scores lift 1.8 while bread → milk manages only 1.04 — the shelf-move budget goes to chips and salsa.

**Key point:** Screen with support (common enough to trust), then rank with lift (beats chance) — confidence alone just rewards whatever item is popular.

### Visualization (canvas `c4`, 720×300)

Horizontal bar chart ranking five rules from the same 200 receipts by lift, with a dashed vertical reference line at lift = 1.

- **Title (bold 15px, `#1a5276`, top center):** "Five Rules from the Same 200 Receipts, Ranked by Lift".
- **Data:** rules and lifts: "diapers → beer" 3.0, "chips → salsa" 1.8, "bread → milk" 1.04, "diapers → milk" 1.0, "beer → milk" 0.75.
- **Layout:** rule labels 12px `#444` right-aligned at x=160; bars start x=170, scale 162.5px per lift unit (max 3.2 → 520px), so widths `[487, 292, 169, 162, 122]`; rows at y=64, 106, 148, 190, 232; height 24px.
- **Bar fills:** 3.0 `rgba(0,131,0,0.55)`, 1.8 `rgba(42,120,214,0.55)`, 1.04 `rgba(201,133,0,0.5)`, 1.0 `rgba(107,114,128,0.35)`, 0.75 `rgba(217,89,38,0.55)`.
- **Value labels:** bold 12px in matching bar color, 8px right of each bar end ("3.0", "1.8", "1.04", "1.0", "0.75").
- **Reference line:** dashed `#1a5276` (dash 4/3) vertical at x=333 (lift = 1) from y=50 to y=262; bold 12px `#1a5276` label "lift = 1: no association" above it at y=44.
- **Annotations:** white bold 13px right-aligned inside the right end of the top bar: "worth acting on"; orange `#d95926` bold 12px right of the bottom bar: "below 1: beer baskets avoid milk".
- **Caption (11px `#444`, centered at y=290):** "from fixed counts: chips 50, salsa 40, both 18; bread 120, milk 160, both 100; beer+milk 30".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- All data is hardcoded (no `Math.random()`); every support/confidence/lift value derives exactly from the fixed basket counts (200 baskets; diapers 40, beer 50, both 30; milk 160, diapers+milk 32, bread 120, bread+milk 100, beer+milk 30; chips 50, salsa 40, both 18).
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
