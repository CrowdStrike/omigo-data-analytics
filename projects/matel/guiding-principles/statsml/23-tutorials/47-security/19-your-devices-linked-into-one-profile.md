# Your Devices, Linked Into One Profile

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Your Devices, Linked Into One Profile

**Subtitle:** A shared address is enough to tie the TV, the phone, and the laptop into one household — nobody has to log in, because the link is inferred

## An Ad on the TV, Then the Same Ad on the Phone

**Tags:** `core idea` (blue), `household graph` (orange), `cross-device linking` (green)

- **The observation** — an ad for a garden bench plays on a living-room TV on Tuesday evening
- **The echo** — on Wednesday the same household's phone shows an ad for that same bench
- **No login** — nobody signed into anything that both devices share, so no account tied them together
- **The shared clue** — both devices reached the internet through the same household public address, at the same evening hours
- **The name for it** — a table of devices with edges meaning "probably the same person or household" is an *identity graph*
- **Two edge types** — a *deterministic* edge comes from one account signed in on both devices; a *probabilistic* edge is inferred from co-occurring signals
- **The commercial reason** — connecting a TV exposure to a phone purchase is *attribution*, and attribution is what makes the graph sellable
- **Granularity mismatch** — consent is normally collected per app or per device, while the linking happens across them

*Example (italic):* The TV and the phone were joined because they appeared behind one public address during the same evening window — an inference, not a signed-in fact (illustrative).

**Key point:** An identity graph edge is usually an inference, not a record — the TV and the phone were linked by co-occurrence, and co-occurrence is evidence, not proof.

### Visualization (canvas `c1`, 720×300)

Node-and-edge diagram: four devices behind one household address, one solid deterministic edge, two dashed probabilistic edges, and one dashed wrong edge reaching a neighbour's device.

- **Title (bold 15px, `#1a5276`, top center at y=24):** "One Solid Edge, Three Inferred Ones — and One Is Wrong".
- **Household plate:** rounded rect x=40, y=52, 430×196, radius 10, fill `rgba(42,120,214,0.06)`, 2px `#2a78d6` border; 12px `#6b7280` label "one household public address" at (255, 70), centered.
- **Neighbour plate:** rounded rect x=510, y=120, 170×90, radius 10, fill `rgba(217,89,38,0.07)`, 2px dashed `#d95926` border (dash 6/4); 12px `#d95926` label "co-tenant, same address" at (595, 138), centered.
- **Device nodes (circles radius 26, 2px border, 12px `#2c3e50` label centered *below* each circle at cy+42):** "laptop" at (110, 110) fill `rgba(42,120,214,0.18)` border `#2a78d6`; "phone" at (300, 100) fill `rgba(42,120,214,0.18)` border `#2a78d6`; "TV" at (150, 205) fill `rgba(74,58,167,0.16)` border `#4a3aa7`; "tablet" at (370, 200) fill `rgba(25,158,112,0.16)` border `#199e70`; neighbour "laptop" at (595, 178) fill `rgba(217,89,38,0.16)` border `#d95926`.
- **Deterministic edge:** solid 3px `#008300` line laptop(110,110) → phone(300,100), drawn behind nodes; bold 12px `#008300` label "signed in on both" at midpoint, centered at (205, 82).
- **Probabilistic edges (2.5px dashed `#2a78d6`, dash 7/5):** phone(300,100) → TV(150,205) and TV(150,205) → tablet(370,200); 12px `#2a78d6` labels "inferred" at (215, 165) and "inferred" at (258, 226), centered.
- **Wrong edge:** 2.5px dashed `#d95926` (dash 7/5) from tablet(370,200) to neighbour laptop(595,178); bold 12px `#d95926` label "inferred — false link" centered at (482, 168) with a small `#d95926` cross (two 10px strokes, 2.5px) drawn at the edge midpoint (482, 189).
- **Legend (bottom left, 12px):** at (52, 272) a 22px solid `#008300` swatch line + `#2c3e50` text "deterministic (login)"; at (250, 272) a 22px dashed `#2a78d6` swatch line + text "probabilistic (inferred)".
- **Caption (12px `#444`, bottom right at (w-12, h-10)):** "illustrative graph".

## Counting the Wrong Links: 10,000 Candidate Pairs

**Tags:** `worked example` (blue), `probabilistic identity` (orange), `precision` (green)

- **The setup** — a graph builder proposes a link whenever two devices share a public address and overlap in activity
- **The sample** — 10,000 candidate device pairs; 6,000 really are one household, 4,000 are not
- **Why the 4,000 exist** — co-tenants in one building, shared office networks, carrier-shared addresses, and visiting guests
- **Sensitivity** — the builder links 5,400 of the 6,000 true pairs, so 5,400 / 6,000 = 90%
- **False-positive rate** — it also links 800 of the 4,000 non-household pairs, so 800 / 4,000 = 20%
- **Total links asserted** — 5,400 + 800 = 6,200 edges are written into the graph as "same household"
- **Precision** — 5,400 / 6,200 = 87.1%, so 800 / 6,200 = 12.9% of asserted links are wrong, roughly 1 in 8
- **What that means** — the graph does not say "probably"; it ships 6,200 flat assertions, 800 of which are false

*Example (italic):* Of 6,200 household edges the builder writes, about 800 join devices that were never in the same home (all counts illustrative).

**Key point:** A household link is a classifier output, so it has a precision — here 5,400 / 6,200 = 87.1%, meaning about one asserted household in eight is not a household at all.

### Visualization (canvas `c2`, 720×300)

2×2 confusion matrix for the 10,000 candidate pairs, with sensitivity, false-positive rate, and precision computed in JS from the four cell counts at render time.

- **Data (hardcoded literals):** `TP = 5400`, `FN = 600`, `FP = 800`, `TN = 3200`. Row totals `TP+FN = 6000` and `FP+TN = 4000`; column total `TP+FP = 6200`. All printed percentages computed from these variables (`TP/(TP+FN)`, `FP/(FP+TN)`, `TP/(TP+FP)`), formatted with `toFixed(1)`.
- **Title (bold 15px, `#1a5276`, top center at y=24):** "10,000 Candidate Pairs: Where the 6,200 Asserted Links Come From".
- **Grid:** 2×2 of 150×70 cells with top-left corner at x=250, y=80, no gaps; each cell 1.5px `#e5e9ef` internal borders and a 2px `#6b7280` outer border.
- **Column headers (bold 12px `#2c3e50`, centered above the grid at y=70):** "linked by builder" at x=325, "not linked" at x=475.
- **Row labels (12px `#2c3e50`, right-aligned ending at x=240, vertically centered in each row):** "really one household" at y=119, "not one household" at y=189; add a 12px `#6b7280` second line under each with the row total: "6,000 pairs" at y=136 and "4,000 pairs" at y=206.
- **Cell fills and bold 16px value text (centered in cell):** TP cell (250,80) fill `rgba(0,131,0,0.16)`, text `#008300` "5,400"; FN cell (400,80) fill `rgba(107,114,128,0.10)`, text `#6b7280` "600"; FP cell (250,150) fill `rgba(217,89,38,0.20)`, text `#d95926` "800"; TN cell (400,150) fill `rgba(107,114,128,0.10)`, text `#6b7280` "3,200".
- **Cell sub-labels (11px, centered, 20px below each value):** "true link" (`#008300`), "missed" (`#6b7280`), "FALSE LINK" (`#d95926`), "correctly skipped" (`#6b7280`).
- **Left stats block (12px `#2c3e50`, left-aligned at x=40, lines at y=105/125/145):** "sensitivity", "5,400 / 6,000", and the computed value in bold `#008300` — e.g. "= 90.0%".
- **Right stats block (left-aligned at x=560, lines at y=105/125/145):** "false-positive rate", "800 / 4,000", computed bold `#d95926` "= 20.0%".
- **Precision callout (below grid):** 12px `#2c3e50` "precision = 5,400 / 6,200" centered at (400, 250); bold 16px `#1a5276` computed value, e.g. "= 87.1%", centered at (400, 272).
- **Annotation (bold 13px `#d95926`, centered at (400, 292)):** computed "about 1 in 8 asserted links is wrong" using `(TP+FP)/FP` rounded to the nearest whole number.
- **Caption (12px `#444`, top right at (w-12, 24) — kept clear of the title):** "counts illustrative".

## Same Classifier, Denser Building: The Base Rate Decides

**Tags:** `base rate` (orange), `where it's used` (blue), `defensive` (green)

- **Change only the population** — in a dense apartment block, 2,000 of 10,000 candidate pairs are true households and 8,000 are not
- **Same 90% sensitivity** — the builder finds 90% × 2,000 = 1,800 of the true households, exactly as before
- **Same 20% false-positive rate** — it also links 20% × 8,000 = 1,600 non-household pairs
- **New precision** — 1,800 / (1,800 + 1,600) = 1,800 / 3,400 = 52.9%, down from 87.1%
- **Name it** — the classifier did not change; the *base rate* of true households did, and precision follows the base rate
- **Asymmetric cost** — a wrong ad is cheap, but the same graph reused for fraud scoring, credit decisions, or law-enforcement requests is not
- **Accuracy travels** — a graph built to ad-grade precision keeps that precision when a buyer applies it to a higher-stakes decision
- **What helps** — separate networks, resetting or disabling per-device ad identifiers, turning off content recognition, and rules treating an inferred household as personal data

*Example (italic):* The same builder is 87.1% precise in low-density housing and 52.9% precise in a dense block — one number cannot describe both (illustrative).

**Key point:** Precision is a property of the classifier *and* the population it runs on — in denser housing the identical matcher drops from 87.1% to 52.9%, and nothing about the code changed.

### Visualization (canvas `c3`, 720×300)

Two stacked bars comparing the same classifier on two populations; each bar splits asserted links into true and false, with precision computed at render time.

- **Data (hardcoded literals):** population A `{ trueLinks: 5400, falseLinks: 800 }` (from 6,000 true / 4,000 false pairs); population B `{ trueLinks: 1800, falseLinks: 1600 }` (from 2,000 true / 8,000 false pairs). Precision computed as `trueLinks / (trueLinks + falseLinks)` per bar, `toFixed(1)`.
- **Title (bold 15px, `#1a5276`, top center at y=24):** "Identical Classifier, Two Populations: 87.1% vs 52.9% Precision".
- **Axes:** origin x=90, baseline y=240, plot width 560, plot height 165; y scale 0 to 7,000 asserted links; gridlines `#e5e9ef` at 2,000 / 4,000 / 6,000 with 12px `#444` right-aligned tick labels ending at x=84; 2px `#999` baseline.
- **Bars (100px wide, centered at x=250 and x=490), stacked bottom-up:** true-link segment fill `rgba(0,131,0,0.30)` with 2px `#008300` border, false-link segment above it fill `rgba(217,89,38,0.30)` with 2px `#d95926` border; segment pixel heights computed as `count / 7000 * 165`.
- **Segment value labels (bold 12px, centered inside each segment):** `#008300` "5,400" and `#d95926` "800" on bar A; `#008300` "1,800" and `#d95926` "1,600" on bar B.
- **Bar captions (12px `#444`, centered below the baseline):** "low-density housing" at y=258 and "6,000 of 10,000 pairs true" at y=274 under bar A; "dense apartment block" at y=258 and "2,000 of 10,000 pairs true" at y=274 under bar B.
- **Precision labels (bold 15px, centered 12px above each bar top):** `#008300` "precision 87.1%" over bar A; `#d95926` "precision 52.9%" over bar B — both strings built from the computed values, not typed.
- **Annotation (bold 13px `#4a3aa7`, centered at (370, 56)):** "same 90% sensitivity, same 20% false-positive rate".
- **Legend (12px, top left at x=40, y=90 and y=108):** 12×12 swatches `rgba(0,131,0,0.30)`/`#008300` "true links" and `rgba(217,89,38,0.30)`/`#d95926` "false links".
- **Caption (12px `#444`, bottom right at (w-12, h-6)):** "counts illustrative".

## "It Can't Be Me, I Never Logged In"

**Tags:** `common mistake` (red), `probabilistic identity` (orange)

- **The first error** — assuming a link needs a login, so an unauthenticated device feels anonymous
- **Why it fails** — co-occurrence alone supports a probabilistic edge; a shared address and a shared evening pattern are enough
- **The mirror error** — a buyer treats the purchased graph as ground truth about who lives where
- **What it actually is** — a classifier output with a measurable error rate that no downstream row exposes
- **No error bars ship** — the edge arrives as "same household", not "same household, 87.1% precise in low-density housing"
- **One documented mechanism** — smart-TV content recognition matches on-screen frames to a reference library, turning viewing into events
- **Usually a setting** — that recognition feature is typically disclosed and switchable, and its defaults vary by device maker and jurisdiction
- **The question to ask** — of any identity graph: what is its precision, and in which population was it measured?

*Example (italic):* A neighbour's evening browsing shapes Alice's ads because one inferred edge crossed an apartment wall (illustrative).

**Common mistake:** Treating "I never logged in" as anonymity, and treating a bought identity graph as fact. Both are the same error at different ends: an inferred edge is a prediction, and every prediction has a precision that belongs in the conversation.

### Visualization (canvas `c4`, 720×300)

Flow schematic: a graph builder's measured precision is attached to its output, and that measurement is dropped at each hand-off until a high-stakes decision consumes a bare assertion.

- **Title (bold 15px, `#1a5276`, top center at y=24):** "The Error Rate Is Measured Once, Then Never Travels".
- **Box style:** rounded rects, radius 8, 2px border, centered 12px `#2c3e50` text (two lines where noted, 18px line spacing).
- **Box 1 (x=30, y=95, 190×62):** fill `rgba(42,120,214,0.14)`, border `#2a78d6`, lines "graph builder" / "measures precision".
- **Precision tag (below box 1):** bold 13px `#008300` "87.1% (low-density)" centered at (125, 180); 12px `#6b7280` "measured, in-house" at (125, 200).
- **Box 2 (x=265, y=95, 190×62):** fill `rgba(107,114,128,0.12)`, border `#6b7280`, lines "graph sold as rows" / "device A = device B".
- **Dropped tag (below box 2):** bold 13px `#d95926` "no precision field" centered at (360, 180); 12px `#6b7280` "assertion only" at (360, 200).
- **Box 3 (x=500, y=95, 190×62):** fill `rgba(217,89,38,0.16)`, border `#d95926`, lines "buyer's decision" / "ads, or fraud scoring".
- **Arrows:** 3px `#6b7280` horizontal arrows from box 1 right edge (220, 126) to (261, 126) and from box 2 right edge (455, 126) to (496, 126), each with a filled triangular head (size 10).
- **Cost row (12px, centered under box 3):** `#008300` "wrong ad: cheap" at (595, 180) and bold 12px `#d95926` "wrong fraud flag: not cheap" at (595, 200).
- **Annotation (bold 13px `#4a3aa7`, centered at (360, 250)):** "ad-grade accuracy, reused for a higher-stakes call".
- **Divider:** 1px `#e5e9ef` horizontal line from x=30 to x=690 at y=222.
- **Caption (12px `#444`, bottom right at (w-12, h-10)):** "schematic; precision figure illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms). Shared helpers: `roundBoxTL` (rounded rect with fill + 2px stroke) and `arrowHead`.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`. Red is reserved for genuine alarm; false links use orange `#d95926`.
- **Data:** no randomness anywhere — every count is a hardcoded literal. The two populations are `6,000 true / 4,000 false` and `2,000 true / 8,000 false` out of 10,000 candidate pairs; the classifier is fixed at 90% sensitivity and a 20% false-positive rate, giving `5,400 + 800 = 6,200` links at `5,400/6,200 = 87.1%` precision and `1,800 + 1,600 = 3,400` links at `1,800/3,400 = 52.9%` precision. Confusion-matrix cells must sum to their stated row totals (5,400+600 = 6,000; 800+3,200 = 4,000) and every printed percentage is computed in JS from the cell literals via `toFixed(1)` — never typed as a string. All figures are invented and labeled illustrative; text numbers must match chart numbers to the digit.
- **Framing:** neutral and mechanism-only. No real company, platform, ad network, data broker, or device maker is named — only "an advertising platform", "a graph builder", "a data buyer", "a device maker". Nothing undisclosed is alleged: the described mechanisms are shared-address inference, per-device advertising identifiers, and on-screen content recognition as a typically disclosed and switchable setting whose defaults vary by vendor and jurisdiction. No credential strings, no realistic addresses or device identifiers — devices are named "laptop", "phone", "TV", "tablet"; people are Alice and Bob. No accuracy figure is presented as a measurement of any real graph.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
