# Bayes Nets & Graphical Models

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Bayes Nets & Graphical Models

**Subtitle:** Draw variables as nodes and "depends on" as arrows — multiply along the arrows to answer any probability question, and copy the picture over time to get HMMs and CRFs

## Why Is the Grass Wet?

**Tags:** `core idea` (blue), `nodes & arrows` (green), `independence` (orange)

- **The mystery** — the lawn is wet at 7am; the two usual suspects are last night's rain and the sprinkler
- **Nodes** — draw one node per question: "did it rain?", "was the sprinkler on?", "is the grass wet?"
- **Arrows** — point an arrow into a node from what it directly depends on: rain → wet, sprinkler → wet
- **Missing arrows** — no arrow between rain and sprinkler declares them independent of each other
- **The definition** — a Bayes net is exactly this: a directed graph plus one small probability table per node

*Example (italic):* One drawing replaces a 7-number joint table: this net needs only 0.20, 0.30, and four wet-grass rows — six numbers total.

**Key point:** The information is in the missing arrows — every absent arrow is an independence claim, and those claims are what let small per-node tables replace one giant joint table.

### Visualization (canvas `c1`, 720×300)

Node-and-arrow diagram of the three-node wet-grass network with each node's probability table written beside it.

- **Title (bold 15px, `#1a5276`, top center at y=24):** "The Wet-Grass Network: Nodes Are Questions, Arrows Are 'Depends On'".
- **Rain node:** rounded rect 130×42 centered at (170, 95), fill `rgba(42,120,214,0.12)`, 2px border `#2a78d6`, label bold 13px `#2a78d6` "Rain?" centered in the rect.
- **Sprinkler node:** rounded rect 150×42 centered at (550, 95), fill `rgba(0,131,0,0.10)`, 2px border `#008300`, label bold 13px `#008300` "Sprinkler on?".
- **Wet-grass node:** rounded rect 150×42 centered at (360, 210), fill `rgba(25,158,112,0.12)`, 2px border `#199e70`, label bold 13px `#199e70` "Grass wet?".
- **Arrows:** 2.5px `#1a5276` lines with filled triangular arrowheads (8px), from (200, 116) to (320, 190) and from (520, 116) to (400, 190).
- **Node tables (12px `#6b7280`):** "P(rain) = 0.20" centered at (170, 135); "P(on) = 0.30" centered at (550, 135).
- **Wet-grass table (12px `#6b7280`, two centered lines at y=268 and y=284):** "P(wet | both) = 0.99   P(wet | rain only) = 0.90" / "P(wet | sprinkler only) = 0.80   P(wet | neither) = 0.05".
- **Annotation (bold 12px `#d95926`, centered at (360, 58)):** "no arrow here = rain and sprinkler are independent".

## Multiplying Along the Arrows

**Tags:** `worked example` (blue), `rule of thumb` (green)

- **The rule** — the chance of any full story = multiply each node's table entry given its parents
- **One story** — "rain, no sprinkler, wet grass" scores 0.20 × 0.70 × 0.90 = 0.126
- **All four ways** — grass ends up wet via four stories scoring 0.059, 0.126, 0.192, and 0.028
- **Add them** — P(grass wet) = 0.059 + 0.126 + 0.192 + 0.028 = 0.405
- **Biggest cause** — "sprinkler only" (0.192) beats "rain only" (0.126) because the sprinkler runs more often

*Example (italic):* By hand: 0.20×0.30×0.99 = 0.059, 0.20×0.70×0.90 = 0.126, 0.80×0.30×0.80 = 0.192, 0.80×0.70×0.05 = 0.028.

**Key point:** Every probability the net can answer comes from one move — multiply along the arrows for each story, then add up the stories consistent with what you observed.

### Visualization (canvas `c2`, 720×300)

Bar chart of the four ways the grass gets wet, plus a fifth bar for their total, with the sum written out as an annotation.

- **Title (bold 15px, `#1a5276`, top center):** "Four Stories That End in Wet Grass (multiply along the arrows)".
- **Data:** labels `["rain & sprinkler", "rain only", "sprinkler only", "neither", "total: wet"]`; values `[0.059, 0.126, 0.192, 0.028, 0.405]`.
- **Axis:** origin x=70, baseline y=235, chart height 160, plot width 580; y scale 0–0.45 with gridlines `#e5e9ef` and 12px `#6b7280` tick labels at 0, 0.1, 0.2, 0.3, 0.4.
- **Bars:** width 80, evenly spaced across the plot; fills `rgba(42,120,214,0.55)` (blue), `rgba(0,131,0,0.5)` (green), `rgba(217,89,38,0.55)` (orange), `rgba(213,81,129,0.5)` (magenta), `rgba(74,58,167,0.55)` (violet, the total).
- **Value labels:** bold 12px in each bar's solid color, centered above each bar: "0.059", "0.126", "0.192", "0.028", "0.405".
- **Category labels:** 12px `#444` centered below the baseline (wrap "rain & sprinkler" onto two lines).
- **Annotation (bold 13px `#008300`, top area at (400, 55)):** "P(grass wet) = 0.059 + 0.126 + 0.192 + 0.028 = 0.405".

## Copy It Over Days: HMMs and CRFs

**Tags:** `where it's used` (blue), `sequences` (green), `model families` (orange)

- **Repeat it** — copy the rain node once per day and chain them: today's rain depends on yesterday's
- **Hide it** — you never observe the rain directly (you were asleep); you only see the grass each morning
- **That's an HMM** — a hidden chain plus one noisy observation per step is a hidden Markov model
- **That's a CRF** — drop the arrow directions and model labels given observations: a conditional random field
- **Same toolkit** — speech recognition, gene tagging, and typo correction all run on this unrolled picture

*Example (italic):* Four mornings of wet/dry readings let you infer the most likely hidden rain sequence — that inference is the Viterbi algorithm.

**Key point:** HMMs and CRFs are not separate inventions — they are this same node-and-arrow picture copied along a sequence, which is why one inference toolkit covers them all.

### Visualization (canvas `c3`, 720×300)

Two-row unrolled-HMM diagram: a hidden chain of daily rain circles on top, each with a downward arrow to an observed wet-grass square, plus a one-line CRF note.

- **Title (bold 15px, `#1a5276`, top center):** "Copy the Net Over Days and You Get an HMM".
- **Hidden nodes:** circles radius 26 centered at (130, 105), (290, 105), (450, 105), (610, 105); fill `rgba(74,58,167,0.10)`, 2px border `#4a3aa7`; labels bold 12px `#4a3aa7` "Rain d1" … "Rain d4" centered in each circle.
- **Chain arrows:** 2.5px `#4a3aa7` horizontal arrows with filled arrowheads between consecutive circles: (156, 105)→(264, 105), (316, 105)→(424, 105), (476, 105)→(584, 105).
- **Observed nodes:** rounded rects 84×34 centered at (130, 195), (290, 195), (450, 195), (610, 195); fill `rgba(25,158,112,0.18)`, 2px border `#199e70`; labels bold 12px `#199e70` "Wet d1" … "Wet d4".
- **Emission arrows:** 2.5px `#1a5276` vertical arrows with filled arrowheads from (130, 131) to (130, 178), and likewise at x=290, 450, 610.
- **Legend line (12px `#6b7280`, centered at y=248):** "hidden daily rain (never seen) + one wet-grass reading per morning = hidden Markov model".
- **CRF note (bold 12px `#d95926`, centered at y=276):** "a CRF keeps this picture but uses undirected edges and models P(labels | observations) directly".

## Arrows Are Not Proof of Cause

**Tags:** `common mistake` (red), `explaining away` (orange)

- **Not causation** — an arrow fitted from data means "helps predict", not a proven physical cause
- **Prior** — before looking outside, P(rain) = 0.20
- **See wet grass** — P(rain | grass wet) rises to 0.46; wet grass is genuine evidence for rain
- **Then spot the sprinkler on** — P(rain | wet, sprinkler on) falls back to 0.24
- **Explaining away** — confirming one cause weakens the rival cause, though they started independent

*Example (italic):* Detective logic: finding the sprinkler running "explains away" the wet grass, so the case for rain drops from 0.46 to 0.24.

**Common mistake:** Reading arrows as proven causes, and expecting independent causes to stay independent once their shared effect is seen — they start competing the moment you observe the effect.

### Visualization (canvas `c4`, 720×300)

Three-bar chart tracing P(rain) as evidence arrives: prior, after seeing wet grass, and after also seeing the sprinkler on.

- **Title (bold 15px, `#1a5276`, top center):** "Explaining Away: The Case for Rain Rises, Then Falls".
- **Data:** labels `["prior P(rain)", "P(rain | grass wet)", "P(rain | wet, sprinkler on)"]`; values `[0.20, 0.46, 0.24]`.
- **Axis:** origin x=90, baseline y=230, chart height 155, plot width 560; y scale 0–0.5 with gridlines `#e5e9ef` and 12px `#6b7280` tick labels at 0, 0.1, 0.2, 0.3, 0.4, 0.5.
- **Bars:** width 120, centered at x=190, 380, 570; fills `rgba(42,120,214,0.55)` (blue), `rgba(0,131,0,0.5)` (green), `rgba(217,89,38,0.55)` (orange).
- **Value labels:** bold 13px in each bar's solid color above each bar: "0.20", "0.46", "0.24".
- **Category labels:** 12px `#444` centered below the baseline (wrap the third label onto two lines).
- **Curved arrow:** 2px `#d55181` dashed arc from the top of the 0.46 bar to the top of the 0.24 bar with a small arrowhead.
- **Annotation (bold 13px `#d55181`, centered at (400, 55)):** "the sprinkler explains away the rain: 0.46 → 0.24".
- **Caption (12px `#6b7280`, bottom center at y=290):** "all three values computed from the section-2 tables (illustrative network)".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- Diagram canvases (`c1`, `c3`) draw rounded rects/circles with 2px colored borders, translucent fills of the same hue, and filled triangular arrowheads on all arrows; all node positions are the literal coordinates above.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
