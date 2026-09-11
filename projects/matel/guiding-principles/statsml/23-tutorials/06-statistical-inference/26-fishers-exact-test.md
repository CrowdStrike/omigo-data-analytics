# Fisher's Exact Test

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Fisher's Exact Test

**Subtitle:** When counts are tiny, don't approximate — list every possible outcome and count the extreme ones; the p-value becomes a fraction you can verify by hand

## A Lady Says She Can Taste the Difference

**Tags:** `core idea` (blue), `tiny counts` (orange), `exact p-value` (green)

- **The claim** — at a 1920s tea party, a lady insists she can taste whether milk was poured first
- **The test** — Fisher shuffles 8 cups, 4 of each kind; she must point out the 4 milk-first cups
- **Her result** — she names all 4 milk-first cups correctly; is that skill, or a lucky guess?
- **Count everything** — there are exactly 70 ways to pick 4 cups from 8; only 1 gets all 4 right
- **Exact p** — a guesser scores 4-of-4 with probability 1/70 ≈ 0.014 — counted, not approximated

*Example (italic):* A random guesser matches the perfect pick in only 1 arrangement out of 70 — so her feat carries p = 1/70 ≈ 0.014.

**Key point:** Fisher's exact test lists every outcome possible with the given totals and counts the fraction at least as extreme as the observed one. With tiny counts you count — no approximation needed.

### Visualization (canvas `c1`, 720×300)

Row of 8 cups showing the true pouring order, the lady's four picks circled in green, and the 1-in-70 arithmetic spelled out below.

- **Title (bold 15px, `#1a5276`, top center):** "8 Cups, 4 Milk-First: Only 1 of 70 Picks Is Perfect".
- **Data:** true order for cups 1–8 hardcoded `['M','T','T','M','M','T','M','T']`; her picks = cups 1, 4, 5, 7 (all correct).
- **Cups:** 8 circles radius 22 centered at y=120, x = 90 + i×77 (i = 0..7); milk-first fill `rgba(42,120,214,0.45)` stroke blue `#2a78d6`, tea-first fill `rgba(217,89,38,0.35)` stroke orange `#d95926`; bold 13px ink letter "M" or "T" centered in each; cup number 12px `#6b7280` above at y=88.
- **Picks:** green `#008300` 3px ring (radius 28) around cups 1, 4, 5, 7, plus a bold 12px green "✓" below each at y=160.
- **Legend (12px `#444`, y=190):** blue swatch "milk poured first", orange swatch "tea poured first", green ring "her 4 picks".
- **Arithmetic:** bold 13px ink `#1a5276` centered at y=228 "ways to choose 4 cups from 8: C(8,4) = 70"; bold 14px green `#008300` centered at y=252 "only 1 pick matches all four → p = 1/70 ≈ 0.014".
- **Caption (12px `#444`, bottom center, y=282):** "shuffle order illustrative — the test depends only on the counts".

## Counting All 70 Guesses by Hand

**Tags:** `worked example` (blue), `hypergeometric` (green)

- **The pieces** — getting k true milk-first cups can happen in C(4,k) × C(4,4−k) different ways
- **The counts** — 0, 1, 2, 3, 4 correct occur in 1, 16, 36, 16, 1 ways — and 1+16+36+16+1 = 70
- **Most likely** — pure guessing lands on exactly 2 right more than half the time: 36/70 ≈ 0.514
- **Her score** — 4-of-4 is the rarest possible outcome: just 1 of the 70 guesses, p = 1/70 ≈ 0.014
- **Near miss** — 3-or-more right happens by pure luck in 17 of the 70 guesses: p ≈ 0.243

*Example (italic):* Check one cell yourself: 2 right and 2 wrong = C(4,2) × C(4,2) = 6 × 6 = 36 of the 70 arrangements.

**Key point:** This bar-by-bar enumeration is the hypergeometric distribution — Fisher's exact test is nothing more than reading a tail off it.

### Visualization (canvas `c2`, 720×300)

Bar chart of the exact null distribution: number of ways a pure guesser gets k milk-first cups right, with her perfect score highlighted.

- **Title (bold 15px, `#1a5276`, top center):** "If She Guessed: Ways to Get k Cups Right (out of 70)".
- **Data:** k = `[0, 1, 2, 3, 4]`; ways = `[1, 16, 36, 16, 1]`; probability labels "1/70 ≈ 0.014", "16/70 ≈ 0.229", "36/70 ≈ 0.514", "16/70 ≈ 0.229", "1/70 ≈ 0.014".
- **Axes:** origin x=70, baseline y=235, chart width 600, height 170; y scale 0–40 with gridlines at 10, 20, 30, 40 in `#e5e9ef` and 12px `#6b7280` labels.
- **Bars:** width 74, centered at x = 130 + k×120; k = 0..3 fill `rgba(42,120,214,0.45)` stroke blue `#2a78d6`; k = 4 fill `rgba(0,131,0,0.4)` stroke green `#008300` (her score); ways count bold 13px `#2c3e50` above each bar; labels "0 right" … "4 right" 12px `#444` below baseline.
- **Annotations:** green `#008300` bold 13px centered above the k=4 bar (below the bracket) with a short vertical arrow down to it: "her result: 1 way in 70"; magenta `#d55181` bold 12px bracket spanning the k=3 and k=4 bars, label above: "3 or more right: 17/70 ≈ 0.243".
- **Caption (12px `#444`, bottom):** "hypergeometric counts: C(4,k) × C(4,4−k)".

## Ten Tasters per Brew: Where Chi-Square Breaks

**Tags:** `where it's used` (blue), `small cells` (orange), `failure mode` (red)

- **New setting** — the tea house tests a new brew: 10 tasters try each version, "would you reorder?"
- **The data** — old brew: 3 of 10 would reorder; new brew: 8 of 10 — a 2×2 table with tiny cells
- **The rule** — chi-square wants every expected cell count ≥ 5; here two expected cells are 4.5
- **Two answers** — chi-square gives p ≈ 0.025 (looks significant); Fisher's exact gives p ≈ 0.070
- **Why** — chi-square leans on a smooth curve that fits badly at n = 20; Fisher counts real tables

*Example (italic):* Trusting the shortcut ships the new brew at p ≈ 0.025; counting exactly says the evidence is weaker — p ≈ 0.070.

**Key point:** On small 2×2 tables the chi-square approximation can overstate significance. Fisher's exact test counts actual tables, so it stays valid — it is the default when expected cells drop below ~5.

### Visualization (canvas `c3`, 720×300)

Two-panel chart: the 2×2 taste-test table on the left, and the two competing p-values as horizontal bars against a 0.05 line on the right, split by a dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "Ten Tasters per Brew: Chi-Square vs Fisher on the Same Table".
- **Left panel (table):** grid lines 1px `#bdc3c7` from x=60 to x=320, row lines at y=75, 115, 155, 195; column headers "reorder" and "no" bold 12px ink `#1a5276`; row labels "old brew", "new brew" bold 12px ink; cell values old = 3, 7 and new = 8, 2 in bold 16px `#2c3e50`, centered; margin totals 11px `#6b7280`: rows 10, 10 and columns 11, 9; caption 12px `#444` at y=225: "expected cells: 5.5 and 4.5 — below the ≥5 rule".
- **Right panel (p bars):** scale 0–0.10 mapped over 240px starting x=430; baseline 2px `#999` at y=200 with ticks "0", "0.05", "0.10" (12px `#6b7280`); chi-square bar value 0.025, 18px tall at y=100, fill `rgba(217,89,38,0.55)`, label bold 12px orange `#d95926` "chi-square p ≈ 0.025"; Fisher bar value 0.070 at y=150, fill `rgba(0,131,0,0.4)`, label bold 12px green `#008300` "Fisher exact p ≈ 0.070"; dashed magenta `#d55181` (dash 4/3) vertical line at the 0.05 position from y=80 to y=200, bold 12px magenta "0.05" above it.
- **Annotation (magenta `#d55181` bold 12px, two lines, centered under the bars at y=240):** "the shortcut crosses 0.05," / "the exact count does not".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.
- **Caption (12px `#444`, bottom right):** "chi-square without continuity correction".

## Exact Means Exact, Not Powerful

**Tags:** `common mistake` (red), `rule of thumb` (green)

- **The word** — "exact" says the p-value has no approximation error, not that the verdict is certain
- **The floor** — 8 cups cap the evidence: a perfect 4-of-4 can never beat p = 1/70 ≈ 0.014
- **Fewer cups** — with 6 cups the best possible p is 1/20 = 0.050; with 4 cups only 1/6 ≈ 0.167
- **Near miss** — 3-of-4 gives p ≈ 0.243, so genuine skill can easily fail to show at this size
- **The fix** — add cups, not tests: 10 cups (5 + 5) push the best possible p to 1/252 ≈ 0.004

*Example (italic):* With only 4 cups, even a flawless performance yields p = 1/6 ≈ 0.167 — that design cannot reach 0.05 at all.

**Common mistake:** Reaching for Fisher's exact test to rescue an underpowered study. It fixes the arithmetic of the p-value; only more data can lower the smallest p the design allows.

### Visualization (canvas `c4`, 720×300)

Bar chart of the smallest p-value each experiment size can ever produce (a perfect score), against a dashed 0.05 line the 4-cup design can never cross.

- **Title (bold 15px, `#1a5276`, top center):** "The Smallest p an Experiment Can Ever Produce".
- **Data:** designs `["4 cups (2+2)", "6 cups (3+3)", "8 cups (4+4)", "10 cups (5+5)"]`; best p = `[0.167, 0.050, 0.014, 0.004]`; bar labels "1/6 ≈ 0.167", "1/20 = 0.050", "1/70 ≈ 0.014", "1/252 ≈ 0.004".
- **Axes:** origin x=70, baseline y=235, chart width 600, height 170; y scale 0–0.20 with gridlines at 0.05, 0.10, 0.15, 0.20 in `#e5e9ef` and 12px `#6b7280` labels.
- **Bars:** width 90, centered at x = 140 + i×150; the 4-cup bar fill `rgba(217,89,38,0.45)` stroke orange `#d95926` (can never reach 0.05); the other three fill `rgba(42,120,214,0.45)` stroke blue `#2a78d6`; bold 13px `#2c3e50` fraction label above each bar; design labels 12px `#444` below baseline.
- **Threshold:** dashed magenta `#d55181` (dash 4/3) horizontal line across the chart at the 0.05 level, bold 12px magenta label "p = 0.05" at its right end.
- **Annotations:** orange `#d95926` bold 13px above the first bar: "4 cups can never reach 0.05 — even perfect"; green `#008300` bold 12px near the last bar: "more cups, smaller floor".
- **Caption (12px `#444`, bottom):** "each bar = p-value of a perfect score in that design".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
