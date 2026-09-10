# The Uniform Distribution

**Page type:** detail page (tutorial page: 4 card-sections, each a two-column layout table — text left 50%, canvas right 50%)
**HTML title tag:** The Uniform Distribution

**Subtitle:** Every value equally likely — the flat shape behind dice rolls and random number generators

## Roll One Die 600 Times

**Tags:** `dice` (blue), `equal chance` (green), `core idea` (blue)

- **The setup** — a fair six-sided die: each face has exactly the same chance, 1/6 ≈ 16.7%
- **The shape** — plot the chances and you get a flat line: no peak, no favorite, no tail
- **600 rolls** — expect about 100 of each face; our tally: 96, 104, 99, 107, 98, 96
- **Flat-ish, not flat** — real tallies wobble around 100; only the true chances are perfectly flat
- **The family** — lottery balls, card cuts, and a spinner all share this no-favorites shape

*Example:* Face 4 came up 107 times and face 1 only 96 — ordinary wobble, not a loaded die.

**Key point:** "Uniform" means the rule is flat — every outcome gets the same slice of probability. It is the shape of pure fairness.

### Visualization (canvas `c1`, 720×300)

Bar chart: tally of 600 die rolls per face against the flat expected line.

- **Title (bold 15px, ink #1a5276, centered):** "600 Rolls of a Fair Die (illustrative tally)"
- **Data:** counts per face 1–6: `[96, 104, 99, 107, 98, 96]` (sums to 600)
- **Axes:** padding top 48 / bottom 46 / left 55 / right 20; y max 130 with labels 0, 50, 100 (gridlines `#e5e9ef` above 0); x labels "face 1" … "face 6"; axis lines `#999`
- **Bars:** width 74, evenly gapped, all blue `#2a78d6`; bold count labels (12px, text `#2c3e50`) above each bar
- **Expected line:** horizontal dashed green line (`#008300`, width 2, dash 7/4) at y=100; green bold 13px left-aligned label above it: "expected: 600 × 1/6 = 100 each — a flat line"
- **Annotations:** orange (`#d95926`) bold 12px centered near baseline: "wobble of a few rolls around 100 is normal"; mute (`#6b7280`) 12px caption: "count of each face in 600 rolls"

## Probability by Ruler: Width Is Everything

**Tags:** `worked example` (green), `random draws` (blue)

- **Die version** — P(roll ≤ 2) = 2 faces ÷ 6 faces = 1/3; just count and divide
- **Die average** — (1+2+3+4+5+6) ÷ 6 = 3.5: the balance point of the flat bar
- **Continuous version** — a random number 0–1: every stretch of equal width is equally likely
- **Read with a ruler** — P(draw lands in 0.20–0.50) = 0.50 − 0.20 = 0.30, exactly 30%
- **No middle bias** — 0.001 and 0.500 and 0.999 are all equally likely draws

*Example:* P(a draw lands in 0.90–1.00) = 0.10 — same as any other slice one tenth wide.

**Key point:** Under a uniform, probability is just length: the chance of an interval equals its width. No formula tables, only subtraction.

### Visualization (canvas `c2`, 720×300)

Continuous uniform density on 0–1 drawn as a flat rectangle with one shaded interval.

- **Title (bold 15px, ink #1a5276, centered):** "Random Number 0–1: Probability of an Interval = Its Width"
- **Layout:** padding top 52 / bottom 58 / left 65 / right 35; axis lines `#999`
- **Density:** flat rectangle spanning 0–1 at 62% of chart height, fill `rgba(42,120,214,0.14)`, stroke blue `#2a78d6` width 3
- **Shaded slice:** interval 0.2–0.5 filled `rgba(0,131,0,0.35)` over the same height
- **X-axis labels (mute 12px):** 0, 0.2, 0.5, 1 with tick marks; caption "the drawn value"
- **Y labels (mute, right-aligned at axis):** "chance" and "density 1" near the density top
- **Annotations:** green (`#008300`) bold 14px centered above the slice: "P(0.2 to 0.5) = 0.5 − 0.2 = 0.30"; magenta (`#d55181`) bold 13px centered inside the slice: "same width anywhere = same 30%"; orange (`#d95926`) bold 12px right-aligned inside the rectangle: "no bulge, no favorite region"

## Why It Matters: Flat Is What "No Bug, No Signal" Looks Like

**Tags:** `where it's used` (blue), `sanity check` (green)

- **Random generators** — every simulation and shuffle starts from uniform draws on 0–1
- **Sampling** — "pick users at random" means each user gets an equal, uniform chance
- **Hashing** — a good hash spreads keys uniformly across buckets; hot buckets mean a bad hash
- **The check** — 1,000 draws into 10 bins should give ~100 each; ours: 96 to 105, all close
- **Bug detector** — a bulge or a hole in this histogram means the "random" code is not random

*Example:* A sampler that returns twice as many IDs ending in 0–4 fails the flatness check instantly.

**Key point:** Uniform is the boring baseline. When something should be fair — a sampler, a hash, an A/B split — deviation from flat is your first bug report.

### Visualization (canvas `c3`, 720×300)

Bar chart: 1,000 random-generator draws binned by first decimal, flat within noise.

- **Title (bold 15px, ink #1a5276, centered):** "Uniformity Check: 1,000 Generator Draws in 10 Bins (illustrative)"
- **Data:** bin counts: `[98, 103, 101, 96, 105, 99, 102, 97, 100, 99]` (sums to 1000)
- **Axes:** padding top 48 / bottom 46 / left 55 / right 20; y max 130 with labels 0, 50, 100 (gridlines `#e5e9ef`); x labels "0.0" … "0.9"
- **Bars:** all aqua `#199e70`; count labels (12px, text `#2c3e50`) above each bar
- **X-axis caption (mute):** "bin = first decimal of the draw (0.0x … 0.9x)"
- **Expected line:** horizontal dashed green line (`#008300`, width 2, dash 7/4) at y=100; green bold 13px left-aligned label above: "a good generator is boring: ~100 per bin"
- **Warning annotation:** red (`#e74c3c`) bold 12px centered near baseline: "a bulge or hole here = a bug in the "random" code"

## The Mistake: Adding Uniforms and Expecting Flat

**Tags:** `common mistake` (red), `combining` (orange)

- **Two dice** — each die is flat, but their sum is not: 7 is six times likelier than 2
- **Count the ways** — sum 2 has one way (1+1); sum 7 has six (1+6, 2+5, 3+4, 4+3, 5+2, 6+1)
- **The triangle** — the sum's shape is a tent peaking at 7 — flatness dies on first contact
- **Keep adding** — sum many dice and the tent smooths into a bell; middles dominate
- **Real-world echo** — totals and averages of fair parts are never uniform themselves

*Example:* Board gamers know it: build your strategy around 6, 7, 8 — not around snake eyes.

**Common mistake:** Assuming combinations of equally-likely parts stay equally likely. Uniform in, triangle out — and with more parts, bell out.

### Visualization (canvas `c4`, 720×300)

Bar chart: the triangular distribution of the sum of two dice.

- **Title (bold 15px, ink #1a5276, centered):** "Two Flat Dice, One Triangular Sum"
- **Data:** sums 2–12 with ways out of 36: `[1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1]`
- **Axes:** padding top 48 / bottom 46 / left 55 / right 20; y scale max 7; x labels 2..12; axis lines `#999`
- **Bars:** sum=7 orange `#d95926`; all others violet `#4a3aa7`; bold labels "1/36" … "6/36" (text `#2c3e50`) above each bar
- **X-axis caption (mute):** "sum of the two dice"
- **Annotations (left-aligned at ~6.2 bars in):** orange bold 13px: "7: six ways — 1+6, 2+5, 3+4, 4+3, 5+2, 6+1"; violet bold 12px: "2: only 1+1 — six times rarer"; magenta (`#d55181`) bold 13px: "uniform in, triangle out"

## Regeneration instructions

- **Layout:** tutorial detail page. `<h1>` (no index number) + `.subtitle`, then four `.card-section` blocks, each with an `<h2>` (1.3rem, `#1a5276`, 2px solid `#2980b9` bottom border) and a `table.layout` (width 100%, border-collapse) containing one row: `td.text-col` (50%) and `td.viz-col` (50%), both `vertical-align: top`, padding 12px.
- **Text column structure:** `.tags` pill row first, then a `<ul>` of one-line bullets each opening with `<b>bold term</b>` (bold terms colored `#1a5276`), one italic `.example` paragraph, one `.key-point` callout (background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem).
- **Tag pills:** inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px. Colors — blue: bg `rgba(26,82,118,0.12)` text `#1a5276`; green: bg `rgba(39,174,96,0.15)` text `#27ae60`; red: bg `rgba(231,76,60,0.12)` text `#e74c3c`; orange: bg `rgba(230,126,34,0.15)` text `#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px solid `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; canvases `width: 100%` with `1px solid #e0e0e0` border, 4px radius.
- **Canvases:** all 720×300 intrinsic; scaled via `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) through a shared `setup(id)` helper. Data arrays are hardcoded literals (no `Math.random()`); invented tallies are labeled "(illustrative)" in chart titles. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- No nav bar, no back/home links, no cross-page links. In regenerated HTML any card links use `.html` extensions (this page has none).
