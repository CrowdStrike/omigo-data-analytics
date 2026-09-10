# Fibonacci & the Golden Ratio

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Fibonacci & the Golden Ratio

**Subtitle:** Add the last two numbers to get the next — the ratio of neighbours locks onto φ ≈ 1.618, the same number the biggest eigenvalue of a 2×2 matrix predicts

## A Rabbit Farm That Adds Its Last Two Months

**Tags:** `core idea` (blue), `recurrence` (green), `running example` (orange)

- **The farm** — a farm starts with 1 newborn pair; newborns take one month to grow into adults
- **The rule** — every adult pair delivers exactly one newborn pair each month, and no pair dies
- **The counts** — pairs by month: 1, 1, 2, 3, 5, 8, 13, 21, 34, 55 — each is the sum of the last two
- **Why add two** — next month = all pairs alive now + one newborn per pair alive a month ago
- **The recurrence** — the rule in symbols is F(n) = F(n−1) + F(n−2), the Fibonacci recurrence

*Example (italic):* Month 7 holds 13 pairs: the 8 pairs from month 6 plus 5 newborns from month 5's pairs.

**Key point:** One tiny local rule — add the last two — generates the whole sequence, and by month 10 the farm already holds 55 pairs.

### Visualization (canvas `c1`, 720×300)

Bar chart of rabbit pairs per month (10 bars), with the 5 and 8 bars highlighted feeding the 13 bar via a "5 + 8 = 13" annotation.

- **Title (bold 15px, `#1a5276`, top center):** "Rabbit Pairs per Month: Each Bar = the Two Before It Added".
- **Data:** `F = [1, 1, 2, 3, 5, 8, 13, 21, 34, 55]`, x labels "m1" … "m10" (12px `#444` below baseline).
- **Axes:** origin x=60, width 620, baseline y=245, chart height 185, scale max 60; 1px `#999` axis lines; each bar has minimum 3px height, slot width 620/10 with 8px insets (bar width slot−16).
- **Bar fills:** bars 5 and 6 (values 5 and 8) `rgba(25,158,112,0.55)` (aqua); bar 7 (value 13) `rgba(217,89,38,0.6)` (orange); all others `rgba(42,120,214,0.45)` (blue); each bar's value in bold 12px `#444` above it.
- **Annotation:** orange `#d95926` bold 13px "5 + 8 = 13" centered over slot 5.5 at baseY−90, with three 2px orange tick marks (from baseY−72 to baseY−62) at slots 4.5, 5.5, 6.5.
- **Callout:** blue `#2a78d6` bold 12px, left-aligned near chart top: "growth explodes: 1 pair → 55 pairs in 10 months".
- **Caption (12px `#444`, bottom center):** "month".

## The Neighbour Ratio Locks Onto 1.618

**Tags:** `worked example` (blue), `golden ratio` (green)

- **Take ratios** — divide each month by the last: 2/1 = 2.00, 3/2 = 1.50, 5/3 ≈ 1.67, 8/5 = 1.60
- **Zigzag in** — the ratios overshoot then undershoot, closing in on 1.618 from both sides
- **Golden ratio** — the limit is φ = (1 + √5)/2 ≈ 1.6180, the positive number with φ² = φ + 1
- **Self-check** — 1.618 × 1.618 ≈ 2.618 = 1.618 + 1, so growth by ×φ keeps the add-two rule
- **Fast lock-in** — by the 34/21 step the ratio ≈ 1.619 already sits within about 0.001 of φ

*Example (italic):* 55/34 ≈ 1.6176 and 89/55 ≈ 1.6182 — each step crosses φ and lands a little closer.

**Key point:** The neighbour ratio converges to φ ≈ 1.618 for almost any starting pair — start with 2 and 6 (2, 6, 8, 14, 22, ...) and the ratios settle on the same limit.

### Visualization (canvas `c2`, 720×300)

Line chart of the consecutive-term ratio zigzagging onto a dashed horizontal φ reference line.

- **Title (bold 15px, `#1a5276`, top center):** "Ratio of Neighbouring Months Zigzags Onto φ ≈ 1.6180".
- **Data:** ratios `[1.000, 2.000, 1.500, 1.667, 1.600, 1.625, 1.615, 1.619, 1.618, 1.618]` with x labels `['1/1', '2/1', '3/2', '5/3', '8/5', '13/8', '21/13', '34/21', '55/34', '89/55']` (11px `#444` below baseline); `phi = 1.618`.
- **Axes:** origin x=70, width 560, baseline y=250, top y=50, y range 0.9–2.1; 1px `#999` axis lines; right-aligned 12px `#444` y labels "1.0", "1.5", "2.0".
- **Phi line:** magenta `#d55181` dashed (dash 5/4) 2px horizontal line at y of 1.618, labeled above its right end (right-aligned at the plot edge) in bold 13px magenta: "φ = (1+√5)/2 ≈ 1.6180".
- **Series:** blue `#2a78d6` 3px line with 4px dots at all 10 points; first five points get bold 12px blue value labels "1.00", "2.00", "1.50", "1.67", "1.60" (offset −10 above when the ratio exceeds φ, +20 below otherwise).
- **Annotation:** green `#008300` bold 13px "within ~0.001 of φ by 34/21" centered above point index 7 (40px above y of 1.619).
- **Caption (12px `#444`, bottom center):** "ratio of consecutive months".

## Why Sunflowers Count in Fibonacci

**Tags:** `where it's used` (blue), `in nature` (green), `golden angle` (orange)

- **Seed packing** — a sunflower places each new seed 137.5° around from the last, a bit further out
- **Golden angle** — 137.5° is 360°(1 − 1/φ); this turn keeps seeds from lining up in rays
- **Spiral families** — the eye picks out two sets of spirals whose counts are Fibonacci neighbours
- **Field guide** — pinecones show 8 and 13 spirals, daisies 21 and 34, sunflower heads 34 and 55
- **Not magic** — the plant repeats one simple growth rule; the Fibonacci counts fall out of geometry

*Example (italic):* Count the spirals on a sunflower head: 34 run one way and 55 the other — consecutive Fibonacci numbers.

**Key point:** Nature doesn't "know" Fibonacci — repeating a fixed turn of the golden angle makes Fibonacci spiral counts inevitable.

### Visualization (canvas `c3`, 720×300)

Dual panel split by a dashed divider at x=360: a deterministic golden-angle phyllotaxis seed pattern (left) and grouped bars of Fibonacci spiral counts in three plants (right).

- **Title (bold 15px, `#1a5276`, top center):** "One Rule — Turn 137.5°, Step Out — Builds the Spirals".
- **Divider:** dashed `#bdc3c7` (dash 4/3) 1px vertical line at x=360 from y=38 to h-12.
- **Left panel (phyllotaxis):** N=190 seeds around center (180, 172); seed i at radius `7.4*sqrt(i)`, angle `i * 137.508°` (radians); 2.6px dots alternating fills `rgba(25,158,112,0.8)` (even i) and `rgba(0,131,0,0.55)` (odd i) — deterministic, no randomness; aqua `#199e70` bold 12px caption centered at bottom: "each seed: previous angle + 137.5°".
- **Right panel (spiral counts):** grouped bar chart, names `['pinecone', 'daisy', 'sunflower']`, pairs `[[8, 13], [21, 34], [34, 55]]`; axis origin x=400, width 280, baseline y=245, chart height 160, scale max 60; per group two 30px-wide bars (first fill `rgba(0,131,0,0.5)` green, second `rgba(42,120,214,0.5)` blue) with bold 12px `#444` value labels above; plant names 12px `#444` below each group; blue `#2a78d6` bold 13px heading above the panel: "spiral counts = Fibonacci neighbours"; caption 12px `#444` "spirals one way vs the other".

## Skipping the Loop: Eigenvalues Give a Closed Form

**Tags:** `closed form` (blue), `eigenvalues` (orange), `rule of thumb` (green)

- **Two-number state** — a month is the pair (this, last); one step maps it to (this + last, this)
- **Matrix step** — that map is multiplication by the 2×2 matrix [[1,1],[1,0]] — a linear system
- **Eigenvalues** — the matrix stretches two special directions, by φ ≈ 1.618 and ψ ≈ −0.618
- **Binet's formula** — F(n) = (φⁿ − ψⁿ) / √5: a closed form, no loop over months needed
- **Dying term** — |ψ| < 1, so ψⁿ/√5 shrinks fast; rounding φⁿ/√5 gives F(n) exactly

*Example (italic):* φ¹⁰/√5 ≈ 55.004, and rounding gives F(10) = 55 — the correction ψ¹⁰/√5 is only 0.004.

**Key point:** The long-run growth rate of a linear recurrence is its largest eigenvalue — Fibonacci grows like φⁿ because φ is the big eigenvalue of [[1,1],[1,0]].

### Visualization (canvas `c4`, 720×300)

Dual panel split by a dashed divider at x=360: the φⁿ/√5 curve against exact F(n) dots (left) and shrinking bars of the |ψⁿ|/√5 correction (right).

- **Title (bold 15px, `#1a5276`, top center):** "Binet: F(n) = (φⁿ − ψⁿ)/√5 — the ψ Term Dies Out".
- **Data:** `F = [1, 1, 2, 3, 5, 8, 13, 21, 34, 55]`; `binet = [0.724, 1.171, 1.894, 3.065, 4.960, 8.025, 12.985, 21.009, 33.994, 55.004]`; `gap = [0.276, 0.171, 0.106, 0.065, 0.040, 0.025, 0.015, 0.010, 0.006, 0.004]`.
- **Divider:** dashed `#bdc3c7` (dash 4/3) 1px vertical line at x=360 from y=38 to h-12.
- **Left panel:** axis origin x=55, width 275, baseline y=240, chart height 170, scale max 60; x positions inset 12px each side over indices 0–9; orange `#d95926` 3px line through the binet values; blue `#2a78d6` 4px dots at exact F(n); x labels "1"–"10" in 11px `#444`; legend at chart top left, bold 12px: orange "orange: φⁿ/√5" and blue "blue dots: exact F(n)"; green `#008300` bold 12px right-aligned annotation near the top point: "55.004 → rounds to 55"; caption 12px `#444` "n (month)".
- **Right panel:** axis origin x=400, width 275, same baseline/height, scale max 0.30; ten bars (width rw/10 with 4px insets, minimum 2px height) filled `rgba(74,58,167,0.5)` (violet); x labels "1"–"10" in 11px `#444`; violet `#4a3aa7` bold 12px value labels "0.276" above the first bar and "0.004" above the last; violet bold 13px heading centered at panel top: "gap = |ψⁿ|/√5, ψ ≈ −0.618"; caption 12px `#444` "|ψ| < 1 so the correction vanishes".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
