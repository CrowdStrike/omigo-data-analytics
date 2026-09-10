# Logarithms & Log Scales

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Logarithms & Log Scales

**Subtitle:** A log axis measures multiplication instead of addition — a hockey-stick growth curve becomes a straight line, and huge skewed numbers become readable

## One Orders Table, Two Pictures

**Tags:** `core idea` (blue), `log axis` (green), `multiplicative change` (orange)

- **The shop** — an online store's monthly orders double every month: 100, 200, 400, ... 12,800
- **Linear axis** — each step up means "+ the same amount"; doubling looks like a hockey stick
- **Log axis** — each step up means "× the same amount"; doubling becomes a straight line
- **Same data** — nothing changed but the ruler; the growth story only reads on the log side
- **Straight = steady** — a straight line on a log axis means a constant growth rate

*Example (italic):* On the linear chart, January to May look like a flat failure — yet the store was doubling the whole time.

**Key point:** A linear axis answers "how much was added?"; a log axis answers "what was it multiplied by?". Pick the axis that matches the question.

### Visualization (canvas `c1`, 720×300)

Dual-panel line chart: the same doubling series on a linear axis (left) and a log10 axis (right), split by a vertical dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "Monthly Orders, Doubling: Linear Axis vs Log Axis".
- **Data:** months J, F, M, A, M, J, J, A; orders `[100, 200, 400, 800, 1600, 3200, 6400, 12800]`.
- **Left panel (linear):** axis origin x=55, width 280, baseline y=245, chart height 185, y scale 0–13,000; blue `#2a78d6` 3px line with 4px dots; month letters 12px `#444` below baseline; orange bold 12px annotation "first 5 months look flat"; caption 12px `#444` "linear: 0 to 13,000".
- **Right panel (log):** axis origin x=400, width 280, same baseline/height; y maps log10(orders) over range 2 to 4.2; green `#008300` 3px line with 4px dots; same month labels; green bold 13px annotation "straight line = ×2 every month"; caption "log: 100 to ~16,000".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.

## Counting Zeros by Hand

**Tags:** `worked example` (blue), `rule of thumb` (green)

- **The trick** — log10 of a number is roughly how many zeros it has: log10(1,000) = 3
- **Check it** — a $10 order scores 1, a $100 order scores 2, a $10,000 order scores 4
- **Half steps** — $3,200 sits near 3.5 because √10 ≈ 3.16, so 3,200 ≈ 10^3.5
- **Equal spacing** — on a log ruler, 10 → 100 and 1,000 → 10,000 are the same distance
- **Compression** — a linear ruler crams 10, 100, and 1,000 into its first pixel-width

*Example (italic):* Order values $10, $100, $1,000, $3,200, $10,000 land at log10 marks 1, 2, 3, 3.5, 4 — you can verify each by counting zeros.

**Key point:** The logarithm is just the exponent: log10(x) asks "10 to what power gives x?". Counting zeros gets you within half a step without a calculator.

### Visualization (canvas `c2`, 720×300)

Two horizontal number-line "rulers" showing five order values plotted on a linear ruler (top) vs a log ruler (bottom).

- **Title (bold 15px, `#1a5276`, top center):** "Five Order Values on a Linear Ruler vs a Log Ruler".
- **Data:** values 10, 100, 1000, 3200, 10000 with labels "$10", "$100", "$1,000", "$3,200", "$10,000" and log10 values "1", "2", "3", "3.5", "4".
- **Rulers:** both from x=70, width 580, 2px `#999` lines; linear ruler at y=95, log ruler at y=205.
- **Linear ruler:** heading bold 12px `#444` "linear ruler ($0 → $10,000)"; blue `#2a78d6` 6px dots positioned proportionally to value/10,000; only "$3,200" and "$10,000" labeled below (the rest overlap); magenta `#d55181` bold 12px annotation: "← $10, $100, $1,000 crushed together here".
- **Log ruler:** heading "log ruler (log10 from 1 → 4)"; green `#008300` 6px dots at (log10(v)−1)/3 of the width; each dot gets its dollar label 12px `#444` below and a green bold "log = N" label above.
- **Takeaway (bold 13px green, bottom center):** "log10 ≈ counting zeros: evenly spread, every value readable".

## Why Skewed Data Forces the Issue

**Tags:** `where it's used` (blue), `skewed data` (orange), `failure mode` (red)

- **Real orders** — 1,000 orders: median ~$35, mean ~$130, biggest $9,800 (illustrative)
- **Linear bins** — 770 of 1,000 orders pile into the first bar; the tail is invisible
- **Log bins** — bin edges at ×3 steps ($3, $10, $32, $100...) reveal a clean hill
- **Everywhere** — incomes, city sizes, file sizes, and word counts share this shape
- **Without it** — you would report "orders are basically all tiny" and miss the structure

*Example (italic):* The same 1,000 orders look like one giant bar on a linear axis but a readable hill on log bins.

**Key point:** When values span several powers of ten, a linear histogram hides everything. Log bins are the standard first move on money, counts, and durations.

### Visualization (canvas `c3`, 720×300)

Dual-panel histogram: linear-binned (left) vs log-binned (right) version of the same 1,000 order values, split by a dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "1,000 Order Values: Linear Bins vs Log Bins (illustrative)".
- **Left panel (linear bins):** counts `[770, 95, 45, 25, 15, 10, 8, 5, 4, 3]` in ten $100-wide bins (20 more orders sit off-scale above $1,000); axis origin x=50, width 290, baseline y=240, chart height 175, scale max 800; bars fill `rgba(42,120,214,0.45)`; bold 12px blue label "770" above the first bar; magenta `#d55181` bold 12px annotation, two lines: "77% in one bar," / "tail invisible"; muted 12px right-aligned note "+20 orders off-scale →" near the axis end; caption 12px `#444` "$100-wide bins, $0 → $1,000".
- **Right panel (log bins):** counts `[30, 140, 310, 290, 150, 60, 15, 5]` in eight bins with edge labels "$1", "$3", "$10", "$32", "$100", "$316", "$1k", "$3.2k" (11px below each bar); axis origin x=395, width 290, scale max 340; bars fill `rgba(0,131,0,0.4)`; green bold 13px annotation "same 1,000 orders: a readable hill"; caption "bin edges multiply by ~3.16 (= √10)".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.

## The Gap That Looks Small

**Tags:** `common mistake` (red), `rule of thumb` (green)

- **Equal gaps lie** — on a log axis, $100 → $1,000 and $10,000 → $100,000 look identical
- **The dollars** — the first gap is $900; the second is $90,000 — one hundred times more
- **Reading rule** — on a log axis, read gaps as ratios ("×10"), never as amounts
- **Small dips** — a barely-visible dip near the top of a log chart can be millions
- **Zero missing** — log10(0) does not exist; a log axis can never start at zero

*Example (italic):* A manager saw two equal-looking steps on a log revenue chart and budgeted the same for both — off by 100×.

**Common mistake:** Treating equal visual gaps on a log axis as equal amounts. Equal gaps mean equal ratios — the higher up the axis, the more dollars each pixel hides.

### Visualization (canvas `c4`, 720×300)

Log-axis number line with two bracketed equal-width gaps, plus small dollar-amount bars underneath showing the true 100× difference.

- **Title (bold 15px, `#1a5276`, top center):** "Two Identical-Looking Gaps on a Log Axis".
- **Log axis:** horizontal line at y=150 from x=70, width 580, log10 range 1–5; decade ticks with labels "$10", "$100", "$1,000", "$10,000", "$100,000" (12px `#444`).
- **Gap A:** blue `#2a78d6` bracket (3px) spanning $100 → $1,000, 6px endpoint dots on the axis, bold 13px label "gap = $900" above.
- **Gap B:** orange `#d95926` bracket spanning $10,000 → $100,000, same styling, label "gap = $90,000".
- **Amount bars (below, from y=210):** heading bold 12px `#444` "the same two gaps, drawn as dollar amounts:"; bar for $900 fill `rgba(42,120,214,0.55)` (minimum 5px long) labeled "$900" in blue; bar for $90,000 fill `rgba(217,89,38,0.55)` spanning 480px max width, labeled "$90,000" in orange; both 18px tall.
- **Takeaway (bold 13px magenta `#d55181`, centered at y=285):** "same gap on screen, 100× bigger in dollars — log gaps are ratios, not amounts".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
