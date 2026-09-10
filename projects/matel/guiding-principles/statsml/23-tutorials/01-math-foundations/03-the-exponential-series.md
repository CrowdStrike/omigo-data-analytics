# The Exponential Series

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** The Exponential Series

**Subtitle:** Compound $1 at 100% interest more and more often and the payout climbs toward one fixed number — e ≈ 2.71828, the natural base of all growth math

## One Dollar at 100%, Compounded Faster and Faster

**Tags:** `core idea` (blue), `compound interest` (green), `a limit` (orange)

- **The deal** — a bank pays 100% yearly interest on $1, so after one year you have $2
- **Split it** — pay 50% twice instead: $1 → $1.50 → $2.25, since mid-year interest earns interest
- **Split more** — quarterly gives $2.4414, monthly $2.6130, daily $2.7146 — climbing but slowing
- **The ceiling** — compounding every instant lands on $2.71828..., the number we call e
- **The formula** — e is the limit of (1 + 1/n)^n as n grows; the interest story is the formula

*Example (italic):* Going from monthly to daily compounding adds about a dime per dollar; going past daily adds almost nothing.

**Key point:** e ≈ 2.71828 is simply what $1 becomes after one year of 100% interest compounded continuously — a ceiling, not an explosion.

### Visualization (canvas `c1`, 720×300)

Bar chart of the value of $1 after one year at six compounding frequencies, with a dashed horizontal ceiling line at e.

- **Title (bold 15px, `#1a5276`, top center):** "$1 at 100% Interest: Value After One Year vs Compounding Frequency".
- **Data:** values `[2.00, 2.25, 2.4414, 2.6130, 2.7146, 2.7183]` with value labels "$2.00", "$2.25", "$2.4414", "$2.6130", "$2.7146", "$2.7183"; frequency names "yearly", "half-yearly", "quarterly", "monthly", "daily", "continuous"; n labels "n=1", "n=2", "n=4", "n=12", "n=365", "n→∞".
- **Layout:** axis origin x=60, width 630, baseline y=250, chart height 195, y scale max 3.0; six slots of width 630/6 with bars inset 10px each side (slot width − 20).
- **Bars:** fill `rgba(42,120,214,0.45)` for the first five, `rgba(0,131,0,0.4)` for "continuous"; bold 12px `#1a5276` value label inside each bar top (16px below bar top); frequency name 12px `#444` at baseline+16; n label 11px `#6b7280` at baseline+30.
- **Ceiling line:** green `#008300` dashed (dash 6/4) 2px horizontal line at y for 2.7183; green bold 13px left-aligned label "e = 2.71828... — the ceiling" just above it.
- **Annotation:** orange `#d95926` bold 12px centered at the third slot, 33px below chart top: "each extra split helps less".

## Adding the Series by Hand

**Tags:** `worked example` (blue), `factorials` (green)

- **Same number** — expand (1 + 1/n)^n and let n grow: it becomes 1 + 1 + 1/2 + 1/6 + 1/24 + ...
- **The pattern** — each denominator is a factorial: 1/2! = 0.5, 1/3! ≈ 0.1667, 1/4! ≈ 0.0417
- **Run the sum** — partial sums go 1, 2, 2.5, 2.6667, 2.7083, 2.7167, 2.7181, homing in on e
- **Fast finish** — terms shrink factorially, so ten terms already match e to six decimals
- **Why factorials** — the k-th binomial term of (1 + 1/n)^n settles at 1/k! as n grows

*Example (italic):* Adding just 1 + 1 + 0.5 + 0.1667 + 0.0417 + 0.0083 on paper gives 2.7167 — within 0.06% of e.

**Key point:** e = 1/0! + 1/1! + 1/2! + 1/3! + ... — the compound-interest limit and this series are the same number reached by two different roads.

### Visualization (canvas `c2`, 720×300)

Dual-panel chart: term sizes 1/k! as bars (left) and partial sums converging to e as a dotted line (right), split by a vertical dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "e as a Series: Term Sizes 1/k! and the Running Total".
- **Data:** terms `[1, 1, 0.5, 0.1667, 0.0417, 0.0083, 0.0014, 0.0002]` with bar labels "1", "1", ".5", ".17", ".04" (last three unlabeled); partial sums `[1, 2, 2.5, 2.6667, 2.7083, 2.7167, 2.7181, 2.7183]`.
- **Left panel (term bars):** axis origin x=50, width 290, baseline y=240, chart height 170, scale max 1.05; eight bars of width 290/8 inset 3px, fill `rgba(42,120,214,0.45)`; "k=0"..."k=7" 11px `#444` below each bar; bold 12px blue `#2a78d6` term label above labeled bars; orange `#d95926` bold 12px annotation "terms vanish factorially" at 62% of panel width, y = baseline−110; caption 12px `#444` "term k = 1/k!" at bottom center.
- **Right panel (partial sums):** axis origin x=395, width 290, same baseline/height; y maps values over range 0.8–3.0; green `#008300` dashed (dash 6/4) 2px horizontal line at 2.7183 with green bold 12px left label "e = 2.71828"; blue `#2a78d6` 3px line with 4px dots at x = rx+20 + i/7 × (rw−40); "k=0"..."k=7" 11px `#444` below each point; magenta `#d55181` bold 12px centered annotation "seven terms already give 2.7181" at y=190; caption "running total after term k" at bottom center.
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.

## Why "Natural": the Curve Whose Slope Is Itself

**Tags:** `where it's used` (blue), `slope = height` (green), `natural base` (orange)

- **Slope = height** — the curve y = e^x rises at a rate equal to its current height everywhere
- **Check it** — at x = 1 the height is 2.72 and the tangent slope is also 2.72; at x = 0 both are 1
- **Other bases** — 2^x also grows, but its slope is only 0.69 × its height, never 1 ×
- **Master curve** — steady continuous growth at rate r for time t is always e^(rt)
- **In practice** — half-lives, population models, and ML's log loss all run on e and its inverse ln

*Example (italic):* A colony whose growth speed always equals its current size traces exactly y = e^x — no other base fits.

**Key point:** e is called natural because e^x is the one exponential that is its own slope; every other base drags a constant factor (the ln of the base) along.

### Visualization (canvas `c3`, 720×300)

Function plot of y = e^x with its tangent line at x=1 (showing slope = height) and a dashed 2^x curve for contrast.

- **Title (bold 15px, `#1a5276`, top center):** "y = eˣ: the Slope Always Equals the Height".
- **Layout:** axis origin x=60, width 630, baseline y=255, top y=45; x range 0–2, y range 0–8; x ticks at 0, 0.5, 1, 1.5, 2 (12px `#444`); y gridlines `#e5e9ef` at 2, 4, 6, 8 with right-aligned 12px `#444` labels.
- **2^x curve:** yellow `#c98500` 2px dashed (dash 5/4) plotted for x = 0 to 2 in steps of 0.05; yellow bold 12px right-aligned label "2ˣ: slope only 0.69 × height" near the right edge at y=210.
- **e^x curve:** blue `#2a78d6` 3px solid, same x range/step; blue bold 13px right-aligned label "y = eˣ" near (x=1.93, y=6.9), 8px above.
- **Tangent at x=1:** green `#008300` 2px dashed (dash 6/4) line y = 2.718x drawn from x=0.35 to x=1.65.
- **Slope triangle:** mute `#6b7280` 1px path from (1, 2.718) right to (1.5, 2.718) up to (1.5, 4.077); 11px mute labels "run 0.5" (centered below the horizontal leg) and "rise 1.36" (left-aligned right of the vertical leg).
- **Points:** 5px ink `#1a5276` dots at (1, 2.718) and (0, 1).
- **Annotations:** mute 12px left-aligned "at x = 0: height 1, slope 1" at 10px right of x=0 origin, y=205; green bold 13px "at x = 1: height 2.72, slope 2.72" at x=0.16 of the axis, y for value 5.2.

## Infinite Compounding Is Not Infinite Money

**Tags:** `common mistake` (red), `rule of thumb` (green)

- **The hope** — people expect compounding infinitely often to turn 100% interest into a fortune
- **The reality** — the yearly multiplier is capped at e ≈ 2.7183, no matter how fine the slices
- **Dime vs penny** — monthly → daily adds 10.2¢ per dollar; daily → continuous adds only 0.4¢
- **Marketing trap** — "continuously compounded" sounds premium but beats daily by under half a cent
- **The real lever** — growth comes from rate × time in e^(rt), not from slicing the year finer

*Example (italic):* A saver switched banks to get continuous compounding at the same rate — and gained 0.4 cents per dollar per year.

**Common mistake:** Believing more frequent compounding compounds forever. It converges to e; the lever that matters is the exponent rt, not the number of slices.

### Visualization (canvas `c4`, 720×300)

Bar chart of the extra cents per dollar gained by each step up in compounding frequency, with the final near-zero step highlighted.

- **Title (bold 15px, `#1a5276`, top center):** "Extra Cents per Dollar Gained by Compounding More Often (100% rate)".
- **Data:** gains `[25.0, 19.1, 17.2, 10.2, 0.4]` with labels "25.0¢", "19.1¢", "17.2¢", "10.2¢", "0.4¢"; from labels "yearly →", "half-yearly →", "quarterly →", "monthly →", "daily →"; to labels "half-yearly", "quarterly", "monthly", "daily", "continuous".
- **Layout:** axis origin x=60, width 630, baseline y=240, chart height 175, scale max 28; five slots of width 630/5 with bars inset 18px each side (slot width − 36); bar height floor 3px.
- **Bars:** fill `rgba(42,120,214,0.45)` for the first four, `rgba(213,81,129,0.55)` for the last ("daily → continuous"); bold 12px value label above each bar, blue `#2a78d6` except magenta `#d55181` for the last; from label 12px `#444` at baseline+16 and to label at baseline+31.
- **Annotation:** magenta `#d55181` bold 13px centered at the fifth slot, two lines: "daily → continuous: under half a cent —" (baseline−120) / "the ceiling is e" (baseline−102).
- **Caption (12px `#444`, bottom center):** "gain per dollar per year from each step up in frequency".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
