# Random Variables

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Random Variables

**Subtitle:** A random variable is a rule that attaches a number to each outcome — two dice land as a pair, and the rule "add them" turns that pair into a number you can chart and average

## One Roll, One Number

**Tags:** `core idea` (blue), `outcome → number` (green), `sample space` (orange)

- **Game night** — two dice are rolled; the raw outcome is an ordered pair like (3,4), not a number
- **36 outcomes** — each die shows 1–6, so there are 6 × 6 = 36 equally likely pairs
- **The rule** — define X = "add the two faces"; the pair (3,4) becomes the number X = 7
- **Many-to-one** — (1,6), (2,5), (3,4), (4,3), (5,2), (6,1) all map to the same number 7
- **Definition (after)** — a random variable is any rule that assigns one number to each outcome

*Example (italic):* The roll (3,4) and the roll (6,1) look different on the table, but the rule "add them" gives both the same value: X = 7.

**Key point:** The dice are random; the rule is not. A random variable is a fixed number-assigning rule applied to a random outcome.

### Visualization (canvas `c1`, 720×300)

A 6×6 grid of all 36 dice pairs, each cell showing its sum, with the six sum-7 cells highlighted and two example mappings called out on the right.

- **Title (bold 15px, `#1a5276`, top center):** "36 Dice Pairs, One Rule: X = die A + die B".
- **Data:** cell (row a, col b) for a = 1..6, b = 1..6 shows the number a+b (values 2 through 12); the six cells where a+b = 7 are (1,6), (2,5), (3,4), (4,3), (5,2), (6,1).
- **Grid:** 6 columns × 6 rows of 28px cells; top-left cell corner at x=100, y=64 (grid spans to x=268, y=232); 1px `#e5e9ef` cell borders; default cell fill `#f4f6f9` with 12px `#2c3e50` sum text centered; sum-7 cells fill `rgba(0,131,0,0.25)` with bold 12px `#008300` text; cell (2,2) (sum 4) outlined 2px blue `#2a78d6`.
- **Headers:** column labels "1".."6" (die B) 12px `#6b7280` centered above each column at y=56 with "die B →" bold 12px `#1a5276` above them at y=42; row labels "1".."6" (die A) 12px `#6b7280` right-aligned at x=92; "die A ↓" bold 12px `#1a5276` rotated or stacked left of the rows at x=68.
- **Right annotation block (from x=330):** green bold 13px "(3,4) → X = 7" at y=100 and "(6,1) → X = 7" at y=124, each with a 2px green arrow pointing to its grid cell; blue bold 13px "(2,2) → X = 4" at y=160 with a 2px blue arrow to cell (2,2); mute 12px `#6b7280` line at y=200: "one rule, 36 inputs, 11 possible values".
- **Caption (12px `#444`, bottom center y=285):** "green diagonal: the 6 pairs that all become X = 7".

## Counting the 36 Ways

**Tags:** `worked example` (blue), `distribution` (green)

- **List the ways** — sum 2 has 1 pair, sum 3 has 2 pairs, and so on up to sum 7 with 6 pairs
- **The counts** — ways for sums 2–12 are 1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1; they total 36
- **Probabilities** — divide by 36: P(X = 7) = 6/36 ≈ 0.17 and P(X = 2) = 1/36 ≈ 0.03
- **The distribution** — the full list of X's values with their probabilities is X's distribution
- **Check by hand** — every bar can be re-derived by listing pairs on paper; nothing is hidden

*Example (italic):* Sum 10 comes from exactly (4,6), (5,5), and (6,4) — three pairs — so P(X = 10) = 3/36 ≈ 0.08.

**Key point:** Once a rule turns outcomes into numbers, counting outcomes becomes a distribution — the triangle shape falls straight out of the 36 pairs.

### Visualization (canvas `c2`, 720×300)

Bar chart of the distribution of X: one bar per sum 2–12, with the count of pairs above each bar and the sum-7 bar highlighted.

- **Title (bold 15px, `#1a5276`, top center):** "How Often Each Sum Appears (out of 36 pairs)".
- **Data:** sums `[2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]`; counts `[1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1]`.
- **Axes:** origin x=60, plot width 600, baseline y=240, chart height 175, y scale 0–6.5; 1px `#999` axis lines; sum labels 12px `#444` centered below the baseline; y-axis label "pairs" 12px `#6b7280`.
- **Bars:** 11 bars evenly spaced with ~10px gaps, fill `rgba(42,120,214,0.45)` with 1px `#2a78d6` stroke; the sum-7 bar instead fill `rgba(0,131,0,0.4)` with 1px `#008300` stroke; each bar's count in 12px `#444` just above it, except the 7 bar which gets bold 12px `#008300` "6".
- **Annotation (bold 13px, `#008300`, upper right area near x=470, y=70):** "P(X = 7) = 6/36 ≈ 0.17 — the most likely sum".
- **Caption (12px `#444`, bottom center y=290):** "counts 1..6..1 add up to all 36 pairs".

## Pricing a Carnival Bet

**Tags:** `where it's used` (blue), `expected value` (green), `illustrative` (orange)

- **The bet** — a stall charges $1 per roll and pays $4 whenever the sum is 7 or 11
- **Win ways** — sum 7 has 6 pairs and sum 11 has 2, so 8 of 36 pairs win: P(win) = 8/36 ≈ 0.22
- **New variable** — the payout Y is another random variable: Y = $4 on 8 pairs, $0 on the other 28
- **Average payout** — E[Y] = $4 × 8/36 ≈ $0.89, so each $1 roll loses about 11 cents on average
- **Everywhere** — revenue per user, clicks per ad, and defects per batch are all random variables

*Example (italic):* Over 360 rolls the stall expects about 80 wins paying $320 total against $360 collected — the distribution priced the game.

**Key point:** You can only average, price, or model something after a rule turns outcomes into numbers — that is the whole job of a random variable.

### Visualization (canvas `c3`, 720×300)

Dual-panel chart: win/lose split of the 36 pairs (left) and the $1 stake next to the $0.89 expected payout (right), split by a vertical dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "The $1 Bet on Sum 7 or 11: Who Wins on Average? (illustrative)".
- **Left panel (win/lose counts):** two bars labeled "win (7 or 11)" and "lose" with counts `[8, 28]`; axis origin x=60, plot width 260, baseline y=235, chart height 160, y scale 0–30; win bar fill `rgba(0,131,0,0.4)` stroke `#008300`, lose bar fill `rgba(213,81,129,0.35)` stroke `#d55181`; counts "8" and "28" bold 12px above the bars in their stroke colors; caption 12px `#444` below: "8 of 36 pairs win: P(win) ≈ 0.22".
- **Right panel (dollars):** two bars labeled "stake" and "expected payout" with values `[1.00, 0.89]`; axis origin x=410, plot width 260, same baseline/height, y scale $0–$1.10; stake bar fill `rgba(42,120,214,0.45)` stroke `#2a78d6` labeled "$1.00" bold 12px blue; payout bar fill `rgba(217,89,38,0.5)` stroke `#d95926` labeled "$0.89" bold 12px orange; orange bold 13px annotation above the gap: "≈ 11¢ house edge per roll".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.
- **Caption (12px `#444`, bottom center y=290):** "E[Y] = $4 × 8/36 ≈ $0.89 — computed from the distribution, before any roll".

## A Rule, Not a Number

**Tags:** `common mistake` (red), `notation` (orange)

- **Not the outcome** — the outcome is the pair (3,4); the random variable is the adding rule itself
- **Not one number** — X has no value until a roll happens; "X = 7" describes one realization
- **Capital vs small** — capital X names the rule; lowercase x = 7 is a value it took on one roll
- **Many rules, one game** — the same 36 pairs also support M = "the larger face", a different variable
- **Different shapes** — X's distribution is a triangle peaking at 7; M's climbs steadily up to 6

*Example (italic):* On the roll (3,4), the sum gives X = 7 while the max gives M = 4 — one outcome, two random variables, two numbers.

**Common mistake:** Treating "the random variable" as the experiment itself. The dice are the experiment; sum and max are two different variables riding on the same 36 outcomes.

### Visualization (canvas `c4`, 720×300)

Dual-panel bar chart: the distribution of X = sum (left) next to the distribution of M = max (right), built from the same 36 pairs, split by a dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "Same 36 Pairs, Two Random Variables".
- **Left panel (X = sum):** values 2–12 with counts `[1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1]`; axis origin x=55, plot width 285, baseline y=235, chart height 160, y scale 0–12; bars fill `rgba(42,120,214,0.45)` stroke `#2a78d6`; value labels 11px `#444` below the baseline; blue bold 12px annotation "X = sum: triangle, peak at 7"; caption 12px `#444` "counts out of 36".
- **Right panel (M = max):** values 1–6 with counts `[1, 3, 5, 7, 9, 11]`; axis origin x=405, plot width 285, same baseline/height, y scale 0–12; bars fill `rgba(74,58,167,0.4)` stroke `#4a3aa7`; value labels 11px `#444` below the baseline; violet bold 12px annotation "M = max: climbs to 11/36 at 6"; caption 12px `#444` "counts out of 36".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.
- **Takeaway (bold 13px `#d55181`, bottom center y=290):** "one experiment, two rules, two different distributions".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- All chart data is hardcoded literal arrays — no `Math.random()`; the carnival-bet dollar figures are labeled "illustrative" in the c3 title.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
