# Zero-Cost Abstractions

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Zero-Cost Abstractions

**Subtitle:** A zero-cost abstraction is a high-level shortcut the compiler unfolds into exactly the machine steps you would have written by hand — so the clear way to say it runs just as fast as the tedious way

## One Receipt, Two Ways to Add It Up

**Tags:** `core idea` (blue), `compiler` (green), `same machine steps` (orange)

- **The receipt** — a grocery app must add 8 item prices to show the total: 22.09
- **The long way** — a hand-written loop: point at the first price, add it, move to the next, repeat
- **The short way** — one high-level line, `total = sum(prices)`, that only says what you want
- **The compiler** — it unfolds the short line into exactly the same 32 machine steps as the loop
- **Zero cost** — the shortcut vanishes before the program ever runs, so clarity costs nothing

*Example (italic):* The one-line sum and the six-line loop compile to the same 32 machine steps — the machine cannot tell which one you wrote.

**Key point:** A zero-cost abstraction is a high-level way of writing something that compiles down to what you would have hand-written — the convenience disappears before the program starts.

### Visualization (canvas `c1`, 720×300)

Convergence diagram: two source-code boxes on the left (hand loop and one-liner) with arrows meeting at a single "what the machine runs" box on the right, showing both collapse to the same 32 steps.

- **Title (bold 15px, `#1a5276`, top center):** "Two Ways to Write It, One Thing the Machine Runs".
- **Box A (hand loop):** rounded rect x=40, y=55, w=260, h=100; fill `rgba(42,120,214,0.10)`, 2px blue `#2a78d6` border; bold 12px blue header "the long way — a hand loop"; below it 11px monospace `#2c3e50` lines: `total = 0` / `for each price:` / `  total = total + price` / `show total`.
- **Box B (one-liner):** rounded rect x=40, y=170, w=260, h=60; fill `rgba(0,131,0,0.10)`, 2px green `#008300` border; bold 12px green header "the short way — one line"; 11px monospace line: `total = sum(prices)`.
- **Arrows:** two 3px `#6b7280` arrows with arrowheads, from the right edge of Box A (y≈105) and Box B (y≈200) converging to the left edge of the machine box (x=440, y≈150); 11px `#6b7280` label "compiler unfolds it" between them near x=350, y=140.
- **Machine box:** rounded rect x=440, y=95, w=240, h=110; fill `#f8f9fa`, 2px ink `#1a5276` border; centered bold 13px ink lines: "what the machine runs" / "32 steps" / "(4 steps × 8 prices)".
- **Annotation (bold 12px orange `#d95926`, near x=440, y=245):** "the shortcut is gone before the program starts".
- **Caption (12px `#444`, bottom right):** "illustrative — machine steps simplified for counting".

## Counting the Machine's Actual Steps

**Tags:** `worked example` (blue), `step counting` (green)

- **Per price** — the machine does 4 steps for each item: load the price, add it, move on, check if done
- **The loop** — 8 prices × 4 steps = 32 steps to reach the total of 22.09
- **The one-liner** — `sum(prices)` compiles to the identical 4 steps per price: 32 steps again
- **A costly version** — if each price sits in a box the machine must open first, it is 6 × 8 = 48 steps
- **The gap** — 48 vs 32 is a 50% tax, and it came from the abstraction, not from your loop-writing skill

*Example (italic):* prices = [3.50, 1.20, 4.75, 2.00, 0.99, 5.25, 1.80, 2.60] sum to 22.09 — and anyone can redo 8 × 4 = 32 and 8 × 6 = 48 by hand.

**Key point:** Hand loop = 32 steps, sum() = 32 steps, boxed version = 48 — "zero-cost" means the step count matches the hand-written loop exactly.

### Visualization (canvas `c2`, 720×300)

Single-panel vertical bar chart: machine steps to add the 8 prices for three versions — hand loop, high-level sum, and boxed prices — with a dashed baseline at 32 making the identical bars obvious.

- **Title (bold 15px, `#1a5276`, top center):** "Adding 8 Prices: Machine Steps by Version".
- **Axes:** origin x=70, baseline y=245, plot width 590, plot height 185; y = steps 0 to 50 with light `#e5e9ef` gridlines at 10, 20, 30, 40, 50 and 12px `#444` tick labels.
- **Bars (120px wide, centered at x = 175, 375, 575), values `[32, 32, 48]`:** "hand loop" fill `rgba(42,120,214,0.35)` border 2px `#2a78d6`; "sum(prices)" fill `rgba(0,131,0,0.30)` border 2px `#008300`; "boxed prices" fill `rgba(217,89,38,0.30)` border 2px `#d95926`; bold 13px value labels "32", "32", "48" above each bar in the bar's border color; 12px `#444` category labels below the baseline.
- **Baseline guide:** horizontal dashed `#6b7280` (dash 4/3) line across the plot at 32 steps.
- **Annotation (bold 12px green `#008300`, near x=250, y=105):** "same 32 steps — the abstraction cost nothing".
- **Caption (12px `#444`, bottom right):** "illustrative step counts — 4 per price, +2 to unbox".

## When "High Level" Stopped Meaning "Slow"

**Tags:** `where it's used` (blue), `history` (green), `write it clearly` (orange)

- **The old rule** — in the interpreter era a high-level line really did run about 30× slower than hand code
- **Early VMs** — smarter runtimes cut the tax to roughly 3×, but "high level" still meant "slower"
- **Modern compilers** — inlining and specialization unfold the shortcut completely: 1×, the same steps
- **For data work** — you can write sum / filter / map pipelines without paying a loop-by-hand penalty
- **The payoff** — the readable version and the fast version are finally the same version
- **The fine print** — true for compiled code; a plain interpreted Python loop still pays the tax

*Example (italic):* The same one-line sum cost about 30× in 1985, about 3× in 2000, and 1× today — the tax on clarity fell to zero.

**Key point:** "High level = slow" was a fact about old tools, not about high-level code — modern compilers made the clear way and the fast way identical.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: three eras on a shared "slowdown vs hand-written" axis, the bar shrinking from 30× to 3× to 1× as compilers learned to unfold abstractions.

- **Title (bold 15px, `#1a5276`, top center):** "The Clarity Tax Over Time: Slowdown vs Hand-Written Code".
- **Axis:** horizontal 2px `#999` line at y=250 from x=230 to x=680 (width 450), slowdown 0× to 32×; 12px `#444` tick labels "0×", "8×", "16×", "24×", "32×" below.
- **Rows (18px-tall rounded bars starting at x=230, centered at y = 95, 155, 215), values `[30, 3, 1]`, each with a left-aligned 12px `#444` label at x=20:**
  - "1985 — interpreted era": bar to 30×, fill `rgba(217,89,38,0.35)`, border 2px orange `#d95926`
  - "2000 — early VMs": bar to 3×, fill `rgba(201,133,0,0.35)`, border 2px yellow `#c98500`
  - "today — optimizing compilers": bar to 1×, fill `rgba(0,131,0,0.35)`, border 2px green `#008300`
- **Value labels:** bold 13px at each bar's right end in the bar's border color: "30×", "3×", "1× — zero cost".
- **Annotation (bold 13px green `#008300`, near x=420, y=215):** "the same high-level line, no longer slower".
- **Caption (12px `#444`, bottom right):** "illustrative magnitudes, not benchmarks".

## Zero-Cost Doesn't Mean Everything Is Free

**Tags:** `common mistake` (red), `still-costly features` (orange)

- **The claim's limit** — zero-cost means "no slower than hand-written", not "this feature is free"
- **Boxed values** — wrapping each price in a box adds an open-the-box step: 48 steps instead of 32
- **Dynamic dispatch** — asking "which add do I call?" at runtime for every item lands at 40 steps
- **The tell** — features that delay decisions to runtime cost steps; ones settled at compile time don't
- **The habit** — measure before assuming; the label "high level" no longer predicts the cost either way

*Example (italic):* Three ways to sum the same 8 prices: plain sum 32 steps, dynamic dispatch 40, boxed prices 48 — same answer, different bills.

**Common mistake:** Hearing "zero-cost abstractions" and assuming every high-level feature is free — only the ones the compiler can fully unfold at compile time are; conveniences that decide things at runtime still charge per item.

### Visualization (canvas `c4`, 720×300)

Horizontal bar chart: three summing features on a shared machine-step axis with a dashed guide at the 32-step hand-written baseline, separating truly zero-cost features from ones that still charge.

- **Title (bold 15px, `#1a5276`, top center):** "Same Total, Different Bills: 8 Prices, Three Features".
- **Axis:** horizontal 2px `#999` line at y=250 from x=230 to x=680 (width 450), machine steps 0 to 50; 12px `#444` tick labels "0", "10", "20", "30", "40", "50" below.
- **Rows (18px-tall rounded bars starting at x=230, centered at y = 95, 155, 215), values `[32, 40, 48]`, each with a left-aligned 12px `#444` label at x=20:**
  - "plain sum() — unfolded at compile time": bar to 32, fill `rgba(0,131,0,0.35)`, border 2px green `#008300`
  - "dynamic dispatch — pick the add at runtime": bar to 40, fill `rgba(201,133,0,0.35)`, border 2px yellow `#c98500`
  - "boxed prices — open each box first": bar to 48, fill `rgba(217,89,38,0.35)`, border 2px orange `#d95926`
- **Value labels:** bold 13px at each bar's right end in the bar's border color: "32", "40", "48".
- **Baseline guide:** vertical dashed `#6b7280` (dash 4/3) line at 32 steps from y=55 to the axis; 11px `#6b7280` label "hand-written baseline: 32" at its top.
- **Annotation (bold 12px violet `#4a3aa7`, near x=430, y=60):** "zero-cost only when the compiler decides, not the runtime".
- **Caption (12px `#444`, bottom right):** "illustrative step counts for the 8-price receipt".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all bar values, step counts, and prices are the hardcoded literal arrays above (no randomness); step counts follow the stated rule of 4 steps per price (+2 to unbox, +1 to dispatch) and the era slowdowns are illustrative magnitudes labeled as such in captions.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
