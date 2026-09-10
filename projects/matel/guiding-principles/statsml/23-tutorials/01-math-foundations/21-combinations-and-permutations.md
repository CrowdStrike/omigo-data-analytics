# Combinations & Permutations

**Page type:** detail page (tutorial card-sections: one `<h2>` per section, two-column `table.layout` — text left 50%, canvas right 50%)
**HTML title tag:** Combinations & Permutations

**Subtitle:** Two ways to count picks — a lock code cares about order, a pizza does not, and the whole difference is one division

## One Pizza, One Lock: Does Order Matter?

**Tags:** `core idea` (blue), `running example` (green)

- **The lock** — 7-2-9 opens it but 9-2-7 does not: every ordering is a different code
- **The pizza** — ham-onion-chili and chili-onion-ham are the same pizza on the plate
- **Count the lock** — 10 digits for slot 1, 9 left, then 8: 10 × 9 × 8 = 720 codes
- **Count the pizza** — same 720 picks, but each pizza got counted 6 times: 720 ÷ 6 = 120
- **The rule** — order matters: multiply and stop; order is noise: divide out the orderings

*Example (italic):* A lock has 720 three-digit codes with no repeated digit, but a menu offers only 120 three-topping pizzas from 10 toppings.

**Key point:** Permutations count ordered picks. Combinations take the same count and divide by the number of ways to shuffle the picks (here 3! = 6), because order is noise.

### Visualization (canvas `c1`, 720×300)

Split diagram: left half a funnel of orderings collapsing into one pizza, right half a two-bar comparison.

- **Title (bold 15px, `#1a5276`, top center):** "Same 3 Picks: 720 Lock Codes, 120 Pizzas".
- **Divider:** vertical dashed line (`#bdc3c7`, dash 4/3) at x=340 from y=38 to y=288.
- **Left panel:** six small boxes (78×22, fill `rgba(42,120,214,0.12)`, stroke blue `#2a78d6`) stacked at x=35, y=60+i·36, each holding a bold blue 12px label: `H-O-C`, `H-C-O`, `O-H-C`, `O-C-H`, `C-H-O`, `C-O-H`. Thin gray lines (`#b6c4d4`) from each box converge to a circle (radius 42) at (255,160), fill `rgba(0,131,0,0.14)`, stroke green `#008300` width 2, containing bold green text "1 pizza" and "{H, O, C}". Bold orange (`#d95926`) caption at (170,288): "3! = 6 orderings, one pizza".
- **Right panel bars:** baseline gray line at y=245 from x=375 to x=700; scale max 760 over 175px height; bars 90px wide at 0.75 alpha: x=400 value 720 blue `#2a78d6` labeled "720" bold above, captions below "lock codes" / "(order matters)"; x=560 value 120 green `#008300` labeled "120", captions "pizzas" / "(order ignored)".
- **Divide arrow:** orange (`#d95926`) line width 2 from (495, top-of-720-bar+30) to (553, top-of-120-bar−14) with filled orange arrowhead, labeled bold 13px "÷ 3! = 6" midway.

## Check It by Hand: 2 Toppings From 4

**Tags:** `worked example` (green), `rule of thumb` (blue)

- **The toppings** — mushroom (M), onion (O), pepper (P), olive (V); pick 2 of them
- **Ordered picks** — 4 choices for the first × 3 for the second = 12 ordered pairs
- **The doubles** — M-O and O-M land as the same pizza, so every pizza appears twice
- **Divide by 2** — 2 picks shuffle in 2! = 2 ways, so 12 ÷ 2 = 6 pizzas
- **List them** — MO, MP, MV, OP, OV, PV: exactly 6, matching the arithmetic

*Example (italic):* For 3 picks the divisor is 3! = 6; for 2 picks it is 2! = 2 — the divisor is always "ways to shuffle k picks".

**Key point:** "Choose k from n" is just: count ordered picks (n × (n−1) × … k terms), then divide by k! to merge the repeats.

### Visualization (canvas `c2`, 720×300)

4×4 matrix of ordered pairs plus a legend/summary panel.

- **Title (bold 15px, `#1a5276`, top center):** "12 Ordered Pairs, but Only 6 Distinct Pizzas".
- **Matrix:** letters M, O, P, V as row and column headers (bold 12px `#1a5276`); grid origin (120,72), 44px cells with 4px gap. Column axis label "second pick" above; rotated label "first pick" on the left (both 12px `#444`).
  - Diagonal cells (r==c): fill `#eceff3`, gray `#9aa4af` em-dash "—" (meaning no repeated topping).
  - Upper triangle (c>r): fill `rgba(0,131,0,0.18)`, stroke green `#008300` 1.5px, bold green pair labels (MO, MP, MV, OP, OV, PV).
  - Lower triangle (c<r): fill `rgba(217,89,38,0.14)`, orange `#d95926` pair labels (OM, PM, PO, VM, VO, VP).
- **Caption under matrix (12px `#444`):** "M mushroom · O onion · P pepper · V olive".
- **Right legend (starting x=380):** three swatches with 12px `#333` labels: green swatch "6 distinct pizzas"; orange swatch "6 repeats — same pizza, other order"; light-gray swatch "no repeated topping".
- **Right text:** bold green 13px "The 6 pizzas: MO, MP, MV, OP, OV, PV"; bold violet (`#4a3aa7`) 14px lines "4 × 3 = 12 ordered pairs" and "12 ÷ 2! = 6 pizzas"; 12px `#444` lines "each pizza shows up once above the diagonal" / "and once below it — divide by 2 to merge them".

## Where a Data Scientist Meets This Counting

**Tags:** `where it's used` (blue), `watch out` (orange)

- **Pairwise tests** — comparing every pair among 20 metrics means choose-2-from-20 = 190 tests
- **Multiple testing** — at a 5% false-alarm rate, 190 tests expect about 9 or 10 false alarms
- **Feature pairs** — interaction features between 50 columns create 1,225 new columns
- **Probability denominators** — P(one specific 3-topping pizza) = 1/120 needs the right count
- **Quadratic growth** — k items give k(k−1)/2 pairs; doubling the items roughly 4x the pairs

*Example (italic):* Going from 10 metrics to 20 metrics grows the pairwise comparisons from 45 to 190 — more than 4x.

**Key point:** Before running "all pairs" of anything, count the pairs first — the count grows with the square of the list, and every pair is another chance for a false alarm.

### Visualization (canvas `c3`, 720×300)

Bar chart of pairwise comparison counts vs number of metrics.

- **Title (bold 15px, `#1a5276`, top center):** "All-Pairs Comparisons Grow With the Square of the List".
- **Data:** k = `[3, 5, 10, 20]` metrics → pairs = `[3, 10, 45, 190]`.
- **Axes/scale:** baseline gray line at y=240 from x=95 to x=665; chart height 170px; y scale max 200; bars 84px wide (min drawn height 4px), 0.75 alpha.
- **Bar colors (one per bar):** aqua `#199e70`, blue `#2a78d6`, violet `#4a3aa7`, magenta `#d55181`.
- **Labels:** value bold 13px above each bar in the bar's color (last bar reads "190 tests"); below each bar 12px `#222` "3 metrics", "5 metrics", "10 metrics", "20 metrics".
- **X caption (12px `#444`, bottom center):** "number of metrics being compared pairwise — pairs = k(k−1)/2".
- **Annotation (bold magenta 13px at (400,78)):** "20 metrics → 190 tests → ~9-10 false alarms at a 5% rate".

## The Mix-Up: a "Combination Lock" Counts Permutations

**Tags:** `common mistake` (red), `rule of thumb` (blue)

- **The name lies** — a combination lock cares about order, so it actually counts permutations
- **The symptom** — a mixed-up answer is off by exactly k!: 6x for triples, 2x for pairs
- **Ask one question** — "if I shuffle my picks, do I get a different outcome?"
- **Yes → permutation** — lock codes, podium finishes, ranked lists: keep all 720
- **No → combination** — pizza toppings, lottery balls, committees: divide down to 120

*Example (italic):* 720 and 120 differ by exactly 3! = 6 — the number of ways to shuffle three picks.

**Common mistake:** Picking the formula by name instead of by the shuffle question — the everyday word "combination" points at the wrong formula.

### Visualization (canvas `c4`, 720×300)

Decision-flow diagram: one question box branching to two answer boxes.

- **Title (bold 15px, `#1a5276`, top center):** "One Question Picks the Formula".
- **Question box:** 330×40 centered at top (y=48), fill `rgba(74,58,167,0.10)`, stroke violet `#4a3aa7` width 2, bold violet 13px text: "Shuffle the picks — is it a different outcome?".
- **Branches:** two 270×92 boxes at y=140 (x=60 blue, x=390 green), faint fill `rgba(0,0,0,0.02)`, 2px colored stroke, connected by arrows from the question box with bold 12px edge labels "YES" (blue) and "NO" (green):
  - Blue `#2a78d6` box: bold "PERMUTATION — keep every ordering"; 12px `#333` "lock codes, podium finishes, ranked lists"; bold blue "10 × 9 × 8 = 720 codes".
  - Green `#008300` box: bold "COMBINATION — divide out orderings"; 12px `#333` "pizza toppings, lottery balls, committees"; bold green "720 ÷ 3! = 120 pizzas".
- **Bottom annotation (bold orange `#d95926` 13px, center, y=270):** "a \"combination lock\" answers YES — the everyday name points at the wrong formula".

## Regeneration instructions

- **Template:** tutorials topic-page layout (see `tutorials/CLAUDE.md` and `most-powerful-signals/07-social-graph-connections.html` skeleton). h1 (no index number) + `.subtitle`, then four `.card-section` blocks, each with an `<h2>` and a `table.layout` (`td.text-col` 50%, `td.viz-col` 50%, cells padded 12px, top-aligned).
- **Left column per section:** `.tags` pill row first, then a `<ul>` of one-line bullets each opening with `<b>bold term</b>` (bold terms in `#1a5276`), one italic `.example` paragraph, one `.key-point` callout.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with 2px `#2980b9` bottom border; section h2 1.3rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem. Canvas `width:100%`, 1px `#e0e0e0` border, 4px radius. `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem. `.example` italic `#555` 0.9rem. `ul` 0.92rem.
- **Tag pills:** `.tag` inline-block 0.72rem bold, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Chart palette object `P`:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette anchors: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- **Canvas:** all canvases 720×300 intrinsic, scaled with `window.devicePixelRatio` via a shared `setup(id)` helper (backing store sized to rendered width × `window.devicePixelRatio` (display capped via `style.maxWidth`), `ctx.scale` back to logical coordinates); data hardcoded as literal arrays, no `Math.random()`.
- No nav bar, no back/home links, no cross-page links. In regenerated HTML any card links use `.html` extensions.
