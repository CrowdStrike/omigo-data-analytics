# Proof by Contradiction

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Proof by Contradiction

**Subtitle:** To prove something is true, assume it is false and follow the logic honestly — when the logic collapses into an impossibility, the collapse itself is the proof

## The Sales Claim That Couldn't Add Up

**Tags:** `core idea` (blue), `assume the opposite` (orange), `worked example` (green)

- **The claim** — a teammate insists all 5 of yesterday's orders were under $30 each
- **The record** — the dashboard shows a $40 average, so the 5 orders total 5 × $40 = $200
- **Step 1, negate** — assume the claim holds: every one of the 5 orders is below $30
- **Step 2, derive** — even at a full $30 apiece the 5 orders reach only 5 × $30 = $150
- **Step 3, conclude** — $150 = $200 is impossible, so some order must have hit $30 or more
- **Nothing was found** — you never located the big order; you proved it exists from the wreckage

*Example (italic):* To prove "some order hit $30 or more", assume none did — arithmetic then demands 150 = 200, which is absurd.

**Key point:** Negate the claim, derive an impossibility, conclude the negation was false. Correct logic cannot squeeze an impossibility out of a truth, so the thing you assumed cannot be true.

### Visualization (canvas `c1`, 720×300)

Dual-panel bar chart: the five assumed orders capped at $30 (left) vs the totals they can and must reach (right), split by a vertical dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "Assume All 5 Orders Are Under $30 — Then Add Them Up".
- **Data:** five orders each drawn at the $30 ceiling `[30, 30, 30, 30, 30]`; right panel bars are the summed ceiling and the average-implied total, both computed in JS (`5 × 30 = 150`, `5 × 40 = 200`) and printed from the computed values.
- **Left panel (orders):** axis origin x=55, width 280, baseline y=245, chart height 185, y scale 0–50; five bars labeled O1–O5 (12px `#444` below baseline), fill `rgba(42,120,214,0.45)`; dashed blue `#2a78d6` line at $30 labeled bold 12px "claim ceiling: $30"; dashed orange `#d95926` line at $40 labeled bold 12px "recorded average: $40"; caption 12px `#444` "best case for the claim: every order at the ceiling".
- **Right panel (totals):** axis origin x=400, width 280, same baseline/height, y scale 0–220; bar "$150" fill `rgba(42,120,214,0.45)` with bold 13px blue value label above; bar "$200" fill `rgba(217,89,38,0.5)` with bold 13px orange label; magenta `#d55181` bold 13px annotation between the bar tops printing the computed gap "$50 missing — impossible"; caption 12px `#444` "5 × $40 average = $200 required".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.

## Why √2 Can Never Be a Fraction

**Tags:** `classic proof` (blue), `worked example` (green)

- **The target** — a fraction a/b whose square is exactly 2 would make √2 an ordinary fraction
- **Near misses** — (3/2)² = 2.25, (7/5)² = 1.96, (17/12)² ≈ 2.0069: they bracket 2, never land on it
- **Assume success** — suppose √2 = a/b where a/b is already reduced to lowest terms
- **First domino** — squaring gives a² = 2b², so a² is even, and only an even a squares to even
- **Second domino** — write a = 2k; then 2b² = 4k², so b² = 2k² and b must be even too
- **The absurdity** — a and b both even contradicts "lowest terms", so no such fraction exists

*Example (italic):* (99/70)² = 9801/4900 = 2.000204 — you can get as close to 2 as you like, but the proof says you can never land on it.

**Key point:** The proof never inspects a single candidate fraction. One assumption plus four honest steps rules out all infinitely many of them at once — that is the power of the method.

### Visualization (canvas `c2`, 720×300)

Two-panel figure: dot plot of near-miss fractions squared (left) and the four-step contradiction chain as stacked flow boxes (right), split by a dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "Assume √2 = a/b: Every Road Leads to 'Both Even'".
- **Data (left):** numerators `[3, 7, 17, 41, 99]` and denominators `[2, 5, 12, 29, 70]`; each squared value and its printed label are computed in JS as `(n/d)²` — `2.25, 1.96, 2.00694, 1.99881, 2.00020` — never hardcoded as text.
- **Left panel (near misses):** axis origin x=55, width 280, baseline y=245, chart height 185, y range 1.90–2.30; five blue `#2a78d6` 6px dots at equal x spacing, fraction labels 12px `#444` below baseline, computed squared value 11px `#444` beside each dot; dashed green `#008300` target line at y-value 2 labeled bold 12px "exactly 2"; magenta `#d55181` bold 12px annotation "close, never exactly 2"; caption 12px `#444` "each fraction squared, zoomed near 2".
- **Right panel (flow):** four rounded boxes at x=395, width 290, height 38, tops at y=52, 104, 156, 208, connected by short ink `#1a5276` arrows; texts 12px centered: box 1 (blue border `#2a78d6`) "assume √2 = a/b in lowest terms"; box 2 (ink border) "a² = 2b² → a is even, so a = 2k"; box 3 (ink border) "then b² = 2k² → b is even too"; box 4 (red border `#e74c3c`, fill `rgba(231,76,60,0.08)`, bold) "both even ⇒ not lowest terms ✗".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.

## Euclid's Trap for the Last Prime

**Tags:** `classic proof` (blue), `number theory` (orange), `worked example` (green)

- **The assumption** — suppose the primes are finite, and 2, 3, 5, 7, 11, 13 is the complete list
- **The troublemaker** — multiply them all to get 30,030, then add 1 to build N = 30,031
- **Remainder 1** — every listed prime divides 30,030 exactly, so each leaves remainder 1 on N
- **No escape** — N must have some prime factor, yet no prime on the "complete" list divides it
- **The absurdity** — 30,031 = 59 × 509, two primes the list forgot; no finite list ever survives
- **Honest step** — the proof needs "every integer above 1 has a prime factor", proved separately

*Example (italic):* The trap is generic: hand Euclid any finite list of primes and "multiply all, add 1" manufactures a number their list cannot factor.

**Key point:** The proof does not say the product-plus-one is always prime (30,031 is not). It says its prime factors — here 59 and 509 — cannot be on the assumed list, which is contradiction enough.

### Visualization (canvas `c3`, 720×300)

Single-panel flow: the "complete" prime list as chips, the product-plus-one construction, the remainder-1 row, and the contradiction box.

- **Title (bold 15px, `#1a5276`, top center):** "The 'Complete' List 2…13 Builds Its Own Counterexample".
- **Data:** primes `[2, 3, 5, 7, 11, 13]`; the product (30,030), N (30,031), each remainder (all 1) and the factor check 59 × 509 are all computed in JS from the prime array and printed from those computed values.
- **Prime chips (y=62):** heading bold 12px `#444` at x=70, y=48: "assumed complete list of primes:"; six rounded chips 44×26, blue border `#2a78d6`, fill `rgba(42,120,214,0.12)`, bold 13px blue centered labels "2" "3" "5" "7" "11" "13", starting x=70 with 56px spacing.
- **Construction line (y=118):** bold 13px ink `#1a5276` centered at x=360, built from the computed product: "2 × 3 × 5 × 7 × 11 × 13 = 30,030 → N = 30,030 + 1 = 30,031".
- **Remainder row (y=158):** heading 12px `#444` "divide N by each listed prime:"; under each chip's x-position, orange `#d95926` bold 12px label "rem 1" taken from the computed `N % p`, with a thin ink arrow from the construction line down to the row.
- **Contradiction box (x=110, y=196, width 500, height 44):** red border `#e74c3c`, fill `rgba(231,76,60,0.08)`; bold 13px `#e74c3c` centered text from the computed factors "30,031 = 59 × 509 — both prime, neither on the list ✗".
- **Takeaway (bold 13px magenta `#d55181`, centered at y=278):** "any finite list of primes falls into the same trap — so the primes never end".

## No Compressor Can Shrink Every File

**Tags:** `worked example` (green), `counting argument` (orange), `where it's used` (blue)

- **The promise** — a lossless compressor that makes every possible file at least one bit smaller
- **The inputs** — every 8-bit file must be handled, and there are 2⁸ = 256 of them
- **The outputs** — files of 7 bits or fewer number 1 + 2 + 4 + 8 + 16 + 32 + 64 + 128 = 255
- **Assume it works** — all 256 inputs land in those 255 slots, each still uniquely decodable
- **The absurdity** — 256 items into 255 slots forces a collision, so one file cannot be recovered
- **The conclusion** — at least one 8-bit file must grow, and the shortfall is exactly 1 at every length

*Example (italic):* At 16 bits the same count runs 65,536 inputs against 65,535 shorter strings — the shortfall is 1 again, so the argument never softens.

**Key point:** Real compressors shrink *typical* files because typical files are a tiny corner of all possible files. Shrinking *every* file is not hard engineering, it is arithmetically impossible.

### Visualization (canvas `c4`, 720×300)

Two-panel figure: bar chart of how many strings exist at each output length below 8 bits (left) and the counting verdict (right), split by a dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "256 Eight-Bit Files, Only 255 Shorter Slots".
- **Data:** lengths `0…7` with counts computed in JS as `2^k` → `[1, 2, 4, 8, 16, 32, 64, 128]`; the total (255) is computed by reduce, the input count (256) as `2^8`, and the deficit as their difference. Every printed number comes from these computations.
- **Left panel (slots per length):** axis origin x=55, width 280, baseline y=238, chart height 168, y scale 0–128 from the computed max; eight bars fill `rgba(42,120,214,0.45)`, blue `#2a78d6` border; count printed bold 11px `#444` above each bar; x labels 12px `#444` "0"…"7" below baseline; axis caption 12px `#444` "output length in bits (must be ≤ 7 to shrink)"; y-axis note bold 12px blue "count = 2^length".
- **Right panel (verdict):** heading bold 13px ink at x=400, y=58 "count both sides:"; two lines 13px `#2c3e50` at y=88 and y=112, printed from computed values: "shorter slots: 1+2+…+128 = 255" and "8-bit files to store: 2⁸ = 256"; a green `#008300` 13px line at y=142 "deficit = 256 − 255 = 1" with the 1 computed; red box (x=395, y=166, width 290, height 52, border `#e74c3c`, fill `rgba(231,76,60,0.08)`) with bold 12px `#e74c3c` two-line text "two files must share one output ✗" / "so at least one 8-bit file grows"; magenta `#d55181` bold 12px caption at y=252 "pigeonhole: more items than slots, no exceptions".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.

## Contradiction Is Not Contrapositive

**Tags:** `common confusion` (red), `logic` (blue), `worked example` (green)

- **The claim** — "if n² is even then n is even" can be proved two ways that look alike but differ
- **Contrapositive** — assume n is odd, write n = 2k+1, get n² = 2(2k²+2k)+1, which is odd; done
- **Contradiction** — assume n² is even *and* n is odd, derive n² odd, which fights the assumption
- **The difference** — contrapositive proves a fresh implication; contradiction needs an explicit clash
- **The sloppiness** — labelling a contrapositive proof "by contradiction" hides that no clash was used
- **The fatal swap** — "if not P then not Q" is the inverse, not the contrapositive, and can be false

*Example (italic):* Take P = "n is divisible by 4" and Q = "n is even": P → Q holds, but the inverse fails at n = 6, which is even yet not divisible by 4.

**Key point:** Only the contrapositive "not Q → not P" is equivalent to "P → Q". The converse and the inverse are different claims, and a proof that quietly swaps in one of them proves nothing.

### Visualization (canvas `c5`, 720×300)

Four-box logic square with a computed counterexample row underneath.

- **Title (bold 15px, `#1a5276`, top center):** "P → Q Survives Only One Rewrite".
- **Data:** P is "divisible by 4", Q is "even"; the truth of each of the four forms and the smallest counterexample are found in JS by scanning n = 1…40 for a number that is even but not divisible by 4 — the scan returns 6, which is printed.
- **Boxes (300 wide, 64 tall):** top-left x=45 y=62 and top-right x=375 y=62; bottom-left x=45 y=150 and bottom-right x=375 y=150.
- **Top-left (blue border `#2a78d6`, fill `rgba(42,120,214,0.10)`):** bold 12px "P → Q: divisible by 4 ⇒ even" with 11px `#6b7280` second line "the original claim — TRUE".
- **Top-right (green border `#008300`, fill `rgba(0,131,0,0.07)`):** bold 12px "¬Q → ¬P: odd ⇒ not divisible by 4" with 11px `#6b7280` second line "contrapositive — equivalent, TRUE".
- **Bottom-left (red border `#e74c3c`, fill `rgba(231,76,60,0.08)`):** bold 12px "Q → P: even ⇒ divisible by 4" with 11px `#6b7280` second line "converse — NOT equivalent, FALSE".
- **Bottom-right (red border `#e74c3c`, fill `rgba(231,76,60,0.08)`):** bold 12px "¬P → ¬Q: not divisible by 4 ⇒ odd" with 11px `#6b7280` second line "inverse — NOT equivalent, FALSE".
- **Counterexample row (bold 13px magenta `#d55181`, centered y=246):** printed from the scan "n = 6: even ✓, divisible by 4 ✗ — kills both red boxes".
- **Footer (12px `#444`, centered y=274):** "the contradiction version adds the clash; the contrapositive never needs one".

## Impossibility Results, Lower Bounds, and One Way to Fool Yourself

**Tags:** `where it's used` (blue), `lower bound` (orange), `common mistake` (red)

- **Impossibility results** — assume the perfect tool exists, then build the input that makes it wrong
- **Halting checker** — a program that asks the checker about itself and then does the opposite ✗
- **Lower bounds** — sorting 5 items must pick one of 5! = 120 orders; 6 comparisons split 2⁶ = 64 ways
- **The count wins** — 64 < 120 kills 6 comparisons, and 2⁷ = 128 ≥ 120 makes 7 the floor
- **Invariants** — assume the queue length went negative, walk the code, find no line that could do it
- **Two ways to fool yourself** — clash with an unproved assumption, or negate the claim incorrectly

*Example (italic):* The opposite of "all 5 orders were under $30" is "at least one was $30 or more" — the list 12, 18, 25, 45, 100 (total $200, average $40) satisfies it with just two big orders.

**Common mistake:** If the "impossibility" only conflicts with something you assumed but never proved — normal residuals, a stable schema, no duplicate rows — you have found a wrong assumption, not a theorem.

### Visualization (canvas `c6`, 720×300)

Two-panel figure: the comparison-sort lower bound as bars against the permutation count (left) and correct-vs-wrong negation of the order claim (right), split by a dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "Counting Forces a Floor; Sloppy Negation Removes It".
- **Data (left):** comparison budgets `c = 1…7` with outcome counts computed in JS as `2^c` → `[2, 4, 8, 16, 32, 64, 128]`; the target `5! = 120` is computed by a factorial loop; the minimum budget is found by scanning for the first `c` with `2^c ≥ 120`, which returns 7 and is printed in the annotation.
- **Left panel (bars):** axis origin x=55, width 280, baseline y=238, chart height 168, y scale 0–140; bars fill `rgba(42,120,214,0.45)` for budgets that fall short and `rgba(0,131,0,0.35)` with green border for those that reach the target, decided by comparing to the computed 120; computed value printed bold 11px above each bar; x labels 12px "1".."7" and caption 12px `#444` "yes/no comparisons allowed".
- **Left annotations:** dashed orange `#d95926` line at the computed 120 labeled bold 12px "5! = 120 orders to tell apart"; magenta `#d55181` bold 12px note "minimum = 7 comparisons" positioned above the first qualifying bar, with the 7 taken from the scan.
- **Right panel (negation boxes):** three boxes x=395, width 290, height 46, tops y=54, 118, 182; box 1 ink border `#1a5276` 12px "claim: all 5 orders were under $30"; box 2 red border `#e74c3c`, fill `rgba(231,76,60,0.08)`, bold 12px two lines "wrong opposite: all 5 were $30 or more" / "too strong — proves the wrong thing"; box 3 green border `#008300`, fill `rgba(0,131,0,0.07)`, bold 12px two lines "right opposite: at least one was $30+" / "computed: 2 of 5 orders qualify".
- **Right caption (12px `#444`, centered y=252):** printed from the literal order array `[12, 18, 25, 45, 100]` with its sum and mean computed in JS: "orders 12, 18, 25, 45, 100 — total 200, average 40".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then six `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- All data is hardcoded literal arrays with derived quantities (sums, products, powers, factorials, squares, remainders, minima) computed in JS at draw time — no randomness anywhere. This page has no links.
