# Minimum-Balance Mathematics

**Page type:** detail page (h2 section per pitfall, each with a two-column obj-table row: text left 50%, canvas right 50%)
**HTML title tag:** 161. Minimum-Balance Mathematics

**Subtitle:** The "free" account that requires a minimum balance has a hidden cost: the foregone return on capital you must park. The requirement is a fee denominated in opportunity cost — invisible on any statement, real in every year's compounding.

## Callout (philosophy box)

**The fundamental problem:** A cost that never appears as a row in a fee table is systematically absent from every "cost of banking" metric computed from statements. A balance requirement is a real charge — rate × balance — but it is paid in return you never earn rather than dollars you can see leave. So the measured cost is not a noisy estimate of the true cost; it is the true cost minus a term, and that term is the larger one.

**Illustrative Example.** Every balance, rate, fee, and customer count below is constructed for arithmetic clarity. None is a measurement of any real institution or product.

## Shared scenario — reconciliation

One scenario runs through the whole page. Every figure on this page derives from these constants.

| Constant | Symbol | Value |
|---|---|---|
| Minimum balance to waive the fee | `MIN_BALANCE` | $1,500.00 |
| Stated monthly maintenance fee if not waived | `MONTHLY_FEE` | $5.00 |
| Alternative yield on the same capital | `RATE` | 4.00% / yr |
| Average balance actually parked (with buffer) | `PARKED` | $1,800.00 |
| Tenure used for the multi-year figures | `TENURE_YEARS` | 5 |
| Customers in the illustrative book | `CUSTOMERS` | 10,000 |

Derived, with the arithmetic that produces each:

| Quantity | Arithmetic | Value |
|---|---|---|
| Explicit annual fee | 12 × $5.00 | $60.00 |
| Implied annual fee at 4.00% | 0.0400 × $1,500.00 | $60.00 |
| Implied monthly equivalent | $60.00 / 12 | $5.00 |
| Break-even rate (implied = explicit) | $5.00 × 12 / $1,500.00 | 4.0000% |
| Implied fee at 0.50% | 0.0050 × $1,500.00 | $7.50 |
| Implied fee at 5.00% | 0.0500 × $1,500.00 | $75.00 |
| Buffered implied fee | 0.0400 × $1,800.00 | $72.00 |
| Buffer break-even rate | $60.00 / $1,800.00 | 3.3333% |
| Simple 5-year foregone return | 5 × 0.0400 × $1,500.00 | $300.00 |
| Compounded 5-year foregone return | $1,500.00 × (1.04⁵ − 1) | $324.98 |
| Compounding gap over 5 years | $324.98 − $300.00 | $24.98 (8.33%) |
| Fee payers in the book | 10,000 − 8,200 waived | 1,800 (18.0%) |
| Explicit fees collected | 1,800 × $60.00 | $108,000 |
| Balance-parkers in the book | 4,200 + 1,300 | 5,500 |
| Implied cost borne by parkers | 5,500 × $60.00 | $330,000 |
| True total cost of the book | $108,000 + $330,000 | $438,000 |
| Share of true cost that is visible | $108,000 / $438,000 | 24.66% |

## A Balance Requirement Is a Fee With No Row in the Fee Table

**$1,500 Parked at 4.00% Is Exactly the $5.00/Month It Waives**

- **The product:** Bank A waives its $5.00 monthly maintenance fee whenever the balance stays above $1,500.
- **The framing:** the account is marketed as free, and on a statement of charges it genuinely reads $0.00.
- **The conversion:** foregone return = rate × balance = 0.0400 × $1,500.00 = $60.00 per year.
- **Same units as the fee:** $60.00 / 12 = $5.00 per month, directly comparable to the stated $5.00 charge.
- **The exact equality:** at 4.00% the two branches cost the same $60.00/yr — the choice is a wash, not a saving.
- **The break-even rate:** solve r × $1,500 = $5.00 × 12 → r = $60.00 / $1,500.00 = 4.0000% exactly.
- **Why it is invisible:** the charge is a return that never arrives, so no ledger line records its absence.
- **The measurement claim:** any cost metric built from statement rows is missing this term by construction.

### Visualization (canvas `c1`, 720×340)

Two side-by-side cost columns — "pay the fee" vs "park the balance" — drawn to the same height because they are equal at the break-even rate, with the break-even arithmetic printed beneath. All values computed in JS from `MIN_BALANCE`, `MONTHLY_FEE`, `RATE`.

- **Title (bold 17px `#1a5276`, centered, y=22):** "Two Ways to Pay the Same $60.00" — subtitle (15px `#555`, y=44): "Illustrative Example — $1,500 minimum, $5.00/mo fee, 4.00% alternative yield".
- **Scale:** $1.00 = 2.2px of bar height; baseline y=250. Both bars are $60.00 → 132px, so both tops sit at y=118.
- **Left column (x=150, width 130):** rect (150,118) 130×132 in `#e74c3c`. Value label "$60.00" bold 17px `#e74c3c` centered above at y=110. Captions centered under baseline: "Explicit fee" bold `#1a5276` at y=272; "12 × $5.00" 15px `#555` at y=292; "appears on the statement" 15px `#555` at y=312.
- **Right column (x=440, width 130):** rect (440,118) 130×132 in `rgba(26,82,118,0.35)` with a 2px dashed (5/4) `#1a5276` outline to mark it as unbilled. Value label "$60.00" bold 17px `#1a5276` centered above at y=110. Captions: "Implied fee" bold `#1a5276` at y=272; "0.0400 × $1,500.00" 15px `#555` at y=292; "appears nowhere" bold 15px `#e74c3c` at y=312.
- **Equality bracket:** thin `#27ae60` 2px horizontal line at y=100 from x=150 to x=570 with 8px end ticks, labeled centered "identical cost at r = 4.00%" bold 15px `#27ae60` at y=92.
- **Break-even readout (15px, left aligned at x=60, y=68):** "Break-even rate = $5.00 × 12 / $1,500.00 = 4.0000%" in `#1a5276`.

## The Implied Fee Moves With the Prevailing Rate While the Product Stands Still

**Same Account, Same Requirement: $7.50/yr at 0.50%, $75.00/yr at 5.00%**

- **The dependence:** the implied fee is rate × balance, and only one of those two terms is set by the bank.
- **Low-rate regime:** at 0.50% the requirement costs 0.0050 × $1,500.00 = $7.50 per year — nearly nothing.
- **High-rate regime:** at 5.00% the same requirement costs 0.0500 × $1,500.00 = $75.00 per year.
- **The swing:** $75.00 / $7.50 = 10.0×, from a product whose terms and features never changed once.
- **Relative to the stated fee:** the implied charge is 12.5% of $60.00 at 0.50% and 125.0% of it at 5.00%.
- **The ranking flip:** below 4.00% parking beats paying; above 4.00% paying beats parking — same product.
- **Why the trend is spurious:** a "cost of banking fell" series can be entirely a rate cycle, not a market change.
- **The measurement claim:** a product's true price is not a property of the product alone, so cache neither.

### Visualization (canvas `c2`, 720×360)

Line chart of implied annual fee against prevailing rate, with the flat explicit-fee line crossing it at the computed break-even rate. Every plotted point and label computed in JS from `MIN_BALANCE` and `MONTHLY_FEE`.

- **Title (bold 17px `#1a5276`, centered, y=22):** "The Same Requirement, Priced by the Rate Cycle" — subtitle (15px `#555`, y=44): "Illustrative Example — implied fee = r × $1,500.00".
- **Axes:** x = rate 0.00%–6.00% mapped to px 100–660; y = annual cost $0–$100 mapped to py 290–80. Axis lines `#333` 1px. x ticks and labels at 0%, 1%, 2%, 3%, 4%, 5%, 6% (15px `#555`, y=308); y ticks and labels at $0, $25, $50, $75, $100 (15px `#555`, right aligned at x=92). Axis captions: "Prevailing rate on the same capital" centered at y=332; "Annual cost" rotated 90° at x=34.
- **Implied-fee line:** solid `#1a5276` 2.5px line from (0.00%, $0.00) to (6.00%, $90.00), computed as `MIN_BALANCE * r` sampled at 121 steps. Label "implied fee = r × $1,500" in `#1a5276` 15px, placed above the line near x=580.
- **Explicit-fee line:** dashed (6/4) `#e74c3c` 2.5px horizontal line at $60.00 across x=100–660, labeled "explicit fee = $60.00/yr" in `#e74c3c` 15px, left aligned at x=108, above the line.
- **Crossing marker:** 7px `#27ae60` dot at (4.00%, $60.00) — x from the computed break-even `MONTHLY_FEE*12/MIN_BALANCE` — with a vertical dashed (4/4) `#27ae60` 1.5px line down to the x-axis and label "break-even 4.0000%" bold 15px `#27ae60` above-right of the dot.
- **Regime shading:** `rgba(39,174,96,0.10)` rect from x=100 to the crossing x, spanning y=80–290, labeled "parking is cheaper" 15px `#27ae60` centered at y=100; `rgba(231,76,60,0.10)` rect from the crossing x to x=660, labeled "paying is cheaper" 15px `#e74c3c` centered at y=100.
- **Endpoint callouts (15px `#555`):** "$7.50 at 0.50%" beside a 5px `#555` dot at (0.50%, $7.50); "$75.00 at 5.00%" beside a 5px `#555` dot at (5.00%, $75.00).
- **Bottom bold red (`#e74c3c`, centered, y=352):** "$75.00 / $7.50 = 10.0× swing in true price, with zero change to the product".

## Ranking Accounts by Stated Monthly Fee Ranks Them on the Wrong Axis

**A, C, B Is the Right Order Below 3.60% and the Exact Reverse Above 4.80%**

- **Three products:** Account A charges $0.00/mo but needs $1,500; Account B charges $5.00/mo with no minimum.
- **The middle option:** Account C charges $3.00/mo and needs $500, so it mixes both cost types.
- **The right cost function:** total annual cost = 12 × monthly fee + r × minimum balance, for each account.
- **At 0.50%:** A = $7.50, C = $38.50, B = $60.00 — the fee-table ranking A < C < B happens to be correct.
- **At 4.50%:** A = $67.50, C = $58.50, B = $60.00 — C now wins and A, the "free" account, is worst.
- **At 6.00%:** A = $90.00, C = $66.00, B = $60.00 — the ranking is the exact reverse of the fee table.
- **A vs C crossover:** $36.00 / $1,000.00 = 3.6000%, the rate above which A stops being the cheapest.
- **C vs B crossover:** $24.00 / $500.00 = 4.8000%, the rate above which the no-minimum account wins outright.
- **The measurement claim:** stated fee is one term of a two-term cost, so it cannot order the alternatives.

### Visualization (canvas `c3`, 720×360)

Three total-cost lines against prevailing rate, with both computed crossover rates marked, and the fee-table ordering printed for contrast. All lines and crossovers computed in JS from the three (fee, minimum) pairs.

- **Title (bold 17px `#1a5276`, centered, y=22):** "Total Cost Reorders the Accounts as the Rate Rises" — subtitle (15px `#555`, y=44): "Illustrative Example — cost = 12 × fee + r × minimum".
- **Axes:** x = rate 0.00%–6.00% mapped to px 100–650; y = annual cost $0–$100 mapped to py 280–80. Axis lines `#333` 1px, x tick labels 0%–6% (14px `#555`, y=298), y tick labels $0–$100 (15px `#555`, right aligned at x=92), "Annual cost" rotated 90° at x=30.
- **Account A line (`#1a5276` 2.5px solid):** `0*12 + r*1500`, from $0.00 at 0% to $90.00 at 6.00%. Label "A — $0/mo, $1,500 min" in `#1a5276` 15px near its right end, above the line.
- **Account B line (`#e74c3c` 2.5px solid):** flat `5*12 = $60.00`. Label "B — $5/mo, no minimum" in `#e74c3c` 15px, left aligned at x=108 just above the line.
- **Account C line (`#e67e22` 2.5px solid):** `3*12 + r*500`, from $36.00 at 0% to $66.00 at 6.00%. Label "C — $3/mo, $500 min" in `#e67e22` 15px near its right end, below the line.
- **Crossover markers:** 6px hollow `#27ae60` circles (2px stroke) at the two computed intersections — A×C at (3.6000%, $54.00) and C×B at (4.8000%, $60.00) — each with a short vertical dashed (4/4) `#27ae60` line to the x-axis and a 14px `#27ae60` label: "A = C at 3.6000%" and "C = B at 4.8000%".
- **Winner band (drawn just under the plot, y=306 to y=314, 8px rects):** `rgba(26,82,118,0.35)` from x=100 to the A×C crossover x; `rgba(230,126,34,0.35)` from there to the C×B crossover x; `rgba(231,76,60,0.35)` from there to x=650. Segment labels 14px in matching colors below at y=328: "A cheapest", "C cheapest", "B cheapest".
- **Fee-table contrast (15px, left aligned at x=100, y=68):** "Fee table says A < C < B at every rate" in `#555`, with "true above 4.80%: B < C < A" bold in `#e74c3c` immediately to its right.
- **Bottom bold red (centered, y=350):** "One product ranking, two crossings — the fee column alone is right only below 3.60%".

## Tiered Waivers Select Who Pays, So Fee Revenue Measures the Waiver Rule

**Waive if Balance ≥ $1,500 OR Direct Deposit ≥ $2,000 — Only 18.0% Ever Pay**

- **The rule:** the fee is waived on either condition, so a customer needs to fail both to be charged.
- **The book of 10,000:** 4,200 meet both conditions, 1,300 meet only the balance test, 2,700 only the deposit test.
- **Who is left:** 10,000 − 4,200 − 1,300 − 2,700 = 1,800 customers meet neither and pay the fee.
- **The waiver rate:** 8,200 / 10,000 = 82.0% waived, so the charged population is 1,800 / 10,000 = 18.0%.
- **Not a random 18%:** an OR of two capacity tests selects precisely the customers with the least capacity.
- **The deposit-only tier:** 2,700 customers pay $0.00 and park $0.00 — for them the account really is free.
- **Three cost regimes, one product:** $0.00, $60.00 implied, or $60.00 explicit, decided by the waiver rule.
- **The measurement claim:** fee revenue is a statistic about the waiver conditions, not about the price.

### Visualization (canvas `c4`, 720×360)

A 2×2 contingency grid of the two waiver conditions with margins that sum, each cell shaded by which cost regime it lands in. Every count, margin, and percentage computed in JS from the four cell counts.

- **Title (bold 17px `#1a5276`, centered, y=22):** "Two Waiver Tests, Four Cells, Three Cost Regimes" — subtitle (15px `#555`, y=44): "Illustrative Example — 10,000 customers, waived on either condition".
- **Grid:** two columns × two rows of 150×72 cells, top-left cell origin (250,96); column x positions 250 and 400; row y positions 96 and 168. Cell borders `#e0e0e0` 1px.
- **Column headers (bold 15px `#1a5276`, centered over each column at y=88):** "Direct deposit ≥ $2,000" at x=325; "No qualifying deposit" at x=475.
- **Row labels (15px `#1a5276`, right aligned at x=242, vertically centered in each row):** "Balance ≥ $1,500" at y=136; "Balance < $1,500" at y=208.
- **Cells (count bold 17px centered, regime caption 14px centered below it):**
  - (balance yes, deposit yes) = 4,200, fill `rgba(39,174,96,0.20)`, caption "free" in `#27ae60`.
  - (balance yes, deposit no) = 1,300, fill `rgba(26,82,118,0.35)`, caption "pays $60 implied" in `#1a5276`.
  - (balance no, deposit yes) = 2,700, fill `rgba(39,174,96,0.20)`, caption "free" in `#27ae60`.
  - (balance no, deposit no) = 1,800, fill `rgba(231,76,60,0.20)`, caption "pays $60 explicit" in `#e74c3c`.
- **Margins (15px `#555`):** row totals right of the grid at x=560 — "5,500" at y=136, "4,500" at y=208; column totals below the grid at y=258 — "6,900" at x=325, "3,100" at x=475; grand total "10,000" bold `#1a5276` at (560,258).
- **Margin check line (14px `#555`, centered, y=282):** "4,200 + 1,300 = 5,500 · 2,700 + 1,800 = 4,500 · 4,200 + 2,700 = 6,900 · 1,300 + 1,800 = 3,100".
- **Highlight:** 2.5px solid `#e74c3c` stroke rect around the (no, no) cell.
- **Bottom bold red (centered, y=316):** "8,200 waived / 1,800 charged → the fee applies to 18.0% selected by inability to qualify".
- **Bottom gray (`#555`, centered, y=340):** "5,500 park capital instead; 2,700 pay in neither form. One product, three prices."

## Average Fee per Customer Spreads the Burden Over People Who Never Bore It

**$10.80 Average, $60.00 Actual — and 14.29% of the Payer's Balance**

- **The dashboard number:** $108,000 collected / 10,000 customers = $10.80 average annual fee per customer.
- **Who actually paid:** all $108,000 came from 1,800 customers, so each of them paid exactly $60.00.
- **The understatement:** $60.00 / $10.80 = 5.556×, because 8,200 zeroes are averaged into the denominator.
- **The payers' balances:** the group that fails both tests holds an average balance of $420.00.
- **Burden on a payer:** $60.00 / $420.00 = 14.29% of the balance consumed by the fee in one year.
- **Burden on a parker:** $60.00 / $1,500.00 = 4.00% of the balance, which is the yield they gave up.
- **The ratio:** 14.29% / 4.00% = 3.571×, so the explicit fee falls hardest as a share of what is held.
- **Why the correlation runs that way:** failing a balance test is the definition of having little to park.
- **The measurement claim:** an average over a selected zero-inflated population describes nobody in it.

### Visualization (canvas `c5`, 720×360)

Paired comparison: the flat average fee bar against the conditional-on-paying bar, plus a burden-as-share-of-balance panel. All figures computed in JS from the collected total, the counts, and the two average balances.

- **Title (bold 17px `#1a5276`, centered, y=22):** "The Average Fee Describes Nobody" — subtitle (15px `#555`, y=44): "Illustrative Example — $108,000 collected from 1,800 of 10,000 customers".
- **Left panel — dollars (heading "Annual fee" bold 15px `#1a5276` at x=60, y=76):** scale $1.00 = 3px, baseline y=250. Average bar rect (100,217.6) 90×32.4 in `rgba(26,82,118,0.35)` ($10.80) with "$10.80" bold 15px `#1a5276` above at y=210 and caption "all 10,000" 14px `#555` at y=270. Payer bar rect (230,70) 90×180 in `#e74c3c` ($60.00) with "$60.00" bold 15px `#e74c3c` above at y=62 and caption "the 1,800 who pay" 14px `#555` at y=270.
- **Ratio annotation:** vertical `#e74c3c` 2px line at x=205 from y=70 to y=217.6 with 6px end ticks, labeled "5.556×" bold 15px `#e74c3c` at x=200, y=150, right aligned.
- **Right panel — burden (heading "Fee as share of balance held" bold 15px `#1a5276` at x=400, y=76):** scale 1% = 11px, baseline y=250. Payer bar rect (430,92.8) 90×157.2 in `#e74c3c` (14.29%) with "14.29%" bold 15px `#e74c3c` above at y=85 and two caption lines 14px `#555` at y=270/288: "payer" / "$60 / $420".
- **Right panel second bar:** parker bar rect (560,206) 90×44 in `rgba(26,82,118,0.35)` (4.00%) with "4.00%" bold 15px `#1a5276` above at y=199 and caption lines "parker" / "$60 / $1,500" at y=270/288.
- **Panel divider:** 1px `#e0e0e0` vertical line at x=380 from y=66 to y=300.
- **Bottom bold red (centered, y=326):** "14.29% vs 4.00% = 3.571× — the visible fee is heaviest where capacity is lowest".
- **Bottom gray (`#555`, centered, y=350):** "Averaging $60 over 8,200 zeroes reports $10.80, a burden no customer in the book experiences."

## Buffer Behaviour Parks More Capital Than the Stated Minimum

**Stated $1,500, Actually Held $1,800 — the Implied Fee Is $72.00, Not $60.00**

- **The tripwire:** the fee applies if the balance dips below $1,500 at any point in the statement cycle.
- **The rational response:** hold a margin above the threshold so ordinary spending variance cannot trip it.
- **The observed behaviour:** average parked balance is $1,800.00, a $300.00 buffer over the stated $1,500.
- **The corrected implied fee:** 0.0400 × $1,800.00 = $72.00/yr against the $60.00 the stated minimum implies.
- **The understatement:** $72.00 / $60.00 = 1.200, so pricing off the stated minimum is 20.0% too low.
- **Buffer break-even:** the requirement now beats the fee only below $60.00 / $1,800.00 = 3.3333%, not 4.00%.
- **When the buffer pays:** $300.00 × 0.0400 = $12.00/yr of yield buys avoidance of $12.00 / $5.00 = 2.4 trips.
- **The asymmetry:** a customer tripping fewer than 2.4 times a year over-insures and loses on the trade.
- **The measurement claim:** model the behaviour the threshold induces, not the threshold that was published.

### Visualization (canvas `c6`, 720×340)

Stacked capital bar showing stated minimum plus buffer, with the implied-fee consequence of each layer, and the buffer break-even computation. All values computed in JS from `MIN_BALANCE`, `PARKED`, `RATE`, `MONTHLY_FEE`.

- **Title (bold 17px `#1a5276`, centered, y=22):** "The Threshold Sets the Floor; Behaviour Sets the Balance" — subtitle (15px `#555`, y=44): "Illustrative Example — $1,500 required, $1,800 actually held, 4.00% yield".
- **Scale:** $100.00 of balance = 30px of bar width; bar 52px tall at y=90. Origin x=60.
- **Stated-minimum segment:** rect (60,90) 450×52 in `rgba(26,82,118,0.35)` ($1,500.00), centered label "stated minimum $1,500.00" 15px `#1a5276`.
- **Buffer segment:** rect (510,90) 90×52 in `#e67e22` ($300.00), centered label "buffer $300" 14px white.
- **Layer callouts (15px, above the bar at y=82, left aligned at each segment start):** "→ $60.00/yr implied" in `#1a5276` at x=60; "→ $12.00/yr" in `#e67e22` at x=510.
- **Total readout (bold 17px, left aligned at x=60, y=176):** "Parked $1,800.00 × 4.00% = $72.00/yr implied" in `#e74c3c`.
- **Comparison line (15px `#555`, x=60, y=200):** "Stated-minimum estimate: $1,500.00 × 4.00% = $60.00/yr → understated by 20.0%".
- **Break-even shift:** two 5px dots on a short horizontal `#333` 1px axis from x=60 to x=660 at y=250, with rate ticks 3.0%, 3.5%, 4.0%, 4.5% labeled 14px `#555` at y=270 (rate 3.00%–4.50% mapped to px 60–660). `#e74c3c` dot at the computed buffered break-even 3.3333% labeled "3.3333% buffered" bold 14px `#e74c3c` above at y=240; `#1a5276` dot at 4.0000% labeled "4.0000% stated" bold 14px `#1a5276` above at y=240.
- **Bottom bold red (centered, y=310):** "Buffer costs $12.00/yr and avoids $5.00 per trip — worth it only above 2.4 trips per year".

## Foregone Return Compounds; the Fee Line Does Not

**Over 5 Years: $324.98 Compounded vs the $300.00 a Simple Multiply Reports**

- **The simple estimate:** 5 years × 0.0400 × $1,500.00 = $300.00, the figure a per-year cost table implies.
- **The correct quantity:** the capital would have grown, so the loss is $1,500.00 × (1.04⁵ − 1) = $324.98.
- **The gap:** $324.98 − $300.00 = $24.98, which is 8.33% more than the simple multiply reports.
- **At 10 years:** $1,500.00 × (1.04¹⁰ − 1) = $720.37 against 10 × $60.00 = $600.00, a 20.06% gap.
- **Why it grows:** the simple form omits return on return, and that omission scales super-linearly in tenure.
- **The fee genuinely is simple:** $5.00/mo paid out of income is a flow, so 5 years of it really is $300.00.
- **The clean consequence:** at 4.00%, paying the fee and investing beats parking by exactly $24.98 over 5 years.
- **Not a wash after all:** the two branches tie for one year and diverge in the parker's disfavour after that.
- **The measurement claim:** annualize the fee, but compound the foregone return — they are different objects.

### Visualization (canvas `c7`, 720×360)

Two cumulative-cost curves over a 10-year horizon — compounded foregone return vs simple annual fee — with the gap at year 5 bracketed. Every plotted point and printed figure computed in JS from `MIN_BALANCE`, `RATE`, `MONTHLY_FEE`.

- **Title (bold 17px `#1a5276`, centered, y=22):** "Simple Fees, Compounding Losses" — subtitle (15px `#555`, y=44): "Illustrative Example — $1,500 parked at 4.00% vs $5.00/mo paid".
- **Axes:** x = years 0–10 mapped to px 90–660; y = cumulative cost $0–$800 mapped to py 290–80. Axis lines `#333` 1px. x ticks at every year, labels 0,2,4,6,8,10 (14px `#555`, y=308); y ticks and labels $0, $200, $400, $600, $800 (14px `#555`, right aligned at x=82). Captions: "Years of tenure" centered at y=330; "Cumulative cost" rotated 90° at x=30.
- **Compounded curve (`#1a5276` 2.5px solid):** points at each year N, value `MIN_BALANCE*(Math.pow(1+RATE,N)-1)`, so year 5 = $324.98 and year 10 = $720.37. Label "compounded foregone return" in `#1a5276` 15px above the curve near x=520.
- **Simple line (`#e74c3c` 2.5px dashed 6/4):** straight from (0, $0.00) to (10, $600.00), value `MONTHLY_FEE*12*N`. Label "simple: N × $60.00" in `#e74c3c` 15px below the line near x=520.
- **Year-5 gap bracket:** vertical `#e67e22` 2px line at the year-5 x from the simple value's py to the compounded value's py, with 6px end ticks, and label "$24.98 gap (8.33%)" bold 15px `#e67e22` to its right. Two 5px dots at the bracket ends in `#1a5276` and `#e74c3c`, labeled "$324.98" and "$300.00" in 14px matching colors.
- **Year-10 gap label (14px `#e67e22`, right aligned at x=654, just above the year-10 compounded point):** "$720.37 vs $600.00 → 20.06%".
- **Bottom bold red (centered, y=352):** "Both branches tie at year 1 at $60.00; by year 5 parking costs $24.98 more".

## Cost of Banking Computed From Statements Misses Three-Quarters of Itself

**$108,000 Visible, $330,000 Implied — the Statement Captures 24.66%**

- **What the ledger shows:** the whole book generated $108,000 of maintenance fees over the year.
- **What the ledger omits:** 5,500 customers parked $1,500 each, forgoing 5,500 × $60.00 = $330,000.
- **The true total:** $108,000 + $330,000 = $438,000 of real annual cost borne by the same 10,000 customers.
- **The visible share:** $108,000 / $438,000 = 24.66%, so 75.34% of the cost has no statement row anywhere.
- **The per-customer error:** $438,000 / 10,000 = $43.80 true average against the reported $10.80, a 4.056× gap.
- **With buffers included:** 5,500 × $72.00 = $396,000 implied, giving $504,000 total and $50.40 per customer.
- **Buffered visible share:** $108,000 / $504,000 = 21.43%, so accounting for behaviour makes it worse, not better.
- **Direction is known, not random:** the omitted term is non-negative always, so the metric is biased low.
- **The measurement claim:** this is not measurement noise — it is a missing term, and it is the larger one.

### Visualization (canvas `c8`, 720×360)

Stacked total-cost bar split into visible and invisible components, with a second bar adding buffer behaviour, and the visible-share percentages printed. All figures computed in JS from the counts, `MONTHLY_FEE`, `MIN_BALANCE`, `PARKED`, `RATE`.

- **Title (bold 17px `#1a5276`, centered, y=22):** "One Book, Two Cost Totals" — subtitle (15px `#555`, y=44): "Illustrative Example — 10,000 customers, 1,800 charged, 5,500 parking".
- **Scale:** $504,000 = 600px of bar width (so $1,000 = 1.1905px); bars 54px tall; origin x=60.
- **Bar 1 — stated minimum (y=96):** label "Stated minimum" 15px `#333` at x=60, y=88. Visible segment rect (60,96) 128.6×54 in `#e74c3c` ($108,000) with label "$108,000" 14px white centered. Invisible segment rect (188.6,96) 392.9×54 in `rgba(26,82,118,0.35)` with a 2px dashed (5/4) `#1a5276` outline ($330,000), centered label "$330,000 implied — no statement row" 14px `#1a5276`. Right readout at x=60, y=176: "total $438,000 · visible 24.66%" bold 15px `#1a5276`.
- **Bar 2 — with buffer (y=204):** label "With buffer behaviour" 15px `#333` at x=60, y=196. Visible segment rect (60,204) 128.6×54 in `#e74c3c` ($108,000) labeled "$108,000". Invisible segment rect (188.6,204) 471.4×54 in `rgba(26,82,118,0.35)` dashed outline ($396,000), centered label "$396,000 implied" 14px `#1a5276`. Right readout at x=60, y=284: "total $504,000 · visible 21.43%" bold 15px `#e74c3c`.
- **Per-customer readouts (14px `#555`):** "reported $10.80/customer · true $43.80" at x=340, y=176; "reported $10.80/customer · true $50.40" at x=340, y=284.
- **Bottom bold red (centered, y=326):** "Reported cost is 24.66% of true cost — a 4.056× understatement, biased in one known direction".
- **Bottom gray (`#555`, centered, y=350):** "$108,000 + $330,000 = $438,000 · $108,000 + $396,000 = $504,000 — both totals reconcile to the counts above."

## Regeneration instructions

- **Layout:** detail page. h1 carrying no index number + `.subtitle` + one `.philosophy` callout (two paragraphs: the fundamental problem, then the "Illustrative Example" disclaimer), then one unnumbered `<h2>` per pitfall followed by a one-row `.obj-table`: left `<td>` (50%) holds an `.obj-title` div — a refined restatement of the pitfall name carrying a concrete number from that section's own bullets, never a copy of the heading — plus a `<ul>` of labeled bullets; right `<td>` (50%, centered) holds the canvas. Even table rows have background `#fafcfe`. The "Shared scenario — reconciliation" tables are md-only reviewer aids and are **not** rendered into the html.
- **Page CSS:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.4em `#1a5276` with 2px `#2980b9` bottom border, padding-bottom 8px; subtitle `#666` 1.05em; ul 0.9em `#333`; `strong` `#1a5276`; `.obj-title` 1.05em weight 600 `#1a5276`; `.philosophy` background `#f0f4f8`, left border 4px solid `#2980b9`, padding 12px 16px, 0.9em. Table cell borders `1px solid #e0e0e0`, padding 20px 24px. `td:first-child` and `td:last-child` are both 50% — shrink a chart via the canvas `style.maxWidth`, never by narrowing the cell. No nav bar, no back/home links, no cross-page links, no `thead`, no status badges.
- **Canvas:** intrinsic `width`/`height` attributes as given per chart (all 720 wide, heights 340–360); a shared `setupCanvas(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms). Chart ids are sequential in page order, `c1` through `c8`.
- **Single source of truth for the arithmetic:** the script declares `MIN_BALANCE = 1500`, `MONTHLY_FEE = 5`, `RATE = 0.04`, `PARKED = 1800`, `TENURE_YEARS = 5`, `CUSTOMERS = 10000`, the four waiver cell counts `[4200, 1300, 2700, 1800]`, the payer average balance `PAYER_BALANCE = 420`, and the three account definitions `[{fee:0,min:1500},{fee:5,min:0},{fee:3,min:500}]` **once**, at the top. Every printed dollar figure, percentage, ratio, break-even rate, crossover rate, and bar dimension is derived from those at render time. No cost figure is typed as a literal inside a chart label. Helpers: `money(v, d)` formats with `$`, thousands separators and `d` decimals; `pct(v, d)` formats a fraction as a percentage; `breakEven(fee, minb)` returns `fee*12/minb`; `crossRate(a, b)` returns `(b.fee - a.fee)*12 / (a.min - b.min)` for the two-account crossovers.
- **No `Math.random()` anywhere on this page.** All data is either a hardcoded literal count (customer counts, balances) or computed from the constants above. If a future edit needs generated data, add an inline seeded generator per chart: `function lcg(seed){var s=seed;return function(){s=(s*16807)%2147483647;return s/2147483647;};}`.
- **The compounding figures must use the compound form.** `MIN_BALANCE*(Math.pow(1+RATE,N)-1)` for the true loss and `MONTHLY_FEE*12*N` for the fee flow; never approximate one with the other, and print the gap as a computed difference.
- **Palette:** primary blue `#1a5276` / `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`, gray text `#555`/`#333`.
- **Naming:** no real bank or product names. The institution is "Bank A", alternatives are "Account A/B/C", people are Alice/Bob. Every constructed figure is labeled "Illustrative Example".
