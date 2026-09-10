# Car Dealerships — Distribution Patterns

**Page type:** detail page (three-column obj-table layout: text left ~38%, histogram canvas middle ~31%, insight canvas right ~31%, one table per section)
**HTML title tag:** Car Dealerships — Distribution Patterns

**Subtitle:** Simulated distribution shapes from car pricing, lot economics, and buyer behavior

## Discount by Day (Month-End Push)

**Label:** DESPERATION SPIKE (color `#795548`)

Discount percentage follows a hockey stick — flat 3-5% for days 1-20, then a ramp toward 12-18% in days 25-31. One explanation: monthly quotas, with margin conceded on day 28 that would never be conceded on day 5.

- Days 1-20: steady 3-5% discount (take-it-or-leave-it)
- Days 25-31: discounts centered ~15%, reaching ~18%
- Plausibly amplified at quarter-end deadlines (not simulated)
- Same car, same dealer, ~3-4x the discount — timing alone

### Visualization (canvas `canvas1`, 420×340)

Bimodal histogram of discount percentages.

- **Title (bold 13px, `#1a5276`, top center):** "Discount % — All Sales in a Month (Pooled)".
- **Data:** 1400 samples uniform 3-5% (early-month days) plus 600 samples Normal(mean 15, sd 2.5) (end-of-month push), generated with seeded RNG (mulberry32, seed 42).
- **Bins/axes:** 30 bins over x range 0-22; x labels formatted "N%"; x-axis label "Discount %".
- **Bars:** fill `rgba(44,62,80,0.5)`, border `#2c3e50`; Gaussian-smoothed (sigma 1.5 bins) density line `#1a5276` width 2 with 95% SE band filled `rgba(230,126,34,0.22)`.

### Visualization (canvas `canvas1b`, 400×340)

Hockey-stick timeline of discount vs day of month.

- **Title (bold 13px, `#2c3e50`, top center):** "Discount Timeline — The Hockey Stick".
- **Desperation zone:** days 25-31 (from x = 24/31 of the width) shaded `rgba(231,76,60,0.12)` with bold 11px `#e74c3c` two-line label "DESPERATION" / "ZONE" at the top of the zone.
- **Curve:** `#2c3e50` line width 3 over days 1-31; days ≤ 20: `4 + 0.5·sin(0.3·day)` (flat ~4%); days > 20: `4 + 14·((day−20)/11)^2.5` (accelerating ramp); y scale 0-20%.
- **Annotation:** bold 11px `#2c3e50` two-line text near the left: "Same car, ~4x discount" / "— just wait", with a horizontal `#e67e22` 2px arrow pointing right toward the ramp.
- **Axes:** L-shaped gray `#999` axes; padding top 40, right 20, bottom 50, left 55; x labels 10px `#555`: "Day 1", "Day 10", "Day 20", "Day 31" with 11px `#333` caption "Day of Month"; y labels "0%", "10%", "20%".

## Model-Year (Cliff When New Arrives)

**Label:** CLEARANCE CLIFF (color `#2980b9`)

Bimodal: discounts hover around 4% for months, then jump to a ~22% mean once the new model year is announced. The car is physically identical the day before and after — in this simulation, ~18% of the price disappears on an announcement.

- Holding phase: discount hovers around 4%
- Clearance phase: discount jumps to ~22% mean
- The car is physically identical before and after announcement
- The price tracks the announcement, not any physical change

### Visualization (canvas `canvas2`, 420×340)

Bimodal histogram of model-year clearance discounts.

- **Title:** "Discount % — Model Year Clearance".
- **Data:** 1200 samples Normal(mean 4, sd 1.5) (holding phase) plus 800 samples Normal(mean 22, sd 4) (clearance cliff).
- **Bins/axes:** 35 bins over x range 0-35; x labels formatted "N%"; x-axis label "Discount %".
- **Bars:** fill `rgba(41,128,185,0.5)`, border `#2980b9`; standard smoothed density line + SE band overlay.

### Visualization (canvas `canvas2b`, 400×340)

Two-phase price timeline with an announcement cliff.

- **Title (bold 13px, `#2980b9`, top center):** "Price Timeline — The Announcement Cliff".
- **Phase backgrounds:** left 65% of the plot filled `rgba(41,128,185,0.08)`, right 35% filled `rgba(231,76,60,0.08)`, separated by a vertical dashed (5/4) `#e74c3c` 2px line at 65% width.
- **Price line:** `#2c3e50` width 3 over t = 0-100; t < 65: flat at 96% of MSRP (~4% discount, matching the histogram's holding mode); t 65-70: cliff drop of the full 18 points to 78% (~22% discount, matching the clearance mode); t ≥ 70: flat around 78% with tiny noise (seeded RNG); y axis spans 70%-100% of MSRP.
- **Labels:** below the dashed line, bold 11px `#e74c3c` "NEW MODEL" / "ANNOUNCED"; left phase headed bold 11px `#2980b9` "Current Model Year" + 10px "— holding value —"; right phase headed bold 11px `#e74c3c` "CLEARANCE".
- **Annotation (bold 10px, `#2c3e50`, four lines at mid-left):** "Same car loses ~18%" / "in one week — not because" / "it changed, but because" / "the NEW one exists."
- **Drop arrow:** vertical `#e74c3c` 3px arrow just right of the cliff spanning the full 96%→78% drop, with a bold 12px computed label "-18%".
- **Axes:** L-shaped gray `#999` axes; padding top 40, right 15, bottom 50, left 50; y labels 10px `#555` "100%" (top) and "70%" (bottom); axis caption "% of MSRP" above the y-axis.

## Lot Age (Weibull Survival — Aging = Pressure)

**Label:** LOT AGING PRESSURE (color `#27ae60`)

Days on lot follows a Weibull distribution (k=1.8, λ=45): median ~37 days, over 80% sold by day 60, but a long tail keeps sitting there. Every extra day accrues floor plan (financing) interest — the aging tail is where margin quietly drains.

- 0-30 days: fresh stock, full margin, no pressure
- 30-60 days: floor plan interest eating into profit
- 60+ days: holding costs can exceed remaining margin
- Floor plan interest = the hidden clock ticking on every car

### Visualization (canvas `canvas3`, 420×340)

Weibull histogram of days on lot.

- **Title:** "Days on Lot (Weibull k=1.8, λ=45)".
- **Data:** 2000 samples from Weibull(k=1.8, λ=45) via inverse CDF: `45 · (−ln(1−U))^(1/1.8)`.
- **Bins/axes:** 35 bins over x range 0-150; x labels rounded integers; x-axis label "Days on Lot".
- **Bars:** fill `rgba(230,126,34,0.5)`, border `#e67e22`; standard smoothed density line + SE band overlay.

### Visualization (canvas `canvas3b`, 400×340)

Survival curve with three colored cost zones.

- **Title (bold 13px, `#e67e22`, top center):** "Lot Aging — Survival & Cost Zones".
- **Zones (x range 0-120 days):** 0-30 filled `rgba(39,174,96,0.15)`, labeled bold 10px `#27ae60` "FRESH" + 9px "Full margin"; 30-60 filled `rgba(241,196,15,0.15)`, labeled `#f39c12` "PRESSURE" + "Interest eating profit"; 60-120 filled `rgba(231,76,60,0.15)`, labeled `#e74c3c` "MUST GO" + "Interest > margin".
- **Survival curve:** `S(t) = exp(−(t/45)^1.8)` drawn in `#2c3e50` width 3 from 100% down toward 0.
- **Annotation:** at t=90, a red `#e74c3c` downward arrow onto the curve with bold 9px two-line label "Holding costs" / "dominate here".
- **Dark callout box (filled `rgba(44,62,80,0.9)`, white bold 10px text, near lower middle):** "Floor plan interest =" / "hidden clock".
- **Axes:** L-shaped gray `#999` axes; padding top 40, right 15, bottom 55, left 50; x labels "0", "30", "60", "90", "120" with 11px `#333` caption "Days on Lot"; y labels "100%" / "0%" and caption "Still on lot" above the axis.

## Interest Rate Impact (Demand Shift)

**Label:** RATE SENSITIVITY (color `#e74c3c`)

When rates go from 3% to 7%, the whole demand distribution shifts left: same cars, same incomes, higher monthly payments. In this simulation the shape stays the same — it just MOVES. A location shift, not a shape change.

- Low rates (3%): mean 65 units/month per dealer
- High rates (7%): mean 42 units/month per dealer
- $40K over 60 months: $719/mo at 3% vs $792/mo at 7% (+$73)
- ~10% demand drop per rate point in this simulation (65 → 42)

### Visualization (canvas `canvas4`, 420×340)

Overlapping-bimodal histogram of monthly sales volume.

- **Title:** "Monthly Sales Volume (Units/Dealer)".
- **Data:** 1500 samples Normal(mean 65, sd 12) (low-rate era) plus 1000 samples Normal(mean 42, sd 12) (high-rate era — same sd, pure location shift).
- **Bins/axes:** 30 bins over x range 15-90; x labels rounded integers; x-axis label "Units Sold / Month".
- **Bars:** fill `rgba(142,68,173,0.5)`, border `#8e44ad`; standard smoothed density line + SE band overlay.

### Visualization (canvas `canvas4b`, 400×340)

Two overlaid Gaussian density curves showing a pure location shift.

- **Title (bold 13px, `#8e44ad`, top center):** "Demand Shift: Low vs High Rates".
- **Curves (Gaussian PDFs over x = 15-90, both normalized to the low-rate peak, scaled to 90% of plot height):**
  - Low rates: N(65, 12) — stroke `#27ae60` width 3, fill `rgba(39,174,96,0.3)`, legend bold 11px "Low rates (3%)" at upper right.
  - High rates: N(42, 12) — stroke `#e74c3c` width 3, fill `rgba(231,76,60,0.3)`, legend "High rates (7%)" at upper left.
- **Shift arrow:** horizontal `#2c3e50` 2px arrow from the low-rate peak (x=65) leftward to the high-rate peak (x=42) with bold 10px label "−23 units/mo" above it.
- **Dark annotation box (filled `rgba(44,62,80,0.9)`, white text, below the axis):** bold 10px "3% → 7% on a $40K/60-mo loan: +$73/month payment" and 10px "$719/mo at 3% vs $792/mo at 7% — mean demand 65 → 42".
- **Axis:** gray `#999` baseline; x labels 10px `#555`: "20", "40", "60", "80"; padding top 40, right 15, bottom 60, left 45.

## Stacked Incentives (EV + Mfr + Dealer)

**Label:** INCENTIVE STACKING (color `#8e44ad`)

Total discount is a sum of several independent incentive distributions, so the result is multimodal with bumps at common stack totals. The sticker price is rarely the paid price — what you pay depends on which incentives you qualify for.

- Base dealer discount: uniform $1K-$3K
- Manufacturer rebate: 40% chance of ~$3K
- EV tax credit: 30% chance of $7,500
- Loyalty/trade-in bonus: ~$1,500
- Total can range from $2K to $16K+ off MSRP

### Visualization (canvas `canvas5`, 420×340)

Multimodal histogram of total incentive stacks.

- **Title:** "Total Incentive Stack ($)".
- **Data:** 2000 samples of `base + mfgRebate + evCredit + loyalty` where base ~ uniform(1000, 3000); mfgRebate = Normal(3000, 500) with probability 0.4 else 0; evCredit = 7500 with probability 0.3 else 0; loyalty ~ Normal(1500, 400); kept if total > 0.
- **Bins/axes:** 40 bins over x range 0-18000; x labels formatted "$NK"; x-axis label "Total Discount ($)".
- **Bars:** fill `rgba(39,174,96,0.5)`, border `#27ae60`; standard smoothed density line + SE band overlay.

### Visualization (canvas `canvas5b`, 400×340)

Waterfall decomposition from MSRP to paid price.

- **Title (bold 12px, `#27ae60`, top center):** "Waterfall: MSRP $45,000 → You Pay $31,000".
- **Bars (6 columns, 70% of slot width, ~80% alpha fills with matching strokes, bold 9px two-line labels below in each bar's color, white bold 10px values on the bars):**
  - "MSRP": full bar $45,000, color `#2c3e50`, value label "$45K".
  - "Dealer / Margin": −$2,000, color `#2980b9`, value "-$2.0K".
  - "Mfg / Rebate": −$3,000, color `#e67e22`, value "-$3.0K".
  - "EV Tax / Credit": −$7,500, color `#27ae60`, value "-$7.5K".
  - "Loyalty": −$1,500, color `#8e44ad`, value "-$1.5K".
  - "YOU / PAY": total bar $31,000, color `#c0392b`, value "$31K".
- **Connectors:** dashed (3/2) gray `#999` step lines between consecutive floating bars.
- **Statement (bold 10px, `#2c3e50`, centered near the top, two lines):** "The sticker price is fiction — the REAL price is a sum of" / "independent incentive distributions".

## Regeneration instructions

- **Layout:** one `.obj-table` per section (full-width, border-collapse), each with a single `<tr>` of three `<td>`s: first 38% (text: `.pitfall-label` span, `<h3>` title, `<p>` paragraph, `<ul>` bullets), second 31% centered (histogram canvas), third 31% centered (insight canvas). Section order as above.
- **Page style:** body system sans-serif, margin 20px, background `#f9f9f9`, text `#333`; h1 `#1a5276` centered; `.subtitle` centered `#666` 0.95em; table cell borders `1px solid #2980b9`, padding 12px; h3 `#1a5276` 1.0em weight 700; paragraphs/bullets 14px, line-height 1.5-1.6; `.pitfall-label` inline-block bold 0.72em uppercase, letter-spacing 0.5px. No nav bar, no back/home links.
- **Pitfall label colors:** assigned by index from the cycling palette `["#795548","#2980b9","#27ae60","#e74c3c","#8e44ad","#e67e22","#16a085","#d35400","#c0392b","#1abc9c"]` via a small script that sets each `.pitfall-label`'s color.
- **Canvases:** intrinsic sizes as given (420×340 histograms, 400×340 insight charts), CSS `width: 100%; height: auto`; every canvas scales by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates).
- **Shared histogram helper:** white background, centered bold 13px `#1a5276` title, gray `#999` L axes (margins 35/20/40/50), per-bin bars with 1px gap, Gaussian-smoothed (sigma 1.5 bins) density line `#1a5276` width 2 over a 95% SE band filled `rgba(230,126,34,0.22)`, 6 x-tick labels 11px `#555` with optional 12px `#333` x-axis label. Data generated with seeded mulberry32(42) RNG and Box-Muller normal sampler.
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, dark slate `#2c3e50`, green `#27ae60`, red `#e74c3c`/`#c0392b`, orange `#e67e22`/`#f39c12`, purple `#8e44ad`.
