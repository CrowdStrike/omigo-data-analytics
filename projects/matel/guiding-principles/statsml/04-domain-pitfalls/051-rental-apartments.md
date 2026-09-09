# Rental Apartments Domain: Data Pitfalls

**Page type:** detail page (one h2 per pitfall, each followed by a two-column obj-table row: text left 50%, canvas right 50%)
**HTML title tag:** Rental Apartments Domain: Data Pitfalls

**Subtitle:** Phantom listings, fake lead-generation posts, seasonal cycles, and rent-control distortions that corrupt rental market data.

## Listing Staleness (Phantom Availability)

- Apartment posted 2 weeks ago is already rented 10 days ago but listing remains active
- 30-50% of listings on major platforms are stale (already rented, withdrawn, or expired)
- Model trained on "available" listings includes unavailable ones → inflated supply estimate
- Availability is a real-time signal that data cannot capture without constant refresh/verification
- Staleness correlates with desirability: best apartments rent fastest → most likely to be stale
- Creates systematic bias: model over-represents undesirable units (they stay listed longer)

**Example:** A rental pricing model trained on home-buying platform/Craigslist data includes 40% phantom listings. The "average available rent" appears lower than reality because the cheap good apartments already rented — only the overpriced or flawed ones remain truly available. Users see prices they can never actually get.

### Visualization (canvas `canvas1`, 720×300)

Stacked timeline bars (available vs stale days per apartment) plus a pie chart inset of stale share.

- **Title (bold 17px `#1a5276`, centered):** "Listing Staleness: What's \"Available\" vs Actually Available".
- **Timeline area:** x from 50 to 480 spanning 30 days; x-axis ticks every 5 days (0–30) with light vertical gridlines `#d5dbdb`; axis label "Days" centered at bottom.
- **Bars (18px high, 6px gap, right-aligned 11px `#2c3e50` row labels):** each apartment shows green `#27ae60` from day 0 to its available day, then red `#e74c3c` to day 30:
  - "Apt A (great deal)" — available 2 days (white 10px in-bar label "Rented day 2")
  - "Apt B (good value)" — available 3 days (white in-bar label "Rented day 3")
  - "Apt C (decent)" — available 8 days
  - "Apt D (average)" — available 15 days
  - "Apt E (overpriced)" — available all 30 days
  - "Apt F (flawed)" — available all 30 days
- **Pie inset (center (580, 100), radius 50):** red slice 40% starting at top, green slice 60%; white bold 13px labels "40%" / "Stale" on the red slice and "60%" / "Real" on the green slice.
- **Annotation (bold 12px red, centered below pie, two lines):** "Data snapshot sees ALL" / "as available".
- **Legend (11px, swatches at x=500):** green square "Actually available"; red square "Stale/phantom".

## Fake Listings for Lead Generation

- Brokers post attractive fake apartments at below-market prices to collect prospective tenant leads
- "Too good to be true" listings are intentional training data poison in rental datasets
- Estimated 10-25% of listings in competitive markets (NYC, SF, London) are fabricated
- Fake listings systematically bias price models downward in high-demand areas
- Professional photos of non-existent or unavailable units make detection difficult
- Bait-and-switch: tenant arrives, apartment "just rented," shown worse unit at higher price

**Example:** A NYC study found that 25% of Craigslist apartment listings in Manhattan were fake, with rents 20-30% below market. A pricing model ingesting this data would systematically undervalue Manhattan rentals, telling users they can find $2,000/mo 1BR in areas where real availability starts at $3,000.

### Visualization (canvas `canvas2`, 720×300)

Overlapping density curves: fake vs real rent distributions and the combined curve the model sees.

- **Title (bold 17px `#1a5276`, centered):** "Fake Listings Poison Price Models".
- **Chart area:** left=80, right=650, top=35, bottom=165; x-axis $1,000–$4,500 with "$" tick labels every $500; axis label "Monthly Rent ($)".
- **Fake distribution:** red dashed Gaussian (`#e74c3c`, dash 5/4, width 2.5), mean $2,200, std $350, amplitude 0.7 of chart height; fill `rgba(231,76,60,0.15)`.
- **Real distribution:** blue solid Gaussian (`#2980b9`, width 2.5), mean $3,000, std $400, amplitude 0.8; fill `rgba(41,128,185,0.15)`.
- **Combined/observed:** gray dashed curve (`#7f8c8d`, dash 3/3, width 2) = Gaussian(3000, 400, 0.6·H) + Gaussian(2200, 350, 0.25·H).
- **Labels (bold 12px):** blue "Real listings (~$3,000)" near x=$3,100; red "Fake listings (~$2,200)" near x=$1,100; gray "Combined (what model sees)" near x=$2,300.
- **Gap annotation:** orange `#e67e22` vertical I-beam segment at x=$2,600 between 30% and 50% chart height, labeled bold 11px orange "Model bias".
- **Bottom annotation (bold 11px red, right-aligned):** "25% fabricated listings pull the mean down".

## Seasonal Pricing Cycles

- Rent peaks in summer (May-August) when most leases turn over and demand is highest
- Winter months (Nov-Feb) are 10-15% cheaper due to lower demand and moving difficulty
- A model trained on full-year averages misses the actionable seasonal signal
- Timing of apartment search matters more than neighborhood choice for short-term savings
- Lease start date creates lock-in: a summer lease locks you into expensive annual renewals
- Supply constrained: fewer units listed in winter means less choice but better prices

**Example:** The same 1BR apartment in Boston: $2,800/mo lease starting June vs $2,400/mo starting January. A model predicting "fair rent = $2,600" is always wrong — overestimates in winter (user overpays) and underestimates in summer (user has unrealistic expectations). The annual average is never the actual price.

### Visualization (canvas `canvas3`, 720×300)

Seasonal sine-wave line chart of a rent index over 24 months with shaded peak/trough seasons and a flat average line.

- **Title (bold 17px `#1a5276`, centered):** "Seasonal Rent Cycle: Summer Peak, Winter Trough".
- **Chart area:** left=60, right=690, top=35, bottom=165; y-axis Rent Index 0.85–1.15 with gridlines and labels every 0.05 (`#d5dbdb` gridlines, `#2c3e50` labels); rotated y-axis label "Rent Index".
- **X-axis:** 24 monthly labels 'J F M A M J J A S O N D' repeated twice, with "Year 1" and "Year 2" group labels below.
- **Data:** 24 monthly values from value = 1.0 + 0.13·sin((monthInYear − 2)·π/6) — peak ~June, trough ~Dec; blue line `#2980b9` width 2.5 with radius-3 blue dots at each month.
- **Shading:** peak months May–Aug tinted `rgba(231,76,60,0.1)` each year; trough months Nov–Feb tinted `rgba(39,174,96,0.1)`.
- **Average line:** horizontal dashed gray (`#7f8c8d`, dash 6/4, width 1.5) at 1.0, labeled 11px gray "Annual average (model uses this)".
- **Annotations (bold 11px, centered):** red "Peak" above the first summit; green two lines near the first trough: "Save 10-15%" / "by timing".

## Amenity Description vs Reality Gap

- "Newly renovated" = painted one wall or replaced cabinet handles
- "Spacious" = 400 sqft studio; "cozy" = can touch both walls from bed
- "Natural light" = one small window; "sun-drenched" = window faces south
- Text features from listings are systematically deceptive marketing language
- No standardized definitions: "luxury" means different things at different price points
- Images are selected/staged: wide-angle lens makes rooms look 50% larger; photos taken in best light

**Example:** NLP model trained on listing descriptions finds "recently updated" correlates with LOWER actual quality scores from tenant reviews. Landlords use aspirational language precisely when reality is worse — the text signal is anti-correlated with truth. A model that takes descriptions at face value makes systematically wrong predictions.

### Visualization (canvas `canvas4`, 720×300)

Paired horizontal bar chart: listing-claim scores vs tenant-survey reality scores with gap labels between the columns.

- **Title (bold 17px `#1a5276`, centered):** "Listing Language vs Tenant-Reported Reality".
- **Columns:** claim bars start at x=100, reality bars at x=500, max bar width 150 for a /10 score; column headers bold 12px: blue "Listing Claim (/10)" and red "Reality - Tenant Survey (/10)"; orange `#e67e22` header "GAP" between them.
- **Rows (right-aligned 11px labels; blue `#2980b9` claim bars, red `#e74c3c` reality bars, white bold 10px in-bar value labels like "8/10"):**
  - "Newly renovated" — claim 8, reality 4 (gap -4.0)
  - "Spacious" — claim 8, reality 4.2 (gap -3.8)
  - "Quiet" — claim 9, reality 5 (gap -4.0)
  - "Great location" — claim 9, reality 6 (gap -3.0)
  - "Natural light" — claim 8, reality 3 (gap -5.0)
- **Gap markers:** dashed red connector line (dash 3/2, width 1.5) from the end of each claim bar to the reality column, with a bold 11px red gap label ("-4.0" etc.) centered above it.

## Rent Control Distortions

- Rent-controlled unit at $800/mo in a $3,000/mo market neighborhood
- Including rent-controlled units in "comparable" analysis destroys pricing model accuracy
- Regulated and market-rate units are fundamentally different populations that must be separated
- Rent control status often not in data — impossible to filter without external registry matching
- Stabilized rents create bimodal distribution: low cluster (controlled) + high cluster (market)
- Model trained on both populations predicts the average ($1,900) which matches NEITHER reality

**Example:** In San Francisco, 30% of units are rent-controlled. A pricing model including these shows "average rent = $2,100" for a neighborhood. But actual market-rate units cost $3,200 and controlled units cost $900. A tenant searching at $2,100 finds literally nothing — that price point doesn't exist in reality.

### Visualization (canvas `canvas5`, 720×300)

Bimodal density curve of rents with a dead zone between the two modes where the model's prediction falls.

- **Title (bold 17px `#1a5276`, centered):** "Rent Control Creates Bimodal Distribution".
- **Chart area:** left=70, right=680, top=35, bottom=160; x-axis $0–$4,500 with "$" tick labels every $500; axis label "Monthly Rent ($)".
- **Distribution:** blue curve (`#2980b9`, width 2, fill `rgba(41,128,185,0.2)`) = Gaussian(mean $900, std $200, amp 0.7·H) + Gaussian(mean $3,200, std $350, amp 0.85·H).
- **Dead zone:** region $1,500–$2,600 tinted `rgba(231,76,60,0.08)`, with bold 11px red three-line centered label: "Dead zone:" / "No apartments" / "at predicted price".
- **Model prediction line:** vertical dashed red (`#e74c3c`, dash 6/4, width 2) at $2,100, labeled bold 12px red two lines above: "Model prediction" / "(average: $2,100)".
- **Mode labels (bold 11px, centered, two lines):** purple `#8e44ad` "Rent controlled" / "30%" over the $900 peak; blue "Market rate" / "70%" over the $3,200 peak.

## Neighborhood Gentrification Trajectory

- Apartment value depends on FUTURE neighborhood state, not current conditions
- Model sees current crime rates, income levels, amenity counts — all static/backward-looking features
- The 5-year trajectory (gentrifying vs declining) determines investment value, not today's snapshot
- Leading indicators (new coffee shops, art galleries, building permits) predict but aren't in standard datasets
- Static features miss the dynamic process: they record the level today, never the rate of change
- A "bad" neighborhood improving fast is better value than a "good" one plateauing at its ceiling
- Gentrification creates non-stationarity: the price process itself changes shape mid-sample
- Historical prices in transforming neighborhoods don't predict those same neighborhoods' future prices

**Example:** In 2015, Bushwick (Brooklyn) had crime rates and income levels similar to East New York. By 2022, Bushwick rents doubled while East New York stayed flat. Any model using 2015 features would have valued them equally — the trajectory was the only differentiator, and it wasn't in the data.

### Visualization (canvas `canvas6`, 720×300)

Two diverging rent-trajectory lines (2015–2025) that start at the same point.

- **Title (bold 17px `#1a5276`, centered):** "Static Features Miss Gentrification Trajectory".
- **Chart area:** left=70, right=650, top=35, bottom=160; x-axis years 2015–2025 (yearly gridlines `#d5dbdb` and labels); y-axis $1,000–$3,500 with "$" labels every $500; axis labels "Year" and rotated "Rent ($/mo)".
- **Bushwick line (green `#27ae60`, width 2.5):** rent = 1500 + 1700·t^1.5 where t = (year − 2015)/10, capped at $3,400; labeled bold 12px green "Bushwick (gentrifying)".
- **East New York line (red `#e74c3c`, width 2.5):** rent = 1400 + 100·t + 30·sin(4t); labeled bold 12px red "East New York (stagnant)".
- **2015 marker:** vertical dashed purple line (`#8e44ad`, dash 4/3, width 1.5) at 2015, with bold 11px purple two-line annotation: "Model sees: both ~$1,450" / "(similar features)".
- **2022 gap:** orange `#e67e22` vertical I-beam at 2022 spanning $1,470–$3,000, labeled bold 10px orange two lines: "Huge gap" / "by 2022".
- **Bottom-right annotation (bold 11px `#1a5276`):** "Trajectory = the signal. Snapshot = misleading.".

## Regeneration instructions

- **Layout:** per pitfall, an `<h2>` section heading followed by a single-row `.obj-table`: full-width table, left `<td>` (40%) with `.obj-title` (repeating the h2 text), a `<ul>` of bullets, and an `.example` callout div (`<strong>Example:</strong>` + text); right `<td>` (60%, centered) holds one canvas 720×300.
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.4em `#1a5276` with 2px solid `#2980b9` bottom border; subtitle `#666` 1.05em; table cell borders `1px solid #e0e0e0`, padding 20px 24px, even rows `#fafcfe`; `.obj-title` 1.05em weight 600 `#1a5276`; bullets 0.9em `#333`; `strong` `#1a5276`; `.example` background `#eaf2f8`, padding 10px 14px, radius 6px, 0.92em. A `.philosophy` style (background `#f0f4f8`, left border `4px solid #2980b9`) is defined but unused. No nav bar, no back/home links.
- **Canvas:** each canvas declares intrinsic `width="720" height="300"`; scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setupCanvas(id)` helper. Chart titles are bold 17px, centered.
- **Palette:** primary blue `#1a5276`, mid blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, dark text `#2c3e50`, gray `#7f8c8d`, gridlines `#d5dbdb`.
