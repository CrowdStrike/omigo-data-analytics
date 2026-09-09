# Commercial Real Estate Domain: Data Pitfalls

**Page type:** detail page (one h2 per pitfall, each followed by a two-column obj-table row: text left 50%, canvas right 50%)
**HTML title tag:** Commercial Real Estate Domain: Data Pitfalls

**Subtitle:** Lease structures, rate regimes, contractual cascades, and tenant credit risk that make commercial property data misleading.

## Lease vs Market Rent Divergence (Lease Roll Risk)

- Tenant signed a 10-year lease in 2019 at $50/sqft; current market rate is now $35/sqft
- The building APPEARS to generate $50/sqft revenue but is actually overvalued relative to market
- When lease expires (lease roll), tenant will renegotiate to market rate → sudden 30% income drop
- Conversely: below-market leases in rising markets hide upside potential until roll date
- Net Operating Income (NOI) based on in-place rents ≠ sustainable NOI at market rents
- Weighted Average Lease Term (WALT) determines when the cliff hits — short WALT = imminent repricing

**Example:** A Manhattan office tower shows $80M NOI on in-place leases signed 2017-2019. Market rents have fallen 35% post-COVID. When leases roll in 2027-2029, NOI drops to $52M. A buyer paying 20x the current NOI ($1.6B) is actually paying 30x the sustainable NOI — they overpaid by $560M.

### Visualization (canvas `canvas1`, 720×300)

Line chart: flat in-place lease rent vs falling market rent, with a red cliff at lease expiry.

- **Title (bold 17px `#1a5276`, centered):** "Lease Roll Risk: In-Place vs Market Rent".
- **Chart area:** left=60, right=w-30, top=35, bottom=h-30; y-axis $/sqft scaled 20–60 with "$" labels at 20, 30, 35, 40, 50, 60 and light gridlines `#ecf0f1`; x-axis years 2019–2032 with labels at 2019, 2021, 2023, 2025, 2027, 2029, 2031.
- **In-place rent line (blue `#2980b9`, width 2.5):** horizontal at $50 from 2019 to 2029.
- **Market rent line (green `#27ae60`, dashed 5/3, width 2):** $50 at 2019 declining to $35 by 2022, then flat at $35 through 2032.
- **Post-cliff line (green solid, width 2.5):** $35 from 2029 to 2032.
- **Cliff arrow:** vertical red line (`#e74c3c`, width 3) at 2029 from $50 down to $35 with a downward arrowhead.
- **Shaded gap:** rectangle `rgba(231,76,60,0.12)` between $35 and $50 from 2022 to 2029.
- **Labels (11px):** blue "In-place lease rent ($50)"; green "Market rent ($35)"; bold red centered "Lease expiry" above the cliff; 10px red two lines "Apparent income" / "(overvalued)" inside the gap; 10px green "Market reality" after the cliff.

## Cap Rate Compression in Low-Rate Environments

- Cap rates (NOI/Value) compressed from 7% to 4% during 2015-2021 due to cheap debt
- Property values "appreciated" 75% but NOT because properties improved — purely monetary illusion
- Rising interest rates (2022+) → expanding cap rates → values crash back to reality
- Same building, same tenants, same income — worth $100M at 4% cap, worth $57M at 7% cap
- Investors mistook monetary accommodation for real value creation
- Historical cap rate data from low-rate era is useless for projecting values in normal-rate environment

**Example:** A suburban office building generating $4M NOI. Valued at $100M in 2021 (4% cap). By 2024, rates rose, cap rates expanded to 7%, same $4M NOI now values building at $57M. A 43% "crash" with zero change in the actual business. Models trained on 2015-2021 appreciation rates extrapolate fantasy.

### Visualization (canvas `canvas2`, 720×300)

Two side-by-side panels: cap rate over time (left) and mirror-image property value over time (right).

- **Title (bold 17px `#1a5276`, centered):** "Cap Rate Compression: Value Illusion from Cheap Debt".
- **Left panel ("Cap Rate", bold 10px blue header):** x 2010–2025 (labels 2010, 2015, 2021, 2025), y 4%–7% with gridlines `#ecf0f1`. Blue line (`#2980b9`, width 2.5): 7% (2010) → 5.5% (2015) → 4% (2021); then orange line (`#e67e22`, width 2.5): 4% (2021) → 5.5% (2023) → 6.5% (2025).
- **Right panel ("Property Value ($4M NOI)", bold 10px green header):** same x years; y $50M–$100M with "$...M" labels. Green line (`#27ae60`, width 2.5): $57M (2010) → $73M (2015) → $100M (2021); then red line (`#e74c3c`, width 2.5): $100M (2021) → $73M (2023) → $62M (2025).
- **Annotations (9px):** purple `#8e44ad`, two lines near the 2016 rise: "Monetary illusion" / "(not real appreciation)"; red near the 2023.5 decline: "Reality repricing".

## Work-From-Home Structural Shift (Regime Change)

- Office demand permanently shifted post-COVID: remote/hybrid work is structural, not temporary
- Models trained on 2015-2019 occupancy data assume workers return to offices — they didn't
- 30% of US office space may be permanently surplus (unprecedented vacancy)
- National office vacancy hit 20%+ in 2024 vs historical norm of 10-12%
- Largest regime change in commercial real estate in 50+ years
- Historical lease absorption rates, tenant expansion patterns, all obsolete post-2020

**Example:** A ML model trained on 2010-2019 data predicts office vacancy in 2024 at 11% (historical mean reversion). Actual vacancy is 22%. The model cannot learn a permanent structural break from cyclical data. Every "recovery" prediction based on past cycles is wrong because this isn't a cycle — it's a one-way shift.

### Visualization (canvas `canvas3`, 720×300)

Line chart: actual office vacancy diverging permanently from a mean-reverting model prediction after a 2020 structural break.

- **Title (bold 17px `#1a5276`, centered):** "WFH Structural Shift: Models Expect Recovery That Won't Come".
- **Chart area:** left=60, right=w-30, top=35, bottom=h-30; x 2010–2026 (labels every 2 years); y vacancy % scaled 5–25 with labels 8–22% every 2% and gridlines `#ecf0f1`.
- **Actual line (red `#e74c3c`, width 2.5)** through (year, %): (2010, 11), (2012, 10.5), (2014, 10), (2016, 10.5), (2018, 11), (2019, 11), (2020, 14), (2021, 17), (2022, 19), (2023, 21), (2024, 22), (2025, 21.5), (2026, 21).
- **Model prediction (gray dashed `#7f8c8d`, dash 6/4, width 2):** (2010, 11) flat to (2019, 11), (2020, 14), (2021, 13), (2022, 12), (2023, 11), (2024, 11), flat to (2026, 11).
- **Shaded gap:** polygon `rgba(231,76,60,0.1)` between the two lines from 2021 to 2026.
- **Structural break:** vertical dashed purple line (`#8e44ad`, dash 3/3, width 1.5) at 2020, labeled bold 10px purple "Structural break".
- **Line labels (10px):** red "Actual vacancy (22%)" near 2024; gray "Model prediction (11%)" near 2024.

## Co-Tenancy Clauses Cascade (Contractual Dominoes)

- Anchor tenant (e.g., Macy's) leaves a shopping mall
- Co-tenancy clause triggers: smaller tenants can reduce rent by 50% or terminate lease if anchor leaves
- One departure triggers contractual domino effect: 5-10 tenants exercise clauses simultaneously
- Mall goes from 95% occupied to 60% in months — not from market forces but contractual triggers
- The cascade is invisible in occupancy data until it happens — a cliff, not a slope
- Financial models using gradual vacancy assumptions miss the non-linear collapse mechanism

**Example:** When Sears closed in a Midwest mall (2018), co-tenancy clauses allowed 8 tenants to slash rent and 4 to leave entirely. NOI dropped 60% in 6 months. The mall's valuation went from $45M to $12M. A linear vacancy model would have predicted gradual 2%/year decline — it got a 35% overnight collapse.

### Visualization (canvas `canvas4`, 720×300)

Domino-block stack (left) plus NOI step-collapse chart vs a gradual-decline model (right).

- **Title (bold 17px `#1a5276`, centered):** "Co-Tenancy Cascade: One Exit Triggers Contractual Dominoes".
- **Domino blocks (left ~38% width, 22px tall, staggered 4px right and shrinking 8px per row, 0.85 alpha, white 10px text):**
  - "Anchor tenant leaves" — red `#e74c3c`
  - "Tenant B: rent cut 50%" — orange `#e67e22`
  - "Tenant C: terminates lease" — red
  - "Tenant D: rent cut 50%" — orange
  - "Tenant E: terminates" — red
  - "Tenant F: terminates" — red
- **NOI step chart (right ~55%):** y-axis $3M–$8M with "$...M" labels and gridlines `#ecf0f1`; red step line (`#e74c3c`, width 2.5) stepping down across 6 equal intervals through values $8M → $7M → $5.5M → $4.5M → $3.2M.
- **Model line:** gray dashed (`#7f8c8d`, dash 5/3, width 1.5) from $8M gently down to $6.5M.
- **Labels (10px, centered):** red "Reality: cascade collapse" below the final step; gray "Model: gradual decline" above the dashed line.

## NNN vs Gross Lease Incomparability

- Triple-Net (NNN): tenant pays rent + property taxes + insurance + maintenance
- Gross lease: landlord pays all expenses, tenant pays one all-inclusive number
- Same building: "$30/sqft NNN" vs "$45/sqft Gross" = roughly equivalent total cost
- But in data they look 50% different ($30 vs $45) — creating massive comparability errors
- Modified Gross adds further confusion: tenant pays some but not all expenses
- Without normalizing lease type, rent comparisons, pricing models, and market analyses are invalid

**Example:** A CRE analytics platform ingests 10,000 lease records. 40% are NNN, 40% are Gross, 20% Modified Gross. Without normalization, the platform shows average rent for an area varying from $28 to $52/sqft depending on which leases are sampled. A model trained on raw $/sqft without lease-type encoding produces predictions with ±40% noise from this single confound.

### Visualization (canvas `canvas5`, 720×300)

Grouped bar chart: reported $/sqft vs effective total cost for three lease types.

- **Title (bold 17px `#1a5276`, centered):** "NNN vs Gross: Same Cost, Incomparable Data".
- **Chart area:** left=80, right=w-40, top=40, bottom=h-30; y-axis $0–$55 with "$" labels at 10, 20, 30, 40, 50 and gridlines `#ecf0f1`; three groups, bars 30px wide, 10px apart within a group.
- **Groups (two-line x labels; orange `#e67e22` reported bar, green `#27ae60` effective bar, white bold 10px "$" value labels inside bar tops):**
  - "Building A (NNN)" — reported $30, effective $46
  - "Building B (Gross)" — reported $45, effective $45
  - "Building C (Mod Gross)" — reported $38, effective $47
- **Normalization connector:** dashed purple line (`#8e44ad`, dash 3/2, width 1) from the top of each reported bar to the top of its effective bar.
- **Legend (10px, top-left):** orange swatch "Reported $/sqft"; green swatch "Effective total cost".
- **Annotation (bold 10px, right-aligned, two lines):** red "Raw data says 50% spread"; green "Reality: within 3%".

## Tenant Credit Risk ≠ Property Value

- co-working startup signed $100M+ in leases across multiple buildings → went bankrupt
- The LEASE looked valuable on paper (long-term, high $/sqft) but tenant couldn't pay
- Property valued on lease quality (term, rate) not building quality → sudden 100% income loss when tenant defaults
- Single-tenant buildings: tenant bankruptcy = building goes from $X income to $0 overnight
- Credit rating of tenant is THE risk factor but rarely modeled as such in property valuation
- "Creditworthy" tenants can deteriorate quickly (retail chains, startups, pandemic-affected businesses)

**Example:** A building leased 100% to co-working startup at above-market rents of $65/sqft was valued at $200M based on the lease income. After co-working startup's bankruptcy, the building's value dropped to $80M (what the building is worth at actual market rents to a creditworthy tenant at $35/sqft). The owner's $200M mortgage now exceeds the building's value — negative equity from tenant risk masquerading as property value.

### Visualization (canvas `canvas6`, 720×300)

Before/after diagram: two building icons with a value-destruction arrow between them and a timeline below.

- **Title (bold 17px `#1a5276`, centered):** "Tenant Default: $200M → $80M Overnight".
- **Left building (BEFORE DEFAULT, bold 11px blue header):** blue `#2980b9` 60×70 rectangle with a 4×3 grid of white 10×10 windows; below: bold 13px green "$200M", then 10px `#2c3e50` lines "co-working startup lease: $65/sqft" and "15-year term".
- **Right building (AFTER DEFAULT, bold 11px red header):** gray `#7f8c8d` rectangle with light gray `#bdc3c7` windows (vacant); below: bold 13px red "$80M", then "Market rent: $35/sqft" and "Re-lease needed".
- **Arrow:** thick red horizontal arrow (`#e74c3c`, width 3, with arrowhead) between the buildings; bold 12px red "-$120M" above it, 10px "Value destroyed" below it.
- **Timeline (purple `#8e44ad`, near bottom):** horizontal line with 3 dots labeled "Signed", "Bankruptcy", "Revaluation".
- **Equation (bold 11px `#1a5276`, centered):** "Lease value ≠ Building value".

## Regeneration instructions

- **Layout:** per pitfall, an `<h2>` section heading followed by a single-row `.obj-table`: full-width table, left `<td>` (40%) with `.obj-title` (repeating the h2 text), a `<ul>` of bullets, and an `.example` callout div (`<strong>Example:</strong>` + text); right `<td>` (60%, centered) holds one canvas 720×300.
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.4em `#1a5276` with 2px solid `#2980b9` bottom border; subtitle `#666` 1.05em; table cell borders `1px solid #e0e0e0`, padding 20px 24px, even rows `#fafcfe`; `.obj-title` 1.05em weight 600 `#1a5276`; bullets 0.9em `#333`; `strong` `#1a5276`; `.example` background `#eaf2f8`, padding 10px 14px, radius 6px, 0.92em. A `.philosophy` style (background `#f0f4f8`, left border `4px solid #2980b9`) is defined but unused. No nav bar, no back/home links.
- **Canvas:** each canvas declares intrinsic `width="720" height="300"`; scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setupCanvas(id)` helper. Chart titles are bold 17px, centered.
- **Palette:** primary blue `#1a5276`, mid blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, dark text `#2c3e50`, gray `#7f8c8d`, gridlines `#ecf0f1`.
