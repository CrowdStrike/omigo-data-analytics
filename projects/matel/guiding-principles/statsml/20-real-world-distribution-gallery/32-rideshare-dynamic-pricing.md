# Rideshare Dynamic Pricing — Distribution Patterns

**Page type:** detail page (three-column obj-table layout: text left ~38%, histogram canvas middle ~31%, insight canvas right ~31%, one table per section)
**HTML title tag:** Rideshare Dynamic Pricing — Distribution Patterns

**Subtitle:** 5 simulated distributions where surge pricing tracks rider urgency more closely than driver scarcity

## Surge Multiplier (Urgency-Based, Not Scarcity)

**Pitfall label:** URGENCY NOT SCARCITY (color `#795548`)

Most rides at 1.0x (N=1500), but spikes at 1.5x, 2.0x, 2.5x, 3.0x during "high demand." In this simulation, the surge spikes line up with moments riders can't wait (rain, late night, airport) more than with driver shortage.

- 1.0x base = majority of rides when users have options
- Here, surge spikes coincide with captive riders, not scarce drivers
- Simulated driver shortage vs surge: r ≈ 0.3 — a weak link
- One reading: prices rise exactly when you can't say no

### Visualization (canvas `canvas1`, 420×340)

Histogram of surge multipliers from a seeded simulation (mulberry32, seed 42, shared across all charts).

- **Title (bold `#1a5276`, top center):** "Surge Multiplier Distribution".
- **Data generation:** 1500 values Normal(1.0, 0.08); 250 values Normal(1.5, 0.12); 220 values Normal(2.0, 0.15); 180 values Normal(2.5, 0.15); 150 values Normal(3.0, 0.18).
- **Bins/axes:** 30 bins, x range 0.8 to 4, x tick labels at 6 evenly spaced values formatted as "N.Nx"; x-axis label "Surge Multiplier". Y implicit (bars scaled to max bin count). Gray `#999` L-shaped axes; margins top 35 / right 20 / bottom 40 / left 50; white background.
- **Bars:** fill `rgba(231,76,60,0.5)`, border `#e74c3c` 0.5px.
- **Overlay:** Gaussian-smoothed density line in `#1a5276` (width 2) with a 95% SE band filled `rgba(230,126,34,0.22)` (smoothing sigma 1.5, effective N clamped 30–200).

### Visualization (canvas `canvas1b`, 400×340)

Scatter plot: driver shortage vs surge multiplier showing weak correlation.

- **Title (bold red `#e74c3c`, top center):** "Driver Shortage vs Surge (r ≈ 0.3)".
- **Data:** 120 points; x = shortage uniform 0–100%; y = surge = 1.0 + (shortage/100)×0.9 + uniform(0, 2.5), capped at 4 (weak correlation, yields r ≈ 0.30 with the seed).
- **Axes:** L-shaped gray `#999` axes; padding top 40 / right 20 / bottom 55 / left 55. X label "Actual Driver Shortage (%)" centered below; Y label "Surge Multiplier" rotated vertical on left. Y maps surge 0.8–4.0 over plot height.
- **Highlight zone:** dashed red (`#e74c3c`, dash 3/3) rectangle over left 40% width × top 50% height, filled `rgba(231,76,60,0.1)`, labeled in bold red 9px "CAPTIVE AUDIENCE" / "PRICING" plus 8px lines "High surge, drivers" / "ARE available".
- **Points:** radius 3.5; red `rgba(231,76,60,0.8)` when shortage < 40% and surge > 2.2 ("exploitative"), otherwise dark slate `rgba(44,62,80,0.5)`.
- **Arrow annotation:** dark red `#c0392b` arrow from ~(45%, 30%) to ~(60%, 45%) of plot area, with bold `#c0392b` text "Here, surge tracks urgency" / "more than scarcity".
- **Bottom strike-through gag:** italic gray 9px text `"Dynamic pricing optimizes marketplace efficiency"` with a red `#e74c3c` strike line through it, followed by bold red 10px "One reading: charges more when you can't say no".

## Price Variation Same Route (Personalized)

**Pitfall label:** SAME RIDE DIFFERENT PRICE (color `#2980b9`)

Same route (A to B, 5 miles) quoted to 500 simulated users at the same time. Prices range from $12 to $28 — wide and person-dependent, not a tight normal. One explanation for a spread this wide: per-rider pricing.

- $16 spread for identical service at identical time
- Alleged factors (from rider reports): acceptance history, battery, urgency signals
- Riders report price-comparers seeing lower quotes, always-accepters seeing higher ones — unverified

### Visualization (canvas `canvas2`, 420×340)

Histogram of quoted prices (same drawHistogram helper as canvas1).

- **Title:** "Same Route Price Quotes (500 Users, Same Time)".
- **Data:** 500 values of 15 + uniform(−3, 13) dollars.
- **Bins/axes:** 30 bins, x range 10 to 30, x tick format "$N" (integer); x-axis label "Price ($)".
- **Bars:** fill `rgba(44,62,80,0.5)`, border `#2c3e50`. Same `#1a5276` density line + `rgba(230,126,34,0.22)` SE band overlay.

### Visualization (canvas `canvas2b`, 400×340)

Horizontal dot strip: 50 individual users' quotes for the identical ride.

- **Title (bold `#2c3e50`, top center):** "50 Users — Same Ride, Same Time".
- **Data:** 50 prices from 15 + uniform(−3, 13), clamped to $12–$28, sorted ascending; plotted in 5 rows of 10 dots each, x positioned by price on a $12–$28 scale, rows spaced 22px around a central gray `#999` horizontal axis line. Padding top 45 / right 20 / bottom 95 / left 40.
- **Dot color gradient:** interpolated by price t=(price−12)/16: r = 44+t×187, g = 62−t×40, b = 180−t×130 at 0.8 alpha (blue for cheap → red for expensive), radius 5 with solid same-color 1px stroke.
- **Spread arrow:** red `#e74c3c` double-ended bracket line spanning from lowest to highest quote below the dots, labeled bold red 11px "~$15 SPREAD for identical service".
- **Legend:** blue `#2980b9` dot + text "Low price (patient, checks prices)"; red `#e74c3c` dot + text "High price (urgent, always accepts)" (bold 9px, left-aligned).
- **Factor box:** thin `#555` outlined box at bottom containing centered 9px `#333` text "Alleged factors: battery, acceptance history, destination type, time pressure".

## Airport/Event Captive Audience (Trapped vs Choice)

**Pitfall label:** CAPTIVE AUDIENCE (color `#27ae60`)

Prices at airport pickup queue vs same-distance city rides. In this simulation, airport rides average ~2x the city rides. The combined distribution is bimodal — one peak for riders with choices, one for captive riders.

- Airport mean $45 vs city mean $22 for same distance
- Fewer alternatives → weaker price constraint
- Bags, tired, no transit → hard to walk away
- "Airport fees" is one story; captive demand is another

### Visualization (canvas `canvas3`, 420×340)

Histogram of combined airport + city prices (bimodal).

- **Title:** "Combined: Airport vs City (Same Distance)".
- **Data:** 1000 values Normal(45, 8) (airport) plus 1000 values Normal(22, 5) (city).
- **Bins/axes:** 35 bins, x range 10 to 70, x tick format "$N"; x-axis label "Price ($)".
- **Bars:** fill `rgba(230,126,34,0.5)`, border `#e67e22`. Same density-line + SE-band overlay.

### Visualization (canvas `canvas3b`, 400×340)

Two overlaid analytic normal curves separating the bimodal mixture.

- **Title (bold `#e67e22`, top center):** "Same Distance, Different Context".
- **Curves:** x range 5–70; city curve Normal(22, 5) — fill `rgba(41,128,185,0.4)`, stroke `#2980b9` width 2.5; airport curve Normal(45, 8) — fill `rgba(231,76,60,0.35)`, stroke `#e74c3c` width 2.5. Both peaks scaled to 85% of plot height. Padding top 40 / right 15 / bottom 60 / left 45; gray `#999` x-axis baseline.
- **Curve labels:** above city peak, blue bold 11px "CITY" with 9px "(has alternatives)"; above airport peak, red bold 11px "AIRPORT" with 9px "(captive)".
- **Gap arrow:** dark red `#c0392b` double-headed horizontal arrow between the two peak positions at ~45% plot height, labeled bold 10px "2x for same distance" above and 9px "fewer alternatives, weaker price constraint" below.
- **Bottom edge labels:** left blue 9px "Can walk away"; right red 9px "Bags, tired, no transit".
- **Bottom annotation:** italic gray 9px 'Marketing: "Airport fees and longer pickup times"' then bold red 9px "The other story: captive demand".

## Low Battery Price Signal (Controversial Factor)

**Pitfall label:** EXPLOITATION SIGNAL (color `#e74c3c`)

In this simulation, riders with <20% battery accept surge ~2x as often as riders with >60%. The premise echoes a widely reported real finding that low-battery riders accept surge more. Your phone knows how desperate you are.

- <20% battery: ~83% accept ("DESPERATION ZONE")
- >60% battery: ~42% accept ("WILL WAIT")
- Company statement: "We don't use battery for pricing"
- Whether battery influences anything else (e.g., offer timing) is unverified

### Visualization (canvas `canvas4`, 420×340)

Histogram of rider battery levels.

- **Title:** "Rider Battery Level Distribution (N=500)".
- **Data:** 500 values uniform 0–100.
- **Bins/axes:** 20 bins, x range 0 to 100, x tick format "N%"; x-axis label "Battery %".
- **Bars:** fill `rgba(142,68,173,0.5)`, border `#8e44ad`. Same density-line + SE-band overlay.

### Visualization (canvas `canvas4b`, 400×340)

Scatter plot: battery level vs surge acceptance rate with shaded zones.

- **Title (bold `#8e44ad`, top center):** "Battery Level vs Surge Acceptance".
- **Data:** 150 points; x = battery uniform 0–100; y = acceptance = 0.9 − 0.006×battery + uniform(−0.1, 0.1), clamped to [0.1, 1.0].
- **Zones:** battery <20% shaded `rgba(231,76,60,0.12)`; battery >60% shaded `rgba(39,174,96,0.08)`. Padding top 38 / right 15 / bottom 65 / left 50; gray `#999` L axes.
- **Points:** radius 3.5; red `rgba(231,76,60,0.8)` if battery <20, green `rgba(39,174,96,0.7)` if battery >60, else purple `rgba(142,68,173,0.5)`.
- **Trend line:** dashed purple `#8e44ad` (dash 5/3, width 2) from y=0.9 at left edge down to y=0.3 at right edge.
- **Zone labels:** red bold 9px "DESPERATION" / "ZONE" at x≈10% near bottom, with 8px "83% accept" / "surge here" near top; green bold 9px "WILL WAIT" at x≈80% near bottom, with 8px "42% accept" near top.
- **Caption (bold purple 10px, centered below plot):** "Your phone knows" / "how desperate you are".
- **Nuance line (8px `#555`):** `Company: "We don't use battery for pricing." — Any other use of it is unverified.`
- **Axis labels:** x "Battery Level (%)"; y rotated "Acceptance Rate".

## End-of-Night Extraction (Time Desperation)

**Pitfall label:** LAST CALL EXTRACTION (color `#8e44ad`)

Surge multiplier flat 1.0-1.3x during the day, then a hockey stick at 1am-3am (bar closing) — riders at closing time are in the worst position to price-compare, walk, or wait. Higher prices do pull drivers to bar districts; they also land on riders who can't refuse. Both readings fit the same shape.

- Day: competitive pricing, 1.0-1.3x (users have options)
- 2am peak: ~2.8x average (2-4x range)
- Safety argument is real — getting drunk people home matters
- The ethical tension IS the distribution shape

### Visualization (canvas `canvas5`, 420×340)

Histogram of surge multipliers, day vs bar-close mixture.

- **Title:** "Surge Multiplier: Day vs Bar Close".
- **Data:** 1500 values uniform 1.0–1.3 (day); 500 values Normal(2.8, 0.5) clamped to [2.0, 4.0] (night spike).
- **Bins/axes:** 30 bins, x range 0.8 to 4.5, x tick format "N.Nx"; x-axis label "Surge Multiplier".
- **Bars:** fill `rgba(39,174,96,0.5)`, border `#27ae60`. Same density-line + SE-band overlay.

### Visualization (canvas `canvas5b`, 400×340)

24-hour surge timeline line chart with shaded day/night zones.

- **Title (bold `#1a5276`, top center):** "24-Hour Surge Timeline".
- **Data:** hourly surge for hr 0–23: hours 1–3 follow 1.0 + 1.8×exp(−0.5×((hr−2)/0.8)²) peaking at 2am ≈ 2.8x; hours 0 and 4 are 1.5 + uniform(0, 0.3); all other hours 1.0 + uniform(0, 0.2). Y scale 0.8 to 3.2. Padding top 38 / right 15 / bottom 55 / left 50.
- **Zones:** 6am–11pm shaded green `rgba(39,174,96,0.08)`; midnight–4am shaded red `rgba(231,76,60,0.1)`; area under the curve for hours 0–5 additionally filled `rgba(231,76,60,0.25)`.
- **Line:** red `#e74c3c`, width 3, connecting all 24 hourly points; gray `#999` L axes.
- **Peak annotation:** dark red `#c0392b` arrow pointing to the 2am peak, with bold 11px "BAR CLOSE" and 9px "2.8x avg surge".
- **Zone labels:** green bold 9px "COMPETITIVE" with 8px "(users have options)" over the day zone; red bold 9px "EXTRACTION" over the night zone.
- **X ticks:** '12am', '4am', '8am', '12pm', '4pm', '8pm', '12am' evenly spaced. **Y ticks:** "1.0x" at baseline, "3.0x" near top (right-aligned, `#555`).
- **Bottom annotation:** 9px `#333` `Safety: "We get drunk people home safely" — also: they can't price-compare or wait.` then bold `#1a5276` 10px "The ethical tension IS the distribution shape".

## Regeneration instructions

- **Layout:** one `.obj-table` per section (five total), each a single `<tr>` with three `<td>`s: text cell 38% (pitfall label span, `<h3>` title, paragraph, `<ul>` bullets), middle cell 31% centered (histogram canvas 420×340), right cell 31% centered (insight canvas 400×340).
- **Page style:** body system sans-serif, margin 20px, background `#f9f9f9`, text `#333`; h1 centered `#1a5276`; `.subtitle` centered `#666` 0.95em; table cells `border: 1px solid #2980b9`, padding 12px, vertical-align top; h3 `#1a5276` 1.0em weight 700; p/li 14px, line-height 1.5–1.6; `.pitfall-label` inline-block bold 0.72em uppercase letter-spacing 0.5px; `canvas { width: 100%; height: auto; }`.
- **Pitfall label colors:** assigned by document order from the cycling palette `["#795548","#2980b9","#27ae60","#e74c3c","#8e44ad","#e67e22","#16a085","#d35400","#c0392b","#1abc9c"]` via a small script that colors each `.pitfall-label`.
- **Data:** all simulated with a seeded mulberry32 RNG (seed 42) shared sequentially across charts, plus a Box-Muller `randNormal(mean, std)` helper; a shared `drawHistogram(canvasId, data, options)` helper draws title, axes, bars, a Gaussian-smoothed density line (`#1a5276`) with 95% SE band (`rgba(230,126,34,0.22)`), and x tick labels.
- **Canvas:** declare intrinsic `width`/`height` attributes per chart; sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, plus accents `#2980b9`, `#8e44ad`, `#2c3e50`, `#c0392b`; gray text `#555`/`#666`/`#333`. No nav bar, no back/home links.
- In regenerated HTML, any card links use `.html` extensions (this page has none).
