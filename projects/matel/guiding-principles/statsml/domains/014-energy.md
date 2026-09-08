# Energy / Utilities: Domain-Specific Pitfalls

**Page type:** detail page (h2 section per pitfall, each with a two-column obj-table row: text left 50%, canvas right 50%)
**HTML title tag:** Domain Pitfalls: Energy / Utilities

**Subtitle:** Energy systems have unique data challenges — weather dominance, invisible distributed generation, cascading failures, and multi-decade planning horizons.

## Callout (philosophy box)

Energy analytics must contend with physical infrastructure constraints, regulatory requirements, and the fundamental mismatch between short-term operational data and decades-long asset planning. Models trained on recent data may catastrophically fail when grid topology changes or when distributed energy resources reach critical mass.

## Weather Dominates 70% of Demand Variation

**Temperature is the dominant signal**

Approximately 70% of electricity demand variation is explained by weather — primarily temperature. This creates several modeling traps:

- Any "demand forecasting model" that omits weather is really just a weather proxy
- Non-weather features (economic growth, EV adoption) are drowned out by temperature noise
- Climate change shifts the temperature-demand curve over time
- Extreme weather events produce demand spikes outside training distributions

**Key risk:** Models appear accurate until an unusual weather year exposes them as temperature parrots.

### Visualization (canvas `canvas1`, 720×300)

Scatter plot with U-shaped trend line: temperature vs electricity demand.

- **Title (bold 13px, `#1a5276`, top center):** "Temperature vs Electricity Demand".
- **Data:** 120 random scatter points; x = temperature uniform in 10–90°F; y = demand from U-curve `0.02*(temp-65)^2 + 30` GW plus uniform noise ±6. Points are 3.5px-radius dots filled `rgba(26, 82, 118, 0.55)`.
- **Trend line:** red `#e74c3c`, width 2.5, tracing the exact U-curve `0.02*(t-65)^2 + 30` for t = 10..90.
- **Axes:** margins top 25 / right 30 / bottom 35 / left 60; plot background `#f8f9fa`; L-shaped axes in `#ccc`. X labels "20°F"…"90°F" every 10°F mapped over range 10–90; Y labels "20 GW", "35 GW", "50 GW", "65 GW", "80 GW" mapped over value range 15–85; light `#eee` gridlines at each label.
- **Labels:** "R² = 0.70" in red `#e74c3c` 11px at top-right of plot; gray `#666` 10px annotations "Heating" near top-left and "Cooling" near top-right of the plot area.

## Behind-the-Meter Generation is Invisible

**Utilities cannot see rooftop solar and home batteries**

With distributed solar and battery storage growing exponentially, a large and increasing share of generation is invisible to the utility:

- Net metering shows only net consumption — true demand is unknowable
- Home batteries shift solar generation to evening peaks, masking patterns
- Apparent "demand reduction" may actually be invisible local generation
- Forecasting models trained on net load will systematically underestimate true demand

**Key risk:** Infrastructure planning based on observed load misses the hidden generation that keeps growing.

### Visualization (canvas `canvas2`, 720×300)

Diagram: utility visibility gap, split by a meter boundary line.

- **Title (bold 13px, `#1a5276`, top center):** "Utility Visibility Gap".
- **Boundary:** vertical dashed red line (`#e74c3c`, dash 6/4, width 2) at x=320 from y=30 to bottom, labeled below in bold red 11px: "UTILITY METER".
- **Left side header (bold 12px `#1a5276`, centered at x=160):** "VISIBLE TO UTILITY".
  - Blue box `#2980b9` at (80,60) 60×40 labeled "Grid" in white 10px.
  - Blue connector lines (`#2980b9`, width 2) from grid box to a branch at x=220.
  - Green box `#27ae60` at (230,65) 70×30 with white text "Net Load" / "5 kW".
  - Green arrow (`#27ae60`, width 2) at y=130 from x=195 to x=280, with green 10px label "Measured: 5 kW" below.
- **Right side header (bold 12px `#e74c3c`, centered at x=520):** "INVISIBLE TO UTILITY".
  - House: light gray box `#f0f0f0` with `#666` border at (420,55) 90×55, labeled "Home Load" / "8 kW" in gray 10px.
  - Solar panel: orange box `#e67e22` at (540,55) 80×30, white text "Solar PV" / "3 kW".
  - Battery: purple box `#8e44ad` at (540,95) 80×25, white text "Battery".
  - Orange arrow (`#e67e22`, width 1.5) from solar box pointing left toward the house at y=70.
- **Equation box (bottom right):** rectangle at (350,135) 330×45, fill `#f0f4f8`, border `#2980b9`; centered dark text 11px, two lines: "True Demand = 8 kW" / "Utility Sees = 8 - 3 = 5 kW (undercount by 37%)".

## Cascading Blackouts and Systemic Risk

**Single-node models miss cascading failure dynamics**

Power grids are networked systems where one failure can trigger cascading collapses:

- A single transmission line trips, redistributing flow to adjacent lines
- Overloaded neighbors trip in sequence — cascade propagates in seconds
- Statistical models treating nodes independently miss systemic coupling
- The 2003 Northeast blackout: one software bug + one untrimmed tree = 55 million without power

**Key risk:** Risk models that treat components independently will always underestimate tail risk in networked infrastructure.

### Visualization (canvas `canvas3`, 720×300)

Network diagram: cascading failure shown as four time-stage snapshots left to right.

- **Title (bold 13px, `#1a5276`, top center):** "Cascading Failure Propagation".
- **Stages (node positions x,y and states):**
  - "t = 0s": (80,70) failed; (80,130) ok; (130,100) ok.
  - "t = 2s": (250,70) failed; (250,130) stressed; (300,100) stressed.
  - "t = 5s": (420,70) failed; (420,130) failed; (470,100) stressed; (470,60) stressed; (470,140) stressed.
  - "t = 8s": (590,70) failed; (590,130) failed; (640,100) failed; (640,60) failed; (640,140) failed; (680,80) failed; (680,120) failed.
- **Node state colors:** ok `#27ae60`, stressed `#e67e22`, failed `#e74c3c`. Nodes are 10px-radius circles with 2px white stroke; failed nodes show a bold white "✗" glyph centered.
- **Edges:** within each stage, connect node pairs closer than 80px; edge color `#e74c3c` if both endpoints failed else `#ccc`; width 1.5 if either endpoint failed else 1.
- **Stage labels:** `#1a5276` 11px centered under each stage cluster at y = h-20 ("t = 0s", "t = 2s", "t = 5s", "t = 8s").
- **Arrows between stages:** gray `#999` right-pointing arrows at y=100 starting at x = 165, 350, 530 (30px long).
- **Legend (bottom left, 10px):** green dot "Normal", orange dot "Overloaded", red dot "Failed".

## 40-Year Asset Lifecycle vs. Prediction Horizon

**Predicting demand patterns 40 years out is impossible**

Power plants and transmission lines are built for 40+ year lifespans, but the world changes unpredictably:

- A gas plant built in 2020 must still operate in 2060 — what does demand look like then?
- EV adoption, heat pumps, AI data centers — each can double local demand
- Regulatory shifts (carbon pricing) can strand assets overnight
- Prediction uncertainty grows exponentially, not linearly, with time horizon

**Key risk:** Presenting 40-year demand forecasts as point estimates hides the enormous uncertainty that should drive planning.

### Visualization (canvas `canvas4`, 720×300)

Fan chart: demand forecast with widening uncertainty bands over 40 years.

- **Title (bold 13px, `#1a5276`, top center):** "Demand Forecast Uncertainty Over Time".
- **Axes:** margins top 30 / right 30 / bottom 35 / left 60; plot background `#f8f9fa`; L-shaped `#ccc` axes. X labels: years 2025, 2030, 2035, 2040, 2045, 2050, 2055, 2060, 2065 mapped over a 40-year span (2025–2065) with `#eee` gridlines. Y-axis rotated label "Demand (GW)" in gray `#666` 10px; value range 30–110 GW mapped to plot height.
- **Central forecast:** line `#1a5276` width 2.5: demand = 60 + 0.8×(years since 2025), i.e. 60 GW at 2025 rising to 92 GW at 2065.
- **Uncertainty bands** (drawn outer to inner, half-width = mult × 0.5 × yr^1.3):
  - 95% CI: mult 3.0, fill `rgba(231, 76, 60, 0.12)`.
  - 80% CI: mult 2.0, fill `rgba(230, 126, 34, 0.18)`.
  - 50% CI: mult 1.0, fill `rgba(41, 128, 185, 0.2)`.
- **Markers:** dashed gray vertical line (dash 4/3) at left edge labeled "Now" (gray 10px); dashed red `#e74c3c` vertical line at right edge labeled "Asset EOL" (red 10px, right-aligned).
- **Legend (below x-axis, 9px):** color swatches with labels "50% CI" (`rgba(41,128,185,0.4)`), "80% CI" (`rgba(230,126,34,0.35)`), "95% CI" (`rgba(231,76,60,0.25)`).

## Smart Meter Rollout Bias

**Early adopters are not representative of the general population**

Smart meter data is increasingly used for analytics, but rollout is not random:

- Early adopters tend to be higher-income, more tech-savvy, and more energy-conscious
- Models trained on early smart meter data learn patterns of engaged consumers
- General population has different load shapes, response to pricing signals, and flexibility
- Demand response programs designed from early data overestimate population-wide flexibility

**Key risk:** Policies calibrated on early-adopter data will underperform when applied to the full customer base.

### Visualization (canvas `canvas5`, 720×300)

Two overlaid daily load-profile curves (0–24 hours) with filled areas.

- **Title (bold 13px, `#1a5276`, top center):** "Daily Load Profile: Early Adopters vs General Population".
- **Axes:** margins top 30 / right 30 / bottom 40 / left 50; plot background `#f8f9fa`; `#ccc` L-axes. X labels "0:00" to "24:00" every 4 hours with `#eee` gridlines; Y label "kW" at top-left; y scale max 4.5 kW.
- **General population curve** (red `#e74c3c` line width 2.5, area fill `rgba(231, 76, 60, 0.15)`): sum of Gaussian bumps — base 1.5, morning peak `1.0*exp(-(hr-7.5)^2/3)`, evening peak `2.5*exp(-(hr-18.5)^2/3.5)`, night dip `-0.4*exp(-(hr-3)^2/8)`.
- **Early adopter curve** (blue `#2980b9` line width 2.5, area fill `rgba(41, 128, 185, 0.15)`): base 1.2, morning `0.8*exp(-(hr-7)^2/4)`, midday solar dip `-0.9*exp(-(hr-13)^2/6)`, evening `1.5*exp(-(hr-19)^2/4)`, night `-0.3*exp(-(hr-3)^2/8)`.
- **Annotations:** blue 10px label "Solar dip" with a short blue pointer line below the early-adopter curve at hour 13; dashed dark vertical connector (dash 2/2) at hour 18.5 between the two curves, labeled "Peak gap" in 9px `#333`.
- **Legend (below x-axis, 11px):** blue line sample + "Early Adopters (smart meter data)"; red line sample + "General Population (true demand)".

## Regeneration instructions

- **Layout:** h1 + `.subtitle` + `.philosophy` callout, then one `h2` per pitfall (1.4em `#1a5276`, bottom border `2px solid #2980b9`, padding-bottom 8px), each followed by a `.obj-table` with a single `<tr>`: left `<td>` (45%) holds `.obj-title` (1.05em, weight 600, `#1a5276`), an intro paragraph, a `<ul>` of bullets, and a **Key risk** paragraph; right `<td>` (55%, centered) holds the canvas.
- **Table style:** full width, border-collapse; cell borders `1px solid #e0e0e0`, padding 20px 24px, vertical-align middle; even rows background `#fafcfe`.
- **Page CSS:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; `.subtitle` `#666` 1.05em; paragraphs `#333` 0.95em; `ul` 0.9em `#333`; `strong` `#1a5276`; `.philosophy` background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, bar/point fill `rgba(26,82,118,0.55)`, gray text `#666`/`#333`.
- **Canvas:** declare intrinsic `width`/`height` attributes as given per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
