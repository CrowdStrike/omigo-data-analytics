# Electricity / Smart Grid

**Page type:** detail page (one h2 per pitfall, each followed by a one-row two-column obj-table: text left 50%, canvas right 50%)
**HTML title tag:** Electricity / Smart Grid - Domain-Specific Pitfalls

**Subtitle:** Statistical pitfalls in grid analytics — temporal aliasing, net metering deceptions, and the cascade from weather forecast to capacity planning.

## 15-Minute Interval Hides Sub-Minute Spikes

**A 20 kW Spike Is Reported as 1.8 kW**

- **The mechanism:** Smart meters report only the average load over each 15-minute interval.
- **The example:** 0.5 kW for 14 minutes then 20 kW for 1 minute averages to ~1.8 kW.
- **What vanishes:** The 20 kW spike that blew the transformer never appears in the data.
- **Who breaks:** Capacity planning, fault detection, and equipment protection read the average.
- **Root cause:** Temporal averaging hides the true instantaneous load behind one number.

### Visualization (canvas `canvas1`, 720×200; HTML attributes declare 720×300 but the setup helper renders 720×200)

Step chart: actual load spike vs reported 15-minute average.

- **Background:** very light blue `#f9fbfd` fill.
- **Axes:** `#333` L-shaped axes from (60, 15) to (60, 165) to (690, 165). Y labels (12px `#555`, right-aligned): "20 kW", "10 kW", "1.8 kW", "0.5 kW", "0". X labels: "0 min", "5 min", "10 min", "14 min", "15 min".
- **Actual line (red `#e74c3c`, width 2.5):** flat at 0.5 kW (y=155) from x=80 to x=560, vertical rise to 20 kW (y=25), flat to x=620, vertical drop back to baseline. Spike area filled `rgba(231,76,60,0.15)`.
- **Reported average line:** dashed blue `#2980b9` (dash 8/4, width 3) horizontal at 1.8 kW (y=140) across the interval.
- **Labels (17px):** "Actual: 20 kW spike" in red near the top; "15-min avg: 1.8 kW" in blue near the average line.
- **Annotation (13px, `#c0392b`, centered near the spike):** '"Invisible in interval data"'.
- **Legend (bottom, 12px):** red swatch "Actual"; blue swatch "Reported Avg".

## Net Metering Obscures Consumption

**The Meter Reads −2 kW While the Wiring Carries 3 kW**

- **The mechanism:** Solar homes report only net value — consumption minus generation.
- **The noon example:** 3 kW consumed, 5 kW generated, so the meter reports −2 kW.
- **What the utility sees:** −2 kW, a number that looks like the house is a supplier.
- **Physical reality:** Internal wiring and the local transformer still carry the full 3 kW.
- **At scale:** Capacity planning on net meter data systematically underestimates true load.

### Visualization (canvas `canvas2`, 720×200; HTML attributes declare 720×300 but the setup helper renders 720×200)

Three-bar chart around a zero baseline: actual load, solar generation, net meter reading.

- **Background:** `#f9fbfd`.
- **Baseline:** dashed `#333` horizontal line (dash 4/3) at y=150 from x=100 to x=650, labeled "0 kW" (12px `#555`) at left.
- **Bars (width 100, gap 80, scale 20 px/kW):**
  - "Actual Load" at x=130: 3 kW above baseline, fill `#e74c3c`, stroke `#c0392b`; labels "Actual Load" above and "3 kW" inside (17px `#c0392b`).
  - "Solar Gen." at x=310: 5 kW above baseline, fill `#f39c12`, stroke `#d68910`; labels "Solar Gen." and "5 kW" (`#d68910`).
  - "Net Meter" at x=490: 2 kW below baseline, fill `#2980b9`, stroke `#1a5276`; labels "Net Meter" below and "-2 kW" inside (`#1a5276`).
- **Annotation (top center, 14px, `#e74c3c`):** "Utility sees -2 kW but wiring carries 3 kW"; a dashed red quadratic arc (dash 3/3, width 1.5) connects the actual-load bar top to the net-meter bar.

## EV Charging as Unpredictable New Load

**Five EVs Push a 100 kW Transformer to 200 kW**

- **Per household:** One EV adds +40 kW of demand whenever it is charging.
- **The design limit:** A residential transformer is rated for a ~100 kW neighborhood peak.
- **The collision:** Five EVs charging at once reach 200 kW — double rated capacity.
- **Why history fails:** EV adoption is a structural break, not a trend in the training window.
- **No warning:** Adoption arrives neighborhood by neighborhood with no prior signal.

### Visualization (canvas `canvas3`, 720×200; HTML attributes declare 720×300 but the setup helper renders 720×200)

Step-up load chart crossing a transformer limit line.

- **Background:** `#f9fbfd`.
- **Axes:** `#333` axes from (60, 10) to (60, 165) to (690, 165). Y scale 0–220 kW over 150px; labels (12px `#555`): "200 kW", "150 kW", "100 kW", "50 kW", "0".
- **Transformer limit:** dashed red `#e74c3c` horizontal line (dash 6/4, width 2) at 100 kW, labeled "Transformer Limit: 100 kW" (13px red, right side above the line).
- **Load steps (blue `#2980b9` line width 2.5 with `rgba(41,128,185,0.2)` area fill down to the axis):** 85 kW (x 80–250), 90 kW (x 250–350), 140 kW (x 350–420), 180 kW (x 420–520), 200 kW (x 520–640).
- **Step annotations (12px `#1a5276`):** "Historical" below the first segment, "+1 EV", "+3 EVs", "+5 EVs" above their respective steps.
- **Title (bottom center, 17px, `#1a5276`):** "Neighborhood Load Over Time".

## Meter Tampering / Theft

**Theft Looks Exactly Like an Efficiency Win: 3–5% of Global Supply**

- **The act:** Commercial customers bypass meters to draw electricity off the books.
- **What the data shows:** A clean, sustained drop labeled "reduced consumption."
- **Model misread:** The drop is scored as "efficiency improved" when it is theft.
- **The scale:** Globally 3–5% of all electricity generated is stolen this way.
- **Why it hides:** The meter itself is compromised, so legitimate data holds no signal.
- **What to change:** Compare transformer load against the sum of downstream meters.

### Visualization (canvas `canvas4`, 720×200; HTML attributes declare 720×300 but the setup helper renders 720×200)

Line chart: reported consumption drops after tampering while true consumption continues.

- **Background:** `#f9fbfd`.
- **Axes:** `#333` axes from (60, 20) to (60, 160) to (690, 160). Y labels (12px `#555`): "High" (y≈50), "Low" (y≈105). Note: the y axis is inverted-meaning here — higher on canvas = higher consumption, so the reported line drops visually downward on the value scale by moving to larger y.
- **Reported line (blue `#2980b9`, width 2.5):** point path `[(80,50),(130,55),(180,48),(230,52),(280,50),(320,53),(340,53),(360,90),(400,95),(450,100),(500,98),(550,102),(600,97),(650,100)]` — steady, then a sustained drop starting at x≈340.
- **Actual line:** dashed red `#e74c3c` (dash 5/4, width 1.5) continuing flat from (340, 53) to (650, 50); the wedge between actual and reported is shaded `rgba(231,76,60,0.1)` (stolen electricity).
- **Tampering marker:** vertical dashed gray `#7f8c8d` line (dash 3/2) at x=340 labeled "Tampering begins" (12px, centered).
- **Annotations (14px):** blue 'Model: "Efficiency improved"' near the reported line's lower level; red "Reality: Theft" near the actual line.
- **Bottom band:** `rgba(155,89,182,0.2)` strip (y 140–158) with purple `#8e44ad` 11px centered text "3-5% of global electricity is stolen (invisible in data)".
- **Legend (top left, 12px):** blue swatch "Reported"; dashed red segment "Actual".

## Demand Response Event Distortion

**Net Savings Over a 4-Hour Window ≈ Zero**

- **The signal:** The utility asks participants to reduce consumption for an event window.
- **What happens first:** Load genuinely drops during the event — the visible savings.
- **The rebound:** Over the next 2 hours deferred loads catch up and consumption overshoots.
- **The catch-up loads:** HVAC re-cooling and delayed laundry return the energy that was saved.
- **The bias:** Event-day dip-and-rebound is an artificial shape absent from normal days.
- **Modeling cost:** Any model trained on event days inherits a pattern that never generalizes.

### Visualization (canvas `canvas5`, 720×200; HTML attributes declare 720×300 but the setup helper renders 720×200)

Smoothed demand curve around a baseline with shaded savings and rebound areas.

- **Background:** `#f9fbfd`.
- **Axes:** `#333` axes from (60, 15) to (60, 160) to (690, 160).
- **Baseline:** dashed gray `#7f8c8d` horizontal line (dash 6/4, width 1.5) at y=80, labeled "Baseline" (13px gray, left).
- **Demand curve (blue `#2980b9`, width 2.5, quadratic-smoothed through points):** `[(80,80),(120,78),(160,82),(200,80),(220,80),(250,110),(280,130),(310,125),(340,120),(360,120),(380,65),(410,40),(440,45),(470,50),(500,55),(530,65),(560,75),(590,78),(620,80),(650,80)]` — dips below baseline during the event (larger y = lower demand on canvas), then overshoots above baseline afterward.
- **Shaded areas:** event-window dip filled `rgba(39,174,96,0.25)` (x 220–360) labeled '"Savings"' in green `#27ae60` 13px; post-event overshoot filled `rgba(231,76,60,0.25)` (x 360–560) labeled '"Rebound"' in red `#e74c3c`.
- **Event bracket:** `#1a5276` bracket below the axis from x=220 to x=360 labeled "DR Event" (11px).
- **Takeaway (top right, 17px, `#1a5276`, right-aligned):** "Net savings ≈ 0".

## Weather Forecast Error Cascade

**±3 °F of Weather Error Becomes ±15% of Capacity Error**

- **The dependency:** Demand forecasts are built on top of temperature forecasts.
- **First amplification:** A ±3 °F temperature error becomes a ±8% demand forecast error.
- **Concrete case:** 100 °F versus 97 °F shifts peak demand by 15%.
- **The chain:** Temperature → demand → reserve margin → dispatch cost, amplifying each step.
- **Net effect:** The final planning decision is far less reliable than the weather input was.

### Visualization (canvas `canvas6`, 720×200; HTML attributes declare 720×300 but the setup helper renders 720×200)

Three widening error bars connected by "amplifies" arrows.

- **Background:** `#f9fbfd`.
- **Title (top center, 14px, `#1a5276`):** "Error Amplification Cascade".
- **Stages (centered vertically at y=100, bar width 80; each drawn as a translucent rectangle in the stage color at 20% alpha (`color + '33'`) with a solid 2px border, a center dot radius 4, a vertical whisker with end caps, the error value in 17px above, and a two-line 13px `#333` stage label below):**
  - x=130: "Temperature / Forecast", error "±3°F", half-height 30, color `#27ae60`.
  - x=350: "Demand / Forecast", error "±8%", half-height 55, color `#f39c12`.
  - x=570: "Capacity / Planning", error "±15%", half-height 80, color `#e74c3c`.
- **Arrows:** `#1a5276` horizontal arrows (width 2, filled triangular heads) between consecutive stages at center height, each labeled "amplifies" (11px) above the midpoint.

## Regeneration instructions

- **Template/layout:** domains detail page. h1 + `.subtitle`, then per pitfall an `<h2>` (blue `#1a5276`, 1.4em, bottom border `2px solid #2980b9`) followed by a one-row `.obj-table`: left `<td>` (40%) with `.obj-title` (1.05em, weight 600, `#1a5276`) holding the one-line punchline and a `<ul>` of labeled bullets below it (`ul { margin: 8px 0 8px 20px; font-size: 0.9em; color: #333; }`, `li { margin: 4px 0; }`); right `<td>` (60%, centered) with the canvas. Each `<li>` is `<strong>Label:</strong> short phrase.` on one line. HTML entities used: `&minus;`, `&asymp;`, `&plusmn;`, `&deg;`, `&rarr;`. Even table rows have background `#fafcfe`; cell borders `1px solid #e0e0e0`, padding 20px 24px.
- **Page CSS:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; `.subtitle` `#666` 1.05em; `strong` in `#1a5276`; `.philosophy` callout style defined (background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em) but unused on this page. No nav bar, no back/home links.
- **Canvases:** six canvases (`canvas1`–`canvas6`) declare `width="720" height="300"` attributes in the HTML, but the shared `setupCanvas(id)` helper overrides each to 720×200 CSS pixels and scales the backing store by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). All charts paint a `#f9fbfd` background; default label font 17px `-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif`.
- **Palette:** `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange; supporting colors `#2980b9`, `#f39c12`, `#d68910`, `#8e44ad`, `#7f8c8d`, `#c0392b`, grays `#555`/`#333`.
- In regenerated HTML, any card/page links use `.html` extensions (this page has none).
