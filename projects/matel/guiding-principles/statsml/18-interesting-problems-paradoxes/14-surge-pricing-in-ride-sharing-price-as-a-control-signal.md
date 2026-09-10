# Surge Pricing in Ride Sharing — Price as a Control Signal

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one h2 + table per aspect, plus philosophy callouts and a summary table)
**HTML title tag:** Surge Pricing in Ride Sharing — Price as a Control Signal

**Subtitle:** A concert ends and the app shows 2.3x. Treat the multiplier as the actuator of a control system, and both why it exists and how it behaves become clear.

## Callout (philosophy box, top)

**The question:** When prices triple outside a stadium, what would happen if they simply... didn't? Hold the price fixed and run the night forward. Who gets a ride?

**The answer:** With price frozen, rides are allocated by tap speed and luck — the shortage doesn't shrink, it just gets rationed randomly. Price is the only knob that acts on *both sides* of the market at once: it cools demand and pulls in supply simultaneously. That makes surge a feedback controller — and it inherits every classic failure mode of one.

## 1. The Problem: 500 Requests, 60 Drivers, Right Now

**Obj-title:** Perishable Inventory

A ride-hailing marketplace can't stockpile its product. An idle driver-minute in the suburbs cannot be stored, shipped, or used to serve someone downtown. Supply exists only at a place and time — and demand arrives in spikes exactly when supply hasn't.

Math-box:

**Setup (illustrative):** Concert lets out at 11pm.
Requests in the zone, next 10 min: `500`
Idle drivers in the zone: `60`

At the frozen price, `440 people` get no ride — no matter how the queue is ordered. First-come-first-served doesn't create a single extra driver; it only decides *which* 440 lose.

- **The queue illusion:** "just make people wait their turn" feels fair but leaves the shortage exactly the same size
- **Cancellation cascade:** long ETAs → riders cancel and re-request → the demand signal inflates further
- **The real question:** not "what is the fair price?" but "what signal moves 100+ drivers toward this block in the next 15 minutes?"

### Visualization (canvas `canvas1`, 720×360)

Line chart: demand spike vs flat supply over a one-hour window, with the shortage region shaded.

- **Layout:** origin at (70, 300), plot width 600, plot height 240. Axes in `#1a5276`, width 2.
- **Data (functions):** demand(t) = 40 + 460·exp(−(t−32)²/120) over t = 0..60 minutes; supply(t) = 60 (constant). Value scale 0–520 mapped to plot height.
- **Shortage shading:** region where demand > supply filled `rgba(231,76,60,0.15)`.
- **Gridlines / y labels:** at 0, 100, 200, 300, 400, 500 in gray `#666`, gridlines `#eee`.
- **Axis labels:** x: "Minutes (10:30pm → 11:30pm)"; y (rotated): "Requests / drivers in zone" — both `#1a5276`, 13px.
- **Demand curve:** red `#e74c3c`, width 2.5. **Supply line:** green `#27ae60`, width 2.5, horizontal at 60.
- **Concert end marker:** dashed gray `#999` vertical line at t=30 (dash 4/4), labeled "concert ends" above the plot in `#666` 11px.
- **Labels (bold 12px):** red "requests" near the demand curve at t=38; green "idle drivers (fixed price: nothing moves them here)" above the supply line at t=2; red two-line annotation at t=40: "the shortage — no queue" / "ordering shrinks it" (at value heights 300 and 260).
- **Title (bold 14px `#1a5276`, top center):** "At a Frozen Price the Shortage Just Gets Rationed by Luck".

## 2. One Knob That Moves Both Curves

**Obj-title:** Where the Market Clears

Raising the multiplier does two things at once. Some riders wait, walk, or take transit — demand falls. Nearby drivers see higher earnings and reposition in — supply rises. The controller's job is to find the multiplier where the two adjusted curves cross.

Math-box:

**Same night, three prices (illustrative):**

At `1.0x`: 500 riders book, 60 drivers available — shortage of 440
At `1.8x`: 260 still book, 150 drivers en route — shortage of 110
At `2.3x`: 190 book, 185 drivers — **market clears**

The 310 riders who dropped out weren't refused — they self-selected by urgency. The person catching a flight pays 2.3x; the person who can wait 20 minutes does.

- **Pricing the place, not the person:** everyone standing in the same zone at the same moment sees the same multiplier — surge is a property of a cell and a time window, computed from aggregate flows
- **Rationing by urgency vs by luck:** the frozen price allocates the same scarce rides — just by tap speed instead of willingness to pay

### Visualization (canvas `canvas2`, 720×360)

Supply/demand curves vs price multiplier, with the market-clearing crossing marked.

- **Layout:** origin at (80, 300), plot width 580, plot height 240. Axes `#1a5276`, width 2.
- **Data (functions):** demand(m) = 500·exp(−0.75·(m−1.0)); supply(m) = 60 + 200·(1 − exp(−1.2·(m−1.0))); m from 1.0 to 3.0. Value scale 0–520.
- **X ticks:** 1.0x, 1.5x, 2.0x, 2.5x, 3.0x in gray `#666` with light `#eee` vertical gridlines.
- **Axis labels:** x: "Price multiplier"; y (rotated): "Rides per window" — `#1a5276`, 13px.
- **Demand curve:** red `#e74c3c`, width 2.5. **Supply curve:** green `#27ae60`, width 2.5.
- **Clearing point:** m* = 2.15 — dashed blue `#1a5276` vertical drop line (dash 5/5, width 1.5) from axis to the curve, filled blue dot radius 6 at (m*, demand(2.15)); bold blue label "market clears" to its upper right.
- **Curve labels (bold 12px):** red "riders still willing to book" near m=1.05; green "drivers responding" near m=2.4.
- **Region notes (12px `#999`):** "left of the point: shortage" near m=1.1 low on the chart; "right: glut" near m=2.55.
- **Title (bold 14px `#1a5276`, top center):** "One Price Moves Both Curves — the Controller Hunts the Crossing".

## 3. The Thermostat Has a Lag — and That's Where It Breaks

**Obj-title:** Feedback Failure Modes

Riders react to a price change in seconds. Drivers take ~15 minutes to physically arrive. A controller with mismatched reaction lags on its two sides can oscillate — like a furnace that only feels the cold from ten minutes ago.

- **Overshoot:** too many drivers converge on the surging zone, supply floods, the multiplier collapses — the drivers who repositioned earn nothing extra and learn to distrust the signal. The actuator degrades its own sensor.
- **Cobweb cycles:** shortage → high price → glut → low price → shortage. Demand and supply react at different speeds, so an aggressive controller ping-pongs between the two.

Math-box:

**The statistician's trap:** to set the multiplier you need elasticity — how much demand falls per unit of price. But price rises *exactly when demand spikes*, so naive regression of demand on price is confounded by the very signal that triggered the price. Observed correlation can even come out positive: high price, high demand.

Identification needs designed variation: `switchback experiments` (flip pricing on/off in time blocks) and natural boundaries (adjacent cells, quantized multiplier steps).

### Visualization (canvas `canvas3`, 720×360)

Line chart: lagged demand and supply peaks over time, with a dashed multiplier line tracking the gap.

- **Layout:** origin at (70, 300), plot width 600, plot height 240. Axes `#1a5276`, width 2.
- **Data (functions):** demand(t) = 60 + 380·exp(−(t−12)²/160); supply(t) = 70 + 300·exp(−(t−28)²/300); t = 0..60. Value scale 0–480.
- **X ticks:** 0, 15, 30, 45, 60 in gray `#666`. X axis label: "Minutes after the spike begins" (`#1a5276`, 13px).
- **Demand curve:** red `#e74c3c`, width 2.5 (peak at t=12). **Supply curve:** green `#27ae60`, width 2.5 (peak at t=28).
- **Multiplier line:** orange `#e67e22`, width 2, dashed (7/5): g(t) = 40 + max(0, demand(t) − supply(t))·0.85.
- **Lag annotation:** dotted blue `#1a5276` vertical lines (dash 3/3) at t=12 and t=28 from top of plot to axis; bold blue 12px label centered between them near the top: "~16 min supply lag".
- **Curve labels (bold 12px):** red "demand (reacts in seconds)" near t=16; green "supply (drivers arriving)" near t=32; orange "multiplier tracks the gap" near t=1 at value 200.
- **Note (12px `#999`, two lines, left-aligned near t=30):** "drivers arrive into a falling multiplier — repositioning" / "paid less than the signal promised".
- **Title (bold 14px `#1a5276`, top center):** "Two Reaction Speeds, One Controller — the Recipe for Oscillation".

## 4. The Complete Picture

Summary table (`.summary-table`, header row + 6 rows):

| Control concept | Home thermostat | Surge pricing |
|---|---|---|
| **Sensor** | Thermometer | App opens, requests, driver GPS pings per zone |
| **Actuator** | Furnace switch | Price multiplier |
| **Setpoint** | 21°C | Requested rides ≈ serviceable rides |
| **Reaction lag** | Minutes for heat to spread | Seconds for riders, ~15 minutes for drivers |
| **Failure mode** | Temperature oscillation | Overshoot, cobweb cycles, drivers distrusting the signal |
| **Hard estimation problem** | — | Elasticity confounded by the demand spike that triggered the price |

## Callout (philosophy box, bottom)

**One sentence:** Surge is not a price — it's the actuator in a feedback loop, and most production incidents in dynamic pricing are control failures (lag, overshoot, sensor distrust), not model-accuracy failures.

## Regeneration instructions

- **Layout:** detail page. h1, `.subtitle`, opening `.philosophy` callout, then per aspect: `<h2>N. Title</h2>` (h2 1.4em `#1a5276`, bottom border `2px solid #2980b9`, padding-bottom 8px) followed by a one-row `.obj-table` — left `<td>` (45%) holds `.obj-title`, paragraph, `.math-box`, bullets; right `<td>` (55%, centered) holds the canvas. Section 4 is a `.summary-table`; page closes with a `.philosophy` callout.
- **Page style:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; subtitle `#666` 1.05em; table cell borders `1px solid #e0e0e0`, padding 20px 24px, vertical-align middle; `.obj-title` 1.05em weight 600 `#1a5276`; `strong` `#1a5276`; ul 0.9em `#333`. No nav bar, no back/home links.
- **Callout style:** `.philosophy` — background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em.
- **Math box:** `.math-box` — background `#f8fafb`, border `1px solid #e0e0e0`, radius 6px, padding 16px 20px, 0.9em; `code` background `#eef2f7`, padding 2px 6px, radius 3px.
- **Summary table:** `.summary-table` — 0.9em, th background `#f0f4f8` `#1a5276` padding 10px 14px left-aligned, td padding 10px 14px, borders `1px solid #e0e0e0`.
- **Canvas:** intrinsic 720×360 each; a shared `setupCanvas(id, w, h)` sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray text `#666`/`#999`, accent `#2980b9`.
