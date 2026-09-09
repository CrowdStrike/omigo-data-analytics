# Airline Scheduling / Cascading Delays

**Page type:** detail page (two-column obj-table layout: text left ~40%, canvas right ~60%, one h2 + one-row table per pitfall)
**HTML title tag:** 105. Airline Scheduling / Cascading Delays

**Subtitle:** Aircraft, crew, and passengers are shared resources — one delay propagates through the entire network, and the data records symptoms far from their causes.

## One Delayed Flight Cascades to 50+

**Aircraft and Crew Are Reused All Day — Delay Propagates Through Every Leg**

- **The chain:** Flight 1 (NYC→CHI) delayed 90 min → the same plane flies Flight 2 (CHI→LAX) 90 min late.
- **Third leg:** Flight 3 (LAX→SEA) then flies 90 min late as well — the delay is never absorbed.
- **Crew branches:** The crew on that first flight is also delayed, and they are due on another aircraft.
- **Parallel path:** So their NEXT flight runs late too, even though that aircraft itself was on time.
- **Exponential propagation:** One initial event fans out through aircraft and crew rotations.
- **Across the day:** The fan-out becomes several simultaneous cascades spanning the whole schedule.

### Visualization (canvas `canvas1`, 720×200)

Horizontal delay-bar cascade for a single aircraft's route.

- **Title (17px `#1a5276` at 10,20):** "Cascade: Single Aircraft Route".
- **Bars:** five horizontal bars starting at x=40, one per leg, y = 40 + i*30, height 22, width = delay*1.2. Legs and delays: NYC→CHI (+90 min), CHI→LAX (+90 min), LAX→SEA (+85 min), SEA→PDX (+80 min), PDX→SFO (+75 min). Fill `rgba(231,76,60, 0.5 + i*0.1)` (deepening red down the chain).
- **Bar labels (14px `#333`, right of each bar):** e.g. "NYC→CHI (+90 min)".
- **Connectors:** short dashed red `#e74c3c` vertical lines (dash 3/3) linking each bar to the next.
- **Annotation (17px red `#e74c3c` at 350,170):** "Exponential: crew branches create parallel cascades".

## Weather at a Hub Affects the Entire Network

**Hub Topology Amplifies Local Weather into Network-Wide Chaos**

- **The event:** A thunderstorm at Atlanta, a Delta hub, delays or cancels 200 flights outright.
- **The amplification:** Passengers connect THROUGH Atlanta from 100 cities to 100 other cities.
- **Itinerary math:** That is 10,000 origin-destination itineraries disrupted by one local storm.
- **Modeling burden:** "Will my flight be delayed?" depends on EVERY other flight at the hub.
- **Full network scope:** And on every hub the route passes through, not just the departure airport.

### Visualization (canvas `canvas2`, 720×200)

Hub-and-spoke network diagram.

- **Title (17px `#1a5276` at 10,20):** "Hub Topology Amplification".
- **Hub:** filled red `#e74c3c` circle radius 30 at (360,110) with white 13px label "ATL"; red 17px "Storm" label above at (340,70).
- **Spokes:** 12 cities placed on an ellipse around the hub (radius 80 horizontal, 70 vertical), each an orange `#f39c12` filled circle radius 8 connected to the hub by a blue `#2980b9` width-2 line.
- **Bottom note (14px `#333` at 100,195):** "100 origins x 100 destinations = 10,000 disrupted itineraries".

## Crew Timeout Regulations Force Cancellations

**A Delay Doesn't Just Shift the Schedule — It Removes Capacity**

- **The rule:** A pilot legally can't fly after 8-10 hours of duty, whatever the schedule says.
- **The cascade:** Flight delayed 3 hours → crew "times out" on arrival at the destination.
- **Capacity gone:** No legal crew remains for the return flight, so that flight is cancelled outright.
- **Second-order cost:** A replacement crew must be flown in as deadhead passengers.
- **Inventory hit:** Those deadhead seats sit on another flight, reducing sellable inventory there.

### Visualization (canvas `canvas3`, 720×200)

Duty-time bars against a legal-limit threshold.

- **Title (17px `#1a5276` at 10,20):** "Crew Duty Time vs. Legal Limit".
- **Axis:** horizontal blue `#2980b9` width-2 line at y=80 from x=50 to 680.
- **Green bar:** `#27ae60` rect (50, 60) 400×15 with white 12px inset label "Scheduled duty: 7h (within limit)".
- **Red bar:** `#e74c3c` rect (50, 90) 550×15 with white inset label "Actual duty with 3h delay: 10h (EXCEEDS limit)".
- **Limit marker:** vertical dashed red line (dash 5/5) at x=500 from y=50 to y=120, labeled in red 14px "8h LEGAL LIMIT" at (505,60).
- **Bottom text:** 17px `#333` "Result: Return flight CANCELLED (capacity removed)" at (50,160); 17px red "+ Deadhead crew occupies seat on another flight" at (50,185).

## Passenger Rebooking Creates Secondary Overload

**"Oversold by 40%" Is a Symptom of a Cancellation 6 Hours Ago Elsewhere**

- **The mechanism:** 200 passengers off a cancelled flight are rebooked onto the next 3 flights.
- **Secondary overload:** Those three flights are now oversold, so passengers get bumped again.
- **Feedback loop:** Each bump generates another rebooking, pushing load further down the schedule.
- **Duration:** The cascade keeps running 24-48 hours after the initial cancellation event.
- **Misleading data:** "Flight oversold by 40%" reads as terrible capacity planning at that flight.
- **True cause:** It is the consequence of a cancellation on a different route hours earlier.

### Visualization (canvas `canvas4`, 720×200)

Bar chart of displaced passengers decaying over time.

- **Title (17px `#1a5276` at 10,20):** "Rebooking Cascade Over Time".
- **Bars:** width 70, spaced 85px starting x=80, baseline y=170, height = value*0.7. X labels (12px `#333`): 0h, 6h, 12h, 18h, 24h, 36h, 48h. Values: 200, 150, 120, 80, 50, 20, 5 (shown above each bar). Fill red `#e74c3c` when value > 100, else orange `#f39c12`.
- **Capacity line:** dashed green `#27ae60` (dash 4/4, width 2) horizontal at the 100-passenger level (y = 170 − 70), labeled "Normal capacity" in green 13px at (600, just above the line).
- **Axis caption (13px `#333` at 200,45):** "Displaced passengers needing rebooking".

## Maintenance Slot Missed — Aircraft Out of Service

**Arriving 3 Hours Late Removes the Aircraft for the Entire Next Day**

- **The setup:** The aircraft has scheduled maintenance at midnight in DEN, arriving 11pm.
- **The delay:** A delay upstream means it actually arrives at 2am, three hours past the plan.
- **The miss:** The maintenance window is gone because the maintenance crew has gone home.
- **Out of service:** The aircraft is grounded until the next available slot, 18 hours later.
- **The fallout:** 4-6 flights are cancelled that have NOTHING to do with the original delay.

### Visualization (canvas `canvas5`, 720×200)

Timeline of maintenance-window events.

- **Title (17px `#1a5276` at 10,20):** "Maintenance Window Miss".
- **Timeline:** blue `#2980b9` width-2 horizontal line at y=100 from x=50 to 680.
- **Event dots (radius 8) with two-line 12px `#333` labels below:** x=150 green `#27ae60` "11pm / Scheduled"; x=250 green "12am / Maint Start"; x=400 red `#e74c3c` "2am / Actual Arrival"; x=550 orange `#f39c12` "6pm next day / Next Slot".
- **Out-of-service band:** light red `rgba(231,76,60,0.15)` rect (400,60) 150×30 with red 13px label "OUT OF SERVICE (18h)" at (405,80).
- **Bottom text (17px `#333` at 150,180):** "4-6 flights cancelled (unrelated to original delay)".

## Fuel Tankering Decisions from Stale Weather

**The Data Was Correct When Computed, Stale When Executed**

- **The decision:** At departure, carry extra fuel (heavy, expensive) or refuel at the destination.
- **The tradeoff:** Which option wins depends on destination fuel price and fuel availability.
- **The input:** That whole calculation rests on a weather forecast from 3 hours earlier.
- **The failure mode:** Weather changed → diversion → insufficient fuel for the diversion → emergency.
- **The data trap:** The input was correct when computed and stale by the time it was executed.

### Visualization (canvas `canvas6`, 720×200)

Rising risk curve from data staleness.

- **Title (17px `#1a5276` at 10,20):** "Decision Staleness Over Time".
- **Curve:** red `#e74c3c` width-3 power curve starting at (80,150), rising as staleness = (x/100)^1.5 * 10 (capped at 110px above baseline) over 500px width.
- **Markers:** green `#27ae60` 8px dot at curve start (80,150) labeled in green 13px "Decision made (data fresh)" at (95,160); red 8px dot at (380,80) labeled in red "Execution (data 3h stale)" at (395,75).
- **Axis labels (14px `#333`):** "Time since forecast →" at (250,190); rotated vertical "Risk ↑" near (30,120).

## Ground Truth Takes 24-48 Hours to Settle

**"Was This Flight Delayed?" Depends on Which Timestamp You Pick**

- **Four clocks:** Gate departure, wheels-off, wheels-on, and gate arrival all differ for one flight.
- **The contradiction:** A flight leaves the gate on time, then sits on the taxiway for 90 minutes.
- **Two verdicts:** It is "on time" by the departure metric and "90 min late" by the arrival metric.
- **Metric gaming:** DOT metrics use specific definitions that airlines can and do optimize toward.
- **Wrong target:** The optimization chases the METRIC rather than the passenger experience.
- **Classification stakes:** Calling an event "cancellation" vs "extreme delay" carries financial consequences.

### Visualization (canvas `canvas7`, 720×200)

Horizontal bars comparing four delay metrics for the same flight.

- **Title (17px `#1a5276` at 10,20):** "Multiple Metrics for Same Flight".
- **Rows (label at x=10, bar from x=160, y = 55 + i*35, height 20, width = value*4, min 2px; value label "+N min" after the bar):** Gate Departure +0 min `#27ae60`; Wheels Off +5 min `#f39c12`; Wheels On +85 min `#e67e22`; Gate Arrival +92 min `#e74c3c`.
- **Threshold marker:** vertical dashed red line (dash 4/4) at the 15-minute position (x = 160 + 15*4) from y=40 to y=170, labeled in red 12px "15 min threshold".
- **Bottom text (17px `#1a5276` at 100,190):** ""On time" by departure, "90 min late" by arrival".

## Schedule Recovery Optimization Is NP-Hard

**Real-Time Recovery Is Heuristic — and Heuristics Strand Passengers**

- **The problem size:** After a disruption, reassign 500 aircraft, 3000 crew members, 50,000 passengers.
- **The objective:** All of it simultaneously, to minimize total cost across the whole network.
- **The constraints:** Crew legalities, aircraft maintenance, passenger connections, gate availability.
- **More constraints:** Plus fuel and airport curfews — combinatorial, unsolvable optimally in real time.
- **The consequence:** Every airline runs heuristics, so recovery is suboptimal by construction.
- **Passenger cost:** Heuristic recovery strands some passengers for days, not hours.
- **Real case:** Southwest meltdown Dec 2022 — 16,700 flights cancelled over 10 days, recovery algorithm failed.

### Visualization (canvas `canvas8`, 720×200)

Recursive branching tree illustrating combinatorial explosion.

- **Title (17px `#1a5276` at 10,20):** "Combinatorial Explosion of Recovery Options".
- **Tree:** recursive binary tree rooted at (100,50) growing downward (initial angle π/2, spread 0.5 rad shrinking 0.8× per level, depth 6, branch length 25 − depth*2); branches at depth > 3 stroked blue `#2980b9`, deeper branches red `#e74c3c`, line width = depth*0.5.
- **Right-side text (13px `#333` at x=400):** "500 aircraft" (y=50), "3,000 crew" (y=70), "50,000 passengers" (y=90), "= billions of combinations" (y=110).
- **Bottom text (17px red `#e74c3c`):** "Southwest 2022: 16,700 flights cancelled" at (300,160); "(recovery algorithm failed)" at (350,185).

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table` layout — one `h2` per pitfall followed by a single-row table: left `<td>` (40%) with `.obj-title` + bullet list, right `<td>` (60%, centered) with one `<canvas width="720" height="300">` (the setup script draws at 720×200 logical size and fixes CSS size to 720×200). Even rows background `#fafcfe`; cell borders `1px solid #e0e0e0`, padding 20px 24px.
- **Page CSS:** body system sans-serif, white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.4em `#1a5276` with 2px `#2980b9` bottom border; subtitle `#666` 1.05em; `.obj-title` 1.05em weight 600 `#1a5276`; `strong` `#1a5276`; ul 0.9em `#333`. `.philosophy` callout style defined but unused. No nav bar, no back/home links.
- **Canvas:** shared `setupCanvas(id)` helper scales the backing store by `window.devicePixelRatio`, sets CSS size 720×200, `ctx.scale` back to logical coordinates, default font 17px system sans.
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#f39c12`/`#e67e22`, gray `#333`/`#666`.
- Note: in regenerated HTML, any card/page links use `.html` extensions (this page has none).
