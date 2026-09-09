# maps platform / Navigation / Driving Directions

**Page type:** detail page (two-column obj-table layout: text left ~40%, canvas right ~60%, one h2 + one-row table per pitfall)
**HTML title tag:** 107. maps platform / Navigation / Driving Directions

**Subtitle:** Real-time routing where the prediction changes reality, traffic data is always stale, and the map is never the territory.

## ETA Prediction from Stale Traffic

**ETA Prediction from Stale Traffic**

- Traffic data is 2-5 minutes old by the time you see it
- On a 20-min drive: conditions at destination are predicted from 5-min-old data
- Traffic can change completely in 5 minutes (accident clears, light cycle changes, school lets out)
- Your ETA is a prediction based on stale observations of a rapidly-changing system

Example: "Arrive at 5:23" = best guess from 5-min-old data about a 20-min-future state.

### Visualization (canvas `c1`, 720×300; drawing occupies top ~200px)

Timeline with staleness bars and a gradient staleness zone.

- **Title (17px `#1a5276` at 20,25):** "Data Staleness vs Drive Duration".
- **Timeline axis:** `#333` width-1.5 line at y=160 from x=60 to 680, with 5 ticks and labels (13px): "0 min", "5 min", "10 min", "15 min", "20 min" evenly spaced.
- **Gradient zone:** rect (60,50) to (680,150) filled with a left-to-right linear gradient from `rgba(231,76,60,0.15)` to `rgba(231,76,60,0.6)`; label 14px `#c0392b` at (180,75): "Data freshness degrades over trip duration".
- **Staleness bars:** red `#e74c3c` 16px-wide bars centered at each tick, heights = staleness*5 for staleness values [2, 5, 8, 12, 17] minutes; each labeled 11px `#1a5276` above the bar as "2min stale" … "17min stale".
- **Uncertainty arrow:** dashed blue `#2980b9` (dash 4/3, width 2) horizontal line at y=45 from x=60 to 680, labeled 12px blue "Prediction uncertainty increases -->>" at (250,42).

## Route Recommendations CREATE Congestion (Braess's Paradox)

**Route Recommendations CREATE Congestion (Braess's Paradox)**

- Maps says "take shortcut through Oak Street" — 10,000 users get same recommendation simultaneously
- Oak Street (normally empty residential) now has 10K cars — worse than the highway
- The recommendation CAUSED the problem it tried to solve
- navigation app effect: algorithm routes everyone to same "fast" route — that route becomes the slowest

Example: The BEST route changes the moment you recommend it to enough people.

### Visualization (canvas `c2`, 720×300; drawing occupies top ~200px)

Before/after bar chart of travel times.

- **Title (17px `#1a5276` at 20,25):** "Braess's Paradox: Route Recommendation Effect".
- **Bars:** four bars, width 80, gap 50, starting x=100, baseline y=180, height = value*3. Labels (11px `#555` below, two lines) and values (12px `#333` above): "Highway (before)" 30 min `#2980b9`; "Oak St (before)" 8 min `#27ae60`; "Highway (after)" 22 min `#5dade2`; "Oak St (after)" 45 min `#e74c3c`.
- **Divider:** dashed `#aaa` vertical line between the before and after pairs; 12px `#666` labels "BEFORE" (x≈160, y=42) and "AFTER recommendation" (right half, y=42).
- **Arrow:** red `#e74c3c` width-2 horizontal arrow at y=60 pointing to the "Oak St (after)" bar, labeled 13px red "10K users routed here".

## Construction/Closure Data is Weeks Stale

**Construction/Closure Data is Weeks Stale**

- Road closed for construction: reported by first user who encounters it
- If Maps doesn't know until Thursday, 4 days of drivers routed into dead ends
- Construction ENDS aren't reported well — road reopened 2 weeks ago but Maps still routes around it
- Stale closure data causes unnecessary detours for weeks

Example: Road reopened 2 weeks ago but Maps still adds 10 min detour for everyone.

### Visualization (canvas `c3`, 720×300; drawing occupies top ~200px)

Event timeline with lag zones.

- **Title (17px `#1a5276` at 20,25):** "Construction Data Lag Timeline".
- **Timeline:** `#333` width-2 line at y=120 from x=50 to 690.
- **Event dots (radius 7) at fractional positions along the timeline, each with a two-line 11px `#333` label above:** 0.0 "Road Closes (Mon)" `#e74c3c`; 0.25 "First Report (Thu)" `#e67e22`; 0.5 "Maps Updates (Fri)" `#f39c12`; 0.7 "Road Reopens (Week 3)" `#27ae60`; 1.0 "Maps Removes (Week 5)" `#2980b9`.
- **Red zone:** `rgba(231,76,60,0.2)` band below the timeline (y=135, height 30) covering positions 0.0-0.25, labeled 11px `#c0392b` "Drivers hit dead end (4 days)".
- **Orange zone:** `rgba(230,126,34,0.2)` band covering positions 0.7-1.0, labeled 11px `#e67e22` "Unnecessary detours (2 weeks)".
- **Summary (13px `#1a5276` at 180, y=185):** "Total wasted driver-hours: thousands over 5-week cycle".

## Incident Reporting Lag + Rubbernecking Effect

**Incident Reporting Lag + Rubbernecking Effect**

- Accident at 3:15pm — first user reports at 3:18pm — Maps updates at 3:20pm
- Traffic from rubbernecking clears by 3:30pm, but Maps STILL shows congestion at 3:35pm
- Users rerouted AFTER the problem resolved
- Opposite-direction rubbernecking = slowdown with NO incident report (invisible congestion)

Example: Maps shows red zone 15 minutes after congestion cleared — phantom rerouting.

### Visualization (canvas `c4`, 720×300; drawing occupies top ~200px)

Dual timeline: reality vs Maps view.

- **Title (17px `#1a5276` at 20,25):** "Incident Reporting Lag vs Reality".
- **Two horizontal `#333` width-1.5 timelines** from x=80 to 680: REALITY at y=80, MAPS VIEW at y=150 (12px `#1a5276` row labels at left).
- **Time markers (10px `#666` above the reality line):** 3:15, 3:18, 3:20, 3:25, 3:30, 3:35, 3:40 evenly spaced across 6 intervals.
- **Reality congestion band:** `rgba(231,76,60,0.4)` (16px tall, centered on the reality line) from 3:15 to 3:30 (positions 0 to 5/6); 11px green `#27ae60` label "Cleared" after the band; 11px red "Accident occurs" below the band start.
- **Maps congestion band:** same red fill on the Maps line from 3:20 (position 2/6) to 3:40 (end) — delayed and extended.
- **Phantom zone:** purple `rgba(155,89,182,0.3)` overlay on the Maps band from 3:30 to 3:40, labeled 11px `#8e44ad` "PHANTOM: Maps red, road clear" below.

## Local Knowledge vs Algorithm (Shortcuts the Map Can't Know)

**Local Knowledge vs Algorithm (Shortcuts the Map Can't Know)**

- Map says "turn right, then U-turn at light" — local knows: cut through gas station parking lot
- Maps can't model: illegal but universally-practiced U-turns, parking lot cut-throughs
- Toll roads with local discount programs, timed signals that locals know
- The "optimal" route ignores human knowledge absent from road graph data

Example: Algorithm's "best" route adds 4 minutes vs local knowledge shortcut.

### Visualization (canvas `c5`, 720×300; drawing occupies top ~200px)

Schematic map comparing two routes from A to B.

- **Title (17px `#1a5276` at 20,25):** "Algorithm Route vs Local Knowledge".
- **Roads:** wide gray `#bbb` 12px zigzag main road from (80,100) via (200,100)→(200,50)→(400,50)→(400,100)→(550,100)→(550,150) to (650,150); lighter `#ddd` 8px shortcut from (200,100) via (350,130) to (550,150).
- **Route highlights:** algorithm route traced in solid blue `#2980b9` width 3 along the main road; local shortcut traced in dashed green `#27ae60` width 3 (dash 5/3).
- **Labels:** blue 13px "Algorithm: 12 min (right turn + U-turn)" at (250,40); green "Local: 8 min (gas station cut-through)" at (250,170).
- **Markers:** red `#e74c3c` 8px circle at start (80,100) with white "A"; green 8px circle at end (650,150) with white "B".
- **Gas station:** `rgba(39,174,96,0.2)` rect (320,110) 60×40 with green 10px label "Gas Stn".

## Phantom Traffic Jams from GPS Drift

**Phantom Traffic Jams from GPS Drift**

- 50 phones on a highway — GPS bounces off buildings — positions OSCILLATE
- Phones appear to slow down/speed up — traffic algorithm: "congestion detected!"
- Actually: GPS noise making stationary phones appear to be slow-moving cars
- Creates fake red zones on the map — users detour around phantom congestion

Example: GPS drift near tall buildings creates non-existent traffic jams that cause real detours.

### Visualization (canvas `c6`, 720×300; drawing occupies top ~200px)

Highway diagram with actual vs GPS-drifted positions.

- **Title (17px `#1a5276` at 20,25):** "GPS Drift Creating Phantom Congestion".
- **Highway:** dark gray `#555` rect (50,85) 620×30 with a white dashed center line (dash 20/15, width 2) at y=100.
- **Actual positions:** green `#27ae60` 5px dots on the upper lane (y=92) at x = 100, 180, 260, 340, 420, 500, 580, 640.
- **GPS-reported positions:** translucent red `rgba(231,76,60,0.6)` 5px dots on the lower lane (y=108), horizontally offset from actual by drifts [8, −12, 15, −20, 25, −8, 18, −15]; thin `rgba(231,76,60,0.4)` drift lines connect each actual/reported pair.
- **Building:** gray `#7f8c8d` rect (300,45) 80×35 with white 10px label "Building"; orange `#f39c12` dashed multipath bounce lines (dash 2/2) from the building down to the road.
- **Phantom zone:** `rgba(231,76,60,0.15)` rect (250,125) 200×30 with red 1.5px outline and red 12px label "PHANTOM: "Congestion detected!"".
- **Legend (11px `#333`, y≈170):** green dot "Actual position"; translucent red dot "GPS-reported (drifted)"; orange text "Signal multipath".

## navigation app-Effect: Routing Through Residential Streets

**navigation app-Effect: Routing Through Residential Streets**

- Algorithm finds: residential side street saves 2 minutes vs highway
- Routes 1000 cars/hour through quiet neighborhood — speed bumps, children, 25mph zone
- Actual savings: 30 seconds if lucky — residents' quality of life destroyed
- Algorithm optimizes travel time without modeling externalities (safety, noise, community)

Example: Political pressure forces city to block cut-through; Maps STILL routes there until enough reports arrive.

### Visualization (canvas `c7`, 720×300; drawing occupies top ~200px)

Hourly traffic bar chart against design capacity.

- **Title (17px `#1a5276` at 20,25):** "Residential Street: Capacity vs Routed Traffic".
- **Bars:** 13 hourly bars from 6am to 6pm (labels 9px `#555` below, baseline y=175, span x=60-690, scale max 1000 over 130px). Routed values: [5, 200, 800, 400, 50, 30, 60, 40, 80, 900, 700, 300, 30]. Bars over 100 (capacity) red `#e74c3c`, others blue `#2980b9`; values > 200 printed in white 9px inside the bar top.
- **Capacity line:** dashed red (dash 5/3, width 2) horizontal at the 100 cars/hr level, labeled 11px red "Design capacity: 100 cars/hr".
- **Legend (11px `#333`, top right):** red swatch "Over capacity (navigation app-routed)"; blue swatch "Within capacity".

## Multi-Modal Comparison Bias

**Multi-Modal Comparison Bias**

- Maps shows: "Drive: 25 min. Transit: 55 min. Bike: 35 min."
- Drive time excludes parking search (10 min), walk from parking (5 min), cost ($15)
- Transit excludes frequency (next bus in 12 min). Bike assumes flat terrain at average speed
- Each mode biased in DIFFERENT directions — comparing directly always favors car

Example: The COMPARISON methodology shapes urban policy: car always wins on paper, transit gets defunded, confirming the bias.

### Visualization (canvas `c8`, 720×300; drawing occupies top ~200px)

Stacked bars: shown time vs hidden time per travel mode.

- **Title (17px `#1a5276` at 20,25):** "Multi-Modal: Shown vs Actual Total Time".
- **Bars:** three stacked bars (width 60, gap 100, starting x=120, baseline y=170, scale 120px per 70 min). Modes: Drive (shown 25 min solid `#2980b9`, hidden +15 min `rgba(41,128,185,0.4)` with dashed outline, side note "+parking +walk"); Transit (shown 55 `#27ae60`, hidden +12 `rgba(39,174,96,0.4)`, note "+wait for next bus"); Bike (shown 35 `#f39c12`, hidden +8 `rgba(243,156,18,0.4)`, note "+lock up +shower"). Shown value printed white inside solid segment; "+N min" in mode color inside hidden segment.
- **Actual-total ticks:** small red `#e74c3c` width-2 dashes above each bar at the total height.
- **Right-side annotations (red, at x=380):** 12px "ACTUAL total: 40 vs 67 vs 43 min" (y=55); "Maps SHOWS: 25 vs 55 vs 35 min" (y=72); 11px "Car bias: 15 min hidden. Transit penalized by visible wait." (y=92).
- **Legend (11px `#333`, y≈117):** solid blue swatch "Shown in app"; translucent outlined swatch "Hidden time".

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table` layout — one `h2` per pitfall followed by a single-row table: left `<td>` (40%) with `.obj-title` div (repeating the section title), a `<ul>` of bullets, and one example `<p>`, right `<td>` (60%, centered) with one `<canvas width="720" height="300">`. Even rows background `#fafcfe`; cell borders `1px solid #e0e0e0`, padding 20px 24px.
- **Page CSS:** body system sans-serif, white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.4em `#1a5276` with 2px `#2980b9` bottom border; subtitle `#666` 1.05em; `.obj-title` 1.05em weight 600 `#1a5276`; `strong` `#1a5276`; ul 0.9em `#333`; p 0.95em `#333`. `.philosophy` callout style defined but unused. No nav bar, no back/home links.
- **Canvas:** one shared IIFE scales all canvases by `window.devicePixelRatio` using their width/height attributes (720×300) and calls `ctx.scale` so drawing stays in logical coordinates; each chart IIFE then draws within a 720×200 coordinate region, 17px system font for titles.
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c` (dark red `#c0392b`), orange `#f39c12`/`#e67e22`, purple `#8e44ad`, gray `#333`/`#555`/`#666`.
- Note: in regenerated HTML, any card/page links use `.html` extensions (this page has none).
