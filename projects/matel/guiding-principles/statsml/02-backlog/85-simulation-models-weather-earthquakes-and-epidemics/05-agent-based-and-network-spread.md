# Agent-Based & Network Spread

**Page type:** detail page (four titled sections, each a two-column row: text left 50%, canvas right 50%)
**HTML title tag:** Agent-Based & Network Spread — Superspreading, Contact Networks, and Worms

**Subtitle:** Compartmental averages assume everyone mixes with everyone; real spread runs on networks with hubs and superspreaders — so when the average stops describing anyone, modelers stop averaging and simulate individuals instead.

**Intro callout (blue-left-border box):** When the mixing assumption breaks, modelers drop the differential equations and simulate the population one person at a time — synthetic households, workplaces, and schools, each agent following a daily routine, each contact a chance to transmit. The payoff is realism the averages cannot express: superspreaders, hub-dominated networks, lumpy cluster-driven outbreaks. And the same machinery describes malware racing across computer networks, where the host is a machine and the clock runs in minutes.

## 1. From averages to agents

When everyone-mixes-with-everyone fails, modelers build the population itself: one software agent per person, each with a home, a workplace or school, and a daily routine.

- **Synthetic population:** agents are generated to match census household structure.
- **Daily schedules:** each agent cycles through home, work or school, and community.
- **Contact events:** transmission fires probabilistically when agents co-locate.
- **Household realism:** family structure decides who infects whom first.
- **National scale:** pandemic planning models simulate millions of agents.
- **Policy levers:** closures and staggered shifts map directly onto schedules.
- **The cost:** every added behavior brings parameters needing data or assumptions.

Key point: Agent-based models trade the clean mathematics of compartments for structural realism, and pay for it with an enormous parameter count — every parameter is a place where an assumption hides.

### Visualization (canvas `c1`, 720×360)

Three location boxes (home, school, workplace) with agent dots; contact edges light up inside the school where an infectious agent is co-located; dashed schedule arrows connect the boxes.

- **Title (bold 14px `#1a5276`, centered, y=22):** "One simulated day: agents move between locations, contacts happen where they meet"
- **Location boxes:** three 180×130 rects at (40, 70), (270, 70), (500, 70); fill `rgba(26,82,118,0.12)` for home and workplace, `#fff` for school; borders 1.5px `#1a5276` (school border 2px `#e67e22`). Bold 12px `#1a5276` centered headers at each box center x, y=90: "HOME", "SCHOOL", "WORKPLACE".
- **Home agents:** four `#999` dots (radius 5) at (80, 140), (120, 172), (162, 138), (98, 182).
- **School agents:** one infectious `#e74c3c` dot (radius 6) at (355, 140); four susceptible `#1a5276` dots (radius 5) at (308, 165), (398, 158), (328, 185), (382, 186). Contact edges: 2px `#e67e22` lines from the infectious dot to (308, 165) and (398, 158); 1px `#ccc` lines between (308,165)–(328,185) and (398,158)–(382,186).
- **Workplace agents:** four `#999` dots (radius 5) at (540, 142), (582, 170), (625, 138), (558, 184).
- **Schedule arrows:** dashed (5/4) 1.5px `#999` horizontal arrows with filled arrowheads at y=135, from x=222 to x=268 and from x=452 to x=498; 10px `#666` centered label "daily schedule" above each arrow at y=124.
- **Transmission note:** bold 11px `#e67e22` centered at (360, 232): "co-located contacts — transmission is probabilistic per contact".
- **Legend (left-aligned rows starting (60, 268), 18px spacing, 10px `#666` labels):** `#e74c3c` dot "infectious agent"; `#1a5276` dot "susceptible agent in contact range"; `#999` dot "agents elsewhere — no exposure"; 22px 2px `#e67e22` line swatch "contact edge where transmission can occur".
- **Caption (12px `#999`, centered, y = h−14):** "No differential equations — just people, places, and probabilistic contacts, millions of times over"

## 2. Superspreading: the average hides the story

R₀ is only a mean, and for many diseases a badly misleading one: the dispersion parameter k measures how unevenly that mean is spread across individuals.

- **R₀ is an average:** it says nothing about who does the transmitting.
- **Dispersion k:** small k means transmission concentrates in very few people.
- **80/20 pattern:** a small minority of cases generate most new infections.
- **The modal case:** most infected people transmit to nobody at all.
- **Backward tracing:** finding the event that infected a case beats tracing forward.
- **Lumpy outbreaks:** cluster-driven spread alternates quiet stretches and explosions.
- **Modeling consequence:** simulate individual variation, not just the mean.

Key point: Two diseases with identical R₀ can behave completely differently: when k is small, the epidemic is a sequence of rare superspreading events, and the mean describes almost no individual case.

### Visualization (canvas `c2`, 720×360)

Histogram of secondary infections per case — a tall bar at zero, a long thin tail out to about 30, and a dashed mean line that describes almost nobody.

- **Title (bold 14px `#1a5276`, centered, y=22):** "Secondary infections per case — the mean falls where almost no case sits"
- **Axes:** 1.5px `#999` baseline at y=290 from x=55 to x=685; 10px `#999` centered x-axis label "secondary infections per case" at (370, 324); 10px `#999` tick labels "0", "5", "10", "15", "20", "25", "30" centered under bar centers at y=308.
- **Bars:** 31 bars for counts 0–30, each 16px wide, left edge x(n) = 62 + n×20, drawn up from y=290. Heights in px: n=0: 215; n=1: 38; n=2: 22; n=3: 15; n=4: 11; n=5: 9; n=6: 7; n=7: 6; n=8: 5; n=9: 5; n=10: 4; n=11: 0; n=12: 8; n=13: 0; n=14: 7; n=15: 0; n=16: 6; n=17: 0; n=18: 10; n=19: 0; n=20: 5; n=21: 0; n=22: 4; n=23: 0; n=24: 9; n=25: 0; n=26: 4; n=27: 0; n=28: 3; n=29: 0; n=30: 7. Fill `rgba(26,82,118,0.35)` for n≤9, `#e74c3c` for n≥10.
- **Mean line:** dashed (6/4) 2px `#e67e22` vertical line at x=130 from y=60 to y=290; bold 11px `#e67e22` left-aligned two-line label at (140, 74) / (140, 90): "mean R₀ ≈ 3" / "describes almost nobody".
- **Zero annotation:** bold 11px `#1a5276` left-aligned at (95, 62): "most cases infect no one", thin 1px `#1a5276` connector from (100, 68) down to the zero bar's top near (74, 74).
- **Tail annotation:** bold 11px `#e74c3c` right-aligned two-line label at (672, 220) / (672, 236): "a few superspreaders" / "seed clusters of 10–30".
- **Caption (12px `#999`, centered, y = h−14):** "Same mean, wildly different stories — dispersion k, not R₀, sets the outbreak's character"

## 3. Network structure decides the epidemic

Contacts are not random draws from the population; they run over a network whose highly connected hubs decide how fast and how far anything spreads.

- **Hubs exist:** some individuals hold vastly more contacts than the median person.
- **Heavy tails:** on hub-dominated networks the epidemic threshold nearly vanishes.
- **Early amplification:** hubs get infected early and rebroadcast to everyone.
- **Targeted protection:** vaccinating hubs beats random doses at equal cost.
- **Finding hubs:** asking people to name a contact samples hubs preferentially.
- **Clustering:** tight communities slow global spread but intensify local spread.
- **Bridges:** a few links between communities carry the epidemic across them.

Key point: On a heavy-tailed contact network the classic threshold intuition fails — even a weakly transmissible pathogen can persist because hubs keep rebroadcasting it, and the cheapest defense is protecting the hubs, not dosing at random.

### Visualization (canvas `c3`, 720×380)

Node-link network with one large hub highlighted red and infection edges radiating from it; a side panel contrasts random immunization with targeted hub immunization at equal doses.

- **Title (bold 14px `#1a5276`, centered, y=22):** "The hub rebroadcasts — protect it first"
- **Main network (left, x=40–420):** hub node radius 13 filled `#e74c3c` at (210, 200); bold 11px `#e74c3c` centered label "HUB" at (210, 176). Twelve peripheral nodes radius 6 at: (335, 200), (315, 135), (255, 90), (180, 82), (115, 118), (80, 178), (92, 248), (142, 296), (215, 312), (287, 286), (332, 252), (150, 160). Infection edges: 1.5px `#e74c3c` lines from the hub to nodes 1, 2, 3, 5, 7, 9, 11, 12 (list order); those nodes filled `#e67e22`. Remaining nodes filled `#bbb`, connected by 1px `#ccc` edges: node2–node3, node5–node6, node7–node8, node9–node10, node4–node5.
- **Divider:** 1px `#e0e0e0` vertical line at x=440 from y=44 to y=340.
- **Side panel (x=450–700):** header 11px `#666` centered at (575, 56): "equal doses, different outcomes". Two mini star networks, centers at (545, 130) and (545, 262), six leaves each at angles 0°, 60°, 120°, 180°, 240°, 300°, leaf distance 45, leaf radius 5, center radius 9.
- **Random-dose star (top):** center filled `#e74c3c`; leaves at 0° and 120° drawn white-filled with 2px `#27ae60` rings (vaccinated), their edges 1px `#ccc`; remaining four leaves filled `#e67e22` with 1.5px `#e74c3c` edges from the center. Bold 11px `#e74c3c` centered label at (545, 196): "random doses — hub still spreads".
- **Hub-shielded star (bottom):** center white-filled with 2.5px `#27ae60` ring; all edges 1px `#ccc`; all leaves filled `#bbb`. Bold 11px `#27ae60` centered label at (545, 328): "hub shielded — spread stalls".
- **Caption (12px `#999`, centered, y = h−14):** "Network position beats headcount — the same dose budget spent on hubs stops far more spread"

## 4. Computer viruses and worms: same equations, silicon host

Replace people with computers and the equations survive: early worm outbreaks traced the same logistic S-curve as an epidemic, only thousands of times faster.

- **Same S-curve:** exponential growth, then saturation as susceptible hosts run out.
- **SIS dynamics:** malware that reinfects cleaned machines fits the SIS model.
- **Patching as vaccination:** a patch removes a host from the susceptible pool.
- **Herd effect:** patching protects the network only once coverage is high.
- **Topology sets speed:** the scanning strategy defines the contact graph.
- **Minutes, not weeks:** internet-scale worms can saturate in tens of minutes.
- **Defense implication:** response must be automated because humans are too slow.

Key point: The host changed from person to machine but the model did not — what changed is the clock. Spread measured in minutes leaves no room for manual response, so defenses must be pre-positioned the way vaccination is: patch coverage before the outbreak, not during it.

### Visualization (canvas `c4`, 720×340)

Logistic S-curve of infected hosts versus time in minutes, with exponential and saturation phases annotated, and a second lower curve showing the same worm against a mostly patched population.

- **Title (bold 14px `#1a5276`, centered, y=22):** "Worm propagation — an epidemic curve with the clock in minutes"
- **Axes:** 1.5px `#999` lines — vertical from (70, 55) to (70, 270), horizontal from (70, 270) to (670, 270); 10px `#999` centered x-axis label "time (minutes)" at (370, 300); 10px `#999` centered tick labels "0", "10", "20", "30" at x=70, 270, 470, 670, y=286; 10px `#999` left-aligned y-axis label "infected hosts" at (76, 50).
- **Susceptible pool line:** dashed (5/4) 1px `#bbb` horizontal line at y=75 from x=70 to x=670; 10px `#999` right-aligned label "susceptible pool" at (665, 68).
- **Unpatched curve:** 2.5px `#e74c3c` polyline over x=70–670 in ~200 steps of y = 270 − 195 / (1 + exp(−(x − 250) / 28)).
- **Patched curve:** 2px `#27ae60` polyline over x=70–670 in ~200 steps of y = 270 − 55 / (1 + exp(−(x − 420) / 45)); 11px `#27ae60` left-aligned label "same worm, mostly patched population" at (445, 232).
- **Phase annotations:** bold 11px `#e67e22` left-aligned "exponential phase" at (110, 150) with a thin 1px `#e67e22` connector from (185, 155) to the red curve near (225, 180); bold 11px `#1a5276` left-aligned "saturation — susceptibles exhausted" at (420, 92).
- **Caption (12px `#999`, centered, y = h−14):** "Patching is vaccination — it flattens the curve only when coverage is already high at outbreak time"

## Regeneration instructions

- **Template:** backlog multi-section detail page. After h1, `.subtitle`, and `.intro` callout, one `.lang-section` per numbered section: `h2` (1.3rem `#1a5276`, `border-bottom: 2px solid #2980b9`, 4px padding-bottom), then a `table.layout` with one `<tr>`: left `<td class="text-col">` (50%) with lead paragraph, bullets, `.key-point`; right `<td class="viz-col">` (50%) with the canvas. Section h2 headings carry the "1.–4." numbers shown above.
- **Bullet style:** each bullet is a bold label + one short complete sentence that fits on one line at normal page width — no wrapping; split dense content into more bullets rather than longer ones. Bullet `<strong>` labels are colored `#1a5276` (via `li strong { color: #1a5276; }`).
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with `border-bottom: 2px solid #2980b9`; `.subtitle` `#666` 0.95rem; `.intro` — background `#f0f4f8`, `border-left: 3px solid #2980b9`, padding 8px 12px, 0.9rem, 32px margin-bottom; `.key-point` — background `#f8f9fa`, `border-left: 3px solid #e74c3c`, padding 8px 12px, 0.9rem; `ul` 0.92rem; canvases `width: 100%`, `border: 1px solid #e0e0e0`, radius 4px; `.lang-section` 40px margin-bottom.
- **Canvases:** intrinsic width 720, heights 360/360/380/340 as specified; shared `setupCanvas(id, h)` helper scales by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). Canvas fonts use `-apple-system, sans-serif`. Every canvas gets a bold 14px `#1a5276` centered title at y=22 and a 12px `#999` centered caption at y = h−14.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, accent `#2980b9`; grays `#555`/`#666`/`#999`/`#bbb`/`#ccc`; translucent fills `rgba(26,82,118,0.12)`, `rgba(26,82,118,0.35)`.
- No nav bar, no back/home links, no cross-references to other pages. Worm content stays at the modeling/defense level (propagation curves, patching) — no operational attack detail.
