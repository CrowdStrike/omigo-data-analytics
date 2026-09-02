# Discrete-Event Simulation

**Page type:** detail page (four titled sections, each a two-column row: text left 50%, canvas right 50%)
**HTML title tag:** Discrete-Event Simulation — Jumping the Clock from Event to Event

**Subtitle:** Jump the clock from event to event and watch waiting lines form and servers saturate — hours of clinic time simulated in milliseconds.

**Intro callout (blue-left-border box):** Most of a waiting room's morning is dead time — nobody arrives, nobody finishes, nothing changes. So instead of ticking the clock one second at a time, this style of model (discrete-event simulation) keeps a sorted to-do list of future happenings and teleports the clock straight to the next one. One small clinic with one doctor and five patients is enough to see the whole machine: arrivals, a waiting line, and a server that stays busy the entire morning.

## 1. Skipping the boring parts

The single trick behind the whole method is a rule about time: between two events the world is frozen, so the clock is allowed to jump.

- **The one rule:** Nothing in the model changes between events, so the time between them can be skipped.
- **Event list:** The engine keeps a time-sorted to-do list of future arrivals and finishes.
- **Jump and update:** Take the earliest item, set the clock to its time, and update the world.
- **Events breed events:** Starting a treatment schedules its own finish event later on the list.
- **Why it is fast:** A 45-minute morning ticked second by second needs 2,700 checks; the same morning is only 10 events.
- **Where randomness enters:** Arrival gaps and service times are drawn once, when each event is scheduled.

Key point: The clock teleports because the model declares that nothing can change between events — all the action lives on the event list, and the empty seconds cost nothing.

### Visualization (canvas `c1`, 720×360)

Two horizontal timelines for the same 45-minute morning: a dense tick-every-second axis on top, and an event-jump axis below with only 10 marked stops connected by hop arcs.

- **Title (bold 14px `#1a5276`, centered, y=22):** "Two ways to move a clock through the same 45-minute morning"
- **Top label:** bold 11px `#666` left-aligned at (60, 100): "tick every second — 2,700 checks, almost all find nothing new".
- **Top axis:** 1.5px `#999` horizontal line at y=130 from x=60 to x=660; 1px `#bbb` vertical ticks from y=122 to y=130 every 4 px from x=60 to x=660 (deterministic loop, 151 ticks); 10px `#999` centered "9:00" at (60, 148) and "9:45" at (660, 148).
- **Bottom label:** bold 11px `#1a5276` left-aligned at (60, 214): "jump to the next event — only 10 stops".
- **Bottom axis:** 1.5px `#999` horizontal line at y=270 from x=60 to x=660; 10px `#999` centered "9:00" at (60, 288) and "9:45" at (660, 288).
- **Event mapping:** minute t maps to x = 60 + t × (600/45); event times 0, 4, 6, 10, 18, 20, 22, 30, 36, 45 → x = 60, 113, 140, 193, 300, 327, 353, 460, 540, 660.
- **Event dots:** filled circles radius 5 on the bottom axis — arrivals (t = 0, 4, 6, 20, 22) in `#27ae60`, service finishes (t = 10, 18, 30, 36, 45) in `#e67e22`.
- **Hop arcs:** 1px `#1a5276` upper semicircle arcs between each pair of consecutive event x-positions (center at the midpoint on y=270, radius = half the gap, drawn from π to 2π).
- **Dot legend (right of the bottom label):** filled `#27ae60` circle radius 5 at (420, 210) with 10px `#666` left-aligned "arrival" at (430, 214); filled `#e67e22` circle radius 5 at (500, 210) with "service finish" at (510, 214).
- **Annotation:** bold 11px `#e74c3c` centered at (360, 320): "the clock teleports — by rule, nothing changes between events".
- **Caption (12px `#999`, centered, y = h−14):** "Illustrative 45-minute clinic morning — event jumps do in 10 steps what ticking does in 2,700"

## 2. A morning at the clinic, event by event

Five patients, one doctor, one waiting line — small enough to redo by hand, and every number in the chart comes from this little table.

- **The cast:** One doctor, one waiting line, five patients across a 45-minute morning.
- **Arrivals:** Patients walk in at 9:00, 9:04, 9:06, 9:20, and 9:22.
- **Treatment times:** Their treatments take 10, 8, 12, 6, and 9 minutes respectively.
- **Trace it by hand:** The doctor serves them back to back, starting at 9:00, 9:10, 9:18, 9:30, and 9:36.
- **The waits:** The five patients wait 0, 6, 12, 10, and 14 minutes — an average of 8.4.
- **A saturated server:** The treatments sum to exactly 45 minutes, so the doctor never idles all morning.
- **Only 10 events:** Five arrivals plus five finishes are the only moments anything changes.

Key point: Everything a discrete-event run produces — waits, line lengths, busy time — falls out of one hand-traceable rule: at each event, either someone joins the line or the doctor takes the next person from it.

### Visualization (canvas `c2`, 720×380)

Step chart of the number of people waiting (not counting the one being treated) across the worked morning, with green dots where an arrival joins the line and orange dots where a treatment finishes.

- **Title (bold 14px `#1a5276`, centered, y=22):** "Queue at the clinic — people waiting after each event"
- **Axes:** 1.5px `#999` lines — vertical from (70, 60) to (70, 300), horizontal from (70, 300) to (650, 300). Y ticks 10px `#999` right-aligned: "0" at (62, 304), "1" at (62, 234), "2" at (62, 164). Y-axis title 11px `#666` rotated −90° centered at (40, 180): "people waiting". X tick labels 10px `#999` centered at y=318: "9:00" at x=70, "9:10" at x=199, "9:18" at x=302, "9:30" at x=457, "9:45" at x=650. X-axis title 11px `#666` centered at (360, 336): "clock time (illustrative morning)".
- **Mapping:** minute t maps to x = 70 + t × (580/45); queue size q maps to y = 300 − 70q.
- **Step path (queue after each event):** (70, 300) → (122, 300) → (122, 230) → (147, 230) → (147, 160) → (199, 160) → (199, 230) → (302, 230) → (302, 300) → (328, 300) → (328, 230) → (354, 230) → (354, 160) → (457, 160) → (457, 230) → (534, 230) → (534, 300) → (650, 300).
- **Fill under the step:** the same path closed down to the baseline y=300, filled `rgba(26,82,118,0.12)`, drawn before the line.
- **Step line:** 2.5px `#1a5276` polyline along the step path.
- **Event dots (radius 4):** arrivals in `#27ae60` at (122, 230), (147, 160), (328, 230), (354, 160); finishes in `#e67e22` at (199, 230), (302, 300), (457, 230), (534, 300).
- **Dot legend (top left):** filled `#27ae60` circle radius 4 at (90, 78) with 10px `#666` left-aligned "arrival joins the line" at (100, 82); filled `#e67e22` circle radius 4 at (90, 98) with "a treatment finishes" at (100, 102).
- **Annotation:** bold 11px `#e74c3c` centered at (255, 140): "peak: two waiting"; thin 1px `#e74c3c` connector from (255, 146) to (180, 158).
- **Caption (12px `#999`, centered, y = h−14):** "Waits: 0, 6, 12, 10, 14 minutes — average 8.4, with the doctor busy the entire 45 minutes"

## 3. Why queues explode near full utilization

The share of time the server is busy (utilization) controls waiting in a brutally nonlinear way — the last 15% of busyness buys almost all of the pain.

- **The dial:** Utilization is simply the fraction of the morning the doctor is busy.
- **Not proportional:** Going from 40% to 80% busy adds some wait; going from 80% to 95% multiplies it.
- **The numbers:** With 10-minute treatments, 80% busy means about 40 minutes of average wait.
- **The cliff:** At 95% busy the same clinic averages over 3 hours of waiting.
- **No slack to recover:** A nearly-full server never catches up after an unlucky burst of arrivals.
- **Little's law:** On average, people in line = arrival rate × time each person spends in line.
- **Feels fine, then chaos:** 80% busy feels comfortable, and the collapse arrives with little warning.

Key point: Waiting time is a hockey stick, not a straight line — a clinic running at 95% busy is not "a bit slower" than one at 80%, it is a different world, and only slack capacity buys the difference back.

### Visualization (canvas `c3`, 720×380)

Hockey-stick curve of average wait versus server utilization for the 10-minute-treatment clinic, with a green marker at 80% and a red marker plus dashed drop line at 95%.

- **Title (bold 14px `#1a5276`, centered, y=22):** "Average wait vs how busy the server is — the hockey stick"
- **Axes:** 1.5px `#999` lines — vertical from (80, 60) to (80, 310), horizontal from (80, 310) to (640, 310). X ticks 10px `#999` centered at y=326: "0%" at x=80, "25%" at x=220, "50%" at x=360, "75%" at x=500, "100%" at x=640. Y ticks 10px `#999` right-aligned at x=72: "0" at y=314, "60" at y=242, "120" at y=170, "180" at y=98. X-axis title 11px `#666` centered at (360, 346): "server utilization (share of time busy)". Y-axis title 11px `#666` rotated −90° centered at (46, 185): "average wait (minutes)".
- **Mapping:** utilization u maps to x = 80 + 560u; wait of m minutes maps to y = 310 − 1.2m.
- **Curve:** 2.5px `#1a5276` polyline through the hardcoded (utilization, wait-minutes) pairs (0.1, 1), (0.2, 3), (0.3, 4), (0.4, 7), (0.5, 10), (0.6, 15), (0.7, 23), (0.8, 40), (0.85, 57), (0.9, 90), (0.95, 190) → points (136, 309), (192, 306), (248, 305), (304, 302), (360, 298), (416, 292), (472, 282), (528, 262), (556, 242), (584, 202), (612, 82).
- **80% marker:** filled `#27ae60` circle radius 5 at (528, 262); bold 10px `#27ae60` centered at (480, 238): "80% busy: 40 min"; thin 1px `#27ae60` connector from (495, 244) to (524, 258).
- **95% marker:** filled `#e74c3c` circle radius 5 at (612, 82); bold 11px `#e74c3c` right-aligned at (600, 78): "95% busy: over 3 hours"; dashed (4/3) 1px `#e74c3c` vertical drop line from (612, 88) to (612, 310).
- **Little's law note:** bold 10px `#8e44ad` left-aligned at (100, 84): "Little's law: people in line = arrival rate × time in line".
- **Caption (12px `#999`, centered, y = h−14):** "Illustrative single-server clinic with 10-minute treatments — the last 15% of busyness buys almost all of the pain"

## 4. Where it's used and what goes wrong

The same clinic skeleton runs emergency rooms, call centers, factory lines, and computer networks — and the same sizing mistake sinks all of them.

- **Everywhere lines form:** Emergency rooms, call centers, factory stations, and network packets are all this model.
- **The classic mistake:** Sizing capacity for the average arrival rate alone, as if arrivals were evenly spaced.
- **Variability rules:** Two clinics with the same 6-per-hour average can have completely different lines.
- **Steady vs bursty:** One patient every 10 minutes means zero waiting; three-at-a-time batches average 8 minutes.
- **Warm-up bias:** The simulated morning starts empty, so early statistics flatter the system — discard them.
- **One run lies:** Rerun many randomized mornings and report the spread, never a single lucky trace.
- **Averages hide tails:** Complaints come from the 95th-percentile wait, not the mean.

Key point: Averages do not size systems — two arrival patterns with the identical average rate produce entirely different waiting lines, so the model must be fed realistic burstiness or it will bless a design that drowns.

### Visualization (canvas `c4`, 720×380)

Same-average comparison over one illustrative hour: arrival tick strips for a steady and a bursty pattern (both 6 per hour), and below them the two resulting people-waiting step lines.

- **Title (bold 14px `#1a5276`, centered, y=22):** "Same average arrivals, very different lines — steady vs bursty (6 per hour each)"
- **Steady strip:** 10px `#27ae60` left-aligned label at (60, 72): "steady: one every 10 min"; 2px `#27ae60` vertical ticks from y=80 to y=92 at x = 60, 160, 260, 360, 460, 560 (minute t maps to x = 60 + 10t).
- **Bursty strip:** 10px `#e74c3c` left-aligned label at (60, 112): "bursty: three at once every 30 min"; 2px `#e74c3c` vertical ticks from y=120 to y=132 at x = 57, 60, 63 and 357, 360, 363 (triples at 9:00 and 9:30).
- **Axes:** 1.5px `#999` lines — vertical from (60, 150) to (60, 310), horizontal from (60, 310) to (660, 310). Y ticks 10px `#999` right-aligned at x=52: "0" at y=314, "1" at y=254, "2" at y=194. Y-axis title 11px `#666` rotated −90° centered at (34, 230): "people waiting". X tick labels 10px `#999` centered at y=326: "9:00" at x=60, "9:30" at x=360, "10:00" at x=660.
- **Mapping:** minute t maps to x = 60 + 10t; queue size q maps to y = 310 − 60q.
- **Steady queue:** 2.5px `#27ae60` horizontal line at y=306 from x=60 to x=660 (drawn 4 px above the axis so it stays visible); bold 10px `#27ae60` right-aligned at (655, 296): "steady: nobody waits".
- **Bursty queue (8-minute treatments):** 2.5px `#e74c3c` step polyline (60, 190) → (140, 190) → (140, 250) → (220, 250) → (220, 310) → (360, 310) → (360, 190) → (440, 190) → (440, 250) → (520, 250) → (520, 310) → (660, 310).
- **Bursty label:** bold 10px `#e74c3c` left-aligned at (68, 182): "bursty: two waiting the moment a batch lands".
- **Wait annotation:** bold 11px `#e74c3c` left-aligned at (530, 220): "average wait: 0 vs 8 min".
- **Caption (12px `#999`, centered, y = h−14):** "Illustrative hour with 8-minute treatments — the average arrival rate is identical, the experience is not"

## Regeneration instructions

- **Template:** backlog multi-section detail page. After h1, `.subtitle`, and `.intro` callout, one `.lang-section` per numbered section: `h2` (1.3rem `#1a5276`, `border-bottom: 2px solid #2980b9`, 4px padding-bottom), then a `table.layout` with one `<tr>`: left `<td class="text-col">` (50%) with lead paragraph, bullets, `.key-point`; right `<td class="viz-col">` (50%) with the canvas. Section h2 headings carry the "1.–4." numbers shown above. The h1 carries no index number.
- **Bullet style:** each bullet is a bold label + one short complete sentence that fits on one line at normal page width; split dense content into more bullets rather than longer ones. Bullet `<strong>` labels are colored `#1a5276` (via `li strong { color: #1a5276; }`).
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with `border-bottom: 2px solid #2980b9`; `.subtitle` `#666` 0.95rem; `.intro` — background `#f0f4f8`, `border-left: 3px solid #2980b9`, padding 8px 12px, 0.9rem, 32px margin-bottom; `.key-point` — background `#f8f9fa`, `border-left: 3px solid #e74c3c`, padding 8px 12px, 0.9rem; `ul` 0.92rem; canvases `width: 100%`, `border: 1px solid #e0e0e0`, radius 4px; `.lang-section` 40px margin-bottom.
- **Canvases:** intrinsic width 720, heights 360/380/380/380 as specified; shared `setupCanvas(id, h)` helper scales by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). Canvas fonts use `-apple-system, sans-serif`. Every canvas gets a bold 14px `#1a5276` centered title at y=22 and a 12px `#999` centered caption at y = h−14.
- **Data discipline:** all chart data is hardcoded literal arrays (no `Math.random`); invented numbers are labeled "illustrative" in the captions; every number quoted in the bullets (2,700 checks, 10 events, waits 0/6/12/10/14, average 8.4, 40 min at 80%, over 3 hours at 95%, 6 per hour, 0 vs 8 min) matches the chart it appears next to.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, accent `#2980b9`; grays `#555`/`#666`/`#999`/`#bbb`; translucent fill `rgba(26,82,118,0.12)`.
- No nav bar, no back/home links, no cross-references to other pages, no item counts in text.
