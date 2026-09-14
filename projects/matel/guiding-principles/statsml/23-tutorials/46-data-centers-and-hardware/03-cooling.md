# Cooling

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Cooling

**Subtitle:** Every watt of electricity a server consumes comes back out as heat — a data center is a building whose main job is moving that heat outdoors

## A 10 kW Rack Is a 10 kW Space Heater

**Tags:** `core idea` (blue), `physics` (green), `heat` (orange)

- **Conservation** — a server turns essentially every watt of electricity into heat; compute is a side effect
- **No exhaust pipe** — nothing leaves as motion or light worth counting; the energy exits as warm air
- **The rack** — a 10 kW rack heats the room like seven 1.5 kW home space heaters stacked in a phone booth
- **Around the clock** — the heater never cycles off: 10 kW for 24 hours is 240 kWh of heat every day
- **The hall** — 200 such racks make 2 MW of heat, and removing it is the building's main job

*Example (italic):* A laptop warms a lap at 30 W; one 10 kW rack in the hall is about 330 laptops' worth of heat, running all day and all night.

**Key point:** Thermodynamically a data center is a machine for turning electricity into hot air — cooling is not an accessory, it is half the building.

### Visualization (canvas `c1`, 720×300)

Horizontal bar chart comparing heat output in kW: household appliances vs a standard rack vs an AI training rack.

- **Title (bold 15px, `#1a5276`, top center):** "Heat Output: Household Appliances vs One Rack (kW)".
- **Axis:** left-aligned 12px `#444` row labels at x=20; bars start at x=230, max width 440 mapped to 40 kW (11 px per kW); no gridlines.
- **Rows (top to bottom at y = 70, 110, 150, 190, 230), label / color / bar width:**
  - "home space heater — 1.5 kW": grey `#6b7280`, width 17
  - "hair dryer — 1.8 kW": grey `#6b7280`, width 20
  - "electric oven — 3 kW": grey `#6b7280`, width 33
  - "standard rack — 10 kW": blue `#2a78d6`, width 110
  - "AI training rack — 40 kW": orange `#d95926`, width 440
- **Bar style:** 16px tall, fills at 0.85 alpha, 11px `#444` kW value labels at bar ends.
- **Annotation (bold 13px `#1a5276`, near x=360, y=185, pointing at the 10 kW bar):** "≈ 7 space heaters, never switched off".
- **Caption (12px `#444`, bottom right):** "wattages typical, illustrative".

## Cold in the Front, Hot out the Back

**Tags:** `worked example` (blue), `hot/cold aisle` (green), `CRAC` (orange)

- **Front to back** — every server inhales cold air through its face and exhausts hot air out its rear
- **The aisles** — rack rows alternate so fronts face fronts (cold aisle) and backs face backs (hot aisle)
- **Containment** — doors and ceiling panels seal each aisle so the two air streams never mix
- **The CRAC** — a computer-room air conditioner chills the hot return back to 22°C and pushes it around again
- **The math** — 10 kW at a 13°C rise (22°C in, 35°C out) needs roughly 1,350 CFM of airflow per rack
- **Mixing penalty** — if streams mix and the rise halves to 6.5°C, the same rack needs ~2,700 CFM

*Example (italic):* 22°C air enters the rack's front, 35°C air leaves its back, and the CRAC loop carries that 13°C difference away, around the clock.

**Key point:** Containment roughly doubles effective cooling capacity without adding a single chiller — purely by keeping supply air and return air separate.

### Visualization (canvas `c2`, 720×300)

Cross-section flow diagram of one contained aisle pair: CRAC unit, cold aisle feeding a rack's front, hot aisle behind it, and the overhead return path closing the loop.

- **Title (bold 15px, `#1a5276`, top center):** "The Air Loop: CRAC → Cold Aisle → Rack → Hot Aisle → Back to CRAC".
- **Floor:** 2px `#999` horizontal line at y=250 from x=30 to x=690.
- **CRAC unit:** rounded box x=45 to x=140, y=120 to y=248, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border, 12px `#2c3e50` label "CRAC unit" centered.
- **Supply path:** 3px blue `#2a78d6` arrow from the CRAC's right edge at y=235 to x=255 (along the floor), then two blue up-arrows in the cold aisle at x=270 and x=300 from y=245 to y=150.
- **Cold aisle label (bold 12px blue `#2a78d6`, centered at x=285, y=105):** "cold aisle 22°C".
- **Rack:** box x=335 to x=430, y=110 to y=248, fill `rgba(107,114,128,0.15)`, 2px `#6b7280` border, 12px label "rack 10 kW"; three short 2px `#6b7280` left-to-right arrows through it at y=140/175/210 (air passing front to back).
- **Hot aisle:** two orange `#d95926` up-arrows at x=460 and x=490 from y=245 to y=95; bold 12px orange label "hot aisle 35°C" centered at x=475, y=80.
- **Containment panels:** 3px `#2c3e50` vertical strokes capping the cold aisle (x=255 and x=330, y=110 to y=150) and the hot aisle (x=435 and x=515, y=110 down from y=60) — sketched, with 11px `#6b7280` label "containment" near x=545, y=125.
- **Return path:** dashed `#d95926` (dash 5/4) 2px arrow from x=475, y=60 leftward along y=45 to x=95, then down into the CRAC's top; 12px `#6b7280` label "hot return" at x=280, y=38.
- **Annotation (bold 13px `#1a5276`, near x=520, y=225):** "sealed aisles: the streams never mix".
- **Caption (12px `#444`, bottom right):** "temperatures illustrative".

## Free Cooling, and When Air Runs Out

**Tags:** `where it's used` (blue), `free cooling` (green), `liquid cooling` (orange)

- **Air's ceiling** — moving heat with air stops scaling around 30–50 kW per rack; fans and ΔT both max out
- **Free cooling** — when outside air is cold or dry enough, the chillers idle and fans alone do the job
- **Siting** — this is why data centers cluster in cool or dry regions rather than next to their users
- **Direct-to-chip** — cold plates pipe liquid onto CPUs and GPUs; water holds ~3,500× more heat than air per volume
- **Immersion** — the densest racks submerge whole servers in dielectric fluid and skip air entirely

*Example (italic):* A 100 kW AI training rack cannot be air-cooled at any fan speed the room can supply — the plumbing arrives with the GPUs.

**Key point:** Cooling method is set by rack density: room air to ~8 kW, contained aisles to ~30 kW, direct-to-chip to ~100 kW, immersion beyond that.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: the approximate per-rack heat each cooling method can remove, with a marked zone where air runs out and liquid takes over.

- **Title (bold 15px, `#1a5276`, top center):** "How Much Heat Each Method Can Pull From One Rack (kW)".
- **Axis:** left-aligned 12px `#444` row labels at x=20; bars start at x=230, max width 440 mapped to 150 kW (~2.9 px per kW).
- **Rows (top to bottom at y = 75, 120, 165, 210), label / color / bar width:**
  - "room air, no containment — ~8 kW": grey `#6b7280`, width 23
  - "contained hot/cold aisles — ~30 kW": blue `#2a78d6`, width 88
  - "direct-to-chip cold plates — ~100 kW": aqua `#199e70`, width 293
  - "immersion — 150+ kW": violet `#4a3aa7`, width 440 with 11px "+" label at the bar end
- **Bar style:** 16px tall, 11px `#444` kW value labels at bar ends.
- **Air ceiling marker:** vertical dashed red `#e74c3c` (dash 4/3) line at x=347 (40 kW) from y=55 to y=235, bold 12px red label "air's practical ceiling ~30–50 kW" at its top (y=48).
- **Annotation (bold 13px `#199e70`, near x=420, y=255):** "past the line, the coolant is a liquid".
- **Caption (12px `#444`, bottom right):** "ceilings approximate, illustrative".

## Cooling Loss Gives You Minutes, Not Hours

**Tags:** `common mistake` (red), `thermal shutdown` (orange)

- **The asymmetry** — a power loss has UPS batteries and generators; a heat emergency has only the room's air
- **The clock** — a dense hall's inlet air climbs roughly 2°C per minute once the chillers stop
- **The thresholds** — servers start throttling above a ~27°C inlet and shut themselves down near 40°C
- **Ten minutes** — at that rate the inlet crosses 40°C about 10 minutes after cooling stops
- **The mistake** — writing cooling runbooks that assume the hours a power runbook gets; they don't exist

*Example (italic):* A chilled-water pump failure at 2am gives the on-call about 10 minutes to shed load before racks start thermal-tripping on their own.

**Common mistake:** Treating a cooling alarm like a power alarm. Batteries buy the power path time; nothing buys the heat path time except turning servers off.

### Visualization (canvas `c4`, 720×300)

Line chart of inlet air temperature in the minutes after total cooling loss: a dense hall vs a low-density hall, against throttle and shutdown thresholds.

- **Title (bold 15px, `#1a5276`, top center):** "Inlet Temperature After the Chillers Stop".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = minutes 0 to 20 with 12px `#444` tick labels every 5 min; y = °C from 20 to 50 (6 px per °C), gridlines `#e5e9ef` at 30 and 45, 12px y labels at 20/30/40/50.
- **Throttle threshold:** dashed orange `#d95926` (dash 4/3) horizontal line at 27°C (y=203), 11px orange label "27°C throttling begins" at its right end.
- **Shutdown threshold:** dashed red `#e74c3c` (dash 4/3) horizontal line at 40°C (y=125), bold 12px red label "40°C thermal shutdown" at its right end.
- **Dense hall line:** red `#e74c3c` 3px line through minutes `[0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20]`, °C `[22, 25.6, 29.2, 32.8, 36.4, 40, 42.5, 44.5, 46, 47.2, 48]` — ~1.8°C/min, easing as it nears equilibrium; 12px red series label "dense hall" near minute 5 above the line.
- **Low-density line:** blue `#2a78d6` 3px line through the same minute grid, °C `[22, 23.2, 24.4, 25.6, 26.8, 28, 29.2, 30.3, 31.4, 32.4, 33.4]`; 12px blue series label "low-density hall" near minute 14 below the line.
- **Crossing marker:** filled red dot (radius 4) where the dense line meets 40°C at minute 10.
- **Annotation (bold 13px red `#e74c3c`, near minute 11, y=85):** "shutdown 10 minutes after cooling stops".
- **Caption (12px `#444`, bottom right):** "heat-up rates illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); appliance wattages, airflow figures, cooling ceilings, and heat-up curves are typical values, invented and labeled illustrative; the 1,350 CFM figure follows from 10 kW at a 13°C air-temperature rise and must stay consistent with the 22°C/35°C aisle temperatures in text and diagram.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
