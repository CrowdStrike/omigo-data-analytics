# Feedback Control Loops

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Feedback Control Loops

**Subtitle:** A feedback control loop measures, compares to a target, and acts — over and over; PID is just three ways of reading the error so a thermostat lands the room on 21° without swinging past it

## A Chilly Room and a Blunt Switch

**Tags:** `core idea` (blue), `the loop` (green), `oscillation` (orange)

- **The room** — a bedroom sits at 15°C on a cold morning, and the owner wants a steady 21°C
- **The loop** — every minute the thermostat measures the temp, compares it to 21°, then acts
- **The blunt switch** — the simplest rule is bang-bang: heater full on below 21°, fully off above
- **Overshoot** — the radiator stays hot after switch-off, so the room coasts up to 22.1°, past target
- **Oscillation** — then it cools to 20.1° before the heat catches up; the room swings ±1° forever

*Example (italic):* By minute 9 the room hits 22.1°, drifts down to 20.1° by minute 14, then climbs back to 22.1° by minute 19 — the same swing repeats all morning.

**Key point:** Measure → compare → act, repeated forever, is a feedback control loop — and an all-or-nothing action makes it oscillate around the target instead of settling on it.

### Visualization (canvas `c1`, 720×300)

Single-panel time series: the bang-bang room temperature sawing around the 21° setpoint, with an on/off heater strip along the bottom showing why each swing happens.

- **Title (bold 15px, `#1a5276`, top center):** "Bang-Bang Thermostat: the Room Never Settles".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 190; x = minutes 0 to 30 (20px per minute), 12px `#444` tick labels "0", "5", ..., "30" every 5 minutes, axis caption "minutes"; y = temperature 14 to 23°C, 12px `#444` tick labels "14°", "16°", ..., "22°", light `#e5e9ef` gridlines at each even degree.
- **Setpoint:** horizontal dashed `#6b7280` (dash 4/3) line at 21° (y≈97); 12px `#6b7280` label "target 21°" at its left end.
- **Temperature curve:** blue `#2a78d6` 3px line through per-minute values (minutes 0–30): `[15.0, 16.2, 17.3, 18.3, 19.2, 20.0, 20.7, 21.3, 21.8, 22.1, 21.9, 21.5, 21.0, 20.5, 20.1, 20.3, 20.8, 21.4, 21.9, 22.1, 21.8, 21.4, 20.9, 20.4, 20.1, 20.4, 20.9, 21.5, 22.0, 22.1, 21.8]`.
- **Heater strip:** 8px-tall orange `rgba(217,89,38,0.55)` bars just above the baseline (y=252 to y=260) over the on intervals minutes 0–7, 13–17, and 22–27 (off once the temp crosses 21°); 11px `#d95926` label "heater on" beside the first bar.
- **Peak/trough markers:** 6px blue dots at (9, 22.1) and (14, 20.1) with bold 12px blue labels "22.1°" above and "20.1°" below.
- **Annotation (bold 13px orange `#d95926`, near x=430, y=75):** "never settles — swings ±1° around 21° forever".
- **Caption (12px `#444`, bottom right):** "illustrative — simple room model, not a real furnace".

## Heating by the Numbers: Power = 10 × Error

**Tags:** `worked example` (blue), `proportional` (green)

- **A dial, not a switch** — instead of on/off, set heater power to 10% for every degree of error
- **Room physics** — every 1% of heater power adds 0.05° a minute; the walls leak 0.2° a minute
- **Minute 0** — temp 15.0°, error 6.0°, power 60%: gain 3.0° minus the 0.2° leak lands at 17.8°
- **Minute 1** — temp 17.8°, error 3.2°, power 32%: gain 1.6° minus 0.2° gives 19.2°
- **Minute 2** — temp 19.2°, error 1.8°, power 18%: +0.9° − 0.2° gives 19.9°; each step is smaller
- **The stall** — by minute 8 the room parks at 20.6°, where 4% power exactly cancels the 0.2° leak

*Example (italic):* Redo minute 1 by hand: error = 21 − 17.8 = 3.2°, power = 10 × 3.2 = 32%, new temp = 17.8 + 1.6 − 0.2 = 19.2°.

**Key point:** "Power = 10 × error" is the P of PID — a proportional response that pushes hard when far from the target and gently when close, so the approach is smooth instead of a swing.

### Visualization (canvas `c2`, 720×300)

Single-panel combo chart: the proportional-controller temperature line climbing and flattening below the setpoint, with the shrinking heater-power bars underneath it.

- **Title (bold 15px, `#1a5276`, top center):** "Proportional Control: Smooth Climb, but Stuck at 20.6°".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 190; x = minutes 0 to 15 (40px per minute), 12px `#444` tick labels "0", "3", "6", "9", "12", "15", axis caption "minutes"; y = temperature 14 to 23°C, tick labels "14°"–"22°" every 2°, light `#e5e9ef` gridlines.
- **Setpoint:** horizontal dashed `#6b7280` (dash 4/3) line at 21°; 12px `#6b7280` label "target 21°" at its left end.
- **Temperature curve:** blue `#2a78d6` 3px line through per-minute values (minutes 0–15): `[15.0, 17.8, 19.2, 19.9, 20.3, 20.4, 20.5, 20.6, 20.6, 20.6, 20.6, 20.6, 20.6, 20.6, 20.6, 20.6]`; fill under the curve `rgba(42,120,214,0.12)`.
- **Power bars:** thin (8px-wide) orange `rgba(217,89,38,0.45)` bars rising from the baseline at each minute, heights scaled so 100% power = 60px; per-minute power values `[60, 32, 18, 11, 7, 5, 5, 4, 4, 4, 4, 4, 4, 4, 4, 4]`; 11px `#d95926` label "heater power % (60 → 4)" above the first bar.
- **Stall marker:** vertical dashed blue (dash 4/3) line at minute 8 from baseline to the curve, bold 12px blue label "parks at 20.6°" beside it.
- **Annotation (bold 13px magenta `#d55181`, near x=430, y=80):** "stuck 0.4° short — proportional droop".
- **Caption (12px `#444`, bottom right):** "illustrative — 1% power = +0.05°/min, leak = 0.2°/min".

## Adding I and D: Why the Full PID Exists

**Tags:** `where it's used` (blue), `integral` (green), `derivative` (orange)

- **The leftover gap** — the P controller stalls 0.4° short, because holding heat needs nonzero power
- **I for integral** — the I-term adds up the error over time, nudging power up until the gap closes
- **D for derivative** — the D-term watches how fast the temp is rising and eases off before target
- **The result** — full PID reaches 21°, brushes 21.3° at minute 6, and settles flat by minute 12
- **Beyond thermostats** — the same loop paces ad spend, autoscales servers, steers cruise control
- **Data science angle** — a metric, a target, and an automated response is this loop, named or not

*Example (italic):* Same cold room, three controllers: bang-bang keeps swinging a degree either side of 21°, P parks at 20.6°, and PID holds 21.0°.

**Key point:** P reacts to the present error, I to its accumulated past, D to its current trend — together they reach the target and stay there without swinging.

### Visualization (canvas `c3`, 720×300)

Single-panel three-curve comparison on one time axis: bang-bang oscillating, proportional-only flattening short, and full PID landing on the setpoint.

- **Title (bold 15px, `#1a5276`, top center):** "Three Controllers, Same Cold Room".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 190; x = minutes 0 to 30 sampled every 2 minutes, 12px `#444` tick labels "0", "5", ..., "30", axis caption "minutes"; y = temperature 14 to 23°C, tick labels "14°"–"22°" every 2°, light `#e5e9ef` gridlines.
- **Setpoint:** horizontal dashed `#6b7280` (dash 4/3) line at 21°; 12px `#6b7280` label "target 21°" at its left end.
- **Shared x grid for all three curves (minutes):** `[0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30]`.
- **Bang-bang:** magenta `#d55181` 2px line, values `[15.0, 17.3, 19.2, 20.7, 21.8, 21.9, 21.0, 20.1, 20.8, 21.9, 21.8, 20.9, 20.1, 20.9, 22.0, 21.8]`; 12px magenta label "bang-bang" near its second peak (x≈18).
- **P-only:** orange `#d95926` 2px dashed (dash 6/4) line, values `[15.0, 19.2, 20.3, 20.5, 20.6, 20.6, 20.6, 20.6, 20.6, 20.6, 20.6, 20.6, 20.6, 20.6, 20.6, 20.6]`; 12px orange label "P only — 20.6°" near its right end.
- **Full PID:** green `#008300` 3px line, values `[15.0, 19.4, 20.9, 21.3, 21.2, 21.1, 21.0, 21.0, 21.0, 21.0, 21.0, 21.0, 21.0, 21.0, 21.0, 21.0]`; fill under `rgba(0,131,0,0.10)`; bold 12px green label "PID — holds 21.0°" near x≈12 above the curve.
- **Overshoot marker:** 6px green dot at (6, 21.3) with 12px green label "21.3° at min 6".
- **Annotation (bold 13px green `#008300`, near x=420, y=70):** "only PID lands on 21° and stays".
- **Caption (12px `#444`, bottom right):** "illustrative — same room model for all three".

## The Gain-Cranking Trap

**Tags:** `common mistake` (red), `instability` (orange)

- **The temptation** — the gentle P controller heats slowly, so the obvious fix is a bigger gain
- **Gain 40** — power = 40 × error races the room from 15.0° to 22.0° in just two minutes
- **The price** — it overshoots the target, then rings between 20.5° and 21.5° for ten more minutes
- **Why it rings** — the room responds with a lag, so a violent correction always arrives too late
- **The real fix** — speed comes from tuning I and D, not brute gain; damping is part of the design

*Example (italic):* Gain 10 crawls to 20.6° and sits still; gain 40 hits 22.0° by minute 2 and is still wobbling around 21° at minute 12.

**Common mistake:** Fixing a slow controller by multiplying its gain — past a point, extra gain buys oscillation, not speed, because every correction lands after the room has already moved on.

### Visualization (canvas `c4`, 720×300)

Single-panel two-curve comparison: the gentle gain-10 climb versus the cranked gain-40 curve that shoots past the setpoint and rings before calming down.

- **Title (bold 15px, `#1a5276`, top center):** "Crank the Gain: Faster Start, Then Ringing".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 190; x = minutes 0 to 15 (40px per minute), 12px `#444` tick labels "0", "3", "6", "9", "12", "15", axis caption "minutes"; y = temperature 14 to 23°C, tick labels "14°"–"22°" every 2°, light `#e5e9ef` gridlines.
- **Setpoint:** horizontal dashed `#6b7280` (dash 4/3) line at 21°; 12px `#6b7280` label "target 21°" at its left end.
- **Gain 10 curve:** blue `#2a78d6` 2px line through per-minute values (minutes 0–15): `[15.0, 17.8, 19.2, 19.9, 20.3, 20.4, 20.5, 20.6, 20.6, 20.6, 20.6, 20.6, 20.6, 20.6, 20.6, 20.6]`; 12px blue label "gain 10 — slow, stalls at 20.6°" near its right end below the curve.
- **Gain 40 curve:** orange `#d95926` 3px line through per-minute values (minutes 0–15): `[15.0, 19.8, 22.0, 21.4, 20.5, 21.5, 21.3, 20.7, 21.3, 20.8, 21.2, 20.9, 21.1, 21.0, 21.0, 21.0]`; 12px orange label "gain 40 — fast, then rings" near x≈7 above the curve.
- **Overshoot marker:** 6px orange dot at (2, 22.0) with bold 12px orange label "22.0° at min 2".
- **Ring band:** light `rgba(217,89,38,0.10)` horizontal band between 20.5° and 21.5° from minute 3 to minute 13, 11px `#d95926` label "ringing zone" inside its right edge.
- **Annotation (bold 13px orange `#d95926`, near x=420, y=70):** "more gain = faster start, then unstable".
- **Caption (12px `#444`, bottom right):** "illustrative — same room model, only the gain changed".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all temperature and power series are the hardcoded arrays above (no randomness); all four charts share one toy room model (1% power = +0.05°/min, leak = 0.2°/min) so the worked-example arithmetic in the text reproduces the c2 and c4 curves exactly; every chart is labeled illustrative.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
