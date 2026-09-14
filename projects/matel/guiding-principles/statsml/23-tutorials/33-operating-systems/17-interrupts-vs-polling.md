# Interrupts vs Polling

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Interrupts vs Polling

**Subtitle:** Two ways to learn that something happened: keep walking over to check the door, or let the doorbell tap you on the shoulder

## The Doorbell vs. the Door Check

**Tags:** `core idea` (blue), `event handling` (green), `hardware` (orange)

- **The bakery** — one clerk works the counter while delivery trucks drop parcels at the back door
- **Polling** — every 2 minutes the clerk stops, walks to the door, and looks: parcel or no parcel
- **The waste** — most walks find an empty doorstep, yet each one still interrupts the counter work
- **Interrupts** — install a doorbell: the clerk ignores the door until a ring taps them on the shoulder
- **The CPU version** — a device raises an interrupt line; the CPU pauses, runs a handler, resumes

*Example (italic):* Between 9:00 and 9:20 the clerk makes 11 door checks but only 2 parcels arrive — the doorbell would have rung exactly twice.

**Key point:** Polling means the busy side repeatedly asks "anything yet?"; an interrupt means the event itself announces its arrival — the checking cost moves from the worker to the doorbell.

### Visualization (canvas `c1`, 720×300)

Two-row timeline of the same 20 minutes: the polling clerk's door checks (mostly empty) vs the doorbell clerk's two rings, with the two parcel arrivals marked on both rows.

- **Title (bold 15px, `#1a5276`, top center):** "Same 20 Minutes, Two Clerks: 11 Walks vs 2 Rings".
- **Axes:** shared time axis, x from 70 to 670 mapping minutes 0–20 ("9:00" to "9:20"), 12px `#444` tick labels every 5 min on a 2px `#999` baseline at y=255; row 1 (polling) centered at y=110, row 2 (doorbell) at y=195, each with a left 12px `#444` label at x=8 ("polling clerk" / "doorbell clerk").
- **Parcel arrivals:** vertical dashed `#6b7280` (dash 4/3) lines at minutes `[7, 16]` spanning both rows, 12px `#6b7280` label "parcel" at top of each.
- **Row 1 checks:** 11 tick marks (10px vertical, 3px) at minutes `[0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20]`; empty checks in mute `#6b7280`, the two that find a parcel (minutes 8 and 16) in green `#008300` with small filled circles.
- **Row 2 rings:** two bell dots (6px filled circles) in blue `#2a78d6` at minutes `[7, 16]` with 12px blue "ring" labels.
- **Annotation (bold 13px orange `#d95926`, near x=200, y=60):** "9 of 11 checks find an empty door".
- **Caption (12px `#444`, bottom right):** "arrival times illustrative".

## One Shift, 240 Checks, 12 Deliveries

**Tags:** `worked example` (blue), `hand math` (green)

- **The shift** — 480 minutes (8 hours), a door check every 2 minutes: 480 / 2 = 240 checks
- **The walk** — each check costs 15 seconds, so 240 × 15 s = 3,600 s = 60 minutes of walking
- **The hits** — only 12 trucks actually arrive, so 240 − 12 = 228 checks (95%) find nothing
- **The wait** — a parcel lands anywhere inside a 2-minute gap, so it waits 60 s on average
- **The doorbell** — 12 rings, 12 walks: about 3 minutes of walking and a ~15 s wait per parcel

*Example (italic):* The polling clerk spends 60 of 480 minutes walking to an almost-always-empty door; the doorbell clerk spends 3 and answers every parcel within seconds.

**Key point:** Polling cost scales with how often you check (240 checks); interrupt cost scales with how often things happen (12 events) — when events are rare, the gap is enormous.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart comparing the shift totals: door checks made, minutes spent walking, and average wait per parcel, polling vs doorbell side by side.

- **Title (bold 15px, `#1a5276`, top center):** "The 8-Hour Shift Scorecard: Polling vs Doorbell".
- **Axis:** bars start at x=240 on a 2px `#999` vertical baseline, extend right, max width 420; left-aligned 12px `#444` metric labels at x=20.
- **Rows (paired bars 14px tall, 6px gap, pairs centered at y = 85, 155, 225):**
  - "door checks made": mute `#6b7280` bar width 420 labeled "240", blue `#2a78d6` bar width 21 labeled "12"
  - "minutes walking": mute bar width 420 labeled "60 min", blue bar width 21 labeled "3 min"
  - "avg wait per parcel": mute bar width 420 labeled "60 s", blue bar width 105 labeled "15 s"
- **Bar style:** polling bars fill `rgba(107,114,128,0.45)`, doorbell bars solid `#2a78d6`, 11px value labels at bar ends; 11px legend top right ("polling" mute swatch, "doorbell" blue swatch).
- **Annotation (bold 13px green `#008300`, right side near y=260):** "95% of checks were wasted walks".
- **Caption (12px `#444`, bottom right):** "check math exact for the stated interval; delivery count illustrative".

## Why Your Laptop Isn't Checking the Keyboard in a Loop

**Tags:** `where it's used` (blue), `efficiency` (green), `latency` (orange)

- **Keyboards** — every keypress raises an interrupt; the CPU never loops asking "key yet?"
- **Battery** — a polling loop pins a core near 12% busy while idle; interrupt-driven code sleeps at ~0%
- **Job status** — a script polling a training job every 30 s can report "running" 29 s after it finished
- **Pipelines** — cron-style "check for new files hourly" is polling; a file-arrival trigger is an interrupt
- **Latency rule** — polling latency averages half the check interval; interrupt latency is near zero

*Example (italic):* A dashboard that polls a database every 30 s shows results up to 30 s stale; a change notification pushes the update in milliseconds.

**Key point:** Data scientists meet this trade constantly — poll-a-status-endpoint vs subscribe-to-an-event — and the same rule applies: rare events plus tight polling means paying for thousands of empty checks.

### Visualization (canvas `c3`, 720×300)

Line chart of CPU busy % over 60 idle seconds with 3 keypresses: a polling loop holds a flat 12% while the interrupt-driven handler sits at 0% with three brief spikes.

- **Title (bold 15px, `#1a5276`, top center):** "One Idle Minute, 3 Keypresses: Busy-Loop vs Interrupt".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = seconds 0 to 60, 12px `#444` tick labels every 15 s; y = CPU busy % 0 to 20, gridlines `#e5e9ef` at 5/10/15 with 12px `#444` labels.
- **Polling line:** mute `#6b7280` 3px flat line through seconds `[0, 15, 30, 45, 60]`, CPU `[12, 12, 12, 12, 12]`, 12px mute label "polling loop: 12% forever" above it near x=45s.
- **Interrupt line:** green `#008300` 3px line at 0% with triangular spikes to 3% at seconds `[10, 24, 41]` (each spike 1 s wide), points `[0, 0, 3, 0, 0, 3, 0, 0, 3, 0, 0]` at seconds `[0, 9.5, 10, 10.5, 23.5, 24, 24.5, 40.5, 41, 41.5, 60]`.
- **Event markers:** small blue `#2a78d6` filled circles on the baseline at seconds `[10, 24, 41]` with 11px blue "key" labels below.
- **Annotation (bold 13px green `#008300`, near x=25s, y=90):** "sleeps between events — work only when tapped".
- **Caption (12px `#444`, bottom right):** "CPU percentages illustrative".

## When the Doorbell Rings Too Often

**Tags:** `common mistake` (red), `interrupt storm` (orange)

- **The confusion** — "interrupts are always better" — false: each ring has its own handling cost
- **The tap tax** — every interrupt forces a stop-save-handle-resume detour before real work continues
- **The storm** — at very high event rates the CPU spends more time answering rings than working
- **The crossover** — at 20 µs per ring, 2,500 events/s costs 5% CPU — matching a 5% polling loop
- **The hybrid** — busy systems switch to polling under load: one sweep collects many waiting events

*Example (italic):* At 10,000 events per second the doorbell approach burns 20% of the CPU on handler overhead, while one polling sweep per millisecond stays at a flat 5%.

**Common mistake:** Treating interrupts as free. Below ~2,500 events/s the doorbell wins; past it the flat-cost polling loop wins — high-throughput network drivers famously flip modes at exactly this kind of threshold.

### Visualization (canvas `c4`, 720×300)

Line chart of CPU overhead vs event rate: interrupt cost grows linearly with events while polling cost stays flat, crossing at 2,500 events per second.

- **Title (bold 15px, `#1a5276`, top center):** "The Crossover: Rings Get Expensive, Sweeps Stay Flat".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = events per second 0 to 10,000, 12px `#444` tick labels "0" / "2.5k" / "5k" / "7.5k" / "10k"; y = CPU overhead % 0 to 20, gridlines `#e5e9ef` at 5/10/15.
- **Interrupt line:** blue `#2a78d6` 3px line through event rates `[0, 2000, 4000, 6000, 8000, 10000]`, CPU `[0, 4, 8, 12, 16, 20]` (20 µs per event), 12px blue label "interrupts: 20 µs per ring" along the slope.
- **Polling line:** mute `#6b7280` 3px flat line through the same rates, CPU `[5, 5, 5, 5, 5, 5]`, 12px mute label "polling sweep: flat 5%" above it near x=7,000.
- **Crossover marker:** vertical dashed `#6b7280` (dash 4/3) line at 2,500 events/s up to the 5% intersection, filled violet `#4a3aa7` 6px circle at the crossing, bold 12px violet label "2,500/s — the lines trade places".
- **Zone labels:** bold 12px green `#008300` "doorbell wins" at x≈900, y≈100; bold 12px orange `#d95926` "polling wins" at x≈7,500, y≈150.
- **Annotation (bold 13px magenta `#d55181`, near x=5,000, y=55):** "busy systems poll on purpose".
- **Caption (12px `#444`, bottom right):** "20 µs handler cost and 5% sweep cost illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); shift arithmetic is exact for the stated interval (480 min / 2 min = 240 checks, 240 × 15 s = 60 min, avg wait = half the 2-min interval = 60 s); delivery counts, CPU percentages, the 20 µs handler cost, and the 2,500 events/s crossover are invented and labeled illustrative.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
