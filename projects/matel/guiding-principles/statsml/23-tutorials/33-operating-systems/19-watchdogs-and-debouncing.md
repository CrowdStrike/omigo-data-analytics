# Watchdogs & Debouncing

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Watchdogs & Debouncing

**Subtitle:** Embedded code's two defensive reflexes — distrust the button (it rattles) and distrust yourself (you might freeze), so filter the input and let a hardware timer reboot you

## The Espresso Machine That Guards Itself

**Tags:** `core idea` (blue), `embedded` (green), `defensive reflexes` (orange)

- **The machine** — a café espresso machine runs one small program on a chip: no keyboard, no reboot button
- **The twitch** — pressing brew once makes the metal contacts rattle: 7 electrical spikes in about 8 ms
- **Debouncing** — the firmware ignores the rattle and waits for the signal to hold steady before counting
- **The freeze** — a bug can hang the brew loop mid-pour, and no one is there to press ctrl-alt-delete
- **The watchdog** — a hardware countdown reboots the chip unless the code checks in ("pets it") on schedule

*Example (italic):* The barista presses brew once; the machine registers exactly one press, and if the firmware ever locks up mid-shot, the chip reboots itself within half a second.

**Key point:** Debouncing collapses a noisy input into one clean event; a watchdog reboots code that stops proving it is alive — the two reflexes that let unattended devices run for years.

### Visualization (canvas `c1`, 720×300)

Two-row flow diagram: the debounce path (rattling button in, one clean press out) above the watchdog path (missed pets in, hardware reset out).

- **Title (bold 15px, `#1a5276`, top center):** "Two Reflexes: Filter the Twitchy Input, Reboot the Frozen Brain".
- **Row 1 (y=95), label 12px `#444` at x=20:** "debounce"; blue `#2a78d6` rounded box at x=120 (width 170) labeled "brew button: 7 spikes in 8 ms", 3px arrow to an aqua `#199e70` box at x=340 (width 180) labeled "hold steady for 20 ms?", 3px arrow to a green `#008300` box at x=570 (width 120) labeled "1 clean press".
- **Row 2 (y=205), label:** "watchdog"; blue box at x=120 labeled "loop pets timer every 100 ms", arrow to a red `#e74c3c` box at x=340 labeled "hang: 500 ms of silence", arrow to an orange `#d95926` box at x=570 labeled "chip resets".
- **Box style:** 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(25,158,112,0.12)` / `rgba(0,131,0,0.12)` / `rgba(231,76,60,0.12)` / `rgba(217,89,38,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=270):** "the hardware assumes the button and the code will both misbehave".
- **Caption (12px `#444`, bottom right):** "timings illustrative".

## Counting One Press, Not Seven

**Tags:** `worked example` (blue), `debouncing` (green), `sampling` (orange)

- **The rattle** — one press produces 7 spikes as the contacts bounce for roughly 8 ms before settling
- **The sampler** — the firmware reads the button pin every 5 ms instead of reacting to every edge
- **The reads** — the nine samples from 0 to 40 ms come out 0, 1, 0, 1, 1, 1, 1, 1, 1
- **The rule** — a press counts only after 4 identical reads in a row: 4 × 5 ms = 20 ms of steady signal
- **Hand-check** — the run of four 1s completes at the 30 ms sample, so exactly one press is registered

*Example (italic):* The finger lands at 3 ms; the bouncing spikes corrupt the 10 ms sample, but from 15 ms on every read is 1, and the press is accepted at 30 ms.

**Key point:** Debouncing trades a tiny delay — 30 ms, invisible to a human — for certainty: every rattle, however messy, collapses into exactly one event.

### Visualization (canvas `c2`, 720×300)

Two-lane logic trace on a shared 0–40 ms axis: the raw bouncing pin signal on top, the debounced output below, with sample dots and their read values between them.

- **Title (bold 15px, `#1a5276`, top center):** "One Press, Seven Spikes: Sampling Every 5 ms Accepts It Once".
- **Axes:** origin x=60, plot width 600 (0–40 ms maps to x=60–660); baseline y=260 with 12px `#444` tick labels every 10 ms; raw lane high y=95 / low y=135, debounced lane high y=185 / low y=225; 12px `#6b7280` lane labels at x=15: "raw pin" (y≈115), "debounced" (y≈205).
- **Raw trace:** blue `#2a78d6` 2px step trace: low from 0 to 3 ms, then alternating high/low with transitions at ms `[3, 3.8, 4.6, 5.2, 6.0, 6.6, 7.4, 8.0, 8.8, 9.4, 10.2, 10.8, 11.4]`, high from 11.4 to 40 ms — seven high pulses in total.
- **Sample markers:** mute `#6b7280` 4px dots on the raw trace at ms `[0, 5, 10, 15, 20, 25, 30, 35, 40]`, with 11px `#6b7280` read labels just below the raw lane: `0, 1, 0, 1, 1, 1, 1, 1, 1`.
- **Debounced trace:** green `#008300` 3px step: low until 30 ms, rising to high at 30 ms.
- **Accept marker:** vertical dashed `#6b7280` (dash 4/3) line at 30 ms spanning both lanes.
- **Annotation (bold 12px green `#008300`, near x=31 ms, above the debounced high level):** "4 matching reads → accepted at 30 ms".
- **Annotation (bold 12px magenta `#d55181`, above the bounce region near 7 ms):** "contact bounce".
- **Caption (12px `#444`, bottom right):** "bounce pattern illustrative".

## Bounce in the Logs, Silence in the Pipeline

**Tags:** `where it's used` (blue), `data quality` (green), `heartbeats` (orange)

- **The log** — the machine uploads raw pin events; one month of data shows 4,200 brew-button events
- **The truth** — the debounced firmware counted 600 real presses; the other 3,600 rows are contact bounce
- **The habit** — search boxes debounce keystrokes the same way: fire the query only after typing pauses
- **The heartbeat** — a data pipeline is watchdogged too: no file arrives for 30 minutes → page someone
- **The rule** — any counter fed by a physical or flaky source needs a debounce step before analysis

*Example (italic):* An analyst who charts raw button events reports a 7× demand spike that is really loose metal contacts, not thirsty customers.

**Key point:** Debouncing and watchdogs are not just firmware tricks — deduplication windows and heartbeat alerts are the same reflexes applied to logs and pipelines.

### Visualization (canvas `c3`, 720×300)

Grouped bar chart: raw event count vs debounced count for three inputs on the machine, showing how much of the raw log is bounce.

- **Title (bold 15px, `#1a5276`, top center):** "Raw Event Counts vs Debounced Truth (one month)".
- **Axes:** origin x=70, baseline y=245, plot width 590, plot height 180; y = events 0 to 4,500, gridlines `#e5e9ef` at 1,000 / 2,000 / 3,000 / 4,000 with 12px `#444` labels.
- **Groups (centers at x = 180, 375, 570), 12px `#444` labels under the baseline:** "brew button", "door sensor", "keypad"; each group has two 55px-wide bars with an 8px gap.
- **Raw bars (left of pair):** fill `rgba(42,120,214,0.30)`, 2px `#2a78d6` border, heights for values `[4200, 1800, 960]`.
- **Debounced bars (right of pair):** solid green `#008300`, values `[600, 450, 320]`.
- **Value labels:** 12px `#2c3e50` above every bar (4,200 / 600 / 1,800 / 450 / 960 / 320).
- **Annotation (bold 13px magenta `#d55181`, near x=430, y=75):** "7× inflation on the brew button — dedupe before you count".
- **Caption (12px `#444`, bottom right):** "event counts illustrative".

## Petting the Watchdog From an Interrupt

**Tags:** `common mistake` (red), `watchdog` (orange), `interrupts` (blue)

- **The contract** — a pet must be proof of real progress: "the brew loop just finished a healthy pass"
- **The shortcut** — a tempting bug: pet the watchdog from a hardware timer interrupt every 100 ms
- **The betrayal** — the timer interrupt keeps firing even while the main loop is completely frozen
- **The result** — the watchdog is fed forever, the reset never comes, and the machine hangs mid-pour
- **The fix** — pet only at the end of a full main-loop pass, so a hang stops the pets within 100 ms

*Example (italic):* The brew loop hangs at 600 ms; a main-loop pet goes silent and the chip resets at 1,100 ms, but an interrupt-driven pet keeps the frozen machine "alive" forever.

**Common mistake:** Wiring the watchdog pet into a timer interrupt. The timer ticks whether or not the program works, so the watchdog ends up guarding the clock instead of the code.

### Visualization (canvas `c4`, 720×300)

Two-lane timeline on a shared 0–1,200 ms axis: pet tick marks from the main loop (they stop at the hang, reset fires) vs from a timer interrupt (they never stop, no reset).

- **Title (bold 15px, `#1a5276`, top center):** "Where the Pet Comes From Decides Whether the Reset Ever Fires".
- **Axis:** origin x=60, plot width 600 (0–1,200 ms maps to x=60–660); baseline y=250 with 12px `#444` tick labels every 300 ms; lane baselines y=110 (row 1) and y=210 (row 2), 2px `#e5e9ef` lane lines, 12px `#444` lane labels at x=15: "pet from main loop", "pet from interrupt".
- **Hang marker:** vertical dashed `#6b7280` (dash 4/3) line at 600 ms spanning both lanes, 12px `#6b7280` label "loop hangs at 600 ms" at its top.
- **Row 1 pets:** green `#008300` 3px vertical ticks (16px tall) at ms `[0, 100, 200, 300, 400, 500, 600]`, nothing after; orange `#d95926` filled marker and short vertical line at 1,100 ms with bold 12px green `#008300` label "reset at 1,100 ms — machine recovers".
- **Row 2 pets:** green 3px ticks at every 100 ms from 0 to 1,200 (thirteen ticks), continuing straight through the hang; bold 12px red `#e74c3c` annotation near x=900, above the lane: "pets never stop — frozen machine never reboots".
- **Annotation (bold 13px orange `#d95926`, centered near y=280):** "a pet must prove progress, not just that a timer ticks".
- **Caption (12px `#444`, bottom right):** "timings illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the bounce transition times, sample reads (0,1,0,1,1,1,1,1,1), monthly event counts (4,200/600, 1,800/450, 960/320), and watchdog timings (100 ms pets, 500 ms timeout, hang at 600 ms) are invented and labeled illustrative; the arithmetic is exact (4 × 5 ms = 20 ms, four 1s complete at 30 ms, 600 + 500 = 1,100 ms reset).
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
