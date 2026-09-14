# The M/M/1 Utilization Curve

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** The M/M/1 Utilization Curve

**Subtitle:** Why systems melt at 80% busy, not 100% — in a single-server queue the average wait grows like ρ/(1−ρ), and that curve is a cliff, not a ramp

## The Server That Slows Down Before It's Full

**Tags:** `core idea` (blue), `queueing` (green), `latency` (orange)

- **The server** — one API server handles requests that each take 100ms of work on average
- **Random arrivals** — requests arrive at random moments, not on a schedule; sometimes they bunch up
- **The bunch** — when two arrive 20ms apart, the second waits 80ms while doing nothing wrong
- **The surprise** — at 90% busy, the average request spends 1000ms in a system whose work takes 100ms
- **The name** — this setup (random arrivals, random service times, one server) is the M/M/1 queue
- **The formula** — average time in system W = 1/(μ−λ): capacity rate minus arrival rate, inverted

*Example (italic):* At λ = 9 requests/s against capacity μ = 10/s, W = 1/(10−9) = 1 second — and 900ms of it is pure waiting in line.

**Key point:** Latency does not grow in proportion to load — it grows like 1/(1−ρ), flat for a long time and then exploding as utilization ρ approaches 100%.

### Visualization (canvas `c1`, 720×300)

Hockey-stick line chart of average latency W = 100ms/(1−ρ) against utilization ρ from 0 to 95%, showing the flat region and the cliff.

- **Title (bold 15px, `#1a5276`, top center):** "Average Latency vs Utilization: Flat, Flat, Flat — Then a Cliff".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = utilization 0% to 100% with 12px `#444` tick labels every 20%; y = average latency 0 to 2000ms, gridlines `#e5e9ef` at 500/1000/1500 with 12px `#444` labels "0.5s"/"1s"/"1.5s".
- **Curve:** blue `#2a78d6` 3px line through utilization `[0, 10, 20, 30, 40, 50, 60, 70, 80, 85, 90, 92, 94, 95]` (%), latency `[100, 111, 125, 143, 167, 200, 250, 333, 500, 667, 1000, 1250, 1667, 2000]` (ms) — exact W = 100/(1−ρ).
- **Marker line:** vertical dashed `#6b7280` (dash 4/3) at 80%, 12px `#6b7280` label "80%: already 5× the work time" near its top.
- **Dots:** 4px filled circles at (50%, 200), (80%, 500), (90%, 1000) in `#1a5276`.
- **Annotation (bold 13px red `#e74c3c`, near x=88%, y=70):** "the last 15% costs more than the first 80%".
- **Caption (12px `#444`, bottom right):** "exact for M/M/1: W = 100ms / (1−ρ)".

## Five Load Levels, Worked by Hand

**Tags:** `worked example` (blue), `exact numbers` (green)

- **The capacity** — μ = 10 requests/second (each takes 100ms), so utilization is ρ = λ/10
- **At 50%** — λ=5/s: W = 1/(10−5) = 200ms — 100ms of work plus a wait of one service time
- **At 80%** — λ=8/s: W = 1/(10−8) = 500ms — the wait alone is 400ms, 4× the service time
- **At 90% and 95%** — λ=9/s gives W = 1 s; λ=9.5/s gives W = 2 s
- **At 99%** — λ=9.9/s: W = 1/(10−9.9) = 10 seconds — a 100ms task now takes 10,000ms
- **The pattern** — the wait is ρ/(1−ρ) service times: 1×, 4×, 9×, 19×, 99× at 50/80/90/95/99%

*Example (italic):* Going from 80% to 90% adds just one extra request per second — and doubles the average latency from 500ms to 1 second.

**Key point:** Every halving of the idle fraction (1−ρ) doubles the latency — 80% to 90% doubles it, 90% to 95% doubles it again, 95% to 97.5% again.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart of average latency at the five utilization levels, log-feel widths so the 10-second bar fits beside the 200ms bar.

- **Title (bold 15px, `#1a5276`, top center):** "Same Server, Same 100ms of Work — Five Arrival Rates".
- **Axis:** vertical 2px `#999` baseline at x=200, bars extend right, max width 460; log-feel achieved by hardcoded pixel widths, not a real log axis.
- **Rows (top to bottom at y = 60, 105, 150, 195, 240), each with a left-aligned 12px `#444` label at x=20:**
  - "50% busy (λ=5/s)": green `#008300` bar width 70, 12px label "200ms" at bar end
  - "80% busy (λ=8/s)": blue `#2a78d6` bar width 160, label "500ms"
  - "90% busy (λ=9/s)": blue `#2a78d6` bar width 230, label "1s"
  - "95% busy (λ=9.5/s)": orange `#d95926` bar width 300, label "2s"
  - "99% busy (λ=9.9/s)": red `#e74c3c` bar width 460, bold 12px red label "10s — 99× the wait"
- **Bar style:** 16px tall, solid fill, 4px radius.
- **Annotation (bold 13px violet `#4a3aa7`, right side near y=32):** "W = 1/(μ−λ), exact".
- **Caption (12px `#444`, bottom right):** "millisecond values exact for M/M/1; bar widths schematic (log-feel)".

## Why Capacity Plans Stop at 60–70%

**Tags:** `where it's used` (blue), `capacity planning` (green), `headroom` (orange)

- **The target** — capacity plans hold steady-state utilization at 60–70%, not "as close to 100% as we can"
- **The reason** — at 65% latency is 286ms, and a 25% traffic spike (to 81%) only pushes it to 533ms
- **The cliff** — at 85% latency is already 667ms, and the same 25% spike lands at 106% — over capacity
- **Over capacity** — when λ > μ the queue never drains; latency grows without bound until something dies
- **The famous last words** — "the CPU is only at 85%, we're fine" reads a number hiding a 6.7× latency multiplier

*Example (italic):* Two teams see the same Tuesday spike; the one planned at 65% serves it at 533ms, the one at 85% is paging on-call within minutes.

**Key point:** The 30–40% idle capacity in a good plan is not waste — it is what keeps ordinary traffic spikes on the flat part of the curve instead of past the cliff.

### Visualization (canvas `c3`, 720×300)

Grouped bar chart: two planning targets (65% vs 85% steady state), each with a normal-day bar and a +25%-spike-day bar; the 85% spike bar runs off the chart.

- **Title (bold 15px, `#1a5276`, top center):** "The Same +25% Spike: Survivable at 65%, Fatal at 85%".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = average latency 0 to 1000ms, gridlines `#e5e9ef` at 250/500/750 with 12px `#444` labels.
- **Group 1 (centered x≈220), 12px `#444` label "planned at 65%" below baseline:** blue `#2a78d6` bar (width 60) to 286ms labeled "normal: 286ms"; orange `#d95926` bar (width 60, 12px gap) to 533ms labeled "spike → 81%: 533ms".
- **Group 2 (centered x≈480), label "planned at 85%":** blue bar to 667ms labeled "normal: 667ms"; red `#e74c3c` bar drawn to the top of the plot (y=65) with an upward arrowhead and bold 12px red label "spike → 106%: queue never drains".
- **Value labels:** 12px `#2c3e50` above each finite bar.
- **Annotation (bold 13px green `#008300`, near x=170, y=80):** "headroom keeps the spike on the flat part".
- **Caption (12px `#444`, bottom right):** "286 / 533 / 667 ms exact for M/M/1, 100ms service; λ > μ has no steady state".

## 85% Busy Is Not 15% Safe

**Tags:** `common mistake` (red), `variability` (orange)

- **The mistake** — reading utilization as linear headroom: "85% busy = 15% margin" — the curve says otherwise
- **Averages hide it** — 85% average over a minute can contain multi-second windows pinned above 100%
- **Variability hurts** — the 1×/4×/9× waits assume exponential service times; burstier ones wait longer
- **Pollaczek–Khinchine** — the wait scales with (1+C²)/2; C² = squared coefficient of variation of service times
- **At 90% busy** — constant 100ms services wait 450ms; exponential, 900ms; heavy-tailed (C²=4), 2250ms

*Example (italic):* One endpoint that occasionally runs a 2-second report raises C² for the whole server — everyone queued behind it waits longer, at every utilization.

**Common mistake:** Treating utilization as the health metric. Users feel latency, not utilization — and near the cliff, latency moves 10× while utilization moves 10 points.

### Visualization (canvas `c4`, 720×300)

Vertical bar chart: average queueing wait at the same 90% utilization for three service-time distributions — constant, exponential, heavy-tailed.

- **Title (bold 15px, `#1a5276`, top center):** "Same 90% Utilization, Three Different Waits".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = average wait 0 to 2400ms, gridlines `#e5e9ef` at 600/1200/1800 with 12px `#444` labels "0.6s"/"1.2s"/"1.8s".
- **Bars (width 110, centered at x = 180, 360, 540), each with a 12px `#444` two-line label below the baseline:**
  - "constant 100ms (C²=0)": green `#008300` bar to 450ms, 12px label "450ms" above
  - "exponential (C²=1) — M/M/1": blue `#2a78d6` bar to 900ms, label "900ms"
  - "heavy-tailed (C²=4)": red `#e74c3c` bar to 2250ms, bold 12px red label "2250ms — 5× the constant case"
- **Marker line:** horizontal dashed `#6b7280` (dash 4/3) at 900ms across the plot, 12px `#6b7280` label "the ρ/(1−ρ) textbook value" at its right end.
- **Annotation (bold 13px orange `#d95926`, near x=430, y=60):** "utilization sets the cliff; variability sets how hard you hit it".
- **Caption (12px `#444`, bottom right):** "exact Pollaczek–Khinchine waits at ρ = 0.90, mean service 100ms".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness). Latencies are exact for M/M/1 with mean service time 100ms (μ = 10/s): W = 1/(μ−λ), i.e. 200/500/1000/2000/10000 ms at ρ = 0.50/0.80/0.90/0.95/0.99, and 286/533/667 ms at ρ = 0.65/0.8125/0.85 (rounded to the millisecond). The c4 waits (450/900/2250 ms) are exact Pollaczek–Khinchine values at ρ = 0.90 for C² = 0/1/4. Only the c2 bar pixel widths are schematic (log-feel); their millisecond labels are exact.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
