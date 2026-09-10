# Exponential Distribution

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Exponential Distribution

**Subtitle:** The waiting time until the next event when events arrive at a steady rate — short waits are common, long waits are rare, and time already waited never changes the odds

## Waiting for the Next Customer

**Tags:** `core idea` (blue), `waiting time` (green), `gaps between events` (orange)

- **The shop** — a coffee shop notes the clock time of each of 12 customer arrivals over 24 minutes
- **The gaps** — the data is the 12 waits between arrivals: 0.4, 3.1, 0.9, 1.6, 5.8, 0.7 min and so on
- **The average** — the gaps sum to 23.7 minutes, so the average wait for the next customer is ≈ 2.0 min
- **The shape** — tiny gaps like 0.2 min are common; long ones like 5.8 min are rare but real
- **The name** — waits that pile up near zero and thin out like this follow the exponential distribution

*Example (italic):* Two customers arrived just 0.2 minutes apart, yet one gap stretched to 5.8 minutes — both from the same steady flow.

**Key point:** The exponential distribution describes the waiting time until the next event when events arrive at a steady average rate — here, about one customer every 2 minutes.

### Visualization (canvas `c1`, 720×300)

Arrival timeline (top) plus a bar strip of the same 12 gaps in observed order (bottom), showing many short gaps and a few long ones.

- **Title (bold 15px, `#1a5276`, top center):** "Twelve Gaps Between Customer Arrivals (24 minutes)".
- **Timeline (top):** horizontal 2px `#999` line at y=100 from x=60 to x=660 mapping 0–24 min; minute labels "0", "6", "12", "18", "24 min" 12px `#444` below the line at their positions; blue `#2a78d6` 6px arrival dots at cumulative times `[0.4, 3.5, 4.4, 6.0, 11.8, 12.5, 14.8, 15.0, 16.1, 20.3, 20.8, 23.7]`.
- **Annotations on timeline:** orange `#d95926` bold 12px "5.8 min gap" with a thin orange bracket spanning minutes 6.0 → 11.8 above the line; magenta `#d55181` bold 12px "0.2 min gap" with an arrow pointing at the dot pair 14.8 / 15.0.
- **Gap bars (bottom):** heading bold 12px `#444` "the same 12 gaps, in arrival order"; gaps `[0.4, 3.1, 0.9, 1.6, 5.8, 0.7, 2.3, 0.2, 1.1, 4.2, 0.5, 2.9]` (minutes); baseline y=265, chart height 100, y scale 0–6 min; 12 bars from x=60, width 42, spacing 8, fill `rgba(42,120,214,0.45)`.
- **Mean line:** dashed green `#008300` (dash 4/3) horizontal line at 2.0 min across the bar panel, green bold 12px label "average gap ≈ 2.0 min" at its right end.

## The Curve Behind the Gaps

**Tags:** `worked example` (blue), `rule of thumb` (green)

- **The rule** — with a 2-minute average, the chance of waiting longer than t minutes is e^(−t/2)
- **Check it** — longer than 2 min: 37%; longer than 4 min: 14%; longer than 6 min: 5%
- **Mode at zero** — the single most likely wait is "right away"; the curve is tallest at t = 0
- **Mean vs median** — the mean is 2.0 min but the median is only 1.4 min; the tail drags the mean up
- **One number** — the whole curve is set by one rate (here 0.5 arrivals per minute); mean = 1/rate

*Example (italic):* Out of 100 waits, expect about 37 to run longer than 2 minutes but only 5 to run longer than 6 minutes.

**Key point:** One parameter — the arrival rate — fixes everything: mean wait = 1/rate, and the chance of waiting past t is e^(−rate × t).

### Visualization (canvas `c2`, 720×300)

Dual panel: the exponential density curve with the tail beyond 4 minutes shaded (left), and survival bars "chance the wait exceeds t" (right), split by a vertical dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "Wait-Time Curve for a 2-Minute Average".
- **Left panel (density):** axis origin x=55, width 280, baseline y=245, chart height 185, x range 0–8 min, y scale 0–0.55; blue `#2a78d6` 3px curve through hardcoded points at t = 0, 0.5, 1, ... 8 with f(t) = `[0.500, 0.389, 0.303, 0.236, 0.184, 0.143, 0.112, 0.087, 0.068, 0.053, 0.041, 0.032, 0.025, 0.019, 0.015, 0.012, 0.009]`; area under the curve beyond t=4 filled `rgba(217,89,38,0.35)` with orange `#d95926` bold 12px annotation "14% wait > 4 min"; dashed green `#008300` vertical line at t=2 labeled "mean 2.0" (green bold 12px, upper position); dashed magenta `#d55181` vertical line at t=1.4 labeled "median 1.4" (magenta bold 12px, lower position so the two labels do not collide); x tick labels "0", "2", "4", "6", "8 min" 12px `#444`; caption 12px `#444` "density f(t) = 0.5·e^(−t/2)".
- **Right panel (survival bars):** axis origin x=400, width 280, same baseline/height, y scale 0–100%; six bars labeled ">0", ">1", ">2", ">3", ">4", ">6" (12px `#444` below) with values `[100, 61, 37, 22, 14, 5]` percent; fill `rgba(0,131,0,0.4)`; bold 12px green value labels "100%", "61%", "37%", "22%", "14%", "5%" above each bar; caption 12px `#444` "chance the wait exceeds t minutes".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.

## The Wait That Never Ages

**Tags:** `core idea` (blue), `memorylessness` (orange), `where it's used` (green)

- **The question** — you have already waited 3 minutes; what is the chance you wait 2 more?
- **The surprise** — it is 37%, exactly the same as for someone who just walked in
- **Memoryless** — the waiting done so far tells you nothing; the clock restarts every instant
- **The test** — after 6 empty minutes the chance of 2 more is still 37%; nothing is ever "due"
- **Why it matters** — queue models, reliability math, and churn gaps all lean on this restart property

*Example (italic):* The barista who says "a rush is due — it's been quiet for 6 minutes" is wrong if arrivals are exponential.

**Key point:** Exponential is the only continuous distribution with no memory: P(wait > s + t | already waited s) = P(wait > t), for any s.

### Visualization (canvas `c3`, 720×300)

Three-bar chart showing that the chance of waiting more than 2 further minutes is identical no matter how long you have already waited.

- **Title (bold 15px, `#1a5276`, top center):** "Chance of Waiting 2 More Minutes, by Time Already Waited".
- **Axes:** origin x=70, baseline y=240, chart height 170, y scale 0–100% with ticks "0", "50", "100%" 12px `#444` on the left.
- **Bars:** three bars, width 120, evenly spaced across width 560; categories "just walked in", "waited 3 min already", "waited 6 min already" (12px `#444` below, wrapped to two lines if needed); all three values 37; fills blue `rgba(42,120,214,0.55)`, aqua `rgba(25,158,112,0.55)`, violet `rgba(74,58,167,0.5)`; bold 13px value label "37%" above each bar in the bar's solid color.
- **Reference line:** dashed ink `#1a5276` (dash 4/3) horizontal line at 37% across the plot, ink bold 13px label "37% every time" at its right end.
- **Annotation (bold 13px green `#008300`, upper area):** "the wait doesn't age — memoryless".
- **Caption (12px `#444`, bottom center):** "P(wait > 2 more min), exponential with a 2-minute average".

## Not Every Wait Is Exponential

**Tags:** `common mistake` (red), `rule of thumb` (green)

- **The trap** — anything called a "waiting time" gets modeled as exponential by reflex
- **Scheduled events** — buses every 10 minutes give gaps bunched near 10, not piled at zero
- **The check** — plot the gaps: an exponential must be tallest at zero and only fall from there
- **Task times** — human task durations bump around a typical value; exponential fits them badly
- **Name mix-up** — exponential is the gap between events; Poisson is the count of events per window

*Example (italic):* A histogram of 100 bus-stop gaps peaks at 10 minutes — a dead giveaway the wait is scheduled, not exponential.

**Common mistake:** Fitting an exponential just because the data is a waiting time. If the gap histogram is not tallest at zero, the memoryless model is the wrong one.

### Visualization (canvas `c4`, 720×300)

Dual-panel histogram: gaps between coffee-shop walk-ins (left, tallest at zero) vs gaps between scheduled buses (right, bump at 10 minutes), split by a dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "Two Kinds of Waiting: Walk-ins vs a Bus Schedule (illustrative)".
- **Left panel (walk-in gaps):** counts `[39, 24, 15, 9, 6, 4, 2, 1]` for 1-minute bins labeled "0–1", "1–2", "2–3", "3–4", "4–5", "5–6", "6–7", "7–8" (11px `#444` below each bar); axis origin x=55, width 280, baseline y=240, chart height 170, y scale max 45; fill `rgba(42,120,214,0.45)`; green `#008300` bold 12px annotation "tallest at zero → exponential-like"; caption 12px `#444` "100 gaps between walk-ins (illustrative)".
- **Right panel (bus gaps):** counts `[2, 8, 24, 32, 22, 9, 3]` for 1-minute bins labeled "7", "8", "9", "10", "11", "12", "13" min (11px `#444`); axis origin x=400, width 280, same baseline/height, y scale max 35; fill `rgba(217,89,38,0.5)`; magenta `#d55181` bold 12px annotation "bump at 10 min → not exponential"; caption 12px `#444` "100 gaps between scheduled buses (illustrative)".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
