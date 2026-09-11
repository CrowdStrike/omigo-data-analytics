# Sampling Theorem & Aliasing

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Sampling Theorem & Aliasing

**Subtitle:** When you snapshot a fast cycle too slowly, the samples trace a different, slower cycle — that's why wagon wheels spin backwards in old westerns

## The Wagon Wheel That Spins Backwards

**Tags:** `core idea` (blue), `snapshots of motion` (green), `aliasing` (orange)

- **The movie** — film is just snapshots: an old western camera grabs 24 still frames every second
- **The wheel** — a wagon wheel spinning forward at 23 revolutions per second, filmed at 24 frames/s
- **Per frame** — between snapshots the wheel turns 23/24 of a turn: +345°, just shy of a full circle
- **What you see** — your eye picks the smallest motion between frames: the spoke slipping back 15°
- **The illusion** — 15° backward per frame × 24 frames/s = a wheel calmly rolling backward at 1 rev/s

*Example (italic):* The stagecoach races forward across the screen while its wheels appear to rotate slowly in reverse — the film never lied, it just sampled too slowly.

**Key point:** Snapshots can't tell "+345° forward" from "−15° backward" — the samples are identical. A too-slow sampler replaces the true fast motion with a slower impostor: an alias.

### Visualization (canvas `c1`, 720×300)

Film-strip of six wheel frames: each wheel drawn as a circle with one bold marked spoke, the spoke stepping backward 15° per frame while labels state the true +345° rotation.

- **Title (bold 15px, `#1a5276`, top center):** "A Wheel at 23 rev/s Filmed at 24 Frames/s".
- **Wheels:** six circles, radius 38, 2px `#6b7280` rims, centers at y=150 and x = 90, 200, 310, 420, 530, 640; four thin 1px `#e5e9ef` spokes plus ONE bold 3.5px blue `#2a78d6` marked spoke per wheel.
- **Marked spoke angles (from 12 o'clock, clockwise positive):** frames 1–6 at 0°, −15°, −30°, −45°, −60°, −75° (the apparent backward step).
- **Frame labels:** "frame 1" … "frame 6", 12px `#444`, centered below each wheel at y=210.
- **True-rotation labels:** "+345°" in 11px `#6b7280` above each of frames 2–6 (y=98).
- **Annotations:** green `#008300` bold 13px top-left (y=52): "true motion: +345° per frame (forward)"; magenta `#d55181` bold 13px below it (y=68): "what you see: −15° per frame = 1 rev/s backward".
- **Caption (12px `#444`, bottom center y=250):** "your eye picks the smallest motion between snapshots — the samples can't tell the difference".

## Three Wheel Speeds, One Camera

**Tags:** `worked example` (blue), `Nyquist limit` (green)

- **Rule** — apparent speed = true speed minus the nearest multiple of 24 (the frame rate)
- **23 rev/s** — 23 − 24 = −1: the wheel looks like it rolls backward at 1 rev/s
- **24 rev/s** — 24 − 24 = 0: one exact turn per frame, so the wheel looks frozen
- **25 rev/s** — 25 − 24 = +1: it looks like a slow forward crawl at 1 rev/s
- **The limit** — a 24 frames/s camera can only show speeds up to 24/2 = 12 rev/s honestly
- **The theorem** — Nyquist–Shannon: to keep a cycle real you need more than 2 samples per cycle

*Example (italic):* Speed the wagon from 23 to 25 rev/s and on film the wheel goes backward 1 rev/s, freezes, then creeps forward 1 rev/s — a 2 rev/s change looks like a direction flip.

**Key point:** Every true speed folds back into the −12…+12 rev/s band the camera can express. Above half the frame rate — the Nyquist limit — the on-screen speed is an alias, not the truth.

### Visualization (canvas `c2`, 720×300)

Folding diagram: apparent speed (y) versus true speed (x) for a 24 frames/s camera — a zigzag that climbs to +12, jumps to −12, and repeats, with the 23/24/25 rev/s cases marked.

- **Title (bold 15px, `#1a5276`, top center):** "Apparent Speed vs True Speed (camera at 24 frames/s)".
- **Axes:** x from 0 to 48 rev/s mapped to x=60…680; y from −12 to +12 rev/s mapped to y=255…55; zero line 1px `#e5e9ef` at y=155; x ticks at 0, 12, 24, 36, 48 with 12px `#444` labels; y labels "+12", "0", "−12" 12px `#444` left of x=60.
- **Zigzag (blue `#2a78d6`, 3px):** segments (0,0)→(12,+12); (12,−12)→(36,+12); (36,−12)→(48,0); vertical jumps at x=12 and x=36 drawn dashed 1.5px `#bdc3c7` (dash 4/3).
- **Nyquist band:** dashed 1.5px `#c98500` horizontal lines at y=+12 and y=−12; yellow `#c98500` bold 12px label "Nyquist limit: ±12 rev/s" near the top line, right side.
- **Marked points (6px dots + bold 12px labels):** magenta `#d55181` at (23,−1) labeled "23 → −1 (backward)"; green `#008300` at (24,0) labeled "24 → frozen"; orange `#d95926` at (25,+1) labeled "25 → +1 (forward)"; stagger labels so none overlap.
- **Caption (12px `#444`, bottom center y=288):** "every speed above 12 rev/s folds back into the band the camera can show".

## The Same Trap in a Metrics Dashboard

**Tags:** `where it's used` (blue), `time series` (orange), `failure mode` (red)

- **The signal** — a server's CPU load swings once every 24 hours: busy days, quiet nights
- **The sampler** — a monitoring job polls every 20 hours to "save cost" (illustrative)
- **The alias** — apparent frequency = |1/24 − 1/20| = 1/120: a stately fake 5-day cycle
- **The fix** — poll more often than every 12 hours: over 2 samples per 24-hour cycle keeps it real
- **Same everywhere** — weekly seasonality probed every 8 days, or 50 Hz hum in slow sensor logs

*Example (italic):* The on-call team spent a week hunting a mysterious "5-day CPU rhythm" that was nothing but the ordinary daily cycle beating against a 20-hour polling job.

**Key point:** An alias is not noise — it is a clean, convincing, completely fictional cycle. No statistical test on the sampled data can unmask it; only sampling faster (or filtering before sampling) can.

### Visualization (canvas `c3`, 720×300)

Line chart over 240 hours: the true 24-hour CPU sine in thin blue, 13 magenta sample dots taken every 20 hours, and a dashed magenta alias curve with a 120-hour period passing exactly through the dots.

- **Title (bold 15px, `#1a5276`, top center):** "Daily CPU Cycle Polled Every 20 Hours Fakes a 5-Day Wave".
- **Axes:** x = hours 0…240 mapped to x=55…685; y midline at y=160, amplitude 75px (curve spans y=85…235); baseline 1px `#e5e9ef` at y=160; x ticks every 48 h labeled "day 0", "day 2", "day 4", "day 6", "day 8", "day 10" (12px `#444`, y=262).
- **True curve (blue `#2a78d6`, 1.5px):** y = sin(2π·t/24) plotted for t = 0…240 step 0.5.
- **Sample dots (magenta `#d55181`, 6px):** at t = 0, 20, 40, 60, 80, 100, 120, 140, 160, 180, 200, 220, 240, value sin(2π·t/24) — i.e. 0, −0.866, −0.866, 0, 0.866, 0.866, 0, −0.866, −0.866, 0, 0.866, 0.866, 0.
- **Alias curve (magenta `#d55181`, 2.5px, dash 6/4):** y = −sin(2π·t/120) for t = 0…240 step 0.5 — passes through every sample dot.
- **Annotations:** blue bold 13px near an early peak (t≈6): "true 24-hour cycle"; magenta bold 13px near the alias trough around t≈30, two lines: "13 samples trace" / "a fake 5-day wave".
- **Caption (12px `#444`, bottom center y=288):** "polling every 20 h < required 12 h — apparent frequency |1/24 − 1/20| = 1/120 (illustrative)".

## "Exactly 2× Is Enough" and Other Traps

**Tags:** `common mistake` (red), `rule of thumb` (green)

- **Strictly more** — the theorem says MORE than 2 samples per cycle; exactly 2 sits on a knife edge
- **Knife edge** — sample a daily sine exactly twice a day at its zero crossings and you record all zeros
- **Comfortable rule** — engineers use 5–10 samples per cycle so the shape survives, not just the frequency
- **No rescue** — averaging or smoothing the sampled series afterwards cannot un-fold an alias
- **Anti-alias** — the real fix is upstream: filter or aggregate the signal before you downsample it

*Example (italic):* A pipeline downsampled hourly data to one reading per 1.25 days; the 1-cycle-per-day signal reappeared as a slow 5-day wave that survived every smoothing pass thrown at it.

**Common mistake:** Believing a downstream fix exists. Once the fast cycle has folded into a slow impostor, the sampled data is permanently ambiguous — the information was lost at capture time.

### Visualization (canvas `c4`, 720×300)

Three side-by-side panels showing the same daily sine sampled at 8/day (shape kept), exactly 2/day (flatlines), and 0.8/day (fake slow wave).

- **Title (bold 15px, `#1a5276`, top center):** "One Cycle-per-Day Signal, Three Sampling Rates".
- **Panels:** widths 190px at x-origins 50, 285, 520; each midline y=165, amplitude 55px; thin 1px `#e5e9ef` midline; panel headings bold 12px `#444` at y=60: "8 samples/day", "exactly 2/day", "0.8 samples/day".
- **True curve (all panels, blue `#2a78d6`, 1.5px):** y = sin(2π·t) for t = 0…5 days mapped across each panel width, step 0.02.
- **Panel A dots (green `#008300`, 4px):** t = 0, 0.125, 0.25, … 5.0 (step 0.125, 41 dots) at sin(2π·t); green bold 12px label "shape kept" at y=250.
- **Panel B dots (yellow `#c98500`, 5px):** t = 0, 0.5, 1.0, … 5.0 (step 0.5, 11 dots) — sin(2π·t) = 0 at every one, so all dots sit on the midline; dashed 2px `#c98500` line through them; yellow bold 12px label "reads as flat zero" at y=250.
- **Panel C dots (magenta `#d55181`, 6px):** t = 0, 1.25, 2.5, 3.75, 5.0 at sin(2π·t) = 0, 1, 0, −1, 0; magenta 2.5px dash 6/4 alias curve y = sin(2π·0.2·t) through them; magenta bold 12px label "fake 5-day wave" at y=250.
- **Dividers:** dashed `#bdc3c7` (dash 4/3) vertical lines at x=262 and x=497 from y=40 to y=270.
- **Caption (12px `#444`, bottom center y=290):** "same blue signal in all three panels — only the sampling rate changes".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
