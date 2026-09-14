# Thread-Pool Starvation

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Thread-Pool Starvation

**Subtitle:** One slow dependency can freeze an entire service — the worker pool fills up with threads that are all doing nothing but waiting

## The Storefront Where Checkout Dies Too

**Tags:** `core idea` (blue), `blocking calls` (orange), `shared pool` (red)

- **The service** — a storefront runs one worker pool of 200 threads serving checkout, search, and recommendations
- **The pattern** — each request grabs a thread, calls a downstream service, and blocks until the reply arrives
- **Normal day** — recommendations answers in 50ms, so its 40 req/s hold only ~2 threads at any instant
- **The slowdown** — the recommendations backend degrades from 50ms to 10s per call; nothing else changes
- **The pile-up** — new recommendation requests keep arriving, each parking a thread for 10 seconds
- **The freeze** — within seconds nearly all 200 threads sit waiting on recommendations; checkout has no thread to run on

*Example (italic):* Five seconds after recommendations slows down, a shopper clicking "Pay now" times out — even though checkout's own payment backend is perfectly healthy.

**Key point:** Thread-pool starvation is when blocked threads waiting on one slow dependency consume the whole shared pool, so every endpoint — including ones that never touch that dependency — stops responding.

### Visualization (canvas `c1`, 720×300)

Stacked area chart of the 200-thread pool over 10 seconds: threads blocked on recommendations (red, growing) vs threads free for everything else (green, shrinking), slowdown at t=2s.

- **Title (bold 15px, `#1a5276`, top center):** "One Slow Dependency Eats the Whole 200-Thread Pool in ~5 Seconds".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = seconds 0 to 10 with 12px `#444` tick labels every 2s; y = threads 0 to 200, gridlines `#e5e9ef` at 50/100/150, top line at 200 labeled "pool size 200".
- **Blocked area (bottom):** red fill `rgba(231,76,60,0.25)` under a 3px `#e74c3c` line through seconds `[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]`, threads `[2, 2, 2, 42, 82, 122, 162, 194, 194, 194, 194]` — flat at 2, then climbing 40 threads/s after the slowdown at t=2, saturating near t=7.
- **Free area (top):** green fill `rgba(0,131,0,0.20)` between the blocked line and the constant total 200, 2px `#008300` upper edge (the ~6 threads serving checkout and search vanish into the red).
- **Slowdown marker:** vertical dashed `#6b7280` (dash 4/3) line at t=2, 12px `#6b7280` label "recommendations: 50ms → 10s" at its top.
- **Annotation (bold 13px red `#e74c3c`, near t=8, y=80):** "all threads waiting — none left to serve checkout".
- **Caption (12px `#444`, bottom right):** "thread counts illustrative; fill rate = 40 req/s".

## Little's Law: The Pool Fills at Rate × Latency

**Tags:** `worked example` (blue), `Little's Law` (green)

- **The law** — threads busy on a dependency L = arrival rate λ × time each call holds a thread W
- **Before** — recommendations: 40 req/s × 0.05s = 2 threads busy at any moment
- **After** — recommendations: 40 req/s × 10s = 400 threads demanded — twice the whole pool
- **The neighbors** — checkout: 25 req/s × 0.08s = 2 threads; search: 35 req/s × 0.12s = 4 threads
- **Hand-check** — normal load uses ~8 of 200 threads; after the slowdown the pool fills in (200 − 8) / 40 ≈ 5 seconds
- **The trap** — demand grows with latency without bound, so any fixed pool saturates eventually

*Example (italic):* The same 40 req/s that needed 2 threads at 50ms needs 400 threads at 10s — a 200× latency increase is a 200× thread demand increase.

**Key point:** L = λ × W means thread demand is proportional to downstream latency; when latency jumps 200×, so does the number of parked threads, and the fixed pool is the first thing to run out.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart: threads demanded per endpoint (Little's Law), with the recommendations-after bar blowing past a dashed 200-thread pool-limit line.

- **Title (bold 15px, `#1a5276`, top center):** "Threads Demanded = Rate × Latency (Pool Holds Only 200)".
- **Axis:** bars start at x=230 and extend right, scale 1.1px per thread, max width 440 (= 400 threads); vertical dashed `#6b7280` (dash 4/3) pool-limit line at x=450 (= 200 threads) with 12px `#6b7280` label "pool limit 200" at its top.
- **Rows (top to bottom at y = 70, 120, 170, 220), each with a left-aligned 12px `#444` label at x=20:**
  - "checkout — 25/s × 0.08s = 2": blue `#2a78d6` bar width 2, 11px value label "2" at bar end
  - "search — 35/s × 0.12s = 4": blue bar width 4, label "4"
  - "recommendations before — 40/s × 0.05s = 2": green `#008300` bar width 2, label "2"
  - "recommendations after — 40/s × 10s = 400": red `#e74c3c` bar width 440 crossing the dashed line, bold 12px red label "400 — 2× the entire pool"
- **Bar style:** 14px tall, healthy bars solid, all with 11px `#444` value labels just past the bar end.
- **Annotation (bold 13px violet `#4a3aa7`, right side near y=250):** "latency ×200 → thread demand ×200".
- **Caption (12px `#444`, bottom right):** "rates and latencies illustrative; arithmetic exact".

## Healthy Endpoints Fail Anyway

**Tags:** `why it matters` (blue), `collateral damage` (red)

- **The symptom** — checkout starts timing out even though its own payment backend answers in 80ms
- **The reason** — a checkout request needs a free thread before it can do anything, and there are none
- **The queue** — arriving requests wait in the acceptor queue until it overflows, then get rejected outright
- **The misdirection** — on-call stares at checkout dashboards; the culprit is a recommendations backend checkout never calls
- **The signature** — every endpoint degrades at once, at the same moment, with one dependency's latency spiked

*Example (italic):* At t=7s the pool saturates: checkout's success rate falls off a cliff from 100% to 0% while its payment dependency still shows a clean 80ms.

**Key point:** A shared thread pool couples the fates of unrelated endpoints — the outage spreads from the slow dependency to everything behind the same pool, which is why the whole service appears down.

### Visualization (canvas `c3`, 720×300)

Line chart of the same 10 seconds: percent of requests completing within 1s, per endpoint — recommendations drops at the slowdown (t=2), checkout drops later at pool exhaustion (t=7).

- **Title (bold 15px, `#1a5276`, top center):** "Checkout Never Calls Recommendations — It Dies 5 Seconds Later Anyway".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = seconds 0 to 10, 12px `#444` tick labels every 2s; y = "% completing within 1s" 0 to 100, gridlines `#e5e9ef` at 25/50/75.
- **Recommendations line:** orange `#d95926` 3px line through seconds `[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]`, percent `[100, 100, 100, 0, 0, 0, 0, 0, 0, 0, 0]` — cliff at t=2 when its latency jumps to 10s.
- **Checkout line:** blue `#2a78d6` 3px line through the same second grid, percent `[100, 100, 100, 100, 100, 100, 100, 40, 0, 0, 0]` — healthy until the pool saturates at t=7, then a cliff.
- **Markers:** vertical dashed `#6b7280` (dash 4/3) lines at t=2 ("dependency slows", 12px `#6b7280` label) and t=7 ("pool exhausted", 12px label).
- **Legend:** 12px labels near the lines — orange "recommendations endpoint" at (t≈4, y≈220), blue "checkout endpoint" at (t≈4, y≈75).
- **Annotation (bold 13px blue `#2a78d6`, near t=8, y=130):** "checkout's own backend: healthy the whole time".
- **Caption (12px `#444`, bottom right):** "success percentages illustrative".

## "Just Add More Threads" Is Not the Fix

**Tags:** `common mistake` (red), `bulkheads` (green), `timeouts` (orange)

- **The reflex** — double the pool to 400 threads; but 40 req/s × 10s demands exactly 400, so it saturates again
- **The math** — Little's Law has no upper bound: at 20s latency the demand is 800; a bigger pool only buys seconds
- **Bulkheads** — give each dependency its own small pool; cap recommendations at 20 threads, keep 180 for the rest
- **Timeouts** — a 2s cap on the downstream call bounds W, so demand caps at 40 × 2 = 80 instead of 400
- **The trade** — with both fixes, recommendation requests fail fast while checkout and search never notice

*Example (italic):* With a 20-thread bulkhead, the slow dependency fills its 20 slots and further recommendation calls are rejected in milliseconds — the other 180 threads keep serving checkout untouched.

**Common mistake:** Sizing the pool for the failure instead of isolating the failure. Capacity fights rate × latency and loses; bulkheads cap how many threads one dependency may hold, and timeouts cap how long it may hold them.

### Visualization (canvas `c4`, 720×300)

Two-row diagram: one shared pool (everything starved) vs bulkhead pools with timeouts (only the slow dependency degrades), drawn as labeled pool boxes.

- **Title (bold 15px, `#1a5276`, top center):** "Shared Pool vs Bulkheads: Contain the Damage to One Dependency".
- **Row 1 (y=95), label 12px `#444` at x=20:** "one shared pool"; a single wide rounded box at x=180, width 380, labeled "200 threads — 194 blocked on recommendations" (12px), fill `rgba(231,76,60,0.12)` with 2px `#e74c3c` border; bold 12px red "✗ checkout + search starved" at x=580.
- **Row 2 (y=205), label:** "bulkheads + 2s timeout"; three rounded boxes side by side — orange `#d95926` box at x=180, width 130, "recs: 20/20 full — fails fast"; green `#008300` box at x=330, width 130, "checkout: 2/90 busy"; green box at x=480, width 130, "search: 4/90 busy"; bold 12px green "✓" at x=630.
- **Box style:** 40px tall, 8px radius, fills `rgba(231,76,60,0.12)` / `rgba(217,89,38,0.15)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text centered.
- **Annotation (bold 13px orange `#d95926`, centered near y=270):** "the slow dependency loses its 20 threads — and nothing else".
- **Caption (12px `#444`, bottom right):** "pool splits illustrative (20 / 90 / 90 of 200)".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); request rates, latencies, and thread counts are invented and labeled illustrative; the Little's Law arithmetic (40 × 0.05 = 2, 40 × 10 = 400, 40 × 2 = 80, (200 − 8) / 40 ≈ 5s) is exact for those illustrative inputs and must match between text and charts.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
