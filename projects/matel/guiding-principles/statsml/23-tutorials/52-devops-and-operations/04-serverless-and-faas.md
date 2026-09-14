# Serverless & FaaS

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Serverless & FaaS

**Subtitle:** You deploy a function, not a server — the platform runs it per event, scales from zero to thousands and back, and bills only for the milliseconds it actually runs

## The Thumbnail Maker With No Server

**Tags:** `core idea` (blue), `scale to zero` (green), `event-driven` (orange)

- **The job** — a photo-book site needs a thumbnail made every time a user uploads a photo
- **The deploy** — you upload one function, `make_thumbnail(event)`; there is no machine to rent or patch
- **The trigger** — each file upload is an event; the platform runs one copy of the function per event
- **The scaling** — a lunchtime burst of uploads spins up 12 parallel instances; by midnight there are 0
- **The bill** — you pay per invocation and per 100ms of runtime; six idle overnight hours cost exactly $0
- **"No servers"** — there are servers, of course; the patching, scaling, and capacity math moved to the provider

*Example (italic):* From midnight to 6am nobody uploads a photo, so zero instances exist and the meter reads zero — an always-on server would have billed those six hours anyway.

**Key point:** Serverless means the unit you deploy is a function and the unit you pay for is an invocation — scale-to-zero is the defining property that no always-on server can match.

### Visualization (canvas `c1`, 720×300)

Dual-line 24-hour timeline: uploads per minute (blue) and running function instances (green) over one day, both resting at zero overnight.

- **Title (bold 15px, `#1a5276`, top center):** "One Day of the Thumbnail Function: Instances Follow Traffic Down to Zero".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = hour of day 0 to 24, 12px `#444` tick labels every 4 hours ("0h" … "24h"); left y = uploads/min 0 to 250, gridlines `#e5e9ef` at 62/125/187.
- **Uploads line:** blue `#2a78d6` 3px line through hours `[0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24]`, uploads/min `[0, 0, 0, 0, 45, 110, 240, 150, 90, 60, 20, 3, 0]`.
- **Instances line:** green `#008300` 3px line, same hour grid, instances `[0, 0, 0, 0, 3, 6, 12, 8, 5, 3, 1, 1, 0]`, scaled so 12 instances plots at the height of 240 uploads/min (×20); 12px green right-edge label "instances (×20)".
- **Zero band:** light green fill `rgba(0,131,0,0.08)` rectangle over hours 0–6 at the bottom 30px of the plot, bold 12px green label "zero instances, zero cost" centered inside it.
- **Annotation (bold 13px blue `#2a78d6`, near hour 12, above the peak):** "lunch burst: 12 instances appear on their own".
- **Caption (12px `#444`, bottom right):** "traffic shape illustrative".

## The First Request After a Quiet Spell

**Tags:** `worked example` (blue), `cold start` (red)

- **The setup** — after 20 idle minutes the platform has torn the last instance down to save resources
- **The cold path** — the next request must provision a container (250ms), start the runtime (400ms), init your code (150ms)
- **Hand-check** — 250 + 400 + 150 = 800ms of setup before the 40ms handler even runs: 840ms total
- **The warm path** — the instance now stays up, so the next requests skip setup and take just the 40ms
- **The shape** — out of 200 morning requests, 188 finish under 150ms; the 12 that follow an idle gap take 600–1000ms
- **The mitigation** — a provisioned warm pool keeps instances loaded and kills the cold tail, at extra always-on cost

*Example (italic):* The 7:02am request lands on a cold platform and takes 840ms — 21× the 40ms a warm instance takes for the identical photo.

**Key point:** The latency histogram of a serverless function is bimodal — a fast warm peak and a slow cold tail — because the first request after idleness pays for building the runtime that later requests reuse.

### Visualization (canvas `c2`, 720×300)

Bimodal latency histogram of 200 requests: tall warm cluster on the left, small cold-start cluster far right, with an empty gap between.

- **Title (bold 15px, `#1a5276`, top center):** "200 Requests, Two Peaks: the Warm Path and the Cold-Start Tail".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = latency 0 to 1000ms, 12px `#444` tick labels every 200ms; y = request count 0 to 130, gridlines `#e5e9ef` at 32/65/97.
- **Warm bars (blue fill `rgba(42,120,214,0.35)`, 2px `#2a78d6` edge):** bins/counts hardcoded — 0–50ms: 120, 50–100ms: 60, 100–150ms: 8; bar width = 30px (50ms of axis).
- **Cold bars (red fill `rgba(231,76,60,0.30)`, 2px `#e74c3c` edge):** 600–700ms: 3, 700–800ms: 4, 800–900ms: 3, 900–1000ms: 2.
- **Labels:** bold 12px blue "188 warm requests" above the first cluster; bold 12px red "12 cold starts" above the right cluster.
- **Setup breakdown inset (12px `#444`, in the empty mid-gap around x=350ms, y=110):** three stacked lines "provision 250ms / runtime 400ms / init code 150ms" with a bold 13px `#1a5276` total line "= 800ms before the 40ms handler".
- **Caption (12px `#444`, bottom right):** "counts illustrative, setup arithmetic exact".

## When Pay-Per-Call Beats Always-On — and When It Doesn't

**Tags:** `where it's used` (blue), `cost crossover` (orange)

- **Shines** — spiky, event-driven, embarrassingly parallel glue: thumbnails, webhook handlers, ETL triggers
- **The prices (illustrative)** — $0.20 per million invocations plus $16 per million GB-seconds; a small server $36/month
- **Per call** — 256MB × 400ms = 0.1 GB-s, so 0.1 × $0.000016 + $0.0000002 = $0.0000018, i.e. $1.80 per million calls
- **The crossover** — $36 ÷ $1.80/M = 20 million calls/month; below that serverless wins, above it the server wins
- **The extremes** — at 1M calls/month serverless costs $1.80 (20× cheaper); at 100M it costs $180 (5× dearer)
- **Fights you** — long jobs (hard execution time limits), stateful services, and latency-critical paths that can't absorb cold starts

*Example (italic):* The thumbnail function handles 1M uploads a month for $1.80; a checkout API doing 100M calls a month would pay $180 on serverless versus $36 always-on.

**Key point:** Per-invocation pricing is a straight line through zero and always-on pricing is a flat line — spiky low-volume work lives left of the crossover, sustained high volume lives right of it.

### Visualization (canvas `c3`, 720×300)

Line chart of monthly cost vs monthly invocations: flat always-on server line vs linear serverless line, crossing at 20M calls.

- **Title (bold 15px, `#1a5276`, top center):** "Monthly Cost: Serverless Line Crosses the $36 Server at 20M Calls".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = invocations/month 0 to 100M, 12px `#444` tick labels every 20M ("0", "20M" … "100M"); y = $/month 0 to 200, gridlines `#e5e9ef` at $50/$100/$150 with left labels.
- **Server line:** orange `#d95926` 3px horizontal line at $36 across the full plot, 12px orange label "always-on server $36/mo" above its left end.
- **Serverless line:** blue `#2a78d6` 3px line through hardcoded points invocations `[0, 20, 40, 60, 80, 100]` (millions), cost `[0, 36, 72, 108, 144, 180]` — exactly $1.80 per million.
- **Crossover marker:** vertical dashed `#6b7280` (dash 4/3) line at x=20M up to $36, green `#008300` 6px dot at (20M, $36), bold 13px green label "crossover: 20M calls" beside it.
- **Region labels:** bold 12px blue "serverless cheaper" centered near (8M, $110 height in the left region — placed above the lines); bold 12px orange "server cheaper" near (75M, just below the serverless line).
- **Caption (12px `#444`, bottom right):** "prices illustrative, arithmetic exact".

## A Function Is Not a Tiny Server

**Tags:** `common mistake` (red), `idempotency` (orange)

- **The confusion** — writing a function that keeps counters, caches, or sessions in local memory
- **Why it breaks** — every invocation may land on a different instance, and instances vanish at scale-to-zero
- **The retry rule** — event sources retry on failure by design, so the same event can arrive twice
- **The mistake** — a non-idempotent handler: a retried "order paid" event charges the customer's card twice
- **The fix** — stateless functions, state in external stores, and an idempotency key checked before side effects

*Example (italic):* A queue redelivers order #4127 after a timeout; the naive function charges $59 twice, while the idempotent one sees the key already recorded and exits without acting.

**Common mistake:** Treating a function as a small always-on server. Instances are disposable and events are at-least-once — code that isn't stateless and idempotent turns built-in retries into double side effects.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: the same retried event through a non-idempotent handler (double charge) vs an idempotent one (second run no-ops).

- **Title (bold 15px, `#1a5276`, top center):** "The Same Event Delivered Twice: Idempotency Decides What Happens".
- **Row 1 (y=95), label 12px `#444` at x=20:** "naive"; blue `#2a78d6` rounded box at x=140 labeled "event: order #4127 ×2" (12px), 3px arrow to a red `#e74c3c` box at x=360 labeled "charge card, no key check", arrow to bold 12px red text at x=580 "✗ charged $59 twice".
- **Row 2 (y=205), label:** "idempotent"; blue box "event: order #4127 ×2", 3px arrow to a green `#008300` box at x=340 labeled "seen key 4127? skip : charge", arrow to bold 12px green text at x=580 "✓ charged $59 once".
- **Box style:** 160–190px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Retry badge:** small violet `#4a3aa7` 11px pill "queue retries after timeout" above the event box in each row.
- **Annotation (bold 13px orange `#d95926`, centered near y=270):** "retries are built into the platform — idempotency is your half of the contract".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); traffic shape and latency histogram counts are invented and labeled illustrative; the cold-start breakdown (250+400+150=800ms, 840ms vs 40ms = 21×) and the cost crossover ($0.20/M invocations + $16/M GB-s, 0.1 GB-s/call, $1.80/M calls, crossover at 20M vs a $36/mo server) are exact arithmetic on the stated illustrative prices.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
