# Exponential Backoff & Retry Storms

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Exponential Backoff & Retry Storms

**Subtitle:** When a service stumbles, every client retrying at once can keep it down long after the original problem is fixed — the outage outlives its cause unless retries back off and spread out

## The Ten-Second Blip That Buried the Server

**Tags:** `core idea` (blue), `retry storm` (orange), `outage` (red)

- **The service** — an orders API normally handles 200 requests per second, with capacity for 500
- **The blip** — a 10-second network hiccup at 12:00:00 makes every request fail
- **The reflex** — every failed client retries every 2 seconds and keeps retrying until it succeeds
- **The pile-up** — stuck clients accumulate at 200/s; after 10 seconds, 2,000 clients are retrying
- **The math** — 2,000 clients firing every 2 seconds is 1,000 requests/s — 5× the normal load
- **The burial** — the network heals at 12:00:10, but 1,000 req/s hits a server built for 500

*Example (italic):* The cause of the outage lasted 10 seconds; the retry traffic it created kept the server down for the next hour.

**Key point:** A retry storm is when the recovery traffic from a failure is itself large enough to prevent recovery — the outage outlives its cause.

### Visualization (canvas `c1`, 720×300)

Line chart of incoming request rate during and after the 10-second blip: the rate ramps from 200/s to 1,000/s as stuck clients stack up, then stays pinned at 1,000/s far above capacity even after the network is fixed.

- **Title (bold 15px, `#1a5276`, top center):** "A 10-Second Blip Creates a 5× Storm That Never Lands".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = seconds after 12:00:00, 0 to 60, 12px `#444` tick labels every 10s; y = requests/s 0 to 1200, gridlines `#e5e9ef` at 300/600/900.
- **Blip band:** light red fill `rgba(231,76,60,0.08)` from x=0s to x=10s, 12px `#6b7280` label "network down" at its top.
- **Traffic line:** orange `#d95926` 3px line through seconds `[0, 2, 4, 6, 8, 10, 20, 30, 40, 50, 60]`, req/s `[200, 360, 520, 680, 840, 1000, 1000, 1000, 1000, 1000, 1000]` — a ramp during the blip, then a plateau.
- **Capacity line:** dashed `#2a78d6` (dash 6/4) 2px horizontal line at 500 req/s, 12px blue label "server capacity 500/s" at its left end.
- **Heal marker:** vertical dashed `#6b7280` (dash 4/3) line at x=10s, 12px `#6b7280` label "network fixed 12:00:10".
- **Annotation (bold 13px red `#e74c3c`, near x=35s, y=90):** "the blip is over — the storm is not".
- **Caption (12px `#444`, bottom right):** "request rates illustrative".

## Backing Off: 1s, 2s, 4s, 8s — Plus a Coin Flip

**Tags:** `worked example` (blue), `exponential backoff` (green), `jitter` (orange)

- **The schedule** — wait 1s before retry 1, then 2s, then 4s, then 8s: each failure doubles the wait
- **One client** — fails at 12:00:00, retries at 0:01, 0:03, 0:07, and 0:15 — fifteen seconds of patience
- **Past the blip** — the 4th retry at 0:15 lands after the 10-second outage is over, and succeeds
- **The flaw** — 2,000 clients on the same schedule still arrive as four synchronized 2,000-request walls
- **Full jitter** — each client waits a random 0–8s before retry 4, smearing the wall across the window
- **Hand-check** — 2,000 retries ÷ 8 seconds = 250 per second, comfortably under the 500/s capacity

*Example (italic):* Without jitter, second 15 delivers all 2,000 retries in one wall; with jitter the same 2,000 arrive at about 250 per second and every one succeeds.

**Key point:** Exponential backoff spaces retries out in time; jitter spaces clients apart from each other — you need both, or the herd just charges on a slower schedule.

### Visualization (canvas `c2`, 720×300)

Bar chart of retry arrivals per second after the 12:00:00 failure: synchronized backoff produces four 2,000-request spikes at 1s/3s/7s/15s, while backoff with full jitter arrives as a flat ~250/s stream under the capacity line.

- **Title (bold 15px, `#1a5276`, top center):** "Same 2,000 Clients: Four Walls Without Jitter, a Trickle With It".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = seconds after failure 0 to 16, 12px `#444` tick labels at 0/1/3/7/15; y = retry arrivals/s 0 to 2200, gridlines `#e5e9ef` at 500/1000/1500/2000.
- **No-jitter spikes:** orange `#d95926` solid bars (10px wide) of height 2000 at seconds `[1, 3, 7, 15]`, 11px orange value label "2,000" above each.
- **Jittered stream:** green `#008300` bars (fill `rgba(0,131,0,0.30)`, 2px `#008300` top edge) at each second across the 7–15s retry-4 jitter window, heights `[250, 245, 260, 250, 255, 240, 250, 255]` — flat around 250/s.
- **Capacity line:** dashed `#2a78d6` (dash 6/4) 2px horizontal line at 500, 12px blue label "capacity 500/s".
- **Annotation (bold 13px green `#008300`, near x=9s, y=80):** "jitter: 2,000 ÷ 8s ≈ 250/s — every retry fits".
- **Caption (12px `#444`, bottom right):** "spike timing exact (1s/2s/4s/8s schedule), jittered heights illustrative".

## Why One Hiccup Becomes a Cascade

**Tags:** `where it's used` (blue), `cascading failure` (red), `circuit breaker` (green)

- **Thundering herd** — anything waiting on one shared event (cache expiry, a restart) fires at the same instant
- **Amplification** — if the browser, API, and order service each try up to 3×, the database sees up to 27 tries per click
- **Retry budget** — cap retry traffic (e.g. at 10% of normal requests) so recovery load stays bounded
- **Circuit breaker** — after repeated failures, stop calling the dependency and send one probe now and then
- **Fail fast** — returning a quick error and retrying later beats hammering a service that is already struggling

*Example (italic):* One tap on "place order" turns into 27 database queries when three layers each independently try three times.

**Key point:** Retries multiply across layers, so a small slowdown deep in the stack can generate an avalanche above it — budgets and circuit breakers exist to cut the multiplication short.

### Visualization (canvas `c3`, 720×300)

Bar chart of retry amplification through a four-layer stack: one click becomes 3, then 9, then 27 attempts as each layer retries 3×.

- **Title (bold 15px, `#1a5276`, top center):** "3 Tries Per Layer: One Click Becomes 27 Database Hits".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = attempts 0 to 30, gridlines `#e5e9ef` at 10/20/30; four bars centered at x = 150, 300, 450, 600, each 70px wide, 12px `#444` labels beneath: "browser", "API gateway", "order service", "database".
- **Bars:** attempts `[1, 3, 9, 27]`; browser blue `#2a78d6`, API gateway aqua `#199e70`, order service yellow `#c98500`, database red `#e74c3c` (the alarm bar); 12px bold value label ("1", "3", "9", "27") above each bar in its own color.
- **Multiplier arrows:** 12px `#6b7280` "×3" labels between consecutive bars, just above bar tops.
- **Annotation (bold 13px red `#e74c3c`, upper left near y=70):** "3 × 3 × 3 = 27 tries from one click".
- **Caption (12px `#444`, bottom right):** "attempt counts exact for 3 tries per layer".

## Two Ways Retries Bite Back

**Tags:** `common mistake` (red), `idempotency` (orange)

- **The double charge** — a payment times out after the server already charged the card; the retry charges it again
- **Idempotency key** — send the same operation id with every retry so the server can answer "already done"
- **Safe vs not** — reads are harmless to retry; "charge card" and "create order" are not, unless keyed
- **Lockstep backoff** — backoff without jitter keeps all 2,000 clients perfectly synchronized
- **Still a storm** — the waves land farther apart, but each wave is still a 2,000-request wall

*Example (italic):* The customer saw one "payment failed" message on screen and two $50 charges on their statement.

**Common mistake:** Adding backoff and stopping there — without jitter the herd stays a herd on a slower schedule, and without idempotency keys every retried write risks doing the work twice.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: retrying a $50 charge without an idempotency key (card billed twice) vs with one (server replays the receipt, billed once).

- **Title (bold 15px, `#1a5276`, top center):** "Retrying a Payment: Why the Retry Must Carry a Key".
- **Row 1 (y=95), label 12px `#444` at x=20:** "no key"; blue `#2a78d6` rounded box at x=150 labeled "charge $50" (12px), 3px arrow to a yellow `#c98500` box at x=330 labeled "charged — reply lost, timeout", 3px arrow to a red `#e74c3c` box at x=545 labeled "retry charges again" with bold 12px red "✗ $100 billed".
- **Row 2 (y=205), label:** "key=ab12"; blue box at x=150 "charge $50, key ab12", arrow to yellow box at x=330 "charged — reply lost, timeout", arrow to a green `#008300` box at x=545 labeled "retry: key seen, replay receipt" with bold 12px green "✓ $50 once".
- **Box style:** 150–175px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(201,133,0,0.12)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=270):** "a retry must be safe before it can be frequent".
- **Caption (12px `#444`, bottom right):** "amounts illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the 200/s normal load, 500/s capacity, 10-second blip, 2,000 stuck clients, and 1,000/s storm rate are invented and labeled illustrative; the backoff schedule (retries at 1s/3s/7s/15s from delays 1-2-4-8), the jitter hand-check (2,000 ÷ 8s = 250/s), and the amplification counts (1/3/9/27 from 3 tries per layer) are exact arithmetic.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
