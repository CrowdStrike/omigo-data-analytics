# Webhooks

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Webhooks

**Subtitle:** Instead of your code asking the payment provider "anything new?" every minute, you hand over a URL and the provider calls you the instant something happens — the API that calls you

## The Shop That Stopped Asking

**Tags:** `core idea` (blue), `push not pull` (green), `HTTP callback` (orange)

- **The shop** — an online coffee-bean shop takes card payments through a payment provider, PayLine
- **The old way** — the shop's server polls PayLine every 60 seconds: "any new charges?" — 1,440 requests a day
- **The waste** — on a slow day with 3 charges, 1,437 of those requests come back empty-handed
- **The lag** — a charge landing right after a poll sits unseen for up to 60 seconds, 30 on average
- **The webhook** — the shop registers `https://shop.example/webhook`; PayLine POSTs there when a charge succeeds
- **The flip** — now PayLine makes 3 requests that day, each arriving about 1 second after the charge

*Example (italic):* A customer pays at 2 min 24 s past the hour; the poller learns at the 3-minute mark, the webhook fires within a second.

**Key point:** A webhook is just an HTTP request in reverse — you give the other service a URL, and it becomes the client that calls your server whenever the event you care about occurs.

### Visualization (canvas `c1`, 720×300)

Two-lane timeline over a 10-minute window: a polling lane (11 evenly spaced polls, mostly empty) vs a webhook lane (3 deliveries hugging the 3 charge events).

- **Title (bold 15px, `#1a5276`, top center):** "Same Three Charges: Eleven Polls vs Three Webhook Calls".
- **Axes:** origin x=60, baseline y=250, plot width 600; x = minutes 0 to 10, 12px `#444` tick labels every 2 minutes; two horizontal lanes as 2px `#e5e9ef` lines at y=110 (label "polling, every 60s", 12px `#6b7280`, x=62 above lane) and y=200 (label "webhook", same style).
- **Charge events:** vertical dashed `#6b7280` (dash 4/3) lines from y=80 to y=230 at minutes `[2.4, 5.1, 8.7]`, 11px `#6b7280` label "charge" at the top of each.
- **Polling lane:** 11 circles (radius 6) at minutes `[0,1,2,3,4,5,6,7,8,9,10]`; empty polls stroked 2px `#6b7280` with white fill; the polls at minutes `[3, 6, 9]` filled blue `#2a78d6` (they finally see a charge).
- **Webhook lane:** 3 filled green `#008300` circles (radius 6) at minutes `[2.42, 5.12, 8.72]`, i.e. right on top of each charge line.
- **Annotation (bold 13px green `#008300`, near x=340, y=60):** "3 calls, ~1 s fresh — polling burns 11 calls and runs up to 60 s stale".
- **Caption (12px `#444`, bottom right):** "charge times illustrative".

## One Event's Delivery Diary

**Tags:** `worked example` (blue), `retries` (green), `idempotency` (orange)

- **The event** — at 14:02:05 a $18.50 charge succeeds; PayLine builds event `evt_9d41` with a JSON body
- **The signature** — PayLine adds header `X-Signature`: HMAC-SHA256 of the body using the shared secret
- **The bad luck** — the shop's server is mid-deploy and down for 40 minutes; attempt 1 gets no answer
- **The backoff** — PayLine retries at +1 min, +5 min, +30 min, +2 h — each gap roughly 5× the last
- **The success** — attempt 5 at 16:02 reaches the recovered server, which verifies the HMAC and returns 200
- **The dedupe** — the handler records `evt_9d41` in a processed-events table; a second copy is skipped

*Example (italic):* Attempts 1–4 fail during the 40-minute outage; attempt 5 at +2 h lands a 200, and the order ships exactly once.

**Key point:** A well-behaved webhook delivery is signed, retried on an exponential backoff schedule, and identified by an event id so the receiver can process duplicates exactly once.

### Visualization (canvas `c2`, 720×300)

Retry timeline for event `evt_9d41`: five delivery attempts on a schematic time axis, a shaded outage band covering the first four, failure marks then a success mark.

- **Title (bold 15px, `#1a5276`, top center):** "evt_9d41: Five Attempts, Exponential Backoff, One Success".
- **Axis:** 2px `#999` horizontal timeline at y=180 from x=60 to x=660; attempt positions hardcoded at x = `[90, 180, 285, 405, 560]` with 12px `#444` labels below at y=205: `"+0", "+1m", "+5m", "+30m", "+2h"` (spacing schematic, not to scale).
- **Outage band:** rectangle x=70 to x=440, y=120 to y=180, fill `rgba(217,89,38,0.12)`, 12px orange `#d95926` label "shop server down 40 min" centered at y=112.
- **Failed attempts:** bold 16px red `#e74c3c` "✗" at the first four positions (y=172), each with 11px `#6b7280` label "no 200" at y=150.
- **Success:** bold 16px green `#008300` "✓ 200" at x=560, y=172, with 11px green label "HMAC ok, processed once" at y=150.
- **Backoff arcs:** thin 1.5px violet `#4a3aa7` arcs connecting consecutive attempts above the line, 11px violet labels "~×5" near each arc apex except the first arc (the initial gap).
- **Annotation (bold 13px violet `#4a3aa7`, near x=430, y=70):** "gaps grow ~5× — the event outlives the outage".
- **Caption (12px `#444`, bottom right):** "retry schedule illustrative; providers publish their own".

## Where the Calls-You Pattern Runs

**Tags:** `where it's used` (blue), `event pipelines` (green)

- **Payments** — charge succeeded, refund issued, subscription renewed: fulfillment starts from a webhook
- **Integrations** — a code push webhook triggers CI; a form submission webhook updates the CRM
- **Pipelines** — one event fans out: mark order paid, email the receipt, decrement inventory, log analytics
- **The cost math** — to notice 25 orders a day, 10-second polling spends 8,640 requests; webhooks spend 25
- **The freshness math** — 5-minute polling is 150 s stale on average; a webhook arrives in about 1 s
- **The trade** — you now run a public endpoint that must be up, fast, and verified

*Example (italic):* Moving the coffee shop from 60-second polling to webhooks cuts 1,440 daily requests to 25 and average delay from 30 s to ~1 s.

**Key point:** Polling cost scales with the clock — every interval, forever — while webhook cost scales with actual events; that is why event pipelines and cross-service integrations are built on webhooks.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: requests per day needed to learn about the same 25 orders, at three polling frequencies vs webhooks, with average staleness noted per row.

- **Title (bold 15px, `#1a5276`, top center):** "25 Orders a Day: Requests Spent Finding Out About Them".
- **Axis:** vertical 2px `#999` baseline at x=230, bars extend right, max width 440; bar widths hardcoded for log-feel, true counts printed at bar ends (11px `#444`).
- **Rows (14px tall bars, top to bottom at y = 70, 120, 170, 220), each with a left-aligned 12px `#444` label at x=20:**
  - "poll every 10 s — 5 s stale": blue `rgba(42,120,214,0.30)` bar width 440, end label "8,640 req/day"
  - "poll every 60 s — 30 s stale": blue bar width 240, end label "1,440 req/day"
  - "poll every 5 min — 150 s stale": blue bar width 120, end label "288 req/day"
  - "webhooks — ~1 s fresh": solid green `#008300` bar width 14, end label "25 req/day"
- **Annotation (bold 13px magenta `#d55181`, right side near y=250):** "polling pays by the clock; webhooks pay by the event".
- **Caption (12px `#444`, bottom right):** "bar widths schematic, request counts exact for 25 orders/day".

## A Public URL Anyone Can POST To

**Tags:** `common mistake` (red), `security` (orange), `duplicates` (blue)

- **The exposure** — `/webhook` is a public URL; anyone who guesses it can POST a fake "charge succeeded"
- **The forgery** — an unverified handler that trusts the body will ship order 4412 with $0 collected
- **The fix** — recompute the HMAC-SHA256 of the raw body with the shared secret; mismatch means reject 401
- **The duplicate** — retries mean the same event can arrive twice; a non-idempotent handler ships twice
- **The slow handler** — doing heavy work before replying times out the sender and triggers more retries
- **The pattern** — verify the signature, record the event id, return 200 fast, do the real work off a queue

*Example (italic):* A forged POST claiming order 4412 is paid sails through an unverified handler; with the HMAC check it is rejected in one line.

**Common mistake:** Treating a webhook body as trusted input. The request came from the open internet, not "from PayLine" — only the signature proves the sender, and only the event id protects you from doing the work twice.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: the same forged POST handled without signature verification (accepted, order ships free) vs with HMAC verification (rejected 401).

- **Title (bold 15px, `#1a5276`, top center):** "The Same Forged POST, With and Without the Signature Check".
- **Row 1 (boxes centered on y=95), label 12px `#444` at x=20:** "no check"; blue `#2a78d6` rounded box at x=150 labeled "POST {order 4412 paid}" (12px), 3px arrow to a red `#e74c3c` box at x=430 labeled "handler trusts body — ships order" with bold 12px red "✗ $0 collected" to its right.
- **Row 2 (boxes centered on y=205), label:** "verify HMAC"; identical blue box "POST {order 4412 paid}", 3px arrow to a green `#008300` box at x=360 labeled "recompute HMAC — mismatch", then arrow to a green box at x=560 labeled "reject 401" with bold 12px green "✓".
- **Box style:** 150–190px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=270):** "verify the sender, dedupe by event id, ack fast — then do the work".
- **Caption (12px `#444`, bottom right):** "order and amounts illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); charge times (2.4 / 5.1 / 8.7 min), the $18.50 charge, event id `evt_9d41`, the 40-minute outage, and the +1m/+5m/+30m/+2h retry schedule are invented and labeled illustrative; polling request counts (8,640 / 1,440 / 288 vs 25) and average staleness (5 s / 30 s / 150 s) are exact arithmetic for the stated intervals and 25 orders/day; c2 and c3 use schematic pixel positions with true values printed as labels.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
