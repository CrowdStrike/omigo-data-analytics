# Notification System

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Notification System

**Subtitle:** One business event — an order shipped — fans out to push, email, SMS, in-app, and webhook, each through its own queue, checks, and retries

## One Shipped Order, Five Ways to Say It

**Tags:** `core idea` (blue), `fan-out` (green), `pipeline` (orange)

- **The event** — at 2:00pm the warehouse marks order #4817 shipped; one fact enters the system
- **The fan-out** — that single event becomes a push, an email, an SMS, an in-app card, and a webhook
- **The service** — a notification service owns the fan-out so product teams don't each rebuild it
- **The checks** — per-user preferences and rate limits run before anything is rendered or sent
- **The queues** — each channel gets its own queue; channel workers call the outside providers

*Example (italic):* One "order #4817 shipped" event turns into five channel-shaped messages without the shipping code knowing any provider exists.

**Key point:** A notification system is a fan-out pipeline: one business event in; preference and rate-limit checks, template rendering, per-channel queues, and channel workers out — the producer never talks to a provider directly.

### Visualization (canvas `c1`, 720×300)

Left-to-right flow diagram: one event box passes through the service's stages, then fans out to five channel queues, each feeding a worker that calls its provider.

- **Title (bold 15px, `#1a5276`, top center):** "One Event In, Five Channels Out".
- **Stage boxes (40px tall, 8px radius, fill `rgba(42,120,214,0.15)`, 12px `#2c3e50` text), left chain centered at y=150:** "order #4817 shipped" at x=15 (width 130), arrow to "prefs + rate limit" at x=170 (width 125), arrow to "render template" at x=320 (width 120).
- **Fan-out arrows:** five 2px `#6b7280` arrows from the right edge of the render box (x=440, y=150) to five queue boxes.
- **Queue boxes (90 wide, 30 tall, fill `rgba(0,131,0,0.12)`, 12px text) at x=490, y = 55, 103, 151, 199, 247:** "push queue", "email queue", "SMS queue", "in-app queue", "webhook queue".
- **Provider labels (11px `#6b7280`, right of each queue at x=595):** "→ push provider", "→ email provider", "→ SMS gateway", "→ app inbox", "→ partner URL".
- **Annotation (bold 12px violet `#4a3aa7`, under the chain near x=170, y=215):** "checks run once, before any channel work".
- **Caption (12px `#444`, bottom left):** "workers omitted for space — one per queue".

## The Afternoon the Email Provider Slowed Down

**Tags:** `worked example` (blue), `per-channel queues` (green)

- **The setup** — during the afternoon rush every channel queue receives 200 notifications per minute
- **The stall** — at 2:03pm the email provider slows down and email drains at only 50 per minute
- **The backlog** — 200 in and 50 out means the email queue grows by 150 per minute while it lasts
- **Hand-check** — four slow minutes (2:03–2:07) leave 4 × 150 = 600 emails queued at 2:07pm
- **The recovery** — draining 350 per minute after 2:07, the 600 backlog clears by 2:11pm
- **The payoff** — push and SMS have their own queues, so they never feel email's bad afternoon

*Example (italic):* At 2:07pm, 600 emails wait in the email queue while pushes keep leaving within a second — one shared queue would have blocked them all.

**Key point:** One queue per channel isolates providers with different speeds and failure moments — a stalled channel backs up its own queue instead of cross-blocking every other channel.

### Visualization (canvas `c2`, 720×300)

Line chart of queue depth per channel across the 12 minutes around the email stall: email climbs to 600 and drains back, push and SMS stay flat near zero.

- **Title (bold 15px, `#1a5276`, top center):** "Email Backs Up 600 Deep — Push and SMS Never Notice".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = minutes after 2:00pm, 0 to 12, 12px `#444` tick labels every 2 minutes ("2:00"…"2:12"); y = queued messages 0 to 700, gridlines `#e5e9ef` at 200/400/600 with 12px labels.
- **Email line:** orange `#d95926` 3px line through minutes `[0,1,2,3,4,5,6,7,8,9,10,11,12]`, depth `[0, 0, 0, 0, 150, 300, 450, 600, 450, 300, 150, 0, 0]`.
- **Push line:** blue `#2a78d6` 2px line, same minutes, depth `[4, 7, 5, 8, 6, 9, 5, 7, 6, 8, 5, 7, 6]` — flat.
- **SMS line:** green `#008300` 2px line, same minutes, depth `[6, 4, 8, 5, 7, 4, 8, 6, 5, 7, 4, 6, 5]` — flat.
- **Stall markers:** vertical dashed `#6b7280` (dash 4/3) lines at minutes 3 and 7, 12px `#6b7280` labels "provider slows" and "recovers" at their tops.
- **Annotation (bold 13px orange `#d95926`, near minute 8, y=70):** "peak backlog 600 = 4 min × 150/min".
- **Legend (12px, top left inside plot):** orange "email", blue "push", green "SMS".
- **Caption (12px `#444`, bottom right):** "rates illustrative — 200/min in, 50/min out during stall".

## Retries, Quiet Hours, and the Collapsed Like

**Tags:** `why it matters` (blue), `idempotency` (green), `user respect` (orange)

- **The retry** — the push worker times out after 5s and retries at 10s; done naively, that's two pushes
- **The key** — every attempt carries idempotency key `order-4817-push`, so the provider sends it once
- **Quiet hours** — the user sleeps 10pm–8am, so a 2am event is held and delivered at 8:00am
- **Rate limit** — a cap like 5 pushes per hour keeps one chatty feature from burning notification trust
- **Collapse** — 10 likes inside a 5-minute window fold into one "10 people liked your photo"

*Example (italic):* A timed-out push retried with the same key `order-4817-push` reaches the phone exactly once; 10 like events reach it as exactly one notification.

**Key point:** The service is a policy engine, not a relay — idempotency keys make retries safe to send twice, and quiet hours, rate limits, and collapsing decide whether to send at all.

### Visualization (canvas `c3`, 720×300)

Two-row diagram: a retry made safe by an idempotency key (top), and ten like events collapsing into one notification (bottom).

- **Title (bold 15px, `#1a5276`, top center):** "Send Twice, Deliver Once — and Ten Events, One Notification".
- **Row 1 (boxes centered at y=95), label 12px `#444` at x=15:** "retry"; blue `#2a78d6` rounded box at x=80 (width 165) labeled "send key=order-4817-push" (12px), small red 12px "✗ timeout 5s" above the arrow; 3px arrow to a blue box at x=290 (width 165) labeled "retry 10s, same key"; 3px arrow to a green `#008300` box at x=500 (width 195) labeled "provider: key seen → 1 push" with bold 12px green "✓ not 2".
- **Row 2 (centered at y=210), label at x=15:** "collapse"; ten 3px vertical orange `#d95926` ticks (14px tall) spread x=80…200 along a thin `#e5e9ef` timeline with 12px `#444` label "10 like events, 2:00–2:05" beneath; 3px arrow to a violet `#4a3aa7` box at x=280 (width 150) labeled "5-min collapse window"; 3px arrow to a green box at x=490 (width 205) labeled "\"10 people liked your photo\"".
- **Box style:** 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(0,131,0,0.12)` / `rgba(74,58,167,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px magenta `#d55181`, centered near y=272):** "the retry is idempotent; the collapse is intentional — both prevent duplicate noise".

## Sent Is Not Delivered

**Tags:** `common mistake` (red), `last hop` (orange)

- **The illusion** — the provider answers 200 OK, but that only means "accepted", not "delivered"
- **Offline phones** — of 1,000 pushes accepted, 780 arrive; the rest hit stale tokens and dead phones
- **Bounces** — of 1,000 emails, 940 land; 60 bounce on full or abandoned mailboxes
- **Webhooks** — the partner endpoint 500s during its own deploy; 965 of 1,000 get through after retries
- **Other hops** — SMS lands 985 of 1,000; in-app lands 990, shown when the user next opens the app
- **The fix** — track a per-message state machine (queued → sent → delivered → failed) from receipts

*Example (italic):* A dashboard counting provider calls reports 100% success on a day when 220 of 1,000 pushes never reached a phone.

**Common mistake:** Treating the provider's acceptance as delivery. The last hop — a phone that is off, a mailbox that bounces, an endpoint that is down — is the least reliable link, and only delivery receipts reveal it.

### Visualization (canvas `c4`, 720×300)

Horizontal paired bars per channel: messages sent (accepted by provider) vs messages actually delivered, out of 1,000 each.

- **Title (bold 15px, `#1a5276`, top center):** "Accepted by the Provider vs Delivered to the User (per 1,000 sent)".
- **Rows (top to bottom, bar pairs centered at y = 75, 115, 155, 195, 235), each with a left-aligned 12px `#444` label at x=20:**
  - "push — 780 delivered": blue `rgba(42,120,214,0.30)` sent bar width 400, overlay green `#008300` delivered bar width 312, bold 12px red `#e74c3c` label "220 lost" at the gap
  - "email — 940 delivered": sent bar width 400, delivered overlay width 376, 11px `#444` "60 bounced"
  - "SMS — 985 delivered": sent bar width 400, delivered overlay width 394
  - "in-app — 990 delivered": sent bar width 400, delivered overlay width 396, 11px `#6b7280` "on next open"
  - "webhook — 965 delivered": sent bar width 400, delivered overlay width 386, 11px `#444` "after retries"
- **Bar style:** bars start at x=250, 14px tall; sent bar drawn first, delivered overlay drawn on top from the same left edge; 11px count labels at delivered bar ends.
- **Scale:** width 400px = 1,000 messages (0.4px per message), no explicit axis.
- **Annotation (bold 13px red `#e74c3c`, right side near y=262):** "the last hop fails silently — count receipts, not send calls".
- **Caption (12px `#444`, bottom left):** "delivery rates illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); queue rates (200/min in, 50/min out, 350/min recovery, 600 peak) and delivery counts (780/940/985/990/965 per 1,000) are invented and labeled illustrative; the backlog arithmetic in the text (4 × 150 = 600, clear by 2:11pm) must match the c2 arrays exactly.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
