# Unreliable Networks

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Unreliable Networks

**Subtitle:** When a service call gets no answer, the network can't tell you why — a timeout is not a diagnosis, it's an absence of information

## The Payment Call That Never Answers

**Tags:** `core idea` (blue), `four causes` (green), `one signal` (orange)

- **The call** — a checkout service asks a payments service to charge $40 for order #7241
- **The silence** — 2 seconds pass with no reply, so the checkout service gives up: timeout
- **Cause one** — the request was dropped in transit; payments never heard anything
- **Cause two** — the payments server is dead; the request arrived at a machine that's gone
- **Cause three** — the server is alive but slow; the reply is coming, just not yet
- **Cause four** — payments did the work and replied, but the reply itself was dropped

*Example (italic):* All four failures look exactly the same from the checkout side: silence, then a timeout at 2 seconds.

**Key point:** A timeout tells you only that no reply arrived in time — it cannot tell you which of the four things happened, or whether the charge went through.

### Visualization (canvas `c1`, 720×300)

Four-lane flow diagram: the same checkout→payments call failing four different ways, every lane ending in the identical observation "no reply by 2s".

- **Title (bold 15px, `#1a5276`, top center):** "Four Different Truths, One Identical Observation".
- **Lanes (y = 78, 128, 178, 228), each with a left-aligned 12px `#444` label at x=20:** "request dropped", "server dead", "server slow", "reply dropped".
- **Lane anatomy:** blue `#2a78d6` rounded box (90×30, 8px radius, fill `rgba(42,120,214,0.15)`) at x=140 labeled "checkout" (12px `#2c3e50`); 2px `#6b7280` arrow toward a second rounded box at x=340 labeled "payments"; return arrow beneath where applicable.
- **Failure marks (bold 14px red `#e74c3c` "✗"):** lane 1 on the forward arrow midpoint (x≈290); lane 2 on the payments box (box redrawn with fill `rgba(231,76,60,0.12)` and red border); lane 3 no ✗ — instead 12px yellow `#c98500` label "reply at 2.6s" past the box; lane 4 on the return arrow midpoint (x≈290, below the lane line).
- **Shared outcome column:** identical mute `#6b7280` rounded box (150×30, fill `rgba(107,114,128,0.10)`) at x=530 in every lane, 12px text "timeout at 2s".
- **Annotation (bold 13px magenta `#d55181`, centered near y=270):** "the caller sees the same signal in all four lanes".
- **Caption (12px `#444`, bottom right):** "timings illustrative".

## Picking a Timeout from 1,000 Latencies

**Tags:** `worked example` (blue), `percentiles` (green), `trade-off` (orange)

- **The data** — 1,000 healthy calls to payments, bucketed: 180 under 100ms, 330 in 100–200ms, 230 in 200–300ms
- **The tail** — 190 in 300–500ms, 50 in 500ms–1s, 15 in 1–2s, 5 in 2–3s; every one eventually succeeded
- **Too short** — a 500ms timeout retries 50+15+5 = 70 of 1,000 healthy calls: 7% false alarms
- **Too long** — a 2s timeout has only 5 false alarms (0.5%), but a truly dead server stalls users 2 full seconds
- **Hand-check** — cumulative counts: 930 done by 500ms, 980 by 1s, 995 by 2s, so p99 lands between 1s and 2s
- **No right answer** — the timeout is a bet on the latency tail, not a detector of failure

*Example (italic):* Moving the timeout from 500ms to 2s trades 70 needless retries per 1,000 calls for a 4× longer stall whenever the server really is down.

**Key point:** Tuning a timeout is balancing two costs — retrying healthy slow calls versus making users wait on dead ones — because the network never says which case you're in.

### Visualization (canvas `c2`, 720×300)

Latency histogram of the 1,000 healthy calls with two candidate timeout lines cutting the tail at different points.

- **Title (bold 15px, `#1a5276`, top center):** "1,000 Healthy Calls: Where Do You Draw the Timeout?".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = calls 0 to 350, gridlines `#e5e9ef` at 100/200/300 with 12px `#444` labels; x = seven categorical buckets, 62px-wide bars starting at x=75 with 85px spacing, 12px `#444` labels under each: "<100ms", "100–200", "200–300", "300–500", "0.5–1s", "1–2s", "2–3s".
- **Bars:** counts `[180, 330, 230, 190, 50, 15, 5]`; first four bars fill `rgba(42,120,214,0.35)` with 2px `#2a78d6` top edge, last three (the tail) fill `rgba(201,133,0,0.30)` with 2px `#c98500` edge; 12px `#444` count label above each bar.
- **Timeout line A:** vertical dashed orange `#d95926` (dash 4/3) between bucket 4 and 5 (x≈405), bold 12px orange label at its top: "500ms → 70 healthy calls retried".
- **Timeout line B:** vertical dashed violet `#4a3aa7` between bucket 6 and 7 (x≈575), bold 12px violet label near its top: "2s → users stall 2s on a dead server".
- **Annotation (bold 13px ink `#1a5276`, upper right area, y≈95):** "every cutoff misfires one way or the other".
- **Caption (12px `#444`, bottom right):** "latency buckets illustrative".

## Retries, Deadlines, and Circuit Breakers

**Tags:** `where it's used` (blue), `idempotency` (green), `deadlines` (orange)

- **Retry danger** — retrying a charge after a lost *reply* charges the card twice; the work already happened
- **Idempotency key** — send the same order id with every attempt so the server can spot a duplicate
- **The dedupe** — payments sees key "order-7241" a second time, skips the charge, resends the old reply
- **Deadlines everywhere** — an RPC with no deadline can wait forever, pinning a thread on a dead peer
- **Circuit breaker** — after repeated timeouts, stop calling for a cool-off instead of piling retries on a sick server
- **Retry storms** — three layers, up to 3 attempts each, turn one click into up to 27 backend calls

*Example (italic):* With key "order-7241" attached, the retry after a lost reply returns the original receipt instead of a second $40 charge.

**Key point:** Because a timeout can't say whether the work happened, safe systems make retries idempotent, give every call a deadline, and back off instead of hammering.

### Visualization (canvas `c3`, 720×300)

Two-row flow diagram: retrying a $40 charge after a lost reply, without an idempotency key (double charge) vs with one (deduped).

- **Title (bold 15px, `#1a5276`, top center):** "The Reply Was Lost — Is It Safe to Retry?".
- **Row 1 (y=95), label 12px `#444` at x=20:** "no key"; blue `#2a78d6` rounded box at x=150 labeled "charge $40 (reply lost)" (12px), 3px arrow to a blue box at x=360 labeled "retry: charge $40", arrow to a red `#e74c3c` box at x=555 labeled "card charged $80" with bold 12px red "✗ double charge".
- **Row 2 (y=205), label:** "key order-7241"; blue box at x=150 "charge $40 + key (reply lost)", 3px arrow to a green `#008300` box at x=360 labeled "retry: same key — duplicate seen", arrow to a green box at x=555 labeled "charged $40 once" with bold 12px green "✓".
- **Box style:** 150–175px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px aqua `#199e70`, centered near y=270):** "idempotency makes the retry safe when you can't know what happened".
- **Caption (12px `#444`, bottom right):** "amounts illustrative".

## A Timeout Doesn't Tell You If the Work Happened

**Tags:** `common mistake` (red), `unknown state` (orange)

- **The assumption** — people treat a timeout as "the call failed", a clean signal that nothing happened
- **The reality** — after a timeout the operation is in an *unknown* state: done, not done, or still running
- **Reply lost** — the charge went through at 0.4s; only the receipt vanished, so the money moved
- **Request lost** — payments never heard a thing; nothing happened and a retry is genuinely needed
- **Still running** — the charge lands at 2.6s, *after* the caller already gave up and moved on
- **The mistake** — writing code with two branches, success and failure, when there are really three outcomes

*Example (italic):* Three different worlds — charged, not charged, charging later — all delivered the caller the exact same 2-second timeout.

**Common mistake:** Assuming a clean failure signal exists. A timeout is silence, not a verdict — code that maps it to "not done" will double-charge, and code that maps it to "done" will drop orders.

### Visualization (canvas `c4`, 720×300)

Three-lane timeline: identical caller experience (send at 0s, silence, timeout at 2s) against three different hidden truths about the $40 charge.

- **Title (bold 15px, `#1a5276`, top center):** "Same Timeout, Three Different Truths About the Money".
- **Time axis:** 2px `#999` horizontal baseline at y=252 from x=150 to x=630, 12px `#444` tick labels "0s", "1s", "2s" at x = 150, 329, 508; the caller's timeout marked with a vertical dashed `#6b7280` (dash 4/3) line at x=508 labeled "timeout" (12px `#6b7280`).
- **Lanes (y = 85, 145, 205), each with a left-aligned 12px `#444` label at x=20:** "reply lost", "request lost", "still running".
- **Lane 1:** blue `#2a78d6` 3px line from x=150 to x=222 (0.4s) ending in a green `#008300` dot with bold 12px green label "charged at 0.4s"; then nothing reaches the caller.
- **Lane 2:** blue 3px line from x=150 to x=190 ending in a red `#e74c3c` "✗" with bold 12px red label "never arrived — not charged".
- **Lane 3:** blue 3px line from x=150 past the timeout line to x=615 (2.6s) ending in a yellow `#c98500` dot with bold 12px yellow label "charged at 2.6s — after the caller quit".
- **Truth column:** 12px `#2c3e50` labels at x=630 per lane: "done", "not done", "done late".
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=272):** "the caller's view is identical in all three lanes".
- **Caption (12px `#444`, bottom right):** "timings illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the latency histogram counts `[180, 330, 230, 190, 50, 15, 5]` sum to 1,000 and drive the text's 70-retries-at-500ms and 5-at-2s figures; the $40/$80 charge amounts and the 0.4s/2.6s completion times are invented and labeled illustrative.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
