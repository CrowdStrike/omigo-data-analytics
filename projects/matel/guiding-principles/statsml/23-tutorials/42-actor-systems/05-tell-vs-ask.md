# Tell vs Ask

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Tell vs Ask

**Subtitle:** An actor can hand off a message and walk away (tell), or hand it off and hold a claim ticket that expires (ask) — fire-and-forget or a Future with a timeout

## Two Ways to Hand Off an Order

**Tags:** `core idea` (blue), `messaging` (green), `Akka-style` (orange)

- **The service** — an order actor receives "order #4471: two lattes, $9.40" and must coordinate the rest
- **The tell** — it tells the email actor "send confirmation for #4471" and immediately moves on
- **No receipt** — a tell has no reply, no waiting, no timeout; the message sits in the mailbox until handled
- **The ask** — it asks the payment actor "charge $9.40" and gets back a Future, not the answer itself
- **The ticket** — the Future completes when the reply arrives, or fails if 2 seconds pass with no reply

*Example (italic):* Order #4471 triggers one ask (payment — the order cannot proceed without the result) and one tell (email — nobody waits on a confirmation mail).

**Key point:** Tell is fire-and-forget with no reply channel; ask returns a Future that must complete within a declared timeout — use ask only when the sender genuinely needs the answer to continue.

### Visualization (canvas `c1`, 720×300)

Two-row flow diagram contrasting the two patterns for order #4471: tell (single arrow, sender free) vs ask (arrow out, dashed reply arrow back, timeout clock).

- **Title (bold 15px, `#1a5276`, top center):** "One Order, Two Handoffs: Tell Walks Away, Ask Waits on a Ticket".
- **Row 1 (tell, y=100), label 12px `#444` at x=20:** "tell"; blue `#2a78d6` rounded box at x=110 labeled "order actor" (12px), solid 3px green `#008300` arrow to a green box at x=430 labeled "email actor: send #4471"; bold 12px green label under the arrow "no reply expected — sender moves on".
- **Row 2 (ask, y=210), label:** "ask"; blue box "order actor" at x=110, solid 3px blue arrow to a blue box at x=430 labeled "payment actor: charge $9.40"; dashed 2px violet `#4a3aa7` arrow (dash 5/4) returning underneath labeled "Future completes" (12px violet); small orange `#d95926` clock circle (radius 12) at x=290, y=250 with bold 12px orange label "timeout 2 s".
- **Box style:** 150–190px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px ink `#1a5276`, top right near y=60):** "ask = tell + temporary reply actor + timer".

## Charging the Card: One Ask, One Timeout

**Tags:** `worked example` (blue), `timeout` (orange)

- **The setup** — the order actor asks the payment actor to charge each order, timeout set to 2,000 ms
- **The day's traffic** — 1,000 charges (illustrative): most replies come back in well under half a second
- **The buckets** — 620 reply under 250 ms, 300 in 250–500 ms, 60 in 500–1,000 ms, 15 in 1,000–2,000 ms
- **The failures** — 5 replies would take over 2,000 ms; their Futures fail with an ask timeout instead
- **Hand-check** — 620 + 300 + 60 + 15 = 995 completed Futures; 995 / 1,000 = 99.5% success at this timeout

*Example (italic):* Order #4471's charge replies in 180 ms — the Future completes 1,820 ms before its 2-second deadline; 5 orders that day are the unlucky timeouts.

**Key point:** The timeout is a business decision, not a constant: set it from the reply-time distribution (here 99.5% finish under 2 s), and decide up front what the order actor does with the 0.5% that fail.

### Visualization (canvas `c2`, 720×300)

Bar chart of payment reply times for the 1,000 charges, bucketed, with a red timeout cutoff line separating completed Futures from failed ones.

- **Title (bold 15px, `#1a5276`, top center):** "1,000 Charges vs a 2-Second Ask Timeout".
- **Axes:** origin x=70, baseline y=245, plot width 590, plot height 180; y = charges 0 to 700, gridlines `#e5e9ef` at 175/350/525 with 12px `#444` labels; x = five category buckets.
- **Bars (width 80, gap 30, left edges from x=90), fill `rgba(42,120,214,0.35)` with 2px `#2a78d6` border, 12px bold `#1a5276` value label above each:** "<250 ms" = 620, "250–500" = 300, "500–1000" = 60, "1000–2000" = 15, ">2000 ms" = 5 — the last bar filled red `rgba(231,76,60,0.35)` with 2px `#e74c3c` border and red value label.
- **Cutoff line:** vertical dashed `#e74c3c` (dash 5/4) 2px line between the 4th and 5th bars, bold 12px red label "timeout = 2,000 ms" rotated horizontal at its top.
- **Annotation (bold 13px green `#008300`, upper right, y=80):** "995 of 1,000 complete — 5 Futures fail".
- **Caption (12px `#444`, bottom right):** "reply-time counts illustrative".

## Timeouts Inside Timeouts

**Tags:** `where it's used` (blue), `cascading timeouts` (orange)

- **The chain** — the order actor asks payment (2,000 ms); payment asks the fraud actor (1,500 ms)
- **The inner call** — the fraud actor gives the card network 1,000 ms before it gives up itself
- **The rule** — each inner timeout must be shorter than the outer one minus the hop's own work
- **The headroom** — here every hop keeps ~500 ms to build a proper failure reply and send it upward
- **The anti-pattern** — set all three to 2,000 ms and the outermost expires first: a bare timeout, no cause

*Example (italic):* The card network stalls for 3 seconds; the fraud actor quits at 1,000 ms, payment reports "fraud check timed out" at ~1,100 ms, and the order actor still has 900 ms to decline the order cleanly.

**Key point:** Ask timeouts cascade — budget them like nested deadlines (2,000 > 1,500 > 1,000 ms here) so failures surface at the hop that caused them, with time left to say so.

### Visualization (canvas `c3`, 720×300)

Nested horizontal timeout-budget bars: three hops drawn as bars inside one another on a shared millisecond scale, showing shrinking budgets and per-hop headroom.

- **Title (bold 15px, `#1a5276`, top center):** "The Timeout Budget Shrinks at Every Hop".
- **Scale:** x-axis 0 to 2,000 ms mapped to pixels x=200 (0 ms) through x=680 (2,000 ms), i.e. 0.24 px/ms; 2px `#999` baseline at y=245 with 12px `#444` ticks at 0 / 500 / 1,000 / 1,500 / 2,000 ms.
- **Rows (bars 26px tall, left-aligned 12px `#444` labels at x=20):**
  - y=80: "order → payment: 2,000 ms", blue `rgba(42,120,214,0.30)` bar from 0 to 2,000 ms (width 480), 2px `#2a78d6` border
  - y=135: "payment → fraud: 1,500 ms", aqua `rgba(25,158,112,0.30)` bar from 0 to 1,500 ms (width 360), 2px `#199e70` border
  - y=190: "fraud → card network: 1,000 ms", orange `rgba(217,89,38,0.30)` bar from 0 to 1,000 ms (width 240), 2px `#d95926` border
- **Headroom markers:** dashed 2px `#6b7280` (dash 4/3) vertical connectors from each bar's right end down to the next bar's right end, each gap labeled bold 11px `#6b7280` "~500 ms headroom".
- **Annotation (bold 13px red `#e74c3c`, below the bars near y=272, centered):** "equal timeouts everywhere = outermost fails first, cause unknown".
- **Caption (12px `#444`, bottom right):** "budgets illustrative; rule: inner < outer − own work".

## Don't Ask When Nobody Needs an Answer

**Tags:** `common mistake` (red), `throughput` (orange)

- **The habit** — teams reach for ask everywhere because a Future feels safer than silence
- **The cost** — every ask allocates a temporary reply actor plus a timeout timer; a tell allocates nothing
- **The blocker** — worse, awaiting each ask before the next order serializes the whole actor
- **The measure** — the email handoff (illustrative): tell sustains 9,500 orders/s; ask-and-wait manages 480
- **The test** — if the sender's next step doesn't change based on the reply, it should be a tell

*Example (italic):* The confirmation email for order #4471 changes nothing about the order's outcome — asking for a "mail sent" reply just adds a timer, a temp actor, and a new way to fail.

**Common mistake:** Using ask as a default and blocking on the Future. Ask is for answers the sender must have; delivery confidence for one-way messages is the mailbox's job, not a reply's.

### Visualization (canvas `c4`, 720×300)

Two-bar throughput comparison for the email handoff: tell (fire-and-forget) vs ask-and-wait, on a shared orders/second axis.

- **Title (bold 15px, `#1a5276`, top center):** "Email Handoff Throughput: Tell 9,500/s vs Ask-and-Wait 480/s".
- **Axes:** origin x=70, baseline y=245, plot width 590, plot height 180; y = orders/s 0 to 10,000, gridlines `#e5e9ef` at 2,500 / 5,000 / 7,500 with 12px `#444` labels.
- **Bar 1 (tell):** green `rgba(0,131,0,0.30)` fill, 2px `#008300` border, width 140, centered near x=250, height for 9,500 (171 px), bold 13px green value label "9,500 orders/s" above, 12px `#444` label "tell (fire-and-forget)" below the baseline.
- **Bar 2 (ask-and-wait):** red `rgba(231,76,60,0.30)` fill, 2px `#e74c3c` border, width 140, centered near x=490, height for 480 (9 px), bold 13px red value label "480 orders/s" above, 12px `#444` label "ask + block on Future" below the baseline.
- **Annotation (bold 13px ink `#1a5276`, centered near x=500, y=110):** "the reply nobody reads costs ~20× the throughput".
- **Caption (12px `#444`, bottom right):** "throughput numbers illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded values above (no randomness); reply-time bucket counts (620/300/60/15/5), timeout budgets (2,000/1,500/1,000 ms), and throughput figures (9,500 vs 480 orders/s) are invented and labeled illustrative; the hand-check 620+300+60+15 = 995 → 99.5% must match text and chart.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
