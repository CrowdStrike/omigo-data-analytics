# Delivery Guarantees in Akka

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Delivery Guarantees in Akka

**Subtitle:** When one actor sends a message to another, Akka promises surprisingly little by default — the message arrives at most once, which means it might not arrive at all

## The Order Confirmation That Never Arrived

**Tags:** `core idea` (blue), `at-most-once` (orange), `default` (green)

- **The setup** — an order service `tell`s a Notifier actor "email a confirmation for order #4821"
- **Fire and forget** — `tell` returns instantly; nothing waits to see if the message landed
- **The loss** — the Notifier's node restarts mid-flight; the message is simply gone
- **The promise** — at-most-once: a message is delivered 0 or 1 times, never 2
- **The count** — of 100 confirmations sent during the restart window, 4 vanish (illustrative)
- **Why default** — no acks, no retries, no state to keep: it is the cheapest guarantee to provide

*Example (italic):* Customer #4821 never gets a confirmation email, and no error is raised anywhere — the send "succeeded" the moment `tell` returned.

**Key point:** Akka's default delivery guarantee is at-most-once — messages can be lost silently, but the transport will never duplicate one on its own.

### Visualization (canvas `c1`, 720×300)

Flow chart of 100 messages sent under at-most-once: a wide band leaves the order service, 96 reach the Notifier, 4 peel off into a "lost" stub — and a zero-width "duplicated" stub makes the never-duplicates half of the promise visible.

- **Title (bold 15px, `#1a5276`, top center):** "At-Most-Once: 100 Sent, 96 Delivered, 4 Lost, 0 Duplicated".
- **Left box:** blue `#2a78d6` rounded box (8px radius, fill `rgba(42,120,214,0.15)`) at x=40, y=110, 150×60, 12px `#2c3e50` label "order service — 100 sends".
- **Right box:** green `#008300` rounded box at x=520, y=110, 160×60, label "Notifier actor — 96 received".
- **Delivered band:** horizontal band from x=190 to x=520 centered at y=140, 26px tall, fill `rgba(0,131,0,0.30)`, bold 12px `#008300` label "96 delivered" centered on it.
- **Lost stub:** thinner band (8px tall, fill `rgba(231,76,60,0.55)`) forking down from x≈340 to a red `#e74c3c` 12px label "4 lost (node restart)" at y=230.
- **Duplicated stub:** dashed `#6b7280` (dash 4/3) line forking up from x≈340 to a 12px `#6b7280` label "0 duplicated — never happens" at y=70.
- **Annotation (bold 13px orange `#d95926`, near x=190, y=250):** "the sender is never told about the 4".
- **Caption (12px `#444`, bottom right):** "counts illustrative".

## Adding Acks: At-Least-Once by Hand

**Tags:** `worked example` (blue), `at-least-once` (green), `duplicates` (orange)

- **The fix** — the Notifier now replies "Ack #4821" after emailing; the sender keeps unacked messages
- **The rule** — resend anything unacked after 2 seconds, until an ack finally comes back
- **The run** — 100 sends: 4 messages lost in flight, 3 delivered fine but their acks lost
- **Hand-check** — 7 resends fire; the 4 lost ones now arrive, the 3 ack-lost ones arrive a 2nd time
- **The totals** — Notifier processes 103 messages for 100 orders: 0 lost, 3 duplicate emails
- **The trade** — at-least-once swaps silent loss for guaranteed delivery plus possible duplicates

*Example (italic):* Order #4907's email was sent and the ack was lost on the way back — the retry means customer #4907 gets the same confirmation twice.

**Key point:** At-least-once is not free and not built into plain `tell` — you build it from acks, a resend buffer, and a timer, and the price is duplicates whenever an ack (not the message) is what got lost.

### Visualization (canvas `c2`, 720×300)

Grouped bar chart comparing the same 100-order run under both guarantees: sent / delivered / lost / duplicates, with at-least-once trading its 4 losses for 3 duplicates.

- **Title (bold 15px, `#1a5276`, top center):** "Same 100 Orders: Loss vs Duplicates".
- **Axes:** origin x=70, baseline y=245, plot width 590, plot height 180; y = messages 0 to 110, gridlines `#e5e9ef` at 25/50/75/100 with 12px `#444` labels.
- **Groups (centered at x = 160, 300, 440, 580, 12px `#444` labels below baseline):** "sent", "delivered", "lost", "duplicates".
- **Bars per group, 40px wide, 8px gap:** at-most-once bar fill `rgba(42,120,214,0.30)` with 2px `#2a78d6` edge, values `[100, 96, 4, 0]`; at-least-once bar fill `rgba(0,131,0,0.30)` with 2px `#008300` edge, values `[100, 100, 0, 3]`.
- **Value labels:** bold 12px in each bar's edge color, 6px above each bar top.
- **Legend (12px, top right at x≈520, y=60):** blue swatch "at-most-once", green swatch "at-least-once".
- **Lost/duplicate highlight:** red `#e74c3c` bold 12px "4 lost" beside the at-most-once lost bar; orange `#d95926` bold 12px "3 dupes" beside the at-least-once duplicates bar.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=80):** "delivery guaranteed — uniqueness no longer is".
- **Caption (12px `#444`, bottom right):** "counts illustrative, same run both ways".

## The One Ordering Promise You Do Get

**Tags:** `where it's used` (blue), `ordering` (green)

- **The pair rule** — messages from sender A to receiver B arrive in the order A sent them
- **Per pair only** — if A and C both write to B, B may see their messages interleaved any way
- **The run** — A sends m1, m2, m3 and C sends m4, m5; B can see m1 m4 m2 m5 m3 — never m2 before m1
- **Loss still allowed** — at-most-once still applies: m2 can vanish, but m3 cannot overtake m1
- **Why it matters** — a "create account" then "set email" pair from one sender lands in that order
- **The design habit** — funnel writes that must stay ordered through a single sender-receiver pair

*Example (italic):* The order service sends "reserve stock" then "charge card" for order #4821 to the same actor — Akka guarantees the charge is never processed before the reservation, though either message can still be lost outright.

**Key point:** Akka guarantees ordering per sender-receiver pair, not globally — two messages from the same sender to the same receiver never swap places, but anything can interleave between them.

### Visualization (canvas `c3`, 720×300)

Message-lane diagram: sender A's lane and sender C's lane converge on receiver B's mailbox, where an interleaved arrival strip shows A's messages still in order and C's messages still in order.

- **Title (bold 15px, `#1a5276`, top center):** "Ordered Per Pair: A's m1<m2<m3 Holds, A vs C Interleaves Freely".
- **Sender boxes (rounded, 8px radius, 120×44, 12px `#2c3e50` text):** blue `#2a78d6` box "sender A" at x=40, y=70; violet `#4a3aa7` box "sender C" at x=40, y=180.
- **Receiver box:** green `#008300` box "receiver B mailbox" at x=560, 130×60, centered at y=140.
- **Arrival strip:** five 56×34 rounded chips in a row from x=230 to x=530 at y=123, arrival order left to right: "m1" (blue), "m4" (violet), "m2" (blue), "m5" (violet), "m3" (blue); fills `rgba(42,120,214,0.15)` / `rgba(74,58,167,0.15)`, 2px edges in the sender's color, bold 12px chip labels.
- **Flow lines:** 2px blue curve from sender A to the strip's left edge; 2px violet curve from sender C to the strip's left edge; 3px `#999` arrow from the strip's right edge into receiver B.
- **Order ticks:** thin blue arcs above the strip connecting m1→m2→m3 with a bold 12px blue "A's order preserved" label at y=78; thin violet arcs below connecting m4→m5 with 12px violet "C's order preserved" at y=205.
- **Annotation (bold 13px orange `#d95926`, bottom center y=265):** "no promise about A vs C — only within each pair".
- **Caption (12px `#444`, bottom right):** "one possible interleaving, illustrative".

## "Exactly-Once" Is Not a Transport Setting

**Tags:** `common mistake` (red), `idempotence` (orange)

- **The wish** — teams ask for exactly-once delivery so no email is ever lost or doubled
- **The catch** — no transport can promise it: a lost ack always forces a choice, drop or resend
- **The real recipe** — exactly-once = at-least-once delivery + idempotent processing at the receiver
- **The dedup** — the Notifier keeps seen order ids; a redelivered #4907 is recognized and skipped
- **The run** — the 103 arrivals from the worked example become exactly 100 emails after dedup
- **The mistake** — flipping on retries without dedup, then blaming the framework for double emails

*Example (italic):* With a seen-ids set, the retried #4907 arrives a second time, matches a stored id, and is dropped — the customer gets exactly one email despite two deliveries.

**Common mistake:** Believing exactly-once is a delivery guarantee a framework can switch on. Delivery can only be at-least-once; "exactly-once" is an effect you build by making the receiver ignore the duplicates.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: the redelivered message #4907 processed naively (two emails) vs through an idempotent receiver with a seen-ids check (one email).

- **Title (bold 15px, `#1a5276`, top center):** "Redelivered #4907: Naive Receiver vs Idempotent Receiver".
- **Row 1 (y=95), label 12px `#444` at x=20:** "no dedup"; blue `#2a78d6` rounded box at x=130 labeled "#4907 arrives twice" (12px), 3px arrow to a red `#e74c3c` box at x=360 labeled "process both", arrow to bold 12px red "✗ 2 emails sent" at x=560.
- **Row 2 (y=205), label:** "seen-ids check"; blue box "#4907 arrives twice" at x=130, 3px arrow to a green `#008300` box at x=330 labeled "id seen? drop copy 2", arrow to a green box at x=530 labeled "process copy 1" with bold 12px green "✓ 1 email".
- **Box style:** 140–170px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Tally chips (top right, y=55):** small 12px `#444` text "103 arrivals → 100 emails" with a 2px `#008300` underline, tying back to the worked example's totals.
- **Annotation (bold 13px magenta `#d55181`, centered near y=270):** "the transport delivers at-least-once; the receiver makes it exactly-once".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all values are the hardcoded numbers above (no randomness); the 100-order run (4 lost, 3 acks lost, 7 resends, 103 processed, 3 duplicates, 100 after dedup) is invented and labeled illustrative; the same run's numbers must stay consistent across c1, c2, and c4. The per-pair ordering rule and the at-most-once default are Akka's documented semantics, not invented.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
