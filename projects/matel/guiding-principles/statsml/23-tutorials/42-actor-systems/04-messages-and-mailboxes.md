# Messages & Mailboxes

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Messages & Mailboxes

**Subtitle:** An actor reads immutable messages from its mailbox one at a time — so its private state never needs a lock

## The Inventory Actor's Inbox

**Tags:** `core idea` (blue), `immutable messages` (green), `no locks` (orange)

- **The actor** — one inventory actor owns the stock count for a warehouse item; nobody else can touch it
- **The messages** — the website sends `Reserve 30`, the loading dock sends `AddStock 40`; both are frozen values
- **The mailbox** — every incoming message lands in the actor's queue in arrival order and waits its turn
- **One at a time** — the actor pops a message, updates its count, and only then pops the next one
- **No lock needed** — since exactly one message runs at a time, the counter can never be read mid-update

*Example (italic):* Ten checkout servers all send `Reserve` messages at once — the mailbox lines them up, and the actor handles them like a single cashier serving a queue.

**Key point:** An actor is a mailbox plus private state: immutable messages queue up, get processed strictly one at a time, and the state needs no lock because only the actor ever touches it.

### Visualization (canvas `c1`, 720×300)

Flow diagram: three sender boxes on the left fanning arrows into one mailbox queue (a horizontal strip of message slots), which feeds a single actor box holding the private counter.

- **Title (bold 15px, `#1a5276`, top center):** "Many Senders, One Mailbox, One Message at a Time".
- **Senders (left column, x=30, y = 80/145/210):** three rounded boxes 130×36, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border, 12px `#2c3e50` labels "website: Reserve 30", "dock: AddStock 40", "app: Reserve 60"; 2px `#6b7280` arrows to the mailbox.
- **Mailbox (center, x=220 to x=430, y=130, height 50):** outer rounded rect 2px `#1a5276` labeled "mailbox" (bold 12px `#1a5276` above); inside, three 60×34 slots at x = 230/295/360, fills `rgba(0,131,0,0.12)` with 2px `#008300` borders, 11px labels "AddStock 40", "Reserve 30", "Reserve 60" left-to-right in arrival order.
- **Actor (right, x=540, y=115, 150×80):** rounded box fill `rgba(74,58,167,0.10)`, 2px `#4a3aa7` border, bold 12px `#4a3aa7` label "inventory actor", 13px `#2c3e50` line "stock = 100" inside; single 3px `#008300` arrow from the front mailbox slot into the actor labeled 11px `#008300` "next".
- **Annotation (bold 13px green `#008300`, centered near y=265):** "one message in flight — the counter never needs a lock".
- **Caption (12px `#444`, bottom right):** "message contents illustrative".

## Five Messages, One Counter, Zero Races

**Tags:** `worked example` (blue), `mailbox order` (green)

- **Start** — the actor's private stock count begins at 100 units
- **M1 AddStock 40** — the dock's delivery lands first: 100 + 40 = 140
- **M2 Reserve 30** — a checkout reserves 30: 140 − 30 = 110
- **M3 Reserve 60** — another checkout takes 60: 110 − 60 = 50
- **M4 AddStock 25** — a returns bin adds 25 back: 50 + 25 = 75
- **M5 Reserve 75** — the last order takes 75: 75 − 75 = 0, never negative, nothing lost

*Example (italic):* On a shared counter, two threads both reading 110 before writing could lose a reservation — in the mailbox, M2 fully finishes before M3 ever starts, so that interleaving cannot exist.

**Key point:** Because messages are applied strictly in mailbox order, every read sees the previous write — the lost-update race that corrupts a shared counter is impossible by construction.

### Visualization (canvas `c2`, 720×300)

Step chart of the stock count as the actor processes the five messages in mailbox order, one labeled step per message.

- **Title (bold 15px, `#1a5276`, top center):** "Stock Count After Each Message, in Mailbox Order".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = processing steps "start" then "M1"–"M5" at even spacing (12px `#444` labels); y = units 0 to 150, gridlines `#e5e9ef` at 50/100/150 with 12px `#444` labels.
- **Step line:** 3px `#2a78d6` horizontal-then-vertical steps through counts `[100, 140, 110, 50, 75, 0]` at steps `[0, 1, 2, 3, 4, 5]`; 5px `#2a78d6` dots at each level with bold 12px `#1a5276` value labels "100", "140", "110", "50", "75", "0" above each dot.
- **Message labels (11px, under the x-axis labels):** "+40" and "+25" in green `#008300` at M1 and M4; "−30", "−60", "−75" in orange `#d95926` at M2, M3, M5.
- **Annotation (bold 13px violet `#4a3aa7`, near step 3, y=75):** "each step sees the previous result — no lost updates".
- **Caption (12px `#444`, bottom right):** "quantities illustrative".

## Scaling With Actors Instead of Locks

**Tags:** `where it's used` (blue), `backpressure` (green), `bounded mailbox` (orange)

- **Many actors** — throughput comes from thousands of item actors running in parallel, not from clever locks
- **No contention** — each actor serializes only its own messages; different items never wait on each other
- **The gauge** — mailbox depth is the built-in health signal: a growing queue means the actor can't keep up
- **The response** — a deep mailbox says shard the actor, slow the senders, or add consumers downstream
- **Bounded mailboxes** — a cap (say 300) drops overflow to dead letters rather than exhaust memory

*Example (italic):* An item goes viral and its actor receives 500 reserves per second while processing 400 — the mailbox grows by 100 per second, and that number is the alarm.

**Key point:** Actor systems trade lock tuning for queue watching — mailbox depth is the backpressure signal, and a bounded mailbox turns overload into visible dropped messages instead of a memory crash.

### Visualization (canvas `c3`, 720×300)

Line chart of mailbox depth over 5 seconds for an overloaded actor (arrivals 500/s, processing 400/s), with a bounded-mailbox cap line at 300 messages.

- **Title (bold 15px, `#1a5276`, top center):** "Mailbox Depth Is the Backpressure Gauge (500 in/s vs 400 out/s)".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = seconds 0 to 5, 12px `#444` tick labels every 1s; y = queued messages 0 to 500, gridlines `#e5e9ef` at 100/200/300/400 with 12px `#444` labels.
- **Unbounded line:** 3px `#d95926` line through seconds `[0, 1, 2, 3, 4, 5]`, depths `[0, 100, 200, 300, 400, 500]` — steady +100/s climb.
- **Bounded line:** 3px `#008300` line through the same seconds, depths `[0, 100, 200, 300, 300, 300]` — flattens at the cap.
- **Cap marker:** horizontal dashed `#6b7280` (dash 4/3) line at depth 300, 12px `#6b7280` label "bounded mailbox cap = 300" at its left end.
- **Labels:** bold 12px orange `#d95926` "unbounded: grows without limit" near (4s, depth 460); bold 12px green `#008300` "bounded: overflow dropped" near (4.2s, depth 265).
- **Annotation (bold 13px ink `#1a5276`, near x=1.5s, y=70):** "a growing mailbox, not a profiler, tells you the actor is overloaded".
- **Caption (12px `#444`, bottom right):** "rates and depths illustrative".

## Two Ways to Break the Guarantee

**Tags:** `common mistake` (red), `mutable message` (orange), `ordering` (blue)

- **The smuggle** — putting a mutable object inside a message hands two actors a live shared reference
- **Back to locks** — sender and receiver now both mutate the same object: the exact race actors removed
- **The fix** — send a frozen copy or plain values; the message must be a fact, not a pointer to one
- **The ordering trap** — only messages from one sender to one receiver keep their order, pairwise
- **No global clock** — messages from two different senders can arrive at the actor in either order

*Example (italic):* A sender posts a mutable order list in a message, then appends to it — the inventory actor reads the list mid-append, and the "no shared state" guarantee is quietly gone.

**Common mistake:** Treating the mailbox as a magic shield. It only protects state that stays private and messages that stay immutable — and it never promises any ordering across different senders.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: a mutable list smuggled inside a message (both sides mutate — race) vs an immutable copy sent as values (safe).

- **Title (bold 15px, `#1a5276`, top center):** "A Mutable Object in a Message Reintroduces Shared State".
- **Row 1 (y=95), label 12px `#444` at x=20:** "mutable ref"; blue `#2a78d6` rounded box at x=150 labeled "msg: pointer to list" (12px); 3px arrow to a red `#e74c3c` box at x=390 labeled "both actors mutate it" with bold 12px red "✗ race is back" at x=580.
- **Row 2 (y=205), label:** "frozen copy"; blue box at x=150 labeled "msg: {item, qty=30}", 3px arrow to a green `#008300` box at x=390 labeled "receiver owns its copy" with bold 12px green "✓ no sharing" at x=580.
- **Box style:** 160–180px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=270):** "pairwise order only: sender→receiver holds, cross-sender order does not".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the worked-example stock counts `[100, 140, 110, 50, 75, 0]` with deltas `+40, −30, −60, +25, −75`, the mailbox depths `[0, 100, 200, 300, 400, 500]` (unbounded) and `[0, 100, 200, 300, 300, 300]` (bounded, cap 300), and the 500/s-in vs 400/s-out rates are invented and labeled illustrative.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
