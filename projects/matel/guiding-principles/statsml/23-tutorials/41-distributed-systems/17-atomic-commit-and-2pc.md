# Atomic Commit & 2PC

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Atomic Commit & 2PC

**Subtitle:** Two-phase commit makes separate databases act like one — every side promises first, then a single decision commits them all, but a dead coordinator can freeze everyone mid-promise

## One Order, Two Databases: Both or Neither

**Tags:** `core idea` (blue), `all or nothing` (green), `atomic commit` (orange)

- **The order** — order #4712 must subtract 2 espresso machines from stock AND record a $180 charge
- **Two databases** — inventory and payments run on different servers, each with its own commit log
- **The danger** — inventory commits, then payments crashes: stock is gone but the customer never paid
- **The rule** — atomic commit means both databases apply the order, or neither does — no half-states
- **The coordinator** — one extra process asks both sides to agree before anything becomes permanent

*Example (italic):* A power blip between the two writes leaves 2 machines missing from stock with no matching $180 payment — exactly the half-state atomic commit exists to prevent.

**Key point:** Atomic commit makes several independent databases behave like one: a single all-or-nothing decision covers every write in the order.

### Visualization (canvas `c1`, 720×300)

Two-row flow diagram: the same order applied without atomic commit (one side lands, one side fails — half-state) vs with atomic commit (both land together or both roll back).

- **Title (bold 15px, `#1a5276`, top center):** "Order #4712 Touches Two Databases: the Half-State Problem".
- **Row 1 (y=95), label 12px `#444` at x=20:** "no atomic commit"; blue `#2a78d6` rounded box at x=170 labeled "order #4712" (12px), 3px arrow to a green `#008300` box at x=360 labeled "inventory −2 ✓", 3px arrow to a red `#e74c3c` box at x=550 labeled "payments $180 ✗ crash" with bold 12px red "stock gone, no payment" beneath.
- **Row 2 (y=205), label:** "atomic commit"; blue box "order #4712" at x=170, 3px arrow to a green box at x=360 labeled "inventory −2 ✓", 3px arrow to a green box at x=550 labeled "payments $180 ✓" with bold 12px green "one decision covers both" beneath.
- **Box style:** 140–170px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(0,131,0,0.12)` / `rgba(231,76,60,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=270):** "both writes, or neither — never one".
- **Caption (12px `#444`, bottom right):** "order and dollar amounts illustrative".

## Prepare, Vote, Commit — Message by Message

**Tags:** `worked example` (blue), `prepare/commit` (green), `in-doubt` (red)

- **Phase 1: prepare** — at 0ms the coordinator sends PREPARE; each database logs the change and locks its rows
- **The vote** — inventory answers YES at 10ms, payments YES at 12ms; one NO would abort both sides
- **The decision** — at 20ms the coordinator writes COMMIT to its own log — that single write IS the outcome
- **Phase 2: commit** — the coordinator sends COMMIT; both databases apply, release locks, ACK by 30ms
- **The crash** — coordinator dies at 15ms, after the votes but before the decision: both sides are in-doubt
- **No unilateral exit** — a YES voter can't abort alone; the coordinator may already have logged COMMIT

*Example (italic):* Both databases voted YES by 12ms; the coordinator crashed at 15ms, so neither can tell whether order #4712 committed until the coordinator comes back and reads its log.

**Key point:** The point of no return is one local log write on the coordinator — everything before it is revocable, everything after it must happen everywhere.

### Visualization (canvas `c2`, 720×300)

Message-sequence diagram: three vertical lifelines (Coordinator, Inventory, Payments), time flowing downward, with the prepare/vote/commit messages of order #4712 and a shaded band marking the crash window where participants get stuck in-doubt.

- **Title (bold 15px, `#1a5276`, top center):** "Two Phases for Order #4712 — and the 15ms Crash Window".
- **Lifelines:** vertical 2px `#1a5276` lines at x=160 ("Coordinator"), x=420 ("Inventory"), x=620 ("Payments"), bold 13px `#1a5276` labels at y=55; lifelines run y=65 to y=275; time labels 12px `#6b7280` on far left at x=25: "0ms" (y=95), "10–12ms" (y=135), "20ms" (y=185), "30ms" (y=235).
- **Prepare arrows (y=95):** two 2px blue `#2a78d6` arrows from coordinator to inventory and to payments, 12px blue label "PREPARE" above the first arrow.
- **Vote arrows:** 2px green `#008300` arrow inventory→coordinator at y=130 labeled "YES (10ms)", 2px green arrow payments→coordinator at y=140 labeled "YES (12ms)" (12px green labels).
- **Crash band:** shaded `rgba(231,76,60,0.10)` horizontal band from y=150 to y=175 across the full plot, dashed `#e74c3c` (dash 4/3) top edge, bold 12px red `#e74c3c` label at x=250, y=165: "coordinator crash here → both stuck in-doubt, locks held".
- **Decision marker (y=185):** small filled violet `#4a3aa7` square (10×10) on the coordinator lifeline, bold 12px violet label "log COMMIT — point of no return" to its right.
- **Commit arrows (y=205):** two 2px aqua `#199e70` arrows from coordinator to both databases, 12px aqua label "COMMIT".
- **Ack arrows (y=235):** two 2px `#6b7280` arrows back to the coordinator, 12px mute label "ACK — locks released".
- **Caption (12px `#444`, bottom right):** "timings illustrative".

## Where 2PC Lives: XA, Sharded Databases, and Why Microservices Skip It

**Tags:** `where it's used` (blue), `XA` (orange), `microservices` (green)

- **XA** — the X/Open XA standard wires 2PC into Java app servers, message queues, and classic ERPs
- **Inside databases** — sharded SQL databases still run 2PC between shards, hidden from the user
- **Microservices** — teams avoid 2PC across services: locks held over HTTP couple everyone's uptime
- **The saga instead** — services commit locally and compensate later (refund the $180 if stock fails)
- **The cost** — a 2PC commit needs 2 round trips and extra log flushes: ~20ms vs ~5ms for one database
- **The trade** — you buy all-or-nothing across machines; you pay latency and a blocking failure mode

*Example (italic):* An orders service calling inventory and payments over HTTP uses a saga with a compensating refund — no team wants its rows locked while another team's service is down.

**Key point:** 2PC survives inside tightly-operated systems — XA middleware and the shards of one distributed database — and is avoided between independently-run services.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart comparing the cost of committing order #4712 three ways: a single-database commit, a healthy 2PC commit, and a 2PC commit whose coordinator dies in-doubt (unbounded wait).

- **Title (bold 15px, `#1a5276`, top center):** "What a Commit Costs: One Database vs 2PC vs 2PC In-Doubt".
- **Axis:** vertical 2px `#999` baseline at x=250, bars extend right, max width 420; x is time (not to scale for the last bar).
- **Rows (bar centers at y = 90, 150, 210), each with a left-aligned 12px `#444` label at x=20:**
  - "single database — 1 log flush": green `#008300` bar width 90, 11px `#444` value label "5 ms" at bar end
  - "2PC, both healthy — 4 messages": blue `#2a78d6` bar width 250, value label "20 ms"
  - "2PC, coordinator dead in-doubt": orange `#d95926` bar width 420 with a jagged/dashed right end, bold 12px red `#e74c3c` label "unbounded — blocked until coordinator returns"
- **Bar style:** 22px tall, fills `rgba(0,131,0,0.30)` / `rgba(42,120,214,0.30)` / `rgba(217,89,38,0.35)` with solid 2px edges in the same hues.
- **Annotation (bold 13px magenta `#d55181`, right side near y=255):** "sagas trade locks for a visible compensation step".
- **Caption (12px `#444`, bottom right):** "latencies illustrative; last bar schematic".

## Atomic, Not Available: Why 2PC Is Called Blocking

**Tags:** `common mistake` (red), `blocking` (orange), `locks` (blue)

- **The guarantee** — 2PC gives atomicity: order #4712 is never half-applied, no matter what crashes
- **The gap** — it does not give availability: in-doubt participants must wait, holding their locks
- **Locks pile up** — the locked payment row blocks every new order that touches the same accounts
- **The math** — coordinator down 40s at 3 orders/sec means 120 orders queued behind in-doubt locks
- **No timeout fix** — aborting on timeout risks contradicting a COMMIT the coordinator already logged
- **The label** — this is why 2PC is called a blocking protocol; consensus-based commit attacks exactly this

*Example (italic):* A 40-second coordinator reboot freezes checkout — 120 orders queue behind one in-doubt transaction — yet not a single order ends up half-applied.

**Common mistake:** Reading "atomic" as "reliable". 2PC never corrupts the databases, but it can stop the world: correctness is guaranteed, progress is not.

### Visualization (canvas `c4`, 720×300)

Timeline chart of a 40-second coordinator outage: a shaded outage band while locks are held, a line of blocked orders climbing to 120, then a fast drain once the coordinator recovers and resolves the in-doubt transaction.

- **Title (bold 15px, `#1a5276`, top center):** "40 Seconds In-Doubt: Zero Corruption, 120 Blocked Orders".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = seconds 0 to 50, 12px `#444` tick labels every 10s; y = blocked orders 0 to 120, gridlines `#e5e9ef` at 30/60/90.
- **Outage band:** shaded `rgba(217,89,38,0.12)` from x=0s to x=40s, dashed orange `#d95926` (dash 4/3) right edge at 40s, 12px orange label "coordinator down — locks held" near the band top.
- **Blocked-orders line:** blue `#2a78d6` 3px line through seconds `[0, 10, 20, 30, 40]`, orders `[0, 30, 60, 90, 120]` (3 orders/sec pile-up), then aqua `#199e70` 3px drain segment through seconds `[40, 44, 48]`, orders `[120, 60, 0]`.
- **Recovery marker:** vertical dashed `#6b7280` line at 40s, 12px `#6b7280` label "coordinator back, reads log, sends COMMIT" at its top.
- **Annotation (bold 13px red `#e74c3c`, near x=18s, y=80):** "atomicity kept, availability lost".
- **Annotation (bold 12px green `#008300`, near x=44s, y=200):** "queue drains, still zero half-applied orders".
- **Caption (12px `#444`, bottom right):** "order rates illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); order #4712, the 2-machine / $180 writes, message timings (PREPARE 0ms, YES 10/12ms, crash 15ms, COMMIT logged 20ms, ACK 30ms), commit latencies (5 ms / 20 ms / unbounded), and the outage pile-up `[0,30,60,90,120]` with drain `[120,60,0]` are invented and labeled illustrative; text numbers must stay in sync with these arrays.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
