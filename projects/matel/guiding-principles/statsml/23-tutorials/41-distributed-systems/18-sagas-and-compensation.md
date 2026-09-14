# Sagas & Compensation

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Sagas & Compensation

**Subtitle:** Split one big distributed transaction into a chain of small local ones — and if a later step fails, run each step's cancel action in reverse order instead of holding locks

## Booking a Trip Without a Global Lock

**Tags:** `core idea` (blue), `no 2PC` (green), `microservices` (orange)

- **The trip** — a travel app books a flight, a hotel, and a car through three separate services
- **Three local commits** — each service commits its own database transaction and immediately moves on
- **The failure** — the car service finds no cars; the flight and hotel are already booked and charged
- **The compensation** — the saga cancels the hotel, then the flight, in reverse booking order
- **No 2PC** — at no moment did any coordinator hold locks across all three services at once

*Example (italic):* The $420 flight and the $540 hotel book fine; when the $190 car fails, both are canceled and the traveler ends up charged $0.

**Key point:** A saga replaces one distributed transaction with a chain of local transactions, each paired with a compensating action that runs if a later step fails.

### Visualization (canvas `c1`, 720×300)

Flow diagram: three forward booking steps left to right, with dashed compensation arrows flowing back right to left after the car step fails.

- **Title (bold 15px, `#1a5276`, top center):** "One Trip, Three Services: Forward Steps and Backward Compensations".
- **Forward row (boxes 160×44, 8px radius, centers at y=110):** green-bordered `#008300` box at x=60 "1. BookFlight ✓ $420" (fill `rgba(0,131,0,0.12)`), green box at x=280 "2. BookHotel ✓ $540", red `#e74c3c` box at x=500 "3. BookCar ✗ no cars" (fill `rgba(231,76,60,0.12)`); 3px `#2a78d6` arrows between boxes, 12px `#2c3e50` box text.
- **Compensation row (dashed arrows, dash 5/4, 2px `#d95926`, at y=200):** arrow from below the car box back to a small orange box at x=290 "CancelHotel −$540", then on to an orange box at x=70 "CancelFlight −$420" (fills `rgba(217,89,38,0.12)`, 12px text); 11px `#6b7280` labels "reverse order" along the arrows.
- **Fail marker:** bold 13px red `#e74c3c` "✗ step 3 fails" just above the car box.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=260):** "failure flows backward through compensations — no locks were ever shared".
- **Caption (12px `#444`, bottom right):** "prices illustrative".

## The Saga Log: Book, Fail, Unwind

**Tags:** `worked example` (blue), `saga log` (green), `orchestration` (orange)

- **The log** — a coordinator writes each step before running it: T1 BookFlight, T2 BookHotel, T3 BookCar
- **Forward pairs** — every action registers its compensation up front: BookHotel ↔ CancelHotel
- **The turn** — T3 fails at 4s, so the log flips from forward mode into compensation mode
- **Reverse replay** — T4 CancelHotel refunds $540 at 5s, T5 CancelFlight refunds $420 at 7s
- **Orchestration** — one central coordinator reads the log and calls each service in turn
- **Choreography** — the alternative: each service listens for the previous step's event; no central brain

*Example (italic):* The card charge peaks at $960 after the hotel books at 2s, then steps back down to $0 as the two compensations run.

**Key point:** The saga log makes recovery restartable — if the coordinator crashes mid-unwind, it replays the log on restart and finishes exactly the compensations still pending.

### Visualization (canvas `c2`, 720×300)

Step chart of the running amount charged to the traveler's card over the 8-second saga, with each log entry marked on the line.

- **Title (bold 15px, `#1a5276`, top center):** "Running Charge on the Card: $0 → $960 → $0 in Five Log Entries".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = seconds 0 to 8 with 12px `#444` tick labels every 2s; y = dollars charged 0 to 1000, gridlines `#e5e9ef` at 250/500/750 with labels "$250/$500/$750".
- **Step line:** blue `#2a78d6` 3px right-angle step line through seconds `[0, 2, 4, 5, 7, 8]`, charged `[420, 960, 960, 420, 0, 0]` (each change is a vertical jump at that second).
- **Event markers:** green `#008300` 5px dots at (0s, $420) and (2s, $960) with 12px green labels "T1 BookFlight +$420" and "T2 BookHotel +$540"; red `#e74c3c` × at (4s, $960) with bold 12px red "T3 BookCar fails"; orange `#d95926` dots at (5s, $420) and (7s, $0) with 12px orange labels "T4 CancelHotel −$540" and "T5 CancelFlight −$420".
- **Annotation (bold 13px aqua `#199e70`, near x=5.5s, y=80):** "peak exposure $960 lasts just 3 seconds".
- **Caption (12px `#444`, bottom right):** "timings illustrative; dollar steps match the log".

## Why Long Business Flows Can't Hold Locks

**Tags:** `where it's used` (blue), `long-running` (green), `eventual consistency` (orange)

- **Long flows** — a booking waits 15 minutes for payment confirmation; a loan approval waits days
- **2PC cost** — two-phase commit locks rows in every service until the slowest participant votes
- **Blocked neighbors** — while those locks are held, other customers touching the same rows must wait
- **Saga cost** — each local transaction holds its own lock about 0.2 s; three steps total 0.6 s
- **The trade** — the saga is eventually consistent: for 3 seconds the trip was half-booked on purpose

*Example (italic):* With 2PC, a 900-second payment wait pins locks in all three services; the saga's locks total 0.6 s and nobody else is blocked.

**Key point:** Sagas keep services available during long-running flows by never holding a lock while waiting on another service — the price is a visible window of temporary inconsistency instead of atomicity.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart comparing total lock hold time for the same 15-minute booking flow under 2PC vs a saga's three local transactions.

- **Title (bold 15px, `#1a5276`, top center):** "Lock Hold Time for One 15-Minute Booking Flow".
- **Axis:** horizontal 2px `#999` baseline at x=250, bars extend right, max width 420; log-feel achieved by hardcoded pixel widths, not a real log axis.
- **Rows (top to bottom at y = 70, 120, 170, 220), each with a left-aligned 12px `#444` label at x=20:**
  - "2PC — locks held 900 s in all 3 services": red `#e74c3c` bar width 420, 11px red label "900 s" at bar end
  - "Saga — flight txn 0.2 s": blue `#2a78d6` bar width 26, 11px `#444` label "0.2 s"
  - "Saga — hotel txn 0.2 s": blue bar width 26, label "0.2 s"
  - "Saga — car txn 0.2 s": blue bar width 26, label "0.2 s"
- **Bar style:** 14px tall, saga bars fill `rgba(42,120,214,0.30)` with solid 1px `#2a78d6` edge, 2PC bar fill `rgba(231,76,60,0.25)` with solid red edge.
- **Annotation (bold 13px magenta `#d55181`, right side near y=250):** "the saga never holds a lock while waiting on another service".
- **Caption (12px `#444`, bottom right):** "pixel widths schematic; seconds exact for this example".

## Canceling Is Not Ctrl-Z

**Tags:** `common mistake` (red), `idempotency` (orange), `visible state` (blue)

- **Not undo** — the hotel confirmation email went out at 2s; canceling at 5s cannot unsend it
- **Visible window** — for 3 seconds, any other system could read a fully confirmed hotel booking
- **Semantic reversal** — CancelHotel is a new business action (a refund), not a rollback of bytes
- **Idempotency** — a retried CancelHotel must refund $540 exactly once, never $1,080
- **Compensable design** — every forward step needs a defined counter-action before it ships

*Example (italic):* A crash makes the coordinator retry CancelHotel; keyed by booking id bk-712, the second call is a no-op and the refund stays $540.

**Common mistake:** Treating compensation as a database rollback. The intermediate state was already visible and acted on — compensation is a forward-moving business action, so every step must be idempotent and compensable by design.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: a naive cancel retried after a crash double-refunds, vs an idempotent cancel keyed by booking id that refunds once.

- **Title (bold 15px, `#1a5276`, top center):** "Retry Safety: the Same Cancel Must Refund Only Once".
- **Row 1 (y=95), label 12px `#444` at x=20:** "naive cancel"; orange `#d95926` rounded box at x=160 labeled "CancelHotel −$540" (12px), 3px arrow with 11px `#6b7280` label "crash → retry" to a red `#e74c3c` box at x=420 labeled "CancelHotel −$540 again" with bold 12px red "✗ refunded $1,080".
- **Row 2 (y=205), label:** "idempotent cancel"; orange box at x=160 "CancelHotel key=bk-712 −$540", 3px arrow labeled "crash → retry" to a green `#008300` box at x=420 labeled "key bk-712 seen — no-op" with bold 12px green "✓ refunded $540".
- **Box style:** 170–190px wide, 40px tall, 8px radius, fills `rgba(217,89,38,0.12)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=270):** "a compensation is a business action — it must survive being run twice".
- **Caption (12px `#444`, bottom right):** "refund amounts illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); prices ($420 flight, $540 hotel, $190 car), the charge steps `[420, 960, 960, 420, 0, 0]` at seconds `[0, 2, 4, 5, 7, 8]`, saga timings, and the 900 s / 0.2 s lock-hold comparison are invented and labeled illustrative/schematic; the double-refund figure $1,080 is exactly 2 × $540.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
