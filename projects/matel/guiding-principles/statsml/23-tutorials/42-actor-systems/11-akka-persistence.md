# Akka Persistence

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Akka Persistence

**Subtitle:** A persistent actor never saves its current state — it saves every event that changed the state, and rebuilds itself by replaying them after a crash

## The Account That Writes Its Diary, Not Its Balance

**Tags:** `core idea` (blue), `event sourcing` (green), `Akka` (orange)

- **The actor** — a bank-account actor holds one customer's balance in memory: 1,100 right now
- **The old way** — save the number 1,100 to a database; yesterday's 950 is overwritten and gone
- **The persistent way** — the actor writes each change as an event: "Deposited 300", "Withdrawn 150"
- **The journal** — events are appended to a write-only log; nothing is ever updated or deleted
- **The state** — the in-memory balance is just a cache; the journal is the real source of truth

*Example (italic):* When a customer deposits 300, the actor appends the event "Deposited 300" to the journal first, and only then bumps its in-memory balance from 800 to 1,100.

**Key point:** A persistent actor persists what happened, not what it currently is — state becomes a derived value you can always recompute from the event log.

### Visualization (canvas `c1`, 720×300)

Flow diagram: a Deposit command enters the actor, the actor appends an event to a growing journal strip, then updates its in-memory balance.

- **Title (bold 15px, `#1a5276`, top center):** "Persist the Event, Then Update the State".
- **Command box:** blue `#2a78d6` rounded box at (x=30, y=110), 160×44, fill `rgba(42,120,214,0.15)`, 12px `#2c3e50` text "command: Deposit 300".
- **Actor circle:** ink `#1a5276` 2px circle, center (300, 132), radius 42, bold 12px label "account actor" inside, 11px `#6b7280` "balance: 800 → 1,100" just below the circle.
- **Arrow 1:** 3px `#2a78d6` arrow from command box to actor circle.
- **Journal strip:** five cells at y=220 (tail of the 8-event journal), each 118×38, 6px radius, starting x=60 with 10px gaps; first cell fill `rgba(107,114,128,0.10)` with 11px `#2c3e50` label "… earlier events"; next three fill `rgba(0,131,0,0.12)` with labels "Withdrawn 300", "Deposited 250", "Withdrawn 100"; fifth cell fill `rgba(0,131,0,0.30)` with bold 11px label "Deposited 300" and bold 11px green `#008300` "appended" above it.
- **Arrow 2:** 3px green `#008300` arrow from the actor circle down to the fifth journal cell, 12px green label "1. persist event" beside it.
- **Arrow 3:** 2px `#6b7280` curved arrow looping from the actor circle back to itself, 12px `#6b7280` label "2. apply to balance" at (x≈420, y≈80).
- **Annotation (bold 13px violet `#4a3aa7`, near x=520, y=250):** "the journal only grows — no overwrites".
- **Caption (12px `#444`, bottom right):** "amounts illustrative".

## Crash, Restart, Replay

**Tags:** `worked example` (blue), `replay` (green)

- **The journal** — 8 events on file: +500, +200, −150, +400, −300, +250, −100, +300 (deposits and withdrawals)
- **The crash** — the server dies at 2:14pm; the in-memory balance of 1,100 vanishes with it
- **The restart** — the actor comes back empty and reads its journal from event 1 onward
- **The fold** — it re-applies each event in order: 500, 700, 550, 950, 650, 900, 800, 1,100
- **Hand-check** — total deposits 1,650 minus total withdrawals 550 = 1,100, same as before the crash

*Example (italic):* After replaying all 8 events the rebuilt balance is exactly 1,100 — the actor resumes as if the crash never happened.

**Key point:** Recovery is just a fold over the journal — replay every event in order and the state comes back exactly, because the events are the state's complete history.

### Visualization (canvas `c2`, 720×300)

Step chart of the balance being rebuilt during replay, one step per journal event, ending at the pre-crash value.

- **Title (bold 15px, `#1a5276`, top center):** "Replaying 8 Events Rebuilds the Balance: 0 → 1,100".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = event number 1 to 8 with 12px `#444` tick labels "e1"–"e8"; y = balance 0 to 1,200, gridlines `#e5e9ef` at 300/600/900/1200.
- **Step line:** blue `#2a78d6` 3px step line through balances `[500, 700, 550, 950, 650, 900, 800, 1100]` at events 1–8, starting from 0 before e1; 5px blue dots at each step.
- **Event labels:** 11px `#6b7280` signed amounts above each dot: "+500", "+200", "−150", "+400", "−300", "+250", "−100", "+300".
- **Target line:** horizontal dashed `#008300` (dash 4/3) line at y for 1,100, 12px green label "pre-crash balance 1,100" at its right end.
- **Annotation (bold 13px green `#008300`, near event 7, y=70):** "last replayed event lands exactly on 1,100".
- **Caption (12px `#444`, bottom right):** "amounts illustrative".

## Snapshots Keep Restarts Fast

**Tags:** `where it's used` (blue), `snapshots` (green), `rule of thumb` (orange)

- **The problem** — a busy account collects 100 events a day; after 500 days the journal holds 50,000 events
- **The cost** — replaying 50,000 events at 20,000 events/sec makes every restart wait about 2.5 seconds
- **The snapshot** — every 1,000 events the actor also saves its current balance as a checkpoint
- **The shortcut** — recovery loads the latest snapshot, then replays only the events after it: at most 999
- **The bonus** — the full journal still exists, so auditors can see every deposit ever made

*Example (italic):* With snapshots every 1,000 events, the 50,000-event account restarts by loading one snapshot and replaying at most 999 events — about 0.05 seconds instead of 2.5.

**Key point:** Snapshots are an optimization, not the source of truth — they bound replay time to the events since the last checkpoint, while the journal keeps the complete history.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: events replayed at restart, without snapshots (grows with journal size) vs with a snapshot every 1,000 events (capped).

- **Title (bold 15px, `#1a5276`, top center):** "Events Replayed at Restart: Snapshot Every 1,000 Caps the Bill".
- **Axis:** horizontal 2px `#999` baseline at x=250, bars extend right, max width 420; linear scale where 50,000 events = 420px.
- **Rows (top to bottom at y = 70, 115, 160, 205), each with a left-aligned 12px `#444` label at x=20:**
  - "journal 5,000 events": blue `#2a78d6` bar width 42 (no snapshot)
  - "journal 20,000 events": blue bar width 168
  - "journal 50,000 events": orange `#d95926` bar width 420 labeled "2.5s replay" (11px)
  - "any size, with snapshots": green bar width 8 with bold 12px green label "≤999 events ≈ 0.05s"
- **Bar style:** 16px tall, no-snapshot bars fill `rgba(42,120,214,0.30)` (orange row `rgba(217,89,38,0.35)`), the with-snapshots row's bar solid green `#008300` width 8, 11px event-count labels at bar ends.
- **Annotation (bold 13px magenta `#d55181`, right side near y=255):** "replay cost stops growing with the journal".
- **Caption (12px `#444`, bottom right):** "counts and 20,000 events/sec rate illustrative".

## Persist the Fact, Not the Request

**Tags:** `common mistake` (red), `command vs event` (orange)

- **The distinction** — a command is a request ("Withdraw 150") that may be refused; an event is a fact ("Withdrawn 150") that already happened
- **The rule** — validate the command first, and persist only the event it produces
- **The mistake** — persisting commands, so recovery re-runs validation on every restart
- **The breakage** — if the overdraft rule changed since, replay now rejects an old withdrawal and rebuilds a different balance
- **The fix** — event handlers apply facts blindly and never validate; all decisions live in the command handler

*Example (italic):* An account journal holds a "Withdraw 150" command from last year; after the overdraft limit is tightened, replay rejects it and the rebuilt balance is 150 too high — history has silently changed.

**Common mistake:** Persisting commands instead of events. Replay must be pure bookkeeping — if recovery can make decisions, the same journal can rebuild two different states.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: replaying a persisted command through changed validation (wrong balance) vs replaying a persisted event (same balance).

- **Title (bold 15px, `#1a5276`, top center):** "Replay a Command vs Replay an Event".
- **Row 1 (y=95), label 12px `#444` at x=20:** "persisted command"; blue `#2a78d6` rounded box at x=170 labeled "Withdraw 150 (request)" (12px), 3px arrow to an orange `#d95926` box at x=380 labeled "re-validate: new rule says no", 3px arrow to a red `#e74c3c` box at x=580 labeled "balance 1,250" with bold 12px red "✗ history changed".
- **Row 2 (y=205), label:** "persisted event"; blue box at x=170 labeled "Withdrawn 150 (fact)", 3px arrow to a green `#008300` box at x=380 labeled "apply blindly: −150", 3px arrow to a green box at x=580 labeled "balance 1,100" with bold 12px green "✓ same as before".
- **Box style:** 150–180px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(217,89,38,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=270):** "decide once in the command handler; replay only records the decision".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); event amounts `[+500, +200, −150, +400, −300, +250, −100, +300]` and running balances `[500, 700, 550, 950, 650, 900, 800, 1100]` must match between text and charts; journal sizes, snapshot interval 1,000, and the 20,000 events/sec replay rate are invented and labeled illustrative.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
