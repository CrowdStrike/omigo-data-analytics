# CQRS & Event Sourcing

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** CQRS & Event Sourcing

**Subtitle:** Store what happened, not what is — keep every event as an append-only log, and rebuild any "current state" you need by replaying it

## The Gift Card That Keeps a Diary

**Tags:** `core idea` (blue), `append-only log` (green), `event sourcing` (orange)

- **The card** — a coffee shop sells a gift card; most systems store one number per card: the balance
- **The overwrite** — every purchase UPDATEs that number in place, and the old value is gone forever
- **The diary** — event sourcing stores the events instead: "loaded $25", "bought latte −$5", one row each
- **The replay** — the balance is only a derived cache; the truth is recomputed by replaying the log
- **The payoff** — the log answers questions the single number cannot: when, what, in which order

*Example (italic):* Card #114's row isn't "balance: $15" — it's five dated lines: load +$25, latte −$5, muffin −$4, refund +$4, latte −$5.

**Key point:** Event sourcing keeps the full history as immutable facts and treats current state as a cached calculation — the log is the truth, the balance is derived.

### Visualization (canvas `c1`, 720×300)

Two side-by-side panels comparing the two storage styles for the same gift card: a state table holding one overwritten number vs an event log holding five dated rows.

- **Title (bold 15px, `#1a5276`, top center):** "Same Card, Two Records: One Overwritten Number vs the Full Story".
- **Left panel ("store what is"):** rounded box at x=50, y=70, width 280, height 190, fill `rgba(42,120,214,0.10)`, 2px `#2a78d6` border; bold 13px `#2a78d6` header "state table" at its top; one centered row "card #114 — balance $15" in 13px `#2c3e50`; below it, 12px `#6b7280` strikethrough ghosts "$20", "$16", "$25" stacked with the label "old values destroyed by UPDATE".
- **Right panel ("store what happened"):** rounded box at x=390, y=70, width 280, height 190, fill `rgba(0,131,0,0.08)`, 2px `#008300` border; bold 13px `#008300` header "event log (append-only)" at its top; five 12px `#2c3e50` rows at 30px spacing: "Mar 1  load  +$25", "Mar 3  latte  −$5", "Mar 5  muffin  −$4", "Mar 8  refund  +$4", "Mar 9  latte  −$5".
- **Arrow:** 2px `#6b7280` arrow from the log box to a small 12px `#1a5276` label "replay → $15" under the right panel.
- **Annotation (bold 13px green `#008300`, centered near x=280, y=285):** "the log can rebuild the number; the number can never rebuild the log".
- **Caption (12px `#444`, bottom right):** "amounts illustrative".

## Replaying the Ledger to $15

**Tags:** `worked example` (blue), `replay` (green)

- **The events** — card #114 has five: +$25 (Mar 1), −$5 (Mar 3), −$4 (Mar 5), +$4 (Mar 8), −$5 (Mar 9)
- **The fold** — start at $0 and apply each event in order: 0 → 25 → 20 → 16 → 20 → 15
- **Hand-check** — 25 − 5 − 4 + 4 − 5 = 15; anyone can redo it on paper and get the same answer
- **Time travel** — "what was the balance on Mar 6?" — replay only events up to Mar 6: $16
- **The snapshot** — long logs get a saved checkpoint so replay starts from it, not from event one

*Example (italic):* An accountant disputing the Mar 8 refund replays the log up to Mar 7 ($16) and after ($20) and sees exactly what the refund did.

**Key point:** State at any moment is a pure function of the events before it — replaying the same log always yields the same balance, so every past state is recoverable.

### Visualization (canvas `c2`, 720×300)

Step chart of the running balance as each of the five events is replayed in order, with each event labeled at its step.

- **Title (bold 15px, `#1a5276`, top center):** "Replaying Card #114: 0 → 25 → 20 → 16 → 20 → 15".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = event sequence with 12px `#444` tick labels `["start", "Mar 1", "Mar 3", "Mar 5", "Mar 8", "Mar 9"]` at even spacing; y = balance $0 to $30, gridlines `#e5e9ef` at 10/20/30 with 12px `#444` labels.
- **Step line:** blue `#2a78d6` 3px step-after line through balances `[0, 25, 20, 16, 20, 15]` at x positions `[60, 160, 260, 360, 460, 560]` (each value holds until the next event).
- **Event markers:** 5px filled circles at each step corner — green `#008300` for credits (+$25 at Mar 1, +$4 at Mar 8), orange `#d95926` for debits (−$5, −$4, −$5); 12px matching-color labels "+$25", "−$5", "−$4", "+$4", "−$5" beside each marker.
- **Time-travel marker:** vertical dashed `#6b7280` (dash 4/3) line between Mar 5 and Mar 8, 12px `#6b7280` label "Mar 6 query → $16" at its top.
- **Annotation (bold 13px violet `#4a3aa7`, near x=430, y=70):** "replay stops anywhere — every past balance is recoverable".
- **Caption (12px `#444`, bottom right):** "amounts illustrative; arithmetic exact".

## One Log, Many Read Models

**Tags:** `where it's used` (blue), `CQRS` (green), `read models` (orange)

- **The split** — CQRS separates the write side (append events) from the read side (query projections)
- **Projections** — background jobs fold the log into shaped tables: balances, daily sales, top items
- **Many views** — the same log feeds a balance lookup, a finance report, and a fraud feature table
- **For data science** — the log is training data with true timestamps; no "state at time T" guesswork
- **New questions** — a projection nobody planned for is built later by replaying the log from day one

*Example (italic):* A churn model needs "refund count in the 30 days before cancellation" — the team replays two years of events and has the feature by afternoon.

**Key point:** Writes go to one append-only log; each read model is a disposable, rebuildable fold of that log — so queries and features never require guessing what the state used to be.

### Visualization (canvas `c3`, 720×300)

Left-to-right flow diagram: commands feed the event log on the write side; three projection boxes on the read side are each fed by an arrow from the log.

- **Title (bold 15px, `#1a5276`, top center):** "CQRS: One Write Path, Three Read Models from the Same Log".
- **Command box:** rounded box at x=30, y=125, width 130, height 50, fill `rgba(42,120,214,0.15)`, 12px `#2c3e50` text "commands: buy, load, refund"; 3px `#2a78d6` arrow to the log.
- **Event log box:** rounded box at x=220, y=110, width 170, height 80, fill `rgba(26,82,118,0.12)`, 2px `#1a5276` border, bold 13px `#1a5276` label "event log (append-only)"; small 11px `#6b7280` note "the single source of truth" beneath the label.
- **Read model boxes (x=470, width 210, height 46, at y = 60, 127, 194):** green `#008300`-bordered box "balance table — card #114: $15", aqua `#199e70`-bordered box "daily sales report", violet `#4a3aa7`-bordered box "ML feature table (refunds/30d)"; each fed by a 2px arrow from the log box, arrow colors matching the box borders; fills `rgba(0,131,0,0.08)`, `rgba(25,158,112,0.08)`, `rgba(74,58,167,0.08)`, 12px `#2c3e50` text.
- **Side labels:** bold 12px `#2a78d6` "WRITE SIDE" above the command box; bold 12px `#008300` "READ SIDE" above the read model column.
- **Annotation (bold 13px aqua `#199e70`, centered near y=280):** "drop any read model and rebuild it by replay — the log never changes".
- **Caption (12px `#444`, bottom right):** "schematic; balance matches the worked example".

## You Can't Edit the Past

**Tags:** `common mistake` (red), `immutability` (orange)

- **The temptation** — the Mar 5 muffin was rung up wrong, so a developer UPDATEs the event to −$3
- **The break** — every projection built before the edit now disagrees with a fresh replay
- **The audit hole** — the log was the proof of what happened; an edited log proves nothing
- **The right fix** — append a correcting event: "Mar 10 price adjustment +$1", balance 15 → 16
- **The rule** — events are immutable facts; mistakes are corrected by new events, never rewrites

*Example (italic):* Accounting works the same way — a wrong journal entry gets a reversing entry, not an eraser.

**Common mistake:** Treating the event log like a normal table and fixing errors with UPDATE or DELETE. That silently invalidates every replay and projection — corrections must be appended as compensating events.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: fixing the −$4 muffin error by editing history (replays diverge) vs by appending a +$1 compensating event (replays agree).

- **Title (bold 15px, `#1a5276`, top center):** "Fixing a Wrong Event: Edit History vs Append a Correction".
- **Row 1 (y=95), label 12px `#444` at x=20:** "edit in place"; blue `#2a78d6` rounded box at x=140 labeled "Mar 5  muffin  −$4" (12px), 3px arrow to a red `#e74c3c` box at x=340 labeled "UPDATE to −$3", arrow to a red box at x=540 labeled "old reports disagree" with bold 12px red "✗ audit trail broken".
- **Row 2 (y=205), label:** "append correction"; blue box at x=140 "Mar 5  muffin  −$4", 3px arrow to a green `#008300` box at x=340 labeled "append Mar 10 +$1", arrow to a green box at x=540 labeled "replay → $16 everywhere" with bold 12px green "✓ history intact".
- **Box style:** 150–170px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=270):** "the log records facts; a fact can be superseded, never unsaid".
- **Caption (12px `#444`, bottom right):** "amounts illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the gift-card ledger is invented and labeled illustrative — five events (+25, −5, −4, +4, −5 on Mar 1/3/5/8/9), running balances `[0, 25, 20, 16, 20, 15]`, Mar 6 time-travel query $16, and the +$1 correction taking 15 → 16; the arithmetic is exact and must match across c1, c2, c3, and c4.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
