# Logical Clocks

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Logical Clocks

**Subtitle:** When machines can't agree what time it is, they can still agree what happened first — by counting events instead of reading wall clocks

## Three Coffee Shops Whose Clocks Disagree

**Tags:** `core idea` (blue), `event counters` (green), `Lamport timestamps` (orange)

- **The chain** — three branches (Downtown, Airport, Mall) share one stock ledger over messages
- **The drift** — Downtown's register clock reads 2:00:07 at the moment Airport's reads 1:59:58
- **The paradox** — a stock note sent at Downtown's 2:00:07 lands at Airport's 1:59:58, "before" it was sent
- **The fix** — each register keeps a plain counter and ticks it up by 1 at every event it performs
- **The stamp** — every outgoing note carries the sender's counter; the receiver takes max(local, stamp) + 1
- **The payoff** — a reply's number is always bigger than the note it answers, no matter what the clocks say

*Example (italic):* Downtown sends a note stamped 2; Airport's counter is 0, so it sets max(0, 2) + 1 = 3 — the receive is numbered after the send even though Airport's wall clock says otherwise.

**Key point:** A logical clock is just a counter: tick before every event, stamp every message, and jump to max + 1 on receive — that alone guarantees causes get smaller numbers than their effects.

### Visualization (canvas `c1`, 720×300)

Two-row flow diagram contrasting the same message under wall clocks (arrives before it was sent) and under counters (receive always numbered after send).

- **Title (bold 15px, `#1a5276`, top center):** "Same Note, Two Ways to Timestamp It".
- **Row 1 (y=105), label 12px `#6b7280` at x=20:** "wall clocks"; blue `#2a78d6` rounded box at x=150 labeled "Downtown sends at 2:00:07" (12px), 3px mute `#6b7280` arrow to a red `#e74c3c` box at x=430 labeled "Airport clock: 1:59:58" with bold 12px red "✗ arrived before it was sent?" beneath.
- **Row 2 (y=215), label:** "counters"; blue box at x=150 labeled "Downtown stamps counter 2", 3px arrow to a green `#008300` box at x=430 labeled "Airport: max(0, 2) + 1 = 3" with bold 12px green "✓ receive numbered after send" beneath.
- **Box style:** 200px wide, 42px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=272):** "count events, don't trust clocks".
- **Caption (12px `#444`, bottom right):** "wall-clock times illustrative; counter values 0, 2, 3 exact by Lamport's rules".

## Numbering Nine Events by Hand

**Tags:** `worked example` (blue), `max + 1 rule` (green), `three processes` (orange)

- **The rules** — tick +1 before every local event; a send carries the new value; a receive takes max(local, stamp) + 1
- **Downtown (A)** — local event 1, sends note m1 stamped 2, local event 3, later receives m3
- **Airport (B)** — receives m1 (stamp 2) with counter 0: max(0, 2) + 1 = 3, then sends m2 stamped 4
- **Mall (C)** — local event 1, receives m2 (stamp 4): max(1, 4) + 1 = 5, then sends m3 stamped 6
- **Back to A** — Downtown receives m3 (stamp 6) with counter 3: max(3, 6) + 1 = 7
- **Hand-check** — every message arrow goes from a smaller number to a bigger one: 2→3, 4→5, 6→7

*Example (italic):* Mall's counter jumps from 1 straight to 5 when the stamp-4 note arrives — max(1, 4) + 1 = 5 — skipping 2, 3, 4 entirely; gaps are normal.

**Key point:** Apply the two rules mechanically and the numbering is forced: A gets 1, 2, 3, 7; B gets 3, 4; C gets 1, 5, 6 — anyone redoing it by hand gets exactly these values.

### Visualization (canvas `c2`, 720×300)

Space-time diagram: three horizontal process lines with numbered event circles and diagonal message arrows, every event stamped by Lamport's rules.

- **Title (bold 15px, `#1a5276`, top center):** "Nine Events, Three Messages, One Consistent Numbering".
- **Process lines:** horizontal 2px `#e5e9ef` lines from x=90 to x=690 at y=100 (Downtown A), y=170 (Airport B), y=240 (Mall C); 12px bold labels at x=20 in the process color.
- **Events:** circles radius 14, 2px colored stroke, white fill, bold 13px timestamp centered inside, colored per process — A blue `#2a78d6`: 1 at x=140, 2 at x=230, 3 at x=330, 7 at x=620; B aqua `#199e70`: 3 at x=310, 4 at x=390; C orange `#d95926`: 1 at x=160, 5 at x=470, 6 at x=540.
- **Message arrows (2.5px magenta `#d55181`, arrowheads):** m1 from (230,100) to (310,170); m2 from (390,170) to (470,240); m3 from (540,240) to (620,100); 11px magenta labels "m1 (stamp 2)", "m2 (stamp 4)", "m3 (stamp 6)" beside each arrow midpoint.
- **Annotation (bold 13px green `#008300`, near x=470, y=272):** "receive at Mall: max(1, 4) + 1 = 5".
- **Caption (12px `#444`, bottom right):** "all nine numbers exact — forced by tick +1 and max(local, stamp) + 1".

## One Order That Every Replica Agrees On

**Tags:** `where it's used` (blue), `versioning` (green), `consensus` (orange)

- **The problem** — real server clocks drift and jump; NTP narrows the gap but never closes it
- **Last-write-wins** — replicated stores that pick winners by wall clock can keep a stale write and drop a newer one
- **With Lamport** — a write made after seeing another always carries a bigger stamp, so the true order survives
- **Total order** — break ties on equal stamps by process name and every replica sorts all events identically
- **Consensus** — Raft's term numbers and Paxos ballot numbers are the same trick: counters that only move forward
- **Versioning** — "which update is newer" in replicated databases is a logical-clock question, not a clock question

*Example (italic):* Airport's follow-up write carries stamp 4 while Downtown's original carries stamp 2, so every replica keeps the follow-up — even though Airport's wall clock (1:59:59) says it came first.

**Key point:** Logical clocks turn "what time was it?" into "what had this machine seen?" — which is the question versioning and consensus actually need answered.

### Visualization (canvas `c3`, 720×300)

Two-row flow diagram: the same two writes ordered by wall clock (stale value wins) vs by Lamport stamp (the follow-up wins on every replica).

- **Title (bold 15px, `#1a5276`, top center):** "Two Writes to One Stock Row: Which One Wins?".
- **Writes (referenced in both rows):** write X = Downtown "beans: 40" (wall 2:00:07, stamp 2); write Y = Airport "beans: 25", made after seeing X (wall 1:59:59, stamp 4).
- **Row 1 (y=105), label 12px `#6b7280` at x=20:** "order by wall clock"; box "Y @ 1:59:59" at x=170, 3px mute arrow to box "X @ 2:00:07" at x=360, arrow to red `#e74c3c` box at x=550 labeled "keeps beans: 40" with bold 12px red "✗ newer write lost".
- **Row 2 (y=215), label:** "order by Lamport stamp"; box "X stamp 2" at x=170, arrow to box "Y stamp 4" at x=360, arrow to green `#008300` box at x=550 labeled "keeps beans: 25" with bold 12px green "✓ same on every replica".
- **Box style:** 130–150px wide, 42px tall, 8px radius, neutral boxes fill `rgba(42,120,214,0.15)` with 12px `#2c3e50` text; result boxes `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=272):** "stamps 2 and 4 come straight from the diagram above".
- **Caption (12px `#444`, bottom right):** "wall-clock times illustrative; stamps 2 and 4 exact from the worked example".

## A Smaller Timestamp Doesn't Mean It Happened First

**Tags:** `common mistake` (red), `concurrency` (orange), `vector clocks` (blue)

- **One direction only** — if event e caused f then stamp(e) < stamp(f); the reverse is not true
- **The trap** — Mall's local event has stamp 1 and Downtown's third event has stamp 3, yet neither caused the other
- **Equal stamps** — Downtown's event 3 and Airport's event 3 share a number, so they are certainly concurrent
- **Can't detect** — from Lamport stamps alone you cannot tell "happened before" apart from "unrelated"
- **The upgrade** — vector clocks keep one counter per process, so incomparable vectors expose concurrency

*Example (italic):* Vector clocks stamp Downtown's third event [3, 0, 0] and Mall's first event [0, 0, 1]; neither vector dominates the other, so the events are provably concurrent — Lamport's 3 vs 1 only hinted at an order that never existed.

**Common mistake:** Reading Lamport timestamps as a causality detector. They are consistent with causality (causes always get smaller numbers) but they cannot certify it — sorting by stamp silently invents an order between concurrent events; detecting concurrency needs vector clocks.

### Visualization (canvas `c4`, 720×300)

Side-by-side comparison of one event pair under Lamport stamps (misleading arrow) and vector clocks (concurrency exposed).

- **Title (bold 15px, `#1a5276`, top center):** "Stamp 1 vs Stamp 3: Ordered Numbers, Concurrent Events".
- **Left half (centered x≈190), header bold 13px `#1a5276` at y=70:** "Lamport"; blue `#2a78d6` rounded box at (110, 105) labeled "Mall event — stamp 1", orange `#d95926` box at (110, 185) labeled "Downtown event — stamp 3", dashed 2px red `#e74c3c` arrow (dash 5/4) from the first to the second with bold 12px red label "1 < 3 ... happened first?" beside it.
- **Right half (centered x≈530), header:** "Vector clocks"; blue box at (450, 105) labeled "Mall — [0, 0, 1]", orange box at (450, 185) labeled "Downtown — [3, 0, 0]", no arrow, bold 12px green `#008300` label "neither ≤ the other → concurrent" between them.
- **Divider:** vertical 1px `#e5e9ef` line at x=360 from y=60 to y=250.
- **Box style:** 200px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(217,89,38,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px magenta `#d55181`, centered near y=272):** "Lamport can rank them; only vectors can say 'unrelated'".
- **Caption (12px `#444`, bottom right):** "stamps 1 and 3, vectors [0,0,1] and [3,0,0] exact for the diagram above".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all values are the hardcoded literals above (no randomness); the Lamport timestamps (A: 1, 2, 3, 7; B: 3, 4; C: 1, 5, 6), message stamps (2, 4, 6), and vector clocks ([3,0,0], [0,0,1]) are exact results of Lamport's rules on the c2 diagram; wall-clock times (2:00:07, 1:59:58, 1:59:59) and stock values (beans 40 / 25) are invented and labeled illustrative.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
