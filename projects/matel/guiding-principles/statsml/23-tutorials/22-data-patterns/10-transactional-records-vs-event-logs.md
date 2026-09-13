# Transactional Records vs Event Logs

**Page type:** detail page (tutorial card-section layout: one h2 per section, two-column `table.layout` with 50% text / 50% viz)
**HTML title tag:** Transactional Records vs Event Logs

**Subtitle:** A record stores the current state of a thing; an event log stores every step that led there — two views of the same reality

## One Bank Balance, Two Ways to Store It

**Tags:** `core idea` (blue), `running example` (green)

- **The record** — one row: account 8321, balance $480 — what is true right now
- **The event log** — five dated transactions, one row each, oldest first, never edited
- **Same reality** — replay the five transactions from $0 and you land on exactly $480
- **The trade** — the record is instant to read; the log remembers how you got there
- **Names you'll hear** — "state table" vs "transaction history", "snapshot" vs "audit trail"

**Example (italic):** Your banking app shows the record ($480) on top and the event log (the transaction list) below it.

**Key point:** The record answers "what is it now?"; the event log answers "how did it get there?" — both describe the same account.

### Shared dataset (used across all four canvases)

| date | what | amount | balance after | amount color |
|------|------|--------|---------------|--------------|
| Aug 1 | paycheck | +$500 | 500 | `#008300` (green) |
| Aug 4 | electricity | -$120 | 380 | `#d55181` (magenta) |
| Aug 9 | groceries | -$40 | 340 | `#d55181` (magenta) |
| Aug 15 | refund | +$200 | 540 | `#008300` (green) |
| Aug 20 | phone bill | -$60 | 480 | `#d55181` (magenta) |

### Visualization (canvas `c1`, 720×300)

Split-panel diagram: the record (one box) on the left vs the event log (five stacked rows) on the right; vertical dashed divider `#bdc3c7` at x=320.

- **Title (bold 15px ink `#1a5276`, top center):** "Account 8321: the Record vs the Event Log"
- **Left panel header (bold 13px blue `#2a78d6`, centered x=160):** "THE RECORD — current state"
  - Record box (45, 105), 230×74, fill `rgba(42,120,214,0.07)`, blue border; 13px `#444` "account_id: 8321", bold 16px blue "balance: $480".
  - 12px mute `#6b7280`: "one row, overwritten on every change"; bold 13px orange `#d95926`: "fast to read — no memory".
- **Right panel header (bold 13px green `#008300`, centered x=520):** "THE EVENT LOG — every step, append-only"
  - Five stacked 300×30 rows starting at (370, 72) with 4px gaps, alternating fills `rgba(0,131,0,0.08)` / `rgba(0,131,0,0.04)`, green borders; each row shows date and description left-aligned in 12px `#444` and the amount right-aligned bold 12px in its color (green for +, magenta for −): the five shared-dataset events.
  - Bold 13px orange caption: "replay these 5 rows from $0 → exactly $480".
- **Bottom caption (bold 12px ink, center, y=292):** "two views of the same account"

## Replaying the Log by Hand to Rebuild the Balance

**Tags:** `worked example` (green), `core idea` (blue)

- **Start** — the account opens Aug 1 at $0, then apply each event in date order
- **Aug 1 paycheck** — 0 + 500 = $500
- **Aug 4 electricity** — 500 − 120 = $380
- **Aug 9 groceries** — 380 − 40 = $340
- **Aug 15 refund** — 340 + 200 = $540, then **Aug 20 phone** — 540 − 60 = $480

**Example (italic):** Five additions on paper and you have rebuilt the bank's record: $480, no peeking needed.

**Key point:** Current state = starting state + every event replayed in order. The record is always derivable from the log.

### Visualization (canvas `c2`, 720×300)

Step chart of the running balance during the replay.

- **Title (bold 15px ink, top center):** "Replaying the 5 Events: the Balance After Each Step"
- **Axes:** x = days of August 0–22 (event days 1, 4, 9, 15, 20 labeled with their dates below the axis); y = $0 to $600 with gridlines (`#e5e9ef`) and labels every $200 ("$0", "$200", "$400", "$600", 12px `#666`); axis lines `#999`; padding: top 55, bottom 55, left 65, right 30.
- **Step line:** blue `#2a78d6`, 3px, starting at (day 0, $0), stepping vertically at each event day to the new balance and extending flat to day 22. Balances after each step: 500, 380, 340, 540, 480.
- **Event dots:** 5px filled circles at each (day, balance) in the event's amount color (green `#008300` for deposits, magenta `#d55181` for debits), each labeled with its amount (bold 12px; labels placed below the dot for events 2, 3 and 5, above otherwise).
- **Final-state annotation (bold 13px orange `#d95926`, right-aligned near the line end):** "ends at $480 — the record, rebuilt"
- **Caption (12px mute, bottom center, y=292):** "the record is just the last point of this line"

## Which Questions Need Which View

**Tags:** `where it's used` (blue), `trade-off` (orange)

- **Record wins** — "can this $50 payment go through?" reads one row in a millisecond
- **Log wins** — "was the electricity bill charged twice?" needs the history, not the total
- **Features live in the log** — spend per week, deposit regularity, days since last salary
- **Debugging lives in the log** — a wrong balance is explained by finding the bad event
- **Most systems keep both** — apps read the record; analytics and audit read the log

**Example (italic):** A churn model built only on current balances missed that half the leavers had stopped depositing months earlier.

**Key point:** Data scientists usually get handed the record but need the log — most predictive signal is in the steps, not the end state.

### Visualization (canvas `c3`, 720×300)

Routing diagram: four question boxes on the left, arrows to the correct store box on the right.

- **Title (bold 15px ink, top center):** "Route the Question to the Right Store"
- **Destination boxes (170×56 at x=520):**
  - "THE RECORD" (bold 13px blue `#2a78d6`) with "1 row, instant" (12px `#444`) at y=60, fill `rgba(42,120,214,0.08)`, blue border.
  - "THE EVENT LOG" (bold 13px green `#008300`) with "full history" (12px `#444`) at y=180, fill `rgba(0,131,0,0.07)`, green border.
- **Question boxes (330×38 at x=30, fill `#fdfdfd`, border in the destination's color, 12px `#444` centered text), each with a colored arrow to its destination:**
  | y-center | question | routes to |
  |----------|----------|-----------|
  | 66 | Can this $50 payment go through? | record (blue) |
  | 116 | Was electricity charged twice? | log (green) |
  | 166 | Average weekly spend? (feature) | log (green) |
  | 216 | Why is the balance wrong? (debug) | log (green) |
- **Captions (bottom center):** bold 13px orange `#d95926` "3 of these 4 questions can only be answered by the log" (y=272); 12px mute '"when", "how often", "why" questions all need history' (y=292).

## The Confusion: You Cannot Recover the Log From the Record

**Tags:** `common mistake` (red), `rule of thumb` (blue)

- **One-way street** — log → record is a replay; record → log is impossible
- **Many histories** — countless different transaction lists all end at exactly $480
- **Overwritten = gone** — a record that only stores "now" destroys "before" on every update
- **Common regret** — "we'll add history later" fails: the past was never written down
- **Rule of thumb** — if a question starts with "when" or "how often", the record cannot answer it

**Example (italic):** Two accounts both show $480 today — one saved steadily, one nearly went broke twice; the record can't tell them apart.

**Key point:** Keep the events and you can always rebuild the state; keep only the state and the history is gone for good.

### Visualization (canvas `c4`, 720×300)

Two-line chart: two very different balance histories converging on the same final value.

- **Title (bold 15px ink, top center):** "Two Very Different Histories, One Identical Record"
- **Axes:** y = $0 to $900 with gridlines every $300 and 12px `#666` labels ("$0", "$300", "$600", "$900"); axis lines `#999`; x = 10 evenly spaced points (months); padding: top 55, bottom 60, left 65, right 165.
- **Series (both end at 480; balances illustrative):**
  - "steady saver" — solid blue `#2a78d6`, 3px: `[200, 240, 280, 310, 350, 380, 410, 440, 460, 480]`
  - "rollercoaster" — dashed (7/5) orange `#d95926`, 3px: `[200, 700, 60, 520, 30, 850, 120, 640, 90, 480]`
- **Shared endpoint:** 7px ink `#1a5276` dot at the final point, labeled bold 13px "both records: $480" to its right.
- **Legend (top right, 12px `#222` with line swatches):** solid blue "steady saver"; dashed orange "rollercoaster".
- **Captions (bottom center):** bold 13px magenta `#d55181` "the record alone cannot tell these two apart — the log can" (y=272); 12px mute "months (balances illustrative)" (y=292, centered on the plot area).

## Regeneration instructions

- **Template:** tutorials topic-page skeleton. `<h1>` (no index number) with 2px `#2980b9` bottom border, `.subtitle` gray line, then four `.card-section` blocks. Each section: `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with `td.text-col` (50%) holding tags/bullets/example/key-point and `td.viz-col` (50%) holding the canvas. No payload blocks on this page.
- **Text column structure:** `.tags` row of colored pill spans (0.72rem bold, radius 10px — blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`); 5 one-line bullets each opening with `<b>` in `#1a5276`; one italic `.example` line (`#555`, 0.9rem); one `.key-point` callout (background `#f8f9fa`, left border 3px solid `#e74c3c`).
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; canvases `width:100%` with 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvases:** intrinsic 720×300; shared `setup(id)` helper scales backing store by `window.devicePixelRatio` and calls `ctx.scale` so drawing stays in logical coordinates; shared `boxAt` and `arrowTo` helpers; a shared `EVENTS` array (the 5-event dataset above) drives c1 and c2. All data hardcoded.
- **Chart palette object `P`:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- In regenerated HTML, any card/grid links use `.html` extensions (this page has none — no cross-page links).
