# How a Card Payment Flows

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** How a Card Payment Flows

**Subtitle:** A card swipe is two events, not one — a 2-second authorization that holds the money, then a capture and settlement that actually moves it days later

## The Two-Second Yes at the Table

**Tags:** `core idea` (blue), `authorization` (green), `card networks` (orange)

- **The bill** — a diner taps a credit card on a $40.00 restaurant bill at 7:42pm
- **The relay** — the terminal sends the request to the gateway/processor, then the card network
- **The issuer** — the bank that issued the card checks the balance and runs fraud checks
- **The hold** — the issuer places a $40.00 hold on the account; no money has moved yet
- **The reply** — an approval code races back down the same chain to the terminal
- **The clock** — the whole round trip finishes in about 2 seconds while the diner waits

*Example (italic):* At 7:42:00pm the card is tapped; at 7:42:02pm the terminal prints "APPROVED 07429C" — the diner's bank has promised the $40, but paid nothing.

**Key point:** Authorization is a real-time question — "will this bank honor $40?" — answered by a hold and an approval code, not by a transfer of money.

### Visualization (canvas `c1`, 720×300)

Horizontal hop diagram of the authorization round trip: five actor boxes connected by arrows, with cumulative millisecond labels showing the 2-second journey out and back.

- **Title (bold 15px, `#1a5276`, top center):** "One Authorization, Four Hops, ~2 Seconds Round Trip".
- **Actor boxes (rounded 8px, 110px wide, 44px tall, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border, 12px `#2c3e50` centered text), left to right at x = 30, 175, 320, 465, 610, all at y=110:** "Terminal (merchant)", "Gateway / Processor", "Card Network", "Issuing Bank", "Decision: hold $40".
- **The fifth box** is styled differently: fill `rgba(0,131,0,0.12)`, 2px `#008300` border — the issuer's approval step, drawn to the right of the issuing bank.
- **Forward arrows (3px `#2a78d6`, above the boxes at y=95):** between consecutive boxes, each with a 12px `#444` cumulative-time label above: "200 ms", "350 ms", "600 ms", "1,400 ms".
- **Return arrows (3px `#008300`, below the boxes at y=170, pointing left):** with 12px `#444` labels below: "1,550 ms", "1,700 ms", "1,900 ms" — the approval code returning network → processor → terminal.
- **Terminal callout (bold 13px green `#008300`, under the terminal box near y=215):** "7:42:02pm — APPROVED 07429C".
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=255):** "a hold is placed — $0.00 has actually moved".
- **Caption (12px `#444`, bottom right):** "hop timings illustrative".

## The Tip Changes the Number That Night

**Tags:** `worked example` (blue), `capture & settlement` (green), `fees` (orange)

- **The tip** — the diner writes an $8.00 tip; the final amount becomes $48.00, not the $40.00 held
- **The batch** — at 11:30pm the restaurant submits its day's captures in one batch to the processor
- **The move** — over the next 2 days money flows issuer → network → acquiring bank → merchant account
- **The interchange** — the issuer keeps 2.00% of $48.00 = $0.96 as its interchange fee
- **The assessment** — the card network keeps 0.125% of $48.00 = $0.06 as its assessment
- **The markup** — the processor/acquirer keeps 0.25% + $0.02 = $0.14; the merchant nets $46.84

*Example (italic):* Auth $40.00 on Tuesday 7:42pm, capture $48.00 in Tuesday's 11:30pm batch, and $46.84 lands in the restaurant's account on Thursday — $1.16 of fees split three ways.

**Key point:** Capture is when the amount becomes final and settlement is when money moves; each middleman takes its cut on the way, so the merchant never receives the full $48.00.

### Visualization (canvas `c2`, 720×300)

Waterfall chart of the $48.00 capture: the full amount on the left, three fee deductions stepping down, and the merchant's net deposit on the right.

- **Title (bold 15px, `#1a5276`, top center):** "Where the $48.00 Goes: $1.16 in Fees, $46.84 to the Merchant".
- **Axes:** origin x=70, baseline y=250, plot width 590, plot height 190; y = dollars 46.50 to 48.00 (zoomed to show the fee steps), gridlines `#e5e9ef` at 47.00 and 47.50 with 12px `#444` labels.
- **Bars (70px wide, centered at x = 130, 250, 370, 490, 610), values mapped onto the zoomed scale:**
  - "Capture $48.00": full bar, fill `rgba(42,120,214,0.30)`, 2px `#2a78d6` border, top at $48.00.
  - "Issuer interchange −$0.96": floating step from $48.00 down to $47.04, solid `#d95926`.
  - "Network assessment −$0.06": floating step $47.04 down to $46.98, solid `#c98500`.
  - "Processor markup −$0.14": floating step $46.98 down to $46.84, solid `#d55181`.
  - "Merchant nets $46.84": full bar, fill `rgba(0,131,0,0.30)`, 2px `#008300` border, top at $46.84.
- **Value labels:** bold 12px `#2c3e50` above each bar top ("$48.00", "−$0.96", "−$0.06", "−$0.14", "$46.84"); category labels 12px `#444` below the baseline, two lines where needed.
- **Connector lines:** dashed 1px `#6b7280` from each step's bottom to the next bar's top.
- **Annotation (bold 13px orange `#d95926`, near x=250, y=70):** "interchange is the biggest slice — it goes to the diner's bank".
- **Caption (12px `#444`, bottom right):** "rates illustrative; arithmetic exact for these rates (2.00% + 0.125% + 0.25% + $0.02 on $48.00)".

## Why the Two Phases Are Deliberately Separate

**Tags:** `where it's used` (blue), `auth vs capture` (green)

- **Tips** — restaurants authorize the bill, then capture bill-plus-tip after the diner signs
- **Hotels** — a hotel holds an estimate at check-in, then captures the real folio at check-out
- **Gas pumps** — the pump authorizes a small amount before it knows how much fuel you'll take
- **Split shipments** — online stores authorize the full order but capture per box as each one ships
- **In the data** — every purchase yields two timestamps and often two amounts; joins must pick one
- **Revenue truth** — dashboards built on auth amounts overstate hotels and understate restaurants

*Example (italic):* The same $40.00 dinner appears as a $40.00 auth event at 7:42pm and a $48.00 capture event at 11:30pm — summing both double-counts the meal.

**Key point:** Auth and capture are separate because the final amount is often unknown at swipe time — and any dataset of card events inherits that two-event, two-amount structure.

### Visualization (canvas `c3`, 720×300)

Grouped horizontal bar chart: authorized amount vs captured amount for four purchase types, showing captures above, below, and split against their auths.

- **Title (bold 15px, `#1a5276`, top center):** "Authorized vs Captured: the Two Numbers Rarely Match".
- **Layout:** row labels 12px `#444` left-aligned at x=20; bars start at x=185, max width 430 scaled so $500 = 430px; baseline vertical 2px `#999` line at x=185.
- **Rows (each row: auth bar on top, capture bar 18px below; bars 14px tall), top to bottom at y = 62, 118, 174, 230:**
  - "Restaurant": auth `rgba(42,120,214,0.30)` width 34 ($40), capture solid `#008300` width 41 ($48) — capture larger (tip added).
  - "Hotel stay": auth width 430 ($500), capture width 398 ($463) — capture smaller (estimate high).
  - "Gas pump": auth width 1 ($1), capture width 33 ($38) — tiny pre-auth, real fuel amount captured.
  - "Split shipment": auth width 77 ($90), capture drawn as two solid `#008300` segments widths 52 ($60) and 26 ($30) separated by a 3px gap.
- **Bar-end labels:** 11px `#444` dollar amounts at each bar's right end ("$40", "$48", "$500", "$463", "$1", "$38", "$90", "$60 + $30").
- **Legend (top right, y=45):** 12px swatches — `rgba(42,120,214,0.30)` "authorized", solid `#008300` "captured".
- **Annotation (bold 13px magenta `#d55181`, near x=380, y=285):** "one purchase, two amounts — pick the capture for revenue".
- **Caption (12px `#444`, bottom right):** "amounts illustrative".

## An Authorization Is Not Money Moved

**Tags:** `common mistake` (red), `holds` (orange)

- **The confusion** — "the charge went through" at auth time; in fact only a hold exists
- **Holds expire** — an uncaptured hold simply falls off after a few days; no money ever moves
- **Captures differ** — the captured amount can legitimately exceed or undercut the hold (the tip)
- **Pending ≠ paid** — the diner's app shows "$40.00 pending" Tuesday, "$48.00 posted" Thursday
- **The mistake** — counting auth events as revenue, or flagging auth/capture gaps as data errors

*Example (italic):* If the restaurant's terminal dies before the 11:30pm batch, the $40.00 hold expires days later and the restaurant is never paid — despite 900 "APPROVED" receipts.

**Common mistake:** Treating an approval code as a completed payment. Authorization is a reservation; until a capture settles, the merchant has revenue on paper and $0.00 in the bank.

### Visualization (canvas `c4`, 720×300)

Two-row timeline over 7 days comparing the same $40.00 authorization when it is captured vs when it is never captured, tracking what actually lands in the merchant's account.

- **Title (bold 15px, `#1a5276`, top center):** "Two Fates of the Same $40 Hold: Captured vs Expired".
- **Shared axis:** horizontal 2px `#999` line at y=255 from x=60 to x=680; 12px `#444` tick labels "Day 0" through "Day 7" every day (ticks every ~88px).
- **Row 1 (track line y=105, label bold 12px `#008300` at x=20: "captured"):** blue `#2a78d6` dot at Day 0 labeled 12px "auth $40 (hold)"; green `#008300` dot at Day 0 + 4h labeled "capture $48"; green filled square at Day 2 labeled bold 12px `#008300` "merchant paid $46.84"; connecting 3px `#008300` line between the events.
- **Row 2 (track line y=185, label bold 12px `#e74c3c` at x=20: "never captured"):** blue dot at Day 0 labeled "auth $40 (hold)"; dashed 2px `#6b7280` (dash 4/3) line continuing to Day 7; red `#e74c3c` X marker at Day 7 labeled bold 12px `#e74c3c` "hold expires — merchant paid $0.00".
- **Hold band:** light band `rgba(42,120,214,0.10)` behind row 2 from Day 0 to Day 7 labeled 11px `#6b7280` "cardholder sees 'pending'".
- **Annotation (bold 13px red `#e74c3c`, centered near y=40):** "an approval code pays nobody — only a settled capture does".
- **Caption (12px `#444`, bottom right):** "hold window illustrative; varies by network and merchant type".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays and amounts above (no randomness); hop timings, hold windows, and the hotel/gas/split-shipment amounts are invented and labeled illustrative; the fee arithmetic is exact for the stated illustrative rates — on a $48.00 capture, interchange 2.00% = $0.96, network assessment 0.125% = $0.06, processor markup 0.25% + $0.02 = $0.14, total fees $1.16, merchant net $46.84. Text numbers and chart numbers must match ($40.00 auth, $48.00 capture, $46.84 net, 2-second auth, funds on Day 2).
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
