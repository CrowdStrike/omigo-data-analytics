# Payment System

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Payment System

**Subtitle:** Money must never be wrong — a payment system trades speed and availability for one property above all others: every dollar is accounted for exactly once

## The $50 Charge That Times Out

**Tags:** `core idea` (blue), `idempotency` (green), `retries` (orange)

- **The order** — a customer clicks Pay on a $50.00 order; the app sends the charge to the card processor
- **The timeout** — 30 seconds pass with no reply; did the charge happen? The app has no idea
- **The naive retry** — send the charge again; if the first one landed, the customer pays $100
- **The idempotency key** — every attempt carries the same key `ord-7841-pay`; the processor stores its result
- **The safe retry** — a repeat with the same key replays the recorded $50 result instead of charging again

*Example (italic):* The retry at 30s carries key `ord-7841-pay`; the processor sees it already charged $50 and returns the original receipt — the card is charged exactly once.

**Key point:** With an idempotency key, a retried request becomes the question "what happened to my first request?" instead of a second charge — retries turn from dangerous into safe.

### Visualization (canvas `c1`, 720×300)

Two-row flow diagram: a timed-out $50 charge retried without an idempotency key (double charge) vs with one (single charge replayed).

- **Title (bold 15px, `#1a5276`, top center):** "Retrying a Timed-Out $50 Charge: No Key vs Idempotency Key".
- **Row 1 (y=95), label 12px `#444` at x=20:** "no key"; blue `#2a78d6` rounded box at x=150 labeled "charge $50 — timeout" (12px), 3px arrow to a blue box at x=340 labeled "retry: charge $50", arrow to a red `#e74c3c` box at x=530 labeled "both landed" with bold 12px red "✗ customer pays $100".
- **Row 2 (y=205), label:** "key ord-7841-pay"; blue box at x=150 "charge $50 — timeout", arrow to a blue box at x=340 labeled "retry: same key", arrow to a green `#008300` box at x=530 labeled "first result replayed" with bold 12px green "✓ charged $50 once".
- **Box style:** 150–170px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=270):** "same key = same answer, never a second charge".
- **Caption (12px `#444`, bottom right):** "amounts illustrative".

## Every Dollar Written Twice: the Ledger

**Tags:** `worked example` (blue), `double entry` (green), `immutable` (orange)

- **The rule** — every movement of money is two rows: a debit in one account, a matching credit in another
- **Capture** — debit customer card $50.00, credit merchant payable $50.00
- **Fee** — debit merchant payable $1.75, credit processor fees $1.75
- **Payout** — debit merchant payable $48.25, credit merchant bank $48.25
- **Hand-check** — merchant payable: +50.00 − 1.75 − 48.25 = 0.00; every debit has an equal credit
- **Derived balances** — a balance is never stored as truth; it is the sum over an append-only ledger

*Example (italic):* To show the merchant's balance the system sums three immutable rows — 50.00 − 1.75 − 48.25 = 0.00 — instead of trusting a mutable number that could have been corrupted.

**Key point:** The immutable double-entry ledger is the source of truth; balances are cached sums that can always be rebuilt from it, so a lost dollar shows up as an entry that doesn't balance — it cannot vanish silently.

### Visualization (canvas `c2`, 720×300)

Ledger diagram: three entry rows for the $50 order, each drawn as a debit box and a credit box, with a running merchant-payable balance column on the right.

- **Title (bold 15px, `#1a5276`, top center):** "One $50 Order = Three Balanced Ledger Entries".
- **Column headers (bold 12px `#1a5276`, y=60):** "debit" at x=90, "credit" at x=310, "merchant payable" at x=560.
- **Rows (top edges at y = 80, 145, 210), each: left 12px `#444` label at x=20 ("capture" / "fee" / "payout"), a blue `#2a78d6` rounded box at x=90 and a green `#008300` rounded box at x=310, box text 12px `#2c3e50`:**
  - capture: debit "customer card $50.00", credit "merchant payable $50.00", balance text bold 13px `#1a5276` at x=560: "$50.00"
  - fee: debit "merchant payable $1.75", credit "processor fees $1.75", balance "$48.25"
  - payout: debit "merchant payable $48.25", credit "merchant bank $48.25", balance "$0.00"
- **Box style:** 190px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` (debit) / `rgba(0,131,0,0.12)` (credit); thin 2px `#6b7280` arrow from each debit box to its credit box.
- **Annotation (bold 13px green `#008300`, centered near y=275):** "debits = credits on every row — the balance is derived, not stored".
- **Caption (12px `#444`, bottom right):** "$1.75 fee illustrative".

## A Payment Is a State Machine

**Tags:** `where it's used` (blue), `state machine` (green), `webhooks` (orange)

- **The states** — created → authorized → captured → settled, with failed and refunded as branch exits
- **One-way doors** — only legal transitions are allowed: you cannot refund a payment that was never captured
- **The webhook** — the processor confirms settlement asynchronously, hours later; the state machine waits
- **UNKNOWN is a state** — a timeout moves the payment to unknown; a background job queries until it resolves
- **Correctness beats availability** — better to show "processing" for a minute than to guess and be wrong

*Example (italic):* The timed-out $50 payment sits in unknown for 40 seconds until a status query returns "captured" — no guessing, and the order ships exactly once.

**Key point:** Modeling every payment as an explicit state machine — with UNKNOWN as a first-class state resolved by querying, never by assuming — is what makes retries, webhooks and refunds safe to run concurrently.

### Visualization (canvas `c3`, 720×300)

State machine diagram: the payment lifecycle as boxes and arrows, with the unknown state shown as a dashed box that resolves by querying the processor.

- **Title (bold 15px, `#1a5276`, top center):** "Payment Lifecycle: Every Transition Is Explicit".
- **Main path (boxes centered at y=150, left to right at x = 90, 240, 390, 560):** blue `#2a78d6` rounded boxes "created", "authorized", "captured", green `#008300` box "settled"; 3px `#2c3e50` arrows between them, 11px `#6b7280` label "webhook" on the captured→settled arrow.
- **Failure branch:** red `#e74c3c` box "failed" centered at (240, 245); 2px red arrow down from "authorized".
- **Refund branch:** orange `#d95926` box "refunded" centered at (560, 245); 2px orange arrow down from "settled".
- **Unknown state:** dashed-border `#6b7280` box "unknown" centered at (390, 60); 2px dashed `#6b7280` arrow up from "authorized" (the in-flight capture that timed out) labeled 11px "timeout", and a curved 2px `#199e70` return arrow labeled bold 12px aqua "query processor" back down to "captured".
- **Box style:** 110px wide, 38px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(0,131,0,0.12)` / `rgba(231,76,60,0.12)` / `rgba(217,89,38,0.12)`, unknown box fill `rgba(107,114,128,0.10)`, 12px `#2c3e50` text.
- **Annotation (bold 13px violet `#4a3aa7`, bottom left near x=60, y=285):** "no arrow, no transition — refund needs capture first".

## Timeout Is Not Failure — Reconciliation Catches Drift

**Tags:** `common mistake` (red), `reconciliation` (green)

- **The mistake** — treating a timeout as a failure: marking the $50 payment failed while the card was charged
- **Silent drift** — the internal ledger now disagrees with the processor by $50 and nothing crashes or alerts
- **Reconciliation** — a nightly job compares the internal ledger against the processor's report, line by line
- **The catch** — Wednesday's totals differ by exactly $50; the job flags the payment and it is repaired
- **Feed vs money** — a stale feed self-heals in seconds; a stored wrong balance is an incident, not a delay

*Example (italic):* Internal Wednesday total $12,880 vs the processor's $12,930 — the $50 gap is the timed-out charge that was wrongly marked failed.

**Common mistake:** Assuming no reply means no charge. A timeout's outcome is UNKNOWN — the system must query or reconcile, never assume, because eventual consistency is fine for a feed but money that is wrong stays wrong until someone finds it.

### Visualization (canvas `c4`, 720×300)

Bar chart of the nightly reconciliation result: processor report total minus internal ledger total for each weekday — flat at zero except one $50 gap.

- **Title (bold 15px, `#1a5276`, top center):** "Nightly Reconciliation: Processor Total − Internal Ledger, by Day".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = five weekday bars labeled "Mon"–"Fri" (12px `#444`, centered under each bar); y = difference $0 to $60, gridlines `#e5e9ef` at $20 and $40 with 12px `#444` labels.
- **Bars (70px wide, centered at x = 130, 245, 360, 475, 590):** differences `[0, 0, 50, 0, 0]` — Mon/Tue/Thu/Fri drawn as 3px-tall gray `#6b7280` stubs on the baseline with 11px `#6b7280` "$0" labels; Wed a solid red `#e74c3c` bar of full $50 height with bold 12px red "$50" label on top.
- **Annotation (bold 13px red `#e74c3c`, near x=360, y=70 with a short 2px red pointer line to the Wed bar):** "the timed-out charge, marked failed internally".
- **Caption (12px `#444`, bottom right):** "totals illustrative — internal Wed $12,880 vs processor $12,930".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all values are the hardcoded numbers above (no randomness); the $50.00 charge, $1.75 fee, $48.25 payout and the daily totals ($12,880 / $12,930, differences `[0, 0, 50, 0, 0]`) are invented and labeled illustrative; the ledger identity 50.00 − 1.75 − 48.25 = 0.00 must hold exactly in both text and chart.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
