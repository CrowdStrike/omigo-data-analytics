# Stripe

**Page type:** detail page (h2 "Overview" then single obj-table row: text left 45%, payload + canvas right 55%; verified badge under subtitle; "Official API References" section below)
**HTML title tag:** Stripe — Platform APIs

**Subtitle:** Lets you pull payments, subscriptions, invoices, refunds, and payouts out of Stripe.

**Verified badge:** Last verified: August 2026

## Overview

### What you can get

- Every payment attempt and how it ended — succeeded, failed, refunded
- Subscriptions, invoices, and their line items
- Disputes (chargebacks) and their status
- Payouts and per-transaction fees — the layer that ties to your bank statement
- A running log of every change (events), also delivered as webhooks

**Key point (callout, red left border `#e74c3c`):** Payment data keeps changing after the fact. A charge that succeeded today can be refunded next week and disputed months later, so revenue for a recent period is always biased upward — the deductions just have not arrived yet. Compare periods at equal age since transaction, and label anything inside the dispute window as provisional.

### Watch out for

- A subscription row shows only where the customer is now. The downgrade in March, the churn in May, the win-back in July — that story exists only in the event log.
- Amounts are integers in the currency's smallest unit. Dividing everything by 100 is wrong for currencies like yen that have no decimals.
- The event log is kept for about a month, not forever. If you want history, store it yourself as it arrives.
- There is no ready-made revenue or MRR number — you compute it, and summing charges conflates annual prepayment with monthly revenue.

### Sample payload

**Payload note (italic):** A balance transaction — the reconciliation layer. Net = amount minus fee, and it tells you which payout it settled into.

```json
{
  "id": "txn_3PqR2sK9LmNoP4Qr1Hj",
  "object": "balance_transaction",
  "amount": 4900,
  "fee": 172,
  "net": 4728,
  "currency": "usd",
  "type": "charge",
  "status": "available",
  "available_on": 1756051200,
  "created": 1755792000,
  "source": "ch_3PqR2sK9LmNoP4Qr1AbCdEfG"
}
```

### Visualization (canvas `subChart`, responsive width × 380)

State-transition diagram (node-and-arrow graph) of Stripe subscription statuses; solid arrows are transitions observable from the current subscription row, dashed purple curved arrows are transitions only recoverable from the event log.

- **Title (bold 13px `#1a5276`, top center):** "Subscription states — what current state shows vs what the event log shows".
- **Subtitle (italic 10px `#888`):** "Solid = observable from the subscription row. Dashed = only recoverable from events."
- **Nodes (label / relative position x,y in plot area / dot color, radius 8, label 11px `#2c3e50` above dot):**
  - "trialing" (0.10, 0.30) `#e67e22`
  - "active" (0.38, 0.30) `#27ae60`
  - "past_due" (0.66, 0.16) `#e67e22`
  - "unpaid" (0.88, 0.16) `#e74c3c`
  - "canceled" (0.66, 0.62) `#e74c3c`
  - "incomplete" (0.10, 0.62) `#8e44ad`
  - "incomplete_expired" (0.10, 0.86) `#e74c3c`
- **Edges (all with arrowheads):**
  - Solid `rgba(26,82,118,0.55)` width 2: trialing→active, active→past_due, past_due→unpaid, active→canceled, incomplete→active, incomplete→incomplete_expired.
  - Dashed `#8e44ad` width 1.6 (dash 5/4), drawn as quadratic curves offset from the straight line, with italic 10px purple midpoint labels: past_due→active labeled "recovered", canceled→active labeled "win-back".
- **Layout:** plot area 66px horizontal padding, 60px top margin, 74px bottom margin; arrowheads filled in edge color (`rgba(26,82,118,0.7)` for solid).
- **Footer (11px `#666`, two lines, left-aligned):** "A customer who went past_due, recovered, cancelled, and returned reads" / "simply \"active\" today. Net position hides both the churn and the save."

## Official API References

- [Stripe API Reference](https://docs.stripe.com/api) — full REST reference for charges, payment intents, subscriptions, invoices, disputes, payouts, balance transactions, and events
- [Webhooks](https://docs.stripe.com/webhooks) — event delivery, signing, retries, and ordering caveats

## Regeneration instructions

- **Layout:** platform-APIs detail page. h1, `.subtitle`, `.verified` badge span, h2 "Overview", then one `.obj-table` (full width, border-collapse) with a single `<tr>`: left `<td>` (45%) holds two `<span class="section-label">` headings ("What you can get", "Watch out for") with bullet lists and a `.key-point` callout (inline style `border-left-color:#e74c3c`) between them; right `<td>` (55%) holds `.payload-note`, `<pre class="payload">`, and `<canvas id="subChart" height="380">`. Below the table, an h2 "Official API References" with a link list.
- **Page CSS:** body system sans-serif, line-height 1.6, color `#2c3e50`, padding 30px 40px, white background; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; `.verified` 0.8em `#888`, 1px `#ddd` border, inline-block, padding 2px 10px, radius 4px; h2 `#1a5276` 1.3em with 2px `#2980b9` bottom border; `.section-label` bold `#1a5276`, block, margin-top 16px (0 for first); `.obj-table td` 16px padding, `1px solid #e0e0e0` border, vertical-align top; li 0.93em; p 0.93em; links `#1a5276`; `pre.payload` `#f8f9fa` background, 3px `#1a5276` left border, monospace 0.78em, pre whitespace, radius 4px, left-aligned; `.payload-note` 0.82em `#666` italic; `.key-point` `#f8f9fa` background, 3px `#1a5276` left border (overridden to `#e74c3c` on this page), padding 10px 14px, 0.93em; canvas block, `width: 100%`, margin-top 12px.
- **Palette:** `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange, `#8e44ad` purple, `rgba(26,82,118,0.55)` solid edge blue.
- **Canvas:** responsive — width from `canvas.getBoundingClientRect().width`, fixed 380px CSS height, backing store sized to rendered width × `window.devicePixelRatio` (display capped via `style.maxWidth`), `ctx.setTransform` reset then `ctx.scale` back to logical coordinates, redrawn on window resize.
- Note: in regenerated HTML, any card/page links use `.html` extensions.
