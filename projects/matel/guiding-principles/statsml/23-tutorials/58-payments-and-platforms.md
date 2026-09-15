# Payments & Platforms

**Page type:** grid page (tutorials category grid: single flat 4-column nav-grid of cards with topic tags)
**HTML title tag:** Payments & Platforms

**Subtitle:** How money actually moves when you pay — the card rails, the bank rails, the platforms stitching them together, and what happens when a payment goes wrong.

## Cards

Each card links to a topic page under `payments-platforms/`. The card shows a colored uppercase subcategory label (`.card-num`), a numbered title, a one-line description, and 2-4 topic tag pills. All cards sit in one flat `.nav-grid`; the colored labels carry the grouping.

| # | Category | Title | Link | Description | Topic tags |
|---|----------|-------|------|-------------|------------|
| 1 | CARD RAILS | How a Card Payment Flows | [58-payments-and-platforms/01-how-a-card-payment-flows.md](58-payments-and-platforms/01-how-a-card-payment-flows.md) | A card swipe is two events, not one — a 2-second authorization that holds the money, then a capture and settlement that moves it days later. | authorization, capture & settlement, two events |
| 2 | CARD RAILS | The Card Networks | [58-payments-and-platforms/02-the-card-networks.md](58-payments-and-platforms/02-the-card-networks.md) | Visa and Mastercard own the rails, not the money — they route the messages between banks while the dollars move bank to bank. | rails not money, four parties, message routing |
| 3 | CARD RAILS | Processors & Gateways | [58-payments-and-platforms/03-processors-and-gateways.md](58-payments-and-platforms/03-processors-and-gateways.md) | A gateway captures the card and a processor moves the money — modern providers sell both, plus the merchant account, behind one API. | gateway, processor, one API |
| 4 | BEYOND THE CARD | Bank-to-Bank Rails | [58-payments-and-platforms/04-bank-to-bank-rails.md](58-payments-and-platforms/04-bank-to-bank-rails.md) | ACH, SEPA, UPI, and Pix move money straight from one bank account to another — no card network in the middle, and the new rails do it in seconds. | account to account, ACH / UPI / Pix, instant rails |
| 5 | BEYOND THE CARD | Wallets & Tokenization | [58-payments-and-platforms/05-wallets-and-tokenization.md](58-payments-and-platforms/05-wallets-and-tokenization.md) | When you tap your phone to pay, the wallet hands the merchant a stand-in number — the real card number never leaves the bank's side of the wall. | tokenization, digital wallets, stand-in number |
| 6 | WHEN PAYMENTS GO WRONG | Chargebacks & Payment Fraud | [58-payments-and-platforms/06-chargebacks-and-payment-fraud.md](58-payments-and-platforms/06-chargebacks-and-payment-fraud.md) | When a cardholder disputes a charge, the money comes straight back out of the merchant's account — and blocking fraud too aggressively costs even more than the fraud itself. | chargebacks, disputes, false declines |

## Regeneration instructions

To rebuild the HTML from this spec:

- **Template:** tutorials category grid. Single page: h1, `.subtitle` paragraph, then one flat `.nav-grid` of `.nav-card` anchors (no h2 section headings).
- **Layout:** `.nav-grid` is CSS grid, `repeat(4, 1fr)`, 16px gap, 15px top margin; responsive: 3 columns below 1400px, 2 below 1100px, 1 below 600px.
- **Links:** the tables above link to the `.md` versions for markdown navigation; in the regenerated HTML, each card's `href` is the same path with an `.html` extension instead.
- **Card structure:** `<a class="nav-card" href="...">` containing `<div class="card-num">SUBCATEGORY LABEL</div>`, `<h3>N. Topic Title</h3>` (unpadded index number matching the 2-digit zero-padded file index), `<p>description</p>`, then `<div class="topics">` of `<span class="topic-tag">` pills.
- **Category label colors:** applied by a small script mapping `.card-num` text to color — "CARD RAILS" `#2980b9`, "BEYOND THE CARD" `#27ae60`, "WHEN PAYMENTS GO WRONG" `#8e44ad`; the CSS default for `.card-num` is `#2980b9`, 0.75em bold, 4px bottom margin.
- **Card style:** background `#ffffff`, border `1px solid #d8d8d8`, radius 10px, padding 20px, shadow `0 2px 4px rgba(0,0,0,0.05)`, transition on border-color/transform; hover: border `#2980b9`, `translateY(-2px)`. h3 `#1a3a4a` 1em with 6px bottom margin; description `#555` 0.85em. Topic tags: background `#f0f0f0`, border `1px solid #ccc`, radius 4px, padding 2px 6px, 0.7em `#666`, in a flex-wrap row with 4px gap and 8px top margin.
- **Page style:** body system sans-serif, background `#f5f5f0`, text `#2a2a2a`, padding 40px, line-height 1.6; universal `* { margin:0; padding:0; box-sizing:border-box }` reset; h1 1.8em `#2980b9` with 10px bottom margin; subtitle `#666` 1.05em with 30px bottom margin. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22` (page accents here use `#2980b9`, `#27ae60`, `#8e44ad`).
- **Canvases:** none on this page; any canvases elsewhere in this series use `window.devicePixelRatio` scaling.
