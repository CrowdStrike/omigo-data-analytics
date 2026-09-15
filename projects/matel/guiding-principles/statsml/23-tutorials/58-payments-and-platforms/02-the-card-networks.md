# The Card Networks

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** The Card Networks

**Subtitle:** Visa and Mastercard own the rails, not the money — they route the messages between banks while the dollars move bank to bank

## The $100 Purchase That Never Touches Visa

**Tags:** `core idea` (blue), `rails not money` (green), `four parties` (orange)

- **The purchase** — Maya buys a $100 espresso machine at a kitchen shop and taps her Visa card
- **Four parties** — Maya's bank issued the card, the shop's bank collects payments, Visa sits between
- **The message** — Visa routes the shop's authorization request to Maya's bank in under a second
- **The money** — the $100 moves from Maya's bank to the shop's bank at settlement; Visa never holds it
- **Not a bank** — Visa and Mastercard hold no deposits, issue no cards, and lend no money to anyone

*Example (italic):* Maya's $100 never passes through Visa — her bank pays the shop's bank; Visa only carried the messages saying "approve" and "settle".

**Key point:** A card network is a switching network: it connects thousands of issuing banks to millions of merchants and routes messages between them — the money itself always moves bank to bank.

### Visualization (canvas `c1`, 720×300)

Flow diagram of the five parties in one purchase: message traffic runs through the network box, money flows bank to bank underneath and skips it.

- **Title (bold 15px, `#1a5276`, top center):** "One $100 Purchase: Messages Go Through Visa, Money Goes Around It".
- **Party boxes (rounded 8px, 40px tall, 12px `#2c3e50` text, centered on y=130):** "Maya (cardholder)" at x=20 w=120 fill `rgba(42,120,214,0.15)`; "Issuing bank" at x=170 w=110 fill `rgba(42,120,214,0.15)`; "Visa network" at x=310 w=110 fill `rgba(74,58,167,0.15)` with 2px `#4a3aa7` border; "Acquiring bank" at x=450 w=110 fill `rgba(42,120,214,0.15)`; "Kitchen shop" at x=590 w=110 fill `rgba(0,131,0,0.12)`.
- **Message arrows (top lane):** dashed 2px `#2a78d6` (dash 5/4) double-headed arrows at y=100 connecting adjacent boxes across all five parties; bold 12px `#2a78d6` label "authorization & settlement messages (~1 second)" centered at y=80.
- **Money arrows (bottom lane):** solid 3px `#008300` arrows at y=200 from issuing bank to acquiring bank (one long arrow passing under the Visa box, not through it) and from acquiring bank to kitchen shop; bold 12px `#008300` label "$100 moves bank to bank — never through Visa" centered at y=228.
- **Annotation (bold 13px violet `#4a3aa7`, above the Visa box at y=60):** "routes messages, holds $0".
- **Caption (12px `#444`, bottom right):** "flow schematic; amounts illustrative".

## Splitting the $100: Cents for the Rails, Dollars for the Risk

**Tags:** `worked example` (blue), `fee split` (green)

- **The sale** — the shop rings up $100 but receives $97.40 after $2.60 in fees (illustrative rates)
- **Interchange** — $1.80 goes to Maya's issuing bank, the party that fronts the money and takes credit risk
- **Network fee** — Visa's cut is about $0.14 — a few cents per transaction for routing the messages
- **Acquirer markup** — $0.66 stays with the shop's bank and its processor for serving the merchant
- **The contrast** — the issuer earns roughly 13× what the network earns on the very same swipe

*Example (italic):* Of the $2.60 in fees on Maya's $100 purchase, Visa keeps 14 cents; the bank that lent her the money keeps $1.80.

**Key point:** The network's per-transaction fee is tiny, but the network writes the fee schedule everyone else lives by — and tiny cents times billions of swipes is an enormous business.

### Visualization (canvas `c2`, 720×300)

Two horizontal bars: the full $100 with a thin fee sliver, then that $2.60 sliver blown up to show the interchange / network / acquirer split.

- **Title (bold 15px, `#1a5276`, top center):** "Where the $100 Goes: $97.40 to the Shop, $2.60 in Fees (illustrative)".
- **Top bar (y=90, x=60, total width 600 = $100 at 6px per dollar, 26px tall):** merchant segment fill `rgba(42,120,214,0.30)` width 584 ($97.40) with 12px `#2c3e50` label "shop receives $97.40" centered inside; fee sliver solid `#d95926` width 16 ($2.60) at the right end.
- **Zoom connectors:** dashed 1.5px `#6b7280` lines from the sliver's two bottom corners (x=644 and x=660, y=116) fanning out to the bottom bar's two top corners (x=60 and x=660, y=184).
- **Bottom bar (y=184, x=60, total width 600 = $2.60 at ~231px per dollar, 26px tall):** interchange segment fill `rgba(0,131,0,0.30)` with 2px `#008300` edge, width 415 ($1.80), 12px label "interchange → issuer $1.80"; network segment solid `#4a3aa7` width 32 ($0.14), bold 12px `#4a3aa7` label "Visa $0.14" above the segment at y=170; acquirer segment fill `rgba(25,158,112,0.30)` with 2px `#199e70` edge, width 152 ($0.66), 12px label "acquirer $0.66".
- **Annotation (bold 13px violet `#4a3aa7`, right side near y=250):** "the network's slice: 14¢ of $100".
- **Caption (12px `#444`, bottom right):** "fee split illustrative; real rates vary by card type and merchant".

## No Deposits, No Credit Risk, Almost No Cost to Scale

**Tags:** `why it matters` (blue), `risk` (orange), `network effect` (green)

- **Issuer risk** — Maya's bank fronts the $100 and eats the loss if she never pays her card bill
- **Acquirer risk** — the shop's bank refunds cardholders if the shop vanishes before delivering goods
- **Network risk** — Visa and Mastercard take neither; a bad loan or a failed merchant costs them nothing
- **The rulebook** — the networks set interchange rates, dispute procedures, and security standards for all
- **The moat** — every new cardholder makes merchants need the rails more, and every merchant pulls in cardholders

*Example (italic):* In a downturn, issuing banks write off bad card loans while the network still collects its cents on every swipe that happens.

**Key point:** Because the network holds no money and takes no credit risk, one more transaction costs it almost nothing — the business scales with message volume, not with a balance sheet.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart answering one question from the running example: if Maya defaults on the $100, who loses what — and who got paid anyway.

- **Title (bold 15px, `#1a5276`, top center):** "Maya Never Pays Her $100 Bill: Who Eats the Loss?".
- **Axis:** vertical 2px `#999` baseline at x=230, bars extend right, max width 420.
- **Rows (bars 18px tall, centered at y = 95, 155, 215), each with a left-aligned 12px `#444` label at x=20:**
  - "Issuing bank (lent the money)": red `#e74c3c` bar width 420 with bold 12px red label "-$100.00 written off" at the bar end
  - "Acquiring bank (shop's bank)": grey `#6b7280` tick width 3 with 12px `#6b7280` label "$0 — goods were delivered, no chargeback"
  - "Visa network (routed messages)": grey tick width 3 for the loss plus a solid green `#008300` bar width 60 starting at x=240 with bold 12px green label "+$0.14 fee already earned"
- **Annotation (bold 13px magenta `#d55181`, right side near y=260):** "the network gets paid whether or not the loan does".
- **Caption (12px `#444`, bottom right):** "bar widths schematic; dollar figures from the illustrative example".

## Not the Bank on the Card: Four Parties vs Three

**Tags:** `common mistake` (red), `Amex vs Visa` (orange)

- **The confusion** — people see the Visa logo and assume Visa issued the card and lent the money
- **Four-party model** — Visa and Mastercard swipes involve a separate issuer, acquirer, network, and merchant
- **Three-party model** — Amex issues its own cards and signs its own merchants; it is the bank and the rails
- **The consequence** — Amex takes the credit risk Visa avoids, and keeps the interchange for itself
- **Reading the card** — "Visa" names the rails; the bank name printed in the corner is who Maya owes

*Example (italic):* When Maya's card says "First National — Visa", First National lent her the $100; Visa only routed the approval message.

**Common mistake:** Calling Visa or Mastercard a credit card company that lends money. They are message routers — in the four-party model the lending, the deposits, and the risk all sit with the banks.

### Visualization (canvas `c4`, 720×300)

Two-row diagram: the four-party chain (Visa/Mastercard, five boxes) above the three-party chain (Amex, one wide box playing three roles).

- **Title (bold 15px, `#1a5276`, top center):** "Four-Party Model (Visa / Mastercard) vs Three-Party Model (Amex)".
- **Row 1 (boxes centered on y=115), label 12px `#444` "four-party" at x=20 y=82:** five rounded boxes (100px wide, 40px tall, 8px radius, 12px `#2c3e50` text) at x = 20, 160, 300, 440, 580 labeled "Cardholder", "Issuing bank", "Visa (rails)", "Acquiring bank", "Merchant"; bank boxes fill `rgba(42,120,214,0.15)`, the rails box fill `rgba(74,58,167,0.15)` with 2px `#4a3aa7` border, cardholder and merchant fill `rgba(0,131,0,0.12)`; 2px `#6b7280` arrows between adjacent boxes.
- **Row 2 (boxes centered on y=215), label 12px `#444` "three-party" at x=20 y=182:** rounded box "Cardholder" at x=20 w=120; wide rounded box "Amex — issuer + rails + acquirer" at x=200 w=320 fill `rgba(217,89,38,0.15)` with 2px `#d95926` border; rounded box "Merchant" at x=580 w=120; 2px `#6b7280` arrows between the three.
- **Annotation (bold 13px orange `#d95926`, centered near y=272):** "one company, all three roles — and all the credit risk".
- **Caption (12px `#444`, bottom right):** "role diagram; simplified".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all figures are the hardcoded values above (no randomness); the $100 purchase and the fee split ($97.40 / $1.80 / $0.14 / $0.66, summing exactly to $100) are invented and labeled illustrative; the structural facts — networks hold no deposits, issue no cards, take no credit risk; four-party vs three-party models — are publicly documented and exact. Text numbers and chart numbers must stay identical.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
