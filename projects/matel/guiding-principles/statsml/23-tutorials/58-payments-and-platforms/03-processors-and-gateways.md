# Processors & Gateways

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Processors & Gateways

**Subtitle:** A gateway captures the card and a processor moves the money — Stripe's real product is selling both, plus the merchant account, behind one API

## The Candle Shop and the Seven-Party Stack

**Tags:** `core idea` (blue), `layer collapse` (green), `Stripe / Adyen / Square` (orange)

- **The shop** — a two-person candle shop wants to take a $50 card payment on its website
- **The gateway** — the secure front door: the API or terminal that captures card details at checkout
- **The processor** — the plumbing behind it: routes authorizations and settlements to banks and networks
- **The 2005 way** — merchant account, gateway vendor, processor contract: months of bank paperwork
- **The collapse** — Stripe, Adyen, Square bundled the stack behind one API; live in an afternoon

*Example (italic):* In 2005 the candle shop's first $50 sale needed seven signed-up parties end to end; today it needs one API key.

**Key point:** The gateway captures the card, the processor moves the money, the acquirer holds the account — modern platforms sell all three as one product, which is why "Stripe" feels like a single thing.

### Visualization (canvas `c1`, 720×300)

Two-row flow diagram: the same $50 payment crossing the 2005 seven-party stack vs today's one-API stack.

- **Title (bold 15px, `#1a5276`, top center):** "The Same $50 Payment: Seven Parties in 2005, One API Today".
- **Row 1 (boxes centered on y=105), label 12px `#444` at x=15, y=68:** "2005 — months of contracts"; seven rounded boxes left to right at x = `[15, 116, 217, 318, 419, 520, 621]`, each 86px wide, 40px tall, 6px radius, fill `rgba(42,120,214,0.15)`, 11px `#2c3e50` two-line labels: "customer", "candle shop", "gateway vendor", "processor", "acquiring bank (merchant acct)", "card network", "issuing bank"; 2px `#6b7280` arrows in the 15px gaps between boxes.
- **Row 2 (boxes centered on y=215), label 12px `#444` at x=15, y=178:** "today — one afternoon"; five boxes: "customer" at x=15 w=86, "candle shop" at x=131 w=86, wide green-fill `rgba(0,131,0,0.12)` box at x=247 w=230 labeled "platform API — gateway + processor + merchant acct" (11px, two lines), "card network" at x=507 w=95, "issuing bank" at x=632 w=80; same arrow style.
- **Annotation (bold 13px green `#008300`, centered near y=272):** "same networks, same $50 — three middle layers become one".
- **Caption (12px `#444`, bottom right):** "party lineup schematic".

## Following One $50 Payment Through the Wire

**Tags:** `worked example` (blue), `auth and settlement` (green)

- **Capture** — the gateway tokenizes the card number so the shop's server never touches raw digits
- **Authorize** — the processor sends the request through the card network to the customer's issuing bank
- **Approve** — the issuing bank checks funds and fraud signals, then answers in about a second
- **Settle** — funds batch through the network a day or two later; the platform pays the shop out
- **The fee** — at 2.9% + $0.30, the $50 sale costs $1.75; the shop's payout is $48.25 (exact arithmetic)

*Example (italic):* Of the $1.75 fee, roughly $1.00 is interchange to the issuing bank, $0.07 network assessments, and $0.68 stays with the platform (split illustrative).

**Key point:** One "create charge" API call fires the whole chain — capture, authorize, settle, pay out — the old layers still exist; they are just hidden behind one bill.

### Visualization (canvas `c2`, 720×300)

Fee-breakdown chart: a full-width strip splitting the $50 sale into payout vs fee, then a zoomed bar chart splitting the $1.75 fee three ways.

- **Title (bold 15px, `#1a5276`, top center):** "Where the $1.75 Fee on a $50 Sale Goes".
- **Top strip (y=58, 22px tall, x=60, total width 600):** green `rgba(0,131,0,0.30)` segment 579px wide labeled inside "merchant payout $48.25" (12px `#2c3e50`), orange `#d95926` segment 21px wide at the right end; 12px orange label "fee $1.75" above the orange segment; 2px `#6b7280` connector lines fanning from the orange segment down to the zoom rows.
- **Zoom rows (bars start at x=60, scale $1.00 = 440px, 16px tall, 12px `#444` labels just above each bar):**
  - y=140: "interchange → issuing bank $1.00": blue `#2a78d6` bar width 440
  - y=190: "platform margin $0.68": orange `#d95926` bar width 299
  - y=240: "network assessments $0.07": violet `#4a3aa7` bar width 31
- **Value labels:** 11px `#444` dollar amounts at each bar's right end.
- **Annotation (bold 13px orange `#d95926`, near x=420, y=205):** "the platform's cut is the price of the collapsed stack".
- **Caption (12px `#444`, bottom right):** "2.9% + $0.30 math exact; three-way split illustrative".

## The Trade: Afternoon Launch vs Interchange-Plus at Scale

**Tags:** `where it's used` (blue), `pricing` (green), `rule of thumb` (orange)

- **Speed** — a developer can accept a live card payment the same afternoon the account is created
- **Bundling** — fraud tooling (Radar), payouts, disputes, and reporting ride along on the same API
- **Flat pricing** — one blended rate is predictable, but it averages cheap and costly cards together
- **Interchange-plus** — pass-through pricing bills true network cost plus a small itemized markup
- **The crossover** — around $1M a year, the $0.48-per-sale gap can fund the extra plumbing (illustrative)

*Example (italic):* At 20,000 $50 sales a month, blended pricing costs $35,000 and interchange-plus about $25,400 — a $9,600 monthly gap.

**Key point:** The platform's product is collapsed complexity — you pay for it in basis points, which is a great trade at small volume and a negotiable one at large volume.

### Visualization (canvas `c3`, 720×300)

Grouped bar chart: monthly fees at three sales volumes, blended flat rate ($1.75/sale) vs interchange-plus ($1.27/sale).

- **Title (bold 15px, `#1a5276`, top center):** "Monthly Fees on $50 Sales: Flat Rate vs Interchange-Plus".
- **Axes:** 2px `#999` baseline at y=245 from x=60 to x=680; no numeric y axis — bar heights follow a true log10 scale (h = 60·(log10($) − 1.6)), values printed on the bars.
- **Groups (centers at x = 170, 370, 570; 12px `#444` labels below baseline):** "200 sales/mo", "2,000 sales/mo", "20,000 sales/mo".
- **Bars (55px wide; blue `#2a78d6` blended bar at center−60, green `#008300` interchange-plus bar at center+5; bold 12px value labels above each bar):**
  - group 1: blended $350 height 57; interchange-plus $254 height 48
  - group 2: blended $3,500 height 117; interchange-plus $2,540 height 108
  - group 3: blended $35,000 height 177; interchange-plus $25,400 height 168
- **Annotation (bold 13px magenta `#d55181`, near x=430, y=45):** "at 20,000 sales/mo the flat rate costs $9,600 more".
- **Caption (12px `#444`, bottom right):** "bar heights log-scaled; $1.75 vs $1.27 per sale — multiplication exact, interchange-plus rate illustrative".

## A Gateway Is Not a Processor

**Tags:** `common mistake` (red), `three jobs` (orange)

- **The confusion** — "gateway" and "processor" get used interchangeably; they are different jobs
- **Gateway job** — capture the card securely at the edge (checkout form, terminal) and tokenize it
- **Processor job** — talk to acquiring banks and card networks to move authorizations and money
- **Acquirer job** — hold the merchant account, carry the fraud/chargeback risk, receive settled funds
- **The mistake** — reading the blended fee as "the network's fee"; much of it is the platform's markup

*Example (italic):* A shop hunting for a "cheaper gateway" in 2005 still needed a processor and a merchant account — three contracts, not one.

**Common mistake:** Treating the platform's one fee as one service. It bundles three historically separate roles — gateway, processor, acquirer — and knowing the seams is what lets you negotiate or unbundle later.

### Visualization (canvas `c4`, 720×300)

Stacked layer diagram: the three roles as separate boxes, with a bracket showing a modern platform spanning all three.

- **Title (bold 15px, `#1a5276`, top center):** "Gateway, Processor, Acquirer: Three Jobs, One Brand Name".
- **Layer boxes (x=180, 340px wide, 44px tall, 8px radius, 12px `#2c3e50` two-line text):**
  - y=70: blue fill `rgba(42,120,214,0.15)`, "GATEWAY — captures & tokenizes the card at checkout"
  - y=132: aqua fill `rgba(25,158,112,0.15)`, "PROCESSOR — routes authorizations & settlement to networks"
  - y=194: orange fill `rgba(217,89,38,0.12)`, "ACQUIRER — holds the merchant account & the risk"
- **Left labels (12px `#444`, right-aligned at x=170):** "what checkout sees" beside the gateway box, "what the banks see" beside the processor box, "who holds the money" beside the acquirer box.
- **Bracket:** 3px `#008300` vertical line at x=545 from y=70 to y=238 with short horizontal ticks at both ends; bold 13px green `#008300` label to its right, rotated or two-line at x=560, y=145: "one modern platform (one API, one fee)".
- **Annotation (bold 13px red `#e74c3c`, centered near y=272):** "the fee looks like one number because three jobs got one bill".
- **Caption (12px `#444`, bottom right):** "role split per public card-scheme documentation".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all values are the hardcoded numbers above (no randomness); the 2.9% + $0.30 fee arithmetic ($1.75 on $50, $48.25 payout, $350 / $3,500 / $35,000 and $254 / $2,540 / $25,400 monthly totals) is exact multiplication; the interchange / assessment / margin split ($1.00 / $0.07 / $0.68), the $1.27 interchange-plus per-sale cost, and the $1M/yr crossover are invented and labeled illustrative; the gateway / processor / acquirer role split and the seven-party 2005 lineup are publicly documented industry structure.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
