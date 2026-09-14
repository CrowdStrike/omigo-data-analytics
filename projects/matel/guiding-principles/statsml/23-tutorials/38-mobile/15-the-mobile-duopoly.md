# The Mobile Duopoly

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** The Mobile Duopoly

**Subtitle:** Nearly every phone runs one of two operating systems, and each store charges a published commission on digital sales — so a $10 subscription pays the developer $7

## The $10 Subscription That Pays Out $7

**Tags:** `core idea` (blue), `store billing` (green), `30% toll` (orange)

- **The app** — a meditation app sells a $10/month subscription inside its iPhone and Android versions
- **The rule** — digital purchases made inside the app must go through the store's own billing system
- **The toll** — the store keeps a published commission before the developer sees any money
- **The payout** — at the headline 30% rate, $3.00 stays with the store and $7.00 reaches the developer
- **The pair** — the same billing rule holds on both major stores, so there is no third door at scale

*Example (italic):* A subscriber pays $10.00 on the first of the month; the developer's payout report shows $7.00 for that subscriber. (Subscriptions often bill at 15% — Play from day one, Apple after the subscriber's first year — 30% is the headline rate.)

**Key point:** On mobile, a store sits between the app and its customer and takes a documented cut of every digital sale — the developer prices at $10 but budgets around $7.

### Visualization (canvas `c1`, 720×300)

Money-flow diagram: one $10 payment moving left to right through the store, with $3.00 branching off to the store and $7.00 arriving at the developer.

- **Title (bold 15px, `#1a5276`, top center):** "Where the $10 Goes: Store Billing at the Headline 30% Rate".
- **Boxes (rounded 8px radius, 40px tall, 12px `#2c3e50` labels):** "Customer pays $10.00" at x=40 y=130 width 170, fill `rgba(42,120,214,0.15)` border 2px `#2a78d6`; "App store billing" at x=290 y=130 width 150, fill `rgba(217,89,38,0.12)` border 2px `#d95926`; "Developer receives $7.00" at x=520 y=130 width 180, fill `rgba(0,131,0,0.12)` border 2px `#008300`.
- **Arrows:** 3px `#2c3e50` arrow from customer box to store box labeled "$10.00" (bold 12px `#2a78d6` above); 3px arrow from store box to developer box labeled "$7.00" (bold 12px `#008300` above).
- **Branch:** 3px `#d95926` arrow from the store box downward to y=240, ending at bold 12px orange `#d95926` label "store keeps $3.00 (30%)".
- **Annotation (bold 13px ink `#1a5276`, centered near y=70):** "the toll is taken before the developer is ever paid".
- **Caption (12px `#444`, bottom right):** "app and price illustrative; 30% is the published standard rate".

## Running the Numbers at 30% and 15%

**Tags:** `worked example` (blue), `commission tiers` (green)

- **The gross** — 1,000 subscribers × $10 means the store collects $10,000 in a month
- **Standard tier** — at 30%: $10,000 − $3,000 = $7,000 paid out to the developer
- **Reduced tier** — both major stores publish a 15% rate for smaller developers (roughly under $1M/yr)
- **Reduced payout** — at 15%: $10,000 − $1,500 = $8,500 — a $1,500 monthly swing on the same sales
- **Hand-check** — per subscriber the tiers pay $7.00 vs $8.50; multiply by 1,000 to match the totals

*Example (italic):* The same 1,000 subscribers are worth $7,000 a month at 30% and $8,500 at 15% — an $18,000 difference over a year.

**Key point:** The published tiers apply to gross billed revenue, so a tier change moves net revenue by exactly the tier gap — qualifying for the 15% program is worth 15 points of margin.

### Visualization (canvas `c2`, 720×300)

Vertical bar chart of one month's revenue for the 1,000-subscriber app: gross billed vs net at the 30% tier vs net at the 15% tier.

- **Title (bold 15px, `#1a5276`, top center):** "One Month, 1,000 Subscribers: Gross vs Net Under Each Tier".
- **Axes:** origin x=70, baseline y=245, plot width 590, plot height 180; y = dollars 0 to $10,000 with gridlines `#e5e9ef` at 2,500/5,000/7,500 and 12px `#444` labels "$2.5k"/"$5k"/"$7.5k"/"$10k".
- **Bars (110px wide, centered at x = 180, 380, 580, 12px `#444` category labels below baseline):**
  - "Gross billed": height for `10000`, fill `rgba(42,120,214,0.35)`, border 2px `#2a78d6`
  - "Net at 30%": height for `7000`, fill `rgba(217,89,38,0.30)`, border 2px `#d95926`
  - "Net at 15%": height for `8500`, fill `rgba(0,131,0,0.25)`, border 2px `#008300`
- **Value labels:** bold 13px `#2c3e50` "$10,000" / "$7,000" / "$8,500" centered above each bar top.
- **Annotation (bold 13px green `#008300`, near x=470, y=70):** "the 15% tier adds $1,500/month on identical sales".
- **Caption (12px `#444`, bottom right):** "subscriber count illustrative; 30% and 15% are the published tiers".

## Two Gatekeepers, One Pricing Decision

**Tags:** `where it's used` (blue), `pricing` (green), `regulation` (orange)

- **The gate** — nearly every smartphone runs one of two operating systems, each with its own store
- **No third door** — reaching mobile customers at scale means accepting each store's published terms
- **Pricing upward** — to net $10.00 per subscriber under a 30% commission, the app must charge $14.29
- **Web checkout** — some apps sell the same subscription on their website, where card fees run ~3%
- **Regulators** — rules in several regions now require stores to allow links to outside payment options

*Example (italic):* To keep $10.00 per subscriber, the app charges $14.29 through store billing at 30%, $11.76 at the 15% tier, or about $10.31 on the web at ~3% card fees.

**Key point:** The duopoly turns the commission into a pricing input — the same target payout implies a different sticker price per channel, which is why web checkout and regulatory rules matter to app economics.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: the sticker price a customer must pay for the developer to net $10.00, across three checkout channels.

- **Title (bold 15px, `#1a5276`, top center):** "The Price of Netting $10.00, Channel by Channel".
- **Axis:** vertical 2px `#999` baseline at x=230, bars extend right, x scaled so $15.00 = 440px; gridlines `#e5e9ef` at the pixel positions for $5 and $10 with 12px `#444` labels below y=250.
- **Rows (top to bottom at y = 85, 145, 205), each with a left-aligned 12px `#444` label at x=20, bars 22px tall:**
  - "Store billing, 30% tier": orange `#d95926` bar for `14.29`, fill `rgba(217,89,38,0.30)`, bold 12px `#d95926` end label "$14.29"
  - "Store billing, 15% tier": blue `#2a78d6` bar for `11.76`, fill `rgba(42,120,214,0.30)`, bold 12px `#2a78d6` end label "$11.76"
  - "Web checkout, ~3% card fee": green `#008300` bar for `10.31`, fill `rgba(0,131,0,0.25)`, bold 12px `#008300` end label "$10.31"
- **Reference line:** vertical dashed `#6b7280` (dash 4/3) line at the pixel position for $10.00, 12px `#6b7280` label "the $10.00 the developer keeps" at its top.
- **Annotation (bold 13px violet `#4a3aa7`, near y=260, centered):** "same payout, three sticker prices — the toll is priced in".
- **Caption (12px `#444`, bottom right):** "card fee illustrative; commission tiers as published".

## Not Every Dollar Pays the Toll

**Tags:** `common mistake` (red), `digital vs physical` (orange)

- **The confusion** — the commission applies to digital goods billed through the store, not to all revenue
- **Physical goods** — a ride, a meal, or a pair of shoes bought in-app pays no store commission
- **Ads** — apps monetized by advertising pay no commission on their ad revenue
- **Carve-outs** — stores publish category exceptions, so the effective rate varies by business model
- **The mistake** — modeling a delivery or marketplace app with a 30% haircut it never actually pays

*Example (italic):* A food-delivery app's $10.00 order pays the store $0 — the meal is a physical good; the meditation app's $10.00 subscription pays $3.00.

**Common mistake:** Applying the 30% figure to the gross revenue of any mobile business. The commission covers in-app digital purchases made through store billing; physical goods and ad revenue sit outside it.

### Visualization (canvas `c4`, 720×300)

Horizontal bar chart: the store's cut of a $10.00 sale across four revenue types, showing where the toll applies and where it is zero.

- **Title (bold 15px, `#1a5276`, top center):** "Store Commission on a $10.00 Sale, by Revenue Type".
- **Axis:** vertical 2px `#999` baseline at x=280, bars extend right, x scaled so $3.00 = 380px; 12px `#444` scale labels "$0" / "$1.50" / "$3.00" below y=255.
- **Rows (top to bottom at y = 75, 125, 175, 225), each with a left-aligned 12px `#444` label at x=20, bars 20px tall:**
  - "Digital subscription, 30% tier": orange `#d95926` bar for `3.00`, fill `rgba(217,89,38,0.30)`, bold 12px `#d95926` end label "$3.00"
  - "Digital subscription, 15% tier": blue `#2a78d6` bar for `1.50`, fill `rgba(42,120,214,0.30)`, bold 12px `#2a78d6` end label "$1.50"
  - "Physical good (meal, ride)": no bar, bold 12px green `#008300` label "$0 — outside store billing" at x=290
  - "Ad revenue": no bar, bold 12px green `#008300` label "$0 — outside store billing" at x=290
- **Annotation (bold 13px magenta `#d55181`, near y=270, centered):** "the toll gates digital goods, not the whole mobile economy".
- **Caption (12px `#444`, bottom right):** "sale amount illustrative; commissioned categories as published in store terms".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded values above (no randomness); the app, its $10 price, the 1,000-subscriber count, and the ~3% card fee are invented and labeled illustrative; the 30% standard and 15% reduced-tier commissions are the stores' published rates; every derived dollar figure follows arithmetically ($7.00/$8.50 per subscriber, $7,000/$8,500/$1,500 monthly, $18,000 yearly, break-even prices $14.29 = 10/0.70, $11.76 = 10/0.85, $10.31 = 10/0.97, and the $3.00/$1.50/$0 commission bars).
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
