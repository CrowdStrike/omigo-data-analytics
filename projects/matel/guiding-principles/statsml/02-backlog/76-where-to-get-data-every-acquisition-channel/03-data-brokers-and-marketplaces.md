# Data Brokers & Marketplaces — Buying Datasets Outright

**Page type:** detail page (four titled sections, each a two-column row: text left 45%, canvas right 55%)
**HTML title tag:** Data Brokers & Marketplaces — Buying Datasets Outright

**Subtitle:** Beyond subscriptions there is a retail market for datasets themselves: cloud marketplaces where a dataset arrives as a shared table in your warehouse, brokers who assemble and sell consumer profiles, and an alternative-data industry that sells investors early glimpses of company performance.

**Intro callout (blue-left-border box):** If commercial feeds are the subscription economy of data, brokers and marketplaces are its retail shelf. Anyone with a budget can browse listings, buy a dataset, and have it delivered — sometimes directly into a SQL warehouse within minutes. The convenience is real, and so is the catch: the further a dataset has traveled from its original collection, the harder it is to know where it came from, who consented to what, and whether you are allowed to use it the way you intend.

## 1. Cloud data marketplaces

The major cloud platforms turned dataset purchase into a checkout flow: browse a catalog, subscribe to a listing, and the data appears in your warehouse as live shared tables.

- **The storefronts:** AWS Data Exchange, Snowflake Marketplace, Databricks Marketplace.
- **Listing variety:** thousands of listings — demographics, weather, firmographics, market data.
- **Seller vetting:** sellers are screened by the platform before listing.
- **Warehouse-native delivery:** no files shipped — you get read access to shared tables.
- **Queryable in minutes:** the dataset sits alongside your own tables immediately.
- **Automatic updates:** the seller's refreshes flow through to your account.
- **Platform-handled commerce:** billing, entitlements, standard license templates.
- **Less procurement friction:** skips most of a direct vendor contract's overhead.
- **Discovery is the quiet win:** a searchable catalog shows a needed dataset exists at all.
- **Often the hardest step:** learning the data exists precedes deciding to buy it.

Key point: Marketplaces compress the acquisition workflow from months of procurement to a subscription click, but the ease of the checkout says nothing about the quality or provenance of what is inside the listing — that diligence still belongs to the buyer.

### Visualization (canvas `c1`, 720×360)

Marketplace flow: seller listing box on the left, platform box in the middle (with billing/license/entitlement stack), buyer warehouse on the right showing the shared table appearing next to internal tables.

- **Title (bold 14px `#1a5276`, centered, y=22):** "From catalog listing to queryable table without a file ever moving"
- **Seller box:** 180×96 at (30, 90), white fill, 2px `#e67e22` border. Bold 12px `#e67e22` "SELLER LISTING"; 11px `#666` lines: "weather history dataset" / "sample + schema shown" / "license terms attached".
- **Platform box:** 200×150 at (260, 66), fill `rgba(26,82,118,0.12)`, 2px `#1a5276` border. Bold 12px `#1a5276` centered "MARKETPLACE"; 11px `#555` centered lines: "AWS Data Exchange" / "Snowflake Marketplace" / "Databricks Marketplace"; below, 11px `#999` centered: "billing · entitlements" / "standard licenses".
- **Buyer box:** 200×150 at (500, 66), white fill, 2px `#27ae60` border. Bold 12px `#27ae60` centered "BUYER WAREHOUSE". Inside, three table rows (170×26, x=515, y=104/136/168): first two white fill 1.5px `#ccc` border, bold 10px `#555` labels "orders (internal)" and "customers (internal)"; third fill `rgba(39,174,96,0.12)`, 1.5px `#27ae60` border, bold 10px `#27ae60` "weather_history (shared)".
- **Arrows:** 2px `#999` arrow from seller to platform (y=138) labeled "lists" (10px `#999` above); 2px `#27ae60` arrow from platform to buyer (y=138) labeled "grants read access" (10px `#27ae60` above).
- **Note (11px `#666`, centered, y=260):** "no files shipped — the platform points your account at the seller's shared tables, and updates flow through automatically"
- **Caption (12px `#999`, centered, y = h−14):** "Checkout-easy acquisition — the diligence on what is inside still belongs to the buyer"

## 2. People-data brokers

A separate industry assembles data about individual consumers from many upstream sources and sells the assembled profiles, segments, and identity links.

- **The assemblers:** Acxiom, Experian's marketing services arm, LiveRamp.
- **Upstream inputs:** public records, purchase histories, warranty cards, loyalty programs.
- **More inputs:** online identifiers, subscriptions, change-of-address files.
- **Population coverage:** assembled profiles span most of the adult population.
- **What is sold:** rarely the raw profile — packaged products instead.
- **Audience segments:** named buckets like "likely new-car intenders".
- **Data appends:** enrichment fields added to a client's customer file.
- **Identity-graph matches:** one person's identifiers linked across devices and channels.
- **The identity graph:** emails, postal addresses, cookies, mobile IDs on one persistent key.
- **Why it matters:** a brand can recognize the same person across unlinked datasets.
- **The subject is not the customer:** the person never chose the broker.
- **No visibility:** the subject usually does not know the profile exists.
- **No recourse:** little practical means to inspect or correct the record.

Key point: Broker data answers "who is this person and what are they like?" at population scale — which is precisely why it is the most regulated and most reputationally sensitive dataset category a team can buy.

### Visualization (canvas `c2`, 720×400)

Fan-in diagram: six upstream source labels on the left converging into a broker profile card in the center, with three product boxes fanning out on the right.

- **Title (bold 14px `#1a5276`, centered, y=22):** "Many faint traces, one assembled profile, three product lines"
- **Upstream sources (11px `#666` right-aligned at x=170, y=64/112/160/208/256/304, each with a 1px `#bbb` connector to the profile card):** "public records", "purchase histories", "warranty & loyalty cards", "subscriptions & surveys", "online identifiers", "postal & change-of-address"
- **Profile card:** 220×180 at (210, 100), fill `rgba(142,68,173,0.08)`, 2px `#8e44ad` border. Bold 12px `#8e44ad` centered at top: "ASSEMBLED PROFILE". Inside, 11px `#555` left-aligned field rows at x=226 (y=148, spaced 22px): "person key: (persistent id)", "household, age band, income band", "interests & purchase propensities", "linked emails, devices, cookies"; 10px `#999` bottom line centered: "the subject never sees this record".
- **Product boxes (each 210×52 at x=490, white fill, 2px border in box color; bold 12px label, 11px `#666` subline; 1.5px `#bbb` connectors from profile card right edge):**
  - y=92 "Audience segments" `#27ae60` — "\"likely new-car intenders\""
  - y=170 "Data appends" `#e67e22` — "enrich a client's customer file"
  - y=248 "Identity matches" `#1a5276` — "same person across devices"
- **Caption (12px `#999`, centered, y = h−14):** "Assembled from sources the subject never connected — sold to buyers the subject never met"

## 3. Alternative data for finance

Investment funds buy unconventional datasets to estimate company performance before official numbers are published — an entire brokered market exists to supply them.

- **Card transaction panels:** aggregated, anonymized card spending from millions of consumers.
- **What they estimate:** a retailer's quarterly revenue, weeks before the earnings report.
- **Foot traffic:** apps embedding location SDKs yield visit counts for stores, malls, factories.
- **Sold as demand indicators:** vendors aggregate the visits and sell the trend.
- **Satellite and aerial imagery:** cars in retailer lots, shadows on oil storage tanks.
- **Pixels to estimates:** overhead imagery becomes supply-and-demand signals.
- **The value decays fast:** a signal is worth the most when few funds have it.
- **Priced on exclusivity:** alpha erodes as subscriptions spread.

Key point: The entire product is the time gap: alternative data is bought because it arrives weeks before the official number. Buyers therefore validate it on exactly one criterion — how well and how early it predicted numbers that were later published.

### Visualization (canvas `c3`, 720×340)

Alt-data timeline: a quarter timeline where card-panel, foot-traffic, and satellite signals accumulate early, followed by the official earnings date late — the gap between signal and announcement shaded as the edge.

- **Title (bold 14px `#1a5276`, centered, y=22):** "The product is the head start on the official number"
- **Timeline:** 2px `#999` line at y=210 from x=50 to x=680 with a filled right-arrowhead; 10px `#999` tick labels below at x=90 "quarter starts", x=350 "quarter ends", x=600 "earnings report".
- **Signal markers (filled dots radius 6 on the line, label bold 11px in color above with thin `#ccc` connectors, 10px `#666` subline):**
  - x=170 `#27ae60`, "Card panel" — "spend estimate, weekly" (label at y=120)
  - x=250 `#e67e22`, "Foot traffic" — "store visits, daily" (label at y=76)
  - x=330 `#8e44ad`, "Satellite" — "parking-lot counts" (label at y=120)
- **Official marker:** filled `#1a5276` dot radius 7 at x=600; bold 12px `#1a5276` above (y=150): "Official revenue"; 10px `#666`: "published to everyone".
- **Edge region:** fill `rgba(39,174,96,0.10)` rectangle from x=350 to x=600 between y=170 and y=210; bold 11px `#27ae60` centered inside (y=192): "the head start — weeks of edge"; dashed (5/4) 1px `#27ae60` vertical borders at x=350 and x=600.
- **Note (11px `#666`, centered, y=252):** "signals accumulate all quarter; the official number arrives weeks after the quarter closes"
- **Caption (12px `#999`, centered, y = h−14):** "Validated on one criterion: how early and how well it predicted the number that was later published"

## 4. Provenance and regulation

The further data travels from its original collection, the murkier the chain of consent becomes — and regulators have started treating that murkiness as the seller's and the buyer's problem.

- **The opaque consent chain:** a brokered record may have passed through four or five hands.
- **Original notice gap:** "we share data with partners" says nothing about your intended use.
- **Registration laws:** Vermont and California require data brokers to register publicly.
- **California DELETE Act:** one request point for deletion across registered brokers.
- **Regulators have acted:** US FTC actions over the sale of sensitive location data.
- **Untouchable categories:** precise traces around health clinics and places of worship.
- **The buyer inherits the risk:** undocumented provenance becomes your liability.
- **What absorbs it:** your models, products, and reputation — not the seller's.

Key point: Provenance is the due-diligence question for purchased data: a seller who cannot explain the collection chain is selling you a liability, not an asset — and "we bought it in good faith" has not protected buyers in practice.

### Visualization (canvas `c4`, 720×340)

Provenance chain: five boxes left to right (collection → aggregator → broker → marketplace → you), with the link between collection and aggregator drawn broken and highlighted in red as the unknown consent step.

- **Title (bold 14px `#1a5276`, centered, y=22):** "The provenance chain — and the link nobody can explain"
- **Chain boxes (each 118×72 at y=90, x=28/166/304/442/580, white fill, 2px border in box color; bold 11px title in color at +10/+20, 10px `#666` sublines at +10, +38/+52):**
  - "COLLECTION" `#27ae60` — "an app, a form," / "a loyalty card"
  - "AGGREGATOR" `#e67e22` — "buys from many" / "collectors"
  - "BROKER" `#8e44ad` — "assembles &" / "segments"
  - "MARKETPLACE" `#1a5276` — "lists & delivers" / "the dataset"
  - "YOU" `#555` — "model it, ship it," / "own the risk"
- **Links:** 2px `#999` connector arrows between consecutive boxes at y=126 — except the first link (collection → aggregator), drawn as a dashed (5/4) 2px `#e74c3c` line with a jagged break gap in the middle and a bold 14px `#e74c3c` "?" centered in the gap (y=120).
- **Broken-link callout:** bold 12px `#e74c3c` centered at y=200: "What did the person actually consent to at collection?"; 11px `#666` centered lines at y=222/240: "the original notice said \"we share with partners\" — it did not describe this sale," / "and after two resales nobody in the chain can reconstruct the terms".
- **Regulation strip:** 640×40 at (40, 262), fill `#f8f9fa`, 1.5px `#ccc` border; 11px `#555` centered lines (y=278/294): "Vermont & California: broker registration · California DELETE Act: one-stop deletion requests" / "US FTC: actions over sale of sensitive location data".
- **Caption (12px `#999`, centered, y = h−14):** "If the seller cannot explain the chain, the dataset is a liability, not an asset"

## Regeneration instructions

- **Template:** backlog multi-section detail page. After h1, `.subtitle`, and `.intro` callout, one `.lang-section` per numbered section: `h2` (1.3rem `#1a5276`, `border-bottom: 2px solid #2980b9`, 4px padding-bottom), then a `table.layout` with one `<tr>`: left `<td class="text-col">` (45%) with lead paragraph, bullets, `.key-point`; right `<td class="viz-col">` (55%) with the canvas. Section h2 headings carry the "1.–4." numbers shown above.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with `border-bottom: 2px solid #2980b9`; `.subtitle` `#666` 0.95rem; `.intro` — background `#f0f4f8`, `border-left: 3px solid #2980b9`, padding 8px 12px, 0.9rem, 32px margin-bottom; `.key-point` — background `#f8f9fa`, `border-left: 3px solid #e74c3c`, padding 8px 12px, 0.9rem; `ul` 0.92rem; canvases `width: 100%`, `border: 1px solid #e0e0e0`, radius 4px; `.lang-section` 40px margin-bottom.
- **Bullet style:** each bullet is one line — a bold label plus a short phrase that must not text-wrap; labels are colored via `li strong { color: #1a5276; }`. Split long content into more labeled bullets rather than wrapping.
- **Canvases:** intrinsic width 720, heights 360/400/340/340 as specified; shared `setupCanvas(id, h)` helper scales by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). Canvas fonts use `-apple-system, sans-serif`.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`; grays `#555`/`#666`/`#888`/`#999`/`#bbb`/`#ccc`; translucent fills `rgba(26,82,118,0.12)`, `rgba(39,174,96,0.12)`, `rgba(39,174,96,0.10)`, `rgba(142,68,173,0.08)`.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
