# Corporate & Market Data Exhaust — Commerce Leaves a Public Trail

**Page type:** detail page (two-column layout table per section: text left 45%, canvas right 55%, one `.lang-section` per topic)
**HTML title tag:** Corporate & Market Data Exhaust — Commerce Leaves a Public Trail

**Subtitle:** Markets and internet infrastructure generate publicly readable data as a side effect of operating. Disclosure rules force it, transparent-by-design ledgers publish it, and open technical registries record it — none of it was collected "about" anyone, yet all of it is readable by everyone.

**Intro callout:** Three distinct mechanisms produce this exhaust. Regulation compels disclosure: a public company must file its financials, its insiders must report their trades. Architecture compels publication: a public blockchain cannot function without every transaction being visible. And participation compels registration: you cannot hold a domain, serve TLS, or route packets without entries in open databases. On top of these sit ordinary commercial surfaces — job postings, prices, reviews, shipping manifests — that firms expose simply by doing business.

## 1. Market data — disclosure by regulation

A public company operates inside a mandatory disclosure regime — the price of access to public capital:

- **Prices and order books** — real-time and historical stock, option, and futures prices; order-book snapshots. The market's core function is publishing a number.
- **SEC EDGAR** — annual 10-K and quarterly 10-Q financials, risk factors, executive pay, material events (8-K), all free and machine-readable.
- **Insider trades** — officers and directors must report their own purchases and sales (Form 4) within days.
- **13F fund holdings** — large fund managers must disclose their equity positions quarterly; whole industries reverse-engineer strategy from these.
- **Short interest** — exchanges publish how much of each stock is sold short.
- **Worldwide equivalents** — Companies House and RNS in the UK, MCA and stock-exchange filings in India, EDINET in Japan. The regime is global, the schemas are not.

**Key point:** Disclosure is asymmetric by design: the company must publish, and anyone — competitor, journalist, model builder — may read. Filings are among the oldest structured public datasets, with decades of consistent history.

### Visualization (canvas `c1`, 720×420)

Hub-and-spoke diagram: a central public-company box ringed by six mandatory-disclosure boxes.

- **Title (bold 14px `#1a5276`, top center):** "What one listed company must expose, continuously".
- **Center box:** 160×84 at center (360, 218), fill `rgba(26,82,118,0.12)`, 2px `#1a5276` stroke; bold 13px "PUBLIC COMPANY", 11px `#666` lines "one ticker" / "one CIK on EDGAR".
- **Disclosure nodes:** 176×46 white boxes with 2px colored stroke, bold 12px title in node color, 10px `#666` subtitle, connected to the center by `#bbb` lines:
  - "Prices & order book" / "every trade, tick by tick" — `#1a5276`, at (118,78)
  - "10-K / 10-Q filings" / "full financials, risk factors" — `#27ae60`, at (602,78)
  - "Insider trades (Form 4)" / "executives' own buys/sells" — `#e74c3c`, at (92,218)
  - "13F fund holdings" / "who owns it, quarterly" — `#8e44ad`, at (628,218)
  - "Short interest" / "bets against it, published" — `#e67e22`, at (118,352)
  - "Material events (8-K)" / "deals, departures, incidents" — `#1a5276`, at (602,352)
- **Caption (12px `#999`, bottom center):** "The same regime worldwide: Companies House (UK), MCA + exchange filings (India), EDINET (Japan)".

## 2. Blockchains — public by design

Bitcoin and Ethereum take transparency to its logical extreme: the ledger only works because everyone can verify it.

- **Every transaction, forever** — sender address, receiver address, amount, timestamp — published to every node, permanently, from the first block onward.
- **Downloadable by anyone** — the full history is a few hundred gigabytes; no permission, no account, no rate limit on running your own node.
- **Pseudonymous, not anonymous** — addresses carry no name, but spending patterns cluster: chain-analysis heuristics group addresses controlled by the same wallet.
- **One join to identity** — the moment any address in a cluster touches an exchange with KYC records, the entire cluster's history — past and future — attaches to a legal name.

**Key point:** This is the most complete financial dataset ever published — every transfer, globally, with perfect retention — and its privacy model is one join away from collapse. A single identified interaction deanonymizes an entire transaction history retroactively.

### Visualization (canvas `c2`, 720×380)

Network diagram: a purple-dashed cluster of six address nodes on the left, one red edge to a KYC exchange box on the right, which points down to a legal-name box.

- **Title (bold 14px `#1a5276`, top center):** "Pseudonymous, not anonymous: one join collapses the cluster".
- **Cluster region:** dashed `#8e44ad` rectangle (dash 5/4) at (40,55) size 300×250, labeled in bold 12px `#8e44ad`: "Cluster: same-wallet heuristics".
- **Address nodes:** six circles radius 17 at (105,125), (235,110), (90,215), (190,180), (280,200), (165,265); fill `rgba(26,82,118,0.35)`, stroke `#1a5276`; each carries a bold 9px monospace pseudo-address label "1A3f..", "1A4f..", … "1A8f..". Intra-cluster edges in `rgba(142,68,173,0.45)` connecting node pairs [0-1, 0-3, 1-3, 2-3, 3-4, 2-5, 3-5, 4-5]. Note below in 10px `#666`: "every transaction between these is on-chain, forever".
- **Identifying edge:** thick 2.5px red `#e74c3c` arrow from node at (280,200) to the exchange box, labeled in bold 11px red: "one deposit".
- **Exchange box:** 160×70 at (430,130), fill `rgba(231,76,60,0.07)`, 2px `#e74c3c` stroke; bold 12px "EXCHANGE"; 11px `#666` lines "KYC: name, ID," / "bank account". A red downward arrow leads to a white 180×46 box at (420,240) with `#27ae60` stroke: bold 12px green "A legal name"; 10px `#666` "now attached to the whole cluster".
- **Caption (12px `#999`, bottom center):** "The join is retroactive: past transactions of every clustered address are re-labeled at once".

## 3. Internet infrastructure registries

The internet's coordination layer is a set of open databases. Operating anything online means writing to them:

- **WHOIS / RDAP** — domain registration records: registrar, creation date, name servers, and (pre-privacy-proxy) registrant contact details.
- **DNS itself** — a globally queryable database; anyone can resolve, enumerate, and archive your records.
- **Certificate Transparency logs** — every TLS certificate ever issued is appended to public, searchable logs. New subdomains leak here the moment a cert is requested — a well-known source of pre-announcement leaks.
- **BGP routing tables** — which networks announce which IP blocks, and who peers with whom, visible to every route collector.
- **IP allocation registries** — RIPE, ARIN and peers publish who holds which address space, with organization names and contacts.

**Key point:** You cannot operate on the internet without registering in public databases — infrastructure participation is inherently public. These registries were built for coordination and trust, but they double as a real-time feed of who is building what.

### Visualization (canvas `c3`, 720×360)

Event timeline: four milestones alternating above/below a horizontal axis, showing a product leaking via infrastructure before launch.

- **Title (bold 14px `#1a5276`, top center):** "A product named \"atlas\" leaks weeks before its announcement".
- **Axis:** horizontal `#888` line at y=190 from x=60 to x=660 with right arrowhead; each event has a filled colored dot (radius 6) on the axis, a colored stem to its box, and a bold 11px `#999` day label on the opposite side of the axis.
- **Event boxes:** 138×84 white with 2px colored stroke; bold 12px title in event color, three 10px `#555` lines:
  - x=120, "day −45", "Cert issued" (`#e74c3c`, box above axis): "atlas.example.com" / "appended to public" / "CT logs, searchable"
  - x=260, "day −38", "DNS goes live" (`#e67e22`, below): "A record resolvable" / "by anyone who" / "guesses or scans"
  - x=400, "day −21", "Job posting" (`#8e44ad`, above): "\"engineer, Atlas" / "platform team\" on" / "the careers page"
  - x=560, "day 0", "Official launch" (`#27ae60`, below): "press release —" / "watchers knew" / "for six weeks"
- **Caption (12px `#999`, bottom center):** "Registering infrastructure is publishing intent — CT logs and DNS have no embargo option".

## 4. Commercial surfaces — business as broadcast

Beyond mandated disclosure, ordinary operations expose readable signals:

- **Job postings** — hiring reveals strategy: which teams are growing, which technologies are in the stack, which cities matter. A new "Head of Payments, Brazil" posting is a roadmap item in public.
- **Layoff notices** — US WARN Act filings are public state records: employer, site, headcount, date — often ahead of any press release.
- **App-store rankings and reviews** — download ranks proxy growth; review text is a free, timestamped complaint corpus.
- **Prices and review corpora** — e-commerce prices are scraped continuously; repricing behavior itself becomes a studied signal.
- **Shipping manifests** — US import bills of lading are public records: shipper, consignee, contents, volume. Sold as competitive intelligence — supply chains readable from customs data.

**Key point:** No single surface is sensitive, but joined they reconstruct a competitor's operations: manifests give the supply chain, postings give the org chart and roadmap, WARN filings give the retreats, app ranks give the traction. The dashboard assembles itself from public parts.

### Visualization (canvas `c4`, 720×420)

Mock competitor-intelligence dashboard: five bordered panels, each with a colored title and a thin `#eee` divider under it.

- **Title (bold 14px `#1a5276`, top center):** "A competitor dashboard assembled entirely from public surfaces".
- **Panel 1 — "Job postings by team" (`#1a5276`, 210×160 at (35,40)):** horizontal bar chart, bars filled `rgba(26,82,118,0.35)` with 1px `#1a5276` stroke, 10px `#555` team labels: ML infra (150), Payments (110), Sales EU (70), Support (30) — bar length = value × 0.8 px. Footnote 10px `#999`: "source: careers page".
- **Panel 2 — "Layoff notices (WARN)" (`#e74c3c`, 210×160 at (255,40)):** a light red record card (`rgba(231,76,60,0.07)` fill, red stroke) with 10px monospace lines "Site: Austin, TX" / "Affected: 214" / "Effective: Oct 15"; note 10px `#666`: "state record, filed before" / "any press coverage"; footnote: "source: state WARN portal".
- **Panel 3 — "App-store rank" (`#27ae60`, 210×160 at (475,40)):** upward trend line in `#27ae60` (2px, 3px dots) through points (490,165), (520,150), (550,155), (580,120), (610,95), (640,75), (668,68); annotation 10px `#666`: "#84 → #12 in finance category"; footnote: "source: public rank charts".
- **Panel 4 — "Import bills of lading" (`#e67e22`, 320×160 at (35,220)):** three-row monospace manifest table with alternating `rgba(230,126,34,0.07)` row shading: "Shenzhen → Long Beach | lithium cells | 40 t", "Shenzhen → Long Beach | lithium cells | 38 t", "Taipei → Oakland | PCB assemblies | 12 t"; note: "supply chain readable from US customs records"; footnote: "source: customs manifest resellers".
- **Panel 5 — "What the join says" (`#8e44ad`, 320×160 at (365,220)):** 11px `#555` inference text: "Building a payments product on ML infra," / "scaling hardware imports for a device," / "expanding sales in Europe while cutting" / "a US support site — traction confirmed" / "by the category-rank climb." Bold 11px `#8e44ad` closing line: "No leak, no insider — public surfaces only."
- **Caption (12px `#999`, bottom center):** "Each panel is innocuous alone; the join reconstructs strategy".

## Regeneration instructions

- **Layout:** backlog detail page (kusto-style 2-col): h1, `.subtitle`, `.intro` callout, then one `.lang-section` per numbered topic. Each section: `<h2>` with 2px `#2980b9` bottom border, then a `table.layout` (border-collapse, full width) with one row: `td.text-col` (45%) holding an intro sentence, a `<ul>` of labeled bullets (bold lead terms), and a `.key-point` div; `td.viz-col` (55%) holding the canvas. No index number in the h1.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; `.intro` background `#f0f4f8` with 3px `#2980b9` left border; `.key-point` background `#f8f9fa` with 3px `#e74c3c` left border; ul 0.92rem. Canvases `width: 100%`, 1px `#e0e0e0` border, 4px radius.
- **Canvases:** intrinsic width 720, heights as given per chart (420/380/360/420); shared `setupCanvas(id, h)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple accent `#8e44ad`, bar fill `rgba(26,82,118,0.35)`, gray text `#666`/`#999`.
- In regenerated HTML, any card links use `.html` extensions (this page has none).
