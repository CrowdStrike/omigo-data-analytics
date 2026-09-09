# Commercial Data Feeds — Subscribing to Someone Else's Collection Machine

**Page type:** detail page (four titled sections, each a two-column row: text left 45%, canvas right 55%)
**HTML title tag:** Commercial Data Feeds — Subscribing to Someone Else's Collection Machine

**Subtitle:** Entire industries run on data they never collect themselves: they subscribe to vendors who operate the sensors, panels, crawlers, and exchange connections, and who deliver the cleaned result on a schedule. You pay for coverage, freshness, and cleaning that no single team could ever build alone.

**Intro callout (blue-left-border box):** A commercial feed is the industrial version of "someone else's data": a vendor runs a permanent collection machine — thousands of sensors, panelists, honeypots, or exchange connections — and sells recurring access to its output. Security teams, trading desks, retailers, and mapping products are all built on subscriptions like these. The newcomer's surprise is how much of the professional data world is bought, not gathered.

## 1. What a feed subscription is

A feed is recurring delivery of a maintained dataset under a license — you are buying access to a stream, not ownership of the data.

- **Recurring delivery:** updates pushed on a contract cadence
- **Value tied to cadence:** the feed is only worth the updates continuing
- **Delivery mechanisms:** query API, bulk file drop, cloud data share
- **Priced per mechanism:** each access mode has its own price
- **License, not ownership:** the contract defines what you may do
- **Internal analysis:** usually allowed
- **Redistribution:** usually not allowed
- **Models for resale:** often negotiated fine print
- **Access expires:** stop using the data when the subscription ends
- **Deletion clauses:** some licenses require deleting historical copies
- **Permanent dependency:** the vendor stays for the life of your models

Key point: A feed purchase is a relationship, not a transaction: you inherit the vendor's collection quality, their update schedule, and their license terms for as long as the models built on the feed stay in production.

### Visualization (canvas `c1`, 720×380)

Pipeline diagram: collection sources fan in to the vendor's machine, one feed arrow out, splitting into three delivery-mechanism boxes, with a license bracket underneath.

- **Title (bold 14px `#1a5276`, centered, y=22):** "You subscribe to the output of a machine someone else runs"
- **Collection sources (left, 11px `#666` right-aligned labels at x=150, y=70/105/140/175, each with a 1px `#bbb` connector line to the vendor box):** "sensors & crawlers", "panels & partners", "exchange connections", "honeypots & scanners"
- **Vendor box:** 190×130 at (170, 58), fill `rgba(26,82,118,0.12)`, 2px `#1a5276` border. Bold 12px `#1a5276` centered: "VENDOR"; 11px `#555` centered lines: "collects continuously," / "cleans, deduplicates," / "resolves entities"; 11px `#999`: "the machine never stops".
- **Feed arrow:** 3px `#1a5276` horizontal line from (360, 123) to (420, 123) with filled arrowhead; bold 11px `#1a5276` above: "the feed".
- **Delivery boxes (each 250×48 at x=430, white fill, 2px border in box color; bold 12px label, 11px `#666` subline):**
  - y=52 "Query API" `#27ae60` — "per-lookup, real-time answers"
  - y=116 "Bulk file drop" `#e67e22` — "daily files into your cloud bucket"
  - y=180 "Warehouse share" `#8e44ad` — "vendor tables appear in your SQL"
  - Connectors: 1.5px `#bbb` lines from the feed arrow's end fanning to each box's left edge.
- **License bracket:** dashed (6/5) 1.5px `#e74c3c` rectangle around the three delivery boxes region (x=424, y=44, 264×192); bold 11px `#e74c3c` centered below (y=258): "LICENSE BOUNDARY"; 11px `#666` centered lines below: "internal analysis: allowed · redistribution: not allowed" / "access ends when the subscription ends".
- **Caption (12px `#999`, centered, y = h−14):** "You buy access to the stream, not ownership of the data"

## 2. The feed markets — a survey

Almost every industry has a vendor ecosystem selling the data the industry runs on; four markets show the range.

- **VirusTotal:** file and URL reputation lookups
- **Recorded Future:** curated threat intelligence
- **Shodan:** index of internet-connected devices
- **abuse.ch:** free community malware and botnet trackers
- **Bloomberg Terminal:** consolidated market data and news
- **LSEG (Refinitiv):** consolidated feeds and news
- **Direct exchange feeds:** raw proprietary data for latency-sensitive firms
- **Nielsen:** audience measurement panels
- **Circana:** retail point-of-sale and shopper panels
- **Panel value:** the market beyond your own checkout
- **HERE and TomTom:** road networks, live traffic, location services
- **Mapping buyers:** automakers and logistics build on top, no fleets

Key point: The pattern repeats across markets: collection has enormous fixed costs and strong economies of scale, so one vendor collects once and thousands of subscribers share the cost — nobody rebuilds Bloomberg or Nielsen in-house.

### Visualization (canvas `c2`, 720×420)

Quadrant map: 2×2 grid of market boxes, each listing example vendors and what the feed contains.

- **Title (bold 14px `#1a5276`, centered, y=22):** "Four feed markets, one business model: collect once, sell to thousands"
- **Grid:** four boxes, each 320×160, at (35, 46), (375, 46), (35, 226), (375, 226); fill in the box's translucent color, 2px border in box color. Inside each: bold 13px box-color market name at +14/+24; 11px `#555` vendor lines at +14, spaced 20px starting +48; 11px `#999` italic-toned "what you get" line as the last line (+142).
  - Top-left `#e74c3c`, fill `rgba(231,76,60,0.07)`: "Threat intelligence" — "VirusTotal — file & URL reputation" / "Recorded Future — curated intel" / "Shodan — internet device index" / "abuse.ch — community feeds, free" / what: "indicators before attackers reach you"
  - Top-right `#1a5276`, fill `rgba(26,82,118,0.07)`: "Financial market data" — "Bloomberg Terminal — data + news" / "LSEG (Refinitiv) — consolidated feeds" / "Exchanges — direct raw feeds" / "(paid per venue, lowest latency)" / what: "prices, books, and news in one pipe"
  - Bottom-left `#27ae60`, fill `rgba(39,174,96,0.07)`: "Consumer & retail panels" — "Nielsen — audience measurement" / "Circana — retail & shopper panels" / "(point-of-sale + panelist diaries)" / "" / what: "the market beyond your own checkout"
  - Bottom-right `#e67e22`, fill `rgba(230,126,34,0.07)`: "Mapping & traffic" — "HERE — roads, traffic, location APIs" / "TomTom — maps + live traffic" / "(licensed by automakers, logistics)" / "" / what: "a road network you never drove"
- **Caption (12px `#999`, centered, y = h−14):** "Collection has huge fixed costs — one vendor collects, thousands of subscribers share the bill"

## 3. What you are actually paying for

The raw records are the cheapest part of the product; the subscription price buys everything the vendor wrapped around them.

- **Coverage breadth:** thousands of sensors see the whole market at once
- **Your alternative:** in-house collection would see only a sliver
- **Update latency:** contracted freshness, minutes to days by market
- **Freshness pricing:** fresher tiers cost sharply more
- **Entity resolution:** dedup, identifier linking, format normalization
- **Cleaning value:** labor-intensive work that makes the feed usable
- **Historical archives:** consistent back-data for backtesting and training
- **Archive pricing:** licensed separately, often the priciest line item
- **The SLA:** uptime guarantees, delivery windows, support turnaround
- **Why SLA matters:** lets you build production on their pipeline

Key point: When comparing vendors, price the wrapper, not the bits: two feeds with "the same data" can differ enormously in coverage, latency, cleaning quality, and archive depth — which is exactly where the price difference lives.

### Visualization (canvas `c3`, 720×360)

Stacked value bar: one wide horizontal bar split into five segments (raw data small, then cleaning, coverage, archive, SLA), each with a callout label; a small annotation marks the raw-data segment as the cheapest part.

- **Title (bold 14px `#1a5276`, centered, y=22):** "Anatomy of a feed price — the raw bits are the smallest slice"
- **Bar:** height 56, y=120, from x=50 to x=670 (total 620), segments left to right with fills and bold 12px white centered labels (label omitted inside if segment too narrow, drawn outside in segment color instead):
  - 60px `#999` — "raw data" (label above the bar in `#999`, 11px, with a thin connector)
  - 150px `#1a5276` — "cleaning & entity resolution"
  - 140px `#27ae60` — "coverage breadth"
  - 150px `#e67e22` — "historical archive"
  - 120px `#8e44ad` — "SLA & support"
- **Segment sublabels (10px `#666`, centered under the bar at each segment midpoint, y=196, two lines where needed):** "the cheapest part", "dedup, linking, normalizing", "whole market, not a sliver", "backtests & training, often extra", "what production is built on".
- **Annotation:** bold 12px `#e74c3c` centered at y=240: "If you could get the raw bits for free, you would still pay for the other four slices."
- **Freshness note (11px `#666`, centered, y=270):** "Fresher tiers cost sharply more: minutes-old threat intel and market data sit at the top of the price ladder."
- **Caption (12px `#999`, centered, y = h−14):** "Price the wrapper, not the bits — that is where vendors actually differ"

## 4. Evaluating before you buy

Feeds are sold on brochures, but they should be bought on tests you run against your own data during the trial.

- **Trial samples:** insist on a real extract from the live feed
- **Not a sales file:** actual fields at the actual cadence
- **Overlap tests:** join trial data against records you already trust
- **Match rate:** shows how much is new and whether shared parts agree
- **Spot-checks:** verify freshness and accuracy on confirmable events
- **License limits:** redistribution rules and derived-product rights
- **End-of-contract terms:** fate of trained models and cached copies
- **Timing:** read the license before price negotiation, not after

Key point: The single most informative test is the overlap test on your own data: a feed that disagrees with records you can verify will also be wrong in the region you cannot verify — you just will not see it there.

### Visualization (canvas `c4`, 720×340)

Evaluation flow: four step boxes left to right connected by arrows, each with a pass/fail note; a red branch from the license step drops to a "walk away" box.

- **Title (bold 14px `#1a5276`, centered, y=22):** "Four checks between the brochure and the signature"
- **Step boxes (each 150×86 at y=70, x=40/200/360/520, white fill, 2px border in step color; bold 12px step-color title at +12/+20, 11px `#666` sublines at +12, +40/+56/+72):**
  - "1. Trial sample" `#1a5276` — "real extract from" / "the live feed, not" / "a sales file"
  - "2. Overlap test" `#27ae60` — "join against data" / "you already trust;" / "measure agreement"
  - "3. Spot-checks" `#e67e22` — "verify freshness &" / "accuracy on events" / "you can confirm"
  - "4. License read" `#8e44ad` — "redistribution, derived" / "products, end-of-" / "contract terms"
- **Arrows:** 2px `#999` horizontal arrows with filled arrowheads between consecutive boxes (y=113).
- **Fail branch:** dashed (6/5) 2px `#e74c3c` line from the bottom of each step box (x at box center, from y=156) down to y=210, converging into a single box 300×46 centered at x=360, y=218: fill `rgba(231,76,60,0.08)`, 2px `#e74c3c` border, bold 12px `#e74c3c` centered "ANY CHECK FAILS → renegotiate or walk away"; 11px `#666` centered below inside: "cheaper than discovering it in production".
- **Pass exit:** 2px `#27ae60` arrow from box 4's right edge to a small 90×46 box at (680−90=590 → use x=... keep inside canvas) — instead: bold 12px `#27ae60` label to the right of box 4 arrowed at y=113: "sign" (arrow from x=670 shortened to fit, arrowhead at x=690).
- **Caption (12px `#999`, centered, y = h−14):** "Run the tests during the trial — the brochure never mentions the region where the feed is wrong"

## Regeneration instructions

- **Template:** backlog multi-section detail page. After h1, `.subtitle`, and `.intro` callout, one `.lang-section` per numbered section: `h2` (1.3rem `#1a5276`, `border-bottom: 2px solid #2980b9`, 4px padding-bottom), then a `table.layout` with one `<tr>`: left `<td class="text-col">` (45%) with lead paragraph, bullets, `.key-point`; right `<td class="viz-col">` (55%) with the canvas. Section h2 headings carry the "1.–4." numbers shown above.
- **Bullet style:** each bullet is a one-line "**Label:** short phrase" — no text-wrapping sentences; labels are bold and colored via `li strong { color: #1a5276; }`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with `border-bottom: 2px solid #2980b9`; `.subtitle` `#666` 0.95rem; `.intro` — background `#f0f4f8`, `border-left: 3px solid #2980b9`, padding 8px 12px, 0.9rem, 32px margin-bottom; `.key-point` — background `#f8f9fa`, `border-left: 3px solid #e74c3c`, padding 8px 12px, 0.9rem; `ul` 0.92rem; canvases `width: 100%`, `border: 1px solid #e0e0e0`, radius 4px; `.lang-section` 40px margin-bottom.
- **Canvases:** intrinsic width 720, heights 380/420/360/340 as specified; shared `setupCanvas(id, h)` helper scales by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). Canvas fonts use `-apple-system, sans-serif`.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`; grays `#555`/`#666`/`#888`/`#999`/`#bbb`/`#ccc`; translucent fills `rgba(26,82,118,0.12)`, `rgba(26,82,118,0.07)`, `rgba(39,174,96,0.07)`, `rgba(230,126,34,0.07)`, `rgba(231,76,60,0.07)`, `rgba(231,76,60,0.08)`.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
