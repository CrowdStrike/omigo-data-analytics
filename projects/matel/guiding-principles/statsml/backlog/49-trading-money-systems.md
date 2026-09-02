# Trading & Money-Making Systems

**Page type:** detail page (backlog-style two-column layout: text left 45%, canvas right 55%, one `.lang-section` per topic; BACKLOG status badge in h1)
**HTML title tag:** Trading & Money-Making Systems

**Status badge:** BACKLOG (inline in h1; on this page the badge is orange `#e67e22` background with white text)

**Subtitle:** How different systems for exchanging value actually work — mechanics, governance, incentive structures, distribution shapes, and who profits from the design

**Intro callout:** For each system, build a detailed card covering: how money flows, who sets the rules, what distributions emerge, where the house edge lives, how participants actually make/lose money, and what statistical signatures distinguish skill from luck in that system.

## 1. Approach per System

- **Flow diagram** — who pays whom, where does money enter/exit the system
- **Governance model** — rules, who changes them, response time to abuse
- **Return distribution** — empirical shape with fat tail analysis
- **House edge anatomy** — decompose the total cost of participating
- **Skill identification** — minimum sample size to detect edge above noise
- **Ruin dynamics** — Kelly fraction, typical overbetting, time to ruin
- **Cross-system arbitrage** — where the same information creates different prices
- **Pathological case** — concrete scenario where a participant is systematically exploited by the system design

### Visualization (canvas `c1`, 720×320)

Flowchart: 2×3 grid of pipeline stage boxes with orange connector arrows between column pairs.

- **Title (bold 14px system-ui, `#1a5276`, centered at y=25):** "Dissection Pipeline: One Card per System".
- **Boxes (210×52, at x0=100 with 100px column gap, y0=55 with 20px row gap; 2 columns × 3 rows, filled row by row):**
  1. "Money flow" / "who pays whom" — fill `rgba(26,82,118,0.35)` (first box)
  2. "Governance" / "who sets rules" — white fill
  3. "Distribution" / "shape + tails" — white fill
  4. "House edge" / "cost anatomy" — white fill
  5. "Skill vs luck" / "sample size" — white fill
  6. "Ruin dynamics" / "Kelly, time to 0" — red fill `#e74c3c` with white text (last box)
  - All boxes stroked `#1a5276` width 1.5; first line bold 13px, second line 12px, dark text `#222` except last box white.
- **Arrows:** one horizontal orange (`#e67e22`, width 2) arrow with filled triangular head between the two boxes of each row.
- **Caption (13px `#444`, centered at y=300):** "plus: cross-system arbitrage and one pathological case where the design exploits the participant".

## 2. Systems to Dissect

Data table (`table.data`, blue header row, bordered cells, first column bold):

| System | Governance | Hours | Key Question |
|--------|-----------|-------|--------------|
| Stock Market (Equities) | SEC, FINRA, exchange rules | 6.5h/day, 252 days/yr | How do market makers, dark pools, and HFTs extract value from retail flow? |
| Options & Derivatives | SEC, CFTC, OCC | Exchange hours + some 24h | Why does selling volatility work until it doesn't — and what's the ruin probability? |
| Forex | Decentralized (interbank), light regulation | 24h Mon-Fri | Where does retail forex broker profit come from — spread, B-book, or both? |
| Crypto (CEX) | Minimal, exchange-defined | 24/7/365 | How does 24/7 liquidity + no circuit breakers change crash dynamics? |
| Crypto (DeFi/DEX) | Smart contract rules, no human governance | 24/7/365 | What is MEV (Maximal Extractable Value) and who actually captures it? |
| Prediction Markets | CFTC (Kalshi), unregulated (Polymarket) | Event-dependent | How does the binary outcome structure change position sizing and Kelly criterion? |
| Sports Betting | State-licensed, odds-maker controlled | Event-dependent | Where exactly is the vig hiding, and what's the true house edge distribution? |
| Casino / Poker | Gaming commissions, mathematically fixed odds | 24/7 | Which games have player-exploitable edge and which are provably negative EV? |
| Mobile Game Economies | Developer-controlled, no regulation | 24/7 | How do virtual currencies create artificial scarcity and inflation by design? |
| In-Game Item Markets | Platform rules (Steam, CS2) | 24/7 | How do skin/item markets behave like unregulated securities with insider info? |
| NFT / Digital Collectibles | None (marketplace T&C only) | 24/7 | What does the dead-collection distribution look like and why is 95%+ illiquid? |
| Real Estate Investment | Local/state/federal (heavy) | Illiquid (days-weeks) | How does illiquidity change the return distribution vs liquid markets? |
| Bond Market | SEC, FINRA, Fed rate-driven | OTC + exchange hours | Why does the same bond yield different returns to different holders (tax, duration)? |
| Commodity Futures | CFTC, exchange rules | Near-24h electronic | How does contango/backwardation create invisible drag on retail ETF holders? |
| Crowdfunding / Equity CF | SEC Reg CF, platform rules | Campaign windows | What's the actual return distribution for equity crowdfunding investors? |
| Peer-to-Peer Lending | SEC, state lending laws | Platform hours | How does adverse selection guarantee the average retail lender loses money? |
| Yield Farming / Staking | Protocol-defined, no recourse | 24/7 | What's the real APY after impermanent loss, smart contract risk, and token dilution? |
| Lottery / Scratch Cards | State-run monopoly | Purchase windows | Why is the expected value transparently negative yet volume increases? |

## 3. Comparison Dimensions

- **Governance:** who sets the rules? Can they change mid-game? What recourse do participants have?
- **Information asymmetry:** who knows what, when? Is insider trading possible/illegal/built-in?
- **House edge / rake:** where is value extracted? Spread, fees, inflation, vig, platform cut?
- **Liquidity profile:** can you exit? How fast? What's the cost of urgency?
- **Return distribution:** normal? Fat-tailed? Bounded? What's the skew? Tail index?
- **Skill vs luck:** how many trades/bets to statistically distinguish skill? (sample size for significance)
- **Ruin probability:** given typical position sizing, what fraction of participants hit zero?
- **Compounding dynamics:** does the system allow compounding? Additive or multiplicative?
- **Manipulation surface:** how easy is it to manipulate price/outcome? Cost of manipulation?
- **Tax treatment:** how does tax structure change the effective return distribution?

### Visualization (canvas `c2`, 720×360)

Horizontal bar chart of market availability (hours per week) from the Hours column.

- **Title (bold 14px, `#1a5276`, centered at y=25):** "Market Availability: Hours per Week".
- **Bars (left=160, right=620, top=55, row height 34, gap 18, scale max 168h):**
  | Name | Hours | Note | Fill |
  |------|-------|------|------|
  | Stock market | 32.5 | 6.5h/day, 5 days | green `#27ae60` (≤40h) |
  | Commodity futures | 115 | near-24h electronic | `rgba(26,82,118,0.35)` |
  | Forex | 120 | 24h Mon-Fri | `rgba(26,82,118,0.35)` |
  | Casino / poker | 168 | 24/7 | red `#e74c3c` (168h) |
  | Crypto (CEX/DeFi) | 168 | 24/7/365 | red `#e74c3c` (168h) |
- Bars stroked `#1a5276` width 1; name label bold 13px `#222` right-aligned left of bar; hours value bold 13px right of bar end; note 12px `#666` below the hours value.
- **Caption (13px `#444`, centered at y=340):** "bounded hours act as forced cooling-off; 24/7 systems never let participants step away".

## 4. Key Themes to Surface

- **24/7 vs bounded hours** — how availability changes volatility clustering and sleep-deprivation risk
- **Regulation = friction = protection** — circuit breakers, settlement delays, and why they exist
- **Virtual currency as control mechanism** — decoupling from real money enables psychological manipulation
- **Liquidity illusion** — systems that appear liquid until you try to exit at scale
- **Survivor bias in success stories** — the distribution of outcomes vs the distribution of narratives
- **Designed addiction loops** — variable ratio reinforcement schedules in trading apps and games

### Visualization (canvas `c3`, 720×340)

Horizontal gradient spectrum bar with tick-marked system labels alternating above/below.

- **Title (bold 14px, `#1a5276`, centered at y=25):** "Governance Spectrum: Heavy Regulation to None".
- **Spectrum bar:** from x=60 to x=660, 16px tall centered at y=170, filled with a linear gradient: green `#27ae60` at 0, orange `#e67e22` at 0.5, red `#e74c3c` at 1.
- **End labels:** bold 13px `#444` — "heavy regulation" (left, y+90) and "no regulation" (right, y+90); 12px sub-labels "friction = protection" (left, y+108) and "no recourse" (right, y+108).
- **Tick marks (stroke `#1a5276` width 2, 36px stems; label bold 12px `#222` centered):** positions as fraction of bar, alternating above/below:
  | Fraction | Label | Side |
  |----------|-------|------|
  | 0.04 | Real estate | above |
  | 0.16 | Stocks / bonds | below |
  | 0.30 | Options / futures | above |
  | 0.44 | Sports betting | below |
  | 0.58 | Forex (retail) | above |
  | 0.72 | Crypto CEX | below |
  | 0.86 | DeFi / game economies | above |
  | 0.97 | NFT | below |
- **Caption (13px `#444`, centered at y=315):** "further right: rules can change mid-game, manipulation is cheaper, and losses have no recourse".

## Regeneration instructions

- **Template/layout:** backlog kusto-style detail page. `<h1>` with inline `<span class="status">BACKLOG</span>` badge, `.subtitle` paragraph, `.intro` callout, then four `.lang-section` blocks. Sections 1, 3, 4 use `table.layout` with one row: left `td.text-col` (45%) with a `<ul>` of bold-labeled bullets, right `td.viz-col` (55%) with the canvas. Section 2 contains only a `table.data`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with 2px `#2980b9` bottom border. h2 1.3rem `#1a5276`, 2px `#2980b9` bottom border. `.subtitle` `#666` 0.95rem. `.intro` background `#f0f4f8`, left border 3px `#2980b9`, 0.9rem. `.status` badge on this page: background `#e67e22`, white text, radius 3px, 0.7em bold. `table.data`: th background `#1a5276` white text, all cells bordered 1px `#ddd`, 0.85rem, first column bold nowrap. `ul` 0.92rem. Canvases `width: 100%`, border 1px `#e0e0e0`, radius 4px.
- **Palette:** `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange, `rgba(26,82,118,0.35)` bar fill.
- **Canvas rendering:** canvases declare intrinsic 720×N size and are scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id, hgt)` helper; fonts are system-ui.
- Note: in regenerated HTML any card/page links use `.html` extensions (this page has none).
