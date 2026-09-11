# Auctions

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Auctions

**Subtitle:** Three bidders, one guitar: pay-your-own-bid auctions force strategic shading, while pay-the-runner-up auctions make honest bidding the best move

## One Guitar, Three Sealed Envelopes

**Tags:** `core idea` (blue), `first-price` (orange), `bid shading` (green)

- **The guitar** — one vintage guitar, three bidders: Ana values it $500, Ben $400, Cara $300
- **Sealed bids** — each writes one secret bid; the highest wins and pays exactly what they wrote
- **Why shade** — bidding her full $500 and winning at $500 leaves Ana zero profit, so she bids less
- **The guessing game** — the best shade depends on rivals' bids, which depend on guesses about hers
- **The name** — this is a first-price sealed-bid auction: the winner pays their own top bid

*Example (italic):* Ana shades to $410 and wins with $90 profit — but a rival bid of $420 would have cost her the guitar.

**Key point:** In a first-price auction your own bid sets your price, so bidding your true value earns nothing — the real game is predicting everyone else's shading.

### Visualization (canvas `c1`, 720×300)

Grouped bar chart: for each of the three bidders, a "true value" bar next to a "sealed bid" bar, showing everyone bidding below value.

- **Title (bold 15px, `#1a5276`, top center):** "First-Price Sealed Bids: True Value vs Shaded Bid (illustrative)".
- **Data:** bidders Ana, Ben, Cara; values `[500, 400, 300]`; shaded bids `[410, 330, 240]`.
- **Legend (top left, y=38):** 12×12 swatch `rgba(42,120,214,0.35)` with `#2a78d6` 1px border labeled "true value" (12px `#444`); 12×12 swatch `rgba(217,89,38,0.6)` labeled "sealed bid".
- **Axes:** L-shaped 1px `#999` axis, vertical from (60, 70) down to baseline y=245, horizontal to x=660; chart height 175, y scale 0–550 (yOf(v) = 245 − v/550×175).
- **Bars:** group i left edge at x = 110 + i×180; value bar 55px wide, fill `rgba(42,120,214,0.35)` with 1.5px `#2a78d6` border; bid bar 55px wide at group left + 63, fill `rgba(217,89,38,0.6)`; bold 12px value labels above each bar ("$500" etc. in `#2a78d6`, "$410" etc. in `#d95926`); bidder names 13px `#444` centered below baseline at group left + 59.
- **Shade bracket (Ana):** magenta `#d55181` 2px vertical bracket at x=235 from yOf(500) to yOf(410) with 6px end ticks; bold 12px magenta label "shade = $90" left-aligned at x=243, vertically centered on the bracket.
- **Annotation (bold 13px `#d95926`, centered at x=500):** two lines at y=88 and y=106: "Ana wins and pays her own $410 —" / "the $90 shade is her profit, if she guessed right".

## Change One Rule: Pay the Runner-Up's Bid

**Tags:** `second-price` (green), `Vickrey` (blue), `dominant strategy` (orange)

- **One rule change** — the highest bid still wins, but the winner pays the second-highest bid
- **Same guitar** — all three bid their true values $500, $400, $300; Ana wins and pays Ben's $400
- **Price decoupled** — your bid decides only whether you win; someone else's bid sets the price
- **The name** — a second-price sealed-bid auction, also called a Vickrey auction
- **No guessing** — truthful bidding is best no matter what rivals do: a dominant strategy

*Example (italic):* Ana writes her honest $500, wins, and pays $400 — a $100 profit with zero modeling of her rivals.

**Key point:** Splitting "who wins" from "what the winner pays" removes the reward for lying — bidding your true value becomes the dominant strategy.

### Visualization (canvas `c2`, 720×300)

Bar chart of the three truthful bids with a dashed horizontal line at the second-highest bid marking the price the winner actually pays.

- **Title (bold 15px, `#1a5276`, top center):** "Second-Price: Bid Your Value, Pay the Runner-Up".
- **Data:** truthful bids Ana 500, Ben 400, Cara 300.
- **Axes:** same L-shaped 1px `#999` axis as c1 (vertical at x=60, baseline y=245, horizontal to x=660), chart height 175, scale 0–550.
- **Bars:** 80px wide at x = 130 + i×180, fill `rgba(0,131,0,0.4)`; Ana's bar gets an extra 2px `#008300` border; bold 12px green bid labels above bars; names 13px `#444` below baseline, Ana's reading "Ana (wins)".
- **Price line:** violet `#4a3aa7` 2px dashed (dash 6/4) horizontal line at yOf(400) across x=60→660; bold 13px violet label "price paid = 2nd-highest = $400" right-aligned at x=655, 8px above the line.
- **Profit bracket (Ana):** blue `#2a78d6` 2px vertical bracket at x=225 from yOf(500) to yOf(400) with 6px end ticks; bold 13px blue label "+$100 profit" left-aligned at x=235.
- **Takeaway (bold 13px `#1a5276`, centered at y=288):** "your bid decides if you win — Ben's bid decides what you pay".

## Why Honesty Wins: Ana's Payoff Table

**Tags:** `worked example` (blue), `payoff table` (green), `dominant strategy` (orange)

- **Ana's options** — value $500; compare bids of $450 (shade), $500 (truthful), $550 (overbid)
- **Rival low ($380)** — all three bids win and pay $380: the same +$120 profit either way
- **Underbid loses** — against a $470 rival, the $450 bid forfeits a win worth +$30
- **Overbid loses** — against a $520 rival, the $550 bid wins at $520 and books a −$20 loss
- **Never beaten** — the truthful row matches or beats both alternatives in every column

*Example (italic):* Truthful payoffs (+$120, +$30, $0) are never worse — shading and overbidding each lose in exactly one case.

**Key point:** Under second-price rules, deviating from your value never helps and sometimes hurts: underbids forfeit profitable wins, overbids buy losses. That is what "dominant strategy" means.

### Visualization (canvas `c3`, 720×300)

A 3×3 payoff table drawn on canvas: rows are Ana's bid choices, columns are the top rival bid, cells show Ana's profit; the truthful row is outlined.

- **Title (bold 15px, `#1a5276`, top center):** "Ana's Payoff Under Second-Price Rules (value = $500)".
- **Subtitle line (12px `#6b7280`, centered at y=44):** "win ⇒ pay the rival's bid; profit = $500 − price".
- **Grid geometry:** cells start at x0=210, column width 155, first row top y0=88, row height 50 (3 rows, 3 columns); 1px `#ccc` cell borders.
- **Column headers:** "top rival bid" 12px `#6b7280` centered over the middle column at y=60; "$380", "$470", "$520" bold 13px `#444` at y=78 over each column.
- **Row labels (right-aligned at x=198, vertically centered per row, 13px `#444`):** "bid $450 (shade)", "bid $500 (truthful)" (bold), "bid $550 (overbid)".
- **Cell contents (bold 13px, centered):** row 1: "+$120", "$0 — lose", "$0 — lose"; row 2: "+$120", "+$30", "$0 — lose"; row 3: "+$120", "+$30", "−$20 loss". Positive cells in `#008300` with `rgba(0,131,0,0.08)` tint; zero cells in `#6b7280` with no tint; the loss cell in `#e74c3c` with `rgba(231,76,60,0.10)` tint.
- **Truthful-row highlight:** 2.5px `#1a5276` rectangle around the middle row spanning from x=60 (covering the row label) to the right edge of the grid.
- **Takeaway (bold 13px `#1a5276`, centered at y=272):** "truthful $500 is never beaten in any column — that's a dominant strategy".

## Ad Auctions and the Revenue Myth

**Tags:** `where it's used` (blue), `common mistake` (red), `revenue equivalence` (green)

- **Ad exchanges** — these marketplaces popularized second-price rules, running ad auctions for years
- **Why sellers chose it** — bidders can submit true values once instead of re-tuning shades daily
- **The myth** — "the seller only collects the second bid, so it must earn less" — not so
- **Revenue equivalence** — bids arrive pre-shaded in first-price, so average revenue matches
- **Our guitar** — first-price collected Ana's shaded $410; second-price collected Ben's $400

*Example (italic):* The seller gives up about $100 versus Ana's value either way — through her shading in one format, through the rule in the other.

**Common mistake:** Assuming second-price means less revenue. Bidders shade under first-price rules, so expected revenue is the same under standard assumptions (revenue equivalence).

### Visualization (canvas `c4`, 720×300)

Two revenue bars — what the seller collects under each format for the same guitar — with a dashed reference line at the winner's true value.

- **Title (bold 15px, `#1a5276`, top center):** "Seller Revenue, Same Guitar: First-Price vs Second-Price (illustrative)".
- **Annotation (bold 13px `#4a3aa7`, centered at y=60):** "nearly identical here — exactly equal on average (revenue equivalence)".
- **Axes:** L-shaped 1px `#999` axis, vertical at x=100 from y=70 to baseline y=235, horizontal to x=620; chart height 165, scale 0–550 (yOf(v) = 235 − v/550×165).
- **Reference line:** `#6b7280` 1.5px dashed (dash 5/4) horizontal line at yOf(500) from x=100 to x=620; 12px `#6b7280` label "winner's true value $500" right-aligned at x=618, 6px above the line.
- **First-price bar:** x=170, width 130, height to $410, fill `rgba(217,89,38,0.55)`; bold 13px `#d95926` label "$410" above; captions centered at x=235: bold 12px `#444` "first-price" (y=257) and 12px "winner pays own shaded bid" (y=274).
- **Second-price bar:** x=420, width 130, height to $400, fill `rgba(0,131,0,0.4)`; bold 13px `#008300` label "$400" above; captions centered at x=485: bold 12px "second-price" (y=257) and 12px "winner pays runner-up's $400" (y=274).

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
