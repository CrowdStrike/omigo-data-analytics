# 3. Auctions & Bidding Mechanics

**Page type:** detail page (numbered h2 sections, each an obj-table row: text left 45% with math-boxes and bullets, canvas right 55%; philosophy callouts at top and bottom)
**HTML title tag:** 3. Auctions & Bidding Mechanics

**Subtitle:** Why second-price auctions make honesty the dominant strategy — and why that guarantee quietly breaks when platforms sell multiple slots.

## Callout (philosophy box, top)

**Core insight:** In a second-price (Vickrey) auction, the winner pays the second-highest bid. This single design choice eliminates all strategic bidding — your best move is always to bid your true value. The platform gets honest signals AND maximum revenue.

## 1. First-Price vs Second-Price Auctions

**Obj-title:** The Strategic Problem

**Math-box 1:**
**First-price auction:**

Winner pays their own bid.
You value the item at $100.
If you bid $100 and win → profit = $0.
So you shade your bid down: $80? $70?

`Everyone is guessing everyone else's strategy.`

**Math-box 2:**
**Second-price auction:**

Winner pays the SECOND-highest bid.
You value the item at $100.
If you bid $100 and win, you pay $75 (the next bid) → profit = $25.

`Your bid doesn't affect your price. Only whether you win.`

- **Overbid ($120):** You might win and pay $105 — more than it's worth to you. Loss.
- **Underbid ($80):** If second-highest is $85, you lose a deal that was profitable. Missed profit.
- **Honest bid ($100):** You win every profitable deal, never overpay. Dominant strategy.

### Visualization (canvas `canvas1`, 720×380)

Two-panel side-by-side comparison of bidder behavior under first-price vs second-price rules.

- **Title (bold 14px `#1a5276`, centered):** "First-Price vs Second-Price Auction" at y=22.
- **Panel headers (bold 13px, centered over each 300px panel):** "FIRST-PRICE" in `#e74c3c` (left panel at x=40), "SECOND-PRICE" in `#27ae60` (right panel at x=380).
- **Left panel rows** (three bidders, 50px apart; name 12px `#333`, bid line bold 12px in bidder color):
  - "Alice (values $100)" — "Bids: $80 (shaded)" in `#2980b9`
  - "Bob (values $90)" — "Bids: $72 (shaded)" in `#e67e22`
  - "Carol (values $70)" — "Bids: $55 (shaded)" in `#8e44ad`
- **Left panel result:** "Alice wins, pays $80" bold 12px `#e74c3c`; then 11px `#666`: "Everyone guessing. Inefficient." / "Alice could have bid $73 and still won."
- **Right panel rows** (same bidders/colors): "Bids: $100 (honest)", "Bids: $90 (honest)", "Bids: $70 (honest)".
- **Right panel result:** "Alice wins, pays $90 (Bob's bid)" bold 12px `#27ae60`; then 11px `#666`: "No guessing needed. Simple." / "Alice profits $10 with zero strategy."
- **Bottom line (bold 12px `#1a5276`, centered at y=350):** "Same outcome, but second-price requires zero game theory from bidders."

## 2. Google's GSP: Second-Price in Name Only

**Obj-title:** Generalized Second-Price (GSP)

**Math-box 1:**
**Google Ads auction (per keyword):**

- Advertisers submit max bid per click
- Winner gets top slot, pays $0.01 above the second bid
- Second place pays $0.01 above third bid
- And so on...

Each advertiser pays just enough to beat the one below them.

**Math-box 2:**
**The catch — GSP is NOT truthful:**

Vickrey's guarantee is for ONE item. With multiple slots, your bid no longer just decides *if* you win — it decides *which slot* you get, and lower slots are cheaper per click.

Shading your bid can drop you to a cheaper slot whose lower price more than compensates for fewer clicks (Edelman, Ostrovsky & Schwarz, 2007).

`Rational GSP bidders shade below true value. Only VCG restores truthfulness with multiple slots.`

- **eBay proxy bidding:** Genuinely truthful — it's a single-item second-price auction, so Vickrey's result applies.
- **VCG:** The multi-slot mechanism that IS truthful — each bidder pays the externality they impose on others. Facebook used it; Google stuck with GSP.
- **The real winner:** The auction designer. Always.

### Visualization (canvas `canvas2`, 720×380)

Table-style diagram of a Google Ads keyword auction with four advertisers and slot pricing.

- **Title (bold 14px `#1a5276`, centered):** 'Google Ads Auction: Keyword "running shoes"' at y=22.
- **Column headers (bold 11px `#666`):** "ADVERTISER" (x=50), "MAX BID/CLICK" (x=200), "ACTUALLY PAYS" (x=370), "POSITION" (x=530), at y=55.
- **Rows** (65px tall; top row background `rgba(41,128,185,0.06)`, alternating `#fafafa`/white below; advertiser bold 13px in row color, bid 13px `#333`, pays bold 13px `#27ae60` (or `#999` for "—"), position 12px `#666`):
  - Nike — $3.50 — $2.51 — Top (`#2980b9`)
  - Adidas — $2.50 — $1.76 — 2nd (`#27ae60`)
  - Puma — $1.75 — $1.01 — 3rd (`#e67e22`)
  - New Balance — $1.00 — — — Below fold (`#999`)
- **Orange dashed connector** (`#e67e22`, dash 4/3, width 1.5) from Nike's "pays" cell down-left to Adidas's bid cell, labeled "pays next + $0.01" 11px `#e67e22`.
- **Takeaway (centered):** "But truthful bidding is NOT optimal here. Say slot 2 gets 80 clicks vs slot 1's 100." bold 12px `#e74c3c` at y=345; "Nike truthful: ($3.50−$2.51)×100 = $99. Nike shades to $2.00, takes slot 2: ($3.50−$1.76)×80 = $139." 11px `#666` at y=365.

## 3. Why Honesty Is a Nash Equilibrium

**Obj-title:** No Incentive to Deviate

**Math-box 1:**
**Nash equilibrium:** A state where no player can improve their outcome by changing only their own strategy.

In a single-item second-price auction:
- If everyone bids honestly, no one can do better by lying.
- Overbidding risks paying more than value (loss).
- Underbidding risks missing profitable wins (opportunity loss).

`Bidding your true value is a weakly dominant strategy.`

**Math-box 2:**
**The beauty of mechanism design:**

You don't need people to be honest or altruistic.
You design the rules so that *selfishness produces honesty*.

The auction doesn't ask "please be honest."
It makes honesty the only rational choice.

- **Vickrey (1961):** Proved this formally. Won the Nobel Prize in Economics.
- **Practical caveat:** Works cleanly with independent private values. Correlated values (e.g., oil leases) need more complex mechanisms.
- **Real platforms:** Google moved from GSP to a first-price auction in 2019 — the theory gets messier at scale.

### Visualization (canvas `canvas3`, 720×380)

Three stacked rounded strategy boxes comparing overbid, honest, and underbid outcomes.

- **Title (bold 14px `#1a5276`, centered):** "Your Strategy Options (You Value Item at $100)" at y=22.
- **Rows** (rounded rects 620×85, radius 8, at x=50, 100px apart starting y=55; fill row color at ~4% alpha (`color + '0a'`), stroke row color width 2; mark bold 24px at x=85; label bold 14px; outcome 12px `#333`; risk 12px `#666`):
  1. "Overbid ($120)" — mark "✗" — outcome "Win if 2nd bid ≤ $120" — risk "If 2nd bid = $105 → you pay $105 for $100 value → LOSS of $5" — color `#e74c3c`.
  2. "Honest ($100)" — mark "✓" — outcome "Win if 2nd bid < $100" — risk "You never pay > $100. Every win is profitable. Best strategy." — color `#27ae60`.
  3. "Underbid ($80)" — mark "✗" — outcome "Win if 2nd bid < $80" — risk "If 2nd bid = $85 → you lose. That deal was worth $15 profit to you." — color `#e67e22`.
- **Bottom line (bold 12px `#27ae60`, centered at y=365):** "Honest bidding: you cannot do better by deviating. That's Nash equilibrium."

## Callout (philosophy box, bottom)

**The takeaway:** The best systems don't require good behavior — they make good behavior the self-interested choice. When you see an auction, ask: "Who designed these rules, and what behavior do the rules reward?" That's the real game.

## Regeneration instructions

- **Layout:** h1 + `.subtitle`, top `.philosophy` callout, then numbered `<h2>` sections (1.4em `#1a5276`, 2px solid `#2980b9` bottom border), each containing a `.obj-table` with one `<tr>`: left `<td>` (45%) holds `.obj-title`, `.math-box` divs, and a `<ul>`; right `<td>` (55%, centered) holds the canvas. Closing `.philosophy` callout after the last section.
- **Page style:** body -apple-system/Segoe UI sans-serif, white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; subtitle `#666` 1.05em; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em weight 600 `#1a5276`; `strong` `#1a5276`; `.philosophy` background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em; `.math-box` background `#f8fafb`, border `1px solid #e0e0e0`, radius 6px, padding 16px 20px, 0.9em, with `code` background `#eef2f7`. No nav bar, no back/home links.
- **Canvas:** intrinsic `width="720" height="380"` attributes; scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setupCanvas(id, w, h)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, grays `#999`/`#666`/`#333`.
- Note: in regenerated HTML, any card links would use `.html` extensions.
