# Nash Equilibrium & Second-Price Auctions

**Page type:** detail page (h2-sectioned two-column obj-table layout: text left 45%, canvas right 55%; philosophy callouts at top and bottom)
**HTML title tag:** Nash Equilibrium & Second-Price Auctions — Case Study

**Subtitle:** Can you design a system where being selfish and being honest are the same thing? Turns out, yes — and it runs the internet's ad economy.

## Callout (philosophy box, top)

**The question:** In an auction, everyone lies. You say "I'll pay $500" but you really mean "$400 — I'm just bidding high so I don't lose." Everyone inflates or deflates. It's a guessing game.

**Now imagine:** What if there was an auction design where the BEST strategy — the one that makes you the most money — is to tell the exact truth about what something is worth to you? No games, no guessing, no bluffing. Just honesty. That's the second-price auction. And it's a Nash Equilibrium — meaning no player can do better by deviating from honesty.

## 1. First-Price vs Second-Price: Why It Changes Everything

**Obj-title:** The Problem With Normal Auctions

Math box 1:

**First-price auction** (the normal kind):
Highest bidder wins and pays what they bid.

You think the painting is worth $500 to you. Do you bid $500?

**No.** Because if you win at $500, you paid exactly what it's worth — zero profit. So you bid less. Maybe $400. But how much less? You're guessing what others will bid. Everyone is lying about their true value. `Nobody bids honestly.`

Math box 2:

**Second-price auction** (Vickrey auction):
Highest bidder wins but pays the SECOND-highest bid.

You think the painting is worth $500. You bid $500.
Someone else bid $350. You win, and pay `$350`.

You got the painting for $150 less than your value. Good deal.
And you didn't have to guess or strategize — you just told the truth.

Bullets:

- **First-price:** Everyone lies. Strategic game. Complicated. Inefficient.
- **Second-price:** Everyone tells the truth. That IS the best strategy. Simple. Efficient.

### Visualization (canvas `canvas1`, 720×380)

Two-panel horizontal bar comparison: first-price (everyone lowballs) vs second-price (everyone bids true value).

- **Title (bold 14px `#1a5276`, top center):** "First-Price vs Second-Price Auction".
- **Panels:** left panel starts at x=30, right at x=390, each 300 wide; panel headers (bold 14px, centered): "First-Price (Standard)" in red `#e74c3c`, "Second-Price (Vickrey)" in green `#27ae60`.
- **Left panel data (bar height 30, row gap 50, bar scale max $550, bars 200px max width, name labels at left):**
  - Alice — true value $500 (faded bar `rgba(0,0,0,0.07)` with `#ccc` border), actual bid $380 (solid overlay `#e74c3c` at 53% alpha, hex suffix `88`); bid label "Bid: $380" in `#e74c3c`, sub-label "(worth $500)" in 10px `#999`.
  - Bob — value $450, bid $340, color `#e67e22`; "Bid: $340" / "(worth $450)".
  - Carol — value $400, bid $310, color `#8e44ad`; "Bid: $310" / "(worth $400)".
- **Left panel result (12px red, centered, three lines):** "Alice wins, pays $380" / "Everyone lied. Alice might have won" / "by bidding less. Unstable."
- **Right panel data (same geometry, all bars green `#27ae60` at 33% alpha fill with solid green 1px border, bid = true value):** Alice $500, Bob $450, Carol $400; labels "Bid: $500 (true value)" etc. in green.
- **Right panel result (12px green, centered, three lines):** "Alice wins, pays $450 (Bob's bid)" / "Everyone told truth. No one can" / "improve by changing. Stable."
- **Bottom comparison line (12px, centered under each panel):** red "❌ Everyone guesses. Unstable." and green "✓ Everyone is honest. Nash Equilibrium."

## 2. Why Lying Can Only Hurt You in a Second-Price Auction

**Obj-title:** The Two Ways to Lie — Both Backfire

Math box 1:

**Your true value:** $500. Let's see what happens if you lie.

**Lie #1: Overbid ($700)**
If highest competitor bid $400 → You win, pay $400. Same as bidding $500.
If highest competitor bid $600 → You win, pay `$600 — MORE than it's worth to you!` You lose $100.

Overbidding can't help you. It can only make you win auctions you don't want to win.

Math box 2:

**Lie #2: Underbid ($300)**
If highest competitor bid $200 → You win, pay $200. Same as bidding $500.
If highest competitor bid $400 → `You lose.` But you WOULD have profited $100 (worth $500, pay $400).

Underbidding can't save you money (you don't pay your bid anyway). It can only make you lose auctions you wanted to win.

Math box 3:

**Bid your true value ($500)**
You win every auction where the price is below your value.
You lose every auction where the price is above your value.
`This is mathematically optimal. No other bid improves outcomes.`

### Visualization (canvas `canvas2`, 720×380)

Three stacked scenario blocks (canvas-drawn) enumerating outcomes for overbid / truthful / underbid.

- **Title (bold 14px `#1a5276`, top center):** "Your Value = $500. What Happens If You Lie?".
- **Blocks:** 600×100 rectangles at x=60, starting y=50, 10px vertical gap. Middle (truthful) block: background `#f0fff4`, border `#27ae60` 1.5px, title in green; outer blocks: background `#fef5f5`, border `#e74c3c` 1.5px, title in red. Block titles (bold 13px, left): "Overbid: $700", "Truthful: $500", "Underbid: $300".
- **Cases per block (two rows each; 12px prompt in `#333` at left, result at x≈280 colored, note at x≈460 in 11px `#666`):**
  - Overbid $700: "If competitor bids $400:" → "Win, pay $400 ✓" (green `#27ae60`), note "Same as bidding $500"; "If competitor bids $600:" → "Win, pay $600 ✗" (red `#e74c3c`), note "Paying MORE than it's worth!".
  - Truthful $500: "If competitor bids $400:" → "Win, pay $400 ✓" (green), note "Profit: $100"; "If competitor bids $600:" → "Lose ✓" (green), note "Correct — too expensive".
  - Underbid $300: "If competitor bids $200:" → "Win, pay $200 ✓" (green), note "Same as bidding $500"; "If competitor bids $400:" → "Lose ✗" (red), note "Missed $100 profit!".
- **Bottom takeaway (13px `#1a5276`, centered):** "Overbid: risk of overpaying. Underbid: risk of missing good deals. Truth: optimal."

## 3. Why This Is a Nash Equilibrium

**Obj-title:** Everyone Bidding Truthfully = Stable State

Math box 1:

**Nash Equilibrium check:**

Can Player A do better by changing strategy (lying)?
→ No. Overbidding risks overpaying. Underbidding risks missing profitable wins.

Can Player B do better by changing strategy?
→ Same logic. No.

`No player can improve by deviating → Nash Equilibrium.`

Paragraph:

**Why this is remarkable:** The system is designed so that selfishness leads to honesty. You're not being truthful because you're virtuous — you're truthful because it's the best strategy for YOU. The auction mechanism makes individual selfishness produce socially optimal outcomes.

Math box 2:

**Where this runs in real life:**

- **Google Ads:** Advertisers bid for keywords. Second-price means they bid their true value for a click. (Actually "generalized second-price" but same idea.)
- **eBay proxy bidding:** You enter max willingness to pay. System bids minimum needed. Winner pays second-highest + increment.
- **Government spectrum auctions:** Telecom companies bid billions for radio spectrum. Truth-telling = equilibrium → efficient allocation.

### Visualization (canvas `canvas3`, 720×380)

Equilibrium diagram: central stable circle with deviation arrows that bounce back, plus real-world applications row.

- **Title (bold 14px `#1a5276`, top center):** "Nash Equilibrium: No One Benefits From Changing".
- **Center circle:** radius 80 at (360, 180); fill `rgba(39,174,96,0.15)`, stroke `#27ae60` 3px. Text centered in green: "EQUILIBRIUM" (bold 14px), then 12px "Everyone bids" / "true value".
- **Deviations (three, at angles −0.4, 0.8, 2.3 rad):** dashed red `#e74c3c` outward arrow (dash 4/3, 2px) from circle edge to radius+70; solid green `#27ae60` 2px return arrow with arrowhead pointing back to the circle. Labels at the outer end: deviation label in red 11px ("Alice overbids", "Bob underbids", "Carol overbids") and green 10px two-line result ("Risks overpaying → / comes back", "Misses deals → / comes back", "Gets burned → / comes back").
- **Applications row (y≈310):** bold 12px `#1a5276` centered header "Where this runs today:"; then four items in 12px `#2980b9` spaced 140px apart starting at x=120: "Google Ads", "eBay Proxy Bids", "AWS Spot Instances", "Spectrum Auctions".
- **Bottom note (12px `#666`, centered):** "All use second-price (or similar) mechanisms → truth-telling is dominant strategy".

## Callout (philosophy box, bottom)

**The design lesson:** A well-designed system makes the selfish choice and the honest choice the same thing. You don't need to trust people to be honest — you need to design mechanisms where honesty is the dominant strategy. That's what Nash Equilibrium teaches system designers: make the math do the enforcement.

## Regeneration instructions

- **Layout:** case-study detail page. h1, `.subtitle`, `.philosophy` callout, then per numbered section: `<h2>` (1.4em `#1a5276`, bottom border `2px solid #2980b9`, padding-bottom 8px) followed by an `.obj-table` (full-width, one `<tr>`; left `<td>` 45% with `.obj-title` + `.math-box` blocks + optional paragraph/bullets, right `<td>` 55% centered holding the canvas). Closing `.philosophy` callout at the end. No nav bar, no back/home links.
- **Math boxes:** `.math-box` — background `#f8fafb`, border `1px solid #e0e0e0`, radius 6px, padding 16px 20px, 0.9em; inline `code` on `#eef2f7`, padding 2px 6px, radius 3px.
- **Callout style:** `.philosophy` — background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em.
- **Page style:** body system sans-serif, white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; subtitle `#666` 1.05em; obj-table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em, weight 600, `#1a5276`; `strong` in `#1a5276`; ul 0.9em `#333`, margin `8px 0 8px 20px`.
- **Canvas:** three canvases (`canvas1`–`canvas3`), each intrinsic 720×380; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setupCanvas(id, w, h)` helper. All are drawn comparison/diagram panels, not data charts. Chart fonts `-apple-system, BlinkMacSystemFont, sans-serif`. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, link blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, block backgrounds `#fef5f5`/`#f0fff4`, gray text `#666`/`#333`/`#999`/`#ccc`.
