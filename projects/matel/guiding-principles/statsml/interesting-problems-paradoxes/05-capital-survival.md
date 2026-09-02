# Survival Buys the Chance to Recover

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one row per section; plus one full-width summary table section)
**HTML title tag:** Survival Buys the Chance to Recover — Capital as Survival Time

**Subtitle:** Staying in the game doesn't promise your losses come back — but going broke guarantees they never do. Capital buys the time to keep that chance alive.

## Callout (philosophy box)

**The question:** Imagine a perfectly fair coin flip — 50-50, no edge for either side. You have $100. Your opponent has $900. You play until one of you is broke. Who goes broke first?

**Your gut says:** "It's a fair game. 50-50 chance for either of us."

**The actual answer:** `You go broke 90% of the time.` Not because the game is unfair. Because your opponent can survive the bad streaks that kill you. A drawdown can only reverse for the player still at the table — surviving never guarantees recovery, but busting out makes the loss permanent. Capital isn't just money — it's survival time. And the player who survives longest collects the money of everyone who didn't.

## Section 1. Even in a Fair Game, the Small Stack Dies First

Math box 1:

**Setup:** Two players. Fair game (50-50). Alice has $100. Bob has $900. They bet $10 per round against each other.

**Question:** Who goes broke first?

**Answer:** Alice goes broke `90% of the time`. Bob goes broke 10% of the time.

**The formula is simple:**
P(Alice goes broke) = Bob's capital / (Alice's capital + Bob's capital)
= 900 / (100 + 900) = `90%`

Paragraph: This isn't about skill. It isn't about strategy. In a perfectly fair coin flip, the player with less money goes broke first. The probability of ruin is exactly proportional to the capital asymmetry.

Math box 2:

**Why?** Both players will experience losing streaks. A 5-bet losing streak costs $50.

For Alice ($100): that's `50% of bankroll` — near catastrophe.
For Bob ($900): that's `5.5% of bankroll` — a blip.

The same variance that is survivable for Bob is fatal for Alice.

### Visualization (canvas `canvas1`, 720×360)

Line chart: simulated bankroll paths of two players in a fair game until the small stack hits zero.

- **Data:** procedurally generated random walk, seeded PRNG (mulberry32, seed 77). Alice starts at $100 (red line), Bob at $900 (green line). Each round: 50-50 coin flip transfers $10 between them; clamp at 0; run up to 300 rounds or until one player is broke. Both paths plotted over rounds played.
- **Plot area:** origin at (70, 310), width 600, height 260. Axes in `#1a5276`, width 2. Horizontal gridlines `#eee` at $0–$1000 in $200 steps with right-aligned gray (`#666`) 11px labels "$0"…"$1000".
- **Axis labels (13px `#1a5276`):** x "Rounds Played" centered below; y "Bankroll ($)" rotated vertical at left.
- **Series:** Bob line `#27ae60` width 2; Alice line `#e74c3c` width 2.5. Zero line emphasized with dashed red (`#e74c3c`, dash 4/3, width 1) across the x-axis.
- **Legend (top right, 12px):** red line swatch + "Alice ($100 start)" in `#e74c3c`; green line swatch + "Bob ($900 start)" in `#27ae60`.
- **Title (bold 14px `#1a5276`, top center):** "Fair Game (50-50) — Small Stack Still Dies".
- **Annotation at ruin point (if Alice goes broke):** red centered text "BROKE" just below axis and "(round N)" just above axis at the final x position.
- **Bottom annotation (12px `#1a5276`, left-aligned below axis):** "No edge. No skill difference. Capital asymmetry alone determines who survives."

## Section 2. Money Flows to Survivors — The Pool Effect

Math box:

**Setup:** 10 players sit at a poker table. Each starts with $100. Total pool = `$1,000`.

After 4 hours:
- 3 players are broke (eliminated) — their combined $300 is gone from their perspective
- 7 players remain — the $1,000 is now distributed among them

After 8 hours:
- 6 players are broke — their $600 is now held by the 4 survivors
- Average survivor stack: `$250` (2.5× starting)

After 12 hours:
- 9 players are broke — the entire $1,000 sits with 1 player

Paragraph: **The eliminated players didn't lose to "the best player."** They lost to variance + insufficient capital to survive it. The last person standing may simply be the one who had the deepest pockets or got lucky at critical moments.

- **Key insight:** Every player who goes broke feeds the pot for survivors
- **The pool only flows one direction:** from eliminated to surviving
- **You can't win back losses if you're out of the game**

### Visualization (canvas `canvas2`, 720×360)

Bar chart: average survivor stack over time as players are eliminated.

- **Title (bold 14px `#1a5276`, top center):** "10 Players, $100 Each — Pool Concentrates Over Time".
- **Data (6 snapshots):** time 0: 10 alive, avg $100; time 1: 8 alive, avg $125; time 2: 6 alive, avg $167; time 3: 4 alive, avg $250; time 4: 2 alive, avg $500; time 5: 1 alive, avg $1000.
- **Plot area:** origin (80, 300), width 580, height 230, y scale $0–$1000. Axes `#1a5276` width 2; gridlines `#eee` at $200 steps with gray "$" labels.
- **Axis labels (13px `#1a5276`):** x "Time →"; y rotated "Avg Stack of Survivors ($)".
- **Bars:** 70px wide, evenly gapped. Fill `rgba(231,76,60,α)` where α = 0.3 + 0.6 × (avgStack/1000) — intensity increases as players die; border `#c0392b` width 1. White bold 14px value label ("$125" etc.) centered inside the bar when the bar is taller than 30px.
- **Below each bar:** "`N` alive" in 12px `#1a5276`; below that (except time 0) "`M` broke" in 11px `#999`.
- **Annotation (12px `#1a5276`, left-aligned bottom):** "Total pool stays $1,000. It just concentrates into fewer hands."

## Section 3. Why the House Is the Ultimate Survivor

Math box 1:

**The numbers:**

You walk in with $1,000. The casino has $500,000,000.

Even in a *fair game* (no house edge):
P(you go broke) = 500M / (1K + 500M) = `99.9998%`

You'd need to be playing a MASSIVELY unfair game *in your favor* just to offset the capital asymmetry.

Paragraph: Now add the house edge (2%). It's not even close. The capital asymmetry alone would destroy you. The edge is just acceleration.

Math box 2:

**The deeper point:**

The house doesn't survive because it's smarter. It survives because:
1. Its bankroll is effectively `infinite relative to any player`
2. No single losing streak can threaten it
3. Every player who goes broke feeds the house's pool
4. New players replenish the supply of small stacks to eliminate

It's a machine that converts capital asymmetry into guaranteed income.

### Visualization (canvas `canvas3`, 720×360)

Curve chart: probability of ruin vs capital ratio on a log-scaled x-axis.

- **Title (bold 14px `#1a5276`, top center):** "Your Probability of Ruin vs Capital Ratio (Fair Game)".
- **Plot area:** origin (90, 300), width 560, height 230. Axes `#1a5276` width 2. Y gridlines `#eee` at 0/25/50/75/100% with gray labels. X ticks (log10 scale from 1 to 500): "1×", "5×", "10×", "50×", "100×", "500×".
- **Axis labels (13px `#1a5276`):** x "Opponent's Capital / Your Capital"; y rotated "P(You Go Broke)".
- **Curve:** P(ruin) = ratio / (1 + ratio), plotted over 200 points on log10 x scale from 1× to 500×; stroke `#e74c3c` width 3; area under curve filled `rgba(231,76,60,0.1)`.
- **Key data points (red 5px dots with 11px `#1a5276` labels above):** ratio 1× → "50%"; 9× → "90%"; 99× → "99%"; 500× → "99.8%".
- **Annotations:** red (`#e74c3c`) two-line text near top right: "Casino: 500,000× your capital" / "→ you're dead before you start". Green (`#27ae60`) text at mid-left: "Equal capital = coin flip".
- **Bottom (12px `#1a5276`, left-aligned):** "Formula: P(ruin) = opponent's capital ÷ (yours + opponent's). No skill. Pure capital ratio."

## Section 4. Where Else This Applies

Full-width summary table (no canvas):

| Domain | Big Stack | Small Stack | How Small Stacks Die |
|--------|-----------|-------------|----------------------|
| **Startups** | Well-funded company | Bootstrapped founder | Can't survive 3 bad quarters — runs out of runway. Funded competitor survives the same downturn and absorbs their customers. |
| **Stock Market** | Institutional investor | Retail trader with margin | A 30% drawdown triggers margin call → forced liquidation at the bottom. Institution rides it out and buys the dip. |
| **Poker** | Deep stack player | Short stack player | Can't survive a bad beat. Gets eliminated. Their chips redistribute to remaining players. |
| **Real Estate** | Landlord with reserves | Landlord leveraged 95% | 3 months of vacancy → can't make mortgage → forced sale at loss. Buyer with cash picks it up below market. |
| **Price Wars** | Giant retail chain | Small retailer | The chain sells at a loss for 2 years. Small retailer can't match it, goes under. The chain raises prices after. |

## Closing callout (philosophy box)

**One sentence:** Capital isn't just money — it's the ability to stay in the game while others get knocked out. The money of the eliminated doesn't disappear; it pools toward survivors. This is why the rich get richer isn't just a saying — it's a mathematical consequence of capital asymmetry in repeated games with variance.

## Regeneration instructions

- **Layout:** detail page. h1, `.subtitle`, opening `.philosophy` callout, then numbered `h2` sections ("1." – "4."). Sections 1–3 each use a `.obj-table` (full-width, one `<tr>`: left `<td>` 45% with `.math-box` blocks / paragraphs / bullets, right `<td>` 55% centered holding the canvas). Section 4 is a full-width `.summary-table` (th row + 5 data rows). Closing `.philosophy` callout at the end. No `.obj-title` divs on this page — text cells start directly with math boxes.
- **Page CSS:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.4em `#1a5276` with 2px `#2980b9` bottom border, padding-bottom 8px; subtitle `#666` 1.05em; p 0.95em `#333`; `strong` `#1a5276`; obj-table cells `1px solid #e0e0e0`, padding 20px 24px, vertical-align middle. No nav bar, no back/home links.
- **Component styles:** `.philosophy` — background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em. `.math-box` — background `#f8fafb`, border `1px solid #e0e0e0`, radius 6px, padding 16px 20px, 0.9em; `code` inside on `#eef2f7` with 2px 6px padding, radius 3px. `.summary-table` — full width, 0.9em, th background `#f0f4f8` color `#1a5276` padding 10px 14px left-aligned, td padding 10px 14px, all borders `1px solid #e0e0e0`.
- **Canvases:** three canvases, each 720×360 intrinsic; a shared `setupCanvas(id, w, h)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, accent blue `#2980b9`, green `#27ae60`, red `#e74c3c`, dark red border `#c0392b`, gray text `#666`/`#333`/`#999`.
- In regenerated HTML, any card links use `.html` extensions (this page has no outbound links).
