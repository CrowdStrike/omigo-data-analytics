# The House Always Wins

**Page type:** detail page (h2 per aspect, each with a two-column obj-table row: text/math-boxes left 50%, canvas right 50%; plus a summary table)
**HTML title tag:** The House Always Wins - Domain-Specific Data Pitfalls

**Subtitle:** Two mathematical mechanisms that guarantee the house profits — one temporal (individual ruin), one cross-sectional (aggregate convergence)

## Callout (philosophy box)

**Core principle:** The house doesn't need to win any particular hand. It needs volume × edge × time, and math does the rest. This isn't folk wisdom — it's the Gambler's Ruin theorem and the Law of Large Numbers working in tandem.

## One Player Keeps Playing (Gambler's Ruin)

**The Leaky Bucket**

A player with finite bankroll playing a negative-EV game will go broke with probability → 1 as plays → ∞. The edge acts as a constant drain. Variance masks it temporarily, but never stops it.

Math box 1:

**Setup:** You have $1,000. House has $1,000,000. You win 49% of the time, lose 51%.

**Edge per bet:** 51% − 49% = `2% drain`

**100 bets × $10:** Expected loss = 100 × $10 × 0.02 = `$20 gone`
**1,000 bets × $10:** Expected loss = `$200 gone`
**5,000 bets × $10:** Expected loss = `$1,000 gone` — entire bankroll

Math box 2:

**The stopwatch version:**
$10 blackjack, 60 hands/hour, 1% house edge.
You're losing `$6/hour`. You don't notice because variance makes individual hours look random.
Weekend trip (20 hours) = `$120 expected loss`.
The math doesn't care that you "felt lucky" on Saturday night.

- **Why you go broke and the house doesn't:** Even in a fair game (50-50), the player with less money goes broke first.
- **Edge on top:** A 2% house edge added to that fair game makes going broke inevitable, not merely likely.
- **Key asymmetry:** Player has finite funds; house has effectively infinite funds relative to any one player

### Visualization (canvas `canvas1`, 720×360)

Line chart: three simulated player-bankroll random walks with negative drift, plus a dashed expected-drain line.

- **Title (bold 14px, top center, `#1a5276`):** "Player Bankroll Over Time ($10 bets, 49% win rate)".
- **Axes:** origin at (70, 310), plot 600×260, `#1a5276` 2px axes. X label "Number of Bets", Y label (rotated) "Bankroll ($)", both 13px `#1a5276`.
- **Y ticks (11px `#666`, with `#eee` 1px gridlines):** $0, $250, $500, $750, $1000. **X ticks:** 0, 1000, 2000, 3000, 4000, 5000.
- **Series:** three random walks (Player A `#e74c3c`, Player B `#e67e22`, Player C `#2980b9`, each 2px) simulated with a seeded mulberry32 PRNG (seeds 42, 137, 256): start bankroll $1,000, 5,000 bets of $10 at 49% win probability, path sampled every 10 bets, clamped at $0 and capped at $1000 for display. (Paths are deterministic given the seeds, not fixed data arrays.)
- **Expected drain line:** dashed `#1a5276` (dash 8/5, 2px) straight from $1,000 at bet 0 to $0 at bet 5,000.
- **Legend (top right inside plot):** colored line swatches + "Player A", "Player B", "Player C", and dashed blue swatch + "Expected drain (−$0.20/bet)".
- **Annotation (12px `#e74c3c`, below x-axis, left):** "Variance hides the bleed — but the drift is always down".

## Many Players, House Aggregates (Law of Large Numbers)

**Volume × Edge = Guaranteed Profit Band**

Across N players making bets, the house's actual profit converges to a tight band around its expected take. Individual players have wild variance; the house has almost none.

Math box 1:

**Setup:** 1,000 players tonight. Each plays 50 hands at $20. House edge = 2%.

**Total handle:** 1,000 × 50 × $20 = `$1,000,000 wagered`
**House expected take:** $1,000,000 × 0.02 = `$20,000 profit`

Some players win. Maybe 300 of 1,000 walk out ahead. But the *sum* across all players = −$20,000 for players, +$20,000 for house.

Math box 2:

**Why the house's variance is tiny:**

One player, 50 bets → high variance (could be +$500 or −$500).
1,000 players, 50,000 total bets → variance shrinks by `√50,000 ≈ 224×`.

House nightly profit ≈ $20,000 ± $3,000. **Tight band.**
The house isn't gambling — it's collecting a tax on volume.

- **Individual player:** High variance, feels like gambling
- **House (aggregate):** Near-zero variance, operates like a business with predictable margin

### Visualization (canvas `canvas2`, 720×360)

Confidence-band chart: house profit as % of handle vs total number of bets N (log x-scale), band narrowing as 1/√N around the +2% expectation.

- **Title (bold 14px, top center, `#1a5276`):** "House Profit as % of Handle (band tightens with more bets)".
- **Axes:** origin at (80, 310), plot 580×240, `#1a5276` 2px. X label "Total Number of Bets (N)", Y label (rotated) "Profit as % of Handle", 13px `#1a5276`.
- **Y scale:** −6% to +10%, ticks every 2% (11px `#666`), `#eee` 1px gridlines; the 0% line is `#999` 1.5px, labeled "Break even" (12px `#999`) just above it at the left.
- **X scale:** log10 from 50 to 50,000; tick labels "50", "200", "500", "2K", "5K", "20K", "50K" at N = 50, 200, 500, 2000, 5000, 20000, 50000.
- **Expected value line:** dashed green `#27ae60` (dash 8/5, 2px) horizontal at +2%, labeled "Expected: +2%" (12px green) near the right end.
- **Confidence band:** ±2σ band around +2% where the half-width in percent = 200/√N, computed at 101 points evenly spaced in log10(N) from 50 to 50,000; fill `rgba(26,82,118,0.15)`, upper and lower edges stroked `#1a5276` 1.5px.
- **Annotations:** at the left (red `#e74c3c`, 12px, two lines): "N=50: wild swings" / "(could lose money)"; at the right (green `#27ae60`, two lines just below the EV line): "N=50K: guaranteed" / "profit band"; centered near the top (11px `#1a5276`, two lines): "±2σ confidence band" / "(shrinks as 1/√N)".
- **Bottom annotation (12px `#1a5276`, below x-axis, left):** "More bets → band tightens → house profit becomes near-certain".

## Two Sides of the Same Coin

Summary table (header row: blank / Player / House):

| | Player | House |
|---|--------|-------|
| **Edge per bet** | −2% | +2% |
| **Volume** | Low (hundreds of bets) | Massive (millions of bets) |
| **Variance** | High — masks the bleed | Tiny — LLN crushes it |
| **Outcome** | Variance hides the drain until broke | Profit guaranteed ± small band |
| **Mechanism** | Random walk with negative drift hits $0 | Aggregate converges to expectation |
| **Timescale** | Long run for one individual | Even a single night with enough players |

## Callout (philosophy box, closing)

**One sentence:** The house exploits both the temporal dimension (any individual who keeps playing long enough) and the cross-sectional dimension (enough players on any given night). Same edge, two collection mechanisms.

## Regeneration instructions

- **Layout:** detail page: h1, `.subtitle`, opening `.philosophy` callout; then per aspect an `<h2>` followed by a one-row `.obj-table` — left `<td>` (45%) with `.obj-title`, an intro paragraph, two `.math-box` blocks, and a `<ul>`; right `<td>` (55%, centered) with the canvas. After the two aspects: an `<h2>` "Two Sides of the Same Coin" with a `.summary-table` (7 rows, `<th>` header row blank/Player/House, row labels in `<strong>`), then a closing `.philosophy` callout. No nav, no cross-links.
- **Page style:** body system sans-serif, white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.4em `#1a5276` with 2px solid `#2980b9` bottom border; subtitle `#666` 1.05em; p 0.95em `#333`; ul 0.9em `#333`; `strong` `#1a5276`.
- **Component styles:** `.obj-table` full-width collapsed, cell borders `1px solid #e0e0e0`, padding 20px 24px, even rows `#fafcfe`; `.obj-title` 1.05em weight 600 `#1a5276`; `.philosophy` background `#f0f4f8`, left border 4px solid `#2980b9`, padding 12px 16px, 0.9em; `.math-box` background `#f8fafb`, border `1px solid #e0e0e0`, radius 6px, padding 16px 20px, 0.9em, with `code` in `#eef2f7` background, 2px 6px padding, radius 3px; `.summary-table` full-width collapsed, 0.9em, th background `#f0f4f8` `#1a5276` left-aligned, all cells bordered `1px solid #e0e0e0` padding 10px 14px.
- **Canvas:** intrinsic 720×360 per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a `setupCanvas(id, w, h)` helper. Canvas 1 uses a seeded mulberry32 PRNG (seeds 42, 137, 256) for reproducible random walks. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, secondary blue `#2980b9`, band fill `rgba(26,82,118,0.15)`, gray text `#666`/`#999`.
