# The House Always Wins

**Page type:** detail page (h2-sectioned two-column obj-table layout: text left 45%, canvas right 55%; one summary-table section; philosophy callouts at top and bottom)
**HTML title tag:** The House Always Wins — Case Study

**Subtitle:** People say this all the time. But what does it actually mean mathematically? There are two distinct mechanisms at work — and both are provable.

## Callout (philosophy box, top)

**The question:** If a casino game is "almost fair" (49% chance of winning), why does the house always end up with the money? After all, you could get lucky. Plenty of people walk out winners on any given night. So how does the house ALWAYS win?

**The answer has two parts:** (1) Any single player who keeps playing long enough will go broke — the small edge drains you over time. (2) Across thousands of players, the house's swings cancel out while profit adds up. Two mechanisms, same result.

## 1. One Player Keeps Playing (The Drain)

**Obj-title:** The Leaky Bucket

A player with finite bankroll playing a negative-expected-value game will go broke with probability → 1 as they keep playing. The edge acts as a constant drain. Variance masks it temporarily, but never stops it.

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

Bullets:

- **Key asymmetry:** Player has finite funds; house has effectively infinite funds relative to any one player
- **Even in a fair game:** The smaller stack goes broke first — the edge just accelerates it

### Visualization (canvas `canvas1`, 720×360)

Line chart: three simulated player-bankroll random walks drifting downward, plus a dashed expected-drain line.

- **Title (bold 14px `#1a5276`, top center):** "Player Bankroll Over Time ($10 bets, 49% win rate)".
- **Plot area:** origin x=70, baseline y=310, width 600, height 260; axes `#1a5276` 2px.
- **Axis labels:** x "Number of Bets" (13px `#1a5276`); y "Bankroll ($)" rotated −90°. Y tick labels $0, $250, $500, $750, $1000 (11px `#666`) with `#eee` gridlines.
- **Simulation:** three paths from a seeded PRNG (mulberry32, seeds 42, 137, 256); each starts at bankroll $1,000, plays up to 5,000 bets of ±$10 with win probability 0.49, stops at $0, samples every 10th bet, clamps at $1,000 top. Line colors: `#e74c3c`, `#e67e22`, `#2980b9`, 2px each. X spans 0–5,000 bets (500 sampled points).
- **Expected drain line:** dashed `#1a5276` (dash 8/5, 2px) straight from ($0 bets, $1,000) top-left down to (5,000 bets, $0) bottom-right.
- **Annotation (12px red `#e74c3c`, left-aligned below the x-axis label area):** "Variance hides the bleed — but the drift is always down".

## 2. Many Players at Once (Volume Averaging)

**Obj-title:** Tax on Volume

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

### Visualization (canvas `canvas2`, 720×360)

Confidence-band chart: house profit (% of handle) converging to +2% as total bets grow (log x-scale).

- **Title (bold 14px `#1a5276`, top center):** "House Profit Band Tightens With More Bets".
- **Plot area:** origin x=80, baseline y=300, width 580, height 230; axes `#1a5276` 2px.
- **Axis labels:** x "Total Bets Placed (N)"; y "Profit as % of Handle" rotated −90°. Y range −6% to +10%, tick labels every 2% (11px `#666`); gridlines `#eee` except the 0% line which is `#999` at 1.5px.
- **Expected value line:** horizontal dashed green `#27ae60` (dash 8/5, 2px) at +2%, labeled "Expected: +2%" (12px green, near the right end above the line).
- **Band:** 101 points across a log₁₀ scale from N=50 to N=50,000; half-width `200/√N` percentage points around 2% (upper = 2 + 200/√N, lower = 2 − 200/√N). Fill `rgba(26,82,118,0.15)`; upper and lower edges stroked `#1a5276` at 1.5px. Band narrows left-to-right toward the +2% line.
- **Annotation (12px `#1a5276`, left-aligned below the axis):** "More bets → band tightens → house profit becomes near-certain".

## 3. The Complete Picture

Summary table (`.summary-table`, header row + 6 rows):

| | Player | House |
|---|--------|-------|
| **Edge per bet** | −2% | +2% |
| **Volume** | Low (hundreds of bets) | Massive (millions of bets) |
| **Variance** | High — masks the bleed | Tiny — averages crush it |
| **Outcome** | Variance hides the drain until broke | Profit guaranteed ± small band |
| **Mechanism** | Random walk with negative drift hits $0 | Aggregate converges to expected profit |
| **Timescale** | Long run for one individual | Even a single night with enough players |

## Callout (philosophy box, bottom)

**One sentence:** The house exploits both the temporal dimension (any individual who keeps playing long enough) and the cross-sectional dimension (enough players on any given night). Same edge, two collection mechanisms.

## Regeneration instructions

- **Layout:** case-study detail page. h1, `.subtitle`, `.philosophy` callout, then per numbered section: `<h2>` (1.4em `#1a5276`, bottom border `2px solid #2980b9`, padding-bottom 8px) followed by an `.obj-table` (full-width, one `<tr>`; left `<td>` 45% with `.obj-title` + intro paragraph + `.math-box` blocks + bullets, right `<td>` 55% centered holding the canvas). Section 3 uses a plain `.summary-table` instead of obj-table/canvas. Closing `.philosophy` callout at the end. No nav bar, no back/home links.
- **Summary table style:** `.summary-table` full width, 0.9em; th background `#f0f4f8`, `#1a5276` text, left-aligned; th/td borders `1px solid #e0e0e0`, padding 10px 14px.
- **Math boxes:** `.math-box` — background `#f8fafb`, border `1px solid #e0e0e0`, radius 6px, padding 16px 20px, 0.9em; inline `code` on `#eef2f7`, padding 2px 6px, radius 3px.
- **Callout style:** `.philosophy` — background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em.
- **Page style:** body system sans-serif, white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; subtitle `#666` 1.05em; obj-table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em, weight 600, `#1a5276`; `strong` in `#1a5276`.
- **Canvas:** intrinsic 720×360 per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setupCanvas(id, w, h)` helper; canvas 1 uses a mulberry32 seeded PRNG for reproducible walks. Chart fonts `-apple-system, BlinkMacSystemFont, sans-serif`. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, link blue `#2980b9`, band fill `rgba(26,82,118,0.15)`, gridlines `#eee`, gray text `#666`/`#333`.
