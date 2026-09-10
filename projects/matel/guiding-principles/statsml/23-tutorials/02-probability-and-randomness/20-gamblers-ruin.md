# Gambler's Ruin

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Gambler's Ruin

**Subtitle:** A finite bankroll playing an unlimited opponent will eventually hit zero — and zero is a trapdoor you cannot climb back out of

## Maya at the Coin-Flip Booth

**Tags:** `core idea` (blue), `random walk` (green), `absorbing barrier` (orange)

- **The booth** — Maya brings $10 to a fair coin-flip booth: heads she wins $1, tails she loses $1
- **Her rule** — she plays until she is broke at $0 or doubles her money at $20, then stops
- **A random walk** — her bankroll drifts up and down $1 at a time with no memory of the past
- **Two exits** — $0 and $20 are absorbing barriers: touch either one and the game ends for good
- **Gambler's ruin** — the question "which barrier does she hit first?" is the gambler's ruin problem

*Example (italic):* On one visit Maya climbs to $19 twice yet is absorbed at $20 a winner; on another she slides through $3, $2, $1 and the booth keeps her $10.

**Key point:** A bankroll in a repeated bet is a random walk between two walls. The walk always ends at a wall — the only questions are which wall, and how likely.

### Visualization (canvas `c1`, 720×300)

Single-panel line chart: two fixed illustrative 20-flip bankroll walks, one absorbed at the $20 barrier and one absorbed at the $0 barrier, with both barriers drawn as dashed lines.

- **Title (bold 15px, `#1a5276`, top center):** "Two Visits to the Booth: Same $10 Start, Opposite Endings (illustrative)".
- **Data:** flips 0–20 on x. Winning walk `[10, 11, 10, 11, 12, 13, 12, 13, 14, 15, 14, 15, 16, 17, 16, 17, 18, 19, 18, 19, 20]`; ruin walk `[10, 9, 10, 9, 8, 9, 8, 7, 6, 7, 6, 5, 4, 5, 4, 3, 2, 3, 2, 1, 0]`.
- **Axes:** origin x=60, plot width 600 (30px per flip), baseline y=250, plot height 195, y maps $0–$20; y ticks at $0, $5, $10, $15, $20 (12px `#444`, left of axis); x labels "0", "5", "10", "15", "20" flips below baseline; caption 12px `#444` under x labels "flips (bet $1 on a fair coin each flip)".
- **Barriers:** dashed (dash 4/3) green `#008300` 2px line across the plot at $20 labeled bold 12px "target $20 — absorbed" at its right end; dashed magenta `#d55181` 2px line at $0 labeled bold 12px "broke $0 — absorbed".
- **Walks:** winning walk green `#008300` 3px line with 3px dots ending in a 6px dot on the $20 barrier; ruin walk magenta `#d55181` 3px line with 3px dots ending in a 6px dot on the $0 barrier; start marked with one blue `#2a78d6` 6px dot at (0, $10) labeled bold 12px blue "start $10".
- **Annotation (bold 13px ink `#1a5276`, upper-left area):** "touch a wall and the game ends".

## The Ruin Formula by Hand

**Tags:** `worked example` (blue), `rule of thumb` (green)

- **Fair-coin formula** — with a fair coin, P(ruin) = 1 − start/target: numbers you can do in your head
- **Maya's game** — start $10, target $20: ruin chance 1 − 10/20 = 50%, a perfectly even split
- **Greedier targets** — keeping $10 but aiming for $50, $100, $200 gives ruin 80%, 90%, 95%
- **No target at all** — "I'll play forever" sets the target to infinity, so ruin becomes 100%
- **House edge** — drop her win chance to 49% per flip and the $20-target ruin rises to about 60%

*Example (italic):* Aiming to turn $10 into $100 at a fair booth fails 90% of the time — the formula is just 1 − 10/100.

**Key point:** Ambition is priced in ruin: with a fair coin the chance of ever reaching a target is start/target. The farther the goal, the more likely $0 comes first.

### Visualization (canvas `c2`, 720×300)

Bar chart: probability of ruin for a $10 start under a fair coin, across five stop targets ending with "no target" at 100%.

- **Title (bold 15px, `#1a5276`, top center):** "Chance of Going Broke First: $10 Start, Fair Coin, by Target".
- **Data:** targets `["$20", "$50", "$100", "$200", "no target"]`; ruin percentages `[50, 80, 90, 95, 100]`.
- **Axes:** origin x=70, plot width 590, baseline y=245, plot height 180, y scale 0–100%; y ticks 0, 25, 50, 75, 100 with gridlines `#e5e9ef`; target labels 12px `#444` below each bar.
- **Bars:** five bars ~80px wide, first four fill `rgba(42,120,214,0.45)` with 2px `#2a78d6` top edge; last bar ("no target") fill `rgba(213,81,129,0.45)` with 2px `#d55181` top edge; bold 13px value labels "50%", "80%", "90%", "95%", "100%" above each bar in the bar's edge color.
- **Formula label (bold 13px ink `#1a5276`, upper-left of plot):** "P(ruin) = 1 − 10 / target".
- **Annotation (bold 12px magenta `#d55181`, above last bar with short arrow):** "play forever = certain ruin".
- **Caption (12px `#444`, bottom center):** "fair coin, $1 bets, start $10 — exact formula, no simulation".

## A Fair Game Against a Deep Pocket

**Tags:** `where it's used` (blue), `finite vs infinite` (orange), `failure mode` (red)

- **Infinite house** — the booth's bank never hits zero, so only Maya's barrier at $0 can absorb
- **Fair is fatal** — against an unlimited opponent a 50% coin ruins her with probability 100%
- **Edge changes everything** — if she wins 55% of flips, ruin drops to (0.45/0.55) raised to her bankroll
- **Bankroll buys safety** — at 55%, ruin is 82% with $1, 13% with $10, 2% with $20, near 0% with $50
- **Beyond casinos** — traders, startups, and insurers face this: positive edge plus thin capital still ruins

*Example (italic):* A trading desk with a genuine 55%-win strategy but only 2 units of capital still blows up 67% of the time.

**Key point:** Against a deep pocket, a fair game is a losing game — full stop. Survival needs both a positive edge and enough bankroll to outlast the swings.

### Visualization (canvas `c3`, 720×300)

Two-series line chart: probability of eventual ruin against an infinite house versus starting bankroll, for a fair 50% coin and for a favorable 55% coin.

- **Title (bold 15px, `#1a5276`, top center):** "Ruin vs an Infinite House: Fair Coin vs a 55% Edge".
- **Data:** bankrolls `["$1", "$2", "$5", "$10", "$20", "$50"]` at six equally spaced x positions; fair-coin (p = 0.50) ruin `[100, 100, 100, 100, 100, 100]`; edge (p = 0.55) ruin `[82, 67, 37, 13, 2, 0]` with the last point labeled "≈0%".
- **Axes:** origin x=70, plot width 580, baseline y=245, plot height 180, y scale 0–110% (ticks 0, 25, 50, 75, 100 with gridlines `#e5e9ef`); bankroll labels 12px `#444` below each x position.
- **Fair series:** orange `#d95926` 3px flat line at 100% with 4px dots; bold 13px orange label above its right end "fair coin: ruin certain at every bankroll".
- **Edge series:** green `#008300` 3px line with 4px dots; bold 12px green value labels "82%", "67%", "37%", "13%", "2%", "≈0%" beside each dot.
- **Annotation (bold 12px ink `#1a5276`, mid-plot near the green curve):** "ruin = (0.45/0.55)^bankroll".
- **Caption (12px `#444`, bottom center):** "$1 bets against an opponent who can never go broke — exact formula".

## Zero Is a Trapdoor, Not a Setback

**Tags:** `common mistake` (red), `absorbing barrier` (orange), `rule of thumb` (green)

- **Absorbing means absorbing** — at $0 there is nothing left to bet, so every later flip is imaginary
- **No bailout** — the "law of averages" cannot rescue a walk that is no longer allowed to move
- **The ghost path** — with credit the same coin sequence recovers, but the real game stopped at flip 14
- **Slow but sure** — Maya's fair game lasts 10 × 10 = 100 flips on average, so ruin can feel far away
- **Streaks are enough** — one ordinary losing stretch near $0 ends the game; no bad luck "evens out" after

*Example (italic):* Maya's cousin says "keep playing, it always comes back" — the dashed path below does come back, but only in a world where the booth extends credit.

**Common mistake:** Believing a fair game must break even in the long run. With a finite bankroll the long run is cut short: $0 absorbs first, and the recovery happens only on paper.

### Visualization (canvas `c4`, 720×300)

Single-panel line chart: one fixed walk that hits $0 at flip 14 and stays there (real, absorbed), overlaid with a dashed ghost continuation showing the recovery that would have happened if negative balances were allowed.

- **Title (bold 15px, `#1a5276`, top center):** "The Recovery That Never Happens (illustrative)".
- **Data:** flips 0–26 on x. Real walk `[10, 9, 8, 7, 8, 7, 6, 5, 4, 5, 4, 3, 2, 1, 0]` for flips 0–14, then flat at 0 for flips 15–26. Ghost walk (same coin sequence, credit allowed), flips 14–26: `[0, -1, -2, -1, 0, -1, 0, 1, 2, 3, 4, 5, 6]`.
- **Axes:** origin x=60, plot width 600 (~23px per flip), baseline for $0 at y=210, plot height maps y range −$3 to $12; y ticks at −$2, $0, $5, $10 (12px `#444`); x labels "0", "5", "10", "15", "20", "25" flips below plot; bold 2px ink `#1a5276` horizontal line at $0 across the full plot width.
- **Real walk:** magenta `#d55181` 3px line with 3px dots for flips 0–14; 7px magenta dot at (14, $0); magenta 3px flat segment along $0 from flip 14 to 26; bold 13px magenta annotation above the flat segment "absorbed at flip 14 — game over".
- **Ghost walk:** grey `#9aa3ad` dashed (dash 5/4) 2px line with 3px hollow dots from flip 14 to 26, dipping to −$2 and rising to $6; bold 12px grey `#6b7280` annotation near its right end, two lines: "with credit it recovers to $6 —" / "but the real game already ended".
- **Shading:** region below the $0 line filled `rgba(213,81,129,0.08)` labeled 11px `#d55181` "not reachable: nothing left to bet".
- **Start marker:** blue `#2a78d6` 6px dot at (0, $10) labeled bold 12px blue "start $10".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- All walk data is hardcoded literal arrays (no `Math.random()`); charts carry "illustrative" labels where the walks are invented; ruin percentages come from the exact formulas stated in the text.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
