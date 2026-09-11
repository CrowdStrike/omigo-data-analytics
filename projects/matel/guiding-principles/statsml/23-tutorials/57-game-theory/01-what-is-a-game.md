# What Is a Game

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** What Is a Game

**Subtitle:** Any situation where your result depends on other people's choices too — described completely by three parts: players, strategies, and payoffs

## Two Food Trucks and One Question

**Tags:** `core idea` (blue), `game theory` (green)

- **The setup** — Taco Cart A and Burger Van B each pick a parking spot: Downtown or the Stadium
- **Players** — whoever makes a choice that affects the outcome: here, the two food trucks
- **Strategies** — the options each player can pick from: park Downtown or park at the Stadium
- **Payoffs** — what every combination of choices pays each player, in dollars earned that day
- **The point** — once you can name players, strategies, and payoffs, you have a game

*Example (italic):* Taco Cart A's best spot depends on where Burger Van B parks — that dependence is what makes this a game, not a solo choice.

**Key point:** A game is any situation where your result depends on other people's choices too — and it is described completely by three parts: players, strategies, and payoffs.

### Visualization (canvas `c1`, 720×300)

A three-box flow diagram (players / strategies / payoffs) on the left, with arrows converging rightward into a small 2×2 payoff-matrix sketch.

- **Title (bold 15px, `#1a5276`, top center):** "Three Parts Turn a Story into a Game".
- **Boxes:** three rectangles at x=46, width 330, height 58, y = 52 / 130 / 208; fill `#fbfcfd`, 2px colored border. Each has a bold 13px header (left-aligned at x+14, y+23) and a 12px `#2c3e50` sub-line (y+44):
  - "PLAYERS" (blue `#2a78d6`) / "Taco Cart A · Burger Van B"
  - "STRATEGIES" (green `#008300`) / "Downtown or Stadium — each truck picks one"
  - "PAYOFFS" (orange `#d95926`) / "$ each earns for every combination"
- **Matrix sketch:** origin x=480, y=92; cells 100×56 (2×2), fill `#fbfcfd`, 1px `#b8c4cf` border; column headers "B: Down" / "B: Stad" bold 12px centered at y = gy−10; row headers "A: Down" / "A: Stad" right-aligned at x = gx−8; cell values bold 12px ink `#1a5276` centered: "300, 300" / "600, 400" (row 1), "400, 600" / "200, 200" (row 2).
- **Caption (12px `#6b7280`, centered under the matrix at y = gy + 2·56 + 20):** "the payoff matrix: (A earns, B earns)".
- **Arrows:** 1.5px `#6b7280` lines from each box's right edge (x=382, box mid-height) converging to (418, 148), with one filled mute arrowhead pointing right toward the matrix.

## Filling In the Payoff Matrix

**Tags:** `worked example` (blue), `payoff matrix` (orange)

- **Crowd values** — the Downtown lunch crowd is worth $600 a day, the Stadium crowd $400
- **Sharing rule** — if both trucks pick the same spot, they split that crowd's money evenly
- **Both Downtown** — they split the $600 crowd, so each truck takes home $300
- **A alone Downtown** — A keeps the whole $600 while B takes the Stadium's full $400
- **Both Stadium** — they split the $400 crowd, so each truck earns only $200
- **Reading a cell** — the row player's number is first: (600, 400) means A earns $600, B $400

*Example (italic):* Trace the (Stadium, Downtown) cell yourself: B alone takes Downtown's $600 while A takes the Stadium's $400.

**Key point:** Every cell of the payoff matrix comes from one shared story — crowd values plus the sharing rule — and the row player's payoff is always listed first.

### Visualization (canvas `c2`, 720×300)

The full 2×2 payoff matrix drawn large; Taco Cart A picks the row, Burger Van B picks the column.

- **Title (bold 15px, `#1a5276`, top center):** "The Food-Truck Game — Daily Earnings (illustrative)".
- **Grid geometry:** origin x=270, y=76; cell width 190, cell height 82 (2 rows × 2 cols).
- **Payoffs `[row=A choice][col=B choice]` as `[A, B]`:** `[[[300,300],[600,400]],[[400,600],[200,200]]]`.
- **Headers:** column labels "Burger Van B: Downtown" / "Burger Van B: Stadium" bold 13px `#2c3e50` centered above each column at y = gy−12; row labels "Taco Cart A: Downtown" / "Taco Cart A: Stadium" right-aligned at x = gx−14, vertically centered per row.
- **Cells:** fill `#fbfcfd`, 1px `#b8c4cf` border; two centered text lines per cell — "A earns $N" bold 14px blue `#2a78d6` at cell y+34, "B earns $N" bold 14px violet `#4a3aa7` at cell y+58. No cell is highlighted.
- **Callout (bold 13px green `#008300`, centered at y = grid bottom + 26):** "one story fills all four cells: crowd value, split when shared, kept when alone".
- **Caption (12px `#6b7280`, centered at y = grid bottom + 46):** "each cell: (A earns, B earns) — the row player's payoff is listed first".

## Spotting Games in the Wild

**Tags:** `where it's used` (blue), `decision vs game` (orange)

- **Pricing rivalry** — two shops setting prices: each shop's sales depend on the other's tag
- **Salary talks** — your counter-offer only works against what the employer is ready to offer
- **Sealed-bid auctions** — your bid wins or loses only relative to bids you cannot see
- **Penalty kicks** — the kicker aims where the keeper won't dive; the keeper guesses back
- **Not a game** — carrying an umbrella against rain is a decision: weather doesn't react to you

*Example (italic):* The rain falls the same whether you carry an umbrella or not — but Burger Van B parks differently depending on what it expects from A.

**Key point:** The test for a game: does the other side think back? Against nature it's a plain decision; against a strategic responder it's a game.

### Visualization (canvas `c3`, 720×300)

Two side-by-side panels split by a light divider at x=360: a decision (you vs indifferent nature) on the left, a game (two responders) on the right.

- **Title (bold 15px, `#1a5276`, top center):** "A Decision vs a Game".
- **Divider:** 1px `#e5e9ef` vertical line at x=360 from y=44 to y=250.
- **Left panel:** header bold 13px `#2c3e50` centered at (185, 54): "a decision: you vs nature". A "You" box at (115, 78, 140×44), fill `#fbfcfd`, 2px blue `#2a78d6` border, bold 13px blue label. A one-way mute arrow (1.5px `#6b7280`) from the box bottom (185,126) down to (185,166) with a downward arrowhead, side label 12px mute "umbrella or not" at (197,146). A cloud of three overlapping circles (centers (152,196) r20, (185,186) r26, (220,196) r19), fill `#e2e8ee`, stroke `#b8c4cf`. Caption 12px `#6b7280` centered at (185, 240): "the rain — it doesn't react to you".
- **Right panel:** header bold 13px `#2c3e50` centered at (540, 54): "a game: you vs a responder". Two boxes fill `#fbfcfd`: "Taco Cart A" at (392, 110, 136×46) with 2px blue border and bold 13px blue label; "Burger Van B" at (556, 110, 136×46) with 2px violet `#4a3aa7` border and bold 13px violet label. Two opposing arrows between the boxes: blue arrow pointing right at y=122, violet arrow pointing left at y=144. Caption 12px `#6b7280` centered at (540, 190): "each truck parks where it expects the other won't".
- **Insight (bold 13px magenta `#d55181`, centered at y=278):** "the difference: the other side thinks back".

## The Common Confusion

**Tags:** `common mistake` (red)

- **Not about fun** — "game" is a technical word for interdependent choice, not for play
- **Not win-lose** — nothing in the definition says one player's gain must be another's loss
- **The food trucks** — splitting up earns $1,000 total; crowding Downtown earns only $600
- **Win-win exists** — at (Downtown, Stadium) both trucks do well: A gets $600 and B gets $400
- **The definition** — a game is just interdependent choice; conflict is one flavor, not the rule

*Example (italic):* Both trucks parking at the Stadium earns $400 total — the worst combined day — while splitting up earns $1,000.

**Common mistake:** Hearing "game" and expecting a loser. In the food-truck game the best outcomes are the split ones — win-win situations are games too.

### Visualization (canvas `c4`, 720×300)

Bar chart of TOTAL money earned per outcome: two low bars for the crowded same-spot outcomes, two tall bars for the two split arrangements.

- **Title (bold 15px, `#1a5276`, top center):** "Total Money Earned per Outcome (illustrative)".
- **Scale:** baseline y=236, plot height 168, value scale 0–1100; axis lines 1px `#999` from (56, baseline−168) down and across to (700, baseline). Bar width 92.
- **Bars (x positions 96 / 252 / 408 / 564):**
  - "both Downtown" = $600, fill `rgba(42,120,214,0.55)`, value label bold 13px blue `#2a78d6`
  - "both Stadium" = $400, same blue styling
  - "A Downtown, B Stadium" = $1,000, fill `rgba(0,131,0,0.45)`, value label bold 13px green `#008300`
  - "A Stadium, B Downtown" = $1,000, same green styling
- **Bar captions:** two 12px `#444` lines below the baseline (y+16 and y+31) per bar as named above.
- **Green annotation (bold 13px green, centered at x=532, y = yOf(1000)−26, above the two split bars):** "win-win — nobody had to lose".
- **Takeaway (bold 13px magenta `#d55181`, centered at y=292):** "a game with no loser: splitting up beats crowding, for both trucks".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
