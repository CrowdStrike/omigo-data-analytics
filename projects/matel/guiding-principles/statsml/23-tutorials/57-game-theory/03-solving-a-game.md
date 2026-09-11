# Solving a Game

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Solving a Game

**Subtitle:** "Solving" a game means predicting where self-interest settles — found by crossing out moves no sensible player would make and checking each side's best reply

## A Move That Wins No Matter What

**Tags:** `core idea` (blue), `dominant strategy` (orange)

- **The rivals** — Store A and Store B sell the same electronics; each week both pick: run ads or not
- **The temptation** — advertising alone steals shoppers: the advertiser makes $700, the other $300
- **The check** — A gains by advertising either way: $700 > $500 if B sits out, $400 > $300 if B ads
- **The name** — a move that beats your alternatives no matter what rivals do is a dominant strategy
- **The catch** — both stores play theirs and land on $400 each, below the $500 of mutual restraint

*Example (italic):* Store B runs the same check from its side and reaches the same answer — advertise.

**Key point:** A dominant strategy ends the guessing: you don't need to predict the rival's move. When both sides have one, the game is solved — even if the solution leaves everyone poorer.

### Visualization (canvas `c1`, 720×300)

The 2×2 ad-war payoff matrix with Store A's two comparisons drawn as arrows and the settled cell outlined.

- **Title (bold 15px, `#1a5276`, top center):** "Advertising Wins Every Comparison (illustrative weekly profit)".
- **Grid geometry:** origin x=270, y=76; cell width 190, cell height 82 (2 rows × 2 cols).
- **Payoffs `[row=A choice][col=B choice]` as `[A, B]`:** `[[[500,500],[300,700]],[[700,300],[400,400]]]`; rows "Store A: no ads" / "Store A: ads", columns "Store B: no ads" / "Store B: ads" (headers bold 13px `#2c3e50`).
- **Cells:** fill `#fbfcfd`, 1px `#b8c4cf` border; per cell "A earns $N" bold 14px blue `#2a78d6` at y+34, "B earns $N" bold 14px violet `#4a3aa7` at y+58.
- **Dominance arrows:** two bold blue 2.5px vertical arrows just left of each column's cells, from the no-ads row down to the ads row, labels "500 → 700" and "300 → 400" bold 12px blue.
- **Equilibrium cell (ads, ads):** fill `rgba(217,89,38,0.08)`, 3px orange `#d95926` border.
- **Callout (bold 13px orange, centered below grid):** "both play their dominant move: $400 each — $100 below mutual restraint".

## Crossing Out Hopeless Moves

**Tags:** `worked example` (blue), `elimination` (green)

- **A third option** — give Store B three choices: no ads, newspaper ads, or online ads
- **The scan** — compare B's columns pair by pair, row by row, hunting for always-worse moves
- **First cut** — online beats newspaper in both rows ($520 > $450, $320 > $250): newspaper goes
- **Second cut** — in what's left, online also beats B's no-ads ($520 > $500, $320 > $300)
- **The finish** — knowing B goes online, A compares $400 to $350 and skips ads: solved

*Example (italic):* Each cross-out shrinks the board, which can expose new cross-outs — repeat until stuck.

**Key point:** Iterated elimination of dominated moves can solve a game outright. Even when it can't, it prunes the board that the best-reply check must search.

### Visualization (canvas `c2`, 720×300)

The 2×3 matrix with the dominated newspaper column crossed out and the surviving prediction highlighted.

- **Title (bold 15px, `#1a5276`, top center):** "Dominated Moves Leave the Board First (illustrative)".
- **Grid geometry:** origin x=190, y=80; cell width 165, cell height 80; columns "B: no ads" / "B: newspaper" / "B: online", rows "A: no ads" / "A: ads".
- **Cell payoffs `[A, B]`:** row A-no-ads: (500,500), (350,450), (400,520); row A-ads: (700,300), (300,250), (350,320). Two lines per cell, A bold 13px blue, B bold 13px violet.
- **Cross-out:** the newspaper column overlaid with two red `#e74c3c` 2.5px diagonal dashed lines corner to corner; bold 12px red note under the column "online beats it in every row".
- **Winner cell (A no ads, B online):** fill `rgba(0,131,0,0.10)`, 3px green `#008300` border.
- **Callout (bold 13px green, centered below grid):** "after two rounds of cross-outs only (no ads, online) survives — solved by elimination alone".

## Best Reply When Nothing Dominates

**Tags:** `worked example` (blue), `best response` (green)

- **The setup** — two teams pick a shared data format: matching on JSON pays (4, 4), on Parquet (5, 5)
- **The mismatch** — choosing differently pays each team just 1: nothing dominates for anyone
- **The marks** — for each rival choice, circle your best reply; do it from both sides of the table
- **The finds** — both-JSON and both-Parquet get circles from each side: two stable predictions
- **The lesson** — games can have several solutions; which one happens depends on history or talk

*Example (italic):* If everyone already writes JSON, JSON stays — even though both teams would prefer Parquet.

**Key point:** When no move dominates, mark every player's best reply to each rival choice; cells where all marks coincide are the game's equilibria — and there may be more than one.

### Visualization (canvas `c3`, 720×300)

The coordination matrix with best-reply marks from both players; the two double-marked cells are the equilibria.

- **Title (bold 15px, `#1a5276`, top center):** "Two Cells Survive the Best-Reply Check (payoffs 1–5, illustrative)".
- **Grid geometry:** origin x=270, y=76; cell width 190, cell height 82; rows "Team X: JSON" / "Team X: Parquet", columns "Team Y: JSON" / "Team Y: Parquet".
- **Payoffs `[X, Y]`:** (JSON,JSON)=(4,4); (JSON,Parquet)=(1,1); (Parquet,JSON)=(1,1); (Parquet,Parquet)=(5,5); X bold 14px blue, Y bold 14px violet.
- **Marks:** a blue circle around X's payoff where it is X's best reply to that column (both matching cells); a violet square around Y's payoff where it is Y's best reply to that row (both matching cells).
- **Equilibrium cells (both matches):** fill `rgba(0,131,0,0.10)`, 3px green border.
- **Callout (bold 13px green, centered below grid):** "two equilibria: (JSON, JSON) and (Parquet, Parquet) — solving can return more than one answer".

## What a "Solution" Buys You

**Tags:** `where it's used` (blue), `mechanism design` (orange)

- **A forecast** — the solution says where self-interested play settles, not what players "should" do
- **The lever** — change payoffs and the settled point moves; that's design, not prediction
- **The tax** — a $250 weekly ad fee turns ads bad either way: $450 < $500 and $150 < $300
- **The result** — not advertising becomes dominant, and both stores keep their $500 weeks
- **The uses** — auction rules, pricing simulators, and platform policies are tested this way

*Example (italic):* Regulators, auction designers, and marketplaces all "solve, tweak payoffs, solve again" before going live.

**Key point:** Solving a game turns rules into predictions. That makes rule design an engineering loop: propose payoffs, find the equilibrium, adjust until the stable outcome is the one you want.

### Visualization (canvas `c4`, 720×300)

Before/after matrices: the ad war without and with a $250 ad tax, with the settled cell moving.

- **Title (bold 15px, `#1a5276`, top center):** "Change the Payoffs, Move the Prediction (illustrative)".
- **Two compact 2×2 matrices side by side (cells 130×62):** left origin x=60, y=90, headed "before" — payoffs as c1, settled cell (ads, ads)=(400,400) with 3px orange border and `rgba(217,89,38,0.08)` fill; right origin x=430, y=90, headed "after a $250 ad tax" — payoffs (500,500) / (300,450) / (450,300) / (150,150), settled cell (no ads, no ads)=(500,500) with 3px green border and `rgba(0,131,0,0.10)` fill. One compact line per cell "500 · 500" bold 13px `#2c3e50` (A first).
- **Arrow:** bold 3px ink arrow from the left matrix to the right at mid height with bold 12px ink label "add the tax".
- **Callout (bold 13px green, centered below):** "the equilibrium moves from ($400, $400) to ($500, $500) — rule design in action".

## Regeneration instructions

To rebuild the HTML from this spec:

- **Template:** tutorial detail page. h1 (no index number), `.subtitle`, then one `.card-section` per h2 above, each a `table.layout` row with `.text-col` (50%: `.tags` pills, one-line `<b>`-led bullets, italic `.example`, `.key-point` callout) and `.viz-col` (50%: one 720×300 canvas).
- **Style:** identical skeleton to the game-theory series (06-nash-equilibrium): body system-ui on `#fff`, h1/h2 `#1a5276` with `#2980b9` underline, `.key-point` with red left border, tag pills blue/green/red/orange.
- **Charts:** shared `setup(id)` sizing each canvas to displayed width × `devicePixelRatio` (720×300 logical), draw functions in `__charts`, debounced resize redraw. Palette `P` as in the series. No `Math.random()`; all numbers hardcoded and matching the text.
