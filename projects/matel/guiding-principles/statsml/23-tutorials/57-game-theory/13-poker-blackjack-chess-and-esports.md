# Poker, Blackjack, Chess & Esports

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Poker, Blackjack, Chess & Esports

**Subtitle:** The games people actually play, sorted by one question — does your opponent adapt to you? — and what game theory says about the ones where they do

## Against the House or Against People

**Tags:** `core idea` (blue), `decision vs game` (orange)

- **The test** — a game needs an opponent whose choices respond to yours; rules that sit still don't
- **Roulette & slots** — pure chance with a fixed edge: no one across the table is adapting to you
- **Blackjack** — the dealer follows printed rules; counting is probability work, not strategy
- **Poker** — every bet answers your habits: raise more, get called more — a true strategic game
- **Board games** — Monopoly and Catan hinge on trades: the players, not the dice, adapt to you
- **The twist** — the house did play a game: it designed rules with an edge — mechanism design at work

*Example (italic):* The casino's game was played once, in a design office; at the tables only poker still is one.

**Key point:** Sort any game by asking "does my opponent adapt to me?" No: it's a decision problem — optimize and accept the odds. Yes: it's a strategic game, and equilibrium thinking applies.

### Visualization (canvas `c1`, 720×300)

Six familiar games in a chip table scored on the one question that matters.

- **Title (bold 15px, `#1a5276`, top center):** "Does the Opponent Adapt to You?".
- **Rows (six, y from 70, 30px apart, name 13px `#2c3e50` left at x=90):** roulette, slots, blackjack, poker, chess, ranked esports.
- **Chips (centered x=430, 130×22):** roulette/slots/blackjack get orange `#d95926` chips "no — fixed rules"; poker/chess/ranked esports get green `#008300` chips "yes — adapts to you".
- **Right-side notes (12px `#6b7280`, x=530):** "decision problem" beside the three orange rows (once, bracketed), "strategic game" beside the three green rows.
- **Callout (bold 13px magenta `#d55181`, centered y=282):** "only the bottom three are games in the game-theory sense".

## Poker's Bluff Is a Mixed Strategy

**Tags:** `worked example` (blue), `mixed strategy` (green)

- **The dilemma** — never bluff and your big bets scream strength; always bluff and calls eat you
- **The mix** — the fix is randomness on purpose: bluff a fraction of the time, unreadably
- **The numbers** — bet $50 into a $100 pot: bluffing 25% makes the rival's call exactly break even
- **The balance** — 1 bluff per 3 value bets leaves no counter-strategy that beats you
- **The name** — deliberately randomized play is a mixed strategy; poker pros call it "balanced"

*Example (italic):* If they fold too much against your 25% mix you profit; if they call too much you profit too.

**Key point:** Bluffing isn't lying — it's equilibrium play. The right bluff ratio is set by pot odds so the opponent's options tie, leaving them nothing to exploit.

### Visualization (canvas `c2`, 720×300)

The opponent's expected value of calling as a function of your bluff frequency, crossing zero at the balanced mix.

- **Title (bold 15px, `#1a5276`, top center):** "Bet $50 into $100: the Caller Breaks Even at 25% Bluffs (illustrative)".
- **Axes:** x = your bluff frequency 0–100% (ticks 0 / 25 / 50 / 75 / 100), y = caller's EV per call, −$50 to +$150 (labels −$50 / $0 / +$150); left margin 80, baseline for $0 marked with a 1px `#bdc3c7` dashed horizontal.
- **Line (blue `#2a78d6`, 3px):** straight from (0%, −$50) to (100%, +$150); crosses $0 at 25%.
- **Equilibrium marker:** magenta `#d55181` filled dot (r=7) at (25%, $0) with dashed vertical drop; bold 13px magenta label "balanced: 25% bluffs — their call ties".
- **Region annotations (bold 12px):** left of 25% in orange "you bluff less → they should fold"; right of 25% in green "you bluff more → they should call".
- **Callout (bold 13px green, centered y=286):** "at the mix, no counter-strategy beats you — that is a mixed-strategy equilibrium".

## Rock, Paper, Scissors — the Esports Engine

**Tags:** `core idea` (blue), `counter cycles` (green), `metagame` (orange)

- **The cycle** — rock beats scissors beats paper beats rock: no move is best, only best-against
- **The mix** — equilibrium is 1/3 each at random; any leaned-on favorite gets punished
- **The RTS echo** — rush beats greedy economy, turtle defense beats rush, economy beats turtle
- **The metagame** — when one build dominates a season, its counter rises next: the cycle turning
- **The scouting** — RTS adds information: paying units to peek is paying to escape the blind mix

*Example (italic):* A patch that buffs "rock" doesn't end the cycle — it just moves where the 1/3s sit.

**Key point:** Cyclic counters force mixing; that single structure explains balanced esports metas, pitcher-batter duels, and why "the best build order" never stays the best for long.

### Visualization (canvas `c3`, 720×300)

The counter triangle drawn twice: classic rock-paper-scissors and its real-time-strategy translation.

- **Title (bold 15px, `#1a5276`, top center):** "One Triangle, Two Games".
- **Two triangles side by side (vertices on circles r=26, arrows 2.5px `#2c3e50` running clockwise vertex to vertex with small arrowheads, each edge labeled "beats" 11px `#6b7280`):**
  - Left (center ~x=200): vertices ROCK (top, blue), SCISSORS (bottom right, violet), PAPER (bottom left, orange); rock→scissors→paper→rock.
  - Right (center ~x=520): vertices RUSH (top, blue), ECONOMY (bottom right, violet), TURTLE (bottom left, orange); rush→economy→turtle→rush; caption under it 11px `#6b7280` "rush beats greed · defense beats rush · greed beats defense".
- **Center label (bold 12px green, between triangles):** "equilibrium: mix 1/3 each".
- **Callout (bold 13px magenta, centered y=284):** "any predictable favorite is a free win for its counter".

## Chess, Checkers, and the Limits of Solving

**Tags:** `where it's used` (blue), `solved games` (green), `perfect information` (orange)

- **Zermelo's promise** — every chess position has a best move; backward induction finds it in theory
- **The wall** — chess has roughly 10^44 positions; no machine will ever visit them all
- **The workaround** — engines search a few moves deep, then judge leaves with an evaluation
- **Actually solved** — tic-tac-toe (draw), Connect Four (first player wins), checkers (draw, 2007)
- **The status** — chess and Go remain unsolved games played superbly: approximation beat proof

*Example (italic):* Checkers took 18 years of compute to prove what club players suspected: perfect play draws.

**Key point:** "Solvable in principle" and "solvable in practice" are different claims. Game theory guarantees chess has an answer; engineering decides whether anyone ever computes it.

### Visualization (canvas `c4`, 720×300)

Log-scale bars of position counts for five perfect-information games, split into solved and unsolved.

- **Title (bold 15px, `#1a5276`, top center):** "Positions to Check, on a Log Scale (approximate)".
- **Horizontal bars (x from 190, scale log10 positions 0–170 mapped to 470px, bar height 24, 34px apart, names 13px right-aligned at x=180):**
  - tic-tac-toe ~10^4 — green, tagged "solved: draw"
  - Connect Four ~10^13 — green, tagged "solved: first player wins"
  - checkers ~10^20 — green, tagged "solved 2007: draw"
  - chess ~10^44 — orange, tagged "unsolved"
  - Go ~10^170 — orange, tagged "unsolved"
  - Bar value labels ("10^44" style) bold 12px inside or at bar end; tags 12px after the bar.
- **Callout (bold 13px ink, centered y=282):** "Zermelo says an answer exists for every one of these — only three have been computed".

## Regeneration instructions

To rebuild the HTML from this spec:

- **Template:** tutorial detail page. h1 (no index number), `.subtitle`, then one `.card-section` per h2 above, each a `table.layout` row with `.text-col` (50%: `.tags` pills, one-line `<b>`-led bullets, italic `.example`, `.key-point` callout) and `.viz-col` (50%: one 720×300 canvas).
- **Style:** identical skeleton to the game-theory series (06-nash-equilibrium): body system-ui on `#fff`, h1/h2 `#1a5276` with `#2980b9` underline, `.key-point` with red left border, tag pills blue/green/red/orange.
- **Charts:** shared `setup(id)` sizing each canvas to displayed width × `devicePixelRatio` (720×300 logical), draw functions in `__charts`, debounced resize redraw. Palette `P` as in the series. No `Math.random()`; the bluffing line is the exact indifference calculation (call $50 to win $150: breakeven at 25%), position counts are order-of-magnitude and labeled approximate.
