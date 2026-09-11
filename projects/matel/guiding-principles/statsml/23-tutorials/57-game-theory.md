# Game Theory

**Page type:** grid page (tutorials category grid: two h2 sections ("101 Intro", "Related Topics"), 4-column nav-grid of cards with topic tags)
**HTML title tag:** Game Theory

**Subtitle:** What happens when your best move depends on everyone else's — from price wars and auctions to splitting credit and counting votes.

## Cards

Each card links to a topic page under `game-theory/`. The card shows a colored uppercase category label (`.card-num`), a numbered title, a one-line description, and 2-4 topic tag pills. Cards are split into two h2 sections, each with its own `.nav-grid`: a "101 Intro" spine followed by "Related Topics"; the colored labels carry the finer grouping.
### 101 Intro

| # | Category | Title | Link | Description | Topic tags |
|---|----------|-------|------|-------------|------------|
| 1 | 101 INTRO | What Is a Game | [57-game-theory/01-what-is-a-game.md](57-game-theory/01-what-is-a-game.md) | Any situation where your result depends on other people's choices too — captured with three parts: players, strategies, and payoffs. | players, strategies, payoff matrix |
| 2 | 101 INTRO | Types of Games | [57-game-theory/02-types-of-games.md](57-game-theory/02-types-of-games.md) | Three quick questions — do gains sum to zero, do players move at once, do they meet again — sort most strategic situations into families. | zero-sum, simultaneous, repeated |
| 3 | 101 INTRO | Solving a Game | [57-game-theory/03-solving-a-game.md](57-game-theory/03-solving-a-game.md) | Predicting where self-interest settles — cross out moves no sensible player makes, then check each side's best reply to the other. | dominant strategy, best response, elimination |
| 4 | 101 INTRO | Sequential Games & Backward Induction | [57-game-theory/04-sequential-games-and-backward-induction.md](57-game-theory/04-sequential-games-and-backward-induction.md) | When moves come in turns, solve from the last move backward — and learn why some threats are too empty to change anyone's behavior. | game tree, backward induction, credible threat |
| 5 | 101 INTRO | Strategies, Policies & Rewards | [57-game-theory/05-strategies-policies-and-rewards.md](57-game-theory/05-strategies-policies-and-rewards.md) | The three words underneath game theory and RL — strategy as a complete plan, policy as its RL name, and reward as the number that defines winning. | strategy, policy, reward |

### Related Topics

| # | Category | Title | Link | Description | Topic tags |
|---|----------|-------|------|-------------|------------|
| 6 | IN PRACTICE | Game Theory in Practice | [57-game-theory/06-game-theory-in-practice.md](57-game-theory/06-game-theory-in-practice.md) | Where payoff thinking earns its keep — auction rules that reward honesty, pricing algorithms locked in wars, and machines that train by playing themselves. | mechanism design, ad auctions, multi-agent ML |
| 7 | STRATEGIC PLAY | Nash Equilibrium | [57-game-theory/07-nash-equilibrium.md](57-game-theory/07-nash-equilibrium.md) | A rest point of self-interest — a set of choices where nobody gains by changing alone, even when everyone would gain by changing together. | best response, price war, strategic choice |
| 8 | STRATEGIC PLAY | Prisoner's Dilemma & Repeated Games | [57-game-theory/08-prisoners-dilemma-and-repeated-games.md](57-game-theory/08-prisoners-dilemma-and-repeated-games.md) | Betraying pays no matter what the other side does — yet mutual betrayal leaves everyone poorer, until meeting the same rival again makes cooperation rational. | payoff matrix, dominant strategy, tit for tat |
| 9 | STRATEGIC PLAY | Auctions | [57-game-theory/09-auctions.md](57-game-theory/09-auctions.md) | Pay-your-own-bid auctions force strategic shading, while pay-the-runner-up auctions make honest bidding the best move. | first-price, second-price, bid shading |
| 10 | COOPERATION & COLLECTIVE CHOICE | Shapley Values | [57-game-theory/10-shapley-values.md](57-game-theory/10-shapley-values.md) | Split a team's payout by averaging what each member adds across every join order — the same math SHAP uses to give features credit for a prediction. | fair split, marginal contribution, SHAP |
| 11 | COOPERATION & COLLECTIVE CHOICE | Voting Paradoxes & Arrow's Theorem | [57-game-theory/11-voting-paradoxes-and-arrows-theorem.md](57-game-theory/11-voting-paradoxes-and-arrows-theorem.md) | Perfectly consistent individual rankings can add up to a circular group preference — and no rule for combining rankings is fair on all counts. | majority cycle, ranking rules, impossibility |
| 12 | INFORMATION IN GAMES | Monty Hall in Real Life | [57-game-theory/12-monty-hall-in-real-life.md](57-game-theory/12-monty-hall-in-real-life.md) | An option gets eliminated mid-decision — whether the survivors stay even or shift 2-to-1 depends on what the eliminator knew and what they were forbidden to cut. | informed elimination, switch or stay, false monty hall |
| 13 | GAMES YOU ALREADY PLAY | Poker, Blackjack, Chess & Esports | [57-game-theory/13-poker-blackjack-chess-and-esports.md](57-game-theory/13-poker-blackjack-chess-and-esports.md) | Which games are truly strategic? Blackjack isn't, poker is — plus bluffing math, the counter cycle behind esports metas, and why chess stays unsolved. | bluffing mix, counter cycles, solved games |
| 14 | SOLVING REAL-WORLD PROBLEMS | How Game Theory Fixes Real Markets | [57-game-theory/14-how-game-theory-fixes-real-markets.md](57-game-theory/14-how-game-theory-fixes-real-markets.md) | Kidney swaps where no money is allowed, the algorithm that matches doctors to hospitals, spectrum auctions, and the road that slowed everyone down. | kidney exchange, stable matching, braess paradox |

## Regeneration instructions

To rebuild the HTML from this spec:

- **Template:** tutorials category grid. Single page: h1, `.subtitle` paragraph, then two `<h2>` sections — "101 Intro" and "Related Topics" — each followed by its own `.nav-grid` of `.nav-card` anchors.
- **Layout:** `.nav-grid` is CSS grid, `repeat(4, 1fr)`, 16px gap, 15px top margin; responsive: 3 columns below 1400px, 2 below 1100px, 1 below 600px.
- **Links:** the tables above link to the `.md` versions for markdown navigation; in the regenerated HTML, each card's `href` is the same path with an `.html` extension instead.
- **Card structure:** `<a class="nav-card" href="...">` containing `<div class="card-num">CATEGORY LABEL</div>`, `<h3>N. Topic Title</h3>` (unpadded index number matching the 2-digit zero-padded file index), `<p>description</p>`, then `<div class="topics">` of `<span class="topic-tag">` pills.
- **Category label colors:** applied by a small script mapping `.card-num` text to color — "101 INTRO" `#8e44ad`, "IN PRACTICE" `#d35400`, "STRATEGIC PLAY" `#2980b9`, "COOPERATION & COLLECTIVE CHOICE" `#27ae60`, "INFORMATION IN GAMES" `#e67e22`, "GAMES YOU ALREADY PLAY" `#16a085`, "SOLVING REAL-WORLD PROBLEMS" `#c0392b`; the CSS default for `.card-num` is `#2980b9`, 0.75em bold, 4px bottom margin.
- **Card style:** background `#ffffff`, border `1px solid #d8d8d8`, radius 10px, padding 20px, shadow `0 2px 4px rgba(0,0,0,0.05)`, transition on border-color/transform; hover: border `#2980b9`, `translateY(-2px)`. h3 `#1a3a4a` 1em with 6px bottom margin; description `#555` 0.85em. Topic tags: background `#f0f0f0`, border `1px solid #ccc`, radius 4px, padding 2px 6px, 0.7em `#666`, in a flex-wrap row with 4px gap and 8px top margin.
- **Page style:** body system sans-serif, background `#f5f5f0`, text `#2a2a2a`, padding 40px, line-height 1.6; universal `* { margin:0; padding:0; box-sizing:border-box }` reset; h1 1.8em `#2980b9` with 10px bottom margin; subtitle `#666` 1.05em with 30px bottom margin. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22` (label accents also use `#8e44ad`, `#d35400`, `#16a085`, `#c0392b`).
- **Canvases:** none on this page; any canvases elsewhere in this series use `window.devicePixelRatio` scaling.
