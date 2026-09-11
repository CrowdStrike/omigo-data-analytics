# Game Theory in Practice

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Game Theory in Practice

**Subtitle:** Where payoff thinking earns its keep in real work — writing auction rules, watching pricing algorithms settle, training models against each other, and splitting credit

## Designing the Rules of the Game

**Tags:** `where it's used` (blue), `mechanism design` (orange)

- **Reverse gear** — mechanism design runs game theory backward: pick rules so selfish play helps you
- **The auction** — a click is worth $2.00 to you; under pay-your-own-bid rules you shade to ~$1.60
- **The problem** — shaded bids hide true values, so the platform allocates ads half-blind
- **The fix** — charge the runner-up's price instead: bidding your honest $2.00 becomes the best move
- **The payoff** — truthful bids reveal real values, so slots go where they're worth most

*Example (italic):* The rule change, not bidder goodwill, is what makes honesty the winning strategy.

**Key point:** When you own the rules, don't predict the players — design the game so that the equilibrium of selfish play is the outcome you wanted all along.

### Visualization (canvas `c1`, 720×300)

Three bars comparing a bidder's true value with their bid under each auction rule.

- **Title (bold 15px, `#1a5276`, top center):** "Good Rules Make Honesty the Winning Strategy (illustrative)".
- **Bars (width 110, baseline y=230, scale $0–$2.20 over 150px):** "true value $2.00" at x=120, ink `#1a5276` at 55% alpha; "first-price bid $1.60" at x=305, orange `#d95926` at 55% alpha, tagged "shaded" bold 12px orange above; "second-price bid $2.00" at x=490, green `#008300` at 45% alpha, tagged "truthful" bold 12px green above. Dollar labels bold 13px above each bar, names 12px `#444` below.
- **Dashed reference line** at the $2.00 level across the chart, 1px `#bdc3c7`.
- **Callout (bold 13px green, centered y=282):** "pay-the-runner-up pricing removes the reason to shade — honesty becomes the equilibrium".

## Prices That Watch Each Other

**Tags:** `where it's used` (blue), `price equilibrium` (green)

- **The bots** — two gas stations' repricing software checks the rival each morning and undercuts
- **The slide** — starting at $3.09 and $3.05, the leapfrogging cuts walk both prices down
- **The floor** — from day 8 both sit at $2.85, where one more cut costs more than it wins
- **The misread** — an analyst who calls that flat line "stable demand" misses the strategy in it
- **The lesson** — flat prices can be a strategic rest point, not a market that stopped moving

*Example (italic):* No one at either station chose $2.85 — the pair of algorithms found the equilibrium on their own.

**Key point:** When automated agents respond to each other, the data they generate settles at equilibria. Reading such data as if it came from passive customers gets the causality wrong.

### Visualization (canvas `c2`, 720×300)

Two step-lines of daily prices leapfrogging down and going flat at the equilibrium.

- **Title (bold 15px, `#1a5276`, top center):** "Two Repricing Bots Walk Each Other Down to $2.85 (illustrative)".
- **Axes:** x = days 1–14 (ticks at 1, 4, 7, 10, 14), y = $2.75–$3.15 (labels $2.85 / $2.95 / $3.05); left margin 70, baseline y=240.
- **Station A (blue `#2a78d6`, 3px step-line):** `[3.09, 3.03, 3.03, 2.95, 2.95, 2.87, 2.87, 2.85, 2.85, 2.85, 2.85, 2.85, 2.85, 2.85]`.
- **Station B (violet `#4a3aa7`, 3px step-line):** `[3.05, 3.05, 2.99, 2.99, 2.91, 2.91, 2.85, 2.85, 2.85, 2.85, 2.85, 2.85, 2.85, 2.85]`.
- **Flat zone marker:** dashed green vertical line at day 8 with bold 12px green label "equilibrium: neither gains by cutting further".
- **Callout (bold 13px magenta `#d55181`, centered y=286):** "a flat line produced by strategy, not by a sleepy market".

## Machines That Train by Playing Themselves

**Tags:** `multi-agent ML` (blue), `self-play` (green)

- **The GAN** — a generator is paid for fooling, a discriminator for catching: opposed payoffs, one game
- **The target** — training aims at the game's equilibrium, not at maximizing either player alone
- **Self-play** — a policy that improves by beating copies of itself climbed to superhuman board play
- **Robustness** — hardening a model against attacks is a defender-vs-attacker game over inputs

*Example (italic):* When GAN training "collapses", one player has broken the game rather than reached its equilibrium.

**Key point:** In adversarial ML, "trained" means "reached the equilibrium of a two-player game" — so game theory, not lone-model optimization, explains when and why training is stable.

### Visualization (canvas `c3`, 720×300)

A two-node loop diagram of the GAN game with mirrored payoffs and the equilibrium as the stated training target.

- **Title (bold 15px, `#1a5276`, top center):** "Training as a Two-Player Game".
- **Nodes (rounded 180×64 boxes, 3px borders):** GENERATOR (blue `#2a78d6`) centered near x=175, y=120; DISCRIMINATOR (violet `#4a3aa7`) centered near x=545, y=120; role lines 12px `#6b7280` inside ("makes fakes" / "calls real or fake").
- **Loop arrows (2.5px):** top arc generator→discriminator labeled "sends fakes →" bold 12px blue; bottom arc discriminator→generator labeled "← caught / fooled signal" bold 12px violet.
- **Payoff labels (bold 12px, under each node):** generator "+1 if fooled · −1 if caught"; discriminator "+1 if caught · −1 if fooled".
- **Center label (bold 13px green, mid-canvas):** "training target: the game's equilibrium".
- **Callout (12px `#6b7280`, centered y=286):** "self-play RL and adversarial robustness share this same two-player structure".

## Splitting the Credit Fairly

**Tags:** `where it's used` (blue), `Shapley` (green)

- **The question** — features A and B lift accuracy together; how much credit does each deserve?
- **The runs** — no features 70%; A alone 78%; B alone 74%; both together 82%
- **Order one** — add A first: A brings +8; B then adds +4 on top
- **Order two** — add B first: B brings +4; A then adds +8 on top
- **The average** — over both orders A earns 8 and B earns 4 — exactly the total lift of 12

*Example (italic):* SHAP explanations and shared-infrastructure cost splits both run on this same averaging idea.

**Key point:** Cooperative game theory splits a joint result by averaging each member's marginal contribution over join orders — credit that always sums to the whole, with no double counting.

### Visualization (canvas `c4`, 720×300)

Two join-order stacked bars on the left, the averaged credit on the right, summing to the total lift.

- **Title (bold 15px, `#1a5276`, top center):** "Average Over Join Orders (accuracy points, illustrative)".
- **Left half — two horizontal stacked bars (scale 12 points = 300px, x from 60, bar height 34):** "add A first" at y=90: segment A +8 (blue, 200px) then B +4 (violet, 100px); "add B first" at y=160: segment B +4 (violet, 100px) then A +8 (blue, 200px); segment labels "A +8" / "B +4" bold 12px white inside, row names 12px `#444` above each bar.
- **Right half — two vertical bars (baseline y=225, scale 12 points = 140px):** "A: 8" blue at x=520, "B: 4" violet at x=610; value labels bold 13px above.
- **Bracket note (bold 12px green, right half):** "8 + 4 = 12 — the whole lift, no double counting".
- **Callout (12px `#6b7280`, centered y=286):** "baseline 70% → together 82%: the 12-point lift is what gets split".

## Regeneration instructions

To rebuild the HTML from this spec:

- **Template:** tutorial detail page. h1 (no index number), `.subtitle`, then one `.card-section` per h2 above, each a `table.layout` row with `.text-col` (50%: `.tags` pills, one-line `<b>`-led bullets, italic `.example`, `.key-point` callout) and `.viz-col` (50%: one 720×300 canvas).
- **Style:** identical skeleton to the game-theory series (06-nash-equilibrium): body system-ui on `#fff`, h1/h2 `#1a5276` with `#2980b9` underline, `.key-point` with red left border, tag pills blue/green/red/orange.
- **Charts:** shared `setup(id)` sizing each canvas to displayed width × `devicePixelRatio` (720×300 logical), draw functions in `__charts`, debounced resize redraw. Palette `P` as in the series. No `Math.random()`; price arrays and Shapley numbers hardcoded exactly as listed and matching the text.
