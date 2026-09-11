# Strategies, Policies & Rewards

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Strategies, Policies & Rewards

**Subtitle:** The three words underneath all of game theory and reinforcement learning — the complete plan, its RL name, and the number that defines winning

## A Strategy Is a Complete Plan

**Tags:** `core idea` (blue), `contingent plan` (green)

- **A move** — one choice at one moment: bet $50, play rock, price at $4 — that's not yet a strategy
- **A strategy** — a complete plan: what you would do in every situation you could ever face
- **One-shot case** — with a single simultaneous choice, plan and move coincide — matrices hide this
- **Chess scale** — a full chess strategy is a reply to every legal position: never written, still real
- **Off the path** — plans cover situations that never happen; threats live exactly there

*Example (italic):* "If they cut price, I cut; if they hold, I hold" is a strategy; "$4" alone is just a move.

**Key point:** A strategy is a function from situations to actions. Equilibria compare whole plans, not single moves — which is why the credibility of never-taken branches can decide outcomes.

### Visualization (canvas `c1`, 720×300)

A plan as a lookup table: every situation the shop could face on the left, the planned action on the right.

- **Title (bold 15px, `#1a5276`, top center):** "A Strategy Answers Every 'What If'".
- **Left column (three situation boxes 200×44, `#fbfcfd` with 2px ink border, bold 12px ink, x=80, y = 70/130/190):** "rival holds at $5" / "rival cuts to $4" / "rival exits the street".
- **Right column (three action boxes 200×44, `rgba(0,131,0,0.08)` with 2px green border, bold 12px green, x=440):** "hold at $5" / "match at $4" / "raise to $6".
- **Arrows:** 2.5px `#6b7280` horizontal arrows from each situation to its action.
- **Contrast chip (orange 1.5px dashed box ~170×34 at bottom left, bold 12px orange):** "a move: '$4' — one cell of the plan".
- **Callout (bold 13px ink, centered y=282):** "the whole table is the strategy — including rows that may never be used".

## Payoffs, Utility, and Reward

**Tags:** `core idea` (blue), `reward` (green), `return` (orange)

- **Payoff** — the number a game assigns each player at the end: profit, points, prison years
- **Utility** — payoffs measure what players actually care about, which is not always cash
- **Reward** — RL pays the same currency in installments: a number after every single step
- **Return** — summing the step rewards (discounted) gives the episode's total: the RL payoff
- **Discounting** — valuing later rewards less; patience decides if cooperation can survive

*Example (italic):* A rat's food pellet, a chess win's +1, and a store's weekly profit all play the same role.

**Key point:** Payoff, utility, reward, return — one idea at different timescales: a number that ranks outcomes. Whoever sets that number defines what "winning" means, so reward design is game design.

### Visualization (canvas `c2`, 720×300)

Per-step rewards as bars along a timeline, with discounted heights overlaid and the return as their sum.

- **Title (bold 15px, `#1a5276`, top center):** "Rewards Arrive in Installments; the Return Adds Them Up (illustrative)".
- **Timeline:** steps 1–4 on the x axis (labels "step 1"…"step 4" 12px `#444`), baseline y=220; full-height bars (blue `rgba(42,120,214,0.55)`, width 70) for rewards `[+2, 0, +1, +3]` with bold 13px blue value labels above.
- **Discount overlay:** inside each bar a darker green bar showing the discounted contribution at γ=0.5: `[2, 0, 0.25, 0.375]`, tagged "×1, ×0.5, ×0.25, ×0.125" 11px `#6b7280` below the value labels.
- **Right summary panel (bold 13px):** "undiscounted return = 6" in blue; "discounted return ≈ 2.6" in green beneath it.
- **Callout (bold 13px orange, centered y=282):** "discounting shrinks the future — impatient players defect, patient players cooperate".

## Policy: a Strategy in RL Clothes

**Tags:** `core idea` (blue), `policy` (green), `RL` (orange)

- **Policy** — RL's word for the plan: a mapping from states to actions, written π(state) → action
- **Same object** — a policy is a strategy with a control-theory pedigree (Bellman, not von Neumann)
- **The state** — RL assumes the state summarizes the past; games condition on information sets
- **Hidden info** — poker plans on what you know, not the full board: information sets do that work
- **Stationary** — a policy that ignores the clock is the special case RL usually searches

*Example (italic):* A thermostat's rulebook, a chess engine's move chooser, and a bidding bot are all policies.

**Key point:** Strategy and policy are the same mathematical thing — a function from situations to actions. The vocabulary marks whether the world reasons back (game) or just rolls dice (MDP).

### Visualization (canvas `c3`, 720×300)

The same plan drawn in both costumes: an information set in a game tree, and the agent–environment loop of RL.

- **Title (bold 15px, `#1a5276`, top center):** "Same Plan, Two Costumes".
- **Left panel ("game theory: strategy", bold 13px ink header):** a mini two-branch tree where two decision nodes are enclosed in one dashed magenta bubble labeled "information set — what you know" (11px), with one arrow out labeled "→ action" bold 12px green.
- **Right panel ("reinforcement learning: policy", bold 13px ink header):** two boxes AGENT (blue 3px border) above ENVIRONMENT (violet 3px border) with a loop: right-side arrow down labeled "action" and left-side arrow up labeled "state, reward" (bold 12px); beside the agent, "π(state) → action" bold 12px blue.
- **Divider:** dashed `#bdc3c7` vertical at x=360.
- **Callout (bold 13px ink, centered y=284):** "one function from situations to actions — two research traditions named it twice".

## When Policies Meet, It's a Game Again

**Tags:** `where it's used` (blue), `multi-agent` (green), `common mistake` (red)

- **Alone** — against fixed transition odds, finding the best policy is an MDP: pure optimization
- **Together** — the moment other learners share the environment, "best" loses its meaning
- **The fix** — solutions become equilibria of policies: each best given what the others learned
- **Self-play** — training against copies of yourself is searching for that equilibrium
- **The seam** — GANs, bidding bots, multi-agent RL: where reinforcement learning becomes game theory

*Example (italic):* A maze solver optimizes; two pricing bots undercutting each other equilibrate.

**Key point:** One learner in a fixed world is optimization; several learners in a shared world is a game. Knowing which side of that seam you're on tells you whether "optimal" even exists.

### Visualization (canvas `c4`, 720×300)

One agent against a fixed world versus two agents sharing one world, with the solution concept under each.

- **Title (bold 15px, `#1a5276`, top center):** "One Learner Optimizes; Two Learners Play".
- **Left panel:** single AGENT box (blue) looping with a WORLD box (gray `#6b7280` border) — arrows "action" / "state, reward"; under it bold 12px blue "MDP → find the optimal policy".
- **Right panel:** AGENT 1 (blue) and AGENT 2 (violet) boxes both arrowed into one SHARED WORLD box (gray), return arrows to each; under it bold 12px green "game → find an equilibrium of policies".
- **Divider:** dashed `#bdc3c7` vertical at x=360.
- **Callout (bold 13px magenta, centered y=284):** "same algorithms, different mathematics — check who else is learning before saying 'optimal'".

## Regeneration instructions

To rebuild the HTML from this spec:

- **Template:** tutorial detail page. h1 (no index number), `.subtitle`, then one `.card-section` per h2 above, each a `table.layout` row with `.text-col` (50%: `.tags` pills, one-line `<b>`-led bullets, italic `.example`, `.key-point` callout) and `.viz-col` (50%: one 720×300 canvas).
- **Style:** identical skeleton to the game-theory series (07-nash-equilibrium): body system-ui on `#fff`, h1/h2 `#1a5276` with `#2980b9` underline, `.key-point` with red left border, tag pills blue/green/red/orange.
- **Charts:** shared `setup(id)` sizing each canvas to displayed width × `devicePixelRatio` (720×300 logical), draw functions in `__charts`, debounced resize redraw. Palette `P` as in the series. No `Math.random()`; reward numbers `[+2, 0, +1, +3]`, γ=0.5, returns 6 and ≈2.6 hardcoded and matching the text.
