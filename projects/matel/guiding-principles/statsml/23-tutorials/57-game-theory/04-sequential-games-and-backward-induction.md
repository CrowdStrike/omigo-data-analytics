# Sequential Games & Backward Induction

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Sequential Games & Backward Induction

**Subtitle:** When moves come in turns, draw the future as a tree and solve it backward — the move you predict for the last turn decides what to do on the first

## A Market Entry Story, Drawn as a Tree

**Tags:** `core idea` (blue), `game tree` (green)

- **The players** — a startup weighs entering a market; the incumbent then reacts to what it sees
- **The order** — moves happen in turns, so a payoff matrix hides who commits first: draw a tree
- **The nodes** — each circle is a decision point; branches are moves; leaves carry the payoffs
- **The stakes** — stay out: ($0, $1000k); enter and face a price war: (−$200k, $300k)
- **The alternative** — enter and be accommodated: ($300k, $600k) — the incumbent shares the market

*Example (italic):* Reading the tree left to right replays the story: the startup decides, then the incumbent answers.

**Key point:** When timing matters, the game tree is the honest picture: it shows who moves first, what each mover knows, and what every path pays at the end.

### Visualization (canvas `c1`, 720×300)

The entry game as a left-to-right tree: startup node, incumbent node after entry, three payoff leaves.

- **Title (bold 15px, `#1a5276`, top center):** "The Entry Game as a Tree ($k per year, illustrative)".
- **Nodes:** startup = blue `#2a78d6` filled circle (r=13) at (110, 160), labeled "STARTUP" bold 13px below; incumbent = violet `#4a3aa7` filled circle (r=13) at (360, 105), labeled "INCUMBENT" bold 13px above.
- **Branches (2.5px `#6b7280` lines, bold 12px `#2c3e50` labels along them):** "enter" from startup up to incumbent node; "stay out" from startup down to leaf at (360, 235); "price war" from incumbent up to leaf at (600, 65); "share" from incumbent down to leaf at (600, 150).
- **Leaves (bold 13px, two lines):** war: "startup −200" (blue) / "incumbent 300" (violet); share: "startup 300" / "incumbent 600"; stay out: "startup 0" / "incumbent 1000".
- **Caption (12px `#6b7280`, bottom center):** "circles = decision points · branches = moves · leaves = payoffs".

## Solving from the Last Move Backward

**Tags:** `worked example` (blue), `backward induction` (green)

- **Start at the end** — the last mover's choice is a plain comparison, free of any guessing
- **The incumbent** — facing entry, it compares $600k (share) with $300k (war) and shares
- **The startup** — predicting that answer, it compares $300k (enter) with $0 (stay out): enter
- **The path** — enter, then share: found by pruning the tree from its leaves to its root
- **The name** — this end-first sweep is backward induction, the workhorse for turn-based games

*Example (italic):* Cover the tree with your hand and reveal it from the right edge — each choice becomes obvious alone.

**Key point:** Solve trees from the last decision backward: settle each final choice, replace it with its outcome, and repeat. Every step is a simple comparison — the sequencing does the work.

### Visualization (canvas `c2`, 720×300)

The same tree with the war branch pruned and the chosen enter→share path highlighted, with numbered steps.

- **Title (bold 15px, `#1a5276`, top center):** "Prune the Last Move First ($k per year, illustrative)".
- **Same layout as c1**; the "price war" branch drawn dashed gray `#b8c4cf` with a red ✗ midway; the "enter" and "share" branches drawn 4px green `#008300`; "stay out" stays thin gray.
- **Step labels (bold 12px):** "step 1: incumbent picks 600 > 300" green, near the incumbent node; "step 2: startup picks 300 > 0" green, near the startup node.
- **Callout (bold 13px green, bottom center):** "prediction: enter, then share — found end-first".

## Empty Threats and Real Commitments

**Tags:** `common mistake` (red), `credible threat` (orange)

- **The growl** — the incumbent warns: "enter and it's a price war" — hoping to scare the startup off
- **The test** — at its own node, war pays $300k against sharing's $600k: it wouldn't follow through
- **The call** — the startup enters anyway; a threat you wouldn't execute changes nothing
- **The fix** — commitment: long-term low-price contracts drop sharing's payoff to $200k
- **The flip** — now war ($300k) is the incumbent's true best reply, and the startup stays out

*Example (italic):* The incumbent spends real money to make its threat costly to abandon — and then never has to fight.

**Key point:** A threat only deters if carrying it out would be the threatener's best move when the time comes. Commitments work by changing your own payoffs so the threat becomes self-enforcing.

### Visualization (canvas `c3`, 720×300)

The incumbent's decision node before and after the commitment, with the startup's resulting choice under each.

- **Title (bold 15px, `#1a5276`, top center):** "A Threat Is Only as Good as Its Payoff ($k, illustrative)".
- **Two panels split by a dashed `#bdc3c7` divider at x=360.**
- **Left panel ("before: the threat is empty", bold 13px ink):** incumbent node with two branch boxes — "share: 600" green 3px border (chosen), "war: 300" gray dashed (ignored); below, bold 12px magenta "startup's read: they'll share → ENTER".
- **Right panel ("after low-price contracts", bold 13px ink):** branch boxes — "share: 200" gray dashed (now worse), "war: 300" orange 3px border (chosen); below, bold 12px magenta "startup's read: war is real → STAY OUT".
- **Callout (bold 13px orange, bottom center):** "the commitment cuts the incumbent's own payoff — and that is exactly why it works".

## The Same Trick Everywhere

**Tags:** `where it's used` (blue), `lookahead` (green)

- **Chess engines** — minimax is backward induction run over the moves the engine can see ahead
- **Negotiation** — seasoned negotiators reason from the final offer backward to shape the first
- **Guarantees** — return policies and warranties are commitment devices that change payoffs
- **Planning ML** — lookahead in RL values a move by the best future it can still lead to

*Example (italic):* "Pick the branch whose worst reply is best" is a chess engine's whole philosophy in one line.

**Key point:** Backward induction is one idea wearing many names — minimax, lookahead, endgame reasoning. Whenever moves come in turns, think from the end of the game to its beginning.

### Visualization (canvas `c4`, 720×300)

A two-ply minimax tree: your move, rival's reply; leaf values propagate up and the best-worst branch wins.

- **Title (bold 15px, `#1a5276`, top center):** "Minimax: Backward Induction with the Names Changed".
- **Root (blue circle, "YOU (maximize)" bold 12px)** at (360, 80); two rival nodes (violet circles, "RIVAL (minimize)") at (200, 160) and (520, 160); leaves at y=245: left rival → values 3 and 8; right rival → values 5 and 6 (bold 14px `#2c3e50` in small boxes).
- **Propagation:** each rival node tagged with its minimum ("min = 3", "min = 5", bold 12px violet); root tagged "max(3, 5) = 5" bold 12px blue; the root→right-rival→5 path drawn 4px green.
- **Annotation (bold 12px green):** "pick the branch whose WORST reply is best".
- **Caption (12px `#6b7280`, bottom center):** "the same end-first sweep chess engines run millions of times per move (illustrative values)".

## Regeneration instructions

To rebuild the HTML from this spec:

- **Template:** tutorial detail page. h1 (no index number), `.subtitle`, then one `.card-section` per h2 above, each a `table.layout` row with `.text-col` (50%: `.tags` pills, one-line `<b>`-led bullets, italic `.example`, `.key-point` callout) and `.viz-col` (50%: one 720×300 canvas).
- **Style:** identical skeleton to the game-theory series (06-nash-equilibrium): body system-ui on `#fff`, h1/h2 `#1a5276` with `#2980b9` underline, `.key-point` with red left border, tag pills blue/green/red/orange.
- **Charts:** shared `setup(id)` sizing each canvas to displayed width × `devicePixelRatio` (720×300 logical), draw functions in `__charts`, debounced resize redraw. Palette `P` as in the series. No `Math.random()`; all numbers hardcoded and matching the text (0/1000, −200/300, 300/600; commitment variant 200 vs 300; minimax leaves 3, 8, 5, 6).
