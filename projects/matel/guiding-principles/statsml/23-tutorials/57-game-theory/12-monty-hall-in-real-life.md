# Monty Hall in Real Life

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Monty Hall in Real Life

**Subtitle:** An option gets eliminated mid-decision — whether the survivors stay even or shift 2-to-1 depends on what the eliminator knew and what they were forbidden to cut

## A Shortlist Cut by Someone Who Knows

**Tags:** `core idea` (blue), `informed elimination` (green), `the setup` (orange)

- **The shortlist** — you, Alice, and Bob are finalists for one role; all look equally likely
- **The insider** — a friend on the hiring panel already knows who won the job
- **The rule** — the friend won't discuss you; they'll only name one of the others who lost
- **The reveal** — they say Bob is out; the race is now you and the untouched Alice
- **The shift** — you stay 1 in 3; Alice rises to 2 in 3 — the reveal steered around the winner

*Example (italic):* They could have named Alice but chose Bob — you were never an option, so that points at Alice.

**Key point:** This is the game-show move in work clothes: an informed, constrained revealer. The reveal tells you nothing about your own chances — your name was never on the table — and plenty about the rival it spared.

### Visualization (canvas `c1`, 720×300)

Three candidate boxes showing where the named-out finalist's probability mass flows, plus the two rules that make the setup a Monty Hall.

- **Title (bold 15px, `#1a5276`, top center):** "The Friend Names Bob Out — Where His 1/3 Goes".
- **Boxes (y=88, width 170, height 86, fill `rgba(26,82,118,0.05)`, 2px stroke):** YOU at x=45 (blue `#2a78d6` stroke, sub-label "your candidacy", value "1/3 (unchanged)"), ALICE at x=275 (green `#008300` stroke, "the spared rival", "1/3 → 2/3"), BOB at x=505 (orange `#d95926` dashed stroke `[6,4]`, "named out by the friend", "1/3 → 0"). Box name bold 13px in stroke color, sub-label 11px `#6b7280`, value bold 14px `#2c3e50`.
- **Flow arc:** green 2.5px quadratic curve from (590, 88) through control (475, 40) to (366, 84) with an arrowhead; bold 12px green label "all of Bob's 1/3 flows to Alice" centered at (480, 44).
- **Rule chips (y=212, 250×34, fill `rgba(230,126,34,0.10)`, 1.5px orange border, bold 12px orange text):** "the friend knows the decision" at x=90, "they will never name you" at x=380.
- **Caption (11px `#6b7280`, centered y=262):** "the two rules that make it Monty (illustrative odds)".
- **Callout (bold 13px magenta `#d55181`, centered y=286):** "the reveal says nothing about you — your name was never on the table".

## Counting the Three Worlds

**Tags:** `worked example` (blue), `enumeration` (green)

- **Only 3 cases** — the winner is you, Alice, or Bob; nothing else about the setup varies
- **Winner is you** — the friend may name Alice or Bob freely; betting on yourself WINS
- **Winner is Alice** — they can't name you or the winner, so they must name Bob; Alice WINS
- **Winner is Bob** — the same bind forces them to name Alice; the spared rival WINS
- **Tally** — you win in 1 of 3 worlds; the spared rival wins in 2 of 3 — just count

*Example (italic):* List the three worlds on a napkin: the rival the friend spares wins in two of them.

**Key point:** In 2 of 3 worlds the friend had no choice — the forced reveal points straight at the winner. The spared rival wins exactly when you are not the winner, which is 2/3 of the time.

### Visualization (canvas `c2`, 720×300)

A three-row enumeration table of the equally likely worlds with back-yourself / back-the-spared-rival outcomes, then tally bars.

- **Title (bold 15px, `#1a5276`, top center):** "All 3 Equally Likely Worlds (The Friend Names a Losing Rival)".
- **Column headers (bold 12px `#6b7280`, y=56):** "winner" at x=110, "friend names" at x=290, "BACK YOURSELF" at x=470, "BACK SPARED RIVAL" at x=615.
- **Rows (y = 88 / 128 / 168, 13px `#2c3e50`, thin `#e5e9ef` separators):**
  - You | "Alice or Bob (their choice)" (plain) | BACK YOURSELF: WIN | BACK SPARED RIVAL: lose
  - Alice | "Bob — forced" (bold orange `#d95926`) | BACK YOURSELF: lose | BACK SPARED RIVAL: WIN
  - Bob | "Alice — forced" (bold orange) | BACK YOURSELF: lose | BACK SPARED RIVAL: WIN
  - WIN cells: green `#008300` bold 13px on a `rgba(0,131,0,0.12)` chip (56×22); "lose" plain 13px magenta `#d55181`.
- **Tally bars (labels 12px `#444` right-aligned at x=170):** "you win" bar at y=219, 140px wide, `rgba(42,120,214,0.55)`, labeled "1 of 3" bold blue; "spared rival" bar at y=247, 280px wide, `rgba(0,131,0,0.4)`, labeled "2 of 3" bold green (3 worlds = 420px scale).
- **Callout (bold 13px green, centered y=290):** "the spared rival wins whenever you were not the winner — 2 of 3 worlds".

## Most Real Eliminations Are Not Monty

**Tags:** `where it's used` (blue), `false monty hall` (red), `renormalize` (green)

- **Stock-out** — the kitchen runs out of one of three specials; pure chance, no hint about the rest
- **House hunt** — one of your three shortlisted homes sells to another buyer; yours keep their order
- **Sports** — a title rival loses its star player; that is evidence about THEM, not the other teams
- **A/B/C test** — variant C dies of its own bad numbers; that says nothing new about A vs B
- **The tell** — none of these removals were aimed around your pick by someone who knew the answer

*Example (italic):* Promoting the long shot "because the field narrowed" is the false Monty Hall — these cuts carry no information about the survivors.

**Key point:** An option eliminated by its own evidence re-scores only itself. The uneven 2/3 shift needs an eliminator who knew the answer and had to steer around your pick — most business cuts fail both tests.

### Visualization (canvas `c3`, 720×300)

Two panels comparing the same survivor pair produced by two different elimination processes, with different resulting odds.

- **Title (bold 15px, `#1a5276`, top center):** "Why Is Bob Out? Own Evidence vs an Informed, Constrained Reveal".
- **Divider:** dashed `#bdc3c7` vertical line at x=360 from y=40 to y=288.
- **Left panel (header bold 13px ink at (195, 54), sub 11px `#6b7280` at (195, 72)):** "Bob withdrew for his own reasons" / "ordinary evidence — survivors renormalize". Bars on baseline y=218 (120px = 100% area, 60px wide): "you" at x=115 and "Alice" at x=225, both 50% tall, `rgba(42,120,214,0.55)`, labels "1/2" bold blue. Bottom note (bold 12px blue, y=250): "ranking unchanged — no shift".
- **Right panel:** "the friend named Bob, knowing the result" / "they would never have named you". Bars: "you" at x=460, 33.3% tall, blue fill, label "1/3"; "Alice" at x=570, 66.7% tall, `rgba(0,131,0,0.4)`, label "2/3" bold green. Bottom note (bold 12px green, y=250): "→ the spared rival is now the 2/3 favorite".
- **Callout (bold 13px magenta, centered y=286):** "same survivor list on the table — different process, different odds".

## The Two Questions That Decide the Odds

**Tags:** `rule of thumb` (green), `common mistake` (red)

- **Question 1** — did the eliminator actually know where the prize was when they cut?
- **Question 2** — were they forbidden to touch your pick (and the prize itself)?
- **Both yes** — the spared option absorbs the eliminated odds: back it at 2/3
- **Either no** — ordinary update: survivors keep their relative ranking, renormalized
- **Got lucky** — a random cut that merely happened to miss the prize leaves a genuine 50/50

*Example (italic):* A director who knows the user-research results must cancel one of three projects and can't cancel yours — that's Monty, and the project they spared just got stronger.

**Common mistake:** Treating every narrowed field the same way. Two errors hide here: ignoring a real Monty cut (leaving 2/3 on the table) and applying the 2/3 shift after an ordinary evidence-based cut (corrupting a sound ranking).

### Visualization (canvas `c4`, 720×300)

A two-question decision flow ending in either the Monty transfer or ordinary renormalization.

- **Title (bold 15px, `#1a5276`, top center):** "Two Questions Before You Re-Score a Narrowed Field".
- **Question boxes (2px ink stroke, `rgba(26,82,118,0.06)` fill, bold 12px text):** Q1 at (40, 70, 205×64) "Q1 — did the cutter KNOW / where the prize was?"; Q2 at (290, 70, 205×64) "Q2 — were they FORBIDDEN / to cut your pick?".
- **Outcome boxes:** MONTY TRANSFER at (545, 62, 150×80), green stroke, `rgba(0,131,0,0.10)` fill, lines "MONTY TRANSFER / switch — the spared / option is 2/3"; ORDINARY ELIMINATION at (180, 205, 330×58), blue stroke, `rgba(42,120,214,0.08)` fill, lines "ORDINARY ELIMINATION / survivors renormalize and keep their ranking".
- **Arrows:** green 2.5px "yes" arrows Q1→Q2 and Q2→Monty box (horizontal at y=102, bold 12px green "yes" labels above); orange 2.5px "no" arrows dropping from each question box down into the ordinary-elimination box (bold 12px orange "no" labels beside).
- **Callout (bold 13px magenta, centered y=290):** "the odds live in the process that removed the option — not in the count of survivors".

## Regeneration instructions

To rebuild the HTML from this spec:

- **Template:** tutorial detail page. h1 (no index number), `.subtitle`, then one `.card-section` per h2 above, each a `table.layout` row with `.text-col` (50%: `.tags` pills, one-line `<b>`-led bullets, italic `.example`, `.key-point` callout) and `.viz-col` (50%: one 720×300 canvas).
- **Style:** body system-ui on `#fff`, text `#2c3e50`, 40px padding; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; section h2 1.3rem `#1a5276` with 2px `#2980b9` bottom border; `.key-point` `#f8f9fa` with 3px `#e74c3c` left border; tag pill classes blue/green/red/orange as in the game-theory series.
- **Charts:** shared `setup(id)` helper sizing each canvas to its displayed width × `devicePixelRatio` (720×300 logical), all draw functions in an `__charts` array, redrawn on debounced window resize (150ms). Palette object `P` as in the series (`blue #2a78d6`, `green #008300`, `magenta #d55181`, `orange #d95926`, `violet #4a3aa7`, `ink #1a5276`, `mute #6b7280`, `grid #e5e9ef`); `arrowHead(ctx, x, y, angle, size, color)` helper for arrow tips.
- **Naming:** fictional people follow the Alice/Bob convention (you, Alice, Bob as job finalists; a "friend on the panel" as the informed revealer). Sections 3–4 stay generic (kitchen specials, houses, teams, A/B/C test variants — the letters there are the experiment-naming convention, not placeholders).
- **Data integrity:** all odds are the exact 1/3 / 2/3 enumeration values; the roles you (your candidacy), Alice (spared rival), Bob (named out) must match between text and every chart.
