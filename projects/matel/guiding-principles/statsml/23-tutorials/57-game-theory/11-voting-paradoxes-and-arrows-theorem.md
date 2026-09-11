# Voting Paradoxes & Arrow's Theorem

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Voting Paradoxes & Arrow's Theorem

**Subtitle:** Nine perfectly consistent individual rankings can add up to a circular group preference — and no rule for combining rankings is fair on all counts, so the winner depends on the rule you pick

## Nine Ballots That Go in a Circle

**Tags:** `core idea` (blue), `worked example` (green), `majority cycle` (orange)

- **The vote** — 9 teammates rank three lunch spots: tacos (A), sushi (B), pizza (C)
- **The ballots** — 4 say A>B>C, 3 say B>C>A, 2 say C>A>B; each person's ranking is consistent
- **A vs B** — 6 of 9 rank A above B (the 4 plus the 2), so the group prefers A to B
- **B vs C** — 7 of 9 rank B above C (the 4 plus the 3), so the group prefers B to C
- **C vs A** — 5 of 9 rank C above A (the 3 plus the 2) — the group's preference loops back
- **No stable pick** — A beats B, B beats C, C beats A: the group has no settled first choice

*Example (italic):* Whichever spot the team books, a majority of at least 5 of 9 exists that would rather switch to one of the others.

**Key point:** Nine rational individual rankings can combine into a circular group ranking. This loop is called a **Condorcet cycle** — "the group" simply has no transitive preference here.

### Visualization (canvas `c1`, 720×300)

Ballot-group cards (left) plus a pairwise-majority triangle whose arrows form a visible A→B→C→A cycle (right).

- **Title (bold 15px, `#1a5276`, top center):** "Nine Ballots, Three Pairwise Majorities — and a Loop (illustrative)".
- **Left — ballot cards:** heading bold 13px `#444` "the ballots (9 people)"; three rounded-corner cards (x=40, width 240, height 52, 16px gaps, starting y=58), fill `#f8f9fa`, 2px colored border; each card shows the group size (bold 13px, border color) and its ranking (bold 16px `#2c3e50`): "4 ballots / A > B > C" (blue `#2a78d6`), "3 ballots / B > C > A" (green `#008300`), "2 ballots / C > A > B" (orange `#d95926`).
- **Right — cycle triangle:** three white circles radius 24, 2.5px `#1a5276` border, bold 16px letters: A at (505,80), B at (620,240), C at (390,240); muted 12px names "tacos"/"sushi"/"pizza" beside each node. Directed 3px arrows (with filled arrowheads, trimmed 30px from node centers): A→B blue, B→C green, C→A orange. Edge labels bold 13px in the arrow's color: "A beats B 6–3", "B beats C 7–2", "C beats A 5–4".
- **Takeaway (bold 13px magenta `#d55181`, centered at canvas midwidth, y=292):** "every winner loses to something: a Condorcet cycle".

## Same Ballots, Three Different Winners

**Tags:** `worked example` (blue), `rule-dependent` (orange)

- **Second vote** — the next Friday the 9 rank again: 4 say A>C>B, 3 say B>C>A, 2 say C>B>A
- **Plurality** — count only first choices: A gets 4, B gets 3, C gets 2 — tacos (A) win
- **Runoff** — drop last-place C; its 2 ballots list B next: B 5, A 4 — sushi (B) wins
- **Pairwise** — C beats A 5–4 and beats B 6–3; head-to-head C beats everyone — pizza (C) wins
- **Same stack** — nobody changed a single ranking; only the counting rule changed

*Example (italic):* From the identical stack of nine ballots you can announce "tacos won", "sushi won", or "pizza won" — each with a defensible tally.

**Key point:** The winner is a property of the counting rule as much as of the ballots. C, which beats every rival head-to-head, is called the **Condorcet winner** — and two popular rules still don't pick it.

### Visualization (canvas `c2`, 720×300)

Three mini bar-chart panels running the same nine ballots through plurality, runoff, and pairwise counting, each crowning a different winner.

- **Title (bold 15px, `#1a5276`, top center):** "Same Nine Ballots, Three Rules, Three Winners (illustrative)".
- **Subcaption (12px `#6b7280`, centered at y=42):** "ballots: 4 × A>C>B, 3 × B>C>A, 2 × C>B>A".
- **Panels** (width 190, bars 42px wide, baseline y=228, chart height 130, value scale max 5; dashed `#bdc3c7` vertical dividers at x=250 and x=485):
  - Panel 1 (x=30), heading "plurality (1st choices)": bars A=4, B=3, C=2; winning bar A solid blue `#2a78d6`, losers `rgba(107,114,128,0.30)`; values bold 13px above bars, letters 12px below; footer bold 13px blue "winner: A (tacos)".
  - Panel 2 (x=265), heading "runoff (C dropped → B)": bars A=4, B=5; winner B solid green `#008300`; footer bold 13px green "winner: B (sushi)".
  - Panel 3 (x=500), heading "pairwise duels won": bars A=0, B=1, C=2; winner C solid orange `#d95926`; footer bold 13px orange "winner: C (pizza)".
- **Takeaway (bold 13px magenta `#d55181`, centered at y=288):** "not one ranking changed — only the counting rule did".

## Arrow's Theorem in Plain Words

**Tags:** `core idea` (blue), `where it's used` (green)

- **The theorem** — with 3+ options, no rank-combining rule passes every fairness test at once
- **Unanimity** — if all 9 rank A above B, the group ranking should put A above B too
- **No dictator** — no single ballot should decide the outcome regardless of the other eight
- **Independence** — the A-vs-B verdict shouldn't flip because C joined or left the menu
- **The catch** — Arrow proved any rule passing unanimity and independence is a dictatorship
- **In data science** — model ensembles, search-result fusion, and judge panels merge ranked lists

*Example (italic):* Rank-averaging five recommendation models into one list is a voting rule — so it inherits some Arrow failure mode.

**Key point:** This is **Arrow's impossibility theorem**: for three or more options, you are choosing which fairness criterion to give up — not whether to give one up.

### Visualization (canvas `c3`, 720×300)

Checklist matrix of four rank-combining rules against four fairness criteria; every row is missing at least one check.

- **Title (bold 15px, `#1a5276`, top center):** "Fairness Checklist: Every Rule Misses Something".
- **Grid:** column headers bold 12px `#444` "unanimity", "no dictator", "independence", "clear winner" over a 340px-wide grid starting x=175, top y=68, row height 40; row labels bold 13px `#1a5276` at x=50; alternating row background `#f8f9fa`; thin `#e5e9ef` vertical gridlines.
- **Rows and marks** (check = green `#008300` 3px checkmark stroke; fail = red `#e74c3c` 3px X stroke):
  - plurality: check, check, X, check — right note "spoilers flip it"
  - runoff: check, check, X, check — "spoilers flip it"
  - Borda points: check, check, X, check — "menu changes flip it"
  - pairwise duels: check, check, check, X — "cycles: no winner"
- **Notes column:** 12px orange `#d95926` at x=545 stating each rule's failure.
- **Takeaway (bold 13px magenta `#d55181`, centered at y=258):** "no row gets four checks — Arrow proved no rank-order rule can (for 3+ options)"; second line 12px `#6b7280` at y=280: "applies wherever ranked lists get merged: ensembles, search fusion, judging panels".

## It Doesn't Make Voting Pointless

**Tags:** `common mistake` (red), `rule of thumb` (green)

- **Wrong takeaway** — "voting is broken, rankings are meaningless" is not what the theorem says
- **Right takeaway** — every rule works fine most of the time; each has a known failure profile
- **Spoiler demo** — with C off the menu, B beats A 5–4 on those same second-Friday ballots
- **Spoiler flips** — put C back and plurality crowns A, though no A-vs-B opinion changed
- **Pick deliberately** — choose the rule whose failure mode is rarest or cheapest in your setting
- **Escape hatches** — scores or ratings instead of ranks change the game (with other trade-offs)

*Example (italic):* Runoff elections, Borda-style sports MVP votes, and Condorcet methods all run every day — each just owns a different weakness.

**Common mistake:** Reading Arrow as "don't bother voting". It is a no-free-lunch result for ranking rules: pick your failure mode consciously instead of assuming your rule has none.

### Visualization (canvas `c4`, 720×300)

Two-panel spoiler demo on the second-Friday ballots: A-vs-B head-to-head (left) flips once C joins the menu and plurality is used (right).

- **Title (bold 15px, `#1a5276`, top center):** "The Spoiler: C Joins the Menu and the A-vs-B Result Flips".
- **Subcaption (12px `#6b7280`, centered at y=42):** "same second-Friday ballots: 4 × A>C>B, 3 × B>C>A, 2 × C>B>A (illustrative)".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=52 to h−40.
- **Left panel** (x=55, width 250, baseline y=226, chart height 130, scale max 5, bars 52px): heading bold 13px `#444` "menu: A and B only"; bars A=4 (`rgba(107,114,128,0.30)`), B=5 (solid green `#008300`); values bold 13px above bars, letters 12px below; footer bold 13px green "B beats A 5–4".
- **Right panel** (x=415, width 250, same scales): heading "add C, count 1st choices"; bars A=4 (solid blue `#2a78d6`, winner), B=3 and C=2 muted gray; footer bold 13px blue "plurality winner: A".
- **Takeaway (bold 13px magenta `#d55181`, centered at y=288):** "no one's A-vs-B opinion changed — the menu did; that is the independence failure".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. A shared `arrow(ctx,x1,y1,x2,y2,color,lw)` helper draws a line plus a filled triangular arrowhead (head size 10, half-angle 0.45 rad). Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`. Red reserved for genuine failure marks (the X's in c3).
- **Data integrity:** all ballots are hardcoded literal arrays labeled "illustrative". Section 1 profile (4× A>B>C, 3× B>C>A, 2× C>A>B) must yield pairwise margins A>B 6–3, B>C 7–2, C>A 5–4. Section 2/4 profile (4× A>C>B, 3× B>C>A, 2× C>B>A) must yield plurality A=4/B=3/C=2, runoff A=4/B=5, pairwise C beats A 5–4 and B 6–3, B beats A 5–4. Text bullets and chart numbers must stay in sync.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
