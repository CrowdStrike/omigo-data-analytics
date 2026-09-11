# Nash Equilibrium

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Nash Equilibrium

**Subtitle:** A rest point of self-interest — a set of choices where nobody benefits from changing alone, even when everyone would benefit from changing together

## Two Coffee Shops, One Price War

**Tags:** `core idea` (blue), `game theory` (green), `strategic choice` (orange)

- **The street** — two coffee shops, Brew A and Brew B, each picks a price: $5 (high) or $4 (low)
- **The pull** — cutting price alone steals the crowd: the cutter earns $400, the other only $100
- **The trap** — if both cut, they split the crowd at thin margins: $200 each instead of $300
- **The rest point** — at (low, low) neither shop can raise its price alone without losing money
- **The name** — a choice pair where no player gains by changing alone is a Nash equilibrium

*Example (italic):* Brew A tries going back to $5 while B stays at $4 — A's profit falls from $200 to $100, so A stays low.

**Key point:** A Nash equilibrium is a rest point of self-interest: given what everyone else is doing, no single player can do better by changing only their own move.

### Visualization (canvas `c1`, 720×300)

A 2×2 payoff matrix drawn as a grid; Brew A picks the row, Brew B picks the column; the (low, low) cell is highlighted as the equilibrium.

- **Title (bold 15px, `#1a5276`, top center):** "Two Coffee Shops Pick a Price — Daily Profit (illustrative)".
- **Grid geometry:** origin x=270, y=76; cell width 190, cell height 82 (2 rows × 2 cols).
- **Payoffs `[row=A choice][col=B choice]` as `[A, B]`:** `[[[300,300],[100,400]],[[400,100],[200,200]]]`.
- **Headers:** column labels "Brew B: $5 (high)" / "Brew B: $4 (low)" bold 13px `#2c3e50` centered above each column at y = gy−12; row labels "Brew A: $5 (high)" / "Brew A: $4 (low)" right-aligned at x = gx−14, vertically centered per row.
- **Cells:** fill `#fbfcfd`, 1px `#b8c4cf` border; two centered text lines per cell — "A earns $N" bold 14px blue `#2a78d6` at cell y+34, "B earns $N" bold 14px violet `#4a3aa7` at cell y+58.
- **Equilibrium cell (bottom-right, A $200 / B $200):** fill `rgba(0,131,0,0.10)`, 3px green `#008300` border.
- **Callout (bold 13px green, centered at y = grid bottom + 26):** "Nash equilibrium: at ($4, $4) neither shop gains by changing alone".
- **Caption (12px `#6b7280`, centered at y = grid bottom + 46):** "each cell: Brew A picks the row, Brew B picks the column".

## Checking Every Cell by Hand

**Tags:** `worked example` (blue), `rule of thumb` (green)

- **The test** — a cell is an equilibrium only if neither player gains by switching their own choice
- **(high, high)** — Brew A cutting to $4 jumps $300 → $400, so this cell fails the test
- **(low, high)** — Brew B earns $100 there; cutting to $4 lifts B to $200, so it fails too
- **(high, low)** — the mirror case: A gains $100 → $200 by cutting, so it fails as well
- **(low, low)** — A's switch drops $200 → $100, and so does B's: nobody moves — equilibrium

*Example (italic):* Ask "would I switch?" twice in each of the four cells — only (low, low) gets two answers of no.

**Key point:** Finding a 2×2 equilibrium needs no advanced math — check every cell, asking each player "would you switch alone?". A cell where both say no is a Nash equilibrium.

### Visualization (canvas `c2`, 720×300)

The same 2×2 matrix redrawn with a deviation check written inside every cell: three cells fail (a profitable escape exists), one holds.

- **Title (bold 15px, `#1a5276`, top center):** "The Cell-by-Cell Test: Would Anyone Switch Alone?".
- **Grid geometry:** origin x=240, y=74; cell width 215, cell height 88.
- **Headers:** columns "B picks $5" / "B picks $4" bold 13px `#2c3e50` centered at y = gy−10; rows "A picks $5" / "A picks $4" right-aligned at x = gx−14.
- **Cell contents (three centered lines at cell y+26 / y+50 / y+70):**
  - (r0,c0): "A $300 · B $300" / "✗ fails — A cuts: $300 → $400" / "a profitable escape exists"
  - (r0,c1): "A $100 · B $400" / "✗ fails — A cuts: $100 → $200" / "a profitable escape exists"
  - (r1,c0): "A $400 · B $100" / "✗ fails — B cuts: $100 → $200" / "a profitable escape exists"
  - (r1,c1): "A $200 · B $200" / "✓ holds — no one gains by leaving" / "both switches lose $100"
- **Styling:** line 1 bold 13px `#2c3e50`; line 2 bold 12px — orange `#d95926` on failing cells, green `#008300` on the holding cell; line 3 12px `#6b7280`. Failing cells: fill `#fbfcfd`, dashed (5/3) 1.5px orange border. Holding cell: fill `rgba(0,131,0,0.10)`, solid 3px green border.
- **Takeaway (bold 13px green, centered at y = grid bottom + 28):** "only ($4, $4) passes both \"would I switch?\" checks".

## Where Equilibria Run Your Day

**Tags:** `where it's used` (blue), `systems of agents` (orange)

- **Pricing** — competing sellers settle at price points where any solo change only loses money
- **Ad auctions** — search-ad bidders converge to stable bids; auction rules are designed around this
- **Traffic** — commuters spread across routes until every used route takes the same time
- **Multi-agent ML** — GAN training and self-play RL are searches for an equilibrium between models
- **The commute** — highway time is 20 + 0.4 min per car; the side road is a flat 40 min

*Example (italic):* With 100 commuters, exactly 50 take the highway — both routes then take 40 minutes and no driver gains by switching.

**Key point:** When many self-interested agents interact, the system does not land on the designed optimum — it lands on an equilibrium. Predicting behavior means finding that rest point.

### Visualization (canvas `c3`, 720×300)

Line chart of trip time vs highway load: a rising highway line crosses a flat side-road line at the equilibrium split of 50/50.

- **Title (bold 15px, `#1a5276`, top center):** "100 Commuters, Two Routes: Where Switching Stops Paying".
- **Axes:** origin x=70, baseline y=250, plot width 570, plot height 180; x maps 0–100 cars, y maps 0–65 minutes. X ticks at 0, 25, 50, 75, 100 (12px `#444`), x-axis caption "cars choosing the highway"; y labels "20 min", "40 min", "60 min" right-aligned left of the axis.
- **Side road:** green `#008300` dashed (7/4) 2.5px horizontal line at t=40; label bold 12px green "side road: flat 40 min" at x = xOf(2), y = yOf(40)+18 (below the line).
- **Highway:** blue `#2a78d6` solid 3px line from (0 cars, 20 min) to (100 cars, 60 min); label bold 12px blue "highway: 20 + 0.4 min per car" left-aligned at x = xOf(55), y=152 (below the line).
- **Equilibrium:** magenta `#d55181` 7px dot at (50 cars, 40 min); dashed `#bdc3c7` (4/3) vertical drop to the baseline; annotation bold 13px magenta, two left-aligned lines at x = xOf(6): "equilibrium: 50 cars each way," (y=100) and "every driver takes 40 min" (y=116); thin 1.5px magenta connector line from (300,112) to just left of the dot.
- **Deviation note (bold 12px orange `#d95926`, left-aligned at x = xOf(52)):** "51st car makes the highway 40.4 min —" (y=172) / "switching alone only makes you slower" (y=188).

## Stable Is Not the Same as Good

**Tags:** `common mistake` (red), `prisoner's dilemma` (orange)

- **The confusion** — "equilibrium" sounds like the best outcome; it only means nobody moves alone
- **Jointly bad** — the (low, low) equilibrium pays $200 each while (high, high) would pay $300
- **Why it unravels** — at (high, high) each shop is one price cut away from $400, so it collapses
- **Escaping costs** — leaving needs coordination: contracts, regulation, or repeated play with trust
- **Design lesson** — good market rules change payoffs so the equilibrium and the good outcome align

*Example (italic):* Both shops would happily sign a "no discounts" pact — proof that the equilibrium is not the best joint outcome.

**Common mistake:** Reading "equilibrium" as "optimal". Equilibrium means stable, not good — players can be locked into an outcome that is worse for everyone, the classic prisoner's dilemma.

### Visualization (canvas `c4`, 720×300)

Grouped bar chart comparing the equilibrium payoffs with the better-but-unstable joint outcome, plus a dashed "defector" ghost bar showing why the good outcome collapses.

- **Title (bold 15px, `#1a5276`, top center):** "Stable vs Good: The Equilibrium Is the Worse Outcome".
- **Scale:** baseline y=232, plot height 168, value scale 0–450; axis lines 1px `#999` from (50, baseline−168) down and across to (690, baseline). Bar width 46.
- **Group 1 (equilibrium, both $4):** bars A and B at x=110 and 164, both value 200, fill `rgba(42,120,214,0.55)`; bold 13px blue "$200" above each; 12px `#444` "A"/"B" below; group caption bold 12px blue centered at x=160, y = baseline+36: "equilibrium (both $4) — stable but poor".
- **Group 2 (both $5):** bars A and B at x=390 and 444, both value 300, fill `rgba(0,131,0,0.45)`; bold 13px green "$300" above each; caption bold 12px green centered at x=440: "better for both (both $5) — good but unstable".
- **Defector ghost bar:** dashed (6/4) 2px orange `#d95926` outlined rectangle at x=590, value 400; bold 13px orange "$400" above; 12px `#444` "defector" below.
- **Arrow:** 2px orange line from (495, yOf(300)−15) to (582, yOf(390)) with a filled orange arrowhead; annotation bold 12px orange, right-aligned at x=560: "one price cut grabs $400 —" (y = yOf(400)−14) / "so ($5, $5) collapses" (y = yOf(400)+2).
- **Takeaway (bold 13px magenta `#d55181`, centered at y=292):** "equilibrium ≠ best collective outcome — stability and quality are different questions".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
