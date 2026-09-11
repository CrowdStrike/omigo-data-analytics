# How Game Theory Fixes Real Markets

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** How Game Theory Fixes Real Markets

**Subtitle:** Four real problems solved by designing the game — kidney swaps with no prices, matching doctors to hospitals, auctioning the airwaves, and a road that made traffic worse

## Kidneys Without Prices

**Tags:** `where it's used` (blue), `kidney exchange` (green)

- **The problem** — a willing donor whose kidney doesn't match their loved one; thousands of pairs
- **No prices** — organ sales are illegal, so no market can clear this: the rules must do the work
- **The swap** — pair 1's donor fits pair 2's patient and vice versa: a two-way exchange
- **The chains** — one altruistic donor can trigger a chain of transplants down a waiting list
- **The design** — matching algorithms pick swaps no group would rather abandon for a side deal

*Example (italic):* Exchange programs now arrange a meaningful share of living-donor kidney transplants.

**Key point:** When money is off the table, allocation is still a game — cooperative game theory designs the swaps so that joining the exchange is every pair's best move.

### Visualization (canvas `c1`, 720×300)

Three incompatible donor–patient pairs arranged in a cycle, each donor giving to the next pair's patient.

- **Title (bold 15px, `#1a5276`, top center):** "A Three-Way Kidney Exchange".
- **Pair boxes (three, 170×70, `#fbfcfd` fill, 2px ink border):** "Pair 1" top center (~x=275, y=60), "Pair 2" bottom right (~x=470, y=180), "Pair 3" bottom left (~x=80, y=180); inside each, "donor · patient" 12px `#6b7280` and "incompatible at home" 11px `#6b7280`.
- **Cycle arrows (green `#008300` 3px curved arrows with heads):** Pair 1 donor → Pair 2 patient, Pair 2 donor → Pair 3 patient, Pair 3 donor → Pair 1 patient; each labeled "kidney" bold 12px green.
- **Callout (bold 13px green, centered y=282):** "each donor gives to the next pair's patient — all three patients get a match".

## Matching Doctors to Hospitals

**Tags:** `worked example` (blue), `stable matching` (green)

- **The task** — thousands of graduates, thousands of residency slots, preferences on both sides
- **The danger** — a bad match unravels: a doctor and hospital who prefer each other defect
- **Stability** — a matching no doctor–hospital pair would abandon together is called stable
- **The algorithm** — deferred acceptance: propose, tentatively hold, reject, repeat until quiet
- **Honesty** — applicants can rank truthfully; gaming their list cannot help them

*Example (italic):* The same machinery assigns students to public schools in several large cities.

**Key point:** Stable matching solves markets where prices are absent or fixed: design the procedure so no pair wants to break the outcome, and the market stops unraveling.

### Visualization (canvas `c2`, 720×300)

Three doctors proposing to hospitals under deferred acceptance, one rejection, and the final stable match.

- **Title (bold 15px, `#1a5276`, top center):** "Deferred Acceptance: Propose, Hold, Reject, Settle (illustrative)".
- **Left column (doctor boxes 150×40 at x=90; bold 12px blue names):** Alice (y=70), Bob (y=135), Carol (y=200).
- **Right column (hospital boxes 150×40 at x=480; bold 12px violet):** Hospital 1, Hospital 2, Hospital 3 at the same heights.
- **Arrows:** solid green 3px final matches Alice→Hospital 1, Bob→Hospital 3, Carol→Hospital 2; one dashed red 2px arrow Bob→Hospital 1 crossed with a small ✗ and 11px red note "round 1: rejected — Hospital 1 holds Alice".
- **Callout (bold 13px green, centered y=282):** "stable: no doctor and hospital both prefer each other to what they got".

## Auctioning the Invisible

**Tags:** `where it's used` (blue), `spectrum auctions` (orange)

- **The asset** — radio spectrum: nothing physical changes hands, yet licenses are worth billions
- **The old way** — hearings and lotteries gave licenses away badly and invited lobbying
- **The design** — simultaneous multi-round auctions let prices discover who values what
- **The rules** — activity rules and bid increments blunt collusion, sniping, and stalling
- **The legacy** — the ad auctions running the web are this design's direct descendants

*Example (italic):* Governments worldwide copied the design after early spectrum auctions raised far more than forecast.

**Key point:** An auction is not a sale, it's a designed game — the rules decide whether bidders reveal values honestly, collude quietly, or wait each other out.

### Visualization (canvas `c3`, 720×300)

Two licenses' prices rising over bidding rounds and settling where demand stops.

- **Title (bold 15px, `#1a5276`, top center):** "Multi-Round Bidding Discovers the Price ($M, illustrative)".
- **Axes:** x = rounds 1–10 (ticks 1 / 4 / 7 / 10), y = $0–$100M (labels $30M / $60M / $90M); left margin 80, baseline y=240.
- **License 1 (blue `#2a78d6` 3px step-line):** `[10, 25, 40, 55, 70, 80, 90, 90, 90, 90]` — flat at $90M from round 7, tagged "demand stops → price found" bold 12px blue.
- **License 2 (violet `#4a3aa7` 3px step-line):** `[10, 20, 35, 50, 60, 60, 60, 60, 60, 60]` — flat at $60M from round 5.
- **Callout (bold 13px green, centered y=286):** "prices rise only while two bidders still want the license — revelation by design".

## The Road That Slowed Everyone Down

**Tags:** `common mistake` (red), `braess paradox` (orange)

- **The setup** — 4,000 commuters, two routes, each with one fixed leg and one crowded leg
- **The balance** — traffic splits 2,000/2,000 and every commute takes 65 minutes
- **The gift** — planners add a superfast shortcut connecting the two routes' midpoints
- **The trap** — self-interest funnels everyone through both crowded legs: 80 minutes each
- **The lesson** — capacity changed the game, and the new equilibrium is worse for all

*Example (italic):* Cities have measured traffic improving after closing roads — the paradox running in reverse.

**Key point:** Individually rational route choices settle at an equilibrium, not an optimum — so adding capacity can genuinely make everyone slower. Infrastructure design must solve the game, not the map.

### Visualization (canvas `c4`, 720×300)

The Braess network: two routes with crowded and fixed legs, plus the free shortcut that ruins both.

- **Title (bold 15px, `#1a5276`, top center):** "Adding a Free Shortcut Costs Everyone 15 Minutes (illustrative)".
- **Nodes (filled circles r=12 with bold 12px labels):** START (ink) at (90, 150), A (blue) at (360, 75), B (violet) at (360, 225), END (ink) at (630, 150).
- **Edges (2.5px `#6b7280` with bold 12px `#2c3e50` labels):** START→A "crowded: N/100 min", A→END "fixed: 45 min", START→B "fixed: 45 min", B→END "crowded: N/100 min".
- **Shortcut:** dashed orange `#d95926` 3px arrow A→B labeled "new shortcut: 0 min" bold 12px orange.
- **Result panel (bold 13px, right of center under the network):** "before: split 2,000/2,000 → 65 min each" in green; "after: everyone takes crowded–shortcut–crowded → 80 min" in red `#e74c3c` beneath it.
- **Callout (bold 13px magenta `#d55181`, centered y=286):** "the equilibrium moved — and no driver can do better by switching back alone".

## Regeneration instructions

To rebuild the HTML from this spec:

- **Template:** tutorial detail page. h1 (no index number), `.subtitle`, then one `.card-section` per h2 above, each a `table.layout` row with `.text-col` (50%: `.tags` pills, one-line `<b>`-led bullets, italic `.example`, `.key-point` callout) and `.viz-col` (50%: one 720×300 canvas).
- **Style:** identical skeleton to the game-theory series (07-nash-equilibrium): body system-ui on `#fff`, h1/h2 `#1a5276` with `#2980b9` underline, `.key-point` with red left border, tag pills blue/green/red/orange.
- **Charts:** shared `setup(id)` sizing each canvas to displayed width × `devicePixelRatio` (720×300 logical), draw functions in `__charts`, debounced resize redraw. Palette `P` as in the series. No `Math.random()`; auction arrays and the Braess numbers (2,000/2,000 split, 65 vs 80 minutes, N/100 legs, 45-minute fixed legs) hardcoded and matching the text.
- **Naming:** fictional people follow the Alice/Bob convention (doctors Alice, Bob, Carol); institutions use descriptive labels (Hospital 1/2/3, Pair 1/2/3, License 1/2).
