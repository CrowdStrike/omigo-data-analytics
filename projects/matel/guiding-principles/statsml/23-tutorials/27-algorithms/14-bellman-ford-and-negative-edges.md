# Bellman-Ford & Negative Edges

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Bellman-Ford & Negative Edges

**Subtitle:** When some roads cost money and some roads pay you, the safe way to find the cheapest route is to re-check every road again and again — and if re-checking never stops paying off, you have found a money loop

## A Courier Map Where One Road Pays You

**Tags:** `core idea` (blue), `negative edges` (orange), `relax every edge` (green)

- **The courier** — a bike courier leaves Home each morning and wants the cheapest route to the Office
- **Road costs** — most streets cost money: Home→Bakery $4, Home→Market $2, Market→Office $2, Bakery→Office $6
- **The paying road** — the Bakery→Market street has a pickup that pays $3, so that leg costs −$3
- **Greedy fails** — a greedy planner locks in "Market for $2" early and never sees the −$3 shortcut behind it
- **The fix** — Bellman-Ford never locks anything in: it re-checks (relaxes) every road, over and over
- **Relaxing** — one relax asks: "is reaching this corner cheaper through that road?" — if yes, update the price

*Example (italic):* Home→Bakery→Market costs 4 − 3 = $1, cheaper than the direct $2 road — but only a planner that keeps re-checking roads after Market "looks done" ever finds it.

**Key point:** Negative edges break greedy lock-in shortest-path methods; Bellman-Ford stays correct by refusing to lock in — it just relaxes every edge repeatedly.

### Visualization (canvas `c1`, 720×300)

Single-panel road map: four labeled nodes with five directed edges, dollar costs on each edge, the paying edge in green, the true cheapest route highlighted and the greedy trap route dashed.

- **Title (bold 15px, `#1a5276`, top center):** "The Courier's Map: One Street Pays −$3".
- **Nodes:** circles radius 26, fill white, 3px `#1a5276` stroke, bold 13px `#1a5276` two-line labels — Home at (110, 165), Bakery at (330, 85), Market at (330, 235), Office at (590, 165).
- **Edges (2px lines with small arrowheads, bold 13px cost labels at midpoints):** Home→Bakery "$4" (`#2a78d6`), Home→Market "$2" (`#6b7280`, dashed 6/4), Bakery→Market "−$3" (`#008300`, 3px), Market→Office "$2" (`#2a78d6`), Bakery→Office "$6" (`#6b7280`).
- **Route highlight:** soft glow under Home→Bakery→Market→Office (8px `rgba(42,120,214,0.18)` stroke beneath those three edges); 12px `#2a78d6` label under Office: "cheapest: 4 − 3 + 2 = $3".
- **Greedy trap label:** 12px `#6b7280` label under the Home→Market edge: "greedy grabs this $2 first".
- **Annotation (bold 13px green `#008300`, near x=430, y=280):** "the −$3 street makes Market cost $1, not $2".
- **Caption (12px `#444`, bottom right):** "illustrative courier map".

## Three Rounds of Relaxing Every Edge

**Tags:** `worked example` (blue), `rounds` (green), `stop early` (orange)

- **Setup** — Home starts at $0, every other corner starts at ∞; the edge list holds all 5 roads
- **Round 1** — relax all 5 edges once: Market drops to $2 then $1 (via Bakery), Bakery $4, Office $4
- **Round 2** — relax all 5 again: Office improves through the cheaper Market, $1 + $2 = $3
- **Round 3** — relax all 5 again: nothing changes, so the answers are final and the loop stops
- **The guarantee** — with V corners, V−1 rounds always suffice; here V−1 = 3 and we finished in 2
- **By hand** — the whole run is 15 tiny "is it cheaper through here?" checks anyone can redo on paper

*Example (italic):* Office is first reached for $4 (Home→Market→Office at old prices), then round 2 re-checks Market→Office and cuts it to $3.

**Key point:** Final prices — Bakery $4, Market $1, Office $3; a round where no edge relaxes means every price is already the cheapest possible.

### Visualization (canvas `c2`, 720×300)

Grouped bar chart: cheapest-known price for Bakery, Market, and Office after each of three rounds, showing round 1 finding most of it, round 2 fixing Office, and round 3 changing nothing.

- **Title (bold 15px, `#1a5276`, top center):** "Cheapest Known Price After Each Round of Relaxation".
- **Axes:** origin x=70, baseline y=245, plot width 590, plot height 185; y = dollars 0 to 5, 12px `#444` tick labels "$0"–"$5" every $1, light `#e5e9ef` gridlines; x = three groups centered at x = 180, 380, 580 with bold 13px `#444` labels "after round 1", "after round 2", "after round 3".
- **Bars (each group has three 42px-wide bars, 10px gaps):** Bakery blue `#2a78d6`, Market green `#008300`, Office orange `#d95926`; heights from values Bakery `[4, 4, 4]`, Market `[1, 1, 1]`, Office `[4, 3, 3]`.
- **Value labels:** bold 12px, bar color, atop each bar: "$4 $1 $4", "$4 $1 $3", "$4 $1 $3".
- **Legend (12px, top left inside plot):** color swatches with "Bakery", "Market", "Office".
- **Change marker:** 13px bold `#d95926` down-arrow glyph above the round-2 Office bar with label "$4 → $3".
- **Annotation (bold 12px `#4a3aa7` violet, above the round-3 group, y=70):** two lines: "no change in round 3 —" / "prices are final, stop".
- **Caption (12px `#444`, bottom right):** "illustrative — 4 corners, so at most 3 rounds needed".

## Money Loops: Arbitrage as a Negative Cycle

**Tags:** `where it's used` (blue), `arbitrage` (orange), `negative cycle` (red)

- **Currencies as corners** — USD, EUR, GBP become nodes; each exchange rate becomes a road between them
- **The log trick** — multiplying rates is awkward, so each edge gets weight −log(rate); products become sums
- **The rates** — USD→EUR at 0.90, EUR→GBP at 0.90, GBP→USD at 1.25; weights 0.105, 0.105, −0.223
- **The loop** — the weights sum to −0.013, a negative cycle: going around the loop multiplies money by 1.0125
- **Detection** — run one extra round after V−1; if any edge still relaxes, a negative cycle exists
- **In money** — $100 → €90 → £81 → $101.25: each lap around the loop mints $1.25 from nothing

*Example (italic):* No single exchange looks special — 0.90, 0.90, 1.25 — yet the three together form a loop whose −log weights sum below zero, and Bellman-Ford's extra round flags it.

**Key point:** A negative cycle in the −log(rate) graph is exactly an arbitrage loop, and Bellman-Ford's "does anything still relax after V−1 rounds?" test finds it.

### Visualization (canvas `c3`, 720×300)

Currency triangle: three nodes in a cycle with each edge labeled by its rate and its −log weight, the running dollar amount shown at each hop, and the negative loop sum called out.

- **Title (bold 15px, `#1a5276`, top center):** "Three Ordinary Rates, One Money Loop".
- **Nodes:** circles radius 30, white fill, 3px `#1a5276` stroke, bold 14px `#1a5276` labels — USD at (200, 100), EUR at (520, 100), GBP at (360, 245).
- **Edges (3px `#2a78d6` curved arrows clockwise USD→EUR→GBP→USD, small arrowheads):** two-line labels at midpoints, 12px — top line bold `#2c3e50` rate ("×0.90", "×0.90", "×1.25"), bottom line `#6b7280` weight ("w = 0.105", "w = 0.105", "w = −0.223"); the GBP→USD weight line in bold green `#008300`.
- **Running money:** bold 12px `#c98500` labels just outside each node: "$100 start" by USD, "€90" by EUR, "£81" by GBP.
- **Loop sum box:** rounded rectangle near (80, 245), 1.5px `#008300` border, bold 13px `#008300` two-line text: "loop sum −0.013 < 0" / "one lap: $100 → $101.25".
- **Annotation (bold 12px `#d95926`, near x=560, y=270):** "extra round still relaxes → cycle flagged".
- **Caption (12px `#444`, bottom right):** "illustrative rates".

## Negative Edges vs Negative Cycles

**Tags:** `common mistake` (red), `no bottom` (orange)

- **Two different things** — a negative edge is one paying road; a negative cycle is a loop that pays overall
- **Edges are fine** — Bellman-Ford handles any mix of negative edges and returns true cheapest prices
- **Cycles are not** — with a reachable negative cycle, every extra lap cuts the cost by the loop's sum
- **No answer exists** — a Bakery↔Market loop summing to −$1 turns a $3 route into $2, $1, $0, −$1, ...
- **The mistake** — feeding negative edges to a greedy planner: it returns a wrong number, silently

*Example (italic):* On the courier map the greedy planner reports Office for $4 and looks perfectly confident — the correct $3 answer needed the re-check it never does.

**Common mistake:** Treating "has a negative edge" and "has a negative cycle" as the same problem — the first just needs Bellman-Ford; the second means "cheapest path" has no answer at all, and the algorithm's job is to say so.

### Visualization (canvas `c4`, 720×300)

Single-panel line chart: cost of the courier's best route as it takes extra laps around a hypothetical −$1 Bakery↔Market loop, a straight staircase heading down with no floor.

- **Title (bold 15px, `#1a5276`, top center):** "A −$1 Loop Has No Bottom".
- **Axes:** origin x=70, baseline y=240, plot width 580, plot height 180; x = laps around the loop `[0, 1, 2, 3, 4, 5]` with 12px `#444` tick labels "0 laps"–"5 laps"; y = route cost −3 to 4, 12px `#444` tick labels "−$3" to "$4" every $1, light `#e5e9ef` gridlines, the $0 gridline solid `#999`.
- **Cost line:** red `#e74c3c` 3px line with 6px dots through points (laps, cost) = `[0, 3], [1, 2], [2, 1], [3, 0], [4, −1], [5, −2]`; bold 12px red value labels "$3", "$2", "$1", "$0", "−$1", "−$2" beside the dots.
- **Reference line:** horizontal dashed `#2a78d6` (dash 4/3) line at cost $3 with 12px `#2a78d6` label at its right end: "honest answer without the loop: $3".
- **Fade cue:** 12px `#6b7280` italic label past the last dot near x=630: "...and on forever".
- **Annotation (bold 13px red `#e74c3c`, near x=250, y=95):** two lines: "every lap subtracts $1 —" / "'cheapest' stops meaning anything".
- **Caption (12px `#444`, bottom right):** "illustrative — loop sum −$1 per lap".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`. Red reserved for the no-bottom cycle chart (a genuine failure state).
- **Data:** all edge costs, round-by-round prices, exchange rates, −log weights, and lap costs are the hardcoded literal values above (no randomness); −log weights are natural-log values rounded to 3 decimals (−ln 0.90 = 0.105, −ln 1.25 = −0.223, sum −0.013); text numbers and chart numbers must stay identical.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
