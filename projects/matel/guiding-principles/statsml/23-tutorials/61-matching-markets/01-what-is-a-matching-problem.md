# What Is a Matching Problem

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** What Is a Matching Problem

**Subtitle:** Two sides each rank the other, no prices change hands — an algorithm, not money, decides who pairs with whom

**Running data (illustrative), shared across this tutorial series — a small job market, 4 candidates × 4 companies (one opening each):**

| Candidate | Ranks companies | Company | Ranks candidates |
|-----------|-----------------|---------|------------------|
| Alice     | A > B > C > D   | A       | Dan > Alice > Bob > Carol |
| Bob       | A > D > C > B   | B       | Carol > Alice > Dan > Bob |
| Carol     | B > A > C > D   | C       | Alice > Bob > Carol > Dan |
| Dan       | A > B > C > D   | D       | Bob > Alice > Carol > Dan |

First choices: Alice, Bob, and Dan all rank Company A first; Carol ranks B first. 4! = 24 complete pairings; n! for n=2..8 is 2, 6, 24, 120, 720, 5040, 40320.

## Two Sides, Each With a List

**Tags:** `core idea` (blue), `two-sided market` (green)

- **The setup** — four candidates and four companies (A–D), one opening at each company
- **Both sides rank** — Alice's list is A > B > C > D; Company A's list is Dan > Alice > Bob > Carol
- **No prices** — salary is fixed by policy, so nobody can bid a candidate away from a rival
- **The task** — an algorithm reads all eight lists and decides who pairs with whom
- **The contrast** — in an ordinary market prices do the allocating; here the lists do all the work

*Example (italic):* At a fruit stall the highest payer gets the last mango; in this job market nobody can outbid anyone — only the eight ranked lists decide.

**Key point:** A matching problem has two sides that rank each other and no price to clear the market — an algorithm, not money, decides who pairs with whom.

### Visualization (canvas `c1`, 720×300)

Left half: two columns of labeled dots (candidates left, companies right) with each candidate's first-choice arrow. Right half: the eight preference lists drawn as two small bordered grids.

- **Title (bold 15px, `#1a5276`, top center):** "One Job Market, Eight Ranked Lists (illustrative)".
- **Divider:** 1px `#e5e9ef` vertical line at x=350 from y=44 to y=262.
- **Dots:** candidates at x=95, companies at x=265, rows y = 80/126/172/218 for Alice/Bob/Carol/Dan and A/B/C/D; filled circles radius 7, candidates blue `#2a78d6`, companies green `#008300`.
- **Dot labels (bold 12px `#2c3e50`):** candidate names right-aligned at x=82 (+4 vertical offset); company names ("Co. A" ... "Co. D") left-aligned at x=278.
- **First-choice arrows (1.5px blue `#2a78d6`, filled arrowhead at company end):** Alice(80)→A(80), Bob(126)→A(80), Carol(172)→B(126), Dan(218)→A(80); arrows run from x=104 to x=254 at interpolated y.
- **Annotation (bold 12px orange `#d95926`, centered at (175, 252)):** "3 of 4 first choices point at A".
- **Right grid 1 (candidates rank companies):** bordered box 1px `#e5e9ef` at x=372, y=52, width 326, height 96; header bold 12px blue `#2a78d6` "candidates rank companies" left-aligned at (384, 70); rule 1px `#e5e9ef` at y=78; rows 12px `#2c3e50` left-aligned at x=384, y = 94/110/126/142: "Alice: A > B > C > D", "Bob: A > D > C > B", "Carol: B > A > C > D", "Dan: A > B > C > D".
- **Right grid 2 (companies rank candidates):** bordered box 1px `#e5e9ef` at x=372, y=158, width 326, height 96; header bold 12px green `#008300` "companies rank candidates" left-aligned at (384, 176); rule 1px `#e5e9ef` at y=184; rows 12px `#2c3e50` left-aligned at x=384, y = 200/216/232/248: "A: Dan > Alice > Bob > Carol", "B: Carol > Alice > Dan > Bob", "C: Alice > Bob > Carol > Dan", "D: Bob > Alice > Carol > Dan".
- **Caption (12px `#6b7280`, centered at y=288):** "arrows show each candidate's first choice — the full lists on the right drive everything".

## One Skeleton, Many Costumes

**Tags:** `where it's used` (blue), `problem family` (green)

- **The original** — the 1962 framing paired two sides in a "marriage market" thought experiment
- **Jobs** — candidates and companies, exactly our running example
- **Schools** — students rank schools, schools prioritize students, and seats are the capacity
- **Kidneys** — patients and donor kidneys, where compatibility plays the role of preference
- **Rides and ads** — riders to drivers, ads to impressions — matched by the millions every day

*Example (italic):* A school with 30 seats is just Company B with 30 openings — the costume changes, the skeleton does not.

**Key point:** Marriage, jobs, schools, kidneys, rides, ads — one recurring structure underneath: two sides, preferences over the other side, and a capacity per participant.

### Visualization (canvas `c2`, 720×300)

A four-row skeleton (side 1 / side 2 / preferences / capacity) mapped across three settings, drawn as a light grid.

- **Title (bold 15px, `#1a5276`, top center):** "Same Skeleton, Three Costumes".
- **Column headers (bold 13px, centered at y=58):** "job market" blue `#2a78d6` at x=258, "school choice" green `#008300` at x=443, "kidney exchange" violet `#4a3aa7` at x=628.
- **Row labels (bold 12px ink `#1a5276`, right-aligned at x=150):** "side 1", "side 2", "preferences", "capacity" at row centers y = 92/134/176/218 (+4 vertical offset).
- **Grid lines (1px `#e5e9ef`):** horizontals at y = 68/110/152/194/236 from x=40 to x=705; verticals at x = 160/350/535 from y=68 to y=236.
- **Cell text (12px `#2c3e50`, centered per column at row centers +4):**
  - side 1: "candidates" / "students" / "patients"
  - side 2: "companies" / "schools" / "donor kidneys"
  - preferences: "both sides rank each other" / "students rank, schools prioritize" / "compatibility, wait time"
  - capacity: "one opening per company" / "seats per school" / "one kidney per patient"
- **Column 2 preferences cell is the longest string — render it 11px if 12px overflows 180px (it does):** "students rank, schools prioritize" at 11px.
- **Caption (bold 12px violet `#4a3aa7`, centered at y=266):** "change the costume, keep the skeleton — two sides, preferences, capacity".
- **Sub-caption (12px `#6b7280`, centered at y=288):** "the same algorithms serve every column".

## Why Four Candidates Are Already Tricky

**Tags:** `worked example` (blue), `combinatorics` (orange)

- **The crowd at the top** — Alice, Bob, and Dan all rank Company A first, but A has one opening
- **The cascade** — whoever loses A must take a lower choice, bumping someone else down in turn
- **Count the pairings** — 4 candidates and 4 companies allow 4! = 24 complete pairings
- **It explodes** — 5 gives 120, 6 gives 720, 8 gives 40,320 — brute force dies almost immediately
- **National scale** — thousands of doctors and hospitals: astronomically many pairings to sift

*Example (italic):* Give A to Bob and both Alice and Dan slide down their lists — one assignment ripples through everyone else's options.

**Key point:** Every pairing decision cascades into everyone else's options, and the count of possible pairings grows as n! — checking them all stops being an option almost immediately.

### Visualization (canvas `c3`, 720×300)

A bar chart of n! for n=2..8 on a log-height scale, with the n=4 bar highlighted as our market.

- **Title (bold 15px, `#1a5276`, top center):** "The Number of Complete Pairings Explodes".
- **Scale note (11px `#6b7280`, left-aligned at (60, 52)):** "bar height on log scale".
- **Axis:** 1px `#999`, y from (60, 70) to (60, 232), x from (60, 232) to (700, 232).
- **Bars:** n = 2..8 at x = 85/173/261/349/437/525/613, width 60; values `[2, 6, 24, 120, 720, 5040, 40320]`; heights proportional to log10(value) `[0.301, 0.778, 1.380, 2.079, 2.857, 3.702, 4.606]` scaled over 162px with max 4.7; fill `rgba(42,120,214,0.5)` with 1px blue `#2a78d6` stroke, except the n=4 bar: fill `rgba(217,89,38,0.5)` with 1px orange `#d95926` stroke.
- **Value labels (bold 12px, centered above each bar top − 8):** "2", "6", "24", "120", "720", "5,040", "40,320"; blue `#2a78d6`, except "24" in orange `#d95926`.
- **Our-market annotation (bold 12px orange `#d95926`, centered at (291, 78)):** "our 4×4 market"; 1.5px orange line from (291, 86) down to (291, bar-top − 24) with a filled downward arrowhead.
- **X labels (12px `#444`, centered at y=250):** "n=2" ... "n=8" under each bar.
- **Caption (12px `#6b7280`, centered at y=288):** "n! complete pairings for n candidates and n companies — brute-force checking dies fast".

## Matching Is Not Ranking

**Tags:** `common mistake` (red), `ranking vs matching` (orange)

- **Ranking** — orders ONE list by one score; the output is a sorted column, nothing more
- **Matching** — must respect TWO sides of preferences plus a one-partner-each capacity
- **Why sorting fails** — a sorted list of candidates still cannot seat three of them at Company A
- **Capacity binds** — Alice, Bob, and Dan all pick A by their own lists; only one can have it
- **Different outputs** — a ranking is an ordered list; a matching is a set of pairs

*Example (italic):* Sorting candidates by grade gives an order; it says nothing about who ends up where when three people's first choice is the same company.

**Common mistake:** Treating a matching problem as a ranking problem. Sorting one list ignores the other side's preferences and the one-seat capacity — a matching's output is a set of pairs, not an order.

### Visualization (canvas `c4`, 720×300)

Two panels: ranking as one sorted column of boxes on the left, matching as two dot columns joined by pairing lines on the right.

- **Title (bold 15px, `#1a5276`, top center):** "Sorting One List vs Pairing Two Sides".
- **Divider:** 1px `#e5e9ef` vertical line at x=360 from y=44 to y=250.
- **Left header (bold 13px violet `#4a3aa7`, centered at (180, 58)):** "RANKING — order one list".
- **Left boxes (x=110, width 140, height 30; y = 76/116/156/196):** "1. Alice", "2. Bob", "3. Carol", "4. Dan"; fill `#fbfcfd`, 2px violet `#4a3aa7` border, centered 12px `#2c3e50` text at box middle +4; 1.5px mute `#6b7280` down arrows between boxes (from box bottom +2 to next top −2, x=180, filled arrowhead).
- **Left footer (bold 12px violet `#4a3aa7`, centered at (180, 246)):** "output: an ordered list".
- **Right header (bold 13px green `#008300`, centered at (545, 58)):** "MATCHING — pair two sides".
- **Right dots:** candidates at x=455, companies at x=645, rows y = 84/126/168/210 for Alice/Bob/Carol/Dan and A/B/C/D; radius 6 circles, candidates blue `#2a78d6`, companies green `#008300`; labels bold 11px `#2c3e50`, names right-aligned at x=443 (+4), company letters left-aligned at x=657 (+4).
- **Pairing lines (2.5px green `#008300`):** Alice(84)–C(168), Bob(126)–D(210), Carol(168)–B(126), Dan(210)–A(84); lines run x=463 to x=637.
- **Right footer (bold 11px `#6b7280`, centered at (545, 232)):** "(one of the 24 possible pairings)".
- **Right footer 2 (bold 12px green `#008300`, centered at (545, 246)):** "output: a set of pairs".
- **Warning line (bold 12px red `#e74c3c`, centered at y=270):** "sorting one list ignores the other side and the one-seat capacity".
- **Caption (12px `#6b7280`, centered at y=288):** "a ranking says who is best — a matching says who goes where".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale(...)`; charts stored in an array, redrawn on debounced window resize; shared `arrowHead(ctx, x, y, dir, color)` helper for filled arrowheads ('right' and 'down').
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red `#e74c3c` only for the ranking-vs-matching warning in c4.
- **Data integrity:** hardcoded arrays only — candidate lists `Alice: A>B>C>D, Bob: A>D>C>B, Carol: B>A>C>D, Dan: A>B>C>D`; company lists `A: Dan>Alice>Bob>Carol, B: Carol>Alice>Dan>Bob, C: Alice>Bob>Carol>Dan, D: Bob>Alice>Carol>Dan`; factorials `[2, 6, 24, 120, 720, 5040, 40320]` with log10 heights `[0.301, 0.778, 1.380, 2.079, 2.857, 3.702, 4.606]`; c4 pairing `Alice–C, Bob–D, Carol–B, Dan–A`; invented setup carries "(illustrative)" in the c1 chart title.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
