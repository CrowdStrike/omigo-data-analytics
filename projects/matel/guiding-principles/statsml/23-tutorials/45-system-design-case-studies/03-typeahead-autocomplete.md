# Typeahead Autocomplete

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Typeahead Autocomplete

**Subtitle:** A search box that suggests "pizza near me" before you finish typing "piz" — every keystroke is a query, and each answer has to land in about 50 milliseconds

## Every Keystroke Is a Search Query

**Tags:** `core idea` (blue), `latency budget` (green), `client tricks` (orange)

- **The box** — a user types "piz" into a search box and five suggestions appear before the next letter
- **Keystroke = query** — typing "pizza" fires a request per letter; a 10-letter search is 10 queries
- **The budget** — a suggestion arriving after ~100 ms feels laggy, so the target is ~50 ms end to end
- **Debounce** — the client waits ~50 ms after each keystroke; fast typing collapses "pizz" into "pizza"
- **Client cache** — backspacing to "piz" reuses the answer already fetched, no new request goes out

*Example (italic):* Typing "pizza" at speed sends four requests, not five — "pizz" is debounced away — and every answer lands in under 50 ms.

**Key point:** Typeahead multiplies traffic — every keystroke is a full query — so the whole design bends around answering each one in a few tens of milliseconds.

### Visualization (canvas `c1`, 720×300)

Bar chart of server response time per keystroke while typing "pizza", with the debounced keystroke shown as a skipped slot and the 100 ms "feels laggy" line above all bars.

- **Title (bold 15px, `#1a5276`, top center):** "Typing 'pizza': Four Requests, Each Answered in Under 50 ms".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = milliseconds 0 to 150, gridlines `#e5e9ef` at 50 and 100 with 12px `#444` labels "50 ms" / "100 ms"; x = five slots centered at x = 120, 240, 360, 480, 600 with 13px `#444` labels `["p", "pi", "piz", "pizz", "pizza"]`.
- **Bars:** width 70px, blue `#2a78d6` fill `rgba(42,120,214,0.35)` with 2px solid tops, heights from times `[38, 41, 36, null, 37]` ms — 12px `#2c3e50` value labels "38 ms" etc. above each bar.
- **Debounced slot ("pizz"):** no bar; a 70×30 dashed `#6b7280` (dash 4/3) outline box sitting on the baseline with 11px `#6b7280` label "debounced — never sent" above it.
- **Laggy line:** red `#e74c3c` dashed (dash 6/4) horizontal line at y for 100 ms, 12px red label "feels laggy above 100 ms" at its right end.
- **Annotation (bold 13px green `#008300`, near x=300, y=85):** "every answer under 50 ms".
- **Caption (12px `#444`, bottom right):** "timings illustrative".

## A Tree That Already Knows the Answer

**Tags:** `worked example` (blue), `trie` (green), `precomputed top-k` (orange)

- **The trie** — a tree of letters: root → p → pi → piz; every stored query is a path from the root
- **Top-k on the node** — node "piz" stores its 3 best completions, precomputed, sorted by frequency
- **The list** — "pizza near me" (14,200), "pizza dough recipe" (9,800), "pizza oven for sale" (7,400)
- **The lookup** — walk 3 edges for "piz", then read the stored list: O(prefix length) plus a constant
- **Hand-check** — no ranking happens at query time; the server does 3 hops and one read, nothing else

*Example (italic):* For "piz" the server touches four nodes — root, p, pi, piz — and returns the three completions already sitting on the last one.

**Key point:** Precomputing top-k at every node moves all ranking work offline — the query path is a pointer walk plus one array read, easily inside the 50 ms budget.

### Visualization (canvas `c2`, 720×300)

Trie walk diagram: four node boxes chained left to right (root → p → pi → piz), with the "piz" node opening into a stored top-3 list panel on the right.

- **Title (bold 15px, `#1a5276`, top center):** "Lookup for 'piz': 3 Hops, Then Read the List Already on the Node".
- **Node boxes (y=130, 70px wide, 44px tall, 8px radius, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border, 13px `#2c3e50` centered text):** "root" at x=50, "p" at x=180, "pi" at x=310, "piz" at x=440; the "piz" box gets a 3px `#008300` border instead.
- **Hop arrows:** 3px `#2a78d6` arrows between consecutive boxes, each with a bold 12px `#2a78d6` label "hop 1" / "hop 2" / "hop 3" above the arrow.
- **Top-3 panel:** rounded box at x=545, y=70, 165px wide, 120px tall, fill `rgba(0,131,0,0.10)`, 2px `#008300` border; 12px bold `#008300` header "stored top-3 (by freq)"; three 12px `#2c3e50` rows: "pizza near me — 14,200", "pizza dough recipe — 9,800", "pizza oven for sale — 7,400"; 2px `#008300` arrow from the "piz" box to the panel labeled "one read" (bold 12px green).
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=245):** "no ranking at query time — the answers were computed before the user typed".
- **Caption (12px `#444`, bottom right):** "query frequencies illustrative".

## Yesterday's Queries Rank Today's Suggestions

**Tags:** `where it's used` (blue), `offline batches` (green), `sharding` (orange)

- **Ranking signal** — suggestions are ordered by how often people historically searched each query
- **Nightly batch** — the day's query log (900M queries, illustrative) is counted into a fresh trie
- **The lag** — today's searches shape tomorrow's suggestions; the batch trie is about a day stale
- **Trending lane** — a small delta trie built from the last 15 minutes of queries catches spikes
- **Prefix sharding** — servers split the alphabet (a–f, g–p, q–z); "piz" always routes to one shard

*Example (italic):* A 9am news event reaches the trending overlay by 9:15, but enters the main trie only after tonight's rebuild.

**Key point:** Freshness is layered — a big, cheap, day-old batch trie does most of the ranking, while a tiny fast lane covers what changed in the last few minutes.

### Visualization (canvas `c3`, 720×300)

Two-lane pipeline flow diagram (slow batch lane and fast trending lane) feeding a row of three prefix shards, with the "piz" request routed to its shard.

- **Title (bold 15px, `#1a5276`, top center):** "Two Update Lanes, Three Prefix Shards".
- **Box style:** 40px tall, 8px radius, 12px `#2c3e50` text; batch boxes fill `rgba(42,120,214,0.15)` with 2px `#2a78d6` border, trending boxes fill `rgba(217,89,38,0.12)` with 2px `#d95926` border, shard boxes fill `rgba(0,131,0,0.10)` with 2px `#008300` border.
- **Row 1 (y=70), 12px `#444` lane label "batch — nightly" at x=15:** blue boxes "day's query log (900M)" at x=150 (170px wide) → "count + rank per prefix" at x=350 (170px wide) → "rebuild trie" at x=550 (130px wide), joined by 3px `#2a78d6` arrows.
- **Row 2 (y=140), lane label "trending — minutes" at x=15:** orange boxes "last 15 min of queries" at x=150 (170px wide) → "small delta trie" at x=380 (150px wide), 3px `#d95926` arrow between them.
- **Merge arrows:** 3px arrows from "rebuild trie" (blue) and "small delta trie" (orange) down to the shard row, meeting above shard 2; bold 12px `#d95926` label "merged at serve time" beside the orange arrow.
- **Row 3 (y=215):** three green shard boxes, 140px wide, at x=110 "shard 1: a–f", x=290 "shard 2: g–p", x=470 "shard 3: q–z"; bold 12px `#008300` label under shard 2 at y=270: "'piz' always routes here".
- **Caption (12px `#444`, bottom right):** "volumes and lane timings illustrative".

## Ranking at Query Time Blows the Budget

**Tags:** `common mistake` (red), `memory vs latency` (orange)

- **The temptation** — skip the stored lists and rank live: find all completions, sort, return 3
- **The blowup** — the prefix "p" has 1.2M completions (illustrative); scanning them takes ~400 ms
- **Worst where it hurts** — short prefixes have the biggest subtrees and also the most traffic
- **The trade** — storing top-3 on every node grows the trie from 4 GB to 10 GB of memory
- **The win** — lookup falls from ~400 ms to ~2 ms; memory is paid once, latency every keystroke

*Example (italic):* At the one-letter prefix "p", the precomputed read returns in 2 ms where a live scan-and-sort of 1.2M completions took 400 ms.

**Common mistake:** Ranking at query time. It looks simpler, but it is slowest exactly on the shortest, highest-traffic prefixes — 2.5× more memory for the stored lists buys a 200× faster lookup.

### Visualization (canvas `c4`, 720×300)

Horizontal bar chart in two groups: lookup latency for prefix "p" (rank-at-query-time vs precomputed) and total trie memory (plain vs with top-3 lists).

- **Title (bold 15px, `#1a5276`, top center):** "The Trade at Prefix 'p': 2.5× the Memory for a 200× Faster Lookup".
- **Axis:** vertical 2px `#999` baseline at x=250, bars extend right, max width 430; latency and memory groups use independent pixel scales (schematic, not one shared axis).
- **Group label (bold 13px `#1a5276`, at x=20, y=60):** "lookup latency, prefix 'p'"; rows at y = 85 and 125, left-aligned 12px `#444` labels at x=20:
  - "rank at query time — scan 1.2M": red `#e74c3c` bar width 430, 11px red label "400 ms" at bar end
  - "precomputed top-3 — one read": green `#008300` bar width 3, 11px green label "2 ms" at bar end
- **Group label (bold 13px `#1a5276`, at x=20, y=180):** "trie memory"; rows at y = 205 and 245:
  - "plain trie, no stored lists": blue `#2a78d6` bar width 130, 11px `#444` label "4 GB"
  - "trie + top-3 on every node": orange `#d95926` bar width 325, 11px `#444` label "10 GB"
- **Bar style:** 16px tall, fills solid at 0.85 alpha with 2px solid borders in the same hue.
- **Annotation (bold 13px magenta `#d55181`, right side near y=160):** "memory is paid once — latency is paid on every keystroke".
- **Caption (12px `#444`, bottom right):** "all sizes and timings illustrative; bar widths schematic".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); keystroke timings, query frequencies (14,200 / 9,800 / 7,400), log volume (900M), completion count (1.2M), latencies (400 ms / 2 ms), and memory sizes (4 GB / 10 GB) are invented and labeled illustrative; the same numbers must appear in both text and charts.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
